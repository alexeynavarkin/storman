# Database schema

PostgreSQL — единственная внешняя зависимость storman. Выбор обоснован:

- Конкурентные writer'ы (Web + FTPS + WebDAV + воркеры индексации одновременно).
- JSONB + GIN для расширяемых метаданных.
- `ltree` для запросов по дереву (materialized path).
- Партиционирование истории (`outbox_history`, `jobs_history`, `audit_log`).
- `citext` для case-insensitive логинов.
- `pgcrypto.gen_random_uuid()` для PK.

Подключение — `pgx/v5 + pgxpool`. Миграции — embedded SQL через `golang-migrate`, файлы в [internal/migrations/sql/](../../internal/migrations/sql/), zero-padded prefix.

## Ключевые таблицы

### `nodes` — дерево файлов и папок

```sql
id            uuid primary key                  -- стабильный ID узла, не меняется при rename/move
parent_id     uuid references nodes(id)        -- родительская папка; NULL только для корня
path          ltree not null                    -- materialized path 'root.docs.report'; gist-индекс для subtree-запросов
name          text not null                     -- имя узла в родителе (последний сегмент path)
type          node_type not null                -- enum: 'file' | 'dir'
backend_kind  text                              -- двойная семантика: у файла — фактический бэкенд (NOT NULL, immutable);
                                                --                    у папки — политика для НОВЫХ детей (NULL = наследовать)
backend_ref   text                              -- backend-specific: для 'flat' — relative path в flat-storage/
                                                --                    для 'cdc'  — manifest id (будущее)
size          bigint                            -- байт; NULL для папок и pending-файлов; заполняется на Commit
mime          text                              -- определяется по magic bytes (НЕ по Content-Type клиента)
mtime         timestamptz                       -- mtime содержимого; обновляется на каждом успешном Commit
sha256        bytea                             -- 32 байта; дублируется из node_meta для быстрого lookup
status        node_status not null              -- enum: 'pending' | 'ready' | 'deleted' | 'broken'
created_at    timestamptz default now()
updated_at    timestamptz default now()
deleted_at    timestamptz                       -- момент перевода в корзину; GC корзины работает по этому полю
unique (parent_id, name)

check (type <> 'file' or (backend_kind is not null and backend_ref is not null))
check (type <> 'dir'  or backend_ref is null)
```

**Индексы:**
- `gist(path)` — ancestor/descendant, `path <@ root` для subtree-операций (корзина, удаление, ACL traverse).
- `btree(sha256)` — поиск дубликатов.
- `btree(parent_id)` — listing.
- `partial btree(deleted_at) WHERE deleted_at IS NOT NULL` — GC корзины.

**Статусы:**
- `pending` — upload идёт, файла нет в целевом пути ещё (`backend_ref` указывает на staging или будущее место).
- `ready` — файл опубликован, контент на месте.
- `deleted` — soft-delete, узел в корзине; `deleted_at` non-NULL.
- `broken` — recovery-флаг: мета есть, контента нет (`fsck` ставит).

### `node_meta` — типизированные частые поля + JSONB

```sql
node_id      uuid primary key references nodes(id) on delete cascade
width        int                              -- ширина изображения/видео в пикселях
height       int
taken_at     timestamptz                      -- EXIF DateTimeOriginal (≠ nodes.mtime для импортированных)
lat          double precision                 -- GPS из EXIF (-90..90)
lng          double precision                 -- GPS из EXIF (-180..180)
duration_ms  int                              -- видео/аудио длительность
extra        jsonb not null default '{}'      -- редкие/расширяемые поля по namespace ('exif.iso', 'ai.faces')
```

**Индексы:**
- `gin(extra jsonb_path_ops)` — поиск по произвольным JSONB-полям.
- `btree(taken_at)` — сортировка фотогалереи по времени съёмки.
- `btree(lat, lng)` — bounding-box запросы (PostGIS опционально).

**Принцип:** часто запрашиваемые поля — в типизированные колонки (можно индексировать, range-запросы); редкие/кастомные — в `extra` JSONB. Переезд из JSONB в колонку — миграция, когда поле становится горячим.

### `permissions` — только явные права (RBAC)

```sql
id         bigserial primary key
node_id    uuid references nodes(id) on delete cascade
user_id    uuid references users(id) on delete cascade
actions    bit(8) not null                  -- битмаска: bit 0=Read, 1=Write, 2=Remove, 3=Admin; 4-7 reserved
created_at timestamptz default now()
unique (node_id, user_id)
```

Виртуальные `Traverse` тут **не хранятся** — вычисляются на лету (см. [ADR-0003](../adr/0003-rbac-computed-traverse.md), [rbac.md](rbac.md)).

### `share_links` — анонимные временные ссылки (только Web)

```sql
token       text primary key                 -- 32 байта crypto/rand → base64url; PK для O(1) lookup
node_id     uuid references nodes(id) on delete cascade
actions     bit(8) not null                  -- те же биты что в permissions; Admin-биты запрещены проверкой в коде
expires_at  timestamptz not null             -- обязательное поле — бессрочных share-links нет
max_uses    int                              -- NULL = без ограничения; иначе used_count < max_uses
used_count  int default 0                    -- инкрементится в Auth при успешной валидации
created_by  uuid references users(id)
created_at  timestamptz default now()
```

### `node_settings` — настройки узла (применимы к файлам и папкам)

Поля наследуются через ltree-lookup ближайшего предка (или самого узла) с не-NULL значением.

```sql
node_id          uuid primary key references nodes(id) on delete cascade
max_versions     int                         -- ROADMAP/Later: версионирование
retention_days   int                         -- ROADMAP/Later: TTL версий
version_policy   text                        -- ROADMAP/Later: 'off' | 'on_change' | ...
created_at       timestamptz default now()
updated_at       timestamptz default now()
```

`storage_backend` сюда НЕ выносится — он живёт прямо в `nodes.backend_kind`.

### `jobs` — горячая очередь async-индексации

```sql
id           bigserial primary key
node_id      uuid not null references nodes(id) on delete cascade
kind         text not null                   -- 'hash' (в MVP); 'exif' | 'mime' | 'face' — ROADMAP/Next
status       text not null                   -- 'pending' | 'in_progress' | 'failed'
                                              -- (терминальные 'done' и окончательный 'failed' переезжают в jobs_history)
attempts     int default 0
last_error   text
locked_until timestamptz                     -- lease воркера; sweeper перехватывает зависшие
created_at   timestamptz default now()
updated_at   timestamptz default now()
```

Воркеры берут задачи через `SELECT ... FOR UPDATE SKIP LOCKED WHERE status='pending' AND kind=$1 ORDER BY created_at LIMIT N`. Индексы: `btree(kind, status, created_at)`, `btree(node_id)`. Таблица остаётся горячей и маленькой — терминальные строки переезжают в `jobs_history`.

### `jobs_history` — append-only архив завершённых задач

```sql
id            bigint                          -- тот же id что был в jobs (не PK после партиционирования)
node_id       uuid                            -- без FK: после удаления узла история сохраняется
kind          text not null
final_status  text not null                   -- 'done' | 'failed'
attempts      int not null
last_error    text                            -- NULL для 'done'
enqueued_at   timestamptz not null            -- бывший jobs.created_at
finished_at   timestamptz not null default now()
) partition by range (finished_at);           -- помесячные партиции; DROP старых через DROP PARTITION
```

Индексы на каждой партиции: `btree(node_id, finished_at)`, `btree(kind, final_status)`. Retention — конфигурируемо.

### `outbox` — журнал незавершённых FS-операций

См. [storage.md § Outbox](storage.md#outbox-атомарность-fs--db), [ADR-0002](../adr/0002-outbox-fs-db-atomicity.md).

```sql
id           bigserial primary key            -- порядок вставки = порядок попыток (FIFO в рамках одного executor'а)
op           text not null                    -- 'create_file' | 'trash' | 'rename' | 'migrate_backend' | ...
node_id      uuid                             -- nullable: некоторые операции (GC) не привязаны к одному узлу
payload      jsonb not null                   -- всё для идемпотентного выполнения: upload_id, backend_ref до/после, …
status       text not null                    -- 'pending' | 'in_progress' | 'failed' (терминальные в outbox_history)
attempts     int default 0
created_at   timestamptz default now()
locked_until timestamptz                      -- lease; истёк = executor умер, запись можно подобрать
```

**Индекс:** `partial btree(status, locked_until) WHERE status IN ('pending','in_progress')` — для быстрого picking активных. Таблица горячая и маленькая по построению; вся история в `outbox_history`.

### `outbox_history` — append-only архив FS-операций

```sql
id            bigint
op            text not null
node_id       uuid                            -- без FK: запись переживает удаление узла
payload       jsonb not null
final_status  text not null                   -- 'done' | 'failed'
attempts      int not null
last_error    text                            -- NULL для 'done'
enqueued_at   timestamptz not null
finished_at   timestamptz not null default now()
) partition by range (finished_at);
```

Индексы: `btree(node_id, finished_at)`, `btree(op, final_status, finished_at)`.

### `audit_log` — append-only журнал событий безопасности

```sql
audit_log (
  id        bigserial primary key,
  ts        timestamptz default now(),       -- ключ партиционирования (помесячно)
  user_id   uuid,                             -- инициатор; NULL для системных событий (cron, GC, sweeper)
  action    text not null,                   -- 'login' | 'login_failed' | 'logout' | 'acl_change' | 'share_*'
                                              -- | 'upload' | 'delete' | 'rename' | 'mkdir' | 'trash_*' | 'backup' | 'recover'
  node_id   uuid,                             -- для FS-событий; NULL для не-FS (login и т.п.)
  ip        inet,                             -- NULL для системных
  result    text not null,                    -- 'ok' | 'denied' (ACL/rate-limit) | 'error'
  details   jsonb                             -- произвольный контекст: для 'acl_change' — diff; для 'share_use' — token id; …
) partition by range (ts);
```

Индексы на каждой партиции: `btree(user_id, ts)`, `btree(node_id, ts)`, `btree(action, ts)`. Retention настраивается (default 12 месяцев).

### `users`, `sessions`, `app_passwords`

См. [auth.md](auth.md).

## Партиционирование history-таблиц

`outbox_history`, `jobs_history`, `audit_log` партиционированы по месяцу (`PARTITION BY RANGE`).

Миграция v1 создаёт партиции на текущий месяц + 3 вперёд. **Cron автосоздания будущих партиций — НЕ реализован**, это в [ROADMAP/Next](../../ROADMAP.md). До тех пор оператор должен следить, чтобы партиции не закончились — иначе INSERT'ы будут отказывать.

Дроп старых партиций — `DROP PARTITION` (O(1)), не `DELETE` (нагружает autovacuum и не освобождает место).

## Расширения

Используются:
- **`ltree`** — `nodes.path`, gist-index, subtree запросы (`path <@`, `path @>`).
- **`citext`** — `users.login` (case-insensitive unique).
- **`pgcrypto`** — `gen_random_uuid()` для UUID-PK.

Все три включаются в миграции `0001_extensions.up.sql`.

## Корневой узел

В миграциях **не создаётся** — это бизнес-операция, выполняется `DBFS.Bootstrap(ctx)` при первом старте процесса. Идемпотентно (NOOP если корень уже есть).
