# Общее описание

Мы разрабатываем персональное файловое хранилище уровня enterprise на чистом Go.

Ключевые особенности:
    Цель: Создать самодостаточную, безопасную и производительную систему для личного использования, которая сочетает удобство облачных хранилищ с полным контролем над данными и простотой администрирования.
    Простота: Запуск одним бинарём Go (внешняя зависимость — PostgreSQL), embedded Web UI и миграции.
    Честное хранение: Пользовательские файлы лежат на диске «как есть». Рядом с каждым файлом и в каждой папке обязательный side-car `*.meta.json` со служебной информацией (ACL, хеши, версии, настройки).
    Надежность: Полная восстановимость метаданных, структуры и прав из файлов на диске при потере БД.
    Безопасность:
        - Гибкий RBAC: в БД хранятся только явные права, traverse-права вычисляются виртуально и кэшируются.
        - Временные анонимные share-links (только Web).
        - Учитывает практики OWASP.
    Функционал:
        - Web и FTP (FTPS) доступ; позднее могут добавляться остальные интерфейсы.
        - Корзина с мягким удалением.
        - Асинхронная индексация с обогащением меты (хеши, EXIF, AI).

### 1. Структуризация требований

#### 📦 Ядро и Хранение
*   **Single Binary + PostgreSQL:** Чистый Go, статическая линковка, embedded assets (web UI, миграции). Внешняя зависимость — PostgreSQL (осознанный компромисс взамен SQLite ради конкурентных writer'ов, JSONB, FTS, `ltree` и партиционирования).
*   **Disk Layout:** Пользовательские файлы лежат «как есть». Рядом с каждым файлом лежит side-car `<filename>.meta.json`, в каждой папке — `.meta.json`. Служебные данные (версии, корзина, журнал, временные загрузки) изолированы в скрытой `.storage_meta/`.
*   **Инвариант side-car:** Storage-слой гарантирует, что `*.meta.json` и `.storage_meta/` нельзя читать/писать/создавать/удалять через публичные API (Web/FTP/share-links). Они принадлежат системе. Наличие side-car обязательно для каждого узла; его отсутствие — inconsistency, лечится `fsck`.
*   **Восстановимость:** Метаданные в БД избыточны по отношению к side-car. Команда `recover` обходит диск, читает side-car'ы и полностью воссоздаёт `nodes`, `permissions`, `node_meta`, `versions`, `folder_settings`.

#### 🔐 Безопасность и RBAC
*   **Модель прав:** `User -> Resource -> Action` (Read/Write/Remove/Admin).
*   **Явные права only:** В БД (`permissions`) и в side-car хранятся **только явно заданные** права. Никаких `is_explicit`/`auto_granted` флагов и виртуальных записей в персистентном состоянии.
*   **Наследование:**
    *   *Down:* если явного права на узле нет — поднимаемся вверх по дереву до первого предка с явным правом. Результат кэшируется в ACL Cache.
    *   *Up (Traverse):* если у пользователя есть любое явное право на потомке узла `N`, то на всех предках `N` виртуально выдаётся `Traverse`. Это чистая функция от `permissions`, вычисляется на лету и кэшируется.
*   **Admin право:** отдельный бит в маске. Проверяется при изменении настроек папки, правил версионирования и ACL.
*   **Анонимный доступ (только Web):** временные share-links (токен + TTL + scope Read/Write/Remove) для файлов и папок. FTP и другие протоколы анонимный доступ **не** поддерживают — scope ограничен сознательно.

#### 📂 Функционал файловой системы
*   **Версионирование:** история изменений. Правила (`max_versions`, `retention_days`) настраиваются на папку, наследуются, могут быть переопределены (требует `Admin`).
*   **Корзина:** мягкое удаление с возможностью восстановления.
*   **Протоколы:** Web UI и FTPS (explicit TLS; plain FTP отключён).

#### 🔍 Индексация и Метаданные
*   **Асинхронность:** двухэтапная обработка.
    *   *Sync:* базовая мета (путь, размер, mtime, тип) пишется сразу в БД и в side-car.
    *   *Async:* воркеры считают хеши, парсят EXIF, делают face recognition; пишут в БД и обновляют side-car.
*   **Расширяемость:** новые экстракторы добавляются как самостоятельные worker-ы, публикующие результаты в `node_meta.extra` (JSONB) + опционально в типизированные колонки для часто используемых полей.

---

### 2. Высокоуровневый дизайн архитектуры

#### 🏗 Общая схема
```text
┌─────────────────────────────────────────────────────────────────┐
│                        Single Binary (Go)                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌────────────┐  ┌────────────┐  ┌──────────────────────────┐   │
│  │   Web UI   │  │    FTPS    │  │       CLI / API          │   │
│  │  (Echo)    │  │(ftpserverlib)│                            │   │
│  └─────┬──────┘  └─────┬──────┘  └────────────┬─────────────┘   │
│        │               │                      │                 │
│  ┌─────▼───────────────▼──────────────────────▼─────────────┐   │
│  │              API Gateway / Router                        │   │
│  └──────────────────────────┬───────────────────────────────┘   │
│                             │                                   │
│  ┌──────────────────────────▼───────────────────────────────┐   │
│  │                 Core Services Layer                      │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────┐  │   │
│  │  │   Auth   │ │   RBAC   │ │    FS    │ │   Indexer   │  │   │
│  │  │  Service │ │ Service  │ │  Service │ │   Service   │  │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └─────────────┘  │   │
│  └──────────────────────────┬───────────────────────────────┘   │
│                             │                                   │
│  ┌──────────────────────────▼───────────────────────────────┐   │
│  │                 Infrastructure Layer                     │   │
│  │  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌─────────────┐  │   │
│  │  │PostgreSQL│ │  Outbox  │ │  Storage │ │   Workers   │  │   │
│  │  │JSONB/FTS │ │ Journal  │ │  Driver  │ │  Pipeline   │  │   │
│  │  │ ltree    │ │          │ │(side-car)│ │             │  │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └─────────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

#### 💾 Структура данных на диске
```text
/storage/
├── photos/
│   ├── .meta.json              # Side-car папки: ACL, folder settings
│   ├── image.jpg               # Файл как есть
│   └── image.jpg.meta.json     # Side-car файла: ACL, sha256, versions refs
├── docs/
│   ├── .meta.json
│   ├── report.pdf
│   └── report.pdf.meta.json
└── .storage_meta/              # Системная папка, невидима через API
    ├── versions/
    │   └── <node_uuid>/
    │       ├── v1.bin
    │       └── v1.meta.json    # original_path, created_by, size, sha256, acl_snapshot
    ├── trash/
    │   └── <node_uuid>/
    │       ├── payload         # удалённый файл/поддерево
    │       └── trash.meta.json # original_path, deleted_by, deleted_at, acl_snapshot
    ├── uploads/                # Временные чанки tus и FTP STOR
    ├── journal/                # Fallback-журнал (если PG недоступна на старте recover)
    └── config.json             # Глобальные настройки (ключ шифрования секретов и т.п.)
```

**Формат side-car (`<file>.meta.json`):**
```json
{
  "schema": 1,
  "node_id": "uuid",
  "type": "file",
  "size": 12345,
  "mtime": "2026-04-01T10:00:00Z",
  "sha256": "hex...",
  "mime": "image/jpeg",
  "acl": [
    { "user_id": "uuid", "actions": ["Read", "Write"] }
  ],
  "versions": [
    { "version_num": 1, "storage_ref": "versions/<uuid>/v1.bin", "sha256": "..." }
  ],
  "extra": { "exif": { ... } }
}
```

**Инварианты side-car:**
- Storage Driver перехватывает все операции над `*.meta.json` и `.storage_meta/`: Web/FTP видят их как несуществующие, попытки записи отклоняются с `EACCES`.
- Side-car пишется атомарно (`tmp → fsync → rename`), пара «файл + side-car» приводится к целевому состоянию через outbox-журнал (см. §3.1).
- Отсутствующий или повреждённый side-car — inconsistency, восстанавливается `fsck` из БД (или наоборот — БД из side-car при `recover`).

#### 🗄️ Модель Базы Данных (PostgreSQL)

Выбор PostgreSQL обоснован:
- Конкурентные writer'ы (Web + FTP + воркеры индексации одновременно).
- JSONB + GIN для расширяемых метаданных.
- `tsvector` + FTS для полнотекстового поиска.
- `ltree` для запросов по дереву (materialized path).
- Партиционирование истории версий/audit log.

**Ключевые таблицы:**

*   **`nodes`** — дерево файлов и папок.
    ```sql
    id           uuid primary key
    parent_id    uuid references nodes(id)
    path         ltree not null            -- materialized path для subtree-запросов
    name         text not null
    type         node_type not null        -- 'file' | 'dir'
    disk_path    text not null
    size         bigint
    mime         text
    mtime        timestamptz
    sha256       bytea                     -- дублируется из meta для быстрого lookup
    status       node_status not null      -- 'pending' | 'ready' | 'deleted'
    created_at   timestamptz default now()
    updated_at   timestamptz default now()
    deleted_at   timestamptz
    unique (parent_id, name)
    ```
    Индексы: gist на `path`, btree на `sha256`, `parent_id`, partial на `deleted_at`.

*   **`node_meta`** — типизированные частые поля + JSONB + FTS.
    ```sql
    node_id      uuid primary key references nodes(id) on delete cascade
    width        int
    height       int
    taken_at     timestamptz
    lat          double precision
    lng          double precision
    duration_ms  int
    extra        jsonb not null default '{}'
    search_tsv   tsvector generated always as (
                   setweight(to_tsvector('simple', coalesce(extra->>'title','')), 'A') ||
                   setweight(to_tsvector('simple', coalesce(extra->>'description','')), 'B')
                 ) stored
    ```
    Индексы: `gin (extra jsonb_path_ops)`, `gin (search_tsv)`, btree на `taken_at`, btree на `(lat, lng)` (PostGIS опционально).

    Принцип: **часто запрашиваемые поля — в типизированные колонки** (можно индексировать, делать range-запросы), **редкие/кастомные — в `extra` JSONB**. Переезд из JSONB в колонку — миграция, когда поле становится горячим.

*   **`permissions`** — только явные права.
    ```sql
    id         bigserial primary key
    node_id    uuid references nodes(id) on delete cascade
    user_id    uuid references users(id) on delete cascade
    actions    bit(8) not null        -- маска: Read|Write|Remove|Admin
    created_at timestamptz default now()
    unique (node_id, user_id)
    ```
    Виртуальные traverse — не здесь, считаются на лету в RBAC Service.

*   **`versions`** — история файлов. Партиционируется по `created_at` при росте.
    ```sql
    id            uuid primary key
    node_id       uuid references nodes(id) on delete cascade
    version_num   int not null
    storage_ref   text not null
    size          bigint
    sha256        bytea
    created_by    uuid references users(id)
    created_at    timestamptz default now()
    meta_snapshot jsonb
    unique (node_id, version_num)
    ```

*   **`share_links`** — Web only.
    ```sql
    token       text primary key      -- 32 байта crypto/rand, base64url
    node_id     uuid references nodes(id) on delete cascade
    actions     bit(8) not null
    expires_at  timestamptz not null
    max_uses    int
    used_count  int default 0
    created_by  uuid references users(id)
    ```

*   **`folder_settings`** — настройки версионирования и т.п.
    ```sql
    node_id         uuid primary key references nodes(id) on delete cascade
    max_versions    int
    retention_days  int
    version_policy  jsonb
    ```
    Наследование — lookup ближайшего предка с настройкой через ltree.

*   **`jobs`** — очередь индексации. `SELECT ... FOR UPDATE SKIP LOCKED`.
*   **`outbox`** — журнал незавершённых FS-операций (см. §3.1).
*   **`users`**, **`sessions`**, **`audit_log`** — см. §3.3.

#### 🔐 Реализация RBAC и Наследования

1.  **Вычисление эффективных прав** для `(user, node)`:
    1. Проверяем ACL Cache.
    2. По `nodes.path` (ltree) находим цепочку предков, берём ближайший, где есть явная запись в `permissions` для этого user → effective actions.
    3. Дополнительно: если на любом **потомке** запрошенного `node` у пользователя есть явное право → в effective добавляется виртуальный `Traverse`. Запрос: `select exists(select 1 from permissions p join nodes n on n.id=p.node_id where p.user_id=$1 and n.path <@ $2)`.
    4. Результат кэшируется (ключ `user_id:node_id`).

2.  **Инвалидация кэша:**
    *   Изменение `permissions` → инвалидируется поддерево (`path <@` префикс).
    *   Перемещение/удаление узла → инвалидируется поддерево.
    *   Epoch-based: на каждое изменение инкрементим «эпоху» префикса; записи старой эпохи отбрасываются lazily при чтении — без явного обхода кэша.

3.  **Admin-действия** (изменение `permissions`, `folder_settings`, rollback версий) требуют бита `Admin` в effective permissions.

#### ⚙️ Индексация: Pipeline Workers

1.  **Trigger:** загрузка через API, перемещение, `fsck`-сканер.
2.  **Stage 1 (Sync, в транзакции загрузки):** вставка в `nodes` (`size`, `mtime`, `type`), запись side-car, вставка job в `jobs`, коммит.
3.  **Stage 2 (Async):** пул горутин берёт задачи через `SELECT ... FOR UPDATE SKIP LOCKED`.
    *   *Hasher:* sha256/md5 → `nodes.sha256`, `node_meta`, обновление side-car.
    *   *ImageProc:* EXIF/Geo/Dimensions → типизированные колонки `node_meta`, side-car.
    *   *AI:* face recognition → `node_meta.extra`, side-car.
4.  Ошибки → retry с backoff, после N попыток — dead letter и audit event.

#### 🔄 Версионирование и Корзина

*   **Настройки** в `folder_settings`, наследуются через ltree-lookup предка.
*   **Создание версии:**
    1. Текущий файл копируется в `.storage_meta/versions/<node_uuid>/v<N>.bin` + `v<N>.meta.json` (с `original_path`, `acl_snapshot`, `sha256`, `size`).
    2. Новая версия записывается через upload path (см. §3.2).
    3. Insert в `versions`, update side-car целевого файла.
*   **Корзина:** файл/папка перемещается в `.storage_meta/trash/<node_uuid>/`, side-car сопровождает данные, создаётся `trash.meta.json` с `original_path`, `deleted_by`, `deleted_at`, `acl_snapshot`. В `nodes` — `status='deleted'`, `deleted_at`.
*   **GC:** фоновый воркер применяет `max_versions` / `retention_days`.

#### 🌐 Интерфейсы

*   **Web:** embedded SPA (React/Svelte) через `embed.FS`. Build step (`npm run build`) — часть релизной сборки, требует Node в CI. API — JSON.
*   **FTP:** `github.com/fclairamb/ftpserverlib` с кастомным Driver.
    *   Только **FTPS** (explicit TLS через `AUTH TLS`). Plain FTP отключён.
    *   **Share-links не поддерживаются** — только именованные пользователи.
    *   Driver фильтрует `.storage_meta/` и `*.meta.json` на всех listing/read/write.

---

### 3. Сквозные механизмы

#### 3.1 Атомарность FS ↔ DB (Outbox + Journal)

Операции затрагивают два независимых хранилища — диск и PostgreSQL. Чистых распределённых транзакций нет, поэтому используем **transactional outbox + идемпотентное доигрывание**.

**Таблица `outbox`:**
```sql
id          bigserial primary key
op          text not null        -- 'create_file' | 'delete' | 'rename' | 'version' | ...
node_id     uuid
payload     jsonb not null       -- всё необходимое для идемпотентного выполнения
status      text not null        -- 'pending' | 'in_progress' | 'failed'
attempts    int default 0
created_at  timestamptz default now()
locked_until timestamptz
```

**Порядок операций (на примере загрузки):**
1. Клиент шлёт данные → Storage Driver пишет во временный файл `.storage_meta/uploads/<upload_id>`, fsync.
2. Транзакция PostgreSQL: insert `nodes(status='pending')`, insert `outbox(op='create_file', payload={upload_id, target_path, side_car})`.
3. Коммит. С этого момента операция **должна быть доведена до конца**.
4. Executor (та же горутина или фоновый worker) выполняет:
   a. Запись `<target>.meta.json.tmp` → fsync → rename.
   b. Rename `uploads/<upload_id>` → `<target_path>` (в пределах одной ФС, атомарно).
5. Транзакция: `nodes.status='ready'`, delete из `outbox`.

**Удаление:**
1. Транзакция: `nodes.status='deleted'`, `deleted_at=now()`, insert outbox `op='trash', payload={disk_path, trash_uuid, acl_snapshot}`.
2. Executor: move файла + side-car в `.storage_meta/trash/<uuid>/`, создание `trash.meta.json`.
3. Delete из `outbox`.

**Rename/move:** аналогично, outbox описывает целевое состояние.

**Идемпотентность:** каждый executor при старте проверяет текущее состояние диска (что уже перемещено/создано) и доводит до целевого. Повторный запуск безопасен.

**Recovery после краша:** при старте процесс читает `outbox where status in ('pending','in_progress')` и доигрывает. Записи старше N минут с истёкшим `locked_until` перехватываются другими инстансами/рестартами.

**Sweeper:** фоновая задача периодически сверяет `outbox` с диском, перезапускает зависшие операции, эскалирует `failed` в audit + алерт.

**Атомарность на уровне одного файла:**
- Запись: `open tmp → write → fsync → rename`. POSIX гарантирует атомарность rename в пределах ФС.
- Side-car пишется так же. Пара «файл + side-car» не атомарна на уровне ФС, но outbox описывает целевое состояние — `fsck` и executor приводят к нему.

#### 3.2 Upload Path

**Web: tus.io** (`github.com/tus/tusd` как embedded handler).
- Resumable, chunked, стандартизированный протокол.
- Чанки пишутся в `.storage_meta/uploads/<upload_id>` (а не в целевой путь).
- По завершении `Upload-Complete` → запускается процедура §3.1 (insert outbox → executor rename).
- Валидация: `max_size`, MIME по magic bytes (не по заголовку `Content-Type`), проверка квот **до** начала аллокации.

**FTP: STOR.**
- Driver перенаправляет `STOR` в `.storage_meta/uploads/<random>`.
- По `TRANSFER COMPLETE` — та же outbox-процедура с rename в целевой путь.

**Общие правила:**
- **Никогда** не писать напрямую в целевой путь — только `tmp → rename`.
- `fsync` перед rename для критичных данных (конфигурируется: `durability=strict|relaxed`).
- Квоты (user, folder) проверяются до старта upload и подтверждаются в транзакции вместе с insert в `nodes`.
- Cleanup временных файлов: фоновый воркер удаляет `uploads/*` старше N часов без связанной `outbox` записи.

#### 3.3 Auth Service (конкретика)

**Хранение паролей:**
- `argon2id`, параметры по умолчанию: `memory=64 MiB, iterations=3, parallelism=2`, tunable через config.
- Таблица `users`:
    ```sql
    id              uuid primary key
    login           citext unique not null
    password_hash   text not null
    totp_secret     bytea           -- шифруется конфиг-ключом
    is_admin        bool default false
    failed_attempts int default 0
    locked_until    timestamptz
    created_at      timestamptz default now()
    ```

**Сессии Web:**
- HttpOnly + Secure + SameSite=Lax cookie (`session_id`).
- Таблица `sessions(id, user_id, created_at, last_seen, expires_at, ip, user_agent)`.
- Sliding expiration (`last_seen + idle_timeout`, max `created_at + absolute_timeout`).
- Logout — delete записи.
- **CSRF:** double-submit cookie + `X-CSRF-Token` header для всех мутирующих запросов. tus эндпоинты защищаются через origin check + Authorization header.
- **Security headers:** `Content-Security-Policy`, `Strict-Transport-Security`, `X-Frame-Options: DENY`, `Referrer-Policy: same-origin`.

**Rate limiting / brute-force:**
- Token bucket in-memory по `(login)` и `(client_ip)` для `/login` и `/share-link/<token>`.
- После N неудач — `failed_attempts++`, при достижении порога `locked_until = now() + backoff(failed_attempts)`.
- Audit event на каждую неудачу.

**FTP auth:**
- Только FTPS. Тот же `users.password_hash`, та же процедура.
- **App passwords:** опциональная таблица `app_passwords(id, user_id, label, hash, created_at, last_used)` — отдельные токены для FTP-клиентов, чтобы не хранить основной пароль. Обязательны для пользователей с включённой 2FA (FTP 2FA не поддерживает).

**2FA (опционально, за флагом в конфиге):**
- TOTP (`github.com/pquerna/otp/totp`).
- `totp_secret` шифруется AEAD-ключом из `.storage_meta/config.json`.

**Политика паролей:**
- min length 12, проверка по embedded топ-10k утечек (или zxcvbn).
- Запрет переиспользования последних N паролей (опционально).

**Share-links (Web only):**
- Токен — 32 байта из `crypto/rand`, base64url.
- Валидация: не истёк, `used_count < max_uses`, scope совпадает с запрошенным action.
- Rate limiting по токену (защита от brute-force перебора).
- Audit event на каждое использование.

**Audit log:**
```sql
audit_log (
  id        bigserial primary key,
  ts        timestamptz default now(),
  user_id   uuid,
  action    text not null,        -- 'login', 'login_failed', 'acl_change', 'share_use', 'delete', ...
  node_id   uuid,
  ip        inet,
  result    text not null,        -- 'ok' | 'denied' | 'error'
  details   jsonb
)
```
Партиционируется по месяцу, retention настраивается.

---

### 4. Критические моменты и рекомендации

1.  **Восстановление БД (`./storman recover`):**
    *   Walk по диску → для каждого узла читаем side-car → воссоздаём `nodes`, `permissions`, `node_meta`, `versions`, `folder_settings`.
    *   `.storage_meta/versions/` и `.storage_meta/trash/` восстанавливаются из их `*.meta.json`.
    *   Верификация: пересчёт sha256 и сверка с side-car. Расхождение → warning + quarantine.
    *   Отсутствующий side-car → warning, узел восстанавливается с owner-only правами и помечается для ручного review.

2.  **Производительность RBAC:**
    *   `ltree` + gist index даёт O(log n) ancestor/descendant lookup.
    *   ACL Cache: LRU с epoch-based invalidation per subtree — без явного обхода при изменениях.
    *   Прогрев кэша при логине для последних использованных путей.

3.  **Консистентность с внешними изменениями:**
    *   Изменения в обход системы (SSH, rsync) → рассинхрон.
    *   Решение: периодический `fsck`-сканер (cron внутри процесса) + опциональный `fsnotify` watcher.
    *   Правило разрешения конфликта: **side-car — источник правды** для ACL и meta; если side-car отсутствует у существующего файла — создаётся из БД; если в БД нет записи, а на диске есть файл с side-car — запись создаётся.

4.  **Безопасность:**
    *   Строгая валидация путей: canonicalize + проверка, что результат внутри `storage_root`, запрет symlink-ов наружу.
    *   Изоляция `.storage_meta/` и `*.meta.json` — инвариант Storage Driver, через который ходят **все** интерфейсы (Web/FTP/CLI API).
    *   OWASP: argon2id, CSRF, rate limiting, HttpOnly cookies, CSP, MIME validation по magic bytes — см. §3.3, §3.2.
    *   TLS обязателен: HTTPS для Web, FTPS для FTP. Plain FTP отключён на уровне listener'а.
