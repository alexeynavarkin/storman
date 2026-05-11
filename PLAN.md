# Общее описание

Мы разрабатываем персональное файловое хранилище уровня enterprise на чистом Go.

Ключевые особенности:
    Цель: Создать самодостаточную, безопасную и производительную систему для личного использования, которая сочетает удобство облачных хранилищ с полным контролем над данными и простотой администрирования.
    Простота: Запуск одним бинарём Go (внешняя зависимость — PostgreSQL), embedded Web UI и миграции.
    Честное хранение: Пользовательские файлы лежат на диске «как есть» — никаких side-car'ов рядом, никакого замусоривания storage_root. Метаданные — в БД. В будущем — опциональный CAS-режим (`CDCFile`), включаемый на уровне директории и наследуемый потомками (см. §5).
    Надежность: Двухуровневая DR-стратегия — периодические `pg_dump` в `<storage-meta-dir>/backups/` → fsck по диску как последняя линия. См. §3.4.
    Безопасность:
        - Гибкий RBAC: в БД хранятся только явные права, traverse-права вычисляются виртуально и кэшируются.
        - Временные анонимные share-links (только Web).
        - Учитывает практики OWASP.
    Функционал:
        - Web и FTP (FTPS) доступ; позднее могут добавляться остальные интерфейсы.
        - Корзина с мягким удалением.
        - Асинхронная индексация с обогащением меты (хеши, EXIF, AI).
    Отложено (см. §5): версионирование файлов, CAS+CDC blobstore.

### 1. Структуризация требований

#### 📦 Ядро и Хранение
*   **Single Binary + PostgreSQL:** Чистый Go, статическая линковка, embedded assets (web UI, миграции). Внешняя зависимость — PostgreSQL (осознанный компромисс взамен SQLite ради конкурентных writer'ов, JSONB, `ltree` и партиционирования).
*   **Disk Layout:** Один параметр в конфиге — `data-dir`. Внутри фиксированная раскладка:
    *   `<data-dir>/config.json` — локальные настройки сервиса (ключ шифрования секретов и т.п.).
    *   `<data-dir>/flat-storage/` — пользовательские файлы как есть (бэкенд `FlatFile` — единственный в MVP). Рядом с файлами **ничего не лежит** — никаких `.meta.json`. Это то, что пользователь видит через FUSE/FTP/Web; то, что бэкапится Time Machine'ом и т.п. Далее в документе обозначается как `storage-dir`.
    *   `<data-dir>/meta-storage/` — служебные данные: корзина, временные загрузки, PG-бэкапы, в будущем — CDC blobstore. Далее в документе обозначается как `storage-meta-dir`.
    *   Оба подкаталога — внутри одного `data-dir`, всегда на одной файловой системе по построению, поэтому `rename` между staging и целевым путём атомарен. Никаких отдельных проверок `st_dev` не требуется.
*   **Storage Abstraction:** Два уровня — `FileSystem` (фасад над деревом узлов для всех протоколов: Web, FTP, FUSE, WebDAV, S3) и `FileBackend` (хранение содержимого одного файла). Каждый узел в `nodes` содержит `backend_kind` + `backend_ref`. Колонка `backend_kind` несёт двойную семантику: у файла — фактический бэкенд содержимого (immutable), у папки — *политика* для новых детей (NULL = наследовать). Бэкенд для **новых** файлов выбирается ltree-lookup'ом ближайшего предка-папки с не-NULL `backend_kind`. Существующие файлы остаются на своём бэкенде; смена политики папки их не трогает. Подробнее — §2 «Storage Abstraction». Готовится почва для `CDCFile` (см. §5), но в MVP реализуется только `FlatFile`.
*   **Инвариант служебных путей:** `storage-meta-dir` физически отделён от `storage-dir` (соседние каталоги внутри `data-dir`), поэтому Web/FTP/FUSE по построению не видят служебные данные. Никакого path-фильтра в Storage Driver не требуется.
*   **DR-стратегия (§3.4):** БД — единственный источник правды для метаданных. Recovery работает по нисходящей: (1) `recover --from-backup` из `pg_dump`-дампа в `<storage-meta-dir>/backups/`; (2) `recover --from-disk` — последняя линия, walks дерево, создаёт узлы со sha256 и owner-only ACL, расширенная мета теряется.

#### 🔐 Безопасность и RBAC
*   **Модель прав:** `User -> Resource -> Action` (Read/Write/Remove/Admin).
*   **Явные права only:** В БД (`permissions`) хранятся **только явно заданные** права. Никаких `is_explicit`/`auto_granted` флагов и виртуальных записей в персистентном состоянии.
*   **Наследование:**
    *   *Down:* если явного права на узле нет — поднимаемся вверх по дереву до первого предка с явным правом. Результат кэшируется в ACL Cache.
    *   *Up (Traverse):* если у пользователя есть любое явное право на потомке узла `N`, то на всех предках `N` виртуально выдаётся `Traverse`. Это чистая функция от `permissions`, вычисляется на лету и кэшируется.
*   **Admin право:** отдельный бит в маске. Проверяется при изменении настроек папки, правил версионирования и ACL. Глобальный администратор = пользователь с `Admin` на корне дерева (наследуется вниз через ltree-lookup ближайшего предка).
*   **Анонимный доступ (только Web):** временные share-links (токен + TTL + scope Read/Write/Remove) для файлов и папок. FTP и другие протоколы анонимный доступ **не** поддерживают — scope ограничен сознательно.

#### 📂 Функционал файловой системы
*   **Корзина:** мягкое удаление с возможностью восстановления.
*   **Протоколы:** Web UI и FTPS (explicit TLS; plain FTP отключён). Архитектура `FileSystem` рассчитана на расширение протоколов (FUSE, WebDAV, S3) без изменений в backend-слое.
*   **Отложено (§5):** версионирование файлов, CDC-бэкенд (`CDCFile`).

#### 🔍 Индексация и Метаданные
*   **Асинхронность:** двухэтапная обработка.
    *   *Sync:* базовая мета (путь, размер, mtime, тип) пишется сразу в БД.
    *   *Async:* воркеры считают хеши, парсят EXIF, делают face recognition; пишут в БД.
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
│  │  │  JSONB   │ │          │ │  Driver  │ │  Pipeline   │  │   │
│  │  │  ltree   │ │          │ │(FileBe.) │ │             │  │   │
│  │  └──────────┘ └──────────┘ └──────────┘ └─────────────┘  │   │
│  └──────────────────────────────────────────────────────────┘   │
└─────────────────────────────────────────────────────────────────┘
```

#### 💾 Структура данных на диске
**Раскладка** (фиксированная, всё внутри `data-dir`):

```text
<data-dir>/                     # = config.data_dir, например /var/lib/storman
├── config.json                 # Локальные настройки сервиса (ключ шифрования секретов и т.п.)
├── flat-storage/               # storage-dir: пользовательские файлы как есть, без side-car
│   ├── photos/
│   │   └── image.jpg
│   └── docs/
│       └── report.pdf
└── meta-storage/               # storage-meta-dir: служебные данные
    ├── trash/
    │   └── <node_uuid>/
    │       ├── payload         # удалённый файл/поддерево
    │       └── trash.meta.json # original_path, deleted_by, deleted_at, acl_snapshot
    │                           # — служебная мета корзинной записи (для undelete без БД),
    │                           #   не side-car пользовательского файла
    ├── uploads/                # Временные чанки tus, FTP STOR и staging для FileWriter
    ├── backups/                # PG-дампы (§3.4): <ISO-ts>.dump
    └── blobs/                  # [§5, отложено] CDC blobstore: chunks/<aa>/<bb>/<sha256>, manifests/<id>.json
```

**Конфигурация (фрагмент):**
```json
{
  "data_dir": "/var/lib/storman"
}
```

`flat-storage/` и `meta-storage/` — фиксированные подкаталоги, не конфигурируются. Это упрощает UX (один параметр), гарантирует атомарность `rename` (одна ФС по построению) и убирает класс ошибок misconfig.

**Зачем разделять `flat-storage` и `meta-storage`:**
- Пользователь может настроить бэкап-инструмент (Time Machine, restic, rsync) на `flat-storage/` — получит чистый архив пользовательских файлов без служебной мешанины. Бэкапы PG лежат отдельно в `meta-storage/backups/`.
- `meta-storage` скрыт от любых внешних обходов `flat-storage` — не нужен path-фильтр в Storage Driver.
- Разные дата-объёмы могут эволюционировать раздельно (квоты, retention, GC).

#### 🧩 Storage Abstraction (FileSystem / FileBackend)

Два уровня с разной ответственностью.

**`FileSystem`** — единый фасад над деревом узлов для всех frontend-протоколов (Web API, FTP, FUSE, WebDAV, S3). Реализация одна, поверх `nodes` в БД. Не знает, как физически хранится содержимое файла.

```go
type FileSystem interface {
    // Дерево
    Stat(ctx, path) (*NodeInfo, error)
    List(ctx, path) ([]NodeInfo, error)
    Mkdir(ctx, path, opts MkdirOpts) error
    Remove(ctx, path) error                  // soft-delete → корзина
    Rename(ctx, oldPath, newPath) error      // в т.ч. между папками с разными бэкендами:
                                             // node остаётся на своём backend_kind, не конвертируется

    // Содержимое
    OpenRead(ctx, path) (FileReader, error)
    OpenWrite(ctx, path, opts WriteOpts) (FileWriter, error)
}

type FileReader interface {
    io.ReaderAt
    io.Closer
    Size() int64
}

type FileWriter interface {
    io.WriterAt
    Truncate(size int64) error
    Commit(ctx) error          // финализация: атомарная публикация
    Abort() error              // отмена: чистит staging, не публикует
    io.Closer                  // Close без предварительного Commit/Abort вернёт ErrUnfinalized — финализацию надо вызвать явно.
}

type WriteOpts struct {
    Mode         WriteMode      // Create | Overwrite | Modify
    ExpectedSize int64          // -1 если неизвестно — для квот/преаллокации
}
```

Этого достаточно для всех протоколов: FUSE `read/write/release` → `ReadAt/WriteAt/Commit|Abort`; FTP `RETR/STOR` → стрим поверх `ReaderAt/WriterAt`; HTTP Range → `ReaderAt`; tus.io → chunked `WriteAt` + `Commit` по `Upload-Complete`; S3 PutObject/Multipart — аналогично.

**`FileBackend`** — низкоуровневое хранение содержимого **одного** файла. Реализации меняются (в MVP только `FlatFile`, в будущем `CDCFile` — §5). `FileSystem` диспатчит вызовы к нужному бэкенду по `nodes.backend_kind` + `nodes.backend_ref`.

```go
type FileBackend interface {
    Name() string                                // "flat" | "cdc" | ...

    OpenRead(ctx, ref BackendRef) (FileReader, error)
    OpenWrite(ctx, ref BackendRef, opts WriteOpts) (FileWriter, error)
    Delete(ctx, ref BackendRef) error
    Stat(ctx, ref BackendRef) (BackendStat, error)

    // Аллоцирует ref для нового файла. Вызывается ДО старта записи,
    // чтобы FS успела зафиксировать (backend_kind, backend_ref) в nodes
    // и outbox в той же транзакции, что и создание узла.
    Allocate(ctx, hint AllocHint) (BackendRef, error)
}

type BackendRef struct {
    Kind string  // == FileBackend.Name()
    Data string  // backend-specific: для flat — относительный путь; для cdc — manifest id
}

type AllocHint struct {
    NodeID       uuid.UUID
    LogicalPath  string  // подсказка для FlatFile: куда положить файл «как есть»
    ExpectedSize int64
}
```

**Реализации:**

- **`FlatFile` (MVP):**
    - `Allocate` → возвращает relative path, основанный на `LogicalPath`.
    - `OpenWrite` → открывает tmpfile в `<storage-meta-dir>/uploads/`, `WriteAt` = `pwrite` в tmpfile. На `Commit` — `fsync` + `rename` поверх целевого пути (атомарно в пределах ФС). На `Abort` — `unlink` tmpfile.
    - `OpenRead` → `os.OpenFile`, возвращает обёртку с `ReadAt`.
    - `Delete` → `unlink`.

- **`CDCFile` (отложено, §5):** наброски — `OpenWrite` использует sparse staging tmpfile с lazy fetch чанков и copy-on-write на `WriteAt`; на `Commit` прогоняется FastCDC, чанки в `<storage-meta-dir>/blobs/chunks/`, манифест в `<storage-meta-dir>/blobs/manifests/<id>.json`. `OpenRead` резолвит `(offset, len)` через таблицу чанков манифеста и делает `pread` по нужному чанку. Random read дешёвый (без сжатия/шифрования на чанк целиком — иначе теряется); см. §5 про trade-offs.

**Жизненный цикл записи (через `FileSystem.OpenWrite`):**

1. FS вычисляет effective backend для нового файла: ltree-lookup ближайшего предка-папки с не-NULL `nodes.backend_kind` (если ни у одного предка нет — default `'flat'`).
2. FS вызывает `backend.Allocate(hint)` → получает `BackendRef`.
3. Транзакция: insert `nodes(status='pending', backend_kind, backend_ref)`, insert `outbox`.
4. Возвращает `FileWriter` (это writer бэкенда).
5. Клиент пишет (`WriteAt`/`Truncate`).
6. Клиент вызывает `Commit` → бэкенд финализирует (для `FlatFile` — rename; для `CDCFile` — chunking + manifest).
7. FS обновляет `nodes(status='ready', size, sha256)`, удаляет outbox.

`Abort` или `Close` без `Commit` → бэкенд чистит staging, FS откатывает узел (через outbox).

**Что сознательно НЕ в интерфейсе:**

- Random write на уровне `FileBackend` — нет. `WriteAt` поддерживается каждым бэкендом по-своему через staging; внешне это просто `FileWriter`.
- Move/Rename в `FileBackend` — нет. Логическое перемещение в дереве — операция `FileSystem`, не трогает `BackendRef`. Если backend хранит ref в виде пути (как `FlatFile`), то FS вызывает `Delete(old) + Allocate(new) + копирование`, либо backend опционально оптимизирует через native rename (не в интерфейсе, а как метод-расширение).
- List/Walk — нет. Обход дерева — это БД.

**Привязка бэкенда к файлу:**

- Политика хранится прямо в `nodes.backend_kind`: у папки колонка nullable и означает «бэкенд для новых детей» (NULL = наследовать); у файла — фактический бэкенд содержимого, NOT NULL и immutable.
- Effective backend для нового файла = ltree-lookup ближайшего предка-папки с не-NULL `backend_kind`; если ни у одного предка не задано — `'flat'` по умолчанию.
- Бэкенд фиксируется в `nodes.backend_kind` при **создании** файла. Существующий файл свой бэкенд не меняет — даже если на родителе изменили политику.
- Миграция между бэкендами (`storman migrate --to=cdc <path>`) — отдельная фоновая операция (§5): для каждого файла читает через старый бэкенд, пишет через новый, атомарно подменяет `(backend_kind, backend_ref)` через outbox.

#### 🗄️ Модель Базы Данных (PostgreSQL)

Выбор PostgreSQL обоснован:
- Конкурентные writer'ы (Web + FTP + воркеры индексации одновременно).
- JSONB + GIN для расширяемых метаданных.
- `ltree` для запросов по дереву (materialized path).
- Партиционирование истории версий/audit log.

**Ключевые таблицы:**

*   **`nodes`** — дерево файлов и папок.
    ```sql
    id            uuid primary key                  -- стабильный идентификатор узла, не меняется при rename/move
    parent_id     uuid references nodes(id)        -- родительская папка; NULL только для корня дерева
    path          ltree not null                   -- materialized path вида 'root.docs.report'; для subtree-запросов через ltree gist
    name          text not null                    -- имя узла внутри родителя (последний сегмент path); ровно то, что видит пользователь
    type          node_type not null               -- enum: 'file' | 'dir'
    backend_kind  text                              -- двойная семантика: для type='file' — фактический бэкенд содержимого (NOT NULL, immutable); для type='dir' — политика бэкенда для НОВЫХ детей в этой ветке, NULL = наследовать от ближайшего предка. CHECK ниже.
    backend_ref   text                              -- backend-specific указатель: для 'flat' — относительный путь в <storage-dir>; для 'cdc' — manifest id. NULL для type='dir'. Семантика opaque для FS.
    check (type <> 'file' or (backend_kind is not null and backend_ref is not null))
    check (type <> 'dir'  or backend_ref is null)
    size          bigint                           -- размер содержимого в байтах; NULL для папок и pending-файлов. Заполняется на FileWriter.Commit.
    mime          text                             -- MIME-тип, определённый по magic bytes (НЕ по Content-Type клиента). NULL для папок.
    mtime         timestamptz                      -- modification time содержимого; обновляется на каждом успешном Commit, не на metadata-операциях.
    sha256        bytea                            -- SHA-256 содержимого, 32 байта. Дублируется из node_meta для быстрого lookup при дедупе/поиске. NULL пока async-hasher не отработал.
    status        node_status not null             -- enum: 'pending' (идёт upload, файла нет в целевом пути) | 'ready' (опубликован) | 'deleted' (в корзине) | 'broken' (recovery-флаг — мета есть, контента нет)
    created_at    timestamptz default now()        -- момент insert'а строки
    updated_at    timestamptz default now()        -- момент последнего изменения любого поля; поддерживается триггером или приложением
    deleted_at    timestamptz                      -- момент перевода в корзину; NULL для активных узлов. По этому полю работает GC корзины.
    unique (parent_id, name)                       -- внутри одной папки имена уникальны; работает и для удалённых (deleted_at IS NOT NULL уходит в trash, имя освобождается на уровне приложения)
    ```
    Индексы: gist на `path` (ancestor/descendant), btree на `sha256` (поиск дубликатов), btree на `parent_id` (listing), partial btree на `deleted_at where deleted_at is not null` (GC).

*   **`node_meta`** — типизированные частые поля + JSONB.
    ```sql
    node_id      uuid primary key references nodes(id) on delete cascade  -- 1:1 с nodes; каскадное удаление вместе с узлом
    width        int                              -- ширина изображения/видео в пикселях; NULL для не-визуального контента
    height       int                              -- высота изображения/видео в пикселях
    taken_at     timestamptz                      -- момент съёмки из EXIF (DateTimeOriginal); отличается от nodes.mtime для импортированных файлов
    lat          double precision                 -- широта GPS из EXIF (-90..90); NULL если нет геотега
    lng          double precision                 -- долгота GPS из EXIF (-180..180)
    duration_ms  int                              -- длительность видео/аудио в миллисекундах
    extra        jsonb not null default '{}'      -- редкие и расширяемые поля: ключи названий полей экстракторов (например 'exif.iso', 'ai.faces'). Каждый воркер пишет свой namespace.
    ```
    Индексы: `gin (extra jsonb_path_ops)` — поиск по произвольным JSONB-полям; btree на `taken_at` — сортировка фотогалереи по времени съёмки; btree на `(lat, lng)` — bounding-box запросы (PostGIS опционально для гео-запросов с радиусом).

    Принцип: **часто запрашиваемые поля — в типизированные колонки** (можно индексировать, делать range-запросы), **редкие/кастомные — в `extra` JSONB**. Переезд из JSONB в колонку — миграция, когда поле становится горячим.

*   **`permissions`** — только явные права.
    ```sql
    id         bigserial primary key                                    -- суррогатный ключ для удобства audit / иностранных ссылок
    node_id    uuid references nodes(id) on delete cascade              -- узел, на который выдано право; удаление узла уносит ACL
    user_id    uuid references users(id) on delete cascade              -- кому выдано; удаление пользователя зачищает его ACL
    actions    bit(8) not null                                          -- битмаска разрешений: бит 0 = Read, 1 = Write, 2 = Remove, 3 = Admin; биты 4-7 зарезервированы
    created_at timestamptz default now()                                -- когда право было выдано; для audit и сортировки в UI
    unique (node_id, user_id)                                           -- одна явная запись на пару (node, user); обновление = UPDATE actions, не INSERT
    ```
    Виртуальные `Traverse` (право пройти через узел к потомку с явным правом) здесь не хранятся — вычисляются на лету в RBAC Service по `exists`-запросу на потомках.

*   **`share_links`** — анонимные временные ссылки, только Web.
    ```sql
    token       text primary key                              -- значение токена в URL, 32 байта crypto/rand → base64url. Сам токен — PK для O(1) lookup и защиты от тайминг-атак (постоянное время поиска).
    node_id     uuid references nodes(id) on delete cascade   -- на какой узел выдан доступ; удаление узла инвалидирует ссылки
    actions     bit(8) not null                                -- те же биты что в permissions: scope ссылки (Read | Read+Write | Read+Write+Remove). Admin-биты запрещены проверкой в Auth Service.
    expires_at  timestamptz not null                           -- момент истечения; обязательное поле — бессрочных share-links нет
    max_uses    int                                            -- максимум использований; NULL = без ограничения по числу использований (только по времени)
    used_count  int default 0                                  -- счётчик использований, инкрементится в Auth при успешной валидации; сравнивается с max_uses
    created_by  uuid references users(id)                      -- кто создал ссылку; для audit и UI «мои ссылки»
    created_at  timestamptz default now()                      -- момент создания; для UI и retention
    ```

*   **`node_settings`** — настройки узла (применимы и к папкам, и к файлам; наследуются через ltree-lookup ближайшего предка/самого узла с не-NULL значением для конкретного поля). Поля версионирования сейчас зарезервированы под §5.
    ```sql
    node_id          uuid primary key references nodes(id) on delete cascade  -- узел, к которому применяется настройка; 1:1 с nodes (любой type)
    max_versions     int                                                      -- §5: сколько версий хранить; NULL = наследовать
    retention_days   int                                                      -- §5: TTL версий в днях; NULL = наследовать
    version_policy   text                                                     -- §5: политика версионирования ('off' | 'on_change' | ...); NULL = наследовать
    created_at       timestamptz default now()                                -- момент применения настройки; для audit
    updated_at       timestamptz default now()                                -- последнее изменение
    ```
    `storage_backend` сюда не выносится — он живёт прямо в `nodes.backend_kind` (см. описание `nodes`). Для остальных полей effective значение узла = ближайший не-NULL предок (включая сам узел); если ни у одного нет — default поля.

*   **`jobs`** — очередь асинхронной индексации (см. §«Индексация»).
    ```sql
    id           bigserial primary key
    node_id      uuid not null references nodes(id) on delete cascade  -- какой узел обрабатывать
    kind         text not null                                          -- тип воркера: 'hash' | 'exif' | 'face' | ...; задаёт, какой worker возьмёт job
    status       text not null                                          -- 'pending' | 'in_progress' | 'failed' (терминальные 'done' и окончательный 'failed' уходят в jobs_history, см. ниже)
    attempts     int default 0                                          -- сколько раз пробовали; используется для backoff и dead-letter порога
    last_error   text                                                   -- последнее сообщение об ошибке (для дебага и audit)
    locked_until timestamptz                                            -- если статус 'in_progress' — до какого момента lease у воркера; sweeper перехватывает зависшие
    created_at   timestamptz default now()
    updated_at   timestamptz default now()
    ```
    Воркеры берут задачи через `SELECT ... FOR UPDATE SKIP LOCKED WHERE status='pending' AND kind=$1 ORDER BY created_at LIMIT N`. Индексы: btree `(kind, status, created_at)`, btree `(node_id)`. Таблица остаётся «горячей» и маленькой — терминальные строки переезжают в `jobs_history`.

*   **`jobs_history`** — append-only архив завершённых индексационных задач.
    ```sql
    id            bigint                                                  -- тот же id, что был в jobs (не PK — после партиционирования уникальность гарантирует приложение)
    node_id       uuid                                                    -- без FK: после удаления узла история сохраняется для аудита
    kind          text not null                                           -- тип воркера
    final_status  text not null                                           -- 'done' | 'failed'
    attempts      int not null
    last_error    text                                                    -- NULL для 'done'
    enqueued_at   timestamptz not null                                    -- бывший jobs.created_at
    finished_at   timestamptz not null default now()                      -- момент архивации
    ) partition by range (finished_at);                                   -- помесячные партиции; drop старых через DROP PARTITION
    ```
    Индексы на каждой партиции: btree `(node_id, finished_at)` для поиска по узлу, btree `(kind, final_status)` для агрегатов. Retention — конфигурируемо (default 90 дней).

*   **`outbox`** — журнал незавершённых FS-операций (см. §3.1).
    ```sql
    id           bigserial primary key                  -- порядок вставки = порядок попыток выполнения (FIFO внутри одного executor'а)
    op           text not null                          -- тип операции: 'create_file' | 'delete' | 'rename' | 'trash' | 'migrate_backend' | ... — определяет executor'а
    node_id      uuid                                   -- какой узел затронут; nullable т.к. некоторые операции (например, GC) могут не быть привязаны к одному узлу
    payload      jsonb not null                         -- всё, что нужно для идемпотентного выполнения: upload_id, backend_ref до/после, trash_uuid, acl_snapshot и т.п.
    status       text not null                          -- 'pending' | 'in_progress' | 'failed'; терминальные исходы уезжают в outbox_history (см. ниже)
    attempts     int default 0                          -- попытки; после порога — статус 'failed' и алерт
    created_at   timestamptz default now()              -- момент enqueue; для recovery «старше N минут — перехватить»
    locked_until timestamptz                            -- lease executor'а; истёкший lease означает, что executor умер и запись можно подобрать
    ```
    Индекс: partial btree `(status, locked_until) where status in ('pending','in_progress')` — для быстрого picking активных записей. Таблица — горячая и маленькая по построению; вся история живёт в `outbox_history`.

*   **`outbox_history`** — append-only архив завершённых FS-операций.
    ```sql
    id            bigint                                                  -- тот же id, что был в outbox
    op            text not null                                           -- тип операции (create_file, trash, rename, migrate_backend, ...)
    node_id       uuid                                                    -- без FK: запись об операции переживает удаление узла
    payload       jsonb not null                                          -- payload, как был в outbox
    final_status  text not null                                           -- 'done' | 'failed'
    attempts      int not null
    last_error    text                                                    -- NULL для 'done'
    enqueued_at   timestamptz not null                                    -- бывший outbox.created_at
    finished_at   timestamptz not null default now()
    ) partition by range (finished_at);                                   -- помесячные партиции
    ```
    Индексы на каждой партиции: btree `(node_id, finished_at)` — «история операций по узлу», btree `(op, final_status, finished_at)` — алерты/агрегаты по `failed`. Retention — конфигурируемо (default 90 дней), сброс старых партиций — `DROP PARTITION`, не `DELETE`.

*   **`users`**, **`sessions`**, **`app_passwords`**, **`audit_log`** — см. §3.3.

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

3.  **Admin-действия** (изменение `permissions`, `node_settings`, а также `nodes.backend_kind` на папке — т.е. смена политики бэкенда) требуют бита `Admin` в effective permissions.

#### ⚙️ Индексация: Pipeline Workers

1.  **Trigger:** загрузка через API, перемещение, `fsck`-сканер.
2.  **Stage 1 (Sync, в транзакции загрузки):** вставка в `nodes` (`size`, `mtime`, `type`, `backend_kind`, `backend_ref`), вставка job в `jobs`, коммит.
3.  **Stage 2 (Async):** пул горутин берёт задачи через `SELECT ... FOR UPDATE SKIP LOCKED`.
    *   *Hasher:* sha256/md5 → `nodes.sha256`, `node_meta`.
    *   *ImageProc:* EXIF/Geo/Dimensions → типизированные колонки `node_meta`.
    *   *AI:* face recognition → `node_meta.extra`.
4.  Ошибки → retry с backoff, после N попыток — `final_status='failed'` в `jobs_history` + audit event.
5.  **Завершение:** успех или окончательный failed → в одной транзакции `INSERT INTO jobs_history (..., final_status, finished_at) ... + DELETE FROM jobs WHERE id=$1`. Горячая `jobs` остаётся короткой, история запросов по узлу — в `jobs_history`.

#### 🔄 Корзина

*   Файл/папка перемещается в `<storage-meta-dir>/trash/<node_uuid>/`, рядом записывается **служебный** `trash.meta.json` с `original_path`, `original_parent_id`, `deleted_by`, `deleted_at`, `acl_snapshot`, `backend_kind`, `backend_ref`. Это не side-car юзер-файла, это самодостаточная запись корзины — позволяет восстановить элемент даже без БД.
*   В `nodes` — `status='deleted'`, `deleted_at`. Перемещение для `FlatFile` — `os.Rename`; для будущих бэкендов — `BackendRef` сохраняется в `trash.meta.json`, физическое содержимое не дублируется.
*   **GC:** фоновый воркер чистит записи старше N дней (настройка retention в config) — удаляет payload через `FileBackend.Delete` и саму папку.
*   **Версионирование** — отложено в §5; место под `versions` зарезервировано в `node_settings`.

#### 🌐 Интерфейсы

*   **Web:** embedded SPA (React/Svelte) через `embed.FS`. Build step (`npm run build`) — часть релизной сборки, требует Node в CI. API — JSON.
*   **FTP:** `github.com/fclairamb/ftpserverlib` с кастомным Driver.
    *   Только **FTPS** (explicit TLS через `AUTH TLS`). Plain FTP отключён.
    *   **Share-links не поддерживаются** — только именованные пользователи.

---

### 3. Сквозные механизмы

#### 3.1 Атомарность FS ↔ DB (Outbox)

Операции затрагивают два независимых хранилища — диск и PostgreSQL. Чистых распределённых транзакций нет, поэтому используем **transactional outbox + идемпотентное доигрывание**. Схема таблицы `outbox` — в §2.

**Порядок операций (на примере загрузки):**
1. Клиент шлёт данные → `FileBackend` пишет в staging внутри `<storage-meta-dir>/uploads/<id>`, fsync.
2. Транзакция PostgreSQL: insert `nodes(status='pending', backend_kind, backend_ref)`, insert `outbox(op='create_file', payload={upload_id, backend_ref})`.
3. Коммит. С этого момента операция **должна быть доведена до конца**.
4. Executor вызывает `FileWriter.Commit` → бэкенд публикует содержимое (для `FlatFile` — `rename` staging → целевой путь).
5. Транзакция: `nodes.status='ready'`, обновление `size`/`sha256`, **архивация outbox-записи в `outbox_history`** (`INSERT INTO outbox_history ... SELECT ... FROM outbox WHERE id=$1; DELETE FROM outbox WHERE id=$1` — в одной транзакции). Терминальные `failed` после исчерпания retry-порога архивируются аналогично, с `final_status='failed'` и `last_error`.

**Удаление:**
1. Транзакция: `nodes.status='deleted'`, `deleted_at=now()`, insert outbox `op='trash', payload={node_id, backend_ref, trash_uuid, acl_snapshot}`.
2. Executor: переносит payload в `<storage-meta-dir>/trash/<uuid>/`, записывает служебный `trash.meta.json`.
3. Архивация в `outbox_history` + delete из `outbox` в одной транзакции (как в п.5 выше).

**Rename/move:** аналогично, outbox описывает целевое состояние. Для `FlatFile` это `os.Rename` файла + при необходимости перевычисление `backend_ref` в `nodes`.

**Идемпотентность:** каждый executor при старте проверяет текущее состояние диска через `FileBackend.Stat` и доводит до целевого. Повторный запуск безопасен.

**Recovery после краша:** при старте процесс читает `outbox where status in ('pending','in_progress')` и доигрывает. Записи старше N минут с истёкшим `locked_until` перехватываются другими инстансами/рестартами.

**Orphan staging files:** если процесс упал до коммита транзакции, в `<storage-meta-dir>/uploads/` может остаться файл без соответствующей записи в `outbox`/`nodes`. Sweeper периодически удаляет `uploads/*` старше TTL без связанной outbox-записи (см. §3.2).

**Sweeper:** фоновая задача периодически сверяет `outbox` с диском, перезапускает зависшие операции, эскалирует `failed` (после исчерпания попыток) → архивирует в `outbox_history` с `final_status='failed'` + поднимает алерт.

**Партиции истории:** отдельный cron создаёт партиции `outbox_history`/`jobs_history` на N месяцев вперёд и дропает партиции старше retention. Дроп `DROP PARTITION` — O(1), не нагружает autovacuum.

**Атомарность на уровне одного файла:**
- Запись: `open tmp → write → fsync → rename`. POSIX гарантирует атомарность rename в пределах ФС.
- Финализация бэкенда (`FileWriter.Commit`) гарантирует либо целевое состояние, либо staging (отменяемое через `Abort`/sweeper). Никакого промежуточного «полузаписанного» файла в целевом дереве не появляется.

#### 3.2 Upload Path

**Web: tus.io** (`github.com/tus/tusd` как embedded handler).
- Resumable, chunked, стандартизированный протокол.
- Чанки пишутся в `<storage-meta-dir>/uploads/<upload_id>` (а не в целевой путь).
- По завершении `Upload-Complete` → запускается процедура §3.1 (insert outbox → executor rename).
- Валидация: `max_size`, MIME по magic bytes (не по заголовку `Content-Type`), проверка квот **до** начала аллокации.

**FTP: STOR.**
- Driver перенаправляет `STOR` в `<storage-meta-dir>/uploads/<random>`.
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
    id              uuid primary key                          -- стабильный ID пользователя; на него ссылаются permissions, sessions, audit_log
    login           citext unique not null                    -- логин в case-insensitive виде; уникален. citext чтобы 'Alex' и 'alex' были одним логином.
    password_hash   text not null                             -- argon2id-хеш в полной encoded-форме ($argon2id$v=19$m=65536,t=3,p=2$<salt>$<hash>) — параметры внутри хеша, не отдельно
    totp_secret     bytea                                     -- секрет TOTP, зашифрован AEAD-ключом из config.json. NULL = 2FA не включена.
    failed_attempts int default 0                             -- счётчик подряд идущих неудачных попыток логина; сбрасывается на 0 при успешном логине
    locked_until    timestamptz                               -- до какого момента аккаунт заблокирован (backoff); NULL = не заблокирован. После expiry проверка снова разрешена.
    created_at      timestamptz default now()                 -- момент регистрации
    ```

**Сессии Web:**
- HttpOnly + Secure + SameSite=Lax cookie (`session_id`).
- Таблица `sessions`:
    ```sql
    id          text primary key                              -- значение cookie, 32 байта crypto/rand → base64url. PK = сам секрет; lookup за O(1).
    user_id     uuid not null references users(id) on delete cascade  -- владелец сессии; logout всех при удалении пользователя
    created_at  timestamptz default now()                     -- момент логина; используется для absolute_timeout
    last_seen   timestamptz default now()                     -- момент последнего запроса; обновляется на каждый authenticated request (батчем, не на каждый чих)
    expires_at  timestamptz not null                          -- эффективный момент expiry = min(last_seen + idle_timeout, created_at + absolute_timeout); кэшируется здесь
    ip          inet                                          -- IP последнего запроса; для audit / детекции переезда сессии
    user_agent  text                                          -- UA последнего запроса; для UI «активные устройства»
    ```
- Sliding expiration: `expires_at = last_seen + idle_timeout`, не превышает `created_at + absolute_timeout`.
- Logout — delete записи. Logout всех устройств — `delete where user_id=$1`.
- **CSRF:** double-submit cookie + `X-CSRF-Token` header для всех мутирующих запросов. tus эндпоинты защищаются через origin check + Authorization header.
- **Security headers:** `Content-Security-Policy`, `Strict-Transport-Security`, `X-Frame-Options: DENY`, `Referrer-Policy: same-origin`.

**Rate limiting / brute-force:**
- Token bucket in-memory по `(login)` и `(client_ip)` для `/login` и `/share-link/<token>`.
- После N неудач — `failed_attempts++`, при достижении порога `locked_until = now() + backoff(failed_attempts)`.
- Audit event на каждую неудачу.

**FTP auth:**
- Только FTPS. Тот же `users.password_hash`, та же процедура.
- **App passwords:** опциональная таблица для отдельных токенов под FTP-клиенты (чтобы не вводить основной пароль в каждый FTP-клиент). Обязательны для пользователей с включённой 2FA (FTP 2FA не поддерживает).
    ```sql
    app_passwords (
      id         uuid primary key,                                           -- ID токена; для UI «отозвать этот token»
      user_id    uuid not null references users(id) on delete cascade,       -- владелец; удаление пользователя зачищает токены
      label      text not null,                                              -- человеко-читаемое имя ('iPhone Files', 'rsync laptop'); только для UI
      hash       text not null,                                              -- argon2id-хеш самого токена; сам токен показывается пользователю один раз при создании
      created_at timestamptz default now(),                                  -- момент создания
      last_used  timestamptz                                                 -- последнее успешное использование; обновляется на login, не на каждый запрос. NULL = ни разу не использовано.
    )
    ```

**2FA (опционально, за флагом в конфиге):**
- TOTP (`github.com/pquerna/otp/totp`).
- `totp_secret` шифруется AEAD-ключом из `<storage-meta-dir>/config.json`.

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
  id        bigserial primary key,                  -- порядок вставки = порядок событий
  ts        timestamptz default now(),              -- момент события; ключ партиционирования по месяцу
  user_id   uuid,                                   -- инициатор; NULL для системных событий (cron snapshot, GC, sweeper)
  action    text not null,                          -- что произошло: 'login' | 'login_failed' | 'logout' | 'acl_change' | 'share_create' | 'share_use' | 'upload' | 'delete' | 'rename' | 'backup' | 'recover' | ...
  node_id   uuid,                                   -- какого узла касается (если применимо); NULL для не-FS событий (login и т.п.)
  ip        inet,                                   -- IP инициатора; NULL для системных событий
  result    text not null,                          -- исход: 'ok' | 'denied' (отказ по ACL/rate-limit) | 'error' (внутренняя ошибка)
  details   jsonb                                   -- произвольный контекст: для 'acl_change' — diff actions; для 'share_use' — token id; для 'login_failed' — причина; etc.
)
```
Партиционируется по месяцу (`partition by range(ts)`), retention настраивается (дефолт 12 месяцев — старые партиции дропаются). Индексы внутри каждой партиции: btree `(user_id, ts)`, btree `(node_id, ts)`, btree `(action, ts)`.

#### 3.4 Бэкапы & Disaster Recovery

БД — единственный источник правды для метаданных. DR-стратегия — две линии.

**Линия 1 (основная): периодические `pg_dump`.** Cron внутри процесса (дефолт раз в 24h, конфигурируется через `backup.interval`) запускает `pg_dump --format=custom` в pipe → файл `<storage-meta-dir>/backups/<ISO-ts>.dump`. Custom format уже сжат, поддерживает партикулярный restore через `pg_restore -t <table>`, содержит схему и checksum'ы. Транзакция `pg_dump` — MVCC snapshot, не блокирует writers. Ручной запуск: `storman backup`.

**Линия 2 (последняя): `recover --from-disk`.** Walk по `storage-dir`, создание `nodes` со sha256 и owner-only ACL. Расширенная мета (EXIF, share_links, custom ACL) теряется. Применяется когда нет ни PG, ни дампа.

**Recovery:** `storman recover --from-backup [<path>]` → `pg_restore` указанного дампа (по умолчанию самого свежего из `<storage-meta-dir>/backups/`) в пустую БД, далее fsck-reconcile с диском (см. §4).

**Retention:** дефолт 7 последних `.dump` файлов, старше — удаляются. Конфигурируется через `backup.retention`.

**Конфигурация:**
```json
"backup": {
  "interval": "24h",
  "retention": 7
}
```

**Стоимость:** для 100k файлов — единицы–десятки MB на дамп (custom format уже сжат). Создание — секунды, не блокирует работу сервиса.

---

### 4. Критические моменты и рекомендации

1.  **Disaster Recovery (по убыванию полноты, см. §3.4):**

    **`storman recover --from-backup [<path>]`** — `pg_restore` указанного дампа (по умолчанию самого свежего `.dump` из `<storage-meta-dir>/backups/`) в пустую БД. После заливки — fsck-reconcile с диском:
    *   Для каждой `nodes` row — `FileBackend.Stat(backend_ref)`. Отсутствует на диске → `status='broken'`, audit warning.
    *   Walk по диску — orphan-файлы (нет ссылки в восстановленной БД) → перемещаются в `<storage-meta-dir>/quarantine/<ts>/` с manifest для ручного review.
    *   Опционально (`--verify-sha256`): полный пересчёт sha256 для всех файлов и сверка с БД. Дорого, но детектит порчу контента.

    **`storman recover --from-disk`** — последняя линия (нет ни PG, ни дампа). Walk по storage_root, для каждого файла:
    *   Создаётся `nodes` с `backend_kind='flat'`, `backend_ref`=relative path, sha256 пересчитывается, mime определяется по magic bytes.
    *   ACL — owner-only для root-пользователя; пользовательские ACL не восстанавливаются.
    *   `node_meta.extra` — пусто; индексация запускается заново (EXIF, AI и т.п. пересчитываются).
    *   `share_links`, `node_settings`, расширенная мета — теряются.
    *   Узлы помечаются `recovered_from_disk=true` в audit для последующего ручного review.

    **Outbox-recovery после краша процесса** (не DR, штатная процедура §3.1) — отдельно от disaster recovery: читает `outbox` из PG, доигрывает незавершённые операции через соответствующие `FileBackend.Commit`/`Abort`.

2.  **Производительность RBAC:**
    *   `ltree` + gist index даёт O(log n) ancestor/descendant lookup.
    *   ACL Cache: LRU с epoch-based invalidation per subtree — без явного обхода при изменениях.
    *   Прогрев кэша при логине для последних использованных путей.

3.  **Консистентность с внешними изменениями:**
    *   Изменения в обход системы (SSH, rsync) → рассинхрон.
    *   Решение: периодический `fsck`-сканер (cron внутри процесса) + опциональный `fsnotify` watcher.
    *   Правило разрешения конфликта: **БД — источник правды** для ACL и meta. Файл на диске без записи в БД → orphan, перемещается в `<storage-meta-dir>/quarantine/` для ручного review. Запись в БД без файла на диске → `status='broken'`, audit warning.

4.  **Безопасность:**
    *   Строгая валидация путей: canonicalize + проверка, что результат внутри `storage_root`, запрет symlink-ов наружу.
    *   Изоляция `storage-meta-dir` — обеспечивается физическим разделением каталогов (`flat-storage/` и `meta-storage/` — соседи внутри `data-dir`), Web/FTP/FUSE по построению не видят служебные данные.
    *   OWASP: argon2id, CSRF, rate limiting, HttpOnly cookies, CSP, MIME validation по magic bytes — см. §3.3, §3.2.
    *   TLS обязателен: HTTPS для Web, FTPS для FTP. Plain FTP отключён на уровне listener'а.

---

### 5. Отложенные расширения

Не входят в MVP, но архитектура заложена так, чтобы добавлять их без переписывания базовых слоёв.

#### 5.1 CDC-бэкенд (`CDCFile`)

**Зачем:** дедупликация, дешёвая дельта-передача (rsync-style), хранение многих версий без линейного роста диска, готовность к remote blob storage (S3 и т.п.).

**Где включается:** `nodes.backend_kind = 'cdc'` на папке (политика для новых детей). Наследуется потомками через ltree-lookup. Применяется к **новым** файлам — существующие остаются на своём бэкенде.

**Раскладка на диске** (под `<storage-meta-dir>/blobs/`):
- `chunks/<aa>/<bb>/<sha256>` — иммутабельные чанки. Имя = SHA-256 содержимого. Шардирование по первым байтам хеша.
- `manifests/<manifest_id>.json` — манифест файла: `{ size, chunk_size_target, chunks: [{hash, offset, len}, ...] }`. `manifest_id` = `BackendRef.Data` в `nodes`.
- `refcounts/<aa>/<bb>/<sha256>` — счётчики ссылок чанков для GC. Альтернативно — PG-таблица `cdc_chunk_refs`:
    ```sql
    cdc_chunk_refs (
      chunk_hash  bytea primary key,                  -- SHA-256 чанка, 32 байта; совпадает с именем файла в blobs/chunks/
      refcount    int not null default 0,             -- сколько манифестов ссылается на этот чанк; 0 = кандидат на удаление
      size        int not null,                       -- размер чанка в байтах; для отчёта об экономии дедупа
      created_at  timestamptz default now(),          -- момент первого появления; для retention 'старше N часов' перед удалением
      last_ref_at timestamptz                         -- момент последнего инкремента refcount; для трассировки
    )
    ```
    Преимущество PG-таблицы перед файлами `refcounts/*` — атомарность инкремента/декремента в той же транзакции, что и insert/delete манифеста. Минус — лишний traffic на PG при массовом upload.

**Чанкование:** FastCDC, target ~1 МБ, диапазон 256 КБ — 4 МБ. Без сжатия и без шифрования по умолчанию (чтобы random read оставался дешёвым). Если шифрование/сжатие добавляется — только seekable-форматы (zstd seekable frames, AEAD по под-фреймам), осознанный trade-off.

**`OpenRead`:** читает манифест → строит таблицу `(offset → chunk)` → `ReadAt(off, len)` находит нужный чанк через бинпоиск, открывает блоб, делает `pread(chunk_fd, off_in_chunk, len)`. Кеши: LRU открытых fd чанков, LRU манифестов в памяти.

**`OpenWrite` (Modify-режим, по выбору пользователя — вариант «б»):**
- Открываем sparse staging tmpfile в `<storage-meta-dir>/uploads/<id>` размером с исходный файл, но без копирования содержимого.
- Поддерживаем in-memory bitmap «какие страницы материализованы».
- При `ReadAt` (если writer хочет читать) и при первом `WriteAt` в страницу — лениво подтягиваем нужные чанки из blobstore в tmpfile (copy-on-write на уровне страниц).
- `WriteAt` после материализации = обычный `pwrite` в tmpfile.
- На `Commit` прогоняем FastCDC через tmpfile (целиком), пишем новые чанки (старые с тем же хешем не пересохраняются — дедуп), пишем новый манифест, возвращаем новый `BackendRef`. Старый манифест и его уникальные чанки удаляются по refcount после atomic swap в `nodes`.
- На `Abort` — `unlink` tmpfile, ничего не публикуется.

**`OpenWrite` (Create/Overwrite):** tmpfile с нуля, обычные `WriteAt`, на `Commit` — chunking + manifest. Без lazy fetch (нечего фетчить).

**GC чанков:** фоновый воркер сверяет `refcounts` с активными манифестами; чанки с refcount=0 старше N часов удаляются.

**Verify/scrub:** периодический проход по чанкам — сверка `sha256(content) == filename`. Самопроверяемость хранения.

**Recover:** манифесты — авторитетный источник, чанки иммутабельны и адресуются по содержимому. Потеря отдельного манифеста = потеря одного файла; чанки остаются и могут быть переиспользованы. Манифесты бэкапятся отдельно (мелкие JSON-файлы).

#### 5.2 Миграция между бэкендами

`storman migrate --to=cdc <path>` — фоновая операция:
1. Walk по поддереву, отбирает файлы с `backend_kind != 'cdc'`.
2. Для каждого файла: читает через текущий `FileBackend` → пишет через целевой → atomic swap `(backend_kind, backend_ref)` в `nodes` через outbox → удаляет старое содержимое через старый `FileBackend.Delete`.
3. Идемпотентно (через outbox); прерывание безопасно, перезапуск возобновляет.
4. Не блокирует чтения/записи: миграция одного файла atomically переключает его на новый ref.

#### 5.3 Версионирование

Удобно ложится поверх `FileBackend` с иммутабельными ref:
- Таблица `versions`:
    ```sql
    versions (
      node_id        uuid not null references nodes(id) on delete cascade,  -- к какому узлу относится версия
      version_num    int not null,                                            -- порядковый номер (1, 2, 3...) внутри node_id; растёт монотонно
      backend_kind   text not null,                                           -- бэкенд этой версии; может отличаться от текущего nodes.backend_kind (миграции)
      backend_ref    text not null,                                           -- иммутабельный ref на содержимое версии в backend'е
      size           bigint not null,                                         -- размер версии в байтах
      sha256         bytea not null,                                          -- SHA-256 версии; сравнение с другими версиями = дедуп-индикатор
      created_by     uuid references users(id),                               -- кто создал версию (т.е. кто записал новое содержимое); NULL если создана системой (миграция, recover)
      created_at     timestamptz default now(),                               -- момент создания версии
      meta_snapshot  jsonb,                                                   -- snapshot relevant полей nodes/node_meta на момент создания: name, mime, mtime, EXIF и т.п. — чтобы при rollback на версию восстановить полный контекст
      primary key (node_id, version_num)
    )
    ```
- Создание версии = текущие `(backend_kind, backend_ref)` копируются записью в `versions`; новая запись становится текущей в `nodes`.
- Для `FlatFile` это требует физического копирования старого файла (или линка) под id-based путь в `<storage-meta-dir>/versions/<uuid>/v<N>` — ради иммутабельности ref.
- Для `CDCFile` копирование бесплатное: ref манифеста уже иммутабелен, просто сохраняем его в `versions` и продолжаем использовать.
- `node_settings.max_versions`, `retention_days`, `version_policy` — поля уже зарезервированы в schema (см. §3.1).
- GC версий — фоновый воркер по политике; в `CDCFile` удаление версии = `Delete(BackendRef)` манифеста + декремент refcount чанков.

#### 5.4 Дополнительные протоколы

`FileSystem` рассчитан на расширение без изменений в backend-слое:
- **FUSE**: один из главных драйверов формы интерфейса. `ReadAt`/`WriteAt`/`Commit` мапятся 1:1 на FUSE-операции. Производительные оговорки (random write по большому CDC-файлу, метаданные `stat`/`readdir`) описаны в обсуждении выбора интерфейса — митигации через кеши и staging внутри `CDCFile`.
- **WebDAV**: тривиально поверх `FileSystem` (PROPFIND/GET/PUT/MKCOL/MOVE/DELETE).
- **S3-compat frontend**: PutObject/GetObject — поверх Open/Open*; Multipart Upload — поверх `WriteAt` + `Commit`.
- **rsync**: дельта-протокол можно обслуживать эффективно из `CDCFile` (отдавать список chunk-хешей вместо файла) — отдельный воркер, не часть `FileSystem`.
