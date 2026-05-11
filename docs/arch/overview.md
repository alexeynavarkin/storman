# System overview & disk layout

storman — single-binary Go-сервер + React SPA (embedded в бинарь) + PostgreSQL. Один процесс на хост; один пользователь / небольшая семья. Цель раздела — дать читателю общую картину за 5 минут перед тем, как нырять в детали подсистем.

## Большая картинка

```text
┌─────────────────────────────────────────────────────────────────┐
│                        Single Binary (Go)                       │
├─────────────────────────────────────────────────────────────────┤
│  ┌────────────┐  ┌────────────┐  ┌────────────┐  ┌───────────┐  │
│  │   Web UI   │  │    FTPS    │  │   WebDAV   │  │   tus.io  │  │
│  │  (React)   │  │(ftpserverlib)│ │(x/net/dav) │  │  (PATCH)  │  │
│  └─────┬──────┘  └─────┬──────┘  └─────┬──────┘  └─────┬─────┘  │
│        │               │               │               │        │
│  ┌─────▼───────────────▼───────────────▼───────────────▼─────┐  │
│  │            HTTP mux + Basic-auth + sessions               │  │
│  └─────────────────────────┬─────────────────────────────────┘  │
│                            │                                    │
│  ┌─────────────────────────▼─────────────────────────────────┐  │
│  │                   storage.FileSystem                      │  │
│  │      единая точка для всех мутаций (см. ADR-0001)         │  │
│  └─────┬─────────────────────────────────────────┬───────────┘  │
│        │                                         │              │
│  ┌─────▼──────┐  ┌──────────┐  ┌─────────────┐  ┌▼────────────┐ │
│  │    dbfs    │  │  outbox  │  │ async jobs  │  │ FileBackend │ │
│  │  (Postgres │◄─┤(see ADR  │  │ (hash, …)   │  │ (FlatFile)  │ │
│  │  + ltree)  │  │  -0002)  │  │             │  │             │ │
│  └─────┬──────┘  └──────────┘  └─────────────┘  └─────┬───────┘ │
│        │                                              │         │
└────────┼──────────────────────────────────────────────┼─────────┘
         │                                              │
   ┌─────▼──────┐                                  ┌────▼─────────┐
   │ PostgreSQL │                                  │  flat-storage│
   │            │                                  │  /, meta-    │
   │   nodes    │                                  │  storage/    │
   │   perms    │                                  │              │
   │   outbox   │                                  │              │
   │   audit    │                                  │              │
   │   ...      │                                  │              │
   └────────────┘                                  └──────────────┘
```

Два независимых хранилища (PostgreSQL + disk), связанные транзакционным outbox-паттерном. Каждая мутация делает первую транзакцию (insert outbox + node `status='pending'`), затем backend публикует контент, затем вторую транзакцию (`status='ready'` + архивация outbox). Crash в любой момент — recovery на старте процесса доигрывает оставшиеся outbox-строки. Подробнее: [storage.md](storage.md) и [ADR-0002](../adr/0002-outbox-fs-db-atomicity.md).

Все delivery-протоколы (Web API, FTPS, WebDAV, tus.io, share-links) ходят через `storage.FileSystem`. Это инвариант, не convention — см. [ADR-0001](../adr/0001-filesystem-boundary.md).

## Раскладка на диске

Один параметр в конфиге — `data_dir`. Внутри фиксированная раскладка, не конфигурируется:

```text
<data-dir>/                     # = config.data_dir, например /var/lib/storman
├── config.json                 # Локальные настройки сервиса (DSN, secrets_key и т.п.)
├── flat-storage/               # storage-dir: пользовательские файлы «как есть», без side-car
│   ├── photos/
│   │   └── image.jpg
│   └── docs/
│       └── report.pdf
└── meta-storage/               # storage-meta-dir: служебные данные
    ├── trash/
    │   └── <node_uuid>/
    │       ├── payload         # удалённый файл/поддерево
    │       └── trash.meta.json # original_path, deleted_by, acl_snapshot, …
    ├── uploads/                # tus-стейджинг + FileWriter staging
    ├── backups/                # PG-дампы: <ISO-ts>.dump
    └── blobs/                  # [отложено] CDC blobstore
```

`flat-storage/` и `meta-storage/` — соседи внутри одного `data-dir`, всегда на одной файловой системе. Это даёт:

- **Атомарный `rename`** между staging и целевым путём (POSIX гарантирует rename в пределах ФС). Никаких отдельных проверок `st_dev`.
- **Изоляция служебных данных от пользовательских.** Web/FTP/WebDAV видят `flat-storage/` как корень своего дерева; служебные `trash/`, `uploads/`, `backups/` лежат рядом и для протоколов недоступны by construction.
- **Чистый бэкап `flat-storage/`** для пользовательских инструментов (Time Machine, restic, rsync). Бэкапы PG идут отдельно в `meta-storage/backups/`.
- **Эволюция раздельно.** Quotas, retention, GC применяются к meta независимо от content.

`flat-storage/` — это то, что [ADR-0006](../adr/0006-flatfile-only-mvp.md) называет «honest storage»: файлы лежат как есть, никаких side-car. CDC blobstore (когда появится, см. ROADMAP) живёт в `meta-storage/blobs/`, не мешая видимому пользователю дереву.

## Конфигурация

`config.json` валидируется при загрузке:

- `data_dir` совпадает с расположением самого файла (защита от копирования конфига между хостами).
- `secrets_key` декодируется в 32 байта (AEAD для шифрования TOTP-секретов и подобного).
- DSN парсится как валидный URL.

Остальные секции (`backup`, `web`, `trash`, `ftp`, `indexing`, `tus`, `webdav`) — опциональные с разумными дефолтами. Подробности по подсистемам — в соответствующих arch-документах.

## Что дальше

- [storage.md](storage.md) — `FileSystem` / `FileBackend` интерфейсы, outbox, upload path, жизненный цикл записи.
- [database.md](database.md) — схема PostgreSQL, партиционирование, индексы.
- [rbac.md](rbac.md) — модель прав, computed Traverse, share-links.
- [auth.md](auth.md) — sessions, CSRF, app-passwords, login security.
- [trash.md](trash.md) — мягкое удаление, GC.
- [indexing.md](indexing.md) — async pipeline workers.
- [interfaces.md](interfaces.md) — детали по каждому delivery-протоколу.
- [backup-dr.md](backup-dr.md) — `pg_dump` + `recover --from-disk`.
