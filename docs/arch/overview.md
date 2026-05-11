# System overview & disk layout

storman is a single-binary Go server + a React SPA (embedded into the binary) + PostgreSQL. One process per host; one user / a small family. The goal of this section is to give the reader the big picture in 5 minutes before diving into subsystem details.

## Big picture

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
│  │      the single point for all mutations (see ADR-0001)    │  │
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

Two independent stores (PostgreSQL + disk), connected by a transactional outbox pattern. Each mutation runs a first transaction (insert outbox + node `status='pending'`), then the backend publishes content, then a second transaction (`status='ready'` + outbox archiving). A crash at any point — recovery on process startup replays the remaining outbox rows. More: [storage.md](storage.md) and [ADR-0002](../adr/0002-outbox-fs-db-atomicity.md).

All delivery protocols (Web API, FTPS, WebDAV, tus.io, share-links) go through `storage.FileSystem`. This is an invariant, not a convention — see [ADR-0001](../adr/0001-filesystem-boundary.md).

## On-disk layout

A single config parameter — `data_dir`. The layout inside is fixed, not configurable:

```text
<data-dir>/                     # = config.data_dir, e.g. /var/lib/storman
├── config.json                 # local service settings (DSN, secrets_key, etc.)
├── flat-storage/               # storage-dir: user files as-is, no side-cars
│   ├── photos/
│   │   └── image.jpg
│   └── docs/
│       └── report.pdf
└── meta-storage/               # storage-meta-dir: service data
    ├── trash/
    │   └── <node_uuid>/
    │       ├── payload         # deleted file/subtree
    │       └── trash.meta.json # original_path, deleted_by, acl_snapshot, …
    ├── uploads/                # tus staging + FileWriter staging
    ├── backups/                # PG dumps: <ISO-ts>.dump
    └── blobs/                  # [deferred] CDC blobstore
```

`flat-storage/` and `meta-storage/` are neighbours inside a single `data-dir`, always on the same filesystem. This gives:

- **Atomic `rename`** between staging and the destination path (POSIX guarantees rename atomicity within a filesystem). No need for separate `st_dev` checks.
- **Isolation of service data from user data.** Web/FTP/WebDAV see `flat-storage/` as the root of their tree; the service-side `trash/`, `uploads/`, `backups/` live next to it and are inaccessible to those protocols by construction.
- **Clean `flat-storage/` backups** for user tools (Time Machine, restic, rsync). PG backups go separately into `meta-storage/backups/`.
- **Independent evolution.** Quotas, retention, GC apply to meta independently of content.

`flat-storage/` is what [ADR-0006](../adr/0006-flatfile-only-mvp.md) calls "honest storage": files are on disk as-is, no side-cars. The CDC blobstore (when it lands, see ROADMAP) lives in `meta-storage/blobs/` and does not interfere with the user-visible tree.

## Configuration

`config.json` is validated on load:

- `data_dir` matches the location of the config file itself (a guard against copying the config between hosts).
- `secrets_key` decodes to 32 bytes (AEAD for encrypting TOTP secrets and similar).
- The DSN parses as a valid URL.

Other sections (`backup`, `web`, `trash`, `ftp`, `indexing`, `tus`, `webdav`) are optional with sensible defaults. Subsystem details live in the corresponding arch documents.

## What's next

- [storage.md](storage.md) — `FileSystem` / `FileBackend` interfaces, the outbox, the upload path, the write lifecycle.
- [database.md](database.md) — PostgreSQL schema, partitioning, indexes.
- [rbac.md](rbac.md) — permission model, computed Traverse, share-links.
- [auth.md](auth.md) — sessions, CSRF, app-passwords, login security.
- [trash.md](trash.md) — soft-delete, GC.
- [indexing.md](indexing.md) — async pipeline workers.
- [interfaces.md](interfaces.md) — details for each delivery protocol.
- [backup-dr.md](backup-dr.md) — `pg_dump` + `recover --from-disk`.
