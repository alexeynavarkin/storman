# Architecture

storman is a single-binary Go server + embedded React SPA, with PostgreSQL on the backend. Single-host, single-user (or family). This page is an index — for the big picture read [arch/overview.md](arch/overview.md) (5 minutes), then drill into the subsystems.

```text
┌─────────────────────────────────────────────────────────────────┐
│                        Single Binary (Go)                       │
├─────────────────────────────────────────────────────────────────┤
│  Web API │ FTPS │ WebDAV │ tus.io │ share-links                  │
│       all delivery interfaces sit on top of:                     │
│              storage.FileSystem  (ADR-0001 — invariant)          │
│   dbfs ────────►  outbox  ────────►  FileBackend (FlatFile)      │
│       │                                  │                       │
│       ▼                                  ▼                       │
│   PostgreSQL                       <data-dir>/flat-storage/      │
└─────────────────────────────────────────────────────────────────┘
```

## Subsystems

- **[overview.md](arch/overview.md)** — the big picture + on-disk layout (`flat-storage/`, `meta-storage/{trash,uploads,backups}`).
- **[storage.md](arch/storage.md)** — `FileSystem` / `FileBackend` interfaces, the outbox pattern, the upload path, the write lifecycle. Foundational for understanding the system's invariants.
- **[database.md](arch/database.md)** — PostgreSQL schema, partitioning, indexes.
- **[rbac.md](arch/rbac.md)** — permission model (Read/Write/Remove/Admin + computed Traverse), share-links.
- **[auth.md](arch/auth.md)** — sessions, CSRF, app-passwords, login security, audit log.
- **[trash.md](arch/trash.md)** — soft-delete, restore, trash GC.
- **[indexing.md](arch/indexing.md)** — async pipeline workers (hash in MVP; EXIF/MIME/face in ROADMAP).
- **[interfaces.md](arch/interfaces.md)** — details of each of the five delivery protocols.
- **[backup-dr.md](arch/backup-dr.md)** — `pg_dump --format=custom` (line 1) + `recover --from-disk` (last-resort line 2).

## Architectural decisions

Load-bearing decisions with alternatives and rationale live in [adr/](adr/). These ADRs are immutable: when a decision changes, a new ADR is written with status `supersedes ADR-NNNN`; the old one is not edited.

Current set:

- **[ADR-0001 FileSystem boundary](adr/0001-filesystem-boundary.md)** — all delivery protocols go through `storage.FileSystem`, bypasses are forbidden. **The cornerstone invariant of the system.**
- **[ADR-0002 Outbox for FS↔DB atomicity](adr/0002-outbox-fs-db-atomicity.md)** — transactional outbox + idempotent re-play; why not XA / 2PC.
- **[ADR-0003 Computed Traverse in RBAC](adr/0003-rbac-computed-traverse.md)** — Traverse permissions are computed virtually from descendant ACLs; why not materialized.
- **[ADR-0004 App-passwords as cross-protocol auth](adr/0004-app-passwords-cross-protocol.md)** — one app-password = all non-web protocols; why not per-protocol scope.
- **[ADR-0005 DR via pg_dump + disk-walk](adr/0005-dr-pg-dump-plus-disk-walk.md)** — two DR lines; why not WAL streaming / replication.
- **[ADR-0006 FlatFile-only MVP](adr/0006-flatfile-only-mvp.md)** — CDC deferred; why not both backends at once.

## Context around the architecture

- **[MISSION.md](MISSION.md)** — north-star (durability) + principles + bets / anti-bets. If an architectural decision does not serve the mission, it is reconsidered.
- **[ROADMAP.md](ROADMAP.md)** — Now / Next / Later. Currently Now = MVP delivered.
- **[risks.md](risks.md)** — risk register. What can go wrong and how we mitigate.

## Where the code lives

```
storman/
├── cmd/storman/          # main, cobra root
├── internal/
│   ├── audit/            # audit log service
│   ├── auth/             # users, sessions, app_passwords, sharelinks
│   ├── backup/           # pg_dump scheduler
│   ├── cli/              # cobra subcommands: serve, init, migrate, backup, recover, bootstrap, useradd
│   ├── config/           # config.json schema + Load
│   ├── datadir/          # disk layout helpers (paths, Bootstrap)
│   ├── db/               # pgxpool + migrate runner; testpg helper
│   ├── ftpsrv/           # FTPS server (ftpserverlib + afero adapter)
│   ├── jobs/             # async indexing pipeline (Service, Pool, hasher)
│   ├── migrations/       # embedded SQL migrations
│   ├── rbac/             # permissions, Traverse computation, ACL cache
│   ├── storage/          # FileSystem + FileBackend interfaces, errors, types
│   │   ├── dbfs/         # the (only) FileSystem implementation, outbox, trash, recovery
│   │   └── flat/         # the (only-in-MVP) FileBackend implementation
│   └── web/              # HTTP server, Web API, tus, WebDAV, share-link routes, SPA serving
└── ui/                   # React 19 + Vite + Tailwind + shadcn SPA (embedded via //go:embed)
```

Any new code that mutates the file tree starts with the question: "are the existing `storage.FileSystem` methods enough?" If yes — add a thin handler in `internal/web/`, `internal/ftpsrv/`, etc. If not — extend the interface and implement it in `dbfs` ([ADR-0001](adr/0001-filesystem-boundary.md)).
