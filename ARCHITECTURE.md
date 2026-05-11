# Architecture

storman — single-binary Go server + embedded React SPA, на бэке PostgreSQL. Single-host, single-user (или семья). Это страница-индекс — для общей картины читать [docs/arch/overview.md](docs/arch/overview.md) (5 минут), далее по подсистемам.

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

## Подсистемы

- **[overview.md](docs/arch/overview.md)** — общая картина + раскладка на диске (`flat-storage/`, `meta-storage/{trash,uploads,backups}`).
- **[storage.md](docs/arch/storage.md)** — `FileSystem` / `FileBackend` интерфейсы, outbox-паттерн, upload path, жизненный цикл записи. Краеугольный для понимания инвариантов системы.
- **[database.md](docs/arch/database.md)** — PostgreSQL schema, партиционирование, индексы.
- **[rbac.md](docs/arch/rbac.md)** — модель прав (Read/Write/Remove/Admin + computed Traverse), share-links.
- **[auth.md](docs/arch/auth.md)** — sessions, CSRF, app-passwords, login security, audit log.
- **[trash.md](docs/arch/trash.md)** — soft-delete, восстановление, GC корзины.
- **[indexing.md](docs/arch/indexing.md)** — async pipeline workers (hash в MVP; EXIF/MIME/face в ROADMAP).
- **[interfaces.md](docs/arch/interfaces.md)** — детали по каждому из пяти delivery-протоколов.
- **[backup-dr.md](docs/arch/backup-dr.md)** — `pg_dump --format=custom` (line 1) + `recover --from-disk` (last resort line 2).

## Архитектурные решения

Load-bearing решения с альтернативами и rationale живут в [docs/adr/](docs/adr/). Эти ADR-ы — immutable: при изменении решения пишется новый ADR со статусом `supersedes ADR-NNNN`, старый не редактируется.

Текущие:

- **[ADR-0001 FileSystem boundary](docs/adr/0001-filesystem-boundary.md)** — все delivery-протоколы ходят через `storage.FileSystem`, обходы запрещены. **Краеугольный инвариант системы.**
- **[ADR-0002 Outbox для FS↔DB атомарности](docs/adr/0002-outbox-fs-db-atomicity.md)** — transactional outbox + idempotent re-play; почему не XA / 2PC.
- **[ADR-0003 Computed Traverse в RBAC](docs/adr/0003-rbac-computed-traverse.md)** — Traverse-права вычисляются виртуально из дочерних ACL; почему не материализованы.
- **[ADR-0004 App-passwords как cross-protocol auth](docs/adr/0004-app-passwords-cross-protocol.md)** — один app-password = все non-web протоколы; почему не per-protocol scope.
- **[ADR-0005 DR pg_dump + disk-walk](docs/adr/0005-dr-pg-dump-plus-disk-walk.md)** — две линии DR; почему не WAL streaming / replication.
- **[ADR-0006 FlatFile-only MVP](docs/adr/0006-flatfile-only-mvp.md)** — CDC отложен; почему не оба backend'а сразу.

## Контекст вокруг архитектуры

- **[MISSION.md](MISSION.md)** — north-star (durability) + принципы + bets / anti-bets. Если архитектурное решение не служит mission'у, его пересматривают.
- **[ROADMAP.md](ROADMAP.md)** — Now / Next / Later. Сейчас Now = MVP доставлен.
- **[docs/risks.md](docs/risks.md)** — риск-регистр. Что может пойти не так, как митигируем.

## Где живёт код

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

Любой новый код, который мутирует файловое дерево, начинается с вопроса: «достаточно ли существующих методов `storage.FileSystem`?». Если да — добавляем тонкий handler в `internal/web/`, `internal/ftpsrv/` и т.п. Если нет — расширяем интерфейс и реализуем в `dbfs` ([ADR-0001](docs/adr/0001-filesystem-boundary.md)).
