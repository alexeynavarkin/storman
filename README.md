# STORMAN

Self-hosted, enterprise-grade personal file storage. One Go binary, PostgreSQL — no other external dependencies. Web UI, FTPS, WebDAV, resumable uploads (tus.io), and temporary share-links — all over a single tree, with one ACL and one audit log.

storman is built for a single user or a family on a single host: cloud UX, but control and data stay with you.

## Features

- **Five delivery interfaces** — Web API + React SPA, FTPS (explicit TLS), WebDAV (`/dav/*`), tus.io resumable uploads, anonymous share-links with TTL.
- **Honest storage** — user files live on disk as-is under `flat-storage/`. No side-cars. Time Machine / restic / rsync produce clean backups directly.
- **RBAC** — Read/Write/Remove/Admin bitmask + computed Traverse, inheritance via an ltree tree, ACL cache with epoch-based invalidation.
- **Durability** — outbox pattern for DB↔disk atomicity, crash recovery on startup, soft-delete via recursive ltree-update with restore.
- **Async indexing** — fault-tolerant queue on `SELECT FOR UPDATE SKIP LOCKED`. In MVP — sha256 hasher; EXIF / MIME / face — in the [roadmap](docs/ROADMAP.md).
- **Disaster recovery** — two lines: periodic `pg_dump --format=custom` + `recover --from-disk` walker as the last line (user content recovers even without a DB).
- **Security** — argon2id passwords, sliding + absolute sessions, double-submit CSRF, mandatory HTTPS/FTPS, app-passwords for non-web protocols, login rate-limit, audit log.

## Quick start

### Docker Compose (production-style)

See [docs/DEPLOY.md](docs/DEPLOY.md).

### Local development

Requirements: Go 1.25+, Node 20+ (for the UI), Docker (for PostgreSQL).

```bash
# One shot: PG in a container + provision the dev data dir + migrations + create dev-user + backend + Vite HMR.
make dev
```

After startup:
- **Backend API:** `http://localhost:8443`
- **UI (Vite HMR):** `http://localhost:5173`
- **Credentials:** `dev` / `devdevdevdev`

See `make help` for more.

## On-disk layout

See [docs/arch/overview.md](docs/arch/overview.md).

## CLI

All commands resolve the config file via `--config=<path>` (highest priority),
the `STORMAN_CONFIG_PATH` env var, or the default `/var/lib/storman/config.json`.
The data directory is whatever directory holds the config file — `init` creates
the layout there on first run.

```
storman init                                             create the layout + config.json
storman migrate                                          apply DB migrations
storman bootstrap                                        create the root tree node
storman useradd    --login=X --password=Y
storman serve                                            run the server
storman backup                                           manual pg_dump (cron does it automatically)
storman recover    from-backup [<path>] | from-disk      DR commands
storman version                                          version from git
```

## Access methods

See [docs/arch/interfaces.md](docs/arch/interfaces.md) and [docs/arch/auth.md](docs/arch/auth.md).

## Architecture and decisions

- **[docs/MISSION.md](docs/MISSION.md)** — purpose, north-star, principles, bets / anti-bets.
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — overview + links to subsystem details (`docs/arch/*`).
- **Subsystem deep-dives** ([docs/arch/](docs/arch/)):
  - [overview](docs/arch/overview.md) — big picture and disk layout.
  - [interfaces](docs/arch/interfaces.md) — Web API, FTPS, WebDAV, tus, share-links.
  - [auth](docs/arch/auth.md) — sessions, CSRF, app-passwords, login security.
  - [rbac](docs/arch/rbac.md) — permission model, inheritance, share-links.
  - [storage](docs/arch/storage.md) — `storage.FileSystem`, dbfs, FlatFile.
  - [database](docs/arch/database.md) — PostgreSQL schema.
  - [indexing](docs/arch/indexing.md) — async metadata pipeline.
  - [trash](docs/arch/trash.md) — soft-delete and restore.
  - [backup-dr](docs/arch/backup-dr.md) — backup and disaster recovery.
- **[docs/adr/](docs/adr/)** — load-bearing decisions with alternatives and rationale:
  - [ADR-0001](docs/adr/0001-filesystem-boundary.md) — all delivery protocols go through `storage.FileSystem` (the cornerstone invariant).
  - [ADR-0002](docs/adr/0002-outbox-fs-db-atomicity.md) — transactional outbox for FS↔DB atomicity.
  - [ADR-0003](docs/adr/0003-rbac-computed-traverse.md) — computed Traverse in RBAC.
  - [ADR-0004](docs/adr/0004-app-passwords-cross-protocol.md) — app-passwords as cross-protocol auth.
  - [ADR-0005](docs/adr/0005-dr-pg-dump-plus-disk-walk.md) — DR via pg_dump + disk-walk.
  - [ADR-0006](docs/adr/0006-flatfile-only-mvp.md) — FlatFile-only MVP, CDC deferred.

## Roadmap & risks

- **[docs/ROADMAP.md](docs/ROADMAP.md)** — Now (MVP delivered) / Next / Later.
- **[docs/RISKS.md](docs/RISKS.md)** — risk register (durability/security/availability) with mitigations.

## Contributing

The cornerstone invariant: all tree mutations go through `storage.FileSystem` — see [ADR-0001](docs/adr/0001-filesystem-boundary.md).

Tests: `go test ./...` for unit, `TEST_POSTGRES_DSN=postgres://... go test ./... -p 1` for integration (PG required; `-p 1` is mandatory — the testpg helper drops the schema per test).

## Tech stack

- **Backend:** Go 1.25, `pgx/v5 + pgxpool`, `golang-migrate` (embedded SQL), `cobra` CLI.
- **Frontend:** React 19, Vite 6, TypeScript strict, Tailwind v4, shadcn/ui, React Router v7, TanStack Query.
- **Database:** PostgreSQL 16 (ltree, citext, pgcrypto, JSONB).
- **Protocols:** `fclairamb/ftpserverlib` (FTPS), `golang.org/x/net/webdav` (WebDAV), `tus-js-client` (UI side).
