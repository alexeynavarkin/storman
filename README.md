# storman

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

The simplest path is Docker. Detailed instructions, including reverse-proxy + TLS, are in [docs/DEPLOY.md](docs/DEPLOY.md).

```bash
mkdir storman && cd storman
# Download docker-compose.yml and .env.example from releases (or from this repo)
mv .env.example .env && chmod 600 .env
sed -i.bak "s/__CHANGE_ME__/$(openssl rand -base64 24)/" .env && rm .env.bak
docker compose up -d
```

Open `http://<host>:8080/setup` for the first-run wizard (the logs show a one-time setup token).

### Local development

Requirements: Go 1.25+, Node 20+ (for the UI), Docker (for PostgreSQL).

```bash
# One shot: PG in a container + provision data-dir + migrations + create dev-user + backend + Vite HMR.
make dev
```

After startup:
- **Backend API:** `http://localhost:8443`
- **UI (Vite HMR):** `http://localhost:5173`
- **Credentials:** `dev` / `devdevdevdev`

See `make help` for more.

### From source (without Docker, manual setup)

```bash
# 1. Build the binary (the UI is embedded into the binary via //go:embed).
make build

# 2. Run PostgreSQL separately (any way you like). For example:
docker run -d --name storman-pg \
  -e POSTGRES_USER=storman -e POSTGRES_PASSWORD=storman -e POSTGRES_DB=storman \
  -p 5432:5432 postgres:16

# 3. Initialize the data-dir, apply migrations, create the root tree node.
./bin/storman init      --data-dir=/var/lib/storman
# Edit /var/lib/storman/config.json — set the DSN.
./bin/storman migrate   --data-dir=/var/lib/storman
./bin/storman bootstrap --data-dir=/var/lib/storman

# 4. Create the first user and grant them root permissions.
./bin/storman useradd   --data-dir=/var/lib/storman --login=admin --password=...
# (then grant via UI or SQL on `permissions`)

# 5. Start the server.
./bin/storman serve     --data-dir=/var/lib/storman
```

## On-disk layout

A single config parameter — `data_dir`. The layout inside is fixed:

```
<data-dir>/
├── config.json           # service settings (DSN, secrets_key, TLS, etc.)
├── flat-storage/         # user files as-is
└── meta-storage/
    ├── trash/            # soft-deleted subtrees (restorable)
    ├── uploads/          # tus staging
    └── backups/          # pg_dump archives
```

See [docs/arch/overview.md](docs/arch/overview.md).

## CLI

```
storman init       --data-dir=<path>                     create the layout + config.json
storman migrate    --data-dir=<path>                     apply DB migrations
storman bootstrap  --data-dir=<path>                     create the root tree node
storman useradd    --data-dir=<path> --login=X --password=Y
storman serve      --data-dir=<path>                     run the server
storman backup     --data-dir=<path>                     manual pg_dump (cron does it automatically)
storman recover    from-backup [<path>] | from-disk      DR commands
storman version                                          version from git
```

## Access methods (summary)

| Surface | URL / port | Auth |
|---|---|---|
| Web UI + API | `https://host/` | session cookie + double-submit CSRF |
| FTPS | `host:2121` (explicit TLS) | login + main password OR app-password |
| WebDAV | `https://host/dav/` | HTTP Basic + app-password |
| tus.io | `https://host/api/tus` | session (used by the frontend for large uploads) |
| share-links | `https://host/share/<token>/` | anonymous (token in URL) |

See [docs/arch/interfaces.md](docs/arch/interfaces.md), [docs/arch/auth.md](docs/arch/auth.md).

## Architecture and decisions

- **[docs/MISSION.md](docs/MISSION.md)** — purpose, north-star, principles, bets / anti-bets.
- **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** — overview + links to subsystem details (`docs/arch/*`).
- **[docs/adr/](docs/adr/)** — load-bearing decisions with alternatives and rationale:
  - [ADR-0001](docs/adr/0001-filesystem-boundary.md) — all delivery protocols go through `storage.FileSystem` (the cornerstone invariant).
  - [ADR-0002](docs/adr/0002-outbox-fs-db-atomicity.md) — transactional outbox for FS↔DB atomicity.
  - [ADR-0003](docs/adr/0003-rbac-computed-traverse.md) — computed Traverse in RBAC.
  - [ADR-0004](docs/adr/0004-app-passwords-cross-protocol.md) — app-passwords as cross-protocol auth.
  - [ADR-0005](docs/adr/0005-dr-pg-dump-plus-disk-walk.md) — DR via pg_dump + disk-walk.
  - [ADR-0006](docs/adr/0006-flatfile-only-mvp.md) — FlatFile-only MVP, CDC deferred.

## Roadmap & risks

- **[docs/ROADMAP.md](docs/ROADMAP.md)** — Now (MVP delivered) / Next / Later.
- **[docs/risks.md](docs/risks.md)** — risk register (durability/security/availability) with mitigations.

## Contributing

Any code that mutates the tree must go through `storage.FileSystem` — this is an invariant ([ADR-0001](docs/adr/0001-filesystem-boundary.md)), not a convention. Direct `os.*` calls against `flat-storage/` or direct SQL against `nodes`/`outbox` that bypasses `dbfs` is a red flag in code review.

Tests: `go test ./...` for unit, `TEST_POSTGRES_DSN=postgres://... go test ./... -p 1` for integration (PG required; `-p 1` is mandatory — the testpg helper drops the schema per test).

## Tech stack

- **Backend:** Go 1.25, `pgx/v5 + pgxpool`, `golang-migrate` (embedded SQL), `cobra` CLI.
- **Frontend:** React 19, Vite 6, TypeScript strict, Tailwind v4, shadcn/ui, React Router v7, TanStack Query.
- **Database:** PostgreSQL 16 (ltree, citext, pgcrypto, JSONB).
- **Protocols:** `fclairamb/ftpserverlib` (FTPS), `golang.org/x/net/webdav` (WebDAV), `tus-js-client` (UI side).
