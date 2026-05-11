# storman

Self-hosted персональное файловое хранилище уровня enterprise. Один Go-бинарь, PostgreSQL — больше внешних зависимостей нет. Веб-интерфейс, FTPS, WebDAV, resumable uploads (tus.io) и временные share-links — всё против одного дерева, с единым ACL и аудитом.

storman сделан для одного пользователя или семьи на single-host: облачный UX, но контроль и данные остаются у вас.

## Возможности

- **Five delivery interfaces** — Web API + React SPA, FTPS (explicit TLS), WebDAV (`/dav/*`), tus.io resumable uploads, анонимные share-links с TTL.
- **Honest storage** — пользовательские файлы лежат на диске «как есть» под `flat-storage/`. Никаких side-car. Time Machine / restic / rsync напрямую делают чистые бэкапы.
- **RBAC** — битмаска Read/Write/Remove/Admin + computed Traverse, наследование через ltree-дерево, ACL cache с epoch-based invalidation.
- **Durability** — outbox-паттерн для атомарности DB↔disk, crash recovery на старте, мягкое удаление через рекурсивный ltree-update с восстановлением.
- **Async indexing** — fault-tolerant очередь на `SELECT FOR UPDATE SKIP LOCKED`. В MVP — sha256-hasher; EXIF / MIME / face — в [roadmap](ROADMAP.md).
- **Disaster recovery** — две линии: периодический `pg_dump --format=custom` + `recover --from-disk` walker как последняя линия (контент пользователя восстанавливается даже без БД).
- **Security** — argon2id-пароли, sliding+absolute sessions, double-submit CSRF, HTTPS/FTPS обязательны, app-passwords для не-web протоколов, login rate-limit, audit log.

## Quick start

### Docker Compose (production-style)

Самый простой способ — Docker. Подробная инструкция, включая reverse-proxy + TLS, — в [DEPLOY.md](DEPLOY.md).

```bash
mkdir storman && cd storman
# Скачайте docker-compose.yml и .env.example из releases (или из этого репо)
mv .env.example .env && chmod 600 .env
sed -i.bak "s/__CHANGE_ME__/$(openssl rand -base64 24)/" .env && rm .env.bak
docker compose up -d
```

Откройте `http://<host>:8080/setup` для first-run wizard (логи показывают одноразовый setup token).

### Локальная разработка

Нужны: Go 1.25+, Node 20+ (для UI), Docker (для PostgreSQL).

```bash
# Один shot: PG в контейнере + provision data-dir + миграции + создание dev-user + backend + Vite HMR.
make dev
```

После запуска:
- **Backend API:** `http://localhost:8443`
- **UI (Vite HMR):** `http://localhost:5173`
- **Credentials:** `dev` / `devdevdevdev`

Подробнее — `make help`.

### Из исходников (без Docker, ручная установка)

```bash
# 1. Соберите бинарь (UI входит в бинарь через //go:embed).
make build

# 2. Запустите PostgreSQL отдельно (любым способом). Например:
docker run -d --name storman-pg \
  -e POSTGRES_USER=storman -e POSTGRES_PASSWORD=storman -e POSTGRES_DB=storman \
  -p 5432:5432 postgres:16

# 3. Инициализируйте data-dir, накатите миграции, создайте root-узел дерева.
./bin/storman init      --data-dir=/var/lib/storman
# Отредактируйте /var/lib/storman/config.json — пропишите DSN.
./bin/storman migrate   --data-dir=/var/lib/storman
./bin/storman bootstrap --data-dir=/var/lib/storman

# 4. Создайте первого пользователя и выдайте ему права на корень.
./bin/storman useradd   --data-dir=/var/lib/storman --login=admin --password=...
# (затем grant через UI или SQL в permissions)

# 5. Запустите сервер.
./bin/storman serve     --data-dir=/var/lib/storman
```

## Раскладка на диске

Один параметр в конфиге — `data_dir`. Внутри фиксировано:

```
<data-dir>/
├── config.json           # настройки сервиса (DSN, secrets_key, TLS, и т.д.)
├── flat-storage/         # пользовательские файлы как есть
└── meta-storage/
    ├── trash/            # soft-deleted поддеревья (восстанавливаются)
    ├── uploads/          # tus-стейджинг
    └── backups/          # pg_dump архивы
```

См. [docs/arch/overview.md](docs/arch/overview.md).

## CLI

```
storman init       --data-dir=<path>                     создать раскладку + config.json
storman migrate    --data-dir=<path>                     накатить миграции БД
storman bootstrap  --data-dir=<path>                     создать корневой узел дерева
storman useradd    --data-dir=<path> --login=X --password=Y
storman serve      --data-dir=<path>                     запустить сервер
storman backup     --data-dir=<path>                     ручной pg_dump (cron делает это автоматом)
storman recover    from-backup [<path>] | from-disk      DR-команды
storman version                                          версия из git
```

## Access methods (summary)

| Surface | URL / port | Auth |
|---|---|---|
| Web UI + API | `https://host/` | session cookie + double-submit CSRF |
| FTPS | `host:2121` (explicit TLS) | login + main password OR app-password |
| WebDAV | `https://host/dav/` | HTTP Basic + app-password |
| tus.io | `https://host/api/tus` | session (используется фронтом для крупных загрузок) |
| share-links | `https://host/share/<token>/` | anonymous (token in URL) |

Подробнее — [docs/arch/interfaces.md](docs/arch/interfaces.md), [docs/arch/auth.md](docs/arch/auth.md).

## Архитектура и решения

- **[MISSION.md](MISSION.md)** — цель, north-star, принципы, bets/anti-bets.
- **[ARCHITECTURE.md](ARCHITECTURE.md)** — обзор + ссылки на детали (`docs/arch/*`).
- **[docs/adr/](docs/adr/)** — load-bearing решения с альтернативами и обоснованием:
  - [ADR-0001](docs/adr/0001-filesystem-boundary.md) — все delivery-протоколы ходят через `storage.FileSystem` (краеугольный инвариант).
  - [ADR-0002](docs/adr/0002-outbox-fs-db-atomicity.md) — transactional outbox для FS↔DB атомарности.
  - [ADR-0003](docs/adr/0003-rbac-computed-traverse.md) — computed Traverse в RBAC.
  - [ADR-0004](docs/adr/0004-app-passwords-cross-protocol.md) — app-passwords как cross-protocol auth.
  - [ADR-0005](docs/adr/0005-dr-pg-dump-plus-disk-walk.md) — DR через pg_dump + disk-walk.
  - [ADR-0006](docs/adr/0006-flatfile-only-mvp.md) — FlatFile-only MVP, CDC отложен.

## Roadmap & risks

- **[ROADMAP.md](ROADMAP.md)** — Now (MVP доставлен) / Next / Later.
- **[docs/risks.md](docs/risks.md)** — риск-регистр (durability/security/availability) с mitigation.

## Contributing

Любой код, мутирующий дерево, должен ходить через `storage.FileSystem` — это инвариант ([ADR-0001](docs/adr/0001-filesystem-boundary.md)), не convention. Прямые `os.*` против `flat-storage/` или прямой SQL против `nodes`/`outbox` мимо `dbfs` — red flag в код-ревью.

Тесты: `go test ./...` для unit, `TEST_POSTGRES_DSN=postgres://... go test ./... -p 1` для integration (нужен PG; `-p 1` обязателен — testpg-helper дропает schema на каждый тест).

## Тех. стек

- **Backend:** Go 1.25, `pgx/v5 + pgxpool`, `golang-migrate` (embedded SQL), `cobra` CLI.
- **Frontend:** React 19, Vite 6, TypeScript strict, Tailwind v4, shadcn/ui, React Router v7, TanStack Query.
- **Database:** PostgreSQL 16 (ltree, citext, pgcrypto, JSONB).
- **Protocols:** `fclairamb/ftpserverlib` (FTPS), `golang.org/x/net/webdav` (WebDAV), `tus-js-client` (UI side).
