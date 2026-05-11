# Deploying Storman on-premise

This guide covers running storman on your own server with Docker Compose. The
stack is two containers: `postgres` (data backend) and `storman` (the
application binary with the SPA embedded). Storman speaks plain HTTP and
expects a reverse proxy (caddy, nginx, traefik, …) to terminate TLS.

## Prerequisites

- Linux host with Docker Engine ≥ 24 and the Compose plugin (`docker compose
  version` must succeed).
- A domain name pointing at the host (recommended) or an internal IP for
  LAN-only deployments.
- A reverse proxy that can terminate TLS. The repository ships an
  [examples/Caddyfile](../examples/Caddyfile) for Caddy.
- ~1 GB free disk for the database + however much you want to store.

## Quick start

Download the three artifacts from the latest [GitHub Release](../../../releases),
or copy them out of this repo:

```bash
mkdir storman && cd storman
curl -LO https://github.com/<owner>/storman/releases/latest/download/docker-compose.yml
curl -LO https://github.com/<owner>/storman/releases/latest/download/.env.example
mv .env.example .env
chmod 600 .env

# Generate a strong database password.
sed -i.bak "s/__CHANGE_ME__/$(openssl rand -base64 24)/" .env && rm .env.bak

# Pull the image and start the stack.
docker compose pull
docker compose up -d
```

Storman is now listening on `127.0.0.1:8080` (HTTP, localhost only). The first
startup logs a one-time setup token — copy it before continuing:

```bash
docker compose logs storman | grep -A1 "FIRST-RUN SETUP REQUIRED"
```

## First-run setup (UI wizard)

Open `http://<host>:8080/setup` in a browser (use SSH tunnelling if the host
isn't local, or finish the reverse-proxy section below and use
`https://your-domain/setup`). The wizard asks for:

- **Setup token** — paste the value from the logs above.
- **Login** + **Password** (≥ 12 characters) for the first administrator.

On success you are redirected to `/login`. The token is invalidated and the
`/setup` page becomes inaccessible. If you ever lose the token without finishing
setup, `docker compose restart storman` issues a fresh one.

## Reverse proxy (TLS)

Storman never terminates TLS itself in this deployment mode. Point your proxy
at `127.0.0.1:8080`. The minimum Caddyfile:

```caddyfile
storman.example.com {
        encode zstd gzip
        reverse_proxy 127.0.0.1:8080
        request_body { max_size 0 }
}
```

A complete example with documentation is in
[examples/Caddyfile](../examples/Caddyfile). Caddy will obtain and renew the TLS
certificate automatically. Verify after a few seconds that
`https://storman.example.com` serves the login page — cookies should be marked
`Secure` (the `STORMAN_TRUST_PROXY=true` env in docker-compose.yml makes the
server honour `X-Forwarded-Proto`).

For nginx/traefik the proxy contract is the same: forward to `127.0.0.1:8080`
and ensure `X-Forwarded-Proto` is set to `https`. Do **not** enable
`STORMAN_TRUST_PROXY` if the storman port is reachable from the public network
— an attacker could spoof the header.

## Upgrading

```bash
# Update STORMAN_IMAGE in .env to the new tag, e.g. alexnav/storman:v0.2.0
docker compose pull storman
docker compose up -d storman
```

The container entrypoint runs `storman boot`, which re-runs `init` and
`migrate` idempotently before starting the server. Schema migrations apply
automatically; if a migration fails the container fails fast and Compose
restarts, so the previous version is not lost — fix the issue and restart.

## Backups

Storman writes hourly `pg_dump`s under `./data/meta-storage/backups/` (interval
and retention are configured in `data/config.json`). For full disaster
recovery you also need a copy of:

- `./data/` — config.json, flat-storage (the file blobs), meta-storage.
- the `pg_data` named volume — live PostgreSQL data.

A typical `restic`/`rsync`/`borg` job on a cron should snapshot both. The
restore command for the database side is `docker compose run --rm storman
recover --data-dir=/var/lib/storman --from-dump=<path-to-dump>`.

## Common operations

All commands run inside an ephemeral container, sharing the same volumes as
the live service:

```bash
# Run a one-off migrate (e.g. before upgrade, although `boot` does it for you).
docker compose run --rm storman migrate --data-dir=/var/lib/storman

# Create another user from the CLI (not as the first admin — use the wizard
# for that; this is for regular users).
docker compose run --rm storman useradd --data-dir=/var/lib/storman --login=bob

# Recover from a pg_dump file.
docker compose run --rm storman recover --data-dir=/var/lib/storman --from-dump=<path>
```

## Configuration

After the first start, the source of truth for runtime settings is
`./data/config.json` (owned by the container's `nonroot` user, mode 0600).
Edit the file and restart storman to apply.

Notable fields:

| Path | Default | Notes |
|---|---|---|
| `database.dsn` | from `STORMAN_DB_DSN` on init | Postgres connection string. |
| `secrets_key` | random base64(32) | DO NOT lose — encrypts TOTP secrets. |
| `web.listen_addr` | `:8080` | Bound inside the container; the host port is in `.env`. |
| `web.trust_proxy_headers` | `true` (from `STORMAN_TRUST_PROXY`) | Honour `X-Forwarded-Proto` for cookies. |
| `web.secure_cookies` | `true` | Emit `Secure` flag when the request is HTTPS. |
| `backup.interval` | `24h` | `pg_dump` frequency. |
| `backup.retention` | `7` | Number of dumps to keep. |
| `trash.retention_days` | `30` | Soft-delete TTL. |

Env vars (`STORMAN_DB_DSN`, `STORMAN_LISTEN_ADDR`, `STORMAN_TRUST_PROXY`,
`STORMAN_SECURE_COOKIES`) seed `config.json` **only on first start**.
Subsequent restarts ignore them.

## Troubleshooting

- **No setup token in logs.** Either users already exist (check
  `docker compose run --rm storman useradd --help`) or the server failed
  before reaching setup — check `docker compose logs storman` for migrate /
  Postgres errors.
- **`Secure` cookies missing behind the proxy.** Verify Caddy/nginx forwards
  `X-Forwarded-Proto: https`. Without it `web.trust_proxy_headers` has no
  request to trust.
- **Postgres healthcheck fails.** Make sure `POSTGRES_PASSWORD` was set
  before the very first `up` — once the database is initialised, Postgres
  remembers the password and changing the env has no effect.
- **`relation "users" does not exist`.** The migrate step did not run.
  `docker compose run --rm storman migrate --data-dir=/var/lib/storman` once,
  then `docker compose up -d storman`.

## Security checklist

- `chmod 600 .env` — it contains the database password.
- `chmod 600 data/config.json` — already done by `init`, but verify after edits.
- Keep `STORMAN_BIND=127.0.0.1:8080` unless you intentionally expose the HTTP
  port; rely on the reverse proxy for public access.
- Only enable `web.trust_proxy_headers` when the service is reachable
  exclusively through a trusted proxy that overrides client-supplied
  `X-Forwarded-*` headers.
- Snapshot `./data/` and the `pg_data` volume regularly. The internal
  `pg_dump` retention is a convenience, not a backup strategy.
