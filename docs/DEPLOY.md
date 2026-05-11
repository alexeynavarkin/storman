# Deploying Storman on-premise

This guide covers running storman on your own server with Docker Compose. The
stack is two containers: `postgres` (data backend) and `storman` (the
application binary with the SPA embedded). Storman speaks plain HTTP and
expects you to put a reverse proxy in front of it for TLS — any proxy will do
(nginx, traefik, an L7 load balancer, etc.).

## Prerequisites

- Linux host with Docker Engine ≥ 24 and the Compose plugin (`docker compose
  version` must succeed).
- A domain name pointing at the host (recommended) or an internal IP for
  LAN-only deployments.
- ~1 GB free disk for the database + however much you want to store.

## Quick start

Download the two deployment artifacts from the latest
[GitHub Release](https://github.com/alexeynavarkin/storman/releases), or copy
them out of the `deployments/` folder of this repo:

```bash
mkdir storman && cd storman
curl -LO https://github.com/alexeynavarkin/storman/releases/latest/download/docker-compose.yml
curl -LO https://github.com/alexeynavarkin/storman/releases/latest/download/.env.example

cp .env.example .env
chmod 600 .env

# Generate a strong database password (or edit .env by hand).
sed -i.bak "s/__CHANGE_ME__/$(openssl rand -base64 24)/" .env && rm .env.bak

# Pull the image and start the stack.
docker compose pull
docker compose up -d
```

`.env` is the only file you need to fill in. Everything else is wired up from
those values.

Storman is now listening on `127.0.0.1:8080` (HTTP, localhost only — change
`STORMAN_BIND` in `.env` if you need otherwise). The first startup logs a
one-time setup token — copy it before continuing:

```bash
docker compose logs storman | grep -A1 "FIRST-RUN SETUP REQUIRED"
```

## First-run setup (UI wizard)

Open `http://<host>:8080/setup` in a browser (use SSH tunnelling if the host
isn't local, or finish your reverse-proxy setup and use
`https://your-domain/setup`). The wizard asks for:

- **Setup token** — paste the value from the logs above.
- **Login** + **Password** (≥ 12 characters) for the first administrator.

On success you are redirected to `/login`. The token is invalidated and the
`/setup` page becomes inaccessible. If you ever lose the token without finishing
setup, `docker compose restart storman` issues a fresh one.

## Reverse proxy (TLS)

Storman never terminates TLS itself in this deployment mode. Point your proxy
at `127.0.0.1:8080` and forward `X-Forwarded-Proto: https` so storman can mark
cookies `Secure` (the `STORMAN_TRUST_PROXY=true` env in `docker-compose.yml`
makes the server honour the header).

Do **not** enable `STORMAN_TRUST_PROXY` if the storman port is reachable from
the public network — an attacker could spoof the header. The default
`STORMAN_BIND=127.0.0.1:8080` keeps it local-only.

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
recover from-backup --path=<path-to-dump>`.

## Common operations

All commands run inside an ephemeral container, sharing the same volumes as
the live service:

All subcommands pick up the config from `/var/lib/storman/config.json` by
default (the path mounted into the container). Override with `--config=<path>`
or `STORMAN_CONFIG_PATH=<path>` if you mounted it somewhere else.

```bash
# Run a one-off migrate (e.g. before upgrade, although `boot` does it for you).
docker compose run --rm storman migrate

# Create another user from the CLI (not as the first admin — use the wizard
# for that; this is for regular users).
docker compose run --rm storman useradd --login=bob

# Recover from a pg_dump file.
docker compose run --rm storman recover from-backup --path=<path>
```

## Configuration

### Environment variables

#### Consumed by the storman binary

| Variable | Used by | Notes |
|---|---|---|
| `STORMAN_CONFIG_PATH` | every command | Path to `config.json`. Overridden by `--config=<path>`. Default `/var/lib/storman/config.json`. |
| `STORMAN_DB_DSN` | `init` / `boot` | Seeds `database.dsn`. |
| `STORMAN_LISTEN_ADDR` | `init` / `boot` | Seeds `web.listen_addr`. |
| `STORMAN_TRUST_PROXY` | `init` / `boot` | Seeds `web.trust_proxy_headers` (`true`/`false`). |
| `STORMAN_SECURE_COOKIES` | `init` / `boot` | Seeds `web.secure_cookies` (`true`/`false`). |

The four seeding variables populate `config.json` **only on first start**.
After the file exists, the on-disk values are authoritative and these env
vars are ignored.

#### Consumed by `deployments/docker-compose.yml`

| Variable | Default | Notes |
|---|---|---|
| `POSTGRES_DB` | `storman` | Database name. |
| `POSTGRES_USER` | `storman` | Database user. |
| `POSTGRES_PASSWORD` | _(required)_ | Set in `.env` before the first `docker compose up`. Once Postgres is initialised, changing this has no effect — the password is baked into the volume. |
| `STORMAN_IMAGE` | `alexnav/storman:latest` | Image tag. Pin to a specific version in production. |
| `STORMAN_BIND` | `127.0.0.1:8080` | `host:port` mapping for the storman container's HTTP port. Defaults to localhost so only a local reverse proxy can reach it. |

### `config.json` reference

After the first start, the source of truth for runtime settings is
`./data/config.json` (owned by the container's `nonroot` user, mode 0600).
Edit the file and restart storman to apply.

JSON does not natively allow comments; the `//` lines below are documentation
only. Strip them if you copy this structure into a real config file. Every
field below is shown with its default value.

```jsonc
{
  // Absolute path to the data directory. Must equal the directory holding
  // this file (validated on load — guards against copying the config between
  // hosts). Written automatically by `storman init`; do not edit by hand.
  "data_dir": "/var/lib/storman",

  "database": {
    // PostgreSQL connection string. Seeded from STORMAN_DB_DSN on init.
    "dsn": "postgres://storman:storman@localhost:5432/storman?sslmode=disable"
  },

  // base64-encoded 32-byte key used to encrypt TOTP secrets at rest.
  // Generated randomly by `storman init`. DO NOT lose this value — without
  // it, existing TOTP secrets become unreadable.
  "secrets_key": "<base64(32 bytes)>",

  "backup": {
    // pg_dump frequency. Use "0s" to disable the scheduled backup loop
    // (manual `storman backup` still works).
    "interval": "24h0m0s",
    // Number of pg_dump archives to keep under meta-storage/backups/.
    // Older dumps are removed after each successful run. Must be >= 0.
    "retention": 7,
    // Optional argv prefix to invoke pg_dump. Default ["pg_dump"] expects
    // the binary on PATH. For dev setups where PostgreSQL runs in a sibling
    // container, set e.g. ["docker", "exec", "-i", "storman-pg", "pg_dump"].
    "pg_dump_cmd": null,
    // Same as pg_dump_cmd but for pg_restore. Default ["pg_restore"].
    "pg_restore_cmd": null
  },

  "web": {
    // Address the HTTP server binds to inside the container. The host-side
    // mapping is in deployments/.env (STORMAN_BIND).
    "listen_addr": ":8080",
    "tls": {
      // PEM cert/key for native TLS termination. Leave both empty to serve
      // plain HTTP behind a reverse proxy (the default deployment). Both
      // fields must be set together for TLS to activate.
      "cert_file": "",
      "key_file": ""
    },
    // Emit the Secure attribute on session and CSRF cookies when the
    // request is HTTPS. Seeded from STORMAN_SECURE_COOKIES on init.
    "secure_cookies": true,
    // Honour X-Forwarded-Proto when deciding if a request is HTTPS. Only
    // safe behind a reverse proxy that strips client-supplied X-Forwarded-*
    // headers — otherwise an attacker can spoof the protocol. Seeded from
    // STORMAN_TRUST_PROXY on init.
    "trust_proxy_headers": false
  },

  "trash": {
    // How many days a trashed entry survives before the GC worker purges
    // it permanently. 0 disables time-based GC (entries stay until an
    // explicit API purge).
    "retention_days": 30,
    // How often the GC worker scans the trash directory. 0 also disables
    // the worker.
    "gc_interval": "1h0m0s"
  },

  "ftp": {
    // Set true to start the explicit-FTPS listener. AUTH TLS is mandatory;
    // the server never accepts plain FTP. Reuses web.tls when ftp.tls is
    // empty.
    "enabled": false,
    // Listener for the FTP control channel.
    "listen_addr": ":2121",
    // IP/hostname returned in PASV replies. Required when the server is
    // reachable through NAT/firewall (point it to the externally reachable
    // address); leave empty for direct LAN access.
    "public_host": "",
    // Inclusive port range for PASV data channels. Open this range in the
    // firewall and (when running in Docker) publish it on the host.
    "passive_port_min": 50000,
    "passive_port_max": 50050,
    // Disconnect clients that idle longer than this (seconds).
    "idle_timeout_sec": 300,
    // FTPS-specific cert/key pair. When both fields are empty, the FTPS
    // listener reuses web.tls.
    "tls": {
      "cert_file": "",
      "key_file": ""
    }
  },

  "indexing": {
    // Number of workers in the async indexing pool (MVP: sha256 hashing).
    "workers": 2,
    // How often each worker polls the jobs table for new work.
    "poll_interval": "5s"
  },

  "tus": {
    // Sweep tus uploads parked under meta-storage/uploads/<id>/ when older
    // than this. 0 disables sweeping — orphans live forever (useful for
    // debugging stuck uploads).
    "retention_hours": 24,
    // Sweep cadence. 0 also disables the sweeper.
    "sweep_interval": "1h0m0s"
  },

  "webdav": {
    // Mount the WebDAV endpoint under <path_prefix>/. Auth is HTTP Basic
    // with an app-password — never enable without TLS in production
    // (Basic credentials travel in clear).
    "enabled": false,
    // URL prefix for the WebDAV interface. Must start with '/' and must
    // not end with '/'.
    "path_prefix": "/dav"
  }
}
```

## Troubleshooting

- **No setup token in logs.** Either users already exist (check
  `docker compose run --rm storman useradd --help`) or the server failed
  before reaching setup — check `docker compose logs storman` for migrate /
  Postgres errors.
- **`Secure` cookies missing behind the proxy.** Verify the proxy forwards
  `X-Forwarded-Proto: https`. Without it `web.trust_proxy_headers` has no
  request to trust.
- **Postgres healthcheck fails.** Make sure `POSTGRES_PASSWORD` was set
  before the very first `up` — once the database is initialised, Postgres
  remembers the password and changing the env has no effect.
- **`relation "users" does not exist`.** The migrate step did not run.
  `docker compose run --rm storman migrate` once, then
  `docker compose up -d storman`.

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
