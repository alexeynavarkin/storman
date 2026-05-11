# Delivery interfaces

storman has five delivery interfaces, all going through a single `storage.FileSystem`:

| Protocol | Transport | Auth | CSRF | Where |
|---|---|---|---|---|
| Web API | HTTPS + JSON | session cookie | double-submit | `internal/web/` |
| FTPS | explicit-TLS FTP | `AuthenticateAppPassword` | n/a | `internal/ftpsrv/` |
| WebDAV | HTTPS / `/dav/*` | HTTP Basic + `AuthenticateAppPassword` | n/a | `internal/web/webdav*.go` |
| tus.io | HTTPS / `/api/tus/*` | session cookie | bypassed (per-file owner check) | `internal/web/tus*.go` |
| share-links | HTTPS / `/share/{token}/*` | anonymous token | n/a | `internal/web/share.go` |

All of them are thin adapters. They perform no business logic over the FS — only parse the protocol request, check ACLs via `rbac.PermissionService.Check`, make a `storage.FileSystem` call, and emit an `audit.Event`.

## Web API

The base application HTTP interface. Cookie sessions + double-submit CSRF.

**Routing** — `http.ServeMux` (Go 1.22+, method-prefix patterns). Middleware composition via `chain(handler, routeKind)`:

- `openRoute` — no session (only `POST /api/auth/login`).
- `authedRead` — session middleware (`/api/auth/me`, `GET /api/fs/*`, listing, audit).
- `authedMutate` — session + CSRF middleware (`POST /api/fs/mkdir`, `DELETE /api/fs/remove`, etc.).

**Key endpoints:**

- `POST /api/auth/login` / `POST /api/auth/logout` / `GET /api/auth/me`
- `GET /api/fs/stat` / `GET /api/fs/list`
- `POST /api/fs/mkdir` / `DELETE /api/fs/remove` / `POST /api/fs/rename`
- `GET /api/fs/read` / `PUT /api/fs/write`
- `GET|POST|DELETE /api/users[/{id}]`, `POST /api/users/{id}/password`
- `GET /api/perm/list` / `POST /api/perm/grant` / `POST /api/perm/revoke`
- `GET /api/trash` / `POST /api/trash/{id}/restore` / `DELETE /api/trash/{id}` (admin)
- `GET /api/audit` (admin)
- `POST /api/share` / `GET /api/share/mine` / `DELETE /api/share/{token}`

Auth details — [auth.md](auth.md). RBAC details — [rbac.md](rbac.md).

## FTPS

Implemented via `github.com/fclairamb/ftpserverlib` + an afero adapter over `storage.FileSystem`.

- **Explicit-TLS only** (`AUTH TLS`). Plain FTP is disabled at the driver settings level (`TLSRequired: MandatoryEncryption`).
- **Auth** — `AuthenticateAppPassword(login, secret)` via `users.AuthenticateAppPassword`. Supports the main password and app-passwords.
- **Share-links** — **not supported**. FTP — only named users.
- **PASV ports** — a configurable range (`ftp.passive_port_min/max`). The operator must open the range on the firewall.
- **`PublicHost`** — a required field for setup behind NAT/firewall.

**Mapping of FTP operations to FileSystem:**

| FTP | FileSystem |
|---|---|
| `STAT` / `LIST` | `Stat` / `List` |
| `RETR` | `OpenRead` → stream over `FileReader.ReadAt` |
| `STOR` | `OpenWrite` → stream over `FileWriter.WriteAt` → `Commit` |
| `MKD` | `Mkdir` |
| `DELE` / `RMD` | `Remove` |
| `RNFR` + `RNTO` | `Rename` |

`REST <offset>` (resume download) is supported via `ReaderAt` — the client specifies an offset, the wrapper positions accordingly.

**ACL:** every operation calls `perms.Check(ctx, user.ID, nodeID, action)`. `rbac.ErrDenied` → `os.ErrPermission` → FTP 550.

Config:

```json
"ftp": {
  "enabled": false,
  "listen_addr": ":2121",
  "public_host": "",
  "passive_port_min": 50000,
  "passive_port_max": 50050,
  "idle_timeout_sec": 300,
  "tls": { "cert_file": "", "key_file": "" }
}
```

`tls` is optional — if empty, `web.tls` is used (shared cert for HTTPS and FTPS).

Implementation — [internal/ftpsrv/](../../internal/ftpsrv/).

## WebDAV

Implemented via `golang.org/x/net/webdav` + an adapter over `storage.FileSystem`. Full history of the decision — the last WebDAV feature, we just shipped it.

- **Mounted under the main HTTPS** on `/dav/*` (path-prefix mux pattern). One port, one TLS certificate, shared audit infrastructure.
- **Auth** — HTTP Basic with `AuthenticateAppPassword` (the same app-password as for FTPS). Sessions/CSRF are not used.
- **Locks** — in-memory (`webdav.NewMemLS()`). Single-node only. A persistent LockSystem is in [ROADMAP/Next](../ROADMAP.md).
- **Overwrite** — implemented as `trash-then-create` (the storage MVP does not have real overwrite — the old version goes to the trash, a new one is created).

**Mapping of WebDAV methods to FileSystem:**

| WebDAV | FileSystem |
|---|---|
| `PROPFIND` | `Stat` + `List` |
| `GET` | `OpenRead` |
| `PUT` | `OpenWrite` + `Commit` |
| `MKCOL` | `Mkdir` |
| `DELETE` | `Remove` |
| `MOVE` | `Rename` |
| `COPY` | the library decomposes into per-file `OpenFile` + `io.Copy` ([slow, see ROADMAP/Next](../ROADMAP.md)) |
| `LOCK` / `UNLOCK` | `webdav.NewMemLS` |
| `OPTIONS` | automatic, responds with `DAV: 1, 2` |

**ACL:** every adapter method (`Stat`, `Mkdir`, `RemoveAll`, `Rename`, `OpenFile`) calls `perms.Check`. `rbac.ErrDenied` → `os.ErrPermission` → WebDAV `403 Forbidden`.

**Audit:** mutating operations emit events with `Details["channel"] = "webdav"` — the admin can filter by surface via `GET /api/audit?action=upload&channel=webdav`.

Config:

```json
"webdav": {
  "enabled": false,
  "path_prefix": "/dav"
}
```

Implementation — [internal/web/webdav.go](../../internal/web/webdav.go), [internal/web/webdav_fs.go](../../internal/web/webdav_fs.go), [internal/web/webdav_file.go](../../internal/web/webdav_file.go).

## tus.io resumable uploads

[tus.io 1.0.0](https://tus.io) — a standardized resumable upload protocol. Used by the frontend for large files with pause/resume.

- **Mount** under `/api/tus/*` in the common HTTP mux.
- **Auth** — session cookie (Web). tus clients do not understand CSRF → `authedRead` (no CSRF), but the session middleware still applies.
- **Per-upload ownership check** — `info.json.user_id` is compared against `UserFromContext(r.Context()).ID`. Cross-user access is impossible.

**Flow:**

1. `OPTIONS /api/tus` — discovery (supported extensions: Creation, Termination).
2. `POST /api/tus` with `Upload-Length`, `Upload-Metadata` (including `filename`, `filetype`, `dir`) → creates `<meta-storage>/uploads/<id>/` with `info.json`. Returns `Location: /api/tus/<id>`.
3. `PATCH /api/tus/<id>` with a chunk body → appends to the staging file, updates the `offset` sidecar (atomic tmp + rename).
4. `HEAD /api/tus/<id>` → current offset (for resume).
5. When `Upload-Length == Upload-Offset` → finalization via `dbfs.ImportPath`, which performs `os.Rename` of staging → `flat-storage/<dir>/<filename>` + an outbox row (like a regular `OpenWrite.Commit`).
6. `DELETE /api/tus/<id>` → cancel the upload, remove staging.

**Sweeper.** A background worker removes stale uploads (untouched for N hours, default 24h). See [internal/web/tus_sweeper.go](../../internal/web/tus_sweeper.go).

Config:
```json
"tus": {
  "retention_hours": 24,
  "sweep_interval": "1h"
}
```

Implementation — [internal/web/tus.go](../../internal/web/tus.go).

## Share-links

Anonymous temporary links. HTTP/HTTPS only. See [rbac.md § Share-links](rbac.md#share-links-web-only).

**Endpoints:**

- `POST /api/share` (auth required, `Admin` on the node) — creating a link.
- `GET /api/share/mine` — my links.
- `GET /api/share/all` (admin-only) — all links in the system.
- `DELETE /api/share/{token}` — revoke (creator or admin).
- `GET /share/{token}/info` (anonymous) — node metadata without content.
- `GET /share/{token}/download` (anonymous) — download (if `Read` in scope).
- `PUT /share/{token}/upload` (anonymous) — upload (if `Write` in scope).

**Validation on every use:**
1. The token exists.
2. `expires_at > now()`.
3. `max_uses IS NULL OR used_count < max_uses`.
4. The requested action ⊆ the link's `actions`.

`statByID` uses a recursive CTE to resolve the node's logical path through the `parent_id` chain (since an anonymous user has no ACL to walk via ltree).

An audit event on every use (`action=share_use`).

Implementation — [internal/web/share.go](../../internal/web/share.go), [internal/auth/sharelinks.go](../../internal/auth/sharelinks.go).

## Auth model comparison

| Surface | Identity | Secret | TLS required | Anonymous OK |
|---|---|---|---|---|
| Web API | user (session) | argon2id password | yes (HSTS) | only login endpoint |
| FTPS | user | app-password or main password | **mandatory** (`AUTH TLS`) | no |
| WebDAV | user | app-password or main password | yes if served by main HTTPS | no |
| tus.io | user (session) | inherited from Web | yes | no |
| share-link | none | URL token | yes (HSTS) | **yes** (by design) |

More — [auth.md](auth.md), [ADR-0004 App-passwords](../adr/0004-app-passwords-cross-protocol.md).
