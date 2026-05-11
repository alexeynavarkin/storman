# Auth: sessions, CSRF, app-passwords, login security

The auth service serves two classes of protocols:
- **Web** — cookie sessions + double-submit CSRF.
- **Non-web** (FTPS, WebDAV, future FUSE/S3/rsync) — HTTP Basic / FTP USER+PASS, with the secret being either the main password or an app-password.

All non-web protocols share one entry point `AuthenticateAppPassword(login, secret)`. Full rationale — [ADR-0004](../adr/0004-app-passwords-cross-protocol.md).

## Password storage

`argon2id`. Default: `memory=64 MiB, iterations=3, parallelism=2`. Parameters are tunable via config; tests use a fast profile (`MemoryKiB: 8 MiB, Iterations: 1`).

Table `users`:

```sql
users (
  id              uuid primary key,
  login           citext unique not null,           -- case-insensitive unique
  password_hash   text not null,                    -- $argon2id$v=19$m=…,t=…,p=…$<salt>$<hash>
  totp_secret     bytea,                            -- AEAD-encrypted; NULL = 2FA off (ROADMAP/Next)
  failed_attempts int default 0,                    -- consecutive failed logins; reset on success
  locked_until    timestamptz,                      -- backoff; NULL = not locked
  created_at      timestamptz default now()
)
```

The argon2id parameters live **inside** the hash (encoded form). This lets us change parameters without migration — old hashes verify with their own parameters, new hashes use the current ones.

## Web sessions

Cookie `storman_session` — HttpOnly + SameSite=Lax. The `Secure` flag is set **per-request** via the helper `secureForRequest(r)` — true if the request came over HTTPS (own TLS) or if `cfg.Web.TrustProxyHeaders=true` and `X-Forwarded-Proto: https`. This lets a single cookie work correctly both with direct HTTPS access and behind a reverse proxy.

Table `sessions`:

```sql
sessions (
  id          text primary key,                     -- 32 bytes crypto/rand → base64url; the secret is the PK for O(1) lookup
  user_id     uuid not null references users(id) on delete cascade,
  created_at  timestamptz default now(),
  last_seen   timestamptz default now(),            -- updated on every authenticated request (batched, not on every tick)
  expires_at  timestamptz not null,                 -- effective expiry = min(last_seen + idle_timeout, created_at + absolute_timeout)
  ip          inet,
  user_agent  text
)
```

**Sliding expiration:** `expires_at = last_seen + idle_timeout`, capped by `created_at + absolute_timeout`. Idle-timeout is configurable (default 30 minutes of activity; absolute — default 24 hours).

**Logout** — `DELETE` the row. Logout-all-devices — `DELETE WHERE user_id = $1`.

## CSRF

Double-submit cookie:
- `storman_csrf` — a non-HttpOnly cookie (JS must be able to read it), `SameSite=Lax`.
- On every **mutating** request the frontend reads the cookie and sends the value in the `X-CSRF-Token` header.
- Middleware `csrfMiddleware` (in `internal/web/server.go`) checks cookie ↔ header, rejecting with 403 on mismatch.

**Exemptions** (do not use CSRF, but still require a session):
- **tus.io** — clients do not understand CSRF. Protection comes from the session middleware + a per-upload `info.json.user_id` check.

**Exemptions** (do not use sessions at all):
- **WebDAV** — Basic auth with an app-password, see below.
- **share-links** — anonymous, token in the URL.

## Security headers

On every Web API response:

- `Content-Security-Policy: default-src 'self'`
- `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- `X-Frame-Options: DENY`
- `Referrer-Policy: same-origin`

## Rate limiting / brute-force

`loginLimiter` (see `internal/web/ratelimit.go`) — a token bucket by `(login)` and `(client_ip)`:
- **10 attempts per minute per login** (login bucket).
- **60 attempts per minute per IP** (IP bucket).

On any failure (rate-limit or wrong password) — `failed_attempts++` in `users`. After the threshold: `locked_until = now() + backoff(failed_attempts)`. An audit event on every failure — action=`login_failed`, result=`denied`, details include `reason`.

**Shared limiter across protocols.** The WebDAV handler (`davAuth`) uses the same `loginLimiter` — WebDAV attempts count as login attempts. This means a brute-force attack via WebDAV cannot "outrun" the web rate-limit.

## App-passwords

For users connecting FTPS/WebDAV clients that do not support 2FA, or who simply do not want to expose the main password to every application.

Table `app_passwords`:

```sql
app_passwords (
  id         uuid primary key,
  user_id    uuid not null references users(id) on delete cascade,
  label      text not null,                          -- 'iPhone Files', 'rsync laptop' — UI only
  hash       text not null,                          -- argon2id hash of the token; the token itself is shown once
  created_at timestamptz default now(),
  last_used  timestamptz                             -- updated on login, not on every request; NULL = never used
)
```

**Flow:**
- In the UI the user picks "Create app-password", enters a label. The backend generates a token (`crypto/rand`, base64url), inserts a hash into `app_passwords`, returns **the token itself once**. The UI shows "copy now, you won't see it again".
- The client (FTPS, WebDAV) sends `login + token` via Basic / USER+PASS.
- `AuthenticateAppPassword(login, secret)`:
  1. Tries `Authenticate(login, secret)` — the main password. Success → return.
  2. On `ErrPasswordMismatch` → loads all `app_passwords` for the user, verifies each via `VerifyPassword(secret, hash)`. Match → `UPDATE last_used`, reset `failed_attempts`, return.
  3. Otherwise — original error.

**Brute-force unified:** the `users.failed_attempts` counters are shared. You cannot "outrun" the web rate-limit through FTPS — the same 10/min per login applies.

**Scope:** one app-password = all non-web protocols of the user. Per-protocol / per-action scope is not implemented (see [ADR-0004 § Future evolution](../adr/0004-app-passwords-cross-protocol.md)).

**Revocation:** `DELETE FROM app_passwords WHERE id = $1`. The UI shows a list with label + last_used.

## 2FA (ROADMAP/Next)

The column `users.totp_secret` is reserved. The plan — TOTP via `github.com/pquerna/otp/totp`. `totp_secret` is encrypted with the AEAD key from `config.secrets_key`. When 2FA is enabled — for FTPS/WebDAV an app-password becomes mandatory (TOTP clients are not supported).

## Audit log

Every mutating operation emits an `audit.Event`:

```go
type Event struct {
    UserID  *uuid.UUID
    Action  string                  // 'login' | 'login_failed' | 'upload' | 'delete' | 'mkdir' | 'rename' | ...
    NodeID  *uuid.UUID
    IP      *netip.Addr
    Result  string                  // 'ok' | 'denied' | 'error'
    Details map[string]any          // arbitrary context; includes "channel": "webdav" for non-web protocols
}
```

`Server.audit(ctx, ev)` is a nil-safe shortcut, never blocks and never returns an error (a failed audit must not break a business operation).

Storage — the `audit_log` table (see [database.md](database.md#audit_log--append-only-journal-of-security-events)), partitioned by month.

Admin-only endpoint `GET /api/audit?filter=...` — filter by user/action/date range. See `internal/web/audit.go`.

## Implementation

- [internal/auth/users.go](../../internal/auth/users.go) — `UserService`, password hashing, brute-force counters.
- [internal/auth/sessions.go](../../internal/auth/sessions.go) — `SessionService`, sliding expiration, Logout.
- [internal/auth/app_passwords.go](../../internal/auth/app_passwords.go) — app-password CRUD + `AuthenticateAppPassword`.
- [internal/auth/sharelinks.go](../../internal/auth/sharelinks.go) — share-link creation / validation / use.
- [internal/web/auth.go](../../internal/web/auth.go) — login/logout/me handlers, CSRF helpers, cookie management.
- [internal/web/ratelimit.go](../../internal/web/ratelimit.go) — token-bucket login limiter.
- [internal/audit/audit.go](../../internal/audit/audit.go) — audit Service.
