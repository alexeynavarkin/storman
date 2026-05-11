# Auth: sessions, CSRF, app-passwords, login security

Auth-сервис обслуживает два класса протоколов:
- **Web** — cookie sessions + double-submit CSRF.
- **Non-web** (FTPS, WebDAV, в будущем FUSE/S3/rsync) — HTTP Basic / FTP USER+PASS, secret — основной пароль или app-password.

Все non-web протоколы делят одну точку входа `AuthenticateAppPassword(login, secret)`. Полное обоснование — [ADR-0004](../adr/0004-app-passwords-cross-protocol.md).

## Хранение паролей

`argon2id`. Дефолт: `memory=64 MiB, iterations=3, parallelism=2`. Параметры tunable через config; в тестах используется быстрый профиль (`MemoryKiB: 8 MiB, Iterations: 1`).

Таблица `users`:

```sql
users (
  id              uuid primary key,
  login           citext unique not null,           -- case-insensitive unique
  password_hash   text not null,                    -- $argon2id$v=19$m=…,t=…,p=…$<salt>$<hash>
  totp_secret     bytea,                            -- AEAD-зашифрован; NULL = 2FA off (ROADMAP/Next)
  failed_attempts int default 0,                    -- подряд неудачных логинов; сброс при успехе
  locked_until    timestamptz,                      -- backoff; NULL = не заблокирован
  created_at      timestamptz default now()
)
```

Параметры argon2id хранятся **внутри** хеша (encoded form). Это позволяет менять параметры без миграции — старые хеши верифицируются со своими параметрами, новые — с актуальными.

## Web sessions

Cookie `storman_session` — HttpOnly + SameSite=Lax. `Secure` флаг ставится **per-request** через helper `secureForRequest(r)` — true если запрос пришёл по HTTPS (own TLS) либо если `cfg.Web.TrustProxyHeaders=true` и `X-Forwarded-Proto: https`. Это позволяет одной cookie работать корректно как с прямым HTTPS-доступом, так и за reverse-proxy.

Таблица `sessions`:

```sql
sessions (
  id          text primary key,                     -- 32 байта crypto/rand → base64url; сам секрет — PK для O(1) lookup
  user_id     uuid not null references users(id) on delete cascade,
  created_at  timestamptz default now(),
  last_seen   timestamptz default now(),            -- обновляется на каждый authenticated request (batched, не на каждый чих)
  expires_at  timestamptz not null,                 -- effective expiry = min(last_seen + idle_timeout, created_at + absolute_timeout)
  ip          inet,
  user_agent  text
)
```

**Sliding expiration:** `expires_at = last_seen + idle_timeout`, не превышает `created_at + absolute_timeout`. Idle-timeout — конфигурируем (дефолт 30 минут активности; absolute — дефолт сутки).

**Logout** — `DELETE` записи. Logout всех устройств — `DELETE WHERE user_id = $1`.

## CSRF

Double-submit cookie:
- `storman_csrf` — non-HttpOnly cookie (JS должен иметь возможность её прочитать), `SameSite=Lax`.
- На каждый **мутирующий** запрос фронт читает cookie и шлёт значение в header `X-CSRF-Token`.
- Middleware `csrfMiddleware` (в `internal/web/server.go`) сверяет cookie ↔ header, отказывает 403 при mismatch.

**Исключения** (не используют CSRF, но всё ещё требуют session):
- **tus.io** — клиенты не понимают CSRF. Защита через session middleware + per-upload `info.json.user_id` check.

**Исключения** (вообще не используют session):
- **WebDAV** — Basic auth с app-password, см. ниже.
- **share-links** — анонимные, токен в URL.

## Security headers

На каждый ответ Web API:

- `Content-Security-Policy: default-src 'self'`
- `Strict-Transport-Security: max-age=31536000; includeSubDomains`
- `X-Frame-Options: DENY`
- `Referrer-Policy: same-origin`

## Rate limiting / brute-force

`loginLimiter` (см. `internal/web/ratelimit.go`) — token bucket по `(login)` и `(client_ip)`:
- **10 attempts per minute per login** (login bucket).
- **60 attempts per minute per IP** (IP bucket).

На любой неудаче (rate-limit или wrong-password) — `failed_attempts++` в `users`. После порога: `locked_until = now() + backoff(failed_attempts)`. Audit event на каждую неудачу — action=`login_failed`, result=`denied`, details включают `reason`.

**Общий limiter для всех протоколов.** WebDAV-handler (`davAuth`) использует тот же `loginLimiter` — попытки через WebDAV считаются как login attempts. Это значит, что brute-force через WebDAV не «обскачет» web-rate-limit.

## App-passwords

Для пользователей, которые цепляют FTPS/WebDAV-клиенты, не умеющие 2FA, либо просто не хотят светить основной пароль в каждое приложение.

Таблица `app_passwords`:

```sql
app_passwords (
  id         uuid primary key,
  user_id    uuid not null references users(id) on delete cascade,
  label      text not null,                          -- 'iPhone Files', 'rsync laptop' — только для UI
  hash       text not null,                          -- argon2id-хеш токена; сам токен показывается один раз
  created_at timestamptz default now(),
  last_used  timestamptz                             -- обновляется на login, не на каждый запрос; NULL = ни разу
)
```

**Flow:**
- Пользователь в UI выбирает «Create app-password», вводит label. Бэкенд генерирует токен (`crypto/rand`, base64url), вставляет хеш в `app_passwords`, возвращает **сам токен один раз**. UI показывает «скопируйте сейчас, потом уже не покажу».
- Клиент (FTPS, WebDAV) шлёт `login + token` через Basic/USER+PASS.
- `AuthenticateAppPassword(login, secret)`:
  1. Пробует `Authenticate(login, secret)` — основной пароль. Успех → возврат.
  2. При `ErrPasswordMismatch` → загружает все `app_passwords` для пользователя, проверяет каждый через `VerifyPassword(secret, hash)`. Match → `UPDATE last_used`, reset `failed_attempts`, возврат.
  3. Иначе — original error.

**Brute-force unified:** счётчики `users.failed_attempts` общие. Невозможно «обскакать» web-rate-limit через FTPS — те же 10/мин per login.

**Scope:** один app-password = все non-web протоколы пользователя. Per-protocol / per-action scope не реализован (см. [ADR-0004 § Future evolution](../adr/0004-app-passwords-cross-protocol.md)).

**Ревокация:** `DELETE FROM app_passwords WHERE id = $1`. UI показывает список с label + last_used.

## 2FA (ROADMAP/Next)

Колонка `users.totp_secret` зарезервирована. План — TOTP через `github.com/pquerna/otp/totp`. `totp_secret` шифруется AEAD-ключом из `config.secrets_key`. При включении 2FA — для FTPS/WebDAV становится обязательным использование app-password (TOTP-клиенты не поддерживаются).

## Audit log

Каждая мутирующая операция выпускает `audit.Event`:

```go
type Event struct {
    UserID  *uuid.UUID
    Action  string                  // 'login' | 'login_failed' | 'upload' | 'delete' | 'mkdir' | 'rename' | ...
    NodeID  *uuid.UUID
    IP      *netip.Addr
    Result  string                  // 'ok' | 'denied' | 'error'
    Details map[string]any          // произвольный контекст; в т.ч. "channel": "webdav" для не-web протоколов
}
```

`Server.audit(ctx, ev)` — nil-safe shortcut, никогда не блокирует и не возвращает ошибку (failed audit не должен ломать business operation).

Хранение — таблица `audit_log` (см. [database.md](database.md#audit_log--append-only-журнал-событий-безопасности)), партиционирована по месяцу.

Admin-only endpoint `GET /api/audit?filter=...` — фильтр по user/action/диапазону дат. См. `internal/web/audit.go`.

## Реализация

- [internal/auth/users.go](../../internal/auth/users.go) — `UserService`, password hashing, brute-force counters.
- [internal/auth/sessions.go](../../internal/auth/sessions.go) — `SessionService`, sliding expiration, Logout.
- [internal/auth/app_passwords.go](../../internal/auth/app_passwords.go) — app-password CRUD + `AuthenticateAppPassword`.
- [internal/auth/sharelinks.go](../../internal/auth/sharelinks.go) — share-link создание / validation / use.
- [internal/web/auth.go](../../internal/web/auth.go) — login/logout/me handlers, CSRF helpers, cookie management.
- [internal/web/ratelimit.go](../../internal/web/ratelimit.go) — token bucket login limiter.
- [internal/audit/audit.go](../../internal/audit/audit.go) — audit Service.
