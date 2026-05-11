# Delivery interfaces

storman имеет пять delivery-интерфейсов, все через единый `storage.FileSystem`:

| Протокол | Транспорт | Auth | CSRF | Где |
|---|---|---|---|---|
| Web API | HTTPS + JSON | session cookie | double-submit | `internal/web/` |
| FTPS | explicit-TLS FTP | `AuthenticateAppPassword` | n/a | `internal/ftpsrv/` |
| WebDAV | HTTPS / `/dav/*` | HTTP Basic + `AuthenticateAppPassword` | n/a | `internal/web/webdav*.go` |
| tus.io | HTTPS / `/api/tus/*` | session cookie | bypassed (per-file owner check) | `internal/web/tus*.go` |
| share-links | HTTPS / `/share/{token}/*` | anonymous token | n/a | `internal/web/share.go` |

Все они — тонкие адаптеры. Никакой бизнес-логики на FS не выполняют — только парсят протокольный запрос, проверяют ACL через `rbac.PermissionService.Check`, делают вызов через `storage.FileSystem`, эмитят `audit.Event`.

## Web API

Базовый прикладной HTTP-интерфейс. Cookie sessions + double-submit CSRF.

**Маршрутизация** — `http.ServeMux` (Go 1.22+, method-prefix patterns). Middleware composition через `chain(handler, routeKind)`:

- `openRoute` — без session (только `POST /api/auth/login`).
- `authedRead` — session middleware (`/api/auth/me`, `GET /api/fs/*`, listing, audit).
- `authedMutate` — session + CSRF middleware (`POST /api/fs/mkdir`, `DELETE /api/fs/remove`, и т.п.).

**Ключевые endpoints:**

- `POST /api/auth/login` / `POST /api/auth/logout` / `GET /api/auth/me`
- `GET /api/fs/stat` / `GET /api/fs/list`
- `POST /api/fs/mkdir` / `DELETE /api/fs/remove` / `POST /api/fs/rename`
- `GET /api/fs/read` / `PUT /api/fs/write`
- `GET|POST|DELETE /api/users[/{id}]`, `POST /api/users/{id}/password`
- `GET /api/perm/list` / `POST /api/perm/grant` / `POST /api/perm/revoke`
- `GET /api/trash` / `POST /api/trash/{id}/restore` / `DELETE /api/trash/{id}` (admin)
- `GET /api/audit` (admin)
- `POST /api/share` / `GET /api/share/mine` / `DELETE /api/share/{token}`

Подробности по auth — [auth.md](auth.md). По RBAC — [rbac.md](rbac.md).

## FTPS

Реализован через `github.com/fclairamb/ftpserverlib` + afero-адаптер над `storage.FileSystem`.

- **Только explicit-TLS** (`AUTH TLS`). Plain FTP — отключён на уровне driver settings (`TLSRequired: MandatoryEncryption`).
- **Auth** — `AuthenticateAppPassword(login, secret)` через `users.AuthenticateAppPassword`. Поддерживает основной пароль и app-passwords.
- **Share-links** — **не поддерживаются**. FTP — только именованные пользователи.
- **PASV ports** — конфигурируемый диапазон (`ftp.passive_port_min/max`). Operator должен открыть диапазон на firewall'е.
- **`PublicHost`** — обязательное поле для setup'а за NAT/firewall'ом.

**Маппинг FTP-операций на FileSystem:**

| FTP | FileSystem |
|---|---|
| `STAT` / `LIST` | `Stat` / `List` |
| `RETR` | `OpenRead` → stream over `FileReader.ReadAt` |
| `STOR` | `OpenWrite` → stream over `FileWriter.WriteAt` → `Commit` |
| `MKD` | `Mkdir` |
| `DELE` / `RMD` | `Remove` |
| `RNFR` + `RNTO` | `Rename` |

`REST <offset>` (resume download) поддерживается через `ReaderAt` — клиент указывает offset, обёртка позиционируется.

**ACL:** каждая операция вызывает `perms.Check(ctx, user.ID, nodeID, action)`. `rbac.ErrDenied` → `os.ErrPermission` → FTP 550.

Конфиг:

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

`tls` опционален — если пустой, используется `web.tls` (общий cert для HTTPS и FTPS).

Реализация — [internal/ftpsrv/](../../internal/ftpsrv/).

## WebDAV

Реализован через `golang.org/x/net/webdav` + адаптер над `storage.FileSystem`. Полная история решения — последняя WebDAV-фича, мы только что её доставили.

- **Mount под главным HTTPS** на `/dav/*` (path-prefix mux pattern). Один порт, один TLS-сертификат, общая audit-инфраструктура.
- **Auth** — HTTP Basic с `AuthenticateAppPassword` (тот же app-password что для FTPS). Sessions/CSRF не используются.
- **Locks** — in-memory (`webdav.NewMemLS()`). Single-node only. Persistent LockSystem — в [ROADMAP/Next](../../ROADMAP.md).
- **Overwrite** — реализован через `trash-then-create` (storage MVP не умеет настоящий overwrite — старая версия уходит в корзину, новая создаётся).

**Маппинг WebDAV-методов на FileSystem:**

| WebDAV | FileSystem |
|---|---|
| `PROPFIND` | `Stat` + `List` |
| `GET` | `OpenRead` |
| `PUT` | `OpenWrite` + `Commit` |
| `MKCOL` | `Mkdir` |
| `DELETE` | `Remove` |
| `MOVE` | `Rename` |
| `COPY` | библиотека декомпозирует на per-file `OpenFile` + `io.Copy` ([slow, см. ROADMAP/Next](../../ROADMAP.md)) |
| `LOCK` / `UNLOCK` | `webdav.NewMemLS` |
| `OPTIONS` | автомат, отвечает `DAV: 1, 2` |

**ACL:** каждый метод адаптера (`Stat`, `Mkdir`, `RemoveAll`, `Rename`, `OpenFile`) вызывает `perms.Check`. `rbac.ErrDenied` → `os.ErrPermission` → WebDAV `403 Forbidden`.

**Audit:** мутирующие операции эмитят events с `Details["channel"] = "webdav"` — admin может фильтровать по surface через `GET /api/audit?action=upload&channel=webdav`.

Конфиг:

```json
"webdav": {
  "enabled": false,
  "path_prefix": "/dav"
}
```

Реализация — [internal/web/webdav.go](../../internal/web/webdav.go), [internal/web/webdav_fs.go](../../internal/web/webdav_fs.go), [internal/web/webdav_file.go](../../internal/web/webdav_file.go).

## tus.io resumable uploads

[tus.io 1.0.0](https://tus.io) — стандартизированный resumable upload протокол. Используется фронтендом для крупных файлов с возможностью паузы/возобновления.

- **Mount** под `/api/tus/*` в общем HTTP-mux'е.
- **Auth** — session cookie (Web). CSRF tus-клиенты не понимают → `authedRead` (без CSRF), но session middleware всё ещё применяется.
- **Per-upload ownership check** — `info.json.user_id` сравнивается с `UserFromContext(r.Context()).ID`. Кросс-пользовательский доступ невозможен.

**Flow:**

1. `OPTIONS /api/tus` — discovery (поддерживаемые extensions: Creation, Termination).
2. `POST /api/tus` с `Upload-Length`, `Upload-Metadata` (включая `filename`, `filetype`, `dir`) → создаёт `<meta-storage>/uploads/<id>/` с `info.json`. Возвращает `Location: /api/tus/<id>`.
3. `PATCH /api/tus/<id>` с body чанка → append к staging-файлу, обновление `offset` sidecar (atomic tmp + rename).
4. `HEAD /api/tus/<id>` → текущий offset (для resume).
5. При `Upload-Length == Upload-Offset` → финализация через `dbfs.ImportPath`, который делает `os.Rename` staging → `flat-storage/<dir>/<filename>` + outbox-запись (как обычный `OpenWrite.Commit`).
6. `DELETE /api/tus/<id>` → отмена upload, удаление staging.

**Sweeper.** Фоновый воркер удаляет stale uploads (нетронуты N часов, default 24h). См. [internal/web/tus_sweeper.go](../../internal/web/tus_sweeper.go).

Конфиг:
```json
"tus": {
  "retention_hours": 24,
  "sweep_interval": "1h"
}
```

Реализация — [internal/web/tus.go](../../internal/web/tus.go).

## Share-links

Анонимные временные ссылки. Только HTTP/HTTPS. См. [rbac.md § Share-links](rbac.md#share-links-только-web).

**Endpoints:**

- `POST /api/share` (auth required, `Admin` на узле) — создание ссылки.
- `GET /api/share/mine` — список моих ссылок.
- `GET /api/share/all` (admin-only) — все ссылки в системе.
- `DELETE /api/share/{token}` — ревокация (создатель или admin).
- `GET /share/{token}/info` (anonymous) — метаданные узла без контента.
- `GET /share/{token}/download` (anonymous) — скачать (если `Read` в scope).
- `PUT /share/{token}/upload` (anonymous) — загрузить (если `Write` в scope).

**Validation на каждое использование:**
1. Токен существует.
2. `expires_at > now()`.
3. `max_uses IS NULL OR used_count < max_uses`.
4. Запрошенное action ⊆ `actions` ссылки.

`statByID` использует recursive CTE для resolve логического пути узла через `parent_id` цепочку (т.к. anonymous user не имеет ACL для walk через ltree).

Audit event на каждое использование (`action=share_use`).

Реализация — [internal/web/share.go](../../internal/web/share.go), [internal/auth/sharelinks.go](../../internal/auth/sharelinks.go).

## Сравнение auth моделей

| Surface | Identity | Secret | TLS required | Anonymous OK |
|---|---|---|---|---|
| Web API | user (session) | argon2id password | yes (HSTS) | only login endpoint |
| FTPS | user | app-password или main password | **mandatory** (`AUTH TLS`) | no |
| WebDAV | user | app-password или main password | yes if served by main HTTPS | no |
| tus.io | user (session) | inherited from Web | yes | no |
| share-link | none | URL token | yes (HSTS) | **yes** (by design) |

Подробнее — [auth.md](auth.md), [ADR-0004 App-passwords](../adr/0004-app-passwords-cross-protocol.md).
