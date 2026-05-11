# Roadmap

_Last updated: 2026-05-11_

Формат — Now / Next / Later (без date-locked Gantt-chart'а). Now — что доставлено / в работе на этом горизонте; Next — то, что мы готовы взять следующим; Later — направления без дат.

## Now — MVP доставлен

| Подсистема | Статус | Где живёт |
|---|---|---|
| Storage core: `FileSystem` + `FlatFile` backend, outbox, корзина, recover-pending | ✅ | [docs/arch/storage.md](docs/arch/storage.md), [ADR-0001](docs/adr/0001-filesystem-boundary.md), [ADR-0002](docs/adr/0002-outbox-fs-db-atomicity.md) |
| Auth: argon2id passwords, sliding+absolute sessions, CSRF double-submit, app-passwords | ✅ | [docs/arch/auth.md](docs/arch/auth.md) |
| RBAC: bitmask Read/Write/Remove/Admin + computed Traverse, share-links | ✅ | [docs/arch/rbac.md](docs/arch/rbac.md), [ADR-0003](docs/adr/0003-rbac-computed-traverse.md) |
| Delivery: Web API, FTPS, WebDAV, tus.io, share-links | ✅ | [docs/arch/interfaces.md](docs/arch/interfaces.md) |
| Audit log + login rate-limit | ✅ | [docs/arch/auth.md](docs/arch/auth.md) |
| Backup / Recover: `pg_dump --format=custom` + `recover --from-disk` | ✅ | [docs/arch/backup-dr.md](docs/arch/backup-dr.md), [ADR-0005](docs/adr/0005-dr-pg-dump-plus-disk-walk.md) |
| Async indexing pipeline (hash worker) | ✅ | [docs/arch/indexing.md](docs/arch/indexing.md) |
| Single binary с embedded React SPA | ✅ | [docs/arch/overview.md](docs/arch/overview.md) |

## Next — горизонт ~квартал

Кандидаты, отсортированы по убыванию ценности:

- **EXIF + MIME enrichment workers.** Использовать существующий `jobs`-механизм, добавить `kind='exif'` / `kind='mime'` executors, писать в `node_meta` типизированные поля + JSONB `extra`. Никаких изменений в `storage.FileSystem` не требует.
- **Cron автосоздание партиций history-таблиц.** `outbox_history`, `jobs_history`, `audit_log` партиционированы по месяцу; миграция создаёт текущий + 3 вперёд. Нужен фоновый воркер, который создаёт следующую партицию до того, как она понадобится (и опционально дропает старые по retention).
- **Persistent WebDAV LockSystem.** Сейчас in-memory (`webdav.NewMemLS`) — single-node only. Для multi-process сценария потребуется Postgres-backed LockSystem.
- **Native `FileSystem.Copy`.** WebDAV COPY сейчас декомпозируется на per-file `OpenFile` / `io.Copy` в библиотеке — N транзакций для дерева из N файлов. Добавить метод в `FileSystem`, реализовать в `dbfs` через `INSERT ... SELECT` по ltree + hardlink на уровне FlatFile (или ref-copy для будущего CDC). Соответствует [ADR-0001](docs/adr/0001-filesystem-boundary.md): расширение интерфейса, не обход.
- **2FA (TOTP).** Колонка `users.totp_secret` уже зарезервирована. Шифрование AEAD-ключом из config, `github.com/pquerna/otp/totp`. Совместима с существующим flow: app-passwords обязательны для FTPS/WebDAV если 2FA включена.
- **`fsck`-сканер.** Cron-задача внутри процесса: walk `flat-storage/`, сверка с `nodes`. Orphan-файлы → `<meta>/quarantine/`. Запись без файла → `status='broken'` + audit.
- **Quotas (per-user, per-folder).** Проверка перед стартом upload, подтверждение в транзакции вместе с insert в `nodes`.

## Later — направления без обещаний

Архитектура заложена так, чтобы добавлять без переписывания базовых слоёв.

- **CDC-бэкенд (`CDCFile`).** Дедупликация на уровне чанков (FastCDC), дельта-передача (rsync-style), готовность к remote blob storage. Включается per-directory через `nodes.backend_kind = 'cdc'`. Наследуется потомками через ltree-lookup; существующие файлы остаются на своём бэкенде. Решение про deferral — [ADR-0006](docs/adr/0006-flatfile-only-mvp.md).
- **Миграция между бэкендами.** `storman migrate --to=cdc <path>` — фоновый walker по поддереву, перекладывает файлы с одного backend'а на другой через `Read → Write → atomic swap (backend_kind, backend_ref)` в outbox.
- **Версионирование файлов.** Таблица `versions(node_id, version_num, backend_kind, backend_ref, ...)`. Поля `max_versions`, `retention_days`, `version_policy` уже зарезервированы в `node_settings`. Удобно ложится поверх immutable backend refs.
- **Дополнительные протоколы.** FUSE (один из главных драйверов формы `FileSystem`-интерфейса — `ReadAt`/`WriteAt`/`Commit` мапятся 1:1 на FUSE-операции), S3-compat фасад (PutObject/GetObject поверх `Open*`), rsync (дельта-протокол эффективен из `CDCFile`: отдавать список chunk-hash'ей вместо файла).
- **AI-обогащение метаданных.** Face recognition, OCR, image classification — отдельные `jobs.kind` executors, результаты в `node_meta.extra` (JSONB).

## Что мы сознательно НЕ берём

См. [MISSION.md § Anti-bets](MISSION.md). Если задача там — её нет в roadmap'е по дизайну.
