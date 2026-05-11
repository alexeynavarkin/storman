# storman

**Mission.** Self-hosted персональное файловое хранилище уровня enterprise: облачный UX, полный контроль над данными, простая эксплуатация. Один Go-бинарь + PostgreSQL — никаких других внешних зависимостей.

**North Star.** Durability — корруппция или потеря пользовательского файла невозможна. Падение процесса, питание хоста, конкурентные writer'ы, перезапись поверх — ни один из этих сценариев не оставляет узел в DB без файла на диске или файл на диске без узла в DB.

## Принципы (do not relax)

- **Хранение durable, well-tested.** DB↔disk consistency обеспечивается outbox-паттерном с crash-recovery на старте процесса (см. [ADR-0002](docs/adr/0002-outbox-fs-db-atomicity.md)).
- **Delivery-интерфейсы secure by default.** Unauthorized access невозможен. TLS обязателен (HTTPS для Web, FTPS для FTP). Plain-FTP — отключён на уровне listener'а.
- **ACL всегда проверяется на backend** (`rbac.PermissionService.Check`). UI/client не доверенный — все проверки безопасности на сервере.
- **Все delivery-протоколы ходят через `storage.FileSystem`** — единая точка, где сходятся outbox, ACL, корзина, индексация, аудит. Обходы запрещены (см. [ADR-0001](docs/adr/0001-filesystem-boundary.md)).
- **Честное хранение.** Пользовательские файлы лежат на диске «как есть». Никаких `.meta.json` рядом с файлами. Метаданные — в БД. Бэкап Time Machine'ом / restic'ом по `flat-storage/` даёт чистый архив без служебной мешанины.

## Bets (current period)

1. **MVP с пятью delivery-интерфейсами** — Web API, FTPS, WebDAV, tus.io resumable uploads, share-links. **Достигнуто (2026 Q2).**
2. **Async indexing pipeline.** Hash worker — в MVP. EXIF / MIME / face recognition workers — в [Next](ROADMAP.md).
3. **Operational hardening.** pg_dump + disk-walk DR ([ADR-0005](docs/adr/0005-dr-pg-dump-plus-disk-walk.md)), audit log, login rate limiting, корзина с retention GC.

## Anti-bets (что мы сознательно не делаем)

- **Multi-tenant / multi-node.** storman — single-user, single-host. Не строим cluster.
- **Своя БД / свой блочный store.** PostgreSQL обоснованный выбор (конкурентные writers, JSONB, ltree, партиционирование) — не пишем альтернативу.
- **Своя реализация WebDAV / FTPS-серверов.** Используем `golang.org/x/net/webdav` и `fclairamb/ftpserverlib`. Своё — только адаптеры к `storage.FileSystem`.
- **Анонимный доступ по любым протоколам кроме Web.** Share-links доступны только через HTTP. FTPS / WebDAV — только именованные пользователи с app-паролями.
