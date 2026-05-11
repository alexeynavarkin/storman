# Backup & disaster recovery

storman storiт метаданные в PostgreSQL и контент на диске. Потеря БД при сохранном диске — катастрофическая. Потеря диска при сохранной БД — тоже. DR-стратегия — две линии defence. Полное обоснование — [ADR-0005](../adr/0005-dr-pg-dump-plus-disk-walk.md).

БД — единственный источник правды для метаданных. Содержимое файлов на диске обладает дополнительной ценностью: даже без БД, файлы пользователя сохраняются «как есть» (см. [ADR-0006 honest storage](../adr/0006-flatfile-only-mvp.md)) и могут быть частично восстановлены через walker.

## Линия 1 — `pg_dump --format=custom`

Внутрипроцессный cron в `internal/backup/`:

- Стартует через `backup.Start(ctx, interval, retention, dataDir, dsn, tools, logger)` в `serve.go`.
- Тикает каждый `interval` (default 24h).
- На каждом тике:
  1. Запускает `pg_dump --format=custom <dsn>` через `os.Exec`. Output идёт в pipe → файл `<meta-storage>/backups/<ISO-ts>.dump`.
  2. Кольцевой retention: после успешного дампа удаляются файлы старше N свежайших (`backup.retention`, default 7).
- Ручной запуск: `storman backup --data-dir=<path>`.

**Custom format** даёт:
- Уже сжат (gzip-внутри, custom-формат специфичный).
- Поддерживает partial restore через `pg_restore -t <table>`.
- Содержит схему и checksum'ы.
- MVCC snapshot — не блокирует writers.

**Стоимость:** для 100k файлов — единицы–десятки MB на дамп. Создание — секунды для compact дампа, минуты для крупных. Не блокирует FS-операции (snapshot MVCC).

**`pg_dump_cmd` / `pg_restore_cmd` конфиг.** Default — `["pg_dump"]` / `["pg_restore"]` (бинари на PATH). Для dev setup с PostgreSQL в Docker:

```json
"backup": {
  "interval": "24h",
  "retention": 7,
  "pg_dump_cmd": ["docker", "exec", "-i", "storman-pg", "pg_dump"],
  "pg_restore_cmd": ["docker", "exec", "-i", "storman-pg", "pg_restore"]
}
```

**Важно для Docker:** `pg_dump` без `--file=-` — пишет в stdout по умолчанию; передавать `--file=-` вместе с `docker exec` ломается (дампа пустой). Этот gotcha решён в `internal/backup/backup.go` — не передаём `--file=-`, читаем stdout процесса.

**`pg_restore` — pipe stdin, не path.** При `docker exec` контейнер не видит host-путь к дампу; решение: открываем дамп на host'е, `cmd.Stdin = file`. См. `Restore` в `internal/backup/backup.go`.

## Линия 2 — `recover --from-disk`

Последняя линия defence: нет ни БД, ни свежего дампа. CLI команда — `storman recover --from-disk --data-dir=<path>`. Реализация — [internal/cli/recover.go](../../internal/cli/recover.go).

Алгоритм:

1. Walk по `<data-dir>/flat-storage/`.
2. Для каждого файла:
   - Создаётся `nodes` со `status='ready'`, `backend_kind='flat'`, `backend_ref` = relative path.
   - sha256 пересчитывается через streaming read.
   - mime определяется по magic bytes.
   - ACL — owner-only для root-пользователя (передаётся через `--owner=<login>`). Пользовательские ACL не восстанавливаются.
   - `node_meta.extra` — пусто; индексационный pipeline пересчитает EXIF / AI и т.п.
3. Узлы помечаются `recovered_from_disk=true` в audit log для последующего ручного review.

**Что теряется при `recover --from-disk`:**
- `share_links` — нет в БД, токены потеряны.
- `node_settings` — кастомные политики версионирования / ретеншна.
- Расширенная мета (EXIF, AI tags).
- Audit log за весь предыдущий период.
- Пользователи и их ACL (кроме root-owner).

**Что сохраняется:**
- Все файлы (содержимое на диске не модифицировано).
- Структура дерева (отражает раскладку в `flat-storage/`).
- sha256 (пересчитанная).
- mime (детектирован).

## Линия 1.5 — `recover --from-backup`

`storman recover --from-backup [<path>]` — `pg_restore` указанного дампа (по умолчанию свежайшего `.dump` из `<meta-storage>/backups/`) в пустую БД. Далее **fsck-reconcile** с диском:

1. Для каждой `nodes` row — `FileBackend.Stat(backend_ref)`. Отсутствует на диске → `status='broken'`, audit warning.
2. Walk по диску — orphan-файлы (нет ссылки в восстановленной БД) → перемещаются в `<meta-storage>/quarantine/<ts>/` с manifest для ручного review.
3. Опционально (`--verify-sha256`) — полный пересчёт sha256 для всех файлов и сверка с БД. Дорого, но детектит порчу контента.

Это даёт consistent восстановление: после reconcile состояние БД и диска согласовано.

## Outbox recovery (не DR, штатная процедура)

При **штатном** старте процесса после краша (не DR-сценарий) — `fs.RecoverPending(ctx)` доигрывает зависшие outbox-операции. Это часть нормального startup'а, выполняется автоматически. См. [storage.md § Recovery после краша](storage.md#recovery-после-краша) и [ADR-0002](../adr/0002-outbox-fs-db-atomicity.md).

`RecoverPending` и DR — разные механизмы. RecoverPending не помогает при потере БД / диска; DR не помогает при половине транзакции.

## RPO / RTO

Для personal-scale (single-user, single-host):
- **RPO** = `backup.interval` (default 24h). Между бэкапами потеря БД = потеря последних суток метаданных. Файлы при этом сохранены — `recover --from-disk` их вернёт, но без ACL/мета.
- **RTO** = минуты для `recover --from-backup`, часы для `recover --from-disk` (зависит от sha256 пересчёта).

Если требуется меньший RPO — `backup.interval: "1h"` (или меньше) + larger retention. WAL-streaming в [ADR-0005 § Future evolution](../adr/0005-dr-pg-dump-plus-disk-walk.md) — backlog.

## Backup integrity check

Не реализован. Идея — periodic `pg_restore --list` на свежий dump в throwaway-БД, проверка checksum'ов. На MVP оператор отвечает.

## Реализация

- [internal/backup/backup.go](../../internal/backup/backup.go) — `Run`, `Restore` (stdin pipe), `LatestDump`, `enforceRetention`.
- [internal/backup/scheduler.go](../../internal/backup/scheduler.go) — `Start(ctx, interval, retention, ...)`.
- [internal/cli/backup.go](../../internal/cli/backup.go) — `storman backup`.
- [internal/cli/recover.go](../../internal/cli/recover.go) — `storman recover --from-backup` / `--from-disk`.
