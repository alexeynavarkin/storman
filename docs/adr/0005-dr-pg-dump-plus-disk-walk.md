---
id: ADR-0005
title: DR-стратегия — pg_dump + disk-walk recovery, без WAL streaming
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0005: DR-стратегия — `pg_dump` + disk-walk recovery, без WAL streaming

## Context and problem statement

storman storiт метаданные в PostgreSQL и контент на диске. Потеря БД при сохранном диске — катастрофическая (нет ACL, нет иерархии, нет share-links, нет EXIF). Потеря диска при сохранной БД — тоже катастрофическая (нет файлов). Нужна disaster recovery стратегия, посильная для single-host self-hosted deployment'а — без отдельных DR-серверов, без paid SaaS.

storman не выходит в multi-node setup (см. MISSION/Anti-bets). Запросы про clustering/replication отметаются.

## Decision drivers

- **Single-host, single-developer.** Нет ops-команды, которая будет мониторить replication lag.
- **Recovery time** допустим в минутах для personal-scale (не SLA-критичный сервис). Recovery point — последние 24 часа допустимо.
- **Простота** — оператор должен понимать что произойдёт при `storman recover` без чтения 300 страниц документации.
- **Минимум зависимостей.** PostgreSQL уже есть. Затаскивать pgBackRest / Barman / WAL-G только для DR — overkill.
- **Возможность работы без БД.** Если БД сгорела вместе с дампом, файлы на диске должны быть восстановимы хотя бы частично.

## Considered options

### Option A — pg_dump + disk-walk recovery (chosen)

Две линии defence:

**Line 1 — `pg_dump --format=custom`.**
- Внутрипроцессный cron (`internal/backup/`) запускает `pg_dump --format=custom` раз в N часов (default 24h) в `<meta>/backups/<ISO-ts>.dump`.
- Custom format — сжат, поддерживает partial restore через `pg_restore -t <table>`, содержит схему и checksum'ы.
- Retention — кольцевой буфер последних N дампов (default 7).
- Recovery: `storman recover --from-backup [<path>]` → `pg_restore` указанного дампа (по умолчанию свежайшего) в пустую БД. После заливки — fsck-reconcile с диском.

**Line 2 — `recover --from-disk`.**
- Если нет ни БД, ни свежего дампа: walk по `flat-storage/`, для каждого файла создаётся `nodes` со sha256, mime по magic bytes, owner-only ACL для root-пользователя.
- `node_meta.extra` — пусто; EXIF и AI-обогащение пересчитаются индексационным pipeline'ом.
- `share_links`, `node_settings`, кастомные ACL — теряются. Audit log — теряется.
- Узлы помечаются `recovered_from_disk=true` в audit log для последующего ручного review.

### Option B — Streaming replication + PITR

Слейв-БД, async-replication, WAL-archive. При падении master — promote slave, без потери данных.

### Option C — Logical replication к S3 (Debezium-style CDC к external store)

Все изменения в PostgreSQL стримятся как events. Recovery — replay events с offset'а.

### Option D — Filesystem-level snapshots (ZFS / btrfs)

Snapshot БД + диска в одной atomic операции на уровне ФС. Restore = rollback к snapshot'у.

## Decision outcome

**Chosen: Option A — `pg_dump` + disk-walk recovery.**

Обоснование:
- `pg_dump --format=custom` — стандартный, нулевой setup, нулевые внешние зависимости. Уже есть в PostgreSQL coreutils.
- Single-host scale: replication-overhead не оправдан. Backup interval 24h приемлем; latency upgrade'а интервала возможен через config.
- Disk-walk recovery — **failsafe**. Когда дамп унесли вместе с инфраструктурой, файлы пользователя всё ещё лежат «как есть» (политика honest storage — см. MISSION). Walker может восстановить хотя бы метаданные из самих файлов. Это уникальная фича honest storage модели.
- Recovery полностью автоматизирована: `storman recover --from-backup` и `storman recover --from-disk` — две команды.

Альтернативы отметены:
- **Replication (B/C)** — требует setup второго хоста, мониторинга lag'а, дополнительной surface attack'и. Не stretch'ит на «один человек self-host».
- **ZFS snapshots (D)** — рабочее решение для тех у кого ZFS. Не делаем встроенно: пользователь, который выбрал ZFS, может настроить snapshot'ы через `cron`. storman не препятствует.

## Consequences

### Positive
- Zero-config DR: достаточно `"backup": {"interval": "24h", "retention": 7}` в config.
- Не нужны дополнительные хосты, ни paid SaaS, ни сетевой трафик за пределы машины.
- Disk-walk recovery работает даже когда вообще всё хранилище БД утрачено.
- pg_dump custom format — стандарт, restorable любой версией pg_restore из той же или будущей major.

### Negative
- RPO = backup interval (default 24h). Между бэкапами потеря БД = потеря последних суток метаданных. Файлы (content) при этом сохранены — disk-walk их вернёт, но без ACL и кастомной меты.
- pg_dump блокирует не writers (MVCC snapshot), но даёт нагрузку на I/O. Для крупных хранилищ может занимать минуты.
- Disk-walk пересчитывает sha256 для всех файлов — дорогая операция, в `--from-disk` recovery затянется по линейному времени.
- В отличие от PITR — нельзя восстановиться на «точку 3 минуты до катастрофы». Только на момент последнего дампа.

### Neutral
- Backup-cmd конфигурируется (`backup.pg_dump_cmd`) — для dev-setup'а где PostgreSQL в Docker используется `["docker", "exec", "-i", "storman-pg", "pg_dump"]`. Restore аналогично.
- Если пользователь хочет более частые дампы — `interval: "1h"` + larger retention. Никаких структурных изменений не требует.

## Future evolution

Если потребуется RPO < backup interval:
- Добавить опциональный WAL-archive (`backup.wal_archive_dir`) — `archive_command` PostgreSQL пишет WAL-сегменты рядом с дампами. Recovery = `pg_restore` + replay WAL до целевого момента.
- Включается флагом, дефолт остаётся `pg_dump`-only.

## Related

- Implementation: [internal/backup/](../../internal/backup/), [internal/cli/recover.go](../../internal/cli/recover.go).
- Architecture: [docs/arch/backup-dr.md](../arch/backup-dr.md).
- Risk register: [docs/risks.md](../risks.md) (RPO trade-off как explicit-risk).
