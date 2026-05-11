---
id: ADR-0005
title: DR strategy — pg_dump + disk-walk recovery, without WAL streaming
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0005: DR strategy — `pg_dump` + disk-walk recovery, without WAL streaming

## Context and problem statement

storman stores metadata in PostgreSQL and content on disk. Losing the DB while the disk is intact is catastrophic (no ACL, no hierarchy, no share-links, no EXIF). Losing the disk while the DB is intact is also catastrophic (no files). A disaster recovery strategy is needed, one that fits a single-host self-hosted deployment — without dedicated DR servers, without paid SaaS.

storman does not target a multi-node setup (see MISSION/Anti-bets). Requests about clustering/replication are out of scope.

## Decision drivers

- **Single-host, single-developer.** No ops team to watch replication lag.
- **Recovery time** in the minutes is acceptable for personal scale (not an SLA-critical service). Recovery point of the last 24 hours is acceptable.
- **Simplicity** — the operator must understand what happens on `storman recover` without reading 300 pages of documentation.
- **Minimum dependencies.** PostgreSQL is already there. Dragging in pgBackRest / Barman / WAL-G just for DR is overkill.
- **Ability to operate without the DB.** If the DB burned along with the dump, files on disk must still be at least partially recoverable.

## Considered options

### Option A — pg_dump + disk-walk recovery (chosen)

Two lines of defence:

**Line 1 — `pg_dump --format=custom`.**
- An in-process cron (`internal/backup/`) runs `pg_dump --format=custom` every N hours (default 24h) into `<meta>/backups/<ISO-ts>.dump`.
- Custom format — compressed, supports partial restore via `pg_restore -t <table>`, contains schema and checksums.
- Retention — a ring buffer of the last N dumps (default 7).
- Recovery: `storman recover --from-backup [<path>]` → `pg_restore` the specified dump (default: the latest) into an empty DB. After loading — fsck-reconcile against the disk.

**Line 2 — `recover --from-disk`.**
- If there is neither DB nor a recent dump: walk `flat-storage/`, for every file create a `nodes` row with sha256, mime by magic bytes, owner-only ACL for the root user.
- `node_meta.extra` — empty; EXIF and AI enrichment is recomputed by the indexing pipeline.
- `share_links`, `node_settings`, custom ACLs — lost. Audit log — lost.
- Nodes are marked `recovered_from_disk=true` in the audit log for later manual review.

### Option B — Streaming replication + PITR

A replica DB, async replication, WAL archive. If the master falls — promote the slave, no data loss.

### Option C — Logical replication to S3 (Debezium-style CDC to an external store)

All changes in PostgreSQL are streamed as events. Recovery — replay events from the offset.

### Option D — Filesystem-level snapshots (ZFS / btrfs)

Snapshot the DB and the disk in a single atomic FS-level operation. Restore = rollback to the snapshot.

## Decision outcome

**Chosen: Option A — `pg_dump` + disk-walk recovery.**

Rationale:
- `pg_dump --format=custom` is standard, zero-setup, zero external dependencies. Already in PostgreSQL coreutils.
- Single-host scale: replication overhead is not justified. A 24h backup interval is acceptable; reducing the interval is possible via config.
- Disk-walk recovery is a **failsafe**. When a dump is lost along with the infrastructure, user files are still "as-is" (honest storage policy — see MISSION). The walker can recover at least metadata from the files themselves. This is a unique feature of the honest-storage model.
- Recovery is fully automated: `storman recover --from-backup` and `storman recover --from-disk` — two commands.

The alternatives are rejected:
- **Replication (B/C)** — requires a second host setup, lag monitoring, additional attack surface. Does not stretch to "one person self-hosted".
- **ZFS snapshots (D)** — a working solution for those who have ZFS. We do not bundle it: a user who chose ZFS can configure snapshots via `cron`. storman does not get in the way.

## Consequences

### Positive
- Zero-config DR: `"backup": {"interval": "24h", "retention": 7}` in config is enough.
- No additional hosts, no paid SaaS, no network traffic off the machine.
- Disk-walk recovery works even when the entire DB storage is lost.
- pg_dump custom format is a standard, restorable by any pg_restore from the same or a future major version.

### Negative
- RPO = backup interval (default 24h). Between backups, losing the DB = losing the last day of metadata. Files (content) are preserved — disk-walk brings them back, but without ACL and custom metadata.
- pg_dump does not block writers (MVCC snapshot) but does load I/O. On large stores it may take minutes.
- Disk-walk recomputes sha256 for every file — an expensive operation, `--from-disk` recovery scales linearly with size.
- Unlike PITR, you cannot recover to "the point 3 minutes before catastrophe". Only to the moment of the last dump.

### Neutral
- Backup command is configurable (`backup.pg_dump_cmd`) — for a dev setup with PostgreSQL in Docker we use `["docker", "exec", "-i", "storman-pg", "pg_dump"]`. Restore similarly.
- If the user wants more frequent dumps — `interval: "1h"` + larger retention. No structural change required.

## Future evolution

If RPO < backup interval is needed:
- Add an optional WAL archive (`backup.wal_archive_dir`) — PostgreSQL's `archive_command` writes WAL segments alongside dumps. Recovery = `pg_restore` + replay WAL up to the target moment.
- Enabled by a flag, default remains `pg_dump`-only.

## Related

- Implementation: [internal/backup/](../../internal/backup/), [internal/cli/recover.go](../../internal/cli/recover.go).
- Architecture: [docs/arch/backup-dr.md](../arch/backup-dr.md).
- Risk register: [docs/risks.md](../risks.md) (RPO trade-off as an explicit risk).
