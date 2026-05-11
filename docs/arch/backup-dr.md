# Backup & disaster recovery

storman stores metadata in PostgreSQL and content on disk. Losing the DB while the disk is intact is catastrophic. Losing the disk while the DB is intact is also catastrophic. The DR strategy is two lines of defence. Full rationale — [ADR-0005](../adr/0005-dr-pg-dump-plus-disk-walk.md).

The DB is the single source of truth for metadata. File content on disk has additional value: even without the DB, user files remain "as-is" (see [ADR-0006 honest storage](../adr/0006-flatfile-only-mvp.md)) and can be partially recovered through a walker.

## Line 1 — `pg_dump --format=custom`

An in-process cron in `internal/backup/`:

- Starts via `backup.Start(ctx, interval, retention, dataDir, dsn, tools, logger)` in `serve.go`.
- Ticks every `interval` (default 24h).
- On each tick:
  1. Runs `pg_dump --format=custom <dsn>` via `os.Exec`. Output goes through a pipe → file `<meta-storage>/backups/<ISO-ts>.dump`.
  2. Ring-buffer retention: after a successful dump, files older than the N most recent are removed (`backup.retention`, default 7).
- Manual run: `storman backup` (resolves config via `--config` / `STORMAN_CONFIG_PATH` / default).

**Custom format** gives:
- Already compressed (gzip inside, format-specific).
- Supports partial restore via `pg_restore -t <table>`.
- Contains the schema and checksums.
- MVCC snapshot — does not block writers.

**Cost:** for 100k files — a few to tens of MB per dump. Creation takes seconds for a compact dump, minutes for large ones. Does not block FS operations (MVCC snapshot).

**`pg_dump_cmd` / `pg_restore_cmd` config.** Default — `["pg_dump"]` / `["pg_restore"]` (binaries on PATH). For a dev setup with PostgreSQL in Docker:

```json
"backup": {
  "interval": "24h",
  "retention": 7,
  "pg_dump_cmd": ["docker", "exec", "-i", "storman-pg", "pg_dump"],
  "pg_restore_cmd": ["docker", "exec", "-i", "storman-pg", "pg_restore"]
}
```

**Important for Docker:** `pg_dump` without `--file=-` — it writes to stdout by default; passing `--file=-` together with `docker exec` is broken (the dump is empty). This gotcha is handled in `internal/backup/backup.go` — we do not pass `--file=-` and read the process stdout instead.

**`pg_restore` — pipe stdin, not a path.** With `docker exec` the container cannot see the host path of the dump; the solution is to open the dump on the host and `cmd.Stdin = file`. See `Restore` in `internal/backup/backup.go`.

## Line 2 — `recover --from-disk`

The last line of defence: there is no DB and no recent dump. CLI — `storman recover from-disk`. Implementation — [internal/cli/recover.go](../../internal/cli/recover.go).

Algorithm:

1. Walk `<data-dir>/flat-storage/`.
2. For every file:
   - Create a `nodes` row with `status='ready'`, `backend_kind='flat'`, `backend_ref` = relative path.
   - sha256 is recomputed via streaming read.
   - mime is detected by magic bytes.
   - ACL — owner-only for the root user (passed via `--owner=<login>`). Per-user ACLs are not restored.
   - `node_meta.extra` — empty; the indexing pipeline will recompute EXIF / AI etc.
3. Nodes are marked `recovered_from_disk=true` in the audit log for later manual review.

**What is lost on `recover --from-disk`:**
- `share_links` — not in the DB, tokens are gone.
- `node_settings` — custom versioning / retention policies.
- Extended metadata (EXIF, AI tags).
- The audit log for the prior period.
- Users and their ACLs (other than root-owner).

**What is preserved:**
- All files (the content on disk is untouched).
- Tree structure (reflects the `flat-storage/` layout).
- sha256 (recomputed).
- mime (detected).

## Line 1.5 — `recover --from-backup`

`storman recover --from-backup [<path>]` — `pg_restore` of the specified dump (by default the latest `.dump` from `<meta-storage>/backups/`) into an empty DB. Then **fsck-reconcile** with the disk:

1. For each `nodes` row — `FileBackend.Stat(backend_ref)`. Missing on disk → `status='broken'`, audit warning.
2. Walk the disk — orphan files (no reference in the restored DB) → moved to `<meta-storage>/quarantine/<ts>/` with a manifest for manual review.
3. Optionally (`--verify-sha256`) — full sha256 recompute for all files and check against the DB. Expensive, but detects content corruption.

This yields consistent recovery: after reconcile, the DB and disk are aligned.

## Outbox recovery (not DR, regular procedure)

On a **regular** process start after a crash (not a DR scenario) — `fs.RecoverPending(ctx)` replays stuck outbox operations. This is part of normal startup, runs automatically. See [storage.md § Crash recovery](storage.md#crash-recovery) and [ADR-0002](../adr/0002-outbox-fs-db-atomicity.md).

`RecoverPending` and DR are different mechanisms. RecoverPending does not help with DB/disk loss; DR does not help with a half-finished transaction.

## RPO / RTO

For personal scale (single-user, single-host):
- **RPO** = `backup.interval` (default 24h). Between backups losing the DB = losing the last day of metadata. Files are preserved — `recover --from-disk` brings them back, but without ACLs/metadata.
- **RTO** = minutes for `recover --from-backup`, hours for `recover --from-disk` (depends on sha256 recompute).

If a smaller RPO is needed — `backup.interval: "1h"` (or less) + larger retention. WAL streaming in [ADR-0005 § Future evolution](../adr/0005-dr-pg-dump-plus-disk-walk.md) — backlog.

## Backup integrity check

Not implemented. The idea — periodic `pg_restore --list` on the freshest dump against a throwaway DB, plus checksum verification. In MVP the operator is responsible.

## Implementation

- [internal/backup/backup.go](../../internal/backup/backup.go) — `Run`, `Restore` (stdin pipe), `LatestDump`, `enforceRetention`.
- [internal/backup/scheduler.go](../../internal/backup/scheduler.go) — `Start(ctx, interval, retention, ...)`.
- [internal/cli/backup.go](../../internal/cli/backup.go) — `storman backup`.
- [internal/cli/recover.go](../../internal/cli/recover.go) — `storman recover --from-backup` / `--from-disk`.
