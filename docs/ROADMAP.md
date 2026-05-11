# Roadmap

_Last updated: 2026-05-11_

Format — Now / Next / Later (no date-locked Gantt chart). Now = what is delivered / in progress on this horizon; Next = what we are ready to pick up next; Later = directions without dates.

## Now — MVP delivered

| Subsystem | Status | Where it lives |
|---|---|---|
| Storage core: `FileSystem` + `FlatFile` backend, outbox, trash, recover-pending | ✅ | [arch/storage.md](arch/storage.md), [ADR-0001](adr/0001-filesystem-boundary.md), [ADR-0002](adr/0002-outbox-fs-db-atomicity.md) |
| Auth: argon2id passwords, sliding+absolute sessions, CSRF double-submit, app-passwords | ✅ | [arch/auth.md](arch/auth.md) |
| RBAC: bitmask Read/Write/Remove/Admin + computed Traverse, share-links | ✅ | [arch/rbac.md](arch/rbac.md), [ADR-0003](adr/0003-rbac-computed-traverse.md) |
| Delivery: Web API, FTPS, WebDAV, tus.io, share-links | ✅ | [arch/interfaces.md](arch/interfaces.md) |
| Audit log + login rate-limit | ✅ | [arch/auth.md](arch/auth.md) |
| Backup / Recover: `pg_dump --format=custom` + `recover --from-disk` | ✅ | [arch/backup-dr.md](arch/backup-dr.md), [ADR-0005](adr/0005-dr-pg-dump-plus-disk-walk.md) |
| Async indexing pipeline (hash worker) | ✅ | [arch/indexing.md](arch/indexing.md) |
| Single binary with embedded React SPA | ✅ | [arch/overview.md](arch/overview.md) |

## Next — ~quarter horizon

Candidates, sorted by decreasing value:

- **EXIF + MIME enrichment workers.** Reuse the existing `jobs` machinery, add `kind='exif'` / `kind='mime'` executors, write typed fields to `node_meta` + JSONB `extra`. No changes to `storage.FileSystem` required.
- **Cron auto-creation of partitions for history tables.** `outbox_history`, `jobs_history`, `audit_log` are partitioned by month; the migration creates the current month + 3 ahead. We need a background worker that creates the next partition before it is needed (and optionally drops old ones per retention).
- **Persistent WebDAV LockSystem.** Currently in-memory (`webdav.NewMemLS`) — single-node only. A multi-process scenario will need a Postgres-backed LockSystem.
- **Native `FileSystem.Copy`.** WebDAV COPY is currently decomposed by the library into per-file `OpenFile` / `io.Copy` — N transactions for a tree of N files. Add a method to `FileSystem`, implement in `dbfs` via `INSERT ... SELECT` over ltree + a hardlink at the FlatFile level (or ref-copy for future CDC). Consistent with [ADR-0001](adr/0001-filesystem-boundary.md): an interface extension, not a bypass.
- **2FA (TOTP).** The `users.totp_secret` column is already reserved. AEAD encryption with the key from config, `github.com/pquerna/otp/totp`. Compatible with the existing flow: app-passwords become mandatory for FTPS/WebDAV when 2FA is enabled.
- **`fsck` scanner.** An in-process cron task: walk `flat-storage/`, reconcile with `nodes`. Orphan files → `<meta>/quarantine/`. A row without a file → `status='broken'` + audit.
- **Quotas (per-user, per-folder).** Checked before an upload starts, confirmed in the same transaction as the `nodes` insert.

## Later — directions without commitments

The architecture is laid out so that these can be added without rewriting base layers.

- **CDC backend (`CDCFile`).** Chunk-level deduplication (FastCDC), delta transfer (rsync-style), readiness for remote blob storage. Enabled per-directory via `nodes.backend_kind = 'cdc'`. Inherited by descendants through ltree lookup; existing files stay on their backend. Decision to defer: [ADR-0006](adr/0006-flatfile-only-mvp.md).
- **Migration between backends.** `storman migrate --to=cdc <path>` — a background walker over the subtree that moves files from one backend to another via `Read → Write → atomic swap (backend_kind, backend_ref)` in the outbox.
- **File versioning.** A table `versions(node_id, version_num, backend_kind, backend_ref, ...)`. The fields `max_versions`, `retention_days`, `version_policy` are already reserved in `node_settings`. Fits neatly on top of immutable backend refs.
- **Additional protocols.** FUSE (one of the primary drivers of the `FileSystem` interface shape — `ReadAt`/`WriteAt`/`Commit` map 1:1 to FUSE operations), an S3-compatible facade (PutObject/GetObject on top of `Open*`), rsync (the delta protocol is efficient from `CDCFile`: hand out a list of chunk hashes instead of the file).
- **AI metadata enrichment.** Face recognition, OCR, image classification — separate `jobs.kind` executors, results in `node_meta.extra` (JSONB).

## What we deliberately do NOT take

See [MISSION.md § Anti-bets](MISSION.md). If a task is there, it is not in the roadmap by design.
