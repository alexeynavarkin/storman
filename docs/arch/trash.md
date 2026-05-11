# Trash: soft-delete with restore

`Remove` in storman is a soft-delete: nodes are marked deleted, physical content moves to `<meta-storage>/trash/<uuid>/`, and can be restored via the admin UI before retention expires.

## Delete lifecycle

When `FileSystem.Remove(ctx, path)` is called (via any protocol):

1. **One transaction:**
   - Recursive `UPDATE nodes SET status='deleted', deleted_at=now() WHERE path <@ $rootPath` — one SQL for the whole subtree (via the gist index on `nodes.path`).
   - Insert `outbox(op='trash', payload={node_id, backend_ref_subtree, trash_uuid, acl_snapshot, original_path, deleted_by})`.
   - Commit.

2. **After commit** — the executor (`runTrashOutbox` in [internal/storage/dbfs/trash.go](../../internal/storage/dbfs/trash.go)):
   - `os.Rename` the physical subtree from `<flat-storage>/<original-path>` into `<meta-storage>/trash/<uuid>/payload`. Atomic (single filesystem).
   - Writes `<meta-storage>/trash/<uuid>/trash.meta.json` via atomic tmp + rename. Content:
     ```json
     {
       "trash_uuid": "...",
       "node_id": "...",
       "original_path": "/docs/old/",
       "original_parent_id": "...",
       "deleted_at": "2026-05-11T...",
       "deleted_by": "user-uuid",
       "backend_kind": "flat",
       "backend_ref": "docs/old",
       "acl_snapshot": [...]
     }
     ```
   - Archive the outbox row into `outbox_history` + DELETE from `outbox` in one transaction.

`trash.meta.json` is a **service-side** trash record, not a side-car for a user file. It is self-contained — it lets us restore an item even if the DB is lost.

## What survives a delete

- The physical content — sits in `<meta-storage>/trash/<uuid>/payload`, untouched.
- The subtree structure — preserved inside `payload/` as it was at delete time.
- ACLs — a snapshot in `trash.meta.json` (for restoring the original rights).
- DB metadata — the `nodes` rows remain with `status='deleted'`, `deleted_at` non-NULL. **Not removed**, so the UI can show "deleted on ...".

## Restore (`Restore`)

`POST /api/trash/{trash_uuid}/restore` (admin-only):

1. Reads `trash.meta.json` for the target trash entry.
2. Verifies that `original_path` is free (not occupied by a new node). If occupied — conflict, 409.
3. **Transaction:**
   - `UPDATE nodes SET status='ready', deleted_at=NULL WHERE path <@ <original_path>` — restores the whole subtree.
   - Insert outbox `op='restore_trash'` with the payload to restore physical paths.
4. Executor: `os.Rename` `<trash>/<uuid>/payload` → `<flat-storage>/<original-path>`. Removes the directory `<trash>/<uuid>/` (including `trash.meta.json`).
5. Outbox archiving.

Implementation — the `Restore` method in [internal/storage/dbfs/gc.go](../../internal/storage/dbfs/gc.go).

## Purge (`Purge`)

`DELETE /api/trash/{trash_uuid}` (admin-only) — permanent deletion:

1. Reads `trash.meta.json`.
2. **Transaction:**
   - `DELETE FROM nodes WHERE id IN (subtree)` — permanent removal of metadata. ON DELETE CASCADE removes related `node_meta`, `permissions`.
   - Insert outbox `op='purge_trash'` with the path to the payload.
3. Executor: `os.RemoveAll(<trash>/<uuid>/)` — removal of the physical files.
4. Outbox archiving.

After Purge, recovery is impossible (unless from `pg_dump`).

## Trash GC

The background worker `StartGC(ctx, interval, retentionDays, logger)` starts in `serve` and ticks every `interval`:

1. `RunGC(ctx, retention)` — `SELECT trash_uuid FROM trash WHERE deleted_at < now() - retention`.
2. For each — calls `Purge`.

Config:
```json
"trash": {
  "retention_days": 30,
  "gc_interval": "1h"
}
```

`retention_days = 0` disables time-based GC. Rows stay until an explicit Purge via the UI.

## Trash visibility

`GET /api/trash` — **admin-only**. Reason: `rbac.Effective` filters out nodes with `deleted_at IS NOT NULL` (a live ACL on a deleted node makes no sense), so regular users cannot see their own deleted items through the existing ACL path. The trash is an administrative tool, not a "personal recycle bin" UI.

This is a deliberate simplification. The alternative (per-user recycle bin respecting the ACL at delete time) is overkill for personal scale.

## Atomicity via the outbox

Trash goes through the same outbox mechanism as a regular write/delete (see [storage.md § Outbox](storage.md#outbox-fs--db-atomicity)). This means:

- Crash after the `status='deleted'` UPDATE but before `os.Rename` — `RecoverPending` on startup replays `os.Rename`.
- Crash after `os.Rename` but before outbox archiving — a re-run of the executor is a no-op (`os.Stat` shows the rename already happened), and archives the outbox.
- A state "`nodes.status='deleted'` + physical file in `flat-storage/`" cannot persist for more than a moment between transactions.

## Restore without a DB

If the DB is lost but `<meta-storage>/trash/` is intact — `trash.meta.json` contains everything needed for manual restore via `recover --from-disk`. The walker, when traversing `trash/`, can either ignore the subtrees (default) or optionally restore them back into `flat-storage/<original_path>`. In MVP this is a manual operation; automation is not yet required.

## Implementation

- [internal/storage/dbfs/remove.go](../../internal/storage/dbfs/remove.go) — `Remove` (recursive UPDATE + outbox).
- [internal/storage/dbfs/trash.go](../../internal/storage/dbfs/trash.go) — `applyTrash`, `runTrashOutbox`, `trash.meta.json` writer.
- [internal/storage/dbfs/gc.go](../../internal/storage/dbfs/gc.go) — `ListTrash`, `Restore`, `Purge`, `RunGC`, `StartGC`.
- [internal/web/trash.go](../../internal/web/trash.go) — HTTP handlers (`/api/trash/*`).
