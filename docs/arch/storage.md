# Storage layer

The storage layer is the single point for all file mutations. Two abstraction levels:

- **`storage.FileSystem`** — a façade over the node tree for all frontend protocols. One implementation — `internal/storage/dbfs/`, on top of `nodes` in PostgreSQL.
- **`storage.FileBackend`** — low-level storage of a single file's content. In MVP the only implementation is `FlatFile` in `internal/storage/flat/`. CDC is deferred ([ADR-0006](../adr/0006-flatfile-only-mvp.md)).

Invariant: **all delivery protocols go only through `FileSystem`**, never through the backend and never through direct SQL. See [ADR-0001](../adr/0001-filesystem-boundary.md).

## `FileSystem` interface

```go
type FileSystem interface {
    // Tree
    Stat(ctx, path) (*NodeInfo, error)
    List(ctx, path) ([]NodeInfo, error)
    Mkdir(ctx, path, opts MkdirOpts) error
    Remove(ctx, path) error                  // soft-delete → trash
    Rename(ctx, oldPath, newPath) error      // including between folders with different backends;
                                             // the node stays on its backend_kind, no conversion

    // Content
    OpenRead(ctx, path) (FileReader, error)
    OpenWrite(ctx, path, opts WriteOpts) (FileWriter, error)
}

type FileReader interface {
    io.ReaderAt
    io.Closer
    Size() int64
}

type FileWriter interface {
    io.WriterAt
    Truncate(size int64) error
    Commit() error          // finalization: atomic publish
    Abort() error           // cancel: cleans staging, does not publish
    io.Closer               // Close without Commit/Abort returns ErrUnfinalized
}

type WriteOpts struct {
    Mode         WriteMode  // Create | Overwrite | Modify  (MVP only Create)
    ExpectedSize int64      // -1 if unknown — for quotas / preallocation
}
```

This is enough for all protocols: FUSE `read/write/release` → `ReadAt/WriteAt/Commit|Abort`; FTP `RETR/STOR` → stream on top of `ReaderAt/WriterAt`; HTTP Range → `ReaderAt`; tus.io → chunked `WriteAt` + `Commit` on `Upload-Complete`; S3 PutObject/Multipart similarly; WebDAV PUT/GET via the same primitives.

### What is deliberately NOT in the interface

- **Random write at the `FileBackend` level** — no. `WriteAt` is supported by each backend through its own staging; from the outside it is just a `FileWriter`.
- **Move/Rename in `FileBackend`** — no. A logical move in the tree is a `FileSystem` operation, it does not touch `BackendRef`. If a backend stores the ref as a path (as `FlatFile` does), then if needed — `Delete(old) + Allocate(new) + copy`. A native rename as an optimization is added *inside* `FileBackend` as a separate method (`RenameRef`), not as an ad-hoc bypass.
- **List/Walk** — no. Walking the tree is a DB job.
- **Native COPY** — no (yet). WebDAV COPY is currently decomposed by `golang.org/x/net/webdav` into per-file `OpenFile` + `io.Copy` — N transactions for a tree of N files. A native `FileSystem.Copy` is in [ROADMAP/Next](../ROADMAP.md).

## `FileBackend` interface

```go
type FileBackend interface {
    Name() string                                // "flat" | "cdc" | ...

    OpenRead(ctx, ref BackendRef) (FileReader, error)
    OpenWrite(ctx, ref BackendRef, opts WriteOpts) (FileWriter, error)
    Delete(ctx, ref BackendRef) error
    Stat(ctx, ref BackendRef) (BackendStat, error)

    // Allocate a ref for a new file. Called BEFORE the write starts, so the
    // FS can record (backend_kind, backend_ref) in nodes and outbox in the
    // same transaction that creates the node.
    Allocate(ctx, hint AllocHint) (BackendRef, error)
}

type BackendRef struct {
    Kind string  // == FileBackend.Name()
    Data string  // backend-specific: for flat — relative path; for cdc — manifest id
}

type AllocHint struct {
    NodeID       uuid.UUID
    LogicalPath  string  // hint for FlatFile: where to put the file as-is
    ExpectedSize int64
}
```

### `FlatFile` (the only implementation in MVP)

- `Allocate` → returns a relative path based on `LogicalPath` (sanitized).
- `OpenWrite` → opens a tmpfile under `<meta-storage>/uploads/`, `WriteAt` = `pwrite` into the tmpfile. On `Commit` — `fsync` + `rename` over the destination path (POSIX-atomic within a filesystem). On `Abort` — `unlink` the tmpfile.
- `OpenRead` → `os.OpenFile`, returns a wrapper with `ReadAt`.
- `Delete` → `unlink`.

See [internal/storage/flat/](../../internal/storage/flat/).

## Binding a backend to a file

- The policy lives right in `nodes.backend_kind`:
  - on a **folder** — nullable, meaning "backend for new children in this branch" (NULL = inherit);
  - on a **file** — NOT NULL and **immutable**, the actual backend of the content.
- The effective backend for a new file = ltree lookup of the nearest parent folder with a non-NULL `backend_kind`. If no ancestor has it set — default `'flat'`.
- The backend is fixed at file **creation** time. An existing file does not change its backend — even if the parent's policy is changed.
- Migration between backends (`storman migrate --to=cdc <path>`) is a separate background operation (see ROADMAP/Later).

In MVP it is always `'flat'`. The dual semantics of the column are an investment in future CDC, see [ADR-0006](../adr/0006-flatfile-only-mvp.md).

## Write lifecycle

`FileSystem.OpenWrite(ctx, path, WriteOpts) → ... → Commit` proceeds as follows:

1. The FS computes the effective backend for the new file (ltree lookup, default `'flat'`).
2. The FS calls `backend.Allocate(hint)` → receives a `BackendRef`.
3. **Transaction 1.** Insert `nodes(status='pending', backend_kind, backend_ref)` + insert `outbox(op='create_file', payload={upload_id, backend_ref})`. Commit.
4. A `FileWriter` (a backend writer) is returned.
5. The client writes (`WriteAt`/`Truncate`), accumulating data in staging.
6. The client calls `Commit` → the backend finalizes (for `FlatFile` — `fsync` + `rename` over the destination path).
7. **Transaction 2.** Update `nodes(status='ready', size, mtime)` + insert a hash job into `jobs` + archive the outbox row into `outbox_history` (`INSERT INTO outbox_history ... SELECT ... FROM outbox WHERE id = $1; DELETE FROM outbox WHERE id = $1` in a single transaction). Commit.

Any `Abort` or `Close` without `Commit` → the backend cleans staging, the FS rolls the node back through the outbox (the executor inspects current state and brings it to a consistent one — in this case, deletes the pending node).

## Outbox: FS ↔ DB atomicity

Two independent systems (disk and Postgres) — no shared transactions. We use a transactional outbox + idempotent re-play. Full rationale — [ADR-0002](../adr/0002-outbox-fs-db-atomicity.md). Here — the mechanics.

### Delete (`Remove`)

1. **Transaction.** Recursive `UPDATE nodes SET status='deleted', deleted_at=now() WHERE path <@ $root_path` (one SQL for the whole subtree). Insert `outbox(op='trash', payload={node_id, backend_ref_subtree, trash_uuid, acl_snapshot})`.
2. After commit: the executor (`runTrashOutbox`) performs `os.Rename` of the physical subtree into `<meta-storage>/trash/<uuid>/payload`, writes `trash.meta.json` (atomic tmp + rename).
3. Archive the outbox row into `outbox_history` in the same transaction as the DELETE from `outbox`.

More — [trash.md](trash.md).

### Rename / Move

Similar: the outbox describes the target state (new `backend_ref` if needed, new `path`). For `FlatFile` that is `os.Rename` of the file + if needed recomputing `backend_ref` in `nodes`.

### Idempotency

Each executor on start checks the current disk state via `FileBackend.Stat` and brings it to the target. If it is already done — no-op. This makes it safe to retry on crash.

### Crash recovery

On startup the process reads `outbox WHERE status IN ('pending', 'in_progress')` and replays each row. Rows with expired `locked_until` are picked up. See `RecoverPending` in [internal/storage/dbfs/recover.go](../../internal/storage/dbfs/recover.go).

### Sweeper

Background task:
- Restarts stuck operations (expired lease).
- Escalates `failed` after exhausting retry attempts — archive with `final_status='failed'` into `outbox_history` + alert in the audit log.
- Removes orphan staging files in `<meta-storage>/uploads/` older than TTL with no associated outbox row.

### Single-file atomicity

- **Write:** `open tmp → write → fsync → rename`. POSIX guarantees rename atomicity within a filesystem.
- **Backend finalization** (`FileWriter.Commit`) guarantees either the target state or staging (cancellable via `Abort` / sweeper). No intermediate "half-written" file ever appears in the target tree.

## Upload path

### Web — tus.io resumable uploads

- A resumable, chunked, standardized protocol ([tus.io 1.0.0](https://tus.io)).
- Each upload gets an `id`; chunks are written into `<meta-storage>/uploads/<id>/` (not into the target path).
- Metadata is stored in `info.json`, progress in `offset` (a sidecar file, rewritten atomically).
- On `Upload-Complete` → the backend calls `dbfs.ImportPath(ctx, logical, srcAbsPath)`, which performs `os.Rename` of the finalized staging file into `flat-storage/` and writes an outbox row through the same path as a regular `OpenWrite.Commit`.
- **Authentication:** session-based, the same as the Web API. tus clients do not understand CSRF, so tus routes go through `authedRead`; the owner check happens via `info.json.user_id`.
- **Sweeper:** a stale upload (untouched for N hours) is automatically deleted. Implementation: [internal/web/tus_sweeper.go](../../internal/web/tus_sweeper.go).

### FTPS — STOR

- The driver redirects `STOR` to `<meta-storage>/uploads/<random>`.
- On `TRANSFER COMPLETE` — the same outbox procedure.

### Common rules

- **Never** write directly into the target path — only `tmp → rename`.
- `fsync` before rename for durability.
- Quotas (when they land in ROADMAP) are checked before the upload starts and confirmed in the same transaction as the `nodes` insert.
- Temporary file cleanup: the sweeper removes staging files older than N hours with no associated outbox row.

## Recovery (admin operations exposed by dbfs)

`dbfs.DBFS` exposes a number of non-interface methods for admin / recovery scenarios. They are **not** part of `FileSystem`, but are allowed (see [ADR-0001 § Exception](../adr/0001-filesystem-boundary.md)) for functions the interface does not model by design:

- `Bootstrap(ctx) (created bool, err)` — creates the root node on first start.
- `RecoverPending(ctx) (acted int, err)` — runs on `serve` startup, replays stuck outbox operations.
- `StartGC(ctx, interval, retentionDays, logger)` — background trash worker (see [trash.md](trash.md)).
- `ListTrash(ctx)` / `Restore(ctx, trashUUID)` / `Purge(ctx, trashUUID)` — admin CRUD over the trash (root-admin only).
- `ImportPath(ctx, logical, srcAbsPath)` — used by tus to finalize an upload via `os.Rename` instead of a backend stream.

All of them go through the same transactions and outbox pattern as the public FS methods. This is **not** a bypass — these are extension points for admin use-cases.
