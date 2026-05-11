# Async indexing pipeline

Uploading a file is the sync critical path; metadata enrichment is async. This gives fast responses to the upload operation and scales separately from the Web/FTP servers.

## Architecture

Two-stage:

**Stage 1 — Sync** (inside the upload transaction):
- `OpenWrite.Commit` updates `nodes(status='ready', size, mtime)`.
- In the same transaction — `INSERT INTO jobs (node_id, kind, status) VALUES ($1, 'hash', 'pending')`.

**Stage 2 — Async** (a separate goroutine pool):
- `JobsPool` starts N workers (configurable). Each worker:
  1. Leases a job from `jobs` via `SELECT ... FOR UPDATE SKIP LOCKED WHERE status='pending' AND kind=$1 ORDER BY created_at LIMIT 1`.
  2. Marks `status='in_progress'`, `locked_until=now() + LeaseDuration`.
  3. Calls the `kind`-specific executor (e.g. hasher).
  4. On success — archives into `jobs_history` (`final_status='done'`).
  5. On error — increments `attempts`; after `MaxAttempts` (default 5) — archives with `final_status='failed'` + audit alert.

## Workers (MVP)

In MVP only one executor is implemented — **hasher**:

- Reads content via `FileBackend.OpenRead`.
- Computes SHA-256 in a streaming manner via `sha256.New()` + `io.Copy`.
- In one transaction: `UPDATE nodes SET sha256 = $1 WHERE id = $2` + `INSERT INTO node_meta (node_id, ...) ON CONFLICT (node_id) DO UPDATE SET ...`.

Implementation — [internal/jobs/hasher.go](../../internal/jobs/hasher.go).

## Roadmap workers

See [ROADMAP/Next](../ROADMAP.md):

- **EXIF extractor** — for `image/*` (via `go-exif` or a built-in parser). Populates the typed fields `node_meta.width/height/taken_at/lat/lng` + JSONB `extra.exif.*` for rare fields (ISO, focal length, etc.).
- **MIME validator** — MIME detection by magic bytes (`net/http.DetectContentType` or `gabriel-vasile/mimetype`). Already partially done on upload, but an executor can recompute for recovered nodes where MIME is unknown.
- **Face recognition / AI tags** — for the future.

A new kind of worker = a new file with an `Executor` implementation + registration in `NewPool` in `serve.go`. No changes in the storage/auth/web layers are required.

## Concurrency and SKIP LOCKED

`SELECT FOR UPDATE SKIP LOCKED` is the standard queue pattern in PostgreSQL:

- Multiple workers can lease jobs from different rows concurrently without blocking each other.
- If a competitor already has a row — `SKIP LOCKED` skips it without waiting.
- A worker crash → `locked_until` expires → the sweeper / the next worker picks it up.

This gives linear scaling with the number of workers without complex coordination.

## Config

```json
"indexing": {
  "workers": 2,
  "poll_interval": "5s"
}
```

- `workers` — number of goroutines in the pool. Default 2.
- `poll_interval` — pause between lease attempts (when the queue is empty). Default 5 seconds.

When the queue is active, workers take jobs back-to-back without the `poll_interval` delay.

## Retry policy

- `MaxAttempts = 5` (hardcoded).
- `LeaseDuration = 5 minutes` (hardcoded) — if a worker does not report in within this time, the job is considered stuck and may be picked up.
- On each failure `attempts++`, `last_error` is updated. After `MaxAttempts` — final `failed`.

Backoff between retries is linear in `attempts` (`poll_interval * attempts`). Not exponential, so failed jobs do not linger too long.

## Archiving into `jobs_history`

Terminal rows (`done` or final `failed`) move into `jobs_history` — an append-only table partitioned by month. See [database.md § jobs_history](database.md#jobs_history--append-only-archive-of-finished-jobs).

In one transaction: `INSERT INTO jobs_history (...) SELECT ... FROM jobs WHERE id = $1; DELETE FROM jobs WHERE id = $1`. This keeps `jobs` hot and small; all history is in the partitioned table.

## Atomic enqueue with upload

The hash job is inserted in **the same transaction** that sets `nodes.status='ready'`:

```sql
-- inside FileWriter.Commit
BEGIN;
UPDATE nodes SET status='ready', size=$1, mtime=now() WHERE id=$2;
INSERT INTO outbox_history (...) SELECT ... FROM outbox WHERE id=$3;
DELETE FROM outbox WHERE id=$3;
INSERT INTO jobs (node_id, kind, status) VALUES ($2, 'hash', 'pending');
COMMIT;
```

This means: **every committed file is guaranteed to get a hash**. A state "file `ready`, but no `jobs` row" cannot exist — it would either be an aborted transaction (in which case `status` is not `ready`) or DB corruption.

## Implementation

- [internal/jobs/jobs.go](../../internal/jobs/jobs.go) — `Service` (Enqueue / Lease / Finish / Release).
- [internal/jobs/pool.go](../../internal/jobs/pool.go) — `Pool` + worker goroutines + retry/release.
- [internal/jobs/hasher.go](../../internal/jobs/hasher.go) — `Hasher` executor.
- Launch in [internal/cli/serve.go](../../internal/cli/serve.go) — `jobs.NewPool(jobSvc, jobs.KindHash, hasher, cfg.Indexing.Workers, ...)`.
