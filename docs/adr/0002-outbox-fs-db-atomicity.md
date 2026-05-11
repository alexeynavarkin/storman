---
id: ADR-0002
title: Transactional outbox for FS↔DB atomicity
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0002: Transactional outbox for FS↔DB atomicity

## Context and problem statement

Every mutating FS operation (`OpenWrite.Commit`, `Remove`, `Rename`, migration between backends) touches two independent stores: PostgreSQL (nodes, metadata, ACL) and the filesystem (`flat-storage/`). There are no shared transactions between them — a process crash, kill -9, power loss or OOM in the middle leaves either a node in the DB without a file on disk, or a file on disk without a node in the DB. Both outcomes violate the durability north-star.

We need a mechanism that **guarantees**: after `Commit` returns to the client, either the state is fully consistent, or the recovery process will bring it to a consistent state (without human intervention).

## Decision drivers

- **No silent data loss** — durability is the north-star.
- **Atomicity on a single host** — we do not build a cluster, single-host crash recovery is the main scenario.
- **Simple to reason about** — a code reviewer must understand in a minute what happens on a crash.
- **PostgreSQL is the only external dependency** — we do not want to drag in Kafka/etcd/RabbitMQ just for coordination.
- **Independent of external transaction managers** — there is no XA coordinator in the architecture.

## Considered options

### Option A — Transactional outbox + idempotent re-play (chosen)

Each FS operation:

1. Stages content: `FileBackend` writes the file into `<meta>/uploads/<id>` (atomic-replaceable staging).
2. In a single PostgreSQL transaction:
   - inserts/updates a row in `nodes` with `status='pending'`;
   - inserts a row into the `outbox(op, node_id, payload, status='pending')` table.
3. Commit. **From this moment the operation must be driven to completion.**
4. The executor reads the outbox row → invokes the corresponding backend action (`Commit` = `rename` staging → target path for `FlatFile`).
5. Final transaction: `nodes.status='ready'` + archiving of the outbox row into `outbox_history`.

**Crash recovery.** On startup the process reads `outbox WHERE status IN ('pending', 'in_progress')` and replays. Each executor is idempotent — it checks the current state via `FileBackend.Stat` and brings it to the target. Re-running the same outbox row is safe.

**Sweeper.** A background task picks up stuck operations (expired `locked_until`), escalates `failed` after exhausting retry attempts (archive with `final_status='failed'` + alert).

### Option B — XA / 2PC distributed transaction

PostgreSQL supports prepared transactions (`PREPARE TRANSACTION`). One could build 2PC between the DB and a custom filesystem transaction manager.

**Downsides:**
- PostgreSQL 2PC is disabled by default (`max_prepared_transactions = 0`); enabling it requires tuning and incurs a penalty.
- A custom transaction manager is needed for the filesystem side, which must durably store prepared state itself.
- Crash recovery still requires the operator to manually resolve in-doubt transactions, or a timeout-based abort with loss.
- Complexity grows multiplicatively with the number of backends and protocols — each must correctly participate in the 2PC protocol.

### Option C — Write-ahead log in the DB, on-disk fix as a second step, no outbox

An analogue of outbox without an explicit table — the state is derived from `nodes.status='pending'` + heartbeats. Recovery is a walk over `nodes WHERE status='pending'`.

**Downsides:**
- A pending state carries no payload (upload_id, target_ref, acl_snapshot, etc.) — it would have to be hidden inside `nodes` or duplicated in `node_meta`. Semantic confusion: `nodes` ends up describing both the past and the intent.
- Hard to extend to operations not tightly tied to a single node (mass trash, ACL rebalancing) — there is no place to put the payload.

## Decision outcome

**Chosen: Option A — transactional outbox + idempotent re-play.**

Rationale:
- A single file — `internal/storage/dbfs/outbox.go` — implements the mechanism. That is compact and testable.
- The `outbox` table is hot and small. It moves into `outbox_history` (partitioned by month) after completion. Reads are cheap.
- Crash recovery is fully automatic, no manual intervention — `fs.RecoverPending` is called in `serve.go` at startup and runs in seconds.
- Minimum dependencies: PostgreSQL, which we have anyway.

## Consequences

### Positive
- No prepared transactions, no external coordinators.
- Every operation is fully described by `(op, payload)` — reads well, tests well, serializes neatly into audit.
- Idempotency makes safe retries possible without fear of "doing it twice".
- Adding a new op is trivial — a new executor + a new payload variant.
- Behaviour under load is predictable: the outbox is a queue, queue depth is measurable, slow operations become visible immediately.

### Negative
- Each mutation is two transactions instead of one (commit-stage + finalize). On SSD this is microseconds; on networked storage it might be noticeable, but storman is single-host.
- Sweeper and executor logic is non-trivial code. If it breaks, deferred operations simply get stuck — metrics on outbox depth and alerts are required.
- `outbox_history` partitions need to be created ahead of time by cron (see ROADMAP/Next) — forget to create them → new rows fall into the default partition and may fail.

### Neutral
- `outbox` is a table. That is fine; alternatives (Redis/Kafka) would have added an external dependency for no win at single-host scale.

## Related

- Implementation: [internal/storage/dbfs/outbox.go](../../internal/storage/dbfs/outbox.go), [internal/storage/dbfs/recover.go](../../internal/storage/dbfs/recover.go), [internal/storage/dbfs/trash.go](../../internal/storage/dbfs/trash.go).
- Architecture: [docs/arch/storage.md](../arch/storage.md).
- Depends on [ADR-0001 FileSystem boundary](0001-filesystem-boundary.md) — the outbox invariant only holds if all mutations go through `dbfs`.
