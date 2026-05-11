---
id: ADR-0001
title: All delivery protocols work only through storage.FileSystem
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0001: All delivery protocols work only through `storage.FileSystem`

## Context and problem statement

storman has five delivery interfaces (Web API, FTPS, WebDAV, tus.io, share-links) and several more on the roadmap (FUSE, S3-compat). Each one is a separate package under `internal/`. All of them mutate the same state: the node tree in PostgreSQL and files on disk under `flat-storage/`.

These two stores have no shared transactions. Atomicity is provided by the outbox pattern ([ADR-0002](0002-outbox-fs-db-atomicity.md)), and the integrity of invariants (ACL checked before mutation, trash as a single logical operation, the indexing hash-job sitting in the same transaction as publishing the file, audit emitted exactly around the mutation) — by a single shim over those stores. That shim is the `internal/storage/dbfs/` package, which implements the `storage.FileSystem` interface.

Question: must delivery protocols strictly go only through this interface, or are "shortcuts" allowed (direct `os.*` calls against `flat-storage/`, direct SQL against `nodes`/`outbox`, direct use of `FileBackend`) for the sake of individual operations' performance?

## Decision drivers

- **Durability** — corruption / loss of files is impossible (north-star from MISSION). A DB ↔ disk split breaks that requirement.
- **Security** — ACL is checked on every operation; a shortcut is a path where the check is easy to forget.
- **Audit completeness** — every mutation must leave a record. A backdoor path = silent mutation.
- **Single source of truth** — indexing (hash-job), trash, recovery are built on the assumption that any mutation goes through one place. A parallel path breaks that assumption.
- **Cognitive load** — N protocols × 5 overlapping invariants = N×5 places where something can be forgotten. One shim reduces it to 1×5 = 5 checks in code review.
- **Speed of individual operations** — a shortcut is sometimes faster (native COPY via `os.Link` vs read+write via `OpenFile`).

## Considered options

### Option A — strict invariant (chosen)
All delivery protocols use **only** the `storage.FileSystem` interface. No protocol handler may:
- call non-interface methods on `dbfs.DBFS` for optimization;
- import a concrete `FileBackend` (`flat`, future `cdc`) and work with it directly;
- do `os.*` against `flat-storage/` or `INSERT/UPDATE` against `nodes` / `outbox` bypassing `dbfs`.

Functional extensions (e.g. native subtree COPY, server-side dedup, fast move between backends) — by **adding a method to `FileSystem`** and implementing it in `dbfs` in a single transaction + a corresponding operation on `FileBackend`. An HTTP handler may intercept a request **before** the protocol library (`webdav.Handler`) so it can dispatch correctly into the new `FileSystem` method — but not for direct disk or DB work.

Exception: `dbfs` exposes non-interface methods for admin/service tasks (`ListTrash`, `Restore`, `Purge`, `RecoverPending`, `StartGC`, `ImportPath`). They are allowed for admin and recovery features that `FileSystem` does not model by design. This still goes through `dbfs` — not around it.

### Option B — convention-based, not enforced
Same as A, but described as "best practice". Shortcuts are allowed with a promise to encapsulate them in one helper that will eventually share the common infrastructure (audit, ACL).

### Option C — laissez-faire, optimize on the spot
Each protocol chooses for itself. The storage layer documents the "right" paths; a protocol may cut a corner if profiling shows it is critical.

## Decision outcome

**Chosen: Option A.**

Rationale:
- The target class of work is durable file storage. The worst thing that can happen is silent data loss or silent ACL bypass. A strict boundary = a single place to enforce.
- N=5 delivery interfaces is already a large fan-out. Any convention without enforcement (Option B) erodes in code review over time — one rushed PR, and the invariant is broken without a trace.
- The cost of a strict invariant is that operations needing a native fast path must come via interface extension. This is **more work once**, but the feature becomes available to all protocols at once (e.g. native COPY is available to WebDAV, Web API, and any future S3 facade).
- The performance penalty is observable — WebDAV COPY really is slow (see ROADMAP/Next about `FileSystem.Copy`). But this is a **manageable** problem: add the method, implement it once — and the fix propagates.

## Consequences

### Positive
- A single control seam for durability, ACL, audit, indexing, trash.
- Testability: FS tests are written once, valid for all protocols.
- A new protocol plugs in as a thin adapter without recreating infrastructure.
- Code review is predictable: "the protocol handler imports `internal/storage/flat`" — an automatic red flag.

### Negative
- Native fast paths require an interface extension — a heavier change than ad-hoc handler optimization.
- Some library-side operations decompose into many small `FileSystem` calls (WebDAV COPY — N transactions for a tree of N files; see ROADMAP/Next).
- The temptation to "bypass just once" comes up sometimes — discipline in code review is required.

### Neutral
- `dbfs` grows non-interface methods (`ListTrash`, `RecoverPending`, etc.) — acceptable as long as they cover admin/recovery use-cases that the main interface does not model by design.
- Some optimizations (CDC native dedup, native COPY between backends) are deferred until we are ready to extend the interface "the right way" rather than the fast way.

## Validation

Violations caught in code review:

- A protocol handler (`internal/web/`, `internal/ftpsrv/`, …) imports `internal/storage/flat` or another backend.
- A handler does `tx.Exec("INSERT INTO nodes …")` or `tx.Exec("INSERT INTO outbox …")` bypassing `dbfs`.
- A handler does `os.Open` / `os.Create` / `os.Rename` against paths inside `flat-storage/`.
- A function appears that takes both a `*pgxpool.Pool` and a filesystem path — almost always a shortcut.

Principle of "how to add optimizations": extend the interface, do not bypass it. If the extension is too invasive — that is a signal either that the optimization is not that important, or that `FileSystem` is due for a revisit.
