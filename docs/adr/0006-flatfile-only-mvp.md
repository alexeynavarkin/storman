---
id: ADR-0006
title: MVP ships only the FlatFile backend; CDC is deferred
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0006: MVP ships only the `FlatFile` backend; CDC is deferred

## Context and problem statement

The `storage.FileBackend` abstraction presumes several implementations:

- **`FlatFile`** — the user file sits on disk as-is, `BackendRef` = relative path in `<storage-dir>`.
- **`CDCFile`** (deferred) — content-defined chunking (FastCDC), chunk-level deduplication, a manifest per file. `BackendRef` = manifest id, content in `<meta>/blobs/`.

CDC offers meaningful advantages: cheap delta transfer (rsync-style), deduplication (identical chunks across files are stored once), linear storage growth with many versions, readiness for remote blob storage. But this functionality is not required in the first version.

Question: what should be done in MVP — only `FlatFile`, or both `FlatFile + CDCFile` with a per-directory choice?

## Decision drivers

- **Time-to-MVP.** Every month before the first working MVP is a cost. CDC is complex engineering (FastCDC, refcount-GC, copy-on-write staging, scrub mechanism).
- **Architectural foresight.** The `FileBackend` interface must support both options without rewriting. That influences the design even if there is only one implementation.
- **Purity of the storage layer.** Honest storage (see MISSION) — a user can point Time Machine at `flat-storage/` and get a clean archive. CDC breaks that.
- **Use-case fit.** Is deduplication needed at personal scale? A single user has tens of thousands of files with few duplicates.

## Considered options

### Option A — FlatFile-only in MVP, CDC deferred (chosen)

- MVP implements only `flat.FlatFile`.
- The `FileBackend` interface is designed to support `CDCFile` without modification (see [docs/arch/storage.md](../arch/storage.md)): `BackendRef.Data` is opaque, `Allocate` takes an `AllocHint`, `OpenWrite` supports a `Modify` mode for future lazy fetch.
- `nodes.backend_kind` is currently always `'flat'`, but the column is nullable on folders (policy for new children).
- CDC moves into [ROADMAP/Later](../ROADMAP.md), including the migration mechanism between backends (`storman migrate --to=cdc <path>`).

### Option B — FlatFile + CDC both in MVP

Implement both backends at once, per-directory choice via `nodes.backend_kind = 'cdc'` on a folder, inherited by descendants.

### Option C — CDC only, no FlatFile

The most minimalistic interface — one backend. All files are automatically chunked and deduplicated.

## Decision outcome

**Chosen: Option A — FlatFile-only in MVP.**

Rationale:
- **FlatFile = honest storage.** This is part of storman's value proposition (see MISSION). Time Machine / restic / rsync see user files directly as-is.
- **MVP is already big.** 5 delivery interfaces, async indexing, RBAC, trash, audit, backup/recover. Adding CDC would double the MVP scope.
- **Use-case fit.** Deduplication wins on repetitive data (backups, versioned archives). For a single-user personal-scale storman the main workload is personal photos/documents. The dedup effect is marginal.
- **The architecture is laid out.** The `FileBackend` interface is already designed for CDC (Allocate, opaque BackendRef, WriteMode.Modify). Adding CDC = a new `cdc.go` file, no changes in existing protocols or dbfs.
- **CDC is not free.** A random read through manifest → binary search → pread on a chunk is more expensive than FlatFile in the typical case. Deduplication is only valuable with significant overlap.

Option C (CDC-only) is rejected because of the honest-storage principle. Option B is premature optimization: CDC can be added later without migrating existing files (the backend is per-file, not per-database).

## Consequences

### Positive
- MVP is finished in a reasonable time.
- The user sees their files as files. Backups with standard tools work.
- Fewer moving parts, less bug surface.
- `FlatFile` is atomic via `tmp + fsync + rename` on one filesystem — a proven simple pattern.

### Negative
- Duplicate files are stored twice. For typical personal-storage scale — OK.
- No cheap delta transfer (rsync through storman, sync with an iPhone app, etc.).
- If CDC is urgently needed — migration of existing FlatFile nodes to CDC is not implemented (it is in [ROADMAP/Later](../ROADMAP.md)).

### Neutral
- The `<meta>/blobs/` directory is reserved in the layout (see [docs/arch/overview.md](../arch/overview.md)) for future CDC. Currently not created.
- The `nodes.backend_kind` column already carries dual semantics (immutable on files, policy on folders) — it works today, just with all values being `'flat'`.

## When to reconsider

Triggers for activating [ROADMAP/Later → CDC](../ROADMAP.md):
- The appearance of an rsync / FUSE scenario with large files where random write makes FlatFile inefficient (a shallow-write through chunked storage is required).
- Growing disk footprint from duplicates (not yet observed in real deployments — single user).
- File versioning (see [ROADMAP/Later](../ROADMAP.md) — CDC makes this near-free).
- Remote blob storage (S3): chunks go to S3, manifests stay local.

## Related

- Implementation: [internal/storage/flat/](../../internal/storage/flat/).
- Architecture: [docs/arch/storage.md](../arch/storage.md).
- ROADMAP: [Later → CDC backend](../ROADMAP.md).
- CDC sketch (what we expect to build when we pick it up): `git show HEAD~:PLAN.md` (the file was removed in the same commit; the "5.1 CDC backend" section is preserved in git history as the original design).
