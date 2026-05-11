---
id: ADR-0003
title: Traverse permissions are computed virtually, not stored in the DB
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0003: Traverse permissions are computed virtually, not stored in the DB

## Context and problem statement

storman's RBAC model is `User → Resource → Action` with a `Read|Write|Remove|Admin` bitmask. The node tree (ltree). The `Traverse` permission (the right to pass through a node `P` to reach a descendant `D` where the user has an explicit grant) is necessary — otherwise no path through the tree can be built.

Question: how should `Traverse` be granted?
- Stored in `permissions` as explicit rows on every ancestor of a granted node — the materialized approach.
- Computed on the fly: "is there at least one explicit grant for this user on a descendant of node `N`" — the virtual approach.

The cost of materialization: every `INSERT INTO permissions(node_id=X, user_id=U, …)` requires inserting N traverse rows into each of `X`'s N ancestors. When moving a node, traverse rows of the ancestors of the old and new locations must be updated. On user deletion — a cascade. The size of `permissions` grows as N × M (N = depth, M = number of grants).

The cost of computation: `SELECT EXISTS (SELECT 1 FROM permissions p JOIN nodes n ON n.id=p.node_id WHERE p.user_id=$1 AND n.path <@ $2)` — an ltree query with a gist index, O(log N).

## Decision drivers

- **Correctness** — it is impossible to forget to update traverse rows on rename/move/delete.
- **Simplicity** — less hidden state = fewer bugs.
- **Performance** — O(log N) ltree lookup is acceptable for personal scale (a single user, up to tens of thousands of nodes).
- **Size of `permissions`** — the table is read by the ACL cache, so it is reasonable to keep it compact.
- **Read latency** is critical; write latency may be slightly higher.

## Considered options

### Option A — Computed Traverse (chosen)

`permissions` stores **only explicit** rights (`Read`/`Write`/`Remove`/`Admin`). Traverse is never materialized.

The effective mask for `(user, node)` is computed as follows:

1. **Down inheritance.** Using `nodes.path` (ltree), find the ancestor chain. Take the nearest ancestor (or the node itself) with an explicit row in `permissions` for this user. This yields effective `Read/Write/Remove/Admin`.
2. **Up traverse.** If the user has an explicit grant on any **descendant** of node `N` — add a virtual `Traverse` to the effective mask for `N`. Query: `SELECT EXISTS (SELECT 1 FROM permissions p JOIN nodes n ON n.id=p.node_id WHERE p.user_id=$1 AND n.path <@ $2)`.
3. The result is cached in the ACL Cache.

`Traverse` is an enum-bit in code (`rbac.Traverse = 1<<4`), but `permissions.actions bit(8)` stores only bits 0-3.

### Option B — Stored Traverse

On every `Grant` to a node `X`, an extra traverse row is inserted into `permissions` on each ancestor of `X` for the same user. On revoke / rename / delete — cascading synchronization.

### Option C — Hybrid: cache a traverse bit in `permissions`, invalidate on the fly

Store an additional bit in `permissions` — "this user has descendant grants under this node" — updated by a trigger. A hybrid with the worst properties of both: still materialization (must be invalidated), but the granularity of the bit is coarser.

## Decision outcome

**Chosen: Option A — computed Traverse.**

Rationale:
- `permissions` stays compact (only explicit grants).
- There is no class of bugs "forgot to update traverse on move" — because there is nothing to update.
- `ltree` + gist index makes ancestor/descendant lookup O(log N), which is enough for personal scale even without a cache.
- The ACL Cache (LRU + epoch-based invalidation per subtree, see [docs/arch/rbac.md](../arch/rbac.md)) makes repeat lookups practically free.

## Consequences

### Positive
- `permissions` is a table of explicit grants only; easy to audit "who was given what".
- Rename/move/delete of a node does not require cascading rewrites of traverse rows.
- User deletion is a simple `DELETE FROM permissions WHERE user_id=$1`, no additional cleanup.
- The RBAC code is simpler: a single `Effective(user, node)` function encapsulates the whole logic.

### Negative
- Every `Check(user, node, Traverse)` does an ltree `EXISTS` query (or hits the cache). On a cold cache — an extra DB roundtrip.
- For a very wide tree, descendant checks can be more expensive than a narrow ancestor walk; not important at storman scale, but for multi-tenant it would be reconsidered.
- The ACL Cache is critical for performance; if it breaks, DB load grows linearly.

### Neutral
- The `Traverse` bit (1<<4) lives only in code (`internal/rbac/`), not in the DB. That is OK; the alternative is to give it bit 4 in `permissions.actions`, but that would invite storing traverse rows.

## Related

- Implementation: [internal/rbac/](../../internal/rbac/).
- Architecture: [docs/arch/rbac.md](../arch/rbac.md).
- ACL cache invalidation — epoch-based per subtree; described in `docs/arch/rbac.md`.
