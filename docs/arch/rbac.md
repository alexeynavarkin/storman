# RBAC: permission model, inheritance, share-links

storman uses an ACL model `User → Resource → Action`, where Resource is a node in the tree (`nodes`) and Action is a bitmask. Full rationale for "computed Traverse" — [ADR-0003](../adr/0003-rbac-computed-traverse.md).

## Action bits

```go
const (
    Read     = 1 << 0   // bit 0 — read content / list directory
    Write    = 1 << 1   // bit 1 — create / write / mkdir
    Remove   = 1 << 2   // bit 2 — trash / purge
    Admin    = 1 << 3   // bit 3 — ACL changes, node_settings, backend_kind policy changes
    Traverse = 1 << 4   // bit 4 — computed (see below), never stored in DB
)
```

In the DB (`permissions.actions bit(8)`) — bits 0-3, bits 4-7 are reserved. `Traverse` lives **only in code** (`internal/rbac/`) and is computed on the fly.

## Storage of permissions

Table [`permissions`](database.md#permissions--explicit-grants-only-rbac):

```sql
permissions(id, node_id, user_id, actions bit(8), created_at)
unique (node_id, user_id)
```

We store **only explicit grants**. No `is_explicit`/`auto_granted` flags, no virtual rows in persisted state.

## Effective permissions for (user, node)

`rbac.PermissionService.Effective(ctx, userID, nodeID) → Mask` works like this:

1. **Check the ACL Cache.** LRU + epoch-based invalidation per subtree. Hit → return.
2. **Down inheritance.** Using `nodes.path` (ltree), find the ancestor chain. Take the nearest ancestor (or the node itself) that has an explicit row in `permissions` for this user. This yields effective `Read | Write | Remove | Admin`.
3. **Up traverse.** If the user has an explicit grant on any **descendant** of node `N` — add a virtual `Traverse` to the effective mask for `N`:
   ```sql
   SELECT EXISTS (
       SELECT 1 FROM permissions p JOIN nodes n ON n.id = p.node_id
       WHERE p.user_id = $1 AND n.path <@ $2
   )
   ```
   `<@` is the ltree "descendant-or-self" operator, using the gist index on `nodes.path`.
4. **Populate the cache** with the result and return.

`Check(ctx, userID, nodeID, want)` wraps `Effective`, returning `rbac.ErrDenied` if `!Has(mask, want)`.

## Cache invalidation

- **A change in `permissions`** (grant/revoke on a node) → invalidates the subtree `path <@ prefix`.
- **A node move/delete** → invalidates the subtree.
- **Epoch-based:** every change bumps the "epoch" of the prefix; cached entries with an older epoch are discarded lazily on read. No explicit cache walk — O(1) per mutation.

## Admin operations

The `Admin` bit in effective permissions is required for:

- Changing `permissions` (grant/revoke).
- Changing `node_settings`.
- Changing `nodes.backend_kind` on a folder (changing the backend policy).
- Viewing the trash (`/api/trash`) — root-admin only (a user with `Admin` on the root).
- Viewing the audit log (`/api/audit`) — root-admin only.

**Root-admin** = a user with `Admin` on the root node. Through ltree lookup that is inherited downward automatically, so root-admin can do anything.

## Share-links (Web only)

Anonymous temporary links with limited scope. HTTP/HTTPS only; FTP, WebDAV, FUSE and other protocols **do not** support anonymous access.

Table [`share_links`](database.md#share_links--anonymous-temporary-links-web-only):

```sql
share_links(token, node_id, actions bit(8), expires_at, max_uses, used_count, created_by, created_at)
```

**Token:** 32 bytes `crypto/rand` → base64url. Used as the table PK — `SELECT` by token is O(1) and constant-time (protection against timing attacks).

**Validation** (on every use):
1. The token exists.
2. `expires_at > now()`.
3. `max_uses IS NULL OR used_count < max_uses`.
4. The requested action is in the link's `actions`.

If OK — `UPDATE used_count = used_count + 1`, serve the content. An audit event on every use.

**Rate limiting per token** — protection against brute-force guessing. Configured in the global rate-limit settings.

**Scope restrictions:**
- `Admin` bits in a share-link are rejected by the Auth Service check (you cannot share the right to change ACLs).
- `actions = 0` is also invalid — there is no point in a link with no permissions.

## Creating a share-link

Endpoint `POST /api/share` accepts `{path, actions, ttl_seconds, max_uses?}`. Requirements:
- The user must have `Admin` on the node (only an owner/admin can share).
- The requested `actions` must be a subset of their own rights on the node.

On success returns `{token, url}`. The token is shown **once** — it cannot be retrieved later (revoke + new instead).

## Implementation

See [internal/rbac/](../../internal/rbac/) and [internal/auth/sharelinks.go](../../internal/auth/sharelinks.go). Tests — [internal/web/server_test.go](../../internal/web/server_test.go) (TestShareLink*, TestPerm*).
