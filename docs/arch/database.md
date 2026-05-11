# Database schema

PostgreSQL is the only external dependency of storman. The choice is justified by:

- Concurrent writers (Web + FTPS + WebDAV + indexing workers simultaneously).
- JSONB + GIN for extensible metadata.
- `ltree` for tree queries (materialized path).
- Partitioning of history (`outbox_history`, `jobs_history`, `audit_log`).
- `citext` for case-insensitive logins.
- `pgcrypto.gen_random_uuid()` for primary keys.

Connection — `pgx/v5 + pgxpool`. Migrations — embedded SQL via `golang-migrate`, files under [internal/migrations/sql/](../../internal/migrations/sql/), zero-padded prefix.

## Key tables

### `nodes` — file and folder tree

```sql
id            uuid primary key                  -- stable node ID, does not change on rename/move
parent_id     uuid references nodes(id)        -- parent folder; NULL only for the root
path          ltree not null                    -- materialized path 'root.docs.report'; gist index for subtree queries
name          text not null                     -- node name within parent (last segment of path)
type          node_type not null                -- enum: 'file' | 'dir'
backend_kind  text                              -- dual semantics: on a file — the actual backend (NOT NULL, immutable);
                                                --                 on a folder — policy for NEW children (NULL = inherit)
backend_ref   text                              -- backend-specific: for 'flat' — relative path in flat-storage/
                                                --                    for 'cdc'  — manifest id (future)
size          bigint                            -- bytes; NULL for folders and pending files; populated on Commit
mime          text                              -- detected by magic bytes (NOT from the client's Content-Type)
mtime         timestamptz                       -- mtime of the content; updated on each successful Commit
sha256        bytea                             -- 32 bytes; duplicated from node_meta for fast lookup
status        node_status not null              -- enum: 'pending' | 'ready' | 'deleted' | 'broken'
created_at    timestamptz default now()
updated_at    timestamptz default now()
deleted_at    timestamptz                       -- moment the node entered the trash; trash GC uses this field
unique (parent_id, name)

check (type <> 'file' or (backend_kind is not null and backend_ref is not null))
check (type <> 'dir'  or backend_ref is null)
```

**Indexes:**
- `gist(path)` — ancestor/descendant, `path <@ root` for subtree operations (trash, delete, ACL traverse).
- `btree(sha256)` — duplicate search.
- `btree(parent_id)` — listing.
- `partial btree(deleted_at) WHERE deleted_at IS NOT NULL` — trash GC.

**Statuses:**
- `pending` — upload in progress, the file is not yet at the target path (`backend_ref` points to staging or the future location).
- `ready` — the file is published, content is in place.
- `deleted` — soft-delete, the node is in the trash; `deleted_at` is non-NULL.
- `broken` — a recovery flag: metadata exists, content does not (set by `fsck`).

### `node_meta` — typed frequent fields + JSONB

```sql
node_id      uuid primary key references nodes(id) on delete cascade
width        int                              -- image/video width in pixels
height       int
taken_at     timestamptz                      -- EXIF DateTimeOriginal (≠ nodes.mtime for imported)
lat          double precision                 -- GPS from EXIF (-90..90)
lng          double precision                 -- GPS from EXIF (-180..180)
duration_ms  int                              -- video/audio duration
extra        jsonb not null default '{}'      -- rare/extensible fields by namespace ('exif.iso', 'ai.faces')
```

**Indexes:**
- `gin(extra jsonb_path_ops)` — search over arbitrary JSONB fields.
- `btree(taken_at)` — photo gallery sort by shoot time.
- `btree(lat, lng)` — bounding-box queries (PostGIS optional).

**Principle:** frequently queried fields go into typed columns (indexable, range queries); rare/custom ones into the `extra` JSONB. Moving from JSONB to a column is a migration once a field becomes hot.

### `permissions` — explicit grants only (RBAC)

```sql
id         bigserial primary key
node_id    uuid references nodes(id) on delete cascade
user_id    uuid references users(id) on delete cascade
actions    bit(8) not null                  -- bitmask: bit 0=Read, 1=Write, 2=Remove, 3=Admin; 4-7 reserved
created_at timestamptz default now()
unique (node_id, user_id)
```

Virtual `Traverse` is **not stored** here — it is computed on the fly (see [ADR-0003](../adr/0003-rbac-computed-traverse.md), [rbac.md](rbac.md)).

### `share_links` — anonymous temporary links (Web only)

```sql
token       text primary key                 -- 32 bytes crypto/rand → base64url; PK for O(1) lookup
node_id     uuid references nodes(id) on delete cascade
actions     bit(8) not null                  -- same bits as in permissions; Admin bits are rejected by code
expires_at  timestamptz not null             -- mandatory field — there are no perpetual share-links
max_uses    int                              -- NULL = no limit; otherwise used_count < max_uses
used_count  int default 0                    -- incremented in Auth on successful validation
created_by  uuid references users(id)
created_at  timestamptz default now()
```

### `node_settings` — per-node settings (applicable to files and folders)

Fields are inherited via ltree lookup of the nearest ancestor (or the node itself) with a non-NULL value.

```sql
node_id          uuid primary key references nodes(id) on delete cascade
max_versions     int                         -- ROADMAP/Later: versioning
retention_days   int                         -- ROADMAP/Later: version TTL
version_policy   text                        -- ROADMAP/Later: 'off' | 'on_change' | ...
created_at       timestamptz default now()
updated_at       timestamptz default now()
```

`storage_backend` is NOT moved here — it lives directly in `nodes.backend_kind`.

### `jobs` — hot queue of async indexing

```sql
id           bigserial primary key
node_id      uuid not null references nodes(id) on delete cascade
kind         text not null                   -- 'hash' (in MVP); 'exif' | 'mime' | 'face' — ROADMAP/Next
status       text not null                   -- 'pending' | 'in_progress' | 'failed'
                                              -- (terminal 'done' and final 'failed' move to jobs_history)
attempts     int default 0
last_error   text
locked_until timestamptz                     -- worker lease; the sweeper picks up stuck rows
created_at   timestamptz default now()
updated_at   timestamptz default now()
```

Workers pick up jobs through `SELECT ... FOR UPDATE SKIP LOCKED WHERE status='pending' AND kind=$1 ORDER BY created_at LIMIT N`. Indexes: `btree(kind, status, created_at)`, `btree(node_id)`. The table stays hot and small — terminal rows move to `jobs_history`.

### `jobs_history` — append-only archive of finished jobs

```sql
id            bigint                          -- the same id that was in jobs (not PK after partitioning)
node_id       uuid                            -- no FK: history survives node deletion
kind          text not null
final_status  text not null                   -- 'done' | 'failed'
attempts      int not null
last_error    text                            -- NULL for 'done'
enqueued_at   timestamptz not null            -- former jobs.created_at
finished_at   timestamptz not null default now()
) partition by range (finished_at);           -- monthly partitions; old ones dropped via DROP PARTITION
```

Indexes per partition: `btree(node_id, finished_at)`, `btree(kind, final_status)`. Retention is configurable.

### `outbox` — journal of unfinished FS operations

See [storage.md § Outbox](storage.md#outbox-fs--db-atomicity), [ADR-0002](../adr/0002-outbox-fs-db-atomicity.md).

```sql
id           bigserial primary key            -- insertion order = attempt order (FIFO within one executor)
op           text not null                    -- 'create_file' | 'trash' | 'rename' | 'migrate_backend' | ...
node_id      uuid                             -- nullable: some operations (GC) are not tied to a single node
payload      jsonb not null                   -- everything needed for idempotent execution: upload_id, backend_ref before/after, …
status       text not null                    -- 'pending' | 'in_progress' | 'failed' (terminals go to outbox_history)
attempts     int default 0
created_at   timestamptz default now()
locked_until timestamptz                      -- lease; expired = executor died, the row can be picked up
```

**Index:** `partial btree(status, locked_until) WHERE status IN ('pending','in_progress')` — for fast picking of active rows. The table is hot and small by construction; all history is in `outbox_history`.

### `outbox_history` — append-only archive of FS operations

```sql
id            bigint
op            text not null
node_id       uuid                            -- no FK: the row outlives node deletion
payload       jsonb not null
final_status  text not null                   -- 'done' | 'failed'
attempts      int not null
last_error    text                            -- NULL for 'done'
enqueued_at   timestamptz not null
finished_at   timestamptz not null default now()
) partition by range (finished_at);
```

Indexes: `btree(node_id, finished_at)`, `btree(op, final_status, finished_at)`.

### `audit_log` — append-only journal of security events

```sql
audit_log (
  id        bigserial primary key,
  ts        timestamptz default now(),       -- partition key (monthly)
  user_id   uuid,                             -- initiator; NULL for system events (cron, GC, sweeper)
  action    text not null,                   -- 'login' | 'login_failed' | 'logout' | 'acl_change' | 'share_*'
                                              -- | 'upload' | 'delete' | 'rename' | 'mkdir' | 'trash_*' | 'backup' | 'recover'
  node_id   uuid,                             -- for FS events; NULL for non-FS (login, etc.)
  ip        inet,                             -- NULL for system
  result    text not null,                    -- 'ok' | 'denied' (ACL/rate-limit) | 'error'
  details   jsonb                             -- arbitrary context: 'acl_change' — diff; 'share_use' — token id; …
) partition by range (ts);
```

Indexes per partition: `btree(user_id, ts)`, `btree(node_id, ts)`, `btree(action, ts)`. Retention is configurable (default 12 months).

### `users`, `sessions`, `app_passwords`

See [auth.md](auth.md).

## Partitioning of history tables

`outbox_history`, `jobs_history`, `audit_log` are partitioned by month (`PARTITION BY RANGE`).

Migration v1 creates partitions for the current month + 3 ahead. **A cron for auto-creating future partitions is NOT implemented**; it lives in [ROADMAP/Next](../ROADMAP.md). Until then the operator must ensure partitions do not run out — otherwise INSERTs will fail.

Dropping old partitions — `DROP PARTITION` (O(1)), not `DELETE` (which loads autovacuum and does not free space).

## Extensions

In use:
- **`ltree`** — `nodes.path`, gist index, subtree queries (`path <@`, `path @>`).
- **`citext`** — `users.login` (case-insensitive unique).
- **`pgcrypto`** — `gen_random_uuid()` for UUID PKs.

All three are enabled in the migration `0001_extensions.up.sql`.

## The root node

It is **not created** in migrations — that is a business operation, performed by `DBFS.Bootstrap(ctx)` on the first process start. Idempotent (NOOP if the root already exists).
