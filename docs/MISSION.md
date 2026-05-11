# storman

**Mission.** Enterprise-grade self-hosted personal file storage: cloud UX, full control over your data, simple operations. A single Go binary + PostgreSQL — no other external dependencies.

**North Star.** Durability — corruption or loss of a user file is impossible. Process crash, host power loss, concurrent writers, overwrite — none of these scenarios leaves a node in the DB without a file on disk, or a file on disk without a node in the DB.

## Principles (do not relax)

- **Storage is durable and well-tested.** DB↔disk consistency is provided by the outbox pattern with crash recovery on process startup (see [ADR-0002](adr/0002-outbox-fs-db-atomicity.md)).
- **Delivery interfaces are secure by default.** Unauthorized access is impossible. TLS is mandatory (HTTPS for Web, FTPS for FTP). Plain FTP is disabled at the listener level.
- **ACL is always checked on the backend** (`rbac.PermissionService.Check`). UI/client is untrusted — all security checks happen on the server.
- **All delivery protocols go through `storage.FileSystem`** — the single point where outbox, ACL, trash, indexing, and audit converge. Bypasses are forbidden (see [ADR-0001](adr/0001-filesystem-boundary.md)).
- **Honest storage.** User files lie on disk as-is. No `.meta.json` next to files. Metadata lives in the DB. A Time Machine / restic backup of `flat-storage/` yields a clean archive without service clutter.

## Bets (current period)

1. **MVP with five delivery interfaces** — Web API, FTPS, WebDAV, tus.io resumable uploads, share-links. **Achieved (2026 Q2).**
2. **Async indexing pipeline.** Hash worker — in MVP. EXIF / MIME / face recognition workers — in [Next](ROADMAP.md).
3. **Operational hardening.** pg_dump + disk-walk DR ([ADR-0005](adr/0005-dr-pg-dump-plus-disk-walk.md)), audit log, login rate limiting, trash with retention GC.

## Anti-bets (what we deliberately do not do)

- **Multi-tenant / multi-node.** storman is single-user, single-host. We do not build a cluster.
- **A custom DB / custom block store.** PostgreSQL is a justified choice (concurrent writers, JSONB, ltree, partitioning) — we do not write an alternative.
- **Custom WebDAV / FTPS server implementations.** We use `golang.org/x/net/webdav` and `fclairamb/ftpserverlib`. Our own code — only adapters to `storage.FileSystem`.
- **Anonymous access over any protocol other than Web.** Share-links are available only over HTTP. FTPS / WebDAV — only named users with app-passwords.
