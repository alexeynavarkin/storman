# Risk register

_Last updated: 2026-05-11. Reviewed monthly; top-3 weekly._

Scales: **Likelihood** and **Impact** — 1–5. **Score** = L × I. Categories: `durability`, `security`, `availability`, `consistency`, `operational`.

## Active risks

| ID | Risk | L | I | Score | Category | Mitigation | Contingency | Owner | Trigger | Status | Last reviewed |
|---|---|---:|---:|---:|---|---|---|---|---|---|---|
| R-001 | The backup interval (default 24h) gives RPO=24h. Losing the DB between dumps = losing the last day of metadata. | 2 | 4 | 8 | durability | `pg_dump` every 24h into `<meta>/backups/`; retention 7. The operator can lower `backup.interval`. | `recover --from-disk` restores content with sha256 + owner-only ACL. Extended metadata is lost (see [backup-dr.md](arch/backup-dr.md)). | alexnav | — | accepted | 2026-05-11 |
| R-002 | Out-of-band changes (SSH, rsync directly into `flat-storage/`) → DB-disk divergence. | 3 | 3 | 9 | consistency | `fsck` scanner in [ROADMAP/Next](ROADMAP.md). Document "trust the DB as the source of truth". | On reconcile, orphan files are moved into `<meta>/quarantine/`. A DB row without a file → `status='broken'`. | alexnav | a user sees a "lost" file | mitigating | 2026-05-11 |
| R-003 | WebDAV in-memory LockSystem (single-node only). On multi-process / client failover, locks are lost. | 2 | 2 | 4 | availability | Currently a single-node deployment — match. Persistent LockSystem in [ROADMAP/Next](ROADMAP.md). | Concurrent-edit conflicts surface to the client as 412 Precondition Failed. | alexnav | move to multi-node | accepted | 2026-05-11 |
| R-004 | Partitions for `outbox_history` / `jobs_history` / `audit_log` are pre-created by the migration 3 months ahead. A cron for auto-creating future partitions is NOT implemented. | 3 | 5 | 15 | operational | Migration v1 creates current + 3 months. **TODO in [ROADMAP/Next](ROADMAP.md)** — an in-process cron for auto-creation. | Manual SQL to create a partition; INSERTs into `outbox_history` / `audit_log` will fail until the partition is created. | alexnav | the date approaches the end of the prepared partitions | open | 2026-05-11 |
| R-005 | Compromising an app-password = compromising the user across all non-web protocols (no per-protocol scope). | 2 | 3 | 6 | security | Brute-force counters are shared with web logins. Argon2id hashes. UI for CRUD + revocation. | Revoke via UI (`DELETE FROM app_passwords`). Per-protocol scope is a backlog item in [ADR-0004 § Future evolution](adr/0004-app-passwords-cross-protocol.md). | alexnav | device/app leak | accepted | 2026-05-11 |
| R-006 | DB password / DSN and secrets_key are stored in `config.json` (mode 0600). Leaking the file = leaking the keys. | 2 | 4 | 8 | security | A mode-0600 check at startup. `secrets_key` is needed to decrypt TOTP secrets (once 2FA lands). | Rotation: a new `secrets_key` via `init --rotate-secrets-key`, re-encrypt TOTP. | alexnav | — | accepted | 2026-05-11 |
| R-007 | `pg_dump` in Docker via `docker exec` has a gotcha: `--file=-` is broken, stdout pipe is required. | 1 | 3 | 3 | operational | Implemented without `--file=-` (see [backup-dr.md](arch/backup-dr.md)). A regression would surface as an empty dump. | Manual `docker exec ... pg_dump` from a terminal. | alexnav | dev setup with Docker PG | mitigated | 2026-05-11 |
| R-008 | Single-host deployment: hardware failure = service outage until the operator restores. | 3 | 4 | 12 | availability | RAID on the host (operator responsibility). Regular `pg_dump` + backup of `<meta>/backups/` off-host. | Restore on a new host via `recover --from-backup` + content from the off-host backup. | alexnav | — | accepted | 2026-05-11 |
| R-009 | Disk-walk recovery (`--from-disk`) recomputes sha256 for every file — linear time in volume. On large stores — hours/days. | 3 | 2 | 6 | operational | Parallel walker (TODO). Chunked processing. | Acceptable for a DR scenario (this is the last line). | alexnav | — | accepted | 2026-05-11 |
| R-010 | Direct Postgres access via DSN from the config (suid/sudo on the host) bypasses RBAC and storman audit. | 2 | 5 | 10 | security | Document: the storman process must run under a dedicated user, and the DSN must be inaccessible to other users. | The audit log alone allows detecting anomalies post-factum. | alexnav | misconfig | accepted | 2026-05-11 |
| R-011 | Overwriting an existing file via WebDAV PUT is implemented as trash-then-create — the old version accumulates in trash until GC. With frequent rewrites the trash grows. | 3 | 1 | 3 | operational | Trash GC (default retention 30 days) cleans old entries. | Manual purge via the `/api/trash` admin UI. | alexnav | — | accepted | 2026-05-11 |
| R-012 | The ACL Cache in RBAC must invalidate correctly on move/rename. A bug = stale permissions = security incident. | 2 | 5 | 10 | security | Epoch-based invalidation per subtree (see [rbac.md](arch/rbac.md)). Invalidation tests (`internal/rbac/permissions_test.go`). | A process restart = empty cache, correct rebuild. | alexnav | regression bug | mitigating | 2026-05-11 |

## Risk review cadence

- **Weekly** — top-3 by score (currently: R-004, R-008, R-012). If the score does not change for 3 reviews in a row — close or revise the mitigation.
- **Monthly** — the full register, additions/closures.

## Closed / out-of-scope risks

Risks with `status: closed` will move here.

## Anti-pattern reminders

- A risk without a mitigation is a **wish**, not a risk. Either write a mitigation, or close it as "accepted".
- A risk without an owner is one nobody is watching. The owner is the only persona right now (`alexnav`).
- "`Likelihood: 5, Impact: 5, Mitigation: TBD`" is security theatre. The score must rest on a concrete mitigation.

## Related

- [MISSION.md](MISSION.md) — the north-star and principles risks aim at.
- [ROADMAP.md](ROADMAP.md) — where mitigations are planned.
- [ADR-0002 Outbox](adr/0002-outbox-fs-db-atomicity.md), [ADR-0005 DR](adr/0005-dr-pg-dump-plus-disk-walk.md) — load-bearing decisions on durability.
