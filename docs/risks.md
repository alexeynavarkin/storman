# Risk register

_Last updated: 2026-05-11. Reviewed monthly; top-3 weekly._

Шкалы: **Likelihood** и **Impact** — 1–5. **Score** = L × I. Категории: `durability`, `security`, `availability`, `consistency`, `operational`.

## Active risks

| ID | Risk | L | I | Score | Category | Mitigation | Contingency | Owner | Trigger | Status | Last reviewed |
|---|---|---:|---:|---:|---|---|---|---|---|---|---|
| R-001 | Backup interval (default 24h) даёт RPO=24h. Потеря БД между дампами = потеря последних суток метаданных. | 2 | 4 | 8 | durability | `pg_dump` каждые 24h в `<meta>/backups/`; retention 7. Оператор может уменьшить `backup.interval`. | `recover --from-disk` восстанавливает контент со sha256 + owner-only ACL. Расширенная мета теряется (см. [backup-dr.md](arch/backup-dr.md)). | alexnav | — | accepted | 2026-05-11 |
| R-002 | Изменения в обход системы (SSH, rsync напрямую в `flat-storage/`) → рассинхрон БД с диском. | 3 | 3 | 9 | consistency | `fsck`-сканер в [ROADMAP/Next](../ROADMAP.md). Документировать «trust BD as source of truth». | Orphan-файлы при reconcile перемещаются в `<meta>/quarantine/`. Запись в БД без файла → `status='broken'`. | alexnav | пользователь видит «потерянный» файл | mitigating | 2026-05-11 |
| R-003 | WebDAV in-memory LockSystem (single-node only). При multi-process / клиентском failover locks теряются. | 2 | 2 | 4 | availability | Сейчас single-node deployment — match. Persistent LockSystem в [ROADMAP/Next](../ROADMAP.md). | Конфликты при concurrent edit видны клиенту как 412 Precondition Failed. | alexnav | переход на multi-node | accepted | 2026-05-11 |
| R-004 | Партиции `outbox_history` / `jobs_history` / `audit_log` создаются миграцией наперёд на 3 месяца. Cron автосоздания будущих партиций НЕ реализован. | 3 | 5 | 15 | operational | Миграция v1 создаёт текущий + 3 месяца. **TODO в [ROADMAP/Next](../ROADMAP.md)** — внутрипроцессный cron для автосоздания. | Manual SQL для создания партиции; INSERT'ы в `outbox_history` / `audit_log` упадут пока партиция не создана. | alexnav | дата приближается к концу заведённых партиций | open | 2026-05-11 |
| R-005 | Компрометация app-password = компрометация пользователя по всем non-web протоколам (нет per-protocol scope). | 2 | 3 | 6 | security | Brute-force counters общие с web logins. Argon2id хеши. UI для CRUD + ревокация. | Ревокация через UI (`DELETE FROM app_passwords`). Per-protocol scope — backlog в [ADR-0004 § Future evolution](adr/0004-app-passwords-cross-protocol.md). | alexnav | утечка устройства/приложения | accepted | 2026-05-11 |
| R-006 | DB password / DSN, secrets_key хранятся в `config.json` (mode 0600). Утечка файла = утечка ключей. | 2 | 4 | 8 | security | Проверка mode 0600 на старте. `secrets_key` нужен для дешифровки TOTP-секретов (когда 2FA добавится). | Ротация: новый `secrets_key` через `init --rotate-secrets-key`, перешифровка TOTP. | alexnav | — | accepted | 2026-05-11 |
| R-007 | `pg_dump` в Docker через `docker exec` имеет gotcha: `--file=-` ломается, нужен stdout pipe. | 1 | 3 | 3 | operational | Реализовано без `--file=-` (см. [backup-dr.md](arch/backup-dr.md)). Регрессия будет видна как пустой dump. | Manual `docker exec ... pg_dump` через terminal. | alexnav | dev setup с Docker PG | mitigated | 2026-05-11 |
| R-008 | Single-host deployment: hardware failure = service outage пока оператор не восстановит. | 3 | 4 | 12 | availability | RAID на хосте (operator responsibility). Регулярный `pg_dump` + бэкап `<meta>/backups/` off-host. | Восстановление на новом хосте через `recover --from-backup` + content из off-host бэкапа. | alexnav | — | accepted | 2026-05-11 |
| R-009 | Disk-walk recovery (`--from-disk`) пересчитывает sha256 для всех файлов — линейное время по объёму. На больших хранилищах — часы/сутки. | 3 | 2 | 6 | operational | Параллельный walker (TODO). Чанковая обработка. | Acceptable для DR-сценария (это последняя линия). | alexnav | — | accepted | 2026-05-11 |
| R-010 | Прямой Postgres-доступ через DSN из config (suid/sudo на хосте) даёт обход RBAC и audit storman. | 2 | 5 | 10 | security | Документировать: storman process должен запускаться под отдельным user, DSN недоступен другим user'ам. | Audit log сам по себе позволяет detect-ить anomalies post-factum. | alexnav | misconfig | accepted | 2026-05-11 |
| R-011 | Перезапись существующего файла через WebDAV PUT реализована как trash-then-create — старая версия копится в trash до GC. На частых rewrite'ах trash растёт. | 3 | 1 | 3 | operational | GC корзины (default retention 30 дней) очищает старые записи. | Manual purge через `/api/trash` admin UI. | alexnav | — | accepted | 2026-05-11 |
| R-012 | ACL Cache в RBAC должен корректно инвалидироваться при move/rename. Баг = stale permissions = security incident. | 2 | 5 | 10 | security | Epoch-based invalidation per subtree (см. [rbac.md](arch/rbac.md)). Тесты на инвалидацию (`internal/rbac/permissions_test.go`). | Restart процесса = empty cache, корректная пересборка. | alexnav | regression bug | mitigating | 2026-05-11 |

## Risk review cadence

- **Weekly** — top-3 по score (сейчас: R-004, R-008, R-012). Если score не меняется 3 review подряд — закрыть или пересмотреть mitigation.
- **Monthly** — full register, добавление/закрытие.

## Closed / out-of-scope risks

Здесь будут переезжать риски с `status: closed`.

## Anti-pattern reminders

- Риск без mitigation — это **wish**, а не риск. Либо записать mitigation, либо закрыть как «accepted».
- Риск без owner — никто за ним не следит. Owner — единственный персонаж сейчас (`alexnav`).
- «`Likelihood: 5, Impact: 5, Mitigation: TBD`» — security theatre. Score должен опираться на конкретный mitigation.

## Related

- [MISSION.md](../MISSION.md) — north-star и принципы, на которые риски целятся.
- [ROADMAP.md](../ROADMAP.md) — где mitigation'ы планируются.
- [ADR-0002 Outbox](adr/0002-outbox-fs-db-atomicity.md), [ADR-0005 DR](adr/0005-dr-pg-dump-plus-disk-walk.md) — load-bearing решения по durability.
