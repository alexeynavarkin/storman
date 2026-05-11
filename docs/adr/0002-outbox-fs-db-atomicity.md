---
id: ADR-0002
title: Transactional outbox для FS↔DB атомарности
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0002: Transactional outbox для FS↔DB атомарности

## Context and problem statement

Каждая мутирующая FS-операция (`OpenWrite.Commit`, `Remove`, `Rename`, миграция между бэкендами) затрагивает два независимых хранилища: PostgreSQL (узлы, мета, ACL) и файловую систему (`flat-storage/`). Между ними нет общих транзакций — process crash, kill -9, power loss или OOM посередине оставляет либо узел в БД без файла на диске, либо файл на диске без узла в БД. Оба исхода нарушают north-star про durability.

Нужен механизм, который **гарантирует**: после возврата `Commit` клиенту либо состояние полностью согласовано, либо процесс восстановления приведёт его к согласованному состоянию (без участия человека).

## Decision drivers

- **No silent data loss** — durability — это north-star.
- **Atomicity on a single host** — мы не строим cluster, single-host crash recovery — главный сценарий.
- **Simple to reason about** — code reviewer должен за минуту понять, что произойдёт при крэше.
- **PostgreSQL — единственная внешняя зависимость** — не хочется тащить Kafka/etcd/RabbitMQ ради coordination.
- **Independent от внешних транзакционных менеджеров** — нет XA-coordinator в архитектуре.

## Considered options

### Option A — Transactional outbox + idempotent re-play (chosen)

Каждая FS-операция:

1. Стадирует контент: `FileBackend` пишет файл в `<meta>/uploads/<id>` (atomic-replaceable staging).
2. В одной транзакции PostgreSQL:
   - вставляет/обновляет строку в `nodes` со `status='pending'`;
   - вставляет строку в таблицу `outbox(op, node_id, payload, status='pending')`.
3. Коммит. **С этого момента операция должна быть доведена до конца.**
4. Executor читает outbox row → вызывает соответствующее действие backend'а (`Commit` = `rename` staging → целевой путь для `FlatFile`).
5. Финальная транзакция: `nodes.status='ready'` + архивация outbox-строки в `outbox_history`.

**Recovery после краша.** На старте процесс читает `outbox WHERE status IN ('pending', 'in_progress')` и доигрывает. Каждый executor идемпотентен — проверяет текущее состояние через `FileBackend.Stat` и доводит до целевого. Повторный запуск той же outbox-строки безопасен.

**Sweeper.** Фоновая задача перехватывает зависшие операции (истёкший `locked_until`), эскалирует `failed` после исчерпания retry-попыток (архивация с `final_status='failed'` + alert).

### Option B — XA / 2PC distributed transaction

PostgreSQL поддерживает prepared transactions (`PREPARE TRANSACTION`). Можно построить 2PC между БД и кастомным файловым transaction-manager'ом.

**Минусы:**
- PostgreSQL 2PC отключён по умолчанию (`max_prepared_transactions = 0`); включение требует тюнинга и накладывает penalty.
- Нужен собственный transaction-manager для filesystem-side, который сам должен durable хранить prepared state.
- Crash recovery всё равно требует ручного resolve in-doubt транзакций оператором, либо timeout-based abort с потерей.
- Сложность роста кратно с числом backend'ов и числом протоколов — каждый должен корректно участвовать в 2PC-протоколе.

### Option C — Write-ahead log в БД, фиксация на диске вторым шагом, без outbox

Аналог outbox, но без явной таблицы — состояние выводится из `nodes.status='pending'` + heartbeat'ов. На recovery walk'ом по `nodes WHERE status='pending'`.

**Минусы:**
- Pending-state не несёт payload (upload_id, target_ref, acl_snapshot и т.п.) — придётся прятать в `nodes` или дублировать в `node_meta`. Семантическая путаница: `nodes` начинает описывать одновременно прошлое и намерение.
- Сложно расширить на операции, не привязанные жёстко к одному узлу (massive trash, ACL ребалансировка) — нет места куда положить payload.

## Decision outcome

**Chosen: Option A — transactional outbox + idempotent re-play.**

Обоснование:
- Один файл — `internal/storage/dbfs/outbox.go` — реализует механизм. Это compact и testable.
- `outbox`-таблица — горячая, маленькая. Идёт в `outbox_history` (партиционированная по месяцу) после завершения. Чтения дешёвые.
- Crash recovery полностью автоматический, без ручного вмешательства — `fs.RecoverPending` вызывается в `serve.go` при старте, отрабатывает за секунды.
- Минимум зависимостей: PostgreSQL, который у нас и так есть.

## Consequences

### Positive
- Никаких prepared transactions, никаких внешних coordinator'ов.
- Каждая операция полностью описана `(op, payload)` — отлично читается, отлично тестируется, удобно сериализовать в audit.
- Идемпотентность даёт безопасный retry без боязни ускорить «уже сделано».
- Расширяемость на новые ops тривиальна — новый код executor'а + новая variant payload'а.
- Поведение под нагрузкой предсказуемо: outbox = очередь, queue-depth measurable, slow operations видно сразу.

### Negative
- Каждая мутация — две транзакции вместо одной (commit-stage + finalize). На SSD это микросекунды; на сетевом хранилище может быть заметно, но storman single-host.
- Sweeper и executor logic — нетривиальный код. Если поломается, deferred операции просто застрянут — нужны метрики «глубина outbox» и алерты.
- Партиции `outbox_history` нужно создавать cron'ом наперёд (см. ROADMAP/Next) — забыли создать → новые записи поедут в default-партицию и могут отказать.

### Neutral
- `outbox` — таблица. Это OK; альтернативы (Redis/Kafka) добавили бы внешнюю зависимость без выигрыша на single-host scale.

## Related

- Implementation: [internal/storage/dbfs/outbox.go](../../internal/storage/dbfs/outbox.go), [internal/storage/dbfs/recover.go](../../internal/storage/dbfs/recover.go), [internal/storage/dbfs/trash.go](../../internal/storage/dbfs/trash.go).
- Architecture: [docs/arch/storage.md](../arch/storage.md).
- Зависит от [ADR-0001 FileSystem boundary](0001-filesystem-boundary.md) — outbox-инвариант держится только если все мутации идут через `dbfs`.
