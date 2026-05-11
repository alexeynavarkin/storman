---
id: ADR-0003
title: Traverse-права вычисляются виртуально, не хранятся в БД
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0003: Traverse-права вычисляются виртуально, не хранятся в БД

## Context and problem statement

RBAC-модель storman — `User → Resource → Action` с битмаской `Read|Write|Remove|Admin`. Дерево узлов (ltree). Право `Traverse` (право пройти через узел `P`, чтобы попасть на потомка `D` где у пользователя есть явное право) — необходимо, иначе невозможно построить путь по дереву.

Вопрос: как выдавать `Traverse`?
- Хранить в `permissions` как явные записи на каждого предка узла с грантом — материализованный подход.
- Вычислять на лету: «есть ли хоть один явный grant у этого пользователя на потомке узла `N`» — virtual подход.

Цена материализации: на каждый `INSERT INTO permissions(node_id=X, user_id=U, …)` нужно дополнительно вставить N traverse-записей в каждого из N предков `X`. При перемещении узла нужно обновить traverse-записи у предков старого и нового места. При удалении пользователя — каскад. Объём `permissions` растёт как N × M (N — глубина, M — количество grant'ов).

Цена вычисления: `SELECT EXISTS (SELECT 1 FROM permissions p JOIN nodes n ON n.id=p.node_id WHERE p.user_id=$1 AND n.path <@ $2)` — ltree-запрос с gist-индексом, O(log N).

## Decision drivers

- **Корректность** — невозможно забыть обновить traverse-записи при rename/move/удалении.
- **Простота** — меньше скрытого состояния = меньше багов.
- **Производительность** — O(log N) ltree-lookup допустим для personal-scale (одиночный пользователь, до десятков тысяч узлов).
- **Размер `permissions`** — таблица читается ACL-cache'ом, разумно держать компактной.
- **Read latency** — критична, write latency — допускается чуть выше.

## Considered options

### Option A — Computed Traverse (chosen)

`permissions` хранит **только явные** права (`Read`/`Write`/`Remove`/`Admin`). Traverse никогда не материализован.

Эффективная маска для `(user, node)` вычисляется так:

1. **Down inheritance.** По `nodes.path` (ltree) находится цепочка предков. Берётся ближайший предок (или сам node), где есть явная запись в `permissions` для этого user. Это даёт effective `Read/Write/Remove/Admin`.
2. **Up traverse.** Если на каком-либо **потомке** узла `N` у user есть явное право — добавляется виртуальный `Traverse` к эффективной маске для `N`. Запрос: `SELECT EXISTS (SELECT 1 FROM permissions p JOIN nodes n ON n.id=p.node_id WHERE p.user_id=$1 AND n.path <@ $2)`.
3. Результат кэшируется в ACL Cache.

`Traverse` — это enum-bit в коде (`rbac.Traverse = 1<<4`), но `permissions.actions bit(8)` хранит только биты 0-3.

### Option B — Stored Traverse

При каждом `Grant` на узел `X` доп. вставляется traverse-запись в `permissions` на каждого предка `X` для того же user. При revoke / rename / удалении — каскадная синхронизация.

### Option C — Hybrid: cache traverse-bit в `permissions`, инвалидация на лету

Хранить в `permissions` дополнительный bit «у этого пользователя есть descendant grant'ы под этим узлом», обновлять триггером. Гибрид с худшими свойствами обоих подходов: всё ещё материализация (надо инвалидировать), но granularity бита грубее.

## Decision outcome

**Chosen: Option A — computed Traverse.**

Обоснование:
- `permissions` остаётся compact (только explicit grants).
- Нет класса багов «забыл обновить traverse при move» — потому что нечего обновлять.
- `ltree` + gist-индекс делает ancestor/descendant lookup O(log N), что для personal-scale даже без cache достаточно.
- ACL Cache (LRU + epoch-based invalidation per subtree, см. [docs/arch/rbac.md](../arch/rbac.md)) делает повторные lookup'ы практически бесплатными.

## Consequences

### Positive
- `permissions` — таблица только явных грантов; легко аудитить «кому что выдано».
- Rename/move/удаление узла не требуют каскадной перезаписи traverse-записей.
- Удаление пользователя — простой `DELETE FROM permissions WHERE user_id=$1`, без дополнительной чистки.
- Код RBAC проще: одна функция `Effective(user, node)` инкапсулирует всю логику.

### Negative
- Каждое `Check(user, node, Traverse)` делает ltree `EXISTS`-запрос (или попадает в кэш). На холодном кэше — extra-DB roundtrip.
- При большой ширине дерева descendant-проверка может быть дороже узкого ancestor-walk'а; для storman-scale неважно, но для multi-tenant это было бы рассмотрено иначе.
- ACL Cache critical для производительности; если он сломается, нагрузка на БД линейно растёт.

### Neutral
- `Traverse`-bit (1<<4) живёт только в коде (`internal/rbac/`), не в БД. Это ОК; альтернатива — выделить ему 4-й бит в `permissions.actions`, но это создаёт соблазн начать хранить traverse-записи.

## Related

- Implementation: [internal/rbac/](../../internal/rbac/).
- Architecture: [docs/arch/rbac.md](../arch/rbac.md).
- ACL cache invalidation — epoch-based per subtree; описана в `docs/arch/rbac.md`.
