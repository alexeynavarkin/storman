# RBAC: модель прав, наследование, share-links

storman использует ACL-модель `User → Resource → Action`, где Resource — узел в дереве (`nodes`), а Action — битмаска. Полное обоснование «computed Traverse» — [ADR-0003](../adr/0003-rbac-computed-traverse.md).

## Биты actions

```go
const (
    Read     = 1 << 0   // bit 0 — read content / list directory
    Write    = 1 << 1   // bit 1 — create / write / mkdir
    Remove   = 1 << 2   // bit 2 — trash / purge
    Admin    = 1 << 3   // bit 3 — ACL changes, node_settings, backend_kind смены политики
    Traverse = 1 << 4   // bit 4 — computed (см. ниже), never stored in DB
)
```

В БД (`permissions.actions bit(8)`) — биты 0-3, биты 4-7 зарезервированы. `Traverse` живёт **только в коде** (`internal/rbac/`), вычисляется на лету.

## Хранение прав

Таблица [`permissions`](database.md#permissions--только-явные-права-rbac):

```sql
permissions(id, node_id, user_id, actions bit(8), created_at)
unique (node_id, user_id)
```

Хранятся **только явно выданные** права. Никаких `is_explicit`/`auto_granted` флагов, никаких виртуальных записей в персистентном состоянии.

## Эффективные права для (user, node)

`rbac.PermissionService.Effective(ctx, userID, nodeID) → Mask` работает так:

1. **Проверить ACL Cache.** LRU + epoch-based invalidation per subtree. Хит → возвращаем.
2. **Down inheritance.** По `nodes.path` (ltree) находится цепочка предков. Берётся ближайший предок (или сам node), где есть явная запись в `permissions` для этого user. Это даёт effective `Read | Write | Remove | Admin`.
3. **Up traverse.** Если на каком-либо **потомке** узла `N` у user есть явное право — добавляется виртуальный `Traverse` к эффективной маске для `N`:
   ```sql
   SELECT EXISTS (
       SELECT 1 FROM permissions p JOIN nodes n ON n.id = p.node_id
       WHERE p.user_id = $1 AND n.path <@ $2
   )
   ```
   `<@` — ltree-оператор «потомок-или-сам», использует gist-индекс на `nodes.path`.
4. **Заполнить кэш** результатом и вернуть.

`Check(ctx, userID, nodeID, want)` — оборачивает `Effective`, возвращает `rbac.ErrDenied` если `!Has(mask, want)`.

## Инвалидация кэша

- **Изменение `permissions`** (grant/revoke на узел) → инвалидируется поддерево `path <@ префикс`.
- **Перемещение/удаление узла** → инвалидируется поддерево.
- **Epoch-based:** на каждое изменение инкрементируется «эпоха» префикса; записи старой эпохи отбрасываются lazily при чтении. Без явного обхода кэша — это O(1) на mutation.

## Admin-операции

Бит `Admin` в effective permissions требуется для:

- Изменения `permissions` (grant/revoke).
- Изменения `node_settings`.
- Изменения `nodes.backend_kind` на папке (смена политики бэкенда).
- Просмотра корзины (`/api/trash`) — только root-admin (пользователь с `Admin` на корне).
- Просмотра audit log (`/api/audit`) — только root-admin.

**Root-admin** = пользователь с `Admin` на корневом узле. Через ltree-lookup это автоматически наследуется вниз, поэтому root-admin может всё.

## Share-links (только Web)

Анонимные временные ссылки с ограниченным scope. Только через HTTP/HTTPS; FTP, WebDAV, FUSE и другие протоколы анонимный доступ **не** поддерживают.

Таблица [`share_links`](database.md#share_links--анонимные-временные-ссылки-только-web):

```sql
share_links(token, node_id, actions bit(8), expires_at, max_uses, used_count, created_by, created_at)
```

**Токен:** 32 байта `crypto/rand` → base64url. Используется как PK таблицы — `SELECT` по токену = O(1) и постоянное время (защита от timing-атаки).

**Validation** (на каждое использование):
1. Токен существует.
2. `expires_at > now()`.
3. `max_uses IS NULL OR used_count < max_uses`.
4. Запрошенное action входит в `actions` ссылки.

Если ОК — `UPDATE used_count = used_count + 1`, выдаём контент. Audit event на каждое использование.

**Rate limiting по токену** — защита от brute-force перебора. Конфигурируется в общих rate-limit настройках.

**Scope ограничения:**
- `Admin`-биты в share-link запрещены проверкой в Auth Service (нельзя поделиться правом менять ACL).
- `actions = 0` тоже невалидно — нет смысла создавать ссылку без прав.

## Создание share-link

Endpoint `POST /api/share` принимает `{path, actions, ttl_seconds, max_uses?}`. Требования:
- Пользователь должен иметь `Admin` на узле (только владелец/админ может поделиться).
- Запрошенные `actions` должны быть подмножеством его собственных прав на узле.

После создания возвращается `{token, url}`. Сам токен показывается **один раз** — повторно получить нельзя (только revoke + новый).

## Реализация

См. [internal/rbac/](../../internal/rbac/) и [internal/auth/sharelinks.go](../../internal/auth/sharelinks.go). Тесты — [internal/web/server_test.go](../../internal/web/server_test.go) (TestShareLink*, TestPerm*).
