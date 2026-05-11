# Trash: soft-delete с возможностью восстановления

`Remove` в storman — soft-delete: узлы помечаются как удалённые, физический контент уезжает в `<meta-storage>/trash/<uuid>/`, можно восстановить через admin UI до истечения retention.

## Жизненный цикл удаления

Когда вызывается `FileSystem.Remove(ctx, path)` (через любой протокол):

1. **Одна транзакция:**
   - Рекурсивный `UPDATE nodes SET status='deleted', deleted_at=now() WHERE path <@ $rootPath` — один SQL для всего поддерева (через gist-индекс на `nodes.path`).
   - Insert `outbox(op='trash', payload={node_id, backend_ref_subtree, trash_uuid, acl_snapshot, original_path, deleted_by})`.
   - Коммит.

2. **После коммита** — executor (`runTrashOutbox` в [internal/storage/dbfs/trash.go](../../internal/storage/dbfs/trash.go)):
   - `os.Rename` физического поддерева из `<flat-storage>/<original-path>` в `<meta-storage>/trash/<uuid>/payload`. Атомарно (одна ФС).
   - Записывает `<meta-storage>/trash/<uuid>/trash.meta.json` через atomic tmp + rename. Содержимое:
     ```json
     {
       "trash_uuid": "...",
       "node_id": "...",
       "original_path": "/docs/old/",
       "original_parent_id": "...",
       "deleted_at": "2026-05-11T...",
       "deleted_by": "user-uuid",
       "backend_kind": "flat",
       "backend_ref": "docs/old",
       "acl_snapshot": [...]
     }
     ```
   - Архивация outbox-строки в `outbox_history` + DELETE из `outbox` в одной транзакции.

`trash.meta.json` — **служебная** запись корзины, а не side-car пользовательского файла. Она самодостаточна — позволяет восстановить элемент даже если БД утрачена.

## Что выживает после удаления

- Физический контент — лежит в `<meta-storage>/trash/<uuid>/payload`, никак не модифицирован.
- Структура поддерева — сохраняется внутри `payload/` как был на момент удаления.
- ACL — снимок в `trash.meta.json` (для восстановления исходных прав).
- Метаданные в БД — `nodes` строки остаются с `status='deleted'`, `deleted_at` non-NULL. **Не удаляются**, чтобы UI мог показывать «удалено такого-то».

## Восстановление (`Restore`)

`POST /api/trash/{trash_uuid}/restore` (admin-only):

1. Читает `trash.meta.json` для целевого trash entry.
2. Проверяет, что `original_path` свободен (не занят новым узлом). Если занят — конфликт, 409.
3. **Транзакция:**
   - `UPDATE nodes SET status='ready', deleted_at=NULL WHERE path <@ <original_path>` — восстанавливает всё поддерево.
   - Insert outbox `op='restore_trash'` с payload восстановления физических путей.
4. Executor: `os.Rename` `<trash>/<uuid>/payload` → `<flat-storage>/<original-path>`. Удаляет каталог `<trash>/<uuid>/` (включая `trash.meta.json`).
5. Архивация outbox.

Реализация — `Restore` метод в [internal/storage/dbfs/gc.go](../../internal/storage/dbfs/gc.go).

## Покупка (`Purge`)

`DELETE /api/trash/{trash_uuid}` (admin-only) — окончательное удаление:

1. Читает `trash.meta.json`.
2. **Транзакция:**
   - `DELETE FROM nodes WHERE id IN (subtree)` — окончательное удаление метаданных. ON DELETE CASCADE убирает связанные `node_meta`, `permissions`.
   - Insert outbox `op='purge_trash'` с путём к payload.
3. Executor: `os.RemoveAll(<trash>/<uuid>/)` — удаление физических файлов.
4. Архивация outbox.

После Purge восстановить невозможно (если только не из `pg_dump`).

## GC корзины

Фоновый воркер `StartGC(ctx, interval, retentionDays, logger)` запускается в `serve` и тикает каждый `interval`:

1. `RunGC(ctx, retention)` — `SELECT trash_uuid FROM trash WHERE deleted_at < now() - retention`.
2. Для каждого — вызывает `Purge`.

Конфиг:
```json
"trash": {
  "retention_days": 30,
  "gc_interval": "1h"
}
```

`retention_days = 0` — отключает time-based GC. Записи остаются до явного Purge через UI.

## Видимость корзины

`GET /api/trash` — **admin-only**. Причина: `rbac.Effective` фильтрует узлы с `deleted_at IS NOT NULL` (живой ACL на удалённом узле не имеет смысла), поэтому обычные пользователи не могут видеть свои собственные удалённые элементы по существующему ACL-пути. Корзина — административный инструмент, не UI «личной мусорки».

Это сознательное упрощение. Альтернатива (per-user мусорка с уважением ACL на момент удаления) — overkill для personal-scale.

## Атомарность через outbox

Trash проходит через тот же outbox-механизм, что и обычная запись/удаление (см. [storage.md § Outbox](storage.md#outbox-атомарность-fs--db)). Это значит:

- Crash после UPDATE `status='deleted'` но до `os.Rename` — `RecoverPending` на старте доигрывает `os.Rename`.
- Crash после `os.Rename` но до архивации outbox — повторный запуск executor'а делает no-op (`os.Stat` показывает что rename уже выполнен), архивирует outbox.
- Невозможно состояние «`nodes.status='deleted'` + физический файл в `flat-storage/`» более чем на мгновение между транзакциями.

## Восстановление без БД

Если БД утрачена, но `<meta-storage>/trash/` цел — `trash.meta.json` содержит всё необходимое для ручного восстановления через `recover --from-disk`. Walker, обходя `trash/`, может либо игнорировать поддеревья (default), либо опционально восстанавливать их обратно в `flat-storage/<original_path>`. На MVP это manual operation; автоматизировать пока не требуется.

## Реализация

- [internal/storage/dbfs/remove.go](../../internal/storage/dbfs/remove.go) — `Remove` (рекурсивный UPDATE + outbox).
- [internal/storage/dbfs/trash.go](../../internal/storage/dbfs/trash.go) — `applyTrash`, `runTrashOutbox`, `trash.meta.json` writer.
- [internal/storage/dbfs/gc.go](../../internal/storage/dbfs/gc.go) — `ListTrash`, `Restore`, `Purge`, `RunGC`, `StartGC`.
- [internal/web/trash.go](../../internal/web/trash.go) — HTTP handlers (`/api/trash/*`).
