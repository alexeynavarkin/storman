# Storage layer

Storage слой — единая точка для всех файловых мутаций. Два уровня абстракции:

- **`storage.FileSystem`** — фасад над деревом узлов для всех frontend-протоколов. Реализация одна — `internal/storage/dbfs/`, поверх `nodes` в PostgreSQL.
- **`storage.FileBackend`** — низкоуровневое хранение содержимого одного файла. В MVP единственная реализация — `FlatFile` в `internal/storage/flat/`. CDC отложен ([ADR-0006](../adr/0006-flatfile-only-mvp.md)).

Инвариант: **все delivery-протоколы ходят только через `FileSystem`**, не через backend и не через прямой SQL. См. [ADR-0001](../adr/0001-filesystem-boundary.md).

## `FileSystem` интерфейс

```go
type FileSystem interface {
    // Дерево
    Stat(ctx, path) (*NodeInfo, error)
    List(ctx, path) ([]NodeInfo, error)
    Mkdir(ctx, path, opts MkdirOpts) error
    Remove(ctx, path) error                  // soft-delete → корзина
    Rename(ctx, oldPath, newPath) error      // в т.ч. между папками с разными бэкендами;
                                             // node остаётся на своём backend_kind, не конвертируется

    // Содержимое
    OpenRead(ctx, path) (FileReader, error)
    OpenWrite(ctx, path, opts WriteOpts) (FileWriter, error)
}

type FileReader interface {
    io.ReaderAt
    io.Closer
    Size() int64
}

type FileWriter interface {
    io.WriterAt
    Truncate(size int64) error
    Commit() error          // финализация: атомарная публикация
    Abort() error           // отмена: чистит staging, не публикует
    io.Closer               // Close без Commit/Abort вернёт ErrUnfinalized
}

type WriteOpts struct {
    Mode         WriteMode  // Create | Overwrite | Modify  (в MVP только Create)
    ExpectedSize int64      // -1 если неизвестно — для квот / преаллокации
}
```

Этого достаточно для всех протоколов: FUSE `read/write/release` → `ReadAt/WriteAt/Commit|Abort`; FTP `RETR/STOR` → стрим поверх `ReaderAt/WriterAt`; HTTP Range → `ReaderAt`; tus.io → chunked `WriteAt` + `Commit` по `Upload-Complete`; S3 PutObject/Multipart — аналогично; WebDAV PUT/GET — через те же примитивы.

### Что сознательно НЕ в интерфейсе

- **Random write на уровне `FileBackend`** — нет. `WriteAt` поддерживается каждым бэкендом по-своему через staging; снаружи это просто `FileWriter`.
- **Move/Rename в `FileBackend`** — нет. Логическое перемещение в дереве — операция `FileSystem`, не трогает `BackendRef`. Если backend хранит ref как путь (как `FlatFile`), при необходимости — `Delete(old) + Allocate(new) + копирование`. Native rename как оптимизация — добавляется *в* `FileBackend` отдельным методом (`RenameRef`), не как ad-hoc обход.
- **List/Walk** — нет. Обход дерева — это БД.
- **Native COPY** — нет (пока). WebDAV COPY сейчас декомпозируется библиотекой `golang.org/x/net/webdav` на per-file `OpenFile` + `io.Copy` — N транзакций для дерева из N файлов. Native `FileSystem.Copy` — в [ROADMAP/Next](../../ROADMAP.md).

## `FileBackend` интерфейс

```go
type FileBackend interface {
    Name() string                                // "flat" | "cdc" | ...

    OpenRead(ctx, ref BackendRef) (FileReader, error)
    OpenWrite(ctx, ref BackendRef, opts WriteOpts) (FileWriter, error)
    Delete(ctx, ref BackendRef) error
    Stat(ctx, ref BackendRef) (BackendStat, error)

    // Allocate ref для нового файла. Вызывается ДО старта записи, чтобы
    // FS успела зафиксировать (backend_kind, backend_ref) в nodes и outbox
    // в той же транзакции, что и создание узла.
    Allocate(ctx, hint AllocHint) (BackendRef, error)
}

type BackendRef struct {
    Kind string  // == FileBackend.Name()
    Data string  // backend-specific: для flat — относительный путь; для cdc — manifest id
}

type AllocHint struct {
    NodeID       uuid.UUID
    LogicalPath  string  // подсказка для FlatFile: куда положить файл «как есть»
    ExpectedSize int64
}
```

### `FlatFile` (единственная реализация в MVP)

- `Allocate` → возвращает relative path, основанный на `LogicalPath` (sanitized).
- `OpenWrite` → открывает tmpfile в `<meta-storage>/uploads/`, `WriteAt` = `pwrite` в tmpfile. На `Commit` — `fsync` + `rename` поверх целевого пути (POSIX-атомарно в пределах ФС). На `Abort` — `unlink` tmpfile.
- `OpenRead` → `os.OpenFile`, возвращает обёртку с `ReadAt`.
- `Delete` → `unlink`.

См. [internal/storage/flat/](../../internal/storage/flat/).

## Привязка бэкенда к файлу

- Политика хранится прямо в `nodes.backend_kind`:
  - у **папки** — nullable, означает «бэкенд для новых детей в этой ветке» (NULL = наследовать);
  - у **файла** — NOT NULL и **immutable**, фактический бэкенд содержимого.
- Effective backend для нового файла = ltree-lookup ближайшего предка-папки с не-NULL `backend_kind`. Если ни у одного предка не задано — default `'flat'`.
- Бэкенд фиксируется при **создании** файла. Существующий файл свой бэкенд не меняет — даже если на родителе изменили политику.
- Миграция между бэкендами (`storman migrate --to=cdc <path>`) — отдельная фоновая операция (см. ROADMAP/Later).

В MVP всегда `'flat'`. Двойная семантика колонки — это инвестиция в будущее CDC, см. [ADR-0006](../adr/0006-flatfile-only-mvp.md).

## Жизненный цикл записи

`FileSystem.OpenWrite(ctx, path, WriteOpts) → ... → Commit` проходит так:

1. FS вычисляет effective backend для нового файла (ltree-lookup, default `'flat'`).
2. FS вызывает `backend.Allocate(hint)` → получает `BackendRef`.
3. **Транзакция 1.** Insert `nodes(status='pending', backend_kind, backend_ref)` + insert `outbox(op='create_file', payload={upload_id, backend_ref})`. Коммит.
4. Возвращается `FileWriter` (это writer бэкенда).
5. Клиент пишет (`WriteAt`/`Truncate`), накапливая в staging.
6. Клиент вызывает `Commit` → бэкенд финализирует (для `FlatFile` — `fsync` + `rename` поверх целевого пути).
7. **Транзакция 2.** Update `nodes(status='ready', size, mtime)` + insert hash-job в `jobs` + архивация outbox-строки в `outbox_history` (`INSERT INTO outbox_history ... SELECT ... FROM outbox WHERE id = $1; DELETE FROM outbox WHERE id = $1` в одной транзакции). Коммит.

Любой `Abort` или `Close` без `Commit` → backend чистит staging, FS откатывает узел через outbox (executor смотрит на текущее состояние и доводит до согласованного, в данном случае — удаляет pending-узел).

## Outbox: атомарность FS ↔ DB

Две независимых системы (диск и Postgres) — нет общих транзакций. Используем transactional outbox + идемпотентное доигрывание. Полное обоснование — [ADR-0002](../adr/0002-outbox-fs-db-atomicity.md). Здесь — механика.

### Удаление (`Remove`)

1. **Транзакция.** Рекурсивный `UPDATE nodes SET status='deleted', deleted_at=now() WHERE path <@ $root_path` (одним SQL'ем для всего поддерева). Insert `outbox(op='trash', payload={node_id, backend_ref_subtree, trash_uuid, acl_snapshot})`.
2. После коммита: executor (`runTrashOutbox`) делает `os.Rename` физического поддерева в `<meta-storage>/trash/<uuid>/payload`, записывает `trash.meta.json` (atomic tmp + rename).
3. Архивация outbox-строки в `outbox_history` в одной транзакции с DELETE из `outbox`.

Подробнее — [trash.md](trash.md).

### Rename / Move

Аналогично: outbox описывает целевое состояние (новый `backend_ref` если нужен, новый `path`). Для `FlatFile` это `os.Rename` файла + при необходимости перевычисление `backend_ref` в `nodes`.

### Идемпотентность

Каждый executor при старте проверяет текущее состояние диска через `FileBackend.Stat` и доводит до целевого. Если уже сделано — no-op. Это позволяет безопасно повторять при крэше.

### Recovery после краша

При старте процесс читает `outbox WHERE status IN ('pending', 'in_progress')` и доигрывает каждую строку. Записи с истёкшим `locked_until` перехватываются. См. `RecoverPending` в [internal/storage/dbfs/recover.go](../../internal/storage/dbfs/recover.go).

### Sweeper

Фоновая задача:
- Перезапускает зависшие операции (истёкший lease).
- Эскалирует `failed` после исчерпания retry-попыток — архивация с `final_status='failed'` в `outbox_history` + alert в audit log.
- Удаляет orphan staging-файлы в `<meta-storage>/uploads/` старше TTL без связанной outbox-записи.

### Атомарность на уровне одного файла

- **Запись:** `open tmp → write → fsync → rename`. POSIX гарантирует атомарность rename в пределах ФС.
- **Финализация бэкенда** (`FileWriter.Commit`) гарантирует либо целевое состояние, либо staging (отменяемое через `Abort` / sweeper). Никакого промежуточного «полузаписанного» файла в целевом дереве не появляется.

## Upload path

### Web — tus.io resumable uploads

- Resumable, chunked, стандартизированный протокол ([tus.io 1.0.0](https://tus.io)).
- Каждый upload получает `id`; чанки пишутся в `<meta-storage>/uploads/<id>/` (а не в целевой путь).
- Метаданные хранятся в `info.json`, прогресс — в `offset` (sidecar-файл, rewriting atomically).
- На `Upload-Complete` → backend вызывает `dbfs.ImportPath(ctx, logical, srcAbsPath)`, который делает `os.Rename` финализированного staging-файла в `flat-storage/` и оформляет outbox-запись через тот же путь, что и обычный `OpenWrite.Commit`.
- **Authentication:** session-based, та же что Web API. CSRF tus-клиенты не понимают, поэтому tus-пути идут через `authedRead`, проверка владельца — в `info.json.user_id`.
- **Sweeper:** stale upload (нетронут N часов) → автоматически удаляется. Реализация: [internal/web/tus_sweeper.go](../../internal/web/tus_sweeper.go).

### FTPS — STOR

- Driver перенаправляет `STOR` в `<meta-storage>/uploads/<random>`.
- По `TRANSFER COMPLETE` — та же outbox-процедура.

### Общие правила

- **Никогда** не писать напрямую в целевой путь — только `tmp → rename`.
- `fsync` перед rename для durability.
- Квоты (когда появятся в ROADMAP) проверяются до старта upload и подтверждаются в транзакции вместе с insert в `nodes`.
- Cleanup временных файлов: sweeper удаляет staging-файлы старше N часов без связанной outbox-записи.

## Recovery (admin operations exposed by dbfs)

`dbfs.DBFS` экспонирует ряд non-interface методов для админских/recovery-сценариев. Они **не** часть `FileSystem`, но допустимы (см. [ADR-0001 § Исключение](../adr/0001-filesystem-boundary.md)) для функций, которые интерфейс by design не моделирует:

- `Bootstrap(ctx) (created bool, err)` — создаёт root-узел при первом старте.
- `RecoverPending(ctx) (acted int, err)` — выполняется на старте `serve`, доигрывает зависшие outbox-операции.
- `StartGC(ctx, interval, retentionDays, logger)` — фоновый воркер корзины (см. [trash.md](trash.md)).
- `ListTrash(ctx)` / `Restore(ctx, trashUUID)` / `Purge(ctx, trashUUID)` — админский CRUD над корзиной (только root-admin).
- `ImportPath(ctx, logical, srcAbsPath)` — используется tus для финализации upload'а через `os.Rename` вместо backend stream.

Все они ходят через те же транзакции и outbox-паттерн, что и публичные FS-методы. Это **не** обход, это extension точки для админских use-case'ов.
