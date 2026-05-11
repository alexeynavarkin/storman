# Async indexing pipeline

Загрузка файла — sync критический путь, обогащение метаданных — async. Это даёт быструю реакцию upload-операции и масштабируется отдельно от Web/FTP-серверов.

## Архитектура

Двухэтапная:

**Stage 1 — Sync** (внутри загрузочной транзакции):
- `OpenWrite.Commit` обновляет `nodes(status='ready', size, mtime)`.
- В той же транзакции — `INSERT INTO jobs (node_id, kind, status) VALUES ($1, 'hash', 'pending')`.

**Stage 2 — Async** (отдельный пул горутин):
- `JobsPool` запускает N воркеров (configurable). Каждый воркер:
  1. Лизит задачу из `jobs` через `SELECT ... FOR UPDATE SKIP LOCKED WHERE status='pending' AND kind=$1 ORDER BY created_at LIMIT 1`.
  2. Помечает `status='in_progress'`, `locked_until=now() + LeaseDuration`.
  3. Вызывает `kind`-specific executor (например, hasher).
  4. На успехе — архивирует в `jobs_history` (`final_status='done'`).
  5. На ошибке — increment `attempts`, после `MaxAttempts` (default 5) — архивирует с `final_status='failed'` + audit alert.

## Workers (MVP)

В MVP реализован только один executor — **hasher**:

- Читает контент через `FileBackend.OpenRead`.
- Считает SHA-256 потоково через `sha256.New()` + `io.Copy`.
- В одной транзакции: `UPDATE nodes SET sha256 = $1 WHERE id = $2` + `INSERT INTO node_meta (node_id, ...) ON CONFLICT (node_id) DO UPDATE SET ...`.

Реализация — [internal/jobs/hasher.go](../../internal/jobs/hasher.go).

## Roadmap workers

См. [ROADMAP/Next](../../ROADMAP.md):

- **EXIF extractor** — для image/* (через `go-exif` или встроенный парсер). Заполняет типизированные поля `node_meta.width/height/taken_at/lat/lng` + JSONB `extra.exif.*` для редких полей (ISO, focal length, и т.п.).
- **MIME validator** — определение MIME по magic bytes (`net/http.DetectContentType` или `gabriel-vasile/mimetype`). Уже частично делается на upload, но executor может пересчитывать для recover-узлов где MIME неизвестен.
- **Face recognition / AI tags** — на будущее.

Новый kind worker'а = новый файл с `Executor` имплементацией + регистрация в `NewPool` в `serve.go`. Изменений в storage/auth/web-слоях не требует.

## Конкурентность и SKIP LOCKED

`SELECT FOR UPDATE SKIP LOCKED` — стандартный паттерн очереди в PostgreSQL:

- Несколько воркеров могут одновременно лизить задачи разных строк без блокировки.
- Если конкурент уже взял строку — `SKIP LOCKED` пропускает её, не ждёт.
- Падение воркера → `locked_until` истекает → sweeper / следующий воркер подбирает.

Это даёт линейное масштабирование числом воркеров без сложного coordination.

## Конфиг

```json
"indexing": {
  "workers": 2,
  "poll_interval": "5s"
}
```

- `workers` — сколько горутин в пуле. Default 2.
- `poll_interval` — пауза между попытками лизить (когда очередь пустая). Default 5 секунд.

При активной очереди воркеры берут задачи back-to-back, без `poll_interval`-задержки.

## Retry политика

- `MaxAttempts = 5` (захардкожено).
- `LeaseDuration = 5 минут` (захардкожено) — если воркер не отчитался за это время, задача считается зависшей и доступна для перехвата.
- На каждой неудаче `attempts++`, `last_error` обновляется. После `MaxAttempts` — финальный failed.

Backoff между retry — линейный по `attempts` (`poll_interval * attempts`). Не экспоненциальный, чтобы failed-задачи не зависали надолго.

## Архивация в `jobs_history`

Терминальные строки (`done` или окончательный `failed`) переезжают в `jobs_history` — append-only партиционированную по месяцу таблицу. См. [database.md § jobs_history](database.md#jobs_history--append-only-архив-завершённых-задач).

В одной транзакции: `INSERT INTO jobs_history (...) SELECT ... FROM jobs WHERE id = $1; DELETE FROM jobs WHERE id = $1`. Это keeps `jobs` горячей и маленькой; вся история — в партиционированной таблице.

## Atomic enqueue с upload

Hash-job вставляется в **той же транзакции**, что и `nodes.status='ready'`:

```sql
-- внутри FileWriter.Commit
BEGIN;
UPDATE nodes SET status='ready', size=$1, mtime=now() WHERE id=$2;
INSERT INTO outbox_history (...) SELECT ... FROM outbox WHERE id=$3;
DELETE FROM outbox WHERE id=$3;
INSERT INTO jobs (node_id, kind, status) VALUES ($2, 'hash', 'pending');
COMMIT;
```

Это значит: **каждый committed файл гарантированно получит hash**. Невозможно состояние «файл `ready`, но `jobs`-записи нет» — это была бы либо abort'нутая транзакция (тогда `status` не `ready`), либо корруппция БД.

## Реализация

- [internal/jobs/jobs.go](../../internal/jobs/jobs.go) — `Service` (Enqueue / Lease / Finish / Release).
- [internal/jobs/pool.go](../../internal/jobs/pool.go) — `Pool` + worker goroutines + retry/release.
- [internal/jobs/hasher.go](../../internal/jobs/hasher.go) — `Hasher` executor.
- Запуск в [internal/cli/serve.go](../../internal/cli/serve.go) — `jobs.NewPool(jobSvc, jobs.KindHash, hasher, cfg.Indexing.Workers, ...)`.
