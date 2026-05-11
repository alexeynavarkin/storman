---
id: ADR-0006
title: В MVP реализуем только FlatFile backend; CDC откладываем
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0006: В MVP реализуем только `FlatFile` backend; CDC откладываем

## Context and problem statement

`storage.FileBackend`-абстракция предполагает несколько реализаций:

- **`FlatFile`** — пользовательский файл лежит на диске как есть, `BackendRef` = relative path в `<storage-dir>`.
- **`CDCFile`** (отложено) — content-defined chunking (FastCDC), дедупликация на уровне чанков, манифест per-file. `BackendRef` = manifest id, контент в `<meta>/blobs/`.

CDC даёт значимые преимущества: дешёвая дельта-передача (rsync-style), дедупликация (одинаковые чанки в разных файлах хранятся однажды), линейный рост размера хранилища при многих версиях, готовность к remote blob storage. Но эту функциональность не нужно поставлять в первой версии.

Вопрос: что реально нужно делать в MVP — только `FlatFile`, или сразу `FlatFile + CDCFile` с per-directory выбором?

## Decision drivers

- **Time-to-MVP.** Каждый месяц до первого работающего MVP — стоимость. CDC — сложная инженерия (FastCDC, refcount-GC, копи-on-write staging, scrub-механизм).
- **Архитектурная заложенность.** Интерфейс `FileBackend` должен поддержать оба варианта без переписывания. Это влияет на дизайн, даже если реализация одна.
- **Чистота storage layer'а.** Honest storage (см. MISSION) — пользователь может настроить Time Machine на `flat-storage/` и получить чистый архив. CDC ломает это.
- **Use-case fit.** Нужна ли дедупликация в personal-scale? У одного пользователя десятки тысяч файлов, дублей мало.

## Considered options

### Option A — FlatFile-only в MVP, CDC отложен (chosen)

- В MVP реализуется только `flat.FlatFile`.
- Интерфейс `FileBackend` спроектирован так, чтобы поддерживать `CDCFile` без изменений (см. [docs/arch/storage.md](../arch/storage.md)): `BackendRef.Data` opaque, `Allocate` принимает `AllocHint`, `OpenWrite` поддерживает `Modify`-режим для будущего lazy fetch.
- `nodes.backend_kind` сейчас всегда `'flat'`, но колонка nullable у папок (политика для новых детей).
- CDC переезжает в [ROADMAP/Later](../../ROADMAP.md), включая миграционный механизм между бэкендами (`storman migrate --to=cdc <path>`).

### Option B — FlatFile + CDC оба в MVP

Реализовать оба бэкенда сразу, per-directory выбор через `nodes.backend_kind = 'cdc'` на папке, наследование потомками.

### Option C — Только CDC, без FlatFile

Самый минималистичный интерфейс — один бэкенд. Все файлы автоматически чанкуются и дедупятся.

## Decision outcome

**Chosen: Option A — FlatFile-only в MVP.**

Обоснование:
- **FlatFile = honest storage.** Это часть value proposition storman (см. MISSION). Время Machine / restic / rsync напрямую видят пользовательские файлы как они есть.
- **MVP уже большой.** 5 delivery-интерфейсов, async indexing, RBAC, корзина, audit, backup/recover. Добавление CDC удвоило бы scope MVP.
- **Use-case fit.** Дедупликация выигрывает на повторяющихся данных (бэкапах, версионных архивах). У single-user personal-scale storman основной workload — личные фото/документы. Эффект дедупа маргинален.
- **Архитектура заложена.** Интерфейс `FileBackend` уже спроектирован под CDC (Allocate, opaque BackendRef, WriteMode.Modify). Добавление CDC = новый файл `cdc.go`, без изменений в существующих протоколах или dbfs.
- **CDC не free.** Random read через манифест → бинпоиск → pread по чанку — дороже FlatFile в типичном случае. Дедупликация ценна только при значимом overlap.

Option C (CDC-only) отвергается из-за honest-storage принципа. Option B — преждевременная оптимизация: можно добавить CDC позже без миграции existing файлов (бэкенд per-file, не per-database).

## Consequences

### Positive
- MVP завершён за разумное время.
- Пользователь видит свои файлы как файлы. Бэкапы стандартными инструментами работают.
- Меньше моveable parts, меньше bug surface.
- `FlatFile` атомарен через `tmp + fsync + rename` на одной ФС — proven simple pattern.

### Negative
- Дублирующиеся файлы хранятся дважды. Для типичного personal-storage scale — ОК.
- Нет дешёвой дельта-передачи (rsync через storman, sync с iPhone-приложением, и т.п.).
- Если CDC потребуется срочно — миграция existing FlatFile-узлов в CDC не реализована (она в [ROADMAP/Later](../../ROADMAP.md)).

### Neutral
- `<meta>/blobs/` каталог зарезервирован в раскладке (см. [docs/arch/overview.md](../arch/overview.md)) под будущий CDC. Сейчас не создаётся.
- Колонка `nodes.backend_kind` уже несёт двойную семантику (immutable у файлов, политика у папок) — это работает уже сейчас, просто значения всегда `'flat'`.

## Когда пересматривать

Триггеры для активации [ROADMAP/Later → CDC](../../ROADMAP.md):
- Появление сценария rsync / FUSE с большими файлами, где random write делает FlatFile неэффективным (требуется shallow-write через chunked-storage).
- Растущий disk-footprint от дублей (пока не замечен в реальных deployments — пользователь один).
- Версионирование файлов (см. [ROADMAP/Later](../../ROADMAP.md) — CDC делает это near-free).
- Remote blob storage (S3): chunks отдаются на S3, manifests остаются локально.

## Related

- Implementation: [internal/storage/flat/](../../internal/storage/flat/).
- Architecture: [docs/arch/storage.md](../arch/storage.md).
- ROADMAP: [Later → CDC-бэкенд](../../ROADMAP.md).
- CDC-набросок (что ожидается реализовать когда возьмёмся): `git show HEAD~:PLAN.md` (файл был удалён в этом же коммите, секция «5.1 CDC-бэкенд» сохранена в git-истории как исходный дизайн).
