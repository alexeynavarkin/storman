---
id: ADR-0004
title: App-passwords — единый секрет для всех non-web протоколов
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0004: App-passwords — единый секрет для всех non-web протоколов

## Context and problem statement

storman имеет несколько delivery-протоколов, не работающих с HTTP-сессиями: **FTPS** (Basic-style auth по протоколу FTP), **WebDAV** (HTTP Basic auth), в будущем — FUSE, S3-compat, rsync. Все они получают `(login, secret)` от клиента и должны его проверить.

Возможные подходы к secret:
- Использовать **основной пароль** пользователя.
- Завести **app-passwords** — отдельные long-lived токены, привязанные к пользователю, ревокабельные независимо от основного пароля.
- Использовать **per-protocol scopes** — каждый app-password ограничен конкретным протоколом.
- Использовать **per-action scopes** — каждый app-password имеет битмаску допустимых действий.

Вопрос: какая модель app-passwords достаточно надёжна и проста, чтобы оправдать включение в MVP?

## Decision drivers

- **2FA-совместимость.** FTPS/WebDAV-клиенты не умеют TOTP. Если у пользователя включена 2FA, основной пароль уже недоступен этим протоколам — нужен другой secret.
- **Ревокация.** Утечка iPhone-приложения / лаптопа со встроенным FTP-клиентом не должна требовать смены основного пароля.
- **Удобство пользователя.** Каждый новый протокол не должен требовать новой entity и нового UI-flow.
- **Простота кода.** Меньше state на app-password = меньше invariant'ов держать в голове.
- **Уязвимость скоупов.** Per-action / per-protocol scopes на токенах выглядят как defence-in-depth, но требуют дисциплины (правильное scope при создании, корректная проверка в каждом протоколе) и легко становятся либо «дай всё» (default), либо «забыли проверить».

## Considered options

### Option A — App-passwords, единый scope, кросс-протокольный (chosen)

- Таблица `app_passwords(id, user_id, label, hash, created_at, last_used)`.
- Один app-password = долгоживущий секрет, привязанный к пользователю. Никаких scope-полей.
- `AuthenticateAppPassword(login, secret)` пробует основной пароль (`Authenticate`), при mismatch — fallback на список `app_passwords` пользователя.
- Используется одинаково FTPS, WebDAV, любым будущим non-web протоколом.
- При успехе — `UPDATE last_used`, сброс счётчика `failed_attempts`. Brute-force-counters общие с основным логином (нельзя пробить storman через FTPS не отметившись на web `failed_attempts`).
- При создании пользователь видит секрет один раз; в БД лежит только argon2id-хеш.
- Ревокация — `DELETE FROM app_passwords WHERE id = $1`.

### Option B — Per-protocol scope на токенах

Каждый app-password имеет колонку `protocol = 'ftps' | 'webdav' | …`. Проверка: токен валиден только если протокол совпал.

### Option C — Per-action scope (битмаска `Read|Write|…`)

Каждый app-password несёт свою actions-маску, накладывается поверх RBAC пользователя как ограничение.

### Option D — OAuth-style refresh/access токены

Полноценная OAuth2 client-credentials схема с TTL и refresh.

## Decision outcome

**Chosen: Option A — единый кросс-протокольный scope.**

Обоснование:
- storman — single-user / family-scale. Нет сценария «дать iPhone доступ только на чтение». Если пользователь хочет ограничить scope, проще создать **отдельного пользователя** в storman с нужным RBAC и выдать ему app-password.
- Per-action scope на токенах — это второй слой авторизации поверх RBAC. Это **усложняет ментальную модель**: «у Alice есть Read+Write на /docs, но её iPhone-токен только Read; а через FTPS у неё ещё какой-то третий scope». Один токен = один user = один набор прав.
- Per-protocol scope даёт минимальный security win (компрометация app-password всё ещё компрометирует пользователя), при этом каждый новый протокол потребует помечать его в таблице и enum-расширения. Когнитивная нагрузка не оправдана.
- OAuth-flow требует client registration + TTL-логика + refresh — это нетривиальный объём кода для одного persona на пользователя.
- 2FA: остаётся в roadmap'е (`users.totp_secret` уже зарезервирован). Когда добавим — правило простое: «при включённой 2FA для FTPS/WebDAV требуется app-password». Сейчас 2FA нет, app-password — opt-in удобство.

## Consequences

### Positive
- Минимум state: одна таблица, одна функция `AuthenticateAppPassword`. Тестируется один раз, валидна для всех протоколов.
- Лёгкая ментальная модель для пользователя: «токен = я + удобный пароль».
- Brute-force unified: counters в `users.failed_attempts` общие, не получится «обскакать» web-rate-limit через FTPS.
- Подключение нового протокола = вызов `AuthenticateAppPassword` + ничего больше.

### Negative
- Компрометация app-password = компрометация пользователя по всем non-web протоколам. Митигация: пользователь должен ревокать токен (UI для CRUD есть).
- Невозможно «выдать read-only WebDAV-доступ другому человеку», не создав ему отдельного пользователя.
- Невозможно ограничить app-password по IP / TTL / max-uses (нет таких полей).

### Neutral
- 2FA пока не реализована — следовательно, app-passwords — это **convenience layer**, а не security requirement. Когда 2FA появится, semantics изменятся (app-password станет обязательным для FTPS/WebDAV), но интерфейс — нет.

## Future evolution

Если потребуется per-protocol или per-action scope:
- Добавить колонку `scopes jsonb` в `app_passwords`. NULL = «все», иначе — список allowed protocols / action-bits.
- Обновить `AuthenticateAppPassword` чтобы возвращать не только `User`, но и `scope`-фильтр, который `web.davAuth` / `ftpsrv.AuthUser` применяют поверх RBAC.

Это совместимое расширение — пока scopes нет, поведение не меняется.

## Related

- Implementation: [internal/auth/app_passwords.go](../../internal/auth/app_passwords.go).
- Usage: [internal/ftpsrv/driver.go](../../internal/ftpsrv/driver.go) (FTPS), [internal/web/webdav.go](../../internal/web/webdav.go) (WebDAV).
- Architecture: [docs/arch/auth.md](../arch/auth.md).
