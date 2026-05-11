---
id: ADR-0004
title: App-passwords — a single secret for all non-web protocols
status: accepted
date: 2026-05-11
deciders: alexnav
---

# ADR-0004: App-passwords — a single secret for all non-web protocols

## Context and problem statement

storman has several delivery protocols that do not work with HTTP sessions: **FTPS** (Basic-style auth at the FTP protocol level), **WebDAV** (HTTP Basic auth), and in the future — FUSE, S3-compat, rsync. All of them receive `(login, secret)` from the client and must verify it.

Possible approaches to the secret:
- Use the user's **main password**.
- Introduce **app-passwords** — separate long-lived tokens tied to a user, revocable independently of the main password.
- Use **per-protocol scopes** — each app-password limited to a specific protocol.
- Use **per-action scopes** — each app-password carries a bitmask of allowed actions.

Question: which app-password model is reliable and simple enough to justify inclusion in MVP?

## Decision drivers

- **2FA compatibility.** FTPS/WebDAV clients cannot do TOTP. If a user has 2FA enabled, the main password is no longer usable for those protocols — a different secret is needed.
- **Revocation.** A leak of an iPhone app / a laptop with an embedded FTP client must not require changing the main password.
- **User convenience.** Each new protocol must not require a new entity and a new UI flow.
- **Code simplicity.** Less state on an app-password = fewer invariants to keep in mind.
- **Vulnerability of scopes.** Per-action / per-protocol scopes on tokens look like defence-in-depth, but require discipline (correct scope at creation, correct check in every protocol) and easily become either "give everything" (default) or "we forgot to check".

## Considered options

### Option A — App-passwords, single scope, cross-protocol (chosen)

- Table `app_passwords(id, user_id, label, hash, created_at, last_used)`.
- One app-password = a long-lived secret tied to the user. No scope fields.
- `AuthenticateAppPassword(login, secret)` tries the main password (`Authenticate`); on mismatch — falls back to the user's `app_passwords` list.
- Used identically by FTPS, WebDAV, and any future non-web protocol.
- On success — `UPDATE last_used`, reset `failed_attempts`. Brute-force counters are shared with the main login (storman cannot be pried via FTPS without marking the web `failed_attempts`).
- On creation the user sees the secret once; the DB stores only an argon2id hash.
- Revocation — `DELETE FROM app_passwords WHERE id = $1`.

### Option B — Per-protocol scope on tokens

Each app-password has a column `protocol = 'ftps' | 'webdav' | …`. Check: the token is valid only if the protocol matches.

### Option C — Per-action scope (bitmask `Read|Write|…`)

Each app-password carries its own action mask, applied on top of the user's RBAC as a restriction.

### Option D — OAuth-style refresh/access tokens

A full OAuth2 client-credentials scheme with TTL and refresh.

## Decision outcome

**Chosen: Option A — a single cross-protocol scope.**

Rationale:
- storman is single-user / family-scale. There is no scenario "give an iPhone read-only access". If a user wants to limit scope, it is simpler to create **a separate user** in storman with the desired RBAC and issue them an app-password.
- Per-action scope on tokens is a second layer of authorization over RBAC. It **complicates the mental model**: "Alice has Read+Write on /docs, but her iPhone token is Read-only; and over FTPS she has yet a third scope". One token = one user = one set of rights.
- Per-protocol scope offers minimal security win (a compromised app-password still compromises the user), while every new protocol would require marking it in the table and extending the enum. The cognitive load is not justified.
- An OAuth flow requires client registration + TTL logic + refresh — non-trivial code volume for one persona per user.
- 2FA: stays in the roadmap (`users.totp_secret` is already reserved). When we add it — the rule is simple: "if 2FA is enabled, FTPS/WebDAV require an app-password". For now there is no 2FA, app-passwords are an opt-in convenience.

## Consequences

### Positive
- Minimum state: one table, one `AuthenticateAppPassword` function. Tested once, valid for all protocols.
- Easy mental model for the user: "a token = me + a convenient password".
- Brute-force unified: `users.failed_attempts` counters are shared, you cannot "outrun" the web rate-limit via FTPS.
- Hooking up a new protocol = call `AuthenticateAppPassword` + nothing else.

### Negative
- A compromised app-password = a compromised user across all non-web protocols. Mitigation: the user must revoke the token (UI CRUD exists).
- Cannot "give a read-only WebDAV access to another person" without creating them a separate user.
- Cannot limit an app-password by IP / TTL / max-uses (no such fields).

### Neutral
- 2FA is not implemented yet — therefore, app-passwords are a **convenience layer**, not a security requirement. Once 2FA arrives, semantics will change (an app-password becomes mandatory for FTPS/WebDAV), but the interface does not.

## Future evolution

If per-protocol or per-action scope becomes necessary:
- Add a `scopes jsonb` column to `app_passwords`. NULL = "all", otherwise — a list of allowed protocols / action bits.
- Update `AuthenticateAppPassword` to return not only `User`, but also a scope filter applied on top of RBAC by `web.davAuth` / `ftpsrv.AuthUser`.

This is a compatible extension — while there are no scopes, behaviour does not change.

## Related

- Implementation: [internal/auth/app_passwords.go](../../internal/auth/app_passwords.go).
- Usage: [internal/ftpsrv/driver.go](../../internal/ftpsrv/driver.go) (FTPS), [internal/web/webdav.go](../../internal/web/webdav.go) (WebDAV).
- Architecture: [docs/arch/auth.md](../arch/auth.md).
