# 0008 — Server-side consumer identity, isolation by construction

Status: accepted

## Context

The dashboard shows a consumer its own traffic. The classic way to build that is
a `consumer_id` parameter: the client authenticates, then asks for the data it
wants to see. That is safe only as long as every handler remembers to authorise
the parameter against the credential — and a proxy that forwards arbitrary request
fields is precisely the place where a client-supplied identity will one day reach
the ledger.

The failure mode is not a wrong number. It is one consumer reading another's
traffic, or writing rows attributed to another consumer.

## Decision

* Identity is **always derived server-side** from the presented credential and the
  live config snapshot: `keys[].consumer_id`, falling back to `keys[].name`. No
  request header, body field, query parameter or metadata contributes to it.
* Authentication is an **extractor** (`Authenticated`), not a middleware layer. A
  handler that needs an identity must take one as an argument, so a new route is
  authenticated by construction rather than by remembering to attach a layer.
* Every dashboard query is filtered by `consumer_id = ?1` where `?1` comes from the
  extractor, and there is no API shape that widens it: no `consumer_id` parameter
  exists to tamper with.
* Upstream credentials are replaced, not appended to: the client's `Authorization`
  header is dropped before the upstream credential is set, so a local key is never
  forwarded.
* There is no cross-consumer or administrative view. The dashboard router contains
  only the five self-scoped routes plus the SSE stream.
* Failures do not echo credentials: `401` responses carry
  `Cache-Control: no-store` and a fixed message, and `SetSensitiveHeadersLayer`
  marks `Authorization` unrenderable before any inner layer can log it.

## Alternatives considered

* **A `consumer_id` query parameter validated against the key** — rejected: the
  parameter is unnecessary, and its validation is a thing someone can forget.
* **Auth as a middleware layer with the identity stuffed into request extensions**
  — rejected: a route declared outside the layer is silently unauthenticated. The
  extractor makes the compiler enforce it.
* **Deriving identity from the upstream's returned model/owner fields** —
  rejected: identity must come from our own configuration, not from a response we
  do not control.
* **An admin key with a cross-consumer view** — rejected for now: it would be the
  one endpoint whose scoping is special, and it is not needed to operate the
  service. `/api/me`, plus the ledger on disk, is enough.
* **Trusting a metadata field for scoping** — rejected: metadata is opaque and
  consumer-scoped by construction, and nothing reads it for authorisation.

## Consequences

* Isolation is provable by reading one extractor and one `WHERE` clause per query,
  rather than by auditing every route.
* **The isolation boundary is the consumer, not the key.** A key authenticates one
  consumer; a consumer may have several keys. To isolate two credentials from each
  other, give them different `consumer_id`s (or omit it so `name` becomes the
  identity) — distinct consumers are mutually invisible, proven by
  `tests/e2e/dashboard_isolation.rs`. To share history across a rotation, give the
  replacement key the same `consumer_id` as the revoked one — this is the staging /
  production pattern below. A dashboard login shows the data of the consumer the
  presented key belongs to; it never shows another consumer's traffic.
* Two keys with the same `consumer_id` share a usage view (useful for a staging and
  a production key under one partner) and one can be revoked without disturbing the
  other — revocation is a hot reload.
* Revoking a key takes effect on the next request, with no restart, because the
  lookup reads the live config snapshot.
* There is no way to ask "how much did all consumers spend?" through the API; that
  question is answered by querying the SQLite file directly.
* **The ledger records no per-key attribution by design.** Local key values are
  credentials and never reach persisted text (`src/config/types.rs::credentials()`);
  a key that is leaked exposes the consumer's view while it remains configured.
  Within-consumer key-level auditing is not available — that is a deliberate
  trade-off for keeping credentials out of the database.

## Evidence

* `src/auth/middleware.rs` — module doc ("Identity is always derived server-side"),
  `Authenticated::from_request_parts`, `extract_bearer_token` (case-insensitive
  scheme, trimmed, non-UTF-8 rejected rather than panicking), `AuthError`
  (`no-store`).
* `src/config/types.rs::KeyConfig::consumer_id` — `consumer_id` or `name`.
* `src/dashboard/api.rs` — module doc ("Isolation"), and `consumer_id = ?1` in
  every query (`summary`, `timeseries`, `requests`, `models`).
* `src/dashboard/mod.rs` — the route list, and the comment that there is no
  administrative or cross-consumer view.
* `src/proxy/client.rs::proxy` — `headers.remove(header::AUTHORIZATION)` before
  `Bearer {api_key}` is set.
* `src/main.rs` — `SetSensitiveHeadersLayer` for `AUTHORIZATION` as the outermost
  layer.
* Tests: `test_extract_bearer_token_*`, `test_auth_error_never_leaks_the_credential`.
