# 0011 — Manager password: one deliberate cross-consumer view

Status: superseded by [0013](0013-manager-sees-all-consumers.md) — the manager
credential itself stands; the `consumers` allow-list below does not. This ADR
records the decision as it was made.

Originally: accepted — partially supersedes ADR 0008's "no cross-consumer view"
decision, which this ADR implements in exactly one gated form.

## Context

The dashboard is, by design, consumer-scoped: a key authenticates exactly one
consumer, and every query is filtered by that consumer's id (ADR 0008). That is
the correct default for a proxy serving isolated partners. It leaves one gap:
the operator of the service itself has no *dashboard* answer to "how much did
this partner / all partners use this month?" — the only answers are reading the
SQLite file directly, or wiring a second, aggregate tool around the ledger.

A cross-consumer view is a capacity that ADR 0008 explicitly rejected "for
now". This ADR adds exactly one, with a guardrail that keeps the rejection
intact for everything except the thing it is for.

## Decision

* Add an optional `manager` block to the config:

  ```yaml
  manager:
    password: <an operator-chosen secret>
    consumers: [consumer-a, consumer-b]   # or [] for *all* consumers
  ```

  The password is a credential: sent as `Authorization: Bearer <password>` on
  the dashboard routes, matched by exact string equality like a key value,
  scrubbed from every ledger/API text path, redacted in `Debug`, and never
  logged by the reload watcher — the identical treatment `keys[].key` gets.
* A manager credential authenticates the dashboard (`/api/me`, the summary,
  timeseries, requests and models queries) with `role: "manager"`. It is
  **not** a proxy credential: presenting it on `/v1/*` returns `403`, and no
  ledger row is ever minted from it (a manager has no consumer to attribute
  metering to).
* The scope is the **intersection** of two sets: the manager's configured
  allow-list (`consumers`) and the `consumers=` query parameter the dashboard
  sends. This is narrow-only: the request can shrink the grant, never widen it.
  An empty allow-list means *all* consumers. A consumer outside the
  allow-list is an empty view, never a silent fall-back to "everything".
* A consumer key's `consumers=` parameter is ignored — identity and scope are
  server-derived from the credential, as ADR 0008 requires. There is no API
  shape that widens a key.
* The manager password is the **one** widening of the consumer-scoped
  dashboard. Nothing else changes: there is still no administrative view that
  writes, no key management surface, no billing, no way to mint rows.

## Alternatives considered

* **No cross-consumer view, keep reading the SQLite file** — rejected as the
  product default: the whole point of the dashboard is that the operator
  answers operational questions there, not by shelling into the ledger.
* **An "admin" boolean on an existing key** — rejected: it conflates two
  different credentials (a partner key and an operator password) in one
  config entry, and it would let the same value both meter requests and read
  everyone's usage.
* **A separate dashboard-authenticated token minted at runtime** — rejected:
  it reintroduces state and a store of its own; a config block is how this
  project already expresses credentials.
* **Per-consumer `consumer_id` parameter the client may pass** — rejected in
  ADR 0008 and still rejected: identity must come from the credential. The
  manager's `consumers=` parameter exists only as a *narrower* cross-product
  of an already-credential-bound allow-list.
* **Making the manager a first-class consumer with its own rows** — rejected:
  the manager is a viewer, not a metered entity; fabricating a consumer_id for
  it would corrupt the ledger's attribution.

## Consequences

* Isolation-by-construction survives: the *default* remains "one key, one
  consumer", and the manager is a named, documented exception that an operator
  opts into with a whole config block. A deployment that never writes a
  `manager:` block is byte-for-byte the ADR 0008 behaviour.
* The "admin key with a cross-consumer view" alternative in ADR 0008 is now
  partially implemented — the cross-consumer *view* exists, gated behind a
  password that cannot proxy. ADR 0008's rejection is retained in spirit:
  nothing else about the dashboard widens.
* Because the manager's own view is still consumer-scoped (just multiplexed
  over the allow-list), the seven invariants are untouched: metering, crash
  recovery, unavailable-is-not-zero, raw-and-rollup agreement, incremental
  streaming, shutdown drain, and per-credential key-scoping all still hold.
* The dashboard renders a consumer selector when `/api/me` reports `manager`;
  the chosen selection narrows `consumers=`. A manager's every request is
  still bound by the allow-list, so a compromised dashboard cannot widen it.
* A manager password that leaks exposes the manager's *view*, exactly as a
  leaked key exposes that key's consumer view. Rotation is a config change and
  a hot reload, like key rotation.
* The ledger is unaffected: no schema change, no new table, no per-key
  attribution added. The `consumers=` intersection is computed in the query
  layer over the existing `consumer_id` column.

## Evidence

* `src/config/types.rs` — `ManagerConfig` (manual `Debug` redacting the
  password), `Config::find_manager` (exact equality, empty-password guard),
  `Config::credentials()` (the password joins the scrubbed credential set).
* `src/config/loader.rs::validate` — rejects empty `manager.password` and
  empty `manager.consumers[i]` entries, naming fields, never values.
* `src/config/hot_reload.rs` — `report_live_changes` names the changed field
  and renders `REDACTED` for the password.
* `src/auth/middleware.rs` — `find_key` first, then `find_manager`; a
  password that matches neither is the same `InvalidKey` 401.
* `src/auth/context.rs` — `ConsumerContext::manager`, `ManagerRole`,
  `consumers()` (None / Some([]) / Some(list)).
* `src/dashboard/api.rs` — `Scope`, `resolve_scope` (intersection), and the
  `consumers=` parameter; `get_me` returns `role` + `consumers`.
* `src/main.rs` — the proxy handler returns `403` before building a request id
  when `consumer.is_manager()`.
* `docs/adr/0008-server-side-consumer-identity.md` — amended Decision and
  Alternatives to record this one exception.
* Tests: `tests/integration/manager.rs` (four), plus unit tests in
  `src/config/{types,loader}.rs`, `src/auth/context.rs`, and
  `src/dashboard/api.rs`.