# 0013 — Manager sees every consumer; config carries only what it must

Status: accepted — supersedes ADR 0011's `manager.consumers` allow-list, and
removes two config fields that decision-era files also carried (`server.listen`,
`keys[].metadata`). The manager credential itself — password, dashboard-only,
refused on `/v1/*` — is ADR 0011's decision unchanged.

## Context

ADR 0011 gated the manager's cross-consumer view behind a config allow-list
(`manager.consumers`). Three forces turned the shape it produced into a defect
rather than a feature:

1. **The allow-list and the dashboard lied to each other.** `/api/me` reported
   the allow-list as the selector's options; with the documented default
   (`consumers: []` = all), the selector rendered nothing and a manager who
   wanted to narrow to one consumer had no way to — the dashboard showed
   "all" with no filter row. The list of what may be seen was also the list of
   what could be seen, in the same file as the data itself: configuring
   visibility meant maintaining a second, hand-written copy of the ledger's
   consumer inventory.
2. **`keys[].metadata` had no reader.** It was carried onto the consumer
   context, metered by nothing, exposed by no endpoint and consumed by no code
   — a map whose only real property was that the content hash had to sort it
   to stay stable across reloads.
3. **`server.listen` split a deployment contract across two files.** The port
   must agree with the container's port mapping, the health-check URL and
   whatever fronts the process — none of which read the proxy's YAML. Changing
   the port meant editing the config *and* the compose file in lockstep, with
   nothing holding the two edits together.

The operator's requirements, confirmed: a manager password sees the usage of
**every** API key by default; `metadata` goes entirely; the listen address
comes from the environment, never the YAML.

## Decision

* **A manager sees every consumer.** `ManagerConfig` is `{ password }`. The
  allow-list is gone: a report of what may be seen must not live in the same
  file as the data it guards, and a default of "everything" does not need a
  list to say so.
* **`consumers=` is a filter, not a grant.** The dashboard's consumer selector
  narrows the view; a name nothing matches is an empty view, never a fall-back
  to everything. A consumer key's `consumers=` parameter is still ignored
  (ADR 0008) — narrowing exists only on top of an already-everything view, so
  no request shape can widen anything.
* **The selector is fed from the ledger.** `/api/me` for a manager returns
  `SELECT DISTINCT consumer_id FROM usage_hourly` — the consumers that
  actually have usage, ordered, from the same rollup table the tabs read. A
  consumer with no terminal row (no traffic yet, or only `in_flight` rows) is
  absent from the selector until it has something to show; that is honest, and
  `All` remains the view either way.
* **`keys[].metadata` is removed.** Nothing read it. The config content hash
  no longer needs the map-ordering caveat, and `ConsumerIdentity` carries only
  what code reads.
* **The listen address is `PARTNER_PORTAL_LISTEN`.** One environment variable,
  the same pattern as `PARTNER_PORTAL_CONFIG`: default `0.0.0.0:8080`; unset
  or empty means the default; an unparseable value is a startup abort whose
  error names the variable *and* the rejected value — binding somewhere the
  operator did not mean is worse than not starting. The container image pins
  `PARTNER_PORTAL_LISTEN=0.0.0.0:8080`, so the published port stays 8080 and
  the compose/nginx/health-check layer is unchanged.

## Alternatives considered

* **Keep the allow-list and feed the selector from the config list** —
  rejected: it is exactly the behaviour that hid the bug (an empty default
  list renders an empty selector), and it makes the operator maintain a
  hand-written copy of the ledger's consumer inventory.
* **Keep the allow-list and intersect it with the ledger's consumers for the
  selector** — rejected: the intersection re-introduces the second-copy
  problem and adds a join whose only effect is to hide consumers the operator
  forgot to list. A filter below "everything" needs no stored ceiling.
* **Derive the selector from `keys[].consumer_id` in the config** — rejected:
  it answers "who is configured", not "who has usage", so a never-used or
  revoked consumer would render as a selectable option over an empty view.
* **Keep `metadata` because someone might use it** — rejected: it is
  unreachable from every surface this product has; a field with no consumer is
  not paid for by keeping an order-stable hash over it.
* **Move `listen` to a CLI flag** — rejected: a flag has to be remembered on
  every invocation, including inside every container entry point; an
  environment variable sits next to `PARTNER_PORTAL_CONFIG`, which deployments
  already set, and the image can default it.
* **Fall back to the default on an invalid `PARTNER_PORTAL_LISTEN`** —
  rejected: binding `0.0.0.0:8080` when the operator wrote a hostname is a
  silent exposure; a startup abort with the value in the error is the honest
  failure.

## Consequences

* **Breaking, three ways.** A config that sets `manager.consumers`, any
  `keys[].metadata`, or `server.listen` fails to parse (`deny_unknown_fields`).
  The failure mode differs by field: startup aborts on the config read at
  boot; a hot reload of a bad file is refused with the last-known-good config
  in force and a log line naming the field — easy to miss in a busy log, so
  this belongs in the release note, not only in this ADR.
* A manager password that leaks now exposes **every** consumer's usage, not a
  configured subset. The old allow-list was never a boundary worth the name
  (same file, same reader as the data it guarded), but the exposure is
  strictly wider; rotation by hot reload, no restart, is the mitigation.
* The selector lags reality by design: a consumer appears after its first
  terminal row and disappears only when retention prunes its rollups.
  `/api/me` is fetched once per login, so a consumer created mid-session
  appears on the next page load. The tabs themselves are not affected — their
  queries read the ledger directly, not the selector list.
* A selection the ledger no longer contains (retention-pruned, or stale after
  a reload) yields an empty view — the honest answer to a filter nothing
  matches; `All` is the way out.
* The seven invariants are untouched. Invariant 7 reads the same with one
  clause widened: identity still never comes from the request, and the manager
  is the same single, deliberate exception — just without a stored ceiling.
* Deployment wiring shrinks: the port is set once, where the deployment is
  described (compose environment, unit file, dev script), and the config file
  can move between machines and ports without edits.

## Evidence

* `src/config/listen.rs` — `LISTEN_ENV`, `DEFAULT_LISTEN_ADDR`,
  `parse_listen_addr` (None/empty → default; garbage → `Err` naming the
  variable and the value) and its four unit tests; `src/main.rs` reads
  `listen_addr()` at the bind site and aborts startup on its error.
* `src/config/types.rs` — `ManagerConfig { password }`, `KeyConfig` without
  `metadata`, the rewritten `Config::hash` doc (no map-typed field left).
* `src/auth/context.rs` / `src/auth/middleware.rs` —
  `ConsumerContext::manager()` takes no allow-list; the extractor builds the
  context from `find_key` / `find_manager` alone.
* `src/dashboard/api.rs` — `resolve_scope` (manager + no parameter →
  `Scope::All`; named consumers → verbatim `Scope::List`; a key is always
  `Scope::One`), `distinct_consumers` (the `usage_hourly` DISTINCT behind
  `/api/me`), `scope_clause` (an empty `IN ()` stays FALSE — unreachable from
  `resolve_scope`, kept as a guard).
* `src/config/hot_reload.rs` — the report no longer carries
  `manager.consumers`, `server.listen` or `metadata`; the password rotation is
  still reported, redacted.
* Tests: `src/dashboard/api.rs`
  (`test_manager_without_a_parameter_sees_every_consumer`,
  `test_manager_requesting_an_unknown_consumer_gets_it_verbatim`,
  `test_distinct_consumers_reads_the_rollup_ordered_and_deduplicated`),
  `src/config/listen.rs` (four), `src/config/loader.rs`
  (`test_reject_unknown_fields_at_every_level` pins `server.listen` as an
  unknown field), and `tests/integration/manager.rs` (four, including
  `manager_me_lists_the_consumers_present_in_the_ledger`, which reads the
  DISTINCT list straight from SQLite and compares it to `/api/me`).
