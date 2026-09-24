# 0012 — Per-key model allow-list

Status: accepted — narrows ADR 0001's "no request transformation" scope: a
per-key capability check on the proxy path is a policy *refusal*, not a routing
or transformation decision (see ADR 0001's closing note).

## Context

A partner holds one local API key and can call any model the upstream
publishes. A deployment that sells two tiers — a cheap tier and a premium tier,
or a sandbox tier and a production tier — has no way to express "this key may
only call `gpt-4o-mini`" today: the model is extracted, metered, and forwarded
for every authenticated request. Restricting a key's model surface has to happen
**before** the upstream is contacted (otherwise the partner gets billed for a
request the policy was supposed to refuse) and **before** a ledger row is minted
(otherwise the refusal itself becomes metering noise).

`/v1/models` is the discovery surface a client uses to choose a model, and it is
currently a transparent, unmetered proxy to the upstream's full catalog. Under a
per-key restriction it must answer "the models *you* may call", or a restricted
key cannot discover its own capabilities.

The user's requirements, confirmed:

1. The allow-list is **per key** (`keys[].allowed_models`), not per consumer or
   global.
2. It is **strict by default**: a key that does not declare the field — or
   declares an empty list — may call **no** model. This is a deliberate breaking
   change: an existing deployment must add a list or its keys stop working.
3. A refused model returns **`404` with an OpenAI-compatible
   `model_not_found` error body**, the shape clients already treat as "this
   model does not exist".
4. **`/v1/models` is filtered** to the same list.

## Decision

* **A new config field per key:**

  ```yaml
  keys:
    - key: ...
      name: ...
      allowed_models:
        - gpt-4o
        - gpt-4o-mini
  ```

  Type: `Vec<String>`, `#[serde(default)]`, validated for blank entries by
  position (`src/config/loader.rs`). `KeyConfig::allows_model(&str)` encodes
  the strict rule: an empty list (or an absent field, which parses to empty)
  allows nothing; only an explicitly listed, exactly-matched name passes.

* **The list rides the credential, never the request.** The middleware copies
  `key_config.allowed_models` onto the `ConsumerContext` at authentication
  time (`src/auth/middleware.rs`); the handler reads it from the context
  (`src/auth/context.rs::allowed_models`). Nothing a client sends can contribute
  to the list — same rule as identity in ADR 0008. A manager context carries an
  empty list (a manager is a viewer, not a metered caller).

* **Enforcement is before metering, for inference endpoints.** In
  `handle_proxy`, after the body is parsed and the model extracted, and
  **before** `RequestRecord::new` / `accept`, a model outside the list returns
  `404` with:

  ```json
  {"error":{"message":"The model '<m>' does not exist",
            "type":"invalid_request_error","code":"model_not_found"}}
  ```

  Consequences, all intentional: the upstream is never contacted for a refused
  model, and **no ledger row is minted** (an unmetered refusal cannot pollute
  the usage views; invariant 2's "every *accepted* request reaches a terminal
  state" is untouched because a refused request is not accepted). Both
  `/v1/chat/completions` and `/v1/responses` are gated the same way.

* **`"unknown"` is never allowed.** A body with no `model`, or a non-JSON body,
  extracts as `"unknown"` (the metering fallback); under strict-by-default that
  name is in no list, so it is refused like any other unlisted model.

* **`/v1/models` is filtered, not proxied verbatim.** The endpoint exits before
  body parsing and is unmetered; the response body is parsed as JSON and its
  `data[]` is rebuilt to keep only entries whose `id` is in the key's list. An
  unparseable or differently-shaped body is forwarded **verbatim** — the proxy
  never invents an empty catalog and never fails discovery with a 500.

* **Model names are not credentials.** The reload watcher logs the before/after
  lists in cleartext (rendering `[]` as `"(none)"`, not `"*"`: an emptied list
  is the strict default, and drawing it like the manager's wildcard-all would
  read backwards). `credentials()`, `redact_credentials` and `REDACTED` are
  untouched; the credential-scrub test still asserts exactly three credentials.

* **Live.** The list is read per request from the config snapshot, so adding or
  removing a model is a hot reload, no restart.

## Alternatives considered

* **Global or per-consumer allow-list** — rejected at the requirement stage:
  the restriction is a property of the *credential* (two keys for one consumer
  can carry different tiers), and per-consumer would conflate it with identity.
* **A deny-list ("everything except …")** — rejected: a deny-list cannot keep
  pace with an upstream catalog that grows, and its default is "allow" which
  fails open. The strict default fails closed.
* **Default-open (`[]` or absent means "everything")** — rejected: it makes the
  feature a no-op for anyone who forgets the field, which is the case the
  feature exists to prevent. The manager view defaults to all because it scopes
  a *view* ([ADR 0013](0013-manager-sees-all-consumers.md)); this field gates
  *traffic*, so it defaults to none.
* **Silently rewriting `model` to the nearest allowed name** — rejected as a
  transformation (ADR 0001). The product refuses; it never lies to the client
  about what it asked for.
* **Proxying the refusal to the upstream** (letting the provider decide) —
  rejected: the whole point is that the upstream is not contacted and the
  ledger does not see the row.
* **Not filtering `/v1/models`** — rejected: a restricted key that sees a
  catalog it cannot call either learns nothing (constant 404s) or is actively
  misled; discovery must agree with enforcement.
* **Buffering `/v1/models` differently from other unmetered passthroughs** —
  rejected: the body is already `collect_capped`; only the `data[]` filter is
  new, and it has a verbatim fallback so an unexpected shape still reaches the
  client unchanged.

## Consequences

* **Breaking change.** Every existing deployment must add `allowed_models` to
  every key; a key that ships without the field serves 404 on every inference
  request. The example config carries the field on both example keys, and the
  README's config table documents the default as `[] (strict: no models)`.
* The refusal path cannot reach the upstream or the ledger — asserted by tests
  that check `upstream.request_count() == 0` and `row_count == 0` together with
  the 404 body, the same "reject before upstream, unmetered" shape already used
  for oversized bodies.
* Invariant 2 is unchanged: a refused request is *not* an accepted one, so
  "every accepted request reaches a terminal state" does not apply to it. No
  `in_flight` row is ever created for a refusal, and crash recovery has nothing
  to resolve.
* Invariant 3 (`NULL ≠ 0`) is untouched: the refusal writes nothing at all,
  rather than writing zero usage.
* The dashboard needs no change: its model filter is usage-derived from the
  requests the key actually made, so a restricted key's dropdown already shows
  only what it can reach.
* Hot reload reports `keys[].allowed_models` by index and value (model names
  are not secrets), with `[]` rendered `(none)` so an emptied list cannot be
  mistaken for "all".

## Evidence

* `src/config/types.rs` — `KeyConfig.allowed_models` (`#[serde(default)]`),
  `KeyConfig::allows_model` (strict membership).
* `src/auth/context.rs` — `ConsumerIdentity.allowed_models`,
  `ConsumerContext::new(…, allowed_models)`, `allowed_models()` accessor;
  `ConsumerContext::manager` sets it empty.
* `src/auth/middleware.rs` — the list is cloned from the config key onto the
  context at authentication, never from the request.
* `src/proxy/handler.rs` — the gate between model extraction and
  `RequestRecord::new`; `model_not_allowed_error` (404 `model_not_found` with
  `x-request-id`); `proxy_unmetered` threads the consumer and applies
  `filter_models_response` to the `/v1/models` body.
* `src/config/loader.rs::validate` — `keys[i].allowed_models[j] cannot be
  blank`, by position, without echoing values as if they were secrets.
* `src/config/hot_reload.rs::report_key_changes` — the per-key branch, `[]` as
  `(none)`.
* `config.example.yaml` — both keys carry the field with the strict comment.
* Tests: `src/config/types.rs::test_allows_model_is_strict_by_default`,
  `src/config/loader.rs::test_reject_blank_allowed_models_entry`,
  `src/config/hot_reload.rs::test_every_changed_field_is_reported` (the
  `keys[].allowed_models` row), and
  `tests/integration/model_allow_list.rs` (seven: allowed-model metering,
  disallowed-model refusal with zero upstream/zero ledger, empty-list denial,
  `/v1/responses` gating, missing/non-JSON model denial, `/v1/models` filtered,
  `/v1/models` empty for an empty list).
