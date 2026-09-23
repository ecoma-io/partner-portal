# 0001 — One upstream, not a routing gateway

Status: accepted

## Context

The product is a proxy between partner consumers and an inference API. The
obvious next feature, and the one every comparable project already has, is
routing: several providers behind one endpoint, failover between them, per-model
destination rules, weighted traffic.

Every one of those features costs the same thing — a request's *destination*
becomes data-dependent and time-dependent, and therefore so does its accounting.
Once two upstreams can serve the same model name, "tokens served" is no longer
answerable from the ledger alone; the ledger has to record which route was taken,
which credential paid, and which provider's numbers are being trusted. Usage
extraction stops being one wire format and becomes a registry.

## Decision

Exactly one upstream, configured as `upstream.base_url` with one
`upstream.api_key`. The proxy appends the request path to that base and forwards;
it does not choose a destination.

The router therefore has a fixed shape: three proxied paths, the dashboard API,
the admin endpoints and an SPA fallback. Anything else under `/v1/` is a 404.

## Alternatives considered

* **Provider routing table in the config** — rejected. It duplicates what a
  gateway (LiteLLM, an egress proxy) already does, and it is the single decision
  that would make `usage_records` unable to describe a request without joining
  another table.
* **Failover to a secondary upstream on 5xx** — rejected. A retried request is a
  second upstream call that may already have consumed tokens; recording it as one
  request would under-report, recording it as two needs a parent/child model the
  ledger does not have.
* **Path rewriting to a deeper API surface** — rejected; a base *path* is
  preserved and the request path appended, which is enough to sit behind an
  existing gateway without inventing a rewrite language (see the base-path test in
  `src/proxy/client.rs`).

## Consequences

* Usage extraction can be written per endpoint and trusted, because there is one
  wire format to understand (`src/proxy/usage.rs`).
* A consumer who wants routing puts a routing layer in front; this proxy stays the
  accounting boundary, which is the thing that must not be duplicated.
* Changing provider means editing `upstream.base_url`, which is hot-reloadable and
  takes effect on the next request.
* The cost is real: no resilience against the upstream's own outage. That is an
  accepted scope limit, not an oversight.

## Evidence

* `src/lib.rs` — "One upstream, three endpoints … deliberately **not** a
  general-purpose LLM gateway: there is no routing, no multi-provider failover and
  no request transformation."
* `src/config/types.rs` — `UpstreamConfig` is a single struct with `base_url` and
  `api_key`, not a list.
* `src/main.rs` — the route table registers exactly three `/v1/*` paths.
* `src/proxy/client.rs::upstream_uri` — `format!("{base}{path}")`: append, never
  choose or rewrite.
* `src/proxy/handler.rs` — `Endpoint::from_path` returns `None` for every other
  path, which becomes a 404.
* `README.md` — the "what it deliberately is not" table.
