# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.1](https://github.com/ecoma-io/partner-portal/compare/v0.1.0...v0.1.1) (2026-09-25)


### Bug Fixes

* **release:** stop uploading the buildx build record that aborted every release ([ebc7f72](https://github.com/ecoma-io/partner-portal/commit/ebc7f7269a3f633c970b62bf4c96f7a39fc50894))
* **release:** stop uploading the buildx build record that aborted every release ([bf584db](https://github.com/ecoma-io/partner-portal/commit/bf584db579edb7ec969a3dad4c8994da0df5baf9)), closes [#23](https://github.com/ecoma-io/partner-portal/issues/23)

## 0.1.0 (2026-09-25)


### ⚠ BREAKING CHANGES

* **config:** a config naming server.listen or keys[].metadata now fails to parse instead of being ignored, and every key must declare allowed_models - a key without one serves 404 for every model.

### Features

* **config:** gate models per key, add a manager credential, move listen to env ([6853cff](https://github.com/ecoma-io/partner-portal/commit/6853cffc6673fbe8810a4572a95d5a68cec9375b))
* core proxy, ledger, dashboard, SSE baseline ([a1dee35](https://github.com/ecoma-io/partner-portal/commit/a1dee35cd96ce794adc3376d3dec3fb3f0329818))
* **dashboard-ui:** an embeddable dashboard with an authenticated live stream ([09b0337](https://github.com/ecoma-io/partner-portal/commit/09b03379e159fb9b831d7f58c6430b666d484ec7))
* **dashboard-ui:** redesign the stats views around the new API ([bbe2989](https://github.com/ecoma-io/partner-portal/commit/bbe2989dcbc9cedd2d59d27ab85a2c7e1397220d))
* **dashboard:** keyset pagination, filters and a weighted timeseries ([bb9b7f8](https://github.com/ecoma-io/partner-portal/commit/bb9b7f87021ec0cec09f118f52dce9a49eb327e1))
* **ledger:** persist bounded upstream error bodies (schema v4) ([0bd5cc6](https://github.com/ecoma-io/partner-portal/commit/0bd5cc61c8ccf72e2c1dbaa7eff8d0ffc6aa0df5))
* **proxy:** reach https upstreams with a TLS connector ([e70c84e](https://github.com/ecoma-io/partner-portal/commit/e70c84ea67dfd13a0d8319bcc5059fbd17568c97))
* **repo:** add a native dev loop for edit-see-change cycles ([dae8cbe](https://github.com/ecoma-io/partner-portal/commit/dae8cbe1ea92e05a5b9f94574f6d645c391bb900))


### Bug Fixes

* close the metering, streaming and credential gaps found in review ([4523695](https://github.com/ecoma-io/partner-portal/commit/45236958fce18c13285146147c8a55c4895d1aee))
* **dashboard:** let the isolation e2e run against a no-dashboard build ([89de3e5](https://github.com/ecoma-io/partner-portal/commit/89de3e59a03ec0ed0c55c73dc55735203999ff91)), closes [#7](https://github.com/ecoma-io/partner-portal/issues/7)
* **dashboard:** require a local API key to log in, and scope every view to it ([b3c5747](https://github.com/ecoma-io/partner-portal/commit/b3c57473f0f585dc49db48da9eb19f23f04cff10))
* **deploy:** keep the runtime image's packages at the current patch level ([b895b19](https://github.com/ecoma-io/partner-portal/commit/b895b199e3e43fd967435a94d1609b9c9cf8d42b)), closes [#9](https://github.com/ecoma-io/partner-portal/issues/9)
* **deploy:** let the smoke paths pass the model gate and assert its refusal ([1501297](https://github.com/ecoma-io/partner-portal/commit/150129721cb80d5a17be4397b68d7d81da43633b))
* **deploy:** poll for the drain line instead of one post-stop read ([0174ce4](https://github.com/ecoma-io/partner-portal/commit/0174ce4169c570dd8217456d033810f7a34c4762)), closes [#6](https://github.com/ecoma-io/partner-portal/issues/6)
* harden metering durability, streaming and request accounting ([9d34fea](https://github.com/ecoma-io/partner-portal/commit/9d34feac1dc43f6e5d60d371b30d1eeaf2464c38))


### Documentation

* bring the README, AGENTS and architecture pages to the new shape ([b05f7c4](https://github.com/ecoma-io/partner-portal/commit/b05f7c4dcd7c03cc33a3c8755c69773a91e9f986))

## [Unreleased]

### Added

- **Proxy** — one OpenAI-compatible upstream, with `/v1/chat/completions`,
  `/v1/responses` and `/v1/models` forwarded verbatim. Hop-by-hop headers are
  stripped, the client's `Authorization` is replaced with the configured upstream
  credential, and the response body is re-framed rather than transformed.
- **Streaming** — server-sent events are forwarded frame by frame, with usage
  recovered by an incremental scanner capped at 256 KiB per event. Time to first
  token is measured at the first data frame.
- **Metering ledger** — SQLite in WAL mode with `synchronous = FULL`, written by a
  single writer task over a bounded queue with micro-batching. Every accepted
  request is durable as `in_flight` before the upstream is contacted and resolved
  to `completed`, `failed` or `interrupted` afterwards. Raw rows and the hourly
  rollup are written in one transaction, and each request is rolled up exactly
  once.
- **Crash recovery** — rows left `in_flight` by a dead process are resolved to
  `interrupted` at startup, with the same-request rollup applied as part of that
  transition. The process refuses to start if recovery fails.
- **Retention** — batched deletion of raw rows and rollups past
  `database.retention_days` on a configurable interval, with an incremental vacuum
  attempt and no full `VACUUM`.
- **Authentication** — local API keys with server-side consumer identity
  derivation (`consumer_id`, falling back to `name`); no request field can
  influence identity. `401` responses carry `Cache-Control: no-store`.
- **Dashboard API** — `/api/me` plus `/api/dashboard/{summary,timeseries,requests,models}`,
  every query scoped to the authenticated consumer. Totals and timeseries read the
  hourly rollup; the request list reads the raw ledger with keyset pagination.
- **Dashboard stream** — `/api/dashboard/events`, an SSE stream that carries
  invalidation only (`{"type":"data_changed"}`), driven by `PRAGMA data_version`
  polling so a change committed by another instance is announced too.
- **Embedded dashboard** — a Vue 3 + Pinia single-page app built by vite and baked
  into the binary by `build.rs`. A build without `dashboard/dist` still succeeds
  and serves a placeholder page.
- **Admin** — `/healthz` (never touches the database), `/readyz` (503 while
  shutting down or while the ledger is degraded) and `/version` (version, commit,
  build time, schema version).
- **Configuration** — a YAML file (`PARTNER_PORTAL_CONFIG`) validated at startup
  and re-read every second by content hash, swapped atomically with no request
  interruption. An invalid reload keeps the last-known-good snapshot.
- **Graceful shutdown** — `SIGTERM`/`SIGINT` fails readiness, drains in-flight
  work, waits for detached finalizes, closes the queue and commits the ledger
  before the database is released.
- **Benchmarks** — `cargo bench --bench ledger_write` drives the real writer and
  reports commit-ack p50/p95/p99 alongside throughput.

### Notes

- Ledger schema version: **2** (`ledger_meta.schema_version`, also returned by
  `/version`). `src/ledger/schema.sql` is applied idempotently at startup; there is
  no migration runner.
- Not implemented on purpose: routing or provider failover, TLS termination, rate
  limiting, pricing, and any cross-consumer or administrative view. See
  [`README.md`](README.md) and [`docs/adr/`](docs/adr/).

[Unreleased]: https://github.com/ecoma-io/partner-portal/commits/main
