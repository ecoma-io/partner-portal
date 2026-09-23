# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

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
