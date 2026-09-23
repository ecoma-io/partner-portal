# Agent guidance

For working **on** this repository. Read it before the first edit: the rules
below are the ones a diff gets rejected for violating, and most of them are not
inferable from the code.

This file is read by Claude Code (through a one-line [`CLAUDE.md`](CLAUDE.md)
that imports it), by Codex and by opencode. Put guidance here, never in
`CLAUDE.md` — content added there reaches one host out of three.

[`CONTRIBUTING.md`](CONTRIBUTING.md) is the process contract: setup, gates,
conventional commits, the pull-request flow, how to add an endpoint and how to
change the schema. It is not repeated here. What follows is what that document
does not say, and what reading any one file will not tell you.

## What this repository is

`partner-portal` — an OpenAI-compatible reverse proxy in front of **one**
upstream, with local API-key authentication, a durable SQLite usage ledger, and a
Vue dashboard embedded in the binary. Three proxied paths
(`/v1/chat/completions`, `/v1/responses`, `/v1/models`), five self-scoped
dashboard endpoints, three admin endpoints, one static SPA.

It is deliberately **not** a general-purpose LLM gateway: no routing, no
multi-provider failover, no request transformation, no pricing. Routing is a
different product with a different failure model; the decisions that shaped this
one are in [`docs/adr/`](docs/adr/) — read the ADR before changing anything it
covers.

## The seven invariants

`src/lib.rs` states them at the crate root, and they are the reason the code is
shaped the way it is. Each is enforced somewhere specific, and each has a test
that goes red when the enforcement is removed.

| # | Invariant | Where it is enforced | Test that goes red |
|---|---|---|---|
| 1 | **Metering is never dropped.** The queue applies backpressure; SQLite contention is retried, not skipped | `src/ledger/writer.rs` (bounded `mpsc`, `BUSY_RETRIES`, `busy_timeout`) | `test_bounded_queue_applies_backpressure_not_loss`, `test_busy_timeout_is_set` |
| 2 | **Every accepted request reaches a terminal state** — `in_flight` is durable before the upstream is contacted, and crash recovery resolves what a crash left behind | `src/proxy/handler.rs` (accept before upstream), `src/ledger/recovery.rs::recover_in_flight` | `test_accept_then_finalize_persists_terminal_state`, `test_recovery_resolves_in_flight_to_interrupted`, `test_unknown_status_reads_as_interrupted_not_completed` |
| 3 | **Unavailable is not zero.** Usage the provider never reported stays `NULL` | `src/proxy/usage.rs`, `src/ledger/types.rs::Usage::status`, `src/ledger/schema.sql` (nullable columns + `CHECK`s) | `test_stream_without_usage_records_null_not_zero`, `test_missing_usage_stays_null_not_zero`, `test_usage_never_fabricated`, `test_schema_rejects_fabricated_states` |
| 4 | **Raw and rollup agree** — one transaction, each request rolled up exactly once | `src/ledger/writer.rs::try_flush` / `finalize_record`, `src/ledger/recovery.rs` | `test_duplicate_finalize_does_not_double_count_rollup`, `test_retention_preserves_raw_rollup_consistency` |
| 5 | **Streaming stays incremental.** Frames are forwarded as they arrive; the body is never buffered to recover usage | `src/proxy/handler.rs::stream_response`, `src/proxy/sse_scan.rs` (`MAX_EVENT_BYTES`) | `test_completed_stream_records_usage_and_ttft`, `test_memory_stays_bounded_across_many_events`, `test_oversized_event_is_truncated_not_buffered_forever` |
| 6 | **Shutdown drains.** Readiness fails, work finishes, the pipeline commits, then the database closes | `src/main.rs::shutdown_signal` + drain sequence, `src/ledger/writer.rs::shutdown` | `test_shutdown_drains_queue_before_stopping`, `test_flush_now_makes_prior_writes_durable` |
| 7 | **Dashboard data is key-scoped.** Identity comes from the credential, never from the request | `src/auth/middleware.rs`, `src/dashboard/api.rs` (`consumer_id = ?1`) | `test_extract_bearer_token_*`, plus `where` clauses a reviewer checks by hand — there is no unit test that can see a missing filter in every query |

If a change weakens one of these, the change is wrong — not the invariant. If you
believe an invariant must change, that is the [changing an
invariant](CONTRIBUTING.md#changing-an-invariant) path: an ADR, the crate doc, the
tests that enforce it, and the superseded ADR, in one pull request.

## Layout

| Path | What it holds |
|---|---|
| `src/main.rs` | Composition root: config load, recovery, listener, layer stack, signal handling, drain |
| `src/lib.rs` | The crate's public surface and the invariants above |
| `src/config/` | YAML types and defaults (`types.rs`), validation (`loader.rs`), the 1 s content-hash hot-reload watcher (`hot_reload.rs`) |
| `src/auth/` | `Authenticated` extractor, Bearer parsing, server-side identity derivation |
| `src/proxy/` | Upstream client (`client.rs`), request handler and `StreamMeter` (`handler.rs`), SSE scanner (`sse_scan.rs`), per-endpoint usage extraction (`usage.rs`) |
| `src/ledger/` | `schema.sql`, single-writer task (`writer.rs`), crash recovery (`recovery.rs`), retention (`retention.rs`), fixed-width timestamps (`timefmt.rs`), pool |
| `src/dashboard/` | Consumer-scoped REST API (`api.rs`) and the invalidation-only SSE stream (`sse.rs`) |
| `src/admin/` | `/healthz`, `/readyz`, `/version` |
| `src/web/` | Embedded asset table, SPA fallback, CSP, cache headers |
| `dashboard/` | Vue 3 + Pinia + vite source; `dashboard/dist` is embedded by `build.rs` and is Git-ignored |
| `tests/` | Integration and end-to-end suites (below) |
| `benches/` | `ledger_write` — commit-ack latency and throughput against the real writer |
| `docs/` | [Architecture overview](docs/architecture/overview.md), [ADRs](docs/adr/) — indexed in [`docs/README.md`](docs/README.md) |
| `deploy/` | Compose file, nginx config, smoke-test script |

## Commands

```bash
cargo build                       # debug build; warns if dashboard/dist is absent
cargo build --release             # LTO thin, stripped — the shipping artifact
cargo test --lib                  # unit tests, the fast loop (~2 s)
cargo test --test integration     # real binary + mock upstream + SQLite read back
cargo test --test e2e -- --test-threads=1   # signals, rolling update, crash recovery
cargo fmt --all                   # rustfmt, no config file — defaults are the style
cargo clippy --all-targets -- -D warnings
cargo bench --bench ledger_write
pnpm --dir dashboard install && pnpm --dir dashboard build   # embeds the dashboard
```

Two facts about the build:

- **`dashboard/dist` is Git-ignored.** A fresh clone builds and runs; the binary
  serves a placeholder page explaining that the dashboard was not built, and
  `build.rs` prints a `cargo:warning` saying so. Never "fix" a missing dashboard
  by committing `dist/` — build it.
- **`build.rs` re-runs on `dashboard/dist`.** Rebuilding the dashboard and then
  `cargo build` is what refreshes the embedded assets; there is no runtime asset
  directory.

## Rules for changes

- **Do not add a second upstream, a model→provider map, or failover.** ADR 0001.
- **Do not add a dependency that replaces ten lines of code you can read.** The
  dependency set is deliberate: `rusqlite` (bundled), `hyper`/`axum`, `serde_yaml`,
  `parking_lot`, `sha2`. Embedding the dashboard is `build.rs` generating
  `include_bytes!`, not an embedding crate; the SSE scanner is hand-written, not a
  JSON-streaming parser. A new dependency needs a reason in the pull request.
- **Never make a request fail because metering is unavailable.** The accept either
  succeeds (and the request proceeds) or fails with `503 metering_error` before the
  upstream is contacted; there is no path where the upstream runs and the record is
  silently skipped.
- **Never write a `0` where the provider said nothing.** See invariant 3; the type
  system carries `Option<u64>` end to end for a reason.
- **Never take identity from the request.** No header, body field, query parameter
  or metadata may reach `consumer_id`. ADR 0008.
- **Never buffer a stream to read its usage.** ADR 0009 — the cap is per event
  (256 KiB), not per response.
- **Do not add a fallback that hides a failure.** A config that fails to parse
  keeps the last-known-good snapshot and logs it; a scan that overflows records
  `unavailable`; a ledger that cannot keep up degrades readiness. All three are
  deliberate, and each is a place someone will try to add a "just this once"
  workaround.
- **No `unwrap`/`expect` outside tests**, except on literal parses that cannot
  fail — the only four in the tree are `src/telemetry/mod.rs:10-11` and
  `src/ledger/timefmt.rs:23,29`, and each says why. The request path returns
  `Result`.
- **Logs never contain credentials or bodies.** `Authorization` is marked
  sensitive at the outermost layer in `src/main.rs`; do not log a body, a key, or a
  header map at debug level.
- **A new config field lands with its example.** `config.example.yaml` is the
  configuration reference, and it must load: `PARTNER_PORTAL_CONFIG=config.example.yaml`
  plus `cargo run` is the check. A field documented in the example but not in
  `src/config/types.rs` (or the reverse) is a defect.
- **A behaviour change lands with its docs.** `docs/architecture/overview.md` and
  the ADR that owns the behaviour are part of the change, not a follow-up.

### Schema changes

`src/ledger/schema.sql` is applied on every startup with `CREATE TABLE IF NOT
EXISTS` and index statements, and it is idempotent (`test_init_schema_is_idempotent`).
There is **no migration runner** and no migration directory: the schema is the
file, embedded in the binary with `include_str!`, and nothing else reads it.

So, for a schema change:

1. Edit `src/ledger/schema.sql` so a fresh database gets the shape you want.
2. Bump `SCHEMA_VERSION` in `src/ledger/mod.rs`. It is recorded in
   `ledger_meta.schema_version` at startup and returned by `/version`
   (`test_schema_version_is_recorded`).
3. Decide, and write down in the pull request, what happens to an existing
   database — including one that a previous binary is still writing during a
   rolling update. Adding a table or a nullable column is safe; anything else
   needs a documented, one-way path.
4. Add the column to the writer's statements **and** to recovery's, or the crash
   path will disagree with the live path.

### Changing an invariant

An invariant is not a preference; changing one means the product changed. Write
an ADR (Context / Decision / Alternatives / Consequences / Evidence), update
`src/lib.rs`'s doc comment, update the ADR this one supersedes, and update the
tests that enforce it — in one pull request.

## Tests

- **Three tiers.** Unit tests live beside the code (`cargo test --lib`, ~2 s, 133
  tests). `tests/integration/` drives the real binary as a child process with a
  mock upstream and reads the SQLite file back. `tests/e2e/` covers what is
  defined at the operating-system boundary — signals, a rolling update, crash
  recovery — and runs serially.
- **A test that only pins the loud direction is not a test.** Everything here
  fails quietly when it fails: a dropped record, a `0` in place of `NULL`, a
  missing `WHERE consumer_id`, a rollup applied twice. Prefer the test that goes
  red when the quiet failure happens.
- **Ledger assertions read SQLite, not the dashboard API.** The API is not a
  faithful view of every column (`NULL` versus `0`, `in_flight` rows) — see the
  harness doc in `tests/common/mod.rs`.
- **Deterministic, no fixed sleeps.** The harness gates on readiness and uses
  ephemeral ports; keep it that way. `test_config_hash_stability` is the cautionary
  example: it hashes one in-memory instance twice with empty metadata, so it stays
  green while `Config::hash` on two *parses* of a file can differ.
- **Benches are not tests.** `cargo bench --bench ledger_write` reports p50/p95/p99
  commit-ack latency; read the table, do not gate on it.

## Commits

Conventional Commits, enforced by `commitlint.config.mjs` on the `commit-msg`
hook. `<type>(<scope>): <subject>`; the type list and the scope list are in that
file, and the scopes are the module map. Breaking changes add `!` and a
`BREAKING CHANGE:` footer. If a commit was AI-assisted, one trailer per pull
request on the last commit: `Assisted-by: <tool>` or `Generated-by: <tool>`.

Landing a change follows the organisation's gate — issue in this repository,
branch pushed to the remote, draft pull request against the default branch,
signed commits, and the merge queue. [`CONTRIBUTING.md`](CONTRIBUTING.md) has the
flow; the cross-repository rules live in the organisation's own `CLAUDE.md`.

## Known gaps

Real, current, and not to be described as working in prose or docs. The four
entries that used to stand here — an audit that was never called, an
`auto_vacuum` setting that never took effect, a `metadata` map that rehashed on
every reload, and a dashboard that could not authenticate its own stream — were
all fixed; what remains is what is genuinely still true.

- **A database created by an earlier build never gives its space back.** Those
  files have `auto_vacuum = NONE`, which is a property of the file and cannot be
  changed in place: retention deletes rows correctly, but the file does not
  shrink, and `configure_sqlite` says so once at startup. The remedy is a one-off
  `VACUUM` on a database that is not being written. Everything this build creates
  is in `INCREMENTAL` mode and reclaims in bounds (below).
- **Reclamation is bounded per sweep, not complete.** A sweep issues at most
  `MAX_VACUUM_STATEMENTS × VACUUM_PAGES_PER_STATEMENT` (32 MiB at the default page
  size) of `incremental_vacuum`, in chunks, so a large delete is reclaimed over
  several sweeps rather than in one long write transaction that would starve the
  metering writer. `pages_reclaimed` in the sweep's log line is the number to
  read; `0` means the database is not in auto-vacuum mode.
- **The dashboard login is a single key field, not an account system.** The key
  is entered on a login screen, validated against the backend (`/api/me`), and
  held in `localStorage['api_key']`; there is no multi-factor, no password reset
  and no per-consumer authentication beyond the key itself. That is deliberate —
  the key *is* the credential — but "login" here means "present a valid key",
  not "authenticate a user". A standalone deployment rotates its keys through
  `config.yaml` and hot reload, not through the dashboard.
- **A reclaimed ledger is only safe on a local filesystem.** The advisory
  instance lock and `busy_timeout` serialisation assume the database file is on a
  real local disk; the two-instances-write-one-file story in `tests/e2e` is proven
  on one host, and SQLite over NFS or a network volume is not supported.
- **Benchmarks are compiled by CI and never run by it.** `cargo check` and
  `cargo clippy` cover `--all-targets`, so `benches/` and `examples/` cannot rot
  silently, but there is no performance gate: a `cargo bench` regression is caught
  by a person running it, not by a build.
- **`deny.toml` carries one advisory ignore** (RUSTSEC-2026-0009, `time`'s RFC
  2822 parser — unreachable here, and the fix needs a newer toolchain than the
  declared MSRV). It is an ignore with a written reachability argument, not a
  clean report; it disappears when `rust-version` rises.
- **The organisation's module-boundary gate does not run here.** `archkeep`
  judges module boundaries from a Moon project graph, and this repository is a
  plain Cargo workspace with no Moon project — so nothing is being exempted, there
  is simply no graph to judge. Adopting it is a tooling change to this repository
  (Moon, a project file, an attestation), not a line in a workflow, which is why
  it has not been done as part of the product work.
