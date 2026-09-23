# Partner Portal

A lightweight OpenAI-compatible reverse proxy with durable SQLite metering, local
API-key authentication, YAML hot reload and an embedded Vue dashboard.

One upstream, three proxied endpoints, one SQLite file as the source of truth. It
exists to answer a single question — *which consumer spent how many tokens on
which model, and can I prove it after a crash?* — and to answer it without
dropping, guessing or fabricating a single record.

Status: pre-release (`0.1.0`), single-VPS deployment target.

## What it is, and what it deliberately is not

| It is | It is not |
|---|---|
| A reverse proxy in front of **one** OpenAI-compatible upstream | A general-purpose LLM gateway: there is no routing, no provider failover, no model→provider mapping |
| A durable usage ledger (SQLite, WAL, `synchronous = FULL`) | A billing or pricing system: it records tokens and counts, never cost |
| A local API-key authenticator that replaces the credential upstream | An identity provider, OIDC client or rate limiter |
| An incremental streaming proxy (frames forwarded as they arrive) | A body transformer: requests and responses pass through verbatim |
| A per-consumer usage dashboard | A multi-tenant admin console: no cross-consumer view exists |
| A single static binary with the dashboard embedded | A TLS terminator: the listener is plain HTTP — put it behind something that speaks TLS |

Three paths are proxied: `/v1/chat/completions`, `/v1/responses`, `/v1/models`.
Anything else under `/v1/` is a JSON `404`; there is no embeddings, audio, image
or batch surface.

## Request path

```
                       ┌──────────────────────────── axum router ────────────────────────────┐
 client                │                                                                     │
   │  Bearer <local    │  /healthz  /readyz  /version        unauthenticated, no usage data   │
   │  key>             │                                                                     │
   ├──────────────────▶│  /v1/chat/completions   /v1/responses   /v1/models                   │
   │                   │        │                                                            │
   │                   │        ├─ Authenticated extractor ──▶ ConsumerContext                 │
   │                   │        │     key looked up in the live config snapshot;               │
   │                   │        │     identity derived server-side, never from the request      │
   │                   │        │                                                            │
   │                   │        ├─ Endpoint::from_path ──▶ 404 for anything else               │
   │                   │        │                                                            │
   │                   │        ├─ /v1/models ──▶ proxied, NOT metered (discovery traffic)    │
   │                   │        │                                                            │
   │                   │        ├─ ledger.accept()  ═══ durable COMMIT (request_status=       │
   │                   │        │                     'in_flight')  ── 503 if it fails        │
   │                   │        │                                                            │
   │                   │        ├─ ProxyClient::proxy() ──────▶ the single upstream           │
   │                   │        │     strip hop-by-hop + Host, replace Authorization,         │
   │                   │        │     drop Accept-Encoding (usage must stay readable)         │
   │                   │        │                                                            │
   │                   │        ├─ non-streaming: buffer ≤ 32 MiB → extract usage            │
   │                   │        │                → ledger.finalize()  ═══ durable COMMIT      │
   │                   │        │                                                            │
   │                   │        └─ streaming: forward each frame immediately, scanning it      │
   │                   │                        with SseUsageScanner → ledger.finalize()      │
   │                   │                        (or the drop guard, if the client vanishes)   │
   │                   │                                                                     │
   │                   │  /api/me   /api/dashboard/{summary,timeseries,requests,models,       │
   │                   │                                    events}                          │
   │                   │        │  every query scoped to the authenticated consumer_id        │
   │                   │                                                                     │
   │                   │  fallback: embedded SPA for pages, JSON 404 for /api/* and /v1/*     │
   └───────────────────┴─────────────────────────────────────────────────────────────────────┘
```

Every response carries `x-request-id`, the UUIDv7 that identifies the ledger row.
That includes the errors this proxy generates itself — a 502 from an unreachable
upstream is the case where the id is worth most, because the ledger row and the
log line exist and the response is the caller's only route to either.

## Endpoints

| Route | Auth | Metered | Notes |
|---|---|---|---|
| `ANY /v1/chat/completions` | Bearer | yes (`chat_completions`) | Streaming and non-streaming. Any method is forwarded as-is |
| `ANY /v1/responses` | Bearer | yes (`responses`) | Streaming and non-streaming |
| `ANY /v1/models` | Bearer | **no** | Discovery traffic consumes no tokens; metering it would add zero-usage noise to every rollup |
| `GET /api/me` | Bearer | — | `consumer_id` and `key_name` for the presented key |
| `GET /api/dashboard/summary` | Bearer | — | Totals for a window, read from the hourly rollup |
| `GET /api/dashboard/timeseries` | Bearer | — | Per-hour buckets for a window, read from the hourly rollup |
| `GET /api/dashboard/requests` | Bearer | — | Keyset-paginated raw rows (max 200 per page) |
| `GET /api/dashboard/models` | Bearer | — | Distinct models this consumer used in the window |
| `GET /api/dashboard/events` | Bearer | — | SSE invalidation stream (`{"type":"data_changed"}`), no usage data |
| `GET /healthz` | none | — | Liveness |
| `GET /readyz` | none | — | Readiness — 503 while shutting down or while the ledger is degraded |
| `GET /version` | none | — | Version, commit, build time, schema version |

Errors are OpenAI-shaped: `{"error":{"message":…,"type":…}}`, with
`"code":"invalid_api_key"` on a rejected credential and `"code":"not_found"` on
an unknown `/v1/*` or `/api/*` path. Auth failures are returned with
`Cache-Control: no-store`.

## Quick start

```bash
# 1. Build. The dashboard is embedded at compile time; without a built
#    dashboard/dist the binary still builds and serves an explanation page.
cargo build --release

# 2. Configure.
cp config.example.yaml config.yaml
$EDITOR config.yaml          # set upstream.base_url, upstream.api_key and at least one local key

# 3. Run.
PARTNER_PORTAL_CONFIG=config.yaml ./target/release/partner-portal

# 4. First request.
curl -s localhost:8080/healthz
curl -s -H "Authorization: Bearer <your-local-key>" \
     -H 'content-type: application/json' \
     -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hello"}]}' \
     http://localhost:8080/v1/chat/completions

# 5. Streaming: -N keeps the response incremental.
curl -sN -H "Authorization: Bearer <your-local-key>" \
     -H 'content-type: application/json' \
     -d '{"model":"gpt-4o","stream":true,"messages":[{"role":"user","content":"hello"}]}' \
     http://localhost:8080/v1/chat/completions
```

### Dashboard

```bash
pnpm --dir dashboard install
pnpm --dir dashboard build     # writes dashboard/dist
cargo build --release          # re-embed the new bundle
```

Then open `http://localhost:8080/`. The bundle is served with a restrictive
`Content-Security-Policy` (`default-src 'self'`, no inline scripts, no framing)
and content-hashed assets are cached immutably while `index.html` is `no-cache`.

The dashboard starts at a **login screen**: enter the local API key assigned to
your consumer. The key is presented to the running server as a Bearer credential
and validated against it before anything else renders — no key, no dashboard. A
rejected key shows the reason and stays on the login screen; the key itself is
never sent anywhere except in the `Authorization` header (never in a URL), and
is retained in this browser's `localStorage` only for the life of the session
you are viewing. **Sign out** clears it and returns to the login screen.

* The API is authenticated by an `Authorization` header, and so is the event
  stream. The bundled client therefore does not use `EventSource`, which cannot
  set a header: it reads `/api/dashboard/events` with `fetch` and parses the
  frames itself, so the "Live" badge works in the shipped bundle. A rejected or
  missing credential is reported as `unauthenticated` and is not retried —
  retrying a credential that cannot succeed is a 401 storm. The same stream is
  readable by anything that can send the header:
  `curl -N -H "Authorization: Bearer <key>" http://localhost:8080/api/dashboard/events`.
* The dashboard is a read-only view of the consumer the key belongs to. There is
  no parameter anywhere in the API that widens that scope, and the model filter
  is built from `/api/dashboard/models` — the models that key's own traffic
  actually used, never a hardcoded list.

## Configuration

YAML, loaded from `PARTNER_PORTAL_CONFIG` or `./config.yaml`. It is re-read once
per second and hashed on every tick (never on mtime, which editors and coarse
filesystems make unreliable); a change that parses and validates is swapped in
atomically, and one that does not is logged and the previous configuration stays
in force.

[`config.example.yaml`](config.example.yaml) is the complete commented reference,
including every default. Summary:

| Setting | Default | Takes effect |
|---|---|---|
| `server.listen` | `0.0.0.0:8080` | restart |
| `server.graceful_shutdown` | `true` | restart |
| `server.shutdown_grace_secs` | `5` | restart |
| `server.max_body_size` | `10485760` (10 MiB) | partly: the body-limit layer is built at startup, the per-request read follows the snapshot |
| `server.cors_allow_origins` | `[]` (no CORS headers) | restart |
| `server.sse_poll_interval_ms` | `500` | restart |
| `upstream.base_url` | — (required, `http://`/`https://`) | live |
| `upstream.api_key` | — (required, non-empty) | live |
| `upstream.timeout_secs` | `120` | live |
| `upstream.connect_timeout_secs` | `10` | restart |
| `keys[].key` | — (required, unique, non-empty) | live |
| `keys[].name` | — (required, non-empty) | live |
| `keys[].consumer_id` | falls back to `name` | live |
| `keys[].metadata` | `{}` | live |
| `database.path` | `partner-portal.db` | restart |
| `database.retention_days` | `60` | restart for the sweep; live for the dashboard's window validation |
| `database.queue_size` | `10000` | restart |
| `database.batch_size` | `100` | restart |
| `database.batch_timeout_ms` | `1000` | restart |
| `database.retention_interval_secs` | `3600` (min 60) | restart |
| `database.retention_batch_size` | `2000` | restart |

`upstream`, `keys` and each key's `key`/`name` are required; the server refuses to
start with no keys, a duplicate key value, or a malformed upstream URL. Never
logged: key values are never emitted — the reloader reports *that*
`upstream.api_key` changed, never what it changed to.

## Accounting and durability

Every guarantee below is enforced in one place and covered by tests in the same
file. Note what this means for the common case: a metering failure is never
silent, and it is never resolved by guessing.

**An accepted request is already durable.** The ledger row is written as
`in_flight` and COMMITted *before* the upstream is contacted. If that write
fails, the request is refused with `503` and `"type":"metering_error"` instead of
being served — an inference request this proxy cannot account for is not worth
serving (`src/proxy/handler.rs`, `LedgerWriter::accept` in `src/ledger/writer.rs`).

**Metering is never dropped.** Producers write into a bounded queue and *await*
capacity when it is full; nothing is discarded on saturation. SQLite
`BUSY`/`LOCKED` failures — which are legitimate during a same-VPS rolling update,
when two instances share the file — are retried six times with exponential
backoff rather than dropped. Every record is acknowledged only after its
transaction has committed, with `synchronous = FULL`, so "finalized" always means
"on disk" (`src/ledger/writer.rs`).

**Every accepted request reaches a terminal state.** `in_flight` resolves to
`completed`, `failed` or `interrupted` — including when the client disconnects
mid-stream (a drop guard hands the final write to a task that shutdown waits
for), and including when the process dies (rows still `in_flight` at the next
startup are resolved to `interrupted` and rolled up). Recovery is not
best-effort: if it fails, the process refuses to start, because an unknown set of
orphaned records would corrupt every usage view until the next restart
(`src/ledger/recovery.rs`, `src/proxy/handler.rs::StreamMeter`).

**Unavailable is not zero.** The provider is the only source of token counts.
When a response carries no usage — a truncated stream, a cap hit, a provider that
simply omits it — the columns stay `NULL` and `usage_status` says
`unavailable` or `partial`. A `CHECK` constraint rejects negative tokens, and the
rollup sums missing usage as `0` while the raw row keeps `NULL`, so no total can
silently absorb an unknown
(`src/proxy/usage.rs`, `src/proxy/sse_scan.rs`, `src/ledger/schema.sql`).

**Raw and rollup agree.** The terminal write and its `usage_hourly` rollup happen
in a single transaction, and the rollup is applied only on the
`in_flight → terminal` transition, so a duplicated finalize cannot
double-count. Crash recovery applies the same rule to the rows it resolves
(`src/ledger/writer.rs::finalize_record`, `src/ledger/recovery.rs`).

**Streaming stays incremental.** Response frames are forwarded to the client the
moment they arrive; the body is never collected to recover usage. Usage is
recovered by scanning frames in flight, with the partial-event buffer capped at
256 KiB — an upstream that never terminates an event is recorded as
`unavailable`, not buffered forever. Time-to-first-token is measured at the first
data frame (`src/proxy/handler.rs`, `src/proxy/sse_scan.rs`).

**The dashboard cannot see across consumers.** Consumer identity is derived
server-side from the presented credential and the live config, and every
dashboard query is filtered by that value. No request field contributes to
identity, so there is nothing for a client to tamper with
(`src/auth/middleware.rs`, `src/dashboard/api.rs`).

## Operations

### Readiness and liveness

`/healthz` is liveness only — it deliberately does not consult the database,
because a restart does not fix a slow disk and a liveness probe that fails on a
transient dependency causes a restart loop.

`/readyz` is the one to wire into a load balancer. It returns `200` only while
the metering pipeline can keep up, and reports its reasoning:

```json
{"ready":true,"ledger_ready":true,"shutting_down":false,"ledger_queue_depth":0,"ledger_committed":10}
```

Readiness fails once more than half the queue is outstanding, once a commit has
failed permanently, or once shutdown has begun — so an instance leaves rotation
*before* it starts rejecting requests. It is not restored automatically: a
process that has lost a metering write has unaccounted traffic, and only a
restart with recovery resolves that. `ledger_committed` counts records, not
requests: each request contributes up to two (accept, finalize), and they
collapse into one when they land in the same batch.

### Shutdown

```
SIGTERM / SIGINT
  → shutting_down = true          readiness now 503; the balancer stops routing
  → wait shutdown_grace_secs      time for the balancer to notice (readiness polls)
  → close the dashboard streams   an SSE body never ends on its own
  → stop accepting connections    axum drains requests already in flight
  → within max(grace, 30s)        requests still running are abandoned here
  → drain the metering pipeline   flush and COMMIT every queued record
  → await detached finalizers     stream drop guards that could not await
  → release the instance          registration + advisory lock
  → close the writer, drop the pool
```

The order is load-bearing: the metering producer is stopped before the consumer,
because closing the writer under a live producer turns a completed request into
an unrecorded one. Draining is not optional and has no setting — only the
listener's behaviour is configurable (`src/main.rs`; `docs/architecture/overview.md`).

The 30-second bound starts at the **signal**, not at startup, and it bounds only
the drain. A request abandoned at it keeps its row: it stays `in_flight` and the
next start's recovery resolves it to `interrupted`. Recorded as a failure, never
silently dropped — and the process still commits everything already finalized
before it exits.

### Retention

A sweep every `retention_interval_secs` deletes raw rows and rollup buckets older
than `retention_days`, in bounded slices that commit individually and release the
write lock between slices — a single `DELETE` over 60 days would hold the write
lock for its whole duration and stall metering into its queue. A sweep is capped
at a fixed number of slices; hitting the cap is logged and the next sweep
continues. `retention_days: 0` clamps to one day rather than wiping the ledger.

Space is reclaimed after a sweep that deleted something, incrementally and in
bounds: the database is created with `auto_vacuum = INCREMENTAL`, and a sweep
issues at most 32 MiB of `PRAGMA incremental_vacuum` in 256 KiB statements, so a
large delete is given back over several sweeps instead of in one long write
transaction that would stall metering. The sweep's log line reports
`pages_reclaimed` and `bytes_reclaimed`.

A full `VACUUM` is never run: it rewrites the whole file and needs an exclusive
lock plus up to 2× the file size in free space. Two things follow. Disk must be
planned for the high-water mark of a retention window, not for the steady state —
reclamation lags deletion by design. And a database **created by an earlier
build** is in `auto_vacuum = NONE`, which cannot be changed in place: retention
deletes its rows, the file never shrinks, and startup says so once. A one-off
`VACUUM` on an idle database is the remedy (`src/ledger/retention.rs`).

### Logs

`tracing` to stdout, filtered by `RUST_LOG` (`partner_portal=info` and
`tower_http=info` by default). Config reloads report which fields moved; key
values and upstream credentials are never logged. `SetSensitiveHeadersLayer`
marks `Authorization` unrenderable before anything inside the stack can log it.

## Deployment

The intended shape is one binary, one YAML file and one SQLite file on a local
disk, behind a load balancer that polls `/readyz` and sends `SIGTERM` on
drain-out. `WAL` mode needs shared memory, so the database cannot live on a
network filesystem. Rolling updates work because of two design choices: readiness
fails before the listener closes, and the second instance's writes contend for the
same database file in a way the writer retries rather than fails.

### Releasing

A release is one merge. `.github/workflows/release.yml` runs Release Please on
every push to `main`, which keeps a Release PR open that accumulates the
Conventional Commits since the last release, carries the `Cargo.toml` /
`Cargo.lock` version bump and the `CHANGELOG.md` entry, and tags `vX.Y.Z` when it
is merged. The rest of that workflow is gated on the release existing: it builds
the dashboard once, compiles both architectures, builds and pushes the image by
digest, and attaches the tarballs, `SHA256SUMS`, the SBOM and the image digest to
the GitHub Release. Versioning and publishing are deliberately one workflow —
a tag written by the default token cannot trigger another one.

The version number is derived from the commits, not chosen: `fix:` is a patch,
`feat:` a minor, `feat!:` or a `BREAKING CHANGE:` footer a major. So the
Conventional Commit rules in [`CONTRIBUTING.md`](CONTRIBUTING.md) are not only a
style gate — they are the input to the release.

### Deployment fixtures

[`deploy/`](deploy/) holds a runnable stack rather than only a description:

| Path | What it is |
|---|---|
| [`docker-compose.yml`](docker-compose.yml) | One instance, one ledger, no edge — for a machine with no orchestrator |
| [`deploy/docker-compose.yml`](deploy/docker-compose.yml) | Two instances over one shared ledger plus an nginx edge, the rolling-update shape |
| [`deploy/config/`](deploy/config/) | The config files that stack mounts, committed with placeholder credentials |
| [`deploy/nginx/`](deploy/nginx/) | The edge config: an upstream file that is a one-line edit to change rotation, no retries, no buffering |
| [`deploy/smoke-test.sh`](deploy/smoke-test.sh) | Drives real traffic through the stack and checks accounting from three independent directions — the upstream's counter, the ledger via the dashboard API, and each instance's committed count |

Read the comments in those files before adapting them: they state the operational
contract an orchestrator has to satisfy ([ADR 0002](docs/adr/0002-sqlite-wal-full-sync-single-writer.md)
and [ADR 0004](docs/adr/0004-bounded-queue-backpressure-not-drop.md)), and the one
thing a naive rolling update gets wrong (recovery is global — a starting instance
resolves the live peer's in-flight rows). TLS is not implemented in the binary;
the edge or your own load balancer terminates it.

## Development

```bash
cargo build                        # debug binary
cargo test --lib                   # unit tests (live alongside each module)
cargo test --test integration      # real binary + mock upstream, ledger read back
cargo test --test e2e -- --test-threads=1   # signals, rolling update; run serially
cargo clippy --all-targets -- -D warnings
cargo fmt --all -- --check
cargo bench --bench ledger_write   # commit-ack p50/p95/p99

pnpm --dir dashboard install
pnpm --dir dashboard dev           # vite dev server, proxies /api and /v1 to :8080
pnpm --dir dashboard typecheck
pnpm --dir dashboard lint
pnpm --dir dashboard build
```

`AGENTS.md` states the invariants a change must not break; `CONTRIBUTING.md`
covers the gates, the commit conventions and how to add an endpoint or change the
schema. Architecture decisions are recorded in `docs/adr/`.

## Documentation

| Document | Contents |
|---|---|
| [`docs/README.md`](docs/README.md) | Index |
| [`docs/architecture/overview.md`](docs/architecture/overview.md) | Request lifecycle and shutdown sequence as text diagrams |
| [`docs/adr/`](docs/adr/) | Architecture Decision Records |
| [`AGENTS.md`](AGENTS.md) | Rules for coding agents working in this repository |
| [`CONTRIBUTING.md`](CONTRIBUTING.md) | Gates, commits, PR flow |
| [`SECURITY.md`](SECURITY.md) | Supported versions and private vulnerability reporting |
| [`CHANGELOG.md`](CHANGELOG.md) | What has changed; Keep a Changelog format |
| [`CODE_OF_CONDUCT.md`](CODE_OF_CONDUCT.md) | Contributor Covenant 2.1 |
| [`config.example.yaml`](config.example.yaml) | Complete commented configuration reference |

## Licence

Apache-2.0. See [`LICENSE`](LICENSE).
