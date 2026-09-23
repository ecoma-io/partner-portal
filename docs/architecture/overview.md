# Architecture overview

Two sequences define this system: what happens to a single request, and what
happens when the process stops. Everything else is supporting structure.

Evidence for each claim is a file path in this repository — this document
describes the code as it is, not as it was intended.

## Layout

| Module | Responsibility |
|---|---|
| `src/main.rs` | Composition root: ledger, broadcaster, router, retention task, shutdown sequence |
| `src/config/` | YAML types and defaults, validation, atomic hot reload |
| `src/auth/` | Bearer extraction, server-side consumer identity (`Authenticated` extractor) |
| `src/proxy/` | Upstream client, request handler, metering lifecycle, SSE usage scanner |
| `src/ledger/` | SQLite pool, schema, bounded write queue, crash recovery, retention |
| `src/dashboard/` | Consumer-scoped REST API and the SSE invalidation stream |
| `src/web/` | Embedded dashboard assets and the SPA fallback |
| `src/admin/` | `/healthz`, `/readyz`, `/version` |

## Startup

```
 1. telemetry init                     tracing to stdout, RUST_LOG filter
 2. ConfigLoader::from_file            parse + validate; failure = exit
 3. LedgerPool::new(path)              open, PRAGMAs, CREATE TABLE IF NOT EXISTS
 4. recover_in_flight()                resolve leftover 'in_flight' rows -> 'interrupted'
                                       + roll them up, in one IMMEDIATE transaction
                                       failure = exit (unknown orphans corrupt every view)
 5. LedgerWriter::new                  spawn the single writer task over a bounded queue
 6. HotReloader::start                 hash the file once per second, swap on change
 7. SseBroadcaster::new + start        dedicated poll connection (query_only) reading
                                       PRAGMA data_version
 8. spawn_retention                    first sweep immediately, then every interval
 9. Router build                       admin + dashboard + /v1/* routes, then the SPA fallback
10. TcpListener::bind, axum::serve     with_graceful_shutdown(shutdown_signal)
```

Steps 3–4 happen before the listener opens, so `in_flight` means what it says
from the first request on (`src/main.rs`).

## Request lifecycle

### Non-streaming

```
  request ──▶ Authenticated extractor ──▶ ConsumerContext (consumer_id, key_name)
              key found in the live config snapshot?  no ──▶ 401 (no-store)
  ──▶ Endpoint::from_path(path)  ──▶ None ──▶ 404 JSON
  ──▶ Endpoint::Models ──▶ proxied, not metered, no ledger row
  ──▶ parse body as JSON (tolerated if not JSON: model becomes "unknown")
  ──▶ request_id = UUIDv7
  ──▶ ledger.accept(record)                     ══ COMMIT (in_flight)
        failure ──▶ mark_unhealthy() + 503 metering_error   [request not forwarded]
  ──▶ ProxyClient::proxy() under timeout_secs
        connect/timeout failure ──▶ finalize(failed) ──▶ 502 / 504
  ──▶ status >= 400? ──▶ buffer (capped), extract error message, finalize(failed), relay status
  ──▶ buffer body (cap 32 MiB) ──▶ extract usage ──▶ finalize(completed)
  ──▶ relay status + upstream headers (+ x-request-id)
```

### Streaming

```
  ──▶ ... same accept, same upstream call ...
  ──▶ wants stream (request field) or upstream Content-Type is text/event-stream
  ──▶ StreamMeter::new (armed drop guard) then AxumBody::from_stream:
        per frame:
          idle timeout (timeout_secs) exceeded ──▶ finish_broken
          upstream error                        ──▶ finish_broken
          data frame: measure TTFT once, count bytes, scanner.feed(), yield to client
        upstream EOF ──▶ finish_completed
  ──▶ response headers relayed, body streams; nothing is buffered
```

The meter resolves the record exactly once (`armed` is swapped atomically):

| Ending | Status | Usage |
|---|---|---|
| Upstream stream ended | `completed` | scanned usage, else `unavailable` |
| Upstream broke or went silent | `failed` | whatever was scanned before the break, else `unavailable` |
| Response body dropped (client vanished) | `interrupted` | whatever was scanned; written by a detached task that shutdown awaits |

A `SseUsageScanner` accumulates one event's `data:` lines across chunk
boundaries, ignores `event:`/`id:`/`retry:`/comment lines, skips `[DONE]`, keeps
the **last** usage it sees (the terminal event carries the totals), and abandons
scanning into `truncated` if a single partial event exceeds 256 KiB
(`src/proxy/sse_scan.rs`).

## Metering lifecycle

```
   request_id
      │
      ├─ INSERT (in_flight, usage_status='unavailable')       ── before upstream contact
      │
      └─ UPSERT (completed|failed|interrupted)
            ├─ raw row: status, http_status, tokens, ttft_ms, duration_ms,
            │           usage_status, error_message
            └─ usage_hourly: +1 request_count, token sums, duration,
                             ttft_count when present,
                             success_count | failure_count
            both in ONE transaction, rollup only on in_flight -> terminal
```

`upsert_hourly` sums an unavailable usage as `0` while the raw row keeps `NULL`,
so a total can never absorb an unknown as if it were a real zero
(`src/ledger/writer.rs`).

## Shutdown

```
  SIGTERM / SIGINT
    │
    ├─ shutting_down = true            /readyz -> 503 {"shutting_down":true}
    │
    ├─ sleep shutdown_grace_secs       the balancer needs a poll interval to notice
    │
    ├─ broadcaster.shutdown()          close the dashboard streams (they never end alone)
    │
    ├─ listener stops                  axum drains requests already accepted
    │
    ├─ max(grace, 30 s) elapsed         requests still running are abandoned as `interrupted`
    │
    ├─ retention_stop_tx.send(())      stop the sweep timer
    │
    ├─ ledger.shutdown():
    │     wait for detached finalizes to enqueue   (≤10 s deadline, logged if missed)
    │     shutting_down = true on the writer       (new writers get an explicit error)
    │     drop the producer handle                 (channel closes -> writer drains)
    │     await the writer task                    (final COMMIT has landed)
    │
    ├─ instance.release()              registration + advisory lock removed
    │
    ├─ drop(state)                     pool + client released; the database can close
    │
    └─ exit
```

Both the 30-second drain bound and the dashboard-stream close exist for the same
reason: the drain must run even when the listener will not stop on its own. The
bound is armed by the signal — applying it to the whole serve future instead
would make an idle server exit, cleanly and silently, thirty seconds after it
started (`tests/e2e/shutdown.rs::an_idle_server_does_not_exit_on_its_own`).

Closing the metering producer before the consumer is what makes this safe:
stopping the writer under a live producer would turn a completed request into an
unrecorded one (`src/main.rs`, `src/ledger/writer.rs::shutdown`).

## Data model

Two tables, one source of truth, one derived rollup.

| Table | Role | Written by |
|---|---|---|
| `usage_records` | The raw ledger: one row per accepted request | the writer task, once in `in_flight`, once at the terminal state |
| `usage_hourly` | Derived hourly aggregate keyed by (hour, consumer, model, endpoint, streaming) | same transaction as the terminal raw write |
| `ledger_meta` | Schema version, last retention run, last recovery run | startup, recovery, retention |

Timestamps are stored as fixed-width 30-character UTC strings
(`2026-09-24T07:12:33.123456789Z`) because SQLite compares TEXT byte-wise: only
equal-width strings make lexicographic order equal chronological order. Hour
buckets are 13 characters and the same applies — the dashboard compares a bucket
against hour-granularity bounds, which is why those bounds are rounded rather
than truncated (`src/ledger/timefmt.rs`, `src/dashboard/api.rs`).

Indexes: a partial index on `in_flight` for recovery, `created_at` for retention,
and `(consumer_id, created_at, id)` so the dashboard's keyset pagination is an
index-only range seek rather than a sort (`src/ledger/schema.sql`).

## Readiness model

`/readyz` reports three independent facts and fails if either of the first two
does: shutdown has begun, the ledger writer is accepting and committing, or the
queue is backed up (over half of `queue_size` outstanding). It fails *before*
requests start being refused, which is what makes an instance drain gracefully
instead of erroring (`src/admin/mod.rs`, `src/ledger/writer.rs::is_ready`).

`/healthz` never consults the database. A liveness probe that fails on a
transient dependency turns a slow disk into a restart loop.

## Isolation model

Identity flows one way only:

```
  Authorization: Bearer <local key>
        │
        ├─ exact match against keys[].key in the live config      (src/auth/middleware.rs)
        │      no match ──▶ 401
        │
        ├─ consumer_id = keys[].consumer_id, else keys[].name     (server-side, config-only)
        │
        ├─ ledger row: consumer_id column                          (src/proxy/handler.rs)
        │
        └─ every dashboard query: WHERE consumer_id = ?1           (src/dashboard/api.rs)
```

The SSE stream carries no usage data at all — only "something changed, refetch" —
so a shared notification bus cannot leak one consumer's traffic to another. The
change signal is `PRAGMA data_version` on a dedicated connection, which moves when
*another* connection commits, and therefore also carries across processes
(`src/dashboard/sse.rs`).
