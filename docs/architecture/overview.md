# Architecture overview

Three sequences define this system: what happens to a single request, what
happens to a billing day, and what happens when the process stops. Everything
else is supporting structure.

Evidence for each claim is a file path in this repository — this document
describes the code as it is, not as it was intended.

## Layout

| Module | Responsibility |
|---|---|
| `src/main.rs` | Composition root: ledger, broadcaster, router, retention and billing tasks, shutdown sequence |
| `src/config/` | YAML types and defaults, validation, atomic hot reload, the SMTP environment credential |
| `src/auth/` | Bearer extraction, server-side consumer identity (`Authenticated` extractor), the consumer scope a query is allowed to widen to |
| `src/apikeys/` | Key hashing and generation, the hashing secret, the `api_keys` store, the in-memory snapshot and its refresher (ADR 0014) |
| `src/billing/` | The commercial layer: pricing, the statement generator, the catch-up walk, the derived service status, the scheduler, the email path, and the partner-facing REST surface (ADR 0015) |
| `src/proxy/` | Upstream client, request handler, metering lifecycle, SSE usage scanner |
| `src/ledger/` | SQLite pool, schema, bounded write queue, crash recovery, retention |
| `src/dashboard/` | Consumer-scoped REST API (plus the one manager widening, ADR 0011/0013) and the SSE invalidation stream |
| `src/web/` | Embedded dashboard assets and the SPA fallback |
| `src/admin/` | `/healthz`, `/readyz`, `/version`, the `ManagerOnly` boundary, the manager-only API-key lifecycle, and the manager-only partner and billing surfaces |

## Startup

```
 1. telemetry init                     tracing to stdout, RUST_LOG filter
 2. ConfigLoader::from_file            parse + validate; failure = exit
 3. LedgerPool::new(path)              open, PRAGMAs, CREATE TABLE IF NOT EXISTS
 4. recover_in_flight()                resolve leftover 'in_flight' rows -> 'interrupted'
                                       + roll them up, in one IMMEDIATE transaction
                                       failure = exit (unknown orphans corrupt every view)
 5. LedgerWriter::new                  spawn the single writer task over a bounded queue
 6. load_secret                        PARTNER_PORTAL_API_KEY_SECRET (>= 32 bytes), from the
                                       environment; missing = exit (ADR 0014)
 7. ApiKeyStore::new + refresher       read the key set, build the in-memory snapshot, then
                                       reload it every api_key_refresh_ms; a read failure =
                                       exit, zero keys = warn and continue
 8. HotReloader::start                 hash the file once per second, swap on change
 9. SseBroadcaster::new + start        dedicated poll connection (query_only) reading
                                       PRAGMA data_version
10. spawn_retention                    first sweep immediately, then every interval
11. spawn_billing                      the daily statement scheduler, over the same single
                                       writer; the SMTP credential is read from the
                                       environment here, never from the config file
12. Router build                       admin + dashboard + /api/billing + /v1/*, then the
                                       SPA fallback
13. TcpListener::bind, axum::serve     with_graceful_shutdown(shutdown_signal)
```

Steps 3–4 happen before the listener opens, so `in_flight` means what it says
from the first request on, and steps 6–7 happen before the router exists, so the
process never serves a request while it cannot answer "who is this?" — a key set
it cannot read is a start-up failure, not an instance that 401s everything
(`src/main.rs`).

## Request lifecycle

### Non-streaming

```
  request ──▶ Authenticated extractor ──▶ ConsumerContext (consumer_id, key_name)
              HMAC of the key in the in-memory key snapshot?  no ──▶ 401 (no-store)
  ──▶ partner suspended?               from the in-memory PartnerRuntimeConfig, derived
                                         from the statement table      ──▶ 403 billing_suspended,
                                         unmetered, upstream never contacted (ADR 0015)
  ──▶ Endpoint::from_path(path)  ──▶ None ──▶ 404 JSON
  ──▶ Endpoint::Models ──▶ proxied, not metered, no ledger row,
                          body filtered to the key's allowed_models (ADR 0012)
  ──▶ parse body as JSON (tolerated if not JSON: model becomes "unknown")
  ──▶ model in the key's allowed_models?  no ──▶ 404 model_not_found, unmetered
  ──▶ model has a price in the snapshot?  no ──▶ 404 model_not_found, unmetered
        (a model with no price is a request that cannot be metered, so it is refused here
         rather than recorded as usage the product could not price)
  ──▶ request_id = UUIDv7
  ──▶ ledger.accept(record)                     ══ COMMIT (in_flight, with the
                                                       price snapshots for this model)
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

The same commit that writes the tokens writes the **price snapshots** —
`input_price_snapshot`, `cached_input_price_snapshot`, `output_price_snapshot` —
for the model the request named, read from the in-memory partner snapshot. They
are written or not written together with the tokens, which is what makes the
statement's arithmetic reproducible later: a statement never looks a price up,
it reads the one the row already carries (`src/proxy/handler.rs`,
`src/ledger/schema.sql`).

## Billing day lifecycle

```
  scheduler tick (billing.scheduler_interval_secs — when the process notices,
                 not the deadline; the deadline is billing.close_delay_minutes)
    │
    ├─ closable_through(now)          yesterday in the configured offset, and only once
    │                                  close_delay_minutes has elapsed
    ├─ per partner:
    │    anchor = MAX(billing_date) for THAT partner, or the day before the
    │             partner's created_at if they have none        [per partner, not global:
    │             a global MAX would step past a partner whose day failed to write
    │             and never retry them]
    │    for each day from anchor to closable_through:
    │      already stated?  ──▶ skip (idempotent; UNIQUE (consumer_id, billing_date)
    │                            makes a duplicate impossible, not merely unlikely)
    │      build the statement from usage_records, joining nothing:
    │         line per (model, input price, cached price, output price) group, so a
    │         mid-day price change stays two lines rather than an average
    │         component_cost(tokens, price) = tokens × price / 10⁶, rounded half away
    │           from zero, once per component; a line total is the sum of its rounded
    │           parts and the statement total is the sum of its lines
    │         a row whose tokens are NULL contributes a COUNT, never a cost
    │      write the statement, its lines, and email_sent_at = NULL in one transaction
    │    walk length bounded by MAX_CATCHUP_DAYS; a longer gap is truncated and says so
    │
    └─ email (invoice partners with email enabled, statement durable either way)
         at-least-once: email_sent_at is the only claim that a message left;
         email_attempts / email_last_error / email_next_retry_at is why it might not have
```

**Suspension is derived, never stored.** `status_for(billing_mode, now, rows)`
takes the partner's contract mode and the statements past their deadline and
answers `active` or `suspended` with the reason. The request path, the partner's
`/api/billing/status`, the manager's summary and the scheduler all call that one
function, so they cannot disagree, and paying an invoice clears a suspension with
no write anywhere. A statement that is incomplete — the product did not account
for its day — or that totals zero is counted as overdue and does not suspend: a
partner does not stop owing the whole ledger the moment they miss a day, and
refusing a paying partner's traffic over `$0.000000` would be the worst possible
reading of "enforce the payment obligation" (`src/billing/status.rs`).

`invoice` and `reconciliation` are different products rather than a flag. An
invoice carries an obligation: a `due_at`, an email, and suspension. A
reconciliation statement has **no** `due_at` — `NULL` is the whole meaning — is
never payable, and a `CHECK` in the schema says so, so a later feature cannot
mark a settlement record unpaid (`src/ledger/schema.sql`,
`src/billing/partner.rs`).

Statements are never deleted by retention. Usage is pruned; a statement outlives
it, which is the whole reason a statement carries its own token counts and prices
(`src/ledger/retention.rs`).

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
    ├─ billing_stop_tx.send(())        stop the scheduler timer; it writes on the same
    │                                   single writer, so it is stopped with the rest
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

Usage in two tables, one source of truth for credentials, one derived rollup, and
the four commercial tables beside them — one file.

| Table | Role | Written by |
|---|---|---|
| `usage_records` | The raw ledger: one row per accepted request, plus the price snapshots frozen onto it | the writer task, once in `in_flight`, once at the terminal state |
| `usage_hourly` | Derived hourly aggregate keyed by (hour, consumer, model, endpoint, streaming) | same transaction as the terminal raw write |
| `api_keys` | The partner key set: keyed hash, prefix, consumer, model list, status, dates (ADR 0014) | the admin surface and `keygen`, never the request path |
| `partners` | The commercial record: one per `consumer_id`, contract mode, terms, billing contact (ADR 0015) | the manager partner surface |
| `partner_models` | A partner's price list: model name and the three per-million prices | replaced atomically by one `PUT` |
| `daily_statements` | One immutable statement per `(consumer_id, billing_date)`, with the total, the incomplete count, and the payment columns | the billing scheduler |
| `statement_lines` | The priced lines a statement is the sum of, each carrying its own snapshot prices and `CHECK` arithmetic | the billing scheduler, with its statement |
| `ledger_meta` | Schema version, last retention run, last recovery run | startup, recovery, retention |

`SCHEMA_VERSION` is 6. The schema is applied on every startup with `CREATE TABLE IF
NOT EXISTS`, so an existing database gains the new tables and columns empty, and
a new binary serves traffic while a previous one is still writing during a rolling
update: a request accepted by an old binary carries `NULL` price snapshots, and a
statement built from it counts that row as incomplete rather than billing it at an
assumed price. Refusing to start on a version the file is not at would make the
rolling update — a supported operation — fail (`src/ledger/schema.sql`,
`src/ledger/mod.rs`).

Timestamps are stored as fixed-width 30-character UTC strings
(`2026-09-24T07:12:33.123456789Z`) because SQLite compares TEXT byte-wise: only
equal-width strings make lexicographic order equal chronological order. Hour
buckets are 13 characters and the same applies — the dashboard compares a bucket
against hour-granularity bounds, which is why those bounds are rounded rather
than truncated (`src/ledger/timefmt.rs`, `src/dashboard/api.rs`).

Money is an integer count of micro-dollars end to end. `MicroUsd` and
`PricePerMillion` have no `f64` anywhere in the path; the decimal string exists
only at the wire and the config edge, and `PricePerMillion::parse` builds the
value digit by digit (`src/billing/pricing.rs`).

Indexes: a partial index on `in_flight` for recovery, `created_at` for retention,
and `(consumer_id, created_at, id)` so the dashboard's keyset pagination is an
index-only range seek rather than a sort (`src/ledger/schema.sql`).

`GET /api/dashboard/timeseries` groups `usage_hourly` by hour and returns the
counts and token sums alongside three raw rollup inputs — `total_duration_ms`,
`total_ttft_ms` and `ttft_count` — rather than a per-hour mean latency or TTFT.
The dashboard buckets those UTC hours into local-time intervals before drawing,
and a mean cannot be re-weighted across a merge; the sums and their weights
(`request_count` for latency, `ttft_count` for TTFT) can, so the interval figures
stay correct at every range. `ttft_count` is zero for an interval in which no
request reported a time to first token, and the client renders a gap there rather
than a zero — the same rule as invariant 3, carried up from the raw row to the
rollup (`src/dashboard/api.rs`, `dashboard/src/views/UsageDashboard.vue`).

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
  Authorization: Bearer <partner key>
        │
        ├─ HMAC-SHA256(secret, key) looked up in the in-memory     (src/auth/middleware.rs)
        │  api_keys snapshot — no SQL on the request path          (src/apikeys/store.rs)
        │      no match, revoked or expired ──▶ 401
        │
        ├─ consumer_id = api_keys.consumer_id                      (server-side, from the row)
        │
        ├─ ledger row: consumer_id column                          (src/proxy/handler.rs)
        │
        ├─ every dashboard query: WHERE consumer_id = ?1           (src/dashboard/api.rs)
        │
        └─ every billing query:    WHERE consumer_id = ?1           (src/billing/api.rs)
```

A `consumers=` parameter widens a query for a **manager** and is ignored for a
partner key. A statement outside the caller's scope is a `404` whose body is
identical for "not yours" and "no such row" — the ids are rowids, so an error
that echoed the requested id back would be an oracle that hands a partner the
size of the table and the day each partner billed. The manager-only surfaces sit
behind `ManagerOnly`, which answers `401` with no credential and `403
manager_required` to a valid partner key, so a partner cannot end its own
suspension with one click and the separation is between roles rather than between
screens (`src/auth/scope.rs`, `src/admin/common.rs`).

The snapshot is rebuilt every `server.api_key_refresh_ms` — that interval is the
bound on how long a sibling instance's revoke, or a key's own expiry, can lag
behind; a mutation this instance performs refreshes it before the response
(ADR 0014).

The one deliberate widening is the optional `manager:` password
(ADR 0011, as widened by ADR 0013). It is matched after the keys, opens
**only** the dashboard routes (never the proxy — `/v1/*` with a manager
password is a 403, so the metering path can never mint a row from it), and
sees **every** consumer: the `consumers=` query parameter is a pure filter
over that view, fed by the selector that `/api/me` fills from the ledger's
distinct `consumer_id`s (a consumer with no terminal row is not offered until
it has usage). A consumer key's `consumers=` parameter is ignored. A
deployment that never writes a `manager:` block keeps the ADR 0008 behaviour
byte-for-byte.

The commercial surface splits along the same line and then draws a second one.
`/api/billing/*` is open to any authenticated credential and answers about the
caller's own `consumer_id` only. `/api/admin/partners` and
`/api/admin/billing/*` are `ManagerOnly`: they create and price a partner,
accept a payment, and read the statement bookkeeping a partner must not see. A
partner key is refused all of them, which is the only thing that keeps
"mark my own statement paid" — one click, and a suspension that was earned ends
— off the table. The partner's own SPA has no payment control to omit later
(`src/billing/api.rs`, `src/admin/billing.rs`, `src/admin/partners.rs`).

The SMTP credential is the same shape of rule one level down: it is
`PARTNER_PORTAL_SMTP_USERNAME` and `PARTNER_PORTAL_SMTP_PASSWORD` in the
environment, read into memory at startup, required as a pair, and never written
to SQLite, never present in YAML, and never rendered by a `Debug`. The config
type has no password field at all, so `Config` cannot become the place one ends
up (`src/config/smtp.rs`).

The SSE stream carries no usage data at all — only "something changed, refetch" —
so a shared notification bus cannot leak one consumer's traffic to another. The
change signal is `PRAGMA data_version` on a dedicated connection, which moves when
*another* connection commits, and therefore also carries across processes
(`src/dashboard/sse.rs`).
