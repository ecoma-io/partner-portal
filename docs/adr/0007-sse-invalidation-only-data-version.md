# 0007 — SSE as invalidation only, `PRAGMA data_version` as the change signal

Status: accepted

## Context

The dashboard should update as traffic arrives, and it should do so correctly in
the deployment this product targets: two instances during a rolling update, one
shared SQLite file. Two questions had to be answered together, because the answer
to one constrains the other.

*What does the stream carry?* A stream that carries data is a second read path
that must be authenticated, scoped and kept consistent with the REST API.

*What triggers a notification?* An in-process event bus is the obvious answer,
and it is wrong here: instance B would never notify its clients about instance A's
writes, so a consumer's view would depend on which instance their browser landed
on.

## Decision

* SSE carries **invalidation only**: `{"type":"data_changed"}`. No usage figures,
  no counts, no identifiers. Every refetch goes through the consumer-scoped REST
  API.
* The change signal is `PRAGMA data_version` on a **dedicated** connection that
  never writes (`query_only = ON`), polled every `server.sse_poll_interval_ms`.
  `data_version` moves when *another* connection commits, which is what makes it
  work across processes.
* SQLite is the single source of truth. There is no cache in the notification
  path, so a missed notification costs staleness, never wrongness.
* Each subscriber gets a bounded broadcast channel (128 notifications); a lagged
  subscriber receives one synthetic change event, which is sufficient because the
  payload is only an instruction to refetch.
* The poller logs an error when the pragma cannot be read, rather than returning a
  constant — a constant would silently stop all invalidation.

## Alternatives considered

* **A per-consumer filtered stream** — rejected: it would put usage data in the
  event payload, and it would make the event bus a place where isolation can be
  broken. Isolation now lives in one place (the SQL query layer, ADR 0008).
* **In-process broadcast on every committed write** — rejected: it cannot see the
  other instance's commits, so an SSE-driven dashboard would under-report during a
  rolling update — exactly when an operator is watching it.
* **SQLite `update_hook` / triggers writing to a queue table** — rejected: another
  write, another table, more schema; `data_version` already answers "did anything
  change".
* **Polling the REST API from the client instead** — the client may still poll; the
  stream exists to make that unnecessary, not mandatory.
* **File mtime polling on the database** — rejected: unreliable under WAL (writes
  go to the `-wal` sidecar, and the main file's mtime lags).
* **Long-polling a REST endpoint** — rejected: the SSE stream is simpler to
  implement correctly with axum's `Sse`, and it has a standard reconnect hint.

## Consequences

* A browser that misses events (lag, reconnect, network blip) converges on the next
  event; correctness does not depend on delivery.
* Two instances each poll their own connection and notify their own clients, so
  rolling updates need no coordination between them.
* One extra idle connection per process, reading one pragma per interval.
* Polling means up to one interval of latency before a change is announced — 500 ms
  by default.
* The stream is identical for every subscriber, which is what makes it safe to
  share: there is nothing in it to leak.

## Evidence

* `src/dashboard/sse.rs` — module docs ("What this channel is, and is not",
  "Why `PRAGMA data_version`"), the `query_only` poll connection, `get_data_version`
  (error ⇒ log + `-1`), `sse_response` (payloads are only `connected` and
  `data_changed`), `SSE_BUFFER`, `KEEPALIVE_INTERVAL`, `RETRY_HINT_MS`.
* `src/dashboard/sse.rs` tests — `test_data_version_changes_across_connections`,
  `test_poll_connection_sees_its_own_writes_are_not_the_signal`,
  `test_broadcast_fans_out_to_every_subscriber`, `test_no_event_when_nothing_changes`.
* `src/main.rs` — the broadcaster is constructed from the database path with
  `server.sse_poll_interval_ms`, and started before the listener binds.
* `src/dashboard/mod.rs` — `/api/dashboard/events` is the only SSE route, and it
  requires an authenticated key.
