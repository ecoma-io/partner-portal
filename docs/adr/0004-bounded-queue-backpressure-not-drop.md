# 0004 — Bounded queue with backpressure, never drop; readiness degrades on persistence failure

Status: accepted

## Context

The ledger writer is a single task committing to SQLite. Request handlers produce
records at whatever rate traffic arrives. Three ways to bridge the two: an
unbounded buffer, a bounded buffer that drops when full, or a bounded buffer that
makes producers wait.

An unbounded buffer converts a slow disk into unbounded memory growth and then an
OOM kill. Dropping on overflow converts a slow disk into silent under-billing,
which is worse than failing: nobody notices. Waiting converts a slow disk into
slower requests, which is honest and recoverable.

There is also the question of what a load balancer should see. If an instance
waits until its queue is completely full and *then* becomes unready, requests
already routed to it are refused; the failure is emitted by the instance, not
avoided.

## Decision

* The queue is a **bounded `mpsc` channel** of `database.queue_size` records.
  Producers `await` capacity. Records are never discarded on saturation, and a
  producer that cannot enqueue gets an explicit error, not a silent drop.
* The writer pulls **at most** `batch_size` records per iteration. Anything beyond
  that stays in the channel — the bound has to be on the channel, not on a local
  buffer, or it is not a bound at all.
* `BUSY`/`LOCKED` failures are retried with exponential backoff (6 attempts, 20 ms
  doubling). Only a permanent failure surfaces to callers.
* Any permanent commit failure calls `mark_unhealthy()`: **readiness latches
  false and is not restored automatically**. A process that lost a metering write
  has unaccounted traffic, and only a restart (with recovery) resolves that.
* Readiness fails when **more than half** the queue is outstanding — queued plus
  in-batch — so that an instance leaves rotation *before* it starts refusing work.
* The shutdown path is the one exception: a drop guard that cannot await uses
  `try_finalize`, which returns `QueueFull` instead of blocking, so process exit
  cannot hang. The failure is logged and degrades readiness rather than being
  swallowed.

## Alternatives considered

* **Unbounded channel** — rejected: turns a persistence problem into an OOM.
* **Drop-oldest / drop-newest on overflow** — rejected: silent loss of billable
  events. If the product cannot account for a request, it must say so.
* **Rejecting requests at the HTTP layer when the queue is full** — rejected as
  the primary mechanism: backpressure is preferred because the request will
  succeed, just later, and the queue's purpose is to absorb bursts. Readiness is
  what prevents the queue from ever filling in a well-run deployment.
* **Blocking the runtime thread to enqueue** — rejected: the writer is the only
  blocking consumer; producers are async and must yield.
* **Auto-recovering readiness after a transient failure** — rejected: the record
  already accepted may or may not have landed, so "healthy again" would be a claim
  the process cannot support.
* **Draining without a deadline at shutdown** — rejected: a stuck database would
  hang process exit forever. The detached-drain wait is bounded (10 s) and logs
  when it expires.

## Consequences

* Under sustained overload, requests get slower and `/readyz` goes red; the load
  balancer moves traffic away and the queue drains.
* A failed commit is visible twice: as an error log naming the real SQLite error,
  and as a red readiness probe until restart.
* Loss is impossible-by-construction except in the explicit, logged drop-guard
  path — which is itself tracked, awaited at shutdown, and counted
  (`detached_pending`).
* `queue_size` becomes a memory/latency dial with a real meaning: the number of
  records that can be in flight through the pipeline.

## Evidence

* `src/ledger/writer.rs` — module docs, `enqueue` (awaits capacity),
  `try_finalize`, `spawn_detached_finalize`, `mark_unhealthy`, `is_ready`,
  `queue_depth`, `shutdown`'s detached-drain deadline, `flush_with_retry`.
* `src/ledger/writer.rs` — readiness computed as
  `commits_are_healthy && depth < config.queue_size / 2`.
* `src/admin/mod.rs::readyz` — `503` unless `!shutting_down && ledger_ready`.
* `src/proxy/handler.rs` — `accept` failure ⇒ 503 `metering_error`;
  every finalize failure calls `mark_unhealthy`.
* Tests: `test_bounded_queue_applies_backpressure_not_loss`,
  `test_readiness_degrades_when_queue_backs_up`,
  `test_shutdown_drains_queue_before_stopping`.
