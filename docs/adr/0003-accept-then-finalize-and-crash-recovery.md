# 0003 — Accept-then-finalize writes, and recovery of stale `in_flight`

Status: accepted

## Context

The proxy's core promise is that a request it served can be accounted for
afterwards — including after the process dies mid-request. A single write at the
end of a request cannot deliver that: between accepting the request and writing
its row, the process can be killed, and the request leaves no trace at all. Under
a metering scheme where "no row" means "no traffic", that is silent under-billing.

The naive fix — write the row up front — introduces its own problem: if the
process dies after that write but before the response, the row stays forever in a
state that is indistinguishable from a request that is genuinely still running.

## Decision

Write the record **twice**:

1. `accept` — INSERT with `request_status = 'in_flight'`, COMMITted **before** the
   upstream is contacted. If this fails, the request is refused with `503`
   (`metering_error`) and never forwarded. This is the awaited, durable half.
2. `finalize` — UPSERT to `completed` / `failed` / `interrupted`, which also
   applies the hourly rollup.

`in_flight` is therefore a *durable* state, and a crash leaves a recoverable
trace instead of a missing row. On the next startup, `recover_in_flight` resolves
every leftover `in_flight` row to `interrupted`, rolls it up as a failure with no
tokens, and stamps `last_recovery_run` — all in one `IMMEDIATE` transaction,
before the listener binds. If recovery fails, the process does not start.

When accept and finalize land in the same batch (the common case under load) the
accept is collapsed away and a single INSERT is issued. This is a pure
optimisation: correctness does not depend on it, and a test asserts the collapsed
path leaves exactly one row.

## Alternatives considered

* **One write, at the end** — rejected: a crash mid-request loses the request
  entirely, which is the failure the product exists to prevent.
* **A write-ahead intent log outside SQLite** — rejected: a second thing that can
  be inconsistent with the ledger, for no benefit over a row in the ledger.
* **Marking interrupted rows lazily, when read** — rejected: it makes every reader
  responsible for fixing data, and leaves the recovery decision to whichever
  query happens to run first.
* **Treating a stale `in_flight` row as `completed`** — rejected outright. Nothing
  observed the request's outcome, so claiming success would be fabrication.
* **Deleting stale `in_flight` rows at startup** — rejected: it would erase the
  evidence that traffic was served.
* **Best-effort recovery (log and continue)** — rejected: starting with an unknown
  set of orphaned records corrupts every usage view and every rollup derived from
  them until the next restart.

## Consequences

* Every request costs at least two ledger writes (one when the batch does not
  collapse).
* `in_flight` is meaningful: it is only ever visible for a live request or a dead
  process's last moments, never as a permanent resting state.
* Recovery re-runs are safe — it only touches rows still `in_flight`, so a second
  pass finds nothing and double-counts nothing.
* Recovery's rollup step runs *before* the status UPDATE, because after the UPDATE
  the rows are no longer selectable as `in_flight`.

## Evidence

* `src/proxy/handler.rs` — `ledger.accept(record)` before `client.proxy(...)`, with
  the 503 `metering_error` path.
* `src/ledger/writer.rs` — `accept` / `finalize` doc comments; `insert_accept`
  (`ON CONFLICT DO NOTHING`); the same-batch collapse in `try_flush`.
* `src/ledger/recovery.rs` — `recover_in_flight`, `INTERRUPT_REASON`,
  `count_in_flight`, and the idempotency test.
* `src/main.rs` — recovery runs before the listener; failure returns an error from
  `main`.
* `src/ledger/schema.sql` — the state `CHECK` constraint and the partial index
  `idx_usage_records_in_flight` that makes the startup scan cheap.
* Tests: `test_accept_then_finalize_persists_terminal_state`,
  `test_single_batch_fast_path_collapses_accept`,
  `test_crash_recovery_end_to_end_with_real_writer`,
  `test_recovery_is_idempotent`.
