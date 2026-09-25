# 0005 — Raw ledger and hourly rollup written in one transaction

Status: accepted

## Context

The dashboard needs two shapes of the same data: per-request rows (to list and
investigate) and per-hour aggregates (to render totals and timeseries without
scanning a retention window of raw rows). Aggregates that are maintained
separately from their source drift, and a drifted aggregate is worse than a slow
query: it is a number nobody can reconcile.

The request lifecycle also has exactly one unambiguous moment when a request's
contribution becomes final: the transition out of `in_flight`.

## Decision

The rollup is applied in the **same transaction** as the terminal raw write, and
**only on the `in_flight → terminal` transition**.

* `finalize_record` reads the existing status first. If it is already terminal, it
  returns without touching the raw row and without a second rollup — so a
  duplicated finalize cannot double-count.
* Both writes happen inside the batch's single `BEGIN IMMEDIATE … COMMIT`.
* The rollup is upserted on `(hour, consumer_id, model, endpoint, streaming)` with
  additive deltas, so concurrent terminal states in one batch accumulate
  correctly.
* `usage_hourly` is the only aggregate; there is no materialised daily or monthly
  table to fall out of step.
* Retention prunes raw and rollup together, so the invariant survives deletion.

## Alternatives considered

* **A background job that recomputes rollups** — rejected: it introduces a window
  where the two disagree, and a scheduler to own.
* **Aggregating on read (`GROUP BY` over raw rows)** — rejected as the primary
  path: it makes dashboard cost proportional to traffic and defeats the retention
  window, since old raw rows are deleted and their aggregates would go with them.
* **Rolling up on accept as well as finalize** — rejected: accept has no terminal
  outcome and no usage, so it would count requests that later fail.
* **A trigger inside SQLite** — rejected: the logic needs the record's own
  semantics (which status counts as success, whether TTFT exists) and the write is
  already inside one transaction; a trigger would put policy in the schema and
  hide it from anyone reading `writer.rs`.
* **Zeroing unavailable usage into the sums** — rejected, see ADR 0006: the sums
  add `0` via `COALESCE(?, 0)` while the raw row keeps `NULL`, so the aggregate
  stays a sum of *reported* usage.

## Consequences

* Raw and rollup cannot disagree, except through a bug in the single function that
  writes both — which is why there is exactly one such function, and why recovery
  and retention tests reuse it instead of reimplementing it.
* A batch costs one transaction regardless of how many rollup buckets it touches.
* `usage_hourly` is 13-character hour buckets, so the dashboard's rollup-based
  queries round their bounds to the hour, while raw-ledger queries use exact
  bounds (`src/dashboard/api.rs`).
* `check_raw_rollup_consistency` exists as an explicit reconciliation helper
  (terminal rows vs. summed `request_count`); it is exercised by the recovery and
  retention test suites.

## Evidence

* `src/ledger/schema.sql` — header comment: "the raw ledger is the source of
  truth; `usage_hourly` is a derived rollup that is always written in the same
  transaction as its raw row".
* `src/ledger/writer.rs::try_flush` — one `transaction_with_behavior(Immediate)`
  for the whole batch, `tx.commit()` at the end.
* `src/ledger/writer.rs::finalize_record` — the already-terminal early return, the
  upsert, then `upsert_hourly`.
* `src/ledger/writer.rs::finalize_in_tx` — exported so recovery/retention tests
  reuse the production write path.
* `src/ledger/recovery.rs::check_raw_rollup_consistency`.
* Tests: `test_duplicate_finalize_does_not_double_count_rollup`,
  `test_batched_accepts_and_finalizes_all_land` (rollup count equals raw count),
  `test_retention_preserves_raw_rollup_consistency`.
