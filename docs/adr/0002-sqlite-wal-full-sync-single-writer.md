# 0002 — SQLite in WAL with `synchronous = FULL`, one writer per process

Status: accepted

## Context

The ledger must survive power loss with a per-request durability promise, run on
the same single VPS as the proxy, and need no operational team. That rules out a
separate database server, and it constrains what SQLite can be trusted to do.

Two requirements pull in opposite directions: every request's terminal write must
be on disk before the response is considered handled (`synchronous = FULL`), and
readers — the dashboard API, the SSE poller, retention — must not block the
writer (WAL).

A rolling update adds a third party: for the overlap window, two instances of
this process hold the same database file open and both write to it.

## Decision

* `PRAGMA journal_mode = WAL` — readers never block the writer.
* `PRAGMA synchronous = FULL` — a COMMIT is durable when it returns. This is the
  setting the whole durability story rests on; the test suite asserts it is active
  (`synchronous == 2`), not merely requested.
* `PRAGMA busy_timeout = 5000` plus application-level retry — contention is
  handled by waiting and retrying, never by failing a write.
* **One writer per process**: a single `Connection` behind a `Mutex`, owned by one
  writer task. Readers get their own connections, opened per read.
* Write transactions use `BEGIN IMMEDIATE`, taking the write lock up front instead
  of upgrading a read snapshot mid-transaction.
* The two-writer model for rolling updates is *tolerated by retry*, not designed
  around: SQLite serialises the two processes' writes, and the loser sees
  `BUSY`/`LOCKED`, which the writer retries with exponential backoff.

## Alternatives considered

* **`synchronous = NORMAL`** — rejected. In WAL mode it can lose the last
  committed transactions on power loss, which is precisely the failure this
  product exists to prevent. Throughput was not the binding constraint.
* **An external database (Postgres, ClickHouse)** — rejected: an extra service to
  run, back up and fail over, for a workload of one row per inference request.
* **`BEGIN DEFERRED`** — rejected: with two processes on one file it can hit
  `SQLITE_BUSY_SNAPSHOT`, where the transaction cannot be upgraded to a write and
  cannot be retried within the same transaction.
* **A lock file or leader election between instances** — rejected as more moving
  parts than the retry loop needs; SQLite already serialises writers correctly.
* **Multiple writer connections in one process** — rejected: it buys nothing
  (`rusqlite` is synchronous and the writes are already batched) and makes
  ordering, batching and shutdown drain much harder to reason about.

## Consequences

* Every terminal write costs an fsync. Batching (ADR 0004/0005) amortises this:
  a batch of 100 records commits once.
* A second instance on the same file is safe but slower under contention; the
  retry loop logs each backoff so this is visible rather than mysterious.
* The database must live on a local disk: WAL needs shared memory, so a network
  filesystem is not supported.
* `busy_timeout` is asserted by a unit test, because silently losing it would
  turn a slow disk into failed commits.

## Evidence

* `src/ledger/mod.rs::configure_sqlite` — the pragma batch, including
  `journal_mode = WAL` and `synchronous = FULL`.
* `src/ledger/mod.rs` tests — `test_synchronous_full_and_wal_are_active`,
  `test_busy_timeout_is_set`.
* `src/ledger/pool.rs` — `writer()` hands out the single `Arc<Mutex<Connection>>`;
  `reader()` opens a fresh connection per use.
* `src/ledger/writer.rs` — `LedgerWriter::new` spawns exactly one writer task;
  `try_flush` uses `TransactionBehavior::Immediate`.
* `src/ledger/writer.rs` — `BUSY_RETRIES` / `BUSY_BACKOFF_BASE` and the retry loop
  in `flush_with_retry`, with the same-VPS rolling update named as the reason.
* `README.md` — "Deployment" records the local-disk requirement.
