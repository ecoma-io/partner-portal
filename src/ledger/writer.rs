//! Ledger writer: bounded queue, micro-batching, single writer, durable acks.
//!
//! # Durability contract
//!
//! * Every record is acknowledged **only after** its transaction has COMMITted
//!   (`synchronous = FULL` in WAL mode). Callers await the ack, so "metering
//!   finalized" always means "durable".
//! * The queue is **bounded**. When it is full, [`LedgerWriter::write`] awaits
//!   capacity rather than dropping the record — backpressure, never loss.
//! * A full batch is written in **one transaction** containing both the raw row
//!   and its hourly rollup, so raw and aggregate can never disagree.
//! * Busy/locked failures are retried with exponential backoff instead of being
//!   discarded, because during a same-VPS rolling update two instances legitimately
//!   contend for the same database file.
//! * Transaction failures are reported with the **real** `rusqlite::Error`.
//!   Errors are never substituted with a synthetic placeholder, so a caller that
//!   decides to degrade readiness is reacting to the true cause.
//!
//! # Lifecycle
//!
//! A request is written twice: [`LedgerWriter::accept`] (status `in_flight`)
//! before the upstream is contacted, then [`LedgerWriter::finalize`] with the
//! terminal state. If both land in the same batch the accept is collapsed away
//! and a single INSERT is issued — the common case under load.

use crate::config::DatabaseConfig;
use crate::ledger::rollup::{BucketKey, Contribution, StoredRow};
use crate::ledger::{RequestRecord, RequestStatus};
use parking_lot::Mutex;
use rusqlite::{Connection, TransactionBehavior};
use std::collections::HashSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::{mpsc, oneshot, watch};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

/// Retries (beyond the first attempt) when SQLite reports BUSY/LOCKED.
const BUSY_RETRIES: u32 = 6;
/// Base backoff for the busy retry loop; grows 2x each attempt.
const BUSY_BACKOFF_BASE: Duration = Duration::from_millis(20);
/// How long the writer sleeps when there is nothing queued (readiness tick).
const IDLE_TICK: Duration = Duration::from_millis(250);
/// How long shutdown waits for detached (drop-guard) writes to enqueue.
const DETACHED_DRAIN_TIMEOUT: Duration = Duration::from_secs(10);

/// Write error types
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    /// The queue is saturated. Callers block on `send`, so this is only
    /// surfaced on the non-blocking path (see [`LedgerWriter::try_accept`]).
    #[error("ledger queue is at capacity")]
    QueueFull,

    #[error("ledger writer is shut down")]
    Shutdown,

    /// The real SQLite error, shared so it can be handed to every waiter in a
    /// failed batch without losing fidelity or requiring `Clone`.
    #[error("SQLite error: {0}")]
    Sqlite(Arc<rusqlite::Error>),
}

impl From<rusqlite::Error> for WriteError {
    fn from(e: rusqlite::Error) -> Self {
        WriteError::Sqlite(Arc::new(e))
    }
}

/// Which lifecycle transition a queued write represents.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WriteOp {
    /// Insert the record as `in_flight`. Never touches the rollup.
    Accept,
    /// Upsert the record to its terminal state and roll it up, exactly once.
    Finalize,
    /// No-op marker whose ack proves everything queued before it is committed.
    /// Carries no record.
    Barrier,
}

/// Ledger writer configuration
#[derive(Debug, Clone)]
pub struct LedgerWriterConfig {
    pub queue_size: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    /// Owning instance, recorded on every accepted row so that recovery on
    /// another instance can tell a stranded request from a live sibling's.
    ///
    /// `None` is the pre-ownership behaviour and is only appropriate for a
    /// single-process database. A production instance always sets it — see
    /// [`crate::ledger::instance`] for what a missing owner costs.
    pub instance_id: Option<String>,
}

impl From<DatabaseConfig> for LedgerWriterConfig {
    fn from(config: DatabaseConfig) -> Self {
        Self {
            queue_size: config.queue_size,
            batch_size: config.batch_size,
            batch_timeout_ms: config.batch_timeout_ms,
            instance_id: None,
        }
    }
}

impl Default for LedgerWriterConfig {
    fn default() -> Self {
        Self {
            queue_size: 10_000,
            batch_size: 100,
            batch_timeout_ms: 10,
            instance_id: None,
        }
    }
}

struct PendingWrite {
    op: WriteOp,
    /// `None` only for [`WriteOp::Barrier`].
    record: Option<RequestRecord>,
    ack: oneshot::Sender<Result<(), WriteError>>,
}

/// Ledger writer with background batch processing.
pub struct LedgerWriter {
    /// The producer handle, taken at shutdown.
    ///
    /// Closing the channel from the producer side means dropping the only
    /// sender, so the handle has to be droppable — hence the `Option`. Taking it
    /// is what tells the writer task that no further records are coming, after
    /// which it drains what is buffered and exits.
    tx: Mutex<Option<mpsc::Sender<PendingWrite>>>,
    ready_rx: watch::Receiver<bool>,
    queue_size: usize,
    task: Mutex<Option<JoinHandle<()>>>,
    /// Total records acknowledged after a successful COMMIT.
    committed: Arc<AtomicU64>,
    /// Latched false when a durable write fails; never reset while running.
    healthy: Arc<AtomicBool>,
    /// Detached finalizes in flight, awaited by [`LedgerWriter::shutdown`].
    detached: Arc<AtomicUsize>,
    /// Set at the start of shutdown; makes new writes fail loudly and quickly.
    shutting_down: AtomicBool,
}

impl LedgerWriter {
    /// Create a new ledger writer and spawn its single background writer task.
    pub fn new(conn: Arc<Mutex<Connection>>, config: LedgerWriterConfig) -> Self {
        // `mpsc::channel(0)` panics, so the clamp has to happen before the
        // channel is built, not just on the field we report.
        let queue_size = config.queue_size.max(1);
        let (tx, rx) = mpsc::channel(queue_size);
        let (ready_tx, ready_rx) = watch::channel(true);
        let committed = Arc::new(AtomicU64::new(0));
        let healthy = Arc::new(AtomicBool::new(true));
        let detached = Arc::new(AtomicUsize::new(0));

        let task = tokio::spawn(Self::writer_task(
            conn,
            rx,
            config,
            ready_tx,
            committed.clone(),
            healthy.clone(),
        ));

        Self {
            tx: Mutex::new(Some(tx)),
            ready_rx,
            queue_size,
            task: Mutex::new(Some(task)),
            committed,
            healthy,
            detached,
            shutting_down: AtomicBool::new(false),
        }
    }

    /// Clone the producer handle, or report that the writer has stopped.
    ///
    /// The lock is held only to clone and is released before any `await`, so a
    /// caller never holds it across a suspension point.
    fn sender(&self) -> Result<mpsc::Sender<PendingWrite>, WriteError> {
        if self.shutting_down.load(Ordering::Acquire) {
            return Err(WriteError::Shutdown);
        }
        self.tx.lock().clone().ok_or(WriteError::Shutdown)
    }

    /// Record that a request was accepted, durably, before it is forwarded.
    ///
    /// Awaits the COMMIT — this is the "accepted request has a ledger identity"
    /// guarantee, and it is what makes crash recovery possible.
    pub async fn accept(&self, record: RequestRecord) -> Result<(), WriteError> {
        debug_assert_eq!(record.request_status, RequestStatus::InFlight);
        self.enqueue(WriteOp::Accept, record).await
    }

    /// Record that a request reached a terminal state, durably.
    ///
    /// Awaits the COMMIT, so once this returns the accounting is guaranteed
    /// durable even if the process dies immediately afterwards.
    pub async fn finalize(&self, record: RequestRecord) -> Result<(), WriteError> {
        debug_assert!(record.is_terminal());
        self.enqueue(WriteOp::Finalize, record).await
    }

    /// Non-blocking finalize used by teardown paths (drop guards) that cannot
    /// await. Returns `QueueFull` rather than blocking, so the caller can log
    /// the loss explicitly instead of hanging process exit.
    pub fn try_finalize(&self, record: RequestRecord) -> Result<(), WriteError> {
        debug_assert!(record.is_terminal());
        let (ack, _rx) = oneshot::channel();
        self.sender()?
            .try_send(PendingWrite {
                op: WriteOp::Finalize,
                record: Some(record),
                ack,
            })
            .map_err(|e| match e {
                mpsc::error::TrySendError::Full(_) => WriteError::QueueFull,
                mpsc::error::TrySendError::Closed(_) => WriteError::Shutdown,
            })
    }

    async fn enqueue(&self, op: WriteOp, record: RequestRecord) -> Result<(), WriteError> {
        let tx = self.sender()?;
        let (ack, ack_rx) = oneshot::channel();

        // Bounded send: when the queue is full this awaits capacity. That is
        // deliberate backpressure — the record is never dropped.
        if tx
            .send(PendingWrite {
                op,
                record: Some(record),
                ack,
            })
            .await
            .is_err()
        {
            return Err(WriteError::Shutdown);
        }

        // `tx` is dropped here. If shutdown is waiting for the channel to close,
        // this clone was the last thing keeping it open, and the record is
        // already in the queue that the writer will drain.
        drop(tx);

        ack_rx.await.map_err(|_| WriteError::Shutdown)?
    }

    /// Whether the writer is accepting work at full rate.
    ///
    /// False once the queue is more than half full, once a commit has failed
    /// permanently, or once the writer has stopped. This drives `/readyz`, so a
    /// degrading ledger takes the instance out of rotation *before* it starts
    /// refusing requests.
    pub fn is_ready(&self) -> bool {
        !self.shutting_down.load(Ordering::Acquire)
            && self.healthy.load(Ordering::Acquire)
            && *self.ready_rx.borrow()
    }

    /// Record that metering failed and take the instance out of rotation.
    ///
    /// Called whenever a durable write cannot be completed. Readiness is not
    /// automatically restored: a process that has lost a metering write has
    /// unaccounted traffic, and only a restart (with recovery) resolves that.
    pub fn mark_unhealthy(&self) {
        if !self.healthy.swap(false, Ordering::AcqRel) {
            return;
        }
        error!("Ledger is unhealthy: readiness is now failing until restart");
    }

    /// Whether this writer has been marked unhealthy.
    pub fn is_healthy(&self) -> bool {
        self.healthy.load(Ordering::Acquire)
    }

    /// Number of detached finalizes not yet enqueued.
    pub fn detached_pending(&self) -> usize {
        self.detached.load(Ordering::Acquire)
    }

    /// Enqueue a terminal record from a context that cannot await (a drop guard).
    ///
    /// The write is never dropped silently: it is tracked so [`Self::shutdown`]
    /// can wait for it, and a failure to complete it is logged and degrades
    /// readiness.
    pub fn spawn_detached_finalize(self: &Arc<Self>, record: RequestRecord) {
        debug_assert!(record.is_terminal());
        let request_id = record.request_id.clone();
        self.detached.fetch_add(1, Ordering::AcqRel);

        let this = Arc::clone(self);

        match tokio::runtime::Handle::try_current() {
            Ok(handle) => {
                handle.spawn(async move {
                    let result = this.finalize(record).await;
                    Self::report_detached_result(&this, result, &request_id);
                    this.detached.fetch_sub(1, Ordering::AcqRel);
                });
            }
            Err(_) => {
                // No runtime: the process is already tearing down. One last
                // synchronous attempt, then report honestly.
                let result = this.try_finalize(record);
                Self::report_detached_result(&this, result, &request_id);
                this.detached.fetch_sub(1, Ordering::AcqRel);
            }
        }
    }

    fn report_detached_result(
        writer: &Arc<Self>,
        result: Result<(), WriteError>,
        request_id: &str,
    ) {
        if let Err(e) = result {
            writer.mark_unhealthy();
            error!(
                request_id,
                error = %e,
                "Lost a metering record for an interrupted stream"
            );
        }
    }

    /// Number of records waiting in the channel (not yet pulled by the writer).
    ///
    /// This is what bounds memory; the writer additionally holds up to
    /// `batch_size` records locally while they are in flight.
    pub fn queue_depth(&self) -> usize {
        match &*self.tx.lock() {
            Some(tx) => tx.max_capacity().saturating_sub(tx.capacity()),
            // The producer handle was taken: every record has either been
            // committed or is being committed by the drain, so nothing is owed.
            None => 0,
        }
    }

    /// Total records durably committed since process start.
    pub fn committed_total(&self) -> u64 {
        self.committed.load(Ordering::Relaxed)
    }

    pub fn queue_capacity(&self) -> usize {
        self.queue_size
    }

    /// Flush everything queued and stop the writer task.
    ///
    /// Ordering matters: closing the channel lets the writer drain its queue and
    /// commit, and awaiting the task handle guarantees the final COMMIT has
    /// landed before the database is closed. The DB writer is never shut down
    /// before its producers.
    pub async fn shutdown(&self) {
        // Drop guards hand their final write to a detached task. Wait for those
        // to be *enqueued* before closing the channel, otherwise a record for an
        // interrupted stream would be refused and lost.
        let deadline = tokio::time::Instant::now() + DETACHED_DRAIN_TIMEOUT;
        while self.detached.load(Ordering::Acquire) > 0 {
            if tokio::time::Instant::now() >= deadline {
                error!(
                    pending = self.detached.load(Ordering::Acquire),
                    "Detached metering writes did not enqueue before the drain deadline"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }

        // Refuse new work first, so a producer arriving now gets an explicit
        // error instead of a record that races the drain.
        self.shutting_down.store(true, Ordering::Release);

        // Dropping the producer handle closes the channel once every in-flight
        // clone is released. The writer then drains what is buffered and exits;
        // the join below guarantees the final COMMIT has landed before the
        // caller closes the database.
        let sender = self.tx.lock().take();
        drop(sender);

        let task = self.task.lock().take();
        if let Some(task) = task {
            if let Err(e) = task.await {
                error!(error = %e, "ledger writer task panicked during shutdown");
            }
        }
        info!(
            committed = self.committed_total(),
            "Ledger writer drained and stopped"
        );
    }

    /// Flush everything queued so far and wait for the COMMIT.
    ///
    /// Enqueues a no-op barrier that the writer acknowledges only after it has
    /// committed the batch containing it, which guarantees everything queued
    /// before this call is durable. The writer keeps running.
    pub async fn flush_now(&self) -> Result<(), WriteError> {
        let (ack, ack_rx) = oneshot::channel();
        let tx = self.sender()?;

        tx.send(PendingWrite {
            op: WriteOp::Barrier,
            record: None,
            ack,
        })
        .await
        .map_err(|_| WriteError::Shutdown)?;

        ack_rx.await.map_err(|_| WriteError::Shutdown)?
    }

    /// Wait until the queue has drained to empty.
    pub async fn wait_for_drain(&self, timeout: Duration) -> bool {
        let deadline = tokio::time::Instant::now() + timeout;
        while self.queue_depth() > 0 {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(Duration::from_millis(2)).await;
        }
        true
    }

    async fn writer_task(
        conn: Arc<Mutex<Connection>>,
        mut rx: mpsc::Receiver<PendingWrite>,
        config: LedgerWriterConfig,
        ready_tx: watch::Sender<bool>,
        committed: Arc<AtomicU64>,
        health: Arc<AtomicBool>,
    ) {
        // Clamped, not trusted: the configuration validator rejects a zero
        // batch size, and this is the belt to that pair of braces. A zero here
        // would make the batched receive unreachable and stall every metering
        // write for the life of the process — a deadlock that still reports
        // itself as ready, which is the worst kind.
        let batch_size = config.batch_size.max(1);
        let queue_capacity = config.queue_size.max(1);
        let instance_id = config.instance_id.as_deref();
        let mut batch: Vec<PendingWrite> = Vec::with_capacity(batch_size.min(1024));
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);
        let mut last_flush = tokio::time::Instant::now();
        let mut commits_are_healthy = true;
        let mut closed = false;

        info!("Ledger writer started");

        loop {
            // The channel is only drained up to `batch_size`. Anything beyond
            // that stays queued, which is what makes the bounded channel a real
            // bound on memory and a real source of backpressure — an unbounded
            // local batch would silently absorb any offer rate.
            let sleep_for = if batch.is_empty() {
                IDLE_TICK
            } else {
                batch_timeout
                    .saturating_sub(last_flush.elapsed())
                    .max(Duration::from_millis(1))
            };

            tokio::select! {
                biased;
                maybe = rx.recv(), if batch.len() < batch_size => {
                    match maybe {
                        Some(pending) => batch.push(pending),
                        None => closed = true,
                    }
                }
                _ = tokio::time::sleep(sleep_for) => {}
            }

            if closed {
                // Sender gone: take everything still buffered, then finish.
                while batch.len() < batch_size {
                    match rx.try_recv() {
                        Ok(pending) => batch.push(pending),
                        Err(_) => break,
                    }
                }
            }

            // A closed channel flushes immediately: the batching window exists to
            // accumulate concurrent records, and there is nothing left to
            // accumulate. Without this, shutdown would wait out the whole window
            // before committing the records it already holds.
            let due = !batch.is_empty()
                && (closed || batch.len() >= batch_size || last_flush.elapsed() >= batch_timeout);
            if due {
                commits_are_healthy =
                    Self::flush_with_retry(&conn, &mut batch, &committed, instance_id);
                if !commits_are_healthy {
                    // Latch the failure: a commit that could not be written means
                    // traffic is unaccounted for, which only a restart resolves.
                    health.store(false, Ordering::Release);
                }
                last_flush = tokio::time::Instant::now();
            }

            // Readiness reflects real outstanding work: queued plus in-batch.
            // True while commits are succeeding and less than half the queue is
            // outstanding, so /readyz turns red before the queue is saturated.
            // Integer division made this `0 < 0` for a queue of one, so a
            // legitimately tiny configuration could never report ready. Doubling
            // the depth expresses "less than half the queue" without the
            // truncation.
            let depth = rx.len() + batch.len();
            let is_ready = commits_are_healthy && depth.saturating_mul(2) < queue_capacity;
            let _ = ready_tx.send(is_ready);

            if closed && batch.is_empty() {
                // Drain anything that raced in after the final try_recv.
                let mut leftover = Vec::new();
                while let Ok(pending) = rx.try_recv() {
                    leftover.push(pending);
                }
                if !leftover.is_empty() {
                    Self::flush_with_retry(&conn, &mut leftover, &committed, instance_id);
                }
                break;
            }
        }

        // Every handle is dropped; the writer is definitively stopped.
        let _ = ready_tx.send(false);
        info!("Ledger writer task exited");
    }

    /// Flush a batch, retrying BUSY/LOCKED with backoff. Returns `false` when
    /// the batch could not be committed (writer is no longer healthy).
    fn flush_with_retry(
        conn: &Arc<Mutex<Connection>>,
        batch: &mut Vec<PendingWrite>,
        committed: &Arc<AtomicU64>,
        instance_id: Option<&str>,
    ) -> bool {
        if batch.is_empty() {
            return true;
        }

        let mut attempt = 0u32;
        loop {
            match Self::try_flush(conn, batch, instance_id) {
                Ok(n) => {
                    committed.fetch_add(n as u64, Ordering::Relaxed);
                    for pending in batch.drain(..) {
                        let _ = pending.ack.send(Ok(()));
                    }
                    return true;
                }
                Err(e) if is_busy(&e) && attempt < BUSY_RETRIES => {
                    attempt += 1;
                    let backoff = BUSY_BACKOFF_BASE * 2u32.pow(attempt - 1);
                    warn!(
                        attempt,
                        backoff_ms = backoff.as_millis() as u64,
                        error = %e,
                        "Ledger commit blocked by SQLite lock; retrying (record retained)"
                    );
                    std::thread::sleep(backoff);
                }
                Err(e) => {
                    error!(
                        error = %e,
                        records = batch.len(),
                        "Ledger commit failed permanently; surfacing error to callers"
                    );
                    let shared = Arc::new(e);
                    for pending in batch.drain(..) {
                        let _ = pending.ack.send(Err(WriteError::Sqlite(shared.clone())));
                    }
                    return false;
                }
            }
        }
    }

    /// One attempt at writing the batch in a single transaction.
    ///
    /// Returns the number of records committed. On failure nothing is drained
    /// from the caller's batch, so a retry re-attempts the whole set.
    fn try_flush(
        conn: &Arc<Mutex<Connection>>,
        batch: &[PendingWrite],
        instance_id: Option<&str>,
    ) -> Result<usize, rusqlite::Error> {
        // Collapse accepts that are superseded by a finalize in the same batch:
        // the finalize upsert inserts the row directly, so writing the
        // intermediate `in_flight` row would be pure overhead. This is a pure
        // optimization — correctness does not depend on it.
        let finalized: HashSet<&str> = batch
            .iter()
            .filter(|p| p.op == WriteOp::Finalize)
            .filter_map(|p| p.record.as_ref())
            .map(|r| r.request_id.as_str())
            .collect();

        let mut conn = conn.lock();
        // IMMEDIATE takes the write lock up front, avoiding the
        // SQLITE_BUSY_SNAPSHOT upgrade deadlock that DEFERRED can hit when two
        // instances (same-VPS rolling update) write concurrently.
        let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

        let mut written = 0usize;
        for pending in batch {
            let Some(record) = pending.record.as_ref() else {
                // Barrier: no row, no write — its only job is to ack.
                continue;
            };
            match pending.op {
                WriteOp::Accept => {
                    if finalized.contains(record.request_id.as_str()) {
                        continue;
                    }
                    insert_accept(&tx, record, instance_id)?;
                }
                WriteOp::Finalize => {
                    finalize_record(&tx, record, instance_id)?;
                }
                WriteOp::Barrier => continue,
            }
            written += 1;
        }

        tx.commit()?;
        Ok(written)
    }
}

/// Insert a fresh record in the `in_flight` state. Idempotent: a duplicate
/// accept (e.g. after a retry) is ignored rather than failing the batch.
fn insert_accept(
    tx: &rusqlite::Transaction,
    record: &RequestRecord,
    instance_id: Option<&str>,
) -> Result<(), rusqlite::Error> {
    tx.execute(
        r#"
        INSERT INTO usage_records (
            request_id, created_at, consumer_id, model, endpoint, streaming,
            http_status, request_status, instance_id, input_tokens, output_tokens,
            cached_tokens, ttft_ms, duration_ms, usage_status, error_message,
            error_body
        ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, NULL, 'in_flight', ?7, NULL, NULL, NULL, NULL, 0, 'unavailable', NULL, NULL)
        ON CONFLICT(request_id) DO NOTHING
        "#,
        rusqlite::params![
            record.request_id,
            format_timestamp(record.created_at),
            record.consumer_id,
            record.model,
            record.endpoint.as_str(),
            record.streaming as i32,
            instance_id,
        ],
    )?;
    Ok(())
}

/// Move a record to its terminal state, keeping the rollup in step with it.
///
/// # Why this is a delta, not a one-shot upsert
///
/// The obvious implementation — roll up when the row transitions, and ignore any
/// later finalize — is only correct if nothing ever writes the same request
/// twice or writes it in the wrong state. Both happen: a rolling update's
/// recovery can resolve a row its owner was still finishing, a drop guard races
/// the normal path, a retry re-sends a batch. The one-shot version turns every
/// one of those into either a double count or — worse — a silently discarded
/// real usage figure, because the second finalize returns early.
///
/// So the rollup is stated as a *contribution* ([`crate::ledger::rollup`]) and
/// each finalize does:
///
/// ```text
///   retract what this row currently contributes  ->  add what it should
/// ```
///
/// which is idempotent (applying it twice retracts and re-adds the same numbers,
/// netting zero), self-correcting (a wrongly-terminal row is fixed rather than
/// ignored), and immune to ordering. The raw row and the rollup delta commit in
/// the caller's transaction, so the two can never disagree.
///
/// A finalize for a request with no accept row yet (the single-batch fast path,
/// where an accept and its finalize collapse into one batch) inserts the
/// terminal row directly and adds its contribution; there is nothing to
/// retract.
fn finalize_record(
    tx: &rusqlite::Transaction,
    record: &RequestRecord,
    instance_id: Option<&str>,
) -> Result<(), rusqlite::Error> {
    let stored = StoredRow::load(tx, &record.request_id)?;

    // Two keys, deliberately. The *retraction* must name the bucket the row was
    // actually written into, or it would leave the old bucket over-counted. The
    // *addition* must name the bucket the record is now in, which can differ:
    // `streaming` is decided at accept from the request body, but the response
    // can turn out to be an SSE stream the client did not ask for. Deriving both
    // from the stored row would silently keep such a request in the wrong
    // bucket; deriving both from the record would silently over-count the old
    // one. So the retract uses the stored key and the add uses the record's.
    //
    // Every other column of the bucket (hour, consumer, model, endpoint) is
    // fixed at accept and never corrected, so the two keys agree on them and a
    // retract/add pair can only ever move a row between buckets, never invent
    // one.
    match &stored {
        Some(existing) => {
            tx.execute(
                r#"
                UPDATE usage_records SET
                    http_status = ?2,
                    request_status = ?3,
                    streaming = ?4,
                    input_tokens = ?5,
                    output_tokens = ?6,
                    cached_tokens = ?7,
                    ttft_ms = ?8,
                    duration_ms = ?9,
                    usage_status = ?10,
                    error_message = ?11,
                    error_body = ?12
                WHERE request_id = ?1
                "#,
                rusqlite::params![
                    record.request_id,
                    record.http_status,
                    record.request_status.as_str(),
                    record.streaming as i32,
                    record.usage.input_tokens,
                    record.usage.output_tokens,
                    record.usage.cached_tokens,
                    record.ttft_ms,
                    record.duration_ms as i64,
                    record.usage_status().as_str(),
                    record.error_message,
                    record.error_body,
                ],
            )?;

            // `None` for a row that was still in flight: nothing was ever rolled
            // up for it, so the first finalize retracts nothing.
            if let Some(previous) = existing.contribution() {
                previous.retract(tx)?;
            }
        }
        None => {
            tx.execute(
                r#"
                INSERT INTO usage_records (
                    request_id, created_at, consumer_id, model, endpoint, streaming,
                    http_status, request_status, instance_id, input_tokens,
                    output_tokens, cached_tokens, ttft_ms, duration_ms,
                    usage_status, error_message, error_body
                ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17)
                "#,
                rusqlite::params![
                    record.request_id,
                    format_timestamp(record.created_at),
                    record.consumer_id,
                    record.model,
                    record.endpoint.as_str(),
                    record.streaming as i32,
                    record.http_status,
                    record.request_status.as_str(),
                    instance_id,
                    record.usage.input_tokens,
                    record.usage.output_tokens,
                    record.usage.cached_tokens,
                    record.ttft_ms,
                    record.duration_ms as i64,
                    record.usage_status().as_str(),
                    record.error_message,
                    record.error_body,
                ],
            )?;
        }
    }

    Contribution::terminal(record, BucketKey::from_record(record)).add(tx)?;
    Ok(())
}

/// Format an `OffsetDateTime` for the `created_at` column.
///
/// Delegates to [`crate::ledger::timefmt`] so every stored timestamp is
/// fixed-width and byte ordering equals chronological ordering.
pub fn format_timestamp(ts: time::OffsetDateTime) -> String {
    crate::ledger::timefmt::format_ts(ts)
}

/// Format an `OffsetDateTime` as its UTC hour bucket, e.g. `2026-09-24T07`.
pub fn format_hour(ts: time::OffsetDateTime) -> String {
    crate::ledger::timefmt::format_hour(ts)
}

/// Apply the complete terminal-state write (raw row upsert + hourly rollup) to
/// an existing transaction.
///
/// Exposed so maintenance paths (recovery, retention tests, migrations) can
/// reuse the exact production write logic rather than reimplementing it and
/// risking raw/rollup divergence.
pub fn finalize_in_tx(
    tx: &rusqlite::Transaction,
    record: &RequestRecord,
) -> Result<(), rusqlite::Error> {
    finalize_record(tx, record, None)
}

/// Whether a SQLite error indicates transient contention rather than a real fault.
fn is_busy(e: &rusqlite::Error) -> bool {
    matches!(
        e,
        rusqlite::Error::SqliteFailure(err, _)
            if matches!(
                err.code,
                rusqlite::ErrorCode::DatabaseBusy | rusqlite::ErrorCode::DatabaseLocked
            )
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{Endpoint, Usage};
    use tempfile::TempDir;

    fn test_writer(path: &std::path::Path, batch_size: usize, timeout_ms: u64) -> LedgerWriter {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        LedgerWriter::new(
            Arc::new(Mutex::new(conn)),
            LedgerWriterConfig {
                // Single-instance tests: no ownership is claimed, so
                // every row reads back with a `NULL` owner.
                instance_id: None,
                queue_size: 100,
                batch_size,
                batch_timeout_ms: timeout_ms,
            },
        )
    }

    fn open(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        conn
    }

    #[tokio::test]
    async fn test_accept_then_finalize_persists_terminal_state() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let writer = test_writer(&path, 1, 1);

        let mut record = RequestRecord::new(
            "req-1".into(),
            "consumer-1".into(),
            "gpt-4".into(),
            Endpoint::ChatCompletions,
            false,
        );
        writer.accept(record.clone()).await.unwrap();

        {
            let conn = open(&path);
            let status: String = conn
                .query_row(
                    "SELECT request_status FROM usage_records WHERE request_id='req-1'",
                    [],
                    |r| r.get(0),
                )
                .unwrap();
            assert_eq!(
                status, "in_flight",
                "accept must be durable before proxying"
            );
        }

        record.complete(200, Usage::new(Some(100), Some(50), Some(20)), 1234);
        writer.finalize(record).await.unwrap();

        let conn = open(&path);
        let (status, input, output, usage_status): (String, i64, i64, String) = conn
            .query_row(
                "SELECT request_status, input_tokens, output_tokens, usage_status
                 FROM usage_records WHERE request_id='req-1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert_eq!(input, 100);
        assert_eq!(output, 50);
        assert_eq!(usage_status, "available");

        // Rollup applied exactly once.
        let (count, success, tokens): (i64, i64, i64) = conn
            .query_row(
                "SELECT request_count, success_count, total_input_tokens
                 FROM usage_hourly WHERE consumer_id='consumer-1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(success, 1);
        assert_eq!(tokens, 100);
    }

    #[tokio::test]
    async fn test_duplicate_finalize_does_not_double_count_rollup() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let writer = test_writer(&path, 1, 1);

        let mut record = RequestRecord::new(
            "req-dup".into(),
            "c".into(),
            "m".into(),
            Endpoint::Responses,
            false,
        );
        record.complete(200, Usage::new(Some(10), Some(5), None), 100);
        writer.finalize(record.clone()).await.unwrap();
        writer.finalize(record).await.unwrap();

        let conn = open(&path);
        let count: i64 = conn
            .query_row(
                "SELECT request_count FROM usage_hourly WHERE consumer_id='c'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(count, 1, "rollup must be applied exactly once per request");
    }

    #[tokio::test]
    async fn test_single_batch_fast_path_collapses_accept() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        // Long batch window so accept and finalize land in the same batch.
        let writer = test_writer(&path, 100, 200);

        let mut record = RequestRecord::new(
            "req-fast".into(),
            "c".into(),
            "m".into(),
            Endpoint::ChatCompletions,
            false,
        );

        // Fire accept without awaiting the ack, then finalize, so both are
        // queued inside one batch window.
        let w = &writer;
        let accept_fut = w.accept(record.clone());
        let finalize_fut = async {
            tokio::time::sleep(Duration::from_millis(5)).await;
            record.complete(200, Usage::new(Some(3), Some(4), None), 42);
            writer.finalize(record).await
        };
        let (a, f) = tokio::join!(accept_fut, finalize_fut);
        a.unwrap();
        f.unwrap();

        let conn = open(&path);
        let (rows, status): (i64, String) = conn
            .query_row(
                "SELECT COUNT(*), MAX(request_status) FROM usage_records WHERE request_id='req-fast'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(rows, 1, "collapsed accept must not leave a second row");
        assert_eq!(status, "completed");
    }

    #[tokio::test]
    async fn test_missing_usage_stays_null_not_zero() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let writer = test_writer(&path, 1, 1);

        let mut record = RequestRecord::new(
            "req-nousage".into(),
            "c".into(),
            "m".into(),
            Endpoint::ChatCompletions,
            false,
        );
        record.complete(200, Usage::default(), 10);
        writer.finalize(record).await.unwrap();

        let conn = open(&path);
        let (input, usage_status): (Option<i64>, String) = conn
            .query_row(
                "SELECT input_tokens, usage_status FROM usage_records WHERE request_id='req-nousage'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(input, None, "unavailable usage must persist as NULL, not 0");
        assert_eq!(usage_status, "unavailable");
    }

    #[tokio::test]
    async fn test_interrupted_counts_as_failure_in_rollup() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let writer = test_writer(&path, 1, 1);

        let mut record = RequestRecord::new(
            "req-int".into(),
            "c".into(),
            "m".into(),
            Endpoint::ChatCompletions,
            true,
        );
        record.interrupt("client disconnected", 900);
        writer.finalize(record).await.unwrap();

        let conn = open(&path);
        let (failures, success): (i64, i64) = conn
            .query_row(
                "SELECT failure_count, success_count FROM usage_hourly WHERE consumer_id='c'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(failures, 1);
        assert_eq!(success, 0);
    }

    #[tokio::test]
    async fn test_shutdown_drains_queue_before_stopping() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        // Large batch window: nothing flushes on its own before shutdown.
        let writer = test_writer(&path, 1000, 60_000);

        for i in 0..25 {
            let mut r = RequestRecord::new(
                format!("req-{i}"),
                "c".into(),
                "m".into(),
                Endpoint::ChatCompletions,
                false,
            );
            r.fail(Some(500), "boom".into(), 1);
            writer
                .sender()
                .unwrap()
                .send(PendingWrite {
                    op: WriteOp::Finalize,
                    record: Some(r),
                    ack: oneshot::channel().0,
                })
                .await
                .unwrap();
        }

        // Give the writer's pull loop a moment; it must NOT be able to flush
        // because the batch window is a minute long.
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert!(
            writer.committed_total() == 0,
            "nothing may be committed before the batch deadline"
        );
        writer.shutdown().await;

        let conn = open(&path);
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 25, "shutdown must commit everything queued");
        let rollup: i64 = conn
            .query_row("SELECT SUM(request_count) FROM usage_hourly", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert_eq!(rollup, 25);
    }

    #[tokio::test]
    async fn test_readiness_degrades_when_queue_backs_up() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        // Nothing flushes for a minute, so outstanding work only grows.
        let writer = test_writer(&path, 1000, 60_000);
        assert!(writer.is_ready());

        // queue_size is 100, so readiness must flip once >50 are outstanding.
        for i in 0..80 {
            let r = RequestRecord::new(
                format!("r{i}"),
                "c".into(),
                "m".into(),
                Endpoint::ChatCompletions,
                false,
            );
            // Awaited sends would block once the channel is full, so push with
            // a bound: readiness must have flipped long before that.
            writer
                .sender()
                .unwrap()
                .send(PendingWrite {
                    op: WriteOp::Accept,
                    record: Some(r),
                    ack: oneshot::channel().0,
                })
                .await
                .unwrap();
        }

        // Readiness is published by the writer task, so allow a tick.
        let mut degraded = false;
        for _ in 0..200 {
            if !writer.is_ready() {
                degraded = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(
            degraded,
            "readiness must go false when the queue backs up (depth={})",
            writer.queue_depth()
        );

        writer.shutdown().await;
    }

    // A multi-thread runtime: this test stalls the writer on the database
    // connection, and on a single-threaded runtime a stalled writer would block
    // the only thread available to drive the sends.
    //
    // Holding the lock across `await` is the point of the test — the stall must
    // outlive each send — and parking_lot is deliberately the sync mutex the
    // writer itself uses, so an async mutex would not reproduce the stall.
    #[allow(clippy::await_holding_lock)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn test_bounded_queue_applies_backpressure_not_loss() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let conn = Arc::new(Mutex::new(Connection::open(&path).unwrap()));
        {
            let guard = conn.lock();
            crate::ledger::configure_sqlite(&guard).unwrap();
            crate::ledger::init_schema(&guard).unwrap();
        }

        // Tiny queue, so saturation is reached in a handful of records.
        let writer = LedgerWriter::new(
            conn.clone(),
            LedgerWriterConfig {
                // Single-instance tests: no ownership is claimed, so
                // every row reads back with a `NULL` owner.
                instance_id: None,
                queue_size: 4,
                batch_size: 4,
                batch_timeout_ms: 10,
            },
        );

        // Stall the writer by holding the database connection, which is what a
        // slow or contended commit does in production. Nothing drains while it
        // is held, so the queue must fill and then refuse to grow.
        let stall = conn.lock();

        // The writer can buffer its channel capacity (4) plus the batch it has
        // already pulled (4). The ninth send must therefore block; the point of
        // this test is that it blocks rather than being discarded.
        const ATTEMPTS: usize = 64;
        let mut sent = 0usize;
        for i in 0..ATTEMPTS {
            let r = RequestRecord::new(
                format!("bp-{i}"),
                "c".into(),
                "m".into(),
                Endpoint::ChatCompletions,
                false,
            );
            let sent_ok = tokio::time::timeout(
                Duration::from_millis(200),
                writer.sender().unwrap().send(PendingWrite {
                    op: WriteOp::Accept,
                    record: Some(r),
                    ack: oneshot::channel().0,
                }),
            )
            .await;
            if sent_ok.is_err() {
                break;
            }
            sent += 1;
        }

        assert!(
            sent < ATTEMPTS,
            "senders must block once the queue is full — backpressure, never drop"
        );
        assert!(
            sent <= 8,
            "buffered records must stay bounded by capacity + batch, got {sent}"
        );

        drop(stall);
        writer.shutdown().await;

        // Everything that was accepted is durable; nothing was silently dropped
        // while the writer was unable to make progress.
        let conn = open(&path);
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows as usize, sent);
    }

    #[tokio::test]
    async fn test_flush_now_makes_prior_writes_durable() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        // Batch window long enough that only the barrier can flush this.
        let writer = test_writer(&path, 1, 60_000);

        let mut r = RequestRecord::new(
            "req-barrier".into(),
            "c".into(),
            "m".into(),
            Endpoint::ChatCompletions,
            false,
        );
        r.fail(Some(502), "upstream".into(), 5);
        writer
            .sender()
            .unwrap()
            .send(PendingWrite {
                op: WriteOp::Finalize,
                record: Some(r),
                ack: oneshot::channel().0,
            })
            .await
            .unwrap();

        writer.flush_now().await.unwrap();

        let conn = open(&path);
        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_records WHERE request_id='req-barrier'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1, "barrier ack must imply the prior record is committed");

        writer.shutdown().await;
    }

    #[tokio::test]
    async fn test_batched_accepts_and_finalizes_all_land() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let writer = test_writer(&path, 50, 5);

        let mut handles = Vec::new();
        for i in 0..40 {
            let w = &writer;
            handles.push(async move {
                let mut r = RequestRecord::new(
                    format!("req-{i}"),
                    format!("consumer-{}", i % 3),
                    "m".into(),
                    Endpoint::ChatCompletions,
                    i % 2 == 0,
                );
                w.accept(r.clone()).await.unwrap();
                r.complete(200, Usage::new(Some(i), Some(i * 2), None), 10);
                w.finalize(r).await.unwrap();
            });
        }
        futures::future::join_all(handles).await;

        writer.shutdown().await;

        let conn = open(&path);
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 40);
        let (rollup, tokens): (i64, i64) = conn
            .query_row(
                "SELECT SUM(request_count), SUM(total_input_tokens) FROM usage_hourly",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        assert_eq!(rollup, 40, "rollup count must match raw count");
        assert_eq!(tokens, (0..40i64).sum::<i64>());
    }
}
