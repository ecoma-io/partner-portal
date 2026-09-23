//! Ledger writer with bounded queue and micro-batching

use crate::config::DatabaseConfig;
use crate::ledger::{RequestRecord, RequestStatus};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tracing::{error, info};

/// Write error types
#[derive(Debug, thiserror::Error)]
pub enum WriteError {
    #[error("Queue full, backpressure applied")]
    QueueFull,

    #[error("Writer shutdown")]
    Shutdown,

    #[error("SQLite error: {0}")]
    Sqlite(#[from] rusqlite::Error),
}

/// Ledger writer configuration
#[derive(Debug, Clone)]
pub struct LedgerWriterConfig {
    pub queue_size: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
}

impl From<DatabaseConfig> for LedgerWriterConfig {
    fn from(config: DatabaseConfig) -> Self {
        Self {
            queue_size: config.queue_size,
            batch_size: config.batch_size,
            batch_timeout_ms: config.batch_timeout_ms,
        }
    }
}

/// Pending write operation
struct PendingWrite {
    record: RequestRecord,
    result_tx: tokio::sync::oneshot::Sender<Result<(), WriteError>>,
}

/// Ledger writer with background batch processing
pub struct LedgerWriter {
    tx: mpsc::Sender<PendingWrite>,
    ready_tx: tokio::sync::watch::Sender<bool>,
    pending_count: Arc<std::sync::atomic::AtomicUsize>,
}

impl LedgerWriter {
    /// Create a new ledger writer
    pub fn new(conn: Arc<Mutex<Connection>>, config: LedgerWriterConfig) -> Self {
        let (tx, rx) = mpsc::channel(config.queue_size);
        let (ready_tx, _ready_rx) = tokio::sync::watch::channel(true);
        let pending_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));

        let pending_count_clone = pending_count.clone();

        tokio::spawn(Self::writer_task(
            conn,
            rx,
            config,
            ready_tx.clone(),
            pending_count_clone,
        ));

        Self {
            tx,
            ready_tx,
            pending_count,
        }
    }

    /// Write a request record (async, queued)
    pub async fn write(&self, record: RequestRecord) -> Result<(), WriteError> {
        let (result_tx, result_rx) = tokio::sync::oneshot::channel();

        let pending = PendingWrite { record, result_tx };

        self.tx
            .send(pending)
            .await
            .map_err(|_| WriteError::Shutdown)?;

        result_rx.await.map_err(|_| WriteError::Shutdown)?
    }

    /// Try to write without waiting (returns immediately if queue full)
    pub fn try_write(&self, record: RequestRecord) -> Result<(), WriteError> {
        let (result_tx, _result_rx) = tokio::sync::oneshot::channel();

        let pending = PendingWrite { record, result_tx };

        self.tx
            .try_send(pending)
            .map_err(|_| WriteError::QueueFull)?;

        // Still need to wait for result, but this is non-blocking send
        // The caller should use write() for proper async behavior
        Ok(())
    }

    /// Check if writer is ready (not in backpressure mode)
    pub fn is_ready(&self) -> bool {
        *self.ready_tx.borrow()
    }

    /// Get pending count
    pub fn pending_count(&self) -> usize {
        self.pending_count
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Set readiness state
    pub fn set_ready(&self, ready: bool) {
        let _ = self.ready_tx.send(ready);
    }

    /// Shutdown the writer (waits for queue to drain)
    pub async fn shutdown(&self) {
        drop(self.tx.clone());
        // Wait for ready signal to indicate writer has stopped
        let mut rx = self.ready_tx.subscribe();
        let _ = rx.wait_for(|_| true).await;
    }

    async fn writer_task(
        conn: Arc<Mutex<Connection>>,
        mut rx: mpsc::Receiver<PendingWrite>,
        config: LedgerWriterConfig,
        ready_tx: tokio::sync::watch::Sender<bool>,
        pending_count: Arc<std::sync::atomic::AtomicUsize>,
    ) {
        let mut batch: Vec<PendingWrite> = Vec::with_capacity(config.batch_size);
        let batch_timeout = Duration::from_millis(config.batch_timeout_ms);
        let mut last_flush = Instant::now();

        info!("Ledger writer started");

        loop {
            let timeout_remaining = batch_timeout.saturating_sub(last_flush.elapsed());

            tokio::select! {
                Some(pending) = rx.recv() => {
                    pending_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    batch.push(pending);

                    if batch.len() >= config.batch_size {
                        Self::flush_batch(&conn, &mut batch, &pending_count);
                        last_flush = Instant::now();
                    }
                }

                _ = tokio::time::sleep(timeout_remaining), if !batch.is_empty() => {
                    Self::flush_batch(&conn, &mut batch, &pending_count);
                    last_flush = Instant::now();
                }

                else => {
                    // Channel closed, drain remaining
                    if !batch.is_empty() {
                        Self::flush_batch(&conn, &mut batch, &pending_count);
                    }
                    break;
                }
            }

            // Update readiness based on queue depth
            let queue_depth = pending_count.load(std::sync::atomic::Ordering::Relaxed);
            let is_ready = queue_depth < config.queue_size / 2;
            let _ = ready_tx.send(is_ready);
        }

        let _ = ready_tx.send(false);
        info!("Ledger writer stopped");
    }

    fn flush_batch(
        conn: &Arc<Mutex<Connection>>,
        batch: &mut Vec<PendingWrite>,
        pending_count: &Arc<std::sync::atomic::AtomicUsize>,
    ) {
        if batch.is_empty() {
            return;
        }

        let mut conn = conn.lock();
        let tx = match conn.transaction() {
            Ok(t) => t,
            Err(e) => {
                error!(error = %e, "Failed to begin transaction");
                for pending in batch.drain(..) {
                    pending_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = pending
                        .result_tx
                        .send(Err(WriteError::Sqlite(rusqlite::Error::InvalidQuery)));
                }
                return;
            }
        };

        // Insert raw records and update rollups
        let results: Vec<Result<(), WriteError>> = batch
            .iter()
            .map(|pending| {
                Self::insert_record(&tx, &pending.record)?;
                Self::upsert_hourly(&tx, &pending.record)?;
                Ok(())
            })
            .collect();

        // Commit transaction
        match tx.commit() {
            Ok(()) => {
                for (pending, result) in batch.drain(..).zip(results) {
                    pending_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = pending.result_tx.send(result);
                }
            }
            Err(e) => {
                error!(error = %e, "Failed to commit transaction");
                for pending in batch.drain(..) {
                    pending_count.fetch_sub(1, std::sync::atomic::Ordering::Relaxed);
                    let _ = pending
                        .result_tx
                        .send(Err(WriteError::Sqlite(rusqlite::Error::InvalidQuery)));
                }
            }
        }
    }

    fn insert_record(
        tx: &rusqlite::Transaction,
        record: &RequestRecord,
    ) -> Result<(), rusqlite::Error> {
        tx.execute(
            r#"
            INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                http_status, request_status, input_tokens, output_tokens,
                cached_tokens, ttft_ms, duration_ms, usage_status, error_message
            ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15)
            "#,
            rusqlite::params![
                record.request_id,
                record
                    .created_at
                    .format(&time::format_description::well_known::Iso8601::DEFAULT)
                    .unwrap(),
                record.consumer_id,
                record.model,
                record.endpoint.as_str(),
                record.streaming as i32,
                record.http_status,
                record.request_status.as_str(),
                record.usage.input_tokens,
                record.usage.output_tokens,
                record.usage.cached_tokens,
                record.ttft_ms,
                record.duration_ms as i64,
                record.usage_status().as_str(),
                record.error_message,
            ],
        )?;
        Ok(())
    }

    fn upsert_hourly(
        tx: &rusqlite::Transaction,
        record: &RequestRecord,
    ) -> Result<(), rusqlite::Error> {
        let hour = record
            .created_at
            .format(&time::format_description::parse("[year]-[month]-[day]T[hour]").unwrap())
            .unwrap();

        let is_success = if record.request_status == RequestStatus::Completed {
            1
        } else {
            0
        };
        let is_failure = if record.request_status == RequestStatus::Failed {
            1
        } else {
            0
        };

        tx.execute(
            r#"
            INSERT INTO usage_hourly (
                hour, consumer_id, model, endpoint, streaming,
                request_count, total_input_tokens, total_output_tokens,
                total_cached_tokens, total_duration_ms, total_ttft_ms,
                ttft_count, success_count, failure_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(hour, consumer_id, model, endpoint, streaming) DO UPDATE SET
                request_count = request_count + 1,
                total_input_tokens = total_input_tokens + ?6,
                total_output_tokens = total_output_tokens + ?7,
                total_cached_tokens = total_cached_tokens + ?8,
                total_duration_ms = total_duration_ms + ?9,
                total_ttft_ms = total_ttft_ms + ?10,
                ttft_count = ttft_count + ?11,
                success_count = success_count + ?12,
                failure_count = failure_count + ?13
            "#,
            rusqlite::params![
                hour,
                record.consumer_id,
                record.model,
                record.endpoint.as_str(),
                record.streaming as i32,
                record.usage.input_tokens.unwrap_or(0) as i64,
                record.usage.output_tokens.unwrap_or(0) as i64,
                record.usage.cached_tokens.unwrap_or(0) as i64,
                record.duration_ms as i64,
                record.ttft_ms.unwrap_or(0) as i64,
                if record.ttft_ms.is_some() { 1 } else { 0 },
                is_success,
                is_failure,
            ],
        )?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::Endpoint;
    use tempfile::TempDir;

    #[tokio::test]
    async fn test_writer_batches_records() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");

        let conn = Connection::open(&path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        let conn = Arc::new(Mutex::new(conn));

        let config = LedgerWriterConfig {
            queue_size: 100,
            batch_size: 10,
            batch_timeout_ms: 100,
        };

        let writer = LedgerWriter::new(conn, config);

        // Write some records
        for i in 0..5 {
            let record = RequestRecord::new(
                format!("req-{}", i),
                "consumer-1".to_string(),
                "gpt-4".to_string(),
                Endpoint::ChatCompletions,
                false,
            );
            writer.write(record).await.unwrap();
        }

        // Wait for flush
        tokio::time::sleep(Duration::from_millis(200)).await;

        // All records should be persisted
        assert_eq!(writer.pending_count(), 0);
    }

    #[tokio::test]
    async fn test_writer_atomic_commit() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");

        let conn = Connection::open(&path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        let conn = Arc::new(Mutex::new(conn));

        let config = LedgerWriterConfig {
            queue_size: 100,
            batch_size: 1,
            batch_timeout_ms: 10,
        };

        let writer = LedgerWriter::new(conn.clone(), config);

        let record = RequestRecord::new(
            "req-atomic".to_string(),
            "consumer-1".to_string(),
            "gpt-4".to_string(),
            Endpoint::ChatCompletions,
            false,
        );

        writer.write(record).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Both raw and hourly should exist
        conn.lock()
            .query_row(
                "SELECT 1 FROM usage_records WHERE request_id = 'req-atomic'",
                [],
                |_| Ok(()),
            )
            .unwrap();
        conn.lock()
            .query_row(
                "SELECT 1 FROM usage_hourly WHERE consumer_id = 'consumer-1'",
                [],
                |_| Ok(()),
            )
            .unwrap();
    }
}
