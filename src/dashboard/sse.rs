use axum::{
    response::IntoResponse,
    response::sse::{Event, KeepAlive, Sse},
};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::warn;

const SSE_BUFFER: usize = 128;

/// SSE broadcaster: polls SQLite `data_version` and broadcasts change
/// notifications to connected dashboard clients.
///
/// It owns a *dedicated* poll connection, wrapped in `Arc<Mutex<_>>`
/// (rusqlite `Connection` is not `Sync`, so we need a mutex to share it).
///
/// SQLite's `data_version` only reflects writes made by *other* connections,
/// so the poller must never share the writer connection — otherwise it would
/// never observe a change. This dedicated connection is exactly what makes
/// cross-instance invalidation work: when instance A writes the shared DB,
/// instance B's poller sees `data_version` change.
#[derive(Clone)]
pub struct SseBroadcaster {
    poll_conn: Arc<Mutex<Connection>>,
    tx: broadcast::Sender<()>,
    poll_interval_ms: u64,
}

impl SseBroadcaster {
    pub fn new(db_path: &PathBuf, poll_interval_ms: u64) -> Result<Self, rusqlite::Error> {
        let poll_conn = Connection::open(db_path)?;
        poll_conn.execute_batch("PRAGMA journal_mode = WAL; PRAGMA busy_timeout = 5000;")?;
        Ok(Self {
            poll_conn: Arc::new(Mutex::new(poll_conn)),
            tx: broadcast::channel(SSE_BUFFER).0,
            poll_interval_ms,
        })
    }

    pub fn start(&self) {
        let conn = self.poll_conn.clone();
        let tx = self.tx.clone();
        let interval = Duration::from_millis(self.poll_interval_ms);
        let mut last_version = get_data_version_from_conn(&conn.lock());

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;
                let current = get_data_version_from_conn(&conn.lock());
                if current != last_version {
                    last_version = current;
                    let _ = tx.send(());
                }
            }
        });
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.tx.subscribe()
    }

    pub fn data_version(&self) -> i64 {
        get_data_version_from_conn(&self.poll_conn.lock())
    }
}

fn get_data_version_from_conn(conn: &Connection) -> i64 {
    conn.query_row("PRAGMA data_version", [], |row| row.get(0))
        .unwrap_or(0)
}

pub fn sse_response(broadcaster: Arc<SseBroadcaster>, _consumer_id: String) -> impl IntoResponse {
    let mut rx = broadcaster.subscribe();
    let stream: futures::stream::BoxStream<'static, Result<Event, axum::Error>> =
        Box::pin(async_stream::stream! {
            loop {
                match rx.recv().await {
                    Ok(()) => {
                        yield Ok(Event::default().data(r#"{"type":"data_changed"}"#));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        warn!(lagged = %n, "SSE client lagged, sending change event");
                        yield Ok(Event::default().data(r#"{"type":"data_changed"}"#));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });
    Sse::new(stream).keep_alive(KeepAlive::default())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_data_version_changes_across_connections() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.db");

        // Writer connection
        let writer = Connection::open(&path).unwrap();
        crate::ledger::configure_sqlite(&writer).unwrap();
        crate::ledger::init_schema(&writer).unwrap();

        let bc = SseBroadcaster::new(&path, 50).unwrap();

        let v1 = bc.data_version();
        writer
            .execute(
                "INSERT INTO usage_records (request_id, created_at, consumer_id, model, endpoint, request_status, duration_ms, usage_status) VALUES ('test', '2024-01-01T00:00:00', 'c1', 'gpt-4', 'chat_completions', 'completed', 100, 'unavailable')",
                [],
            )
            .unwrap();

        let v2 = bc.data_version();
        assert_ne!(v1, v2, "data_version must change across connections");
    }

    #[tokio::test]
    async fn test_broadcast_fans_out() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.db");

        let writer = Connection::open(&path).unwrap();
        crate::ledger::configure_sqlite(&writer).unwrap();
        crate::ledger::init_schema(&writer).unwrap();

        let bc = Arc::new(SseBroadcaster::new(&path, 50).unwrap());
        bc.start();

        let mut rx1 = bc.subscribe();
        let mut rx2 = bc.subscribe();
        let mut rx3 = bc.subscribe();

        writer
            .execute(
                "INSERT INTO usage_records (request_id, created_at, consumer_id, model, endpoint, request_status, duration_ms, usage_status) VALUES ('test2', '2024-01-01T00:00:00', 'c1', 'gpt-4', 'chat_completions', 'completed', 100, 'unavailable')",
                [],
            )
            .unwrap();

        let r1 = tokio::time::timeout(Duration::from_secs(2), rx1.recv()).await;
        let r2 = tokio::time::timeout(Duration::from_secs(2), rx2.recv()).await;
        let r3 = tokio::time::timeout(Duration::from_secs(2), rx3.recv()).await;

        assert!(r1.is_ok(), "first subscriber should receive");
        assert!(r2.is_ok(), "second subscriber should receive");
        assert!(r3.is_ok(), "third subscriber should receive");
    }
}
