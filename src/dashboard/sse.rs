//! Realtime dashboard invalidation over SSE.
//!
//! # What this channel is, and is not
//!
//! SSE here carries **invalidation only**: the event body says "something
//! changed, refetch", never any usage data. SQLite is the single source of
//! truth, and every refetch goes through the consumer-scoped REST API. That has
//! two consequences worth stating explicitly:
//!
//! * A client that misses an event (lag, reconnect, a second instance) still
//!   sees correct data — it just refreshes on the next event or on its own.
//! * Because the payload carries no data, a shared event bus cannot leak one
//!   consumer's traffic to another. Isolation is enforced by the query layer,
//!   not by slicing the stream per subscriber.
//!
//! # Why `PRAGMA data_version`
//!
//! `data_version` changes when *another* connection commits a write, which makes
//! it a cheap, exact change signal that works **across processes**. That last
//! part is what makes a same-VPS rolling update correct: instance A writes the
//! shared database, and instance B's poller notices and notifies its own SSE
//! clients. An in-memory bus could never do that.
//!
//! The poller therefore needs its *own* connection: it must never share the
//! ledger writer's connection, because `data_version` deliberately ignores
//! writes made by the connection that performs the query.

use axum::{
    extract::State,
    response::sse::{Event, KeepAlive, Sse},
};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::broadcast;
use tracing::{error, warn};

use crate::auth::Authenticated;
use crate::proxy::handler::AppState;

/// Buffered notifications per subscriber before it is considered lagged.
const SSE_BUFFER: usize = 128;
/// Comment ping interval, so idle proxies do not close the stream.
const KEEPALIVE_INTERVAL: Duration = Duration::from_secs(15);
/// Client reconnect delay suggested to the browser.
const RETRY_HINT_MS: u64 = 3_000;

/// Polls SQLite's `data_version` on a dedicated connection and fans change
/// notifications out to connected dashboard clients.
pub struct SseBroadcaster {
    poll_conn: Arc<Mutex<Connection>>,
    tx: broadcast::Sender<()>,
    poll_interval_ms: u64,
}

impl SseBroadcaster {
    /// Open the dedicated poll connection.
    pub fn new(db_path: &Path, poll_interval_ms: u64) -> Result<Self, rusqlite::Error> {
        let poll_conn = Connection::open(db_path)?;
        // This connection never writes; it only needs to read the pragma and
        // must not be blocked by an in-flight writer.
        poll_conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA query_only = ON;
             PRAGMA busy_timeout = 5000;",
        )?;

        Ok(Self {
            poll_conn: Arc::new(Mutex::new(poll_conn)),
            tx: broadcast::channel(SSE_BUFFER).0,
            // A zero interval would spin; one millisecond is the useful floor.
            poll_interval_ms: poll_interval_ms.max(1),
        })
    }

    /// Spawn the polling task.
    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let interval = Duration::from_millis(this.poll_interval_ms);

        tokio::spawn(async move {
            let mut last_version = this.data_version();
            loop {
                tokio::time::sleep(interval).await;
                let current = this.data_version();
                if current != last_version {
                    last_version = current;
                    // `send` fails when nobody is subscribed, which is normal.
                    let _ = this.tx.send(());
                }
            }
        });
    }

    pub fn subscribe(&self) -> broadcast::Receiver<()> {
        self.tx.subscribe()
    }

    /// Current `data_version` of the database, as seen by this connection.
    pub fn data_version(&self) -> i64 {
        get_data_version(&self.poll_conn.lock())
    }

    /// Number of connected subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.tx.receiver_count()
    }
}

fn get_data_version(conn: &Connection) -> i64 {
    match conn.query_row("PRAGMA data_version", [], |row| row.get(0)) {
        Ok(v) => v,
        Err(e) => {
            // Returning a constant here would silently stop invalidating, so say
            // so rather than passing it off as an unchanged version.
            error!(error = %e, "could not read PRAGMA data_version");
            -1
        }
    }
}

/// `GET /api/dashboard/events` — invalidation stream.
pub async fn sse_handler(
    State(state): State<Arc<AppState>>,
    Authenticated(_consumer): Authenticated,
) -> impl axum::response::IntoResponse {
    let broadcaster = state.broadcaster.clone();
    sse_response(broadcaster)
}

/// Build the SSE response.
///
/// The stream is notification-only, so it is identical for every consumer and
/// carries no usage data.
pub fn sse_response(broadcaster: Arc<SseBroadcaster>) -> impl axum::response::IntoResponse {
    let mut rx = broadcaster.subscribe();

    let stream: futures::stream::BoxStream<'static, Result<Event, axum::Error>> =
        Box::pin(async_stream::stream! {
            // Tell the client the stream is live so the UI can show a real
            // connection state instead of assuming success.
            yield Ok(Event::default()
                .event("connected")
                .retry(Duration::from_millis(RETRY_HINT_MS))
                .data(r#"{"type":"connected"}"#));

            loop {
                match rx.recv().await {
                    Ok(()) => {
                        yield Ok(Event::default().data(r#"{"type":"data_changed"}"#));
                    }
                    Err(broadcast::error::RecvError::Lagged(n)) => {
                        // A lagged client has missed notifications but no data:
                        // one change event restores correctness.
                        warn!(missed = n, "SSE subscriber lagged; re-sending a change event");
                        yield Ok(Event::default().data(r#"{"type":"data_changed"}"#));
                    }
                    Err(broadcast::error::RecvError::Closed) => break,
                }
            }
        });

    Sse::new(stream).keep_alive(
        KeepAlive::new()
            .interval(KEEPALIVE_INTERVAL)
            .text("keep-alive"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn db(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        conn
    }

    fn insert(conn: &Connection, id: &str) {
        conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint,
                request_status, duration_ms, usage_status
             ) VALUES (?1, '2026-09-24T07:00:00.000000000Z', 'c1', 'gpt-4',
                       'chat_completions', 'completed', 100, 'unavailable')",
            rusqlite::params![id],
        )
        .unwrap();
    }

    #[test]
    fn test_data_version_changes_across_connections() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.db");
        let writer = db(&path);
        let bc = SseBroadcaster::new(&path, 50).unwrap();

        let before = bc.data_version();
        insert(&writer, "t1");
        assert_ne!(
            before,
            bc.data_version(),
            "data_version must reflect a write from another connection"
        );
    }

    #[test]
    fn test_poll_connection_sees_its_own_writes_are_not_the_signal() {
        // Documents the semantics the design depends on: a connection's own
        // writes do not move its own data_version, which is why the poller must
        // be a separate connection from the writer.
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.db");
        let conn = db(&path);

        let before = get_data_version(&conn);
        insert(&conn, "self");
        assert_eq!(
            before,
            get_data_version(&conn),
            "own writes must not change own data_version"
        );
    }

    #[tokio::test]
    async fn test_broadcast_fans_out_to_every_subscriber() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.db");
        let writer = db(&path);

        let bc = Arc::new(SseBroadcaster::new(&path, 20).unwrap());
        bc.start();
        assert_eq!(bc.subscriber_count(), 0);

        let mut rx1 = bc.subscribe();
        let mut rx2 = bc.subscribe();
        let mut rx3 = bc.subscribe();
        assert_eq!(bc.subscriber_count(), 3);

        // Give the poller a baseline reading before the write.
        tokio::time::sleep(Duration::from_millis(60)).await;
        insert(&writer, "fanout");

        for (i, rx) in [&mut rx1, &mut rx2, &mut rx3].into_iter().enumerate() {
            let got = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
            assert!(got.is_ok(), "subscriber {i} must receive the change event");
        }
    }

    #[tokio::test]
    async fn test_no_event_when_nothing_changes() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.db");
        let _writer = db(&path);

        let bc = Arc::new(SseBroadcaster::new(&path, 20).unwrap());
        bc.start();
        let mut rx = bc.subscribe();

        let got = tokio::time::timeout(Duration::from_millis(200), rx.recv()).await;
        assert!(got.is_err(), "an idle database must not produce events");
    }

    #[tokio::test]
    async fn test_multiple_writes_produce_notifications() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("test.db");
        let writer = db(&path);
        let bc = Arc::new(SseBroadcaster::new(&path, 20).unwrap());
        bc.start();
        let mut rx = bc.subscribe();

        tokio::time::sleep(Duration::from_millis(60)).await;
        insert(&writer, "w1");
        let first = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
        assert!(first.is_ok());

        insert(&writer, "w2");
        let second = tokio::time::timeout(Duration::from_secs(3), rx.recv()).await;
        assert!(
            second.is_ok(),
            "every write must invalidate, not just the first"
        );
    }
}
