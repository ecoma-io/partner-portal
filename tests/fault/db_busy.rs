//! A database that cannot be written to right now.
//!
//! Two regimes, and the difference between them is the point:
//!
//! * A lock that clears inside SQLite's `busy_timeout` is absorbed silently.
//!   Nothing is lost, nothing is degraded, and the request is simply *later* —
//!   which is the correct outcome when the acceptance cannot be written yet.
//! * A lock that outlives `busy_timeout` makes the write attempt fail with
//!   BUSY, and the writer's retry loop is what has to keep the record. If that
//!   loop were absent the record would be dropped, silently, at the exact moment
//!   the database was under the most contention.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{
    Behaviour, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT, chat_request, open_db,
    raw_rollup_totals, row_count, wait_for_terminal_count,
};
use http::{Method, StatusCode};

/// Hold SQLite's write lock on a second connection, until released.
///
/// `BEGIN IMMEDIATE` takes the RESERVED lock up front, which is what the ledger's
/// single writer contends on. WAL mode means readers are unaffected: the reader
/// connections the tests use keep working while this is held, which is how the
/// tests can observe the ledger during the outage.
struct WriteLock {
    release: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl WriteLock {
    fn take(path: &Path) -> Self {
        let conn = open_db(path);
        conn.execute_batch("BEGIN IMMEDIATE")
            .expect("acquire the ledger write lock");

        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            // Held until the test says so. The timeout is a safety net, not the
            // mechanism: a panicking test must not leave the lock held forever.
            let _ = rx.recv_timeout(Duration::from_secs(60));
            let _ = conn.execute_batch("COMMIT");
        });

        Self {
            release: Some(tx),
            thread: Some(thread),
        }
    }

    /// Release the lock and wait for it to actually be gone.
    fn release(&mut self) {
        if let Some(tx) = self.release.take() {
            let _ = tx.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

impl Drop for WriteLock {
    fn drop(&mut self) {
        self.release();
    }
}

#[tokio::test]
async fn a_short_lock_delays_the_forward_without_losing_the_record() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 5,
        completion: 7,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = Arc::new(TestClient::new());

    let mut lock = WriteLock::take(&server.db_path);

    let url = server.url("/v1/chat/completions");
    let key = server.key().to_string();
    let request = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .call(Method::POST, &url, Some(&key), chat_request("gpt-4o"), &[])
                .await
        }
    });

    // The acceptance has to be durable before the upstream is contacted, and it
    // cannot be written while the lock is held. So the request must still be
    // outstanding, and nothing may have been forwarded — a request the ledger
    // cannot account for is not served.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !request.is_finished(),
        "the request must wait for its durable acceptance instead of being forwarded unmetered"
    );
    assert_eq!(
        upstream.request_count(),
        0,
        "nothing may reach the upstream before the acceptance is durable"
    );
    assert_eq!(row_count(&server.open_db()), 0);

    lock.release();

    let response = request.await.expect("request task");
    assert_eq!(response.status, StatusCode::OK);

    let rows = wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;
    assert_eq!(rows[0].request_status, "completed");
    assert_eq!(rows[0].input_tokens, Some(5));
    assert_eq!(rows[0].output_tokens, Some(7));
    assert_eq!(upstream.request_count(), 1, "the request is forwarded once");
    assert_eq!(raw_rollup_totals(&server.open_db()), (1, 1));

    // The lock never outlived `busy_timeout`, so the writer never had to retry
    // and readiness was never in question.
    assert!(
        !server.logs().contains("blocked by SQLite lock"),
        "a lock within busy_timeout is absorbed silently; log:\n{}",
        server.logs()
    );
    let ready = client
        .get(&server.url("/readyz"), None)
        .await
        .expect("readyz");
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(ready.json()["ledger_ready"], true);
}

#[tokio::test]
async fn a_lock_past_busy_timeout_is_retried_and_the_record_is_kept() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 4,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = Arc::new(TestClient::new());

    let mut lock = WriteLock::take(&server.db_path);

    let url = server.url("/v1/chat/completions");
    let key = server.key().to_string();
    let request = tokio::spawn({
        let client = client.clone();
        async move {
            client
                .call(Method::POST, &url, Some(&key), chat_request("gpt-4o"), &[])
                .await
        }
    });

    // The first write attempt blocks for the whole busy_timeout (5s), fails with
    // SQLITE_BUSY, and must be retried rather than dropped.
    let saw_retry = crate::common::wait_until(Duration::from_secs(15), || {
        server.logs().contains("blocked by SQLite lock")
    })
    .await;
    assert!(
        saw_retry,
        "exceeding busy_timeout must surface as a retry, not a lost record; log:\n{}",
        server.logs()
    );

    // Still nothing forwarded, still nothing committed: the record is retained
    // in memory and re-attempted.
    assert!(
        !request.is_finished(),
        "the request must still be waiting for its durable acceptance"
    );
    assert_eq!(upstream.request_count(), 0);
    assert_eq!(row_count(&server.open_db()), 0);

    lock.release();

    let response = request.await.expect("request task");
    assert_eq!(response.status, StatusCode::OK);

    let rows = wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;
    assert_eq!(rows[0].request_status, "completed");
    assert_eq!(rows[0].input_tokens, Some(3));
    assert_eq!(rows[0].output_tokens, Some(4));
    assert_eq!(upstream.request_count(), 1);
    assert_eq!(
        raw_rollup_totals(&server.open_db()),
        (1, 1),
        "the retried record must be committed exactly once"
    );

    // The retry succeeded, so the writer is not marked unhealthy: a transient
    // lock is not a metering failure.
    assert!(
        !server.logs().contains("commit failed permanently"),
        "a retried commit must not be reported as a permanent failure; log:\n{}",
        server.logs()
    );
    let ready = client
        .get(&server.url("/readyz"), None)
        .await
        .expect("readyz");
    assert_eq!(ready.status, StatusCode::OK);
    assert_eq!(ready.json()["ledger_ready"], true);
}
