//! More concurrent requests than the metering pipeline can absorb.
//!
//! The contract is backpressure, not loss: when the writer cannot keep up, the
//! producers wait for queue space instead of dropping the record. A proxy that
//! shed metering records under load would under-report exactly the traffic that
//! costs the most, and it would do it silently.
//!
//! The queue here is deliberately tiny (`queue_size: 2`) and the database is
//! deliberately wedged (a second connection holds the write lock), so a handful
//! of concurrent requests is enough to saturate it without a load generator.

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Duration;

use crate::common::{
    Behaviour, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT, chat_request,
    in_flight_count, open_db, raw_rollup_totals, row_count, wait_for_terminal_count,
};
use http::{Method, StatusCode};

/// Concurrent requests, well past a queue of two plus one in-flight batch.
const CONCURRENCY: usize = 20;

struct WriteLock {
    release: Option<std::sync::mpsc::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl WriteLock {
    fn take(path: &std::path::Path) -> Self {
        let conn = open_db(path);
        conn.execute_batch("BEGIN IMMEDIATE")
            .expect("acquire the ledger write lock");

        let (tx, rx) = std::sync::mpsc::channel::<()>();
        let thread = std::thread::spawn(move || {
            let _ = rx.recv_timeout(Duration::from_secs(60));
            let _ = conn.execute_batch("COMMIT");
        });

        Self {
            release: Some(tx),
            thread: Some(thread),
        }
    }

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
async fn a_saturated_queue_applies_backpressure_instead_of_dropping_records() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 2,
        completion: 3,
        cached: 0,
    })
    .await;
    // One record per batch: every record is an individual COMMIT, so the queue
    // is the only thing buffering work.
    let spec = Spec::new(&upstream).with_queue(2, 1, 5);
    let server = TestServer::start(spec).await;
    let client = Arc::new(TestClient::new());

    let mut lock = WriteLock::take(&server.db_path);

    let mut requests = Vec::new();
    for _ in 0..CONCURRENCY {
        let client = client.clone();
        let url = server.url("/v1/chat/completions");
        let key = server.key().to_string();
        requests.push(tokio::spawn(async move {
            client
                .call(Method::POST, &url, Some(&key), chat_request("gpt-4o"), &[])
                .await
        }));
    }

    // Give every request time to arrive, then check the state under saturation:
    // the writer is wedged, the queue is full, and the producers are parked
    // waiting for space. Nothing has been dropped and nothing was forwarded
    // unmetered.
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        requests.iter().all(|r| !r.is_finished()),
        "requests must block on their durable acceptance, not fail"
    );
    assert_eq!(
        upstream.request_count(),
        0,
        "no request may be forwarded before its acceptance is durable"
    );
    assert_eq!(row_count(&server.open_db()), 0);

    // Readiness is allowed to be red here — the instance is out of rotation
    // *before* it starts shedding load, which is the point of the queue being
    // part of the readiness calculation.
    lock.release();

    let mut ids = HashSet::new();
    for request in requests {
        let response = request.await.expect("request task");
        assert_eq!(
            response.status,
            StatusCode::OK,
            "a saturated queue must delay a request, never reject it"
        );
        ids.insert(response.request_id().expect("the request id is echoed"));
    }
    assert_eq!(ids.len(), CONCURRENCY, "every request has its own identity");

    let rows = wait_for_terminal_count(&server.db_path, CONCURRENCY as i64, WAIT_TIMEOUT).await;
    assert!(rows.iter().all(|r| r.request_status == "completed"));

    assert_eq!(
        upstream.request_count(),
        CONCURRENCY,
        "each request must be forwarded exactly once"
    );
    assert_eq!(in_flight_count(&server.open_db()), 0);
    assert_eq!(
        raw_rollup_totals(&server.open_db()),
        (CONCURRENCY as i64, CONCURRENCY as i64),
        "every record must be committed and rolled up"
    );

    let logs = server.logs();
    for lost in [
        "Lost a metering record",
        "commit failed permanently",
        "Refusing request",
        "Ledger is unhealthy",
    ] {
        assert!(
            !logs.contains(lost),
            "backpressure must not degrade into loss ({lost}); log:\n{logs}"
        );
    }

    // Once the writer catches up, the queue empties and the instance is ready
    // again — saturation is a phase, not a latch.
    let mut recovered = false;
    for _ in 0..500 {
        match client.get(&server.url("/readyz"), None).await {
            Ok(response) if response.status == StatusCode::OK => {
                let body = response.json();
                assert_eq!(body["ledger_queue_depth"], 0);
                assert_eq!(body["ledger_ready"], true);
                recovered = true;
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    assert!(recovered, "readiness must return once the queue drains");
}
