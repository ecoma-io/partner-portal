//! Readiness as a function of the metering queue's configured depth.
//!
//! `LedgerWriter` treats the queue's configured depth as its definition of
//! "outstanding work": readiness is published as false once more than half of it
//! is in flight. That arithmetic has to survive a queue configured absurdly
//! small. The naive form — `depth < queue_size / 2` — computes `depth < 0` for a
//! depth of one and latches readiness false for the life of the process, and an
//! instance that serves and accounts for traffic correctly while reporting
//! itself permanently unready is a silent deployment failure: it never enters
//! rotation and nothing says why.
//!
//! Both degenerate depths are therefore asserted here. One must work; the other
//! must be refused before anything is served.

use std::time::Duration;

use crate::common::{
    Behaviour, MockUpstream, READY_TIMEOUT, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    chat_request, in_flight_count, raw_rollup_totals, row_count, wait_for_terminal_count,
};
use http::{Method, StatusCode};

/// Assert that an instance configured with `queue_size` reaches readiness and
/// serves a metered request.
async fn assert_tiny_queue_is_usable(queue_size: usize) {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_queue(queue_size, 1, 5);
    let dir = tempfile::TempDir::new().expect("temp dir");

    // `spawn_in`, not `start_in`: readiness is the subject here, so it is
    // asserted explicitly below rather than assumed by the starter.
    let server = TestServer::spawn_in(dir.path(), spec).await;
    server.await_healthz(READY_TIMEOUT).await;
    let client = TestClient::new();

    let mut ready = None;
    for _ in 0..200 {
        let response = client
            .get(&server.url("/readyz"), None)
            .await
            .expect("the instance is alive and answering");
        if response.status == StatusCode::OK {
            ready = Some(response);
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let ready = ready.unwrap_or_else(|| {
        panic!(
            "an idle instance with queue_size={queue_size} must report ready; log:\n{}",
            server.logs()
        )
    });
    assert_eq!(ready.json()["ledger_ready"], true);
    assert_eq!(ready.json()["shutting_down"], false);
    assert_eq!(
        ready.json()["ledger_queue_depth"],
        0,
        "an idle queue is empty by definition"
    );

    // Ready is not enough on its own: the instance has to work, and the queue has
    // to empty again so readiness is not a one-off.
    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_request("gpt-4o"),
            &[],
        )
        .await;
    assert_eq!(response.status, StatusCode::OK);
    let rows = wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;
    assert_eq!(rows[0].request_status, "completed");
    assert_eq!(raw_rollup_totals(&server.open_db()), (1, 1));
    assert_eq!(in_flight_count(&server.open_db()), 0);
    assert_eq!(row_count(&server.open_db()), 1);
}

#[tokio::test]
async fn a_queue_of_one_still_reaches_readiness() {
    assert_tiny_queue_is_usable(1).await;
}

/// A queue of zero is refused at startup, before anything is served.
///
/// This is the other half of the same edge: `tokio::sync::mpsc::channel(0)`
/// panics, and the configuration is validated so the process fails with a
/// readable reason instead. A refused start is the honest outcome — an instance
/// that cannot buffer a single record has no way to honour the acceptance
/// guarantee under load.
#[tokio::test]
async fn a_queue_of_zero_is_refused_at_startup() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_queue(0, 1, 5);
    let dir = tempfile::TempDir::new().expect("temp dir");

    let mut server = TestServer::spawn_in(dir.path(), spec).await;
    let status = server.wait_exit(READY_TIMEOUT).await;
    assert!(
        !status.success(),
        "an unusable configuration must not start, got {status}"
    );
    let logs = server.logs();
    assert!(
        logs.contains("queue_size"),
        "the refusal must name the offending setting; log:\n{logs}"
    );
    assert_eq!(
        upstream.request_count(),
        0,
        "nothing may be served by an instance that refused to start"
    );
    assert!(
        !server.db_path.exists(),
        "a refused start must not create a ledger it never uses"
    );
}
