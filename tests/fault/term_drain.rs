//! SIGTERM during an in-flight stream: the graceful path.
//!
//! The ordering under test is the one documented at the top of `main.rs`:
//! readiness fails first, then the listener stops accepting, then the request in
//! flight is allowed to finish, and only then is the metering pipeline drained.
//! A shutdown that closed the writer underneath a live producer would turn a
//! completed request into an unrecorded one — the single failure this product
//! exists to avoid.

use std::time::Duration;

use crate::common::{
    Behaviour, BodyReader, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    chat_stream_request, in_flight_count, raw_rollup_totals, wait_for_terminal,
};
use http::{Method, StatusCode};

/// Long enough that readiness can be observed, and confirmed to be a *phase*,
/// while the stream is still running.
const GRACE_SECS: u64 = 3;

#[tokio::test]
async fn a_sigterm_lets_the_in_flight_request_finish_and_drains_the_queue() {
    let upstream = MockUpstream::start(Behaviour::ChatStream {
        prompt: 6,
        completion: 6,
        cached: 0,
        events: 8,
        delay_ms: 120,
    })
    .await;
    let spec = Spec::new(&upstream).with_shutdown_grace(GRACE_SECS);
    let mut server = TestServer::start(spec).await;
    let client = TestClient::new();

    let response = client
        .send(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_stream_request("gpt-4o"),
            &[],
        )
        .await
        .expect("streaming request");
    assert_eq!(response.status(), StatusCode::OK);
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("the forwarded stream carries the request id")
        .to_string();

    let mut reader = BodyReader::new(response.into_body());
    reader
        .read_until("token-0", WAIT_TIMEOUT)
        .await
        .expect("the stream must start");
    upstream.wait_for_requests(1, WAIT_TIMEOUT).await;

    server.sigterm();

    // Readiness must fail while the instance can still serve. That is the window
    // a load balancer needs to take it out of rotation before the listener
    // closes, and this is the phase the request below must survive.
    let mut unready = None;
    for _ in 0..500 {
        match client.get(&server.url("/readyz"), None).await {
            Ok(response) if response.status == StatusCode::SERVICE_UNAVAILABLE => {
                unready = Some(response);
                break;
            }
            _ => tokio::time::sleep(Duration::from_millis(20)).await,
        }
    }
    let unready = unready.expect("readiness must fail during the grace period");
    assert_eq!(unready.json()["ready"], false);
    assert_eq!(unready.json()["shutting_down"], true);
    assert_eq!(
        unready.json()["ledger_ready"],
        true,
        "the ledger is still accepting; only shutdown is in progress"
    );

    // The stream in flight must still complete, intact.
    let body = reader
        .read_to_end(Duration::from_secs(15))
        .await
        .expect("the in-flight stream must be allowed to finish");
    let text = String::from_utf8_lossy(&body).to_string();
    assert!(
        text.contains("data: [DONE]"),
        "the draining shutdown must not truncate the response; got:\n{text}"
    );

    let status = server.wait_exit(Duration::from_secs(20)).await;
    assert_eq!(status.code(), Some(0), "shutdown must be a clean exit");

    // The request that was in flight finished, so it must be recorded as
    // completed — not interrupted, and not lost.
    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "completed");
    assert_eq!(row.input_tokens, Some(6));
    assert_eq!(row.output_tokens, Some(6));

    assert_eq!(in_flight_count(&server.open_db()), 0);
    assert_eq!(raw_rollup_totals(&server.open_db()), (1, 1));

    let logs = server.logs();
    assert!(
        logs.contains("readiness now fails"),
        "readiness must be failed before the drain; log:\n{logs}"
    );
    assert!(
        logs.contains("draining the metering pipeline"),
        "the pipeline must be drained explicitly; log:\n{logs}"
    );
    assert!(
        logs.contains("shutdown complete"),
        "shutdown must run to completion; log:\n{logs}"
    );
    assert!(
        !logs.contains("commit failed permanently"),
        "a graceful shutdown must not lose a commit; log:\n{logs}"
    );

    // Restarting against the same database finds nothing to recover: the drain
    // left no `in_flight` record behind.
    let restarted = TestServer::start_in(&server.root, server.spec.clone()).await;
    assert_eq!(in_flight_count(&restarted.open_db()), 0);
    assert_eq!(raw_rollup_totals(&restarted.open_db()), (1, 1));
}
