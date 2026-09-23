//! SIGKILL while a request is in flight.
//!
//! The accept is durable before the upstream is contacted, so a request killed
//! mid-flight must leave a row behind. On the next start that row must be
//! resolved to `interrupted` — never left `in_flight` (which is
//! indistinguishable from a request still running) and never lost.

use std::time::Duration;

use crate::common::{
    Behaviour, BodyReader, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT, chat_request,
    chat_stream_request, find_row, in_flight_count, open_db, raw_rollup_totals, row_count,
    wait_for_terminal, wait_for_terminal_count,
};
use http::{Method, StatusCode};

#[tokio::test]
async fn a_request_killed_mid_flight_is_recovered_as_interrupted() {
    let upstream = MockUpstream::start(Behaviour::StreamHang).await;
    // A generous upstream timeout, so the request is still in flight when the
    // process dies rather than having timed out on its own.
    let spec = Spec::new(&upstream).with_upstream_timeout(60);
    let dir = tempfile::TempDir::new().expect("temp dir");

    let mut server = TestServer::start_in(dir.path(), spec.clone()).await;
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

    // The request reached the upstream, which means the accept was already
    // durable before it was forwarded: that is the state a crash interrupts.
    upstream.wait_for_requests(1, WAIT_TIMEOUT).await;
    assert!(
        crate::common::wait_until(WAIT_TIMEOUT, || {
            find_row(&open_db(&server.db_path), &request_id).is_some()
        })
        .await,
        "the accept must be durable before the upstream is contacted"
    );

    // Hold the response open so the request is genuinely mid-stream when the
    // process dies. `_`-prefixed binding: the body is dropped at end of scope,
    // not here.
    let _reader = BodyReader::new(response.into_body());

    server.sigkill();
    let status = server.wait_exit(Duration::from_secs(10)).await;
    assert!(
        !status.success(),
        "SIGKILL must terminate the process, got {status}"
    );

    // The row survives the kill, still in `in_flight`: nothing rolled it back
    // and nothing resolved it, so recovery is what has to.
    let before = find_row(&open_db(&server.db_path), &request_id).expect("row survives the crash");
    assert_eq!(
        before.request_status, "in_flight",
        "a killed request must be left in_flight for recovery to resolve"
    );

    // Restart against the same database, the same port, the same config.
    let server = TestServer::start_in(dir.path(), spec.clone()).await;

    let recovered = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(recovered.request_status, "interrupted");
    assert_eq!(recovered.usage_status, "unavailable");
    assert_eq!(recovered.input_tokens, None);
    assert_eq!(recovered.output_tokens, None);
    assert_eq!(recovered.consumer_id, server.consumer());
    // The exact wording is not the contract; that a reason is recorded, and that
    // it names the cause (a request that was in flight), is.
    assert!(
        recovered
            .error_message
            .as_deref()
            .unwrap_or_default()
            .contains("in flight"),
        "the recovery reason must say the request was in flight, got {:?}",
        recovered.error_message
    );

    // Exactly one record: recovery resolved the row, it did not add another.
    assert_eq!(row_count(&server.open_db()), 1);
    assert_eq!(in_flight_count(&server.open_db()), 0);
    assert_eq!(
        raw_rollup_totals(&server.open_db()),
        (1, 1),
        "the recovered request must be rolled up exactly once, as a failure"
    );

    // The recovered instance is fully functional: new traffic is served and
    // metered normally.
    upstream.set_behaviour(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    });
    let follow_up = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_request("gpt-4o"),
            &[],
        )
        .await;
    assert_eq!(follow_up.status, StatusCode::OK);
    wait_for_terminal_count(&server.db_path, 2, WAIT_TIMEOUT).await;
    assert_eq!(raw_rollup_totals(&server.open_db()), (2, 2));
    assert_eq!(in_flight_count(&server.open_db()), 0);
}
