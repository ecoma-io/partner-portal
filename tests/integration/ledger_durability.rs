//! Ledger durability: one terminal record per accepted request, raw and rollup
//! in agreement, and `NULL` never collapsed into `0`.
//!
//! These are read directly from SQLite rather than through the dashboard API,
//! because the API deliberately hides the distinctions under test (`NULL` versus
//! `0`, `in_flight` rows, duplicate ids).

use std::time::Duration;

use crate::common::{
    Behaviour, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT, all_rows, chat_request,
    chat_stream_request, in_flight_count, raw_rollup_totals, row_count, wait_for_terminal,
    wait_for_terminal_count,
};
use bytes::Bytes;
use http::{Method, StatusCode};
use serde_json::json;

#[tokio::test]
async fn every_accepted_request_has_exactly_one_terminal_record() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 10,
        completion: 5,
        cached: 2,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;

    const REQUESTS: usize = 24;
    let mut sent = Vec::new();
    for i in 0..REQUESTS {
        // A mix of models, so the rollup has to distinguish them.
        sent.push(tokio::spawn({
            let url = server.url("/v1/chat/completions");
            let key = server.key().to_string();
            let model = if i % 2 == 0 { "gpt-4o" } else { "gpt-4o-mini" };
            async move {
                let client = TestClient::new();
                client
                    .call(Method::POST, &url, Some(&key), chat_request(model), &[])
                    .await
            }
        }));
    }
    for handle in sent {
        let response = handle.await.expect("request task");
        assert_eq!(response.status, StatusCode::OK);
    }

    let rows = wait_for_terminal_count(&server.db_path, REQUESTS as i64, WAIT_TIMEOUT).await;

    // Exactly one row per request, no duplicates, no orphans: the ids the proxy
    // minted are unique and every row reached a terminal state.
    let unique: std::collections::HashSet<&str> =
        rows.iter().map(|r| r.request_id.as_str()).collect();
    assert_eq!(unique.len(), REQUESTS, "request ids must be unique");
    assert!(
        rows.iter().all(|r| r.is_terminal()),
        "no request may be left in_flight"
    );
    assert_eq!(in_flight_count(&server.open_db()), 0);

    // The rollup agrees with the raw ledger, exactly.
    let (terminal, rolled) = raw_rollup_totals(&server.open_db());
    assert_eq!(terminal, REQUESTS as i64);
    assert_eq!(rolled, REQUESTS as i64, "rollup count must equal raw count");
}

#[tokio::test]
async fn a_response_without_usage_is_recorded_as_unavailable_not_zero() {
    let upstream = MockUpstream::start(Behaviour::ChatJsonWithoutUsage).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

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

    let request_id = response.request_id().expect("x-request-id header");
    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "completed");
    assert_eq!(
        row.usage_status, "unavailable",
        "the provider reported nothing; the ledger must say so"
    );
    assert_eq!(row.input_tokens, None);
    assert_eq!(row.output_tokens, None);
    assert_eq!(row.cached_tokens, None);

    // The rollup still counts the request, but contributes no tokens to the
    // sums — a zero-token request and an unmeasured one must not be conflated.
    let db = server.open_db();
    let (requests, tokens): (i64, i64) = db
        .query_row(
            "SELECT SUM(request_count), SUM(total_input_tokens) FROM usage_hourly",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("rollup totals");
    assert_eq!(requests, 1);
    assert_eq!(tokens, 0);
    let unavailable: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM usage_records WHERE usage_status = 'unavailable' AND input_tokens IS NULL",
            [],
            |r| r.get(0),
        )
        .expect("count unavailable");
    assert_eq!(unavailable, 1);
}

#[tokio::test]
async fn raw_rows_and_the_rollup_agree_token_for_token() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 7,
        completion: 3,
        cached: 1,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    // Two models, three requests each, alternating stream/non-stream, so the
    // rollup's uniqueness key (hour, consumer, model, endpoint, streaming) is
    // exercised rather than collapsing everything into one row.
    for model in ["gpt-4o", "gpt-4o-mini"] {
        for stream in [false, true] {
            for _ in 0..3 {
                let (url, body) = if stream {
                    (
                        server.url("/v1/chat/completions"),
                        chat_stream_request(model),
                    )
                } else {
                    (server.url("/v1/chat/completions"), chat_request(model))
                };
                let response = client
                    .call(Method::POST, &url, Some(server.key()), body, &[])
                    .await;
                assert_eq!(
                    response.status,
                    StatusCode::OK,
                    "model {model} stream {stream}"
                );
            }
        }
    }

    let rows = wait_for_terminal_count(&server.db_path, 12, WAIT_TIMEOUT).await;
    let db = server.open_db();

    let raw_input: i64 = rows.iter().filter_map(|r| r.input_tokens).sum();
    let raw_output: i64 = rows.iter().filter_map(|r| r.output_tokens).sum();
    let raw_cached: i64 = rows.iter().filter_map(|r| r.cached_tokens).sum();

    let (requests, input, output, cached, success): (i64, i64, i64, i64, i64) = db
        .query_row(
            "SELECT SUM(request_count), SUM(total_input_tokens), SUM(total_output_tokens),
                    SUM(total_cached_tokens), SUM(success_count)
             FROM usage_hourly",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .expect("rollup totals");

    assert_eq!(requests, 12);
    assert_eq!(
        input, raw_input,
        "rollup input tokens must equal the raw rows"
    );
    assert_eq!(output, raw_output);
    assert_eq!(cached, raw_cached);
    assert_eq!(success, 12);

    // Four distinct rollup buckets: 2 models x 2 streaming modes.
    let buckets: i64 = db
        .query_row("SELECT COUNT(*) FROM usage_hourly", [], |r| r.get(0))
        .expect("bucket count");
    assert_eq!(
        buckets, 4,
        "streaming is part of the rollup key, so stream and non-stream are separate buckets"
    );
}

#[tokio::test]
async fn a_failed_request_is_counted_as_a_failure_in_the_rollup() {
    let upstream = MockUpstream::start(Behaviour::Error {
        status: 503,
        message: "provider overloaded".into(),
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .post_json(
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            json!({"model": "gpt-4o", "messages": []}),
        )
        .await;
    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);

    wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;
    let db = server.open_db();
    let (failures, success, requests): (i64, i64, i64) = db
        .query_row(
            "SELECT SUM(failure_count), SUM(success_count), SUM(request_count) FROM usage_hourly",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
        )
        .expect("rollup totals");
    assert_eq!((requests, failures, success), (1, 1, 0));
}

#[tokio::test]
async fn an_interrupted_stream_is_terminal_and_counts_as_a_failure() {
    let upstream = MockUpstream::start(Behaviour::ChatStream {
        prompt: 9,
        completion: 4,
        cached: 0,
        events: 20,
        delay_ms: 150,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
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
        .expect("x-request-id")
        .to_string();

    // Abandon the client mid-stream: dropping the body is the disconnect.
    drop(response);

    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(
        row.request_status, "interrupted",
        "a client that disconnects mid-stream must not be recorded as a success"
    );
    assert_eq!(row.usage_status, "unavailable");
    assert_eq!(in_flight_count(&server.open_db()), 0);

    // A request that never completed still happened, and the ledger says so.
    let db = server.open_db();
    let failures: i64 = db
        .query_row("SELECT SUM(failure_count) FROM usage_hourly", [], |r| {
            r.get(0)
        })
        .expect("failure count");
    assert_eq!(failures, 1);
}

#[tokio::test]
async fn a_duplicate_request_id_cannot_be_written() {
    // The schema's uniqueness is the last line of defence: even if a record were
    // enqueued twice, the ledger cannot hold two rows for one request id, and the
    // rollup is applied only on the first terminal transition.
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_request("gpt-4o"),
            &[],
        )
        .await;
    let request_id = response.request_id().expect("x-request-id");
    wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;

    let db = server.open_db();
    let duplicate = db.execute(
        "INSERT INTO usage_records (
            request_id, created_at, consumer_id, model, endpoint, streaming,
            http_status, request_status, input_tokens, output_tokens,
            cached_tokens, ttft_ms, duration_ms, usage_status, error_message
         ) VALUES (?1, '2026-01-01T00:00:00.000000000Z', 'c', 'm', 'chat_completions', 0,
                   NULL, 'in_flight', NULL, NULL, NULL, NULL, 0, 'unavailable', NULL)",
        [&request_id],
    );
    assert!(
        duplicate.is_err(),
        "request_id must be UNIQUE: a second row for one request is a double count waiting to happen"
    );
    assert_eq!(row_count(&db), 1);
    assert_eq!(raw_rollup_totals(&db), (1, 1));
}

#[tokio::test]
async fn a_request_body_larger_than_the_limit_never_reaches_the_ledger() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream).with_max_body_size(8 * 1024)).await;
    let client = TestClient::new();

    let oversized = vec![b'a'; 64 * 1024];
    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            Bytes::from(oversized),
            &[],
        )
        .await;

    assert_eq!(response.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(upstream.request_count(), 0, "nothing may be forwarded");
    assert_eq!(
        row_count(&server.open_db()),
        0,
        "a request the proxy never accepted has no ledger record"
    );
    // Give any (incorrect) asynchronous write a chance to appear before passing.
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(row_count(&server.open_db()), 0);
    assert_eq!(all_rows(&server.open_db()).len(), 0);
}
