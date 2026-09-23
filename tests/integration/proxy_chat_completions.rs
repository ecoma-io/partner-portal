//! The request lifecycle through the proxy: what is forwarded, what is recorded,
//! and what a client sees when the upstream misbehaves.
//!
//! Every assertion here is made either against the *mock's* record of the request
//! (what was actually sent upstream) or against the *ledger* (what was recorded).
//! The proxy's own responses are treated as one more observable, not as the
//! source of truth.

use std::time::{Duration, Instant};

use crate::common::{
    ABORT_COMPLETION_TOKENS, ABORT_PROMPT_TOKENS, Behaviour, BodyReader, MockUpstream, Spec,
    TestClient, TestServer, UPSTREAM_KEY, WAIT_TIMEOUT, free_port, in_flight_count, raw_request,
    raw_rollup_totals, row_count, wait_for_single_terminal, wait_for_terminal,
};
use bytes::Bytes;
use http::{Method, StatusCode};
use serde_json::json;

/// A plain request body for the chat endpoint.
fn chat_request(model: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&json!({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}],
        }))
        .unwrap(),
    )
}

#[tokio::test]
async fn non_streaming_success_records_usage_and_forwards_the_body() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 100,
        completion: 50,
        cached: 20,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_request("gpt-4o-mini"),
            &[],
        )
        .await;

    assert_eq!(response.status, StatusCode::OK);
    let request_id = response.request_id().expect("x-request-id header");
    assert_eq!(
        response.json()["usage"]["prompt_tokens"],
        100,
        "the upstream body must reach the client unchanged"
    );

    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "completed");
    assert_eq!(row.http_status, Some(200));
    assert_eq!(row.usage_status, "available");
    assert_eq!(row.input_tokens, Some(100));
    assert_eq!(row.output_tokens, Some(50));
    assert_eq!(row.cached_tokens, Some(20));
    // The model is taken from the request, not from the upstream's echo.
    assert_eq!(row.model, "gpt-4o-mini");
    assert_eq!(row.endpoint, "chat_completions");
    assert!(!row.streaming);
    assert_eq!(row.consumer_id, server.consumer());

    assert_eq!(raw_rollup_totals(&server.open_db()), (1, 1));
}

#[tokio::test]
async fn streaming_success_records_usage_from_the_final_chunk_and_stays_incremental() {
    let upstream = MockUpstream::start(Behaviour::ChatStream {
        prompt: 42,
        completion: 17,
        cached: 5,
        events: 4,
        delay_ms: 120,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let started = Instant::now();
    let response = client
        .send(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            Bytes::from(
                serde_json::to_vec(&json!({
                    "model": "gpt-4o",
                    "stream": true,
                    "messages": [{"role": "user", "content": "hello"}],
                }))
                .unwrap(),
            ),
            &[("accept", "text/event-stream")],
        )
        .await
        .expect("streaming request");

    assert_eq!(response.status(), StatusCode::OK);
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("x-request-id header")
        .to_string();
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
        "text/event-stream"
    );

    let mut reader = BodyReader::new(response.into_body());

    // The first token must arrive long before the stream finishes: the body is
    // forwarded frame by frame, never buffered to recover usage.
    reader
        .read_until("\"token-0\"", Duration::from_secs(5))
        .await
        .expect("first token");
    let first_token_at = started.elapsed();

    let body = reader
        .read_to_end(Duration::from_secs(10))
        .await
        .expect("stream completes");
    let total = started.elapsed();
    let text = String::from_utf8_lossy(&body).to_string();

    assert!(
        text.contains("data: [DONE]"),
        "the terminator must be forwarded"
    );
    assert!(text.contains("\"completion_tokens\":17"));
    assert!(
        first_token_at < Duration::from_millis(400),
        "the first token must not wait for the whole stream (took {first_token_at:?} of {total:?})"
    );
    assert!(
        first_token_at < total,
        "incremental forwarding means the first frame precedes completion"
    );

    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "completed");
    assert_eq!(row.input_tokens, Some(42));
    assert_eq!(row.output_tokens, Some(17));
    assert_eq!(row.cached_tokens, Some(5));
    assert!(row.streaming);
    assert!(
        row.ttft_ms.is_some(),
        "time to first token must be measured for a stream"
    );
    assert_eq!(in_flight_count(&server.open_db()), 0);
}

#[tokio::test]
async fn upstream_500_is_recorded_as_failed_with_the_provider_message() {
    let upstream = MockUpstream::start(Behaviour::Error {
        status: 500,
        message: "upstream exploded".into(),
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

    assert_eq!(response.status, StatusCode::INTERNAL_SERVER_ERROR);

    // No `x-request-id`: the proxy only sets it when it forwards an upstream
    // response, and a locally generated error is not one. The record is found
    // instead by being the only one in the ledger.
    let row = wait_for_single_terminal(&server.db_path, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "failed");
    assert_eq!(row.http_status, Some(500));
    assert_eq!(row.usage_status, "unavailable");
    assert_eq!(row.input_tokens, None);
    assert_eq!(
        row.error_message.as_deref(),
        Some("upstream exploded"),
        "the provider's own message must be recorded, not a generic one"
    );
}

#[tokio::test]
async fn upstream_connection_refused_is_recorded_as_failed() {
    // Nothing is listening on this port: the proxy cannot even open a connection.
    let dead_url = format!("http://127.0.0.1:{}", free_port());
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let spec = Spec::new(&upstream)
        .with_upstream_url(&dead_url)
        .with_upstream_timeout(5);
    let server = TestServer::start(spec).await;
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

    assert_eq!(response.status, StatusCode::BAD_GATEWAY);

    let row = wait_for_single_terminal(&server.db_path, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "failed");
    assert_eq!(row.http_status, Some(502));
    assert_eq!(row.usage_status, "unavailable");
    assert!(
        row.error_message
            .as_deref()
            .unwrap_or_default()
            .contains("upstream connection failed"),
        "got {:?}",
        row.error_message
    );
    assert_eq!(
        upstream.request_count(),
        0,
        "nothing may reach the upstream"
    );
}

#[tokio::test]
async fn hop_by_hop_headers_are_stripped_and_host_is_rewritten() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;

    // Hand-rolled HTTP/1.1: a compliant client (including hyper's) consumes
    // `Connection:` itself and would never put an arbitrary token in it, so the
    // `x-custom-hop` case can only be exercised off a raw socket.
    let body = chat_request("gpt-4o");
    let request = format!(
        "POST /v1/chat/completions HTTP/1.1\r\n\
         host: proxy.invalid\r\n\
         authorization: Bearer {}\r\n\
         content-type: application/json\r\n\
         content-length: {}\r\n\
         connection: close, x-custom-hop\r\n\
         keep-alive: timeout=5\r\n\
         proxy-authorization: Basic c2VjcmV0\r\n\
         x-custom-hop: should-not-cross\r\n\
         accept-encoding: gzip, br\r\n\
         x-keep-me: yes\r\n\
         \r\n\
         {}",
        server.key(),
        body.len(),
        String::from_utf8_lossy(&body),
    );
    let response = raw_request(&server.addr, &request).await;
    assert_eq!(response.status, StatusCode::OK, "raw: {}", response.raw);

    let seen = upstream.wait_for_requests(1, WAIT_TIMEOUT).await;
    let forwarded = &seen[0];
    assert_eq!(forwarded.path, "/v1/chat/completions");
    assert_eq!(forwarded.method, "POST");

    for header in [
        "connection",
        "keep-alive",
        "proxy-authorization",
        "proxy-authenticate",
        "te",
        "trailer",
        "upgrade",
        "transfer-encoding",
    ] {
        assert!(
            forwarded.header(header).is_none(),
            "hop-by-hop header {header} must not be forwarded (saw {:?})",
            forwarded.header(header)
        );
    }
    assert!(
        forwarded.header("x-custom-hop").is_none(),
        "a header named by Connection must not be forwarded"
    );
    assert!(
        forwarded.header("accept-encoding").is_none(),
        "a compressed body could not be scanned for usage"
    );
    assert_eq!(
        forwarded.header("x-keep-me"),
        Some("yes"),
        "end-to-end headers must survive"
    );
    assert_eq!(
        forwarded.header("host"),
        Some(upstream.addr.to_string().as_str()),
        "Host must describe the upstream connection, not the client's"
    );

    // The body must arrive byte-for-byte.
    assert_eq!(forwarded.json()["model"], "gpt-4o");
}

#[tokio::test]
async fn authorization_is_replaced_with_the_upstream_credential() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 4,
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
    assert_eq!(response.status, StatusCode::OK);

    let seen = upstream.wait_for_requests(1, WAIT_TIMEOUT).await;
    let forwarded = &seen[0];
    let authorization = forwarded.header("authorization").expect("authorization");

    assert_eq!(authorization, format!("Bearer {UPSTREAM_KEY}"));
    assert!(
        !authorization.contains(server.key()),
        "the client credential must never reach the upstream"
    );
}

#[tokio::test]
async fn models_endpoint_is_proxied_and_deliberately_not_metered() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .get_json(&server.url("/v1/models"), Some(server.key()))
        .await;

    assert_eq!(response.status, StatusCode::OK);
    assert_eq!(response.json()["data"][0]["id"], "mock-model");

    let seen = upstream.wait_for_requests(1, WAIT_TIMEOUT).await;
    assert_eq!(seen[0].path, "/v1/models");
    assert_eq!(seen[0].method, "GET");
    assert_eq!(
        seen[0].header("authorization"),
        Some(format!("Bearer {UPSTREAM_KEY}").as_str())
    );

    // Discovery consumes no tokens and is excluded from the ledger on purpose;
    // recording it would put zero-usage noise into every usage view.
    assert_eq!(
        row_count(&server.open_db()),
        0,
        "/v1/models must not create a ledger record"
    );
}

#[tokio::test]
async fn an_unknown_path_returns_json_404_not_the_spa() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .get_json(&server.url("/v1/embeddings"), Some(server.key()))
        .await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
    assert_eq!(
        response.header("content-type").as_deref(),
        Some("application/json"),
        "an API path must never fall back to the HTML entry point"
    );
    assert_eq!(response.json()["error"]["code"], "not_found");
    assert_eq!(upstream.request_count(), 0, "nothing may be forwarded");
    assert_eq!(row_count(&server.open_db()), 0);
}

#[tokio::test]
async fn a_stream_that_ends_without_usage_is_completed_with_null_tokens() {
    let upstream = MockUpstream::start(Behaviour::ChatStreamWithoutUsage).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .send(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            Bytes::from(
                serde_json::to_vec(&json!({
                    "model": "gpt-4o",
                    "stream": true,
                    "messages": [],
                }))
                .unwrap(),
            ),
            &[],
        )
        .await
        .expect("streaming request");
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("x-request-id")
        .to_string();
    let _ = BodyReader::new(response.into_body())
        .read_to_end(Duration::from_secs(5))
        .await
        .expect("body completes");

    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(
        row.request_status, "completed",
        "a stream that ends cleanly completed, even without a usage event"
    );
    assert_eq!(row.usage_status, "unavailable");
    assert_eq!(row.input_tokens, None, "unreported usage must stay NULL");
    assert_eq!(row.output_tokens, None);
    assert_ne!(row.input_tokens, Some(0), "unavailable is not zero");

    // The rollup sums it as 0 tokens while the raw row keeps NULL, so a total
    // can never be confused with a real zero-token request.
    let (input,): (i64,) = server
        .open_db()
        .query_row(
            "SELECT COALESCE(SUM(total_input_tokens), 0) FROM usage_hourly",
            [],
            |r| Ok((r.get(0)?,)),
        )
        .unwrap();
    assert_eq!(input, 0);
}

#[tokio::test]
async fn an_upstream_stream_that_breaks_is_failed_and_keeps_partial_usage() {
    let upstream = MockUpstream::start(Behaviour::StreamAbort { after: 2 }).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .send(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            Bytes::from(
                serde_json::to_vec(&json!({
                    "model": "gpt-4o",
                    "stream": true,
                    "messages": [],
                }))
                .unwrap(),
            ),
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

    // The client sees the stream end; the ledger is what records that it broke.
    let mut reader = BodyReader::new(response.into_body());
    let _ = reader.read_to_end(Duration::from_secs(5)).await;

    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(
        row.request_status, "failed",
        "a broken upstream stream must not be recorded as a success"
    );
    assert_eq!(row.input_tokens, Some(ABORT_PROMPT_TOKENS));
    assert_eq!(row.output_tokens, Some(ABORT_COMPLETION_TOKENS));
    assert!(
        row.error_message
            .as_deref()
            .unwrap_or_default()
            .contains("upstream stream broke"),
        "got {:?}",
        row.error_message
    );
}

#[tokio::test]
async fn a_hanging_stream_is_broken_by_the_idle_timeout() {
    let upstream = MockUpstream::start(Behaviour::StreamHang).await;
    let spec = Spec::new(&upstream).with_upstream_timeout(2);
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    let response = client
        .send(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            Bytes::from(
                serde_json::to_vec(&json!({
                    "model": "gpt-4o",
                    "stream": true,
                    "messages": [],
                }))
                .unwrap(),
            ),
            &[],
        )
        .await
        .expect("streaming request");
    let request_id = response
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("x-request-id")
        .to_string();

    let mut reader = BodyReader::new(response.into_body());
    // The first token arrives; then the upstream says nothing at all.
    reader
        .read_until("token-0", Duration::from_secs(2))
        .await
        .expect("first token");

    let row = wait_for_terminal(&server.db_path, &request_id, Duration::from_secs(15)).await;
    assert_eq!(row.request_status, "failed");
    assert!(
        row.error_message
            .as_deref()
            .unwrap_or_default()
            .contains("sent no data for"),
        "the idle timeout must be the recorded cause, got {:?}",
        row.error_message
    );
}

#[tokio::test]
async fn malformed_upstream_json_records_unavailable_usage_not_a_parse_panic() {
    let upstream = MockUpstream::start(Behaviour::MalformedJson).await;
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
    assert_eq!(response.body, Bytes::from_static(b"{ this is not json"));
    let request_id = response.request_id().expect("x-request-id");

    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "completed");
    assert_eq!(row.usage_status, "unavailable");
    assert_eq!(row.input_tokens, None);
}

#[tokio::test]
async fn an_oversized_request_body_is_rejected_before_it_is_forwarded() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_max_body_size(1024);
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    let big = Bytes::from(vec![b'x'; 8 * 1024]);
    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            big,
            &[],
        )
        .await;

    assert_eq!(response.status, StatusCode::PAYLOAD_TOO_LARGE);
    assert_eq!(
        upstream.request_count(),
        0,
        "an unreadable body must not be forwarded"
    );
    assert_eq!(
        row_count(&server.open_db()),
        0,
        "a request that was never accepted has no ledger identity"
    );
}

#[tokio::test]
async fn an_oversized_upstream_response_is_refused_rather_than_truncated() {
    // The proxy buffers non-streaming responses to find `usage`, but only under a
    // cap: a broken or hostile upstream must not be able to exhaust memory. A body
    // that hits the cap is not a body that arrived whole, and nothing has been
    // sent to the client yet, so the honest answer is a failure. Forwarding the
    // truncated prefix (under the upstream's own `Content-Length`) and recording
    // the request as `completed` would state that a request succeeded which was
    // in fact served incomplete.
    const CAP: usize = 32 * 1024 * 1024;
    let upstream = MockUpstream::start(Behaviour::OversizedResponse {
        bytes: CAP + 2 * 1024 * 1024,
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

    assert_eq!(response.status, StatusCode::BAD_GATEWAY);
    assert_eq!(response.json()["error"]["type"], "upstream_error");
    assert_eq!(
        response.json()["error"]["message"],
        "Upstream response could not be read in full"
    );

    // Locally generated errors carry no `x-request-id` (it is echoed only when an
    // upstream response is forwarded), so the record is identified as "the ledger
    // holds exactly this one".
    let row = wait_for_single_terminal(&server.db_path, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "failed");
    assert_eq!(row.usage_status, "unavailable");
    assert_eq!(row.input_tokens, None);
    assert_eq!(row.output_tokens, None);
    assert_eq!(row.http_status, Some(502));
    assert!(
        row.error_message
            .as_deref()
            .unwrap_or_default()
            .contains("buffering cap"),
        "the recorded reason must name the cap, got {:?}",
        row.error_message
    );
    assert_eq!(upstream.request_count(), 1);
}

/// A query string is part of the request the upstream has to see, and must never
/// stop the request being recognised for what it is.
///
/// This guards a defect that was observed against the built binary: `main.rs`
/// passes `uri().path_and_query()` into `handle_proxy` so the query reaches the
/// upstream, while `Endpoint::from_path` used to compare the whole string
/// against three exact paths. Every request carrying a query was classified as
/// an unknown endpoint and answered 404 without being forwarded or metered:
///
/// ```text
///   POST /v1/chat/completions?beta=true  -> 404 {"error":{"message":"Not Found",...}}
///   GET  /v1/models?x=1                  -> 404
///   POST /v1/chat/completions            -> reaches the upstream
/// ```
///
/// `from_path` now strips the query before matching, so the query is forwarded
/// as request data and is never read as identity.
#[tokio::test]
async fn a_query_string_does_not_make_an_inference_endpoint_unknown() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 4,
        completion: 5,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    // The query claims an identity the configuration does not grant. It must be
    // forwarded as request data, not read as identity, and above all not read as
    // an unknown route.
    let response = client
        .call(
            Method::POST,
            &format!(
                "{}?consumer_id=consumer-from-the-query",
                server.url("/v1/chat/completions")
            ),
            Some(server.key()),
            chat_request("gpt-4o"),
            &[],
        )
        .await;

    assert_eq!(
        response.status,
        StatusCode::OK,
        "a query string must not turn a known endpoint into a 404: {}",
        response.text()
    );
    let request_id = response.request_id().expect("x-request-id");

    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(row.endpoint, "chat_completions");
    assert_eq!(
        row.consumer_id,
        server.consumer(),
        "identity comes from the configuration, never from the query string"
    );
    assert_eq!(upstream.request_count(), 1);
}

#[tokio::test]
async fn the_responses_endpoint_is_metered_non_streaming_and_streaming() {
    let upstream = MockUpstream::start(Behaviour::ResponsesJson {
        input: 11,
        output: 22,
        cached: 3,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let non_stream = client
        .post_json(
            &server.url("/v1/responses"),
            Some(server.key()),
            json!({"model": "gpt-5", "input": "hello"}),
        )
        .await;
    assert_eq!(non_stream.status, StatusCode::OK);
    let first = wait_for_terminal(
        &server.db_path,
        &non_stream.request_id().expect("x-request-id"),
        WAIT_TIMEOUT,
    )
    .await;
    assert_eq!(first.endpoint, "responses");
    assert_eq!(first.input_tokens, Some(11));
    assert_eq!(first.output_tokens, Some(22));
    assert_eq!(first.cached_tokens, Some(3));
    assert_eq!(first.model, "gpt-5");

    upstream.set_behaviour(Behaviour::ResponsesStream {
        input: 60,
        output: 40,
        cached: 0,
        events: 3,
        delay_ms: 0,
    });

    let stream = client
        .send(
            Method::POST,
            &server.url("/v1/responses"),
            Some(server.key()),
            Bytes::from(
                serde_json::to_vec(&json!({
                    "model": "gpt-5",
                    "stream": true,
                    "input": "hello",
                }))
                .unwrap(),
            ),
            &[("accept", "text/event-stream")],
        )
        .await
        .expect("responses stream");
    assert_eq!(stream.status(), StatusCode::OK);
    let request_id = stream
        .headers()
        .get("x-request-id")
        .and_then(|v| v.to_str().ok())
        .expect("x-request-id")
        .to_string();

    let body = BodyReader::new(stream.into_body())
        .read_to_end(Duration::from_secs(5))
        .await
        .expect("responses stream completes");
    assert!(String::from_utf8_lossy(&body).contains("response.completed"));

    let second = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(second.endpoint, "responses");
    assert_eq!(
        second.input_tokens,
        Some(60),
        "usage from the terminal response.completed event must be recorded"
    );
    assert_eq!(second.output_tokens, Some(40));
    assert!(second.streaming);

    let (terminal, rolled) = raw_rollup_totals(&server.open_db());
    assert_eq!((terminal, rolled), (2, 2));
}

#[tokio::test]
async fn a_non_streaming_client_request_against_an_sse_upstream_still_meters_usage() {
    // The proxy decides streaming from the upstream's content type as well as
    // the client's `stream` flag, so a mislabelled request is still accounted
    // for and still forwarded incrementally.
    let upstream = MockUpstream::start(Behaviour::chat_stream(9, 4)).await;
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
    assert_eq!(
        response.header("content-type").as_deref(),
        Some("text/event-stream")
    );

    let row = wait_for_terminal(
        &server.db_path,
        &response.request_id().expect("x-request-id"),
        WAIT_TIMEOUT,
    )
    .await;
    assert_eq!(row.request_status, "completed");
    assert_eq!(row.input_tokens, Some(9));
    assert_eq!(row.output_tokens, Some(4));
}

#[tokio::test]
async fn a_cached_usage_object_is_recorded_verbatim() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1000,
        completion: 250,
        cached: 900,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .post_json(
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            TestClient::chat_body("gpt-4o", false),
        )
        .await;

    let row = wait_for_terminal(
        &server.db_path,
        &response.request_id().expect("x-request-id"),
        WAIT_TIMEOUT,
    )
    .await;
    assert_eq!(row.cached_tokens, Some(900));

    let (cached,): (i64,) = server
        .open_db()
        .query_row("SELECT total_cached_tokens FROM usage_hourly", [], |r| {
            Ok((r.get(0)?,))
        })
        .unwrap();
    assert_eq!(cached, 900, "cached tokens must reach the rollup too");
}

#[tokio::test]
async fn an_upstream_error_message_is_recorded_verbatim() {
    let upstream = MockUpstream::start(Behaviour::Error {
        status: 429,
        message: "slow down".into(),
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .post_json(
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            TestClient::chat_body("gpt-4o", false),
        )
        .await;

    assert_eq!(response.status, StatusCode::TOO_MANY_REQUESTS);
    let row = wait_for_single_terminal(&server.db_path, WAIT_TIMEOUT).await;
    assert_eq!(row.http_status, Some(429));
    assert_eq!(row.request_status, "failed");
    assert_eq!(row.error_message.as_deref(), Some("slow down"));
}

#[tokio::test]
async fn an_upstream_error_body_that_is_not_json_is_not_invented() {
    // The provider's body carries no machine-readable message, so the ledger
    // must say only what is known — the status — rather than dressing up the
    // raw HTML/text as an error message.
    let upstream = MockUpstream::start(Behaviour::ErrorText {
        status: 503,
        body: "<html><body>upstream maintenance</body></html>".into(),
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .post_json(
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            TestClient::chat_body("gpt-4o", false),
        )
        .await;

    assert_eq!(response.status, StatusCode::SERVICE_UNAVAILABLE);
    let row = wait_for_single_terminal(&server.db_path, WAIT_TIMEOUT).await;
    assert_eq!(row.http_status, Some(503));
    assert_eq!(row.request_status, "failed");
    assert_eq!(
        row.error_message.as_deref(),
        Some("upstream returned 503 Service Unavailable")
    );
}

#[tokio::test]
async fn a_key_echoed_by_an_angry_upstream_is_redacted_in_the_ledger() {
    // Providers really do quote the rejected credential back ("Incorrect API key
    // provided: sk-…"). That reason is persisted and served through the
    // dashboard, so the credential must not survive the trip into storage.
    let upstream = MockUpstream::start(Behaviour::Error {
        status: 401,
        message: format!("Incorrect API key provided: {UPSTREAM_KEY}. Check your account."),
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .post_json(
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            TestClient::chat_body("gpt-4o", false),
        )
        .await;

    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    let row = wait_for_single_terminal(&server.db_path, WAIT_TIMEOUT).await;
    let reason = row.error_message.expect("a failed request records why");
    assert!(
        !reason.contains(UPSTREAM_KEY),
        "the upstream credential was committed to the ledger: {reason}"
    );
    assert!(
        reason.contains("<redacted>"),
        "the reason should still say the key was rejected: {reason}"
    );
    // The client still gets the provider's own explanation, and it is the
    // client's own response — not storage — so the upstream's text is forwarded
    // as the upstream wrote it. Redaction is a storage guarantee here.
    assert!(
        response.text().contains("Incorrect API key provided"),
        "the provider's error document is forwarded verbatim"
    );
}

#[tokio::test]
async fn a_locally_generated_error_is_traceable_to_a_ledger_row() {
    // A 502 the proxy produced itself — nothing was ever forwarded. Without a
    // request id on the response there is nothing to correlate the client's
    // report with: the row exists, the log line exists, and the caller can
    // reach neither. `Spec::new` points at the mock, so this one points at a
    // port nothing is listening on.
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let dead_port = free_port();
    let server = TestServer::start(
        Spec::new(&upstream).with_upstream_url(&format!("http://127.0.0.1:{dead_port}")),
    )
    .await;
    let client = TestClient::new();

    let response = client
        .post_json(
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            TestClient::chat_body("gpt-4o", false),
        )
        .await;

    assert_eq!(response.status, StatusCode::BAD_GATEWAY);
    let header = response
        .request_id()
        .expect("a self-generated error must carry its identity");

    let row = wait_for_single_terminal(&server.db_path, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "failed");
    assert_eq!(
        row.request_id, header,
        "the id on the response must be the id in the ledger"
    );
}
