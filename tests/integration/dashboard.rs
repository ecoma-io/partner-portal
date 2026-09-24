//! The dashboard query API: isolation between consumers, keyset pagination, and
//! what happens to query parameters a client controls.

use std::time::Duration;

use crate::common::{
    Behaviour, CLIENT_KEY, KeySpec, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    chat_request, open_db, wait_for_terminal, wait_for_terminal_count,
};
use http::{Method, StatusCode};
use serde_json::Value;

/// A second credential, deliberately mapped to a different consumer.
const OTHER_KEY: &str = "sk-local-other-key";

/// Make one metered request and return the response.
async fn chat(
    client: &TestClient,
    server: &TestServer,
    key: &str,
    model: &str,
    status_ok: bool,
) -> crate::common::HttpResponse {
    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(key),
            chat_request(model),
            &[],
        )
        .await;
    assert_eq!(response.status, StatusCode::OK, "model {model}");
    assert!(status_ok);
    response
}

/// A server with two keys mapped to two distinct consumers.
async fn two_consumer_server(upstream: &MockUpstream) -> TestServer {
    let spec = Spec::new(upstream).with_keys(vec![
        KeySpec::new(CLIENT_KEY, "primary").with_consumer("consumer-a"),
        KeySpec::new(OTHER_KEY, "secondary").with_consumer("consumer-b"),
    ]);
    TestServer::start(spec).await
}

#[tokio::test]
async fn each_key_sees_only_its_own_usage() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 100,
        completion: 50,
        cached: 20,
    })
    .await;
    let server = two_consumer_server(&upstream).await;
    let client = TestClient::new();

    // consumer-a: two requests against gpt-4o.
    for _ in 0..2 {
        chat(&client, &server, CLIENT_KEY, "gpt-4o", true).await;
    }
    // consumer-b: one request against a different model.
    let first_b = chat(&client, &server, OTHER_KEY, "gpt-4o-mini", true).await;
    wait_for_terminal(
        &server.db_path,
        &first_b.request_id().expect("x-request-id"),
        WAIT_TIMEOUT,
    )
    .await;
    wait_for_terminal_count(&server.db_path, 3, WAIT_TIMEOUT).await;

    let a = client
        .get_json(&server.url("/api/dashboard/summary"), Some(CLIENT_KEY))
        .await;
    assert_eq!(a.status, StatusCode::OK);
    assert_eq!(a.json()["total_requests"], 2);
    assert_eq!(a.json()["success_count"], 2);
    assert_eq!(a.json()["total_input_tokens"], 200);
    assert_eq!(a.json()["total_output_tokens"], 100);
    assert_eq!(a.json()["total_cached_tokens"], 40);

    let b = client
        .get_json(&server.url("/api/dashboard/summary"), Some(OTHER_KEY))
        .await;
    assert_eq!(b.status, StatusCode::OK);
    assert_eq!(
        b.json()["total_requests"],
        1,
        "the other consumer's traffic must not leak into this view"
    );
    assert_eq!(b.json()["total_input_tokens"], 100);

    // The request list is scoped the same way.
    let a_requests = client
        .get_json(
            &server.url("/api/dashboard/requests?limit=200"),
            Some(CLIENT_KEY),
        )
        .await;
    let a_items = a_requests.json()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(a_items.len(), 2);
    assert!(a_items.iter().all(|item| item["model"] == "gpt-4o"));

    let b_requests = client
        .get_json(
            &server.url("/api/dashboard/requests?limit=200"),
            Some(OTHER_KEY),
        )
        .await;
    let b_items = b_requests.json()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(b_items.len(), 1);
    assert_eq!(b_items[0]["model"], "gpt-4o-mini");

    // The model list is derived from the caller's own traffic only.
    let a_models = client
        .get_json(&server.url("/api/dashboard/models"), Some(CLIENT_KEY))
        .await;
    assert_eq!(a_models.json()["models"], serde_json::json!(["gpt-4o"]));
    let b_models = client
        .get_json(&server.url("/api/dashboard/models"), Some(OTHER_KEY))
        .await;
    assert_eq!(
        b_models.json()["models"],
        serde_json::json!(["gpt-4o-mini"])
    );

    // The timeseries carries the same totals, per hour.
    let a_series = client
        .get_json(&server.url("/api/dashboard/timeseries"), Some(CLIENT_KEY))
        .await;
    let points = a_series.json()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let requests: u64 = points
        .iter()
        .map(|p| p["requests"].as_u64().unwrap_or(0))
        .sum();
    assert_eq!(requests, 2, "timeseries must sum to the same traffic");
}

#[tokio::test]
async fn requests_paginate_without_gaps_or_duplicates() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 5,
        completion: 5,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    const TOTAL: usize = 7;
    for i in 0..TOTAL {
        let model = if i % 2 == 0 { "gpt-4o" } else { "gpt-4o-mini" };
        let response = chat(&client, &server, CLIENT_KEY, model, true).await;
        wait_for_terminal(
            &server.db_path,
            &response.request_id().expect("x-request-id"),
            WAIT_TIMEOUT,
        )
        .await;
    }

    // Walk the pages. The cursor is opaque and signed: the test only follows it,
    // and must never build one itself.
    let mut seen: Vec<String> = Vec::new();
    let mut cursor: Option<String> = None;
    let mut ended = false;
    for _ in 0..10 {
        let url = match &cursor {
            Some(cursor) => format!(
                "{}?limit=3&cursor={}",
                server.url("/api/dashboard/requests"),
                urlencode(cursor)
            ),
            None => format!("{}?limit=3", server.url("/api/dashboard/requests")),
        };
        let page = client.get_json(&url, Some(CLIENT_KEY)).await;
        assert_eq!(page.status, StatusCode::OK, "page {url}");
        let items = page.json()["data"].as_array().cloned().unwrap_or_default();
        assert!(items.len() <= 3);
        seen.extend(
            items
                .iter()
                .map(|item| item["request_id"].as_str().unwrap_or_default().to_string()),
        );
        match page.json()["next_cursor"].as_str() {
            Some(next) => cursor = Some(next.to_string()),
            None => {
                ended = true;
                break;
            }
        }
    }
    assert!(ended, "the last page must report no next cursor");

    assert_eq!(seen.len(), TOTAL, "every request must appear exactly once");
    let unique: std::collections::HashSet<&String> = seen.iter().collect();
    assert_eq!(unique.len(), TOTAL, "no request may appear on two pages");

    // The order is the one the query promises: created_at DESC, id DESC.
    let db = open_db(&server.db_path);
    let expected: Vec<String> = {
        let mut stmt = db
            .prepare(
                "SELECT request_id FROM usage_records
                 WHERE consumer_id = 'test-consumer'
                 ORDER BY created_at DESC, id DESC",
            )
            .expect("prepare ordering query");
        stmt.query_map([], |r| r.get::<_, String>(0))
            .expect("query ordering")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect ordering")
    };
    assert_eq!(expected.len(), TOTAL);
    assert_eq!(seen, expected, "the API order must match the ledger order");

    // The cursor is signed, so one a client assembles from the ledger's own
    // columns is not a cursor this server ever issued. It must be refused rather
    // than honoured: an unauthenticated keyset cursor is a row-id probe.
    let last = &seen[TOTAL - 1];
    let row = crate::common::find_row(&db, last).expect("last row");
    let forged = format!("{}|{}", row.created_at, row.id);
    let refused = client
        .get_json(
            &format!(
                "{}?limit=3&cursor={}",
                server.url("/api/dashboard/requests"),
                urlencode(&forged)
            ),
            Some(CLIENT_KEY),
        )
        .await;
    assert_eq!(
        refused.status,
        StatusCode::BAD_REQUEST,
        "a hand-built cursor is not one this server signed"
    );
}

#[tokio::test]
async fn query_parameters_are_clamped_or_rejected() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 5,
        completion: 5,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    for _ in 0..3 {
        chat(&client, &server, CLIENT_KEY, "gpt-4o", true).await;
    }
    wait_for_terminal_count(&server.db_path, 3, WAIT_TIMEOUT).await;

    let requests = server.url("/api/dashboard/requests");

    // limit=0 is clamped up to one row rather than returning nothing.
    let zero = client
        .get_json(&format!("{requests}?limit=0"), Some(CLIENT_KEY))
        .await;
    assert_eq!(zero.status, StatusCode::OK);
    assert_eq!(zero.json()["data"].as_array().map(|a| a.len()), Some(1));

    // An absurd limit is clamped down to the documented maximum, not honoured.
    let huge = client
        .get_json(&format!("{requests}?limit=999999"), Some(CLIENT_KEY))
        .await;
    assert_eq!(huge.status, StatusCode::OK);
    assert!(
        huge.json()["data"]
            .as_array()
            .map(|a| a.len())
            .unwrap_or(usize::MAX)
            <= 200,
        "limit must be clamped to the maximum page size"
    );

    // A cursor that cannot be parsed is a client error, not an empty page.
    let bad_cursor = client
        .get_json(&format!("{requests}?cursor=garbage"), Some(CLIENT_KEY))
        .await;
    assert_eq!(bad_cursor.status, StatusCode::BAD_REQUEST);
    assert!(
        bad_cursor.json()["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("cursor"),
        "the error must say what was wrong: {}",
        bad_cursor.text()
    );

    // range=custom without bounds is a client error.
    let no_bounds = client
        .get_json(&format!("{requests}?range=custom"), Some(CLIENT_KEY))
        .await;
    assert_eq!(no_bounds.status, StatusCode::BAD_REQUEST);

    // A range older than the retention window is rejected rather than silently
    // returning less than was asked for.
    let outside = client
        .get_json(
            &format!("{requests}?range=custom&start=2020-01-01T00:00:00Z&end=2020-01-02T00:00:00Z"),
            Some(CLIENT_KEY),
        )
        .await;
    assert_eq!(outside.status, StatusCode::BAD_REQUEST);
    assert!(
        outside.json()["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("retention")
    );

    // A limit that is not a number is rejected by the extractor.
    let not_a_number = client
        .get_json(&format!("{requests}?limit=abc"), Some(CLIENT_KEY))
        .await;
    assert_eq!(not_a_number.status, StatusCode::BAD_REQUEST);

    // An unrecognised range falls back to a day of data rather than everything
    // or nothing: the recent requests are still there.
    let nonsense = client
        .get_json(&format!("{requests}?range=whenever"), Some(CLIENT_KEY))
        .await;
    assert_eq!(nonsense.status, StatusCode::OK);
    assert_eq!(
        nonsense.json()["data"].as_array().map(|a| a.len()),
        Some(3),
        "an unknown range must fall back to a bounded window that still contains the data"
    );
}

#[tokio::test]
async fn the_model_and_status_filters_narrow_the_result() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 5,
        completion: 5,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    chat(&client, &server, CLIENT_KEY, "gpt-4o", true).await;
    chat(&client, &server, CLIENT_KEY, "gpt-4o", true).await;
    chat(&client, &server, CLIENT_KEY, "gpt-4o-mini", true).await;
    wait_for_terminal_count(&server.db_path, 3, WAIT_TIMEOUT).await;

    let by_model = client
        .get_json(
            &server.url("/api/dashboard/requests?model=gpt-4o&limit=200"),
            Some(CLIENT_KEY),
        )
        .await;
    let items = by_model.json()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(items.len(), 2);
    assert!(items.iter().all(|i| i["model"] == "gpt-4o"));

    let summary = client
        .get_json(
            &server.url("/api/dashboard/summary?model=gpt-4o"),
            Some(CLIENT_KEY),
        )
        .await;
    assert_eq!(summary.json()["total_requests"], 2);

    // `all` (in any case) means no filter at all.
    let all = client
        .get_json(
            &server.url("/api/dashboard/requests?model=ALL&limit=200"),
            Some(CLIENT_KEY),
        )
        .await;
    assert_eq!(all.json()["data"].as_array().map(|a| a.len()), Some(3));

    // Nothing failed, so the failure filter is empty — and an unknown status is
    // treated as no filter rather than as "match nothing".
    let failed = client
        .get_json(
            &server.url("/api/dashboard/requests?status=failed&limit=200"),
            Some(CLIENT_KEY),
        )
        .await;
    assert_eq!(failed.json()["data"].as_array().map(|a| a.len()), Some(0));
    let bogus = client
        .get_json(
            &server.url("/api/dashboard/requests?status=not-a-status&limit=200"),
            Some(CLIENT_KEY),
        )
        .await;
    assert_eq!(bogus.json()["data"].as_array().map(|a| a.len()), Some(3));
}

#[tokio::test]
async fn api_me_is_scoped_to_the_credential_presented() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = two_consumer_server(&upstream).await;
    let client = TestClient::new();

    for (key, consumer, name) in [
        (CLIENT_KEY, "consumer-a", "primary"),
        (OTHER_KEY, "consumer-b", "secondary"),
    ] {
        let me = client.get_json(&server.url("/api/me"), Some(key)).await;
        assert_eq!(me.status, StatusCode::OK);
        assert_eq!(me.json()["consumer_id"], consumer);
        assert_eq!(me.json()["key_name"], name);
    }
}

/// Percent-encode a cursor for the query string.
///
/// ISO timestamps contain `:` and `+`, which are not safe unencoded in a query.
fn urlencode(value: &str) -> String {
    let mut out = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' | b'|' => {
                out.push(byte as char)
            }
            other => out.push_str(&format!("%{other:02X}")),
        }
    }
    out
}

#[tokio::test]
async fn the_requests_endpoint_reports_the_recorded_fields() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 33,
        completion: 11,
        cached: 4,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = chat(&client, &server, CLIENT_KEY, "gpt-4o", true).await;
    let request_id = response.request_id().expect("x-request-id");
    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;

    let page = client
        .get_json(
            &server.url("/api/dashboard/requests?limit=10"),
            Some(CLIENT_KEY),
        )
        .await;
    let item = &page.json()["data"][0];
    assert_eq!(item["request_id"], row.request_id);
    assert_eq!(item["created_at"], row.created_at);
    assert_eq!(item["model"], "gpt-4o");
    assert_eq!(item["endpoint"], "chat_completions");
    assert_eq!(item["streaming"], false);
    assert_eq!(item["http_status"], 200);
    assert_eq!(item["request_status"], "completed");
    assert_eq!(item["usage_status"], "available");
    assert_eq!(item["input_tokens"], 33);
    assert_eq!(item["output_tokens"], 11);
    assert_eq!(item["cached_tokens"], 4);
    assert_eq!(item["error_message"], Value::Null);
    assert!(item["duration_ms"].as_u64().is_some());
}

#[tokio::test]
async fn a_consumer_with_no_traffic_gets_an_empty_but_valid_view() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = two_consumer_server(&upstream).await;
    let client = TestClient::new();

    let summary = client
        .get_json(&server.url("/api/dashboard/summary"), Some(OTHER_KEY))
        .await;
    assert_eq!(summary.status, StatusCode::OK);
    assert_eq!(summary.json()["total_requests"], 0);
    // No division by zero, no invented latency.
    assert_eq!(summary.json()["avg_latency_ms"], Value::Null);
    assert_eq!(summary.json()["success_rate"], 0.0);

    let series = client
        .get_json(&server.url("/api/dashboard/timeseries"), Some(OTHER_KEY))
        .await;
    assert_eq!(series.status, StatusCode::OK);
    assert_eq!(series.json()["data"].as_array().map(|a| a.len()), Some(0));

    let models = client
        .get_json(&server.url("/api/dashboard/models"), Some(OTHER_KEY))
        .await;
    assert_eq!(models.json()["models"].as_array().map(|a| a.len()), Some(0));

    // A short pause so a late write would show up as a failure, not a race.
    tokio::time::sleep(Duration::from_millis(50)).await;
    let requests = client
        .get_json(
            &server.url("/api/dashboard/requests?limit=200"),
            Some(OTHER_KEY),
        )
        .await;
    assert_eq!(requests.json()["data"].as_array().map(|a| a.len()), Some(0));
    assert_eq!(requests.json()["next_cursor"], Value::Null);
}
