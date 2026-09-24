//! The manager credential: a password that opens a cross-consumer dashboard
//! view (ADR 0008, ADR 0013), and the guardrails around it.
//!
//! The manager is the *only* widening of the consumer-scoped dashboard, and
//! this file pins what that means on the wire:
//!
//! - the password authenticates `/api/me` and the dashboard queries, with a
//!   role of `manager` and a consumer list read from the ledger — a report of
//!   what there is to see, not the definition of what may be seen;
//! - a manager sees every consumer, and `consumers=` narrows the view to the
//!   names it carries, verbatim: an unknown name is an empty view, never a
//!   fall-back to everything;
//! - the password is *not* a proxy credential: presenting it on `/v1/*` is a
//!   403, so the metering path can never mint a row from it;
//! - a regular key's `consumers=` parameter is ignored (identity is
//!   server-derived, ADR 0008).

use crate::common::{
    Behaviour, CLIENT_KEY, KeySpec, ManagerSpec, MockUpstream, Spec, TestClient, TestServer,
    WAIT_TIMEOUT, chat_request, wait_for_terminal_count,
};
use http::{Method, StatusCode};
use serde_json::Value;

/// The manager password configured in the fixture.
const MANAGER_PASSWORD: &str = "sk-manager-password-do-not-guess";
/// A second consumer key, sharing the server with [`CLIENT_KEY`].
const OTHER_KEY: &str = "sk-other-key";

/// A server with two consumers and a manager password over both of them.
async fn manager_server(upstream: &MockUpstream) -> TestServer {
    let spec = Spec::new(upstream)
        .with_keys(vec![
            KeySpec::new(CLIENT_KEY, "primary").with_consumer("consumer-a"),
            KeySpec::new(OTHER_KEY, "secondary").with_consumer("consumer-b"),
        ])
        .with_manager(ManagerSpec::new(MANAGER_PASSWORD));
    TestServer::start(spec).await
}

/// Drive one metered chat request as a given key and wait for it to land.
async fn chat(client: &TestClient, server: &TestServer, key: &str, model: &str) {
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
}

/// The consumer list the ledger itself would offer `/api/me`, read straight
/// from SQLite. Ledger assertions read SQLite, not (only) the API.
fn ledger_consumers(server: &TestServer) -> Vec<String> {
    let db = server.open_db();
    let mut stmt = db
        .prepare("SELECT DISTINCT consumer_id FROM usage_hourly ORDER BY consumer_id ASC")
        .expect("prepare distinct consumer query");
    let rows = stmt
        .query_map([], |row| row.get::<_, String>(0))
        .expect("query consumers");
    let mut out: Vec<String> = rows.map(|row| row.expect("consumer row")).collect();
    out.sort();
    out
}

/// `/api/me` offers the consumers actually present in the ledger, not a list
/// from the config — that is what makes the dashboard's consumer selector work
/// for a manager with no allow-list (the bug ADR 0013 fixed).
#[tokio::test]
async fn manager_me_lists_the_consumers_present_in_the_ledger() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 100,
        completion: 50,
        cached: 20,
    })
    .await;
    let server = manager_server(&upstream).await;
    let client = TestClient::new();

    chat(&client, &server, CLIENT_KEY, "gpt-4o").await;
    chat(&client, &server, CLIENT_KEY, "gpt-4o").await;
    chat(&client, &server, OTHER_KEY, "gpt-4o-mini").await;
    wait_for_terminal_count(&server.db_path, 3, WAIT_TIMEOUT).await;

    let me = client
        .get_json(&server.url("/api/me"), Some(MANAGER_PASSWORD))
        .await;
    assert_eq!(me.status, StatusCode::OK, "manager /api/me failed");
    let body = me.json();
    assert_eq!(body["role"], "manager");
    assert_eq!(body["key_name"], "manager");
    assert_eq!(
        body["consumers"],
        serde_json::json!(ledger_consumers(&server)),
        "the selector list must be the ledger's distinct consumers"
    );

    // A regular key's `/api/me` is unchanged in shape, just with `role=consumer`.
    let consumer_me = client
        .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
        .await;
    assert_eq!(consumer_me.status, StatusCode::OK);
    let body = consumer_me.json();
    assert_eq!(body["role"], "consumer");
    assert_eq!(body["consumer_id"], "consumer-a");
    assert_eq!(
        body["consumers"],
        Value::Array(vec![]),
        "a consumer key carries no consumer list"
    );
}

/// With nothing in the ledger there is nothing to offer: the selector list is
/// empty while the manager's view is still every consumer.
#[tokio::test]
async fn manager_me_returns_an_empty_consumer_list_on_an_empty_ledger() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 100,
        completion: 50,
        cached: 20,
    })
    .await;
    let server = manager_server(&upstream).await;
    let client = TestClient::new();

    let me = client
        .get_json(&server.url("/api/me"), Some(MANAGER_PASSWORD))
        .await;
    assert_eq!(me.json()["role"], "manager");
    assert_eq!(
        me.json()["consumers"],
        Value::Array(vec![]),
        "an empty ledger offers no consumer"
    );
}

#[tokio::test]
async fn manager_sees_every_consumer_and_narrows_by_request() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 100,
        completion: 50,
        cached: 20,
    })
    .await;
    let server = manager_server(&upstream).await;
    let client = TestClient::new();

    // consumer-a makes 2 requests, consumer-b makes 1.
    chat(&client, &server, CLIENT_KEY, "gpt-4o").await;
    chat(&client, &server, CLIENT_KEY, "gpt-4o").await;
    chat(&client, &server, OTHER_KEY, "gpt-4o-mini").await;
    wait_for_terminal_count(&server.db_path, 3, WAIT_TIMEOUT).await;

    // Full view: every consumer.
    let summary = client
        .get_json(
            &server.url("/api/dashboard/summary"),
            Some(MANAGER_PASSWORD),
        )
        .await;
    assert_eq!(summary.status, StatusCode::OK);
    let total_requests = summary.json()["total_requests"].as_u64().unwrap_or(0);
    assert_eq!(
        total_requests,
        3,
        "a manager sees every consumer: {}",
        summary.text()
    );

    // Narrow to consumer-a only via the `consumers` parameter.
    let a_summary = client
        .get_json(
            &server.url("/api/dashboard/summary?consumers=consumer-a"),
            Some(MANAGER_PASSWORD),
        )
        .await;
    assert_eq!(
        a_summary.json()["total_requests"],
        2,
        "consumers= must narrow the view: {}",
        a_summary.text()
    );

    // Narrowing is verbatim: an unknown consumer must NOT fall back to
    // everything — an empty view is the honest answer to a filter nothing
    // matches.
    let unknown = client
        .get_json(
            &server.url("/api/dashboard/summary?consumers=ghost"),
            Some(MANAGER_PASSWORD),
        )
        .await;
    assert_eq!(
        unknown.json()["total_requests"],
        0,
        "a name nothing matches is empty, not everything: {}",
        unknown.text()
    );

    // The request list is scoped the same way, and each row is labelled with its
    // consumer so the cross-consumer table can show them.
    let full_page = client
        .get_json(
            &server.url("/api/dashboard/requests?limit=200"),
            Some(MANAGER_PASSWORD),
        )
        .await;
    let full_items = full_page.json()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(full_items.len(), 3);
    let consumers: std::collections::HashSet<&str> = full_items
        .iter()
        .map(|item| item["consumer_id"].as_str().unwrap_or_default())
        .collect();
    assert!(
        consumers.contains("consumer-a") && consumers.contains("consumer-b"),
        "request rows must carry their consumer_id: {full_items:?}"
    );

    let narrow_page = client
        .get_json(
            &server.url("/api/dashboard/requests?limit=200&consumers=consumer-b"),
            Some(MANAGER_PASSWORD),
        )
        .await;
    let narrow_items = narrow_page.json()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert_eq!(narrow_items.len(), 1);
    assert_eq!(narrow_items[0]["consumer_id"], "consumer-b");

    // Models and timeseries follow the same scope.
    let models = client
        .get_json(&server.url("/api/dashboard/models"), Some(MANAGER_PASSWORD))
        .await;
    assert_eq!(
        models.json()["models"],
        serde_json::json!(["gpt-4o", "gpt-4o-mini"])
    );

    // A consumer key cannot use `consumers=` to widen into another consumer. The
    // parameter is ignored for a key, and the key's own identity still governs.
    let widened = client
        .get_json(
            &server.url("/api/dashboard/summary?consumers=consumer-b"),
            Some(CLIENT_KEY),
        )
        .await;
    assert_eq!(
        widened.json()["total_requests"],
        2,
        "a key's consumers= parameter must be ignored: {}",
        widened.text()
    );
}

#[tokio::test]
async fn manager_password_is_not_a_proxy_credential() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 100,
        completion: 50,
        cached: 20,
    })
    .await;
    let server = manager_server(&upstream).await;
    let client = TestClient::new();

    // The manager password on the inference path is forbidden, not proxied: it
    // must never mint a metered row under an empty consumer_id.
    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(MANAGER_PASSWORD),
            chat_request("gpt-4o"),
            &[],
        )
        .await;
    assert_eq!(
        response.status,
        StatusCode::FORBIDDEN,
        "the manager password must not proxy requests: {}",
        response.text()
    );

    // And it must not have written a ledger row.
    let db = server.open_db();
    let count: i64 = db
        .query_row(
            "SELECT COUNT(*) FROM usage_records WHERE consumer_id = ''",
            [],
            |row| row.get(0),
        )
        .expect("count empty-consumer rows");
    assert_eq!(count, 0, "the manager password must not meter anything");
}
