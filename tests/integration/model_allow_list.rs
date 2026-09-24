//! The per-key model allow-list (ADR 0012): a key may only call the models its
//! config lists, a refused model never reaches the upstream and never mints a
//! ledger row, and `/v1/models` discovery is filtered to the same list.

use crate::common::{
    Behaviour, CLIENT_KEY, KeySpec, MockUpstream, Spec, TestClient, TestServer, row_count,
    wait_for_terminal,
};
use bytes::Bytes;
use http::{Method, StatusCode};
use serde_json::{Value, json};

const ALLOWED: &[&str] = &["gpt-4o", "gpt-4o-mini"];

fn chat_request(model: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&json!({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}],
        }))
        .unwrap(),
    )
}

/// A restricted key on the default consumer name, so `server.key()` works.
fn restricted_key(models: &[&str]) -> KeySpec {
    KeySpec::new(CLIENT_KEY, "restricted").with_allowed_models(models)
}

async fn start_restricted(models: &[&str]) -> (MockUpstream, TestServer, TestClient) {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 5,
        completion: 5,
        cached: 0,
    })
    .await;
    let server =
        TestServer::start(Spec::new(&upstream).with_keys(vec![restricted_key(models)])).await;
    let client = TestClient::new();
    (upstream, server, client)
}

#[tokio::test]
async fn an_allowed_model_reaches_the_upstream_and_is_metered() {
    let (upstream, server, client) = start_restricted(ALLOWED).await;

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

    // The upstream saw it and a single ledger row exists.
    upstream
        .wait_for_requests(1, std::time::Duration::from_secs(10))
        .await;
    let row = wait_for_terminal(
        &server.db_path,
        &request_id,
        std::time::Duration::from_secs(10),
    )
    .await;
    assert_eq!(row.model, "gpt-4o");
}

#[tokio::test]
async fn a_disallowed_model_is_refused_before_the_upstream_and_is_not_metered() {
    let (upstream, server, client) = start_restricted(&["gpt-4o"]).await;

    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_request("gpt-4o-mini"),
            &[],
        )
        .await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
    let body: Value = response.json();
    assert_eq!(body["error"]["code"], "model_not_found");
    assert_eq!(body["error"]["type"], "invalid_request_error");
    assert_eq!(
        body["error"]["message"],
        "The model 'gpt-4o-mini' does not exist"
    );
    assert!(
        response.request_id().is_some(),
        "a locally-generated error still carries a traceable request id"
    );

    // The refusal happens before the upstream is contacted and before the
    // ledger is written: no traffic, no rows.
    assert_eq!(upstream.request_count(), 0, "nothing may be forwarded");
    assert_eq!(row_count(&server.open_db()), 0);
}

#[tokio::test]
async fn an_empty_allow_list_is_denied_every_model() {
    let (upstream, server, client) = start_restricted(&[]).await;

    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_request("gpt-4o"),
            &[],
        )
        .await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
    assert_eq!(response.json()["error"]["code"], "model_not_found");
    assert_eq!(upstream.request_count(), 0);
    assert_eq!(row_count(&server.open_db()), 0);
}

#[tokio::test]
async fn responses_endpoint_is_gated_the_same_way() {
    let (upstream, server, client) = start_restricted(&["gpt-4o"]).await;

    let response = client
        .call(
            Method::POST,
            &server.url("/v1/responses"),
            Some(server.key()),
            Bytes::from(
                serde_json::to_vec(&json!({
                    "model": "gpt-4o-mini",
                    "input": "hello",
                }))
                .unwrap(),
            ),
            &[],
        )
        .await;

    assert_eq!(response.status, StatusCode::NOT_FOUND);
    assert_eq!(response.json()["error"]["code"], "model_not_found");
    assert_eq!(upstream.request_count(), 0, "nothing may be forwarded");
    assert_eq!(row_count(&server.open_db()), 0);
}

#[tokio::test]
async fn a_missing_or_non_json_model_is_denied() {
    let (upstream, server, client) = start_restricted(&["gpt-4o"]).await;

    // A body with no `model` field at all.
    let no_model = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            Bytes::from_static(br#"{"messages":[]}"#),
            &[],
        )
        .await;
    assert_eq!(no_model.status, StatusCode::NOT_FOUND);
    assert_eq!(no_model.json()["error"]["code"], "model_not_found");
    assert_eq!(upstream.request_count(), 0);
    assert_eq!(row_count(&server.open_db()), 0);

    // A non-JSON body.
    let not_json = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            Bytes::from("not json"),
            &[],
        )
        .await;
    assert_eq!(not_json.status, StatusCode::NOT_FOUND);
    assert_eq!(not_json.json()["error"]["code"], "model_not_found");
    assert_eq!(upstream.request_count(), 0);
    assert_eq!(row_count(&server.open_db()), 0);
}

#[tokio::test]
async fn v1_models_is_filtered_to_the_keys_allow_list() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    // The mock serves mock-model / mock-model-mini; the key allows one of them.
    let server = TestServer::start(
        Spec::new(&upstream).with_keys(vec![restricted_key(&["gpt-4o", "mock-model"])]),
    )
    .await;
    let client = TestClient::new();

    let response = client
        .get_json(&server.url("/v1/models"), Some(server.key()))
        .await;
    assert_eq!(response.status, StatusCode::OK);

    let data = response.json()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    let ids: Vec<String> = data
        .iter()
        .map(|m| m["id"].as_str().unwrap_or_default().to_string())
        .collect();
    assert_eq!(
        ids,
        vec!["mock-model"],
        "only allowed models may appear: {ids:?}"
    );

    // No ledger row: discovery is still unmetered.
    assert_eq!(row_count(&server.open_db()), 0);
}

#[tokio::test]
async fn v1_models_for_an_empty_list_key_returns_no_models() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream).with_keys(vec![restricted_key(&[])])).await;
    let client = TestClient::new();

    let response = client
        .get_json(&server.url("/v1/models"), Some(server.key()))
        .await;
    assert_eq!(response.status, StatusCode::OK);

    let data = response.json()["data"]
        .as_array()
        .cloned()
        .unwrap_or_default();
    assert!(
        data.is_empty(),
        "an empty allow-list must expose no models: {data:?}"
    );
    assert_eq!(row_count(&server.open_db()), 0);
}
