//! Authentication and identity.
//!
//! The proxy's whole isolation story rests on one claim: the identity written to
//! the ledger and used to scope every dashboard query is derived from the
//! presented key and the server's own configuration — never from anything the
//! client sent. These tests attack that claim from the request body, from
//! headers, and from a key pair that shares a human-readable name.

use std::time::Duration;

use crate::common::{
    Behaviour, CLIENT_KEY, KeySpec, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    chat_request, in_flight_count, raw_rollup_totals, row_count, wait_for_terminal,
    wait_for_terminal_count,
};
use http::{Method, StatusCode};
use serde_json::json;

/// A second credential, distinct from the default one.
const OTHER_KEY: &str = "sk-local-other-key";

#[tokio::test]
async fn a_request_without_a_credential_is_rejected_before_anything_else() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            None,
            chat_request("gpt-4o"),
            &[],
        )
        .await;

    assert_eq!(response.status, StatusCode::UNAUTHORIZED);
    assert_eq!(response.json()["error"]["code"], "invalid_api_key");
    assert_eq!(
        response.header("cache-control").as_deref(),
        Some("no-store"),
        "a rejected credential must never be cached"
    );
    assert_eq!(
        upstream.request_count(),
        0,
        "an unauthenticated request must not be forwarded"
    );
    assert_eq!(
        row_count(&server.open_db()),
        0,
        "an unauthenticated request must not be metered"
    );
}

#[tokio::test]
async fn a_malformed_credential_is_rejected_not_guessed() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    // Every one of these is a header a client could plausibly send. None of them
    // is a bearer token, and none may be interpreted as one — including a bare
    // credential with no scheme, and a valid credential behind the wrong scheme.
    for header in [
        "Basic dXNlcjpwYXNz".to_string(),
        "Bearer".to_string(),
        "Bearer ".to_string(),
        CLIENT_KEY.to_string(),
        format!("Basic {CLIENT_KEY}"),
        "token sk-local-test-key".to_string(),
    ] {
        let response = client
            .call(
                Method::POST,
                &server.url("/v1/chat/completions"),
                None,
                chat_request("gpt-4o"),
                &[("authorization", header.as_str())],
            )
            .await;

        assert_eq!(
            response.status,
            StatusCode::UNAUTHORIZED,
            "{header:?} must be rejected"
        );
        // The credential is never echoed back.
        assert!(
            !response.text().contains(CLIENT_KEY),
            "the response must not contain the credential"
        );
    }

    assert_eq!(upstream.request_count(), 0);
    assert_eq!(row_count(&server.open_db()), 0);
}

#[tokio::test]
async fn an_unknown_key_is_rejected_and_the_ledger_stays_clean() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    for path in ["/v1/chat/completions", "/v1/models", "/api/me"] {
        let response = if path == "/v1/chat/completions" {
            client
                .call(
                    Method::POST,
                    &server.url(path),
                    Some("sk-not-a-configured-key"),
                    chat_request("gpt-4o"),
                    &[],
                )
                .await
        } else {
            client
                .get_json(&server.url(path), Some("sk-not-a-configured-key"))
                .await
        };

        assert_eq!(response.status, StatusCode::UNAUTHORIZED, "path {path}");
    }

    assert_eq!(upstream.request_count(), 0);
    assert_eq!(row_count(&server.open_db()), 0);
}

#[tokio::test]
async fn a_valid_key_is_accepted_and_identified() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let me = client
        .get_json(&server.url("/api/me"), Some(server.key()))
        .await;
    assert_eq!(me.status, StatusCode::OK);
    assert_eq!(me.json()["consumer_id"], server.consumer());
    assert_eq!(me.json()["key_name"], server.consumer());

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
    assert_eq!(row.consumer_id, server.consumer());
}

#[tokio::test]
async fn identity_comes_from_the_configuration_not_the_request() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_keys(vec![
        KeySpec::new(CLIENT_KEY, "key-name").with_consumer("consumer-from-config"),
    ]);
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    // A client that asserts an identity in the body and in a header must still be
    // metered under its configured consumer. (The query-string vector is covered
    // in the proxy suite, next to the routing it depends on.)
    let mut body = TestClient::chat_body("gpt-4o", false);
    body["consumer_id"] = json!("consumer-from-the-client");
    body["user"] = json!("consumer-from-the-client");

    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(CLIENT_KEY),
            serde_json::to_vec(&body).map(Into::into).unwrap(),
            &[
                ("x-consumer-id", "consumer-from-the-header"),
                ("x-forwarded-user", "consumer-from-the-header"),
            ],
        )
        .await;
    assert_eq!(response.status, StatusCode::OK);

    let request_id = response.request_id().expect("x-request-id header");
    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(
        row.consumer_id, "consumer-from-config",
        "the identity is the configured one, whatever the client claims"
    );

    let me = client
        .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
        .await;
    assert_eq!(me.json()["consumer_id"], "consumer-from-config");
    assert_eq!(me.json()["key_name"], "key-name");
}

#[tokio::test]
async fn two_keys_configured_for_one_consumer_share_it_and_a_distinct_one_does_not() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_keys(vec![
        KeySpec::new(CLIENT_KEY, "primary").with_consumer("shared-consumer"),
        KeySpec::new(OTHER_KEY, "secondary").with_consumer("shared-consumer"),
    ]);
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    for key in [CLIENT_KEY, OTHER_KEY] {
        let me = client.get_json(&server.url("/api/me"), Some(key)).await;
        assert_eq!(me.status, StatusCode::OK);
        assert_eq!(
            me.json()["consumer_id"],
            "shared-consumer",
            "both keys are configured for the same consumer"
        );
        assert_ne!(
            me.json()["key_name"],
            "shared-consumer",
            "the key name is reported separately from the consumer it maps to"
        );
    }

    // Both keys' traffic accrues to the one consumer they share.
    for key in [CLIENT_KEY, OTHER_KEY] {
        let response = client
            .call(
                Method::POST,
                &server.url("/v1/chat/completions"),
                Some(key),
                chat_request("gpt-4o"),
                &[],
            )
            .await;
        assert_eq!(response.status, StatusCode::OK);
    }

    let db = server.open_db();
    wait_for_terminal_count(&server.db_path, 2, WAIT_TIMEOUT).await;
    let (terminal, rolled) = raw_rollup_totals(&db);
    assert_eq!(terminal, 2);
    assert_eq!(rolled, 2, "two keys, one consumer, one rollup row");
    let shared = crate::common::rows_for_consumer(&db, "shared-consumer");
    assert_eq!(shared.len(), 2);
}

#[tokio::test]
async fn a_revoked_key_stops_working_without_a_restart() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_keys(vec![
        KeySpec::new(CLIENT_KEY, "primary"),
        KeySpec::new(OTHER_KEY, "secondary"),
    ]);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    // Both keys work to begin with.
    for key in [CLIENT_KEY, OTHER_KEY] {
        assert_eq!(
            client
                .get_json(&server.url("/api/me"), Some(key))
                .await
                .status,
            StatusCode::OK
        );
    }

    let pid_before = server.pid();

    // Revoke the second key: same file, one entry fewer. The watcher polls on its
    // own schedule, so wait for the effect rather than assuming a delay.
    let revoked = spec
        .clone()
        .with_keys(vec![KeySpec::new(CLIENT_KEY, "primary")]);
    server.write_config(&revoked);

    let mut revoked_observed = false;
    for _ in 0..300 {
        if client
            .get_json(&server.url("/api/me"), Some(OTHER_KEY))
            .await
            .status
            == StatusCode::UNAUTHORIZED
        {
            revoked_observed = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        revoked_observed,
        "a revoked key must stop authenticating within a few seconds"
    );

    // The still-configured key is untouched, and no request reached the upstream.
    assert_eq!(
        client
            .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
            .await
            .status,
        StatusCode::OK
    );
    assert_eq!(upstream.request_count(), 0);
    assert_eq!(in_flight_count(&server.open_db()), 0);
    assert_eq!(
        server.pid(),
        pid_before,
        "revocation must not need a restart"
    );
}

#[tokio::test]
async fn a_dashboard_query_requires_a_credential() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    for path in [
        "/api/me",
        "/api/dashboard/summary",
        "/api/dashboard/timeseries",
        "/api/dashboard/requests",
        "/api/dashboard/models",
    ] {
        let anonymous = client.get_json(&server.url(path), None).await;
        assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED, "path {path}");

        let authenticated = client.get_json(&server.url(path), Some(server.key())).await;
        assert_eq!(authenticated.status, StatusCode::OK, "path {path}");
    }
}
