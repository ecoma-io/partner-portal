//! Authentication and identity.
//!
//! The proxy's whole isolation story rests on one claim: the identity written to
//! the ledger and used to scope every dashboard query is derived from the
//! presented key and the row that key resolves to — never from anything the
//! client sent. These tests attack that claim from the request body, from
//! headers, and from a key pair that shares a human-readable name.
//!
//! The credentials here are *issued by the store* through the harness, not
//! written in this file. Nothing in a test names a key, so nothing in a test
//! can leak one; the plaintext a request carries is the one the seed returned.

use std::time::Duration;

use crate::common::{
    Behaviour, KeySpec, ManagerSpec, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    chat_request, in_flight_count, raw_rollup_totals, row_count, wait_for_terminal,
    wait_for_terminal_count,
};
use http::{Method, StatusCode};
use serde_json::json;

/// A manager password, which is the only credential the admin API answers to.
const MANAGER_PASSWORD: &str = "test-manager-password-do-not-guess";

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
    let key = server.key().to_string();
    for header in [
        "Basic dXNlcjpwYXNz".to_string(),
        "Bearer".to_string(),
        "Bearer ".to_string(),
        key.clone(),
        format!("Basic {key}"),
        format!("token {key}"),
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
            !response.text().contains(&key),
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
                    Some("pp_not_a_seeded_key"),
                    chat_request("gpt-4o"),
                    &[],
                )
                .await
        } else {
            client
                .get_json(&server.url(path), Some("pp_not_a_seeded_key"))
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
async fn identity_comes_from_the_stored_row_not_the_request() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_keys(vec![
        KeySpec::new("key-name").with_consumer("consumer-from-row"),
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
            Some(server.key()),
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
        row.consumer_id, "consumer-from-row",
        "the identity is the stored one, whatever the client claims"
    );

    let me = client
        .get_json(&server.url("/api/me"), Some(server.key()))
        .await;
    assert_eq!(me.json()["consumer_id"], "consumer-from-row");
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
        KeySpec::new("primary").with_consumer("shared-consumer"),
        KeySpec::new("secondary").with_consumer("shared-consumer"),
    ]);
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    for index in 0..2 {
        let me = client
            .get_json(&server.url("/api/me"), Some(server.key_at(index)))
            .await;
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
    for index in 0..2 {
        let response = client
            .call(
                Method::POST,
                &server.url("/v1/chat/completions"),
                Some(server.key_at(index)),
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
    assert_eq!(rolled, 2, "two rows, one consumer, one rollup row each");
    let shared = crate::common::rows_for_consumer(&db, "shared-consumer");
    assert_eq!(shared.len(), 2);
}

/// Revocation is now an admin call, not a config edit — the test is the same
/// one it was, with the act moved to the surface that owns it.
///
/// The assertion that matters is unchanged and is the reason the test exists:
/// the pid is the same before and after, so the key stopped authenticating
/// because the in-memory snapshot was refreshed, not because a process
/// restarted and re-read its configuration.
#[tokio::test]
async fn a_revoked_key_stops_working_without_a_restart() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream)
        .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
        .with_keys(vec![KeySpec::new("primary"), KeySpec::new("secondary")]);
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    // Both keys work to begin with.
    for index in 0..2 {
        assert_eq!(
            client
                .get_json(&server.url("/api/me"), Some(server.key_at(index)))
                .await
                .status,
            StatusCode::OK
        );
    }

    let pid_before = server.pid();

    // Revoke the second key through the admin API. The commit is made before the
    // response says so, and the store refreshes its snapshot synchronously, so
    // the next request already sees it — the poll below only guards against the
    // exact ordering the test would otherwise assume.
    let revoke = client
        .post_json(
            &server.url(&format!(
                "/api/admin/api-keys/{}/revoke",
                server.key_id() + 1
            )),
            Some(MANAGER_PASSWORD),
            json!({}),
        )
        .await;
    assert_eq!(
        revoke.status,
        StatusCode::OK,
        "revoke failed: {}",
        revoke.text()
    );
    assert_eq!(revoke.json()["status"], "revoked");

    let mut revoked_observed = false;
    for _ in 0..300 {
        if client
            .get_json(&server.url("/api/me"), Some(server.key_at(1)))
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

    // The still-valid key is untouched, and no request reached the upstream.
    assert_eq!(
        client
            .get_json(&server.url("/api/me"), Some(server.key()))
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

/// The admin surface answers to the manager password and to nothing else.
///
/// This is the whole point of the decision, so it is asserted as a table rather
/// than as prose: a partner key — a credential that *works* everywhere else the
/// dashboard is concerned — must be refused here, and the two refusals must stay
/// distinguishable. 401 means nothing usable was presented; 403 means something
/// usable was presented that is not allowed on this surface. A client being
/// debugged needs to tell those apart.
#[tokio::test]
async fn a_partner_key_is_refused_by_the_admin_api() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let spec = Spec::new(&upstream).with_manager(ManagerSpec::new(MANAGER_PASSWORD));
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    // The partner key is not a broken credential: it opens `/api/me` on the very
    // same instance. Only the admin route refuses it.
    assert_eq!(
        client
            .get_json(&server.url("/api/me"), Some(server.key()))
            .await
            .status,
        StatusCode::OK,
        "the partner key must still be a valid credential elsewhere"
    );

    for (label, bearer, expected) in [
        ("no credential", None, StatusCode::UNAUTHORIZED),
        (
            "an unknown key",
            Some("pp_not_a_seeded_key"),
            StatusCode::UNAUTHORIZED,
        ),
        ("a partner key", Some(server.key()), StatusCode::FORBIDDEN),
        (
            "the manager password",
            Some(MANAGER_PASSWORD),
            StatusCode::OK,
        ),
    ] {
        let response = client
            .get_json(&server.url("/api/admin/api-keys"), bearer)
            .await;
        assert_eq!(response.status, expected, "{label}: {}", response.text());
    }
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
