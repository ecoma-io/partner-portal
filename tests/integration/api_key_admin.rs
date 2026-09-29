//! The manager-only api-key lifecycle.
//!
//! `POST/GET/PATCH/rotate/revoke` on `/api/admin/api-keys` is the only surface
//! that can create a credential or end one, so this file is where the
//! lifecycle's promises are pinned:
//!
//! - a key exists once, in SQLite, and the plaintext is returned exactly twice
//!   in its life — by the create that issued it and by the rotate that replaced
//!   it — and never again by any endpoint;
//! - an identity is what the *row* says, not what the request asked for;
//! - a key is only a credential: model access and its immutable-at-accept price
//!   live on the partner account, so key issuance never smuggles an allow-list
//!   into `api_keys` (ADR 0015);
//! - revocation and expiry both stop authentication on the next request against
//!   this instance, without a restart;
//! - a revoked key is an audit record, not a deleted row, so a rotation leaves a
//!   trail;
//! - and the plaintext never lands in the database file, the log, the version
//!   endpoint, or a refusal body.
//!
//! The refusal side — a partner key is 403 here, no credential is 401 — lives
//! in `auth::a_partner_key_is_refused_by_the_admin_api`, next to the other
//! statements about what a credential may do.
//!
//! No test in this file writes a key down. Every plaintext here is the one the
//! store generated and handed back, which is why there is nothing in the source
//! to leak.

use crate::common::{
    Behaviour, HttpResponse, ManagerSpec, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    chat_request, row_count, wait_for_terminal, wait_for_terminal_count,
};
use bytes::Bytes;
use http::{Method, StatusCode};
use serde_json::{Value, json};

use partner_portal::apikeys::KEY_PREFIX_LEN;

/// The manager password. Nothing else may call this surface.
const MANAGER_PASSWORD: &str = "test-manager-password-for-key-admin";

/// A spec with **no** seeded key: on these tests the admin API is the only
/// source of credentials, so a key that authenticates can only be one this API
/// issued.
fn admin_spec(upstream: &MockUpstream) -> Spec {
    Spec::new(upstream)
        .with_keys(vec![])
        .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
}

async fn admin_server(upstream: &MockUpstream) -> TestServer {
    TestServer::start(admin_spec(upstream)).await
}

/// Issue a key and assert it was created rather than merely reported.
async fn issue(client: &TestClient, server: &TestServer, body: Value) -> HttpResponse {
    let response = client
        .post_json(
            &server.url("/api/admin/api-keys"),
            Some(MANAGER_PASSWORD),
            body,
        )
        .await;
    assert_eq!(
        response.status,
        StatusCode::CREATED,
        "issuing a key failed: {}",
        response.text()
    );
    response
}

/// The one plaintext a create or a rotate hands back.
fn secret_of(response: &HttpResponse) -> String {
    response.json()["key_secret"]
        .as_str()
        .unwrap_or_else(|| {
            panic!(
                "a create/rotate response must carry the plaintext once: {}",
                response.text()
            )
        })
        .to_string()
}

async fn me(client: &TestClient, server: &TestServer, key: &str) -> HttpResponse {
    client.get_json(&server.url("/api/me"), Some(key)).await
}

async fn chat(client: &TestClient, server: &TestServer, key: &str, model: &str) -> HttpResponse {
    client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(key),
            chat_request(model),
            &[],
        )
        .await
}

/// Open the commercial account a key authenticates for.
///
/// Key issuance deliberately does not do this implicitly. A credential can be
/// provisioned before finance has supplied an email, mode and prices, but then
/// it can authenticate and call no model. Tests that need inference put the
/// account in the only surface that owns these facts rather than reintroducing
/// an `allowed_models` field to the key endpoint.
async fn create_partner(
    client: &TestClient,
    server: &TestServer,
    consumer_id: &str,
    models: &[&str],
) -> HttpResponse {
    let models = models
        .iter()
        .map(|model| {
            json!({
                "model": model,
                "input_per_million": "0.095",
                "cached_input_per_million": "0.002375",
                "output_per_million": "0.475",
            })
        })
        .collect::<Vec<_>>();
    let response = client
        .post_json(
            &server.url("/api/admin/partners"),
            Some(MANAGER_PASSWORD),
            json!({
                "consumer_id": consumer_id,
                "name": format!("{consumer_id} partner"),
                "billing_mode": "invoice",
                "models": models,
            }),
        )
        .await;
    assert_eq!(
        response.status,
        StatusCode::CREATED,
        "creating partner {consumer_id} failed: {}",
        response.text()
    );
    response
}

/// Replace a partner's authoritative model/pricing map.
async fn replace_models(
    client: &TestClient,
    server: &TestServer,
    consumer_id: &str,
    models: &[&str],
) -> HttpResponse {
    let models = models
        .iter()
        .map(|model| {
            json!({
                "model": model,
                "input_per_million": "0.095",
                "cached_input_per_million": "0.002375",
                "output_per_million": "0.475",
            })
        })
        .collect::<Vec<_>>();
    client
        .call(
            Method::PUT,
            &server.url(&format!("/api/admin/partners/{consumer_id}/models")),
            Some(MANAGER_PASSWORD),
            Bytes::from(serde_json::to_vec(&json!({ "models": models })).expect("serialize")),
            &[],
        )
        .await
}

/// Send a request with a method `TestClient` has no helper for.
async fn send_json(client: &TestClient, method: Method, url: &str, body: Value) -> HttpResponse {
    client
        .call(
            method,
            url,
            Some(MANAGER_PASSWORD),
            serde_json::to_vec(&body).expect("serialize body").into(),
            &[],
        )
        .await
}

/// Byte-search the database's files. WAL mode means the newest rows may still be
/// in the `-wal`, so all three files are searched rather than the main one only.
fn db_files_contain(server: &TestServer, needle: &str) -> bool {
    let base = server.db_path.to_string_lossy().to_string();
    ["", "-wal", "-shm"].iter().any(|suffix| {
        let path = format!("{base}{suffix}");
        match std::fs::read(&path) {
            Ok(bytes) => contains(&bytes, needle.as_bytes()),
            Err(_) => false,
        }
    })
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    !needle.is_empty() && haystack.windows(needle.len()).any(|w| w == needle)
}

// ---------------------------------------------------------------------------
// Issue, identify, list
// ---------------------------------------------------------------------------

/// The plaintext is returned once, and the identity it carries is the row's.
#[tokio::test]
async fn a_created_key_authenticates_for_the_identity_it_was_issued_to() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let server = admin_server(&upstream).await;
    let client = TestClient::new();

    // Nothing is seeded, so without this call the instance authenticates nobody.
    assert_eq!(
        me(&client, &server, "pp_not_a_key").await.status,
        StatusCode::UNAUTHORIZED
    );

    create_partner(&client, &server, "acme", &["gpt-4o"]).await;
    let created = issue(
        &client,
        &server,
        json!({
            "name": "acme-partner",
            "consumer_id": "acme",
        }),
    )
    .await;
    let secret = secret_of(&created);

    let body = created.json();
    assert_eq!(body["name"], "acme-partner");
    assert_eq!(body["consumer_id"], "acme");
    assert_eq!(body["status"], "active");
    assert_eq!(body["revoked_at"], Value::Null);
    assert_eq!(body["expires_at"], Value::Null);
    assert_eq!(
        body["key_prefix"],
        secret[..KEY_PREFIX_LEN],
        "the prefix is the operator-visible identification of the key"
    );
    assert_eq!(
        created.header("cache-control").as_deref(),
        Some("no-store"),
        "a response carrying a credential must never be cached"
    );

    // The credential works, and it is the consumer the row names — not the
    // caller, who is the manager here.
    let me_response = me(&client, &server, &secret).await;
    assert_eq!(me_response.status, StatusCode::OK, "{}", me_response.text());
    assert_eq!(me_response.json()["consumer_id"], "acme");
    assert_eq!(me_response.json()["key_name"], "acme-partner");

    // And that identity is what the ledger records.
    let chat_response = chat(&client, &server, &secret, "gpt-4o").await;
    assert_eq!(
        chat_response.status,
        StatusCode::OK,
        "{}",
        chat_response.text()
    );
    let request_id = chat_response.request_id().expect("x-request-id");
    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(row.consumer_id, "acme");
}

/// Every read path returns the row without the secret.
///
/// The point of asserting on the whole body rather than on a field is that a
/// field-by-field check passes for a response that leaks the secret somewhere
/// nobody thought to look.
#[tokio::test]
async fn no_read_path_returns_a_stored_secret() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = admin_server(&upstream).await;
    let client = TestClient::new();

    create_partner(&client, &server, "a", &["gpt-4o"]).await;
    create_partner(&client, &server, "b", &["gpt-4o-mini"]).await;
    let first = issue(
        &client,
        &server,
        json!({"name": "first", "consumer_id": "a"}),
    )
    .await;
    let second = issue(
        &client,
        &server,
        json!({"name": "second", "consumer_id": "b"}),
    )
    .await;
    let secrets = [secret_of(&first), secret_of(&second)];
    let first_id = first.json()["id"].as_i64().expect("id");
    let second_id = second.json()["id"].as_i64().expect("id");

    let list = client
        .get_json(&server.url("/api/admin/api-keys"), Some(MANAGER_PASSWORD))
        .await;
    assert_eq!(list.status, StatusCode::OK, "{}", list.text());
    let listed = list.json();
    let items = listed.as_array().expect("a list of keys");
    assert_eq!(items.len(), 2, "{}", list.text());
    // Newest first, so an operator sees what they just issued.
    assert_eq!(items[0]["id"], second_id);
    assert_eq!(items[1]["id"], first_id);

    let single = client
        .get_json(
            &server.url(&format!("/api/admin/api-keys/{first_id}")),
            Some(MANAGER_PASSWORD),
        )
        .await;
    assert_eq!(single.status, StatusCode::OK, "{}", single.text());

    for (label, response) in [("the listing", &list), ("a single key", &single)] {
        let text = response.text();
        for secret in &secrets {
            assert!(
                !text.contains(secret.as_str()),
                "{label} must not contain a plaintext key: {text}"
            );
        }
        assert!(!text.contains("key_secret"), "{label}: {text}");
        assert!(!text.contains("key_hash"), "{label}: {text}");
    }

    // The prefixes are shown, so an operator can tell two keys apart in a list
    // without either secret being recoverable from them.
    let list_text = list.text();
    for secret in &secrets {
        assert!(
            list_text.contains(&secret[..KEY_PREFIX_LEN]),
            "every key's prefix must be listed: {list_text}"
        );
    }

    // An unknown id is a 404 rather than an empty 200, and an unknown filter is
    // refused rather than silently ignored — a filter that quietly does nothing
    // shows an operator more than they asked for.
    let missing = client
        .get_json(
            &server.url("/api/admin/api-keys/99999"),
            Some(MANAGER_PASSWORD),
        )
        .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);

    let bad_filter = client
        .get_json(
            &server.url("/api/admin/api-keys?status=everything"),
            Some(MANAGER_PASSWORD),
        )
        .await;
    assert_eq!(
        bad_filter.status,
        StatusCode::BAD_REQUEST,
        "{}",
        bad_filter.text()
    );
}

// ---------------------------------------------------------------------------
// Editing
// ---------------------------------------------------------------------------

/// Key metadata and the partner's model map are separate writes, so changing
/// capability never re-issues a credential.
#[tokio::test]
async fn a_key_can_be_renamed_and_its_partner_models_changed_without_rotating_it() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let server = admin_server(&upstream).await;
    let client = TestClient::new();

    // Issued before the account's model map exists: it can authenticate and
    // look at its own view, but it cannot call anything. The API key carries no
    // model list that could make this behave differently.
    create_partner(&client, &server, "acme", &[]).await;
    let created = issue(
        &client,
        &server,
        json!({"name": "unrestricted", "consumer_id": "acme"}),
    )
    .await;
    let secret = secret_of(&created);
    let id = created.json()["id"].as_i64().expect("id");

    assert_eq!(me(&client, &server, &secret).await.status, StatusCode::OK);
    let refused = chat(&client, &server, &secret, "gpt-4o").await;
    assert_eq!(
        refused.status,
        StatusCode::NOT_FOUND,
        "an empty allow-list denies every model: {}",
        refused.text()
    );
    assert_eq!(upstream.request_count(), 0);

    let patched = send_json(
        &client,
        Method::PATCH,
        &server.url(&format!("/api/admin/api-keys/{id}")),
        json!({"name": "renamed"}),
    )
    .await;
    assert_eq!(patched.status, StatusCode::OK, "{}", patched.text());
    assert_eq!(patched.json()["name"], "renamed");
    assert!(
        patched.json().get("allowed_models").is_none(),
        "a key response must not pretend credentials own capability: {}",
        patched.text()
    );
    assert!(
        patched.json().get("key_secret").is_none(),
        "an update must never re-issue the secret: {}",
        patched.text()
    );

    let models = replace_models(&client, &server, "acme", &["gpt-4o"]).await;
    assert_eq!(models.status, StatusCode::OK, "{}", models.text());
    assert_eq!(
        models.json(),
        json!([{
            "model": "gpt-4o",
            "input_per_million": "0.095",
            "cached_input_per_million": "0.002375",
            "output_per_million": "0.475",
        }])
    );

    // The same plaintext now passes the partner model map, which proves the
    // commercial edit took effect *and* the credential was not replaced.
    let allowed = chat(&client, &server, &secret, "gpt-4o").await;
    assert_eq!(allowed.status, StatusCode::OK, "{}", allowed.text());

    // The rename is visible under the same credential.
    let after = me(&client, &server, &secret).await;
    assert_eq!(after.json()["key_name"], "renamed");

    // A rejected edit leaves the row alone rather than half-applied.
    let blank = send_json(
        &client,
        Method::PATCH,
        &server.url(&format!("/api/admin/api-keys/{id}")),
        json!({"name": "   "}),
    )
    .await;
    assert_eq!(blank.status, StatusCode::BAD_REQUEST, "{}", blank.text());
    assert_eq!(
        me(&client, &server, &secret).await.json()["key_name"],
        "renamed",
        "a refused edit must not have been applied"
    );

    wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;
    assert_eq!(row_count(&server.open_db()), 1);
}

// ---------------------------------------------------------------------------
// Ending a key's life
// ---------------------------------------------------------------------------

/// A revoked key stops authenticating immediately, and is an audit record.
#[tokio::test]
async fn a_revoked_key_is_refused_and_cannot_be_brought_back() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let server = admin_server(&upstream).await;
    let client = TestClient::new();

    create_partner(&client, &server, "acme", &["gpt-4o"]).await;
    let created = issue(
        &client,
        &server,
        json!({"name": "short-lived", "consumer_id": "acme"}),
    )
    .await;
    let secret = secret_of(&created);
    let id = created.json()["id"].as_i64().expect("id");
    assert_eq!(me(&client, &server, &secret).await.status, StatusCode::OK);

    let revoked = client
        .post_json(
            &server.url(&format!("/api/admin/api-keys/{id}/revoke")),
            Some(MANAGER_PASSWORD),
            json!({}),
        )
        .await;
    assert_eq!(revoked.status, StatusCode::OK, "{}", revoked.text());
    assert_eq!(revoked.json()["status"], "revoked");
    assert!(
        revoked.json()["revoked_at"].is_string(),
        "a revocation must be timestamped: {}",
        revoked.text()
    );
    assert!(
        !revoked.text().contains(&secret),
        "a revoke response must not echo the secret"
    );

    // It stops authenticating on the very next request — no restart, no poll.
    let refused = me(&client, &server, &secret).await;
    assert_eq!(refused.status, StatusCode::UNAUTHORIZED);

    // Revoking again is idempotent: the caller's intent is already satisfied.
    let again = client
        .post_json(
            &server.url(&format!("/api/admin/api-keys/{id}/revoke")),
            Some(MANAGER_PASSWORD),
            json!({}),
        )
        .await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.text());
    assert_eq!(again.json()["status"], "revoked");

    // It stays in the listing as revoked — the row is the record that it existed
    // and was withdrawn, which is the whole reason revocation is not a delete.
    let page = client
        .get_json(
            &server.url("/api/admin/api-keys?status=revoked"),
            Some(MANAGER_PASSWORD),
        )
        .await;
    let revoked_items = page.json();
    let revoked_items = revoked_items.as_array().expect("a list");
    assert_eq!(revoked_items.len(), 1, "{}", page.text());
    assert_eq!(revoked_items[0]["id"], id);
    assert_eq!(revoked_items[0]["name"], "short-lived");
    assert!(
        revoked_items[0].get("key_prefix").is_some(),
        "a revoked key keeps its prefix for the audit trail"
    );

    // A revoked key is terminal: it cannot be renamed back into service, and it
    // cannot be rotated into a live successor.
    let patched = send_json(
        &client,
        Method::PATCH,
        &server.url(&format!("/api/admin/api-keys/{id}")),
        json!({"name": "alive-again"}),
    )
    .await;
    assert_eq!(
        patched.status,
        StatusCode::CONFLICT,
        "a revoked key must not be editable: {}",
        patched.text()
    );
    assert_eq!(patched.json()["error"]["code"], "key_not_active");

    let rotated = client
        .post_json(
            &server.url(&format!("/api/admin/api-keys/{id}/rotate")),
            Some(MANAGER_PASSWORD),
            json!({}),
        )
        .await;
    assert_eq!(rotated.status, StatusCode::CONFLICT, "{}", rotated.text());
    assert_eq!(rotated.json()["error"]["code"], "key_not_active");
}

/// Rotation mints a successor for the same identity and revokes the predecessor.
#[tokio::test]
async fn a_rotation_replaces_the_secret_for_the_same_consumer() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let server = admin_server(&upstream).await;
    let client = TestClient::new();

    create_partner(&client, &server, "acme", &["gpt-4o"]).await;
    let created = issue(
        &client,
        &server,
        json!({
            "name": "rolling",
            "consumer_id": "acme",
            "expires_at": "2999-01-01T00:00:00Z",
        }),
    )
    .await;
    let old = secret_of(&created);
    let old_id = created.json()["id"].as_i64().expect("id");
    assert_eq!(me(&client, &server, &old).await.status, StatusCode::OK);

    let rotated = client
        .post_json(
            &server.url(&format!("/api/admin/api-keys/{old_id}/rotate")),
            Some(MANAGER_PASSWORD),
            json!({}),
        )
        .await;
    assert_eq!(rotated.status, StatusCode::CREATED, "{}", rotated.text());
    let new = secret_of(&rotated);
    let new_id = rotated.json()["id"].as_i64().expect("id");

    assert_ne!(new, old, "a rotation must issue a different secret");
    assert_ne!(
        new_id, old_id,
        "the successor is a new row, so the predecessor stays as the record of \
         what was issued before"
    );
    // The successor inherits the identity, name and expiry. Its partner's
    // model map stays where it was: a rotation changes the secret and nothing
    // else.
    assert_eq!(rotated.json()["name"], "rolling");
    assert_eq!(rotated.json()["consumer_id"], "acme");
    assert!(rotated.json().get("allowed_models").is_none());
    assert_eq!(
        rotated.json()["expires_at"],
        "2999-01-01T00:00:00.000000000Z"
    );
    assert_eq!(rotated.json()["status"], "active");

    // The old secret is refused, and refused as an *unknown* credential rather
    // than as a distinct "revoked" answer: one refusal for both means the
    // response is not an oracle for which keys once existed.
    let refused = me(&client, &server, &old).await;
    assert_eq!(refused.status, StatusCode::UNAUTHORIZED);
    assert_eq!(refused.json()["error"]["code"], "invalid_api_key");

    // The new one works, under the same consumer.
    let survivor = me(&client, &server, &new).await;
    assert_eq!(survivor.status, StatusCode::OK, "{}", survivor.text());
    assert_eq!(survivor.json()["consumer_id"], "acme");
    assert_eq!(survivor.json()["key_name"], "rolling");

    // And the predecessor is still in the listing, revoked, with a timestamp.
    let page = client
        .get_json(&server.url("/api/admin/api-keys"), Some(MANAGER_PASSWORD))
        .await;
    let items = page.json();
    let items = items.as_array().expect("a list");
    let predecessor = items
        .iter()
        .find(|item| item["id"] == old_id)
        .unwrap_or_else(|| panic!("the predecessor must still be listed: {}", page.text()));
    assert_eq!(predecessor["status"], "revoked");
    assert!(predecessor["revoked_at"].is_string());
}

/// A key past its expiry never authenticates, and expiry is a filter rather than
/// a status.
#[tokio::test]
async fn an_expired_key_never_authenticates() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = admin_server(&upstream).await;
    let client = TestClient::new();

    create_partner(&client, &server, "acme", &["gpt-4o"]).await;
    let past = issue(
        &client,
        &server,
        json!({
            "name": "stale",
            "consumer_id": "acme",
            "expires_at": "2020-01-01T00:00:00Z",
        }),
    )
    .await;
    let stale = secret_of(&past);

    // A key the table has already declared stale must never authenticate, even
    // though nothing has revoked it.
    let refused = me(&client, &server, &stale).await;
    assert_eq!(refused.status, StatusCode::UNAUTHORIZED);
    assert_eq!(refused.json()["error"]["code"], "invalid_api_key");
    assert!(
        !refused.text().contains(&stale),
        "the refusal must not echo the credential"
    );

    // It is still `active` in the row and still listed with its expiry, because
    // expiry is evaluated when the snapshot is built, not stored as a third
    // status: an operator looking at the table sees *why* it stopped working.
    let body = past.json();
    assert_eq!(body["status"], "active");
    assert!(body["revoked_at"].is_null());
    assert_eq!(body["expires_at"], "2020-01-01T00:00:00.000000000Z");

    // A key with a lifetime still ahead of it is unaffected by the same code
    // path, so the refusal above is the clock and not a blanket rule.
    create_partner(&client, &server, "later", &["gpt-4o"]).await;
    let future = issue(
        &client,
        &server,
        json!({
            "name": "later",
            "consumer_id": "later",
            "expires_at": "2999-01-01T00:00:00Z",
        }),
    )
    .await;
    assert_eq!(
        me(&client, &server, &secret_of(&future)).await.status,
        StatusCode::OK
    );

    // An expiry that does not parse is refused rather than dropped, because a
    // dropped expiry is a credential that never dies.
    let typo = client
        .post_json(
            &server.url("/api/admin/api-keys"),
            Some(MANAGER_PASSWORD),
            json!({
                "name": "typo",
                "consumer_id": "typo",
                "expires_at": "next tuesday",
            }),
        )
        .await;
    assert_eq!(typo.status, StatusCode::BAD_REQUEST, "{}", typo.text());

    assert_eq!(
        upstream.request_count(),
        0,
        "no refused request may reach the upstream"
    );
    assert_eq!(
        row_count(&server.open_db()),
        0,
        "no refused request may be metered"
    );
}

// ---------------------------------------------------------------------------
// Durability
// ---------------------------------------------------------------------------

/// A key issued through the API is a database row, so it outlives the process
/// that minted it.
///
/// Nothing is seeded before either start, so after the restart the only
/// credential that can authenticate is the one the admin API created — a
/// restart that came back with an empty key set would fail this test, and so
/// would one that re-read the key from anywhere but the database.
#[tokio::test]
async fn a_key_issued_through_the_api_survives_a_restart() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let spec = admin_spec(&upstream);
    let mut server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    create_partner(&client, &server, "acme", &["gpt-4o"]).await;
    let created = issue(
        &client,
        &server,
        json!({"name": "durable", "consumer_id": "acme"}),
    )
    .await;
    let secret = secret_of(&created);
    let row = wait_until_usable(&client, &server, &secret).await;
    assert_eq!(row.consumer_id, "acme");

    let pid = server.pid();
    server.sigterm();
    let status = server.wait_exit(WAIT_TIMEOUT).await;
    assert!(
        status.success(),
        "a drained shutdown must exit cleanly: {status}"
    );

    // The restart re-seeds nothing (the spec holds no keys), so the database is
    // the only place this credential can come from.
    let server = TestServer::start_in(&server.root, spec).await;
    assert_ne!(server.pid(), pid, "this must be a new process");

    let revived = me(&client, &server, &secret).await;
    assert_eq!(
        revived.status,
        StatusCode::OK,
        "a key issued through the admin API must survive a restart: {}",
        revived.text()
    );
    assert_eq!(revived.json()["consumer_id"], "acme");
    assert_eq!(revived.json()["key_name"], "durable");

    // Still a working credential, not merely a valid identity.
    let chat_response = chat(&client, &server, &secret, "gpt-4o").await;
    assert_eq!(chat_response.status, StatusCode::OK);
}

/// Wait until the issued key can be used and return the ledger row it produced.
async fn wait_until_usable(
    client: &TestClient,
    server: &TestServer,
    secret: &str,
) -> crate::common::LedgerRow {
    let response = chat(client, server, secret, "gpt-4o").await;
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let request_id = response.request_id().expect("x-request-id");
    wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await
}

// ---------------------------------------------------------------------------
// The plaintext
// ---------------------------------------------------------------------------

/// The plaintext is a credential the deployment holds, not one the deployment
/// stores, logs, or hands back a second time.
#[tokio::test]
async fn the_plaintext_never_reaches_the_database_the_log_or_a_read_endpoint() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let server = admin_server(&upstream).await;
    let client = TestClient::new();

    create_partner(&client, &server, "acme", &["gpt-4o"]).await;
    let created = issue(
        &client,
        &server,
        json!({"name": "watched", "consumer_id": "acme"}),
    )
    .await;
    let secret = secret_of(&created);

    // Use it, so the request path has had every chance to log or persist it.
    let response = chat(&client, &server, &secret, "gpt-4o").await;
    assert_eq!(response.status, StatusCode::OK);
    let request_id = response.request_id().expect("x-request-id");
    wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;

    // The prefix *is* stored, which is what makes the negative below a real
    // negative: a search that finds the head of the plaintext would find the
    // whole of it, so finding only the head is evidence, not an artefact.
    let prefix = &secret[..KEY_PREFIX_LEN];
    assert!(
        db_files_contain(&server, prefix),
        "the prefix identifies the key and is meant to be stored"
    );
    assert!(
        !db_files_contain(&server, &secret),
        "the plaintext must never be written to the database"
    );

    let logs = server.logs();
    assert!(
        !logs.contains(&secret),
        "the plaintext must never be logged; log:\n{logs}"
    );
    assert!(
        logs.contains("acme"),
        "the key's *identity* is what a log line may name; log:\n{logs}"
    );

    // The unauthenticated surface must not carry it either.
    let version = client.get_json(&server.url("/version"), None).await;
    assert_eq!(version.status, StatusCode::OK);
    assert!(!version.text().contains(&secret), "{}", version.text());

    // Nor the refusal — the 401 body is the one an attacker sees most often, and
    // the one most tempting to make "helpful".
    let revoked = client
        .post_json(
            &server.url(&format!(
                "/api/admin/api-keys/{}/revoke",
                created.json()["id"].as_i64().expect("id")
            )),
            Some(MANAGER_PASSWORD),
            json!({}),
        )
        .await;
    assert_eq!(revoked.status, StatusCode::OK);
    let refused = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(&secret),
            chat_request("gpt-4o"),
            &[],
        )
        .await;
    assert_eq!(refused.status, StatusCode::UNAUTHORIZED);
    assert!(
        !refused.text().contains(&secret),
        "a refusal must not echo the credential: {}",
        refused.text()
    );
    assert_eq!(refused.header("cache-control").as_deref(), Some("no-store"));
}
