//! A key change made on one instance reaches the other.
//!
//! The api key set lives in one database and is read from an in-memory snapshot,
//! so a change committed by *this* process is visible to it at once while a
//! sibling carries on with what it loaded at start-up. During a rolling update
//! both instances exist at once, so that gap is the normal case: an instance
//! that never noticed a revocation would keep authenticating a withdrawn
//! credential until someone restarted it, and nothing would say so.
//!
//! The bound is `server.api_key_refresh_ms` — one second by default, which is
//! what these tests are given, because that is the number a deployment gets.
//! Both tests therefore assert *when* the sibling learned, not merely that it
//! eventually did: "eventually" is what a poll interval that was never read
//! would also satisfy.

use crate::harness::*;

/// The id of the key whose stored prefix heads `plaintext`, read through the
/// admin listing rather than from a fixture.
///
/// The admin API is the only surface that publishes ids, and a test that read
/// the seed's id out of the harness would be asserting on the harness instead of
/// on the product. The prefix is the operator-visible identification of a key,
/// which is exactly what an operator would match on.
async fn key_id_for(client: &ProxyClient, instance: &Instance, plaintext: &str) -> i64 {
    let (status, body) = client
        .get(
            &instance.base_url(),
            "/api/admin/api-keys",
            Some(MANAGER_PASSWORD),
        )
        .await;
    assert_eq!(status, 200, "the manager must be able to list keys: {body}");

    let items: Value = serde_json::from_str(&body).expect("the listing is JSON");
    items
        .as_array()
        .expect("the listing is an array")
        .iter()
        .find(|item| {
            item["key_prefix"]
                .as_str()
                .is_some_and(|prefix| plaintext.starts_with(prefix))
        })
        .and_then(|item| item["id"].as_i64())
        .unwrap_or_else(|| panic!("no listed key matches the plaintext's prefix: {body}"))
}

/// Two instances, one database, both accepting `key`.
async fn pair(
    dir: &tempfile::TempDir,
    db_path: &std::path::Path,
    upstream: &str,
) -> (Instance, Instance, String, ProxyClient) {
    let a = Instance::start("a", dir.path(), db_path, upstream);
    let key = a.key().to_string();
    let b = Instance::start_existing("b", dir.path(), db_path, upstream, vec![key.clone()]);
    assert!(a.wait_ready(WAIT).await, "A never became ready");
    assert!(b.wait_ready(WAIT).await, "B never became ready");

    let client = ProxyClient::new();
    // Both must accept the credential to begin with, or the refusal asserted
    // later would be the fixture's doing rather than the revocation's.
    for instance in [&a, &b] {
        assert_eq!(
            client.chat(&instance.base_url(), "e2e-ring", &key).await,
            Ok(200),
            "both instances must serve the shared key before anything is revoked"
        );
    }
    (a, b, key, client)
}

// ---------------------------------------------------------------------------

/// The revocation is effective on the instance that made it immediately, and on
/// its sibling within one refresh interval — without a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_revocation_on_one_instance_reaches_the_other_without_a_restart() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let (a, b, key, client) = pair(&dir, &db_path, &upstream).await;
    let id = key_id_for(&client, &a, &key).await;

    let (status, body) = client
        .request_json(
            "POST",
            &a.base_url(),
            &format!("/api/admin/api-keys/{id}/revoke"),
            Some(MANAGER_PASSWORD),
            json!({}),
        )
        .await;
    assert_eq!(status, 200, "the revoke must be accepted: {body}");
    assert_eq!(
        serde_json::from_str::<Value>(&body).expect("JSON")["status"],
        "revoked"
    );

    // A committed the change through its own store, which refreshes before
    // returning: there is nothing to wait for here, and waiting would hide a
    // regression in that.
    let (status, _) = client.get(&a.base_url(), "/api/me", Some(&key)).await;
    assert_eq!(
        status, 401,
        "the instance that performed the revoke must refuse on the very next request"
    );

    // B learns it from the poll. The wait is bounded by the configured interval,
    // not by patience: one second plus slack, so a poller that stopped running
    // fails here instead of timing out politely.
    let pid = b.pid();
    let started = Instant::now();
    let mut learned_at = None;
    while started.elapsed() < Duration::from_secs(10) {
        let (status, _) = client.get(&b.base_url(), "/api/me", Some(&key)).await;
        if status == 401 {
            learned_at = Some(started.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let learned_at = learned_at.expect(
        "the sibling never stopped authenticating the revoked key; a credential \
         that outlives its revocation is the failure this test exists to catch",
    );
    eprintln!(
        "sibling observed the revocation after {} ms",
        learned_at.as_millis()
    );
    assert!(
        learned_at < Duration::from_secs(5),
        "the sibling took {} ms to notice; the refresh interval is one second, \
         so something other than the poll is going on",
        learned_at.as_millis()
    );
    assert_eq!(
        b.pid(),
        pid,
        "the sibling must learn this from the database, not from a restart"
    );

    // And the sibling refuses it on the inference path too, not only on
    // `/api/me`: the two go through the same extractor, but the metering path is
    // the one that matters.
    assert_eq!(
        client.chat(&b.base_url(), "e2e-ring", &key).await,
        Ok(401),
        "the sibling must refuse the revoked key on the proxy path as well"
    );

    drop(a);
    drop(b);
    let _ = mock.models_seen();
}

/// A key issued on one instance starts working on the other within one refresh
/// interval — the ordering a rolling update depends on.
///
/// The new instance of a rolling update is the one that must be provisioned
/// *first*: it starts against the database and loads whatever keys exist. This
/// is the same property from the other direction — a key that exists in the
/// database becomes usable everywhere without a redeploy.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_issued_on_one_instance_starts_working_on_the_other() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let (a, b, _seeded, client) = pair(&dir, &db_path, &upstream).await;

    let (status, body) = client
        .request_json(
            "POST",
            &a.base_url(),
            "/api/admin/api-keys",
            Some(MANAGER_PASSWORD),
            json!({
                "name": "issued-on-a",
                "consumer_id": "tester",
                "allowed_models": ["e2e-ring"],
            }),
        )
        .await;
    assert_eq!(status, 201, "issuing a key failed: {body}");
    let issued: Value = serde_json::from_str(&body).expect("JSON");
    let plaintext = issued["key_secret"]
        .as_str()
        .expect("create returns the plaintext once")
        .to_string();

    // A knows it at once.
    assert_eq!(
        client.chat(&a.base_url(), "e2e-ring", &plaintext).await,
        Ok(200),
        "the issuing instance must accept its own key immediately"
    );

    // B picks it up from the database, within the interval and without a
    // restart.
    let pid = b.pid();
    let started = Instant::now();
    let mut accepted_at = None;
    while started.elapsed() < Duration::from_secs(10) {
        if client.chat(&b.base_url(), "e2e-ring", &plaintext).await == Ok(200) {
            accepted_at = Some(started.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let accepted_at = accepted_at.expect(
        "a key issued on one instance never reached the other; a rolling update \
         that provisions a key on the new instance would serve 401s",
    );
    eprintln!(
        "sibling accepted the new key after {} ms",
        accepted_at.as_millis()
    );
    assert!(
        accepted_at < Duration::from_secs(5),
        "the sibling took {} ms; the refresh interval is one second",
        accepted_at.as_millis()
    );
    assert_eq!(
        b.pid(),
        pid,
        "the sibling must not need a restart to see it"
    );

    // The identity the sibling applies is the row's, whichever instance issued
    // it — the credential carries the consumer, the caller does not.
    let (status, body) = client.get(&b.base_url(), "/api/me", Some(&plaintext)).await;
    assert_eq!(status, 200, "{body}");
    let me: Value = serde_json::from_str(&body).expect("JSON");
    assert_eq!(me["consumer_id"], "tester");
    assert_eq!(me["key_name"], "issued-on-a");

    drop(a);
    drop(b);
    let _ = mock.models_seen();
}
