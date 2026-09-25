//! Configuration hot reload: what happens to a running process when the file it
//! was started with is rewritten, truncated, deleted, or changed under load.
//!
//! The contract is: a valid change is picked up without a restart, and an
//! invalid one is refused — the process keeps serving the last configuration it
//! could parse. A reload that silently adopted a half-written file would
//! repoint traffic or drop credentials at exactly the moment an operator is
//! editing them.

use std::time::Duration;

use crate::common::{
    Behaviour, CLIENT_KEY, KeySpec, MockUpstream, Spec, TestClient, TestServer, UPSTREAM_KEY,
    WAIT_TIMEOUT, chat_request, chat_stream_request, upstream_bearer, wait_for_terminal,
    wait_for_terminal_count,
};
use http::{Method, StatusCode};

/// A second credential, used to prove a reload landed.
const ROTATED_KEY: &str = "sk-local-rotated-key";

/// Wait until the child's log contains `needle`.
///
/// The watcher ticks every second and reads the file on each tick, so the effect
/// is observed rather than assumed.
async fn wait_for_log(server: &TestServer, needle: &str, timeout: Duration) -> bool {
    crate::common::wait_until(timeout, || server.logs().contains(needle)).await
}

#[tokio::test]
async fn a_valid_change_is_picked_up_without_a_restart() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_keys(vec![KeySpec::new(CLIENT_KEY, "before")]);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    let before = client
        .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
        .await;
    assert_eq!(before.json()["key_name"], "before");
    let pid = server.pid();

    // Rename the key and add a second one: a change the running process has no
    // way to know about except by re-reading the file.
    let updated = spec.clone().with_keys(vec![
        KeySpec::new(CLIENT_KEY, "after"),
        KeySpec::new(ROTATED_KEY, "added").with_consumer("added-consumer"),
    ]);
    server.write_config(&updated);

    let mut adopted = false;
    for _ in 0..500 {
        let renamed = client
            .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
            .await;
        let added = client
            .get_json(&server.url("/api/me"), Some(ROTATED_KEY))
            .await;
        if renamed.json()["key_name"] == "after" && added.status == StatusCode::OK {
            assert_eq!(added.json()["consumer_id"], "added-consumer");
            adopted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(adopted, "the renamed and added keys must be adopted");
    assert_eq!(server.pid(), pid, "a reload must not restart the process");
    assert!(
        wait_for_log(&server, "Configuration reloaded", Duration::from_secs(2)).await,
        "the reload must be logged; log:\n{}",
        server.logs()
    );
}

#[tokio::test]
async fn an_invalid_config_is_refused_and_the_old_one_keeps_serving() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    assert_eq!(
        client
            .get_json(&server.url("/api/me"), Some(server.key()))
            .await
            .status,
        StatusCode::OK
    );
    let pid = server.pid();

    server.write_config_raw("upstream: [this is not a mapping\nkeys: ]\n");

    assert!(
        wait_for_log(&server, "Failed to reload config", Duration::from_secs(10)).await,
        "the watcher must report the rejected reload; log:\n{}",
        server.logs()
    );

    // The key still works and the process is the same one.
    let me = client
        .get_json(&server.url("/api/me"), Some(server.key()))
        .await;
    assert_eq!(me.status, StatusCode::OK);
    assert_eq!(me.json()["consumer_id"], server.consumer());

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
    assert_eq!(server.pid(), pid);
}

#[tokio::test]
async fn a_half_written_config_is_refused() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    // What a non-atomic write looks like at the instant it is observed: a
    // syntactically valid prefix that has not been closed off yet. Treating it
    // as the new configuration would drop the trailing keys.
    let full = server.read_config();
    let truncated = &full[..full.len() / 2];
    server.write_config_raw(truncated);

    assert!(
        wait_for_log(&server, "Failed to reload config", Duration::from_secs(10)).await,
        "a truncated file must be refused, not adopted"
    );
    assert_eq!(
        client
            .get_json(&server.url("/api/me"), Some(server.key()))
            .await
            .status,
        StatusCode::OK
    );
}

#[tokio::test]
async fn a_deleted_config_keeps_the_old_one_and_a_new_file_is_adopted() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_keys(vec![KeySpec::new(CLIENT_KEY, "first")]);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    std::fs::remove_file(&server.config_path).expect("remove the config file");
    assert!(
        wait_for_log(&server, "Failed to reload config", Duration::from_secs(10)).await,
        "a missing config file must be refused, not treated as an empty config"
    );
    let me = client
        .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
        .await;
    assert_eq!(
        me.status,
        StatusCode::OK,
        "the old config must keep serving"
    );

    // Putting a valid file back must be picked up again: the failure above is
    // not a latch.
    let restored = spec
        .clone()
        .with_keys(vec![KeySpec::new(CLIENT_KEY, "second")]);
    server.write_config(&restored);

    let mut adopted = false;
    for _ in 0..300 {
        let me = client
            .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
            .await;
        if me.json()["key_name"] == "second" {
            adopted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(adopted, "recovery after a missing file must not be a latch");
}

#[tokio::test]
async fn a_repointed_upstream_takes_effect_on_the_next_request() {
    let first = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let second = MockUpstream::start(Behaviour::ChatJson {
        prompt: 2,
        completion: 2,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&first);
    let server = TestServer::start(spec.clone()).await;
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
    assert_eq!(first.request_count(), 1);
    assert_eq!(second.request_count(), 0);

    // Repoint the upstream *and* rotate the credential in one change.
    let moved = spec
        .clone()
        .with_upstream_url(&second.url())
        .with_upstream_key("sk-rotated-upstream");
    server.write_config(&moved);

    let mut seen_on_second = None;
    for _ in 0..300 {
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
        if second.request_count() > 0 {
            seen_on_second = second.requests().into_iter().next();
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }

    let forwarded = seen_on_second.expect("traffic must move to the new upstream");
    assert_eq!(
        upstream_bearer(&forwarded).as_deref(),
        Some("sk-rotated-upstream"),
        "the rotated credential must be used, not the one captured at startup"
    );
    assert_ne!(
        upstream_bearer(&forwarded).as_deref(),
        Some(UPSTREAM_KEY),
        "the credential captured at startup must no longer be used"
    );
}

#[tokio::test]
async fn a_request_in_flight_survives_a_reload() {
    let upstream = MockUpstream::start(Behaviour::ChatStream {
        prompt: 8,
        completion: 4,
        cached: 0,
        events: 6,
        delay_ms: 80,
    })
    .await;
    let spec = Spec::new(&upstream).with_keys(vec![KeySpec::new(CLIENT_KEY, "initial")]);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    // Start a stream that takes roughly half a second to finish.
    let response = client
        .send(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(CLIENT_KEY),
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

    // Rewrite the config underneath the live request: a new key name and a
    // different batch window. The request already accepted must be unaffected.
    let reloaded = spec
        .clone()
        .with_keys(vec![KeySpec::new(CLIENT_KEY, "reloaded")]);
    server.write_config(&reloaded);

    let mut reader = crate::common::BodyReader::new(response.into_body());
    let body = reader
        .read_to_end(Duration::from_secs(10))
        .await
        .expect("the stream must finish");
    let text = String::from_utf8_lossy(&body).to_string();
    assert!(
        text.contains("data: [DONE]"),
        "the stream must complete intact"
    );

    let row = wait_for_terminal(&server.db_path, &request_id, WAIT_TIMEOUT).await;
    assert_eq!(row.request_status, "completed");
    assert_eq!(row.input_tokens, Some(8));
    assert_eq!(row.output_tokens, Some(4));
}

#[tokio::test]
async fn rapid_rewrites_converge_on_the_last_valid_config() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_keys(vec![KeySpec::new(CLIENT_KEY, "v0")]);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    // Successive rewrites, each observed before the next one is written, so a
    // reload in progress can never be what the next assertion is racing.
    for name in ["v1", "v2", "v3", "final"] {
        let next = spec.clone().with_keys(vec![KeySpec::new(CLIENT_KEY, name)]);
        server.write_config(&next);

        let mut observed = false;
        for _ in 0..500 {
            let me = client
                .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
                .await;
            if me.json()["key_name"] == name {
                observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(observed, "rewrite to {name} must take effect");
    }

    // An invalid write after the last valid one must not win: the configuration
    // in force stays the one that was last parsed successfully.
    server.write_config_raw("keys: ]\n");
    assert!(
        wait_for_log(&server, "Failed to reload config", Duration::from_secs(10)).await,
        "the invalid write must be refused"
    );

    // And it stays there while the invalid file sits on disk.
    tokio::time::sleep(Duration::from_millis(1500)).await;
    let me = client
        .get_json(&server.url("/api/me"), Some(CLIENT_KEY))
        .await;
    assert_eq!(me.status, StatusCode::OK);
    assert_eq!(me.json()["key_name"], "final");

    // The reloaded configuration is the one being used for new traffic too.
    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(CLIENT_KEY),
            chat_request("gpt-4o"),
            &[],
        )
        .await;
    assert_eq!(response.status, StatusCode::OK);
    wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;
}
