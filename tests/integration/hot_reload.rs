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
    Behaviour, ManagerSpec, MockUpstream, Spec, TestClient, TestServer, UPSTREAM_KEY, WAIT_TIMEOUT,
    chat_request, chat_stream_request, upstream_bearer, wait_for_terminal, wait_for_terminal_count,
};
use http::{Method, StatusCode};

/// The upstream credential each convergence step rotates to, so the reload is
/// observed from the *upstream's* side: it is the one place a reloaded field
/// is visible without a config field being echoed back over the API.
fn upstream_credential(step: &str) -> String {
    format!("sk-upstream-{step}")
}

/// Wait until the child's log contains `needle`.
///
/// The watcher ticks every second and reads the file on each tick, so the effect
/// is observed rather than assumed.
async fn wait_for_log(server: &TestServer, needle: &str, timeout: Duration) -> bool {
    crate::common::wait_until(timeout, || server.logs().contains(needle)).await
}

/// The observable is a rotated **upstream** credential, not a key name.
///
/// This suite used to prove a reload landed by renaming a key and reading the
/// new name back from `/api/me`. Keys are rows in the database now, so the file
/// cannot change them and the test would pass without testing anything. The
/// property under test is unchanged — a valid change is adopted by the running
/// process, and adoption is not a restart — only the thing that moved did.
#[tokio::test]
async fn a_valid_change_is_picked_up_without_a_restart() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    let pid = server.pid();
    let rotated = upstream_credential("after");

    // A field the running process re-reads per request: the credential the proxy
    // presents to the upstream. Nothing else in the process can know the value
    // changed except that it re-read the file.
    let updated = spec
        .clone()
        .with_upstream_key(&rotated)
        .with_sse_poll_interval(250);
    server.write_config(&updated);

    let mut adopted = false;
    for _ in 0..500 {
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

        if upstream
            .requests()
            .iter()
            .any(|r| upstream_bearer(r).as_deref() == Some(rotated.as_str()))
        {
            adopted = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    assert!(
        adopted,
        "the rotated upstream credential must be adopted; requests:\n{:?}",
        upstream
            .requests()
            .iter()
            .map(upstream_bearer)
            .collect::<Vec<_>>()
    );
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

    server.write_config_raw("upstream: [this is not a mapping\n");

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

/// A config that does not parse is refused, and the process keeps serving on
/// the last one it could.
///
/// This replaced a test that cut the rendered file at its halfway point. That
/// cut only proved anything when a `keys:` block happened to sit past the
/// halfway mark, and the block is gone, so the cut is no longer guaranteed to
/// land on anything incomplete. What replaces it is the set of truncations that
/// are *always* a syntax error regardless of where the block boundaries fall,
/// plus a statement of the limit, which is a property of YAML and not of this
/// product: a cut that removes whole trailing blocks is a **valid** document.
///
/// That limit is real and worth writing down. `server:`, `manager:` and
/// `database:` are all optional blocks with defaults, so a file that has been
/// truncated exactly between two block boundaries is a smaller configuration
/// rather than a broken one, and the loader adopts it. An atomic write
/// (write a temporary file, then rename) is what makes that a non-event; the
/// watcher is not a transaction, and pretending otherwise here would be a claim
/// the code does not make. See ADR 0013 on which fields are live.
#[tokio::test]
async fn a_config_that_does_not_parse_is_refused_and_the_process_keeps_serving() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_manager(ManagerSpec::new("half-written-manager"));
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    // Each of these is a file mid-write. They fail in four different ways
    // (unterminated flow sequence, a bare line where a mapping is expected, a
    // tab where indentation is expected, an unterminated quoted scalar) and
    // none of them depends on where the blocks happen to sit, which is what the
    // halfway-point cut used to rely on. They are written whole rather than
    // sliced out of the rendered file so that the property under test is the
    // parser's, not the fixture's layout.
    for (label, truncated) in [
        (
            "unterminated flow sequence",
            "upstream:\n  base_url: [\"http://127.0.0.1:1\"\n  api_key: \"sk\"\n".to_string(),
        ),
        (
            "a bare line where a mapping is expected",
            "upstream:\n  base_url: \"http://127.0.0.1:1\"\n  api_key: \"sk\"\nnot-a-block\n"
                .to_string(),
        ),
        (
            "a tab where indentation is expected",
            "upstream:\n\tbase_url: \"http://127.0.0.1:1\"\n".to_string(),
        ),
        (
            "an unterminated quoted scalar",
            "upstream:\n  base_url: \"http://127.0.0.1:1\n  api_key: \"sk\"\n".to_string(),
        ),
    ] {
        server.write_config_raw(&truncated);
        assert!(
            wait_for_log(&server, "Failed to reload config", Duration::from_secs(10)).await,
            "{label}: a config that does not parse must be refused; log:\n{}",
            server.logs()
        );
        assert_eq!(
            client
                .get_json(&server.url("/api/me"), Some(server.key()))
                .await
                .status,
            StatusCode::OK,
            "{label}: a refused reload must not disturb the running configuration"
        );
    }

    // The intact document is adopted again, so the refusals above are about the
    // malformed writes and not about this fixture. The manager password is what
    // makes the adoption observable: it is a live credential, so the process
    // answering to the new one is the file having been read.
    let recovered = Spec::new(&upstream).with_manager(ManagerSpec::new("recovered-manager"));
    server.write_config(&recovered);
    assert!(
        wait_for_log(&server, "Configuration reloaded", Duration::from_secs(10)).await,
        "an intact config must still be adopted; log:\n{}",
        server.logs()
    );
    assert_eq!(
        client
            .get_json(&server.url("/api/me"), Some("recovered-manager"))
            .await
            .status,
        StatusCode::OK,
        "the reloaded manager password must be the one in force"
    );
}

/// A missing file is refused, and putting a valid one back is picked up again —
/// the failure is not a latch. The adopted config is observed through the
/// upstream credential, for the same reason as the first test.
#[tokio::test]
async fn a_deleted_config_keeps_the_old_one_and_a_new_file_is_adopted() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    // Prime the upstream's view: one request under the credential the process
    // started with.
    assert_eq!(
        client
            .call(
                Method::POST,
                &server.url("/v1/chat/completions"),
                Some(server.key()),
                chat_request("gpt-4o"),
                &[],
            )
            .await
            .status,
        StatusCode::OK
    );

    std::fs::remove_file(&server.config_path).expect("remove the config file");
    assert!(
        wait_for_log(&server, "Failed to reload config", Duration::from_secs(10)).await,
        "a missing config file must be refused, not treated as an empty config"
    );

    // Still serving on the last-good configuration, with the old credential.
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
        upstream_bearer(upstream.requests().last().expect("a request was made")).as_deref(),
        Some(UPSTREAM_KEY),
        "a refused reload must not change what the process presents"
    );

    // Putting a valid file back must be picked up again.
    let restored = upstream_credential("restored");
    server.write_config(&spec.clone().with_upstream_key(&restored));

    let mut adopted = false;
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
        if upstream
            .requests()
            .iter()
            .any(|r| upstream_bearer(r).as_deref() == Some(restored.as_str()))
        {
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
    let spec = Spec::new(&upstream);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    // Start a stream that takes roughly half a second to finish.
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

    // Rewrite the config underneath the live request: a different upstream
    // credential and a different batch window. The request already accepted must
    // be unaffected — it holds the configuration it was admitted under.
    server.write_config(
        &spec
            .clone()
            .with_upstream_key(&upstream_credential("reloaded")),
    );

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

/// Successive rewrites converge on the last valid one, and an invalid write
/// after it does not win.
///
/// The observable is again the upstream credential, so each step is checked at
/// the only place the value is visible. A key rename used to be the signal; a
/// key is a database row now, so a file rewrite cannot move it and the test
/// would have gone green while proving nothing.
#[tokio::test]
async fn rapid_rewrites_converge_on_the_last_valid_config() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream);
    let server = TestServer::start(spec.clone()).await;
    let client = TestClient::new();

    // Successive rewrites, each observed before the next one is written, so a
    // reload in progress can never be what the next assertion is racing.
    for step in ["v1", "v2", "v3", "final"] {
        let credential = upstream_credential(step);
        let before = upstream.request_count();
        server.write_config(&spec.clone().with_upstream_key(&credential));

        let mut observed = false;
        for _ in 0..500 {
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
            if upstream
                .requests()
                .into_iter()
                .skip(before)
                .any(|r| upstream_bearer(&r).as_deref() == Some(credential.as_str()))
            {
                observed = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(observed, "rewrite to {step} must take effect");
    }

    // An invalid write after the last valid one must not win: the configuration
    // in force stays the one that was last parsed successfully.
    server.write_config_raw("upstream: [this is not a mapping\n");
    assert!(
        wait_for_log(&server, "Failed to reload config", Duration::from_secs(10)).await,
        "the invalid write must be refused"
    );

    // And it stays there while the invalid file sits on disk.
    let before = upstream.request_count();
    tokio::time::sleep(Duration::from_millis(1500)).await;
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
    assert!(
        upstream
            .requests()
            .into_iter()
            .skip(before)
            .all(|r| upstream_bearer(&r).as_deref() == Some(upstream_credential("final").as_str())),
        "the last valid config must stay in force while an invalid file sits on disk"
    );

    // Every request this test issued was really served, and really metered:
    // the convergence claim is only meaningful if traffic never stopped.
    let requests = upstream.request_count();
    assert!(requests > 4, "only {requests} requests were served");
    let rows = wait_for_terminal_count(&server.db_path, requests as i64, WAIT_TIMEOUT).await;
    assert_eq!(rows.len() as usize, requests);
}
