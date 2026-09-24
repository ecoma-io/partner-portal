//! Dashboard isolation and the realtime invalidation channel.

use crate::harness::*;

// ---------------------------------------------------------------------------

/// Two keys, two consumers, one proxy: neither may see the other's data.
///
/// This is the invariant the dashboard is judged on. It is checked against the
/// real binary with real traffic, because the failure mode this guards against —
/// a scope that is applied in one handler and forgotten in another — is
/// invisible to a unit test of the store.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn dashboard_is_isolated_per_key_and_ignores_a_client_supplied_identity() {
    const KEY_A: &str = "key-alpha";
    const KEY_B: &str = "key-beta";

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let port = free_port();
    let config_path = dir.path().join("config.yaml");
    write_config_multi(
        &config_path,
        &db_path,
        &upstream,
        &[(KEY_A, "alpha-key", "alpha"), (KEY_B, "beta-key", "beta")],
    );

    let child = Command::new(binary_path())
        .env("PARTNER_PORTAL_CONFIG", &config_path)
        .env("PARTNER_PORTAL_LISTEN", format!("127.0.0.1:{port}"))
        .env("RUST_LOG", "warn")
        .current_dir(dir.path())
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .spawn()
        .expect("failed to spawn partner-portal");

    let instance = Instance {
        child,
        port,
        config_path,
    };
    assert!(
        instance.wait_ready(WAIT).await,
        "instance never became ready"
    );
    let base = instance.base_url();

    let client = ProxyClient::new();

    // --- Two consumers produce distinguishable traffic ----------------------
    for i in 0..3 {
        assert_eq!(
            client
                .chat_with_key(&base, &format!("alpha-{i}"), KEY_A)
                .await,
            Ok(200)
        );
    }
    for i in 0..2 {
        assert_eq!(
            client
                .chat_with_key(&base, &format!("beta-{i}"), KEY_B)
                .await,
            Ok(200)
        );
    }

    // The ledger really does hold both consumers' rows.
    let conn = Connection::open(&db_path).unwrap();
    let consumers: Vec<String> = column(
        &conn,
        "SELECT DISTINCT consumer_id FROM usage_records ORDER BY consumer_id",
    );
    assert_eq!(
        consumers,
        vec!["alpha".to_string(), "beta".to_string()],
        "the fixture must have produced traffic for both consumers"
    );
    assert_eq!(mock.models_seen().len(), 5);

    // --- Identity comes from the key, never from the request -----------------
    let (status, body) = client.get(&base, "/api/me", Some(KEY_A)).await;
    assert_eq!(status, 200, "GET /api/me failed: {body}");
    let me: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(me["consumer_id"], "alpha");
    assert_eq!(me["key_name"], "alpha-key");

    // A header claiming to be someone else is not an identity.
    let (status, body) = client
        .get_with_headers(&base, "/api/me", Some(KEY_A), &[("x-consumer-id", "beta")])
        .await;
    assert_eq!(status, 200);
    let me: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        me["consumer_id"], "alpha",
        "a client-supplied consumer identity must be ignored"
    );

    // --- Summary is scoped to the authenticated consumer --------------------
    let (status, body) = client
        .get(&base, "/api/dashboard/summary", Some(KEY_A))
        .await;
    assert_eq!(status, 200, "GET summary failed: {body}");
    let summary: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        summary["total_requests"], 3,
        "alpha's summary must count only alpha's requests: {body}"
    );
    assert_eq!(summary["success_count"], 3);
    assert_eq!(
        summary["total_input_tokens"], 30,
        "3 requests x 10 prompt tokens: {body}"
    );
    assert_eq!(summary["total_output_tokens"], 15);
    assert_eq!(summary["total_cached_tokens"], 6);

    let (_, body) = client
        .get(&base, "/api/dashboard/summary", Some(KEY_B))
        .await;
    let beta_summary: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        beta_summary["total_requests"], 2,
        "beta's summary must count only beta's requests: {body}"
    );
    assert_eq!(beta_summary["total_input_tokens"], 20);

    // --- The request list is scoped, and an identity parameter changes nothing
    for probe in [
        "/api/dashboard/requests",
        // A client-supplied selector must not widen the scope.
        "/api/dashboard/requests?consumer_id=beta",
        "/api/dashboard/requests?consumer_id=alpha",
    ] {
        let (status, body) = client.get(&base, probe, Some(KEY_A)).await;
        assert_eq!(status, 200, "GET {probe} failed: {body}");
        let page: Value = serde_json::from_str(&body).unwrap();
        let models: Vec<String> = page["data"]
            .as_array()
            .expect("data must be an array")
            .iter()
            .map(|item| item["model"].as_str().unwrap_or_default().to_string())
            .collect();

        assert_eq!(
            models.len(),
            3,
            "GET {probe} returned {} rows",
            models.len()
        );
        assert!(
            models.iter().all(|m| m.starts_with("alpha-")),
            "GET {probe} leaked another consumer's rows: {models:?}"
        );
    }

    // The same for the metrics endpoints that take a model filter.
    let (status, body) = client
        .get(
            &base,
            "/api/dashboard/timeseries?consumer_id=beta",
            Some(KEY_A),
        )
        .await;
    assert_eq!(status, 200, "GET timeseries failed: {body}");
    let series: Value = serde_json::from_str(&body).unwrap();
    let series_text = series.to_string();
    assert!(
        !series_text.contains("beta-"),
        "timeseries leaked another consumer's models: {body}"
    );

    // --- Unauthenticated access is refused ----------------------------------
    for probe in [
        "/api/me",
        "/api/dashboard/summary",
        "/api/dashboard/requests",
        "/api/dashboard/models",
        "/api/dashboard/events",
    ] {
        let (status, _) = client.get(&base, probe, None).await;
        assert_eq!(status, 401, "GET {probe} without a key must be 401");
    }

    let (status, _) = client.get(&base, "/api/me", Some("not-a-real-key")).await;
    assert_eq!(status, 401, "an unknown key must be rejected");

    // --- The realtime channel is authenticated and streams SSE ---------------
    {
        let request = hyper::Request::builder()
            .method("GET")
            .uri(format!("{base}/api/dashboard/events"))
            .header("authorization", format!("Bearer {KEY_A}"))
            .body(
                Full::new(Bytes::new())
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .unwrap();

        let response = client.client.request(request).await.unwrap();
        assert_eq!(response.status().as_u16(), 200);
        let content_type = response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default()
            .to_string();
        assert!(
            content_type.contains("text/event-stream"),
            "the events endpoint must stream SSE, got {content_type:?}"
        );
        // Dropping the body cancels the stream; the test does not need an event.
        drop(response);
    }

    // --- The dashboard is served from the same origin, and static assets do
    //     not require a key ------------------------------------------------
    let (status, body) = client.get(&base, "/", None).await;
    assert_eq!(status, 200, "the dashboard index must be served");
    assert!(
        body.contains("<div id=\"app\"") || body.contains("<!DOCTYPE html>"),
        "the dashboard index must be HTML"
    );

    // A 404 under /api/* must be JSON, not the SPA's HTML.
    let (status, body) = client.get(&base, "/api/nope", Some(KEY_A)).await;
    assert_eq!(status, 404);
    assert!(
        body.contains("application/json") || body.trim_start().starts_with('{'),
        "an unknown API path must not fall back to the SPA: {body}"
    );

    drop(instance);
}

// ---------------------------------------------------------------------------
