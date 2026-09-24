//! Configuration hot reload: a valid change lands, an invalid one is refused.

use crate::harness::*;

// ---------------------------------------------------------------------------

/// The configuration contract: a change is picked up within about a second, and
/// a change that does not validate leaves the previous configuration in force.
///
/// Both halves matter. A reload that takes a minute is not a reload; a reload
/// that applies half a broken file takes a working proxy down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_reload_applies_a_valid_change_and_refuses_an_invalid_one() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let mut instance = Instance::start("a", dir.path(), &db_path, &upstream);
    assert!(instance.wait_ready(WAIT).await);

    let client = ProxyClient::new();
    let base = instance.base_url();

    // --- Baseline: the configured upstream credential is the one used -------
    assert_eq!(client.chat(&base, "reload-0").await, Ok(200));
    assert_eq!(
        mock.last_auth().as_deref(),
        Some(format!("Bearer {UPSTREAM_KEY}").as_str()),
        "the proxy must present the configured upstream credential"
    );

    // --- A valid change lands within about one poll interval ---------------
    let started = Instant::now();
    write_config_full(
        &instance.config_path,
        &db_path,
        &upstream,
        "rotated-upstream-secret",
        "rotated-local-key",
        "rotated",
        E2E_MODELS,
    );

    let mut applied = None;
    while started.elapsed() < Duration::from_secs(10) {
        // The new local key must be the one that authenticates from now on.
        if client
            .chat_with_key(&base, "reload-probe", "rotated-local-key")
            .await
            == Ok(200)
        {
            applied = Some(started.elapsed());
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }

    let applied = applied.expect("the reloaded configuration never took effect");
    eprintln!("hot reload applied after {} ms", applied.as_millis());

    // The poll interval is one second; allow generous slack for scheduling and
    // for a request to complete, but not so much that a 10-second poll passes.
    assert!(
        applied < Duration::from_secs(4),
        "a config change took {} ms to apply; the contract is about one second",
        applied.as_millis()
    );

    // The rotated upstream credential is now the one presented upstream.
    assert_eq!(
        mock.last_auth().as_deref(),
        Some("Bearer rotated-upstream-secret"),
        "the reloaded upstream credential must be the one sent"
    );

    // The old local key is gone: the key list was replaced, not merged.
    assert_eq!(
        client.chat(&base, "reload-old-key").await,
        Ok(401),
        "a key removed by the reload must stop working"
    );

    // --- An invalid change is refused, and the working config stays ---------
    let before_health = raw_get(&base, "/healthz").await;
    let before_ready = raw_get(&base, "/readyz").await;

    // (a) unparseable YAML
    write_raw(&instance.config_path, "this: [is not: valid yaml\n");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        client
            .chat_with_key(&base, "reload-badyaml", "rotated-local-key")
            .await,
        Ok(200),
        "an unparseable config must not take the proxy down"
    );

    // (b) parseable but invalid: a key list that fails validation
    write_raw(
        &instance.config_path,
        "upstream:\n  base_url: \"http://example.com\"\n  api_key: \"k\"\nkeys: []\n",
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        client
            .chat_with_key(&base, "reload-invalid", "rotated-local-key")
            .await,
        Ok(200),
        "a config that fails validation must not take the proxy down"
    );

    assert_eq!(
        raw_get(&base, "/healthz").await,
        before_health,
        "liveness must not be disturbed by a rejected reload"
    );
    assert_eq!(
        raw_get(&base, "/readyz").await,
        before_ready,
        "readiness must not be disturbed by a rejected reload"
    );

    // --- Recovery: a valid config is still accepted afterwards -------------
    let recovered = Instant::now();
    write_config_full(
        &instance.config_path,
        &db_path,
        &upstream,
        "final-upstream-secret",
        "final-local-key",
        "final",
        E2E_MODELS,
    );

    let mut ok = false;
    while recovered.elapsed() < Duration::from_secs(10) {
        if client
            .chat_with_key(&base, "reload-final", "final-local-key")
            .await
            == Ok(200)
        {
            ok = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
    assert!(ok, "a valid config after a rejected one must still apply");
    assert_eq!(
        mock.last_auth().as_deref(),
        Some("Bearer final-upstream-secret")
    );

    let exit = instance
        .terminate_and_wait(WAIT)
        .await
        .expect("instance did not exit after SIGTERM");
    assert!(exit.success(), "instance exited with {exit:?}");
}

// ---------------------------------------------------------------------------
