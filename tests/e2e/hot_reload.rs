//! Configuration hot reload: a valid change lands, an invalid one is refused.

use crate::harness::*;

// ---------------------------------------------------------------------------

/// The configuration contract: a change is picked up within about a second, and
/// a change that does not validate leaves the previous configuration in force.
///
/// Both halves matter. A reload that takes a minute is not a reload; a reload
/// that applies half a broken file takes a working proxy down.
///
/// The observable is the **upstream** credential. It used to be the local key:
/// the old config carried a `keys:` list, so a reload could rotate the
/// credential a client presented and the test read the effect straight off the
/// auth path. Keys are database rows now — a file rewrite cannot move one — so
/// the assertion is rebased on the other credential the file still holds, and
/// the property under test (a valid change is adopted, an invalid one is not,
/// and recovery is not a latch) is unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn hot_reload_applies_a_valid_change_and_refuses_an_invalid_one() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let mut instance = Instance::start("a", dir.path(), &db_path, &upstream);
    assert!(instance.wait_ready(WAIT).await);

    let client = ProxyClient::new();
    let base = instance.base_url();
    let key = instance.key().to_string();

    // --- Baseline: the configured upstream credential is the one used -------
    assert_eq!(client.chat(&base, "reload-0", &key).await, Ok(200));
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
    );

    let mut applied = None;
    while started.elapsed() < Duration::from_secs(10) {
        if mock.last_auth().as_deref() == Some("Bearer rotated-upstream-secret") {
            applied = Some(started.elapsed());
            break;
        }
        // Keep a request in flight so the credential is actually presented: the
        // mock only records an Authorization header on a request it serves.
        let _ = client.chat(&base, "reload-probe", &key).await;
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

    // The reloaded process is still the one serving, on the same key: a reload
    // moves the file's values, it does not re-issue a credential.
    assert_eq!(
        client.chat(&base, "reload-same-key", &key).await,
        Ok(200),
        "the seeded key must keep working across a reload"
    );
    assert_eq!(
        client
            .chat_with_key(&base, "reload-old-key", "a-key-that-never-existed")
            .await,
        Ok(401),
        "an unknown credential must not start working because the file changed"
    );

    // --- An invalid change is refused, and the working config stays ---------
    let before_health = raw_get(&base, "/healthz").await;
    let before_ready = raw_get(&base, "/readyz").await;

    // (a) unparseable YAML
    write_raw(&instance.config_path, "this: [is not: valid yaml\n");
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        client.chat(&base, "reload-badyaml", &key).await,
        Ok(200),
        "an unparseable config must not take the proxy down"
    );
    assert_eq!(
        mock.last_auth().as_deref(),
        Some("Bearer rotated-upstream-secret"),
        "an unparseable file must not change what the process presents"
    );

    // (b) parseable but invalid: a base URL with no host, which is a field the
    // loader validates rather than a field serde rejects. The old case here was
    // an empty key list, which is no longer a config concern at all.
    write_raw(
        &instance.config_path,
        "upstream:\n  base_url: \"http://\"\n  api_key: \"k\"\n",
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        client.chat(&base, "reload-invalid", &key).await,
        Ok(200),
        "a config that fails validation must not take the proxy down"
    );
    assert_eq!(
        mock.last_auth().as_deref(),
        Some("Bearer rotated-upstream-secret"),
        "a config that fails validation must not be adopted"
    );

    // A `keys:` block is no longer a deprecated field that quietly stops
    // mattering — it is a parse error naming the field. An operator upgrading
    // with the old file must be told by the process, not by a 401 later.
    write_raw(
        &instance.config_path,
        "upstream:\n  base_url: \"http://example.com\"\n  api_key: \"k\"\nkeys: []\n",
    );
    tokio::time::sleep(Duration::from_millis(2500)).await;
    assert_eq!(
        client.chat(&base, "reload-keys-block", &key).await,
        Ok(200),
        "a config carrying a keys block must not take the proxy down"
    );
    assert_eq!(
        mock.last_auth().as_deref(),
        Some("Bearer rotated-upstream-secret"),
        "a config carrying a keys block must not be adopted"
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
    );

    let mut ok = false;
    while recovered.elapsed() < Duration::from_secs(10) {
        if client.chat(&base, "reload-final", &key).await == Ok(200)
            && mock.last_auth().as_deref() == Some("Bearer final-upstream-secret")
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
