//! The same-VPS rolling update: two instances, one SQLite file.

use crate::harness::*;

// ---------------------------------------------------------------------------
// Tests

/// The full rolling update: two instances, one database, one upstream.
///
/// Traffic runs continuously across the whole sequence, so requests are in
/// flight while B starts, while both serve, and while A drains.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn rolling_update_loses_nothing_and_duplicates_nothing() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    // --- A running, serving traffic ----------------------------------------
    let mut a = Instance::start("a", dir.path(), &db_path, &upstream);
    assert!(
        a.wait_healthy(WAIT).await,
        "instance A never became healthy"
    );
    assert!(a.wait_ready(WAIT).await, "instance A never became ready");

    let rotation = Rotation::new();
    rotation.add(a.base_url());
    let traffic = rotation.run(4, ProxyClient::new());

    // Let A settle into serving.
    tokio::time::sleep(Duration::from_millis(400)).await;
    let accepted_before_b = rotation.accepted();
    assert!(
        accepted_before_b > 0,
        "instance A served nothing before B started"
    );

    // --- Start B, wait for it to be healthy and ready ----------------------
    let mut b = Instance::start("b", dir.path(), &db_path, &upstream);
    assert!(
        b.wait_healthy(WAIT).await,
        "instance B never became healthy"
    );
    assert!(b.wait_ready(WAIT).await, "instance B never became ready");

    // --- Both instances in rotation: the concurrent-writer window ----------
    let a_url = a.base_url();
    rotation.add(b.base_url());
    tokio::time::sleep(Duration::from_millis(800)).await;

    let accepted_overlap = rotation.accepted();
    assert!(
        accepted_overlap > accepted_before_b,
        "no traffic was served during the overlap window"
    );

    // --- A leaves rotation --------------------------------------------------
    rotation.remove(&a_url);

    a.signal(libc::SIGTERM);
    assert!(
        a.wait_unready(Duration::from_secs(10)).await,
        "instance A must fail readiness while it drains, before the listener closes"
    );

    let exit = a
        .wait_exit(WAIT)
        .await
        .expect("instance A did not exit after SIGTERM");
    assert!(exit.success(), "instance A exited with {exit:?}");

    // B must be unaffected by A's departure.
    assert_eq!(
        raw_get(&b.base_url(), "/readyz").await,
        Some(200),
        "instance B must still be ready after A left"
    );

    // --- Keep serving on B, then stop ---------------------------------------
    let accepted_after_a = rotation.accepted();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(
        rotation.accepted() > accepted_after_a,
        "instance B did not take over the traffic after A left rotation"
    );

    rotation.stop();
    for handle in traffic {
        let _ = handle.await;
    }

    // B drains too, so every record it accepted is committed before inspection.
    let b_exit = b
        .terminate_and_wait(WAIT)
        .await
        .expect("instance B did not exit after SIGTERM");
    assert!(b_exit.success(), "instance B exited with {b_exit:?}");

    // --- Verify -------------------------------------------------------------

    let ledger = read_ledger(&db_path);
    assert_ledger_sound(&ledger);
    assert_no_divergence(&mock, &ledger);

    eprintln!(
        "rolling update: {} requests crossed the overlap, upstream saw {}",
        ledger.models.len(),
        mock.models_seen().len()
    );
    eprintln!(
        "rolling update: ledger statuses {:?}",
        status_counts(&db_path)
    );

    // The run must be big enough to be meaningful. Anything less and the
    // concurrent-writer window could pass by having served almost nothing.
    assert!(
        ledger.models.len() > 50,
        "only {} requests crossed the rolling update; too few to prove anything",
        ledger.models.len()
    );

    // The rollup is written in the same transaction as the raw rows, so it must
    // agree with them.
    let conn = Connection::open(&db_path).unwrap();
    let rolled_up: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(request_count), 0) FROM usage_hourly",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        rolled_up as usize,
        ledger.models.len(),
        "the hourly rollup must account for every raw record"
    );
}

/// A request still in flight when SIGTERM arrives must not be lost: it has to
/// reach a terminal state, and the database must still be consistent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sigterm_with_a_request_in_flight_keeps_the_ledger_consistent() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let mut instance = Instance::start("a", dir.path(), &db_path, &upstream);
    assert!(instance.wait_ready(WAIT).await);

    // Stall the upstream so the request is genuinely in flight when the signal
    // lands.
    mock.set_hang(true);

    let client = ProxyClient::new();
    let base = instance.base_url();
    let request = tokio::spawn(async move { client.chat(&base, "model-in-flight").await });

    // Give the request time to be accepted and forwarded.
    tokio::time::sleep(Duration::from_millis(600)).await;

    let exit = instance
        .terminate_and_wait(WAIT)
        .await
        .expect("instance did not exit after SIGTERM");
    assert!(exit.success(), "instance exited with {exit:?}");

    // The client's request ends however it ends — the drain may wait for the
    // upstream timeout, or the connection may close. What matters is the ledger.
    let _ = tokio::time::timeout(WAIT, request).await;

    let ledger = read_ledger(&db_path);
    assert_ledger_sound(&ledger);
    assert_no_divergence(&mock, &ledger);
    assert_eq!(
        ledger.models,
        vec!["model-in-flight".to_string()],
        "the accepted request must be recorded exactly once"
    );

    // It was accepted and forwarded, so it cannot be recorded as a success the
    // upstream never sent.
    let conn = Connection::open(&db_path).unwrap();
    let status: String = conn
        .query_row("SELECT request_status FROM usage_records", [], |row| {
            row.get(0)
        })
        .unwrap();
    assert_ne!(
        status, "completed",
        "a request the upstream never answered must not be recorded as completed"
    );
}

/// A request accepted by an instance that is then hard-killed is resolved by
/// crash recovery on the next start, not by the drain path.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn killed_instance_requests_are_recovered_not_left_in_flight() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    {
        let mut instance = Instance::start("a", dir.path(), &db_path, &upstream);
        assert!(instance.wait_ready(WAIT).await);

        mock.set_hang(true);

        let client = ProxyClient::new();
        let base = instance.base_url();
        let request = tokio::spawn(async move { client.chat(&base, "model-killed").await });

        tokio::time::sleep(Duration::from_millis(600)).await;

        // SIGKILL: no drain, no flush, no chance to resolve the record.
        instance.child.kill().unwrap();
        let _ = instance.child.wait();
        let _ = tokio::time::timeout(Duration::from_secs(10), request).await;
    }

    // The record is committed and stuck in_flight: nothing has resolved it.
    {
        let conn = Connection::open(&db_path).unwrap();
        let status: String = conn
            .query_row(
                "SELECT request_status FROM usage_records WHERE model = 'model-killed'",
                [],
                |row| row.get(0),
            )
            .expect("the accepted request must have been committed before the kill");
        assert_eq!(
            status, "in_flight",
            "the killed process cannot have resolved its own record"
        );
    }

    // Restart: recovery runs before the listener opens.
    mock.set_hang(false);
    let mut restarted = Instance::start("b", dir.path(), &db_path, &upstream);
    assert!(restarted.wait_ready(WAIT).await);

    let ledger = read_ledger(&db_path);
    assert_eq!(ledger.integrity, "ok");
    assert_eq!(
        ledger.in_flight, 0,
        "recovery must resolve stranded requests"
    );
    assert_eq!(
        ledger.request_ids.len(),
        1,
        "recovery must resolve the existing row, not insert a second one"
    );

    let conn = Connection::open(&db_path).unwrap();
    let status: String = conn
        .query_row(
            "SELECT request_status FROM usage_records WHERE model = 'model-killed'",
            [],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(
        status, "interrupted",
        "a request stranded by a crash is interrupted, not completed"
    );

    let _ = restarted.terminate_and_wait(WAIT).await;
}

/// The same request must never be recorded twice, even in the window where two
/// instances are writing the same database.
///
/// This runs the overlap window several times because a duplicate only appears
/// under a specific interleaving, and one run is not evidence that the
/// interleaving never happens.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn repeated_overlap_windows_never_duplicate_or_lose_a_record() {
    for run in 0..3 {
        let dir = tempfile::TempDir::new().unwrap();
        let db_path = dir.path().join("ledger.db");
        let (mock, upstream) = start_mock_upstream().await;

        let mut a = Instance::start("a", dir.path(), &db_path, &upstream);
        assert!(a.wait_ready(WAIT).await, "run {run}: A never became ready");

        let rotation = Rotation::new();
        rotation.add(a.base_url());
        let traffic = rotation.run(6, ProxyClient::new());

        // Start B while A is already busy, so the overlap begins under load.
        tokio::time::sleep(Duration::from_millis(150)).await;
        let mut b = Instance::start("b", dir.path(), &db_path, &upstream);
        assert!(b.wait_ready(WAIT).await, "run {run}: B never became ready");

        rotation.add(b.base_url());
        tokio::time::sleep(Duration::from_millis(400)).await;

        // Quiesce *before* signalling, so no request is in flight when either
        // instance starts shutting down. That is what makes the assertion below
        // sharp: with no in-flight request, every recorded row must be
        // `completed` with real usage, and a single `interrupted` row means
        // something marked a live request terminal while it was still running.
        //
        // Starting B is exactly when that can happen: B's startup recovery runs
        // while A is still serving, and if recovery is blind to A's liveness it
        // resolves A's in-flight rows to `interrupted` — after which A's own
        // finalize finds the row already terminal and quietly does nothing.
        // The client still got its 200 with usage; the ledger says interrupted
        // and the tokens are gone.
        rotation.stop();
        for handle in traffic {
            let _ = handle.await;
        }
        tokio::time::sleep(Duration::from_millis(200)).await;

        // Both instances are told to stop at nearly the same time, which is the
        // worst case for the ledger: two draining writers committing together.
        a.signal(libc::SIGTERM);
        b.signal(libc::SIGTERM);

        assert!(a.wait_exit(WAIT).await.expect("A did not exit").success());
        assert!(b.wait_exit(WAIT).await.expect("B did not exit").success());

        let ledger = read_ledger(&db_path);
        assert_ledger_sound(&ledger);
        assert_no_divergence(&mock, &ledger);
        assert!(
            ledger.models.len() > 20,
            "run {run}: only {} requests crossed the overlap window; too few to \
             exercise concurrent writers",
            ledger.models.len()
        );

        // No request was in flight at shutdown, and the upstream answered every
        // one of them with usage, so nothing may be recorded as failed or
        // interrupted, and no token count may be missing.
        let conn = Connection::open(&db_path).unwrap();
        let wrong: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_records \
                 WHERE request_status <> 'completed' OR input_tokens IS NULL \
                    OR output_tokens IS NULL OR usage_status <> 'available'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(
            wrong,
            0,
            "run {run}: {wrong} request(s) were served successfully but recorded as \
             something other than a completed request with usage. Statuses {:?}",
            status_counts(&db_path)
        );

        eprintln!(
            "overlap run {run}: {} requests, statuses {:?}",
            ledger.models.len(),
            status_counts(&db_path)
        );
    }
}

// ---------------------------------------------------------------------------
