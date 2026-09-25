//! Shutdown must finish, and must finish after the metering pipeline drains.
//!
//! The documented sequence is `SIGTERM -> readiness false -> stop accepting ->
//! drain active requests -> drain metering -> COMMIT pending -> close DB ->
//! exit`. The failure this file guards against is subtler than a missing drain:
//! if the *listener* never finishes stopping, the drain never runs at all —
//! the process sits until the orchestrator loses patience and SIGKILLs it, and
//! everything still queued is lost.
//!
//! A dashboard browser tab holds an open event stream for as long as the page
//! is open. That is a long-lived response body, and axum's graceful shutdown
//! waits for response bodies, not just for request handlers. So the case is not
//! hypothetical: one operator with the dashboard open is the normal state of
//! this product.

use crate::harness::*;

/// A graceful shutdown completes promptly even while a dashboard stream is open.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn shutdown_completes_while_a_dashboard_stream_is_open() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let mut instance = Instance::start("a", dir.path(), &db_path, &upstream);
    assert!(
        instance.wait_ready(WAIT).await,
        "instance never became ready"
    );

    let client = ProxyClient::new();
    let base = instance.base_url();

    // One real request, so there is something in the ledger to protect.
    assert_eq!(client.chat(&base, "before-shutdown").await, Ok(200));

    // An operator with the dashboard open: the event stream is held by a task
    // that reads it exactly as a browser would — slowly, and until it closes.
    let stream_task = {
        let client = client.clone();
        let base = base.clone();
        tokio::spawn(async move {
            use futures::StreamExt;
            let request = hyper::Request::builder()
                .method("GET")
                .uri(format!("{base}/api/dashboard/events"))
                .header("authorization", format!("Bearer {LOCAL_KEY}"))
                .body(
                    Full::new(Bytes::new())
                        .map_err(|never| match never {})
                        .boxed(),
                )
                .unwrap();

            let response = match client.client.request(request).await {
                Ok(response) => response,
                Err(_) => return,
            };
            let mut stream = response.into_body().into_data_stream();
            // Drain for as long as the server keeps the stream open.
            while let Some(Ok(_)) = stream.next().await {}
        })
    };

    // Give the subscription time to register, then confirm the stream is live by
    // generating a change.
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(client.chat(&base, "during-stream").await, Ok(200));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(
        !stream_task.is_finished(),
        "the event stream must still be open when shutdown starts"
    );

    // Shutdown with the stream held open.
    let started = std::time::Instant::now();
    let exit = tokio::time::timeout(WAIT, instance.terminate_and_wait(WAIT))
        .await
        .expect("the instance did not exit within the wait budget")
        .expect("the instance did not exit after SIGTERM");

    let elapsed = started.elapsed();
    eprintln!(
        "shutdown with an open stream took {} ms",
        elapsed.as_millis()
    );

    assert!(
        exit.success(),
        "the instance exited with a failure status: {exit:?}"
    );
    assert!(
        elapsed < Duration::from_secs(20),
        "a graceful shutdown took {} ms; an open event stream must not hold it open",
        elapsed.as_millis()
    );

    // The stream must end, not hang, once the server stops.
    let _ = tokio::time::timeout(Duration::from_secs(5), stream_task).await;

    // And the metering drain must still have happened: the listener stopping is
    // only useful if the records behind it are committed.
    let ledger = read_ledger(&db_path);
    assert_ledger_sound(&ledger);
    assert_no_divergence(&mock, &ledger);
    assert_eq!(
        ledger.models.len(),
        2,
        "both requests must be in the ledger after the drain: {:?}",
        ledger.models
    );
}

/// A shutdown with a queued backlog commits everything it accepted.
///
/// The listener is stopped before the metering drain, so the drain cannot be
/// skipped by a request arriving during shutdown. This checks the other
/// direction: nothing accepted before the signal is left behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn shutdown_commits_every_accepted_request() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let mut instance = Instance::start("a", dir.path(), &db_path, &upstream);
    assert!(
        instance.wait_ready(WAIT).await,
        "instance never became ready"
    );

    let client = ProxyClient::new();
    let base = instance.base_url();

    // A burst that is still arriving when the signal lands.
    let mut sent = 0usize;
    let mut tasks = Vec::new();
    for _ in 0..200 {
        let client = client.clone();
        let base = base.clone();
        tasks.push(tokio::spawn(async move {
            // A fixed ring: the strict per-key allow-list (ADR 0012) cannot
            // enumerate `burst-{i}`; this burst only counts acceptances.
            client.chat(&base, E2E_MODELS[0]).await
        }));
    }

    // Signal while the burst is in flight.
    tokio::time::sleep(Duration::from_millis(20)).await;
    instance.signal(libc::SIGTERM);

    for task in tasks {
        if let Ok(Ok(200)) = task.await {
            sent += 1;
        }
    }

    let exit = instance
        .wait_exit(WAIT)
        .await
        .expect("the instance did not exit after SIGTERM");
    assert!(exit.success(), "the instance exited with {exit:?}");

    let ledger = read_ledger(&db_path);
    assert_ledger_sound(&ledger);

    eprintln!(
        "shutdown drain: {} requests accepted, {} ledger rows, statuses {:?}",
        sent,
        ledger.models.len(),
        status_counts(&db_path)
    );

    // Every request that got a 200 was accepted; every accepted request has a
    // row. The upstream is the witness for "accepted and forwarded".
    assert_no_divergence(&mock, &ledger);
    assert!(
        sent > 0,
        "the burst must have produced at least one accepted request"
    );
}

/// An idle server keeps serving: nothing bounds it until something asks it to
/// stop.
///
/// This is the regression test for a drain bound applied to the wrong phase.
/// `tokio::time::timeout(drain_bound, serve)` reads as the natural way to say
/// "shutdown gets thirty seconds", but it arms at startup, so the process
/// abandoned its (nonexistent) in-flight requests and exited with status 0
/// thirty seconds after it began listening — with no signal sent, and a
/// reassuring "drain bound" line in the log. Under `restart: unless-stopped`
/// that is a crash loop whose period is the drain bound.
///
/// Every other test in this suite finishes inside the bound, which is exactly
/// why the bug survived a green suite: only *not* exiting can distinguish a
/// serving server from a crashed one, and that takes real time. The clock here
/// is deliberately longer than the bound (30 s) plus a margin for a slow
/// machine; the cost is paid once, in the one test that can see it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_server_does_not_exit_on_its_own() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    let mut instance = Instance::start("a", dir.path(), &db_path, &upstream);
    assert!(
        instance.wait_ready(WAIT).await,
        "instance never became ready"
    );

    // Longer than `MIN_DRAIN_BOUND`. If the bound is armed at startup, the
    // process is gone well before this returns.
    tokio::time::sleep(Duration::from_secs(35)).await;

    assert!(
        instance
            .wait_exit(Duration::from_millis(50))
            .await
            .is_none(),
        "the server exited on its own with nothing in flight"
    );

    // And it is still *working*, not merely alive: an exit that left the
    // listener dead but the process running would be just as broken.
    let client = ProxyClient::new();
    assert_eq!(
        client.chat(&instance.base_url(), "gpt-4o").await,
        Ok(200),
        "the instance must still be proxying after the drain bound has passed"
    );

    // The signal that the bound was waiting for still ends it cleanly.
    let status = instance.terminate_and_wait(WAIT).await;
    assert_eq!(
        status.and_then(|s| s.code()),
        Some(0),
        "a graceful shutdown after the bound must still exit 0"
    );

    drop(mock);
}
