//! Realtime invalidation for writes made by another process.

use crate::harness::*;

// ---------------------------------------------------------------------------

/// The dashboard must notice a change written by **another process**.
///
/// This is the requirement that makes SSE safe to treat as an optimisation
/// rather than a source of truth: the invalidation is derived from the database
/// itself (`PRAGMA data_version`), so an instance whose traffic is served
/// entirely by the other container still refreshes its view. An in-memory event
/// bus would pass a single-process test and fail here.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_instance_notifies_about_writes_made_by_another_instance() {
    const KEY: &str = "local-test-key";

    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (mock, upstream) = start_mock_upstream().await;

    // A serves the traffic; B only serves the dashboard.
    let a = Instance::start("a", dir.path(), &db_path, &upstream);
    let b = Instance::start("b", dir.path(), &db_path, &upstream);
    assert!(a.wait_ready(WAIT).await, "A never became ready");
    assert!(b.wait_ready(WAIT).await, "B never became ready");

    let client = ProxyClient::new();

    // Open the stream against B before any traffic exists for B to notice.
    let reader = {
        let client = client.clone();
        let base = b.base_url();
        tokio::spawn(async move {
            client
                .read_sse_until(
                    &base,
                    "/api/dashboard/events",
                    KEY,
                    "data_changed",
                    Duration::from_secs(15),
                )
                .await
        })
    };

    // Let the subscription register before generating the change.
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Traffic goes to A only. B never sees this request directly.
    assert_eq!(
        client.chat(&a.base_url(), "sse-cross-instance").await,
        Ok(200)
    );

    let outcome = tokio::time::timeout(Duration::from_secs(20), reader)
        .await
        .expect("the SSE reader never finished")
        .expect("the SSE reader task panicked");

    let (text, elapsed) = outcome.expect("instance B never reported the write made by instance A");

    eprintln!(
        "cross-instance SSE invalidation arrived after {} ms",
        elapsed.as_millis()
    );
    assert!(
        text.contains(r#""type":"connected""#),
        "the stream must announce itself before sending changes: {text:?}"
    );

    // The stream is a notification, not a data channel: it must not carry any
    // number a UI could render. Every payload is exactly one of the two known
    // shapes.
    for line in text.lines().filter(|l| l.starts_with("data:")) {
        let payload = line.trim_start_matches("data:").trim();
        assert!(
            payload == r#"{"type":"connected"}"# || payload == r#"{"type":"data_changed"}"#,
            "the SSE payload must carry no usage data, found {payload:?}"
        );
    }

    // Both instances are still healthy and agree on the data.
    let (status, body) = client
        .get(&a.base_url(), "/api/dashboard/summary", Some(KEY))
        .await;
    assert_eq!(status, 200);
    let summary: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(summary["total_requests"], 1);

    let (status, body) = client
        .get(&b.base_url(), "/api/dashboard/summary", Some(KEY))
        .await;
    assert_eq!(status, 200);
    let summary: Value = serde_json::from_str(&body).unwrap();
    assert_eq!(
        summary["total_requests"], 1,
        "both instances must read the same ledger"
    );

    drop(a);
    drop(b);
    let _ = mock.models_seen();
}
