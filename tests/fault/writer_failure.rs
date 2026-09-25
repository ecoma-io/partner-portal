//! A metering pipeline that cannot write at all.
//!
//! Fault injection: the raw table is renamed out from under the running process
//! from a second connection. That is the analogue of schema damage or a partial
//! restore — a failure that is not transient, so the writer's BUSY retry loop
//! cannot help. It is deliberately not something the product does to itself.
//!
//! The contract in that state is the opposite of "keep serving": a request the
//! ledger cannot account for must be refused, loudly, and the instance must take
//! itself out of rotation. Serving traffic that produces no usage record is the
//! failure this product exists to prevent, and it would be invisible.

use crate::common::{
    Behaviour, HttpResponse, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    chat_request, open_db, raw_rollup_totals, row_count, wait_for_terminal_count,
};
use http::{Method, StatusCode};

/// Rename the raw ledger table away, simulating unrecoverable schema damage.
fn break_schema(db_path: &std::path::Path) {
    open_db(db_path)
        .execute_batch("ALTER TABLE usage_records RENAME TO usage_records_offline")
        .expect("rename the raw usage table");
}

/// Put the table back, so the process can be observed recovering its writes
/// (but not, by design, its readiness).
fn repair_schema(db_path: &std::path::Path) {
    open_db(db_path)
        .execute_batch("ALTER TABLE usage_records_offline RENAME TO usage_records")
        .expect("restore the raw usage table");
}

/// Rows in the renamed table, for the window in which the ordinary readers
/// cannot run.
fn offline_row_count(db_path: &std::path::Path) -> i64 {
    open_db(db_path)
        .query_row("SELECT COUNT(*) FROM usage_records_offline", [], |r| {
            r.get(0)
        })
        .expect("count rows in the renamed table")
}

async fn chat(client: &TestClient, server: &TestServer) -> HttpResponse {
    client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_request("gpt-4o"),
            &[],
        )
        .await
}

#[tokio::test]
async fn a_permanent_writer_failure_refuses_requests_and_fails_readiness() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1,
        completion: 1,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    break_schema(&server.db_path);

    // A request that cannot be durably accepted is not forwarded. Refusing is
    // the only honest answer: the alternative is serving unmetered traffic.
    let refused = chat(&client, &server).await;
    assert_eq!(refused.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(refused.json()["error"]["type"], "metering_error");
    assert_eq!(
        refused.json()["error"]["message"],
        "Metering unavailable; request not forwarded"
    );
    assert_eq!(
        upstream.request_count(),
        0,
        "a request that cannot be recorded must not reach the upstream"
    );

    // Readiness reports the failure instead of silently pretending to work.
    let ready = client
        .get(&server.url("/readyz"), None)
        .await
        .expect("readyz");
    assert_eq!(ready.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(ready.json()["ready"], false);
    assert_eq!(ready.json()["ledger_ready"], false);
    assert_eq!(
        ready.json()["shutting_down"],
        false,
        "a metering failure is not a shutdown"
    );

    // Liveness stays up: the process degrades, it does not crash and it does not
    // exit into a restart loop it cannot win.
    let health = client
        .get(&server.url("/healthz"), None)
        .await
        .expect("healthz");
    assert_eq!(health.status, StatusCode::OK);

    // Still refusing, still not forwarding: no silent serving on a second try.
    let refused_again = chat(&client, &server).await;
    assert_eq!(refused_again.status, StatusCode::SERVICE_UNAVAILABLE);
    assert_eq!(upstream.request_count(), 0);
    assert_eq!(
        offline_row_count(&server.db_path),
        0,
        "a refused request must leave no record behind"
    );

    let logs = server.logs();
    assert!(
        logs.contains("Refusing request: could not durably record its acceptance"),
        "the refusal must be logged with its cause; log:\n{logs}"
    );
    assert!(
        logs.contains("commit failed permanently"),
        "a non-BUSY failure is permanent and must not be retried as if it were a lock; log:\n{logs}"
    );
    assert!(
        !logs.contains("blocked by SQLite lock"),
        "a missing table is not a lock, so the BUSY retry loop must not spin on it; log:\n{logs}"
    );

    // Repairing the schema lets the writer work again — the process is not
    // wedged — but readiness stays failed. That latch is deliberate: a process
    // that has lost a metering write has unaccounted traffic, and only a restart
    // (with recovery) resolves that.
    repair_schema(&server.db_path);

    let recovered = chat(&client, &server).await;
    assert_eq!(
        recovered.status,
        StatusCode::OK,
        "writes resume once the schema is usable again"
    );
    let rows = wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;
    assert_eq!(rows[0].request_status, "completed");
    assert_eq!(
        row_count(&server.open_db()),
        1,
        "only the request that was actually accepted may be recorded"
    );
    assert_eq!(raw_rollup_totals(&server.open_db()), (1, 1));

    let still_unready = client
        .get(&server.url("/readyz"), None)
        .await
        .expect("readyz");
    assert_eq!(
        still_unready.status,
        StatusCode::SERVICE_UNAVAILABLE,
        "readiness is not restored by a repair; only a restart resolves it"
    );
    assert_eq!(still_unready.json()["ledger_ready"], false);
}
