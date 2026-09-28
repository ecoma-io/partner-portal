//! A key table a second connection has damaged.
//!
//! Fault injection: `api_keys` is renamed out from under the running process,
//! the same way `writer_failure.rs` damages the ledger. What must happen next is
//! the **opposite** of that file's contract, and the contrast is the point:
//!
//! - a request the *ledger* cannot account for is refused, because serving
//!   unmetered traffic is the failure this product exists to prevent;
//! - a request whose *credential* cannot be re-read from the database is still
//!   served, because authentication does not read the database. It reads the
//!   in-memory snapshot, which holds the last key set that was known good.
//!
//! That asymmetry is the whole reason the snapshot exists, and this is its
//! end-to-end form: the unit tests can state the property, only a real process
//! can show that no per-request query crept into the request path. If
//! authentication ever grew one, every request below would fail against a table
//! that does not exist — while the ledger was healthy enough to meter them,
//! which is the shape of the bug worth catching.
//!
//! It is deliberately not something the product does to itself: a dropped table
//! is schema damage or a partial restore, and the honest response to *that* is a
//! loud log, which is also asserted below.

use std::time::Duration;

use crate::common::{
    Behaviour, ManagerSpec, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT, chat_request,
    wait_for_terminal_count,
};
use http::{Method, StatusCode};
use serde_json::json;

/// A manager password, so the admin surface can be probed while the table is
/// gone.
const MANAGER_PASSWORD: &str = "test-manager-password-for-damage";

/// Move `api_keys` aside, exactly as `writer_failure.rs` moves `usage_records`.
fn break_key_table(db_path: &std::path::Path) {
    crate::common::open_db(db_path)
        .execute_batch("ALTER TABLE api_keys RENAME TO api_keys_offline")
        .expect("rename the api key table");
}

#[tokio::test]
async fn a_damaged_key_table_stops_neither_authentication_nor_metering() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 3,
        completion: 2,
        cached: 0,
    })
    .await;
    let spec = Spec::new(&upstream).with_manager(ManagerSpec::new(MANAGER_PASSWORD));
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    let key = server.key().to_string();

    // The credential works before the damage, so a pass below cannot be the
    // fixture's doing.
    assert_eq!(
        client
            .get_json(&server.url("/api/me"), Some(&key))
            .await
            .status,
        StatusCode::OK
    );

    break_key_table(&server.db_path);

    // The load-bearing assertion: requests keep being authenticated, forwarded
    // and metered with no readable key table anywhere.
    for index in 0..3 {
        let response = client
            .call(
                Method::POST,
                &server.url("/v1/chat/completions"),
                Some(&key),
                chat_request("gpt-4o"),
                &[],
            )
            .await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "request {index} must authenticate from the snapshot, not from SQLite: {}",
            response.text()
        );
    }
    let rows = wait_for_terminal_count(&server.db_path, 3, WAIT_TIMEOUT).await;
    assert_eq!(rows.len(), 3);
    assert_eq!(upstream.request_count(), 3);

    // The instance says so rather than serving silently from a snapshot nobody
    // can confirm: a poll that cannot read the key set is reported.
    let mut warned = false;
    for _ in 0..240 {
        if server.logs().contains("api key snapshot reload failed") {
            warned = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        warned,
        "a reload that cannot read the key set must be logged; log:\n{}",
        server.logs()
    );

    // And the damage is real, so the assertions above are not vacuous. A key
    // that could still be *issued* would mean the table was never touched.
    let refused = client
        .post_json(
            &server.url("/api/admin/api-keys"),
            Some(MANAGER_PASSWORD),
            json!({"name": "cannot-be-issued", "consumer_id": "acme", "allowed_models": []}),
        )
        .await;
    assert_eq!(
        refused.status,
        StatusCode::INTERNAL_SERVER_ERROR,
        "the write path must fail against a table that is gone: {}",
        refused.text()
    );
    assert_eq!(refused.json()["error"]["code"], "internal_error");
    assert!(
        !refused.text().contains("api_keys"),
        "a refusal must not hand database internals to the client: {}",
        refused.text()
    );

    // The credential that was already known still works after all of that — the
    // snapshot is not consumed by a failed reload.
    assert_eq!(
        client
            .get_json(&server.url("/api/me"), Some(&key))
            .await
            .status,
        StatusCode::OK
    );
}
