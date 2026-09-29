//! The operator's commercial surface: opening an account, pricing it, and
//! settling its statements.
//!
//! The real binary runs as a child process and the ledger is read back from
//! SQLite, because the claims worth pinning here are the ones a `200` cannot
//! show on its own:
//!
//! - a **rejected price list changes nothing**. The response is a 400, and the
//!   interesting half is that the *previous* list is still the stored one — a
//!   replacement that validated each entry and then applied them one at a time
//!   would leave a partner half-priced, which is a partner whose invoice
//!   disagrees with their contract and with itself.
//! - **a statement is priced and cannot be paid twice**, and one that carries no
//!   payment obligation refuses the payment with 409 rather than recording money
//!   that changed hands for a bill that was never owed.
//! - **a delete is refused once there is billing history.** Statements are
//!   financial records; a partner that has been invoiced is not a row an
//!   operator may drop.
//! - the whole surface is **`ManagerOnly`**, so a partner key is 403 here and
//!   never 401 — it is authenticated, just not an operator.
//!
//! # Why the statements are written by the harness
//!
//! Same reason as `tests/integration/billing.rs`: a day closes once, about a
//! second after the process starts, so usage for a day the scheduler will close
//! has to be on disk before the process exists. What the *request path* writes is
//! covered there; what is not is what an operator does to a statement afterwards.

use crate::common::{
    Behaviour, HttpResponse, KeySpec, ManagerSpec, MockUpstream, Spec, TestClient, TestServer,
    WAIT_TIMEOUT, open_db,
};
use bytes::Bytes;
use http::{Method, StatusCode};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::path::Path;

const MANAGER_PASSWORD: &str = "sk-admin-manager-do-not-guess";
const PARTNER: &str = "acme";

/// The price the harness meters at, in micro-dollars per million tokens, and the
/// tokens one request costs, so a statement's total is an exact number rather
/// than something a reader has to trust.
const INPUT_PER_MILLION: i64 = 2_500_000;
const OUTPUT_PER_MILLION: i64 = 10_000_000;
const INPUT_TOKENS: i64 = 1_000;
const OUTPUT_TOKENS: i64 = 2_000;

/// `1000 × 2_500_000 / 10⁶` = 2500 and `2000 × 10_000_000 / 10⁶` = 20000.
const DAY_TOTAL: i64 = 22_500;

/// Yesterday, the way the server computes it: a day only closes once it has
/// ended.
fn yesterday() -> String {
    let today = time::OffsetDateTime::now_utc().date();
    (today - time::Duration::days(1))
        .to_string()
        .split('T')
        .next()
        .expect("a date has no time part")
        .to_string()
}

/// One request's worth of usage on the day under test, with the price snapshots
/// the metering write would have left on it.
fn write_usage(conn: &Connection, consumer: &str, at: &str) {
    conn.execute(
        "INSERT INTO usage_records (request_id, created_at, consumer_id, model, endpoint,
            streaming, http_status, request_status, usage_status, input_tokens, output_tokens,
            cached_tokens, ttft_ms, duration_ms, input_price_snapshot,
            cached_input_price_snapshot, output_price_snapshot)
         VALUES (?1, ?2, ?3, 'gpt-4o', 'chat_completions', 0, 200, 'completed', 'available',
                 ?4, ?5, 0, 10, 100, ?6, ?7, ?8)",
        rusqlite::params![
            format!("admin-seed-{consumer}-{at}"),
            at,
            consumer,
            INPUT_TOKENS,
            OUTPUT_TOKENS,
            INPUT_PER_MILLION,
            INPUT_PER_MILLION,
            OUTPUT_PER_MILLION,
        ],
    )
    .expect("seed a usage row onto a closed day");
}

/// A day of usage for each consumer named, on invoice terms.
fn seed_a_day(consumers: &[&str]) -> impl Fn(&Path) + Send + Sync + 'static {
    let consumers: Vec<String> = consumers.iter().map(|c| c.to_string()).collect();
    move |path: &Path| {
        let conn = open_db(path);
        let at = format!("{}T12:00:00.000000000Z", yesterday());
        for consumer in &consumers {
            write_usage(&conn, consumer, &at);
        }
    }
}

/// One server: the named consumers on invoice terms unless a spec says
/// otherwise, a manager over them, and a day of usage waiting to be closed.
async fn admin_server(upstream: &MockUpstream, keys: Vec<KeySpec>) -> TestServer {
    let consumers: Vec<String> = keys
        .iter()
        .map(|k| k.effective_consumer_id().to_string())
        .collect();
    let borrowed: Vec<&str> = consumers.iter().map(String::as_str).collect();
    TestServer::start(
        Spec::new(upstream)
            .with_keys(keys)
            .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
            .starting_ledger(seed_a_day(&borrowed)),
    )
    .await
}

/// The standard server: `acme` on invoice terms with a day of usage, and a
/// second partner with none, for the operations that do not need money.
async fn server_with_partners(upstream: &MockUpstream) -> TestServer {
    admin_server(
        upstream,
        vec![
            KeySpec::new("primary").with_consumer(PARTNER),
            KeySpec::new("empty").with_consumer("beta"),
        ],
    )
    .await
}

fn key_for<'a>(server: &'a TestServer, consumer: &str) -> &'a str {
    let index = server
        .seeded
        .iter()
        .position(|key| key.consumer_id == consumer)
        .expect("the spec seeds a key for this consumer");
    server.key_at(index)
}

/// Call a manager route, with the manager credential.
async fn call(
    client: &TestClient,
    server: &TestServer,
    method: Method,
    path: &str,
    body: Option<Value>,
) -> HttpResponse {
    let payload = body.map(|value| Bytes::from(value.to_string()));
    client
        .call(
            method,
            &server.url(path),
            Some(MANAGER_PASSWORD),
            payload.unwrap_or_default(),
            &[],
        )
        .await
}

/// The stored price list, read from SQLite rather than from the API.
///
/// The API's list is what the operator was shown; the table is what the request
/// path will actually meter at. A replacement that answered 200 while writing
/// something else would pass every assertion made against the response and fail
/// the first partner's invoice.
fn stored_prices(conn: &Connection, consumer: &str) -> Vec<(String, i64, i64)> {
    let mut stmt = conn
        .prepare(
            "SELECT model, input_price_micro_usd_per_million, \
                    output_price_micro_usd_per_million \
             FROM partner_models WHERE consumer_id = ?1 ORDER BY model",
        )
        .expect("prepare the price query");
    stmt.query_map([consumer], |row| {
        Ok((row.get(0)?, row.get(1)?, row.get(2)?))
    })
    .expect("run the price query")
    .collect::<Result<Vec<_>, _>>()
    .expect("read the stored prices")
}

/// Wait for the statement for the day under test, then return it with its lines.
///
/// A day the scheduler has not closed yet is normal, not a failure, so this
/// polls — and it waits for that *particular* day, because the harness anchors
/// each partner two days back and a first-row-wins assertion would pass on the
/// fixture's own empty anchor. `consumer` is not filtered on: the list is the
/// manager's, and narrowing it to one partner would hide exactly the two
/// contracts this file compares.
async fn await_statement(server: &TestServer, consumer: &str) -> Value {
    let client = TestClient::new();
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let response = client
            .get(
                &server.url("/api/admin/billing/statements"),
                Some(MANAGER_PASSWORD),
            )
            .await
            .expect("read the statement list");
        if let Some(found) = response.json()["statements"].as_array().and_then(|rows| {
            rows.iter()
                .find(|row| row["billing_date"] == yesterday() && row["consumer_id"] == consumer)
        }) {
            let id = found["id"].as_i64().expect("a statement id");
            return client
                .get(
                    &server.url(&format!("/api/admin/billing/statements/{id}")),
                    Some(MANAGER_PASSWORD),
                )
                .await
                .expect("read the statement")
                .json();
        }
        assert!(
            std::time::Instant::now() < deadline,
            "no statement for {} was issued within {WAIT_TIMEOUT:?}",
            yesterday()
        );
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
    }
}

/// A partner is opened with the prices it will be billed at, and a create that
/// collides with an existing one is a 409 rather than a silent reset.
#[tokio::test]
async fn a_partner_is_opened_with_its_prices_and_a_second_create_is_refused() {
    let mut upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = admin_server(
        &upstream,
        vec![
            // `beta` exists with no key, so the create below is a real creation
            // rather than a collision with the seeded fixture.
            KeySpec::new("primary").with_consumer(PARTNER),
        ],
    )
    .await;
    let client = TestClient::new();

    // A partner with no key: seeded keys are one way in, not the only one. An
    // operator can open an account before the partner has a credential, and the
    // price list is what the account exists for.
    let created = call(
        &client,
        &server,
        Method::POST,
        "/api/admin/partners",
        Some(json!({
            "consumer_id": "beta",
            "name": "Beta Corp",
            "billing_email": "accounts@beta.example",
            "billing_mode": "invoice",
            "payment_terms_minutes": 1440,
            "models": [{
                "model": "gpt-4o",
                "input_per_million": "0.095",
                "cached_input_per_million": "0.002375",
                "output_per_million": "0.475"
            }]
        })),
    )
    .await;
    assert_eq!(created.status, StatusCode::CREATED, "{}", created.text());
    let body = created.json();
    assert_eq!(body["consumer_id"], "beta");
    assert_eq!(body["name"], "Beta Corp");
    assert_eq!(body["billing_mode"], "invoice");
    assert_eq!(body["payment_terms_minutes"], 1440);
    assert_eq!(
        body["emails_statements"], true,
        "an address plus an obligation"
    );
    assert_eq!(
        body["total_billed"], "$0.000000",
        "a partner with no statement has billed nothing"
    );

    // The prices came back as the operator typed them, not as a float's
    // approximation of them. `$0.002375` has seven decimal places; a JSON number
    // could not carry it exactly, which is why the wire is a string.
    assert_eq!(body["models"][0]["model"], "gpt-4o");
    assert_eq!(body["models"][0]["input_per_million"], "0.095");
    assert_eq!(body["models"][0]["cached_input_per_million"], "0.002375");
    assert_eq!(body["models"][0]["output_per_million"], "0.475");

    // And they are the stored ones, in micro-dollars: `0.095` is 95 000.
    let conn = server.open_db();
    assert_eq!(
        stored_prices(&conn, "beta"),
        vec![("gpt-4o".to_string(), 95_000, 475_000)],
        "the table, not the response, is what the request path meters at"
    );

    // Terms are optional and default to the product default rather than to
    // zero, because zero terms mean an invoice is overdue the moment it is
    // issued — a configuration nobody would write on purpose and a partner
    // would be suspended for.
    let defaulted = call(
        &client,
        &server,
        Method::POST,
        "/api/admin/partners",
        Some(json!({
            "consumer_id": "gamma",
            "name": "Gamma",
            "billing_mode": "reconciliation"
        })),
    )
    .await;
    assert_eq!(
        defaulted.status,
        StatusCode::CREATED,
        "{}",
        defaulted.text()
    );
    assert_eq!(
        defaulted.json()["payment_terms_minutes"],
        720,
        "the default is 12 hours, and it is stated rather than implied"
    );
    assert_eq!(
        defaulted.json()["emails_statements"],
        false,
        "a reconciliation statement owes nothing, so there is nothing to mail"
    );

    // A second create against an existing partner. A create that quietly reset
    // the billing mode or the terms is how a contract changes by accident.
    let again = call(
        &client,
        &server,
        Method::POST,
        "/api/admin/partners",
        Some(json!({
            "consumer_id": "beta",
            "name": "Beta Renamed By Mistake",
            "billing_mode": "reconciliation"
        })),
    )
    .await;
    assert_eq!(again.status, StatusCode::CONFLICT);
    assert_eq!(again.json()["error"]["code"], "partner_exists");
    assert_eq!(
        again.json()["error"]["message"]
            .as_str()
            .unwrap_or_default(),
        format!(
            "a partner with consumer_id beta already exists; use \
             PATCH /api/admin/partners/beta to change it"
        ),
        "the refusal names the call that does the job instead"
    );

    let conn = server.open_db();
    let (name, mode): (String, String) = conn
        .query_row(
            "SELECT name, billing_mode FROM partners WHERE consumer_id = 'beta'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?)),
        )
        .expect("read the partner back");
    assert_eq!(name, "Beta Corp", "the refused create changed nothing");
    assert_eq!(mode, "invoice");

    upstream.stop().await;
}

/// A replacement price list is applied whole or not at all, and the two halves
/// of that claim are separate assertions on purpose.
#[tokio::test]
async fn a_rejected_price_list_leaves_the_previous_one_in_place() {
    let mut upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = server_with_partners(&upstream).await;
    let client = TestClient::new();
    let path = format!("/api/admin/partners/{PARTNER}/models");

    // Start from a list that is not the seeded one, so "unchanged" is a claim
    // about what the operator last set rather than about the fixture.
    let narrowed = call(
        &client,
        &server,
        Method::PUT,
        &path,
        Some(json!({ "models": [
            { "model": "gpt-4o", "input_per_million": "1.00",
              "cached_input_per_million": "0.10", "output_per_million": "4.00" }
        ]})),
    )
    .await;
    assert_eq!(narrowed.status, StatusCode::OK, "{}", narrowed.text());
    let conn = server.open_db();
    assert_eq!(
        stored_prices(&conn, PARTNER),
        vec![("gpt-4o".to_string(), 1_000_000, 4_000_000)],
        "a whole list replaced: `1.00` and `4.00` per million are integers"
    );

    // Each of these is refused, and after each one the stored list must still be
    // the one above. The bug this pins is a replacement that validated each
    // entry and then applied them in a loop, leaving a partner half-priced.
    let rejected: Vec<(&str, Value, &str)> = vec![
        (
            "an unparseable price",
            json!({ "models": [
                { "model": "gpt-4o-mini", "input_per_million": "free",
                  "cached_input_per_million": "0.10", "output_per_million": "4.00" }
            ]}),
            "input_per_million",
        ),
        (
            "a model listed twice",
            json!({ "models": [
                { "model": "gpt-4o", "input_per_million": "2.00",
                  "cached_input_per_million": "0.10", "output_per_million": "4.00" },
                { "model": "gpt-4o", "input_per_million": "3.00",
                  "cached_input_per_million": "0.10", "output_per_million": "4.00" }
            ]}),
            "listed twice",
        ),
        (
            "a blank model name",
            json!({ "models": [
                { "model": "   ", "input_per_million": "1.00",
                  "cached_input_per_million": "0.10", "output_per_million": "4.00" }
            ]}),
            "must not be blank",
        ),
    ];

    for (what, body, expected) in rejected {
        let response = call(&client, &server, Method::PUT, &path, Some(body.clone())).await;
        assert_eq!(
            response.status,
            StatusCode::BAD_REQUEST,
            "{what}: {}",
            response.text()
        );
        assert_eq!(
            response.json()["error"]["code"],
            "invalid_request",
            "{what}"
        );
        let message = response.json()["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .to_string();
        assert!(
            message.contains(expected),
            "{what}: the refusal must name what is wrong, got {message:?}"
        );

        let conn = server.open_db();
        assert_eq!(
            stored_prices(&conn, PARTNER),
            vec![("gpt-4o".to_string(), 1_000_000, 4_000_000)],
            "{what}: a refused replacement must leave the whole previous list in place"
        );
    }

    upstream.stop().await;
}

/// An empty list is not the same as a broken one: it is how an operator says
/// "this partner may call nothing", and it must be applied, not refused.
#[tokio::test]
async fn an_empty_price_list_is_applied_and_stops_the_partner_calling_anything() {
    let mut upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = server_with_partners(&upstream).await;
    let client = TestClient::new();
    let key = key_for(&server, PARTNER).to_string();
    let path = format!("/api/admin/partners/{PARTNER}/models");

    let emptied = call(
        &client,
        &server,
        Method::PUT,
        &path,
        Some(json!({ "models": [] })),
    )
    .await;
    assert_eq!(emptied.status, StatusCode::OK, "{}", emptied.text());
    assert_eq!(
        emptied.json().as_array().map(Vec::len),
        Some(0),
        "the response echoes the list it set, not the one before"
    );

    let conn = server.open_db();
    assert!(
        stored_prices(&conn, PARTNER).is_empty(),
        "an empty list is a configuration, and it is stored as one"
    );

    // The consequence: the partner can no longer call anything, and the refusal
    // happens before the upstream and before the ledger. The ledger count is
    // taken *before* the request rather than compared to a constant, so the
    // claim is "this request added nothing" and not "the fixture happens to have
    // one row" — the harness seeds one row per partner.
    let before = upstream.request_count();
    let rows_before: i64 = {
        let conn = server.open_db();
        conn.query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
            .expect("count the ledger")
    };
    let response = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(&key),
            Bytes::from(TestClient::chat_body("gpt-4o", false).to_string()),
            &[],
        )
        .await;
    assert_eq!(
        response.status,
        StatusCode::NOT_FOUND,
        "{}",
        response.text()
    );
    assert_eq!(response.json()["error"]["code"], "model_not_found");
    assert_eq!(
        upstream.request_count(),
        before,
        "an unpriced model is refused before the upstream is contacted"
    );

    let conn = server.open_db();
    let rows: i64 = conn
        .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
        .expect("count the ledger");
    assert_eq!(
        rows, rows_before,
        "a refused model minted no ledger row: {rows_before} before, {rows} after"
    );

    upstream.stop().await;
}

/// The payment path: a settlement record refuses a payment, an obligation takes
/// exactly one, and a repeat is answered rather than refused.
#[tokio::test]
async fn a_payment_is_recorded_once_and_only_on_an_obligation() {
    let mut upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = admin_server(
        &upstream,
        vec![
            KeySpec::new("primary").with_consumer(PARTNER),
            // Reconciliation owes nothing, so its statement must refuse a
            // payment outright — the two statements below are the two contracts.
            KeySpec::new("settled")
                .with_consumer("beta")
                .on_reconciliation(),
        ],
    )
    .await;
    let client = TestClient::new();

    let invoice = await_statement(&server, PARTNER).await;
    assert_eq!(invoice["billing_mode"], "invoice");
    assert_eq!(
        invoice["total_amount_micro_usd"], DAY_TOTAL,
        "priced from the snapshot on the usage row"
    );
    assert_eq!(invoice["paid_at"], Value::Null);
    assert_eq!(invoice["outstanding"], true);

    // The settlement record, priced the same way, and payable by nobody.
    let settlement = await_statement(&server, "beta").await;
    assert_eq!(settlement["billing_mode"], "reconciliation");
    let beta_id = settlement["id"].as_i64().expect("beta's statement id");
    assert_eq!(
        settlement["total_amount_micro_usd"], DAY_TOTAL,
        "a settlement record is priced exactly as an invoice is; it simply owes nothing"
    );
    assert_eq!(
        settlement["due_at"],
        Value::Null,
        "a statement with no obligation can have no deadline, in any configuration"
    );

    let refused = call(
        &client,
        &server,
        Method::POST,
        &format!("/api/admin/billing/statements/{beta_id}/mark-paid"),
        Some(json!({ "paid_by": "someone" })),
    )
    .await;
    assert_eq!(
        refused.status,
        StatusCode::CONFLICT,
        "a statement that owes nothing cannot have a payment recorded against it"
    );
    assert_eq!(refused.json()["error"]["code"], "statement_not_payable");
    assert!(
        refused.json()["error"]["message"]
            .as_str()
            .unwrap_or_default()
            .contains("carries no payment obligation"),
        "the refusal says why, not just that: {}",
        refused.text()
    );

    // Nothing was written, which the table says and the response cannot.
    let conn = server.open_db();
    let still_unpaid: Option<String> = conn
        .query_row(
            "SELECT paid_at FROM daily_statements WHERE id = ?1",
            [beta_id],
            |r| r.get(0),
        )
        .expect("read the settlement record");
    assert!(
        still_unpaid.is_none(),
        "a refused payment must not write a timestamp"
    );

    // The obligation, then. One call records it, with the reference the
    // operator's own books would carry.
    let id = invoice["id"].as_i64().expect("the invoice id");
    let paid = call(
        &client,
        &server,
        Method::POST,
        &format!("/api/admin/billing/statements/{id}/mark-paid"),
        Some(json!({
            "paid_by": "accounts",
            "reference": "wire-2026-03-02-0001",
            "note": "net 12"
        })),
    )
    .await;
    assert_eq!(paid.status, StatusCode::OK, "{}", paid.text());
    let after = paid.json();
    assert!(
        after["paid_at"].is_string(),
        "a payment carries a time: {after}"
    );
    assert_eq!(after["paid_by"], "accounts");
    assert_eq!(after["payment_reference"], "wire-2026-03-02-0001");
    assert_eq!(
        after["outstanding"], false,
        "a paid statement is not outstanding"
    );

    // The second call is answered, not refused: the caller's intent is already
    // satisfied, and a 409 would make an operator wonder whether the first one
    // worked. What it must not do is *reattribute* the payment.
    let again = call(
        &client,
        &server,
        Method::POST,
        &format!("/api/admin/billing/statements/{id}/mark-paid"),
        Some(json!({ "paid_by": "someone-else" })),
    )
    .await;
    assert_eq!(again.status, StatusCode::OK, "{}", again.text());
    assert_eq!(
        again.json()["paid_at"],
        after["paid_at"],
        "the payment time is the first one, not a second one"
    );
    assert_eq!(
        again.json()["paid_by"],
        "accounts",
        "a second call cannot reattribute a payment to someone else"
    );
    assert_eq!(
        again.json()["payment_reference"],
        after["payment_reference"],
        "nor replace the reference the books were matched on"
    );

    // A statement that does not exist is a 404, so a mistyped id is
    // distinguishable from a settlement record that cannot take a payment.
    let missing = call(
        &client,
        &server,
        Method::POST,
        "/api/admin/billing/statements/999999/mark-paid",
        Some(json!({ "paid_by": "accounts" })),
    )
    .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    assert_eq!(missing.json()["error"]["code"], "not_found");

    upstream.stop().await;
}

/// A partner is a financial record once it has been invoiced, and an operator
/// may not delete one.
#[tokio::test]
async fn a_partner_with_billing_history_cannot_be_deleted() {
    let mut upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = server_with_partners(&upstream).await;
    let client = TestClient::new();
    let acme = await_statement(&server, PARTNER).await["id"]
        .as_i64()
        .expect("acme's statement id");

    // A partner that never called anything has no statement and is deletable.
    // That is the legitimate case: an account opened by mistake, or one whose
    // price list was set before any traffic. The scheduler issues a statement
    // for every partner every day, so this is only reachable for a partner
    // created after the last close — which is why it is created here.
    let opened = call(
        &client,
        &server,
        Method::POST,
        "/api/admin/partners",
        Some(json!({
            "consumer_id": "gamma",
            "name": "Gamma",
            "billing_mode": "invoice"
        })),
    )
    .await;
    assert_eq!(opened.status, StatusCode::CREATED, "{}", opened.text());

    let deleted = call(
        &client,
        &server,
        Method::DELETE,
        "/api/admin/partners/gamma",
        None,
    )
    .await;
    assert_eq!(deleted.status, StatusCode::OK, "{}", deleted.text());
    assert_eq!(deleted.json()["deleted"], true);
    let conn = server.open_db();
    let gone: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partners WHERE consumer_id = 'gamma'",
            [],
            |r| r.get(0),
        )
        .expect("count partners");
    assert_eq!(gone, 0, "the row is gone, not merely hidden");
    let prices: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partner_models WHERE consumer_id = 'gamma'",
            [],
            |r| r.get(0),
        )
        .expect("count prices");
    assert_eq!(prices, 0, "and its prices went with it, not left orphaned");

    // And a partner with a statement is not. The count is in the message
    // because an operator who typed a wrong id needs to know whether the
    // refusal is about history or about the id.
    let refused = call(
        &client,
        &server,
        Method::DELETE,
        &format!("/api/admin/partners/{PARTNER}"),
        None,
    )
    .await;
    assert_eq!(
        refused.status,
        StatusCode::BAD_REQUEST,
        "{}",
        refused.text()
    );
    let message = refused.json()["error"]["message"]
        .as_str()
        .unwrap_or_default()
        .to_string();
    assert!(
        message.contains("billing history is a financial record"),
        "the refusal says what the statement is: {message:?}"
    );

    let conn = server.open_db();
    let still_there: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partners WHERE consumer_id = ?1",
            [PARTNER],
            |r| r.get(0),
        )
        .expect("count partners");
    assert_eq!(still_there, 1, "a refused delete changed nothing");
    let statement: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM daily_statements WHERE id = ?1",
            [acme],
            |r| r.get(0),
        )
        .expect("count the statement");
    assert_eq!(
        statement, 1,
        "and the statement it refused to orphan is intact"
    );

    // A partner that does not exist is a 404, so a typo is told from a refusal.
    let missing = call(
        &client,
        &server,
        Method::DELETE,
        "/api/admin/partners/nobody",
        None,
    )
    .await;
    assert_eq!(missing.status, StatusCode::NOT_FOUND);
    assert_eq!(missing.json()["error"]["code"], "not_found");

    upstream.stop().await;
}

/// Every commercial route is `ManagerOnly`: no credential is 401, a partner key
/// is 403, and the two are never confused.
#[tokio::test]
async fn the_commercial_surface_is_manager_only() {
    let mut upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = server_with_partners(&upstream).await;
    let client = TestClient::new();
    let partner_key = key_for(&server, PARTNER).to_string();

    let models_path = format!("/api/admin/partners/{PARTNER}/models");
    let routes: Vec<(&str, Method, Option<Value>)> = vec![
        ("/api/admin/partners", Method::GET, None),
        ("/api/admin/billing/summary", Method::GET, None),
        ("/api/admin/billing/statements", Method::GET, None),
        (models_path.as_str(), Method::GET, None),
        (
            "/api/admin/partners",
            Method::POST,
            Some(json!({ "consumer_id": "x", "name": "x", "billing_mode": "invoice" })),
        ),
        (
            models_path.as_str(),
            Method::PUT,
            Some(json!({ "models": [] })),
        ),
        (
            "/api/admin/billing/statements/1/mark-paid",
            Method::POST,
            Some(json!({ "paid_by": "someone" })),
        ),
    ];

    for (path, method, body) in routes {
        let call_without = client
            .call(
                method.clone(),
                &server.url(path),
                None,
                body.clone()
                    .map(|v| Bytes::from(v.to_string()))
                    .unwrap_or_default(),
                &[],
            )
            .await;
        assert_eq!(
            call_without.status,
            StatusCode::UNAUTHORIZED,
            "{method} {path} with no credential"
        );

        let call_with_partner = client
            .call(
                method.clone(),
                &server.url(path),
                Some(&partner_key),
                body.map(|v| Bytes::from(v.to_string())).unwrap_or_default(),
                &[],
            )
            .await;
        assert_eq!(
            call_with_partner.status,
            StatusCode::FORBIDDEN,
            "{method} {path} with a partner key: {}",
            call_with_partner.text()
        );
        assert_eq!(
            call_with_partner.json()["error"]["code"],
            "manager_required",
            "{method} {path} must say which credential it wants"
        );
    }

    // And nothing above was applied. A 403 that had created a partner anyway
    // would be the worst shape of this bug, so the table is asked.
    let conn = server.open_db();
    let strangers: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM partners WHERE consumer_id = 'x'",
            [],
            |r| r.get(0),
        )
        .expect("count partners");
    assert_eq!(strangers, 0, "a refused create created nothing");

    upstream.stop().await;
}

/// The operator's own view of what is owed, read across the whole scope.
#[tokio::test]
async fn the_summary_reports_what_is_owed_and_never_owes_a_settlement_record() {
    let mut upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = admin_server(
        &upstream,
        vec![
            KeySpec::new("primary").with_consumer(PARTNER),
            KeySpec::new("settled")
                .with_consumer("beta")
                .on_reconciliation(),
        ],
    )
    .await;
    let client = TestClient::new();
    await_statement(&server, PARTNER).await;
    await_statement(&server, "beta").await;

    let response = client
        .get(
            &server.url("/api/admin/billing/summary"),
            Some(MANAGER_PASSWORD),
        )
        .await
        .expect("read the summary");
    assert_eq!(response.status, StatusCode::OK, "{}", response.text());
    let summary = response.json();

    // Both partners have a statement, and both statements are priced.
    assert_eq!(summary["partners"], 2);
    assert!(
        summary["statements"].as_i64().expect("a count") >= 2,
        "each seeded partner has at least its own day: {}",
        summary
    );

    // The total is exactly one day's bill. Not both partners' and not the
    // anchors': a settlement record owes nothing, and the zero-amount anchor the
    // harness writes costs nothing, so the only money in the system is acme's
    // one day. Counting either of the other two would be a total that does not
    // match the sum of the statements behind it.
    assert_eq!(
        summary["unpaid_total_micro_usd"], DAY_TOTAL,
        "the reconciliation statement owes nothing and the anchor costs nothing: {summary}"
    );
    assert_eq!(summary["unpaid_total"], "$0.022500");
    assert_eq!(
        summary["suspended_partners"], 0,
        "nothing is past its date yet, so nobody is suspended: {summary}"
    );

    // And the count agrees with the rows, computed a second way from the full
    // list rather than pinned to a number this file would have to keep updating
    // as the fixture changes. What is being pinned is the *rule*: unpaid means
    // invoice, unpaid and not yet paid.
    let all = client
        .get(
            &server.url("/api/admin/billing/statements?limit=200"),
            Some(MANAGER_PASSWORD),
        )
        .await
        .expect("read every statement");
    let all = all.json();
    let rows = all["statements"].as_array().expect("a statement list");
    let owed: Vec<&Value> = rows
        .iter()
        .filter(|s| s["billing_mode"] == "invoice" && s["paid_at"].is_null())
        .collect();
    assert!(
        !owed.is_empty()
            && owed
                .iter()
                .all(|s| s["total_amount_micro_usd"] != Value::Null),
        "the fixture owes at least acme's day: {all}"
    );
    assert_eq!(
        summary["unpaid_statements"],
        owed.len() as i64,
        "the summary and the full list must count the same rows: {summary}"
    );

    let unpaid_filter = client
        .get(
            &server.url("/api/admin/billing/statements?unpaid=true&limit=200"),
            Some(MANAGER_PASSWORD),
        )
        .await
        .expect("read the outstanding statements");
    let outstanding = unpaid_filter.json();
    let filtered = outstanding["statements"]
        .as_array()
        .expect("a statement list");
    assert_eq!(
        filtered.len() as i64,
        summary["unpaid_statements"],
        "the filter and the summary count the same rows: {outstanding}"
    );
    for statement in filtered {
        assert_eq!(
            statement["billing_mode"], "invoice",
            "the unpaid filter must never surface a settlement record: {statement}"
        );
        assert_eq!(statement["paid_at"], Value::Null);
    }
    // And a settlement record is *present* in the full list but absent from the
    // unpaid one, which is the difference between "not counted as owed" and
    // "not issued at all".
    assert!(
        rows.iter().any(|s| s["billing_mode"] == "reconciliation"),
        "a settlement record exists and is priced: {all}"
    );
    assert!(
        !filtered
            .iter()
            .any(|s| s["billing_mode"] == "reconciliation"),
        "and it owes nothing, so it is not outstanding: {outstanding}"
    );

    upstream.stop().await;
}
