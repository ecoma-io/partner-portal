//! Suspension, and the three things that must not cause one.
//!
//! The fault injected here is not a broken process or a locked database — it is a
//! **commercial fault**: a bill the partner has not paid. The claims worth
//! pinning are the ones that fail in the quiet direction, and each of them is
//! the same shape as a defect this product already has a rule about:
//!
//! - a suspended partner is refused **before the upstream and before the
//!   ledger**. Not "the request failed" — it failed for the right reason, at the
//!   right point, and wrote nothing. A refusal that reached the upstream would
//!   cost money for traffic the partner is not being served, and a refusal that
//!   minted a row would put an unpriceable entry on the very statement that
//!   caused it.
//! - **resuming writes nothing to the partner row.** The status is derived, so a
//!   payment cannot leave a stored flag behind, and a crash between the payment
//!   and the next refresh cannot strand a partner. The partner row is read
//!   before and after for exactly that reason.
//! - **an incomplete statement does not suspend.** It is a bill the product
//!   cannot defend, because it knows it did not account for the day.
//! - **a zero-amount statement does not suspend**, and **a reconciliation
//!   statement is never suspended at all**. Refusing a paying partner's traffic
//!   over `$0.000000` is the worst possible reading of "enforce the payment
//!   obligation".
//!
//! Each of the last three is a day that *would* suspend under the plain
//! predicate — invoice, unpaid, past its date — and does not. A suite that only
//! pinned the loud direction would pass against an implementation that suspends
//! on all four.
//!
//! # How a bill is made overdue without moving the clock
//!
//! `due_at` is the period end plus the partner's payment terms, so a partner on
//! **zero** terms is due the moment its day closes. That is the one way to make a
//! statement past its date inside a test: the clock is never faked, no row is
//! hand-written with a deadline in the past, and the bill is a statement the
//! real scheduler generated from real usage rows. `KeySpec::with_payment_terms`
//! carries it, and the product treats it as a legitimate configuration — "pay
//! immediately" — rather than as the mistake the 720-minute default prevents.
//!
//! # Which day
//!
//! The harness anchors every seeded partner two days back with a zero-amount
//! statement, and the walk resumes *from* the anchor, so the oldest day the
//! scheduler will ever generate for a seeded partner is **yesterday** — the day
//! after the anchor. That is the bill under test here. Reaching behind it would
//! mean writing the anchor by hand, which is a fixture asserting against a shape
//! the product does not produce — the reason the harness uses its own generator.

use std::path::Path;
use std::time::Duration;

use crate::common::{
    Behaviour, KeySpec, ManagerSpec, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    chat_request, open_db,
};
use http::{Method, StatusCode};
use rusqlite::Connection;
use serde_json::{Value, json};

const MANAGER_PASSWORD: &str = "sk-fault-manager-do-not-guess";
const PARTNER: &str = "acme";
const SETTLED: &str = "beta";
const INCOMPLETE: &str = "gamma";
const FREE: &str = "delta";

/// The one message a suspension produces, on every path and for every reason.
///
/// Pinned as a literal rather than read out of the module under test: a test
/// that compared the response against `suspension_message()` would pass against a
/// message that had been changed to say the opposite.
const SUSPENDED_MESSAGE: &str = "Service is suspended because an invoice is overdue";

/// Micro-dollars per million tokens, as the harness meters. Large enough that a
/// day of traffic is unambiguously non-zero — which is what makes "a
/// zero-amount statement does not suspend" a claim about the predicate rather
/// than about the fixture having no usage.
const INPUT_PER_MILLION: i64 = 2_500_000;
const OUTPUT_PER_MILLION: i64 = 10_000_000;

/// The tokens one seeded request carries, so a statement's total is a number a
/// reader can check rather than trust.
const INPUT_TOKENS: i64 = 1_000;
const OUTPUT_TOKENS: i64 = 2_000;

/// `1000 × 2_500_000 / 10⁶` = 2500 and `2000 × 10_000_000 / 10⁶` = 20000.
const DAY_TOTAL: i64 = 22_500;

/// Payment terms of zero: the invoice is due at the end of the period it bills.
const TERMS_MINUTES: i64 = 0;

/// The day before the billing day the anchor lands on, which is the oldest day
/// the walk will close for a seeded partner.
///
/// Asked of the *product's* clock rather than computed from UTC: the anchor is
/// placed with `BillingDay::of(now, timezone)`, so a billing day is not always a
/// UTC date. Deriving this from UTC instead would put the fixture a day off for
/// every deployment whose calendar is not UTC, and the tests would pass only on
/// the machine that wrote them.
fn billable_day() -> String {
    use partner_portal::billing::{BillingDay, BillingTimezone};
    let timezone = BillingTimezone::default();
    let today = BillingDay::of(partner_portal::ledger::timefmt::now(), timezone);
    today.previous().to_string()
}

/// One metered request's worth of usage, priced by the harness's snapshots.
///
/// The request id is counted off the ledger rather than composed from the
/// consumer and the day: the harness re-runs a starting hook for every process it
/// starts, and two instances over one shared directory is a thing this file does
/// on purpose. A composed id would collide on the second run and the second
/// instance's fixture would be missing rows that the first one's contributed —
/// which would show up as a statement total that quietly depended on how many
/// processes the test happened to start.
fn write_usage(conn: &Connection, consumer: &str, day: &str) {
    let next: i64 = conn
        .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
        .expect("count the ledger");
    conn.execute(
        "INSERT INTO usage_records (request_id, created_at, consumer_id, model, endpoint,
            streaming, http_status, request_status, usage_status, input_tokens, output_tokens,
            cached_tokens, ttft_ms, duration_ms, input_price_snapshot,
            cached_input_price_snapshot, output_price_snapshot)
         VALUES (?1, ?2, ?3, 'gpt-4o', 'chat_completions', 0, 200, 'completed', 'available',
                 ?4, ?5, 0, 10, 100, ?6, ?6, ?7)",
        rusqlite::params![
            format!("fault-usage-{next}"),
            format!("{day}T12:00:00.000000000Z"),
            consumer,
            INPUT_TOKENS,
            OUTPUT_TOKENS,
            INPUT_PER_MILLION,
            OUTPUT_PER_MILLION,
        ],
    )
    .expect("seed a usage row");
}

/// A request the provider reported nothing for: `NULL` tokens and an
/// `unavailable` status, never zero. Invariant 3, and the reason the day it
/// lands on is counted as incomplete rather than billed at nothing.
fn write_unreported_usage(conn: &Connection, consumer: &str, day: &str) {
    let next: i64 = conn
        .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
        .expect("count the ledger");
    conn.execute(
        "INSERT INTO usage_records (request_id, created_at, consumer_id, model, endpoint,
            streaming, http_status, request_status, usage_status, input_tokens, output_tokens,
            cached_tokens, ttft_ms, duration_ms, input_price_snapshot,
            cached_input_price_snapshot, output_price_snapshot)
         VALUES (?1, ?2, ?3, 'gpt-4o', 'chat_completions', 0, 200, 'completed', 'unavailable',
                 NULL, NULL, NULL, NULL, 100, ?4, ?4, ?5)",
        rusqlite::params![
            format!("fault-unreported-{next}"),
            format!("{day}T09:00:00.000000000Z"),
            consumer,
            INPUT_PER_MILLION,
            OUTPUT_PER_MILLION,
        ],
    )
    .expect("seed a request with no reported usage");
}

/// A request the operator priced at zero. The row is complete and priced; the
/// money is nil, and it has to be *typed* to mean that — which is why the
/// snapshots are literally zero rather than absent.
fn write_free_usage(conn: &Connection, consumer: &str, day: &str) {
    let next: i64 = conn
        .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
        .expect("count the ledger");
    conn.execute(
        "INSERT INTO usage_records (request_id, created_at, consumer_id, model, endpoint,
            streaming, http_status, request_status, usage_status, input_tokens, output_tokens,
            cached_tokens, ttft_ms, duration_ms, input_price_snapshot,
            cached_input_price_snapshot, output_price_snapshot)
         VALUES (?1, ?2, ?3, 'gpt-4o', 'chat_completions', 0, 200, 'completed', 'available',
                 5000, 5000, 0, 10, 100, 0, 0, 0)",
        rusqlite::params![
            format!("fault-free-{next}"),
            format!("{day}T09:00:00.000000000Z"),
            consumer,
        ],
    )
    .expect("seed a free request");
}

/// The fixture: a day of usage for every partner, and every partner differs in
/// exactly one way.
///
/// `acme` is the ordinary case — a priced day, invoice terms, nothing to excuse
/// it. The other three each carry the one thing that must *not* suspend, and they
/// carry it against the same priced day where they can, so a difference in the
/// outcome below is a difference about the predicate and not about the fixture.
fn seed_days() -> impl Fn(&Path) + Send + Sync + 'static {
    move |path: &Path| {
        let conn = open_db(path);
        let day = billable_day();
        write_usage(&conn, PARTNER, &day);
        // A settlement record priced exactly as an invoice would be: the mode
        // is the only difference between this and acme's bill.
        write_usage(&conn, SETTLED, &day);
        // An incomplete day: one priced request the product could account for,
        // and one whose usage the provider never reported.
        write_usage(&conn, INCOMPLETE, &day);
        write_unreported_usage(&conn, INCOMPLETE, &day);
        // A complete day, at a price of zero.
        write_free_usage(&conn, FREE, &day);
    }
}

/// The spec: four partners and a manager, with the days already on disk and a
/// scheduler that closes a day the instant it ends.
fn spec(upstream: &MockUpstream) -> Spec {
    seeded_spec(upstream, seed_days())
}

/// The same partners and keys, over a ledger the caller has already prepared.
///
/// Split out because the harness re-runs a starting hook for *every* process it
/// starts, and a second instance on a shared directory would otherwise seed a
/// second day on top of the first — the two-instance test has to be given a spec
/// whose ledger is already complete, or it would be measuring its own fixture.
fn seeded_spec(upstream: &MockUpstream, start: impl Fn(&Path) + Send + Sync + 'static) -> Spec {
    Spec::new(upstream)
        .with_keys(vec![
            KeySpec::new("primary")
                .with_consumer(PARTNER)
                .with_payment_terms(TERMS_MINUTES),
            KeySpec::new("settled")
                .with_consumer(SETTLED)
                .on_reconciliation(),
            KeySpec::new("unaccounted")
                .with_consumer(INCOMPLETE)
                .with_payment_terms(TERMS_MINUTES),
            KeySpec::new("free")
                .with_consumer(FREE)
                .with_payment_terms(TERMS_MINUTES),
        ])
        .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
        .starting_ledger(start)
}

async fn server(upstream: &MockUpstream) -> TestServer {
    TestServer::start(spec(upstream)).await
}

fn key_of(server: &TestServer, consumer: &str) -> String {
    let index = server
        .seeded
        .iter()
        .position(|key| key.consumer_id == consumer)
        .expect("the spec seeds a key for this consumer");
    server.key_at(index).to_string()
}

/// The statement for one partner's billable day, with its lines, once it exists.
///
/// Polls, because a day the scheduler has not closed yet is a normal condition.
/// It waits for that *particular* day and that partner, because the harness
/// anchors each partner two days back and a first-row-wins assertion would pass
/// on the anchor — a statement with no usage in it, which asserts nothing.
async fn await_statement(server: &TestServer, consumer: &str) -> Value {
    let client = TestClient::new();
    let day = billable_day();
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let listed = client
            .get(
                &server.url("/api/admin/billing/statements"),
                Some(MANAGER_PASSWORD),
            )
            .await
            .expect("read the statement list");
        if let Some(found) = listed.json()["statements"].as_array().and_then(|rows| {
            rows.iter()
                .find(|row| row["consumer_id"] == consumer && row["billing_date"] == day)
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
            "no statement for {consumer} on {day} within {WAIT_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// What a partner's own service status says.
///
/// The endpoint answers with one entry per consumer *in scope*, and a partner key
/// scopes it to exactly one, so the entry found by `consumer_id` is that
/// partner's. Selected by identity rather than by position, because "the first
/// one in the list" is an assertion about ordering that a status object has no
/// reason to make — and because a helper that guessed would report a neighbour's
/// status, which is the one thing this file must never do.
async fn service_status(server: &TestServer, consumer: &str, credential: &str) -> Value {
    let listed = TestClient::new()
        .get(&server.url("/api/billing/status"), Some(credential))
        .await
        .expect("read the service status")
        .json();
    listed["statuses"]
        .as_array()
        .unwrap_or_else(|| panic!("a status list, not {listed}"))
        .iter()
        .find(|entry| entry["consumer_id"] == consumer)
        .cloned()
        .unwrap_or_else(|| panic!("a status for {consumer}, not {listed}"))
}

/// One inference request with `key`, and its response.
async fn call_inference(
    client: &TestClient,
    server: &TestServer,
    key: &str,
) -> crate::common::HttpResponse {
    client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(key),
            chat_request("mock-model"),
            &[],
        )
        .await
}

/// Wait until the request path refuses this key, within the documented
/// propagation bound.
///
/// The suspension sits in the in-memory snapshot, which refreshes on the 1s
/// interval a real deployment accepts (ADR 0014) — not in a store the request
/// path queries per call. A request made before the snapshot was published is
/// served, and being served for that instant is the propagation bound working,
/// not the product. So "the partner is suspended" is asserted as a condition
/// with a deadline, exactly the shape the resume loop uses: the test fails if
/// the bound lengthens rather than passing at a sleep that happened to work.
async fn wait_until_refused(client: &TestClient, server: &TestServer, key: &str) {
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let response = call_inference(client, server, key).await;
        if response.status == StatusCode::FORBIDDEN {
            assert_eq!(response.json()["error"]["code"], "billing_suspended");
            return;
        }
        assert_eq!(
            response.status,
            StatusCode::OK,
            "a suspended partner's request must be refused, never something else: {}",
            response.text()
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the request path had not seen the suspension after {WAIT_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The partner row as the database holds it — a tuple is enough, and it is
/// deliberate: `SELECT *` would silently widen when a column is added, and the
/// claim is that *nothing* about this row changes.
fn partner_row(conn: &Connection, consumer: &str) -> Option<(String, i64)> {
    conn.query_row(
        "SELECT billing_mode, payment_terms_minutes FROM partners WHERE consumer_id = ?1",
        [consumer],
        |row| Ok((row.get(0)?, row.get(1)?)),
    )
    .ok()
}

/// An overdue invoice refuses the partner's traffic before the upstream and
/// before the ledger, and says which bill it is.
#[tokio::test]
async fn an_overdue_invoice_is_refused_before_the_upstream_and_the_ledger() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1000,
        completion: 2000,
        cached: 0,
    })
    .await;
    let server = server(&upstream).await;
    let client = TestClient::new();
    let key = key_of(&server, PARTNER);

    // The bill under test exists, is priced, and is past its date. Asserted
    // first: a 403 with no such statement would be a suspension for some other
    // reason, and the rest of the test would still pass.
    let statement = await_statement(&server, PARTNER).await;
    assert_eq!(statement["billing_mode"], "invoice");
    assert_eq!(
        statement["total_amount_micro_usd"], DAY_TOTAL,
        "the bill under test has to owe something: {statement}"
    );
    assert_eq!(statement["incomplete_usage_count"], 0, "and be complete");
    assert!(statement["due_at"].is_string(), "{statement}");
    assert_eq!(statement["paid_at"], Value::Null);
    assert_eq!(statement["can_suspend"], true, "{statement}");

    let status_before = service_status(&server, PARTNER, &key).await;
    assert_eq!(
        status_before["suspended"], true,
        "the gateway status says suspended, so the 403 below must follow: {status_before}"
    );

    // The request path gates on the *in-memory* snapshot, which refreshes on the
    // interval a real deployment accepts (1s, ADR 0014) — not on the database
    // the status API reads fresh. The statement was just closed, so the very
    // first request can arrive before the snapshot that carries the suspension
    // was published; being served for that instant is the propagation bound
    // working, not the product. So the counters are read *after* the request
    // path first enforces the refusal, and the claim is "from the moment the
    // gate knows, nothing more reaches the upstream and nothing more is
    // minted". Polling for that moment is a condition with a deadline, never a
    // sleep: the test fails if the documented bound lengthens, rather than
    // passing at a number that happened to work.
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    let refused = loop {
        let response = call_inference(&client, &server, &key).await;
        if response.status == StatusCode::FORBIDDEN {
            break response;
        }
        assert_eq!(
            response.status,
            StatusCode::OK,
            "a suspended partner's request must pass or be refused, never something else: {}",
            response.text()
        );
        assert!(
            std::time::Instant::now() < deadline,
            "the request path had not seen the suspension after {WAIT_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    };
    assert_eq!(refused.status, StatusCode::FORBIDDEN, "{}", refused.text());
    assert_eq!(refused.json()["error"]["code"], "billing_suspended");
    // The type is the *OpenAI-compatible* category, `permission_error`, matching
    // every other refusal the request path issues — a suspension is a refusal
    // like a disallowed model, not a billing-system failure.
    assert_eq!(refused.json()["error"]["type"], "permission_error");
    assert_eq!(
        refused.json()["error"]["message"],
        SUSPENDED_MESSAGE,
        "one message on every refusal, so a partner can be told what happened"
    );

    // Read from here, not before the loop: a request served during the
    // propagation instant legitimately reached the upstream, and this assertion
    // is about the enforcement that follows, not about the race.
    let upstream_before = upstream.request_count();
    let rows_before: i64 = {
        let conn = server.open_db();
        conn.query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
            .expect("count the ledger")
    };
    let again = call_inference(&client, &server, &key).await;
    assert_eq!(again.status, StatusCode::FORBIDDEN, "{}", again.text());
    assert_eq!(
        upstream.request_count(),
        upstream_before,
        "once the request path knows, a suspended partner's request must never \
         reach the upstream"
    );
    let conn = server.open_db();
    let rows_after: i64 = conn
        .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
        .expect("count the ledger");
    assert_eq!(
        rows_after, rows_before,
        "and it must mint no ledger row: one here would be an unpriceable entry \
         on the very statement that caused the refusal"
    );

    // A 403 that looked like a 401 would be the worst shape of this bug: a
    // partner whose SDK discards and re-issues the credential would loop forever
    // against a key that is perfectly valid. The key still works, and says so.
    let identity = client
        .get(&server.url("/api/me"), Some(&key))
        .await
        .expect("read /api/me");
    assert_eq!(
        identity.status,
        StatusCode::OK,
        "the credential is still valid"
    );
    let identity = identity.json();
    assert_eq!(identity["role"], "consumer");
    assert_eq!(identity["consumer_id"], PARTNER);

    // And the partner's own status carries the bill, because "an invoice is
    // overdue" with no bill attached is not an answer to "which one, for how
    // much".
    let status = service_status(&server, PARTNER, &key).await;
    assert_eq!(status["suspended"], true, "{status}");
    assert_eq!(status["status"], "suspended");
    assert_eq!(status["reason"]["code"], "invoice_overdue");
    assert_eq!(status["reason"]["statement_id"], statement["id"]);
    assert_eq!(status["reason"]["billing_date"], billable_day());
    assert_eq!(
        status["reason"]["amount_micro_usd"], statement["total_amount_micro_usd"],
        "the reason quotes the same total the statement does"
    );
    assert_eq!(
        status["reason"]["amount"], statement["total_amount"],
        "in the same rendered form, so the two screens agree"
    );
    assert_eq!(status["message"], SUSPENDED_MESSAGE);

    // Discovery too. A partner who cannot generate must not be able to browse
    // the catalog: a suspension that covered only inference would look partial
    // to the partner, and would hand the upstream's catalog to someone who is
    // not being served.
    let models = client.get(&server.url("/v1/models"), Some(&key)).await;
    let models = models.expect("read /v1/models");
    assert_eq!(
        models.status,
        StatusCode::FORBIDDEN,
        "a suspended partner must not browse the catalog: {}",
        models.text()
    );
    assert_eq!(models.json()["error"]["code"], "billing_suspended");
    assert_eq!(models.json()["error"]["message"], SUSPENDED_MESSAGE);
    assert_eq!(
        upstream.request_count(),
        upstream_before,
        "including for discovery"
    );
}

/// Paying the bill resumes the partner, and resuming writes nothing to it.
#[tokio::test]
async fn paying_the_bill_resumes_the_partner_without_writing_to_it() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1000,
        completion: 2000,
        cached: 0,
    })
    .await;
    let server = server(&upstream).await;
    let client = TestClient::new();
    let key = key_of(&server, PARTNER);

    let statement = await_statement(&server, PARTNER).await;
    let id = statement["id"].as_i64().expect("the statement id");

    // Suspended to begin with, so a pass after the payment cannot be the
    // fixture's doing. The request path gates on the in-memory snapshot, which
    // refreshes on the documented 1s bound (ADR 0014), so the first refusal is
    // polled for rather than asserted on the first call — a call served during
    // the propagation instant is the bound working, and this claim is about
    // the payment that follows, not about that race. The resume loop below uses
    // the same shape, so both directions of the derivation are tested the same
    // way.
    wait_until_refused(&client, &server, &key).await;

    // The partner row as it stands. If resumption depended on a write to it,
    // this is the thing that would change — and a stored flag is exactly what
    // the derivation exists to remove.
    let before = {
        let conn = server.open_db();
        partner_row(&conn, PARTNER)
    };
    assert_eq!(before, Some(("invoice".to_string(), TERMS_MINUTES)));

    let paid = client
        .call(
            Method::POST,
            &server.url(&format!("/api/admin/billing/statements/{id}/mark-paid")),
            Some(MANAGER_PASSWORD),
            bytes::Bytes::from(
                json!({ "paid_by": "fault-suite", "reference": "wire-1" }).to_string(),
            ),
            &[],
        )
        .await;
    assert_eq!(paid.status, StatusCode::OK, "{}", paid.text());

    let after = {
        let conn = server.open_db();
        partner_row(&conn, PARTNER)
    };
    assert_eq!(
        after, before,
        "resuming must not write to the partner: the status is derived from the \
         statements, so a flag there would be a second source of truth for a \
         payment to have to clear and a crash could leave stale"
    );

    // The status is read from a snapshot refreshed on the same interval as the
    // key set, so a payment takes effect on the next sweep rather than
    // instantly. Waited as a condition with a deadline, never as a sleep: a
    // sleep would either be too short and flaky or too long and slow, and would
    // be asserting a duration this test does not control.
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let response = call_inference(&client, &server, &key).await;
        if response.status == StatusCode::OK {
            break;
        }
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "the partner must come back as 200 or stay 403, never something else: {}",
            response.text()
        );
        assert_eq!(response.json()["error"]["code"], "billing_suspended");
        assert!(
            std::time::Instant::now() < deadline,
            "the partner was still suspended after {WAIT_TIMEOUT:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let status = service_status(&server, PARTNER, &key).await;
    assert_eq!(status["suspended"], false, "{status}");
    assert_eq!(status["status"], "active");
    assert_eq!(
        status["reason"],
        Value::Null,
        "a resumed partner has no reason; a stale one would be telling a partner \
         they still owe a bill they paid"
    );
    // The zero-amount anchor the harness wrote is still unpaid, and still costs
    // nothing. It is past its date exactly when the *day it bills* is past its
    // date — `due_at` is the period end plus the partner's terms, and this
    // partner is on zero — so the day before the bill under test was the
    // statement's deadline, and by now that has gone by. Counting it and not
    // suspending on it is the same rule the next test pins from the other side.
    assert_eq!(status["overdue_statements"], 1, "{status}");
    assert_eq!(
        status["overdue_amount_micro_usd"], 0,
        "an outstanding statement for $0.000000 is outstanding and worth nothing: {status}"
    );
}

/// The three statements that must not suspend, each pinned as a case where the
/// plain predicate — invoice, unpaid, past its date — would have suspended.
#[tokio::test]
async fn only_a_complete_non_zero_overdue_invoice_suspends() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1000,
        completion: 2000,
        cached: 0,
    })
    .await;
    let server = server(&upstream).await;
    let client = TestClient::new();

    let incomplete = await_statement(&server, INCOMPLETE).await;
    let settlement = await_statement(&server, SETTLED).await;
    let free = await_statement(&server, FREE).await;

    // Each of these would suspend under `invoice && unpaid && due_at <= now`, so
    // the assertions on the statements themselves come first: if a fixture had
    // drifted — a day that is no longer past its date, a price that is no longer
    // zero — the status assertions below would pass for the wrong reason.
    assert_eq!(incomplete["billing_mode"], "invoice");
    assert!(incomplete["due_at"].is_string(), "{incomplete}");
    assert_eq!(incomplete["paid_at"], Value::Null);
    assert!(
        incomplete["total_amount_micro_usd"].as_i64().unwrap_or(0) > 0,
        "the incomplete day still costs money for the usage that *was* reported: {incomplete}"
    );
    assert_eq!(
        incomplete["incomplete_usage_count"], 1,
        "and it says how much of the day it could not account for: {incomplete}"
    );
    assert_eq!(
        incomplete["can_suspend"], false,
        "the statement itself knows it may not suspend: {incomplete}"
    );

    assert_eq!(settlement["billing_mode"], "reconciliation");
    assert_eq!(
        settlement["due_at"],
        Value::Null,
        "a settlement record has no deadline in any configuration: {settlement}"
    );
    assert_eq!(settlement["paid_at"], Value::Null);
    assert_eq!(
        settlement["total_amount_micro_usd"], DAY_TOTAL,
        "and it is priced exactly as an invoice is; it simply owes nothing: {settlement}"
    );
    assert_eq!(settlement["can_suspend"], false, "{settlement}");

    assert_eq!(free["billing_mode"], "invoice");
    assert!(free["due_at"].is_string(), "{free}");
    assert_eq!(free["paid_at"], Value::Null);
    assert_eq!(
        free["total_amount_micro_usd"], 0,
        "a day of usage the operator priced at zero states at zero: {free}"
    );
    assert_eq!(
        free["incomplete_usage_count"], 0,
        "and it is complete: {free}"
    );
    assert_eq!(
        free["can_suspend"], false,
        "a statement with nothing to enforce may not suspend: {free}"
    );

    // And all three keep serving.
    for (consumer, why) in [
        (
            INCOMPLETE,
            "an incomplete bill is one the product cannot defend",
        ),
        (SETTLED, "a settlement record owes nothing"),
        (FREE, "a statement for $0.000000 has nothing to enforce"),
    ] {
        let key = key_of(&server, consumer);
        let response = call_inference(&client, &server, &key).await;
        assert_eq!(
            response.status,
            StatusCode::OK,
            "{consumer} must keep serving — {why}: {}",
            response.text()
        );

        let status = service_status(&server, consumer, &key).await;
        assert_eq!(status["suspended"], false, "{consumer}: {status}");
        assert_eq!(status["status"], "active", "{consumer}: {status}");
        assert_eq!(status["reason"], Value::Null, "{consumer}: {status}");
    }

    // "Not suspending" must not have become "not recorded". An operator chasing
    // a balance has to see the bill; what changed is only the enforcement.
    let listed = client
        .get(
            &server.url("/api/admin/billing/statements?unpaid=true&limit=200"),
            Some(MANAGER_PASSWORD),
        )
        .await
        .expect("read the outstanding statements")
        .json();
    let unpaid: Vec<String> = listed["statements"]
        .as_array()
        .expect("a statement list")
        .iter()
        .map(|row| row["consumer_id"].as_str().unwrap_or_default().to_string())
        .collect();
    assert!(
        unpaid.iter().any(|c| c == INCOMPLETE),
        "the incomplete bill is still outstanding and must still be counted: {unpaid:?}"
    );
    assert!(
        unpaid.iter().any(|c| c == FREE),
        "and so is the zero-amount one: {unpaid:?}"
    );
    assert!(
        !unpaid.iter().any(|c| c == SETTLED),
        "but a settlement record is not owed, so it is not outstanding: {unpaid:?}"
    );

    // The settlement partner is priced and issued and still owes nothing.
    // Asserted through the *unpaid* list rather than a status field: the service
    // status is a statement about enforcement, and reconciliation is not an
    // enforced state — the claim here is that nothing is owed, so the query that
    // decides what is owed is the one worth asking. `beta` appearing in the
    // statement list and absent from the outstanding one is what a settlement
    // record looks like from the outside.
    let status = service_status(&server, SETTLED, &key_of(&server, SETTLED)).await;
    assert_eq!(status["overdue_statements"], 0, "{status}");
    assert_eq!(status["overdue_amount_micro_usd"], 0, "{status}");
}

/// A payment is recorded once, on a snapshot that is refreshed the same way a
/// key set is — which is what makes the two-boundary argument hold. Marking a
/// statement paid through a *sibling* instance must resume the partner here too,
/// and must not write to the partner row on either side.
#[tokio::test]
async fn a_payment_made_on_one_instance_resumes_the_partner_on_another() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 1000,
        completion: 2000,
        cached: 0,
    })
    .await;

    // One directory, so both processes share one ledger — the arrangement a
    // rolling update actually has.
    let dir = tempfile::TempDir::new().expect("temp dir");
    let port = crate::common::free_port();
    let shared = spec(&upstream).with_port(port);
    let first = TestServer::start_in(dir.path(), shared.clone()).await;
    let statement = await_statement(&first, PARTNER).await;
    let id = statement["id"].as_i64().expect("the statement id");
    let key = key_of(&first, PARTNER);
    let first_url = first.url("");

    // A second instance over the same file, on another port. The harness's
    // advisory lock permits this — it is the arrangement `tests/e2e` uses to
    // prove two instances lose nothing.
    let second = TestServer::start_in(
        dir.path(),
        seeded_spec(&upstream, |_path: &Path| {}).with_port(crate::common::free_port()),
    )
    .await;

    let client = TestClient::new();

    // Suspended on both, before the payment. Each instance's request path
    // reads its own in-memory snapshot, so each is polled until it enforces
    // the suspension within the propagation bound — the claim is that *both*
    // come to refuse, not that the first call after startup does.
    for url in [first_url.as_str(), second.base_url.as_str()] {
        let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            let response = client
                .call(
                    Method::POST,
                    &format!("{url}/v1/chat/completions"),
                    Some(&key),
                    chat_request("mock-model"),
                    &[],
                )
                .await;
            if response.status == StatusCode::FORBIDDEN {
                assert_eq!(response.json()["error"]["code"], "billing_suspended");
                break;
            }
            assert_eq!(
                response.status,
                StatusCode::OK,
                "{url}: {}",
                response.text()
            );
            assert!(
                std::time::Instant::now() < deadline,
                "{url} had not seen the suspension after {WAIT_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // Paid through the first, which the second only ever hears about.
    let payment = client
        .call(
            Method::POST,
            &format!("{first_url}/api/admin/billing/statements/{id}/mark-paid"),
            Some(MANAGER_PASSWORD),
            bytes::Bytes::from(json!({ "paid_by": "sibling" }).to_string()),
            &[],
        )
        .await;
    assert_eq!(payment.status, StatusCode::OK, "{}", payment.text());

    // Both resume, each within the refresh interval. Bounded, not slept: the
    // bound is the product's own propagation guarantee and this test should fail
    // if it lengthens, not merely if it is longer than a number chosen here.
    for url in [first_url.as_str(), second.base_url.as_str()] {
        let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
        loop {
            let response = client
                .call(
                    Method::POST,
                    &format!("{url}/v1/chat/completions"),
                    Some(&key),
                    chat_request("mock-model"),
                    &[],
                )
                .await;
            if response.status == StatusCode::OK {
                break;
            }
            assert_eq!(
                response.status,
                StatusCode::FORBIDDEN,
                "{url}: {}",
                response.text()
            );
            assert!(
                std::time::Instant::now() < deadline,
                "{url} was still suspended after {WAIT_TIMEOUT:?}"
            );
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    // The partner row is untouched by the payment, on the instance that took it
    // and on the sibling that only learned about it.
    let conn = first.open_db();
    assert_eq!(
        partner_row(&conn, PARTNER),
        Some(("invoice".to_string(), TERMS_MINUTES)),
        "a payment recorded on one instance must not write a resume flag anywhere"
    );
}
