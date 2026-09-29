//! The partner's billing surface: what a day of usage becomes, and what the
//! partner is told about it.
//!
//! The real binary runs as a child process, the mock upstream answers, the usage
//! is written, and the statement is read back — because the claims worth pinning
//! here are the ones the API cannot show on its own:
//!
//! - a statement is built from the **price snapshot on the usage row**, not from
//!   the partner's current price list, so an operator's price change does not
//!   retroactively reprice yesterday's traffic;
//! - a request the provider did not report usage for is **counted and not
//!   billed**, rather than billed at zero. That is invariant 3 with money
//!   attached, and it fails the same quiet way.
//!
//! The statements are produced by the real scheduler, not by a fixture insert.
//!
//! # Why the usage rows are written by the harness
//!
//! A day closes once, and it closes about a second after the process starts. A
//! test that made a request first would have its usage land on a day the
//! scheduler had already closed, with nothing left to walk. So the day of usage
//! under test is written by [`Spec::starting_ledger`], before the process
//! exists, and the scheduler closes it for real on its first tick.
//!
//! The price snapshots on those rows are the harness's, and that is the point
//! rather than a compromise: a row metered by the request path would carry
//! whatever the seeded partner's price list said, and the claim under test is
//! that the *row's* price wins over the partner's current list. A row whose
//! price differs from the partner's list by an order of magnitude is the only
//! thing that can make that claim falsifiable — with equal prices it would pass
//! against an implementation that simply re-read the list.

use crate::common::{
    Behaviour, KeySpec, ManagerSpec, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT,
    open_db,
};
use bytes::Bytes;
use http::{Method, StatusCode};
use rusqlite::Connection;
use serde_json::{Value, json};
use std::path::Path;

const MANAGER_PASSWORD: &str = "sk-billing-manager-do-not-guess";

/// The price the harness meters at on the day under test, in micro-dollars per
/// million tokens. Deliberately ten times the seeded partner price list, so a
/// statement built from the wrong source is off by a factor of ten rather than
/// by a rounding error.
const INPUT_PER_MILLION: i64 = 2_500_000;
const CACHED_PER_MILLION: i64 = 1_250_000;
const OUTPUT_PER_MILLION: i64 = 10_000_000;

const INPUT_TOKENS: i64 = 1_000;
const OUTPUT_TOKENS: i64 = 2_000;

/// What one day of the seeded traffic costs at the snapshots above:
/// `1000 × 2_500_000 / 10⁶` = 2500 and `2000 × 10_000_000 / 10⁶` = 20000.
const DAY_TOTAL: i64 = 22_500;

/// The billing date the scheduler will close, computed the same way the server
/// does: yesterday in the configured calendar, because today has not ended yet.
fn yesterday() -> String {
    let today = time::OffsetDateTime::now_utc().date();
    (today - time::Duration::days(1))
        .to_string()
        .split('T')
        .next()
        .expect("a date has no time part")
        .to_string()
}

/// Write one request's worth of usage onto a closed day, with its price
/// snapshots, exactly as the metering write would have left it.
fn write_usage(
    conn: &Connection,
    consumer: &str,
    model: &str,
    at: &str,
    input_tokens: Option<i64>,
    output_tokens: Option<i64>,
) {
    let (status, input, output) = match (input_tokens, output_tokens) {
        (Some(i), Some(o)) => ("available", Some(i), Some(o)),
        // The provider said nothing. `NULL`, never `0` — invariant 3, and the
        // whole reason the statement has an incomplete count to report.
        _ => ("unavailable", None, None),
    };
    // The request id is the primary key, so two rows on one day need two ids.
    // `rowid` is only distinct among rows that exist, so it is read as the count
    // at the moment of insertion rather than assumed.
    let next: i64 = conn
        .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
        .expect("count the ledger");
    conn.execute(
        "INSERT INTO usage_records (request_id, created_at, consumer_id, model, endpoint,
            streaming, http_status, request_status, usage_status, input_tokens, output_tokens,
            cached_tokens, ttft_ms, duration_ms, input_price_snapshot,
            cached_input_price_snapshot, output_price_snapshot)
         VALUES (?1, ?2, ?3, ?4, 'chat_completions', 0, 200, 'completed', ?5, ?6, ?7, 0,
                 10, 100, ?8, ?9, ?10)",
        rusqlite::params![
            format!("seed-{consumer}-{model}-{at}-{next}"),
            at,
            consumer,
            model,
            status,
            input,
            output,
            INPUT_PER_MILLION,
            CACHED_PER_MILLION,
            OUTPUT_PER_MILLION,
        ],
    )
    .expect("seed a usage row onto a closed day");
}

/// A day of usage for each consumer named, priced at the snapshots above.
fn seed_a_day(consumers: &[&str]) -> impl Fn(&Path) + Send + Sync + 'static {
    let consumers: Vec<String> = consumers.iter().map(|c| c.to_string()).collect();
    move |path: &Path| {
        let conn = open_db(path);
        let at = format!("{}T12:00:00.000000000Z", yesterday());
        for consumer in &consumers {
            write_usage(
                &conn,
                consumer,
                "gpt-4o",
                &at,
                Some(INPUT_TOKENS),
                Some(OUTPUT_TOKENS),
            );
        }
    }
}

/// One server, one partner on invoice terms, a manager over it, a day of usage
/// waiting to be closed.
async fn billing_server(upstream: &MockUpstream) -> TestServer {
    TestServer::start(
        Spec::new(upstream)
            .with_keys(vec![KeySpec::new("primary").with_consumer("acme")])
            .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
            .starting_ledger(seed_a_day(&["acme"])),
    )
    .await
}

/// The key index whose consumer is `consumer`, so a helper can be called with the
/// name the test used and read exactly the row the API would.
fn key_index_for(server: &TestServer, consumer: &str) -> usize {
    server
        .seeded
        .iter()
        .position(|key| key.consumer_id == consumer)
        .expect("the spec seeds a key for the consumer under test")
}

/// Wait for the statement for `day` to be issued, then return it with its lines.
///
/// A day the scheduler has not closed yet is a *normal* condition, not a
/// failure, so this polls. And it waits for a *particular* day rather than any
/// statement: the harness anchors each partner two days back, so a test that
/// accepted the first row it saw would pass on the fixture's own anchor — a
/// statement with no usage in it, which asserts nothing.
async fn await_statement(server: &TestServer, consumer: &str, day: &str) -> Value {
    let key = key_index_for(server, consumer);
    let client = TestClient::new();
    let deadline = std::time::Instant::now() + WAIT_TIMEOUT;
    loop {
        let response = client
            .get(
                &server.url("/api/billing/statements"),
                Some(server.key_at(key)),
            )
            .await
            .expect("read the statement list");
        if let Some(found) = response.json()["statements"]
            .as_array()
            .and_then(|rows| rows.iter().find(|row| row["billing_date"] == day))
        {
            let id = found["id"].as_i64().expect("a statement id");
            return client
                .get(
                    &server.url(&format!("/api/billing/statements/{id}")),
                    Some(server.key_at(key)),
                )
                .await
                .expect("read the statement back")
                .json();
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "no statement for {consumer} on {day} within {WAIT_TIMEOUT:?}; \
                 server log:\n{}",
                server.logs()
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    }
}

/// Read a partner's own statement list, as that partner's key sees it.
async fn statements_for_partner(server: &TestServer, consumer: &str) -> Value {
    TestClient::new()
        .get(
            &server.url("/api/billing/statements"),
            Some(server.key_at(key_index_for(server, consumer))),
        )
        .await
        .expect("read the statement list")
        .json()
}

/// The id of a statement view, which is a number and not a string.
fn beta_id_of(statement: &Value) -> i64 {
    statement["id"].as_i64().expect("a statement id")
}

/// The derived service status, as `credential` sees it.
///
/// A partner key gets one entry — their own — and a manager gets one per
/// partner, which is why this takes a credential rather than defaulting to the
/// first key: the two are different reads, not two views of one.
async fn read_status(server: &TestServer, credential: &str) -> Value {
    TestClient::new()
        .get(&server.url("/api/billing/status"), Some(credential))
        .await
        .expect("read the status")
        .json()
}

/// The request count and token totals of a statement, which live on its lines
/// rather than on the statement itself.
fn line_totals(statement: &Value) -> (i64, i64, i64) {
    let lines = statement["lines"]
        .as_array()
        .expect("lines on a detail read");
    let sum = |field: &str| -> i64 {
        lines
            .iter()
            .map(|line| line[field].as_i64().unwrap_or(0))
            .sum()
    };
    (
        sum("request_count"),
        sum("input_tokens"),
        sum("output_tokens"),
    )
}

#[tokio::test]
async fn a_day_of_usage_becomes_one_statement_priced_at_the_snapshot() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = billing_server(&upstream).await;

    let statement = await_statement(&server, "acme", &yesterday()).await;
    assert_eq!(statement["billing_mode"], "invoice");
    assert_eq!(statement["billing_date"], yesterday());
    assert_eq!(line_totals(&statement), (1, INPUT_TOKENS, OUTPUT_TOKENS));
    assert_eq!(statement["total_amount_micro_usd"], DAY_TOTAL);
    assert_eq!(statement["incomplete_usage_count"], 0);
    assert_eq!(statement["has_incomplete_usage"], false);
    assert!(statement["due_at"].is_string(), "an invoice has a deadline");
    assert_eq!(statement["outstanding"], true, "an unpaid invoice is owed");

    // The period is the day, in the calendar the configuration named, and the
    // cutoff is not before the period ends — the close delay is what makes the
    // boundary safe, and a statement whose cutoff preceded its own period end
    // would be claiming the last requests were already in.
    let period_start = statement["period_start"].as_str().expect("a period");
    let period_end = statement["period_end"].as_str().expect("a period");
    let cutoff = statement["billing_cutoff_at"].as_str().expect("a cutoff");
    assert!(
        period_start.starts_with(&yesterday()),
        "{period_start} does not open on the billed day"
    );
    assert!(period_end > period_start, "the period must have length");
    assert!(cutoff >= period_end, "a cutoff precedes its own period end");
    assert_eq!(statement["currency"], "USD");
    assert_eq!(statement["total_amount"], "$0.022500");
}

#[tokio::test]
async fn a_day_is_billed_once_however_many_times_the_scheduler_runs() {
    // The walk is idempotent, and the price is idempotence's proof: a second
    // pass that appended rather than deduplicated would show up as a duplicated
    // line. The scheduler has run many times by the time this reads the row —
    // the wait above is at least a second against a one-second interval — so a
    // duplicate would be there.
    let upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = billing_server(&upstream).await;
    let statement = await_statement(&server, "acme", &yesterday()).await;

    let lines = statement["lines"].as_array().expect("lines");
    assert_eq!(lines.len(), 1, "one model, one line");
    assert_eq!(lines[0]["request_count"], 1);
    assert_eq!(statement["total_amount_micro_usd"], DAY_TOTAL);

    // And exactly one statement exists per closed day, which is what the unique
    // constraint is there to make true even when two instances race.
    let rows: Vec<String> = {
        let conn = server.open_db();
        let mut stmt = conn
            .prepare("SELECT billing_date FROM daily_statements WHERE consumer_id = 'acme'")
            .expect("prepare the statement query");
        stmt.query_map([], |row| row.get::<_, String>(0))
            .expect("read the statements")
            .collect::<Result<Vec<_>, _>>()
            .expect("collect the statements")
    };
    assert_eq!(
        rows.len(),
        2,
        "the anchor the harness wrote and the day under test, and no more: {rows:?}"
    );
    assert_eq!(rows.iter().filter(|d| *d == &yesterday()).count(), 1);
}

#[tokio::test]
async fn a_price_change_does_not_reprice_traffic_that_was_already_metered() {
    // The claim that costs a partner real money. The statement reads the
    // snapshot on the usage row, which the harness priced at $2.50 / $10 per
    // million, so replacing the partner's list with something ten times dearer
    // must leave the issued statement and its lines exactly as they were.
    let upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = billing_server(&upstream).await;
    let client = TestClient::new();

    let statement = await_statement(&server, "acme", &yesterday()).await;
    let id = statement["id"].as_i64().expect("a statement id");
    assert_eq!(statement["total_amount_micro_usd"], DAY_TOTAL);

    let replaced = client
        .call(
            Method::PUT,
            &server.url("/api/admin/partners/acme/models"),
            Some(MANAGER_PASSWORD),
            Bytes::from(
                json!({"models": [{
                    "model": "gpt-4o",
                    "input_per_million": "25",
                    "cached_input_per_million": "12.5",
                    "output_per_million": "100",
                }]})
                .to_string(),
            ),
            &[],
        )
        .await;
    assert_eq!(replaced.status, StatusCode::OK);

    // The statement is immutable: the same id, the same total, the same prices.
    let after = client
        .get(
            &server.url(&format!("/api/billing/statements/{id}")),
            Some(server.key_at(0)),
        )
        .await
        .expect("read the statement back")
        .json();
    assert_eq!(after["total_amount_micro_usd"], DAY_TOTAL);
    let line = &after["lines"][0];
    assert_eq!(
        line["input_per_million"], "2.5",
        "the line keeps its snapshot"
    );
    assert_eq!(line["output_per_million"], "10");
    assert_eq!(line["total_cost_micro_usd"], DAY_TOTAL);

    // And the replacement is real: it is the price the next request is metered
    // at. A change that were merely *ignored* would pass everything above.
    let (in_p, out_p): (i64, i64) = server
        .open_db()
        .query_row(
            "SELECT input_price_micro_usd_per_million, output_price_micro_usd_per_million
             FROM partner_models WHERE consumer_id = 'acme' AND model = 'gpt-4o'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("the replaced price is durable");
    assert_eq!((in_p, out_p), (25_000_000, 100_000_000));
}

#[tokio::test]
async fn a_request_without_reported_usage_is_counted_and_not_billed() {
    // Invariant 3 with a money consequence. A row whose tokens were never
    // reported records `NULL`; the statement must count it and add nothing,
    // rather than billing a request it cannot price — and rather than billing it
    // at zero, which is the same mistake wearing a different hat.
    let upstream = MockUpstream::start(Behaviour::ChatStreamWithoutUsage).await;
    let server = TestServer::start(
        Spec::new(&upstream)
            .with_keys(vec![KeySpec::new("primary").with_consumer("acme")])
            .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
            .starting_ledger(|path| {
                let conn = open_db(path);
                let at = format!("{}T12:00:00.000000000Z", yesterday());
                // One measurable request and one that reported nothing: the
                // count has to be one, not two and not zero.
                write_usage(
                    &conn,
                    "acme",
                    "gpt-4o",
                    &at,
                    Some(INPUT_TOKENS),
                    Some(OUTPUT_TOKENS),
                );
                write_usage(&conn, "acme", "gpt-4o", &at, None, None);
            }),
    )
    .await;

    let statement = await_statement(&server, "acme", &yesterday()).await;
    assert_eq!(
        statement["incomplete_usage_count"], 1,
        "the unmeasured request is counted, not hidden"
    );
    assert_eq!(statement["has_incomplete_usage"], true);
    assert_eq!(
        statement["total_amount_micro_usd"], DAY_TOTAL,
        "the unmeasured request contributes no money, and the measured one is \
         still charged in full"
    );
    assert_eq!(
        line_totals(&statement).0,
        1,
        "only the measurable one is billed"
    );
    // Crucially: the statement exists and is visible, rather than being
    // withheld until the figure is known. A partner is told there is a gap, not
    // left waiting for a bill that may never come.
    assert_eq!(statement["billing_date"], yesterday());

    // The row itself still says `NULL`, so the count above is a count of a real
    // absence rather than of a zero the writer invented.
    let (tokens, status): (Option<i64>, String) = server
        .open_db()
        .query_row(
            "SELECT input_tokens, usage_status FROM usage_records
             WHERE usage_status = 'unavailable'",
            [],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .expect("the unmeasured row");
    assert_eq!(tokens, None, "an unreported count is NULL, not 0");
    assert_eq!(status, "unavailable");
}

#[tokio::test]
async fn a_reconciliation_partner_is_issued_a_settlement_record_that_owes_nothing() {
    // The terms are declared with the key, before the process exists — a mode
    // applied after the first tick would leave the first day an invoice, and the
    // test would be asserting on its own fixture rather than on the product.
    let upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = TestServer::start(
        Spec::new(&upstream)
            .with_keys(vec![
                KeySpec::new("primary")
                    .with_consumer("beta")
                    .on_reconciliation(),
            ])
            .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
            .starting_ledger(seed_a_day(&["beta"])),
    )
    .await;

    let statement = await_statement(&server, "beta", &yesterday()).await;
    assert_eq!(statement["billing_mode"], "reconciliation");
    assert_eq!(
        statement["total_amount_micro_usd"], DAY_TOTAL,
        "a settlement record is still priced; it simply is not owed"
    );
    assert!(
        statement["due_at"].is_null(),
        "a settlement record has no deadline, and NULL is the meaning"
    );
    assert_eq!(statement["outstanding"], false, "nothing is owed");
    assert_eq!(statement["can_suspend"], false);
    assert!(statement["paid_at"].is_null(), "and nothing has been paid");

    // The list agrees: a settlement record is not an unpaid invoice, and the
    // outstanding totals are the invoices'.
    let list = statements_for_partner(&server, "beta").await;
    assert_eq!(list["unpaid_count"], 0);
    assert_eq!(list["unpaid_total_micro_usd"], 0);
    assert_eq!(list["unpaid_total"], "$0.000000");
}

#[tokio::test]
async fn a_partner_sees_only_its_own_statements() {
    // Two partners, one billing day. The scope comes from the credential and
    // from nothing else — not from a query parameter the client can widen.
    let upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = TestServer::start(
        Spec::new(&upstream)
            .with_keys(vec![
                KeySpec::new("primary").with_consumer("acme"),
                KeySpec::new("secondary").with_consumer("beta"),
            ])
            .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
            .starting_ledger(seed_a_day(&["acme", "beta"])),
    )
    .await;

    let acme = await_statement(&server, "acme", &yesterday()).await;
    let beta = await_statement(&server, "beta", &yesterday()).await;
    assert_ne!(acme["id"], beta["id"]);
    assert_eq!(acme["consumer_id"], "acme");
    assert_eq!(beta["consumer_id"], "beta");

    // `acme`'s key asking for `beta` by parameter changes nothing.
    let widened = TestClient::new()
        .get(
            &server.url("/api/billing/statements?consumers=beta"),
            Some(server.key_at(key_index_for(&server, "acme"))),
        )
        .await
        .expect("read the list")
        .json();
    let rows = widened["statements"].as_array().expect("an array");
    assert!(
        rows.iter().all(|row| row["consumer_id"] == "acme"),
        "a `consumers` parameter must not widen a partner key's scope: {rows:?}"
    );
    assert_eq!(
        rows.len(),
        2,
        "acme sees their own two closed days and no part of beta's"
    );

    // And reaching beta's statement by id is a 404 that cannot be told apart
    // from an id that does not exist. A statement id is a rowid, so it is
    // guessable; distinguishing the two cases would confirm that beta's
    // statement exists.
    let beta_id = beta["id"].as_i64().expect("beta's statement id");
    let client = TestClient::new();
    let cross = client
        .get(
            &server.url(&format!("/api/billing/statements/{beta_id}")),
            Some(server.key_at(key_index_for(&server, "acme"))),
        )
        .await
        .expect("the request itself is answered");
    assert_eq!(
        cross.status,
        StatusCode::NOT_FOUND,
        "another consumer's statement must not be readable"
    );
    let absent = client
        .get(
            &server.url("/api/billing/statements/999999"),
            Some(server.key_at(key_index_for(&server, "acme"))),
        )
        .await
        .expect("the request itself is answered");
    assert_eq!(absent.status, StatusCode::NOT_FOUND);
    assert_eq!(
        absent.json(),
        cross.json(),
        "a foreign id and a nonexistent id must be indistinguishable"
    );
    // Including the message. "no statement with id 4" against "no statement
    // with id 999999" is the same shape with a different number, and a
    // partner walking 1, 2, 3, … would learn exactly how many statements the
    // organisation has issued and which day each partner billed.
    let message = cross.json()["error"]["message"]
        .as_str()
        .expect("an error message")
        .to_string();
    assert!(
        !message.contains(&beta_id.to_string()),
        "the 404 names the id it was asked for: {message}"
    );
}

#[tokio::test]
async fn a_partner_key_is_refused_the_manager_billing_surface() {
    // The payment control is an authorization boundary, not a hidden button.
    let upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = billing_server(&upstream).await;
    let client = TestClient::new();
    let key = server.key_at(0);

    for path in [
        "/api/admin/billing/summary",
        "/api/admin/billing/statements",
    ] {
        let response = client
            .get(&server.url(path), Some(key))
            .await
            .expect("call the manager route");
        assert_eq!(
            response.status,
            StatusCode::FORBIDDEN,
            "{path} must refuse a partner key"
        );
        assert_eq!(response.json()["error"]["code"], "manager_required");
    }

    // Marking a statement paid is the same boundary, and it must refuse the
    // *write* as firmly as the read. A hidden button is not an authorization.
    let marked = client
        .call(
            Method::POST,
            &server.url("/api/admin/billing/statements/1/mark-paid"),
            Some(key),
            Bytes::from(json!({"paid_by": "manager"}).to_string()),
            &[],
        )
        .await;
    assert_eq!(marked.status, StatusCode::FORBIDDEN);
    assert_eq!(marked.json()["error"]["code"], "manager_required");

    // A refused write must not have written anything, which is the failure a
    // UI-only check would miss: a button that is hidden in the SPA and a route
    // that is open is a payment control anybody can press.
    let paid: Option<String> = server
        .open_db()
        .query_row(
            "SELECT paid_by FROM daily_statements WHERE id = 1",
            [],
            |r| r.get(0),
        )
        .expect("the statement the refused write named");
    assert_eq!(paid, None, "a refused mark-paid must not have written");

    // And with no credential at all the answer is 401, not 403: the two say
    // different things about whether this server recognises the caller.
    let anonymous = client
        .get(&server.url("/api/admin/billing/summary"), None)
        .await
        .expect("call the manager route anonymously");
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn a_partners_service_status_is_served_and_derived() {
    // Two partners, one day, two contracts. The status is *derived* from the
    // statement table rather than stored, so what decides it is only the
    // deadline and whether the money is in — which is exactly what the tests
    // below move.
    //
    // `beta` is on the product's default 720 minutes, measured from the *period
    // end* rather than from the moment the statement was written. The period
    // ended at midnight, so the deadline is midday; whether the statement is
    // overdue the moment it is issued therefore depends on the time of day the
    // suite happens to run, which is the one thing a test must not depend on.
    //
    // The deadline is moved to a fixed instant in the past instead. That is not
    // a workaround: the status is *derived*, and the row that produces it is
    // the same row the API reads, so moving it moves the answer the same way a
    // real day's passage would. What is left — that the derived status follows
    // the statement table rather than a stored copy — is the claim.
    //
    // `acme` is left alone as the control, and is expected to be active.
    let upstream = MockUpstream::start(Behaviour::chat_stream(1_000, 2_000)).await;
    let server = TestServer::start(
        Spec::new(&upstream)
            .with_keys(vec![
                KeySpec::new("primary")
                    .with_consumer("acme")
                    .with_payment_terms(30 * 24 * 60),
                KeySpec::new("secondary").with_consumer("beta"),
            ])
            .with_manager(ManagerSpec::new(MANAGER_PASSWORD))
            .starting_ledger(seed_a_day(&["acme", "beta"])),
    )
    .await;
    let client = TestClient::new();
    let acme = await_statement(&server, "acme", &yesterday()).await;
    let beta = await_statement(&server, "beta", &yesterday()).await;

    // Both statements carry a deadline, and it is a property of the contract
    // rather than of when the process happened to run: same day, same close,
    // different terms.
    assert!(acme["due_at"].is_string());
    assert!(beta["due_at"].is_string());
    assert_ne!(acme["due_at"], beta["due_at"], "terms are per partner");
    let beta_id = beta_id_of(&beta);

    let statuses = read_status(&server, MANAGER_PASSWORD).await;
    let rows = statuses["statuses"].as_array().expect("a status list");
    assert_eq!(rows.len(), 2, "one status per partner in scope");

    let acme = &rows
        .iter()
        .find(|r| r["consumer_id"] == "acme")
        .expect("acme");
    // `beta` needs no binding here: it has nothing outstanding yet, and the row
    // that makes it interesting is produced below by moving a deadline.

    assert_eq!(acme["status"], "active");
    assert_eq!(
        acme["suspended"], false,
        "terms run a month; nothing is due yet"
    );
    assert_eq!(acme["overdue_statements"], 0);
    assert_eq!(acme["overdue_amount_micro_usd"], 0);
    assert_eq!(acme["overdue_amount"], "$0.000000");
    assert_eq!(acme["reason"], Value::Null, "nothing to explain");

    // Put `beta`'s deadline in the past, as a day of not paying would.
    let past = format!("{}T00:00:00.000000000Z", yesterday());
    server
        .open_db()
        .execute(
            "UPDATE daily_statements SET due_at = ?1 WHERE id = ?2",
            rusqlite::params![past, beta_id],
        )
        .expect("pull the deadline into the past");

    let overdue = read_status(&server, MANAGER_PASSWORD).await;
    let beta = &overdue["statuses"]
        .as_array()
        .expect("a status list")
        .iter()
        .find(|r| r["consumer_id"] == "beta")
        .expect("beta");
    assert_eq!(beta["status"], "suspended");
    assert_eq!(beta["suspended"], true);
    // Two, not one, and that is the design rather than an off-by-one: *every*
    // passed-and-unpaid invoice counts as overdue, and `status_for` is the one
    // that decides which of them can actually suspend. A partner does not stop
    // owing the whole ledger the moment they miss a day, so the figure shown has
    // to be the whole ledger — otherwise the dashboard reports "1 overdue" while
    // two invoices are past their date, and the second one appears as a surprise
    // after the first is paid.
    assert_eq!(
        beta["overdue_statements"], 2,
        "yesterday's bill and the empty day before it are both past their date"
    );
    assert_eq!(
        beta["overdue_amount_micro_usd"], DAY_TOTAL,
        "the empty day costs nothing, so the total is unchanged"
    );
    let reason = &beta["reason"];
    assert_eq!(reason["code"], "invoice_overdue");
    // The one that suspends is the bill with money in it — an empty day is
    // stated, because the record is the point, but there is nothing to enforce
    // and refusing a paying partner's traffic over `$0.000000` would be the
    // worst possible reading of "enforce the payment obligation".
    assert_eq!(
        reason["statement_id"], beta_id,
        "the bill, not the empty day"
    );
    assert_eq!(reason["billing_date"], yesterday());
    assert_eq!(reason["amount_micro_usd"], DAY_TOTAL);
    assert_eq!(reason["amount"], "$0.022500");
    assert!(reason["due_at"].is_string());
    // A reason names the statement it is about, so the dashboard can link the
    // "suspended" banner to the bill that caused it rather than to nothing.
    // A status row has no id of its own — the pair above is the identity — so
    // the reason's id is the only one in scope, and it is the one the row the
    // service status was asked about.
    let id = reason["statement_id"]
        .as_i64()
        .expect("the reason names a statement id");

    // Paying it — through the manager API, so the whole chain is real — removes
    // the suspension, because the status is derived from the same rows the
    // payment wrote. A stored status would have to be cleared separately and
    // would be the one thing a crash could leave behind.
    let paid = client
        .call(
            Method::POST,
            &server.url(&format!("/api/admin/billing/statements/{id}/mark-paid")),
            Some(MANAGER_PASSWORD),
            Bytes::from(
                json!({
                    "paid_by": "accounts",
                    "reference": "wire-2026-09-29-0001",
                    "note": "paid in full by bank transfer",
                })
                .to_string(),
            ),
            &[],
        )
        .await;
    assert_eq!(paid.status, StatusCode::OK, "{}", paid.text());

    let resumed = read_status(&server, MANAGER_PASSWORD).await;
    let beta = &resumed["statuses"]
        .as_array()
        .expect("a status list")
        .iter()
        .find(|r| r["consumer_id"] == "beta")
        .expect("beta")
        .clone();
    assert_eq!(beta["suspended"], false, "a paid partner serves again");
    assert_eq!(beta["status"], "active");
    assert_eq!(beta["reason"], Value::Null, "nothing left to explain");
    // Still one row past its date and unpaid: the harness's anchor, which is a
    // real statement the generator wrote for a day beta used nothing, and which
    // has been past its deadline since yesterday. It stays in the count — that
    // is what the count measures, and a partner does not stop having a past-date
    // statement because it paid a different one — and it still does not suspend,
    // because a zero-amount bill has nothing to enforce.
    //
    // This is the pairing the two fields exist for, and paying the bill is what
    // exposes it: `overdue_statements` is 1 while the partner is `active`. An
    // implementation that derived `suspended` from the count, or that hid a
    // zero-amount statement from the count to make the two agree, would get one
    // of the two answers wrong and there would be no way to tell which.
    assert_eq!(
        beta["overdue_statements"], 1,
        "the zero-amount anchor remains"
    );
    assert_eq!(beta["overdue_amount_micro_usd"], 0, "and it costs nothing");
    assert_eq!(beta["overdue_amount"], "$0.000000");

    // The payment is on the statement itself, and `paid_by` arrived unchanged —
    // a free-text field an operator typed is the record of who said so.
    let after = client
        .get(
            &server.url(&format!("/api/billing/statements/{id}")),
            Some(MANAGER_PASSWORD),
        )
        .await
        .expect("read the statement back")
        .json();
    assert!(after["paid_at"].is_string());
    assert_eq!(after["paid_by"], "accounts");
    assert_eq!(after["payment_reference"], "wire-2026-09-29-0001");
    assert_eq!(after["payment_note"], "paid in full by bank transfer");
    assert_eq!(after["outstanding"], false);
    assert_eq!(after["can_suspend"], false);

    // Paying twice is not refused, and it is not a second payment either. The
    // caller's intent is already satisfied, so the statement is answered as it
    // stands and nothing is written: a 409 would make an operator who clicked
    // twice wonder whether the first click had worked, and a second write would
    // move `paid_at` and lose the record of the first.
    //
    // What makes that a real claim rather than a comment is that the *original*
    // attribution survives it — the second call carried a different `paid_by`,
    // and the statement still names the first one.
    let again = client
        .call(
            Method::POST,
            &server.url(&format!("/api/admin/billing/statements/{id}/mark-paid")),
            Some(MANAGER_PASSWORD),
            Bytes::from(json!({"paid_by": "someone-else"}).to_string()),
            &[],
        )
        .await;
    assert_eq!(
        again.status,
        StatusCode::OK,
        "an intent already met is answered"
    );
    assert_eq!(again.json()["paid_at"], after["paid_at"], "nothing moved");
    let reattributed = client
        .get(
            &server.url(&format!("/api/billing/statements/{id}")),
            Some(MANAGER_PASSWORD),
        )
        .await
        .expect("read the statement back")
        .json();
    assert_eq!(
        reattributed["paid_by"], "accounts",
        "a second call cannot reattribute a payment"
    );
}
