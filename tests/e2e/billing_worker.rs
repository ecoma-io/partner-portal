//! The billing scheduler at the operating-system boundary.
//!
//! Everything here is about what the *process* does over time, which is why it
//! belongs in the e2e tier and not beside the worker:
//!
//! - a day is closed by a running instance, the instance is stopped with
//!   SIGTERM, and a second instance is started against the same ledger — the
//!   day is closed exactly once, and what makes that true is the
//!   `UNIQUE (consumer_id, billing_date)` constraint, not a promise the code
//!   makes about itself;
//! - a process that was not running for several days issues the days it missed,
//!   oldest first, each with its own total;
//! - the walk's anchor is `MAX(billing_date)` **per partner**. A global anchor
//!   would advance past a partner the first one had already stated and would
//!   never retry them — the failure that loses a bill without any error;
//! - the catch-up is bounded by `MAX_CATCHUP_DAYS`, and the bound is announced
//!   rather than applied silently;
//! - today is never billed, because today has not ended.
//!
//! # The clock
//!
//! A day closes once, and it closes about a second after the process starts, so
//! the usage rows have to be written *before* the process exists. Every fixture
//! here seeds them against the ledger the child is about to open, and the real
//! scheduler closes those days on its first tick. No statement is ever inserted
//! by hand: the row under test is one the product wrote.
//!
//! The billing calendar is UTC — the product's default, and the one the walk
//! and the generator agree on — so a "day N" is the UTC date N days before the
//! process's own today and every instant on it is a UTC instant. That keeps the
//! fixture and the scheduler on one calendar without declaring an offset twice.

use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::Duration;

use partner_portal::billing::statements::MAX_CATCHUP_DAYS;
use partner_portal::billing::store::BillingStore;
use partner_portal::billing::{BillingDay, BillingTimezone, Generator};
use partner_portal::ledger::LedgerPool;
use partner_portal::ledger::timefmt;
use rusqlite::Connection;

use crate::harness::*;

/// The two partners the anchor test needs, and the consumers they authenticate
/// as. Distinct consumer ids because the anchor is per partner, and two rows
/// with one id would be one partner.
const FIRST: &str = "e2e-alpha";
const SECOND: &str = "e2e-beta";

/// How long a poll for a condition may run before the test gives up.
///
/// Generous, because the e2e tier is slow and this is a bound rather than a
/// wait: every wait here is for a statement to appear, and the scheduler looks
/// every second, so the real wait is a couple of seconds. Nothing sleeps for a
/// fixed duration hoping something happened.
const PATIENCE: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Days and prices
// ---------------------------------------------------------------------------

/// Today in the billing calendar every instance in this file runs on: UTC, which
/// is `BillingTimezone::default()` and what every configuration here loads.
fn today() -> BillingDay {
    BillingDay::of(timefmt::now(), BillingTimezone::default())
}

/// The billing day `days` before today. `yesterday()` is `days_ago(1)`, the
/// newest day a scheduler can ever close.
fn days_ago(days: i64) -> BillingDay {
    let day = today();
    BillingDay::from_date(day.date() - time::Duration::days(days))
}

/// A UTC instant twelve hours into the billing day — safely inside the period,
/// and never near either boundary, so no assertion here depends on where a day
/// starts.
fn midday(day: BillingDay) -> String {
    format!("{day}T12:00:00.000000000Z")
}

/// The price the fixtures meter at, in micro-dollars per million tokens.
///
/// Ten times the price list the harness seeds on the partner, so a statement
/// built from the partner's *current* list instead of the row's frozen snapshot
/// is off by a factor of ten rather than by a rounding error. That is the same
/// reason `tests/integration/billing.rs` uses its own figures, and the two
/// suites disagreeing with each other is a second thing that would make the
/// wrong source visible.
const INPUT_PER_MILLION: i64 = 950_000;
const CACHED_PER_MILLION: i64 = 23_750;
const OUTPUT_PER_MILLION: i64 = 4_750_000;

const INPUT_TOKENS: i64 = 1_000_000;
const CACHED_TOKENS: i64 = 200_000;
const OUTPUT_TOKENS: i64 = 100_000;

/// What `requests` requests cost in micro-dollars, computed rather than written
/// down: a change to the prices or the token counts above cannot then leave a
/// stale constant behind that makes every total assertion vacuous.
fn total_for(requests: i64) -> i64 {
    let uncached = INPUT_TOKENS - CACHED_TOKENS;
    requests
        * (uncached * INPUT_PER_MILLION
            + CACHED_TOKENS * CACHED_PER_MILLION
            + OUTPUT_TOKENS * OUTPUT_PER_MILLION)
        / 1_000_000
}

// ---------------------------------------------------------------------------
// Fixture rows
// ---------------------------------------------------------------------------

/// One `usage_records` row the fixture writes.
///
/// The tokens follow the metering writer's layout: a snapshot in force, and a
/// `cached_tokens` count present, so the row is billable. `created_at` is the
/// only field a test varies, and it is the one that decides which day (if any)
/// the row lands on.
#[derive(Clone)]
struct UsageRow {
    day: BillingDay,
    consumer: &'static str,
    model: &'static str,
    input_tokens: i64,
    output_tokens: i64,
}

/// A statement to anchor a partner in the past, built by the product's own
/// generator against the seeded rows — never inserted by hand.
struct StatementAnchor {
    consumer: &'static str,
    day: BillingDay,
}

// ---------------------------------------------------------------------------
// The instance under test
// ---------------------------------------------------------------------------

/// A `partner-portal` child process configured for a billing test.
///
/// The billing block is written here rather than through the harness because
/// these tests need three things the harness does not offer together: a
/// configuration whose `close_delay_minutes` is zero (so a day closes the
/// instant it ends and no test waits out a real delay), a `scheduler_interval`
/// of a second, and — for the truncation test — the child's log, which is the
/// only observable that says the bound was hit.
struct BillingInstance {
    child: Child,
    base_url: String,
    /// The caller owns the temp directory; this keeps the paths it resolves to.
    _dir: PathBuf,
    /// Held so the config file stays on disk for the child's watcher.
    _config_path: PathBuf,
    log_path: PathBuf,
    /// Held so the database path the child opened is on record.
    _db: PathBuf,
}

impl BillingInstance {
    /// Start an instance running against `db_path`, seeding the ledger first.
    ///
    /// The two partners are seeded through the harness's own `seed_keys` — so
    /// every partner row and price list this suite reads is one the product
    /// wrote — and then `rows` and `anchors` are planted before the child
    /// exists (a day closes about a second after startup, so usage has to be
    /// there first).
    ///
    /// The caller owns `dir` and `db_path`: the restart test needs the second
    /// instance to open the *same* file the first one wrote.
    async fn start(
        name: &str,
        dir: &Path,
        db_path: &Path,
        upstream: &str,
        rows: &[UsageRow],
        anchors: &[StatementAnchor],
    ) -> Self {
        let config_path = dir.join(format!("config-{name}.yaml"));
        let log_path = dir.join(format!("log-{name}.txt"));

        seed_keys(db_path, &[seed_key(FIRST), seed_key(SECOND)]);
        apply_ledger_fixtures(db_path, rows, anchors);

        let (child, port) = spawn(&config_path, &log_path, dir, db_path, upstream);
        let base_url = format!("http://127.0.0.1:{port}");

        Self {
            child,
            base_url,
            _dir: dir.to_path_buf(),
            _config_path: config_path,
            log_path,
            _db: db_path.to_path_buf(),
        }
    }

    /// Start an instance against a ledger another instance already seeded.
    ///
    /// A rolling update is two instances over one database, and the second one
    /// must use the rows the first one left behind. Seeding again would issue
    /// fresh plaintexts and hit the "one active key per partner" partial unique
    /// index, which is exactly what the first instance's credentials are for.
    async fn start_existing(name: &str, dir: &Path, db_path: &Path, upstream: &str) -> Self {
        let config_path = dir.join(format!("config-{name}.yaml"));
        let log_path = dir.join(format!("log-{name}.txt"));

        let (child, port) = spawn(&config_path, &log_path, dir, db_path, upstream);
        let base_url = format!("http://127.0.0.1:{port}");

        Self {
            child,
            base_url,
            _dir: dir.to_path_buf(),
            _config_path: config_path,
            log_path,
            _db: db_path.to_path_buf(),
        }
    }

    /// The child's process id.
    fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Deliver a signal with `kill(2)` directly — the e2e harness's own way, so
    /// the test does not depend on a `kill` binary being present.
    fn signal(&self, signal: libc::c_int) {
        let rc = unsafe { libc::kill(self.pid() as libc::pid_t, signal) };
        assert_eq!(
            rc,
            0,
            "kill({}, {signal}) failed: {}",
            self.pid(),
            std::io::Error::last_os_error()
        );
    }

    /// Wait for the child to exit; `None` on timeout.
    async fn wait_exit(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let deadline = std::time::Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) if std::time::Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Ok(None) => return None,
                Err(_) => return None,
            }
        }
    }

    /// SIGTERM and wait for exit.
    async fn terminate_and_wait(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        self.signal(libc::SIGTERM);
        self.wait_exit(timeout).await
    }
}

impl Drop for BillingInstance {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

/// Spawn the child with the billing block the e2e tier needs.
///
/// `close_delay_minutes: 0` closes a day the instant it ends, `scheduler_interval_secs: 1`
/// ticks once a second, and `PARTNER_PORTAL_SMTP_*` are unset so no statement is
/// emailed — the statements themselves are what these tests read.
///
/// The log is the child's stderr, redirected to `log_path`: `tracing` writes
/// there, and it is the only observable that can say the catch-up bound was hit.
/// The port comes from `PARTNER_PORTAL_LISTEN`, exactly as in the harness.
fn spawn(
    config_path: &Path,
    log_path: &Path,
    dir: &Path,
    db_path: &Path,
    upstream: &str,
) -> (Child, u16) {
    let port = free_port();
    let yaml = format!(
        r#"server:
  shutdown_grace_secs: 1
  sse_poll_interval_ms: 100
upstream:
  base_url: "{upstream}"
  api_key: "{UPSTREAM_KEY}"
  timeout_secs: 15
  connect_timeout_secs: 2
manager:
  password: "{MANAGER_PASSWORD}"
database:
  path: "{db}"
  queue_size: 5000
  batch_size: 50
  batch_timeout_ms: 20
  retention_interval_secs: 3600
billing:
  timezone_offset_minutes: 0
  close_delay_minutes: 0
  scheduler_interval_secs: 1
  email:
    enabled: false
"#,
        db = db_path.display(),
    );
    write_raw(config_path, &yaml);

    let log = std::fs::File::create(log_path).expect("create the instance's log file");
    let dup = log
        .try_clone()
        .expect("duplicate the log handle for stderr");
    Command::new(binary_path())
        .env("PARTNER_PORTAL_CONFIG", config_path)
        .env("PARTNER_PORTAL_LISTEN", format!("127.0.0.1:{port}"))
        .env(
            "PARTNER_PORTAL_API_KEY_SECRET",
            std::str::from_utf8(TEST_SECRET).expect("the test secret is ASCII"),
        )
        .env("RUST_LOG", "warn")
        .current_dir(dir)
        .stdin(Stdio::null())
        .stdout(Stdio::from(log))
        .stderr(Stdio::from(dup))
        .spawn()
        .map(|child| (child, port))
        .expect("failed to spawn partner-portal")
}

/// Write the usage rows, then the anchors, into a ledger the child will open.
///
/// The order matters and is the reason the anchors are built here rather than
/// as raw inserts: an anchor is a statement, and a statement is built by
/// aggregating a day of usage. The generator runs against the rows as they are.
fn apply_ledger_fixtures(db_path: &Path, rows: &[UsageRow], anchors: &[StatementAnchor]) {
    let pool = Arc::new(LedgerPool::new(db_path.to_path_buf()).expect("create the test ledger"));
    let store = BillingStore::new(Arc::clone(&pool));

    if !rows.is_empty() {
        pool.write(|conn| {
            // The seeded request ids are a counter, and a fresh ledger already
            // has rows in it, so the counter starts where the file is rather
            // than at zero — a collision here would surface as a confusing
            // uniqueness error rather than as the fixture fault it is.
            let first: i64 =
                conn.query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))?;
            for (offset, row) in rows.iter().enumerate() {
                let next = first + offset as i64;
                conn.execute(
                    "INSERT INTO usage_records (
                         request_id, created_at, consumer_id, model, endpoint, streaming,
                         http_status, request_status, usage_status, input_tokens, output_tokens,
                         cached_tokens, ttft_ms, duration_ms, input_price_snapshot,
                         cached_input_price_snapshot, output_price_snapshot, instance_id) \
                     VALUES (?1, ?2, ?3, ?4, 'chat_completions', 0, 200, 'completed', \
                             'available', ?5, ?6, ?7, NULL, 120, ?8, ?9, ?10, 'e2e-billing')",
                    rusqlite::params![
                        format!("e2e-billing-{next}"),
                        midday(row.day),
                        row.consumer,
                        row.model,
                        row.input_tokens,
                        row.output_tokens,
                        CACHED_TOKENS,
                        INPUT_PER_MILLION,
                        CACHED_PER_MILLION,
                        OUTPUT_PER_MILLION,
                    ],
                )?;
            }
            Ok(())
        })
        .expect("write the seeded usage rows");
    }

    // The generator, at the configuration the instance is about to be started
    // with. The product's own default is a five-minute close delay, and a
    // statement built under a delay the process will not use would be a fixture
    // asserting against a shape no deployment produces.
    let generator = Generator::new(BillingTimezone::default(), 0);
    for anchor in anchors {
        let Some(partner) = store
            .get_partner(anchor.consumer)
            .expect("read the seeded partner")
        else {
            panic!(
                "no partner row for {}; the fixture seeds one",
                anchor.consumer
            );
        };
        let conn = pool.reader().expect("a reader connection for the anchor");
        let draft = generator
            .statement_for_day(
                &conn,
                &partner,
                anchor.day,
                generator.cutoff_for(anchor.day),
            )
            .expect("build the anchoring statement");
        store
            .write_statement(&draft)
            .expect("write the anchoring statement");
    }
}

fn seed_key(consumer: &str) -> SeedKey {
    SeedKey::new(consumer, consumer)
}

// ---------------------------------------------------------------------------
// Assertions read the ledger's own tables
// ---------------------------------------------------------------------------

/// Every statement the ledger holds for a partner, newest first: the row's own
/// totals, not an API's rendering of them. The API is not a faithful view of
/// every column, and a statement is one place where `NULL` versus a number is
/// the whole point.
fn statements_for(db_path: &Path, consumer: &str) -> Vec<StatementRow> {
    let conn = Connection::open(db_path).expect("open the ledger to read statements");
    let mut stmt = conn
        .prepare(
            "SELECT billing_date, total_amount_micro_usd, incomplete_usage_count \
             FROM daily_statements WHERE consumer_id = ?1 \
             ORDER BY billing_date DESC",
        )
        .expect("prepare the statement read");
    let rows = stmt
        .query_map([consumer], |row| {
            Ok(StatementRow {
                billing_date: row.get(0)?,
                total_micro_usd: row.get(1)?,
                incomplete_usage_count: row.get(2)?,
            })
        })
        .expect("read the statement rows");
    rows.map(|r| r.expect("a readable statement row")).collect()
}

#[derive(Debug)]
struct StatementRow {
    billing_date: String,
    total_micro_usd: i64,
    incomplete_usage_count: i64,
}

/// Poll the ledger until `predicate` holds, or fail at `PATIENCE`.
async fn wait_until_ledger(
    db_path: &Path,
    log_path: &Path,
    mut predicate: impl FnMut(&Connection) -> bool,
) {
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        let conn = Connection::open(db_path).expect("open the ledger to poll");
        let done = predicate(&conn);
        drop(conn);
        if done {
            return;
        }
        if std::time::Instant::now() >= deadline {
            let log = std::fs::read_to_string(log_path).unwrap_or_default();
            let ledger: String = Connection::open(db_path)
                .ok()
                .and_then(|conn| {
                    let mut stmt = conn
                        .prepare(
                            "SELECT consumer_id || '/' || billing_date || ' $' || \
                             total_amount_micro_usd FROM daily_statements \
                             ORDER BY billing_date",
                        )
                        .ok()?;
                    let rows = stmt
                        .query_map([], |r| r.get::<_, String>(0))
                        .ok()?
                        .collect::<Result<Vec<_>, _>>()
                        .ok()?;
                    Some(rows.join("\n"))
                })
                .unwrap_or_else(|| "<unreadable>".to_string());
            panic!(
                "the ledger never satisfied the condition within {PATIENCE:?};\nstatements:\n{ledger}\nchild log:\n{log}",
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

// ---------------------------------------------------------------------------
// The three process-level behaviours
// ---------------------------------------------------------------------------

/// A day is closed exactly once across a restart.
///
/// Two instances over one ledger is what a rolling update is, and the second
/// one must not write the day the first one already closed. The property is the
/// `UNIQUE (consumer_id, billing_date)` constraint, and the observable is a
/// statement row that appears once and never changes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_day_closed_by_one_instance_is_not_closed_again_by_the_next() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (_mock, upstream) = start_mock_upstream().await;

    // One request yesterday, so the day the first instance closes has something
    // on it. The row is seeded before the child exists (the clock section).
    let rows = [UsageRow {
        day: days_ago(1),
        consumer: FIRST,
        model: "e2e-ring",
        input_tokens: INPUT_TOKENS,
        output_tokens: OUTPUT_TOKENS,
    }];
    let anchors = [StatementAnchor {
        consumer: FIRST,
        day: days_ago(2),
    }];

    let mut first =
        BillingInstance::start("first", dir.path(), &db_path, &upstream, &rows, &anchors).await;
    assert!(
        wait_for_status(&first.base_url, "/readyz", 200, PATIENCE).await,
        "the first instance never became ready"
    );

    // The scheduler closes yesterday on its first tick. The statement appears
    // within a couple of seconds; nothing sleeps for a fixed duration. The
    // anchor at days_ago(2) is itself a statement row, so FIRST holds two: the
    // anchor and the day the walk actually wrote.
    let expected = total_for(1);
    wait_until_ledger(&db_path, &first.log_path, |conn| {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM daily_statements WHERE consumer_id = ?1",
                [FIRST],
                |row| row.get(0),
            )
            .unwrap();
        count == 2
    })
    .await;
    let after_first = statements_for(&db_path, FIRST);
    assert_eq!(after_first.len(), 2);
    assert_eq!(after_first[0].billing_date, days_ago(1).to_string());
    assert_eq!(after_first[0].total_micro_usd, expected);
    assert_eq!(after_first[0].incomplete_usage_count, 0);
    assert_eq!(after_first[1].billing_date, days_ago(2).to_string());
    assert_eq!(after_first[1].total_micro_usd, 0);

    // SIGTERM, then a second instance over the same ledger. Its first tick
    // finds the day already stated and writes nothing: the count stays one and
    // the single row's total is untouched.
    assert!(
        first.terminate_and_wait(PATIENCE).await.unwrap().success(),
        "the first instance must exit cleanly"
    );

    let mut second =
        BillingInstance::start_existing("second", dir.path(), &db_path, &upstream).await;
    assert!(
        wait_for_status(&second.base_url, "/readyz", 200, PATIENCE).await,
        "the second instance never became ready"
    );

    // Let the second instance's scheduler actually run for a few ticks, then
    // read the ledger again. The walk uses `MAX(billing_date)` as its anchor,
    // so the stated yesterday is the starting point: the second walk reaches
    // nothing and the original two rows are untouched — the anchor and the day.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let after_second = statements_for(&db_path, FIRST);
    assert_eq!(
        after_second.len(),
        2,
        "the anchor and the stated day, across a restart"
    );
    assert_eq!(after_second[0].billing_date, days_ago(1).to_string());
    assert_eq!(after_second[0].total_micro_usd, expected);
    assert_eq!(after_second[0].incomplete_usage_count, 0);
    assert_eq!(after_second[1].billing_date, days_ago(2).to_string());
    assert_eq!(after_second[1].total_micro_usd, 0);

    let _ = second.terminate_and_wait(PATIENCE).await;
}

/// A process that was not running for several days issues the days it missed,
/// oldest first, each with its own total.
///
/// The fixture plants usage on three distinct past days and no statements, so
/// the walk (anchored at the day before the partner existed) must issue all
/// three on one pass, in calendar order, each carrying exactly its own day's
/// tokens.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_long_absence_catches_up_oldest_first_with_each_days_own_total() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (_mock, upstream) = start_mock_upstream().await;

    let rows = [1, 2, 3]
        .map(|days| UsageRow {
            day: days_ago(days),
            consumer: FIRST,
            model: "e2e-ring",
            input_tokens: INPUT_TOKENS,
            output_tokens: OUTPUT_TOKENS,
        })
        .to_vec();
    let anchors = [StatementAnchor {
        consumer: FIRST,
        day: days_ago(4),
    }];

    let mut instance =
        BillingInstance::start("catchup", dir.path(), &db_path, &upstream, &rows, &anchors).await;
    assert!(
        wait_for_status(&instance.base_url, "/readyz", 200, PATIENCE).await,
        "the instance never became ready"
    );

    let expected_total = total_for(1);
    // The fixture anchors at days_ago(4), so the partner holds that anchor
    // *plus* the three caught-up days: four rows once the walk has run.
    wait_until_ledger(&db_path, &instance.log_path, |conn| {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM daily_statements WHERE consumer_id = ?1",
                [FIRST],
                |row| row.get(0),
            )
            .unwrap();
        count == 4
    })
    .await;

    let stated = statements_for(&db_path, FIRST);
    assert_eq!(stated.len(), 4);
    // Newest first in the read; the walk wrote oldest first, so in calendar
    // order the three stated days each carry their own day's total, and the
    // anchor (the oldest) carries the zero the fixture gave it.
    assert_eq!(stated[3].billing_date, days_ago(4).to_string());
    assert_eq!(stated[3].total_micro_usd, 0);
    assert_eq!(stated[2].billing_date, days_ago(3).to_string());
    assert_eq!(stated[2].total_micro_usd, expected_total);
    assert_eq!(stated[1].billing_date, days_ago(2).to_string());
    assert_eq!(stated[1].total_micro_usd, expected_total);
    assert_eq!(stated[0].billing_date, days_ago(1).to_string());
    assert_eq!(stated[0].total_micro_usd, expected_total);
    for row in &stated {
        assert_eq!(row.incomplete_usage_count, 0);
    }

    let _ = instance.terminate_and_wait(PATIENCE).await;
}

/// The walk's anchor is per partner: a statement written for one partner does
/// not advance another partner's walk.
///
/// Both partners have usage on the same day and no statements. If the walk
/// used a global anchor that moved as soon as *any* partner was stated, the
/// second partner's day would be skipped — a bill lost without an error. The
/// per-partner anchor means each partner's day is stated on the same pass.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_anchor_is_per_partner_and_one_partner_never_advances_another() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (_mock, upstream) = start_mock_upstream().await;

    let rows = [
        UsageRow {
            day: days_ago(1),
            consumer: FIRST,
            model: "e2e-ring",
            input_tokens: INPUT_TOKENS,
            output_tokens: OUTPUT_TOKENS,
        },
        UsageRow {
            day: days_ago(1),
            consumer: SECOND,
            model: "e2e-ring",
            input_tokens: INPUT_TOKENS,
            output_tokens: OUTPUT_TOKENS,
        },
    ];
    let anchors = [
        StatementAnchor {
            consumer: FIRST,
            day: days_ago(2),
        },
        StatementAnchor {
            consumer: SECOND,
            day: days_ago(2),
        },
    ];

    let mut instance =
        BillingInstance::start("anchor", dir.path(), &db_path, &upstream, &rows, &anchors).await;
    assert!(
        wait_for_status(&instance.base_url, "/readyz", 200, PATIENCE).await,
        "the instance never became ready"
    );

    let expected = total_for(1);
    // Both partners hold an anchor at days_ago(2) — two rows already there —
    // and each gets one more for days_ago(1): four rows once the walk has run.
    wait_until_ledger(&db_path, &instance.log_path, |conn| {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM daily_statements \
                 WHERE consumer_id IN (?1, ?2)",
                [FIRST, SECOND],
                |row| row.get(0),
            )
            .unwrap();
        count == 4
    })
    .await;

    for consumer in [FIRST, SECOND] {
        let rows = statements_for(&db_path, consumer);
        assert_eq!(
            rows.len(),
            2,
            "anchor plus the partner's own day: {consumer}"
        );
        assert_eq!(rows[0].billing_date, days_ago(1).to_string());
        assert_eq!(rows[0].total_micro_usd, expected);
        assert_eq!(rows[0].incomplete_usage_count, 0);
        assert_eq!(rows[1].billing_date, days_ago(2).to_string());
        assert_eq!(rows[1].total_micro_usd, 0);
    }

    let _ = instance.terminate_and_wait(PATIENCE).await;
}

/// The catch-up is bounded by `MAX_CATCHUP_DAYS`, and the bound is announced —
/// the truncation says so in the log rather than being applied silently.
///
/// A partner whose last statement is further back than the bound must not get
/// a statement per day since then; the walk starts at the bound and writes
/// `MAX_CATCHUP_DAYS` statements. The log line is the observable that says the
/// hole exists — the only thing that distinguishes "walked to the bound" from
/// "walked to a partner's creation day".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_catch_up_bound_is_announced_in_the_log_when_it_truncates() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (_mock, upstream) = start_mock_upstream().await;

    // A partner with no statements, created forty days ago: the walk's anchor
    // is the day before they existed, which is further back than the month the
    // walk may cover. `seed_keys` stamps `created_at = now`, so the row is
    // backdated by hand — the fixture is about a partner the deployment has
    // known for a while, not about a partner minted this second.
    let rows = [UsageRow {
        day: days_ago(1),
        consumer: FIRST,
        model: "e2e-ring",
        input_tokens: INPUT_TOKENS,
        output_tokens: OUTPUT_TOKENS,
    }];
    let anchors = [];
    seed_keys(&db_path, &[seed_key(FIRST)]);
    apply_ledger_fixtures(&db_path, &rows, &anchors);
    {
        let pool = Arc::new(
            LedgerPool::new(db_path.clone()).expect("open the ledger to backdate the partner"),
        );
        pool.write(|conn| {
            conn.execute(
                "UPDATE partners SET created_at = ?1, updated_at = ?1 \
                 WHERE consumer_id = ?2",
                rusqlite::params![midday(days_ago(40)), FIRST],
            )
            .map(|_| ())
        })
        .expect("backdate the partner's creation");
    }

    let mut instance =
        BillingInstance::start_existing("bound", dir.path(), &db_path, &upstream).await;
    assert!(
        wait_for_status(&instance.base_url, "/readyz", 200, PATIENCE).await,
        "the instance never became ready"
    );

    // The walk writes the whole bound — `MAX_CATCHUP_DAYS` statements from
    // `oldest_closeable(yesterday)` through yesterday — not one per day since
    // the partner was created. Each of the 30 empty days is a zero statement;
    // yesterday carries the one seeded request.
    wait_until_ledger(&db_path, &instance.log_path, |conn| {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM daily_statements WHERE consumer_id = ?1",
                [FIRST],
                |row| row.get(0),
            )
            .unwrap();
        count == MAX_CATCHUP_DAYS
    })
    .await;
    let stated = statements_for(&db_path, FIRST);
    assert_eq!(stated.len(), MAX_CATCHUP_DAYS as usize);
    assert_eq!(stated[0].billing_date, days_ago(1).to_string());
    assert_eq!(stated[0].total_micro_usd, total_for(1));
    // The rest of the bound is empty days: the walk does not invent usage that
    // is not there, so each is stated at zero.
    for row in &stated[1..] {
        assert_eq!(row.total_micro_usd, 0);
        assert_eq!(row.incomplete_usage_count, 0);
    }

    // The bound was hit, and the worker says so. `billing_statement_backlog_truncated`
    // is the only observable that distinguishes "walked to the bound" from
    // "walked to a partner's creation day".
    wait_for_log_line(&instance.log_path, "billing_statement_backlog_truncated").await;

    let _ = instance.terminate_and_wait(PATIENCE).await;
}

/// Today is never billed: a closeable day is a day that ended.
///
/// The fixture plants usage on today itself, and no statement may name today —
/// the day is still running and a request accepted at 23:59:59 has not been
/// finalised. The scheduler must leave it alone entirely: no statement, and the
/// usage remains unbilled where it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn today_is_never_billed_because_today_has_not_ended() {
    let dir = tempfile::TempDir::new().unwrap();
    let db_path = dir.path().join("ledger.db");
    let (_mock, upstream) = start_mock_upstream().await;

    let rows = [UsageRow {
        day: today(),
        consumer: FIRST,
        model: "e2e-ring",
        input_tokens: INPUT_TOKENS,
        output_tokens: OUTPUT_TOKENS,
    }];
    let anchors = [StatementAnchor {
        consumer: FIRST,
        day: days_ago(1),
    }];

    let mut instance =
        BillingInstance::start("tomorrow", dir.path(), &db_path, &upstream, &rows, &anchors).await;
    assert!(
        wait_for_status(&instance.base_url, "/readyz", 200, PATIENCE).await,
        "the instance never became ready"
    );

    // A genuine absence needs real time, but a bounded one: the scheduler ticks
    // every second, and several ticks with no statement for today is the
    // observable. Nothing sleeps "long enough"; the bound is fixed and the
    // assertion is a negative one the ticks are allowed to disprove.
    tokio::time::sleep(Duration::from_secs(4)).await;
    let stated = statements_for(&db_path, FIRST);
    // The anchor at days_ago(1) is the one statement the partner holds; today
    // must never appear among them — the day is still running and a request
    // accepted at 23:59:59 has not been finalised.
    assert_eq!(stated.len(), 1, "only the anchor, never today: {stated:?}");
    assert_eq!(stated[0].billing_date, days_ago(1).to_string());
    assert_eq!(stated[0].total_micro_usd, 0);

    // And the usage is where it was: no row was consumed into a statement.
    let conn = Connection::open(&db_path).unwrap();
    let usage: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM usage_records \
             WHERE consumer_id = ?1 AND created_at LIKE ?2",
            rusqlite::params![FIRST, format!("{}T%", today())],
            |row| row.get(0),
        )
        .unwrap();
    assert_eq!(usage, 1);

    let _ = instance.terminate_and_wait(PATIENCE).await;
}

// ---------------------------------------------------------------------------
// Log reading
// ---------------------------------------------------------------------------

/// Wait for `needle` to appear in a child's log, or fail at `PATIENCE`.
async fn wait_for_log_line(log_path: &Path, needle: &str) {
    let deadline = std::time::Instant::now() + PATIENCE;
    loop {
        let content = std::fs::read_to_string(log_path).unwrap_or_default();
        if content.contains(needle) {
            return;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "the log never contained {needle:?} within {PATIENCE:?}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}
