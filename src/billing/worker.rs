//! The billing scheduler: close days, then send what was closed.
//!
//! # What a tick does, in order
//!
//! 1. Ask the generator which day is closeable at `now` — the newest day whose
//!    period ended and whose close delay has passed. Nothing closeable means
//!    nothing to do, and that is the ordinary case for most of the day.
//! 2. For each partner, walk forward from the partner's own last statement to
//!    that day and write one statement per day. The walk is idempotent: the
//!    table's `UNIQUE (consumer_id, billing_date)` decides, not this code.
//! 3. Claim and send the statement emails that are due.
//! 4. Observe each partner's derived service status and log the transitions.
//!
//! # Why statements do not go through the metering queue
//!
//! The ledger's writer task exists to apply backpressure to *requests*: it is
//! the component that must never drop a metered record, and it is sized and
//! tuned for that. Billing is not on the request path and must not be able to
//! stall it — a statement generator that shared the queue would make a slow
//! aggregation a reason to stop accepting traffic. Instead each step here is a
//! short `LedgerPool::read`/`write` of its own, on `spawn_blocking`, and a tick
//! that is slow is a tick that finishes late.
//!
//! # Why the anchor is per partner
//!
//! The walk's anchor is `MAX(billing_date)` for *that* partner, not a global
//! one, and the reason is retries: if a day is written for one partner and
//! fails for another, a global anchor would have already moved past it, and the
//! second partner's statement would never be attempted again. The failure only
//! happens when something has already gone wrong, which is exactly when a
//! silent loss is least likely to be noticed.
//!
//! A partner with no statement at all is anchored at the day *before* they were
//! created, so a new partner is not billed for days they did not exist. That
//! also makes the first run after a deployment (or after an upgrade that
//! created the partner rows) a no-op rather than a month of back-billed
//! statements for usage that predates pricing.
//!
//! # Why a day with no usage is still written
//!
//! A day with nothing billable is a real statement with a real total of zero:
//! it is the record of that day, it is what the partner sees when they ask "did
//! you bill me for the 3rd", and it is what makes the anchor move so no day is
//! ever walked twice. A zero statement asks for nothing — it is not emailed
//! ([`crate::billing::store::BillingStore::claim_emails`]) and it cannot suspend
//! anyone ([`crate::billing::status`]).
//!
//! # Suspension is observed here, not decided here
//!
//! The status is derived from the statement table by
//! [`crate::billing::status::status_for`] in every place that needs it. This
//! module only *notices* when it changes, so that `partner_suspended` and
//! `partner_resumed` appear in the log at the moment they become true. The log
//! line is an aid; deleting it would change nothing about who is served, which
//! is the property worth having.

use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use time::OffsetDateTime;
use tokio::sync::watch;
use tracing::{debug, info, warn};

use crate::auth::Scope;
use crate::billing::email::{self, Settings};
use crate::billing::partner::Partner;
use crate::billing::period::{BillingDay, BillingTimezone};
use crate::billing::statements::Generator;
use crate::billing::status::{OverdueRow, ServiceStatus, status_for};
use crate::billing::store::{BillingError, BillingStore, EmailOutcome, Statement};
use crate::billing::{EMAIL_CLAIM_TTL_MINUTES, EMAIL_RETRY_BASE_MINUTES, EMAIL_RETRY_MAX_MINUTES};
use crate::config::{BillingConfig, SmtpCredentials};
use crate::ledger::{LedgerPool, timefmt};

/// How many statements one tick will try to email.
///
/// A bound rather than "all of them" because the email phase runs inside one
/// `spawn_blocking` call and each send is a socket conversation with a 10s
/// connect timeout: an unbounded batch is a tick that can outlive its interval.
/// Whatever is left is claimed by the next tick, oldest due date first.
pub const EMAIL_BATCH: i64 = 50;

/// The shortest scheduler interval this will run at, in seconds.
///
/// `scheduler_interval_secs: 0` is a busy loop with a database read in it. The
/// dev loop wants a short interval; one second is as short as it gets.
const MIN_INTERVAL_SECS: u64 = 1;

/// One tick's tally, for the log line and for tests.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct Tick {
    /// Days the walk reached, across every partner.
    pub days: usize,
    /// Statements written by this tick.
    pub statements_created: usize,
    /// Statements that already existed (a re-run, or the peer instance won).
    pub statements_existing: usize,
    /// Statements written that carry usage the product could not measure.
    pub statements_incomplete: usize,
    /// Emails the relay accepted.
    pub emails_sent: usize,
    /// Emails that failed and were scheduled for a retry.
    pub emails_failed: usize,
    /// Claimed statements that could not be prepared (a missing partner row).
    pub emails_skipped: usize,
    /// (partner, day) writes that raised an error.
    pub failures: usize,
}

impl Tick {
    pub fn did_anything(&self) -> bool {
        self.statements_created > 0 || self.emails_sent > 0 || self.emails_failed > 0
    }
}

/// The scheduler, wired to a pool, a config and an instance identity.
#[derive(Clone)]
pub struct Worker {
    store: BillingStore,
    generator: Generator,
    /// `None` when statement email is switched off or unconfigured. Not an
    /// error: a deployment that does not email statements is a supported one,
    /// and the statements are written either way.
    email: Option<Settings>,
    interval: Duration,
    instance_id: String,
    /// The last status this worker observed per consumer, so a transition can be
    /// logged once instead of once per tick.
    observed: Arc<Mutex<HashMap<String, bool>>>,
}

impl Worker {
    /// Build a worker from the billing configuration.
    ///
    /// Returns an error only for a billing timezone that cannot be used, which
    /// the config loader also refuses — so this is a second reading of a fact
    /// that has already been checked, for the caller that built a
    /// [`BillingConfig`] some other way.
    pub fn new(
        pool: Arc<LedgerPool>,
        billing: &BillingConfig,
        credentials: Option<SmtpCredentials>,
        instance_id: String,
    ) -> Result<Self, crate::billing::period::TimezoneError> {
        let timezone = BillingTimezone::from_offset_minutes(billing.timezone_offset_minutes)?;
        let settings = Settings::from_config(&billing.email, credentials);
        let email = if billing.email.enabled && settings.is_complete() {
            Some(settings)
        } else {
            if billing.email.enabled {
                // Enabled but incomplete. The loader refuses this, so reaching
                // here means a config that was assembled rather than parsed;
                // saying so once is better than a statement that silently never
                // arrives.
                warn!("billing_email_unconfigured");
            }
            None
        };
        Ok(Self {
            store: BillingStore::new(pool),
            generator: Generator::new(timezone, billing.close_delay_minutes),
            email,
            interval: Duration::from_secs(billing.scheduler_interval_secs.max(MIN_INTERVAL_SECS)),
            instance_id,
            observed: Arc::new(Mutex::new(HashMap::new())),
        })
    }

    pub fn interval(&self) -> Duration {
        self.interval
    }

    /// Whether this worker will attempt to send statement email at all.
    pub fn sends_email(&self) -> bool {
        self.email.is_some()
    }

    /// Run the scheduler for the life of the process.
    ///
    /// Each tick is blocking SQLite work and, when there is mail to send, a
    /// blocking socket conversation — both belong on `spawn_blocking`, and the
    /// select below is what makes the process able to stop while one is
    /// running. A tick in flight when the stop signal arrives is *not*
    /// cancelled: it finishes, and the loop then exits. Nothing here is
    /// wake-on-shutdown critical, but a half-written statement would be, so the
    /// signal is only ever observed between ticks.
    pub fn spawn(self, mut stop_rx: watch::Receiver<()>) {
        let interval = self.interval;
        tokio::spawn(async move {
            loop {
                let worker = self.clone();
                match tokio::task::spawn_blocking(move || worker.run_once(timefmt::now())).await {
                    Ok(Ok(tick)) => {
                        if tick.did_anything() {
                            info!(
                                statements_created = tick.statements_created,
                                statements_existing = tick.statements_existing,
                                incomplete = tick.statements_incomplete,
                                emails_sent = tick.emails_sent,
                                emails_failed = tick.emails_failed,
                                failures = tick.failures,
                                "billing_tick"
                            );
                        } else {
                            debug!("billing tick had nothing to do");
                        }
                    }
                    Ok(Err(e)) => {
                        warn!(error = %e, "billing tick failed; will retry next interval")
                    }
                    Err(e) => warn!(error = %e, "billing task panicked"),
                }

                tokio::select! {
                    _ = stop_rx.changed() => break,
                    _ = tokio::time::sleep(interval) => {}
                }
            }
        });
    }

    /// One tick, against a caller-supplied `now`.
    ///
    /// `now` is a parameter rather than read from the clock here so that the
    /// scheduler's decisions — which day is closeable, what the anchor is, when
    /// a retry comes due — are testable at an instant a test chooses. Calls that
    /// *write* a timestamp still take the real clock, because a row stamped with
    /// a test's idea of now would be a row the database disagrees with.
    pub fn run_once(&self, now: OffsetDateTime) -> Result<Tick, BillingError> {
        let mut tick = Tick::default();
        self.close_days(now, &mut tick)?;
        self.send_emails(now, &mut tick);
        self.observe_status(now);
        Ok(tick)
    }

    /// Write every statement the walk reaches, for every partner.
    fn close_days(&self, now: OffsetDateTime, tick: &mut Tick) -> Result<(), BillingError> {
        let Some(through) = self.generator.closable_through(now) else {
            // Before the first closeable day of the deployment: there is no day
            // that has finished and settled yet.
            return Ok(());
        };

        for partner in self.store.list_partners()? {
            let anchor = self.anchor_for(&partner, through)?;
            let days = self
                .generator
                .days_to_close(Some(&anchor.to_string()), through);

            if !days.is_empty()
                && self
                    .generator
                    .catchup_truncated(Some(&anchor.to_string()), through)
            {
                // The bound (a month) cut the walk short. That is a hole in the
                // statement history and an operator needs to know it is there
                // rather than discover it in a partner's question about a bill.
                warn!(
                    consumer_id = %partner.consumer_id,
                    anchor = %anchor,
                    through = %through,
                    days = days.len(),
                    "billing_statement_backlog_truncated"
                );
            }

            for day in days {
                tick.days += 1;
                self.close_day(&partner, day, tick);
            }
        }
        Ok(())
    }

    /// Write one (partner, day) statement, reporting its outcome.
    ///
    /// Every error is caught here rather than propagated: one partner's day
    /// failing must not stop the other nine from being billed, and the failed
    /// day is retried on the next tick because it never became the anchor.
    fn close_day(&self, partner: &Partner, day: BillingDay, tick: &mut Tick) {
        let cutoff = self.generator.cutoff_for(day);
        // A reader connection of its own, through the same pool the store uses:
        // the generator's aggregate runs against `usage_records`, and it must
        // not hold the writer lock while it does. WAL is what lets it read
        // while the metering writer is committing.
        let draft = match self
            .store
            .pool()
            .reader()
            .map_err(BillingError::from)
            .map(|conn| {
                self.generator
                    .statement_for_day(&conn, partner, day, cutoff)
            }) {
            Ok(Ok(draft)) => draft,
            Ok(Err(e)) | Err(e) => {
                tick.failures += 1;
                warn!(
                    consumer_id = %partner.consumer_id,
                    billing_date = %day,
                    error = %e,
                    "billing_statement_read_failed"
                );
                return;
            }
        };

        match self.store.write_statement(&draft) {
            Ok(outcome) => {
                let statement = outcome.statement();
                if outcome.was_created() {
                    tick.statements_created += 1;
                    info!(
                        statement_id = statement.id,
                        consumer_id = %statement.consumer_id,
                        billing_date = %statement.billing_date,
                        billing_mode = %statement.billing_mode,
                        amount = %statement.total(),
                        lines = draft.lines.len(),
                        incomplete_usage_count = statement.incomplete_usage_count,
                        "billing_statement_created"
                    );
                    if statement.has_incomplete_usage() {
                        tick.statements_incomplete += 1;
                        // Louder than the creation line, and separate, because
                        // this is the one thing on a statement that can make it
                        // unenforceable: it never suspends its partner, so it is
                        // also the one that can go unnoticed for a long time.
                        warn!(
                            statement_id = statement.id,
                            consumer_id = %statement.consumer_id,
                            billing_date = %statement.billing_date,
                            incomplete_usage_count = statement.incomplete_usage_count,
                            "billing_statement_incomplete"
                        );
                    }
                } else {
                    // Not a failure: the row is there, and it is there once.
                    tick.statements_existing += 1;
                    debug!(
                        statement_id = statement.id,
                        consumer_id = %statement.consumer_id,
                        billing_date = %statement.billing_date,
                        reason = "already exists",
                        "billing_statement_skipped"
                    );
                }
            }
            Err(e) => {
                tick.failures += 1;
                warn!(
                    consumer_id = %partner.consumer_id,
                    billing_date = %day,
                    error = %e,
                    "billing_statement_write_failed"
                );
            }
        }
    }

    /// The day the walk starts from for this partner.
    fn anchor_for(
        &self,
        partner: &Partner,
        through: BillingDay,
    ) -> Result<BillingDay, BillingError> {
        if let Some(day) = self
            .store
            .last_statement_date_for(&partner.consumer_id)?
            .and_then(|text| BillingDay::parse(&text))
        {
            return Ok(day);
        }
        // No statement yet. The day before the partner existed is the last
        // "closed" day, so the walk starts on the day they were created — a
        // partner owes nothing for days before they existed, and the first run
        // of a deployment must not back-bill the month of usage that predates
        // it.
        match timefmt::parse_ts(&partner.created_at) {
            Some(created) => Ok(BillingDay::of(created, self.generator.timezone()).previous()),
            None => {
                // A `created_at` this cannot read is not evidence that the
                // partner is new. Falling back to the catch-up bound bills a
                // month at most, and the walk says so loudly above.
                warn!(consumer_id = %partner.consumer_id, "billing_partner_created_at_unreadable");
                Ok(self.generator.oldest_closeable(through))
            }
        }
    }

    /// Claim the emails that are due and send them.
    ///
    /// A failure anywhere in here is recorded against that statement, not
    /// returned: one unreachable address must not stop the batch.
    fn send_emails(&self, now: OffsetDateTime, tick: &mut Tick) {
        let Some(settings) = &self.email else {
            return;
        };

        let claimed = match self.store.claim_emails(
            now,
            &self.instance_id,
            EMAIL_BATCH,
            EMAIL_CLAIM_TTL_MINUTES,
        ) {
            Ok(claimed) => claimed,
            Err(e) => {
                warn!(error = %e, "billing_email_claim_failed");
                return;
            }
        };

        for statement in claimed {
            let partner = match self.store.get_partner(&statement.consumer_id) {
                Ok(Some(partner)) => partner,
                Ok(None) => {
                    tick.emails_skipped += 1;
                    // A statement whose partner row is gone. The claim is
                    // released so a later tick can try again after the row is
                    // restored, rather than holding the lease until it lapses.
                    warn!(
                        statement_id = statement.id,
                        "billing_email_without_a_partner"
                    );
                    let _ = self.store.record_email_attempt(
                        statement.id,
                        EmailOutcome::Failed {
                            error: "no partner row for this statement",
                            retry_at: now + time::Duration::minutes(EMAIL_RETRY_MAX_MINUTES),
                        },
                    );
                    continue;
                }
                Err(e) => {
                    warn!(statement_id = statement.id, error = %e, "billing_email_partner_read_failed");
                    continue;
                }
            };

            // `Scope::All` because the worker holds no credential and is not
            // answering a partner: it has already chosen which statement it is
            // sending, and this read is part of building that one email rather
            // than a lookup anybody can aim at a different row.
            let lines = match self.store.statement_lines(statement.id, &Scope::All) {
                Ok(lines) => lines,
                Err(e) => {
                    warn!(statement_id = statement.id, error = %e, "billing_email_lines_read_failed");
                    continue;
                }
            };

            debug!(
                statement_id = statement.id,
                consumer_id = %statement.consumer_id,
                attempts = statement.email_attempts + 1,
                "billing_email_attempt"
            );

            let outcome = email::statement_message(
                &statement,
                &lines,
                &partner.name,
                &settings.from_address,
                &partner.billing_email,
            )
            .and_then(|message| email::send(settings, &message));

            match outcome {
                Ok(()) => {
                    tick.emails_sent += 1;
                    if let Err(e) = self
                        .store
                        .record_email_attempt(statement.id, EmailOutcome::Sent)
                    {
                        warn!(statement_id = statement.id, error = %e, "billing_email_record_failed");
                    }
                    // The recipient and the amount, never anything about the
                    // message's content: this line is written to the operator's
                    // log, which is not a place for a partner's bill in full.
                    info!(
                        statement_id = statement.id,
                        consumer_id = %statement.consumer_id,
                        billing_date = %statement.billing_date,
                        amount = %statement.total(),
                        "billing_email_sent"
                    );
                }
                Err(e) => {
                    tick.emails_failed += 1;
                    let retry_at = now + retry_delay(statement.email_attempts);
                    let error = e.to_string();
                    if let Err(record) = self.store.record_email_attempt(
                        statement.id,
                        EmailOutcome::Failed {
                            error: &error,
                            retry_at,
                        },
                    ) {
                        warn!(statement_id = statement.id, error = %record, "billing_email_record_failed");
                    }
                    warn!(
                        statement_id = statement.id,
                        consumer_id = %statement.consumer_id,
                        attempts = statement.email_attempts + 1,
                        retry_at = %timefmt::format_ts(retry_at),
                        error = %error,
                        "billing_email_failed"
                    );
                }
            }
        }
    }

    /// Log the partners whose derived service status changed since last tick.
    fn observe_status(&self, now: OffsetDateTime) {
        let partners = match self.store.list_partners() {
            Ok(partners) => partners,
            Err(e) => {
                warn!(error = %e, "billing_status_read_failed");
                return;
            }
        };

        for partner in partners {
            let rows = match self.store.overdue_statements(Some(&partner.consumer_id)) {
                Ok(rows) => rows,
                Err(e) => {
                    warn!(consumer_id = %partner.consumer_id, error = %e, "billing_status_read_failed");
                    continue;
                }
            };
            let overdue: Vec<OverdueRow> = rows.iter().map(Statement::to_overdue_row).collect();
            let status: ServiceStatus = status_for(partner.billing_mode.as_str(), now, &overdue);

            let previous = self
                .observed
                .lock()
                .insert(partner.consumer_id.clone(), status.is_suspended());
            // A first observation is not a transition. Logging every partner as
            // suspended at start-up would be a log line about the state of the
            // world, not about something that happened.
            let Some(previous) = previous else {
                continue;
            };
            match (previous, status.is_suspended()) {
                (false, true) => warn!(
                    consumer_id = %partner.consumer_id,
                    status = %status,
                    "partner_suspended"
                ),
                (true, false) => info!(consumer_id = %partner.consumer_id, "partner_resumed"),
                _ => {}
            }
        }
    }
}

/// How long to wait before the next attempt, given the attempts already made.
///
/// Doubling from [`EMAIL_RETRY_BASE_MINUTES`] and capped at
/// [`EMAIL_RETRY_MAX_MINUTES`]. An SMTP relay that refused once refuses again
/// immediately, so a short fixed interval is a busy loop with a database write
/// in it; a day is the ceiling, at which point a statement that cannot be
/// delivered is a thing an operator should be looking at anyway — and the
/// statement itself, which is the record, is already durable.
fn retry_delay(attempts: i64) -> time::Duration {
    let exponent = attempts.clamp(0, 16) as u32;
    let minutes = EMAIL_RETRY_BASE_MINUTES
        .saturating_mul(1i64 << exponent)
        .min(EMAIL_RETRY_MAX_MINUTES);
    time::Duration::minutes(minutes)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::email::test_smtp::{Relay, Running};
    use crate::config::EmailConfig;
    use crate::ledger::{LedgerPool, timefmt};
    use rusqlite::params;
    use tempfile::TempDir;
    use time::macros::datetime;

    /// An instant on the day after the billing day every test uses.
    fn after_the_27th() -> OffsetDateTime {
        datetime!(2026-09-28 01:00 UTC)
    }

    /// The billing day every statement test is about.
    const DAY: &str = "2026-09-27";

    fn billing(port: u16) -> BillingConfig {
        BillingConfig {
            timezone_offset_minutes: 0,
            close_delay_minutes: 5,
            scheduler_interval_secs: 5,
            email: EmailConfig {
                enabled: true,
                smtp_host: "127.0.0.1".to_string(),
                smtp_port: port,
                from_address: "billing@portal.test".to_string(),
            },
        }
    }

    fn email_off() -> BillingConfig {
        BillingConfig {
            email: EmailConfig {
                enabled: false,
                ..EmailConfig::default()
            },
            ..billing(0)
        }
    }

    /// A pool with the schema applied.
    fn pool() -> (TempDir, Arc<LedgerPool>) {
        let dir = TempDir::new().unwrap();
        let pool = Arc::new(LedgerPool::new(dir.path().join("ledger.db")).unwrap());
        (dir, pool)
    }

    fn partner(pool: &LedgerPool, consumer_id: &str, mode: &str, email: &str, created_at: &str) {
        pool.write(|conn| {
            conn.execute(
                "INSERT INTO partners (consumer_id, name, billing_email, billing_mode, \
                 payment_terms_minutes, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, 720, ?5, ?5)",
                params![
                    consumer_id,
                    format!("{consumer_id} Ltd"),
                    email,
                    mode,
                    created_at
                ],
            )
            .map(|_| ())
        })
        .unwrap();
    }

    /// Insert a `usage_records` row the way the metering writer does.
    #[allow(clippy::too_many_arguments)]
    fn usage(
        pool: &LedgerPool,
        consumer_id: &str,
        id: &str,
        created_at: &str,
        model: &str,
        input: Option<i64>,
        output: Option<i64>,
        cached: Option<i64>,
        snapshot: Option<(i64, i64, i64)>,
    ) {
        let (input_price, cached_price, output_price) = match snapshot {
            Some((a, b, c)) => (Some(a), Some(b), Some(c)),
            None => (None, None, None),
        };
        pool.write(|conn| {
            conn.execute(
                "INSERT INTO usage_records (
                     request_id, created_at, consumer_id, model, endpoint, streaming,
                     http_status, request_status, instance_id, input_tokens, output_tokens,
                     cached_tokens, ttft_ms, duration_ms, usage_status, error_message,
                     error_body, input_price_snapshot, cached_input_price_snapshot,
                     output_price_snapshot) \
                 VALUES (?1, ?2, ?3, ?4, 'chat_completions', 0, 200, 'completed', 'inst', \
                         ?5, ?6, ?7, NULL, 10, 'available', NULL, NULL, ?8, ?9, ?10)",
                params![
                    id,
                    created_at,
                    consumer_id,
                    model,
                    input,
                    output,
                    cached,
                    input_price,
                    cached_price,
                    output_price
                ],
            )
            .map(|_| ())
        })
        .unwrap();
    }

    fn worker(pool: Arc<LedgerPool>, billing: &BillingConfig) -> Worker {
        Worker::new(pool, billing, None, "instance-under-test".to_string()).expect("a timezone")
    }

    /// The statement this suite is about. Named apart from `worker`/`statement`
    /// locals on purpose: a shadowing local would make the next call in the same
    /// test a type error, which is a confusing way to learn about a rename.
    fn stated(pool: &Arc<LedgerPool>, consumer_id: &str) -> Option<crate::billing::Statement> {
        BillingStore::new(Arc::clone(pool))
            .get_statement(consumer_id, DAY)
            .unwrap()
    }

    /// A partner created on the first instant of the billing day, with one
    /// priced request later that day. The creation instant is what bounds the
    /// walk, so the walk is exactly one day and every count in this suite is
    /// about a single day rather than about how far back the anchor reached.
    fn one_priced_day(pool: &LedgerPool) {
        partner(
            pool,
            "acme",
            "invoice",
            "billing@acme.test",
            "2026-09-27T00:00:00.000000000Z",
        );
        usage(
            pool,
            "acme",
            "req-1",
            "2026-09-27T10:00:00.000000000Z",
            "gpt-4o",
            Some(1_000_000),
            Some(100_000),
            Some(0),
            // $2.50/M in, $1.25/M cached, $10/M out: 1M input at 2.5 and 100k out at 10.
            Some((2_500_000, 1_250_000, 10_000_000)),
        );
    }

    #[test]
    fn test_a_closed_day_becomes_a_statement_with_the_priced_amount() {
        let (_dir, pool) = pool();
        one_priced_day(&pool);
        let tick = worker(pool.clone(), &email_off())
            .run_once(after_the_27th())
            .unwrap();

        assert_eq!(tick.statements_created, 1, "{tick:?}");
        assert_eq!(tick.failures, 0, "{tick:?}");
        let statement = stated(&pool, "acme").expect("a statement for the billing day");
        // 1,000,000 × $2.50/M = $2.50; 100,000 × $10/M = $1.00.
        assert_eq!(statement.total().as_i64(), 3_500_000);
        assert_eq!(statement.incomplete_usage_count, 0);
        assert_eq!(statement.billing_date, DAY);
        assert!(statement.due_at.is_some(), "an invoice carries a deadline");
    }

    /// The walk is idempotent: a second tick at the same instant finds the
    /// statement already there and writes nothing. The unique index is the
    /// mechanism, and this is the property it buys.
    #[test]
    fn test_a_second_tick_writes_no_second_statement() {
        let (_dir, pool) = pool();
        one_priced_day(&pool);
        let worker = worker(pool.clone(), &email_off());
        assert_eq!(
            worker
                .run_once(after_the_27th())
                .unwrap()
                .statements_created,
            1
        );

        let again = worker.run_once(after_the_27th()).unwrap();
        assert_eq!(again.statements_created, 0, "{again:?}");
        // The statement is the anchor, so the second walk does not even reach
        // the day: it has nothing before it and nothing after it. That is the
        // first of the two things that stop a duplicate, and the unique index
        // is the second — a walk that *does* reach a stated day is the subject
        // of the next test.
        assert_eq!(again.days, 0, "{again:?}");
        assert_eq!(again.failures, 0, "{again:?}");

        let count: i64 = pool
            .read(|conn| {
                conn.query_row("SELECT COUNT(*) FROM daily_statements", [], |row| {
                    row.get(0)
                })
            })
            .unwrap();
        assert_eq!(
            count, 1,
            "one day, one statement, however often it is asked"
        );
        // And the amount is the one day's, not the two days' a second write
        // would have made it.
        assert_eq!(stated(&pool, "acme").unwrap().total().as_i64(), 3_500_000);
    }

    /// The other half of idempotency: a walk that *does* arrive at a day which
    /// is already stated. Two instances overlap during a rolling update, and
    /// each reads its anchor once and then walks up to a month of days, so one
    /// can reach a day the other has just committed.
    ///
    /// This is driven by calling the day's own step directly, because the
    /// overlap is a property of the walk and the walk's anchor is what a live
    /// schedule hides: by the time a second tick reads the anchor, it is the
    /// statement. What matters is that the day is refused rather than
    /// duplicated, and that the refusal is not a failure.
    #[test]
    fn test_a_day_that_is_already_stated_is_not_stated_twice() {
        let (_dir, pool) = pool();
        one_priced_day(&pool);
        let worker = worker(pool.clone(), &email_off());
        worker.run_once(after_the_27th()).unwrap();
        let before = stated(&pool, "acme").unwrap();

        // The same (partner, day) again, as an instance that read its anchor
        // before the statement was committed would see it.
        let partner = BillingStore::new(pool.clone())
            .get_partner("acme")
            .unwrap()
            .expect("the partner");
        let mut tick = Tick::default();
        worker.close_day(&partner, BillingDay::parse(DAY).unwrap(), &mut tick);

        assert_eq!(tick.statements_created, 0, "{tick:?}");
        assert_eq!(tick.statements_existing, 1, "{tick:?}");
        assert_eq!(
            tick.failures, 0,
            "a day already stated is not an error: {tick:?}"
        );

        let count: i64 = pool
            .read(|conn| {
                conn.query_row("SELECT COUNT(*) FROM daily_statements", [], |row| {
                    row.get(0)
                })
            })
            .unwrap();
        assert_eq!(count, 1);
        // And the row is untouched — no line was appended to it, so the total
        // is still the one day's.
        let after = stated(&pool, "acme").unwrap();
        assert_eq!(after.id, before.id);
        assert_eq!(after.total().as_i64(), before.total().as_i64());
    }

    /// A partner's walk starts on the day they were created, so usage that
    /// predates the partner row is never billed. This is what stops the first
    /// run after a deployment from back-billing every day a ledger happens to
    /// hold — days that predate both the partner and any price they agreed to.
    #[test]
    fn test_a_partner_is_not_billed_for_days_before_they_existed() {
        let (_dir, pool) = pool();
        // A week of usage that predates the partner row by a week.
        usage(
            &pool,
            "acme",
            "req-old",
            "2026-09-20T10:00:00.000000000Z",
            "gpt-4o",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((2_500_000, 1_250_000, 10_000_000)),
        );
        partner(
            &pool,
            "acme",
            "invoice",
            "billing@acme.test",
            "2026-09-27T00:00:00.000000000Z",
        );

        let tick = worker(pool.clone(), &email_off())
            .run_once(after_the_27th())
            .unwrap();
        assert_eq!(
            tick.statements_created, 1,
            "the creation day, and nothing before it: {tick:?}"
        );

        let store = BillingStore::new(pool.clone());
        // The 20th and every day between it and the partner row have no
        // statement at all: not a zero one, none.
        for day in ["2026-09-20", "2026-09-26"] {
            assert!(
                store.get_statement("acme", day).unwrap().is_none(),
                "no statement should exist for {day}, and none is the point"
            );
        }
        // And the day the partner does have is zero: the usage that predates
        // them is nowhere on a bill.
        let statement = store.get_statement("acme", DAY).unwrap().expect(DAY);
        assert_eq!(statement.total().as_i64(), 0);
    }

    /// Usage the provider never reported is counted on the statement and never
    /// becomes money. The count is the whole point: it is what makes the total
    /// readable as "less than the day's real cost" rather than as the cost.
    #[test]
    fn test_usage_that_cannot_be_measured_is_counted_and_not_charged() {
        let (_dir, pool) = pool();
        one_priced_day(&pool);
        // A second request whose usage the provider never reported: complete
        // request, no tokens in the ledger.
        usage(
            &pool,
            "acme",
            "req-2",
            "2026-09-27T11:00:00.000000000Z",
            "gpt-4o",
            None,
            None,
            None,
            Some((2_500_000, 1_250_000, 10_000_000)),
        );

        let tick = worker(pool.clone(), &email_off())
            .run_once(after_the_27th())
            .unwrap();
        assert_eq!(tick.statements_created, 1);
        assert_eq!(tick.statements_incomplete, 1, "{tick:?}");

        let statement = stated(&pool, "acme").unwrap();
        assert_eq!(statement.incomplete_usage_count, 1);
        // The measured request is still billed at its own price: an incomplete
        // day is not a day of zero.
        assert_eq!(statement.total().as_i64(), 3_500_000);
        // And it cannot suspend anyone.
        assert!(!statement.can_suspend());
    }

    #[test]
    fn test_a_day_with_no_usage_is_stated_at_zero() {
        let (_dir, pool) = pool();
        partner(
            &pool,
            "acme",
            "invoice",
            "billing@acme.test",
            "2026-09-27T00:00:00.000000000Z",
        );
        let tick = worker(pool.clone(), &email_off())
            .run_once(after_the_27th())
            .unwrap();
        assert_eq!(tick.statements_created, 1);
        let statement = stated(&pool, "acme").unwrap();
        assert_eq!(statement.total().as_i64(), 0);
        assert!(!statement.can_suspend());
        assert!(
            statement.is_outstanding(),
            "an invoice with a deadline and no payment"
        );
    }

    /// The whole email path, against a real relay: claimed, sent, recorded.
    #[test]
    fn test_an_invoice_statement_is_emailed_once_and_recorded() {
        let (_dir, pool) = pool();
        one_priced_day(&pool);
        let relay = Running::start(Relay::default());
        let worker = worker(pool.clone(), &billing(relay.port()));

        let tick = worker.run_once(after_the_27th()).unwrap();
        assert_eq!(tick.statements_created, 1, "{tick:?}");
        assert_eq!(tick.emails_sent, 1, "{tick:?}");
        assert_eq!(tick.emails_failed, 0, "{tick:?}");

        let (commands, data) = relay.finish();
        assert!(
            commands.contains(&"RCPT TO:<billing@acme.test>".to_string()),
            "{commands:?}"
        );
        assert!(
            commands.contains(&"MAIL FROM:<billing@portal.test>".to_string()),
            "{commands:?}"
        );
        // The body is the statement, and it carries the amount.
        assert!(
            data.iter().any(|line| line.contains("$3.500000")),
            "{data:?}"
        );

        let statement = stated(&pool, "acme").unwrap();
        assert!(
            statement.email_sent_at.is_some(),
            "the send is recorded durably"
        );
        assert_eq!(statement.email_attempts, 1);
        assert!(statement.email_last_error.is_none());
        assert!(statement.email_next_retry_at.is_none());
    }

    /// A relay that refuses is not a lost statement: the failure is recorded
    /// with the relay's own words and a retry time, and the next attempt happens
    /// when that time comes — not on every tick.
    #[test]
    fn test_a_refused_email_is_recorded_and_retried_on_its_own_schedule() {
        let (_dir, pool) = pool();
        one_priced_day(&pool);
        let relay = Running::start(Relay {
            rcpt_code: 550,
            rcpt_text: "5.1.1 mailbox unavailable",
            ..Relay::default()
        });
        let first = worker(pool.clone(), &billing(relay.port()));

        let tick = first.run_once(after_the_27th()).unwrap();
        assert_eq!(tick.emails_sent, 0, "{tick:?}");
        assert_eq!(tick.emails_failed, 1, "{tick:?}");
        let (commands, _) = relay.finish();
        assert!(!commands.is_empty(), "the relay was actually contacted");

        let statement = stated(&pool, "acme").unwrap();
        assert!(statement.email_sent_at.is_none(), "nothing was sent");
        assert_eq!(statement.email_attempts, 1);
        let error = statement
            .email_last_error
            .clone()
            .expect("the relay's refusal");
        assert!(error.contains("550"), "{error}");
        assert!(error.contains("mailbox unavailable"), "{error}");
        let retry_at = timefmt::parse_ts(&statement.email_next_retry_at.clone().unwrap()).unwrap();
        // The retry is measured from the tick's own instant, not from the wall
        // clock: a scheduler that computed its backoff from `now()` would be
        // untestable here, and this assertion is what keeps it honest.
        assert!(
            retry_at > after_the_27th(),
            "the retry is in the future: {retry_at}"
        );

        // A tick ten minutes later — past the claim's lease, before the retry
        // is due — does not claim it again and is not a failure. Both halves
        // matter: a scheduler that retried on every tick would hammer a relay
        // that is refusing, and the `0` is only meaningful because the old
        // relay is dead by now, so an attempt would have shown up as a failure.
        let quiet = first.run_once(datetime!(2026-09-28 01:10 UTC)).unwrap();
        assert_eq!(quiet.emails_failed, 0, "{quiet:?}");
        assert_eq!(quiet.emails_sent, 0, "{quiet:?}");
        let after = stated(&pool, "acme").unwrap();
        assert_eq!(
            after.email_attempts, 1,
            "no second attempt before the retry is due"
        );

        // And once the retry comes due it is attempted again, against a relay
        // that works — the statement is not silently dropped after one bad
        // night on the relay's side.
        let relay = Running::start(Relay::default());
        let retrying = worker(pool.clone(), &billing(relay.port()));
        let retried = retrying.run_once(datetime!(2026-09-28 01:16 UTC)).unwrap();
        assert_eq!(retried.emails_sent, 1, "{retried:?}");
        assert!(stated(&pool, "acme").unwrap().email_sent_at.is_some());
    }

    /// A reconciliation partner gets the statement and no email: there is
    /// nothing to ask for, so there is nothing to send.
    #[test]
    fn test_a_reconciliation_statement_is_never_emailed() {
        let (_dir, pool) = pool();
        partner(
            &pool,
            "beta",
            "reconciliation",
            "billing@beta.test",
            "2026-09-27T00:00:00.000000000Z",
        );
        usage(
            &pool,
            "beta",
            "req-1",
            "2026-09-27T10:00:00.000000000Z",
            "gpt-4o",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((2_500_000, 1_250_000, 10_000_000)),
        );
        let relay = Running::start(Relay::default());
        let worker = worker(pool.clone(), &billing(relay.port()));

        let tick = worker.run_once(after_the_27th()).unwrap();
        assert_eq!(tick.statements_created, 1, "{tick:?}");
        assert_eq!(tick.emails_sent, 0, "{tick:?}");
        let (commands, _) = relay.heard();
        assert!(
            commands.is_empty(),
            "the relay was never contacted: {commands:?}"
        );

        let statement = BillingStore::new(pool.clone())
            .get_statement("beta", DAY)
            .unwrap()
            .unwrap();
        assert_eq!(
            statement.total().as_i64(),
            2_500_000,
            "the settlement record is real"
        );
        assert!(statement.due_at.is_none());
        assert!(statement.email_sent_at.is_none());
    }

    /// A zero statement is the record of a day with nothing on it. Emailing it
    /// would train a partner to filter statement mail, including the one that
    /// says they owe something.
    #[test]
    fn test_a_zero_statement_is_not_emailed() {
        let (_dir, pool) = pool();
        partner(
            &pool,
            "acme",
            "invoice",
            "billing@acme.test",
            "2026-09-27T00:00:00.000000000Z",
        );
        let relay = Running::start(Relay::default());
        let worker = worker(pool.clone(), &billing(relay.port()));

        let tick = worker.run_once(after_the_27th()).unwrap();
        assert_eq!(tick.statements_created, 1, "{tick:?}");
        assert_eq!(tick.emails_sent, 0, "{tick:?}");
        let (commands, _) = relay.heard();
        assert!(commands.is_empty(), "{commands:?}");
    }

    /// `enabled: false` is a deployment that does not email: no relay is
    /// contacted and no statement is ever marked as attempted.
    #[test]
    fn test_a_deployment_with_email_off_never_contacts_a_relay() {
        let (_dir, pool) = pool();
        one_priced_day(&pool);
        let relay = Running::start(Relay::default());
        let mut config = billing(relay.port());
        config.email.enabled = false;
        let worker = worker(pool.clone(), &config);
        assert!(!worker.sends_email());

        let tick = worker.run_once(after_the_27th()).unwrap();
        assert_eq!(tick.statements_created, 1);
        assert_eq!(tick.emails_sent, 0);
        let (commands, _) = relay.heard();
        assert!(commands.is_empty(), "{commands:?}");
        assert!(stated(&pool, "acme").unwrap().email_sent_at.is_none());
    }

    /// The close delay is what keeps the last few minutes of a day out of the
    /// statement: a request accepted at 23:59:59 must land on the day it was
    /// accepted, and the writer may not have committed it by midnight. Closing
    /// the day late is what makes the day closed *complete*.
    #[test]
    fn test_a_day_is_closed_only_after_the_close_delay() {
        let (_dir, pool) = pool();
        one_priced_day(&pool);
        let worker = worker(pool.clone(), &email_off());

        // 00:01 on the 28th: the 27th ended a minute ago and the 5-minute close
        // delay has not passed, so the 27th is not closeable and there is
        // nothing to state.
        let early = worker.run_once(datetime!(2026-09-28 00:01 UTC)).unwrap();
        assert_eq!(early.days, 0, "{early:?}");
        assert_eq!(early.statements_created, 0, "{early:?}");
        assert!(stated(&pool, "acme").is_none());

        // Past the delay the same tick closes it — the usage that arrived at
        // 10:00 that day is on the statement, whole.
        let settled = worker.run_once(datetime!(2026-09-28 00:06 UTC)).unwrap();
        assert_eq!(settled.days, 1, "{settled:?}");
        assert_eq!(settled.statements_created, 1, "{settled:?}");
        let statement = stated(&pool, "acme").expect("the day, closed");
        assert_eq!(statement.total().as_i64(), 3_500_000);
        assert_eq!(statement.incomplete_usage_count, 0);
    }

    #[test]
    fn test_a_partner_with_no_address_on_file_is_stated_but_not_emailed() {
        let (_dir, pool) = pool();
        partner(
            &pool,
            "acme",
            "invoice",
            "",
            "2026-09-27T00:00:00.000000000Z",
        );
        usage(
            &pool,
            "acme",
            "req-1",
            "2026-09-27T10:00:00.000000000Z",
            "gpt-4o",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((2_500_000, 1_250_000, 10_000_000)),
        );
        let relay = Running::start(Relay::default());
        let tick = worker(pool.clone(), &billing(relay.port()))
            .run_once(after_the_27th())
            .unwrap();

        assert_eq!(tick.statements_created, 1, "{tick:?}");
        assert_eq!(tick.emails_sent, 0, "{tick:?}");
        assert_eq!(tick.emails_failed, 0, "{tick:?}");
        let (commands, _) = relay.heard();
        assert!(commands.is_empty(), "{commands:?}");
        // And no failure is recorded against it: nothing was attempted, and a
        // missing address is a fact for the admin list rather than an error.
        let statement = stated(&pool, "acme").unwrap();
        assert_eq!(statement.email_attempts, 0);
        assert!(statement.email_last_error.is_none());
    }

    #[test]
    fn test_the_retry_backoff_doubles_and_stops_at_the_ceiling() {
        assert_eq!(retry_delay(0).whole_minutes(), EMAIL_RETRY_BASE_MINUTES);
        assert_eq!(retry_delay(1).whole_minutes(), EMAIL_RETRY_BASE_MINUTES * 2);
        assert_eq!(retry_delay(2).whole_minutes(), EMAIL_RETRY_BASE_MINUTES * 4);
        assert_eq!(retry_delay(3).whole_minutes(), EMAIL_RETRY_BASE_MINUTES * 8);
        // The ceiling, which is what stops an unbounded shift from overflowing
        // into a retry scheduled in a different century.
        assert_eq!(retry_delay(64).whole_minutes(), EMAIL_RETRY_MAX_MINUTES);
        assert_eq!(
            retry_delay(i64::MAX).whole_minutes(),
            EMAIL_RETRY_MAX_MINUTES
        );
    }

    #[test]
    fn test_a_reconciliation_partner_is_never_suspended_by_the_worker_path() {
        // The worker reads the same predicate the request path does. A
        // reconciliation partner can have a large, old, unpaid statement and is
        // still active.
        let row = OverdueRow {
            billing_mode: "invoice".to_string(),
            id: 1,
            billing_date: "2020-01-01".to_string(),
            due_at: "2020-01-02T00:00:00.000000000Z".to_string(),
            total_amount_micro_usd: 1_000_000,
            incomplete_usage_count: 0,
        };
        let now = time::macros::datetime!(2026-09-29 00:00 UTC);
        assert!(status_for("invoice", now, std::slice::from_ref(&row)).is_suspended());
        assert!(status_for("reconciliation", now, std::slice::from_ref(&row)).is_active());
    }
}
