//! Turning a day of usage into a statement.
//!
//! # What this module decides, and what it deliberately refuses to decide
//!
//! It decides *which* days are closeable, *which* usage rows go on a statement,
//! and *what an incomplete day looks like*. It does not decide what anything
//! costs — that is [`crate::billing::pricing`] — and it does not decide where a
//! statement is stored — that is [`crate::billing::store`].
//!
//! # The three reasons a row is not charged
//!
//! A row is billed only when the provider reported it in full *and* a complete
//! price was in force when it was accepted. Everything else is counted as
//! incomplete, and the statement says so:
//!
//! | what is wrong | why it is not charged |
//! |---|---|
//! | no `input_tokens` or no `output_tokens` | the provider never said what the request cost |
//! | no cached count, and the cached price differs from the input price | the prompt was partly cached and there is no way to tell how much |
//! | no price snapshot | the request was accepted with no billing configuration |
//!
//! In every case the alternative is to invent a number, and every invented
//! number here is a wrong amount on a customer's bill. The count of uncharged
//! requests rides on the statement instead, and — this is the part that matters
//! — a statement carrying a non-zero count **never suspends the partner**. See
//! [`crate::billing::status`]. An invoice the product knows it cannot defend is
//! not an invoice to cut someone off over.
//!
//! # Why a day can be billed twice in one statement and never twice in two
//!
//! The `(model, price snapshot)` grouping means a price change mid-day produces
//! two lines rather than one averaged line. The statement itself is guarded by
//! `UNIQUE (consumer_id, billing_date)`, so re-running the worker is a no-op.
//! Those two facts together are the invariant: each partner and day produces at
//! most one durable statement, and its amount is a function of snapshots that
//! cannot move afterwards.

use std::fmt;

use rusqlite::Connection;
use time::OffsetDateTime;

use crate::billing::CURRENCY;
use crate::billing::partner::Partner;
use crate::billing::period::{BillingDay, BillingPeriod, BillingTimezone};
use crate::billing::pricing::{PriceError, billable, needs_cached_count, price_line};
use crate::billing::store::{
    BillingError, StatementDraft, StatementLineDraft, UsageGroup, usage_groups,
};
use crate::ledger::timefmt;

/// How far back an empty database is walked when the worker starts.
///
/// A partner with no statements at all — a new deployment, or a partner added
/// today — has no "last closed day" to continue from, and walking back to the
/// beginning of the ledger would produce a statement per day since it opened.
/// The bound is a month: long enough that a worker that was down for a holiday
/// still catches up, short enough that a first run on a year-old ledger does not
/// try to write 365 statements in one tick.
pub const MAX_CATCHUP_DAYS: i64 = 31;

/// Deciding which days are closeable, and building one day's statement.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Generator {
    timezone: BillingTimezone,
    close_delay_minutes: i64,
}

impl Generator {
    pub fn new(timezone: BillingTimezone, close_delay_minutes: i64) -> Self {
        Self {
            timezone,
            close_delay_minutes: close_delay_minutes.max(0),
        }
    }

    pub fn timezone(self) -> BillingTimezone {
        self.timezone
    }

    pub fn close_delay_minutes(self) -> i64 {
        self.close_delay_minutes
    }

    /// The latest day that may be closed at `now`, if any.
    ///
    /// A day becomes closeable `close_delay_minutes` after its period ends. The
    /// delay exists because a request accepted at 23:59:59 is metered before the
    /// upstream is contacted and finalised afterwards, so at the instant the
    /// clock crosses midnight the day is still receiving rows. Closing it then
    /// would omit the last requests of the day — a systematic under-bill at
    /// exactly the boundary a customer can see.
    ///
    /// `None` when no day has closed yet: `close_delay_minutes` is added to the
    /// period end, so a deployment whose first day has not finished has nothing
    /// to close, and returning "today" would be closing a day that is still
    /// running.
    pub fn closable_through(self, now: OffsetDateTime) -> Option<BillingDay> {
        let today = BillingDay::of(now, self.timezone);
        let yesterday = today.previous();
        if self.cutoff_for(yesterday) <= now {
            return Some(yesterday);
        }
        // The delay can push yesterday's cutoff into today, in which case
        // yesterday is not closeable yet but the day before it certainly is.
        let day_before = yesterday.previous();
        if self.cutoff_for(day_before) <= now {
            return Some(day_before);
        }
        None
    }

    /// When a day may be closed: the end of its period plus the delay.
    ///
    /// Total. A `close_delay_minutes` of zero means "close it the instant it
    /// ends", which is a legitimate configuration and is *not* treated as
    /// missing.
    pub fn cutoff_for(self, day: BillingDay) -> OffsetDateTime {
        day.end_exclusive_utc(self.timezone) + time::Duration::minutes(self.close_delay_minutes)
    }

    /// The days a partner still owes a statement for, oldest first.
    ///
    /// `last_closed` is the newest `billing_date` already in the table, or
    /// `None` for a partner with no statements at all. The walk starts the day
    /// after it — or at most [`MAX_CATCHUP_DAYS`] before the newest closeable
    /// day — and ends at `through` inclusive.
    pub fn days_to_close(self, last_closed: Option<&str>, through: BillingDay) -> Vec<BillingDay> {
        let floor = self.oldest_closeable(through);
        let mut day = match last_closed.and_then(BillingDay::parse) {
            Some(last) => last.next(),
            None => floor,
        };
        if day < floor {
            day = floor;
        }
        let mut days = Vec::new();
        while day <= through {
            days.push(day);
            day = day.next();
        }
        days
    }

    /// The oldest day the walk will close in one pass.
    ///
    /// `MAX_CATCHUP_DAYS` counting `through` itself, so the bound is the number
    /// of statements a single tick can write rather than one more than that.
    pub fn oldest_closeable(self, through: BillingDay) -> BillingDay {
        through.previous_n(MAX_CATCHUP_DAYS - 1)
    }

    /// Whether the catch-up bound cut the walk short.
    ///
    /// A caller logs this rather than swallowing it: silently starting a month
    /// before today is a statement history with an unexplained hole in it, and
    /// the hole is exactly what an operator would need to know about. `true`
    /// whenever there is no last closed day, because then the walk started at
    /// the bound and cannot know what precedes it.
    pub fn catchup_truncated(self, last_closed: Option<&str>, through: BillingDay) -> bool {
        match last_closed.and_then(BillingDay::parse) {
            Some(last) => last.next() < self.oldest_closeable(through),
            None => true,
        }
    }

    /// Build one day's statement for one partner.
    ///
    /// Reads `usage_records` for the half-open period and nothing else. The
    /// cutoff instant is stored on the statement but does not filter the rows:
    /// a request accepted inside the period and finalised before the cutoff is
    /// that day's usage, whichever instant it was written at. A request accepted
    /// inside the period that finalises *after* the cutoff is billed in the day
    /// it was accepted only if it landed in time — and if it did not, the row
    /// keeps `in_flight`/`interrupted` and is counted as incomplete, which is
    /// the honest outcome and not a silently missing charge.
    pub fn statement_for_day(
        self,
        conn: &Connection,
        partner: &Partner,
        day: BillingDay,
        cutoff: OffsetDateTime,
    ) -> Result<StatementDraft, BillingError> {
        let period = BillingPeriod::for_day(day, self.timezone);
        let (start, end) = period.to_bounds();
        let groups = usage_groups(conn, &partner.consumer_id, &start, &end)?;

        let mut lines = Vec::new();
        let mut incomplete_usage_count: i64 = 0;
        for group in &groups {
            // The rows the read already declined to sum. They are counted here
            // whatever happens below, because nothing in this branch can make
            // them measurable.
            incomplete_usage_count += group.incomplete_count;

            // Nothing billable in the group: there is no line to price and no
            // token sum to read, so it is counted and skipped rather than
            // handed to the pricer as an empty group.
            if group.request_count == 0 {
                continue;
            }

            match line_for(group) {
                Ok(line) => lines.push(line),
                // Counted, not dropped and not charged. The count is what makes
                // the statement defensible: it says "this many requests were not
                // measured", which is a fact about the day, and a total that
                // pretends otherwise is the defect. Reaching here means the
                // read's own classification and this one disagreed; the
                // requests are counted as incomplete because refusing to price
                // them is the honest answer, and the whole group is added
                // because none of it was charged.
                Err(_reason) => incomplete_usage_count += group.request_count,
            }
        }

        Ok(StatementDraft {
            consumer_id: partner.consumer_id.clone(),
            billing_date: day,
            billing_mode: partner.billing_mode,
            period_start: start,
            period_end: end,
            billing_cutoff_at: timefmt::format_ts(cutoff),
            due_at: partner.due_at_for(period.end),
            incomplete_usage_count,
            lines,
        })
    }
}

impl BillingDay {
    /// `n` days before this one.
    fn previous_n(self, n: i64) -> Self {
        Self::from_date(self.date() - time::Duration::days(n))
    }
}

/// Why a group of usage rows could not be charged.
///
/// Carried rather than logged at the point of refusal, because the interesting
/// question is not "what went wrong once" but "which model, and is it the same
/// one every day" — and that question is asked by the operator, from the log,
/// hours later.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IncompleteReason {
    /// The provider did not report input or output tokens.
    UsageMissing,
    /// The provider did not say how much of the prompt was cached, and the two
    /// prices differ — so assuming zero would charge the cached part at the
    /// input price.
    CachedCountMissing,
    /// No price was in force when the request was accepted.
    NoPriceSnapshot,
    /// More cached tokens than input tokens, which cannot be true.
    CachedExceedsInput,
    /// A price times a count did not fit in micro-dollars.
    Overflow,
}

impl fmt::Display for IncompleteReason {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(match self {
            IncompleteReason::UsageMissing => "the provider did not report usage in full",
            IncompleteReason::CachedCountMissing => {
                "the provider did not report a cached-token count and the cached price differs"
            }
            IncompleteReason::NoPriceSnapshot => {
                "no billing configuration was in force when the request was accepted"
            }
            IncompleteReason::CachedExceedsInput => {
                "the upstream reported more cached tokens than input tokens"
            }
            IncompleteReason::Overflow => "pricing the usage overflowed micro-dollars",
        })
    }
}

impl From<PriceError> for IncompleteReason {
    fn from(error: PriceError) -> Self {
        match error {
            // A price snapshot that is *absent* is a different fact from usage
            // the provider did not report, and the two are told apart because
            // they have different remedies: one is a missing configuration, the
            // other is a provider that will keep doing it.
            PriceError::NoPriceSnapshot { .. } => IncompleteReason::NoPriceSnapshot,
            PriceError::IncompleteUsage => IncompleteReason::UsageMissing,
            PriceError::CachedExceedsInput { .. } => IncompleteReason::CachedExceedsInput,
            PriceError::Overflow { .. } => IncompleteReason::Overflow,
            // A price parsed from a database column rather than from text, so
            // these two cannot arise here. Mapping them rather than panicking
            // keeps the function total: a row with an unreadable price is
            // incomplete usage, which is a thing that happens.
            PriceError::NotANumber(_) | PriceError::OutOfRange(_) => {
                IncompleteReason::NoPriceSnapshot
            }
        }
    }
}

/// A summed token column as a count, or `None` if it is absent or negative.
///
/// Negative cannot occur — the table has `CHECK (... >= 0)` — but the cast is
/// fallible and the fallible answer here is "not reported", which is the
/// direction that refuses to charge rather than the one that charges a wrapped
/// `u64`. `SUM` over an empty set is also `NULL`, which is the same answer for
/// the same reason.
fn token_count(value: Option<i64>) -> Option<u64> {
    value.and_then(|v| u64::try_from(v).ok())
}

/// Price one `(model, price)` group into a statement line.
///
/// The three refusals, in order, and the order matters:
///
/// 1. `billable` refuses usage the provider did not fully report, and refuses it
///    as `Err` rather than substituting a zero.
/// 2. `needs_cached_count` refuses a missing cached count *only when it could
///    change the answer*. When the two prices are equal, a cached count cannot
///    affect the amount, and [`billable`] treats the absence as nothing to
///    assume. This is the one place the product reasons about a missing count
///    without inventing it.
/// 3. `price_line` does the arithmetic, checked, and refuses rather than
///    wrapping.
pub fn line_for(group: &UsageGroup) -> Result<StatementLineDraft, IncompleteReason> {
    let Some(prices) = group.prices else {
        return Err(IncompleteReason::NoPriceSnapshot);
    };
    if needs_cached_count(prices) && group.cached_tokens.is_none() {
        return Err(IncompleteReason::CachedCountMissing);
    }
    let (input, output, cached) = billable(
        token_count(group.input_tokens),
        token_count(group.output_tokens),
        token_count(group.cached_tokens),
    )?;
    let cost = price_line(input, cached, output, prices)?;
    Ok(StatementLineDraft {
        model: group.model.clone(),
        prices,
        request_count: group.request_count,
        input_tokens: input as i64,
        cached_input_tokens: cached as i64,
        output_tokens: output as i64,
        cost,
    })
}

/// The currency every statement is issued in.
pub const STATEMENT_CURRENCY: &str = CURRENCY;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::partner::BillingMode;
    use crate::billing::pricing::{PricePerMillion, PricingSnapshot};
    use crate::ledger::{LedgerPool, timefmt};
    use rusqlite::params;
    use std::sync::Arc;
    use time::macros::datetime;

    fn tz(minutes: i32) -> BillingTimezone {
        BillingTimezone::from_offset_minutes(minutes).expect("a usable offset")
    }

    fn day(text: &str) -> BillingDay {
        BillingDay::parse(text).expect("a billing day")
    }

    fn partner(mode: BillingMode) -> Partner {
        Partner {
            consumer_id: "acme".to_string(),
            name: "Acme".to_string(),
            billing_email: "billing@acme.test".to_string(),
            billing_mode: mode,
            payment_terms_minutes: 720,
            created_at: String::new(),
            updated_at: String::new(),
        }
    }

    fn prices(input: i64, cached: i64, output: i64) -> PricingSnapshot {
        PricingSnapshot::new(
            PricePerMillion::new(input),
            PricePerMillion::new(cached),
            PricePerMillion::new(output),
        )
    }

    /// A pool with the schema applied and `acme` partnered, so the foreign keys
    /// the billing tables carry are satisfied.
    fn pool() -> (tempfile::TempDir, Arc<LedgerPool>) {
        let dir = tempfile::TempDir::new().unwrap();
        let pool = Arc::new(LedgerPool::new(dir.path().join("ledger.db")).unwrap());
        pool.write(|conn| {
            conn.execute(
                "INSERT INTO partners (consumer_id, name, billing_email, billing_mode, \
                 payment_terms_minutes, created_at, updated_at) \
                 VALUES ('acme', 'Acme', 'billing@acme.test', 'invoice', 720, \
                         '2026-09-01T00:00:00.000000000Z', '2026-09-01T00:00:00.000000000Z')",
                [],
            )
            .map(|_| ())
        })
        .unwrap();
        (dir, pool)
    }

    /// Insert a `usage_records` row the way the writer does, with the price
    /// snapshot columns set from `snapshot`.
    #[allow(clippy::too_many_arguments)]
    fn usage(
        pool: &LedgerPool,
        id: &str,
        created_at: &str,
        model: &str,
        request_status: &str,
        usage_status: &str,
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
                 VALUES (?1, ?2, 'acme', ?3, 'chat_completions', 0, 200, ?4, 'inst', \
                         ?5, ?6, ?7, NULL, 10, ?8, NULL, NULL, ?9, ?10, ?11)",
                params![
                    id,
                    created_at,
                    model,
                    request_status,
                    input,
                    output,
                    cached,
                    usage_status,
                    input_price,
                    cached_price,
                    output_price
                ],
            )
            .map(|_| ())
        })
        .unwrap();
    }

    fn generator() -> Generator {
        Generator::new(tz(0), 5)
    }

    // -- the closeability boundary -------------------------------------------

    #[test]
    fn test_a_day_is_closeable_only_after_its_period_and_the_delay() {
        // Midnight on the 28th ends the 27th, and the 5-minute delay means the
        // 27th is closeable from 00:05.
        let g = generator();
        assert_eq!(
            g.cutoff_for(day("2026-09-27")),
            datetime!(2026-09-28 00:05 UTC)
        );
        assert_eq!(
            g.closable_through(datetime!(2026-09-28 00:05 UTC)),
            Some(day("2026-09-27"))
        );
        assert_eq!(
            g.closable_through(datetime!(2026-09-29 12:00 UTC)),
            Some(day("2026-09-28"))
        );
    }

    #[test]
    fn test_a_delay_past_midnight_does_not_skip_a_day() {
        // A one-hour delay: at 00:30 on the 28th, the 27th is not closeable yet
        // — its cutoff is 01:00 — but the 26th is. The naive "yesterday is
        // closeable" implementation closes the 27th early here, and the naive
        // "return yesterday or nothing" one closes the 26th *and the 27th*
        // together an hour later, which is a double bill.
        let g = Generator::new(tz(0), 60);
        assert_eq!(
            g.closable_through(datetime!(2026-09-28 00:30 UTC)),
            Some(day("2026-09-26"))
        );
        assert_eq!(
            g.closable_through(datetime!(2026-09-28 01:00 UTC)),
            Some(day("2026-09-27"))
        );
    }

    #[test]
    fn test_the_boundary_is_the_period_end_plus_the_delay_and_nothing_earlier() {
        // A zero delay, pinned on both sides of the midnight that ends the
        // 27th: one second before it, the 27th is still receiving rows and the
        // newest *closed* day is the 26th. One second after, it is the 27th.
        // This is the whole reason a delay exists — a request accepted at
        // 23:59:59 finalises after midnight.
        let g = Generator::new(tz(0), 0);
        assert_eq!(
            g.closable_through(datetime!(2026-09-27 23:59:59 UTC)),
            Some(day("2026-09-26"))
        );
        assert_eq!(
            g.closable_through(datetime!(2026-09-28 00:00:00 UTC)),
            Some(day("2026-09-27"))
        );
    }

    #[test]
    fn test_a_non_utc_billing_day_closes_on_its_own_midnight() {
        // UTC+07: the 27th runs from 17:00 on the 26th UTC to 17:00 on the 27th,
        // so it is closeable at 17:05 UTC — not at 00:05 UTC, which would be
        // closing it eight hours into its own day.
        let g = Generator::new(tz(7 * 60), 5);
        assert_eq!(
            g.cutoff_for(day("2026-09-27")),
            datetime!(2026-09-27 17:05 UTC)
        );
        assert_eq!(
            g.closable_through(datetime!(2026-09-27 17:05 UTC)),
            Some(day("2026-09-27"))
        );
    }

    // -- the catch-up walk ---------------------------------------------------

    #[test]
    fn test_the_walk_continues_from_the_last_closed_day() {
        let g = generator();
        let days = g.days_to_close(Some("2026-09-25"), day("2026-09-27"));
        assert_eq!(days, vec![day("2026-09-26"), day("2026-09-27")]);
    }

    #[test]
    fn test_a_day_already_closed_produces_no_work() {
        let g = generator();
        assert!(
            g.days_to_close(Some("2026-09-27"), day("2026-09-27"))
                .is_empty()
        );
        // Even a statement "from the future" — a clock that went backwards —
        // yields nothing rather than a statement for a day that has not closed.
        assert!(
            g.days_to_close(Some("2026-09-30"), day("2026-09-27"))
                .is_empty()
        );
    }

    #[test]
    fn test_a_long_absence_catches_up_but_is_bounded() {
        let g = generator();
        // Down for three days: all three are closed, oldest first.
        let days = g.days_to_close(Some("2026-09-24"), day("2026-09-27"));
        assert_eq!(days.first(), Some(&day("2026-09-25")));
        assert_eq!(days.last(), Some(&day("2026-09-27")));
        assert_eq!(days.len(), 3);

        // A year-old ledger with no statements is not walked to the epoch. The
        // bound counts the day it closes on, so it is exactly the number of
        // statements one tick can write.
        let days = g.days_to_close(None, day("2026-09-27"));
        assert_eq!(days.len(), MAX_CATCHUP_DAYS as usize);
        assert_eq!(days.first(), Some(&g.oldest_closeable(day("2026-09-27"))));
        assert_eq!(days.last(), Some(&day("2026-09-27")));
        assert!(g.catchup_truncated(None, day("2026-09-27")));
        assert!(!g.catchup_truncated(Some("2026-09-25"), day("2026-09-27")));
    }

    // -- building a day ------------------------------------------------------

    #[test]
    fn test_one_day_of_usage_becomes_one_statement_with_one_line() {
        let (_dir, pool) = pool();
        usage(
            &pool,
            "r1",
            "2026-09-27T10:00:00.000000000Z",
            "gpt-4o",
            "completed",
            "available",
            Some(1_000),
            Some(500),
            Some(0),
            Some((95_000, 47_500, 475_000)),
        );
        usage(
            &pool,
            "r2",
            "2026-09-27T11:00:00.000000000Z",
            "gpt-4o",
            "completed",
            "available",
            Some(2_000),
            Some(1_000),
            Some(0),
            Some((95_000, 47_500, 475_000)),
        );

        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();

        assert_eq!(draft.lines.len(), 1);
        assert_eq!(draft.incomplete_usage_count, 0);
        assert_eq!(draft.period_start, "2026-09-27T00:00:00.000000000Z");
        assert_eq!(draft.period_end, "2026-09-28T00:00:00.000000000Z");
        let line = &draft.lines[0];
        assert_eq!(line.request_count, 2);
        assert_eq!(line.input_tokens, 3_000);
        assert_eq!(line.output_tokens, 1_500);
        // 3000 × 95_000 / 1e6 = 285, 1500 × 475_000 / 1e6 = 712 (712.5 rounds
        // away from zero), so the total is 997 micro-USD — $0.000997.
        assert_eq!(line.cost.total.as_i64(), 285 + 713);
        assert_eq!(draft.total().unwrap().as_i64(), 998);
        assert_eq!(draft.due_at, Some(datetime!(2026-09-28 12:00 UTC)));
    }

    #[test]
    fn test_a_price_change_mid_day_produces_two_lines_not_one_average() {
        let (_dir, pool) = pool();
        usage(
            &pool,
            "r1",
            "2026-09-27T10:00:00.000000000Z",
            "gpt-4o",
            "completed",
            "available",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((95_000, 95_000, 475_000)),
        );
        usage(
            &pool,
            "r2",
            "2026-09-27T20:00:00.000000000Z",
            "gpt-4o",
            "completed",
            "available",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((47_500, 47_500, 475_000)),
        );

        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();

        assert_eq!(
            draft.lines.len(),
            2,
            "one line per price, not one per model"
        );
        assert_eq!(draft.lines[0].prices.input.as_i64(), 47_500);
        assert_eq!(draft.lines[1].prices.input.as_i64(), 95_000);
        // 1e6 tokens at each price: 47_500 and 95_000 micro-USD.
        assert_eq!(draft.total().unwrap().as_i64(), 142_500);
    }

    #[test]
    fn test_usage_the_provider_did_not_report_is_counted_and_not_charged() {
        let (_dir, pool) = pool();
        usage(
            &pool,
            "good",
            "2026-09-27T10:00:00.000000000Z",
            "gpt-4o",
            "completed",
            "available",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((95_000, 95_000, 475_000)),
        );
        // No output tokens: the provider never said what the completion cost.
        usage(
            &pool,
            "partial",
            "2026-09-27T11:00:00.000000000Z",
            "gpt-4o",
            "failed",
            "partial",
            Some(500),
            None,
            Some(0),
            Some((95_000, 95_000, 475_000)),
        );
        // Interrupted mid-stream, no usage at all.
        usage(
            &pool,
            "unavailable",
            "2026-09-27T12:00:00.000000000Z",
            "gpt-4o",
            "interrupted",
            "unavailable",
            None,
            None,
            None,
            Some((95_000, 95_000, 475_000)),
        );

        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();

        // The two unmeasurable requests are counted and add nothing.
        assert_eq!(draft.incomplete_usage_count, 2);
        assert_eq!(draft.lines.len(), 1);
        assert_eq!(draft.total().unwrap().as_i64(), 95_000);
    }

    #[test]
    fn test_a_request_accepted_with_no_price_is_incomplete_not_free() {
        let (_dir, pool) = pool();
        usage(
            &pool,
            "unpriced",
            "2026-09-27T10:00:00.000000000Z",
            "gpt-4o",
            "completed",
            "available",
            Some(1_000_000),
            Some(1_000_000),
            Some(0),
            None,
        );

        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();

        assert_eq!(draft.incomplete_usage_count, 1);
        assert!(draft.lines.is_empty());
        // And a statement is still produced: a day the product could not
        // measure is a fact to record, not a day to skip silently.
        assert_eq!(draft.total().unwrap().as_i64(), 0);
    }

    #[test]
    fn test_an_unreported_cached_count_is_only_fatal_when_it_changes_the_price() {
        // Cached priced differently, cached count missing: refuse, because
        // assuming zero would bill the whole prompt at the input price.
        let differing = UsageGroup {
            model: "gpt-4o".into(),
            prices: Some(prices(95_000, 10_000, 475_000)),
            request_count: 4,
            input_tokens: Some(1_000),
            cached_tokens: None,
            output_tokens: Some(10),
            // Irrelevant to `line_for`, which prices a group it is given: these
            // tests are about what makes a group unpriceable, and the count of
            // the requests already set aside is not part of that decision.
            incomplete_count: 0,
        };
        assert_eq!(
            line_for(&differing),
            Err(IncompleteReason::CachedCountMissing)
        );

        // Cached priced the same, cached count missing: the answer cannot
        // change, so nothing is assumed and the line is priced.
        let equal = UsageGroup {
            prices: Some(prices(95_000, 95_000, 475_000)),
            ..differing.clone()
        };
        let line = line_for(&equal).expect("an unknowable count is not an incomplete one");
        assert_eq!(line.cached_input_tokens, 0);
        assert_eq!(line.cost.uncached_input_tokens, 1_000);
    }

    #[test]
    fn test_cached_tokens_are_not_charged_twice() {
        let group = UsageGroup {
            model: "gpt-4o".into(),
            prices: Some(prices(95_000, 9_500, 475_000)),
            request_count: 1,
            incomplete_count: 0,
            input_tokens: Some(1_000_000),
            cached_tokens: Some(1_000_000),
            output_tokens: Some(0),
        };
        let line = line_for(&group).unwrap();
        // All of the prompt was cached, so the input component is zero and the
        // cached component carries the whole cost — 1e6 × 9_500 / 1e6.
        assert_eq!(line.cost.input_cost.as_i64(), 0);
        assert_eq!(line.cost.cached_input_cost.as_i64(), 9_500);
        assert_eq!(line.cost.total.as_i64(), 9_500);
        assert_eq!(line.cost.uncached_input_tokens, 0);
    }

    #[test]
    fn test_more_cached_than_input_is_incomplete_rather_than_clamped() {
        // Not clamped to `input`, which would invent a usage figure. The row is
        // refused and counted, and the day is marked as one the product could
        // not measure.
        let group = UsageGroup {
            model: "gpt-4o".into(),
            prices: Some(prices(95_000, 10_000, 475_000)),
            request_count: 3,
            incomplete_count: 0,
            input_tokens: Some(100),
            cached_tokens: Some(500),
            output_tokens: Some(10),
        };
        assert_eq!(line_for(&group), Err(IncompleteReason::CachedExceedsInput));
    }

    #[test]
    fn test_a_cached_count_beyond_the_group_sum_is_still_caught() {
        // The guard is on the summed counts, not on each request, because that
        // is what the statement line carries. A group whose total cached count
        // exceeds its input total cannot be priced, whatever the individual rows
        // looked like.
        let group = UsageGroup {
            model: "gpt-4o".into(),
            prices: Some(prices(95_000, 10_000, 475_000)),
            request_count: 2,
            incomplete_count: 0,
            input_tokens: Some(50),
            cached_tokens: Some(51),
            output_tokens: Some(1),
        };
        assert_eq!(line_for(&group), Err(IncompleteReason::CachedExceedsInput));
    }

    #[test]
    fn test_usage_outside_the_period_is_not_billed_into_it() {
        let (_dir, pool) = pool();
        // 23:59:59 on the 27th is inside; 00:00:00 on the 28th is the 28th's.
        usage(
            &pool,
            "inside",
            "2026-09-27T23:59:59.999999999Z",
            "gpt-4o",
            "completed",
            "available",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((95_000, 95_000, 475_000)),
        );
        usage(
            &pool,
            "next-day",
            "2026-09-28T00:00:00.000000000Z",
            "gpt-4o",
            "completed",
            "available",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((95_000, 95_000, 475_000)),
        );

        let conn = pool.reader().unwrap();
        let g = generator();
        let the_27th = g
            .statement_for_day(
                &conn,
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();
        let the_28th = g
            .statement_for_day(
                &conn,
                &partner(BillingMode::Invoice),
                day("2026-09-28"),
                datetime!(2026-09-29 00:05 UTC),
            )
            .unwrap();

        assert_eq!(the_27th.lines[0].request_count, 1);
        assert_eq!(the_28th.lines[0].request_count, 1);
        // The period end is exclusive, so no request is billed twice or not at
        // all at the boundary.
        assert_eq!(the_27th.total().unwrap().as_i64(), 95_000);
        assert_eq!(the_28th.total().unwrap().as_i64(), 95_000);
    }

    #[test]
    fn test_another_partner_usage_is_not_on_this_statement() {
        let (_dir, pool) = pool();
        pool.write(|conn| {
            conn.execute(
                "INSERT INTO partners (consumer_id, name, billing_email, billing_mode, \
                 payment_terms_minutes, created_at, updated_at) \
                 VALUES ('beta', 'Beta', '', 'invoice', 720, ?1, ?1)",
                params![timefmt::format_ts(datetime!(2026-09-01 00:00 UTC))],
            )
            .map(|_| ())
        })
        .unwrap();
        pool.write(|conn| {
            conn.execute(
                "INSERT INTO usage_records (
                     request_id, created_at, consumer_id, model, endpoint, streaming,
                     http_status, request_status, duration_ms, usage_status,
                     input_tokens, output_tokens, cached_tokens,
                     input_price_snapshot, cached_input_price_snapshot,
                     output_price_snapshot) \
                 VALUES ('beta-1', '2026-09-27T10:00:00.000000000Z', 'beta', 'gpt-4o', \
                         'chat_completions', 0, 200, 'completed', 10, 'available', \
                         1000, 1000, 0, 95_000, 95_000, 475_000)",
                [],
            )
            .map(|_| ())
        })
        .unwrap();

        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();
        assert!(draft.lines.is_empty(), "beta's usage is beta's bill");
        assert_eq!(draft.total().unwrap().as_i64(), 0);
    }

    #[test]
    fn test_a_day_with_no_usage_at_all_is_an_empty_statement() {
        // The caller decides not to write it; this module reports what the day
        // contains, which is nothing. The distinction matters: an empty draft is
        // "no usage", and a draft with no lines and a non-zero incomplete count
        // is "usage the product could not measure".
        let (_dir, pool) = pool();
        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();
        assert!(draft.lines.is_empty());
        assert_eq!(draft.incomplete_usage_count, 0);
        assert_eq!(draft.total().unwrap().as_i64(), 0);
    }

    #[test]
    fn test_a_reconciliation_day_gets_a_statement_with_no_deadline() {
        let (_dir, pool) = pool();
        usage(
            &pool,
            "r1",
            "2026-09-27T10:00:00.000000000Z",
            "gpt-4o",
            "completed",
            "available",
            Some(1_000_000),
            Some(0),
            Some(0),
            Some((95_000, 95_000, 475_000)),
        );
        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Reconciliation),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();
        // A real amount and no deadline: the settlement record, not an invoice
        // pretending to be one.
        assert_eq!(draft.total().unwrap().as_i64(), 95_000);
        assert_eq!(draft.due_at, None);
        assert_eq!(draft.billing_mode, BillingMode::Reconciliation);
    }

    #[test]
    fn test_a_free_model_is_stated_at_zero_rather_than_skipped() {
        // A zero price is a configured price. The statement records the usage;
        // it is not a day the product failed to measure.
        let (_dir, pool) = pool();
        usage(
            &pool,
            "free",
            "2026-09-27T10:00:00.000000000Z",
            "local-model",
            "completed",
            "available",
            Some(1_000_000),
            Some(1_000_000),
            Some(0),
            Some((0, 0, 0)),
        );
        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();
        assert_eq!(draft.lines.len(), 1);
        assert_eq!(draft.incomplete_usage_count, 0);
        assert_eq!(draft.total().unwrap().as_i64(), 0);
    }

    #[test]
    fn test_a_usage_row_that_is_still_in_flight_is_incomplete() {
        // Accepted but not yet finalised when the day closed. It has no usage
        // and no terminal state, so it is unmeasurable — which is exactly why
        // the close delay exists.
        let (_dir, pool) = pool();
        usage(
            &pool,
            "flying",
            "2026-09-27T23:59:59.000000000Z",
            "gpt-4o",
            "in_flight",
            "unavailable",
            None,
            None,
            None,
            Some((95_000, 95_000, 475_000)),
        );
        let draft = generator()
            .statement_for_day(
                &pool.reader().unwrap(),
                &partner(BillingMode::Invoice),
                day("2026-09-27"),
                datetime!(2026-09-28 00:05 UTC),
            )
            .unwrap();
        assert_eq!(draft.incomplete_usage_count, 1);
        assert!(draft.lines.is_empty());
    }

    #[test]
    fn test_the_currency_is_carried_on_every_statement() {
        assert_eq!(STATEMENT_CURRENCY, "USD");
    }
}
