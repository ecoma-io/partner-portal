//! Billing days, and the instant one of them closes.
//!
//! # A billing day is a local calendar day, not a UTC one
//!
//! The product requirement is a statement "for 2026-09-27", and the person
//! reading that statement thinks in a calendar, not in `Z`. A UTC boundary
//! would put the last hour of 23:59 on the previous day's statement for any
//! deployment west of Greenwich. So the day boundary is a configured
//! [`BillingTimezone::offset_minutes`] from UTC, and the *stored* timestamps
//! stay UTC — the offset is applied when a day is named and when a cutoff is
//! computed, and nowhere else. Every `daily_statements` row carries its own
//! `period_start`/`period_end` in UTC, so a statement does not depend on the
//! configuration still being what it was when the statement was written.
//!
//! # Why the offset is minutes and not a zone name
//!
//! A zone name (`Asia/Ho_Chi_Minh`) means a tz database, which means a
//! dependency and a rule that changes when the operating system updates its
//! database — a bill that moves by an hour because a maintenance package
//! landed. A fixed offset from UTC has neither problem, and the day boundary
//! is a business decision, not an astronomical one. Half-hour and 45-minute
//! offsets are representable, so this is not a UTC-only world.
//!
//! The offset is a **fixed** number of minutes from UTC and is *not* a
//! daylight-saving rule. A deployment that needs DST moves its offset
//! deliberately, at a boundary it chooses, rather than discovering at 02:00 that
//! a statement is now two hours long.

use std::fmt;
use time::{Date, OffsetDateTime, Time};

use crate::ledger::timefmt;

/// A billing calendar day: a date in the configured billing timezone.
///
/// `Date` rather than a string so the type cannot carry a non-date, and so the
/// arithmetic that produces "yesterday" and "the day before that" is calendar
/// arithmetic — `Date - 1` knows about month and year lengths, and rolling back
/// by 86 400 seconds does not.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct BillingDay {
    date: Date,
}

impl BillingDay {
    pub fn from_date(date: Date) -> Self {
        Self { date }
    }

    pub fn date(self) -> Date {
        self.date
    }

    /// Parse `YYYY-MM-DD`, the form stored in `daily_statements.billing_date`
    /// and the form a statement's subject line uses.
    pub fn parse(text: &str) -> Option<Self> {
        Date::parse(
            text,
            &time::format_description::well_known::Iso8601::DEFAULT,
        )
        .ok()
        .map(Self::from_date)
    }

    /// The calendar day an instant falls in, in `tz`.
    pub fn of(instant: OffsetDateTime, tz: BillingTimezone) -> Self {
        Self::from_date(instant.to_offset(tz.utc_offset()).date())
    }

    /// The day before this one, crossing month and year boundaries.
    pub fn previous(self) -> Self {
        Self::from_date(self.date - time::Duration::DAY)
    }

    /// The day after this one.
    pub fn next(self) -> Self {
        Self::from_date(self.date + time::Duration::DAY)
    }

    /// The first instant of this day, in UTC.
    ///
    /// Total, and therefore infallible: a `PrimitiveDateTime` is the local
    /// wall clock, this one's offset is known, and there is no second clock to
    /// disagree with. A DST transition inside a 24-hour day is the case this
    /// design gives up on, and it gives it up by being a *fixed* offset — see
    /// the module docs.
    pub fn start_utc(self, tz: BillingTimezone) -> OffsetDateTime {
        self.date
            .with_time(Time::MIDNIGHT)
            .assume_offset(tz.utc_offset())
    }

    /// The first instant of the *next* day, in UTC — the exclusive end of this
    /// day. Computed rather than derived by the caller as `start + 24h`,
    /// because that shortcut is only right for a zero offset and would
    /// misattribute the tail of every day for any other.
    pub fn end_exclusive_utc(self, tz: BillingTimezone) -> OffsetDateTime {
        self.next().start_utc(tz)
    }
}

impl fmt::Display for BillingDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(&self.date, f)
    }
}

/// The configured billing calendar's distance from UTC.
///
/// Construction is fallible so an out-of-range offset is a configuration error
/// at load time rather than a panic at 02:00 the first time a day closes.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BillingTimezone {
    offset_minutes: i32,
}

impl BillingTimezone {
    /// The range accepted: `UTC-12:00` to `UTC+14:00`, which is the whole of the
    /// real-world set of civil offsets — Baker Island on one end, Kiritimati on
    /// the other. A wider range would be accepted configuration rather than a
    /// calendar anyone bills on, and a narrower one would exclude a real
    /// deployment; the bound is a statement about the world, not about the
    /// `time` crate, which would happily hold ±23:59.
    pub const MIN_OFFSET_MINUTES: i32 = -12 * 60;
    pub const MAX_OFFSET_MINUTES: i32 = 14 * 60;

    pub fn from_offset_minutes(offset_minutes: i32) -> Result<Self, TimezoneError> {
        if !(Self::MIN_OFFSET_MINUTES..=Self::MAX_OFFSET_MINUTES).contains(&offset_minutes) {
            return Err(TimezoneError::OutOfRange {
                offset_minutes,
                min: Self::MIN_OFFSET_MINUTES,
                max: Self::MAX_OFFSET_MINUTES,
            });
        }
        Ok(Self { offset_minutes })
    }

    pub fn offset_minutes(self) -> i32 {
        self.offset_minutes
    }

    pub fn utc_offset(self) -> time::UtcOffset {
        time::UtcOffset::from_whole_seconds(self.offset_minutes * 60)
            .unwrap_or(time::UtcOffset::UTC)
    }
}

impl Default for BillingTimezone {
    /// UTC. The default is the one that reproduces the machine's idea of a day
    /// on a UTC host and the least surprising one anywhere else, and it is a
    /// documented default rather than a fallback: a deployment that bills on a
    /// local calendar says so in the configuration.
    fn default() -> Self {
        Self { offset_minutes: 0 }
    }
}

impl fmt::Display for BillingTimezone {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let sign = if self.offset_minutes < 0 { '-' } else { '+' };
        let (hours, minutes) = (
            self.offset_minutes.abs() / 60,
            self.offset_minutes.abs() % 60,
        );
        if minutes == 0 {
            write!(f, "UTC{sign}{hours:02}")
        } else {
            write!(f, "UTC{sign}{hours:02}:{minutes:02}")
        }
    }
}

/// The configured offset is not a usable offset from UTC.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum TimezoneError {
    OutOfRange {
        offset_minutes: i32,
        min: i32,
        max: i32,
    },
}

impl fmt::Display for TimezoneError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            TimezoneError::OutOfRange {
                offset_minutes,
                min,
                max,
            } => write!(
                f,
                "billing.timezone_offset_minutes is {offset_minutes}, outside {min}..={max}"
            ),
        }
    }
}

impl std::error::Error for TimezoneError {}

/// The half-open UTC window one billing day covers.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BillingPeriod {
    pub start: OffsetDateTime,
    pub end: OffsetDateTime,
}

impl BillingPeriod {
    pub fn for_day(day: BillingDay, tz: BillingTimezone) -> Self {
        Self {
            start: day.start_utc(tz),
            end: day.end_exclusive_utc(tz),
        }
    }

    /// Whether an instant falls inside this period. Half-open, so the instant a
    /// day closes belongs to the *next* day and no request is billed twice or
    /// not at all.
    pub fn contains(&self, instant: OffsetDateTime) -> bool {
        instant >= self.start && instant < self.end
    }

    pub fn to_bounds(self) -> (String, String) {
        (timefmt::format_ts(self.start), timefmt::format_ts(self.end))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn tz(minutes: i32) -> BillingTimezone {
        BillingTimezone::from_offset_minutes(minutes).expect("a usable offset")
    }

    fn day(text: &str) -> BillingDay {
        BillingDay::parse(text).expect("a billing day")
    }

    fn period(day: &str, tz: BillingTimezone) -> BillingPeriod {
        BillingPeriod::for_day(BillingDay::parse(day).expect("a billing day"), tz)
    }

    #[test]
    fn test_a_day_renders_in_the_form_a_statement_is_addressed_by() {
        assert_eq!(day("2026-09-27").to_string(), "2026-09-27");
        assert_eq!(BillingDay::parse("2026-09-27"), Some(day("2026-09-27")));
        assert_eq!(BillingDay::parse("not-a-date"), None);
        assert_eq!(BillingDay::parse("2026-13-01"), None);
    }

    #[test]
    fn test_utc_is_the_default_and_bills_on_utc_days() {
        let tz = BillingTimezone::default();
        assert_eq!(tz.offset_minutes(), 0);
        let period = period("2026-09-27", tz);
        assert_eq!(period.start, datetime!(2026-09-27 00:00 UTC));
        assert_eq!(period.end, datetime!(2026-09-28 00:00 UTC));
    }

    #[test]
    fn test_a_negative_offset_puts_the_night_before_utc_in_the_right_day() {
        // UTC-07:00, the American west coast outside daylight saving. 02:30 UTC
        // on the 28th is 19:30 on the 27th locally, so it is the 27th's usage.
        let period = period("2026-09-27", tz(-7 * 60));
        assert_eq!(period.start, datetime!(2026-09-27 07:00 UTC));
        assert_eq!(period.end, datetime!(2026-09-28 07:00 UTC));
        assert!(period.contains(datetime!(2026-09-28 02:30 UTC)));
        assert!(!period.contains(datetime!(2026-09-27 06:59:59 UTC)));
    }

    #[test]
    fn test_a_positive_offset_puts_the_evening_in_the_right_day() {
        // UTC+07:00. 20:00 UTC on the 27th is 03:00 on the 28th locally.
        let period = period("2026-09-28", tz(7 * 60));
        assert_eq!(period.start, datetime!(2026-09-27 17:00 UTC));
        assert!(period.contains(datetime!(2026-09-27 20:00 UTC)));
        assert!(!period.contains(datetime!(2026-09-27 16:59:59 UTC)));
    }

    #[test]
    fn test_a_half_hour_offset_is_representable() {
        // India and parts of Australia bill on a +05:30 calendar; a design that
        // only took whole hours would have excluded them.
        let period = period("2026-09-27", tz(5 * 60 + 30));
        assert_eq!(period.start, datetime!(2026-09-26 18:30 UTC));
        assert_eq!(period.end, datetime!(2026-09-27 18:30 UTC));
    }

    #[test]
    fn test_the_period_is_half_open_so_no_instant_is_billed_twice() {
        let tz = tz(0);
        let period = period("2026-09-27", tz);
        assert!(period.contains(period.start), "the first instant is inside");
        assert!(!period.contains(period.end), "the end is the next day");
        let before = period.start - time::Duration::seconds(1);
        assert!(!period.contains(before));
    }

    #[test]
    fn test_day_arithmetic_crosses_month_and_year_boundaries() {
        assert_eq!(day("2026-10-01").previous(), day("2026-09-30"));
        assert_eq!(day("2026-01-01").previous(), day("2025-12-31"));
        // 2028 is a leap year, so February has 29 days.
        assert_eq!(day("2028-03-01").previous(), day("2028-02-29"));
        assert_eq!(day("2026-03-01").previous(), day("2026-02-28"));
        assert_eq!(day("2026-12-31").next(), day("2027-01-01"));
    }

    #[test]
    fn test_the_day_of_an_instant_follows_the_configured_offset() {
        let tz = tz(5 * 60 + 30);
        // 18:29 UTC on the 26th is 23:59 on the 26th in +05:30.
        assert_eq!(
            BillingDay::of(datetime!(2026-09-26 18:29 UTC), tz),
            day("2026-09-26")
        );
        // One second later it is already the 27th.
        assert_eq!(
            BillingDay::of(datetime!(2026-09-26 18:30 UTC), tz),
            day("2026-09-27")
        );
    }

    #[test]
    fn test_an_offset_outside_the_representable_range_is_refused() {
        assert!(BillingTimezone::from_offset_minutes(0).is_ok());
        // UTC+14 is the furthest real-world offset; UTC+15 is not one.
        assert!(BillingTimezone::from_offset_minutes(14 * 60).is_ok());
        assert!(BillingTimezone::from_offset_minutes(-12 * 60).is_ok());
        assert!(BillingTimezone::from_offset_minutes(15 * 60).is_err());
        assert!(BillingTimezone::from_offset_minutes(-13 * 60).is_err());
        assert!(BillingTimezone::from_offset_minutes(i32::MAX).is_err());
        assert_eq!(
            BillingTimezone::from_offset_minutes(25 * 60),
            Err(TimezoneError::OutOfRange {
                offset_minutes: 1500,
                min: -720,
                max: 840
            })
        );
    }

    #[test]
    fn test_a_timezone_renders_the_way_a_config_comment_would_say_it() {
        assert_eq!(tz(0).to_string(), "UTC+00");
        assert_eq!(tz(-7 * 60).to_string(), "UTC-07");
        assert_eq!(tz(5 * 60 + 30).to_string(), "UTC+05:30");
    }

    #[test]
    fn test_period_bounds_are_stored_as_canonical_utc_strings() {
        let (start, end) = period("2026-09-27", tz(-7 * 60)).to_bounds();
        assert_eq!(start, "2026-09-27T07:00:00.000000000Z");
        assert_eq!(end, "2026-09-28T07:00:00.000000000Z");
        // Byte-comparable, which is what the aggregation query relies on.
        assert!(start < end);
    }
}
