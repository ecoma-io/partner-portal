//! Canonical timestamp formatting for the ledger.
//!
//! # Why a fixed-width format
//!
//! Timestamps are stored as TEXT and compared with SQLite's byte-wise
//! comparison (`>=`, `<`, `ORDER BY`, retention cutoffs, pagination cursors).
//! That is only equivalent to chronological order if every stored string has
//! the **same width**. `time`'s `Iso8601::DEFAULT` output is variable width — it
//! trims trailing zeros and drops the fractional part entirely when it is zero,
//! so `...07:12:33.5Z` would sort *before* `...07:12:33Z`, inverting time order.
//!
//! Everything therefore goes through this module, which always emits a fixed
//! 30-character UTC string with nine fractional digits.

use std::sync::LazyLock;
use time::{OffsetDateTime, format_description::FormatItem};

/// Fixed-width timestamp: `2026-09-24T07:12:33.123456789Z` (30 chars).
pub static TS_FORMAT: LazyLock<Vec<FormatItem<'static>>> = LazyLock::new(|| {
    time::format_description::parse(
        "[year]-[month]-[day]T[hour]:[minute]:[second].[subsecond digits:9]Z",
    )
    .expect("timestamp format is a valid literal")
});

/// Fixed-width hour bucket: `2026-09-24T07` (13 chars).
pub static HOUR_FORMAT: LazyLock<Vec<FormatItem<'static>>> = LazyLock::new(|| {
    time::format_description::parse("[year]-[month]-[day]T[hour]")
        .expect("hour format is a valid literal")
});

/// Format an instant for the `created_at` column. Never fails: falls back to
/// the Unix epoch rather than propagating a formatting error into the ledger.
pub fn format_ts(ts: OffsetDateTime) -> String {
    ts.to_offset(time::UtcOffset::UTC)
        .format(&TS_FORMAT)
        .unwrap_or_else(|_| "1970-01-01T00:00:00.000000000Z".to_string())
}

/// Format an instant as its UTC hour bucket, for `usage_hourly.hour`.
pub fn format_hour(ts: OffsetDateTime) -> String {
    ts.to_offset(time::UtcOffset::UTC)
        .format(&HOUR_FORMAT)
        .unwrap_or_else(|_| "1970-01-01T00".to_string())
}

/// Current time, truncated to whole microseconds.
///
/// `OffsetDateTime::now_utc()` carries nanosecond precision that differs between
/// runs; truncating keeps values stable and comparable in tests and cursors.
pub fn now() -> OffsetDateTime {
    let now = OffsetDateTime::now_utc();
    OffsetDateTime::from_unix_timestamp_nanos((now.unix_timestamp_nanos() / 1_000) * 1_000)
        .unwrap_or(now)
}

/// Parse a stored timestamp back into an instant.
///
/// Accepts the canonical fixed-width form and, for robustness, any ISO 8601
/// input — both are returned as UTC.
pub fn parse_ts(s: &str) -> Option<OffsetDateTime> {
    if let Ok(ts) = OffsetDateTime::parse(s, &TS_FORMAT) {
        return Some(ts.to_offset(time::UtcOffset::UTC));
    }
    OffsetDateTime::parse(s, &time::format_description::well_known::Iso8601::DEFAULT)
        .ok()
        .map(|ts| ts.to_offset(time::UtcOffset::UTC))
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    #[test]
    fn test_fixed_width_regardless_of_fraction() {
        // The whole point: zero and non-zero fractional parts must produce the
        // same string length, so byte order equals time order.
        let whole = datetime!(2026-09-24 07:12:33 UTC);
        let frac = datetime!(2026-09-24 07:12:33.500 UTC);
        let nanos = datetime!(2026-09-24 07:12:33.000000001 UTC);

        let a = format_ts(whole);
        let b = format_ts(frac);
        let c = format_ts(nanos);

        assert_eq!(a.len(), 30, "got {a}");
        assert_eq!(b.len(), 30, "got {b}");
        assert_eq!(c.len(), 30, "got {c}");

        // Lexicographic order is chronological order: whole second, then one
        // nanosecond later, then half a second later.
        assert!(a < c, "{a} must sort before {c}");
        assert!(c < b, "{c} must sort before {b}");
        assert!(a < b);
    }

    #[test]
    fn test_chronological_order_matches_lexicographic_across_day_boundary() {
        let mut times = [
            datetime!(2026-09-24 23:59:59.999999999 UTC),
            datetime!(2026-09-25 00:00:00 UTC),
            datetime!(2026-09-24 07:00:00 UTC),
            datetime!(2026-01-01 00:00:00 UTC),
            datetime!(2026-12-31 23:59:59.5 UTC),
        ];
        times.sort();
        let strings: Vec<String> = times.iter().copied().map(format_ts).collect();
        let mut sorted_strings = strings.clone();
        sorted_strings.sort();
        assert_eq!(strings, sorted_strings);
    }

    #[test]
    fn test_hour_bucket_is_thirteen_chars_and_ordered() {
        let h1 = format_hour(datetime!(2026-09-24 07:59:59.9 UTC));
        let h2 = format_hour(datetime!(2026-09-24 08:00:00 UTC));
        assert_eq!(h1, "2026-09-24T07");
        assert_eq!(h1.len(), 13);
        assert!(h1 < h2);
    }

    #[test]
    fn test_format_is_utc_normalized() {
        let plus_two = datetime!(2026-09-24 09:00:00 +2);
        assert_eq!(format_ts(plus_two), "2026-09-24T07:00:00.000000000Z");
    }

    #[test]
    fn test_parse_round_trip() {
        let ts = datetime!(2026-09-24 07:12:33.123456789 UTC);
        let s = format_ts(ts);
        assert_eq!(parse_ts(&s), Some(ts));
    }

    #[test]
    fn test_parse_accepts_iso8601_without_fraction() {
        let parsed = parse_ts("2026-09-24T07:12:33Z").expect("must parse");
        assert_eq!(parsed, datetime!(2026-09-24 07:12:33 UTC));
    }

    #[test]
    fn test_parse_rejects_garbage() {
        assert!(parse_ts("not-a-timestamp").is_none());
        assert!(parse_ts("").is_none());
    }

    #[test]
    fn test_hour_bucket_comparison_against_hour_bounds() {
        // Regression: a rollup hour bucket must be comparable to hour-granularity
        // bounds. Comparing a 13-char bucket against a 16-char minute bound made
        // the current hour drop out of dashboard summaries.
        let bucket = format_hour(datetime!(2026-09-24 07:30:00 UTC));
        let start = format_hour(datetime!(2026-09-24 07:12:00 UTC));
        let end = format_hour(datetime!(2026-09-24 07:45:00 UTC));
        assert!(
            bucket >= start && bucket <= end,
            "same-hour bucket must be in range"
        );
    }
}
