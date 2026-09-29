//! Money.
//!
//! # Why integers, and why micro-dollars
//!
//! A price is a number that gets multiplied by an unbounded token count and
//! summed across a day's requests. Done in `f64` that is a rounding bug
//! waiting for a large enough day: `0.1 + 0.2 != 0.3` is a curiosity in a
//! unit test and a reconciliation incident on an invoice. So prices and
//! amounts are integers here, and every intermediate is `i128` so a large
//! request cannot wrap one.
//!
//! The unit is **one micro-dollar** — a millionth of a dollar, `0.000001 USD`.
//! That is not arbitrary: the smallest price this product needs to express is
//! about `$0.002375 / M`, which is `2375` in this unit, and a per-token cost
//! at that price is `0.002375` micro-dollars. Any finer unit and the smallest
//! real price would need more precision than the API is asked to take.
//!
//! # Rounding
//!
//! **Half away from zero**, applied once, at the point a component cost is
//! produced, and never again. That is the whole policy, and the rest of it is
//! about not applying it twice:
//!
//! * Each of the three components (input, cached input, output) is rounded to
//!   whole micro-dollars independently, because a statement line shows them
//!   separately and a line whose parts do not add to its total is not an
//!   invoice, it is a puzzle.
//! * A line's total is the **sum of its rounded components**, never a fresh
//!   multiplication of the summed tokens. Recomputing would round a fourth
//!   time and disagree with the parts by up to a micro-dollar per line.
//! * A statement's total is the **sum of its lines' totals**, so
//!   `sum(lines) == statement.total` holds exactly, in the database, with no
//!   reconciliation pass.
//!
//! The consequence to accept rather than hide: a request small enough to cost
//! less than half a micro-dollar rounds to nothing. At a realistic price that
//! is a fraction of a token, and the alternative — accumulating a fractional
//! remainder across requests — would make a statement total depend on which
//! requests happened to be in it, which is a worse property than a fraction of
//! a micro-dollar.
//!
//! # What is never done here
//!
//! There is no `unwrap`, no saturating arithmetic and no default. A missing
//! input, a missing price or a provider that reports more cached tokens than
//! input tokens are all errors, and the caller records the request as
//! *billing-incomplete* and charges nothing for it. Inventing a `0` is the one
//! outcome this module exists to make impossible: `0` is a price, a real one,
//! and it is the answer a customer would be most upset to be given by accident.

use std::fmt;

/// A price or an amount, in micro-dollars (1 USD = 1_000_000).
///
/// `i64` rather than `i128` for storage and for the value itself: the largest
/// amount this could hold is about 9.2 trillion dollars, and the intermediates
/// that produce it are `i128` (see [`component_cost`]). Keeping the stored type
/// narrow is what lets it bind straight to SQLite's `INTEGER` with no
/// conversion and no chance of a silent narrowing.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct MicroUsd(i64);

/// A price, in micro-dollars per **million** tokens.
///
/// A separate type from [`MicroUsd`] because the two are not interchangeable and
/// the compiler should say so. A price times a token count is an amount; adding
/// two prices is meaningless, and a single `i64` for both would let that
/// mistake compile.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Default, Hash)]
pub struct PricePerMillion(i64);

/// Tokens per price unit. A price is *per million* tokens, which is how every
/// provider quotes one.
pub const TOKENS_PER_PRICE_UNIT: i128 = 1_000_000;

/// Decimal places a price is configured with, and therefore the precision of a
/// micro-dollar amount when a human writes one.
pub const PRICE_DECIMAL_PLACES: u32 = 6;

impl MicroUsd {
    pub const ZERO: Self = Self(0);

    /// The underlying integer, for SQLite.
    pub fn as_i64(self) -> i64 {
        self.0
    }

    /// Build from a raw integer. Only for values that came out of this module
    /// or out of the database — not for arithmetic in a `u64` token count.
    pub fn from_i64(v: i64) -> Self {
        Self(v)
    }

    /// Add, refusing to wrap.
    ///
    /// A silently wrapped total is a negative invoice, and the sum that caused
    /// it would be a day's usage at a price large enough to matter — so the
    /// failure is reported rather than clamped to `i64::MAX`, which would be
    /// just as wrong in the other direction and harder to notice.
    pub fn checked_add(self, other: Self) -> Option<Self> {
        self.0.checked_add(other.0).map(Self)
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }
}

impl fmt::Display for MicroUsd {
    /// Render with the unit spelled out, e.g. `$0.095000`.
    ///
    /// The sign goes *outside* the unit — `-$0.095000`, not `$-0.095000` — so a
    /// negative amount reads the way a finance tool would write it. No amount
    /// here is ever negative: a price is a non-negative `i64`, token counts are
    /// `u64`, and every sum is checked. The branch exists because a formatter
    /// that assumes its own invariant is a formatter that produces nonsense
    /// silently.
    ///
    /// The trailing digits are deliberate: an amount rendered without them
    /// loses sub-cent precision on the way to an email, and a partner reading
    /// `$0.1` cannot tell it from `$0.10`.
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let body = format_micros(self.0);
        if let Some(digits) = body.strip_prefix('-') {
            write!(f, "-${digits}")
        } else {
            write!(f, "${body}")
        }
    }
}

impl PricePerMillion {
    pub const ZERO: Self = Self(0);

    pub fn new(micro_usd_per_million: i64) -> Self {
        Self(micro_usd_per_million)
    }

    pub fn as_i64(self) -> i64 {
        self.0
    }

    pub fn is_zero(self) -> bool {
        self.0 == 0
    }

    /// Parse a price as an operator writes it: dollars per million tokens,
    /// as a decimal string.
    ///
    /// `"0.095"` is `95000`, `"0.0475"` is `47500`, `"0.002375"` is `2375`.
    ///
    /// Parsed digit by digit rather than through `f64`, because the whole
    /// reason the unit is micro-dollars is that the arithmetic is exact, and
    /// routing the configuration through a float would reintroduce the bug one
    /// layer below the one that was fixed. The unit is six decimal places — the
    /// smallest real price, `$0.002375`, is `2375` of them — and a longer
    /// fraction is rounded half away from zero, after which further digits
    /// cannot affect the result.
    ///
    /// Rejected, rather than coerced: an empty string, a sign, scientific
    /// notation, whitespace, any non-digit, and an empty fraction. `"-0.1"` is a
    /// refund, and a refund is not something a partner can be configured into by
    /// accident.
    pub fn parse(text: &str) -> Result<Self, PriceError> {
        let (whole, fraction) = match text.split_once('.') {
            Some((whole, fraction)) => (whole, Some(fraction)),
            None => (text, None),
        };

        // A leading `.` is a price written the short way (".095"), so an empty
        // whole part is allowed; a trailing `.` is a number that stopped typing
        // and is refused, as is a bare ".". Both are read by an operator as
        // unambiguous, and only one of them is.
        let whole_digits = whole.bytes().all(|b| b.is_ascii_digit());
        let fraction_digits = match fraction {
            // A trailing `.` is a number that stopped typing.
            Some("") => false,
            Some(f) => f.bytes().all(|b| b.is_ascii_digit()),
            None => true,
        };
        if !whole_digits || !fraction_digits || (whole.is_empty() && fraction.is_none()) {
            return Err(PriceError::NotANumber(text.to_string()));
        }

        // The whole part is accumulated as a plain decimal integer and scaled
        // into the unit once at the end; the fraction is then added at its own
        // place value. Every step is integer, so the value never passes through
        // a float: `1.005` is exactly 1_005_000, not the 1_004_999 that
        // `(1.005 * 1e6) as i64` would produce.
        let mut whole_value: i128 = 0;
        for b in whole.bytes() {
            whole_value = whole_value
                .checked_mul(10)
                .and_then(|v| v.checked_add(i128::from(b - b'0')))
                .ok_or_else(|| PriceError::OutOfRange(text.to_string()))?;
        }
        let mut micros = whole_value
            .checked_mul(10_i128.pow(PRICE_DECIMAL_PLACES))
            .ok_or_else(|| PriceError::OutOfRange(text.to_string()))?;

        let mut seen = 0u32;
        for b in fraction.unwrap_or_default().bytes() {
            if seen < PRICE_DECIMAL_PLACES {
                micros += i128::from(b - b'0') * 10_i128.pow(PRICE_DECIMAL_PLACES - seen - 1);
                seen += 1;
            } else if b >= b'5' {
                // Half away from zero, and only ever upward here because the
                // parser refused a sign: an amount parsed this way is positive.
                micros += 1;
                break;
            } else {
                break;
            }
        }

        i64::try_from(micros)
            .map(Self)
            .map_err(|_| PriceError::OutOfRange(text.to_string()))
    }

    /// Render as the decimal string an operator would type, dropping the
    /// trailing zeros [`format_micros`] pads to the fixed six places:
    /// `95000` is `"0.095"`, `1000` is `"0.001"`, and `0` is `"0"`.
    ///
    /// [`PricePerMillion::parse`] reads that form back to the same value, so
    /// this is the round-trip an API and an email should show a human — the
    /// fixed-width [`fmt::Display`] rendering is for the cost column, where
    /// columns line up.
    pub fn to_decimal_string(self) -> String {
        let rendered = format_micros(self.0);
        let trimmed = rendered.trim_end_matches('0');
        let trimmed = trimmed.strip_suffix('.').unwrap_or(trimmed);
        trimmed.to_string()
    }
}

impl fmt::Display for PricePerMillion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&format_micros(self.0))
    }
}

/// The three prices a model is metered at, as they were when a request was
/// accepted.
///
/// A *snapshot*, not a lookup: it is written onto the usage row at accept time
/// so that a price change during the day produces two snapshots inside one
/// statement instead of silently repricing the hours before it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PricingSnapshot {
    pub input: PricePerMillion,
    pub cached_input: PricePerMillion,
    pub output: PricePerMillion,
}

impl PricingSnapshot {
    pub fn new(
        input: PricePerMillion,
        cached_input: PricePerMillion,
        output: PricePerMillion,
    ) -> Self {
        Self {
            input,
            cached_input,
            output,
        }
    }

    /// The three prices as a tuple, for a SQL `IN` clause over a grouped
    /// aggregate.
    pub fn as_tuple(self) -> (i64, i64, i64) {
        (
            self.input.as_i64(),
            self.cached_input.as_i64(),
            self.output.as_i64(),
        )
    }

    /// Build from the three columns of a `usage_records` row.
    ///
    /// The `Option`s are the whole point: `NULL` means the request was accepted
    /// with no billing configuration, and collapsing that to `0` here would
    /// charge a real, wrong price of nothing. It is an error instead, and the
    /// statement records the request as incomplete.
    pub fn from_columns(
        input: Option<i64>,
        cached_input: Option<i64>,
        output: Option<i64>,
    ) -> Result<Self, PriceError> {
        Ok(Self::new(
            PricePerMillion::new(require(input, "input_price_snapshot")?),
            PricePerMillion::new(require(cached_input, "cached_input_price_snapshot")?),
            PricePerMillion::new(require(output, "output_price_snapshot")?),
        ))
    }
}

/// Why a row cannot be priced.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum PriceError {
    /// One of the three prices was not configured when the request was
    /// accepted. Not a zero price — an absent one.
    NoPriceSnapshot { column: &'static str },
    /// The provider reported incomplete usage, so there is nothing to charge.
    IncompleteUsage,
    /// More cached tokens than input tokens. A provider cannot have cached more
    /// of the prompt than the prompt was long, so this row's usage is not
    /// believable and the honest response is to charge nothing for it and say
    /// so. Clamping to `input` would invent a usage figure, which is the one
    /// thing this module must never do.
    CachedExceedsInput {
        input_tokens: u64,
        cached_tokens: u64,
    },
    /// A token count times a price did not fit in an `i64` micro-dollars.
    Overflow { context: &'static str },
    /// A configured price was not a plain decimal number.
    NotANumber(String),
    /// A configured price did not fit in the storage type.
    OutOfRange(String),
}

impl fmt::Display for PriceError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PriceError::NoPriceSnapshot { column } => {
                write!(f, "no billing configuration: {column} is not set")
            }
            PriceError::IncompleteUsage => {
                write!(f, "the provider did not report usage in full")
            }
            PriceError::CachedExceedsInput {
                input_tokens,
                cached_tokens,
            } => write!(
                f,
                "upstream reported {cached_tokens} cached tokens out of {input_tokens} \
                 input tokens"
            ),
            PriceError::Overflow { context } => {
                write!(f, "the amount overflowed micro-dollars while {context}")
            }
            PriceError::NotANumber(text) => {
                write!(f, "{text:?} is not a price in dollars per million tokens")
            }
            PriceError::OutOfRange(text) => write!(f, "the price {text:?} is out of range"),
        }
    }
}

impl std::error::Error for PriceError {}

fn require(value: Option<i64>, column: &'static str) -> Result<i64, PriceError> {
    value.ok_or(PriceError::NoPriceSnapshot { column })
}

/// `tokens × price / 1_000_000`, rounded half away from zero, in `i128`.
///
/// The intermediates are `i128` because `tokens` is a `u64` a provider chose
/// and `price` is an `i64` an operator chose, and their product is up to 2^127 —
/// which fits. It is computed in full and only then narrowed, so the value that
/// reaches the narrowing is the exact one.
fn component_cost(tokens: u64, price: PricePerMillion) -> Result<MicroUsd, PriceError> {
    let overflow = || PriceError::Overflow {
        context: "pricing a token count",
    };
    let product = i128::from(tokens)
        .checked_mul(i128::from(price.as_i64()))
        .ok_or_else(overflow)?;
    // Every step is checked, including the two used by the rounding itself, so
    // a provider-reported count near the top of `u64` against a price near the
    // top of `i64` is a refusal rather than a panic. A panic here would be
    // inside a `spawn_blocking` in the statement worker, and a billing worker
    // that dies on a large number stops billing.
    let rounded = product
        .checked_mul(2)
        .and_then(|p| p.checked_add(TOKENS_PER_PRICE_UNIT))
        .map(|p| p / (TOKENS_PER_PRICE_UNIT * 2))
        .ok_or_else(overflow)?;
    i64::try_from(rounded).map(MicroUsd).map_err(|_| overflow())
}

/// The three component costs of one line, each rounded independently.
///
/// The `u64` token counts are the *reported* ones. Cached tokens are a subset
/// of input tokens, so input is charged at the input price only on the portion
/// that was not cached: charging the reported `input_tokens` at the input price
/// *and* the cached ones at the cached price would bill every cached request
/// twice for its prompt.
pub fn components(
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    prices: PricingSnapshot,
) -> Result<(MicroUsd, MicroUsd, MicroUsd), PriceError> {
    if cached_input_tokens > input_tokens {
        return Err(PriceError::CachedExceedsInput {
            input_tokens,
            cached_tokens: cached_input_tokens,
        });
    }
    let uncached = input_tokens - cached_input_tokens;

    Ok((
        component_cost(uncached, prices.input)?,
        component_cost(cached_input_tokens, prices.cached_input)?,
        component_cost(output_tokens, prices.output)?,
    ))
}

/// One line of a statement: the tokens, the prices, and what they cost.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LineCost {
    pub uncached_input_tokens: u64,
    pub input_cost: MicroUsd,
    pub cached_input_cost: MicroUsd,
    pub output_cost: MicroUsd,
    pub total: MicroUsd,
}

impl LineCost {
    /// The sum of the three components.
    ///
    /// The parts are added; the sum is not recomputed from the tokens. That is
    /// what makes `input + cached + output == total` hold for every line
    /// instead of holding approximately, and it is the difference between a
    /// statement that adds up and one an operator has to reconcile by hand.
    pub fn total(
        input_cost: MicroUsd,
        cached_input_cost: MicroUsd,
        output_cost: MicroUsd,
        uncached_input_tokens: u64,
    ) -> Result<Self, PriceError> {
        let total = input_cost
            .checked_add(cached_input_cost)
            .and_then(|v| v.checked_add(output_cost))
            .ok_or(PriceError::Overflow {
                context: "adding up a line's component costs",
            })?;
        Ok(Self {
            uncached_input_tokens,
            input_cost,
            cached_input_cost,
            output_cost,
            total,
        })
    }
}

/// Price a line and return it whole, or explain why it cannot be priced.
///
/// This is the only entry point the statement generator uses. Everything about
/// the refusal — which rows, which reason — is the caller's to record; this
/// function's contract is that a returned `LineCost` is fully determined by the
/// arguments and a returned `Err` has charged nothing.
pub fn price_line(
    input_tokens: u64,
    cached_input_tokens: u64,
    output_tokens: u64,
    prices: PricingSnapshot,
) -> Result<LineCost, PriceError> {
    let (input_cost, cached_input_cost, output_cost) =
        components(input_tokens, cached_input_tokens, output_tokens, prices)?;
    LineCost::total(
        input_cost,
        cached_input_cost,
        output_cost,
        input_tokens - cached_input_tokens,
    )
}

/// Split a `Usage` into the two numbers a charge is computed from, or refuse.
///
/// `None` for either token count is [`PriceError::IncompleteUsage`], not a
/// zero. This is invariant 3 arriving at the money: "unavailable is not zero"
/// has to mean it in dollars too, and the only way it can is if the function
/// that multiplies by a price is the function that refuses.
pub fn billable(
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cached_input_tokens: Option<u64>,
) -> Result<(u64, u64, u64), PriceError> {
    let Some(input_tokens) = input_tokens else {
        return Err(PriceError::IncompleteUsage);
    };
    let Some(output_tokens) = output_tokens else {
        return Err(PriceError::IncompleteUsage);
    };
    // An unreported cached count is *not* zero, so a line priced with a
    // different cached price is incomplete rather than assuming the cheaper
    // case. Assuming `0` cached would bill the whole prompt at the input price
    // when the provider may have cached most of it — that is not a rounding
    // question, it is the wrong amount in a direction the customer prefers.
    // When the two prices are equal the question does not arise, and no
    // assumption is made either way.
    Ok((
        input_tokens,
        output_tokens,
        cached_input_tokens.unwrap_or(0),
    ))
}

/// Whether a cached count must be known before this line can be charged.
pub fn needs_cached_count(prices: PricingSnapshot) -> bool {
    prices.cached_input != prices.input
}

/// Format a micro-dollar amount as a plain decimal, with the unit implied.
///
/// `95000` is `"0.095"`. Implemented by integer division and remainder so the
/// output is exact for every value `i64` can hold, including negatives.
fn format_micros(micros: i64) -> String {
    let scale = 10_i64.pow(PRICE_DECIMAL_PLACES);
    let negative = micros < 0;
    // `unsigned_abs` so `-i64::MIN` is not a panic. An amount this large cannot
    // occur — `checked_add` would have refused long before — but the formatter
    // is also used for prices read straight out of the database, and a debug
    // formatter that can panic on a value a row contains is a bad neighbour.
    let magnitude = micros.unsigned_abs();
    let scale_u = scale as u64;
    let whole = magnitude / scale_u;
    let fraction = magnitude % scale_u;

    let mut out = String::with_capacity(16);
    if negative && (whole > 0 || fraction > 0) {
        out.push('-');
    }
    out.push_str(&whole.to_string());
    out.push('.');
    out.push_str(&format!(
        "{fraction:0>width$}",
        width = PRICE_DECIMAL_PLACES as usize
    ));
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    fn price(v: i64) -> PricePerMillion {
        PricePerMillion::new(v)
    }

    fn snapshot(input: i64, cached: i64, output: i64) -> PricingSnapshot {
        PricingSnapshot::new(price(input), price(cached), price(output))
    }

    #[test]
    fn test_a_price_parses_from_the_way_an_operator_writes_one() {
        // The four prices in the product's own pricing table, so the mapping
        // between what a human types and what is stored is pinned.
        for (text, expected) in [
            ("0.095", 95_000i64),
            ("0.0475", 47_500),
            ("0.475", 475_000),
            ("0.002375", 2_375),
            ("0", 0),
            ("0.0", 0),
            ("1", 1_000_000),
            ("12.5", 12_500_000),
        ] {
            assert_eq!(
                PricePerMillion::parse(text).unwrap(),
                price(expected),
                "{text} must parse to {expected} micro-USD per million"
            );
        }
    }

    #[test]
    fn test_price_parsing_is_exact_where_a_float_would_not_be() {
        // 0.1 + 0.2 in f64 is 0.30000000000000004. Every one of these is a
        // price someone could type, and every one must be stored exactly, so
        // the rendering is a plain decimal, not a binary fraction.
        for (text, expected) in [
            ("0.1", 100_000i64),
            ("0.2", 200_000),
            ("0.3", 300_000),
            ("0.7", 700_000),
            ("1.005", 1_005_000),
            ("2.675", 2_675_000),
            ("0.095", 95_000),
        ] {
            let parsed = PricePerMillion::parse(text).unwrap();
            assert_eq!(parsed, price(expected), "{text} must be stored exactly");
            // Round-tripping through the rendering must not change the value.
            let shown = parsed.to_decimal_string();
            assert_eq!(
                PricePerMillion::parse(&shown).unwrap(),
                parsed,
                "{text} -> {shown} must round-trip"
            );
        }
    }

    #[test]
    fn test_a_price_with_more_precision_than_micro_dollars_rounds_half_away_from_zero() {
        // 7 fractional digits: the last one decides, and it must decide the
        // same way every time.
        assert_eq!(PricePerMillion::parse("0.0000005").unwrap(), price(1));
        assert_eq!(PricePerMillion::parse("0.0000004").unwrap(), price(0));
        assert_eq!(PricePerMillion::parse("0.0000004999").unwrap(), price(0));
        assert_eq!(PricePerMillion::parse("0.00000050001").unwrap(), price(1));
    }

    #[test]
    fn test_a_price_that_is_not_a_number_is_refused_rather_than_coerced() {
        // A negative price is a refund, and a refund is not something a partner
        // gets configured into by a typo. Scientific notation, a leading sign
        // and stray text are all refused for the same reason: each of them
        // would otherwise become some number, and which number is not obvious
        // to whoever is reading the API response.
        for text in [
            "-0.1",
            "+0.1",
            "1e-3",
            "0.095 ",
            " 0.095",
            "0.095 USD",
            "",
            ".",
            "0.",
            "abc",
            "0,095",
        ] {
            assert_eq!(
                PricePerMillion::parse(text),
                Err(PriceError::NotANumber(text.to_string())),
                "{text:?} must be refused, not coerced"
            );
        }
    }

    #[test]
    fn test_a_price_too_large_to_store_is_refused() {
        // Not a wrapping number: refusing is the only honest answer, because a
        // wrapped price is a negative or tiny price and both are wrong.
        let err = PricePerMillion::parse("99999999999999999999").unwrap_err();
        assert_eq!(err, PriceError::OutOfRange("99999999999999999999".into()));
    }

    #[test]
    fn test_a_million_tokens_at_a_price_costs_exactly_that_price() {
        // The definition, checked: a million tokens at $0.095/M is $0.095.
        let (input, cached, output) =
            components(1_000_000, 0, 0, snapshot(95_000, 95_000, 475_000)).unwrap();
        assert_eq!(input, MicroUsd(95_000));
        assert_eq!(cached, MicroUsd(0));
        assert_eq!(output, MicroUsd(0));
    }

    #[test]
    fn test_cached_tokens_are_charged_at_the_cached_price_and_the_rest_at_the_input_price() {
        // input=1000, cached=800, output=200 — the case from the product
        // requirements, and the one that catches a double charge.
        let line = price_line(1000, 800, 200, snapshot(95_000, 10_000, 475_000)).unwrap();

        // 200 uncached × 95000/1e6 = 19
        assert_eq!(line.uncached_input_tokens, 200);
        assert_eq!(line.input_cost, MicroUsd(19));
        // 800 cached × 10000/1e6 = 8
        assert_eq!(line.cached_input_cost, MicroUsd(8));
        // 200 output × 475000/1e6 = 95
        assert_eq!(line.output_cost, MicroUsd(95));
        assert_eq!(line.total, MicroUsd(122));
    }

    #[test]
    fn test_cached_tokens_are_never_charged_twice() {
        // The whole point, stated as an inequality: charging the reported input
        // count at the input price *and* the cached count at the cached price
        // would put this number well above the true cost.
        let prices = snapshot(95_000, 10_000, 475_000);
        let line = price_line(1000, 800, 200, prices).unwrap();
        // What the same request would cost if all 1000 input tokens were billed
        // at the input price and no cached rate were applied at all.
        let unadjusted = price_line(1000, 0, 200, prices).unwrap();
        assert_eq!(unadjusted.uncached_input_tokens, 1000);
        assert!(
            line.input_cost < unadjusted.input_cost,
            "the uncached portion must be 200 tokens, not 1000"
        );
        assert_eq!(line.uncached_input_tokens, 200);
    }

    #[test]
    fn test_more_cached_tokens_than_input_is_refused_rather_than_clamped() {
        // A provider cannot have cached more of the prompt than the prompt was
        // long. Clamping would invent a usage figure; this refuses and lets the
        // caller record the row as incomplete.
        let err = price_line(100, 200, 50, snapshot(95_000, 10_000, 475_000)).unwrap_err();
        assert_eq!(
            err,
            PriceError::CachedExceedsInput {
                input_tokens: 100,
                cached_tokens: 200
            }
        );

        // Exactly equal is not an error: every token cached is a real provider
        // behaviour, and it must bill as zero uncached rather than be refused.
        let line = price_line(100, 100, 0, snapshot(95_000, 10_000, 475_000)).unwrap();
        assert_eq!(line.uncached_input_tokens, 0);
        assert_eq!(line.input_cost, MicroUsd(0));
        assert_eq!(line.cached_input_cost, MicroUsd(1));
    }

    #[test]
    fn test_a_zero_price_charges_nothing_and_is_not_an_error() {
        // An operator may genuinely configure a model as free. That is a
        // price, it was typed to mean that, and it is different in kind from an
        // absent price — which this module refuses.
        let line = price_line(1_000_000, 0, 1_000_000, snapshot(0, 0, 0)).unwrap();
        assert_eq!(line.total, MicroUsd(0));
        assert_eq!(line.uncached_input_tokens, 1_000_000);
    }

    #[test]
    fn test_a_missing_price_is_refused_and_is_never_read_as_zero() {
        // Invariant 3 reaching the money. If this returned `Ok(0)` the cheapest
        // possible bug in the product would be free inference for everyone.
        assert_eq!(
            PricingSnapshot::from_columns(Some(1), Some(2), None),
            Err(PriceError::NoPriceSnapshot {
                column: "output_price_snapshot"
            })
        );
        assert_eq!(
            PricingSnapshot::from_columns(None, Some(2), Some(3)),
            Err(PriceError::NoPriceSnapshot {
                column: "input_price_snapshot"
            })
        );
        assert_eq!(
            PricingSnapshot::from_columns(Some(1), None, Some(3)),
            Err(PriceError::NoPriceSnapshot {
                column: "cached_input_price_snapshot"
            })
        );
    }

    #[test]
    fn test_a_complete_set_of_nulls_is_a_refusal_not_a_free_request() {
        assert_eq!(
            PricingSnapshot::from_columns(None, None, None),
            Err(PriceError::NoPriceSnapshot {
                column: "input_price_snapshot"
            })
        );
    }

    #[test]
    fn test_rounding_is_half_away_from_zero_and_applied_once() {
        // 1 token at 95000/M is 0.095 micro-dollars -> 0.
        // 1 token at 500000/M is exactly 0.5 -> 1.
        // 3 tokens at 166667/M is 0.500001 -> 1.
        // Each component is rounded on its own, which is why a line's parts can
        // each be a micro-dollar off the exact value and still be correct.
        assert_eq!(component_cost(1, price(95_000)).unwrap(), MicroUsd(0));
        assert_eq!(component_cost(1, price(500_000)).unwrap(), MicroUsd(1));
        assert_eq!(component_cost(3, price(166_667)).unwrap(), MicroUsd(1));
        assert_eq!(component_cost(1, price(499_999)).unwrap(), MicroUsd(0));
        // A half rounds up, not down: the convention is stated in the module
        // docs and this is where it is pinned.
        assert_eq!(component_cost(2, price(250_000)).unwrap(), MicroUsd(1));
    }

    #[test]
    fn test_a_line_total_is_the_sum_of_its_parts_and_never_a_fourth_rounding() {
        // Two input tokens, one of them cached, and one output token, all at
        // $0.5/M: each component is exactly half a micro-dollar and each rounds
        // up, so the line is three micro-dollars where the exact arithmetic
        // says 1.5. A total recomputed from the summed tokens would round the
        // 2.5 down to 2 and a partner would be reading a bill that does not
        // add up. `price_line(2, 1, 1, ..)` and the three components below are
        // the two answers, and the sum of the parts is the one that ships.
        let prices = snapshot(500_000, 500_000, 500_000);
        let line = price_line(2, 1, 1, prices).unwrap();
        assert_eq!(line.uncached_input_tokens, 1);
        assert_eq!(line.input_cost, component_cost(1, price(500_000)).unwrap());
        assert_eq!(
            line.cached_input_cost,
            component_cost(1, price(500_000)).unwrap()
        );
        assert_eq!(line.output_cost, component_cost(1, price(500_000)).unwrap());
        assert_eq!(line.total, MicroUsd(3));

        // Recomputing the total from the summed tokens is the answer the code
        // refuses to give, and the two genuinely differ.
        let tokens_recomputed = component_cost(2, price(500_000))
            .unwrap()
            .checked_add(component_cost(2, price(500_000)).unwrap())
            .unwrap();
        assert_eq!(tokens_recomputed, MicroUsd(2));
        assert_ne!(tokens_recomputed, line.total);
    }

    #[test]
    fn test_a_statement_total_is_the_sum_of_its_line_totals() {
        // The property the uniqueness constraint and the invoice both rest on.
        let prices = snapshot(95_000, 10_000, 475_000);
        let lines = [
            price_line(1000, 800, 200, prices).unwrap(),
            price_line(7, 0, 3, prices).unwrap(),
            price_line(999_983, 1, 1, prices).unwrap(),
        ];
        let summed = lines
            .iter()
            .fold(MicroUsd(0), |acc, l| acc.checked_add(l.total).unwrap());
        let recomputed = price_line(1000 + 7 + 999_983, 801, 200 + 3 + 1, prices)
            .unwrap()
            .total;
        assert_ne!(
            summed, recomputed,
            "aggregating per line and re-deriving from the totals need not agree — \
             which is why statements carry lines"
        );
    }

    #[test]
    fn test_incomplete_usage_is_refused_rather_than_billed_as_zero() {
        // No input, or no output, or neither: the provider did not say, and
        // "did not say" is not "zero".
        assert_eq!(
            billable(None, Some(10), Some(0)),
            Err(PriceError::IncompleteUsage)
        );
        assert_eq!(
            billable(Some(10), None, Some(0)),
            Err(PriceError::IncompleteUsage)
        );
        assert_eq!(billable(None, None, None), Err(PriceError::IncompleteUsage));
        // Both present is billable, whatever the cached count.
        assert_eq!(billable(Some(10), Some(3), None), Ok((10, 3, 0)));
    }

    #[test]
    fn test_a_cached_count_is_required_exactly_when_the_prices_differ() {
        // Assuming zero cached when the provider did not report it would bill
        // the whole prompt at the input price. When the two prices are the
        // same the question cannot change the answer, and nothing is assumed.
        let differing = snapshot(95_000, 10_000, 475_000);
        assert!(needs_cached_count(differing));
        assert!(!needs_cached_count(snapshot(95_000, 95_000, 475_000)));
        assert!(!needs_cached_count(snapshot(0, 0, 0)));
    }

    #[test]
    fn test_a_large_token_count_does_not_wrap() {
        // A provider-reported count of 5x10^14 tokens at $1000/M is $5x10^14
        // micro-dollars — $500 trillion, which no statement will ever show and
        // which still fits: the `i128` intermediate is what keeps a day's usage
        // from wrapping to something small and billable-looking.
        let cost = component_cost(500_000_000_000_000, price(1_000_000_000)).unwrap();
        assert_eq!(cost, MicroUsd(500_000_000_000_000_000));
        // And one that genuinely does not fit — the count alone overflows the
        // `i128` intermediate — is refused rather than clamped or panicked on.
        let err = component_cost(u64::MAX, price(i64::MAX)).unwrap_err();
        assert!(matches!(err, PriceError::Overflow { .. }), "{err}");
    }

    #[test]
    fn test_an_amount_that_would_overflow_the_sum_is_refused() {
        let huge = MicroUsd(i64::MAX);
        assert!(huge.checked_add(MicroUsd(1)).is_none());
        assert_eq!(huge.checked_add(MicroUsd(0)), Some(huge));
    }

    #[test]
    fn test_amounts_render_with_their_sub_cent_precision() {
        assert_eq!(MicroUsd(95_000).to_string(), "$0.095000");
        assert_eq!(MicroUsd(0).to_string(), "$0.000000");
        assert_eq!(MicroUsd(1).to_string(), "$0.000001");
        assert_eq!(MicroUsd(1_000_000).to_string(), "$1.000000");
        assert_eq!(MicroUsd(-95_000).to_string(), "-$0.095000");
        // The `Display` rendering of a price is fixed-width, so amounts line up
        // in a table; the decimal string is the human form.
        assert_eq!(price(95_000).to_string(), "0.095000");
        assert_eq!(price(95_000).to_decimal_string(), "0.095");
        assert_eq!(price(1_000).to_decimal_string(), "0.001");
        assert_eq!(price(0).to_decimal_string(), "0");
    }

    #[test]
    fn test_formatting_cannot_panic_on_the_largest_value() {
        // A formatter that panics on a value a row can contain turns a bad row
        // into a 500 on a read path.
        let _ = MicroUsd(i64::MIN).to_string();
        let _ = MicroUsd(i64::MAX).to_string();
    }

    #[test]
    fn test_a_zero_snapshot_is_all_three_prices_present_and_means_free() {
        // Distinct from `None`, which is the test that matters: the columns can
        // hold 0, and 0 is a legitimate configured price.
        let free = PricingSnapshot::from_columns(Some(0), Some(0), Some(0)).unwrap();
        assert_eq!(free, snapshot(0, 0, 0));
        assert!(free.input.is_zero());
        assert!(!needs_cached_count(free));
    }

    #[test]
    fn test_the_price_tuple_is_what_the_statement_groups_by() {
        // A line is per (model, price snapshot); the group key has to be the
        // three prices exactly, or a mid-day change collapses into one line.
        let s = snapshot(95_000, 10_000, 475_000);
        assert_eq!(s.as_tuple(), (95_000, 10_000, 475_000));
        assert_ne!(s.as_tuple(), snapshot(96_000, 10_000, 475_000).as_tuple());
        assert_ne!(s.as_tuple(), snapshot(95_000, 11_000, 475_000).as_tuple());
        assert_ne!(s.as_tuple(), snapshot(95_000, 10_000, 476_000).as_tuple());
    }
}
