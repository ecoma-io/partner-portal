//! Service status, derived rather than stored.
//!
//! # Why there is no `status` column on `partners`
//!
//! Suspension is a *consequence*, not a fact. It exists because a partner on
//! invoice terms has a complete statement whose `due_at` has passed and no
//! payment against it. Storing that as a column means something has to keep the
//! column in step with the statements, and the two things that can move it — an
//! overdue statement appearing and a manager clicking *Mark as Paid* — are the
//! two things that most need to be right. A stored flag that is one write behind
//! either is a partner who is suspended after paying, or a partner who keeps
//! serving after the deadline; both are wrong in the direction that costs
//! money.
//!
//! So the status is a pure function of the statement table:
//!
//! ```text
//! invoice        && ∃ complete statement with paid_at IS NULL
//!               && ∃ that statement with due_at <= now
//!             => Suspended
//! otherwise    => Active
//! ```
//!
//! and the same expression runs in the request path's snapshot refresh, in the
//! dashboard read, and in the admin API. There is no fourth place for it to
//! disagree with.
//!
//! # What "complete" means here, and why it is part of the predicate
//!
//! A statement carrying `incomplete_usage_count > 0` did not manage to account
//! for every request in its day. Suspending a partner over an invoice the
//! product itself knows is incomplete is a bill the product cannot defend, so an
//! incomplete statement never suspends. It is still a statement, it is still
//! shown, and it is still owed — the count rides along on it so a human can see
//! why. That is a deliberate limit on automatic enforcement, and it is the
//! product requirement rather than a compromise this module made.
//!
//! # Reconciliation is never suspended
//!
//! A reconciliation partner is statemented for the settlement record and owes
//! nothing. There is no deadline to miss, so there is nothing to enforce, and
//! the honest implementation of "enforce the payment obligation" for them is to
//! not enforce one. It is deliberately **not** modelled as an unlimited
//! invoice, a zero price, or a `due_at` in the year 9999: each of those would
//! be a real invoice with a real total, showing on a real bill, and would make
//! "does this partner owe money" a question with the wrong answer. The mode is
//! checked first and the rest of the predicate does not run for it.
//!
//! # Lag is accepted
//!
//! The request path evaluates this from a snapshot refreshed on the same
//! periodic sweep as the API-key snapshot, so a partner whose deadline passed
//! thirty seconds ago is still served for up to one refresh interval. That lag
//! is the price of never opening SQLite on the request path, and it is bounded
//! by the same `api_key_refresh_ms` that already bounds a key revocation.

use std::fmt;

use crate::billing::pricing::MicroUsd;
use crate::ledger::timefmt;
use time::OffsetDateTime;

/// The oldest statement that can suspend a partner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverdueStatement {
    /// The statement's `id`, for the log line and for the API that explains the
    /// refusal.
    pub id: i64,
    /// `YYYY-MM-DD`, so the log says which bill.
    pub billing_date: String,
    /// The instant the payment was due.
    pub due_at: OffsetDateTime,
    /// What is owed, in micro-dollars.
    pub amount: MicroUsd,
}

/// Whether this partner may be served.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServiceStatus {
    Active,
    /// A complete invoice is past its due date. The field is the reason; the
    /// request path turns it into `403 billing_suspended`.
    Suspended {
        reason: SuspensionReason,
    },
}

/// Why a partner is suspended, in the terms a human needs: which bill, for how
/// much, and since when.
///
/// Carried on the status rather than logged and forgotten, because the same
/// value is what the dashboard shows a partner asking why their calls stopped
/// — "an invoice is overdue" with no bill attached is not an answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SuspensionReason {
    /// The oldest overdue complete statement, which is the one the partner has
    /// to deal with first.
    InvoiceOverdue(OverdueStatement),
}

impl ServiceStatus {
    pub fn is_active(&self) -> bool {
        matches!(self, ServiceStatus::Active)
    }

    pub fn is_suspended(&self) -> bool {
        !self.is_active()
    }

    /// The overdue bill this suspension is about, if there is one.
    pub fn suspension_reason(&self) -> Option<&SuspensionReason> {
        match self {
            ServiceStatus::Active => None,
            ServiceStatus::Suspended { reason } => Some(reason),
        }
    }

    /// The message a suspended request is refused with.
    ///
    /// One string, one meaning, on every refusal. It says the *cause* and not
    /// the remedy, because the remedy is a manager marking a bill paid and
    /// telling the partner that already happened.
    pub fn suspension_message(&self) -> Option<&'static str> {
        match self {
            ServiceStatus::Active => None,
            ServiceStatus::Suspended { .. } => {
                Some("Service is suspended because an invoice is overdue")
            }
        }
    }
}

impl fmt::Display for ServiceStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            ServiceStatus::Active => f.write_str("active"),
            ServiceStatus::Suspended { reason } => match reason {
                SuspensionReason::InvoiceOverdue(statement) => write!(
                    f,
                    "suspended: statement {} for {} of {} was due {}",
                    statement.id,
                    statement.billing_date,
                    statement.amount,
                    timefmt::format_ts(statement.due_at)
                ),
            },
        }
    }
}

/// One row of the overdue-statement lookup, as the snapshot refresh reads it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OverdueRow {
    pub billing_mode: String,
    pub id: i64,
    pub billing_date: String,
    pub due_at: String,
    pub total_amount_micro_usd: i64,
    pub incomplete_usage_count: i64,
}

/// Decide a partner's service status.
///
/// The `invoice` test is on the *partner*, and the rest is on the statements —
/// so the one query this is derived from is
/// `SELECT ... FROM daily_statements WHERE consumer_id = ? AND billing_mode =
/// 'invoice' AND paid_at IS NULL AND due_at <= ? ORDER BY due_at LIMIT 1`, and
/// the partner's own mode decides whether that query is worth running at all.
///
/// Rows arrive in `due_at` order and the first complete one wins, so a partner
/// with three overdue bills is suspended *for the oldest*, and marking the
/// newest paid does not lift a suspension that is still owed. [`status_for`]
/// therefore re-evaluates the whole set after every payment: resuming is not
/// "clear the flag", it is "ask again".
pub fn status_for(
    billing_mode: &str,
    now: OffsetDateTime,
    overdue: &[OverdueRow],
) -> ServiceStatus {
    // Reconciliation owes nothing, so nothing here applies. Checked first, and
    // with no numeric comparison, so no configuration of a reconciliation
    // partner can produce a deadline.
    if billing_mode != "invoice" {
        return ServiceStatus::Active;
    }

    let cutoff = now;
    overdue
        .iter()
        // Two kinds of statement the product cannot defend, and neither may
        // suspend:
        //
        // * An incomplete one did not manage to account for its day, so its
        //   total is a number the product knows is short. Skipped, not charged
        //   less.
        // * A zero-amount one has nothing to enforce. A day with no billable
        //   usage is still stated — the record is the point of the statement —
        //   and a day of usage entirely at a configured price of zero states at
        //   zero as well. Both are ordinary, and refusing a paying partner's
        //   traffic over `$0.000000` would be the worst possible reading of
        //   "enforce the payment obligation".
        .filter(|row| row.incomplete_usage_count == 0 && row.total_amount_micro_usd > 0)
        .find_map(|row| {
            let due_at = timefmt::parse_ts(&row.due_at)?;
            if due_at > cutoff {
                return None;
            }
            Some(ServiceStatus::Suspended {
                reason: SuspensionReason::InvoiceOverdue(OverdueStatement {
                    id: row.id,
                    billing_date: row.billing_date.clone(),
                    due_at,
                    amount: MicroUsd::from_i64(row.total_amount_micro_usd),
                }),
            })
        })
        .unwrap_or(ServiceStatus::Active)
}

#[cfg(test)]
mod tests {
    use super::*;
    use time::macros::datetime;

    fn now() -> OffsetDateTime {
        datetime!(2026-09-28 12:00 UTC)
    }

    fn row(id: i64, day: &str, due_at: &str, amount: i64, incomplete: i64) -> OverdueRow {
        OverdueRow {
            billing_mode: "invoice".to_string(),
            id,
            billing_date: day.to_string(),
            due_at: due_at.to_string(),
            total_amount_micro_usd: amount,
            incomplete_usage_count: incomplete,
        }
    }

    /// Overdue: due 2026-09-28T00:00, which is before `now()`.
    fn overdue(id: i64, amount: i64) -> OverdueRow {
        row(
            id,
            "2026-09-27",
            "2026-09-28T00:00:00.000000000Z",
            amount,
            0,
        )
    }

    /// Not yet due: due in the future relative to `now()`.
    fn future(id: i64, amount: i64) -> OverdueRow {
        row(
            id,
            "2026-09-28",
            "2026-09-29T00:00:00.000000000Z",
            amount,
            0,
        )
    }

    #[test]
    fn test_an_invoice_partner_with_nothing_outstanding_is_active() {
        assert_eq!(status_for("invoice", now(), &[]), ServiceStatus::Active);
        // A statement that is not yet due is not a reason to stop serving.
        assert_eq!(
            status_for("invoice", now(), &[future(1, 500_000)]),
            ServiceStatus::Active
        );
    }

    #[test]
    fn test_a_complete_invoice_past_its_due_date_suspends() {
        let status = status_for("invoice", now(), &[overdue(7, 1_250_000)]);
        let Some(reason) = status.suspension_reason() else {
            panic!("an overdue complete invoice must suspend, got {status:?}");
        };
        let SuspensionReason::InvoiceOverdue(statement) = reason;
        assert_eq!(statement.id, 7);
        assert_eq!(statement.billing_date, "2026-09-27");
        assert_eq!(statement.amount, MicroUsd::from_i64(1_250_000));
        assert_eq!(statement.due_at, datetime!(2026-09-28 00:00 UTC));
        assert_eq!(
            status.suspension_message(),
            Some("Service is suspended because an invoice is overdue")
        );
    }

    #[test]
    fn test_the_due_instant_itself_suspends() {
        // `<=`, not `<`: at the exact deadline the money is late. A boundary
        // that lets one more request through is a boundary nobody would have
        // written deliberately.
        let exact = row(1, "2026-09-27", "2026-09-28T12:00:00.000000000Z", 1, 0);
        assert!(status_for("invoice", now(), std::slice::from_ref(&exact)).is_suspended());

        // One nanosecond before the deadline, with the clock reading that same
        // instant: not yet late. The two sides of the boundary are both pinned,
        // because `<=` and `<` differ only here and nowhere else.
        let just_before = datetime!(2026-09-28 11:59:59.999999999 UTC);
        assert!(status_for("invoice", just_before, std::slice::from_ref(&exact)).is_active());
        let just_after = datetime!(2026-09-28 12:00:00.000000001 UTC);
        assert!(status_for("invoice", just_after, &[exact]).is_suspended());
    }

    #[test]
    fn test_an_incomplete_statement_never_suspends() {
        // The product knows it did not account for every request in that day, so
        // the bill it produced is not one it can enforce.
        let incomplete = row(
            1,
            "2026-09-27",
            "2026-09-28T00:00:00.000000000Z",
            900_000,
            3,
        );
        assert_eq!(
            status_for("invoice", now(), &[incomplete]),
            ServiceStatus::Active
        );
    }

    #[test]
    fn test_an_incomplete_statement_does_not_mask_a_complete_one() {
        // Skipping the incomplete row must fall through to the next one, not
        // return "active" on the first row it cannot use.
        let incomplete = row(9, "2026-09-26", "2026-09-27T00:00:00.000000000Z", 10, 1);
        assert!(status_for("invoice", now(), &[incomplete, overdue(8, 20)]).is_suspended());
    }

    #[test]
    fn test_the_oldest_overdue_statement_is_the_one_named() {
        // Marking the newest bill paid must not look like paying the oldest, so
        // the reason has to be the oldest one.
        let status = status_for(
            "invoice",
            now(),
            &[overdue(5, 1), overdue(3, 2), overdue(9, 3)],
        );
        let Some(reason) = status.suspension_reason() else {
            panic!("must suspend");
        };
        let SuspensionReason::InvoiceOverdue(statement) = reason;
        assert_eq!(statement.billing_date, "2026-09-27");
    }

    #[test]
    fn test_a_reconciliation_partner_is_never_suspended_however_old_the_statement() {
        // No mode of configuration reaches this: even a zero-amount statement
        // due at the epoch does not suspend. The check is first and there is no
        // fallback path.
        let ancient = row(1, "2026-01-01", "2026-01-02T00:00:00.000000000Z", 0, 0);
        assert_eq!(
            status_for("reconciliation", now(), &[ancient]),
            ServiceStatus::Active
        );
        assert_eq!(
            status_for("reconciliation", now(), &[overdue(1, 1_000_000)]),
            ServiceStatus::Active
        );
    }

    #[test]
    fn test_an_unknown_mode_is_treated_as_reconciliation_and_never_suspends() {
        // Defensive. The column has a CHECK, so this cannot reach here from the
        // database; the branch is so that a value that somehow got past it
        // cannot be the thing that stops a partner being served.
        let weird = row(1, "2026-09-27", "2026-01-02T00:00:00.000000000Z", 0, 0);
        assert_eq!(status_for("PayPal", now(), &[weird]), ServiceStatus::Active);
    }

    #[test]
    fn test_paying_one_of_several_overdue_bills_does_not_resume_the_partner() {
        // The resume question, asked the way the product asks it: re-evaluate
        // rather than clear a flag. One bill paid, one still outstanding.
        let still_overdue = overdue(4, 750_000);
        assert!(status_for("invoice", now(), &[still_overdue]).is_suspended());
    }

    #[test]
    fn test_a_row_with_an_unreadable_due_date_is_not_treated_as_overdue() {
        // A timestamp this function cannot read is not evidence of a missed
        // deadline. Refusing to suspend on unparseable data is the direction
        // that cannot silently cut a paying partner off, and the row is
        // visible in the admin list regardless.
        let unreadable = OverdueRow {
            billing_mode: "invoice".to_string(),
            id: 1,
            billing_date: "2026-09-27".to_string(),
            due_at: "not a timestamp".to_string(),
            total_amount_micro_usd: 500_000,
            incomplete_usage_count: 0,
        };
        assert_eq!(
            status_for("invoice", now(), &[unreadable]),
            ServiceStatus::Active
        );
    }

    #[test]
    fn test_status_renders_the_bill_it_is_about() {
        let status = status_for("invoice", now(), &[overdue(7, 1_250_000)]);
        let rendered = status.to_string();
        assert!(rendered.contains("statement 7"), "{rendered}");
        assert!(rendered.contains("2026-09-27"), "{rendered}");
        assert!(rendered.contains("$1.250000"), "{rendered}");
        assert_eq!(ServiceStatus::Active.to_string(), "active");
    }

    #[test]
    fn test_an_active_status_has_no_suspension_message_to_leak() {
        // The message is the body of a 403. A caller that formats it for an
        // active partner has made a mistake, and this is where that mistake is
        // visible.
        assert_eq!(ServiceStatus::Active.suspension_message(), None);
    }
}
