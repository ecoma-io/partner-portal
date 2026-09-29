//! Billing: partners, prices, and what they owe.
//!
//! # What this module is
//!
//! Daily **postpaid** usage billing. Each partner gets one statement per
//! billing day, built from the usage the provider reported and the prices that
//! were in force when each request was accepted. A partner on `invoice` terms
//! is emailed the statement, and their service is suspended while a complete
//! statement is past its due date. A partner on `reconciliation` terms is
//! statemented for the settlement record and owes nothing.
//!
//! # What this module is not
//!
//! Not a payment platform. There is no Stripe, no wallet, no subscription, no
//! coupons, no tax engine, no automatic charging, and no payment-provider
//! abstraction. Payment is a human clicking *Mark as Paid*, and that is the
//! entire requirement; an abstraction with one implementation is a seam nobody
//! is asking for yet.
//!
//! # The rules that hold everywhere in here
//!
//! **Nothing is invented.** A missing usage figure, a missing price, a
//! provider that reports more cached tokens than input tokens — each of these
//! makes a request *not billable*, and a not-billable request contributes a
//! count to the statement and no money to it. `NULL` is never `0`; "we do not
//! know what this cost" and "this was free" are different answers, and
//! confusing them is the one failure this product cannot have, because a
//! partner pays the difference.
//!
//! **The request path does no billing work.** Prices and service status are
//! resolved from an in-memory snapshot (see [`partner`]) and snapshotted onto
//! the usage row at accept time. Nothing in the request path queries the
//! ledger for a price, an invoice or a due date.
//!
//! **A statement is immutable once issued.** It carries its own token counts,
//! prices and costs, so it does not depend on `usage_records` surviving
//! retention — a bill that changed after it was sent would be a bug a
//! customer would find.
//!
//! **At most one statement per partner per day**, enforced by a unique
//! constraint rather than by a check, because a duplicate invoice is not a
//! failure the code can absorb.

pub mod api;
pub mod email;
pub mod partner;
pub mod period;
pub mod pricing;
pub mod statements;
pub mod status;
pub mod store;
pub mod worker;

pub use partner::{BillingMode, ModelPrice, Partner, PartnerRuntimeConfig, PartnerSnapshot};
pub use period::{BillingDay, BillingPeriod, BillingTimezone};
pub use pricing::{MicroUsd, PricePerMillion, PricingSnapshot};
pub use statements::Generator;
pub use status::{ServiceStatus, SuspensionReason};
pub use store::{BillingError, BillingStore, NewPartner, PartnerPatch, Statement, StatementLine};

/// The currency every amount in this module is denominated in.
///
/// Held as data rather than assumed in code so that a second currency is a
/// deliberate migration — a column, an ADR and a rate table — rather than
/// something that appears because a partner asked. ISO 4217, as stored.
pub const CURRENCY: &str = "USD";

/// Default payment terms, in minutes: twelve hours from the end of the billing
/// period.
///
/// Short enough that an unpaid invoice has a consequence within the same
/// working day, which is the point of suspending anyone. Counted from the end
/// of the period rather than from when the statement was generated, so the
/// deadline is a property of the contract and not of when this process happened
/// to run.
pub const DEFAULT_PAYMENT_TERMS_MINUTES: i64 = 720;

/// Minutes to wait before a failed email is retried.
///
/// Doubles at each attempt up to a ceiling, because an SMTP server that refused
/// once will refuse again immediately and a 30-second retry loop against a
/// broken mail relay is just a busy loop with a database write in it.
pub const EMAIL_RETRY_BASE_MINUTES: i64 = 15;

/// Ceiling on the retry backoff.
pub const EMAIL_RETRY_MAX_MINUTES: i64 = 24 * 60;

/// How long an instance holds a statement's send lease.
///
/// Long enough that a slow SMTP conversation does not have its lease stolen out
/// from under it, short enough that an instance killed mid-send does not block
/// the statement for the rest of the day.
pub const EMAIL_CLAIM_TTL_MINUTES: i64 = 15;
