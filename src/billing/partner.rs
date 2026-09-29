//! What a partner *is*, and the shape the request path reads it in.
//!
//! # Two types, one partner
//!
//! [`Partner`] is the durable row: the commercial facts an operator edits and
//! the database stores. [`PartnerRuntimeConfig`] is what the request path
//! carries — the same facts flattened into the form a request can use without
//! touching SQLite: which models this partner may call, what each costs, and
//! whether the partner is currently allowed to call at all.
//!
//! They are separate because they answer different questions. The row answers
//! "what is configured"; the runtime config answers "what do I do with *this*
//! request", and it has to answer in constant time with no I/O, from a snapshot
//! that was built once and is replaced wholesale.
//!
//! # The model list is authoritative in one place
//!
//! [`PartnerRuntimeConfig::models`] is *the* answer to both "may this partner
//! call this model" and "what does this model cost". Historically those were
//! two questions with two answers — a JSON allow-list on the key row, and
//! nothing at all for the price — and the failure mode of two sources of truth
//! is a model a partner can call at a price nobody configured. There is one
//! map here, it is built from `partner_models` and nowhere else, and a model
//! absent from it is a model this partner cannot call.
//!
//! # The price travels with the acceptance
//!
//! [`PartnerRuntimeConfig::pricing_for`] is read at *accept* time and written
//! onto the usage row (see [`crate::ledger`]). It is not read at statement
//! time, because by then the operator may have changed the price and the
//! request happened under the old one. A price change mid-day therefore
//! produces two price groups in one statement, which is the correct and less
//! convenient answer.

use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use time::OffsetDateTime;

use crate::billing::pricing::PricingSnapshot;
use crate::billing::status::ServiceStatus;

/// How a partner settles what they owe.
///
/// Two modes, and the difference is a *payment obligation*, not a price. An
/// `invoice` partner is billed and suspended when they do not pay. A
/// `reconciliation` partner is statemented so the numbers exist in one place at
/// settlement time and owes nothing on any deadline.
///
/// Deliberately not modelled as "an invoice with no due date" or "a price of
/// zero": both would leave a real amount on a real statement with no
/// obligation behind it, and every later question — what is outstanding, what is
/// overdue, what may be suspended — would have to remember that some of those
/// rows do not count. Here the mode is the first thing checked and the rest of
/// the logic does not run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum BillingMode {
    Invoice,
    Reconciliation,
}

impl BillingMode {
    /// The value stored in `partners.billing_mode` and every `CHECK` on it.
    pub const fn as_str(self) -> &'static str {
        match self {
            BillingMode::Invoice => "invoice",
            BillingMode::Reconciliation => "reconciliation",
        }
    }

    /// Read the stored value. `None` for anything else — the column has a
    /// `CHECK`, so an unrecognised mode means the row did not come from this
    /// product, and guessing a mode for it would be guessing whether a partner
    /// owes money.
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "invoice" => Some(BillingMode::Invoice),
            "reconciliation" => Some(BillingMode::Reconciliation),
            _ => None,
        }
    }

    /// Whether this mode carries a payment obligation.
    pub fn owes_payment(self) -> bool {
        matches!(self, BillingMode::Invoice)
    }
}

impl fmt::Display for BillingMode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// The durable commercial facts about a partner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Partner {
    pub consumer_id: String,
    pub name: String,
    /// Where a statement is sent. Empty means "no address on file" and is a
    /// reason not to send, never a reason to fail a statement: the statement is
    /// the record, the email is a courtesy on top of it.
    pub billing_email: String,
    pub billing_mode: BillingMode,
    /// How long after the end of a billing period payment is due, in minutes.
    /// Applies to [`BillingMode::Invoice`] only; a reconciliation statement has
    /// no deadline to compute from it.
    pub payment_terms_minutes: i64,
    pub created_at: String,
    pub updated_at: String,
}

impl Partner {
    /// The instant payment for a period ending at `period_end` falls due.
    ///
    /// `period_end` is the *exclusive* end of the billing day, so terms of 720
    /// minutes on the 27th mean the money is late from 12:00 on the 28th —
    /// counted from the contract's period boundary, not from when the worker
    /// happened to run. A worker restart at 03:00 must not move a deadline.
    ///
    /// `None` for a reconciliation partner: there is no deadline, and returning
    /// a plausible-looking one would be inventing a payment obligation.
    pub fn due_at_for(&self, period_end: OffsetDateTime) -> Option<OffsetDateTime> {
        if !self.billing_mode.owes_payment() {
            return None;
        }
        period_end.checked_add(time::Duration::minutes(self.payment_terms_minutes))
    }
}

/// One model's prices, as configured.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ModelPrice {
    pub model: String,
    pub prices: PricingSnapshot,
}

/// A partner as the request path sees them.
///
/// Immutable, built from the database by the snapshot refresh, and replaced
/// wholesale rather than mutated — a request sees one consistent partner or the
/// previous one, never a half-applied admin edit.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartnerRuntimeConfig {
    pub consumer_id: String,
    /// The name of the key that authenticated this request. Presentation only;
    /// it grants nothing.
    pub key_name: String,
    pub billing_mode: BillingMode,
    /// Whether statements for this partner are emailed. Derived from the mode,
    /// because a reconciliation partner has no invoice to send.
    pub billing_email: String,
    pub service_status: ServiceStatus,
    /// Model name to prices. Both the allow-list and the price list, by
    /// construction — see the module docs.
    models: BTreeMap<String, PricingSnapshot>,
}

impl PartnerRuntimeConfig {
    pub fn new(
        consumer_id: String,
        key_name: String,
        billing_mode: BillingMode,
        billing_email: String,
        service_status: ServiceStatus,
        models: BTreeMap<String, PricingSnapshot>,
    ) -> Self {
        Self {
            consumer_id,
            key_name,
            billing_mode,
            billing_email,
            service_status,
            models,
        }
    }

    /// A partner with no models configured. They can call nothing, which is the
    /// only safe reading of "not configured": the alternative is a partner who
    /// can call everything.
    pub fn empty(
        consumer_id: String,
        key_name: String,
        billing_mode: BillingMode,
        service_status: ServiceStatus,
    ) -> Self {
        Self::new(
            consumer_id,
            key_name,
            billing_mode,
            String::new(),
            service_status,
            BTreeMap::new(),
        )
    }

    /// Whether this partner may call `model`.
    pub fn allows(&self, model: &str) -> bool {
        self.models.contains_key(model)
    }

    /// What `model` costs, or `None` if this partner cannot call it.
    ///
    /// One lookup answers both questions, so there is no path where the model
    /// gate and the pricing lookup can disagree about whether a model is
    /// permitted.
    pub fn pricing_for(&self, model: &str) -> Option<PricingSnapshot> {
        self.models.get(model).copied()
    }

    /// The configured models, in a stable order for output.
    ///
    /// Sorted rather than in insertion order so `/v1/models` and the admin API
    /// return the same list for the same configuration, which is what makes a
    /// cached response and a diff readable.
    pub fn allowed_models(&self) -> Vec<String> {
        self.models.keys().cloned().collect()
    }

    pub fn models(&self) -> &BTreeMap<String, PricingSnapshot> {
        &self.models
    }

    pub fn is_suspended(&self) -> bool {
        self.service_status.is_suspended()
    }

    /// Whether a statement for this partner is emailed to a human.
    ///
    /// Both conditions matter: a reconciliation statement has no payment
    /// obligation to communicate, and an empty address cannot receive one.
    pub fn emails_statements(&self) -> bool {
        self.billing_mode.owes_payment() && !self.billing_email.is_empty()
    }
}

/// The in-memory partner snapshot the request path reads.
///
/// Keyed by the key hash that authenticates, exactly like the snapshot it
/// replaces, so resolving a request is still one hash and one map lookup. The
/// value is an [`Arc`] because the same partner configuration is not rebuilt
/// per request: cloning the handle is a refcount bump, and the map inside is
/// shared.
#[derive(Debug, Default)]
pub struct PartnerSnapshot {
    by_hash: std::collections::HashMap<String, Arc<PartnerRuntimeConfig>>,
}

impl PartnerSnapshot {
    pub fn from_rows(rows: Vec<(String, Arc<PartnerRuntimeConfig>)>) -> Self {
        Self {
            by_hash: rows.into_iter().collect(),
        }
    }

    /// Number of keys that will authenticate.
    pub fn len(&self) -> usize {
        self.by_hash.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_hash.is_empty()
    }

    /// Resolve a key hash to the partner it belongs to.
    pub fn get(&self, hash: &str) -> Option<&Arc<PartnerRuntimeConfig>> {
        self.by_hash.get(hash)
    }

    /// Every authenticating key and the partner it resolves to, in no
    /// particular order.
    ///
    /// Used to compare one snapshot against the next when deciding whether a
    /// partner's service status *changed* — see
    /// [`crate::apikeys::ApiKeyStore::refresh`]. Order is deliberately not
    /// promised: the only caller builds a map keyed by hash, because two
    /// snapshots taken a second apart are only comparable by identity.
    pub fn entries(&self) -> impl Iterator<Item = (&str, &Arc<PartnerRuntimeConfig>)> {
        self.by_hash.iter().map(|(hash, cfg)| (hash.as_str(), cfg))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::pricing::PricePerMillion;
    use crate::billing::status::{OverdueRow, SuspensionReason, status_for};
    use time::macros::datetime;

    fn snapshot(input: i64, cached: i64, output: i64) -> PricingSnapshot {
        PricingSnapshot::new(
            PricePerMillion::new(input),
            PricePerMillion::new(cached),
            PricePerMillion::new(output),
        )
    }

    fn partner(mode: BillingMode, models: &[(&str, PricingSnapshot)]) -> PartnerRuntimeConfig {
        PartnerRuntimeConfig::new(
            "acme".to_string(),
            "acme-key".to_string(),
            mode,
            "billing@acme.test".to_string(),
            ServiceStatus::Active,
            models
                .iter()
                .map(|(name, prices)| (name.to_string(), *prices))
                .collect(),
        )
    }

    #[test]
    fn test_the_two_modes_round_trip_through_the_stored_value() {
        assert_eq!(BillingMode::parse("invoice"), Some(BillingMode::Invoice));
        assert_eq!(
            BillingMode::parse("reconciliation"),
            Some(BillingMode::Reconciliation)
        );
        assert_eq!(BillingMode::Invoice.as_str(), "invoice");
        assert_eq!(BillingMode::Reconciliation.as_str(), "reconciliation");
        assert_eq!(BillingMode::Invoice.to_string(), "invoice");
    }

    #[test]
    fn test_an_unrecognised_mode_is_refused_rather_than_assumed() {
        // The column has a CHECK, so this is defence against a row that did not
        // come from here. Defaulting to `invoice` would start suspending people
        // over a value nobody configured; defaulting to `reconciliation` would
        // stop collecting. Neither is a guess worth making.
        assert_eq!(BillingMode::parse(""), None);
        assert_eq!(BillingMode::parse("Invoice"), None);
        assert_eq!(BillingMode::parse("unlimited"), None);
        assert_eq!(BillingMode::parse("free"), None);
    }

    #[test]
    fn test_only_invoice_carries_a_payment_obligation() {
        assert!(BillingMode::Invoice.owes_payment());
        assert!(!BillingMode::Reconciliation.owes_payment());
    }

    #[test]
    fn test_payment_terms_are_counted_from_the_end_of_the_period() {
        // 720 minutes is twelve hours, so a day ending at midnight on the 28th
        // is due at noon on the 28th — and not at noon on the 29th.
        let acme = Partner {
            consumer_id: "acme".into(),
            name: "Acme".into(),
            billing_email: "billing@acme.test".into(),
            billing_mode: BillingMode::Invoice,
            payment_terms_minutes: 720,
            created_at: String::new(),
            updated_at: String::new(),
        };
        let period_end = datetime!(2026-09-28 00:00 UTC);
        assert_eq!(
            acme.due_at_for(period_end),
            Some(datetime!(2026-09-28 12:00 UTC))
        );
    }

    #[test]
    fn test_zero_payment_terms_due_immediately_and_are_not_refused() {
        // "Pay when the day closes" is a legitimate contract, and a zero here is
        // a real deadline rather than a missing value.
        let acme = Partner {
            consumer_id: "acme".into(),
            name: "Acme".into(),
            billing_email: String::new(),
            billing_mode: BillingMode::Invoice,
            payment_terms_minutes: 0,
            created_at: String::new(),
            updated_at: String::new(),
        };
        let period_end = datetime!(2026-09-28 00:00 UTC);
        assert_eq!(acme.due_at_for(period_end), Some(period_end));
    }

    #[test]
    fn test_a_reconciliation_partner_has_no_due_date_at_all() {
        // Not a far-future one, not a zero one, and not the period end: no
        // deadline exists, so nothing is returned for anyone to store.
        let acme = Partner {
            consumer_id: "acme".into(),
            name: "Acme".into(),
            billing_email: String::new(),
            billing_mode: BillingMode::Reconciliation,
            payment_terms_minutes: 720,
            created_at: String::new(),
            updated_at: String::new(),
        };
        assert_eq!(acme.due_at_for(datetime!(2026-09-28 00:00 UTC)), None);
    }

    #[test]
    fn test_one_map_answers_both_permission_and_price() {
        // The defect this design exists to prevent: a model the partner may call
        // at a price nobody configured, or a price with no permission behind it.
        let acme = partner(
            BillingMode::Invoice,
            &[("gpt-4o", snapshot(95_000, 10_000, 475_000))],
        );
        assert!(acme.allows("gpt-4o"));
        assert_eq!(
            acme.pricing_for("gpt-4o"),
            Some(snapshot(95_000, 10_000, 475_000))
        );
        // A model with no entry: no permission *and* no price, together.
        assert!(!acme.allows("gpt-5"));
        assert_eq!(acme.pricing_for("gpt-5"), None);
    }

    #[test]
    fn test_a_partner_with_no_models_configured_can_call_nothing() {
        let empty = PartnerRuntimeConfig::empty(
            "acme".to_string(),
            "acme-key".to_string(),
            BillingMode::Invoice,
            ServiceStatus::Active,
        );
        assert!(!empty.allows("gpt-4o"));
        assert!(empty.allowed_models().is_empty());
        assert_eq!(empty.pricing_for("gpt-4o"), None);
    }

    #[test]
    fn test_the_model_list_is_sorted_so_two_reads_of_one_config_agree() {
        let acme = partner(
            BillingMode::Invoice,
            &[
                ("gpt-5", snapshot(1, 1, 1)),
                ("claude-4", snapshot(2, 2, 2)),
                ("gpt-4o", snapshot(3, 3, 3)),
            ],
        );
        assert_eq!(
            acme.allowed_models(),
            vec!["claude-4", "gpt-4o", "gpt-5"]
                .into_iter()
                .map(String::from)
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_a_suspended_partner_is_suspended_for_its_own_overdue_bill() {
        // The two halves joined: `status_for` produces the status, the runtime
        // config carries it, and the request path asks the runtime config.
        let overdue = OverdueRow {
            billing_mode: "invoice".into(),
            id: 12,
            billing_date: "2026-09-27".into(),
            due_at: "2026-09-28T00:00:00.000000000Z".into(),
            total_amount_micro_usd: 4_200,
            incomplete_usage_count: 0,
        };
        let status = status_for(
            BillingMode::Invoice.as_str(),
            datetime!(2026-09-28 12:00 UTC),
            &[overdue],
        );
        let mut acme = partner(
            BillingMode::Invoice,
            &[("gpt-4o", snapshot(95_000, 10_000, 475_000))],
        );
        acme.service_status = status;
        assert!(acme.is_suspended());
        let Some(SuspensionReason::InvoiceOverdue(statement)) =
            acme.service_status.suspension_reason()
        else {
            panic!("the reason must name the bill");
        };
        assert_eq!(statement.id, 12);
    }

    #[test]
    fn test_a_reconciliation_partner_is_active_even_with_a_statement_due_in_the_past() {
        // End to end through the two modules: the mode makes the deadline
        // meaningless, so no amount of statement history suspends.
        let ancient = OverdueRow {
            billing_mode: "reconciliation".into(),
            id: 1,
            billing_date: "2026-01-01".into(),
            due_at: "2026-01-02T00:00:00.000000000Z".into(),
            total_amount_micro_usd: 9_999_999,
            incomplete_usage_count: 0,
        };
        let status = status_for(
            BillingMode::Reconciliation.as_str(),
            datetime!(2026-09-28 12:00 UTC),
            &[ancient],
        );
        let mut acme = partner(BillingMode::Reconciliation, &[]);
        acme.service_status = status;
        assert!(!acme.is_suspended());
    }

    #[test]
    fn test_only_an_invoice_with_an_address_is_emailed() {
        let mut acme = partner(BillingMode::Invoice, &[]);
        assert!(
            acme.emails_statements(),
            "an invoice with an address is sent"
        );

        acme.billing_email = String::new();
        assert!(!acme.emails_statements(), "no address, nothing to send to");

        acme.billing_email = "billing@acme.test".into();
        acme.billing_mode = BillingMode::Reconciliation;
        assert!(
            !acme.emails_statements(),
            "a reconciliation statement has no payment obligation to announce"
        );
    }

    #[test]
    fn test_the_snapshot_resolves_a_hash_to_a_shared_partner_config() {
        let acme = Arc::new(partner(BillingMode::Invoice, &[]));
        let snapshot = PartnerSnapshot::from_rows(vec![("abc123".to_string(), acme.clone())]);
        assert_eq!(snapshot.len(), 1);
        let resolved = snapshot.get("abc123").expect("the key resolves");
        assert!(
            Arc::ptr_eq(resolved, &acme),
            "the handle is shared, not copied"
        );
        assert!(snapshot.get("not-a-key").is_none());
        assert!(PartnerSnapshot::default().is_empty());
    }
}
