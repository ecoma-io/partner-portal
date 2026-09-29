//! Consumer context and identity
//!
//! A request's identity is what authenticated it, and nothing else. For a
//! partner API key that is a [`PartnerRuntimeConfig`] out of the in-memory
//! snapshot — the same object that decides the model allow-list, the price of
//! the model being called and whether the partner is currently suspended. For
//! the manager password there is no partner at all, and the context says so
//! rather than inventing one.

use std::sync::Arc;

use serde::{Deserialize, Serialize};

use crate::billing::partner::PartnerRuntimeConfig;
use crate::billing::pricing::PricingSnapshot;
use crate::billing::status::ServiceStatus;

/// What an authenticated credential is allowed to see.
///
/// A key value views exactly one consumer's usage. The manager password — when
/// configured — views every consumer (ADR 0013). This is serialized into
/// `/api/me` so the dashboard can switch between the two presentations, and it
/// is carried on every `ConsumerContext` so a handler can decide its query
/// scope.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq, Default)]
pub enum ManagerRole {
    /// A key-derived context: scoped to exactly [`ConsumerIdentity::consumer_id`].
    #[default]
    Consumer,
    /// A manager-password context: every consumer, narrowable per request.
    Manager,
}

/// Consumer identity derived from an authenticated credential.
///
/// Identity is server-side derived from the key store's snapshot, never from a
/// client-supplied value (ADR 0008). A manager context has an empty
/// `consumer_id`: it is not backed by any single consumer, and `scope` is the
/// authoritative description of what it may see.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerIdentity {
    /// Unique consumer identifier (server-side derived, never from client).
    ///
    /// Empty for a manager context: the manager is not a consumer, and any
    /// consumer_id the manager "is" would be fabricated.
    pub consumer_id: String,

    /// Human-readable key name.
    ///
    /// `"manager"` for the manager credential, which is the label the
    /// dashboard header shows.
    pub key_name: String,

    /// How far this credential may scope its queries.
    #[serde(default)]
    pub scope: ManagerRole,

    /// Models this credential may call (ADR 0012 as amended by ADR 0015).
    ///
    /// Materialized here from the partner's configured models so the
    /// serialized identity is self-describing, and read through
    /// [`ConsumerContext::allowed_models`], which takes it from the partner
    /// config rather than from this copy. Strict: an empty list allows no
    /// model. Empty for a manager context — a manager is a viewer, never a
    /// metered caller, so it has no model capability of its own.
    #[serde(default)]
    pub allowed_models: Vec<String>,
}

/// Request-scoped consumer context
#[derive(Debug, Clone)]
pub struct ConsumerContext {
    pub identity: ConsumerIdentity,

    /// The partner this credential belongs to, or `None` for a manager.
    ///
    /// Held as the snapshot's `Arc` rather than a copy: the pricing map and the
    /// service status come from the object the refresh published, so a request
    /// cannot be holding a configuration that was never published.
    partner: Option<Arc<PartnerRuntimeConfig>>,
}

impl ConsumerContext {
    /// A key-derived context: exactly one consumer, and its partner config.
    ///
    /// The model list on the identity is a copy of the partner's, taken once
    /// here. It cannot drift, because a [`PartnerRuntimeConfig`] is immutable:
    /// an admin edit builds a new one and publishes it whole.
    pub fn new(partner: Arc<PartnerRuntimeConfig>) -> Self {
        let identity = ConsumerIdentity {
            consumer_id: partner.consumer_id.clone(),
            key_name: partner.key_name.clone(),
            scope: ManagerRole::Consumer,
            allowed_models: partner.allowed_models(),
        };
        Self {
            identity,
            partner: Some(partner),
        }
    }

    /// A manager context: every consumer, narrowable per request by the
    /// dashboard's `consumers=` parameter — a view filter, never a grant.
    pub fn manager() -> Self {
        Self {
            identity: ConsumerIdentity {
                consumer_id: String::new(),
                key_name: "manager".to_string(),
                scope: ManagerRole::Manager,
                allowed_models: Vec::new(),
            },
            partner: None,
        }
    }

    /// Get the consumer ID for ledger records.
    ///
    /// Only call this after narrowing to the single-consumer case: a manager
    /// context has no meaningfully "own" consumer and returns `""`.
    pub fn consumer_id(&self) -> &str {
        &self.identity.consumer_id
    }

    /// Whether this is a manager password session.
    pub fn is_manager(&self) -> bool {
        self.identity.scope == ManagerRole::Manager
    }

    /// Models this credential may call. A manager context is empty.
    ///
    /// Taken from the partner config when there is one, which is the same map
    /// [`ConsumerContext::pricing_for`] reads — so a model that has a price has
    /// a permission, and neither can be observed without the other.
    pub fn allowed_models(&self) -> Vec<String> {
        match &self.partner {
            Some(partner) => partner.allowed_models(),
            None => Vec::new(),
        }
    }

    /// Whether this credential may call `model`.
    pub fn allows_model(&self, model: &str) -> bool {
        self.partner
            .as_ref()
            .is_some_and(|partner| partner.allows(model))
    }

    /// What `model` costs for this credential, from the same map the allow-list
    /// comes from.
    pub fn pricing_for(&self, model: &str) -> Option<PricingSnapshot> {
        self.partner
            .as_ref()
            .and_then(|partner| partner.pricing_for(model))
    }

    /// Whether this credential may be served at all.
    ///
    /// A manager is always active: the password is not backed by a partner, has
    /// no invoice and is not a metered caller, so there is nothing to suspend.
    pub fn service_status(&self) -> ServiceStatus {
        match &self.partner {
            Some(partner) => partner.service_status.clone(),
            None => ServiceStatus::Active,
        }
    }

    /// The partner behind this credential, if any. `None` for a manager.
    pub fn partner(&self) -> Option<&PartnerRuntimeConfig> {
        self.partner.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::partner::BillingMode;
    use crate::billing::pricing::PricePerMillion;
    use crate::billing::status::{OverdueRow, SuspensionReason, status_for};
    use std::collections::BTreeMap;
    use time::macros::datetime;

    fn partner_with(models: &[(&str, i64)]) -> Arc<PartnerRuntimeConfig> {
        Arc::new(PartnerRuntimeConfig::new(
            "acme".to_string(),
            "acme-key".to_string(),
            BillingMode::Invoice,
            "billing@acme.test".to_string(),
            ServiceStatus::Active,
            models
                .iter()
                .map(|(name, price)| {
                    (
                        name.to_string(),
                        PricingSnapshot::new(
                            PricePerMillion::new(*price),
                            PricePerMillion::new(*price),
                            PricePerMillion::new(*price),
                        ),
                    )
                })
                .collect::<BTreeMap<_, _>>(),
        ))
    }

    #[test]
    fn test_consumer_context_is_pinned_to_one_consumer() {
        let c = ConsumerContext::new(partner_with(&[("gpt-4o", 95_000)]));
        assert!(!c.is_manager(), "a key context is a consumer context");
        assert_eq!(c.consumer_id(), "acme");
        assert_eq!(c.identity.key_name, "acme-key");
    }

    #[test]
    fn test_the_model_list_reads_from_the_partner_not_from_a_copy() {
        let c = ConsumerContext::new(partner_with(&[("gpt-4o", 95_000), ("gpt-5", 1)]));
        // The serialized identity carries the list…
        assert_eq!(c.identity.allowed_models, vec!["gpt-4o", "gpt-5"]);
        // …and the accessor reads the authoritative one.
        assert_eq!(c.allowed_models(), vec!["gpt-4o", "gpt-5"]);
        assert!(c.allows_model("gpt-4o"));
        assert!(!c.allows_model("gpt-4o-mini"));
    }

    #[test]
    fn test_permission_and_price_come_from_one_lookup() {
        let c = ConsumerContext::new(partner_with(&[("gpt-4o", 95_000)]));
        assert!(c.allows_model("gpt-4o"));
        let prices = c
            .pricing_for("gpt-4o")
            .expect("a configured model has a price");
        assert_eq!(prices.input.as_i64(), 95_000);

        // A model with no permission has no price, and vice versa. A request
        // for one is refused at the gate long before this is asked.
        assert!(!c.allows_model("gpt-5"));
        assert_eq!(c.pricing_for("gpt-5"), None);
    }

    /// A manager context is backed by no consumer of its own and carries no
    /// per-consumer grant: what it may view is decided by its role alone.
    #[test]
    fn test_manager_context_has_no_consumer_of_its_own() {
        let m = ConsumerContext::manager();
        assert!(m.is_manager());
        assert_eq!(m.consumer_id(), "", "a manager is no single consumer");
        assert!(m.allowed_models().is_empty(), "a manager is a viewer");
        assert!(!m.allows_model("gpt-4o"));
        assert_eq!(m.pricing_for("gpt-4o"), None);
        assert!(m.partner().is_none());
        assert!(
            m.service_status().is_active(),
            "the manager password is not a metered caller and cannot be suspended"
        );
    }

    #[test]
    fn test_a_suspended_partner_carries_that_status_into_the_context() {
        let overdue = OverdueRow {
            billing_mode: "invoice".into(),
            id: 3,
            billing_date: "2026-09-27".into(),
            due_at: "2026-09-28T00:00:00.000000000Z".into(),
            total_amount_micro_usd: 1_000,
            incomplete_usage_count: 0,
        };
        let status = status_for("invoice", datetime!(2026-09-28 12:00 UTC), &[overdue]);
        let partner = {
            let base = partner_with(&[("gpt-4o", 95_000)]);
            Arc::new(PartnerRuntimeConfig::new(
                base.consumer_id.clone(),
                base.key_name.clone(),
                base.billing_mode,
                base.billing_email.clone(),
                status,
                base.models().clone(),
            ))
        };
        let c = ConsumerContext::new(partner);
        let status = c.service_status();
        assert!(status.is_suspended());
        assert_eq!(
            status.suspension_message(),
            Some("Service is suspended because an invoice is overdue")
        );
        // And the identity is still intact: suspension is not an authentication
        // failure, and it does not change who the caller is.
        assert_eq!(c.consumer_id(), "acme");
        let Some(SuspensionReason::InvoiceOverdue(statement)) = status.suspension_reason() else {
            panic!("the reason must name the bill");
        };
        assert_eq!(statement.id, 3);
    }

    #[test]
    fn test_roles_compare_and_serialize() {
        assert_eq!(ManagerRole::Consumer, ManagerRole::Consumer);
        assert_ne!(ManagerRole::Consumer, ManagerRole::Manager);
        let rendered = serde_json::to_string(&ManagerRole::Manager).unwrap();
        assert_eq!(rendered, "\"Manager\"");
    }
}
