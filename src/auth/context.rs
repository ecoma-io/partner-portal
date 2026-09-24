//! Consumer context and identity

use serde::{Deserialize, Serialize};

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
/// Identity is server-side derived from the config snapshot, never from a
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

    /// Models this key may call (ADR 0012). Strict: an empty list allows no
    /// model. Empty for a manager context — a manager is a viewer, never a
    /// metered caller, so it has no model capability of its own.
    #[serde(default)]
    pub allowed_models: Vec<String>,
}

/// Request-scoped consumer context
#[derive(Debug, Clone)]
pub struct ConsumerContext {
    pub identity: ConsumerIdentity,
}

impl ConsumerContext {
    /// A key-derived context: exactly one consumer.
    pub fn new(consumer_id: String, key_name: String, allowed_models: Vec<String>) -> Self {
        Self {
            identity: ConsumerIdentity {
                consumer_id,
                key_name,
                scope: ManagerRole::Consumer,
                allowed_models,
            },
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
    pub fn allowed_models(&self) -> &[String] {
        &self.identity.allowed_models
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx(consumer_id: &str) -> ConsumerContext {
        ConsumerContext::new(consumer_id.to_string(), "some-name".to_string(), Vec::new())
    }

    #[test]
    fn test_consumer_context_is_pinned_to_one_consumer() {
        let c = ctx("acme");
        assert!(!c.is_manager(), "a key context is a consumer context");
        assert_eq!(c.consumer_id(), "acme");
    }

    /// A manager context is backed by no consumer of its own and carries no
    /// per-consumer grant: what it may view is decided by its role alone.
    #[test]
    fn test_manager_context_has_no_consumer_of_its_own() {
        let m = ConsumerContext::manager();
        assert!(m.is_manager());
        assert_eq!(m.consumer_id(), "", "a manager is no single consumer");
        assert!(m.allowed_models().is_empty(), "a manager is a viewer");
    }

    #[test]
    fn test_roles_compare_and_serialize() {
        assert_eq!(ManagerRole::Consumer, ManagerRole::Consumer);
        assert_ne!(ManagerRole::Consumer, ManagerRole::Manager);
        let rendered = serde_json::to_string(&ManagerRole::Manager).unwrap();
        assert_eq!(rendered, "\"Manager\"");
    }
}
