//! Consumer context and identity

use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

/// Consumer identity derived from authenticated API key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerIdentity {
    /// Unique consumer identifier (server-side derived, never from client)
    pub consumer_id: String,

    /// Human-readable key name
    pub key_name: String,

    /// Optional metadata from key config.
    ///
    /// Ordered, matching [`crate::config::KeyConfig::metadata`]: this value is
    /// carried straight through from the config, and reordering it here would
    /// put the config's hashing problem back.
    #[serde(default)]
    pub metadata: BTreeMap<String, String>,
}

/// Request-scoped consumer context
#[derive(Debug, Clone)]
pub struct ConsumerContext {
    pub identity: ConsumerIdentity,
}

impl ConsumerContext {
    pub fn new(consumer_id: String, key_name: String, metadata: BTreeMap<String, String>) -> Self {
        Self {
            identity: ConsumerIdentity {
                consumer_id,
                key_name,
                metadata,
            },
        }
    }

    /// Get the consumer ID for ledger records
    pub fn consumer_id(&self) -> &str {
        &self.identity.consumer_id
    }
}
