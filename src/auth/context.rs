//! Consumer context and identity

use serde::{Deserialize, Serialize};

/// Consumer identity derived from authenticated API key
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ConsumerIdentity {
    /// Unique consumer identifier (server-side derived, never from client)
    pub consumer_id: String,

    /// Human-readable key name
    pub key_name: String,

    /// Optional metadata from key config
    #[serde(default)]
    pub metadata: std::collections::HashMap<String, String>,
}

/// Request-scoped consumer context
#[derive(Debug, Clone)]
pub struct ConsumerContext {
    pub identity: ConsumerIdentity,
}

impl ConsumerContext {
    pub fn new(
        consumer_id: String,
        key_name: String,
        metadata: std::collections::HashMap<String, String>,
    ) -> Self {
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
