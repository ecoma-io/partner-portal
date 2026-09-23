//! Configuration types

use hex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::HashMap;

/// Main configuration structure
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Config {
    /// Server configuration
    #[serde(default)]
    pub server: ServerConfig,

    /// Upstream OpenAI-compatible endpoint
    pub upstream: UpstreamConfig,

    /// Local API keys for authentication
    pub keys: Vec<KeyConfig>,

    /// Database configuration
    #[serde(default)]
    pub database: DatabaseConfig,
}

impl Config {
    /// Compute a hash of the configuration for change detection
    pub fn hash(&self) -> String {
        let yaml = serde_yaml::to_string(self).unwrap_or_default();
        let mut hasher = Sha256::new();
        hasher.update(yaml.as_bytes());
        hex::encode(hasher.finalize())
    }

    /// Find a key by its value
    pub fn find_key(&self, key_value: &str) -> Option<&KeyConfig> {
        self.keys.iter().find(|k| k.key == key_value)
    }
}

/// Upstream configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpstreamConfig {
    /// Base URL for the upstream OpenAI-compatible API
    pub base_url: String,

    /// API key to use for upstream requests
    pub api_key: String,

    /// Request timeout in seconds
    #[serde(default = "default_upstream_timeout")]
    pub timeout_secs: u64,

    /// Connect timeout in seconds
    #[serde(default = "default_connect_timeout")]
    pub connect_timeout_secs: u64,
}

fn default_upstream_timeout() -> u64 {
    120
}
fn default_connect_timeout() -> u64 {
    10
}

impl UpstreamConfig {
    /// Normalize base URL (remove trailing slash)
    pub fn normalized_base_url(&self) -> String {
        self.base_url.trim_end_matches('/').to_string()
    }
}

/// Local API key configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyConfig {
    /// The API key value (used for authentication)
    pub key: String,

    /// Human-readable name for the key
    pub name: String,

    /// Optional consumer ID (derived from key if not specified)
    #[serde(default)]
    pub consumer_id: Option<String>,

    /// Optional metadata
    #[serde(default)]
    pub metadata: HashMap<String, String>,
}

impl KeyConfig {
    /// Get or derive the consumer ID
    pub fn consumer_id(&self) -> &str {
        self.consumer_id.as_deref().unwrap_or(&self.name)
    }
}

/// Server configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerConfig {
    /// Listen address
    #[serde(default = "default_listen_addr")]
    pub listen: String,

    /// Enable graceful shutdown on SIGTERM
    #[serde(default = "default_true")]
    pub graceful_shutdown: bool,

    /// Maximum request body size in bytes (default: 10MB)
    #[serde(default = "default_max_body_size")]
    pub max_body_size: usize,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            listen: default_listen_addr(),
            graceful_shutdown: default_true(),
            max_body_size: default_max_body_size(),
        }
    }
}

fn default_listen_addr() -> String {
    "0.0.0.0:8080".to_string()
}
fn default_true() -> bool {
    true
}
fn default_max_body_size() -> usize {
    10 * 1024 * 1024
} // 10MB

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DatabaseConfig {
    /// Path to SQLite database file
    #[serde(default = "default_db_path")]
    pub path: String,

    /// Retention period in days
    #[serde(default = "default_retention_days")]
    pub retention_days: u32,

    /// Maximum queue size for metering writes
    #[serde(default = "default_queue_size")]
    pub queue_size: usize,

    /// Batch size for writes
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,

    /// Batch timeout in milliseconds
    #[serde(default = "default_batch_timeout_ms")]
    pub batch_timeout_ms: u64,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: default_db_path(),
            retention_days: default_retention_days(),
            queue_size: default_queue_size(),
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
        }
    }
}

fn default_db_path() -> String {
    "partner-portal.db".to_string()
}
fn default_retention_days() -> u32 {
    60
}
fn default_queue_size() -> usize {
    10_000
}
fn default_batch_size() -> usize {
    100
}
fn default_batch_timeout_ms() -> u64 {
    1000
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_hash_stability() {
        let config = Config {
            server: ServerConfig::default(),
            upstream: UpstreamConfig {
                base_url: "https://api.openai.com".to_string(),
                api_key: "test-key".to_string(),
                timeout_secs: 120,
                connect_timeout_secs: 10,
            },
            keys: vec![KeyConfig {
                key: "local-key".to_string(),
                name: "test".to_string(),
                consumer_id: None,
                metadata: HashMap::new(),
            }],
            database: DatabaseConfig::default(),
        };

        let hash1 = config.hash();
        let hash2 = config.hash();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_upstream_url_normalization() {
        let upstream = UpstreamConfig {
            base_url: "https://api.openai.com/".to_string(),
            api_key: "test".to_string(),
            timeout_secs: 120,
            connect_timeout_secs: 10,
        };
        assert_eq!(upstream.normalized_base_url(), "https://api.openai.com");
    }
}
