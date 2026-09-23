//! Configuration loader with validation

use crate::config::Config;
use std::fs;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    #[error("YAML parse error: {0}")]
    YamlParse(#[from] serde_yaml::Error),

    #[error("No keys configured")]
    NoKeys,

    #[error("Duplicate key: {0}")]
    DuplicateKey(String),

    #[error("Invalid upstream URL: {0}")]
    InvalidUpstreamUrl(String),

    #[error("Invalid configuration: {0}")]
    Invalid(String),
}

/// Configuration loader
pub struct ConfigLoader;

impl ConfigLoader {
    /// Load configuration from a YAML file
    pub fn from_file(path: &Path) -> Result<Config, ConfigError> {
        let content = fs::read_to_string(path)?;
        Self::parse_yaml(&content)
    }

    /// Parse a YAML string into a validated config
    pub fn parse_yaml(yaml: &str) -> Result<Config, ConfigError> {
        let config: Config = serde_yaml::from_str(yaml)?;
        Self::validate(&config)?;
        Ok(config)
    }

    /// Validate configuration
    pub fn validate(config: &Config) -> Result<(), ConfigError> {
        // Must have at least one key
        if config.keys.is_empty() {
            return Err(ConfigError::NoKeys);
        }

        // Check for duplicate keys
        let mut seen_keys = std::collections::HashSet::new();
        for key in &config.keys {
            if !seen_keys.insert(&key.key) {
                return Err(ConfigError::DuplicateKey(key.key.clone()));
            }
        }

        // Validate upstream URL
        if config.upstream.base_url.is_empty() {
            return Err(ConfigError::InvalidUpstreamUrl("empty".to_string()));
        }

        if !config.upstream.base_url.starts_with("http://")
            && !config.upstream.base_url.starts_with("https://")
        {
            return Err(ConfigError::InvalidUpstreamUrl(format!(
                "must start with http:// or https://: {}",
                config.upstream.base_url
            )));
        }

        // Validate upstream API key
        if config.upstream.api_key.is_empty() {
            return Err(ConfigError::Invalid(
                "upstream.api_key cannot be empty".to_string(),
            ));
        }

        // Validate key values
        for key in &config.keys {
            if key.key.is_empty() {
                return Err(ConfigError::Invalid(
                    "key value cannot be empty".to_string(),
                ));
            }
            if key.name.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "key '{}' has empty name",
                    key.key
                )));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_CONFIG: &str = r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test

keys:
  - key: local-key-1
    name: Test Key 1
  - key: local-key-2
    name: Test Key 2
    consumer_id: custom-consumer
"#;

    #[test]
    fn test_load_valid_config() {
        let config = ConfigLoader::parse_yaml(VALID_CONFIG).unwrap();
        assert_eq!(config.keys.len(), 2);
        assert_eq!(config.upstream.base_url, "https://api.openai.com");
    }

    #[test]
    fn test_reject_empty_keys() {
        let yaml = r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
keys: []
"#;
        let err = ConfigLoader::parse_yaml(yaml).unwrap_err();
        assert!(matches!(err, ConfigError::NoKeys));
    }

    #[test]
    fn test_reject_duplicate_keys() {
        let yaml = r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
keys:
  - key: same-key
    name: Key 1
  - key: same-key
    name: Key 2
"#;
        let err = ConfigLoader::parse_yaml(yaml).unwrap_err();
        assert!(matches!(err, ConfigError::DuplicateKey(_)));
    }

    #[test]
    fn test_reject_invalid_url() {
        let yaml = r#"
upstream:
  base_url: not-a-url
  api_key: sk-test
keys:
  - key: key1
    name: Key 1
"#;
        let err = ConfigLoader::parse_yaml(yaml).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidUpstreamUrl(_)));
    }

    #[test]
    fn test_reject_empty_upstream_api_key() {
        let yaml = r#"
upstream:
  base_url: https://api.openai.com
  api_key: ""
keys:
  - key: key1
    name: Key 1
"#;
        let err = ConfigLoader::parse_yaml(yaml).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn test_use_defaults() {
        let config = ConfigLoader::parse_yaml(VALID_CONFIG).unwrap();
        assert_eq!(config.server.listen, "0.0.0.0:8080");
        assert_eq!(config.database.retention_days, 60);
    }
}
