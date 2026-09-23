//! Configuration loader with validation

use crate::config::Config;
use http::Uri;
use std::collections::HashMap;
use std::fs;
use std::path::Path;
use thiserror::Error;

use super::types::REDACTED;

#[derive(Error, Debug)]
pub enum ConfigError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),

    /// Only the redacted message is kept, never the `serde_yaml::Error` itself.
    ///
    /// serde's "invalid type" family echoes the offending scalar back — writing
    /// `upstream: sk-…` instead of nesting the block produces `invalid type:
    /// string "sk-…", expected struct UpstreamConfig` — and a scalar in this
    /// file can be a credential. This error is `Display`-formatted into a log
    /// line by the reload watcher, so the value cannot come along. What is
    /// kept is everything an operator needs to find the fault: the field path,
    /// the expected type, and the line and column.
    #[error("YAML parse error: {0}")]
    YamlParse(String),

    #[error("No keys configured")]
    NoKeys,

    /// Names the entries by position and by *name*, which is how an operator
    /// finds them. A key value is a credential and this error reaches the log
    /// (src/config/hot_reload.rs), so it never appears here.
    #[error(
        "duplicate key value: keys[{index}] (name '{name}') repeats keys[{first_index}] (name '{first_name}')"
    )]
    DuplicateKey {
        index: usize,
        name: String,
        first_index: usize,
        first_name: String,
    },

    #[error("Invalid upstream URL: {0}")]
    InvalidUpstreamUrl(String),

    #[error("Invalid configuration: {0}")]
    Invalid(String),
}

impl From<serde_yaml::Error> for ConfigError {
    fn from(e: serde_yaml::Error) -> Self {
        ConfigError::YamlParse(redact_scalars(&e.to_string()))
    }
}

/// Replace every scalar a `serde_yaml` message echoed back with a placeholder.
///
/// serde quotes the offending value two ways — double quotes for strings
/// (`invalid type: string "pp-…", expected struct KeyConfig`) and backticks for
/// everything else (`invalid type: integer `50000`, expected a sequence`). Both
/// are values, and either can be a credential that was put in the wrong place.
/// Backticked *identifiers* are field and variant names (`unknown field
/// `queue_siz`, expected one of `upstream`, `keys``), which is exactly the part
/// that has to survive: they are what names the fault.
fn redact_scalars(message: &str) -> String {
    let mut out = String::with_capacity(message.len());
    let mut rest = message;

    while let Some(pos) = rest.find(['"', '`']) {
        out.push_str(&rest[..pos]);
        let delimiter = rest.as_bytes()[pos];
        let body = &rest[pos + 1..];

        // A double-quoted string is escaped by serde's formatter, so its end is
        // the first unescaped quote; a backticked token is never escaped.
        let end = if delimiter == b'"' {
            let bytes = body.as_bytes();
            let mut i = 0;
            loop {
                match bytes.get(i) {
                    None => break None,
                    Some(b'\\') => i += 2,
                    Some(b'"') => break Some(i),
                    Some(_) => i += 1,
                }
            }
        } else {
            body.find('`')
        };

        let (content, remainder) = match end {
            Some(i) => (&body[..i], &body[i + 1..]),
            None => (body, ""),
        };

        out.push(delimiter as char);
        if delimiter == b'`' && is_token(content) {
            out.push_str(content);
        } else {
            out.push_str(REDACTED);
        }
        // An unterminated quote is not a shape serde produces, but do not
        // invent a closing delimiter for it either.
        if end.is_some() {
            out.push(delimiter as char);
        }
        rest = remainder;
    }

    out.push_str(rest);
    out
}

/// Whether a backticked span is a name rather than a value.
///
/// `true`, `false` and numbers are values; a leading `-` on `-5` must not pass
/// the same test a `-` in a name would.
fn is_token(content: &str) -> bool {
    let mut chars = content.chars();
    match chars.next() {
        Some(c) if c.is_ascii_alphabetic() || c == '_' => {}
        _ => return false,
    }
    chars.all(|c| c.is_ascii_alphanumeric() || c == '_')
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

        // Check for duplicate keys, remembering *where* each value was first
        // seen so the error can name both entries by position.
        let mut seen_keys: HashMap<&str, usize> = HashMap::new();
        for (index, key) in config.keys.iter().enumerate() {
            if let Some(&first_index) = seen_keys.get(key.key.as_str()) {
                return Err(ConfigError::DuplicateKey {
                    index,
                    name: key.name.clone(),
                    first_index,
                    first_name: config.keys[first_index].name.clone(),
                });
            }
            seen_keys.insert(&key.key, index);
        }

        validate_base_url(&config.upstream.base_url)?;

        // Validate upstream API key
        if config.upstream.api_key.is_empty() {
            return Err(ConfigError::Invalid(
                "upstream.api_key cannot be empty".to_string(),
            ));
        }

        // Validate key values. The index is the only way to identify an entry
        // here: one has no value to print and the other's name is the thing
        // that is missing.
        for (index, key) in config.keys.iter().enumerate() {
            if key.key.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "keys[{index}] has an empty key value"
                )));
            }
            if key.name.is_empty() {
                return Err(ConfigError::Invalid(format!(
                    "keys[{index}] has an empty name; a key is identified by its name in logs and resolves to it as its consumer_id"
                )));
            }
        }

        // A metering queue capacity of zero is not a small queue, it is no
        // queue: tokio refuses to build a zero-capacity channel, so the process
        // panics at startup. Refusing the config says the same thing in a
        // sentence an operator can act on.
        if config.database.queue_size == 0 {
            return Err(ConfigError::Invalid(
                "database.queue_size must be at least 1; a zero-capacity metering queue cannot be built"
                    .to_string(),
            ));
        }

        // `batch_size: 0` is the dangerous one. The writer only arms its
        // receive while its batch is below this size, so a zero batch never
        // pulls a record, never commits and never sees the channel close: the
        // process stays ready, keeps accepting traffic, and hangs on shutdown
        // with every record unaccounted for (src/ledger/writer.rs).
        if config.database.batch_size == 0 {
            return Err(ConfigError::Invalid(
                "database.batch_size must be at least 1; a zero batch never arms the writer's receive, so metering would stall while readiness stayed green"
                    .to_string(),
            ));
        }

        // `batch_timeout_ms` is deliberately not checked. The writer clamps its
        // wait to a minimum of 1 ms and flushes as soon as the deadline has
        // passed, so 0 means "commit as soon as anything is queued" rather than
        // a stall (src/ledger/writer.rs).

        Ok(())
    }
}

/// Validate `upstream.base_url` as what it is used for: a prefix the request
/// path is appended to.
///
/// A prefix test is not enough. `http://` has the right prefix and no host, so
/// the server would start and every request would fail at connect time with a
/// 502 — the documented behaviour is a refusal at startup. Parsing the URI also
/// rejects `not-a-url`, which parses as an authority with no scheme and would
/// otherwise slip past a host-only check.
///
/// The error message names the field and the reason, never the value:
/// `upstream.base_url` may be the one place an operator puts a credential, and
/// an error path is exactly where a secret must not be copied.
fn validate_base_url(base_url: &str) -> Result<(), ConfigError> {
    let invalid = |reason: &str| {
        ConfigError::InvalidUpstreamUrl(format!("upstream.base_url {reason}: '{base_url}'"))
    };

    let uri: Uri = base_url
        .parse()
        .map_err(|_| invalid("is not a valid URL (expected http://host or https://host)"))?;

    match uri.scheme_str() {
        Some("http") | Some("https") => {}
        Some(scheme) => return Err(invalid(&format!("must use http or https, not '{scheme}'"))),
        None => return Err(invalid("has no scheme (expected http:// or https://)")),
    }

    // `https://user:pass@host` parses, and hyper then **silently drops** the
    // userinfo: the upstream receives a `Host` header with no credentials and no
    // `Authorization` derived from them. Measured, not assumed — a probe against
    // a local listener showed `Host: 127.0.0.1:port` and only our Bearer header.
    // So a base URL written this way does not authenticate anything, while
    // leaving the credential in a URI that error strings and diagnostics tend to
    // print. Refusing is the only honest answer: the configuration cannot work
    // as written, and accepting it would leak a secret for nothing.
    if uri
        .authority()
        .map(|authority| authority.as_str().contains('@'))
        .unwrap_or(false)
    {
        return Err(ConfigError::InvalidUpstreamUrl(
            "upstream.base_url must not embed credentials: the userinfo is \
             discarded before the request is sent, so it would leak the value \
             without authenticating anything. Use upstream.api_key"
                .to_string(),
        ));
    }

    match uri.authority() {
        Some(authority) if !authority.host().is_empty() => Ok(()),
        _ => Err(invalid("has no host")),
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

    /// A valid config with `upstream.base_url` replaced, for the URL cases.
    fn config_with_base_url(base_url: &str) -> String {
        VALID_CONFIG.replace(
            "base_url: https://api.openai.com",
            &format!("base_url: {base_url}"),
        )
    }

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
        // Both entries are named by position and by name, and the *value* they
        // share — a credential — is not in the message.
        let ConfigError::DuplicateKey {
            index,
            name,
            first_index,
            first_name,
        } = err
        else {
            panic!("expected DuplicateKey, got {err}");
        };
        assert_eq!(index, 1);
        assert_eq!(name, "Key 2");
        assert_eq!(first_index, 0);
        assert_eq!(first_name, "Key 1");
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

    /// Every rejected URL must be rejected *at load*. `http://` has the right
    /// prefix and no host, so a prefix test starts the process and every request
    /// then fails at connect time with a 502.
    #[test]
    fn test_reject_base_url_without_a_host() {
        for base_url in ["http://", "https://", "https://:8080", "not a url"] {
            let yaml = config_with_base_url(base_url);
            let err = ConfigLoader::parse_yaml(&yaml).unwrap_err();
            assert!(
                matches!(err, ConfigError::InvalidUpstreamUrl(_)),
                "{base_url} must be refused at load, got {err}"
            );
            assert!(
                err.to_string().contains("upstream.base_url"),
                "the error must name the field: {err}"
            );
        }
    }

    #[test]
    fn test_reject_base_url_with_a_non_http_scheme() {
        let err = ConfigLoader::parse_yaml(&config_with_base_url("ftp://host")).unwrap_err();
        assert!(matches!(err, ConfigError::InvalidUpstreamUrl(_)));
        assert!(
            err.to_string().contains("http or https"),
            "the error must say what is wrong: {err}"
        );
    }

    #[test]
    fn test_accept_base_url_with_a_path_and_a_port() {
        for base_url in [
            "https://gateway.internal:8443/openai/v1",
            "https://api.openai.com",
            "https://api.openai.com/",
            "http://127.0.0.1:9000",
        ] {
            let config = ConfigLoader::parse_yaml(&config_with_base_url(base_url))
                .unwrap_or_else(|e| panic!("{base_url} must be accepted: {e}"));
            assert_eq!(config.upstream.base_url, base_url);
        }
    }

    /// A zero queue or a zero batch is a configuration that cannot work; the
    /// failure it produces at runtime is silent, which is why it is refused
    /// here instead.
    #[test]
    fn test_reject_zero_queue_size() {
        let yaml = VALID_CONFIG.replace("keys:", "database:\n  queue_size: 0\nkeys:");
        let err = ConfigLoader::parse_yaml(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("database.queue_size"),
            "the error must name the field: {err}"
        );
    }

    #[test]
    fn test_reject_zero_batch_size() {
        let yaml = VALID_CONFIG.replace("keys:", "database:\n  batch_size: 0\nkeys:");
        let err = ConfigLoader::parse_yaml(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("database.batch_size"),
            "the error must name the field: {err}"
        );
    }

    #[test]
    fn test_accept_the_smallest_workable_batch_and_queue() {
        let yaml = VALID_CONFIG.replace(
            "keys:",
            "database:\n  queue_size: 1\n  batch_size: 1\nkeys:",
        );
        let config = ConfigLoader::parse_yaml(&yaml).unwrap();
        assert_eq!(config.database.queue_size, 1);
        assert_eq!(config.database.batch_size, 1);
    }

    /// A typo'd key is a parse error, not a silently discarded intention: the
    /// server would otherwise run on the default while the file says otherwise.
    #[test]
    fn test_reject_unknown_fields_at_every_level() {
        let cases = [
            ("queue_siz", "queue_siz: 50000\n"),
            (
                "upstream",
                "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\n  timeout_secsX: 10\n",
            ),
            ("database", "database:\n  batch_sise: 5\n"),
            ("server", "server:\n  listenX: 0.0.0.0:1\n"),
            ("keys", "keys:\n  - key: a\n    name: b\n    metadota: 1\n"),
        ];
        for (field, block) in cases {
            let yaml = if block.starts_with("upstream:") {
                format!("{block}keys:\n  - key: key1\n    name: Key 1\n")
            } else if block.starts_with("keys:") {
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\n{block}"
                )
            } else {
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys:\n  - key: key1\n    name: Key 1\n{block}"
                )
            };
            let err = ConfigLoader::parse_yaml(&yaml)
                .expect_err(&format!("the unknown field {field} must be refused"));
            assert!(
                err.to_string().contains(field),
                "the error must name the offending field {field}: {err}"
            );
        }
    }

    /// The reference configuration is part of the contract, not documentation
    /// that happens to be nearby: it is what an operator copies. A field that
    /// lands in one and not the other is a defect, and an unknown key is now a
    /// parse error, so a rename that misses the example fails here.
    #[test]
    fn test_example_config_loads() {
        let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("config.example.yaml");
        let config = ConfigLoader::from_file(&path)
            .unwrap_or_else(|e| panic!("config.example.yaml must load: {e}"));
        assert!(
            !config.keys.is_empty(),
            "the example must demonstrate at least one key"
        );
    }

    /// The hard contract: an error for a config holding credentials must never
    /// render one. Every failure path in this file is checked, because the
    /// loader's errors are logged by the reload watcher.
    #[test]
    fn test_no_error_renders_a_credential() {
        const VALUE: &str = "pp-local-key-do-not-log";

        let cases: [(&str, String); 6] = [
            (
                "duplicate key value",
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys:\n  - key: {VALUE}\n    name: First\n  - key: {VALUE}\n    name: Second\n"
                ),
            ),
            (
                "empty key name",
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys:\n  - key: {VALUE}\n    name: \"\"\n"
                ),
            ),
            (
                "empty key value",
                "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys:\n  - key: \"\"\n    name: Named\n".to_string(),
            ),
            (
                "no keys",
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys: []\n# {VALUE}\n"
                ),
            ),
            (
                "credential in the wrong place",
                format!("upstream: {VALUE}\nkeys:\n  - key: key1\n    name: Key 1\n"),
            ),
            (
                "credential where a list is expected",
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys: [{VALUE}]\n"
                ),
            ),
        ];

        for (name, yaml) in cases {
            let err =
                ConfigLoader::parse_yaml(&yaml).expect_err(&format!("{name} must fail to load"));
            let rendered = err.to_string();
            assert!(
                !rendered.contains(VALUE),
                "the {name} error rendered the credential: {rendered}"
            );
            assert!(
                !format!("{err:?}").contains(VALUE),
                "the {name} error's Debug rendered the credential"
            );
        }
    }

    /// The credential path that is not a variant of its own: an upstream key
    /// echoed back by serde when the block is malformed.
    #[test]
    fn test_no_error_renders_the_upstream_key() {
        const SECRET: &str = "sk-upstream-do-not-log";
        let yaml = format!(
            "upstream:\n  base_url: https://api.openai.com\n  api_key: \"\"\n  api_keyX: {SECRET}\nkeys:\n  - key: key1\n    name: Key 1\n"
        );
        let err = ConfigLoader::parse_yaml(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("api_keyX"),
            "the error must name the offending field: {err}"
        );
        assert!(
            !err.to_string().contains(SECRET),
            "the error rendered the upstream credential: {err}"
        );
    }

    #[test]
    fn test_redact_scalars_keeps_names_and_drops_values() {
        // The two shapes serde produces, plus the identifiers that must survive.
        assert_eq!(
            redact_scalars(
                r#"keys[0]: invalid type: string "sk-secret-here", expected struct KeyConfig at line 5 column 5"#
            ),
            r#"keys[0]: invalid type: string "<redacted>", expected struct KeyConfig at line 5 column 5"#
        );
        assert_eq!(
            redact_scalars(
                "keys: invalid type: integer `1234567890`, expected a sequence at line 4 column 7"
            ),
            "keys: invalid type: integer `<redacted>`, expected a sequence at line 4 column 7"
        );
        assert_eq!(
            redact_scalars(
                "unknown field `queue_siz`, expected one of `upstream`, `keys`, `database` at line 7 column 1"
            ),
            "unknown field `queue_siz`, expected one of `upstream`, `keys`, `database` at line 7 column 1"
        );
        // A negative number is a value even though it starts with a `-`.
        assert_eq!(redact_scalars("integer `-5`"), "integer `<redacted>`");
        // Escapes inside a quoted value must not end the span early: serde
        // renders a `String` with `{:?}`, so a quote or a backslash inside the
        // value arrives escaped.
        assert_eq!(
            redact_scalars("string \"a\\\"b\" tail"),
            "string \"<redacted>\" tail"
        );
    }
}
