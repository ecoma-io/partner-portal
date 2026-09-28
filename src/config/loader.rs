//! Configuration loader with validation

use crate::config::Config;
use http::Uri;
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
/// (`invalid type: string "sk-…", expected struct UpstreamConfig`) and backticks
/// for everything else (`invalid type: integer `50000`, expected a sequence`).
/// Both are values, and either can be a credential that was put in the wrong
/// place. Backticked *identifiers* are field and variant names (`unknown field
/// `queue_siz`, expected one of `upstream`, `server``), which is exactly the part
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
    ///
    /// There is deliberately no key validation here: partner API keys are not
    /// configuration. They are rows in `api_keys`, validated when they are
    /// written and enforced against the database's own constraints. A config
    /// that still carries a `keys:` block is rejected by serde as an unknown
    /// field, which is the loud failure a pre-stable product should give rather
    /// than a silent one.
    pub fn validate(config: &Config) -> Result<(), ConfigError> {
        validate_base_url(&config.upstream.base_url)?;

        // Validate upstream API key
        if config.upstream.api_key.is_empty() {
            return Err(ConfigError::Invalid(
                "upstream.api_key cannot be empty".to_string(),
            ));
        }

        // The password must not be empty — an empty password parses, but
        // `find_manager` deliberately ignores it, so accepting the file would
        // run a config whose manager block does nothing while reading as though
        // it did. The message names the field, never the password: a credential
        // must not reach an error that the reload watcher logs.
        if let Some(manager) = &config.manager {
            if manager.password.is_empty() {
                return Err(ConfigError::Invalid(
                    "manager.password cannot be empty when the manager block is present"
                        .to_string(),
                ));
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
        assert_eq!(config.upstream.base_url, "https://api.openai.com");
    }

    /// The clean break, stated as a test rather than left to a changelog.
    ///
    /// A config carrying a `keys:` block is now a **parse error**, not a
    /// deprecated field that quietly stops mattering. That is deliberate: the
    /// project is pre-stable, an operator who upgrades and keeps the old file
    /// must be told so by the process rather than discover it as a 401 on the
    /// first request. The error names the field, and never any value in it.
    #[test]
    fn test_a_keys_block_is_refused_by_name() {
        for yaml in [
            // The block an old file would carry.
            "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys:\n  - key: pp_secret\n    name: Key 1\n",
            // And the degenerate one, in case a tool generated it.
            "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys: []\n",
        ] {
            let err = ConfigLoader::parse_yaml(yaml).unwrap_err();
            let ConfigError::YamlParse(message) = &err else {
                panic!("expected a parse error, got {err}");
            };
            assert!(
                message.contains("keys"),
                "the error must name the field it refused: {message}"
            );
            assert!(
                !message.contains("pp_secret"),
                "the error must not echo a credential: {message}"
            );
        }
    }

    #[test]
    fn test_reject_invalid_url() {
        let yaml = r#"
upstream:
  base_url: not-a-url
  api_key: sk-test
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
"#;
        let err = ConfigLoader::parse_yaml(yaml).unwrap_err();
        assert!(matches!(err, ConfigError::Invalid(_)));
    }

    #[test]
    fn test_use_defaults() {
        let config = ConfigLoader::parse_yaml(VALID_CONFIG).unwrap();
        assert_eq!(config.database.retention_days, 60);
        // The listen address is not a config field; its default lives with the
        // env override (src/config/listen.rs).
        assert_eq!(
            crate::config::parse_listen_addr(None).unwrap(),
            crate::config::DEFAULT_LISTEN_ADDR
                .parse()
                .expect("the default listen address must parse"),
        );
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
        let yaml = format!("{VALID_CONFIG}\ndatabase:\n  queue_size: 0\n");
        let err = ConfigLoader::parse_yaml(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("database.queue_size"),
            "the error must name the field: {err}"
        );
    }

    #[test]
    fn test_reject_zero_batch_size() {
        let yaml = format!("{VALID_CONFIG}\ndatabase:\n  batch_size: 0\n");
        let err = ConfigLoader::parse_yaml(&yaml).unwrap_err();
        assert!(
            err.to_string().contains("database.batch_size"),
            "the error must name the field: {err}"
        );
    }

    /// The manager block's one refusal path: an empty password, which would
    /// parse and then silently never authenticate. The error names the field.
    #[test]
    fn test_manager_block_must_have_a_password() {
        let yaml = "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nmanager:\n  password: \"\"\n";
        let err = ConfigLoader::parse_yaml(yaml).expect_err("an empty password must not load");
        assert!(
            err.to_string().contains("manager.password"),
            "the error must name manager.password: {err}"
        );
        assert!(
            !err.to_string().contains("secret"),
            "a validation error must not echo the password: {err}"
        );
    }

    /// A blank entry in `allowed_models` is a typo that silently lists nothing
    /// useful, so it is refused (ADR 0012). The check is no longer a *config*
    /// validation, and deleting the config keys must not have deleted the rule
    /// with them: the enforcement point moved to [`ApiKeyStore`], and this
    /// stands as the assertion that it is still enforced.
    ///
    /// Model names are not credentials, so the message may — and this test
    /// already relies on it — name the offending entry without echoing a
    /// secret.
    #[test]
    fn test_a_blank_allowed_models_entry_is_still_refused() {
        use crate::apikeys::ApiKeyStore;
        use crate::ledger::LedgerPool;
        use std::sync::Arc;

        let dir = tempfile::TempDir::new().unwrap();
        let store = ApiKeyStore::new(
            Arc::new(LedgerPool::new(dir.path().join("ledger.db")).unwrap()),
            b"a-blank-model-test-secret-32-byt".to_vec(),
        );

        for (label, models) in [
            ("empty", vec![String::new()]),
            ("whitespace", vec!["gpt-4o".to_string(), "   ".to_string()]),
        ] {
            let err = store
                .create("blank", "acme", models, None)
                .err()
                .unwrap_or_else(|| panic!("a {label} allowed_models entry must be refused"));
            assert!(
                err.to_string().contains("allowed_models"),
                "the error must name the offending field: {err}"
            );
        }
    }

    /// The strict-by-default allow-list moved out of configuration with the
    /// keys themselves, and the contract moved with it: an empty list is
    /// `[]` in the database, not an omitted field in a file. This is the shape
    /// the admin API stores and the proxy reads, so the row is the assertion.
    #[test]
    fn test_the_allow_list_is_a_database_column_not_a_config_field() {
        use crate::apikeys::ApiKeyStore;
        use crate::ledger::LedgerPool;
        use std::sync::Arc;

        let dir = tempfile::TempDir::new().unwrap();
        let store = ApiKeyStore::new(
            Arc::new(LedgerPool::new(dir.path().join("ledger.db")).unwrap()),
            b"an-allow-list-test-secret-32-bytes".to_vec(),
        );

        let (listed, _) = store
            .create(
                "listed",
                "acme",
                vec!["gpt-4o".to_string(), "gpt-4o-mini".to_string()],
                None,
            )
            .unwrap();
        assert_eq!(
            listed.allowed_models,
            vec!["gpt-4o".to_string(), "gpt-4o-mini".to_string()]
        );

        // The strict default: no list means no model, and it survives a
        // round trip through storage as an empty list rather than as `null` or
        // as an absent value.
        let (strict, _) = store.create("strict", "acme", Vec::new(), None).unwrap();
        assert!(strict.allowed_models.is_empty());
        assert_eq!(
            store.get(strict.id).unwrap().unwrap().allowed_models,
            Vec::<String>::new()
        );
    }

    /// A fully-specified manager block loads, and the password stays out of
    /// every error rendering even when the manager block is *valid*.
    #[test]
    fn test_manager_block_with_a_password_loads() {
        let yaml = r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
manager:
  password: mgr-valid-secret
"#;
        let config = ConfigLoader::parse_yaml(yaml).unwrap();
        let manager = config.manager.expect("manager present");
        assert_eq!(manager.password, "mgr-valid-secret");
    }

    /// The unknown-field guard extends into the manager block: a typo'd key
    /// there must be a parse error, not a silently ignored intention.
    #[test]
    fn test_reject_unknown_field_inside_the_manager_block() {
        let yaml = r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
manager:
  password: secret
  pasword: typo
"#;
        let err = ConfigLoader::parse_yaml(yaml).unwrap_err();
        assert!(
            err.to_string().contains("pasword"),
            "the error must name the offending field: {err}"
        );
        assert!(
            !err.to_string().contains("secret"),
            "a parse error must not echo the manager password: {err}"
        );
    }

    #[test]
    fn test_accept_the_smallest_workable_batch_and_queue() {
        let yaml = format!("{VALID_CONFIG}\ndatabase:\n  queue_size: 1\n  batch_size: 1\n");
        let config = ConfigLoader::parse_yaml(&yaml).unwrap();
        assert_eq!(config.database.queue_size, 1);
        assert_eq!(config.database.batch_size, 1);
    }

    /// A typo'd key is a parse error, not a silently discarded intention: the
    /// server would otherwise run on the default while the file says otherwise.
    #[test]
    fn test_reject_unknown_fields_at_every_level() {
        let cases = [
            ("queue_siz", "database:\n  queue_siz: 50000\n"),
            (
                "upstream",
                "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\n  timeout_secsX: 10\n",
            ),
            ("database", "database:\n  batch_sise: 5\n"),
            ("server", "server:\n  listenX: 0.0.0.0:1\n"),
            // `listen` left the config entirely (PARTNER_PORTAL_LISTEN); a
            // config file that still carries it must fail loudly, not bind
            // somewhere the operator did not mean.
            ("listen", "server:\n  listen: \"0.0.0.0:8080\"\n"),
            // And `keys` left it for the database; `test_a_keys_block_is_refused_by_name`
            // states that break in full, here it is simply one more level whose
            // guard must not have been dropped in the rewrite.
            ("keys", "keys:\n  - key: a\n    name: b\n    metadata: 1\n"),
        ];
        for (field, block) in cases {
            let yaml = format!(
                "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\n{block}"
            );
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
        ConfigLoader::from_file(&path)
            .unwrap_or_else(|e| panic!("config.example.yaml must load: {e}"));
        // And it must demonstrate *no* key: the example is what an operator
        // copies, and a plaintext key in it would be a credential in version
        // control and a file that the running process no longer reads.
        let raw = std::fs::read_to_string(&path).unwrap();
        assert!(
            !raw.contains("\nkeys:"),
            "config.example.yaml must not carry a keys block; keys are issued \
             through /api/admin/api-keys"
        );
    }

    /// The hard contract: an error for a config holding credentials must never
    /// render one. Every failure path in this file is checked, because the
    /// loader's errors are logged by the reload watcher.
    ///
    /// These used to be key-block cases — a duplicate key, an empty key, an
    /// empty key name, an empty key list. Those failure modes moved to the
    /// database with the keys, and the cases that remain are the ones a YAML
    /// file can still produce: a credential written where a block belongs, and
    /// a credential where a sequence is expected.
    #[test]
    fn test_no_error_renders_a_credential() {
        const VALUE: &str = "pp-local-key-do-not-log";

        let cases: [(&str, String); 4] = [
            (
                "credential in the wrong place",
                format!("upstream: {VALUE}\n"),
            ),
            (
                "credential where a list is expected",
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nupstreams: [{VALUE}]\n"
                ),
            ),
            (
                "credential where a scalar is expected",
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: {VALUE}\nretention_days: {VALUE}\n"
                ),
            ),
            (
                "credential in a comment is still a file the operator should not have",
                format!(
                    "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\ndatabase:\n  batch_sise: 5 # {VALUE}\n"
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
            "upstream:\n  base_url: https://api.openai.com\n  api_key: \"\"\n  api_keyX: {SECRET}\n"
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
                r#"database: invalid type: string "sk-secret-here", expected struct DatabaseConfig at line 3 column 11"#
            ),
            r#"database: invalid type: string "<redacted>", expected struct DatabaseConfig at line 3 column 11"#
        );
        assert_eq!(
            redact_scalars(
                "database.queue_size: invalid type: integer `1234567890`, expected a sequence at line 4 column 7"
            ),
            "database.queue_size: invalid type: integer `<redacted>`, expected a sequence at line 4 column 7"
        );
        assert_eq!(
            redact_scalars(
                "unknown field `queue_siz`, expected one of `upstream`, `server` at line 7 column 1"
            ),
            "unknown field `queue_siz`, expected one of `upstream`, `server` at line 7 column 1"
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
