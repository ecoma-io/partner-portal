//! Configuration types

use hex;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// What a secret renders as wherever it might be logged or `Debug`-printed.
///
/// `keys[].key` and `upstream.api_key` are plaintext credentials in a file; the
/// file's permissions are their security boundary. Nothing in this process may
/// copy one into a log line, an error value or a panic message, so types that
/// hold one redact on render and the loader redacts on error.
pub(crate) const REDACTED: &str = "<redacted>";

/// Main configuration structure
///
/// Unknown fields are rejected rather than ignored: a typo'd key would
/// otherwise be accepted, silently discarded, and the process would run with
/// the default while the operator believes the file says otherwise.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// Optional operator credential with a cross-consumer dashboard view.
    ///
    /// When present, the dashboard login accepts this password (sent as
    /// `Authorization: Bearer <password>`) and the resulting session can see
    /// the usage of every consumer. This is the one deliberate widening
    /// of the otherwise strictly consumer-scoped dashboard; see docs/adr/0008
    /// and docs/adr/0013, which superseded the allow-list it once carried.
    #[serde(default)]
    pub manager: Option<ManagerConfig>,
}

impl Config {
    /// Compute a hash of the configuration for change detection
    ///
    /// Every field participates, through serde — the hash covers exactly what
    /// the file says, so a field added to this struct is covered without this
    /// function being touched. What it must *not* do is depend on how a value
    /// happens to be laid out in memory, which is why this configuration holds
    /// no map-typed field: a `HashMap` serialises in iteration order, two
    /// parses of an unchanged file would hash differently, and the watcher
    /// would swap a config that did not change — every second.
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

    /// Every credential this configuration holds.
    ///
    /// The upstream key, every local key, *and* the manager password, because
    /// the rule that matters is "no credential in this file reaches text we
    /// persist or serve" — stating it over the whole set makes it true by
    /// construction, instead of resting on an argument about which paths could
    /// see which secret. In practice only the upstream key can appear in
    /// upstream text (the local keys are never sent upstream), so including the
    /// rest costs a comparison each.
    ///
    /// Values too short to be credentials are left out deliberately: scrubbing
    /// every occurrence of a two-character string would garble ordinary words in
    /// a recorded reason and hide the real message.
    pub fn credentials(&self) -> Vec<&str> {
        std::iter::once(self.upstream.api_key.as_str())
            .chain(self.keys.iter().map(|k| k.key.as_str()))
            .chain(self.manager.as_ref().map(|m| m.password.as_str()))
            .filter(|secret| secret.len() >= MIN_SCRUBBED_SECRET_LEN)
            .collect()
    }
}

/// Shortest value worth scrubbing out of recorded text.
///
/// A real API key is long; anything below this is a configuration mistake rather
/// than a credential, and replacing it everywhere would corrupt the reason it is
/// embedded in.
pub const MIN_SCRUBBED_SECRET_LEN: usize = 8;

/// Replace every credential in `text` with [`REDACTED`].
///
/// Applied to text on its way *into* the ledger and *out* of the API, never to
/// anything on the request path: an upstream that echoes the credential it was
/// given — "Incorrect API key provided: sk-…" is a real provider behaviour —
/// would otherwise have that value committed to `error_message` and served back
/// through the dashboard, which is a credential outliving its request.
pub fn redact_credentials(text: &str, credentials: &[&str]) -> String {
    let mut out = text.to_string();
    for secret in credentials {
        if secret.len() >= MIN_SCRUBBED_SECRET_LEN && out.contains(secret) {
            out = out.replace(secret, REDACTED);
        }
    }
    out
}

/// Upstream configuration
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

/// Manual, so `api_key` renders as a redaction. `ConfigSnapshot` derives
/// `Debug`, and a derived `Debug` here would put the upstream credential into
/// any `{:?}` of a snapshot anywhere in the process.
impl std::fmt::Debug for UpstreamConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UpstreamConfig")
            .field("base_url", &self.redacted_base_url())
            .field("api_key", &REDACTED)
            .field("timeout_secs", &self.timeout_secs)
            .field("connect_timeout_secs", &self.connect_timeout_secs)
            .finish()
    }
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

    /// The base URL with any embedded credentials removed, for logging.
    ///
    /// `https://user:secret@host` is a valid URL and passes validation, so it
    /// can reach any log line that renders `base_url`. Both halves of a
    /// userinfo are secret in practice — some providers carry the key as the
    /// *username* — so neither is kept: the value is replaced with a marker
    /// that says a credential is there without saying what it is. Use this
    /// everywhere the URL is rendered; `base_url` itself is only for building
    /// the request, where the credential is meant to travel.
    pub fn redacted_base_url(&self) -> String {
        match redact_userinfo(&self.base_url) {
            Some(redacted) => redacted,
            None => self.base_url.clone(),
        }
    }
}

/// Strip the `user:pass@` part of a URL, if it has one.
///
/// Deliberately string surgery rather than a URL parse: the whole point is to
/// never hold the credential in a parsed form that might be formatted back out,
/// and `base_url` is validated separately. Returns `None` when there is nothing
/// to redact, so the caller can hand back the original unchanged.
fn redact_userinfo(url: &str) -> Option<String> {
    let scheme_end = url.find("://")? + 3;
    let rest = &url[scheme_end..];
    // The authority ends at the first `/`, `?` or `#`; userinfo can only be
    // before that.
    let authority_end = rest.find(['/', '?', '#']).unwrap_or(rest.len());
    let at = rest[..authority_end].rfind('@')?;

    let mut out = String::with_capacity(url.len());
    out.push_str(&url[..scheme_end]);
    out.push_str("<credentials>@");
    out.push_str(&rest[at + 1..]);
    Some(out)
}

/// Local API key configuration
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyConfig {
    /// The API key value (used for authentication)
    pub key: String,

    /// Human-readable name for the key
    pub name: String,

    /// Optional consumer ID (derived from key if not specified)
    #[serde(default)]
    pub consumer_id: Option<String>,

    /// Models this key may call. **Strict**: an empty list (or a key that omits
    /// the field entirely) is allowed *no* model — every inference request is
    /// refused with `404 model_not_found` before it reaches the upstream. A
    /// model already in use must be added here or the key stops working the
    /// moment this ships. Read live per request (ADR 0012).
    #[serde(default)]
    pub allowed_models: Vec<String>,
}

/// Manual, so `key` renders as a redaction. A key value is a credential, and a
/// derived `Debug` here would emit it from any `{:?}` of a config snapshot —
/// including the ones in diagnostics written by other modules.
impl std::fmt::Debug for KeyConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("KeyConfig")
            .field("key", &REDACTED)
            .field("name", &self.name)
            .field("consumer_id", &self.consumer_id)
            .finish()
    }
}

impl KeyConfig {
    /// Get or derive the consumer ID
    pub fn consumer_id(&self) -> &str {
        self.consumer_id.as_deref().unwrap_or(&self.name)
    }

    /// Whether this key may call `model`.
    ///
    /// Strict by default: an empty list (or a key that never declared the
    /// field) allows nothing. Only an explicitly listed name passes.
    pub fn allows_model(&self, model: &str) -> bool {
        !self.allowed_models.is_empty() && self.allowed_models.iter().any(|m| m == model)
    }
}

/// Credential granting a **cross-consumer** dashboard view.
///
/// This is the one deliberate exception to the "dashboard is consumer-scoped"
/// rule in ADR 0008: an operator who presents this password (as
/// `Authorization: Bearer <password>`) may view the usage of **every**
/// consumer (ADR 0013). It is a *credential*, exactly like a key value, so
/// it is scrubbed from ledger text, redacted in `Debug`, and never logged by
/// the reload watcher — the same treatment `keys[].key` gets.
#[derive(Clone, Default, Serialize, Deserialize)]
#[serde(deny_unknown_fields, default)]
pub struct ManagerConfig {
    /// The password. Required when the `manager` block is present.
    pub password: String,
}

/// Manual, so `password` renders as a redaction. A manager password is a
/// credential — the same exact-equality bearer the keys are — and a derived
/// `Debug` here would emit it from any `{:?}` of a config snapshot.
impl std::fmt::Debug for ManagerConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ManagerConfig")
            .field("password", &REDACTED)
            .finish()
    }
}

impl Config {
    /// Match a presented Bearer token against the manager password.
    ///
    /// Exact string equality, like a key value. Returns the manager config so a
    /// caller can tell a manager credential from a key.
    pub fn find_manager(&self, token: &str) -> Option<&ManagerConfig> {
        self.manager
            .as_ref()
            .filter(|m| !m.password.is_empty() && m.password == token)
    }
}

/// Server configuration
///
/// The listen address is deliberately **not** here: it is an environment
/// property, not application configuration — `PARTNER_PORTAL_LISTEN`
/// (see [`crate::config::listen_addr`]). A port that lived in this file would
/// have to be changed in lockstep with the container runtime's port mapping,
/// in a different file, with nothing keeping the two honest.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ServerConfig {
    /// Wait for in-flight requests to finish on SIGTERM.
    ///
    /// This controls only whether the listener waits for work already accepted.
    /// Draining the metering pipeline is not optional and happens either way —
    /// there is no setting that lets a shutdown drop committed-but-unwritten
    /// usage.
    #[serde(default = "default_true")]
    pub graceful_shutdown: bool,

    /// How long readiness keeps failing before the listener stops accepting.
    ///
    /// A load balancer learns that an instance is leaving by polling readiness,
    /// which takes at least one poll interval. Closing the listener at the same
    /// instant readiness flips produces connection errors at the balancer;
    /// waiting this long lets it stop sending first. Too short is a visible
    /// error, so it errs on the generous side.
    #[serde(default = "default_shutdown_grace_secs")]
    pub shutdown_grace_secs: u64,

    /// Maximum request body size in bytes (default: 10MB)
    #[serde(default = "default_max_body_size")]
    pub max_body_size: usize,

    /// Origins allowed to call this server from a browser.
    ///
    /// Empty means no CORS headers at all, which is correct for the intended
    /// deployment: server-side clients and the dashboard's own same-origin UI.
    /// Listing origins is an explicit opt-in for browser callers, and it stays
    /// an allow-list because this proxy holds an upstream credential — a
    /// wildcard would let any page on the internet use it.
    #[serde(default)]
    pub cors_allow_origins: Vec<String>,

    /// How often the dashboard's SSE poller checks SQLite for changes.
    #[serde(default = "default_sse_poll_interval_ms")]
    pub sse_poll_interval_ms: u64,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            graceful_shutdown: default_true(),
            shutdown_grace_secs: default_shutdown_grace_secs(),
            max_body_size: default_max_body_size(),
            cors_allow_origins: Vec::new(),
            sse_poll_interval_ms: default_sse_poll_interval_ms(),
        }
    }
}

fn default_true() -> bool {
    true
}
fn default_shutdown_grace_secs() -> u64 {
    5
}
fn default_max_body_size() -> usize {
    10 * 1024 * 1024
} // 10MB
fn default_sse_poll_interval_ms() -> u64 {
    500
}

/// Database configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
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

    /// How often retention runs, in seconds.
    #[serde(default = "default_retention_interval_secs")]
    pub retention_interval_secs: u64,

    /// Rows deleted per retention slice.
    ///
    /// Retention is sliced so the write lock is released between slices. A single
    /// `DELETE` over 60 days of records would hold the ledger's write lock for its
    /// whole duration, and every request accepted during that window would wait
    /// on the metering queue instead of being served.
    #[serde(default = "default_retention_batch_size")]
    pub retention_batch_size: usize,
}

impl Default for DatabaseConfig {
    fn default() -> Self {
        Self {
            path: default_db_path(),
            retention_days: default_retention_days(),
            queue_size: default_queue_size(),
            batch_size: default_batch_size(),
            batch_timeout_ms: default_batch_timeout_ms(),
            retention_interval_secs: default_retention_interval_secs(),
            retention_batch_size: default_retention_batch_size(),
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
fn default_retention_interval_secs() -> u64 {
    3600
}
fn default_retention_batch_size() -> usize {
    2_000
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
                allowed_models: Vec::new(),
            }],
            database: DatabaseConfig::default(),
            manager: None,
        };

        let hash1 = config.hash();
        let hash2 = config.hash();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_config_hash_changes_when_a_value_changes() {
        let base = r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
  timeout_secs: 120
keys:
  - key: local-key
    name: test
database:
  batch_size: 100
"#;

        let original = crate::config::ConfigLoader::parse_yaml(base).unwrap();

        for (name, changed) in [
            ("upstream api_key", base.replace("sk-test", "sk-other")),
            (
                "upstream timeout",
                base.replace("timeout_secs: 120", "timeout_secs: 121"),
            ),
            ("key value", base.replace("local-key", "local-key-2")),
            (
                "database",
                base.replace("batch_size: 100", "batch_size: 101"),
            ),
        ] {
            let other = crate::config::ConfigLoader::parse_yaml(&changed).unwrap();
            assert_ne!(
                original.hash(),
                other.hash(),
                "a changed {name} must change the hash, or the watcher misses the reload"
            );
        }
    }

    #[test]
    fn test_every_configured_credential_is_scrubbed_from_text() {
        // The upstream key is the one that can appear in upstream text, but the
        // point of scrubbing the whole set is that the rule holds for any of
        // them without an argument about reachability — including the manager
        // password, which could otherwise reach an `error_message` if an
        // upstream echoed it the way providers echo keys.
        let config = crate::config::ConfigLoader::parse_yaml(
            r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-upstream-secret
keys:
  - key: pp-local-secret
    name: test
manager:
  password: mgr-secret-password
"#,
        )
        .unwrap();
        let credentials = config.credentials();
        assert_eq!(credentials.len(), 3);

        let echo = "Incorrect API key provided: sk-upstream-secret. \
                    The key pp-local-secret is also rejected, \
                    and neither is mgr-secret-password.";
        let scrubbed = redact_credentials(echo, &credentials);
        assert!(!scrubbed.contains("sk-upstream-secret"), "{scrubbed}");
        assert!(!scrubbed.contains("pp-local-secret"), "{scrubbed}");
        assert!(!scrubbed.contains("mgr-secret-password"), "{scrubbed}");
        assert_eq!(scrubbed.matches(REDACTED).count(), 3, "{scrubbed}");
        // The rest of the message survives: the reason must still be readable.
        assert!(
            scrubbed.contains("Incorrect API key provided"),
            "{scrubbed}"
        );

        // Unrelated text is untouched.
        assert_eq!(
            redact_credentials("upstream returned 500", &credentials),
            "upstream returned 500"
        );
    }

    #[test]
    fn test_a_value_too_short_to_be_a_credential_is_left_alone() {
        // Scrubbing a two-character value would rewrite ordinary words and hide
        // the message the operator needs to read.
        let credentials = ["ab"];
        let text = "the about table was locked";
        assert_eq!(redact_credentials(text, &credentials), text);
    }

    /// The credentials in a config must not travel with its `Debug`, because
    /// `ConfigSnapshot` derives `Debug` and anything holding one can be printed.
    #[test]
    fn test_debug_redacts_secrets() {
        let config = crate::config::ConfigLoader::parse_yaml(
            r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-upstream-secret
keys:
  - key: pp-local-secret
    name: test
manager:
  password: mgr-secret-password
"#,
        )
        .unwrap();

        let rendered = format!("{config:?}");
        assert!(
            !rendered.contains("sk-upstream-secret"),
            "upstream.api_key must not render: {rendered}"
        );
        assert!(
            !rendered.contains("pp-local-secret"),
            "keys[].key must not render: {rendered}"
        );
        assert!(
            !rendered.contains("mgr-secret-password"),
            "manager.password must not render: {rendered}"
        );
        assert_eq!(rendered.matches(REDACTED).count(), 3, "{rendered}");
        // Still identifiable: the name is what an operator locates a key by,
        // and the redacted `password` label is what says a manager block is
        // there without saying what it holds.
        assert!(rendered.contains("test"), "{rendered}");
        assert!(rendered.contains("password"), "{rendered}");
    }

    /// The manager credential is matched by exact equality like a key value, and
    /// the empty-password guard keeps a block that failed validation from
    /// silently authenticating as "manager".
    #[test]
    fn test_find_manager_matches_by_exact_password_and_never_an_empty_one() {
        let config = crate::config::ConfigLoader::parse_yaml(
            r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
keys:
  - key: pp-local-secret
    name: test
manager:
  password: mgr-password-exact
"#,
        )
        .unwrap();

        assert!(
            config.find_manager("mgr-password-exact").is_some(),
            "the exact password matches"
        );

        assert!(
            config.find_manager("mgr-password-exact ").is_none(),
            "no trimming"
        );
        assert!(
            config.find_manager("mgr-password-exacT").is_none(),
            "no case folding"
        );
        assert!(config.find_manager("").is_none());
    }

    /// A config without a `manager` block must not authenticate as manager, and
    /// neither must a block whose password is empty — `find_manager` carries a
    /// defensive guard even though validation refuses the latter at load.
    #[test]
    fn test_find_manager_is_none_without_a_manager_block_or_with_an_empty_password() {
        let without_block = crate::config::ConfigLoader::parse_yaml(
            r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
keys:
  - key: pp-local-secret
    name: test
"#,
        )
        .unwrap();
        assert!(without_block.find_manager("anything").is_none());

        let with_empty_password = Config {
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
                allowed_models: Vec::new(),
            }],
            database: DatabaseConfig::default(),
            manager: Some(ManagerConfig {
                password: String::new(),
            }),
        };
        assert!(
            with_empty_password.find_manager("").is_none(),
            "an empty password must never authenticate"
        );
    }

    /// The strict-by-default contract: only an explicitly listed name passes.
    #[test]
    fn test_allows_model_is_strict_by_default() {
        let key = |list: &[&str]| KeyConfig {
            key: "k".to_string(),
            name: "n".to_string(),
            consumer_id: None,
            allowed_models: list.iter().map(|s| s.to_string()).collect(),
        };

        // A declared list admits exactly its members.
        let listed = key(&["gpt-4o", "gpt-4o-mini"]);
        assert!(listed.allows_model("gpt-4o"));
        assert!(listed.allows_model("gpt-4o-mini"));

        // Anything outside the list is refused.
        assert!(!listed.allows_model("gpt-5"));
        assert!(
            !listed.allows_model("GPT-4o"),
            "matching is exact, not case-insensitive"
        );

        // An empty list — the default for a key that omits the field — is the
        // strict case: *no* model is allowed at all, never "everything".
        let empty = key(&[]);
        assert!(!empty.allows_model("gpt-4o"));
        assert!(!empty.allows_model("anything"));
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

    fn upstream_with(base_url: &str) -> UpstreamConfig {
        UpstreamConfig {
            base_url: base_url.to_string(),
            api_key: "upstream-secret".to_string(),
            timeout_secs: 120,
            connect_timeout_secs: 10,
        }
    }

    #[test]
    fn test_a_base_url_without_credentials_is_rendered_verbatim() {
        let upstream = upstream_with("https://api.openai.com/v1");
        assert_eq!(upstream.redacted_base_url(), "https://api.openai.com/v1");
    }

    #[test]
    fn test_credentials_embedded_in_the_base_url_never_reach_a_log_line() {
        // Both halves can be the secret — some providers carry the key as the
        // username — so neither half may survive.
        for (base_url, secrets, host) in [
            (
                "https://user:password@api.example.com/v1",
                &["user", "password"][..],
                "api.example.com/v1",
            ),
            (
                "https://sk-the-secret@api.example.com",
                &["sk-the-secret"][..],
                "api.example.com",
            ),
            ("http://user@host:8080", &["user"][..], "host:8080"),
        ] {
            let redacted = upstream_with(base_url).redacted_base_url();
            assert!(redacted.contains("<credentials>@"), "{redacted}");
            for secret in secrets {
                assert!(
                    !redacted.contains(secret),
                    "{secret:?} survived: {redacted}"
                );
            }
            // The host and path still identify which upstream this is.
            assert!(redacted.contains(host), "{redacted}");
            assert!(redacted.starts_with("http"), "{redacted}");
        }

        // `Debug` is the other way a snapshot reaches a log, so it must redact
        // too — it already redacts the api_key, and the URL is a credential
        // carrier for exactly the same reason.
        let rendered = format!(
            "{:?}",
            upstream_with("https://user:password@api.example.com")
        );
        assert!(!rendered.contains("password"), "{rendered}");
        assert!(!rendered.contains("upstream-secret"), "{rendered}");
        assert!(rendered.contains("<credentials>@"), "{rendered}");
    }

    #[test]
    fn test_redaction_does_not_confuse_an_at_sign_in_a_path_or_query() {
        // A human-readable `@` after the authority is not userinfo, and must
        // not be truncated as if it were.
        let upstream = upstream_with("https://api.example.com/v1/models@latest?x=a@b");
        assert_eq!(
            upstream.redacted_base_url(),
            "https://api.example.com/v1/models@latest?x=a@b"
        );
    }
}
