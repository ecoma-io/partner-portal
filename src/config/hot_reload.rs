//! Hot-reload mechanism with atomic config swaps

use super::types::REDACTED;
use crate::config::{Config, ConfigLoader};
use parking_lot::RwLock;
use std::fmt::Display;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::watch;
use tracing::{info, warn};

/// Snapshot of configuration with its hash
#[derive(Debug, Clone)]
pub struct ConfigSnapshot {
    pub config: Arc<Config>,
    pub hash: String,
}

impl ConfigSnapshot {
    pub fn new(config: Config) -> Self {
        let hash = config.hash();
        Self {
            config: Arc::new(config),
            hash,
        }
    }
}

/// Hot-reloader for configuration
pub struct HotReloader {
    path: PathBuf,
    current: Arc<RwLock<ConfigSnapshot>>,
    stop_tx: watch::Sender<()>,
    reload_tx: watch::Sender<()>,
}

impl HotReloader {
    /// Create a new hot-reloader
    pub fn new(path: PathBuf, initial_config: Config) -> Self {
        let snapshot = ConfigSnapshot::new(initial_config);
        let current = Arc::new(RwLock::new(snapshot));
        let (stop_tx, _) = watch::channel(());
        let (reload_tx, _) = watch::channel(());

        Self {
            path,
            current,
            stop_tx,
            reload_tx,
        }
    }

    /// Start the hot-reload background task
    pub fn start(&self) {
        let path = self.path.clone();
        let current = self.current.clone();
        let mut stop_rx = self.stop_tx.subscribe();
        let mut reload_rx = self.reload_tx.subscribe();

        tokio::spawn(async move {
            let mut last_hash = current.read().hash.clone();

            info!(path = %path.display(), "Started config hot-reload watcher");

            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        info!("Config hot-reload watcher stopped");
                        break;
                    }
                    _ = reload_rx.changed() => {}
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {}
                }

                // The file is read and hashed on every tick rather than gated on
                // its mtime. A config file is small, one read per second costs
                // nothing, and mtime is not a reliable change signal: editors
                // that write-then-rename, and filesystems with coarse mtime
                // granularity, both produce changes that leave mtime looking
                // identical. Missing a reload is the failure this watcher exists
                // to prevent, so it does not depend on mtime at all.
                match ConfigLoader::from_file(&path) {
                    Ok(new_config) => {
                        let new_hash = new_config.hash();
                        if new_hash != last_hash {
                            let previous = current.read();
                            let snapshot = ConfigSnapshot::new(new_config);
                            report_changes(&previous.config, &snapshot.config);
                            drop(previous);

                            // The swap is the moment new configuration becomes
                            // visible to in-flight requests, so it happens after
                            // every line describing the change has been emitted.
                            *current.write() = snapshot;
                            info!(
                                old_hash = %short(&last_hash),
                                new_hash = %short(&new_hash),
                                "Configuration reloaded"
                            );
                            last_hash = new_hash;
                        }
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to reload config, keeping current");
                    }
                }
            }
        });
    }

    /// Stop the hot-reloader
    pub fn stop(&self) {
        let _ = self.stop_tx.send(());
    }

    /// The shared configuration handle.
    ///
    /// The server state and the watcher must hold the *same* lock, or a reload
    /// would swap a snapshot nobody reads. Handing out this handle is the
    /// contract that keeps hot reload real.
    pub fn handle(&self) -> Arc<RwLock<ConfigSnapshot>> {
        self.current.clone()
    }

    /// Get current configuration snapshot
    pub fn current(&self) -> ConfigSnapshot {
        self.current.read().clone()
    }

    /// Trigger an immediate reload check
    pub fn trigger_reload(&self) {
        let _ = self.reload_tx.send(());
    }
}

/// Abbreviate a config hash for logs.
fn short(hash: &str) -> &str {
    match hash.char_indices().nth(12) {
        Some((idx, _)) => &hash[..idx],
        None => hash,
    }
}

/// Log which parts of the configuration changed.
///
/// A single "configuration reloaded" line is not enough to operate this: the
/// difference between "I edited a key's display name" and "I repointed the
/// upstream" is the difference between a no-op and every subsequent request
/// changing destination. Secret values are never logged, only whether they moved.
///
/// The two classes below are reported differently and on purpose. A field the
/// running process reads from the snapshot is in force from the next request on.
/// A field that was captured when the process started is *not* in force, no
/// matter what the file now says — and an operator who is not told so will
/// believe a value the process is ignoring, which is the failure this whole
/// path exists to avoid. Those are warnings, one per field. Which class a field
/// belongs to is not a matter of taste: it follows from where the process reads
/// it (src/main.rs, src/proxy/handler.rs, src/ledger/writer.rs).
fn report_changes(old: &Config, new: &Config) {
    report_live_changes(old, new);
    report_restart_required_changes(old, new);
}

/// Fields the running process re-reads from the live snapshot.
fn report_live_changes(old: &Config, new: &Config) {
    if old.upstream.base_url != new.upstream.base_url {
        applied(
            "upstream.base_url",
            &old.upstream.base_url,
            &new.upstream.base_url,
        );
    }
    if old.upstream.api_key != new.upstream.api_key {
        // That the credential moved is the operational fact; its value is not.
        applied("upstream.api_key", REDACTED, REDACTED);
    }
    if old.upstream.timeout_secs != new.upstream.timeout_secs {
        applied(
            "upstream.timeout_secs",
            old.upstream.timeout_secs,
            new.upstream.timeout_secs,
        );
    }

    report_key_changes(old, new);

    // The manager password is read per request from the snapshot, like a key
    // value, so a rotation is live from the next request. That it moved is the
    // operational fact; the value is a credential and is not logged.
    let old_password = old.manager.as_ref().map(|m| m.password.as_str());
    let new_password = new.manager.as_ref().map(|m| m.password.as_str());
    if old_password != new_password {
        applied("manager.password", REDACTED, REDACTED);
    }

    if old.server.max_body_size != new.server.max_body_size {
        // Half live, and saying so is the point: the per-request read follows
        // the snapshot, but the layer that rejects an oversized body at the
        // edge is built from the startup value, so a raise is capped by it.
        warn!(
            field = "server.max_body_size",
            old = old.server.max_body_size,
            new = new.server.max_body_size,
            "partly applied on reload: a smaller limit takes effect now, a larger one is still capped by the body-limit layer built at startup"
        );
    }
}

/// Fields whose value was captured when the process started.
fn report_restart_required_changes(old: &Config, new: &Config) {
    if old.server.graceful_shutdown != new.server.graceful_shutdown {
        needs_restart(
            "server.graceful_shutdown",
            old.server.graceful_shutdown,
            new.server.graceful_shutdown,
        );
    }
    if old.server.shutdown_grace_secs != new.server.shutdown_grace_secs {
        needs_restart(
            "server.shutdown_grace_secs",
            old.server.shutdown_grace_secs,
            new.server.shutdown_grace_secs,
        );
    }
    if old.server.cors_allow_origins != new.server.cors_allow_origins {
        needs_restart(
            "server.cors_allow_origins",
            old.server.cors_allow_origins.join(","),
            new.server.cors_allow_origins.join(","),
        );
    }
    if old.server.sse_poll_interval_ms != new.server.sse_poll_interval_ms {
        needs_restart(
            "server.sse_poll_interval_ms",
            old.server.sse_poll_interval_ms,
            new.server.sse_poll_interval_ms,
        );
    }
    if old.upstream.connect_timeout_secs != new.upstream.connect_timeout_secs {
        needs_restart(
            "upstream.connect_timeout_secs",
            old.upstream.connect_timeout_secs,
            new.upstream.connect_timeout_secs,
        );
    }

    if old.database.path != new.database.path {
        needs_restart("database.path", &old.database.path, &new.database.path);
    }
    if old.database.retention_days != new.database.retention_days {
        // The one field whose answer is not a flat no: the dashboard resolves a
        // query window against the live snapshot, while the sweep that deletes
        // rows runs on the interval cloned at startup.
        warn!(
            field = "database.retention_days",
            old = old.database.retention_days,
            new = new.database.retention_days,
            "partly applied on reload: the dashboard's window validation follows the new value, the retention sweep keeps the startup value until restart"
        );
    }
    if old.database.queue_size != new.database.queue_size {
        needs_restart(
            "database.queue_size",
            old.database.queue_size,
            new.database.queue_size,
        );
    }
    if old.database.batch_size != new.database.batch_size {
        needs_restart(
            "database.batch_size",
            old.database.batch_size,
            new.database.batch_size,
        );
    }
    if old.database.batch_timeout_ms != new.database.batch_timeout_ms {
        needs_restart(
            "database.batch_timeout_ms",
            old.database.batch_timeout_ms,
            new.database.batch_timeout_ms,
        );
    }
    if old.database.retention_interval_secs != new.database.retention_interval_secs {
        needs_restart(
            "database.retention_interval_secs",
            old.database.retention_interval_secs,
            new.database.retention_interval_secs,
        );
    }
    if old.database.retention_batch_size != new.database.retention_batch_size {
        needs_restart(
            "database.retention_batch_size",
            old.database.retention_batch_size,
            new.database.retention_batch_size,
        );
    }
}

/// Report the key set by position, never by value.
///
/// A local key value is a credential: which entry changed is operational
/// information, what it changed to is not. The set follows the snapshot per
/// request, so every line here is a live change.
fn report_key_changes(old: &Config, new: &Config) {
    if old.keys.len() != new.keys.len() {
        info!(
            field = "keys",
            old = old.keys.len(),
            new = new.keys.len(),
            "applied on reload: the key set changed size"
        );
    }

    for (index, (a, b)) in old.keys.iter().zip(new.keys.iter()).enumerate() {
        if a.key != b.key {
            info!(
                field = "keys[].key",
                index, "applied on reload: credential replaced (value not logged)"
            );
        }
        if a.name != b.name {
            applied_indexed("keys[].name", index, &a.name, &b.name);
        }
        if a.consumer_id() != b.consumer_id() {
            applied_indexed(
                "keys[].consumer_id",
                index,
                a.consumer_id(),
                b.consumer_id(),
            );
        }
        if a.allowed_models != b.allowed_models {
            // Model names are not credentials — they are the same identifiers
            // the ledger stores in cleartext — so the list itself is logged.
            // Strict-by-default note: an empty list means *no* models, so it
            // renders as "(none)" rather than looking like an opening-up.
            let render = |models: &[String]| match models {
                [] => "(none)".to_string(),
                list => list.join(","),
            };
            applied_indexed(
                "keys[].allowed_models",
                index,
                render(&a.allowed_models),
                render(&b.allowed_models),
            );
        }
    }
}

/// A field the running process reads from the live snapshot.
fn applied(field: &str, old: impl Display, new: impl Display) {
    info!(
        field,
        old = %old,
        new = %new,
        "applied on reload: configuration field changed"
    );
}

/// As [`applied`], for a field belonging to one entry of a list.
fn applied_indexed(field: &str, index: usize, old: impl Display, new: impl Display) {
    info!(
        field,
        index,
        old = %old,
        new = %new,
        "applied on reload: configuration field changed"
    );
}

/// A field whose value was captured at startup: the running process keeps the
/// old one, so the change is reported as *not* in force rather than as a reload.
fn needs_restart(field: &str, old: impl Display, new: impl Display) {
    warn!(
        field,
        old = %old,
        new = %new,
        "configuration field changed but is NOT in force until restart"
    );
}

impl Drop for HotReloader {
    fn drop(&mut self) {
        self.stop();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn make_config_yaml() -> String {
        r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
keys:
  - key: test-key
    name: Test
"#
        .to_string()
    }

    #[tokio::test]
    async fn test_hot_reload_detects_change() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.yaml");

        // Write initial config
        std::fs::write(&config_path, make_config_yaml()).unwrap();

        let config = ConfigLoader::from_file(&config_path).unwrap();
        let reloader = HotReloader::new(config_path.clone(), config);
        reloader.start();

        let initial = reloader.current();
        let initial_hash = initial.hash.clone();

        // Wait a bit for the watcher to start
        tokio::time::sleep(Duration::from_millis(100)).await;

        // Modify config
        let new_yaml = r#"
upstream:
  base_url: https://api.openai.com
  api_key: sk-test-updated
keys:
  - key: test-key
    name: Test
"#;
        std::fs::write(&config_path, new_yaml).unwrap();

        // Wait for reload
        tokio::time::sleep(Duration::from_millis(1500)).await;

        let updated = reloader.current();
        assert_ne!(updated.hash, initial_hash);
        assert_eq!(updated.config.upstream.api_key, "sk-test-updated");

        reloader.stop();
    }

    #[tokio::test]
    async fn test_invalid_config_keeps_current() {
        let temp_dir = TempDir::new().unwrap();
        let config_path = temp_dir.path().join("config.yaml");

        std::fs::write(&config_path, make_config_yaml()).unwrap();

        let config = ConfigLoader::from_file(&config_path).unwrap();
        let reloader = HotReloader::new(config_path.clone(), config);
        reloader.start();

        let initial = reloader.current();
        let initial_hash = initial.hash.clone();

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Write invalid config
        std::fs::write(&config_path, "invalid: yaml: content:").unwrap();

        tokio::time::sleep(Duration::from_millis(1500)).await;

        // Should still have old config
        let current = reloader.current();
        assert_eq!(current.hash, initial_hash);

        reloader.stop();
    }

    /// Collects what a report wrote, so a test can assert on the lines rather
    /// than on a human reading the journal.
    #[derive(Clone, Default)]
    struct LogCapture(std::sync::Arc<parking_lot::Mutex<Vec<u8>>>);

    impl std::io::Write for LogCapture {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0.lock().extend_from_slice(buf);
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }

    /// Run `report_changes` with the log captured, returning everything it wrote.
    fn capture_report(old: &Config, new: &Config) -> String {
        use tracing_subscriber::layer::SubscriberExt;

        let capture = LogCapture::default();
        let subscriber = tracing_subscriber::registry().with(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer({
                    let capture = capture.clone();
                    move || capture.clone()
                }),
        );

        tracing::subscriber::with_default(subscriber, || report_changes(old, new));

        String::from_utf8(capture.0.lock().clone()).unwrap()
    }

    fn config_from(yaml: &str) -> Config {
        ConfigLoader::parse_yaml(yaml).unwrap()
    }

    /// Every field is reported. The bug: a field with no branch at all gets a
    /// config swap and no log line, so an operator editing it reasonably
    /// believes the new value is in force while the process keeps the old one.
    #[test]
    fn test_every_changed_field_is_reported() {
        const BASE: &str = r#"
server:
  graceful_shutdown: true
  shutdown_grace_secs: 5
  max_body_size: 10485760
  cors_allow_origins: []
  sse_poll_interval_ms: 500
upstream:
  base_url: https://api.openai.com
  api_key: sk-test
  timeout_secs: 120
  connect_timeout_secs: 10
keys:
  - key: test-key
    name: Test
    allowed_models:
      - gpt-4o
      - gpt-4o-mini
database:
  path: "./partner-portal.db"
  retention_days: 60
  queue_size: 10000
  batch_size: 100
  batch_timeout_ms: 1000
  retention_interval_secs: 3600
  retention_batch_size: 2000
"#;

        // Each replacement carries its field name, so it can only match the
        // field it names: a bare scalar would also rewrite every other field
        // holding the same digits.
        let changes = [
            (
                "server.graceful_shutdown",
                "graceful_shutdown: true",
                "graceful_shutdown: false",
            ),
            (
                "server.shutdown_grace_secs",
                "shutdown_grace_secs: 5",
                "shutdown_grace_secs: 7",
            ),
            (
                "server.max_body_size",
                "max_body_size: 10485760",
                "max_body_size: 1048576",
            ),
            (
                "server.sse_poll_interval_ms",
                "sse_poll_interval_ms: 500",
                "sse_poll_interval_ms: 1000",
            ),
            (
                "upstream.base_url",
                "base_url: https://api.openai.com",
                "base_url: https://other.example",
            ),
            (
                "upstream.api_key",
                "api_key: sk-test",
                "api_key: sk-changed",
            ),
            (
                "upstream.timeout_secs",
                "timeout_secs: 120",
                "timeout_secs: 30",
            ),
            (
                "upstream.connect_timeout_secs",
                "connect_timeout_secs: 10",
                "connect_timeout_secs: 3",
            ),
            ("keys[].key", "key: test-key", "key: test-key-2"),
            ("keys[].name", "name: Test", "name: Renamed"),
            (
                "database.path",
                "path: \"./partner-portal.db\"",
                "path: \"./other.db\"",
            ),
            (
                "database.retention_days",
                "retention_days: 60",
                "retention_days: 30",
            ),
            (
                "database.queue_size",
                "queue_size: 10000",
                "queue_size: 500",
            ),
            ("database.batch_size", "batch_size: 100", "batch_size: 50"),
            (
                "database.batch_timeout_ms",
                "batch_timeout_ms: 1000",
                "batch_timeout_ms: 2000",
            ),
            (
                "database.retention_interval_secs",
                "retention_interval_secs: 3600",
                "retention_interval_secs: 600",
            ),
            (
                "database.retention_batch_size",
                "retention_batch_size: 2000",
                "retention_batch_size: 100",
            ),
        ];

        // `cors_allow_origins` is covered separately: it is a list, so the
        // change is an insert rather than a scalar substitution.
        let old = config_from(BASE);
        let with_cors = config_from(&BASE.replace(
            "  cors_allow_origins: []",
            "  cors_allow_origins:\n    - \"https://partners.example.com\"",
        ));
        let rendered = capture_report(&old, &with_cors);
        assert!(
            rendered.contains("server.cors_allow_origins"),
            "a change to server.cors_allow_origins must be reported:\n{rendered}"
        );

        // The `manager` block is optional, so adding one (or dropping it, or
        // rotating its password) is an insert-and-remove shape like CORS.
        // Password changes are reported without their value.
        let with_manager = config_from(&format!(
            r#"{BASE}manager:
  password: pp-manager-secret
"#
        ));
        let rendered = capture_report(&old, &with_manager);
        assert!(
            rendered.contains("field=\"manager.password\""),
            "adding a manager password must be reported by name:\n{rendered}"
        );
        assert!(
            !rendered.contains("pp-manager-secret"),
            "the report rendered the manager password:\n{rendered}"
        );

        // `keys[].allowed_models` is a per-key list (ADR 0012) with a
        // hand-written report branch. Model names are not credentials, so the
        // values are logged; an emptied list renders as "(none)", the strict
        // default, not as "*" (which would falsely read as "all models").
        let models_changed = config_from(&BASE.replace(
            "    allowed_models:\n      - gpt-4o\n      - gpt-4o-mini\n",
            "    allowed_models:\n      - gpt-4o\n      - gpt-5\n",
        ));
        assert_ne!(
            old.hash(),
            models_changed.hash(),
            "the allowed_models edit does not change the config, so it tests nothing"
        );
        let rendered = capture_report(&old, &models_changed);
        assert!(
            rendered.contains("field=\"keys[].allowed_models\""),
            "a change to keys[].allowed_models must be reported by name:\n{rendered}"
        );
        assert!(
            rendered.contains("gpt-4o-mini") && rendered.contains("gpt-5"),
            "the report should log the model names (they are not credentials):\n{rendered}"
        );

        // Strict-by-default rendering: an emptied list must read as "(none)",
        // never as a wildcard that reads as "every model allowed".
        let emptied = config_from(&BASE.replace(
            "    allowed_models:\n      - gpt-4o\n      - gpt-4o-mini\n",
            "    allowed_models: []\n",
        ));
        let rendered = capture_report(&old, &emptied);
        assert!(
            rendered.contains("(none)"),
            "an emptied allowed_models must render as (none), not *:\n{rendered}"
        );

        // A separate password rotation is the shape the extraction above
        // rewrites, so report it directly rather than via string surgery on a
        // BASE that has no manager block.
        let base_with_manager = format!(
            r#"{BASE}manager:
  password: pp-manager-old
"#
        );
        let rotated = config_from(
            &base_with_manager.replace("password: pp-manager-old", "password: pp-manager-new"),
        );
        let rendered = capture_report(&old, &rotated);
        assert!(
            rendered.contains("field=\"manager.password\""),
            "a manager password rotation must be reported by name:\n{rendered}"
        );
        assert!(
            !rendered.contains("pp-manager-old") && !rendered.contains("pp-manager-new"),
            "the report rendered a manager password:\n{rendered}"
        );

        for (field, from, to) in changes {
            let new = config_from(&BASE.replace(from, to));
            assert_ne!(
                old.hash(),
                new.hash(),
                "the {field} case does not change the config, so it tests nothing"
            );

            let rendered = capture_report(&old, &new);
            assert!(
                rendered.contains(&format!("field=\"{field}\"")),
                "a change to {field} must be reported by name:\n{rendered}"
            );
        }
    }

    /// A field captured at startup must say so, loudly. Silence, or an "applied"
    /// line, would tell the operator to expect a value the process is ignoring.
    #[test]
    fn test_restart_required_fields_warn_that_they_are_not_in_force() {
        const BASE: &str = "upstream:\n  base_url: https://api.openai.com\n  api_key: sk-test\nkeys:\n  - key: test-key\n    name: Test\ndatabase:\n  batch_size: 100\n";
        let old = config_from(BASE);
        let new = config_from(&BASE.replace("batch_size: 100", "batch_size: 50"));

        let rendered = capture_report(&old, &new);
        assert!(rendered.contains("WARN"), "{rendered}");
        assert!(
            rendered.contains("field=\"database.batch_size\""),
            "the warning must name the field:\n{rendered}"
        );
        assert!(
            rendered.contains("NOT in force until restart"),
            "the warning must say the new value is not in force:\n{rendered}"
        );
    }

    /// The hard contract for this path: the report must never carry a
    /// credential, whether it moved or not.
    #[test]
    fn test_report_never_logs_a_credential() {
        const SECRET_LOCAL: &str = "pp-local-do-not-log";
        const SECRET_UPSTREAM: &str = "sk-upstream-do-not-log";
        const SECRET_MANAGER: &str = "pp-manager-do-not-log";

        let old = config_from(&format!(
            "upstream:\n  base_url: https://api.openai.com\n  api_key: {SECRET_UPSTREAM}\nkeys:\n  - key: {SECRET_LOCAL}\n    name: Test\nmanager:\n  password: {SECRET_MANAGER}\n"
        ));
        let new = config_from(
            "upstream:\n  base_url: https://other.example\n  api_key: sk-rotated-do-not-log\nkeys:\n  - key: pp-rotated-do-not-log\n    name: Test\nmanager:\n  password: pp-manager-rotated-do-not-log\n",
        );

        let rendered = capture_report(&old, &new);
        for secret in [
            SECRET_LOCAL,
            SECRET_UPSTREAM,
            SECRET_MANAGER,
            "sk-rotated-do-not-log",
            "pp-rotated-do-not-log",
            "pp-manager-rotated-do-not-log",
        ] {
            assert!(
                !rendered.contains(secret),
                "the report rendered a credential ({secret}):\n{rendered}"
            );
        }
        // The change itself must still be reported: a silent rotation is worse
        // than a redacted one.
        assert!(
            rendered.contains("field=\"upstream.api_key\""),
            "{rendered}"
        );
        assert!(rendered.contains("field=\"keys[].key\""), "{rendered}");
        assert!(
            rendered.contains("field=\"manager.password\""),
            "{rendered}"
        );
        assert!(rendered.contains(REDACTED), "{rendered}");
    }
}
