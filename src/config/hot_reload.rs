//! Hot-reload mechanism with atomic config swaps

use crate::config::{Config, ConfigLoader};
use parking_lot::RwLock;
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
fn report_changes(old: &Config, new: &Config) {
    if old.upstream.base_url != new.upstream.base_url {
        info!(
            old = %old.upstream.base_url,
            new = %new.upstream.base_url,
            "upstream base_url changed"
        );
    }
    if old.upstream.api_key != new.upstream.api_key {
        info!("upstream api_key changed");
    }
    if old.upstream.timeout_secs != new.upstream.timeout_secs
        || old.upstream.connect_timeout_secs != new.upstream.connect_timeout_secs
    {
        info!(
            timeout_secs = new.upstream.timeout_secs,
            connect_timeout_secs = new.upstream.connect_timeout_secs,
            "upstream timeouts changed"
        );
    }

    if old.keys.len() != new.keys.len() {
        info!(
            old = old.keys.len(),
            new = new.keys.len(),
            "key set changed"
        );
    } else {
        let changed = old
            .keys
            .iter()
            .zip(new.keys.iter())
            .filter(|(a, b)| a.key != b.key || a.consumer_id() != b.consumer_id())
            .count();
        if changed > 0 {
            info!(count = changed, "credential or consumer identity changed");
        }
    }

    if old.server.max_body_size != new.server.max_body_size
        || old.server.shutdown_grace_secs != new.server.shutdown_grace_secs
    {
        // Worth calling out: these are read when the listener is built or when a
        // request arrives, so the new value takes effect without a restart only
        // where the reading path re-reads the snapshot.
        info!("server limits changed");
    }
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
}
