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

        tokio::spawn(async move {
            let mut last_hash = current.read().hash.clone();
            let mut last_mtime: Option<std::time::SystemTime> = Self::get_mtime(&path);

            info!(path = %path.display(), "Started config hot-reload watcher");

            loop {
                tokio::select! {
                    _ = stop_rx.changed() => {
                        info!("Config hot-reload watcher stopped");
                        break;
                    }
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // Check modification time first (cheaper than reading file)
                        if let Some(mtime) = Self::get_mtime(&path) {
                            if last_mtime == Some(mtime) {
                                continue;
                            }
                            last_mtime = Some(mtime);
                        }

                        // Try to load new config
                        match ConfigLoader::from_file(&path) {
                            Ok(new_config) => {
                                let new_hash = new_config.hash();
                                if new_hash != last_hash {
                                    info!(
                                        old_hash = %last_hash,
                                        new_hash = %new_hash,
                                        "Configuration changed, applying"
                                    );

                                    let snapshot = ConfigSnapshot::new(new_config);
                                    *current.write() = snapshot;
                                    last_hash = new_hash;
                                }
                            }
                            Err(e) => {
                                warn!(error = %e, "Failed to reload config, keeping current");
                            }
                        }
                    }
                }
            }
        });
    }

    /// Stop the hot-reloader
    pub fn stop(&self) {
        let _ = self.stop_tx.send(());
    }

    /// Get current configuration snapshot
    pub fn current(&self) -> ConfigSnapshot {
        self.current.read().clone()
    }

    /// Trigger an immediate reload check
    pub fn trigger_reload(&self) {
        let _ = self.reload_tx.send(());
    }

    fn get_mtime(path: &PathBuf) -> Option<std::time::SystemTime> {
        std::fs::metadata(path).ok()?.modified().ok()
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
