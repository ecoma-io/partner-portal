//! Snapshot refresh.
//!
//! # The problem
//!
//! Authentication reads an in-memory [`ApiKeySnapshot`], and the snapshot is
//! rebuilt only when something rebuilds it. An administrator who revokes a key
//! through *this* process is seen immediately — [`ApiKeyStore::mutate`] refreshes
//! before returning. A second instance running against the same database sees
//! nothing at all, and would keep authenticating a revoked key until it was
//! restarted. During a rolling update both instances exist at once, so that is
//! the normal case, not an edge one.
//!
//! # Why the reload is periodic and unconditional
//!
//! The obvious design is change detection: poll `PRAGMA data_version`, which
//! moves when **another** connection commits a write, and reload when it moves.
//! That design is wrong here, and the reason is expiry. A key's lifetime runs
//! out with no write anywhere — no event, no row change, no moved version. The
//! expiry test itself is in `load_active`'s SQL, so a key that has gone stale
//! simply stops being selected, and nothing announces that it happened. Only a
//! periodic reload can drop it, and a reload that waits for a change signal is a
//! reload that never comes for the one transition an operator most needs to
//! happen on time.
//!
//! So every tick reloads. What that costs is one indexed `SELECT` over
//! `status = 'active'`, on a table measured in dozens of rows, through a fresh
//! read-only connection from [`LedgerPool::read`] — the same connection the
//! dashboard opens per query. The interval is the propagation bound in both
//! directions: the delay before a sibling's revoke reaches this instance, and
//! the delay before an expiry does. It is configurable because the trade-off is
//! deployment-specific — a single instance has nothing to wait for, and a test
//! needs a smaller number.
//!
//! # What a failed reload does
//!
//! It keeps the last good snapshot and warns. Swapping in an empty snapshot would
//! take every partner offline because a poll failed once; a key expiring during
//! that window stays honoured for one more interval, which is the same bound an
//! ordinary poll already accepts. There is deliberately no path where a poll
//! failure starts serving a key set nobody chose — that would be a fallback hiding
//! a failure, and a credential that outlives its revocation is the failure this
//! module exists to prevent.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use crate::apikeys::store::ApiKeyStore;

/// Lower bound on the poll interval.
///
/// A configured zero would not mean "as fast as possible", it would mean a task
/// that reloads in a tight loop and competes with the metering writer for the
/// single write lock. The floor exists so a zero is a slow instance rather than
/// a busy one.
pub const MIN_REFRESH_INTERVAL_MS: u64 = 1;

/// Default poll interval, in milliseconds.
///
/// One second is the bound this instance accepts between a state change and its
/// effect on this instance's authentication — a sibling's commit, or a key's own
/// expiry — and it is documented in ADR 0014.
pub const DEFAULT_REFRESH_INTERVAL_MS: u64 = 1_000;

/// Rebuilds the key snapshot from the database on an interval.
pub struct ApiKeyRefresher {
    store: Arc<ApiKeyStore>,
    interval: Duration,
    /// Keys loaded by [`ApiKeyRefresher::new`], for the start-up log line.
    initial_keys: usize,
}

impl ApiKeyRefresher {
    /// Load the first snapshot and prepare the poller.
    ///
    /// The initial load is here rather than left to the poller's first tick for
    /// one reason: the instance must fail to start if it cannot read its own key
    /// set. A silent empty key set is a total outage that looks like a
    /// configuration mistake, and every 401 it produces would point at the wrong
    /// file.
    ///
    /// `db_path` is taken as well as the store so the start-up failure can name
    /// the file it could not read.
    pub fn new(
        db_path: &Path,
        store: Arc<ApiKeyStore>,
        interval_ms: u64,
    ) -> Result<Self, StartupError> {
        let initial_keys = store.refresh().map_err(|source| StartupError::KeySet {
            path: db_path.to_path_buf(),
            source,
        })?;

        Ok(Self {
            store,
            interval: Duration::from_millis(interval_ms.max(MIN_REFRESH_INTERVAL_MS)),
            initial_keys,
        })
    }

    /// How many keys were loaded at start-up, for the start-up log line.
    ///
    /// Taken from the load above rather than `store.active_count()`: that reads
    /// the *current* snapshot, which a refresh since start-up would already have
    /// changed, and a log line about start-up should not report a later state.
    pub fn initial_key_count(&self) -> usize {
        self.initial_keys
    }

    /// Spawn the polling task. Returns immediately.
    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let interval = this.interval;

        tokio::spawn(async move {
            // The first reload waits one interval, because the constructor
            // already performed one. A key committed in between is picked up by
            // this tick, and a key committed *before* the constructor's load is
            // in the snapshot already — so there is no window in which a
            // committed key is invisible for longer than one interval.
            loop {
                tokio::time::sleep(interval).await;
                this.reload().await;
            }
        });
    }

    /// Rebuild the snapshot from the database.
    ///
    /// Runs on the blocking pool: it opens a SQLite connection, and a saturated
    /// request path should not be paying for that on its own executor thread.
    ///
    /// Public because the tests drive it directly — a poller whose only entry
    /// point is a spawned task cannot be asserted on without sleeping.
    pub async fn reload(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || this.store.refresh()).await;

        match outcome {
            Ok(Ok(count)) => debug!(keys = count, "api key snapshot reloaded"),
            Ok(Err(e)) => {
                warn!(error = %e, "api key snapshot reload failed; keeping the last good key set")
            }
            // The task cannot panic — `refresh` is ordinary fallible code — but
            // treating a JoinError as "nothing to do" would be a silent gap in
            // coverage, so it is reported.
            Err(e) => warn!(error = %e, "api key snapshot reload task did not finish"),
        }
    }

    /// The interval the poller runs at, after the floor was applied. Exposed so
    /// a test can assert the floor without sleeping for it.
    pub fn interval(&self) -> Duration {
        self.interval
    }
}

/// Why this instance cannot start with a key refresher.
///
/// Fatal on purpose, and never downgraded to an empty snapshot: an instance
/// that cannot say which keys it accepts would answer every request with a 401
/// that points at the configuration file instead of at the database.
#[derive(Debug)]
pub enum StartupError {
    /// The key set could not be read from the database.
    KeySet {
        path: std::path::PathBuf,
        source: crate::apikeys::store::ApiKeyError,
    },
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartupError::KeySet { path, source } => write!(
                f,
                "could not load the partner api key set from {}: {source}",
                path.display()
            ),
        }
    }
}

impl std::error::Error for StartupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StartupError::KeySet { source, .. } => Some(source),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apikeys::store::ApiKeyError;
    use crate::ledger::LedgerPool;
    use std::sync::Arc;

    const SECRET: &[u8] = b"a-refresher-test-secret-of-32-bytes!!";
    /// Short enough to keep the test fast, long enough that the `max(1)` floor
    /// does not fire.
    const INTERVAL_MS: u64 = 20;

    struct Fixture {
        // The temp dir owns the database file; dropping it would delete it.
        _dir: tempfile::TempDir,
        refresher: Arc<ApiKeyRefresher>,
        /// The instance under test: its own pool, its own snapshot.
        store: Arc<ApiKeyStore>,
        /// A second pool on the same file, standing in for a sibling instance.
        sibling_pool: Arc<LedgerPool>,
    }

    impl Fixture {
        fn new() -> Self {
            let dir = tempfile::TempDir::new().unwrap();
            let path = dir.path().join("ledger.db");

            let store = Arc::new(ApiKeyStore::new(
                Arc::new(LedgerPool::new(path.clone()).unwrap()),
                SECRET.to_vec(),
            ));
            store.refresh().unwrap();

            let refresher =
                Arc::new(ApiKeyRefresher::new(&path, Arc::clone(&store), INTERVAL_MS).unwrap());
            // A distinct pool: `LedgerPool::new` opens its own connection, which
            // is what makes this a second connection rather than the same one
            // talking to itself.
            let sibling_pool = Arc::new(LedgerPool::new(path.clone()).unwrap());

            Self {
                _dir: dir,
                refresher,
                store,
                sibling_pool,
            }
        }

        /// Insert a key through the sibling's writer connection — the write a
        /// real second process would make.
        fn sibling_creates(&self, name: &str, consumer: &str) -> String {
            let store = ApiKeyStore::new(Arc::clone(&self.sibling_pool), SECRET.to_vec());
            let (_, plaintext) = store
                .create(name, consumer, vec!["gpt-4o".to_string()], None)
                .expect("the sibling must be able to issue a key");
            plaintext
        }

        /// Wait for `predicate`, or fail with what was true at the deadline.
        async fn wait_for(&self, what: &str, predicate: impl Fn() -> bool) {
            let deadline = std::time::Instant::now() + Duration::from_secs(5);
            while std::time::Instant::now() < deadline {
                if predicate() {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            panic!(
                "timed out waiting for {what}; the instance still holds {} key(s)",
                self.store.active_count()
            );
        }
    }

    /// The reload happens on the timer rather than on a change signal, and this
    /// is why: an expiry produces no write at all.
    ///
    /// A change-driven poller passes every other test in this file and fails
    /// this one, which is the point of having it.
    #[tokio::test]
    async fn test_a_stored_expiry_stops_authenticating_on_its_own() {
        let f = Fixture::new();
        let (_row, plaintext) = f
            .store
            .create(
                "brief",
                "acme",
                vec![],
                Some(time::OffsetDateTime::now_utc() + time::Duration::milliseconds(50)),
            )
            .unwrap();
        assert!(f.store.authenticate(&plaintext).is_some());

        f.refresher.start();
        f.wait_for("the key to expire and be dropped", || {
            f.store.authenticate(&plaintext).is_none()
        })
        .await;
    }

    #[tokio::test]
    async fn test_a_key_created_by_a_sibling_starts_authenticating_without_a_restart() {
        let f = Fixture::new();
        assert!(f.store.authenticate("pp_anything").is_none());

        let plaintext = f.sibling_creates("from-a-sibling", "acme");
        assert!(
            f.store.authenticate(&plaintext).is_none(),
            "the sibling's key is not in this instance's snapshot yet"
        );

        f.refresher.start();
        f.wait_for("the sibling's key to reach this instance", || {
            f.store.authenticate(&plaintext).is_some()
        })
        .await;

        let auth = f
            .store
            .authenticate(&plaintext)
            .expect("now it authenticates");
        assert_eq!(auth.consumer_id, "acme");
        assert_eq!(auth.name, "from-a-sibling");
    }

    #[tokio::test]
    async fn test_a_key_revoked_by_a_sibling_stops_authenticating_within_one_interval() {
        let f = Fixture::new();
        let plaintext = f.sibling_creates("short-lived", "acme");
        f.refresher.start();
        f.wait_for("the initial key to propagate", || {
            f.store.authenticate(&plaintext).is_some()
        })
        .await;

        // Revoke through the sibling, so the commit lands on a connection this
        // instance's own mutation path never touched.
        let sibling = ApiKeyStore::new(Arc::clone(&f.sibling_pool), SECRET.to_vec());
        let id = sibling.list().unwrap()[0].id;
        sibling.revoke(id).unwrap();

        f.wait_for("the revocation to propagate", || {
            f.store.authenticate(&plaintext).is_none()
        })
        .await;
    }

    #[tokio::test]
    async fn test_a_local_mutation_is_visible_without_waiting_for_the_poll() {
        // The refresher is what makes sibling writes visible; a write through
        // *this* instance's own store is refreshed inside `mutate` before it
        // returns, so the next request already sees it.
        let f = Fixture::new();
        let (row, plaintext) = f.store.create("local", "acme", vec![], None).unwrap();

        assert!(f.store.authenticate(&plaintext).is_some());
        f.store.revoke(row.id).unwrap();
        assert!(
            f.store.authenticate(&plaintext).is_none(),
            "a local revoke must not wait for a poll interval"
        );
    }

    #[tokio::test]
    async fn test_a_failed_reload_keeps_the_last_good_snapshot() {
        // A poll that fails must not empty the key set: that would take every
        // partner offline because of one bad read. The snapshot is the last
        // state that was known good, and the next poll corrects it.
        let f = Fixture::new();
        let plaintext = f.sibling_creates("survivor", "acme");
        f.refresher.start();
        f.wait_for("the initial key to propagate", || {
            f.store.authenticate(&plaintext).is_some()
        })
        .await;

        // Rename the table out from under the snapshot, which is exactly what
        // `tests/fault/writer_failure.rs` does to the ledger.
        f.sibling_pool
            .write(|conn| {
                conn.execute_batch("ALTER TABLE api_keys RENAME TO api_keys_offline")?;
                Ok(())
            })
            .unwrap();

        f.refresher.reload().await;
        assert!(
            f.store.authenticate(&plaintext).is_some(),
            "a reload that fails must leave the last good key set in place"
        );

        // And the recovery is not a latch: restoring the table lets the poller
        // catch up rather than leaving the process stuck on a stale set.
        f.sibling_pool
            .write(|conn| {
                conn.execute_batch("ALTER TABLE api_keys_offline RENAME TO api_keys")?;
                Ok(())
            })
            .unwrap();
        let late = f.sibling_creates("issued-while-broken", "acme");

        f.refresher.reload().await;
        assert!(
            f.store.authenticate(&late).is_some(),
            "a reload after the failure must pick the database back up"
        );
        assert!(f.store.authenticate(&plaintext).is_some());
    }

    #[test]
    fn test_construction_loads_the_key_set_and_refuses_to_start_without_one() {
        // Startup's contract, stated as a test: an instance that cannot read its
        // own keys does not run. Starting empty would be a total outage that
        // looks like a configuration mistake, and every 401 would point at the
        // config file instead of at the database.
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ledger.db");
        let sibling = Arc::new(LedgerPool::new(path.clone()).unwrap());
        ApiKeyStore::new(Arc::clone(&sibling), SECRET.to_vec())
            .create("existing", "acme", vec!["gpt-4o".to_string()], None)
            .unwrap();

        let store = Arc::new(ApiKeyStore::new(
            Arc::new(LedgerPool::new(path.clone()).unwrap()),
            SECRET.to_vec(),
        ));
        let refresher = ApiKeyRefresher::new(&path, store, INTERVAL_MS).unwrap();
        assert_eq!(refresher.initial_key_count(), 1);

        // Now break the table and prove the same construction fails loudly
        // rather than producing an instance that authenticates nobody. The drop
        // has to come *after* the store is built: `LedgerPool::new` applies
        // `schema.sql`, so opening a pool on a table-less file would recreate
        // the table and the failure would never be exercised.
        let broken = Arc::new(ApiKeyStore::new(
            Arc::new(LedgerPool::new(path.clone()).unwrap()),
            SECRET.to_vec(),
        ));
        sibling
            .write(|conn| {
                conn.execute_batch("DROP TABLE api_keys")?;
                Ok(())
            })
            .unwrap();

        match ApiKeyRefresher::new(&path, broken, INTERVAL_MS) {
            Err(StartupError::KeySet {
                source: ApiKeyError::Database(_),
                ..
            }) => {}
            Err(other) => panic!("expected a key-set failure, got {other}"),
            Ok(_) => panic!(
                "an instance that cannot read its keys must fail to start, \
                 not run with an empty key set"
            ),
        }
    }

    #[test]
    fn test_a_zero_interval_is_clamped_to_the_floor() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("ledger.db");
        let store = Arc::new(ApiKeyStore::new(
            Arc::new(LedgerPool::new(path.clone()).unwrap()),
            SECRET.to_vec(),
        ));

        let refresher = ApiKeyRefresher::new(&path, store, 0).unwrap();
        assert_eq!(
            refresher.interval(),
            Duration::from_millis(MIN_REFRESH_INTERVAL_MS),
            "a zero interval must not become a spinning task"
        );
    }
}
