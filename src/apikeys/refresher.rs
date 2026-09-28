//! Multi-instance snapshot refresh.
//!
//! # The problem
//!
//! Authentication reads an in-memory [`ApiKeySnapshot`], and the snapshot is
//! rebuilt only when something tells the instance the key set moved. An
//! administrator who revokes a key through *this* process is seen immediately —
//! [`ApiKeyStore::mutate`] refreshes before returning. A second instance running
//! against the same database sees nothing at all, and would keep authenticating a
//! revoked key until it was restarted. During a rolling update both instances
//! exist at once, so that is the normal case, not an edge one.
//!
//! # The signal
//!
//! `PRAGMA data_version` changes when **another** connection commits a write,
//! which makes it an exact change signal that works across processes. The
//! consequence that dictates the shape of this file: the poller needs its *own*
//! connection, because `data_version` deliberately ignores writes made by the
//! connection that performs the query. Sharing the metering writer's connection
//! would mean a local write never moved its own version — and worse, it would put
//! a pragma poll on the lock every request's write contends for.
//!
//! The connection is opened `query_only`, so it cannot write even by accident,
//! and it is not the pool's: [`LedgerPool::reader`] opens and closes a connection
//! per call, which is right for a dashboard query and wrong for a poll that runs
//! once a second for the life of the process.
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
//!
//! # The interval reloads on, and the two things that forces
//!
//! `data_version` moves **only when another connection commits a write**. It is
//! a change *signal*, and two states that must not wait for one both require an
//! unconditional reload:
//!
//! * **Expiry.** A key's lifetime runs out with no write anywhere — no event, no
//!   row change. The expiry test itself is in `load_active`'s SQL, so a key that
//!   has gone stale simply stops being selected; nothing announces that it
//!   happened. Only a periodic reload can drop it.
//! * **The window before the first poll.** A key committed between the store's
//!   initial load and the poller's first read moves the version *before* the
//!   baseline is taken, so it is never seen as a change. Starting with a null
//!   baseline and reloading on the first tick closes that window rather than
//!   leaving it to the first write after startup to discover.
//!
//! So every tick that reports a change is a full reload, and so is the first
//! one. That costs one indexed read of a table that is a few dozen rows at most —
//! which is what makes the interval configurable rather than a property to
//! engineer around.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use rusqlite::Connection;
use tracing::{debug, warn};

use crate::apikeys::store::ApiKeyStore;

/// Lower bound on the poll interval, mirroring [`crate::dashboard::sse`].
///
/// A configured zero would not mean "as fast as possible", it would mean a task
/// that spins on the pragma and competes with the metering writer for CPU.
pub const MIN_REFRESH_INTERVAL_MS: u64 = 1;

/// Default poll interval, in milliseconds.
///
/// One second is the bound this instance accepts between a state change and its
/// effect on this instance's authentication — a sibling's commit, or a key's own
/// expiry — and it is documented in ADR 0014. It is configurable because the
/// trade-off is deployment-specific: a single instance has nothing to wait for,
/// and a test needs a smaller number.
pub const DEFAULT_REFRESH_INTERVAL_MS: u64 = 1_000;

/// Polls SQLite's `data_version` on a dedicated connection and reloads the key
/// snapshot when another connection commits.
pub struct ApiKeyRefresher {
    poll_conn: Arc<Mutex<Connection>>,
    store: Arc<ApiKeyStore>,
    interval: Duration,
    /// Keys loaded by [`ApiKeyRefresher::new`], for the start-up log line.
    initial_keys: usize,
}

impl ApiKeyRefresher {
    /// Open the poll connection and load the first snapshot.
    ///
    /// The load is here rather than left to the poller's first tick for two
    /// reasons: the instance must fail to start if it cannot read its own key
    /// set (a silent empty key set is a total outage that looks like a config
    /// mistake), and a key committed by a sibling *during* this call is picked
    /// up by the very next poll instead of waiting for the write after it.
    ///
    /// Separate from [`ApiKeyStore::new`] on purpose: the store is constructed in
    /// unit tests that never want a second connection, and the refresher is
    /// constructed only by the process that has a database path.
    pub fn new(
        db_path: &Path,
        store: Arc<ApiKeyStore>,
        interval_ms: u64,
    ) -> Result<Self, StartupError> {
        let poll_conn = Connection::open(db_path)?;

        // WAL so the poll never blocks the writer, `query_only` so this
        // connection cannot write even by mistake, and the same busy timeout the
        // pool uses — a poll that gives up early would report a spurious change
        // and a reload that fails for the same reason.
        poll_conn.execute_batch(
            "PRAGMA journal_mode = WAL;
             PRAGMA query_only = ON;
             PRAGMA busy_timeout = 5000;",
        )?;

        // Startup's contract: this instance knows which keys it will accept, or
        // it does not run. The caller turns this error into a start-up failure.
        let initial_keys = store.refresh()?;

        Ok(Self {
            poll_conn: Arc::new(Mutex::new(poll_conn)),
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

    /// Spawn the polling task.
    pub fn start(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let interval = this.interval;

        tokio::spawn(async move {
            loop {
                tokio::time::sleep(interval).await;

                // Read the version *before* the reload, not after: the snapshot
                // is then, at worst, as stale as the version that was sampled
                // before it was built. Reading it afterwards would race a commit
                // that lands mid-reload — the new key would be in the snapshot
                // but its version would look already-seen, and the commit that
                // arrived in between would be lost until the next one.
                let _version_sampled_before = this.data_version();
                this.reload_if_changed().await;
            }
        });
    }

    /// Reload the snapshot because the database moved.
    ///
    /// The reload runs on the blocking pool: it opens a SQLite connection, and
    /// the metering writer holds the single write lock and a saturated request
    /// path should not be paying for a pragma read on its own executor thread.
    pub async fn reload_if_changed(self: &Arc<Self>) {
        let this = Arc::clone(self);
        let outcome = tokio::task::spawn_blocking(move || this.store.refresh()).await;

        match outcome {
            Ok(Ok(count)) => debug!(keys = count, "api key snapshot reloaded"),
            // The task cannot panic — `refresh` is ordinary fallible code — but
            // treating a JoinError as "unchanged" would be a silent gap in
            // coverage, so it is reported.
            Ok(Err(e)) => {
                warn!(error = %e, "api key snapshot reload failed; keeping the last good key set")
            }
            Err(e) => warn!(error = %e, "api key snapshot reload task did not finish"),
        }
    }

    /// Current `data_version` as this connection sees it.
    pub fn data_version(&self) -> Option<i64> {
        get_data_version(&self.poll_conn.lock())
    }
}

fn get_data_version(conn: &Connection) -> Option<i64> {
    match conn.query_row("PRAGMA data_version", [], |row| row.get(0)) {
        Ok(v) => Some(v),
        Err(e) => {
            // `None` rather than a constant: returning a sentinel would risk a
            // stale version comparing equal to a later real one and silently
            // skipping a refresh. A caller that cannot read the pragma must
            // reload — that is the safe direction to be wrong in.
            warn!(error = %e, "could not read PRAGMA data_version");
            None
        }
    }
}

/// Why this instance cannot start with a key refresher.
///
/// Distinct from [`crate::apikeys::store::ApiKeyError`] so a caller can tell
/// "the key store failed" from "the thing that watches it could not start", and
/// so a start-up failure is never reported as a routine store error. Both
/// variants are fatal: neither leaves an instance that could serve correctly.
#[derive(Debug)]
pub enum StartupError {
    /// The poll connection could not be opened or configured.
    Connection(rusqlite::Error),
    /// The key set could not be read.
    ///
    /// Fatal on purpose, and never downgraded to an empty snapshot: an instance
    /// that cannot say which keys it accepts would answer every request with a
    /// 401 that points at the configuration file instead of at the database.
    KeySet(crate::apikeys::store::ApiKeyError),
}

impl std::fmt::Display for StartupError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StartupError::Connection(e) => {
                write!(f, "could not open the api key refresh connection: {e}")
            }
            StartupError::KeySet(e) => {
                write!(f, "could not load the partner api key set: {e}")
            }
        }
    }
}

impl std::error::Error for StartupError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            StartupError::Connection(e) => Some(e),
            StartupError::KeySet(e) => Some(e),
        }
    }
}

impl From<rusqlite::Error> for StartupError {
    fn from(e: rusqlite::Error) -> Self {
        StartupError::Connection(e)
    }
}

impl From<crate::apikeys::store::ApiKeyError> for StartupError {
    fn from(e: crate::apikeys::store::ApiKeyError) -> Self {
        StartupError::KeySet(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::LedgerPool;

    const SECRET: &[u8] = b"a-refresher-test-secret-of-32-bytes!!";
    /// Short enough to keep the test fast, long enough that the `max(1)` floor
    /// does not fire.
    const INTERVAL_MS: u64 = 20;

    struct Fixture {
        // The temp dir owns the database file; dropping it would delete it.
        _dir: tempfile::TempDir,
        refresher: Arc<ApiKeyRefresher>,
        /// The instance under test: its own pool, its own poll connection.
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
                store,
                refresher,
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

    #[test]
    fn test_the_poll_connection_sees_another_connections_write() {
        let f = Fixture::new();
        let before = f.refresher.data_version().expect("pragma must read");

        f.sibling_creates("from-a-sibling", "acme");

        let after = f.refresher.data_version().expect("pragma must read");
        assert_ne!(
            before, after,
            "a commit by another connection must move data_version, or a \
             sibling's revoke would never reach this instance"
        );
    }

    #[test]
    fn test_the_poll_connection_is_query_only() {
        // The refresher is a reader by construction. A write attempt on its
        // connection must fail, because a refresher that could write would be
        // able to commit — and a commit would not move its own data_version, so
        // it would never see its own effect and would reload forever.
        let f = Fixture::new();
        let result = f
            .refresher
            .poll_conn
            .lock()
            .execute("UPDATE api_keys SET name = 'tampered'", []);
        assert!(result.is_err(), "the poll connection must not be writable");
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

        // Revoke through the sibling, so the commit lands on a connection the
        // refresher is not watching.
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
    async fn test_a_stored_expiry_stops_authenticating_on_its_own() {
        // Expiry is filtered at load time, so the only thing that ends a key's
        // life on schedule is the reload. This is the test that would go red if
        // the refresher were removed rather than merely broken.
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
        f.refresher
            .poll_conn
            .lock()
            .execute_batch("DROP TABLE api_keys")
            .ok();

        f.refresher.reload_if_changed().await;
        assert!(
            f.store.authenticate(&plaintext).is_some(),
            "a reload that fails must leave the last good key set in place"
        );
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
            Err(StartupError::KeySet(_)) => {}
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
            refresher.interval,
            Duration::from_millis(MIN_REFRESH_INTERVAL_MS),
            "a zero interval must not become a spinning task"
        );
    }
}
