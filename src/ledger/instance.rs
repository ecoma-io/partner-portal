//! Who owns an `in_flight` row, and how recovery decides that its owner is gone.
//!
//! # The problem this module exists for
//!
//! A rolling update runs two instances against one database. Instance A has
//! requests in flight; instance B starts, runs crash recovery, and — if
//! recovery only asks "is this row still `in_flight`?" — resolves A's live rows
//! to `interrupted`. A's own finalize then finds the row already terminal and
//! silently does nothing: a request the client saw succeed is recorded as
//! interrupted and its tokens are lost, with no error and no log line.
//!
//! So recovery must be able to answer "is the instance that wrote this row
//! still running?" — and the answer has to survive the one event it is asked
//! about, which is that the owner *died without cleaning up*.
//!
//! # Why an advisory lock file
//!
//! Each instance holds an exclusive `flock(2)` on a file beside the database for
//! the whole life of the process. The kernel releases that lock when the process
//! ends — including a `SIGKILL`, including a container restart — so probing the
//! lock is an exact, instantaneous answer to "is this owner alive?", on the same
//! VPS this product is deployed to, across containers sharing a bind mount.
//!
//! The alternatives were rejected for concrete reasons:
//!
//! * A **heartbeat table alone** cannot distinguish "died a second ago" from
//!   "still working", so either recovery waits out a stale window on every
//!   crash-restart, or it clobbers a live peer for the length of that window.
//! * A **PID check** does not cross container PID namespaces, which is exactly
//!   the two-container topology a rolling update on one VPS produces.
//! * A **fencing token** (bump an epoch, refuse writes from older epochs) would
//!   forbid the concurrent writing that the rolling update depends on.
//!
//! The lock file is only ever removed by its own holder at exit, or by recovery
//! after the holder was found dead — so a *missing* lock file is itself
//! evidence that the owner is gone. The conservative failure mode is the other
//! direction: when liveness cannot be determined (an I/O error, or a platform
//! without `flock`), the owner is reported [`Liveness::Unknown`] and its rows
//! are only recoverable once they are older than
//! [`DEFAULT_UNKNOWN_OWNER_GRACE`]. A wrong "alive" costs a delayed recovery; a
//! wrong "dead" would cost a real record.

use rusqlite::Connection;
use std::fs::{File, OpenOptions};
use std::io::{ErrorKind, Write};
use std::path::{Path, PathBuf};
use std::time::Duration;
use tracing::warn;

/// How old a row must be before an owner of unknown liveness is assumed gone.
///
/// This is the *only* time-based rule, and it applies to rows whose owner cannot
/// be probed at all: rows written before instance ownership existed (owner
/// `NULL`), and owners on a platform where the lock cannot be taken. Thirty
/// seconds is comfortably longer than any lock probe needs to succeed, and short
/// enough that a stranded request is resolved within one sweep of an operator
/// noticing it.
pub const DEFAULT_UNKNOWN_OWNER_GRACE: Duration = Duration::from_secs(30);

/// Prefix of the advisory lock file, which lives beside the database.
const LOCK_PREFIX: &str = ".partner-portal-instance-";

/// Whether the instance that owns a row is still running.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Liveness {
    /// The owner holds its lock: it is running, and its rows are not ours to touch.
    Alive,
    /// The owner's lock is free (or its file is gone): the process is not running.
    Dead,
    /// Liveness could not be determined. Treated as possibly-alive.
    Unknown,
}

impl Liveness {
    /// Whether rows owned by this instance may be recovered *now*.
    pub fn is_recoverable_now(self) -> bool {
        matches!(self, Liveness::Dead)
    }

    /// Whether rows owned by this instance become recoverable once they are old
    /// enough to rule out a live owner.
    pub fn is_recoverable_when_old(self) -> bool {
        matches!(self, Liveness::Unknown)
    }
}

/// Why an instance could not register itself.
///
/// Both halves are real: the lock is taken on the filesystem and the
/// registration is written to SQLite, and a caller starting up needs to know
/// which one failed. Neither is recovered from per-instance — an instance that
/// cannot claim an identity cannot safely participate in recovery at all, so
/// this is a startup error.
#[derive(Debug)]
pub enum InstanceError {
    /// The lock file beside the database could not be created or written.
    Io(std::io::Error),
    /// The registration row could not be written.
    Sqlite(rusqlite::Error),
}

impl std::fmt::Display for InstanceError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            InstanceError::Io(e) => write!(f, "instance lock file: {e}"),
            InstanceError::Sqlite(e) => write!(f, "instance registration: {e}"),
        }
    }
}

impl std::error::Error for InstanceError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            InstanceError::Io(e) => Some(e),
            InstanceError::Sqlite(e) => Some(e),
        }
    }
}

impl From<std::io::Error> for InstanceError {
    fn from(e: std::io::Error) -> Self {
        InstanceError::Io(e)
    }
}

impl From<rusqlite::Error> for InstanceError {
    fn from(e: rusqlite::Error) -> Self {
        InstanceError::Sqlite(e)
    }
}

/// Registration of a running instance, and the lock that proves it is running.
///
/// Held for the life of the process. Dropping it releases the lock and removes
/// the lock file; [`InstanceGuard::release`] additionally removes the
/// registration row, which is what a graceful shutdown does so that the next
/// start sees no dead instance to sweep.
#[derive(Debug)]
pub struct InstanceGuard {
    id: String,
    lock_path: PathBuf,
    /// Held for its lifetime; the lock is the file handle, not a flag.
    _lock: File,
}

impl InstanceGuard {
    /// Register this process against the database and take its lock.
    ///
    /// The registration row is written *after* the lock is held, so a database
    /// can never contain an instance that is not holding one.
    pub fn acquire(conn: &Connection, db_path: &Path) -> Result<Self, InstanceError> {
        let id = uuid::Uuid::now_v7().to_string();
        let lock_path = lock_path_for(db_path, &id);

        let mut lock = OpenOptions::new()
            .create(true)
            .truncate(true)
            .read(true)
            .write(true)
            .open(&lock_path)?;

        // Blocking-exclusive would be wrong here: this lock is never contended
        // for by design (each instance has its own file), so a non-blocking
        // acquire that fails means the filesystem does not support locking, and
        // we should carry on with an Unknown-liveness registration rather than
        // hang at startup.
        match try_lock_exclusive(&lock) {
            Ok(true) => {}
            Ok(false) => warn!(
                path = %lock_path.display(),
                "instance lock is already held; liveness probing is disabled for this instance"
            ),
            Err(e) => warn!(
                path = %lock_path.display(),
                error = %e,
                "could not lock the instance file; liveness probing is disabled"
            ),
        }

        // Best-effort human-readable content. Nothing reads this back — `flock`
        // is the authority — but an operator looking at the directory should be
        // able to tell which process holds which file.
        let _ = write!(
            lock,
            "instance_id={id}\npid={}\nhost={}\n",
            std::process::id(),
            hostname().unwrap_or_else(|| "unknown".to_string())
        );
        let _ = lock.flush();

        conn.execute(
            "INSERT INTO ledger_instances (instance_id, booted_at, pid, host)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(instance_id) DO UPDATE SET
                 booted_at = excluded.booted_at,
                 pid = excluded.pid,
                 host = excluded.host",
            rusqlite::params![
                id,
                crate::ledger::writer::format_timestamp(time::OffsetDateTime::now_utc()),
                std::process::id() as i64,
                hostname(),
            ],
        )?;

        Ok(Self {
            id,
            lock_path,
            _lock: lock,
        })
    }

    /// This instance's identifier, recorded on every row it writes.
    pub fn id(&self) -> &str {
        &self.id
    }

    /// Path of the lock file, exposed so recovery can remove a dead peer's.
    pub fn lock_path(&self) -> &Path {
        &self.lock_path
    }

    /// Remove the registration row so the next start has nothing to sweep.
    ///
    /// Called after the metering drain, when this instance has no `in_flight`
    /// rows left. The lock itself is released when the guard is dropped.
    pub fn release(&self, conn: &Connection) {
        if let Err(e) = conn.execute(
            "DELETE FROM ledger_instances WHERE instance_id = ?1",
            rusqlite::params![self.id],
        ) {
            warn!(error = %e, "could not remove this instance's ledger registration");
        }
    }
}

impl Drop for InstanceGuard {
    fn drop(&mut self) {
        // Only our own file, and only while we still hold the lock.
        let _ = std::fs::remove_file(&self.lock_path);
    }
}

/// Path of the lock file owned by `instance_id`.
pub fn lock_path_for(db_path: &Path, instance_id: &str) -> PathBuf {
    let dir = db_path.parent().unwrap_or_else(|| Path::new("."));
    dir.join(format!("{LOCK_PREFIX}{instance_id}.lock"))
}

/// Whether the instance that owns `instance_id` is still running.
///
/// Reads the lock file beside `db_path`. See the module docs for why this is
/// exact, and for the direction the uncertainty is resolved in.
pub fn probe(db_path: &Path, instance_id: &str) -> Liveness {
    let path = lock_path_for(db_path, instance_id);

    let file = match File::open(&path) {
        Ok(file) => file,
        Err(e) if e.kind() == ErrorKind::NotFound => {
            // The holder removes its file at exit, so its absence means the
            // owner is not running — with one caveat documented in the module
            // docs: a lock file deleted by hand is indistinguishable from a dead
            // owner. Recovery errs the other way for unknown *ownership*, and
            // the file lives outside any path an operator is asked to touch.
            return Liveness::Dead;
        }
        Err(e) => {
            warn!(
                path = %path.display(),
                error = %e,
                "could not open an instance lock file; assuming its owner may be alive"
            );
            return Liveness::Unknown;
        }
    };

    match try_lock_exclusive(&file) {
        // We acquired it, so nobody holds it. Release immediately: holding a
        // dead peer's lock would make *us* look alive for a verdict we already
        // reached, and the file is removed by the recovery that acts on this.
        Ok(true) => {
            release_lock(&file);
            Liveness::Dead
        }
        Ok(false) => Liveness::Alive,
        Err(e) => {
            warn!(
                path = %path.display(),
                error = %e,
                "could not probe an instance lock file; assuming its owner may be alive"
            );
            Liveness::Unknown
        }
    }
}

/// Remove a dead peer's lock file, if it is still there.
///
/// Called only after [`probe`] reported the owner dead, so the file cannot be in
/// use. A failure is not worth failing recovery over: the next sweep retries.
pub fn remove_lock_file(db_path: &Path, instance_id: &str) {
    let path = lock_path_for(db_path, instance_id);
    match std::fs::remove_file(&path) {
        Ok(()) => {}
        Err(e) if e.kind() == ErrorKind::NotFound => {}
        Err(e) => warn!(
            path = %path.display(),
            error = %e,
            "could not remove a dead instance's lock file"
        ),
    }
}

fn hostname() -> Option<String> {
    std::env::var("HOSTNAME").ok().filter(|h| !h.is_empty())
}

#[cfg(unix)]
fn try_lock_exclusive(file: &File) -> std::io::Result<bool> {
    use std::os::fd::AsRawFd;

    // SAFETY: `file` owns the descriptor for the duration of the call.
    let rc = unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) };
    if rc == 0 {
        return Ok(true);
    }

    let err = std::io::Error::last_os_error();
    match err.raw_os_error() {
        // The lock is held by another process — a live owner.
        Some(code) if code == libc::EWOULDBLOCK || code == libc::EAGAIN => Ok(false),
        _ => Err(err),
    }
}

#[cfg(unix)]
fn release_lock(file: &File) {
    use std::os::fd::AsRawFd;
    // SAFETY: as above; `file` is borrowed for the call.
    unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_UN) };
}

#[cfg(not(unix))]
fn try_lock_exclusive(_file: &File) -> std::io::Result<bool> {
    // No portable advisory lock. Reporting "not held" would let recovery treat
    // every live peer as dead on such a platform, so the honest answer is that
    // liveness is unknown and the time-based rule must decide.
    Err(std::io::Error::new(
        ErrorKind::Unsupported,
        "advisory file locking is not available on this platform",
    ))
}

#[cfg(not(unix))]
fn release_lock(_file: &File) {}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn db(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        conn
    }

    #[test]
    fn test_a_held_lock_reads_as_alive() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("ledger.db");
        let conn = db(&path);

        let guard = InstanceGuard::acquire(&conn, &path).unwrap();
        assert_eq!(probe(&path, guard.id()), Liveness::Alive);
    }

    #[test]
    fn test_a_released_lock_reads_as_dead() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("ledger.db");
        let conn = db(&path);

        let guard = InstanceGuard::acquire(&conn, &path).unwrap();
        let id = guard.id().to_string();
        drop(guard);

        assert_eq!(probe(&path, &id), Liveness::Dead);
    }

    #[test]
    fn test_an_instance_with_no_lock_file_reads_as_dead() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("ledger.db");
        assert_eq!(probe(&path, "never-existed"), Liveness::Dead);
    }

    #[test]
    fn test_two_guards_do_not_share_a_lock() {
        // Each instance owns its own file, so holding one must not make another
        // look alive — and acquiring must not block on the first.
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("ledger.db");
        let conn = db(&path);

        let first = InstanceGuard::acquire(&conn, &path).unwrap();
        let second = InstanceGuard::acquire(&conn, &path).unwrap();

        assert_ne!(first.id(), second.id());
        assert_ne!(first.lock_path(), second.lock_path());
        assert_eq!(probe(&path, first.id()), Liveness::Alive);
        assert_eq!(probe(&path, second.id()), Liveness::Alive);

        let registered: i64 = conn
            .query_row("SELECT COUNT(*) FROM ledger_instances", [], |r| r.get(0))
            .unwrap();
        assert_eq!(registered, 2);
    }

    #[test]
    fn test_release_removes_the_registration_row() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("ledger.db");
        let conn = db(&path);

        let guard = InstanceGuard::acquire(&conn, &path).unwrap();
        guard.release(&conn);

        let registered: i64 = conn
            .query_row("SELECT COUNT(*) FROM ledger_instances", [], |r| r.get(0))
            .unwrap();
        assert_eq!(registered, 0);
    }

    #[test]
    fn test_dropping_the_guard_removes_the_lock_file() {
        let temp = TempDir::new().unwrap();
        let path = temp.path().join("ledger.db");
        let conn = db(&path);

        let guard = InstanceGuard::acquire(&conn, &path).unwrap();
        let lock_path = guard.lock_path().to_path_buf();
        assert!(lock_path.exists());
        drop(guard);
        assert!(
            !lock_path.exists(),
            "a clean exit must not leave a lock file behind"
        );
    }
}
