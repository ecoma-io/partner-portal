//! Ledger: durable usage metering.
//!
//! SQLite in WAL mode with `synchronous = FULL`. All writes go through a single
//! writer task fed by a bounded queue with micro-batching ([`writer`]), so the
//! invariants the product promises are enforced in one place:
//!
//! * **Never lose data.** The queue bounds memory and applies backpressure;
//!   it never drops. Busy/locked database failures are retried, not discarded.
//! * **Every request has a terminal state.** A record is committed as
//!   `in_flight` before the upstream is contacted, and resolved to
//!   `completed` / `failed` / `interrupted` afterwards — or by [`recovery`] if
//!   the process died first.
//! * **Raw and rollup agree.** Both are written in the same transaction, and the
//!   rollup is applied exactly once per request. `finalize` is *idempotent by
//!   construction*: it retracts the row's previous rollup contribution before
//!   adding the new one, so a duplicate or corrected finalize cannot double
//!   count and cannot silently discard a real usage figure.
//! * **Unavailable is not zero.** Missing usage persists as `NULL` with
//!   `usage_status = 'unavailable'`; token columns are never fabricated.

pub mod instance;
pub mod pool;
pub mod recovery;
pub mod retention;
pub mod rollup;
pub mod timefmt;
pub mod types;
pub mod writer;

pub use instance::{InstanceGuard, Liveness};
pub use pool::LedgerPool;
pub use recovery::{RecoveryContext, RecoveryReport, recover_in_flight};
pub use types::{Endpoint, RequestRecord, RequestStatus, Usage, UsageStatus};
pub use writer::{LedgerWriter, LedgerWriterConfig, WriteError};

/// Current schema version. Bump when `schema.sql` changes incompatibly.
///
/// History:
/// * 1 — initial ledger
/// * 2 — `cached_tokens`
/// * 3 — `usage_records.instance_id` + `ledger_instances` (ownership-aware recovery)
pub const SCHEMA_VERSION: u32 = 3;

/// A failure to bring the database up to the schema this binary expects.
#[derive(Debug)]
pub enum SchemaError {
    /// The database could not be read or altered.
    Sqlite(rusqlite::Error),
    /// The database was written by a *newer* build than this one.
    ///
    /// Refusing is the only safe answer: an older binary cannot know what a
    /// newer schema means, and silently stamping the file with its own version
    /// would relabel data it does not understand — the failure that turns a
    /// rollback into a corrupted ledger.
    UnsupportedVersion { found: u32, supported: u32 },
    /// A column the migration needs is missing even after the migration ran.
    MigrationIncomplete { column: &'static str },
}

impl std::fmt::Display for SchemaError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SchemaError::Sqlite(e) => write!(f, "{e}"),
            SchemaError::UnsupportedVersion { found, supported } => write!(
                f,
                "ledger schema version {found} was written by a newer build; \
                 this binary supports up to {supported}. Refusing to open it: \
                 downgrading would relabel data this build cannot read"
            ),
            SchemaError::MigrationIncomplete { column } => write!(
                f,
                "ledger migration did not produce the column {column:?}; the \
                 database is neither the old nor the new schema"
            ),
        }
    }
}

impl std::error::Error for SchemaError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            SchemaError::Sqlite(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for SchemaError {
    fn from(e: rusqlite::Error) -> Self {
        SchemaError::Sqlite(e)
    }
}

/// Bring the database to [`SCHEMA_VERSION`], migrating an older one forward.
///
/// The recorded version is *compared*, never overwritten blindly. `schema.sql`
/// is additive and idempotent, so it is safe to run against an existing file;
/// the steps that cannot be expressed idempotently in SQL (adding a column to a
/// table that may already carry it) run afterwards, guarded by the version and
/// by a column check.
pub fn init_schema(conn: &rusqlite::Connection) -> Result<(), SchemaError> {
    let found = read_schema_version(conn)?;
    if found > SCHEMA_VERSION {
        return Err(SchemaError::UnsupportedVersion {
            found,
            supported: SCHEMA_VERSION,
        });
    }

    conn.execute_batch(include_str!("schema.sql"))?;

    // v3: per-row instance ownership, so recovery on one instance can tell a
    // stranded request from a live sibling's request during a rolling update.
    if !column_exists(conn, "usage_records", "instance_id")? {
        conn.execute_batch("ALTER TABLE usage_records ADD COLUMN instance_id TEXT;")?;
    }
    if !column_exists(conn, "usage_records", "instance_id")? {
        return Err(SchemaError::MigrationIncomplete {
            column: "usage_records.instance_id",
        });
    }
    // One partial index serves both recovery and the in-flight audit: the
    // candidate set is tiny, and narrowing the index to in-flight rows keeps it
    // cheap to maintain on the write path.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_usage_records_instance_in_flight
             ON usage_records(instance_id) WHERE request_status = 'in_flight';",
    )?;

    write_schema_version(conn, SCHEMA_VERSION)?;
    Ok(())
}

/// The version currently recorded in the database, or 0 when the database has
/// no ledger in it yet.
pub fn read_schema_version(conn: &rusqlite::Connection) -> Result<u32, SchemaError> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT value FROM ledger_meta WHERE key = 'schema_version'",
            [],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|e| match e {
            // No meta table at all: a fresh file, which is version 0.
            rusqlite::Error::SqliteFailure(_, Some(ref msg)) if msg.contains("no such table") => {
                Ok(None)
            }
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })?;

    Ok(match stored {
        None => 0,
        Some(value) => value.trim().parse::<u32>().map_err(|_| {
            SchemaError::Sqlite(rusqlite::Error::InvalidColumnType(
                0,
                "schema_version".to_string(),
                rusqlite::types::Type::Text,
            ))
        })?,
    })
}

fn write_schema_version(conn: &rusqlite::Connection, version: u32) -> Result<(), SchemaError> {
    conn.execute(
        "INSERT INTO ledger_meta (key, value) VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![version.to_string()],
    )?;
    Ok(())
}

fn column_exists(
    conn: &rusqlite::Connection,
    table: &str,
    column: &str,
) -> Result<bool, rusqlite::Error> {
    let mut stmt = conn.prepare(&format!("PRAGMA table_info({table})"))?;
    let mut rows = stmt.query([])?;
    while let Some(row) = rows.next()? {
        let name: String = row.get(1)?;
        if name == column {
            return Ok(true);
        }
    }
    Ok(false)
}

/// The dashboard pagination cursor signing key.
///
/// A per-database random secret, generated on first use. It is deliberately not
/// any process's configuration secret: every instance sharing a database must
/// accept the cursors the others issued, which a per-process key would break on
/// every rolling update. It exists so a cursor cannot be forged and so the
/// global row id it carries is not legible to the partner holding it.
pub fn cursor_key(conn: &rusqlite::Connection) -> Result<Vec<u8>, rusqlite::Error> {
    for _ in 0..3 {
        if let Some(key) = read_cursor_key(conn)? {
            return Ok(key);
        }
        // Another instance may be racing us; `OR IGNORE` keeps that harmless.
        conn.execute(
            "INSERT OR IGNORE INTO ledger_meta (key, value)
             VALUES ('cursor_key', hex(randomblob(32)))",
            [],
        )?;
    }
    Err(rusqlite::Error::QueryReturnedNoRows)
}

fn read_cursor_key(conn: &rusqlite::Connection) -> Result<Option<Vec<u8>>, rusqlite::Error> {
    let stored: Option<String> = conn
        .query_row(
            "SELECT value FROM ledger_meta WHERE key = 'cursor_key'",
            [],
            |row| row.get(0),
        )
        .map(Some)
        .or_else(|e| match e {
            rusqlite::Error::QueryReturnedNoRows => Ok(None),
            other => Err(other),
        })?;

    Ok(stored
        .as_deref()
        .and_then(|hex_str| hex::decode(hex_str).ok())
        .filter(|bytes| !bytes.is_empty()))
}

/// `PRAGMA auto_vacuum` value meaning incremental vacuum.
const AUTO_VACUUM_INCREMENTAL: i64 = 2;

/// Configure a SQLite connection for durability and concurrent readers.
///
/// **Order matters.** `auto_vacuum` is a persistent property of the file and is
/// only honoured if it is set before the database has ever been written to;
/// `journal_mode = WAL` writes the header, and once that has happened the only
/// way to change `auto_vacuum` is a full `VACUUM`. So `auto_vacuum` is settled
/// first, then the rest.
///
/// **Setting it is a write, so it is not done on connections that do not need
/// it.** That is not an optimisation. `PRAGMA auto_vacuum = INCREMENTAL` opens a
/// write transaction and appends a page-1 frame to the WAL *every time it runs*,
/// even when the database is already in incremental mode — measured: one 4 KiB
/// WAL frame and, under `synchronous = FULL`, one fsync per execution. The pool
/// opens a fresh reader connection for every query, so a dashboard polling its
/// own summary would have written to the ledger over and over, and — because
/// `PRAGMA data_version` counts commits by other connections — each of those
/// writes announced "data changed" to the very client that caused it. A
/// dashboard open on an idle proxy refreshed itself in a loop for as long as it
/// stayed open. The statement is therefore issued only where it can still take
/// effect: on a database that has no schema yet.
///
/// Verifying rather than assuming is deliberate: a database created by an
/// earlier build is already past the point where this can take effect, and that
/// is a fact an operator needs told once, at startup, not discovered when the
/// ledger file never shrinks.
pub fn configure_sqlite(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    let auto_vacuum: i64 = conn.query_row("PRAGMA auto_vacuum", [], |row| row.get(0))?;
    if auto_vacuum != AUTO_VACUUM_INCREMENTAL {
        // A database with no schema is one this process is about to create, and
        // the only one where the setting can still be honoured without a VACUUM.
        let tables: i64 =
            conn.query_row("SELECT COUNT(*) FROM sqlite_master", [], |row| row.get(0))?;
        if tables == 0 {
            // Before any write to the file: without this, `PRAGMA
            // incremental_vacuum` in the retention sweep frees nothing and
            // deleted space is never reclaimed.
            conn.execute_batch("PRAGMA auto_vacuum = INCREMENTAL;")?;
        } else {
            tracing::warn!(
                auto_vacuum,
                "incremental vacuum is not active for this database: it was created \
                 by an earlier build, and enabling it now needs a one-off `VACUUM`. \
                 Retention still deletes rows correctly; the file will not shrink \
                 until that is run"
            );
        }
    }

    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = FULL;
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 5000;
        PRAGMA temp_store = MEMORY;
        "#,
    )?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn new_db(path: &std::path::Path) -> rusqlite::Connection {
        let conn = rusqlite::Connection::open(path).unwrap();
        configure_sqlite(&conn).unwrap();
        conn
    }

    /// The property every reader connection depends on: configuring a
    /// connection must not write to the database.
    ///
    /// Asserted through `PRAGMA data_version`, which is precisely the signal the
    /// dashboard's change poller watches: it moves when *another* connection
    /// commits, so a configurating connection that writes is indistinguishable,
    /// from the poller's side, from a request having been metered. This is what
    /// broke: `PRAGMA auto_vacuum = INCREMENTAL` writes a page-1 frame every time
    /// it runs, including on a database already in incremental mode, so every
    /// reader connection announced a change to every open dashboard.
    #[test]
    fn test_configuring_a_connection_does_not_write_to_the_database() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let setup = new_db(&path);
        init_schema(&setup).unwrap();

        let watcher = rusqlite::Connection::open(&path).unwrap();
        let before: i64 = watcher
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .unwrap();

        // Exactly what `LedgerPool::reader` does for every read.
        let reader = rusqlite::Connection::open(&path).unwrap();
        configure_sqlite(&reader).unwrap();
        reader
            .query_row("SELECT COUNT(*) FROM usage_records", [], |row| {
                row.get::<_, i64>(0)
            })
            .unwrap();
        drop(reader);

        let after: i64 = watcher
            .query_row("PRAGMA data_version", [], |row| row.get(0))
            .unwrap();
        assert_eq!(
            before, after,
            "opening and configuring a reader connection must not commit anything"
        );
    }

    #[test]
    fn test_a_new_database_is_created_with_incremental_vacuum() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let conn = new_db(&path);
        init_schema(&conn).unwrap();

        // Still set on the file that this process created, which is the case
        // the guard in `configure_sqlite` must not have skipped.
        let auto_vacuum: i64 = conn
            .query_row("PRAGMA auto_vacuum", [], |row| row.get(0))
            .unwrap();
        assert_eq!(auto_vacuum, AUTO_VACUUM_INCREMENTAL);
    }

    #[test]
    fn test_schema_version_is_recorded() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        init_schema(&conn).unwrap();

        assert_eq!(read_schema_version(&conn).unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn test_init_schema_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        init_schema(&conn).unwrap();
        init_schema(&conn).unwrap();

        let n: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='usage_records'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(n, 1);
        assert_eq!(read_schema_version(&conn).unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn test_fresh_database_is_left_with_an_explicit_version() {
        // A brand-new file must not stay at 0: recovery and reporting both need
        // to know a schema was actually applied.
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        assert_eq!(read_schema_version(&conn).unwrap(), 0);
        init_schema(&conn).unwrap();
        assert_eq!(read_schema_version(&conn).unwrap(), 3);
    }

    #[test]
    fn test_older_schema_is_migrated_forward_not_relabelled() {
        // Build a v2 database: the ledger as it shipped before instance
        // ownership, with no `instance_id` column and the version stamped 2.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("legacy.db");
        let conn = new_db(&path);
        conn.execute_batch(
            "CREATE TABLE usage_records (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                request_id TEXT NOT NULL UNIQUE,
                created_at TEXT NOT NULL,
                consumer_id TEXT NOT NULL,
                model TEXT NOT NULL,
                endpoint TEXT NOT NULL,
                streaming INTEGER NOT NULL DEFAULT 0,
                http_status INTEGER,
                request_status TEXT NOT NULL,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cached_tokens INTEGER,
                ttft_ms INTEGER,
                duration_ms INTEGER NOT NULL,
                usage_status TEXT NOT NULL,
                error_message TEXT
             );
             CREATE TABLE ledger_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO ledger_meta (key, value) VALUES ('schema_version', '2');
             INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, duration_ms, usage_status
             ) VALUES ('legacy-1', '2026-01-01T00:00:00.000000000Z', 'c1', 'm',
                       'chat_completions', 0, 'completed', 10, 'unavailable');",
        )
        .unwrap();

        init_schema(&conn).unwrap();

        assert_eq!(read_schema_version(&conn).unwrap(), 3);
        assert!(
            column_exists(&conn, "usage_records", "instance_id").unwrap(),
            "the migration must actually add the column, not just relabel the file"
        );
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1, "migrating must not touch existing rows");
        let owner: Option<String> = conn
            .query_row(
                "SELECT instance_id FROM usage_records WHERE request_id = 'legacy-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            owner, None,
            "a pre-migration row has no known owner and must stay NULL"
        );
    }

    #[test]
    fn test_newer_schema_is_refused_rather_than_overwritten() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        init_schema(&conn).unwrap();
        write_schema_version(&conn, SCHEMA_VERSION + 1).unwrap();

        let err = init_schema(&conn).expect_err("a newer schema must be refused");
        assert!(
            matches!(
                err,
                SchemaError::UnsupportedVersion {
                    found,
                    supported
                } if found == SCHEMA_VERSION + 1 && supported == SCHEMA_VERSION
            ),
            "unexpected error: {err}"
        );
        assert_eq!(
            read_schema_version(&conn).unwrap(),
            SCHEMA_VERSION + 1,
            "a refused open must not relabel the database"
        );
    }

    #[test]
    fn test_cursor_key_is_stable_and_stored_once() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        init_schema(&conn).unwrap();

        let first = cursor_key(&conn).unwrap();
        assert_eq!(first.len(), 32, "the key is 32 random bytes");
        assert_eq!(
            first,
            cursor_key(&conn).unwrap(),
            "the key must be stable for the life of the database"
        );
        assert_eq!(first, cursor_key(&conn).unwrap());
    }

    #[test]
    fn test_cursor_key_is_generated_when_absent() {
        // A database that predates the cursor key must get one, not an error.
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        init_schema(&conn).unwrap();
        conn.execute("DELETE FROM ledger_meta WHERE key = 'cursor_key'", [])
            .unwrap();

        let key = cursor_key(&conn).unwrap();
        assert_eq!(key.len(), 32);
        assert_eq!(
            key,
            cursor_key(&conn).unwrap(),
            "the generated key must be persisted, not regenerated per call"
        );
    }

    #[test]
    fn test_synchronous_full_and_wal_are_active() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));

        let journal: String = conn
            .query_row("PRAGMA journal_mode", [], |r| r.get(0))
            .unwrap();
        assert_eq!(journal.to_lowercase(), "wal");

        // synchronous 2 == FULL. Durability of every COMMIT depends on this.
        let sync: i64 = conn
            .query_row("PRAGMA synchronous", [], |r| r.get(0))
            .unwrap();
        assert_eq!(sync, 2, "synchronous must be FULL (2)");
    }

    #[test]
    fn test_busy_timeout_is_set() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        let t: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t, 5000);
    }

    #[test]
    fn test_schema_rejects_fabricated_states() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        init_schema(&conn).unwrap();

        // An unknown status must be rejected by the CHECK constraint rather
        // than silently stored.
        let err = conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, duration_ms, usage_status
             ) VALUES ('x', '2026-01-01T00:00:00.000000000Z', 'c', 'm',
                       'chat_completions', 0, 'bogus', 0, 'unavailable')",
            [],
        );
        assert!(
            err.is_err(),
            "invalid request_status must violate the CHECK"
        );

        // Negative tokens must be rejected too.
        let err = conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, input_tokens, duration_ms, usage_status
             ) VALUES ('y', '2026-01-01T00:00:00.000000000Z', 'c', 'm',
                       'chat_completions', 0, 'completed', -5, 0, 'available')",
            [],
        );
        assert!(err.is_err(), "negative tokens must violate the CHECK");
    }
}
