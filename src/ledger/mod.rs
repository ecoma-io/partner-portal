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
//!   rollup is applied exactly once per request.
//! * **Unavailable is not zero.** Missing usage persists as `NULL` with
//!   `usage_status = 'unavailable'`; token columns are never fabricated.

pub mod pool;
pub mod recovery;
pub mod retention;
pub mod timefmt;
pub mod types;
pub mod writer;

pub use pool::LedgerPool;
pub use recovery::{RecoveryReport, recover_in_flight};
pub use types::{Endpoint, RequestRecord, RequestStatus, Usage, UsageStatus};
pub use writer::{LedgerWriter, LedgerWriterConfig, WriteError};

/// Initialize the database schema.
pub fn init_schema(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(include_str!("schema.sql"))?;
    // The schema is created with `INSERT OR IGNORE`, so an existing database
    // keeps its old version row. Assert the current version explicitly.
    conn.execute(
        "INSERT INTO ledger_meta (key, value) VALUES ('schema_version', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![SCHEMA_VERSION.to_string()],
    )?;
    Ok(())
}

/// Current schema version. Bump when `schema.sql` changes incompatibly.
pub const SCHEMA_VERSION: u32 = 2;

/// Configure a SQLite connection for durability and concurrent readers.
///
/// `auto_vacuum` must be set **before any table is created** to take effect
/// without a full `VACUUM`, which is why it lives here rather than in the
/// retention sweep: it makes space reclaimable incrementally later.
pub fn configure_sqlite(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(
        r#"
        PRAGMA journal_mode = WAL;
        PRAGMA synchronous = FULL;
        PRAGMA foreign_keys = ON;
        PRAGMA busy_timeout = 5000;
        PRAGMA temp_store = MEMORY;
        PRAGMA auto_vacuum = INCREMENTAL;
        "#,
    )?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_schema_version_is_recorded() {
        let dir = TempDir::new().unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("t.db")).unwrap();
        configure_sqlite(&conn).unwrap();
        init_schema(&conn).unwrap();

        let v: String = conn
            .query_row(
                "SELECT value FROM ledger_meta WHERE key = 'schema_version'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(v, SCHEMA_VERSION.to_string());
    }

    #[test]
    fn test_init_schema_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("t.db")).unwrap();
        configure_sqlite(&conn).unwrap();
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
    }

    #[test]
    fn test_synchronous_full_and_wal_are_active() {
        let dir = TempDir::new().unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("t.db")).unwrap();
        configure_sqlite(&conn).unwrap();

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
        let conn = rusqlite::Connection::open(dir.path().join("t.db")).unwrap();
        configure_sqlite(&conn).unwrap();
        let t: i64 = conn
            .query_row("PRAGMA busy_timeout", [], |r| r.get(0))
            .unwrap();
        assert_eq!(t, 5000);
    }

    #[test]
    fn test_schema_rejects_fabricated_states() {
        let dir = TempDir::new().unwrap();
        let conn = rusqlite::Connection::open(dir.path().join("t.db")).unwrap();
        configure_sqlite(&conn).unwrap();
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
