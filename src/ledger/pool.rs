//! SQLite connection pool for the ledger
//!
//! Single-writer, multiple-reader model with proper WAL configuration.

use crate::ledger::{SchemaError, configure_sqlite, init_schema};
use parking_lot::Mutex;
use rusqlite::Connection;
use std::path::PathBuf;
use std::sync::Arc;

/// Ledger connection pool
pub struct LedgerPool {
    path: PathBuf,
    writer: Arc<Mutex<Connection>>,
}

impl LedgerPool {
    /// Create a new ledger pool at the given path.
    ///
    /// Returns [`SchemaError`] rather than a bare `rusqlite::Error`, because
    /// bringing the file up to the expected schema can also fail for reasons that
    /// are not SQLite errors — most importantly a database written by a *newer*
    /// build, which must be refused rather than relabelled. Those reasons must
    /// reach the caller intact or an operator sees "the ledger did not open"
    /// with no way to tell a corrupt file from a rollback.
    pub fn new(path: PathBuf) -> Result<Self, SchemaError> {
        let conn = Connection::open(&path)?;
        configure_sqlite(&conn)?;
        init_schema(&conn)?;

        Ok(Self {
            path,
            writer: Arc::new(Mutex::new(conn)),
        })
    }

    /// Get a writer connection (single writer only)
    pub fn writer(&self) -> Arc<Mutex<Connection>> {
        self.writer.clone()
    }

    /// Create a new reader connection
    pub fn reader(&self) -> Result<Connection, rusqlite::Error> {
        let conn = Connection::open(&self.path)?;
        configure_sqlite(&conn)?;
        Ok(conn)
    }

    /// Get the database path
    pub fn path(&self) -> &PathBuf {
        &self.path
    }

    /// Run a read operation with a fresh connection
    pub fn read<T, F>(&self, f: F) -> Result<T, rusqlite::Error>
    where
        F: FnOnce(&Connection) -> Result<T, rusqlite::Error>,
    {
        let conn = self.reader()?;
        f(&conn)
    }

    /// Run a write operation
    pub fn write<T, F>(&self, f: F) -> Result<T, rusqlite::Error>
    where
        F: FnOnce(&mut Connection) -> Result<T, rusqlite::Error>,
    {
        let mut conn = self.writer.lock();
        f(&mut conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_pool_creates_schema() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");

        let pool = LedgerPool::new(path.clone()).unwrap();

        // Verify schema was created
        pool.read(|conn| {
            let count: i64 = conn.query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='usage_records'",
                [],
                |row| row.get(0),
            )?;
            assert_eq!(count, 1);
            Ok(())
        })
        .unwrap();
    }

    #[test]
    fn test_wal_mode_enabled() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("test.db");

        let pool = LedgerPool::new(path).unwrap();

        pool.read(|conn| {
            let mode: String = conn.query_row("PRAGMA journal_mode", [], |row| row.get(0))?;
            assert_eq!(mode.to_lowercase(), "wal");
            Ok(())
        })
        .unwrap();
    }
}
