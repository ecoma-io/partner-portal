//! Ledger module for durable usage metering
//!
//! Uses SQLite with WAL mode for durability. All writes go through a bounded
//! queue with micro-batching to balance throughput and latency.

mod pool;
mod types;
mod writer;

pub use pool::LedgerPool;
pub use types::{Endpoint, RequestRecord, RequestStatus, Usage, UsageStatus};
pub use writer::{LedgerWriter, LedgerWriterConfig, WriteError};

/// Initialize the database schema
pub fn init_schema(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
    conn.execute_batch(include_str!("schema.sql"))?;
    Ok(())
}

/// Configure SQLite for durability
pub fn configure_sqlite(conn: &rusqlite::Connection) -> Result<(), rusqlite::Error> {
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
