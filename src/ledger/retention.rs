//! Retention: prune ledger data older than the configured window.
//!
//! # Why batched, not one big DELETE
//!
//! A single `DELETE FROM usage_records WHERE created_at < ?` over millions of
//! rows holds the write lock for as long as the scan and rewrite take, which
//! during a same-VPS rolling update means the *other* instance's metering stalls
//! and its bounded queue backs up. Retention therefore deletes in bounded
//! slices, releasing the write lock between slices so the ledger writer can
//! interleave. Each slice commits on its own, so a crash mid-sweep leaves the
//! database consistent and the next sweep simply continues.
//!
//! A full `VACUUM` is never run: it rewrites the whole database file and needs
//! an exclusive lock and up to 2x the file size in free disk. Space is reclaimed
//! incrementally instead, via `auto_vacuum = INCREMENTAL` (set before the schema
//! is created) and bounded `PRAGMA incremental_vacuum(N)`.

use crate::ledger::timefmt;
use parking_lot::Mutex;
use rusqlite::{Connection, TransactionBehavior};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Rows deleted per slice. Small enough that the write lock is held for a few
/// milliseconds, large enough that a 60-day sweep is not thousands of round trips.
pub const DEFAULT_BATCH_SIZE: usize = 2_000;

/// Upper bound on slices per sweep, so one sweep cannot run unboundedly long.
pub const DEFAULT_MAX_BATCHES: u32 = 500;

/// Pause between slices, letting the metering writer take the lock. Without this
/// a sweep can starve the writer even though each individual slice is short.
pub const INTER_BATCH_PAUSE: Duration = Duration::from_millis(5);

/// Pages of free space to reclaim per sweep, passed to `incremental_vacuum`.
const INCREMENTAL_VACUUM_PAGES: i64 = 8_192;

/// What a retention sweep accomplished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionStats {
    pub raw_deleted: u64,
    pub hourly_deleted: u64,
    pub batches: u32,
    /// True when the sweep hit its slice budget with data still eligible; the
    /// next scheduled sweep continues from where this one stopped.
    pub hit_budget: bool,
    pub duration_ms: u64,
}

impl RetentionStats {
    pub fn total_deleted(&self) -> u64 {
        self.raw_deleted + self.hourly_deleted
    }
}

/// Compute the retention cutoff: records strictly older than this are eligible.
pub fn cutoff_for(retention_days: u32) -> String {
    let days = retention_days.max(1) as i64;
    timefmt::format_ts(timefmt::now() - time::Duration::days(days))
}

/// Run one bounded retention sweep.
///
/// Takes the writer mutex per slice rather than for the whole sweep, so metering
/// writes interleave. Returns the number of rows removed.
pub fn run_retention(
    conn: &Arc<Mutex<Connection>>,
    retention_days: u32,
    batch_size: usize,
    max_batches: u32,
) -> Result<RetentionStats, rusqlite::Error> {
    let started = Instant::now();
    let cutoff = cutoff_for(retention_days);
    let batch_size = batch_size.max(1) as i64;

    let mut stats = RetentionStats::default();

    // --- Raw ledger slices ------------------------------------------------
    loop {
        if stats.batches >= max_batches {
            stats.hit_budget = true;
            break;
        }

        let deleted = {
            let mut guard = conn.lock();
            let tx = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
            // Delete by id, chosen from an indexed range scan, so the DELETE
            // touches a bounded number of rows instead of scanning the table.
            let n = tx.execute(
                "DELETE FROM usage_records WHERE id IN (
                     SELECT id FROM usage_records WHERE created_at < ?1 ORDER BY id LIMIT ?2
                 )",
                rusqlite::params![cutoff, batch_size],
            )?;
            tx.commit()?;
            n as u64
        };

        stats.raw_deleted += deleted;
        stats.batches += 1;

        if (deleted as i64) < batch_size {
            break;
        }
        std::thread::sleep(INTER_BATCH_PAUSE);
    }

    // --- Hourly rollup slices --------------------------------------------
    // Rollups are ~1000x smaller than raw rows, so this normally completes in a
    // single slice; it is still batched for the pathological case.
    loop {
        if stats.batches >= max_batches {
            stats.hit_budget = true;
            break;
        }

        let deleted = {
            let mut guard = conn.lock();
            let tx = guard.transaction_with_behavior(TransactionBehavior::Immediate)?;
            let n = tx.execute(
                "DELETE FROM usage_hourly WHERE id IN (
                     SELECT id FROM usage_hourly WHERE hour < ?1 ORDER BY id LIMIT ?2
                 )",
                rusqlite::params![cutoff, batch_size],
            )?;
            tx.commit()?;
            n as u64
        };

        stats.hourly_deleted += deleted;
        stats.batches += 1;

        if (deleted as i64) < batch_size {
            break;
        }
        std::thread::sleep(INTER_BATCH_PAUSE);
    }

    // --- Reclaim free pages incrementally ---------------------------------
    // Bounded, non-blocking-ish, and safe: it only ever moves free pages out of
    // the file. Failure here is not fatal (the sweep already committed).
    if stats.total_deleted() > 0 {
        let guard = conn.lock();
        if let Err(e) = guard.execute_batch(&format!(
            "PRAGMA incremental_vacuum({INCREMENTAL_VACUUM_PAGES});"
        )) {
            warn!(error = %e, "incremental_vacuum failed; space will be reclaimed later");
        }
        let _ = guard.execute(
            "INSERT INTO ledger_meta (key, value) VALUES ('last_retention_run', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![timefmt::format_ts(timefmt::now())],
        );
    }

    stats.duration_ms = started.elapsed().as_millis() as u64;
    Ok(stats)
}

/// Log a sweep at the level its outcome deserves.
pub fn report(stats: &RetentionStats, retention_days: u32) {
    if stats.total_deleted() == 0 {
        info!(
            retention_days,
            duration_ms = stats.duration_ms,
            "Retention sweep found nothing to prune"
        );
    } else {
        info!(
            retention_days,
            raw_deleted = stats.raw_deleted,
            hourly_deleted = stats.hourly_deleted,
            batches = stats.batches,
            duration_ms = stats.duration_ms,
            hit_budget = stats.hit_budget,
            "Retention sweep complete"
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{Endpoint, RequestRecord, Usage};
    use tempfile::TempDir;

    fn setup(path: &std::path::Path) -> Arc<Mutex<Connection>> {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        Arc::new(Mutex::new(conn))
    }

    /// Insert a terminal record with an explicit age in days.
    fn insert(conn: &Arc<Mutex<Connection>>, id: &str, age_days: i64) {
        let created = timefmt::now() - time::Duration::days(age_days);
        let mut record = RequestRecord::new(
            id.to_string(),
            "c".to_string(),
            "m".to_string(),
            Endpoint::ChatCompletions,
            false,
        );
        record.created_at = created;
        record.complete(200, Usage::new(Some(10), Some(20), Some(5)), 50);

        let mut guard = conn.lock();
        let tx = guard.transaction().unwrap();
        crate::ledger::writer::finalize_in_tx(&tx, &record).unwrap();
        tx.commit().unwrap();
    }

    fn count(conn: &Arc<Mutex<Connection>>, table: &str) -> i64 {
        let guard = conn.lock();
        guard
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |r| r.get(0))
            .unwrap()
    }

    #[test]
    fn test_prunes_only_rows_past_retention() {
        let dir = TempDir::new().unwrap();
        let conn = setup(&dir.path().join("t.db"));

        insert(&conn, "old-1", 90);
        insert(&conn, "old-2", 61);
        insert(&conn, "edge", 59);
        insert(&conn, "new", 1);

        let stats = run_retention(&conn, 60, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES).unwrap();
        assert_eq!(stats.raw_deleted, 2, "only >60d rows are eligible");
        assert_eq!(count(&conn, "usage_records"), 2);

        let remaining: Vec<String> = {
            let guard = conn.lock();
            let mut stmt = guard
                .prepare("SELECT request_id FROM usage_records ORDER BY request_id")
                .unwrap();
            stmt.query_map([], |r| r.get::<_, String>(0))
                .unwrap()
                .collect::<Result<Vec<_>, _>>()
                .unwrap()
        };
        assert_eq!(remaining, vec!["edge", "new"]);
    }

    #[test]
    fn test_prunes_rollups_past_retention() {
        let dir = TempDir::new().unwrap();
        let conn = setup(&dir.path().join("t.db"));

        insert(&conn, "old", 90);
        insert(&conn, "new", 1);
        assert_eq!(count(&conn, "usage_hourly"), 2);

        let stats = run_retention(&conn, 60, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES).unwrap();
        assert_eq!(stats.hourly_deleted, 1);
        assert_eq!(count(&conn, "usage_hourly"), 1);
    }

    #[test]
    fn test_batching_covers_more_than_one_slice() {
        let dir = TempDir::new().unwrap();
        let conn = setup(&dir.path().join("t.db"));

        for i in 0..50 {
            insert(&conn, &format!("old-{i}"), 100);
        }

        // batch_size 10 forces multiple slices; all 50 must still be removed.
        let stats = run_retention(&conn, 60, 10, DEFAULT_MAX_BATCHES).unwrap();
        assert_eq!(stats.raw_deleted, 50);
        assert!(stats.batches >= 5, "must have taken multiple slices");
        assert!(!stats.hit_budget);
        assert_eq!(count(&conn, "usage_records"), 0);
    }

    #[test]
    fn test_slice_budget_bounds_work_and_is_reported() {
        let dir = TempDir::new().unwrap();
        let conn = setup(&dir.path().join("t.db"));

        for i in 0..50 {
            insert(&conn, &format!("old-{i}"), 100);
        }

        // Budget of 2 slices x 10 rows = at most 20 deleted this sweep.
        let stats = run_retention(&conn, 60, 10, 2).unwrap();
        assert_eq!(
            stats.raw_deleted, 20,
            "two raw slices, then the budget stops the sweep"
        );
        assert!(
            stats.hit_budget,
            "caller must learn the sweep was truncated"
        );
        assert_eq!(count(&conn, "usage_records"), 30);
        assert_eq!(
            stats.hourly_deleted, 0,
            "the budget must stop the sweep before rollup pruning too"
        );

        // A later sweep continues from where this one stopped.
        let stats2 = run_retention(&conn, 60, 100, DEFAULT_MAX_BATCHES).unwrap();
        assert_eq!(stats2.raw_deleted, 30);
        assert!(!stats2.hit_budget);
        assert_eq!(count(&conn, "usage_records"), 0);
        // Rollups for the pruned rows are pruned too, leaving consistency.
        let (terminal, rolled) =
            crate::ledger::recovery::check_raw_rollup_consistency(&conn.lock()).unwrap();
        assert_eq!(terminal, 0);
        assert_eq!(rolled, 0);
    }

    #[test]
    fn test_retention_never_touches_recent_data() {
        let dir = TempDir::new().unwrap();
        let conn = setup(&dir.path().join("t.db"));
        insert(&conn, "a", 0);
        insert(&conn, "b", 30);

        let stats = run_retention(&conn, 60, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES).unwrap();
        assert_eq!(stats.total_deleted(), 0);
        assert_eq!(count(&conn, "usage_records"), 2);
        assert_eq!(count(&conn, "usage_hourly"), 2);
    }

    #[test]
    fn test_retention_preserves_raw_rollup_consistency() {
        let dir = TempDir::new().unwrap();
        let conn = setup(&dir.path().join("t.db"));

        insert(&conn, "old", 100);
        insert(&conn, "recent", 5);

        run_retention(&conn, 60, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES).unwrap();

        let (terminal, rolled) =
            crate::ledger::recovery::check_raw_rollup_consistency(&conn.lock()).unwrap();
        assert_eq!(terminal, 1);
        assert_eq!(rolled, 1, "retention must prune raw and rollup together");
    }

    #[test]
    fn test_cutoff_is_fixed_width_and_orders_correctly() {
        let cutoff = cutoff_for(60);
        assert_eq!(cutoff.len(), 30);
        let parsed = timefmt::parse_ts(&cutoff).expect("cutoff must parse");
        let expected = timefmt::now() - time::Duration::days(60);
        assert!((parsed - expected).whole_seconds().abs() < 5);
    }

    #[test]
    fn test_zero_days_still_prunes_something_not_everything() {
        // Guard rail: retention_days = 0 must not be able to wipe the ledger.
        let dir = TempDir::new().unwrap();
        let conn = setup(&dir.path().join("t.db"));
        insert(&conn, "just-now", 0);

        let stats = run_retention(&conn, 0, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES).unwrap();
        assert_eq!(stats.total_deleted(), 0, "retention_days=0 clamps to 1 day");
        assert_eq!(count(&conn, "usage_records"), 1);
    }
}
