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
//! is created) and bounded `PRAGMA incremental_vacuum(N)` — drained, not issued
//! once. See [`reclaim_free_pages`] for why that distinction is the difference
//! between reclaiming a sweep's worth of pages and reclaiming exactly one.

use crate::ledger::timefmt;
use parking_lot::Mutex;
use rusqlite::{Connection, TransactionBehavior};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Rows deleted per slice. Small enough that the write lock is held for a few
/// milliseconds, large enough that a 60-day sweep is not thousands of round trips.
pub const DEFAULT_BATCH_SIZE: usize = 2_000;

/// Upper bound on slices per sweep, so one sweep cannot run unboundedly long.
pub const DEFAULT_MAX_BATCHES: u32 = 500;

/// Pause between slices, letting the metering writer take the lock. Without this
/// a sweep can starve the writer even though each individual slice is short.
pub const INTER_BATCH_PAUSE: Duration = Duration::from_millis(5);

/// Pages reclaimed per `incremental_vacuum` statement.
///
/// One statement is one write transaction, so this is how long the metering
/// writer can be held off by reclamation: 64 pages is 256 KiB at the default
/// page size. See [`reclaim_free_pages`] for why the statement has to be drained
/// rather than issued once.
const VACUUM_PAGES_PER_STATEMENT: i64 = 64;

/// Upper bound on reclamation statements per sweep — 128 × 64 pages, 32 MiB.
///
/// A sweep is a background chore, not a maintenance window: reclaiming more than
/// this is left to the next sweep, which costs nothing because the free pages
/// stay on the freelist until something removes them.
const MAX_VACUUM_STATEMENTS: u32 = 128;

/// What a retention sweep accomplished.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RetentionStats {
    pub raw_deleted: u64,
    pub hourly_deleted: u64,
    pub batches: u32,
    /// True when the sweep hit its slice budget with data still eligible; the
    /// next scheduled sweep continues from where this one stopped.
    pub hit_budget: bool,
    /// Pages of disk actually returned to the filesystem by reclamation.
    ///
    /// Reported because it is the only thing that distinguishes "reclaimed
    /// space" from "deleted rows": a database without auto-vacuum deletes every
    /// eligible row and reclaims nothing, and without this number that failure
    /// is invisible in the logs.
    pub pages_reclaimed: u64,
    /// The database's page size, so `pages_reclaimed` can be reported in bytes.
    pub page_size: u32,
    pub duration_ms: u64,
}

impl RetentionStats {
    /// Bytes returned to the filesystem by reclamation.
    pub fn bytes_reclaimed(&self) -> u64 {
        self.pages_reclaimed * self.page_size as u64
    }
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

    // Read rather than assume: the page size is a property of the file, and a
    // database created by another tool may not use SQLite's 4096-byte default.
    // Reporting bytes derived from a guessed page size would be a number an
    // operator cannot trust.
    let page_size = conn
        .lock()
        .query_row("PRAGMA page_size", [], |r| r.get::<_, u32>(0))
        .unwrap_or(4096);

    let mut stats = RetentionStats {
        page_size,
        ..Default::default()
    };

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
    // Only ever moves free pages out of the file, and never blocks the writer
    // for longer than one bounded statement. Failure here is not fatal: the
    // sweep has already committed, so the rows are gone either way and the next
    // sweep will try again.
    if stats.total_deleted() > 0 {
        if let Err(e) = reclaim_free_pages(conn, &mut stats) {
            warn!(error = %e, "incremental_vacuum failed; space will be reclaimed later");
        }

        let guard = conn.lock();
        let _ = guard.execute(
            "INSERT INTO ledger_meta (key, value) VALUES ('last_retention_run', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![timefmt::format_ts(timefmt::now())],
        );
    }

    stats.duration_ms = started.elapsed().as_millis() as u64;
    Ok(stats)
}

/// Return free pages at the end of the file to the filesystem, in bounded
/// statements.
///
/// **`PRAGMA incremental_vacuum(N)` does not do N pages of work by itself.**
/// Its bytecode performs *one* unit of work, emits a result row, decrements a
/// counter, and jumps back while the counter is positive — so how much is
/// reclaimed is decided by how many times the statement is *stepped*, and `N` is
/// only the ceiling on that loop. `Connection::execute_batch` steps exactly once
/// (its `if false` guard exists precisely because some pragmas return rows), so
/// the obvious spelling —
/// `execute_batch("PRAGMA incremental_vacuum(8192)")` — reclaims **one page,
/// reports success, and logs nothing wrong**. Measured, not inferred: deleting
/// 1000 rows of ~4 KiB from an auto-vacuum database left the file 4 KiB smaller,
/// and 8192 was indistinguishable from 1.
///
/// Draining the rows explicitly is therefore both the fix and the accounting:
/// one row is one reclaimed page. A statement that yields fewer rows than it
/// asked for had nothing left to move, which is the signal to stop. On a
/// database without auto-vacuum the pragma yields no rows at all and this costs
/// one cheap statement — which is why `configure_sqlite` warns when it finds one.
fn reclaim_free_pages(
    conn: &Arc<Mutex<Connection>>,
    stats: &mut RetentionStats,
) -> Result<(), rusqlite::Error> {
    for _ in 0..MAX_VACUUM_STATEMENTS {
        let reclaimed = {
            let guard = conn.lock();
            let mut stmt = guard.prepare(&format!(
                "PRAGMA incremental_vacuum({VACUUM_PAGES_PER_STATEMENT});"
            ))?;
            let mut rows = stmt.query([])?;
            let mut reclaimed = 0u64;
            // Stepping to exhaustion is the whole point: each row is one page
            // moved out of the file, and abandoning the statement after one row
            // is the defect this function exists to avoid.
            while rows.next()?.is_some() {
                reclaimed += 1;
            }
            reclaimed
        };

        stats.pages_reclaimed += reclaimed;

        // Fewer rows than asked for means the file had nothing left to move —
        // either the freelist is exhausted or the size floor (`finalDbSize`,
        // which keeps pointer-map pages) has been reached. Either way, another
        // statement would do nothing.
        if reclaimed < VACUUM_PAGES_PER_STATEMENT as u64 {
            break;
        }
        // Let the metering writer in between statements.
        std::thread::sleep(INTER_BATCH_PAUSE);
    }

    if stats.pages_reclaimed == MAX_VACUUM_STATEMENTS as u64 * VACUUM_PAGES_PER_STATEMENT as u64 {
        // Hit the cap with reclamation still going. Say so: a silent cap reads
        // as "reclamation is slow" when it is really "reclamation stopped early,
        // and the next sweep continues".
        debug!(
            pages = stats.pages_reclaimed,
            "incremental vacuum hit its per-sweep budget; the next sweep continues"
        );
    }
    Ok(())
}

/// Log a sweep at the level its outcome deserves.
pub fn report(stats: &RetentionStats, retention_days: u32) {
    if stats.total_deleted() == 0 {
        info!(
            retention_days,
            duration_ms = stats.duration_ms,
            "Retention sweep found nothing to prune"
        );
    } else if stats.pages_reclaimed == 0 {
        // Rows went, the file did not shrink. On a database created before
        // auto-vacuum was enabled this is expected and permanent until an
        // operator runs `VACUUM`; `configure_sqlite` warns about it at startup.
        // Saying so here means the two lines appear together in the log.
        info!(
            retention_days,
            raw_deleted = stats.raw_deleted,
            hourly_deleted = stats.hourly_deleted,
            batches = stats.batches,
            duration_ms = stats.duration_ms,
            hit_budget = stats.hit_budget,
            "Retention sweep complete; no disk space reclaimed (the database is \
             not in auto-vacuum mode — see the startup warning)"
        );
    } else {
        info!(
            retention_days,
            raw_deleted = stats.raw_deleted,
            hourly_deleted = stats.hourly_deleted,
            batches = stats.batches,
            pages_reclaimed = stats.pages_reclaimed,
            bytes_reclaimed = stats.bytes_reclaimed(),
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

    /// Insert a record carrying a large error message, so the database has
    /// something to actually reclaim.
    fn insert_fat(conn: &Arc<Mutex<Connection>>, id: &str, age_days: i64, bytes: usize) {
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
        record.error_message = Some("x".repeat(bytes));

        let mut guard = conn.lock();
        let tx = guard.transaction().unwrap();
        crate::ledger::writer::finalize_in_tx(&tx, &record).unwrap();
        tx.commit().unwrap();
    }

    /// Bytes the database occupies, with the WAL folded back in.
    ///
    /// In WAL mode a file-size change — including the truncation that
    /// `incremental_vacuum` performs — is itself a WAL record, so the main file
    /// only shrinks when the WAL is checkpointed. SQLite checkpoints on its own;
    /// the test forces it so the assertion is deterministic rather than
    /// dependent on how many pages happened to pass the autocheckpoint
    /// threshold. Measuring the main file alone after a truncating checkpoint is
    /// the honest number: it is what the disk holds once the WAL is folded in.
    fn checkpointed_db_bytes(conn: &Arc<Mutex<Connection>>, path: &std::path::Path) -> u64 {
        conn.lock()
            .execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .expect("checkpoint");
        std::fs::metadata(path).expect("database file").len()
    }

    #[test]
    fn test_retention_reclaims_disk_space_on_a_database_this_build_created() {
        // The whole point of `auto_vacuum = INCREMENTAL`: retention deletes rows,
        // and the space those rows occupied must come back. Without it the file
        // grows forever at ~6M records / 60 days, which is the load this product
        // is built for.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let conn = setup(&path);

        // ~4 MiB of payload, then delete all of it.
        for i in 0..1_000 {
            insert_fat(&conn, &format!("old-{i}"), 100, 4_000);
        }
        let full = checkpointed_db_bytes(&conn, &path);
        assert!(full > 4 * 1024 * 1024, "fixture must actually be large");

        let stats = run_retention(&conn, 60, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES).unwrap();
        assert_eq!(stats.raw_deleted, 1_000);

        // ~1000 freed pages, and the per-sweep budget is 128 statements x 64
        // pages = 8192, so one sweep is enough to reclaim all of it.
        assert!(
            stats.pages_reclaimed > 500,
            "reclamation must free the deleted rows' pages, not one page: {} \
             reclaimed",
            stats.pages_reclaimed
        );
        assert_eq!(
            stats.bytes_reclaimed(),
            stats.pages_reclaimed * stats.page_size as u64,
            "the byte count must be derived from the database's own page size"
        );

        let after = checkpointed_db_bytes(&conn, &path);
        assert!(
            after < full / 2,
            "deleting every row must return the disk space: {full} -> {after} bytes"
        );
    }

    #[test]
    fn test_reclamation_does_not_stop_after_one_page() {
        // The exact defect this guards: `PRAGMA incremental_vacuum(N)` is a
        // looping statement whose bytecode emits one row per page reclaimed, and
        // `Connection::execute_batch` steps it exactly once — so the obvious
        // spelling of the call reclaimed a single 4 KiB page per sweep no matter
        // what N said, and reported success while doing it. Measured before the
        // fix: 1000 deleted rows of ~4 KiB left the file exactly one page
        // smaller, and N=8192 was indistinguishable from N=1.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let conn = setup(&path);

        for i in 0..500 {
            insert_fat(&conn, &format!("old-{i}"), 100, 4_000);
        }
        let before = conn
            .lock()
            .query_row("PRAGMA page_count", [], |r| r.get::<_, i64>(0))
            .unwrap();

        run_retention(&conn, 60, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES).unwrap();

        let after = conn
            .lock()
            .query_row("PRAGMA page_count", [], |r| r.get::<_, i64>(0))
            .unwrap();
        assert!(
            before - after > 100,
            "one sweep must reclaim far more than one page: {before} -> {after}"
        );
    }

    #[test]
    fn test_a_database_from_an_earlier_build_cannot_reclaim_without_a_vacuum() {
        // The counterpart, and the reason `configure_sqlite` warns instead of
        // staying silent when it finds `auto_vacuum` other than INCREMENTAL: on a
        // database created before this build, the setting cannot be turned on
        // without a one-off `VACUUM`, so retention deletes the rows but the file
        // does not shrink. The warning is the only thing that stops an operator
        // from concluding the reclamation is broken.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("old.db");
        let conn = Connection::open(&path).unwrap();
        // What an earlier build left behind: the default, no auto-vacuum.
        conn.execute_batch("PRAGMA auto_vacuum = NONE;").unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        let conn = Arc::new(Mutex::new(conn));

        for i in 0..1_000 {
            insert_fat(&conn, &format!("old-{i}"), 100, 4_000);
        }
        let full = checkpointed_db_bytes(&conn, &path);

        let stats = run_retention(&conn, 60, DEFAULT_BATCH_SIZE, DEFAULT_MAX_BATCHES).unwrap();
        assert_eq!(stats.raw_deleted, 1_000);

        let after = checkpointed_db_bytes(&conn, &path);
        assert_eq!(
            after, full,
            "without auto_vacuum the file must not shrink — this is what the \
             startup warning is for"
        );
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
