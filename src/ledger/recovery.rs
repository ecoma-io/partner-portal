//! Recovery of requests stranded by a process that is no longer running.
//!
//! Every accepted request is written durably as `in_flight` *before* the proxy
//! contacts the upstream. If the process dies mid-request, that row survives.
//! Recovery resolves such rows to the `interrupted` terminal state and rolls
//! them up — so no accepted request is ever left without a terminal state, and
//! no request silently disappears.
//!
//! # The hard part: "stranded" is not the same as "still running"
//!
//! A rolling update runs two instances against one database. Instance A has
//! requests in flight; instance B starts and runs recovery. If recovery only
//! asks "is this row still `in_flight`?", it resolves A's live rows to
//! `interrupted` — and A's own finalize then finds the row already terminal.
//! With a non-idempotent finalize that usage is silently discarded; with the
//! idempotent one in [`crate::ledger::rollup`] it is *corrected*, but the client
//! was still told the request succeeded and the record still lies about it for
//! as long as B's write stands.
//!
//! So a row is only recovered when its owner is known to be gone. Ownership is
//! recorded per row (`usage_records.instance_id`) and liveness is decided by an
//! advisory lock the owner holds for its whole life — see
//! [`crate::ledger::instance`] for why that is exact, and for the direction the
//! remaining uncertainty is resolved in.
//!
//! Three cases, in decreasing order of confidence:
//!
//! | Owner | Verdict | Recovered |
//! |---|---|---|
//! | holds its lock | alive | never |
//! | lock is free or absent | dead | immediately |
//! | `NULL`, or unprobeable | unknown | once older than the grace period |
//!
//! The third case exists for rows written before instance ownership existed —
//! which is exactly what an in-place upgrade of a running deployment produces,
//! and where a live old-build instance must not be clobbered either.

use rusqlite::{Connection, Transaction, TransactionBehavior};
use tracing::{info, warn};

use crate::ledger::instance::{self, Liveness};
use crate::ledger::rollup::{BucketKey, Contribution};
use crate::ledger::timefmt;

/// Outcome of a recovery pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Requests that were stranded by a process that is gone, now resolved to
    /// `interrupted`.
    pub recovered: u64,
    /// Registrations of dead instances that were swept away.
    pub instances_swept: u64,
}

impl RecoveryReport {
    pub fn is_empty(&self) -> bool {
        self.recovered == 0 && self.instances_swept == 0
    }
}

/// Reason recorded on rows recovered after a process restart.
pub const INTERRUPT_REASON: &str = "owning process is gone; request was in flight";

/// Everything recovery needs to know about the instance running it.
#[derive(Debug, Clone)]
pub struct RecoveryContext<'a> {
    /// This instance. Its own rows are never recovered, and it is never probed.
    pub instance_id: String,
    /// Where the database (and therefore the instance lock files) live.
    pub db_path: &'a std::path::Path,
    /// How long a row owned by an unprobeable owner must sit before it may be
    /// recovered. See [`instance::DEFAULT_UNKNOWN_OWNER_GRACE`].
    pub unknown_owner_grace: std::time::Duration,
    /// The instant the pass runs at, so the SQL and the caller share one clock.
    pub now: time::OffsetDateTime,
}

impl<'a> RecoveryContext<'a> {
    /// A context for a running instance, with the defaults for grace and clock.
    pub fn new(instance_id: impl Into<String>, db_path: &'a std::path::Path) -> Self {
        Self {
            instance_id: instance_id.into(),
            db_path,
            unknown_owner_grace: instance::DEFAULT_UNKNOWN_OWNER_GRACE,
            now: time::OffsetDateTime::now_utc(),
        }
    }

    /// Override the grace period for owners whose liveness cannot be probed.
    pub fn with_grace(mut self, grace: std::time::Duration) -> Self {
        self.unknown_owner_grace = grace;
        self
    }

    /// Fix the clock, for tests that need a deterministic "now".
    pub fn at(mut self, now: time::OffsetDateTime) -> Self {
        self.now = now;
        self
    }

    fn old_before(&self) -> String {
        timefmt::format_ts(
            self.now - time::Duration::seconds(self.unknown_owner_grace.as_secs() as i64),
        )
    }
}

/// SQL predicate selecting the rows this pass may recover.
///
/// Kept in one place so the rows the rollup deltas are computed for and the rows
/// the update touches are provably the same set. `?1` is the grace cutoff.
const STRANDED_PREDICATE: &str = r#"
    request_status = 'in_flight' AND (
        (instance_id IS NOT NULL AND EXISTS (
            SELECT 1 FROM _pp_recoverable g
            WHERE g.instance_id = usage_records.instance_id
              AND (g.require_old = 0 OR usage_records.created_at < ?1)))
        OR (instance_id IS NULL AND created_at < ?1)
    )
"#;

/// Resolve every stranded `in_flight` request to `interrupted`.
///
/// Runs in a single `IMMEDIATE` transaction: the liveness probes, the rollup
/// deltas and the raw-row updates are applied together, so a concurrent
/// instance can never observe a half-applied recovery.
pub fn recover_in_flight(
    conn: &mut Connection,
    ctx: &RecoveryContext<'_>,
) -> Result<RecoveryReport, rusqlite::Error> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;
    let (report, swept) = recover_in_tx(&tx, ctx)?;

    if !report.is_empty() {
        tx.execute(
            "INSERT INTO ledger_meta (key, value) VALUES ('last_recovery_run', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
            rusqlite::params![timefmt::format_ts(ctx.now)],
        )?;
    }
    tx.commit()?;

    // Only after the commit: the lock files of instances we just declared dead
    // and swept. A failure here costs nothing — the next pass retries.
    for id in &swept {
        instance::remove_lock_file(ctx.db_path, id);
    }

    Ok(report)
}

/// The body of a recovery pass, inside the caller's transaction.
///
/// Returns the report and the ids of the instances that were swept, whose lock
/// files may only be removed once the sweep is committed.
fn recover_in_tx(
    tx: &Transaction<'_>,
    ctx: &RecoveryContext<'_>,
) -> Result<(RecoveryReport, Vec<String>), rusqlite::Error> {
    let dead = classify_candidates(tx, ctx)?;
    stage_recoverable(tx, &dead)?;

    let old_before = ctx.old_before();
    let stranded = select_stranded(tx, &old_before)?;

    for row in &stranded {
        // An in-flight row was never rolled up, so there is nothing to retract —
        // only the interrupted contribution to add. It counts as a failure and
        // carries no tokens, because none were ever observed; inventing zeros
        // would be a fabrication.
        Contribution {
            key: BucketKey {
                hour: timefmt::format_hour_str(&row.created_at),
                consumer_id: row.consumer_id.clone(),
                model: row.model.clone(),
                endpoint: row.endpoint.clone(),
                streaming: row.streaming,
            },
            metrics: Contribution::interrupted(row.duration_ms),
        }
        .add(tx)?;
    }

    let recovered = tx.execute(
        &format!(
            "UPDATE usage_records SET
                 request_status = 'interrupted',
                 usage_status = 'unavailable',
                 error_message = ?2
             WHERE {STRANDED_PREDICATE}"
        ),
        rusqlite::params![old_before, INTERRUPT_REASON],
    )? as u64;

    // The rows rolled up and the rows updated come from one predicate, so a
    // disagreement means a row changed under us — which must surface, not be
    // silently absorbed into the rollup.
    debug_assert_eq!(
        recovered,
        stranded.len() as u64,
        "the rollup and the update must act on the same rows"
    );

    let instances_swept = sweep_registrations(tx, &dead)?;

    if recovered > 0 {
        info!(
            recovered,
            instances_swept, "Recovered requests stranded by a process that is gone"
        );
    }

    Ok((
        RecoveryReport {
            recovered,
            instances_swept,
        },
        dead.swept_instance_ids,
    ))
}

/// One stranded row, read before anything is written.
struct StrandedRow {
    created_at: String,
    consumer_id: String,
    model: String,
    endpoint: String,
    streaming: i32,
    duration_ms: i64,
}

/// Read the rows this pass will recover.
///
/// Read up front, because the rollup delta needs the state the row had before
/// the update ran, in the same transaction.
fn select_stranded(
    tx: &Transaction<'_>,
    old_before: &str,
) -> Result<Vec<StrandedRow>, rusqlite::Error> {
    let mut stmt = tx.prepare(&format!(
        "SELECT created_at, consumer_id, model, endpoint, streaming, duration_ms
         FROM usage_records
         WHERE {STRANDED_PREDICATE}"
    ))?;

    let rows = stmt.query_map(rusqlite::params![old_before], |row| {
        Ok(StrandedRow {
            created_at: row.get(0)?,
            consumer_id: row.get(1)?,
            model: row.get(2)?,
            endpoint: row.get(3)?,
            streaming: row.get(4)?,
            duration_ms: row.get(5)?,
        })
    })?;

    rows.collect()
}

/// Candidate owners of in-flight rows, split by liveness.
#[derive(Debug, Default)]
struct DeadInstances {
    /// Owners whose lock is free: gone, so their rows are recoverable now.
    confirmed: Vec<String>,
    /// Owners whose liveness could not be determined: their rows become
    /// recoverable once they are older than the grace period.
    unknown: Vec<String>,
    /// Every id examined, for reporting.
    swept_instance_ids: Vec<String>,
}

/// Probe every instance that could own an in-flight row.
///
/// The candidate set is the union of registered instances and the distinct
/// owners actually present on in-flight rows, so an owner whose registration row
/// was lost is still classified rather than falling through to the time-based
/// rule — and a registered instance with no in-flight rows is swept promptly
/// rather than lingering.
fn classify_candidates(
    tx: &Transaction<'_>,
    ctx: &RecoveryContext<'_>,
) -> Result<DeadInstances, rusqlite::Error> {
    let mut stmt = tx.prepare(
        "SELECT instance_id FROM ledger_instances WHERE instance_id <> ?1
         UNION
         SELECT DISTINCT instance_id FROM usage_records
             WHERE request_status = 'in_flight'
               AND instance_id IS NOT NULL AND instance_id <> ?1",
    )?;

    let candidates: Vec<String> = stmt
        .query_map(rusqlite::params![ctx.instance_id], |row| row.get(0))?
        .collect::<Result<_, _>>()?;

    let mut dead = DeadInstances::default();
    for candidate in candidates {
        match instance::probe(ctx.db_path, &candidate) {
            Liveness::Alive => {}
            Liveness::Dead => {
                dead.swept_instance_ids.push(candidate.clone());
                dead.confirmed.push(candidate);
            }
            Liveness::Unknown => {
                dead.unknown.push(candidate);
            }
        }
    }
    Ok(dead)
}

/// Publish the recoverable owners to SQLite in a temporary table.
///
/// A temp table rather than a generated `IN (…)` list: the list is data, and
/// splicing data into SQL is how an injection or a parameter-limit bug gets in.
fn stage_recoverable(tx: &Transaction<'_>, dead: &DeadInstances) -> Result<(), rusqlite::Error> {
    tx.execute_batch(
        "CREATE TEMP TABLE IF NOT EXISTS _pp_recoverable (
             instance_id TEXT PRIMARY KEY,
             require_old INTEGER NOT NULL
         );
         DELETE FROM _pp_recoverable;",
    )?;

    {
        let mut insert = tx.prepare(
            "INSERT OR REPLACE INTO _pp_recoverable (instance_id, require_old) VALUES (?1, ?2)",
        )?;
        for id in &dead.confirmed {
            insert.execute(rusqlite::params![id, 0])?;
        }
        for id in &dead.unknown {
            insert.execute(rusqlite::params![id, 1])?;
        }
    }
    Ok(())
}

/// Remove registrations belonging to instances that are confirmed gone.
///
/// Unknown instances keep their registration: deleting it would demote their
/// rows to the time-based rule, which is a weaker statement than the one the
/// registration supports.
fn sweep_registrations(tx: &Transaction<'_>, dead: &DeadInstances) -> Result<u64, rusqlite::Error> {
    let mut swept = 0u64;
    for id in &dead.confirmed {
        swept += tx.execute(
            "DELETE FROM ledger_instances WHERE instance_id = ?1",
            rusqlite::params![id],
        )? as u64;
    }
    Ok(swept)
}

/// Verify that no request is left without a terminal state.
///
/// Used by tests and by the startup sequence as a self-check: after recovery
/// the count of `in_flight` rows must be zero before the server accepts traffic.
pub fn count_in_flight(conn: &Connection) -> Result<u64, rusqlite::Error> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM usage_records WHERE request_status = 'in_flight'",
        [],
        |row| row.get(0),
    )?;
    Ok(n as u64)
}

/// Assert the raw/rollup invariant: every terminal request contributes exactly
/// one to some rollup bucket. Returns the deltas, which must both be zero.
pub fn check_raw_rollup_consistency(conn: &Connection) -> Result<(i64, i64), rusqlite::Error> {
    let terminal: i64 = conn.query_row(
        "SELECT COUNT(*) FROM usage_records WHERE request_status <> 'in_flight'",
        [],
        |row| row.get(0),
    )?;
    let rolled: i64 = conn.query_row(
        "SELECT COALESCE(SUM(request_count), 0) FROM usage_hourly",
        [],
        |row| row.get(0),
    )?;
    Ok((terminal, rolled))
}

/// Warn loudly if the rollup has drifted from the raw ledger.
///
/// Run once, in the background, shortly after startup. It is deliberately not
/// on the startup path: it counts every terminal row, which on a database at the
/// retention limit is a full scan, and a diagnostic must not delay a listener
/// that is already correct. It is also not run on the recovery sweep, for the
/// same reason.
pub fn audit_consistency(conn: &Connection) {
    match check_raw_rollup_consistency(conn) {
        Ok((terminal, rolled)) if terminal != rolled => {
            warn!(
                terminal,
                rolled,
                drift = terminal - rolled,
                "Raw ledger and hourly rollup disagree"
            );
        }
        Ok(_) => info!("Raw ledger and hourly rollup are consistent"),
        Err(e) => warn!(error = %e, "Failed to audit ledger consistency"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::instance::InstanceGuard;
    use crate::ledger::{Endpoint, LedgerWriter, LedgerWriterConfig, RequestRecord, Usage};
    use parking_lot::Mutex;
    use std::path::Path;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn setup(path: &Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        conn
    }

    /// A recovery context that treats unowned rows as immediately recoverable.
    ///
    /// The unit tests below insert rows with no owner, standing in for requests
    /// written before instance ownership existed. A zero grace says "assume the
    /// grace has already elapsed", which is the honest way to exercise the
    /// time-based rule without making the test sleep.
    ///
    /// The clock is pinned rather than read from the wall. The unowned-row rule
    /// is a comparison against `now`, so a test that inserts a fixed timestamp
    /// and asks a moving clock about it passes or fails depending on the day it
    /// is run — which is exactly what happened when these rows were written with
    /// a timestamp that later turned out to be in the future. Pinning both ends
    /// of the comparison is what makes the rule testable.
    fn ctx<'a>(path: &'a Path) -> RecoveryContext<'a> {
        RecoveryContext::new("test-instance", path)
            .with_grace(Duration::ZERO)
            .at(PINNED_NOW)
    }

    /// Later than every fixed timestamp the tests insert, and never the wall
    /// clock, so "old enough" is a property of the fixture and not of the day.
    const PINNED_NOW: time::OffsetDateTime = time::macros::datetime!(2026-09-24 12:00:00 UTC);

    fn insert_stranded(conn: &Connection, request_id: &str, ts: &str, consumer: &str) {
        conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, duration_ms, usage_status
             ) VALUES (?1, ?2, ?3, 'm', 'chat_completions', 0, 'in_flight', 0, 'unavailable')",
            rusqlite::params![request_id, ts, consumer],
        )
        .unwrap();
    }

    #[test]
    fn test_recovery_resolves_in_flight_to_interrupted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);

        // Simulate a process that died mid-request: an accept was committed,
        // but no finalize ever landed.
        conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, duration_ms, usage_status
             ) VALUES ('req-x', '2026-09-24T07:12:33.000000000Z', 'c1', 'gpt-4',
                       'chat_completions', 1, 'in_flight', 0, 'unavailable')",
            [],
        )
        .unwrap();
        assert_eq!(count_in_flight(&conn).unwrap(), 1);

        let report = recover_in_flight(&mut conn, &ctx(&path)).unwrap();
        assert_eq!(report.recovered, 1);
        assert!(!report.is_empty());
        assert_eq!(count_in_flight(&conn).unwrap(), 0);

        let (status, usage_status, err): (String, String, String) = conn
            .query_row(
                "SELECT request_status, usage_status, error_message
                 FROM usage_records WHERE request_id='req-x'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "interrupted");
        assert_eq!(usage_status, "unavailable");
        assert_eq!(err, INTERRUPT_REASON);

        // Recovered requests are rolled up as failures, with no fabricated tokens.
        let (count, failures, tokens): (i64, i64, i64) = conn
            .query_row(
                "SELECT request_count, failure_count, total_input_tokens
                 FROM usage_hourly WHERE consumer_id='c1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(count, 1);
        assert_eq!(failures, 1);
        assert_eq!(tokens, 0);
    }

    #[test]
    fn test_recovery_is_idempotent() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);
        insert_stranded(&conn, "req-y", "2026-09-24T07:12:33.000000000Z", "c1");

        assert_eq!(
            recover_in_flight(&mut conn, &ctx(&path)).unwrap().recovered,
            1
        );
        // A second pass finds nothing and must not double-count the rollup.
        assert_eq!(
            recover_in_flight(&mut conn, &ctx(&path)).unwrap().recovered,
            0
        );

        let (terminal, rolled) = check_raw_rollup_consistency(&conn).unwrap();
        assert_eq!(terminal, 1);
        assert_eq!(rolled, 1, "rollup must not be applied twice");
    }

    #[test]
    fn test_recovery_noop_when_nothing_in_flight() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);
        assert_eq!(
            recover_in_flight(&mut conn, &ctx(&path)).unwrap().recovered,
            0
        );
    }

    #[test]
    fn test_recovery_handles_multiple_hours_and_consumers() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);

        // Three records, three distinct rollup buckets: two consumers share an
        // hour, and one crosses the hour boundary. A rollup keyed on the hour
        // alone, or on the consumer alone, would produce fewer.
        for (i, (ts, consumer)) in [
            ("2026-09-24T07:00:00.000000000Z", "a"),
            ("2026-09-24T07:59:59.000000000Z", "b"),
            ("2026-09-24T08:00:00.000000000Z", "b"),
        ]
        .iter()
        .enumerate()
        {
            insert_stranded(&conn, &format!("r{i}"), ts, consumer);
        }

        assert_eq!(
            recover_in_flight(&mut conn, &ctx(&path)).unwrap().recovered,
            3
        );

        let buckets: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_hourly", [], |r| r.get(0))
            .unwrap();
        assert_eq!(buckets, 3, "hour boundaries must not be merged");

        let (terminal, rolled) = check_raw_rollup_consistency(&conn).unwrap();
        assert_eq!(terminal, rolled);
    }

    /// The defect this module exists to prevent.
    ///
    /// A live sibling's in-flight row must survive recovery untouched. If it did
    /// not, the sibling would later finalize a row that was already terminal and
    /// the client's successful request would be recorded as interrupted.
    #[test]
    fn test_a_live_instances_in_flight_row_is_never_recovered() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);

        // A sibling instance is running and holds its lock.
        let sibling = InstanceGuard::acquire(&conn, &path).unwrap();
        conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, instance_id, duration_ms, usage_status
             ) VALUES ('live-1', '2026-09-24T07:00:00.000000000Z', 'c1', 'm',
                       'chat_completions', 0, 'in_flight', ?1, 0, 'unavailable')",
            rusqlite::params![sibling.id()],
        )
        .unwrap();

        let report = recover_in_flight(&mut conn, &ctx(&path)).unwrap();
        assert_eq!(
            report.recovered, 0,
            "a live instance's in-flight row must not be touched"
        );
        assert_eq!(count_in_flight(&conn).unwrap(), 1);
        assert_eq!(
            sibling_registration_count(&conn),
            1,
            "a live instance's registration must not be swept"
        );

        // The row is still the sibling's to resolve, and it can be.
        assert_eq!(
            status_of(&conn, "live-1"),
            "in_flight",
            "the row must still be in flight for its owner to finalize"
        );
        drop(sibling);
    }

    /// After the sibling dies — by any means, since the kernel releases its lock
    /// — its rows become recoverable without waiting out a grace period.
    #[test]
    fn test_a_dead_instances_row_is_recovered_immediately() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);

        let id = {
            let sibling = InstanceGuard::acquire(&conn, &path).unwrap();
            conn.execute(
                "INSERT INTO usage_records (
                    request_id, created_at, consumer_id, model, endpoint, streaming,
                    request_status, instance_id, duration_ms, usage_status
                 ) VALUES ('dead-1', '2026-09-24T07:00:00.000000000Z', 'c1', 'm',
                           'chat_completions', 0, 'in_flight', ?1, 0, 'unavailable')",
                rusqlite::params![sibling.id()],
            )
            .unwrap();
            sibling.id().to_string()
        };

        assert_eq!(
            instance::probe(&path, &id),
            Liveness::Dead,
            "dropping the guard must be observable as death"
        );

        let report = recover_in_flight(&mut conn, &ctx(&path)).unwrap();
        assert_eq!(report.recovered, 1);
        assert_eq!(report.instances_swept, 1);
        assert_eq!(count_in_flight(&conn).unwrap(), 0);
        assert_eq!(sibling_registration_count(&conn), 0);
        assert_eq!(
            status_of(&conn, "dead-1"),
            "interrupted",
            "a stranded row must reach the interrupted terminal state"
        );
    }

    /// Rows from a build that predates instance ownership are only recovered
    /// once they are old enough to rule out a live owner.
    #[test]
    fn test_an_unowned_row_is_not_recovered_before_the_grace_period() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);
        let now = timefmt::now();

        conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, duration_ms, usage_status
             ) VALUES ('legacy-1', ?1, 'c1', 'm', 'chat_completions', 0,
                       'in_flight', 0, 'unavailable')",
            rusqlite::params![timefmt::format_ts(now)],
        )
        .unwrap();

        // A fresh row could belong to a live old-build instance mid-request.
        let report = recover_in_flight(
            &mut conn,
            &RecoveryContext::new("me", &path)
                .with_grace(Duration::from_secs(30))
                .at(now),
        )
        .unwrap();
        assert_eq!(
            report.recovered, 0,
            "an unowned row must not be recovered while it could still be live"
        );

        // Once it is demonstrably older than any live owner's request, it is.
        let report = recover_in_flight(
            &mut conn,
            &RecoveryContext::new("me", &path)
                .with_grace(Duration::from_secs(30))
                .at(now + time::Duration::seconds(31)),
        )
        .unwrap();
        assert_eq!(report.recovered, 1);
    }

    #[test]
    fn test_recovery_never_touches_its_own_instances_rows() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);

        let guard = InstanceGuard::acquire(&conn, &path).unwrap();
        conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, instance_id, duration_ms, usage_status
             ) VALUES ('mine-1', '2026-09-24T07:00:00.000000000Z', 'c1', 'm',
                       'chat_completions', 0, 'in_flight', ?1, 0, 'unavailable')",
            rusqlite::params![guard.id()],
        )
        .unwrap();

        let report =
            recover_in_flight(&mut conn, &RecoveryContext::new(guard.id(), &path)).unwrap();
        assert_eq!(report.recovered, 0);
        assert_eq!(count_in_flight(&conn).unwrap(), 1);
        assert_eq!(sibling_registration_count(&conn), 1);
    }

    fn sibling_registration_count(conn: &Connection) -> i64 {
        conn.query_row("SELECT COUNT(*) FROM ledger_instances", [], |r| r.get(0))
            .unwrap()
    }

    fn status_of(conn: &Connection, request_id: &str) -> String {
        conn.query_row(
            "SELECT request_status FROM usage_records WHERE request_id = ?1",
            rusqlite::params![request_id],
            |r| r.get(0),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_crash_recovery_end_to_end_with_real_writer() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");

        // Instance 1: accept several requests, then drop the writer without
        // finalizing them (an abrupt process death).
        {
            let conn = Arc::new(Mutex::new(setup(&path)));
            let writer = LedgerWriter::new(
                conn,
                LedgerWriterConfig {
                    // The rows this writer leaves behind must be attributable to
                    // an owner recovery can probe, or the test would be
                    // exercising the unowned-row rule instead of the dead-owner
                    // rule it is named for.
                    instance_id: Some("dead-instance".to_string()),
                    queue_size: 100,
                    batch_size: 1,
                    batch_timeout_ms: 1,
                },
            );
            for i in 0..3 {
                writer
                    .accept(RequestRecord::new(
                        format!("crash-{i}"),
                        "c".into(),
                        "m".into(),
                        Endpoint::ChatCompletions,
                        false,
                    ))
                    .await
                    .unwrap();
            }
            // Simulate death: never finalize, never shutdown cleanly.
            std::mem::forget(writer);
        }

        // Instance 2 starts up and recovers.
        let mut conn = setup(&path);
        assert_eq!(count_in_flight(&conn).unwrap(), 3);
        let report = recover_in_flight(&mut conn, &ctx(&path)).unwrap();
        assert_eq!(report.recovered, 3);

        let (terminal, rolled) = check_raw_rollup_consistency(&conn).unwrap();
        assert_eq!(terminal, 3);
        assert_eq!(rolled, 3);
        assert_eq!(count_in_flight(&conn).unwrap(), 0);

        // Every recovered request has a deterministic terminal state.
        let interrupted: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM usage_records WHERE request_status='interrupted'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(interrupted, 3);

        drop(conn);
        let _ = tokio::time::sleep(Duration::from_millis(1)).await;
    }

    #[test]
    fn test_clean_completion_is_not_touched_by_recovery() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);

        // A completed record must survive recovery untouched, and its rollup
        // must not be double-counted by a later recovery pass.
        let mut record = RequestRecord::new(
            "done-1".into(),
            "c".into(),
            "m".into(),
            Endpoint::Responses,
            false,
        );
        record.complete(200, Usage::new(Some(5), Some(6), None), 7);

        let tx = conn.transaction().unwrap();
        crate::ledger::writer::finalize_in_tx(&tx, &record).unwrap();
        tx.commit().unwrap();

        assert_eq!(
            recover_in_flight(&mut conn, &ctx(&path)).unwrap().recovered,
            0
        );
        let (terminal, rolled) = check_raw_rollup_consistency(&conn).unwrap();
        assert_eq!(terminal, 1);
        assert_eq!(rolled, 1);
    }
}
