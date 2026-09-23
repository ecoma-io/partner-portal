//! Crash recovery for the ledger.
//!
//! Every accepted request is written durably as `in_flight` *before* the proxy
//! contacts the upstream. If the process dies mid-request, that row survives.
//! On the next startup this module resolves every leftover `in_flight` row to
//! the `interrupted` terminal state and rolls it up — so no accepted request is
//! ever left without a terminal state, and no request silently disappears.
//!
//! Recovery is idempotent: it only ever looks at rows still marked `in_flight`,
//! and both the status transition and the rollup happen in one transaction.

use rusqlite::{Connection, TransactionBehavior};
use tracing::{info, warn};

/// Outcome of a recovery pass.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Requests that were in flight when the previous process died, now
    /// resolved to `interrupted`.
    pub recovered: u64,
}

impl RecoveryReport {
    pub fn is_empty(&self) -> bool {
        self.recovered == 0
    }
}

/// Reason recorded on rows recovered after a process restart.
pub const INTERRUPT_REASON: &str = "process restarted while request was in flight";

/// Resolve every leftover `in_flight` request to `interrupted`.
///
/// Runs in a single `IMMEDIATE` transaction so a concurrent instance (same-VPS
/// rolling update) cannot interleave a half-applied recovery.
pub fn recover_in_flight(conn: &mut Connection) -> Result<RecoveryReport, rusqlite::Error> {
    let tx = conn.transaction_with_behavior(TransactionBehavior::Immediate)?;

    let in_flight: i64 = tx.query_row(
        "SELECT COUNT(*) FROM usage_records WHERE request_status = 'in_flight'",
        [],
        |row| row.get(0),
    )?;

    if in_flight == 0 {
        tx.commit()?;
        return Ok(RecoveryReport::default());
    }

    // Roll up first: after the UPDATE below the rows are no longer selectable
    // as `in_flight`, and the rollup needs their original state. Interrupted
    // requests count as failures and contribute no tokens, because no usage was
    // ever observed for them — inventing zeros would be a fabrication.
    tx.execute(
        r#"
        INSERT INTO usage_hourly (
            hour, consumer_id, model, endpoint, streaming,
            request_count, total_input_tokens, total_output_tokens,
            total_cached_tokens, total_duration_ms, total_ttft_ms,
            ttft_count, success_count, failure_count
        )
        SELECT
            substr(created_at, 1, 13), consumer_id, model, endpoint, streaming,
            1, 0, 0, 0, duration_ms, 0, 0, 0, 1
        FROM usage_records
        WHERE request_status = 'in_flight'
        ON CONFLICT(hour, consumer_id, model, endpoint, streaming) DO UPDATE SET
            request_count = request_count + 1,
            total_duration_ms = total_duration_ms + excluded.total_duration_ms,
            failure_count = failure_count + 1
        "#,
        [],
    )?;

    tx.execute(
        r#"
        UPDATE usage_records SET
            request_status = 'interrupted',
            usage_status = 'unavailable',
            error_message = ?1
        WHERE request_status = 'in_flight'
        "#,
        rusqlite::params![INTERRUPT_REASON],
    )?;

    let recovered = tx.changes();

    tx.execute(
        "INSERT INTO ledger_meta (key, value) VALUES ('last_recovery_run', ?1)
         ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        rusqlite::params![crate::ledger::writer::format_timestamp(
            time::OffsetDateTime::now_utc()
        )],
    )?;

    tx.commit()?;

    info!(
        recovered,
        "Recovered in-flight requests left by a previous process"
    );
    Ok(RecoveryReport { recovered })
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

/// Warn loudly if the rollup has drifted from the raw ledger. Called at startup
/// after recovery.
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
    use crate::ledger::{Endpoint, LedgerWriter, LedgerWriterConfig, RequestRecord, Usage};
    use parking_lot::Mutex;
    use std::sync::Arc;
    use std::time::Duration;
    use tempfile::TempDir;

    fn setup(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        conn
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
             ) VALUES ('req-x', '2026-09-24T07:12:33.000Z', 'c1', 'gpt-4',
                       'chat_completions', 1, 'in_flight', 0, 'unavailable')",
            [],
        )
        .unwrap();
        assert_eq!(count_in_flight(&conn).unwrap(), 1);

        let report = recover_in_flight(&mut conn).unwrap();
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

        conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, duration_ms, usage_status
             ) VALUES ('req-y', '2026-09-24T07:12:33.000Z', 'c1', 'gpt-4',
                       'chat_completions', 0, 'in_flight', 0, 'unavailable')",
            [],
        )
        .unwrap();

        assert_eq!(recover_in_flight(&mut conn).unwrap().recovered, 1);
        // A second pass finds nothing and must not double-count the rollup.
        assert_eq!(recover_in_flight(&mut conn).unwrap().recovered, 0);

        let (terminal, rolled) = check_raw_rollup_consistency(&conn).unwrap();
        assert_eq!(terminal, 1);
        assert_eq!(rolled, 1, "rollup must not be applied twice");
    }

    #[test]
    fn test_recovery_noop_when_nothing_in_flight() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let mut conn = setup(&path);
        assert_eq!(recover_in_flight(&mut conn).unwrap().recovered, 0);
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
            ("2026-09-24T07:00:00.000Z", "a"),
            ("2026-09-24T07:59:59.000Z", "b"),
            ("2026-09-24T08:00:00.000Z", "b"),
        ]
        .iter()
        .enumerate()
        {
            conn.execute(
                "INSERT INTO usage_records (
                    request_id, created_at, consumer_id, model, endpoint, streaming,
                    request_status, duration_ms, usage_status
                 ) VALUES (?1, ?2, ?3, 'm', 'chat_completions', 0, 'in_flight', 0, 'unavailable')",
                rusqlite::params![format!("r{i}"), ts, consumer],
            )
            .unwrap();
        }

        assert_eq!(recover_in_flight(&mut conn).unwrap().recovered, 3);

        let buckets: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_hourly", [], |r| r.get(0))
            .unwrap();
        assert_eq!(buckets, 3, "hour boundaries must not be merged");

        let (terminal, rolled) = check_raw_rollup_consistency(&conn).unwrap();
        assert_eq!(terminal, rolled);
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
        let report = recover_in_flight(&mut conn).unwrap();
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

        assert_eq!(recover_in_flight(&mut conn).unwrap().recovered, 0);
        let (terminal, rolled) = check_raw_rollup_consistency(&conn).unwrap();
        assert_eq!(terminal, 1);
        assert_eq!(rolled, 1);
    }
}
