//! The hourly rollup, expressed as retractable contributions.
//!
//! `usage_hourly` is derived from `usage_records`, and the two must never
//! disagree. The obvious implementation — "add to the bucket when a row becomes
//! terminal, and never touch it again" — holds only as long as nothing ever
//! writes the same request twice. That assumption is exactly what gets violated
//! in practice: a finalize is retried, a drop guard races the normal path, a
//! rolling update's recovery resolves a row that its owner was still finishing.
//!
//! So the arithmetic is stated once, as a **contribution**: the exact amount one
//! request adds to one bucket. Finalize is then expressed as
//!
//! ```text
//!   retract(previous contribution of this row)  ->  add(new contribution)
//! ```
//!
//! which is *idempotent by construction* — applying the same finalize twice
//! subtracts and re-adds the same numbers — and *self-correcting*: a row that
//! reaches the wrong terminal state is corrected rather than ignored, instead of
//! the old behaviour where a second finalize was silently dropped along with the
//! real usage it carried.
//!
//! Both operations take the caller's transaction. The raw row and its rollup
//! delta therefore commit together or not at all.

use rusqlite::{Transaction, params};

use crate::ledger::types::{RequestRecord, RequestStatus};

/// The bucket a request rolls up into.
///
/// The column set matches `usage_hourly`'s UNIQUE constraint, so a bucket is
/// identified by a value that actually exists in the schema rather than by a
/// parallel notion of identity that could drift from it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BucketKey {
    pub hour: String,
    pub consumer_id: String,
    pub model: String,
    pub endpoint: String,
    pub streaming: i32,
}

impl BucketKey {
    /// The bucket for a request, from the timestamp it was accepted at.
    ///
    /// The *accept* timestamp is used, never the completion time: a request that
    /// starts at 09:59 and finishes at 10:01 belongs to the hour it was served
    /// in, and using the completion time would let a retracted contribution land
    /// in a different bucket from the one it was added to.
    pub fn from_record(record: &RequestRecord) -> Self {
        Self {
            hour: crate::ledger::timefmt::format_hour(record.created_at),
            consumer_id: record.consumer_id.clone(),
            model: record.model.clone(),
            endpoint: record.endpoint.as_str().to_string(),
            streaming: i32::from(record.streaming),
        }
    }
}

/// What one request adds to its bucket.
///
/// Tokens are the *known* values: an unavailable usage contributes 0 to the sum
/// while the raw row keeps `NULL` and `usage_status = 'unavailable'`, so no
/// consumer can mistake the rollup's zero for a real zero-token request.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct Metrics {
    pub input_tokens: i64,
    pub output_tokens: i64,
    pub cached_tokens: i64,
    pub duration_ms: i64,
    pub ttft_ms: i64,
    pub ttft_count: i32,
    pub success_count: i32,
    pub failure_count: i32,
}

/// One request's effect on one bucket.
#[derive(Debug, Clone)]
pub struct Contribution {
    pub key: BucketKey,
    pub metrics: Metrics,
}

impl Contribution {
    /// The contribution of a terminal record, into the bucket `key`.
    ///
    /// `key` is passed in rather than derived so that a finalize can keep a
    /// row's *stored* bucket. A request's consumer, model, endpoint and hour are
    /// written once at accept and never updated, so re-deriving the key from the
    /// incoming record would be equivalent — but only by luck. Using the stored
    /// key makes the retract and the add provably target the same bucket, which
    /// is what keeps the rollup a running sum rather than a running guess.
    pub fn terminal(record: &RequestRecord, key: BucketKey) -> Self {
        Self {
            key,
            metrics: Metrics {
                input_tokens: record.usage.input_tokens.unwrap_or(0) as i64,
                output_tokens: record.usage.output_tokens.unwrap_or(0) as i64,
                cached_tokens: record.usage.cached_tokens.unwrap_or(0) as i64,
                duration_ms: record.duration_ms as i64,
                ttft_ms: record.ttft_ms.unwrap_or(0) as i64,
                ttft_count: i32::from(record.ttft_ms.is_some()),
                success_count: i32::from(record.request_status == RequestStatus::Completed),
                failure_count: i32::from(matches!(
                    record.request_status,
                    RequestStatus::Failed | RequestStatus::Interrupted
                )),
            },
        }
    }

    /// The contribution of a request stranded mid-flight and recovered as
    /// interrupted: a failure that consumed no observable tokens.
    pub fn interrupted(duration_ms: i64) -> Metrics {
        Metrics {
            failure_count: 1,
            duration_ms,
            ..Metrics::default()
        }
    }

    /// Apply this contribution to its bucket, creating it if needed.
    pub fn add(&self, tx: &Transaction<'_>) -> Result<(), rusqlite::Error> {
        let m = &self.metrics;
        tx.execute(
            r#"
            INSERT INTO usage_hourly (
                hour, consumer_id, model, endpoint, streaming,
                request_count, total_input_tokens, total_output_tokens,
                total_cached_tokens, total_duration_ms, total_ttft_ms,
                ttft_count, success_count, failure_count
            ) VALUES (?1, ?2, ?3, ?4, ?5, 1, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13)
            ON CONFLICT(hour, consumer_id, model, endpoint, streaming) DO UPDATE SET
                request_count = request_count + 1,
                total_input_tokens = total_input_tokens + ?6,
                total_output_tokens = total_output_tokens + ?7,
                total_cached_tokens = total_cached_tokens + ?8,
                total_duration_ms = total_duration_ms + ?9,
                total_ttft_ms = total_ttft_ms + ?10,
                ttft_count = ttft_count + ?11,
                success_count = success_count + ?12,
                failure_count = failure_count + ?13
            "#,
            params![
                self.key.hour,
                self.key.consumer_id,
                self.key.model,
                self.key.endpoint,
                self.key.streaming,
                m.input_tokens,
                m.output_tokens,
                m.cached_tokens,
                m.duration_ms,
                m.ttft_ms,
                m.ttft_count,
                m.success_count,
                m.failure_count,
            ],
        )?;
        Ok(())
    }

    /// Undo this contribution.
    ///
    /// Retraction is only ever applied to a contribution that was added, so a
    /// bucket's counters cannot go below zero. A bucket that empties out is
    /// deleted rather than left as a zeroed row, so `SUM(request_count)` over
    /// `usage_hourly` stays an exact count of terminal raw rows and an emptied
    /// hour does not reappear on the dashboard as a real bucket with no traffic.
    pub fn retract(&self, tx: &Transaction<'_>) -> Result<(), rusqlite::Error> {
        let m = &self.metrics;
        tx.execute(
            r#"
            UPDATE usage_hourly SET
                request_count = request_count - 1,
                total_input_tokens = total_input_tokens - ?6,
                total_output_tokens = total_output_tokens - ?7,
                total_cached_tokens = total_cached_tokens - ?8,
                total_duration_ms = total_duration_ms - ?9,
                total_ttft_ms = total_ttft_ms - ?10,
                ttft_count = ttft_count - ?11,
                success_count = success_count - ?12,
                failure_count = failure_count - ?13
            WHERE hour = ?1 AND consumer_id = ?2 AND model = ?3
              AND endpoint = ?4 AND streaming = ?5
            "#,
            params![
                self.key.hour,
                self.key.consumer_id,
                self.key.model,
                self.key.endpoint,
                self.key.streaming,
                m.input_tokens,
                m.output_tokens,
                m.cached_tokens,
                m.duration_ms,
                m.ttft_ms,
                m.ttft_count,
                m.success_count,
                m.failure_count,
            ],
        )?;

        tx.execute(
            "DELETE FROM usage_hourly
             WHERE hour = ?1 AND consumer_id = ?2 AND model = ?3
               AND endpoint = ?4 AND streaming = ?5 AND request_count <= 0",
            params![
                self.key.hour,
                self.key.consumer_id,
                self.key.model,
                self.key.endpoint,
                self.key.streaming,
            ],
        )?;
        Ok(())
    }
}

/// A stored raw row, as the rollup needs to see it.
///
/// Loaded before a finalize writes anything, because it describes the state the
/// rollup currently reflects — the state that has to be retracted first.
#[derive(Debug, Clone)]
pub struct StoredRow {
    pub status: RequestStatus,
    pub key: BucketKey,
    pub contribution: Metrics,
}

impl StoredRow {
    /// Read the rollup-relevant columns of a raw row, if it exists.
    pub fn load(
        tx: &Transaction<'_>,
        request_id: &str,
    ) -> Result<Option<StoredRow>, rusqlite::Error> {
        let row = tx
            .query_row(
                "SELECT created_at, consumer_id, model, endpoint, streaming,
                        request_status, duration_ms, ttft_ms,
                        input_tokens, output_tokens, cached_tokens
                 FROM usage_records WHERE request_id = ?1",
                params![request_id],
                |row| {
                    Ok((
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, String>(2)?,
                        row.get::<_, String>(3)?,
                        row.get::<_, i32>(4)?,
                        row.get::<_, String>(5)?,
                        row.get::<_, i64>(6)?,
                        row.get::<_, Option<i64>>(7)?,
                        row.get::<_, Option<i64>>(8)?,
                        row.get::<_, Option<i64>>(9)?,
                        row.get::<_, Option<i64>>(10)?,
                    ))
                },
            )
            .map(Some)
            .or_else(|e| match e {
                rusqlite::Error::QueryReturnedNoRows => Ok(None),
                other => Err(other),
            })?;

        Ok(row.map(
            |(
                created_at,
                consumer_id,
                model,
                endpoint,
                streaming,
                status,
                duration,
                ttft,
                input,
                output,
                cached,
            )| {
                let status = RequestStatus::from_str_lossy(&status);
                StoredRow {
                    status,
                    key: BucketKey {
                        hour: crate::ledger::timefmt::format_hour_str(&created_at),
                        consumer_id,
                        model,
                        endpoint,
                        streaming,
                    },
                    contribution: Metrics {
                        input_tokens: input.unwrap_or(0),
                        output_tokens: output.unwrap_or(0),
                        cached_tokens: cached.unwrap_or(0),
                        duration_ms: duration,
                        ttft_ms: ttft.unwrap_or(0),
                        ttft_count: i32::from(ttft.is_some()),
                        success_count: i32::from(status == RequestStatus::Completed),
                        failure_count: i32::from(matches!(
                            status,
                            RequestStatus::Failed | RequestStatus::Interrupted
                        )),
                    },
                }
            },
        ))
    }

    /// The rollup delta this row is currently responsible for.
    ///
    /// `None` for an `in_flight` row: nothing was ever rolled up for it, because
    /// only terminal states are. This is what makes the first finalize a pure
    /// `add`, with no retraction, and every later one a `retract` then `add`.
    pub fn contribution(&self) -> Option<Contribution> {
        self.status.is_terminal().then(|| Contribution {
            key: self.key.clone(),
            metrics: self.contribution,
        })
    }

    /// The hour bucket this row belongs to, for a caller that must keep using it.
    pub fn key(&self) -> &BucketKey {
        &self.key
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::types::{Endpoint, Usage};
    use rusqlite::Connection;
    use tempfile::TempDir;

    fn setup(path: &std::path::Path) -> Connection {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        conn
    }

    /// The instant every test record is accepted at.
    ///
    /// Pinned, not `now`: the bucket name is derived from the accept time, so a
    /// test that asserts on `2026-09-24T07` while the record carries the wall
    /// clock only passes on the morning of that day. Both the fixture row and the
    /// in-memory record use this same value, which is also what the production
    /// path does — a record's `created_at` is what `insert_accept` writes.
    const ACCEPTED_AT: time::OffsetDateTime = time::macros::datetime!(2026-09-24 07:00:00 UTC);

    fn insert_raw(conn: &Connection, request_id: &str, status: &str) {
        conn.execute(
            "INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, duration_ms, usage_status
             ) VALUES (?1, '2026-09-24T07:00:00.000000000Z', 'c1', 'gpt-4',
                       'chat_completions', 0, ?2, 5, 'unavailable')",
            params![request_id, status],
        )
        .unwrap();
    }

    fn bucket(conn: &Connection) -> (i64, i64, i64, i64, i64) {
        conn.query_row(
            "SELECT request_count, success_count, failure_count,
                    total_input_tokens, total_duration_ms
             FROM usage_hourly WHERE hour = '2026-09-24T07'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)),
        )
        .unwrap()
    }

    fn record(id: &str, input: u64, output: u64) -> RequestRecord {
        let mut record = RequestRecord::new(
            id.to_string(),
            "c1".to_string(),
            "gpt-4".to_string(),
            Endpoint::ChatCompletions,
            false,
        );
        record.created_at = ACCEPTED_AT;
        record.complete(200, Usage::new(Some(input), Some(output), None), 5);
        record
    }

    #[test]
    fn test_add_then_retract_is_a_no_op() {
        let temp = TempDir::new().unwrap();
        let mut conn = setup(&temp.path().join("t.db"));
        insert_raw(&conn, "r1", "in_flight");

        let entry = Contribution::terminal(
            &record("r1", 10, 4),
            BucketKey::from_record(&record("r1", 10, 4)),
        );

        let tx = conn.transaction().unwrap();
        entry.add(&tx).unwrap();
        assert_eq!(bucket(&tx), (1, 1, 0, 10, 5));
        entry.retract(&tx).unwrap();
        // The bucket empties out completely, so it is removed rather than left
        // as a zeroed row that would show up as a real hour with no traffic.
        let remaining: i64 = tx
            .query_row("SELECT COUNT(*) FROM usage_hourly", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 0);
        tx.commit().unwrap();
    }

    #[test]
    fn test_retract_keeps_other_contributions_in_the_bucket() {
        let temp = TempDir::new().unwrap();
        let mut conn = setup(&temp.path().join("t.db"));

        let first = Contribution::terminal(
            &record("r1", 10, 4),
            BucketKey::from_record(&record("r1", 10, 4)),
        );
        let second = Contribution::terminal(
            &record("r2", 7, 3),
            BucketKey::from_record(&record("r2", 7, 3)),
        );

        let tx = conn.transaction().unwrap();
        first.add(&tx).unwrap();
        second.add(&tx).unwrap();
        assert_eq!(bucket(&tx), (2, 2, 0, 17, 10));
        first.retract(&tx).unwrap();
        assert_eq!(
            bucket(&tx),
            (1, 1, 0, 7, 5),
            "retracting one contribution must leave the others intact"
        );
        tx.commit().unwrap();
    }

    #[test]
    fn test_retracting_an_absent_bucket_invents_nothing() {
        // A retraction for a bucket that does not exist must not create one with
        // negative counters.
        let temp = TempDir::new().unwrap();
        let mut conn = setup(&temp.path().join("t.db"));

        let entry = Contribution::terminal(
            &record("ghost", 10, 4),
            BucketKey::from_record(&record("ghost", 10, 4)),
        );
        let tx = conn.transaction().unwrap();
        entry.retract(&tx).unwrap();

        let remaining: i64 = tx
            .query_row("SELECT COUNT(*) FROM usage_hourly", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 0);
        tx.commit().unwrap();
    }

    #[test]
    fn test_stored_row_of_an_in_flight_request_has_no_contribution() {
        let temp = TempDir::new().unwrap();
        let mut conn = setup(&temp.path().join("t.db"));
        insert_raw(&conn, "r1", "in_flight");

        let tx = conn.transaction().unwrap();
        let stored = StoredRow::load(&tx, "r1").unwrap().unwrap();
        assert_eq!(stored.status, RequestStatus::InFlight);
        assert!(
            stored.contribution().is_none(),
            "an in-flight row was never rolled up, so it has nothing to retract"
        );
        tx.commit().unwrap();
    }

    #[test]
    fn test_stored_row_of_a_completed_request_matches_what_was_added() {
        let temp = TempDir::new().unwrap();
        let mut conn = setup(&temp.path().join("t.db"));
        insert_raw(&conn, "r1", "in_flight");

        let terminal = record("r1", 10, 4);
        let key = BucketKey::from_record(&terminal);
        let added = Contribution::terminal(&terminal, key.clone());

        let tx = conn.transaction().unwrap();
        added.add(&tx).unwrap();
        tx.execute(
            "UPDATE usage_records SET request_status = 'completed', input_tokens = 10,
                output_tokens = 4, duration_ms = 5, usage_status = 'available'
             WHERE request_id = 'r1'",
            [],
        )
        .unwrap();

        let stored = StoredRow::load(&tx, "r1").unwrap().unwrap();
        let retract = stored
            .contribution()
            .expect("a terminal row owes a retraction");
        assert_eq!(
            retract.metrics, added.metrics,
            "the stored row must describe exactly the contribution that was added"
        );
        assert_eq!(retract.key, added.key);

        // And retracting it really does empty the bucket.
        retract.retract(&tx).unwrap();
        let remaining: i64 = tx
            .query_row("SELECT COUNT(*) FROM usage_hourly", [], |r| r.get(0))
            .unwrap();
        assert_eq!(remaining, 0);
        tx.commit().unwrap();
    }

    #[test]
    fn test_unknown_status_loads_as_interrupted_a_failure() {
        let temp = TempDir::new().unwrap();
        let mut conn = setup(&temp.path().join("t.db"));

        // The schema's CHECK constraint is the first line of defence and rejects
        // this write outright, which is asserted separately in `ledger::tests`.
        // Here the question is the second line: a row that carries a status this
        // build does not recognise — written by a newer build, or by anything
        // else sharing the file — must read back as a *failure*, never as a
        // success-shaped state. Disabling the constraint is how such a row is
        // produced at all.
        conn.execute_batch("PRAGMA ignore_check_constraints = ON;")
            .unwrap();
        insert_raw(&conn, "r1", "weird");

        let tx = conn.transaction().unwrap();
        let stored = StoredRow::load(&tx, "r1").unwrap().unwrap();
        assert_eq!(stored.status, RequestStatus::Interrupted);
        let contribution = stored.contribution().unwrap();
        assert_eq!(contribution.metrics.failure_count, 1);
        assert_eq!(contribution.metrics.success_count, 0);
        tx.commit().unwrap();
    }
}
