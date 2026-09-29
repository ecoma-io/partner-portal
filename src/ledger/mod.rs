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
/// * 4 — bounded `usage_records.error_body` for non-2xx upstream responses
/// * 5 — `api_keys`: partner API keys move out of `config.yaml` and into the
///   database (ADR 0014). A new table needs no data migration, so this is
///   additive in both directions: an older binary ignores it, and a database
///   created by an older build gains an empty one.
/// * 6 — billing: `partners`, `partner_models`, `daily_statements`,
///   `statement_lines`, the three price-snapshot columns on `usage_records`,
///   and a partial unique index making "one partner, one active key" a
///   database fact (ADR 0015).
///
/// **The jump from 5 to 6 is the one that is not backward compatible**, and
/// that is deliberate. A v5 binary that opened a v6 database would find
/// `api_keys` free of any uniqueness, and would happily create a second active
/// key for a partner — the one thing v6 exists to prevent. The refusal in the
/// next line is therefore load-bearing rather than conservative, and a rolling
/// update that spans this change needs a coordinated restart, which
/// `docs/architecture/overview.md` says.
pub const SCHEMA_VERSION: u32 = 6;

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

    // v6, and it has to happen *here*: `schema.sql` creates
    // `idx_api_keys_one_active_per_consumer`, and that index is the first
    // statement that fails on a v5 database holding two active keys for one
    // consumer. A v5 deployment could legitimately be in that state — v5 had no
    // such constraint — so the rows are resolved before the constraint is
    // allowed to exist. Collapsing afterwards would be too late: the migration
    // would abort on `execute_batch` and never reach it.
    //
    // Nothing below may assume the constraint is absent, and nothing above may
    // assume the table exists: on a database this build creates, `api_keys` is
    // created by the `execute_batch` that follows.
    if found < 6 {
        collapse_duplicate_active_keys(conn)?;
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
    // v4: a bounded upstream error body. Adding a nullable column is safe while
    // an older binary is still writing during a rolling update: it neither
    // changes old statements nor requires a value from them.
    if found < 4 && !column_exists(conn, "usage_records", "error_body")? {
        conn.execute_batch("ALTER TABLE usage_records ADD COLUMN error_body TEXT;")?;
    }
    if !column_exists(conn, "usage_records", "error_body")? {
        return Err(SchemaError::MigrationIncomplete {
            column: "usage_records.error_body",
        });
    }
    // v6: the three price snapshots a metered request is billed against
    // (ADR 0015). Three nullable columns rather than a table rebuild, which
    // would be the only way to add a CHECK spanning them and would rewrite the
    // one table every request writes.
    //
    // The `found < 6` guard matters for a specific case: `schema.sql` already
    // ran, so on a database this build created *just now* the columns exist
    // while `found` is still 5. Without the guard the ALTER would run against
    // a table that has them and fail with "duplicate column name" — a startup
    // abort on a brand-new database. The `column_exists` re-check below is
    // what makes the result verifiable rather than assumed.
    if found < 6 {
        for column in [
            "input_price_snapshot",
            "cached_input_price_snapshot",
            "output_price_snapshot",
        ] {
            if !column_exists(conn, "usage_records", column)? {
                conn.execute_batch(&format!(
                    "ALTER TABLE usage_records ADD COLUMN {column} INTEGER;"
                ))?;
            }
            if !column_exists(conn, "usage_records", column)? {
                return Err(SchemaError::MigrationIncomplete {
                    column: "usage_records.input_price_snapshot",
                });
            }
        }
    }

    // v6: model capability moves from `api_keys.allowed_models` to
    // `partner_models`, and the old column is dropped. The copy has to run
    // before the drop, obviously, and both run after `schema.sql` because the
    // destination table is created by it.
    //
    // The duplicate-active-key collapse is *not* here — it ran above, before
    // `schema.sql`, for the reason stated there.
    if found < 6 {
        migrate_model_allow_list(conn)?;
        drop_legacy_allow_list_column(conn)?;
    }

    // One partial index serves both recovery and the in-flight audit: the
    // candidate set is tiny, and narrowing the index to in-flight rows keeps it
    // cheap to maintain on the write path.
    conn.execute_batch(
        "CREATE INDEX IF NOT EXISTS idx_usage_records_instance_in_flight
             ON usage_records(instance_id) WHERE request_status = 'in_flight';",
    )?;

    // The billing tables exist by the time this runs — `schema.sql` is
    // idempotent — but the data migration into them has not, so the per-column
    // checks above must be re-read *after* it rather than before. A migration
    // that half-applied is a database in exactly the state this module exists
    // to prevent, and it is the only honest way to find out.
    for table in [
        "partners",
        "partner_models",
        "daily_statements",
        "statement_lines",
    ] {
        let n: i64 = conn.query_row(
            "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
            [table],
            |row| row.get(0),
        )?;
        if n != 1 {
            return Err(SchemaError::MigrationIncomplete { column: table });
        }
    }

    write_schema_version(conn, SCHEMA_VERSION)?;
    Ok(())
}

/// Move model capability out of `api_keys.allowed_models` and into
/// `partner_models` (ADR 0015, amending ADR 0012).
///
/// **Then the column is dropped** — see [`drop_legacy_allow_list_column`],
/// which the caller runs immediately after this. Two sources of truth for the
/// same question is the defect this migration exists to remove, and a column
/// that is merely "still there for now" is a column the next change will read.
/// The copy runs first and the drop second: a crash between the two leaves the
/// old column still populated and the next start re-runs the copy, which is
/// idempotent.
///
/// **Empty on a database this build created**, which is why it returns early
/// when the column is gone: a fresh database has no `allowed_models` at all,
/// `found` is 0, and the `SELECT` below would fail on a column that was never
/// there.
///
/// # Why the model names are *not* copied into `partner_models`
///
/// They cannot be, honestly. `partner_models` is a price list as well as an
/// allow-list — one row is "may this partner call this model, and what does it
/// cost" — and a v5 database carries no price for any model anywhere. The only
/// value this migration could write is zero, and zero means free: an operator
/// who upgraded and never re-entered their prices would serve every partner at
/// no charge, indefinitely, with the mistake visible only on an invoice that
/// reconciles to nothing. That is the one failure mode a metering product must
/// not have, and it is exactly the fabrication the pricing table exists to
/// prevent — "zero is allowed, but it has to be typed to mean it".
///
/// So the names are read, logged, and left unpriced, and the consequence is
/// loud on purpose: until an operator prices them, every request for them is
/// refused with `model_not_allowed`, the partner's `/v1/models` is empty, and
/// the partner is `model`-less in `GET /api/admin/partners`. One call to
/// `PUT /api/admin/partners/{consumer_id}/models` fixes it. A rejected request
/// an operator hears about today is worth more than a free one nobody notices.
///
/// The names are taken per `consumer_id` as the union of the allow-lists of
/// that consumer's *active* keys, because that is the set the proxy was
/// actually serving — the log names what the partner will lose, in the order
/// they will lose it. If the union is empty there is nothing to report and
/// nothing to price, which is the same statement.
///
/// A `partners` row *is* written for every consumer that has a key, because the
/// commercial record cannot depend on someone remembering to create one: a
/// partner with keys and no commercial record would be metered at full rate and
/// never statemented. Its `billing_mode` is `invoice` — the default, and the
/// only mode a v5 deployment could have had, since the mode did not exist — and
/// its billing email is left empty rather than invented, because an address
/// this process never received must not be guessed into a table that sends
/// mail. Both are operator-editable.
fn migrate_model_allow_list(conn: &rusqlite::Connection) -> Result<(), SchemaError> {
    if !column_exists(conn, "api_keys", "allowed_models")? {
        return Ok(());
    }
    let now = writer::format_timestamp(time::OffsetDateTime::now_utc());
    // The union, per consumer, of the models on that consumer's *active* keys.
    // `json_each` expands the array; a key with an empty list contributes no
    // rows, which is why the second statement below exists.
    let mut stmt = conn.prepare(
        r#"
        SELECT consumer_id, MIN(name) AS name, json_group_array(DISTINCT model) AS models
        FROM (
            SELECT k.consumer_id AS consumer_id, k.name AS name, je.value AS model
            FROM api_keys k, json_each(k.allowed_models) AS je
            WHERE k.status = 'active'
        )
        GROUP BY consumer_id
        "#,
    )?;
    let migrations: Vec<(String, String, String)> = stmt
        .query_map([], |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get::<_, Option<String>>(2)?.unwrap_or_default(),
            ))
        })?
        .collect::<Result<Vec<_>, _>>()?;

    // Collected while the transaction runs and logged once it has committed:
    // a line about a migration that then rolled back would be a lie, and the
    // operator would go looking for prices on a database that never changed.
    let mut unpriced: Vec<(String, Vec<String>)> = Vec::new();

    let tx = conn.unchecked_transaction()?;
    for (consumer_id, name, models_json) in &migrations {
        tx.execute(
            "INSERT INTO partners (consumer_id, name, billing_email, billing_mode,
                                   payment_terms_minutes, created_at, updated_at)
             VALUES (?1, ?2, '', 'invoice', 720, ?3, ?3)
             ON CONFLICT(consumer_id) DO NOTHING",
            rusqlite::params![consumer_id, name, now],
        )?;

        // The SQL already produced a JSON array of strings; a parse failure
        // here would mean SQLite's `json_group_array` returned something else,
        // and continuing past it would migrate an unknown shape.
        let models: Vec<String> = serde_json::from_str(models_json).map_err(|e| {
            SchemaError::Sqlite(rusqlite::Error::FromSqlConversionFailure(
                0,
                rusqlite::types::Type::Text,
                Box::new(e),
            ))
        })?;
        // The blank check is the one piece of filtering left, and it is not a
        // price decision: an empty model name can never match a request, so
        // naming it in the log would send an operator looking for a model that
        // does not exist.
        let models: Vec<String> = models
            .into_iter()
            .filter(|m| !m.trim().is_empty())
            .collect();
        if !models.is_empty() {
            unpriced.push((consumer_id.clone(), models));
        }
    }
    // A key with an empty allow-list never reached the query above, so it has no
    // `partners` row either. Give it one: a commercial record is owed
    // regardless of what the key is currently allowed to call.
    tx.execute(
        "INSERT INTO partners (consumer_id, name, billing_email, billing_mode,
                               payment_terms_minutes, created_at, updated_at)
         SELECT consumer_id, MIN(name), '', 'invoice', 720, ?1, ?1
         FROM api_keys GROUP BY consumer_id
         ON CONFLICT(consumer_id) DO NOTHING",
        rusqlite::params![now],
    )?;
    // Emptied before the drop, not merely dropped. If the drop below fails —
    // an old SQLite, an unexpected constraint — the row data must not be left
    // available to a reader that has not been told the column is dead. An empty
    // list is also what the old `CHECK (json_valid(...))` requires while the
    // column still exists.
    tx.execute_batch("UPDATE api_keys SET allowed_models = '[]';")?;
    tx.commit()?;

    // After the commit, and one line per partner rather than one for the whole
    // migration: an operator reads their partner's name, not a count. This is
    // the only time it can be said — the column it reads is gone by the time
    // this returns — so it names the models in full rather than pointing at a
    // table that no longer holds them.
    for (consumer_id, models) in &unpriced {
        tracing::warn!(
            consumer_id = %consumer_id,
            models = %models.join(", "),
            "billing_models_unpriced_after_migration"
        );
    }
    Ok(())
}

/// Drop `api_keys.allowed_models`, the column model capability used to live in.
///
/// # Why a table rebuild rather than `ALTER TABLE ... DROP COLUMN`
///
/// SQLite refuses to drop a column that a `CHECK` constraint mentions, and the
/// v5 table had `CHECK (json_valid(allowed_models) AND
/// json_type(allowed_models) = 'array')`. The rebuild is the standard,
/// documented alternative: create the table in its v6 shape, copy every column
/// that survives, drop the old one, rename. `INSERT ... SELECT` names both
/// column lists explicitly, so the copy cannot silently misalign if the two
/// shapes ever differ.
///
/// # What it costs, and what is preserved
///
/// The rebuild is one transaction, and it rewrites `api_keys`, which is small:
/// it holds one row per key ever issued, not one per request. `id` is carried
/// across explicitly so a `daily_statements`-style reference to a key id, or an
/// operator's note about key 17, still means the same row afterwards.
/// `AUTOINCREMENT` survives the rename, so the sequence does not restart.
///
/// `DROP TABLE` takes the indexes with it, so all three are recreated here with
/// the same definitions `schema.sql` uses. Leaving that to the next startup
/// would mean one window in which the one-active-key-per-partner constraint does
/// not exist — and the window is exactly when the migration is running.
fn drop_legacy_allow_list_column(conn: &rusqlite::Connection) -> Result<(), SchemaError> {
    if !column_exists(conn, "api_keys", "allowed_models")? {
        return Ok(());
    }

    let tx = conn.unchecked_transaction()?;
    tx.execute_batch(
        r#"
        CREATE TABLE api_keys_v6 (
            id          INTEGER PRIMARY KEY AUTOINCREMENT,
            name        TEXT NOT NULL,
            consumer_id TEXT NOT NULL,
            key_prefix  TEXT NOT NULL,
            key_hash    TEXT NOT NULL,
            status      TEXT NOT NULL DEFAULT 'active',
            created_at  TEXT NOT NULL,
            updated_at  TEXT NOT NULL,
            expires_at  TEXT,
            revoked_at  TEXT,

            UNIQUE (key_hash),
            CHECK (status IN ('active', 'revoked')),
            CHECK ((status = 'active' AND revoked_at IS NULL)
                OR (status = 'revoked' AND revoked_at IS NOT NULL))
        );

        INSERT INTO api_keys_v6 (
            id, name, consumer_id, key_prefix, key_hash,
            status, created_at, updated_at, expires_at, revoked_at
        )
        SELECT id, name, consumer_id, key_prefix, key_hash,
               status, created_at, updated_at, expires_at, revoked_at
        FROM api_keys;

        DROP TABLE api_keys;
        ALTER TABLE api_keys_v6 RENAME TO api_keys;

        CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys(status);
        CREATE UNIQUE INDEX IF NOT EXISTS idx_api_keys_one_active_per_consumer
            ON api_keys(consumer_id) WHERE status = 'active';
        CREATE INDEX IF NOT EXISTS idx_api_keys_consumer ON api_keys(consumer_id);
        "#,
    )?;

    // Verified rather than assumed. A half-applied rebuild is the exact state
    // this module refuses to leave a database in, and `column_exists` on a
    // dropped column is the cheap way to find out.
    if column_exists(&tx, "api_keys", "allowed_models")? {
        return Err(SchemaError::MigrationIncomplete {
            column: "api_keys.allowed_models",
        });
    }
    tx.commit()?;
    Ok(())
}

/// Leave at most one active key per `consumer_id`, so the partial unique index
/// can exist.
///
/// Revoking the older rows is the choice, and the loss is real: those key
/// plaintexts stop working. It is still the right one, because the alternative —
/// dropping the newest — would take away the key an operator had just issued,
/// and revoking both would take the partner offline entirely. The oldest is the
/// one that has demonstrably been replaced, because a rotation writes the
/// replacement first.
///
/// This runs before `schema.sql` on a pre-v6 database — it has to, because
/// `schema.sql` is what creates the index, and creating it is the first
/// statement that fails on the rows this function exists to resolve. It
/// therefore also has to tolerate a database with no `api_keys` table at all,
/// which is exactly the state of one this build was just handed.
///
/// It is idempotent, so a second run on a database already in the v6 shape
/// revokes nothing and logs nothing.
fn collapse_duplicate_active_keys(conn: &rusqlite::Connection) -> Result<(), SchemaError> {
    if !table_exists(conn, "api_keys")? {
        return Ok(());
    }
    let now = writer::format_timestamp(time::OffsetDateTime::now_utc());
    let revoked = conn.execute(
        r#"
        UPDATE api_keys
        SET status = 'revoked',
            revoked_at = ?1,
            updated_at = ?1
        WHERE id IN (
            SELECT id FROM api_keys
            WHERE status = 'active'
              AND id NOT IN (
                  SELECT MAX(id) FROM api_keys
                  WHERE status = 'active' GROUP BY consumer_id
              )
        )
        "#,
        rusqlite::params![now],
    )?;
    if revoked > 0 {
        tracing::warn!(
            revoked,
            "revoked duplicate active API keys while migrating to one key per partner; \
             the newest key for each consumer_id was kept"
        );
    }
    Ok(())
}

/// Whether a table exists in this database.
fn table_exists(conn: &rusqlite::Connection, table: &str) -> Result<bool, SchemaError> {
    let n: i64 = conn.query_row(
        "SELECT COUNT(*) FROM sqlite_master WHERE type = 'table' AND name = ?1",
        [table],
        |row| row.get(0),
    )?;
    Ok(n == 1)
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
        assert_eq!(read_schema_version(&conn).unwrap(), SCHEMA_VERSION);
    }

    #[test]
    fn test_a_fresh_database_carries_the_api_keys_table() {
        // The table is the source of truth for credentials, so its absence
        // would be a silent "no partner may call" rather than a failure.
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        init_schema(&conn).unwrap();

        for column in [
            "id",
            "name",
            "consumer_id",
            "key_prefix",
            "key_hash",
            "status",
            "created_at",
            "updated_at",
            "expires_at",
            "revoked_at",
        ] {
            assert!(
                column_exists(&conn, "api_keys", column).unwrap(),
                "api_keys.{column} must exist"
            );
        }

        // And the column that must *not* exist. `allowed_models` was the
        // per-key model list, and it is gone rather than unused: two columns
        // that could each answer "may this partner call this model" is the
        // second source of truth ADR 0015 removes, and a column left in place
        // is a column a later change reaches for.
        assert!(
            !column_exists(&conn, "api_keys", "allowed_models").unwrap(),
            "api_keys.allowed_models must have been dropped, not merely ignored"
        );

        let keys: i64 = conn
            .query_row("SELECT COUNT(*) FROM api_keys", [], |r| r.get(0))
            .unwrap();
        assert_eq!(keys, 0, "a fresh database has no keys yet");
    }

    /// The billing tables exist on a fresh database, and the constraints that
    /// make the billing invariant enforceable actually exist with them.
    ///
    /// Asserted by *attempting the duplicate*, not by reading `sqlite_master`:
    /// a test that lists indexes goes green on an index that is present and
    /// wrong, and the failure that matters here is a second statement for one
    /// partner and day — which is a second bill.
    #[test]
    fn test_a_fresh_database_carries_the_billing_tables_and_their_constraints() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("t.db"));
        init_schema(&conn).unwrap();

        for table in [
            "partners",
            "partner_models",
            "daily_statements",
            "statement_lines",
        ] {
            assert!(
                table_exists(&conn, table).unwrap(),
                "{table} must exist on a fresh database"
            );
        }

        let now = "2026-09-01T00:00:00.000000000Z";
        conn.execute(
            "INSERT INTO partners (consumer_id, name, billing_email, billing_mode, \
             payment_terms_minutes, created_at, updated_at) \
             VALUES ('acme', 'Acme', 'billing@acme.test', 'invoice', 720, ?1, ?1)",
            [now],
        )
        .unwrap();

        let statement = |date: &str| {
            conn.execute(
                "INSERT INTO daily_statements (consumer_id, billing_date, billing_mode, \
                 currency, period_start, period_end, billing_cutoff_at, \
                 total_amount_micro_usd, incomplete_usage_count, due_at, created_at, \
                 updated_at) \
                 VALUES ('acme', ?1, 'invoice', 'USD', ?2, ?2, ?2, 0, 0, ?3, ?2, ?2)",
                rusqlite::params![date, now, "2026-09-02T00:00:00.000000000Z"],
            )
        };

        // One statement per partner and day: the second must be refused. This
        // is the whole of the statement idempotency guarantee — re-running the
        // worker is a no-op because the schema says so, not because the worker
        // remembers.
        statement("2026-08-31").expect("the first statement for a day is accepted");
        assert!(
            statement("2026-08-31").is_err(),
            "a second statement for one partner and day must be refused"
        );
        statement("2026-09-01").expect("but the next day is its own statement");

        // A reconciliation statement may carry no payment lifecycle at all.
        assert!(
            conn.execute(
                "INSERT INTO daily_statements (consumer_id, billing_date, billing_mode, \
                 currency, period_start, period_end, billing_cutoff_at, \
                 total_amount_micro_usd, incomplete_usage_count, due_at, created_at, \
                 updated_at) \
                 VALUES ('acme', '2026-09-02', 'reconciliation', 'USD', ?1, ?1, ?1, 0, 0, \
                         '2026-09-03T00:00:00.000000000Z', ?1, ?1)",
                [now],
            )
            .is_err(),
            "a reconciliation statement with a due date is a payment obligation it \
             does not have"
        );

        // The one-active-key-per-partner index, as the same kind of assertion:
        // the schema must refuse the second live credential, because that is a
        // race and a check in Rust cannot close it.
        conn.execute(
            "INSERT INTO api_keys (name, consumer_id, key_prefix, key_hash, status, \
             created_at, updated_at) \
             VALUES ('first', 'acme', 'pp_one', 'hash-one', 'active', ?1, ?1)",
            [now],
        )
        .unwrap();
        assert!(
            conn.execute(
                "INSERT INTO api_keys (name, consumer_id, key_prefix, key_hash, status, \
                 created_at, updated_at) \
                 VALUES ('second', 'acme', 'pp_two', 'hash-two', 'active', ?1, ?1)",
                [now],
            )
            .is_err(),
            "one partner must not be able to hold two active keys"
        );
        // A revoked one is history allowed to accumulate: the index is partial
        // on `status = 'active'` for exactly this reason.
        conn.execute(
            "INSERT INTO api_keys (name, consumer_id, key_prefix, key_hash, status, \
             created_at, updated_at, revoked_at) \
             VALUES ('old', 'acme', 'pp_old', 'hash-old', 'revoked', ?1, ?1, ?1)",
            [now],
        )
        .expect("a revoked key is history, not a second credential");
    }

    #[test]
    fn test_older_schema_is_migrated_forward_not_relabelled() {
        // Build a v3 database: the ledger as it shipped before bounded error
        // bodies, with ownership present and the version stamped 3.
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
                instance_id TEXT,
                input_tokens INTEGER,
                output_tokens INTEGER,
                cached_tokens INTEGER,
                ttft_ms INTEGER,
                duration_ms INTEGER NOT NULL,
                usage_status TEXT NOT NULL,
                error_message TEXT
             );
             CREATE TABLE ledger_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO ledger_meta (key, value) VALUES ('schema_version', '3');
             INSERT INTO usage_records (
                request_id, created_at, consumer_id, model, endpoint, streaming,
                request_status, duration_ms, usage_status
             ) VALUES ('legacy-1', '2026-01-01T00:00:00.000000000Z', 'c1', 'm',
                       'chat_completions', 0, 'completed', 10, 'unavailable');",
        )
        .unwrap();

        init_schema(&conn).unwrap();

        assert_eq!(read_schema_version(&conn).unwrap(), SCHEMA_VERSION);
        assert!(
            column_exists(&conn, "usage_records", "error_body").unwrap(),
            "the migration must actually add the column, not just relabel the file"
        );
        // A v3 file gains `api_keys` and nothing else: a new table is additive,
        // so there is no row for the migration to carry over — and, stated
        // plainly, a deployment that upgrades finds no partner keys waiting,
        // because keys moved out of `config.yaml` (ADR 0014).
        let key_table: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM sqlite_master WHERE type='table' AND name='api_keys'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(key_table, 1, "the migration must create api_keys");
        let rows: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(rows, 1, "migrating must not touch existing rows");
        let error_body: Option<String> = conn
            .query_row(
                "SELECT error_body FROM usage_records WHERE request_id = 'legacy-1'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(
            error_body, None,
            "a pre-migration row has no captured upstream error body"
        );
    }

    /// The whole v5 upgrade, in one test, because its three parts are one
    /// state a real v5 deployment could be in and only one of them is merely
    /// inconvenient.
    ///
    /// Two active keys for one consumer is *legal* in v5 — nothing enforced the
    /// rule — and the partial unique index that enforces it now is created by
    /// `schema.sql`, which means an unresolved duplicate aborts the migration at
    /// `execute_batch`. That is a start-up failure on the machine being
    /// upgraded, so it is resolved first and the test asserts which key
    /// survived: the newest, because the oldest is the one a rotation replaced.
    ///
    /// The model names have no price anywhere in a v5 database, and this is the
    /// assertion that matters most. Writing them into `partner_models` at zero
    /// would be a price nobody typed, meaning "free", and it would read as a
    /// successful upgrade. They are left unpriced instead and logged
    /// (`billing_models_unpriced_after_migration`), which refuses requests for
    /// them until an operator prices them — loud, and one API call from fixed.
    ///
    /// And the `partners` row is written for *every* consumer with a key, the
    /// one with an empty allow-list included: a commercial record is owed
    /// regardless of what a key may call, and a partner with keys and no record
    /// would be metered and never statemented.
    #[test]
    fn test_a_v5_database_upgrades_without_a_fabricated_price() {
        let dir = TempDir::new().unwrap();
        let conn = new_db(&dir.path().join("v5.db"));
        conn.execute_batch(
            "CREATE TABLE api_keys (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                consumer_id TEXT NOT NULL,
                key_prefix TEXT NOT NULL,
                key_hash TEXT NOT NULL,
                status TEXT NOT NULL DEFAULT 'active',
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                expires_at TEXT,
                revoked_at TEXT,
                allowed_models TEXT NOT NULL DEFAULT '[]',
                UNIQUE (key_hash),
                CHECK (json_valid(allowed_models) AND json_type(allowed_models) = 'array')
             );
             CREATE TABLE ledger_meta (key TEXT PRIMARY KEY, value TEXT NOT NULL);
             INSERT INTO ledger_meta (key, value) VALUES ('schema_version', '5');
             INSERT INTO api_keys (name, consumer_id, key_prefix, key_hash, status,
                                   created_at, updated_at, allowed_models)
             VALUES ('acme-old', 'acme', 'pp_acme_old', 'hash-acme-old', 'active',
                     '2026-01-01T00:00:00.000000000Z', '2026-01-01T00:00:00.000000000Z',
                     '[\"gpt-4o\"]'),
                    ('acme-prod', 'acme', 'pp_acme', 'hash-acme', 'active',
                     '2026-02-01T00:00:00.000000000Z', '2026-02-01T00:00:00.000000000Z',
                     '[\"gpt-4o\", \"gpt-4o-mini\"]'),
                    ('beta-prod', 'beta', 'pp_beta', 'hash-beta', 'active',
                     '2026-02-01T00:00:00.000000000Z', '2026-02-01T00:00:00.000000000Z',
                     '[]');",
        )
        .unwrap();

        init_schema(&conn).expect("a v5 database with two active keys must still migrate");

        assert_eq!(read_schema_version(&conn).unwrap(), SCHEMA_VERSION);
        assert!(
            !column_exists(&conn, "api_keys", "allowed_models").unwrap(),
            "the allow-list column must be dropped, not left as a second source of truth"
        );

        // One live credential, and it is the newer of the two.
        let live: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM api_keys WHERE consumer_id = 'acme' \
                 AND status = 'active'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(live, 1, "the duplicate active key must have been revoked");
        let kept: i64 = conn
            .query_row(
                "SELECT id FROM api_keys WHERE consumer_id = 'acme' \
                 AND status = 'active'",
                [],
                |r| r.get(0),
            )
            .unwrap();
        assert_eq!(kept, 2, "the newest key is the one that was just issued");
        let revoked_at: Option<String> = conn
            .query_row("SELECT revoked_at FROM api_keys WHERE id = 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        assert!(
            revoked_at.is_some(),
            "a key revoked by the migration must carry its revocation time"
        );

        // The commercial record, for both consumers.
        let partners: i64 = conn
            .query_row("SELECT COUNT(*) FROM partners", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            partners, 2,
            "a consumer with keys is owed a partner record even with no models"
        );
        let (name, email, mode): (String, String, String) = conn
            .query_row(
                "SELECT name, billing_email, billing_mode FROM partners \
                 WHERE consumer_id = 'acme'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(name, "acme-prod", "the record is named after the live key");
        assert_eq!(
            email, "",
            "an address this process never received must not be invented"
        );
        assert_eq!(mode, "invoice", "v5 had one mode, and this is it");

        // The point of the test: no price was invented to carry the names over.
        let priced: i64 = conn
            .query_row("SELECT COUNT(*) FROM partner_models", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            priced, 0,
            "a v5 database carries no prices, and a zero here would serve the \
             partner for free for as long as nobody noticed"
        );

        // And the constraint survived the table rebuild, which is the other
        // thing a rebuild can silently lose: the index is recreated inside the
        // same transaction that drops the old table.
        let now = "2026-03-01T00:00:00.000000000Z";
        assert!(
            conn.execute(
                "INSERT INTO api_keys (name, consumer_id, key_prefix, key_hash, status, \
                 created_at, updated_at) \
                 VALUES ('acme-second', 'acme', 'pp_two', 'hash-two', 'active', ?1, ?1)",
                [now],
            )
            .is_err(),
            "the migrated database must still refuse a second live key"
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
