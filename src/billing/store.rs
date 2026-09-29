//! The billing tables' repository: the only code that writes `partners`,
//! `partner_models`, `daily_statements` or `statement_lines`.
//!
//! # One write boundary, the same one everything else uses
//!
//! Every mutation goes through [`LedgerPool::write`], which locks the single
//! `Arc<Mutex<Connection>>` the metering writer holds. That is what keeps "one
//! controlled SQLite write serialisation boundary per process" true with a
//! second and third writer in the process: an operator editing a price, the
//! statement worker closing a day and the proxy metering a request all contend
//! for one lock instead of racing three connections at one WAL file.
//!
//! Nothing here goes through the metering queue. The queue exists to apply
//! backpressure to request traffic; a statement is not request traffic, and
//! routing it through would make closing a day fail because the ledger was busy
//! — and would put a multi-second aggregation on the path that meters requests.
//!
//! # Nothing on the request path calls any of this
//!
//! Authentication reads an in-memory snapshot (see
//! [`crate::billing::partner::PartnerSnapshot`]); a statement is read from here
//! by the dashboard and the admin API, both of which are allowed to open SQLite.
//!
//! # Where the money is not computed
//!
//! This module moves rows. It does not price anything: the aggregation that
//! turns usage into lines lives in [`crate::billing::statements`] and the
//! arithmetic lives in [`crate::billing::pricing`]. A repository that also did
//! arithmetic would be a second place for the rounding rule to live.

use std::sync::Arc;

use rusqlite::types::Value;
use rusqlite::{Connection, OptionalExtension, Row, params, params_from_iter};
use time::OffsetDateTime;

use crate::auth::{Scope, scope_clause};
use crate::billing::partner::{BillingMode, ModelPrice, Partner};
use crate::billing::period::BillingDay;
use crate::billing::pricing::{LineCost, MicroUsd, PricingSnapshot};
use crate::ledger::pool::LedgerPool;
use crate::ledger::timefmt;

/// A failure in the billing store.
///
/// Deliberately its own type rather than a reuse of `ApiKeyError`: the two
/// repositories share a pool and nothing else, and a caller that has to tell
/// "the partner does not exist" from "the key is revoked" should not be reading
/// an enum documented about keys.
#[derive(Debug)]
pub enum BillingError {
    /// SQLite refused the operation.
    Database(rusqlite::Error),
    /// No partner has that `consumer_id`.
    PartnerNotFound(String),
    /// A partner with that `consumer_id` already exists.
    PartnerExists(String),
    /// A payment was recorded against a statement that carries no payment
    /// obligation — a reconciliation statement, or one the caller read as
    /// payable and that changed underneath them.
    ///
    /// Its own variant rather than `Invalid`, because the two are different
    /// answers to a client: `Invalid` is a request that was wrong when it was
    /// written, and this is a request that was right when it was written and
    /// wrong against the state it met.
    StatementNotPayable { id: i64, billing_mode: String },
    /// A field was empty or malformed before it reached SQL.
    Invalid(String),
}

impl std::fmt::Display for BillingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BillingError::Database(e) => write!(f, "billing database error: {e}"),
            BillingError::PartnerNotFound(id) => write!(f, "no partner with consumer_id {id}"),
            BillingError::PartnerExists(id) => write!(f, "partner {id} already exists"),
            BillingError::StatementNotPayable { id, billing_mode } => write!(
                f,
                "statement {id} is a {billing_mode} statement and carries no payment obligation"
            ),
            BillingError::Invalid(msg) => write!(f, "invalid billing configuration: {msg}"),
        }
    }
}

impl std::error::Error for BillingError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            BillingError::Database(e) => Some(e),
            _ => None,
        }
    }
}

impl From<rusqlite::Error> for BillingError {
    fn from(e: rusqlite::Error) -> Self {
        BillingError::Database(e)
    }
}

type Result<T> = std::result::Result<T, BillingError>;

/// `SQLITE_CONSTRAINT_UNIQUE`, the extended code behind a `UNIQUE` violation.
const SQLITE_CONSTRAINT_UNIQUE: i32 = 2067;
/// `SQLITE_CONSTRAINT_PRIMARYKEY`. A duplicate `partners.consumer_id` reports
/// this rather than the unique code below, because that column is the table's
/// primary key — the same duplicate, a different code, and reading only one of
/// the two turned "that partner already exists" into "database error".
const SQLITE_CONSTRAINT_PRIMARYKEY: i32 = 1555;
/// `SQLITE_CONSTRAINT_FOREIGNKEY`. A `partner_models` row for a partner that
/// does not exist is this, and it reads as "no such partner" to a caller.
const SQLITE_CONSTRAINT_FOREIGNKEY: i32 = 787;

/// Whether a SQLite error is a uniqueness violation, whichever code reports it.
///
/// Both are needed and both occur here: `partners.consumer_id` is a primary key
/// (1555) while `daily_statements (consumer_id, billing_date)` is a `UNIQUE`
/// constraint (2067). A caller that wants "something already existed" wants both.
fn is_uniqueness_violation(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(e, _)
            if e.extended_code == SQLITE_CONSTRAINT_UNIQUE
                || e.extended_code == SQLITE_CONSTRAINT_PRIMARYKEY
    )
}

/// The commercial facts needed to open an account for a partner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NewPartner {
    pub consumer_id: String,
    pub name: String,
    pub billing_email: String,
    pub billing_mode: BillingMode,
    pub payment_terms_minutes: i64,
}

/// The editable half of a partner. `None` means "leave it as it is", which is
/// what makes a PATCH with one field in it safe.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct PartnerPatch {
    pub name: Option<String>,
    pub billing_email: Option<String>,
    pub billing_mode: Option<BillingMode>,
    pub payment_terms_minutes: Option<i64>,
}

/// One line of a statement, as it is written.
///
/// The token counts are the day's totals for one (model, price) group and the
/// `cost` is what [`crate::billing::pricing::price_line`] made of them — carried
/// together so the row and its arithmetic cannot come from two different
/// readings of the same usage.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementLineDraft {
    pub model: String,
    pub prices: PricingSnapshot,
    pub request_count: i64,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub output_tokens: i64,
    pub cost: LineCost,
}

/// A statement as it is written, before it has an id or a `created_at`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementDraft {
    pub consumer_id: String,
    pub billing_date: BillingDay,
    pub billing_mode: BillingMode,
    /// The half-open UTC range the statement covers, already formatted.
    pub period_start: String,
    pub period_end: String,
    /// When the statement was issued. Never `period_end`: a request accepted
    /// inside the period can finalize after it, and this is the instant the
    /// worker decided no more of that day's rows were coming.
    pub billing_cutoff_at: String,
    pub due_at: Option<OffsetDateTime>,
    pub incomplete_usage_count: i64,
    pub lines: Vec<StatementLineDraft>,
}

impl StatementDraft {
    /// What the statement totals, which is the sum of its lines.
    ///
    /// Summed here rather than stored from the aggregate query, because the
    /// lines are what a partner is shown and a total that disagrees with them
    /// is a statement nobody can check.
    pub fn total(&self) -> Result<MicroUsd> {
        let mut total = MicroUsd::from_i64(0);
        for line in &self.lines {
            total = total.checked_add(line.cost.total).ok_or_else(|| {
                BillingError::Invalid("a statement total overflowed micro-dollars".to_string())
            })?;
        }
        Ok(total)
    }
}

/// A statement row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Statement {
    pub id: i64,
    pub consumer_id: String,
    pub billing_date: String,
    pub billing_mode: String,
    pub currency: String,
    pub period_start: String,
    pub period_end: String,
    pub billing_cutoff_at: String,
    pub total_amount_micro_usd: i64,
    pub incomplete_usage_count: i64,
    pub due_at: Option<String>,
    pub paid_at: Option<String>,
    pub paid_by: Option<String>,
    pub payment_reference: Option<String>,
    pub payment_note: Option<String>,
    pub email_sent_at: Option<String>,
    pub email_attempts: i64,
    pub email_last_error: Option<String>,
    pub email_next_retry_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
}

impl Statement {
    pub fn billing_mode_value(&self) -> Option<BillingMode> {
        BillingMode::parse(&self.billing_mode)
    }

    pub fn is_paid(&self) -> bool {
        self.paid_at.is_some()
    }

    /// Whether anything is still owed on this statement.
    ///
    /// A reconciliation statement is never "unpaid" in the sense that matters:
    /// it carries no payment obligation, so it is not outstanding. Reading it
    /// as outstanding is the mistake the billing mode exists to prevent.
    pub fn is_outstanding(&self) -> bool {
        self.billing_mode_value()
            .is_some_and(BillingMode::owes_payment)
            && !self.is_paid()
    }

    pub fn total(&self) -> MicroUsd {
        MicroUsd::from_i64(self.total_amount_micro_usd)
    }

    pub fn has_incomplete_usage(&self) -> bool {
        self.incomplete_usage_count > 0
    }

    /// This statement as [`status_for`](crate::billing::status::status_for) reads
    /// it.
    ///
    /// The mapping lives here, once, because both readers of that predicate — the
    /// snapshot refresh's worker and the billing read API — have to hand it the
    /// same fields. Two copies of this struct literal would be two places for a
    /// column to be dropped, and a dropped `total_amount_micro_usd` reads as a
    /// zero statement, which is a statement that cannot suspend anyone.
    ///
    /// `to_overdue_row` does not decide *whether* the row is overdue; the caller
    /// gets those rows from [`BillingStore::overdue_statements`], whose `WHERE` is
    /// the definition.
    pub fn to_overdue_row(&self) -> crate::billing::status::OverdueRow {
        crate::billing::status::OverdueRow {
            billing_mode: self.billing_mode.clone(),
            id: self.id,
            billing_date: self.billing_date.clone(),
            // A row reaching here has a deadline — the query that produced it
            // requires one — but the column is nullable, and a missing deadline
            // must not become the Unix epoch, which would read as overdue since
            // 1970. It becomes the empty string, which `status_for` compares as
            // "not yet overdue" because it compares formatted timestamps.
            due_at: self.due_at.clone().unwrap_or_default(),
            total_amount_micro_usd: self.total_amount_micro_usd,
            incomplete_usage_count: self.incomplete_usage_count,
        }
    }

    /// Whether this statement can suspend the partner that owes it.
    ///
    /// The same three facts [`crate::billing::status::status_for`] checks, in
    /// the same order, because the two are read as one answer: what suspends a
    /// partner, and whether *this* statement is what does it. A statement with
    /// nothing on it cannot: there is no debt to enforce, and cutting a partner
    /// off over `$0.000000` would be the product failing at the one thing this
    /// predicate is for.
    pub fn can_suspend(&self) -> bool {
        self.is_outstanding()
            && !self.has_incomplete_usage()
            && self.total_amount_micro_usd > 0
            && self.due_at.is_some()
    }
}

/// A statement line row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatementLine {
    pub id: i64,
    pub statement_id: i64,
    pub model: String,
    pub prices: PricingSnapshot,
    pub request_count: i64,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    pub uncached_input_tokens: i64,
    pub output_tokens: i64,
    pub input_cost_micro_usd: i64,
    pub cached_input_cost_micro_usd: i64,
    pub output_cost_micro_usd: i64,
    pub total_cost_micro_usd: i64,
}

/// One (model, price) group of a day's usage, straight out of the aggregate.
///
/// `None` in the three price columns is a row that was accepted with no billing
/// configuration. It is carried as `None` all the way to the decision, because
/// the decision is "this day is incomplete" and collapsing it to `0` first
/// would make it a line that charges nothing.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct UsageGroup {
    pub model: String,
    pub prices: Option<PricingSnapshot>,
    /// Requests in this group whose usage the provider reported in full — the
    /// population the token sums beside this field cover.
    pub request_count: i64,
    pub input_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    /// Requests in this group whose usage cannot be priced, counted separately
    /// so that they are never inside a sum.
    ///
    /// The count is a fact about the day and it is what makes a statement
    /// defensible; the tokens are absent because there is no honest number to
    /// add. Summing complete and incomplete rows together would produce a total
    /// that is neither the measured usage nor the whole usage, and nobody
    /// reading the statement could tell which — so the two populations are
    /// counted separately and only one of them is priced.
    pub incomplete_count: i64,
}

/// The result of writing a statement.
///
/// `Created` and `Existed` are not a success/failure pair: both mean the
/// database now holds exactly one statement for that partner and day. The
/// distinction is what the worker logs and whether it sends mail — a re-run must
/// not email a second time.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementOutcome {
    Created(Box<Statement>),
    Existed(Box<Statement>),
}

impl StatementOutcome {
    pub fn statement(&self) -> &Statement {
        match self {
            StatementOutcome::Created(s) | StatementOutcome::Existed(s) => s,
        }
    }

    pub fn was_created(&self) -> bool {
        matches!(self, StatementOutcome::Created(_))
    }
}

/// Why a statement write was refused before it reached SQL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum StatementRejected {
    /// A line claims more cached tokens than input tokens.
    InconsistentLine(String),
    /// The billing mode wants a deadline and `due_at` is missing.
    MissingDueDate,
    /// A reconciliation statement carries a deadline.
    UnexpectedDueDate,
}

impl std::fmt::Display for StatementRejected {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatementRejected::InconsistentLine(model) => write!(
                f,
                "line for {model:?} bills more cached tokens than the prompt had"
            ),
            StatementRejected::MissingDueDate => {
                write!(f, "an invoice statement must carry a due date")
            }
            StatementRejected::UnexpectedDueDate => {
                write!(f, "a reconciliation statement must not carry a due date")
            }
        }
    }
}

impl From<StatementRejected> for BillingError {
    fn from(e: StatementRejected) -> Self {
        BillingError::Invalid(e.to_string())
    }
}

/// The billing tables, over the shared pool.
#[derive(Clone)]
pub struct BillingStore {
    pool: Arc<LedgerPool>,
}

impl BillingStore {
    pub fn new(pool: Arc<LedgerPool>) -> Self {
        Self { pool }
    }

    pub fn pool(&self) -> &Arc<LedgerPool> {
        &self.pool
    }

    // -----------------------------------------------------------------------
    // Partners
    // -----------------------------------------------------------------------

    /// Open an account for a partner. Fails if one already exists, because
    /// silently resetting an existing partner's billing mode or payment terms
    /// from a create call is how a contract gets changed by accident.
    pub fn create_partner(&self, new: NewPartner) -> Result<Partner> {
        let new = new.validated()?;
        let now = timefmt::format_ts(timefmt::now());
        let result = self.pool.write(|conn| {
            conn.execute(
                "INSERT INTO partners (consumer_id, name, billing_email, billing_mode, \
                 payment_terms_minutes, created_at, updated_at) \
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                params![
                    new.consumer_id,
                    new.name,
                    new.billing_email,
                    new.billing_mode.as_str(),
                    new.payment_terms_minutes,
                    now
                ],
            )
            .map(|_| ())
        });

        match result {
            Ok(()) => self
                .get_partner(&new.consumer_id)?
                .ok_or_else(|| BillingError::PartnerNotFound(new.consumer_id.clone())),
            Err(e) if is_uniqueness_violation(&e) => {
                Err(BillingError::PartnerExists(new.consumer_id))
            }
            Err(e) => Err(BillingError::Database(e)),
        }
    }

    pub fn get_partner(&self, consumer_id: &str) -> Result<Option<Partner>> {
        let consumer_id = consumer_id.to_string();
        self.pool
            .read(|conn| {
                conn.query_row(
                    &format!("SELECT {PARTNER_COLUMNS} FROM partners WHERE consumer_id = ?1"),
                    [&consumer_id],
                    row_to_partner,
                )
                .optional()
            })
            .map_err(Into::into)
    }

    /// Every partner, in `consumer_id` order so two reads agree.
    pub fn list_partners(&self) -> Result<Vec<Partner>> {
        self.pool
            .read(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {PARTNER_COLUMNS} FROM partners ORDER BY consumer_id"
                ))?;
                let rows = stmt.query_map([], row_to_partner)?;
                rows.collect()
            })
            .map_err(Into::into)
    }

    /// Apply a patch. Only the fields that are `Some` are written, in one
    /// statement, so two concurrent edits cannot interleave into a partner that
    /// is half one operator's and half the other's.
    pub fn update_partner(&self, consumer_id: &str, patch: PartnerPatch) -> Result<Partner> {
        if let Some(terms) = patch.payment_terms_minutes {
            if terms < 0 {
                return Err(BillingError::Invalid(
                    "payment_terms_minutes must not be negative".to_string(),
                ));
            }
        }
        if let Some(name) = patch.name.as_deref() {
            if name.trim().is_empty() {
                return Err(BillingError::Invalid("name must not be blank".to_string()));
            }
        }
        if patch == PartnerPatch::default() {
            return self
                .get_partner(consumer_id)?
                .ok_or_else(|| BillingError::PartnerNotFound(consumer_id.to_string()));
        }

        let now = timefmt::format_ts(timefmt::now());
        let updated = self.pool.write(|conn| {
            // `COALESCE(?n, column)` is what makes "absent means unchanged" a
            // property of the statement rather than of the caller remembering
            // to pass the old value back.
            conn.execute(
                "UPDATE partners SET \
                     name = COALESCE(?2, name), \
                     billing_email = COALESCE(?3, billing_email), \
                     billing_mode = COALESCE(?4, billing_mode), \
                     payment_terms_minutes = COALESCE(?5, payment_terms_minutes), \
                     updated_at = ?6 \
                 WHERE consumer_id = ?1",
                params![
                    consumer_id,
                    patch.name,
                    patch.billing_email,
                    patch.billing_mode.map(BillingMode::as_str),
                    patch.payment_terms_minutes,
                    now
                ],
            )
        })?;

        if updated == 0 {
            return Err(BillingError::PartnerNotFound(consumer_id.to_string()));
        }
        self.get_partner(consumer_id)?
            .ok_or_else(|| BillingError::PartnerNotFound(consumer_id.to_string()))
    }

    /// Delete a partner, and with it every price. Refused if any statement
    /// exists, by the foreign key — the error is reported as `Invalid` because
    /// the reason is a rule of the product and not a database fault.
    pub fn delete_partner(&self, consumer_id: &str) -> Result<()> {
        let deleted = self.pool.write(|conn| {
            conn.execute("DELETE FROM partners WHERE consumer_id = ?1", [consumer_id])
        })?;
        if deleted == 0 {
            return Err(BillingError::PartnerNotFound(consumer_id.to_string()));
        }
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Models and prices
    // -----------------------------------------------------------------------

    /// A partner's configured models, in model order.
    pub fn models(&self, consumer_id: &str) -> Result<Vec<ModelPrice>> {
        let consumer_id = consumer_id.to_string();
        self.pool
            .read(|conn| {
                let mut stmt = conn.prepare(
                    "SELECT model, input_price_micro_usd_per_million, \
                     cached_input_price_micro_usd_per_million, \
                     output_price_micro_usd_per_million \
                     FROM partner_models WHERE consumer_id = ?1 ORDER BY model",
                )?;
                let rows = stmt.query_map([&consumer_id], |row| {
                    Ok(ModelPrice {
                        model: row.get(0)?,
                        prices: PricingSnapshot::new(
                            crate::billing::pricing::PricePerMillion::new(row.get(1)?),
                            crate::billing::pricing::PricePerMillion::new(row.get(2)?),
                            crate::billing::pricing::PricePerMillion::new(row.get(3)?),
                        ),
                    })
                })?;
                rows.collect()
            })
            .map_err(Into::into)
    }

    /// Replace a partner's whole model list and price list, atomically.
    ///
    /// Replace rather than merge, because the request is "this is what this
    /// partner may call", and a merge cannot express removing a model — the
    /// operation an operator most needs when they discover a partner is calling
    /// something they should not. The delete and the insert are one
    /// transaction, so there is no instant at which a partner can call nothing
    /// because the old rows are gone and the new ones have not arrived.
    ///
    /// The caller must have validated the prices; [`ModelPrice`] carries
    /// `PricePerMillion`, which cannot be negative, so the `CHECK`s on the table
    /// are a backstop rather than the guard.
    pub fn replace_models(&self, consumer_id: &str, models: &[ModelPrice]) -> Result<()> {
        let mut seen = std::collections::BTreeSet::new();
        for entry in models {
            let model = entry.model.trim();
            if model.is_empty() {
                return Err(BillingError::Invalid(
                    "a model name must not be blank".to_string(),
                ));
            }
            if !seen.insert(model.to_string()) {
                return Err(BillingError::Invalid(format!(
                    "{model:?} is listed twice; a model has one price"
                )));
            }
        }

        let now = timefmt::format_ts(timefmt::now());
        let consumer = consumer_id.to_string();
        // Stored verbatim, not trimmed. The check above trims because a
        // whitespace-only name is a typo; *storing* the trimmed form would
        // silently rename a model the operator configured. The request path
        // compares model names literally, so a name rewritten here is a name
        // nobody can call — the operator would see a model in the partner's
        // list and a 404-ish refusal from the gate, with nothing to explain it.
        let rows: Vec<(String, i64, i64, i64)> = models
            .iter()
            .map(|entry| {
                let (input, cached, output) = entry.prices.as_tuple();
                (entry.model.clone(), input, cached, output)
            })
            .collect();

        let written = self.pool.write(|conn| {
            let tx = conn.transaction()?;
            tx.execute(
                "DELETE FROM partner_models WHERE consumer_id = ?1",
                [&consumer],
            )?;
            for (model, input, cached, output) in &rows {
                tx.execute(
                    "INSERT INTO partner_models (consumer_id, model, \
                     input_price_micro_usd_per_million, \
                     cached_input_price_micro_usd_per_million, \
                     output_price_micro_usd_per_million, created_at, updated_at) \
                     VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?6)",
                    params![consumer, model, input, cached, output, now],
                )?;
            }
            tx.execute(
                "UPDATE partners SET updated_at = ?2 WHERE consumer_id = ?1",
                params![consumer, now],
            )?;
            tx.commit()
        });

        match written {
            Ok(()) => Ok(()),
            // A price for a partner that does not exist. The caller asked about
            // the wrong partner, and saying so is more useful than "constraint
            // failed".
            Err(rusqlite::Error::SqliteFailure(e, _))
                if e.extended_code == SQLITE_CONSTRAINT_FOREIGNKEY =>
            {
                Err(BillingError::PartnerNotFound(consumer_id.to_string()))
            }
            Err(e) => Err(BillingError::Database(e)),
        }
    }

    // -----------------------------------------------------------------------
    // Statements
    // -----------------------------------------------------------------------

    /// Write a statement and its lines, or return the one that is already
    /// there.
    ///
    /// `INSERT ... ON CONFLICT DO NOTHING` against
    /// `UNIQUE (consumer_id, billing_date)` is the entire idempotency
    /// guarantee, and it is in the database rather than in this function
    /// because the race it has to survive — two instances, one day, a restart
    /// mid-flight — is between processes and not between calls. When the insert
    /// does nothing, the lines are not written either: the statement that is
    /// already there owns its own lines, and appending to them would be a
    /// second statement's worth of usage folded into the first.
    pub fn write_statement(&self, draft: &StatementDraft) -> Result<StatementOutcome> {
        validate_draft(draft)?;
        let total = draft.total()?;
        let now = timefmt::format_ts(timefmt::now());
        let consumer_id = draft.consumer_id.clone();
        let billing_date = draft.billing_date.to_string();

        let inserted = self.pool.write(|conn| {
            let tx = conn.transaction()?;
            let changed = tx.execute(
                "INSERT INTO daily_statements (
                     consumer_id, billing_date, billing_mode, currency,
                     period_start, period_end, billing_cutoff_at,
                     total_amount_micro_usd, incomplete_usage_count,
                     due_at, created_at, updated_at
                 ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?11)
                 ON CONFLICT(consumer_id, billing_date) DO NOTHING",
                params![
                    draft.consumer_id,
                    billing_date,
                    draft.billing_mode.as_str(),
                    crate::billing::CURRENCY,
                    draft.period_start,
                    draft.period_end,
                    draft.billing_cutoff_at,
                    total.as_i64(),
                    draft.incomplete_usage_count,
                    draft.due_at.map(timefmt::format_ts),
                    now
                ],
            )?;
            if changed == 0 {
                tx.commit()?;
                return Ok(false);
            }
            let statement_id = tx.last_insert_rowid();
            for line in &draft.lines {
                let (input_price, cached_price, output_price) = line.prices.as_tuple();
                tx.execute(
                    "INSERT INTO statement_lines (
                         statement_id, model,
                         input_price_micro_usd_per_million,
                         cached_input_price_micro_usd_per_million,
                         output_price_micro_usd_per_million,
                         request_count, input_tokens, cached_input_tokens,
                         uncached_input_tokens, output_tokens,
                         input_cost_micro_usd, cached_input_cost_micro_usd,
                         output_cost_micro_usd, total_cost_micro_usd
                     ) VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14)",
                    params![
                        statement_id,
                        line.model,
                        input_price,
                        cached_price,
                        output_price,
                        line.request_count,
                        line.input_tokens,
                        line.cached_input_tokens,
                        line.cost.uncached_input_tokens as i64,
                        line.output_tokens,
                        line.cost.input_cost.as_i64(),
                        line.cost.cached_input_cost.as_i64(),
                        line.cost.output_cost.as_i64(),
                        line.cost.total.as_i64(),
                    ],
                )?;
            }
            tx.commit()?;
            Ok(true)
        })?;

        let statement = self
            .get_statement(&consumer_id, &billing_date)?
            .ok_or_else(|| BillingError::PartnerNotFound(consumer_id.clone()))?;
        Ok(if inserted {
            StatementOutcome::Created(Box::new(statement))
        } else {
            StatementOutcome::Existed(Box::new(statement))
        })
    }

    pub fn get_statement(
        &self,
        consumer_id: &str,
        billing_date: &str,
    ) -> Result<Option<Statement>> {
        self.pool
            .read(|conn| {
                conn.query_row(
                    &format!(
                        "SELECT {STATEMENT_COLUMNS} FROM daily_statements \
                         WHERE consumer_id = ?1 AND billing_date = ?2"
                    ),
                    params![consumer_id, billing_date],
                    row_to_statement,
                )
                .optional()
            })
            .map_err(Into::into)
    }

    /// One statement by id, restricted to `scope`.
    ///
    /// The restriction is in the `WHERE` and not a check after the read. A
    /// statement id is a rowid, so it is guessable: a partner asking for
    /// `/{id}` has to be answered by a query that *cannot* return another
    /// partner's bill, and the caller renders the `None` as a 404. A comparison
    /// after the read would work until a second caller was added and forgot it;
    /// this way there is no reading path that can forget (invariant 7).
    pub fn get_statement_by_id(&self, id: i64, scope: &Scope) -> Result<Option<Statement>> {
        let (scope_sql, scope_params) = scope_clause(scope);
        self.pool
            .read(|conn| {
                let mut params: Vec<Value> = Vec::with_capacity(scope_params.len() + 1);
                params.push(Value::Integer(id));
                params.extend(scope_params);
                conn.query_row(
                    &format!(
                        "SELECT {STATEMENT_COLUMNS} FROM daily_statements \
                         WHERE id = ? AND {scope_sql}"
                    ),
                    params_from_iter(params.iter()),
                    row_to_statement,
                )
                .optional()
            })
            .map_err(Into::into)
    }

    /// One statement by id, with no scope filter.
    ///
    /// Private, and the only caller is [`Self::mark_paid`], which is reached
    /// from the manager-only admin route: recording a payment is an operator
    /// action against a statement id the operator chose. Scoping *that* to a
    /// credential would be scope-shaped theatre. Every read a partner can reach
    /// goes through [`Self::get_statement_by_id`] and carries a scope.
    fn statement_by_id(&self, id: i64) -> Result<Option<Statement>> {
        self.pool
            .read(|conn| {
                conn.query_row(
                    &format!("SELECT {STATEMENT_COLUMNS} FROM daily_statements WHERE id = ?1"),
                    [id],
                    row_to_statement,
                )
                .optional()
            })
            .map_err(Into::into)
    }

    /// A page of statements, newest first, restricted to `scope`.
    ///
    /// Bounded by `limit` and `offset` rather than unbounded: a partner that has
    /// been with the product for two years has hundreds of these, and the
    /// dashboard shows a page. `limit` is clamped by the caller, not here — this
    /// function honours what it is given, so an unbounded read is a caller's
    /// visible decision rather than a silent default.
    ///
    /// `only_unpaid` is the manager's *what is still owed* view: invoices with no
    /// payment recorded, ordered by deadline rather than by date, because the
    /// next thing that happens to an unpaid invoice is that it suspends someone.
    /// A reconciliation statement is never in that view — it carries no
    /// obligation to be owed. For a partner key the flag cannot hide anything
    /// (a partner sees only its own statements either way), but it is honoured
    /// rather than ignored, so both surfaces answer the same question the same
    /// way.
    pub fn list_statements(
        &self,
        scope: &Scope,
        limit: i64,
        offset: i64,
        only_unpaid: bool,
    ) -> Result<Vec<Statement>> {
        let (scope_sql, scope_params) = scope_clause(scope);
        self.pool
            .read(|conn| {
                let sql = if only_unpaid {
                    format!(
                        "SELECT {STATEMENT_COLUMNS} FROM daily_statements \
                         WHERE {scope_sql} AND paid_at IS NULL AND billing_mode = 'invoice' \
                         ORDER BY due_at, consumer_id, id LIMIT ? OFFSET ?"
                    )
                } else {
                    format!(
                        "SELECT {STATEMENT_COLUMNS} FROM daily_statements \
                         WHERE {scope_sql} \
                         ORDER BY billing_date DESC, id DESC LIMIT ? OFFSET ?"
                    )
                };
                let mut params = scope_params;
                params.push(Value::Integer(limit));
                params.push(Value::Integer(offset));
                let mut stmt = conn.prepare(&sql)?;
                let rows = stmt.query_map(params_from_iter(params.iter()), row_to_statement)?;
                rows.collect()
            })
            .map_err(Into::into)
    }

    /// What `scope` owes: how many invoices are unpaid, and what they total.
    ///
    /// Aggregated in SQL rather than by summing a page in Rust, because a page is
    /// capped and a total that silently stopped at the cap would understate what a
    /// partner owes. Reconciliation statements are excluded — they are never owed
    /// (see [`Statement::is_outstanding`]) — and so is anything already paid.
    ///
    /// This is the number for a dashboard header, not the number that suspends
    /// anyone: `can_suspend` additionally requires a complete statement with a
    /// deadline and a non-zero amount, and a partner who is suspended is
    /// suspended by one statement, not by a sum.
    pub fn outstanding_totals(&self, scope: &Scope) -> Result<(i64, MicroUsd)> {
        let (scope_sql, scope_params) = scope_clause(scope);
        self.pool
            .read(|conn| {
                conn.query_row(
                    &format!(
                        "SELECT COUNT(*), COALESCE(SUM(total_amount_micro_usd), 0) \
                         FROM daily_statements \
                         WHERE {scope_sql} AND billing_mode = 'invoice' AND paid_at IS NULL"
                    ),
                    params_from_iter(scope_params.iter()),
                    |row| Ok((row.get(0)?, MicroUsd::from_i64(row.get(1)?))),
                )
            })
            .map_err(Into::into)
    }

    /// How many statements `scope` can see, for paging.
    ///
    /// Scoped like the page it counts, so a manager narrowing to one consumer
    /// gets that consumer's total rather than the organisation's — a page count
    /// wider than the page itself is how a paginator starts offering empty
    /// pages.
    pub fn count_statements(&self, scope: &Scope) -> Result<i64> {
        let (scope_sql, scope_params) = scope_clause(scope);
        self.pool
            .read(|conn| {
                conn.query_row(
                    &format!("SELECT COUNT(*) FROM daily_statements WHERE {scope_sql}"),
                    params_from_iter(scope_params.iter()),
                    |row| row.get(0),
                )
            })
            .map_err(Into::into)
    }

    /// A statement's lines, in a stable order, for a caller in `scope`.
    ///
    /// `statement_lines` carries no `consumer_id` of its own, so the filter that
    /// keeps one partner's per-model prices and token counts out of another's
    /// response cannot live in this table's own `WHERE` — it has to come
    /// through the statement. Joining on `daily_statements` and applying the
    /// scope there is what puts that check *in the query*, instead of leaving it
    /// to depend on every caller having fetched the statement first and
    /// returned early when it was not theirs. A statement that is not the
    /// caller's yields no rows, which is the same answer the statement lookup
    /// gives, so the two agree rather than one of them being the only thing
    /// standing between a guessable row id and a competitor's prices.
    pub fn statement_lines(&self, statement_id: i64, scope: &Scope) -> Result<Vec<StatementLine>> {
        let (scope_sql, mut scope_params) = scope_clause(scope);
        scope_params.push(rusqlite::types::Value::from(statement_id));

        self.pool
            .read(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT l.id, l.statement_id, l.model, \
                     l.input_price_micro_usd_per_million, \
                     l.cached_input_price_micro_usd_per_million, \
                     l.output_price_micro_usd_per_million, \
                     l.request_count, l.input_tokens, l.cached_input_tokens, \
                     l.uncached_input_tokens, l.output_tokens, \
                     l.input_cost_micro_usd, l.cached_input_cost_micro_usd, \
                     l.output_cost_micro_usd, l.total_cost_micro_usd \
                     FROM statement_lines l \
                     JOIN daily_statements s ON s.id = l.statement_id \
                     WHERE {scope_sql} AND l.statement_id = ?{} \
                     ORDER BY l.model, l.input_price_micro_usd_per_million, \
                              l.cached_input_price_micro_usd_per_million, \
                              l.output_price_micro_usd_per_million, l.id",
                    scope_params.len()
                ))?;
                let rows = stmt.query_map(params_from_iter(scope_params.iter()), |row| {
                    Ok(StatementLine {
                        id: row.get(0)?,
                        statement_id: row.get(1)?,
                        model: row.get(2)?,
                        prices: PricingSnapshot::new(
                            crate::billing::pricing::PricePerMillion::new(row.get(3)?),
                            crate::billing::pricing::PricePerMillion::new(row.get(4)?),
                            crate::billing::pricing::PricePerMillion::new(row.get(5)?),
                        ),
                        request_count: row.get(6)?,
                        input_tokens: row.get(7)?,
                        cached_input_tokens: row.get(8)?,
                        uncached_input_tokens: row.get(9)?,
                        output_tokens: row.get(10)?,
                        input_cost_micro_usd: row.get(11)?,
                        cached_input_cost_micro_usd: row.get(12)?,
                        output_cost_micro_usd: row.get(13)?,
                        total_cost_micro_usd: row.get(14)?,
                    })
                })?;
                rows.collect()
            })
            .map_err(Into::into)
    }

    /// Record a payment. Idempotent, and refused for a statement that has none
    /// to record.
    ///
    /// The guard is in the `WHERE`: `paid_at IS NULL` means a second call
    /// changes nothing and reports the statement as it already stands, so a
    /// retried request cannot overwrite the first payment's reference. A
    /// reconciliation statement is refused outright — there is no payment to
    /// record against a bill that carries no obligation, and the row would
    /// violate the table's `CHECK` anyway.
    pub fn mark_paid(
        &self,
        id: i64,
        paid_by: &str,
        reference: Option<&str>,
        note: Option<&str>,
    ) -> Result<Statement> {
        if paid_by.trim().is_empty() {
            return Err(BillingError::Invalid(
                "a payment must name who recorded it".to_string(),
            ));
        }
        let now = timefmt::format_ts(timefmt::now());
        self.pool.write(|conn| {
            conn.execute(
                "UPDATE daily_statements SET \
                     paid_at = ?2, paid_by = ?3, payment_reference = ?4, \
                     payment_note = ?5, updated_at = ?2 \
                 WHERE id = ?1 AND billing_mode = 'invoice' AND paid_at IS NULL",
                params![id, now, paid_by, reference, note],
            )
        })?;

        // Read back rather than trusting the change count: a statement that was
        // already paid matches no row either, and the two must not be reported
        // as the same thing.
        let statement = self
            .statement_by_id(id)?
            .ok_or_else(|| BillingError::PartnerNotFound(format!("statement {id}")))?;
        if !statement.is_paid() {
            return Err(BillingError::StatementNotPayable {
                id,
                billing_mode: statement.billing_mode,
            });
        }
        Ok(statement)
    }

    /// Statements that are past their due date and still owed, oldest first.
    ///
    /// One partner's, when `consumer_id` is given. The predicate is exactly the
    /// one the snapshot refresh uses, deliberately: a second definition of
    /// "overdue" would be a second answer to "is this partner suspended".
    pub fn overdue_statements(&self, consumer_id: Option<&str>) -> Result<Vec<Statement>> {
        let now = timefmt::format_ts(timefmt::now());
        self.pool
            .read(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "SELECT {STATEMENT_COLUMNS} FROM daily_statements \
                     WHERE billing_mode = 'invoice' AND paid_at IS NULL \
                       AND due_at IS NOT NULL AND due_at <= ?1 \
                       AND (?2 IS NULL OR consumer_id = ?2) \
                     ORDER BY due_at, id"
                ))?;
                let rows = stmt.query_map(params![now, consumer_id], row_to_statement)?;
                rows.collect()
            })
            .map_err(Into::into)
    }

    /// Everything ever statemented for a partner, across every day.
    ///
    /// This is the number a statement would have to be re-derived from if the
    /// lines were ever lost, and the number the admin partner list shows. It is
    /// read from `daily_statements` and never recomputed from `usage_records`:
    /// retention prunes usage, and a lifetime total that silently shrank when a
    /// sweep ran would be worse than no total at all.
    pub fn total_billed(&self, consumer_id: &str) -> Result<MicroUsd> {
        let sum: i64 = self.pool.read(|conn| {
            conn.query_row(
                "SELECT COALESCE(SUM(total_amount_micro_usd), 0) FROM daily_statements \
                 WHERE consumer_id = ?1",
                [consumer_id],
                |row| row.get(0),
            )
        })?;
        Ok(MicroUsd::from_i64(sum))
    }

    /// The latest billing date a *partner* has a statement for, or `None`.
    ///
    /// The worker walks forward from here, so a process that was down for a week
    /// closes seven days rather than one. Per partner rather than global, and
    /// that is the whole reason it takes a `consumer_id`: the walk's anchor is
    /// also its retry marker. If a day is written for one partner and fails for
    /// another, a global `MAX` would have advanced past it and the second
    /// partner's statement would never be attempted again — a silent loss of
    /// that partner's bill, on the path that only runs when something has
    /// already gone wrong.
    pub fn last_statement_date_for(&self, consumer_id: &str) -> Result<Option<String>> {
        self.pool
            .read(|conn| {
                conn.query_row(
                    "SELECT MAX(billing_date) FROM daily_statements WHERE consumer_id = ?1",
                    [consumer_id],
                    |row| row.get::<_, Option<String>>(0),
                )
            })
            .map_err(Into::into)
    }

    /// Statement email bookkeeping, written in one statement.
    ///
    /// `email_sent_at` is the only claim that a message left, and it is set
    /// exactly once: `WHERE email_sent_at IS NULL` means a retry that races a
    /// success cannot un-record it.
    pub fn record_email_attempt(&self, id: i64, outcome: EmailOutcome<'_>) -> Result<()> {
        let now = timefmt::format_ts(timefmt::now());
        match outcome {
            EmailOutcome::Sent => {
                self.pool.write(|conn| {
                    conn.execute(
                        "UPDATE daily_statements SET \
                             email_sent_at = COALESCE(email_sent_at, ?2), \
                             email_attempts = email_attempts + 1, \
                             email_last_error = NULL, \
                             email_next_retry_at = NULL, \
                             email_claim_instance = NULL, \
                             email_claimed_until = NULL, \
                             updated_at = ?2 \
                         WHERE id = ?1",
                        params![id, now],
                    )
                    .map(|_| ())
                })?;
            }
            EmailOutcome::Failed { error, retry_at } => {
                self.pool.write(|conn| {
                    conn.execute(
                        "UPDATE daily_statements SET \
                             email_attempts = email_attempts + 1, \
                             email_last_error = ?2, \
                             email_next_retry_at = ?3, \
                             email_claim_instance = NULL, \
                             email_claimed_until = NULL, \
                             updated_at = ?4 \
                         WHERE id = ?1",
                        params![id, error, timefmt::format_ts(retry_at), now],
                    )
                    .map(|_| ())
                })?;
            }
        }
        Ok(())
    }

    /// Take the send lease on up to `limit` statements, and return them.
    ///
    /// Two instances must never email the same statement at the same time, and
    /// a crash between sending and committing is an accepted duplicate. A lease
    /// with an expiry is what makes the crash survivable: the row is claimed for
    /// `EMAIL_CLAIM_TTL_MINUTES`, and a claim held by an instance that died
    /// lapses on its own rather than needing the next instance to clean up.
    ///
    /// The `UPDATE ... RETURNING` is one statement, so the claim and the read
    /// cannot be separated by another instance's claim.
    ///
    /// # What is not claimed, and why the predicate lives here
    ///
    /// Three conditions beyond "not yet sent", and all three are properties of
    /// the statement rather than of the sender — so they belong in the query,
    /// where they are one answer, rather than in a caller that has to remember
    /// them:
    ///
    /// * **An invoice.** A reconciliation statement is a settlement record, and
    ///   the partner owes nothing on it, so there is nothing to ask for. This is
    ///   the same rule as the request path's, read from the same column.
    /// * **An address on file.** [`Partner::emails_statements`] is
    ///   `invoice && !billing_email.is_empty()`; this is that second half. A
    ///   partner with no address is not a send failure to record — nothing was
    ///   attempted, and the missing address is visible in the admin partner
    ///   list, which is where an operator can fix it.
    /// * **Something to say.** A day with no billable usage and nothing it could
    ///   not measure is stated at zero, and a message saying "you owe $0.000000"
    ///   is noise a partner learns to filter — including the one that matters. A
    ///   statement the product could not fully measure *is* worth sending even at
    ///   zero, because the explanation is the point.
    ///
    /// # Why `now` is a parameter
    ///
    /// The caller passes the instant its tick was made at, and this function
    /// reads no clock of its own. A second "now" inside one tick is a second
    /// answer to *is the retry due?* and *has this lease lapsed?*, and the two
    /// answers can disagree in the same tick: the retry the worker just
    /// scheduled against the tick's instant would be compared against a wall
    /// clock a few milliseconds ahead of it, and a lease read against a clock
    /// behind it. Both directions are wrong in a way that costs a partner an
    /// email or sends them two, so there is exactly one instant, and the caller
    /// owns it. It also makes the predicate testable against a simulated clock
    /// rather than only against today.
    ///
    /// [`Partner::emails_statements`]: crate::billing::partner::Partner::emails_statements
    pub fn claim_emails(
        &self,
        now: time::OffsetDateTime,
        instance_id: &str,
        limit: i64,
        claim_ttl_minutes: i64,
    ) -> Result<Vec<Statement>> {
        let now_text = timefmt::format_ts(now);
        let claim_until = timefmt::format_ts(now + time::Duration::minutes(claim_ttl_minutes));
        // A write, on the writer connection: the claim is a mutation and must
        // serialise with the metering writer like every other. The statement is
        // one `UPDATE ... RETURNING`, so this instance cannot read a row that
        // another instance claimed in between.
        self.pool
            .write(|conn| {
                let mut stmt = conn.prepare(&format!(
                    "UPDATE daily_statements SET \
                         email_claim_instance = ?1, email_claimed_until = ?2 \
                     WHERE id IN ( \
                         SELECT d.id FROM daily_statements d \
                         WHERE d.email_sent_at IS NULL \
                           AND d.billing_mode = 'invoice' \
                           AND d.paid_at IS NULL \
                           AND (d.total_amount_micro_usd > 0 OR d.incomplete_usage_count > 0) \
                           AND EXISTS ( \
                               SELECT 1 FROM partners p \
                               WHERE p.consumer_id = d.consumer_id \
                                 AND TRIM(p.billing_email) <> '' \
                           ) \
                           AND (d.email_next_retry_at IS NULL OR d.email_next_retry_at <= ?3) \
                           AND (d.email_claimed_until IS NULL OR d.email_claimed_until <= ?3) \
                         ORDER BY d.due_at, d.id LIMIT ?4 \
                     ) \
                     RETURNING {STATEMENT_COLUMNS}"
                ))?;
                let rows = stmt.query_map(
                    params![instance_id, claim_until, now_text, limit],
                    row_to_statement,
                )?;
                rows.collect::<rusqlite::Result<Vec<_>>>()
            })
            .map_err(Into::into)
    }

    /// The partner's billing email, for the worker to send to.
    ///
    /// Read separately from the snapshot because a statement can be issued for
    /// a partner whose key is currently revoked — the account still owes the
    /// money, and there is no snapshot entry to read the address from.
    pub fn statement_recipient(&self, consumer_id: &str) -> Result<Option<String>> {
        self.pool
            .read(|conn| {
                conn.query_row(
                    "SELECT billing_email FROM partners WHERE consumer_id = ?1",
                    [consumer_id],
                    |row| row.get::<_, String>(0),
                )
                .optional()
            })
            .map_err(Into::into)
    }
}

/// The result of a send attempt.
pub enum EmailOutcome<'a> {
    Sent,
    Failed {
        error: &'a str,
        retry_at: OffsetDateTime,
    },
}

const PARTNER_COLUMNS: &str = "consumer_id, name, billing_email, billing_mode, \
     payment_terms_minutes, created_at, updated_at";

const STATEMENT_COLUMNS: &str = "id, consumer_id, billing_date, billing_mode, currency, \
     period_start, period_end, billing_cutoff_at, total_amount_micro_usd, \
     incomplete_usage_count, due_at, paid_at, paid_by, payment_reference, \
     payment_note, email_sent_at, email_attempts, email_last_error, \
     email_next_retry_at, created_at, updated_at";

fn row_to_partner(row: &Row<'_>) -> rusqlite::Result<Partner> {
    let mode: String = row.get(3)?;
    Ok(Partner {
        consumer_id: row.get(0)?,
        name: row.get(1)?,
        billing_email: row.get(2)?,
        // The column has a `CHECK`, so this cannot be an unrecognised value
        // from a database this product wrote. Reading it as `invoice` is the
        // only safe residue: it is the mode that bills, and the alternative is
        // to pretend the partner owes nothing.
        billing_mode: BillingMode::parse(&mode).unwrap_or(BillingMode::Invoice),
        payment_terms_minutes: row.get(4)?,
        created_at: row.get(5)?,
        updated_at: row.get(6)?,
    })
}

fn row_to_statement(row: &Row<'_>) -> rusqlite::Result<Statement> {
    Ok(Statement {
        id: row.get(0)?,
        consumer_id: row.get(1)?,
        billing_date: row.get(2)?,
        billing_mode: row.get(3)?,
        currency: row.get(4)?,
        period_start: row.get(5)?,
        period_end: row.get(6)?,
        billing_cutoff_at: row.get(7)?,
        total_amount_micro_usd: row.get(8)?,
        incomplete_usage_count: row.get(9)?,
        due_at: row.get(10)?,
        paid_at: row.get(11)?,
        paid_by: row.get(12)?,
        payment_reference: row.get(13)?,
        payment_note: row.get(14)?,
        email_sent_at: row.get(15)?,
        email_attempts: row.get(16)?,
        email_last_error: row.get(17)?,
        email_next_retry_at: row.get(18)?,
        created_at: row.get(19)?,
        updated_at: row.get(20)?,
    })
}

impl NewPartner {
    fn validated(mut self) -> Result<Self> {
        self.consumer_id = self.consumer_id.trim().to_string();
        self.name = self.name.trim().to_string();
        self.billing_email = self.billing_email.trim().to_string();
        if self.consumer_id.is_empty() {
            return Err(BillingError::Invalid(
                "consumer_id must not be blank".to_string(),
            ));
        }
        if self.name.is_empty() {
            return Err(BillingError::Invalid("name must not be blank".to_string()));
        }
        if self.payment_terms_minutes < 0 {
            return Err(BillingError::Invalid(
                "payment_terms_minutes must not be negative".to_string(),
            ));
        }
        Ok(self)
    }
}

/// Refuse a statement that contradicts itself before it reaches SQL.
///
/// The table has `CHECK`s for all of this. Checking here as well is not
/// redundancy for its own sake: a constraint violation inside the worker's
/// transaction aborts the whole day's statement, and the failure an operator
/// reads is a SQLite message. Naming the defect in the language of the
/// statement is what makes it fixable.
fn validate_draft(draft: &StatementDraft) -> std::result::Result<(), StatementRejected> {
    match (draft.billing_mode.owes_payment(), draft.due_at) {
        (true, None) => return Err(StatementRejected::MissingDueDate),
        (false, Some(_)) => return Err(StatementRejected::UnexpectedDueDate),
        _ => {}
    }
    for line in &draft.lines {
        if line.cost.uncached_input_tokens as i64 != line.input_tokens - line.cached_input_tokens {
            return Err(StatementRejected::InconsistentLine(line.model.clone()));
        }
        if line.cached_input_tokens > line.input_tokens {
            return Err(StatementRejected::InconsistentLine(line.model.clone()));
        }
    }
    Ok(())
}

/// A day's usage, grouped by model and by the price the rows were metered at.
///
/// # Why the price columns are in the grouping
///
/// A price change during a day produces two groups for one model, and the
/// statement keeps both. Averaging them into one line would be smaller and
/// wrong: the operator changed the price for a reason, and the statement is how
/// anyone finds out what it cost. The grouping key is the same tuple as
/// `statement_lines`' `UNIQUE` constraint, so the row this produces is the row
/// the table will accept.
///
/// # Why `usage_status = 'available'` is not in the WHERE
///
/// Because the incomplete ones have to be *counted*, and a query that filtered
/// them out could not count them. Every row in the period is classified by what
/// it actually contains — the tokens and the prices — rather than by the label
/// beside them, and the two populations come back as two counts: the summable
/// one and the counted one.
///
/// The half-open `created_at >= ? AND created_at < ?` is on the textual form of
/// the timestamp, which is fixed-width and therefore sorts as it compares (see
/// [`crate::ledger::timefmt`]) — the index on
/// `(consumer_id, created_at, id)` is used directly.
pub fn usage_groups(
    conn: &Connection,
    consumer_id: &str,
    period_start: &str,
    period_end: &str,
) -> rusqlite::Result<Vec<UsageGroup>> {
    // `billable` is the sum filter, not the authority. `line_for` re-applies the
    // same rules in Rust to every group it is handed, and a group it refuses is
    // counted as incomplete rather than charged (see the caller), so a
    // divergence between the two cannot invent a charge — the worst it can do
    // is decline to charge something billable, which is the safe direction and
    // shows up in the statement's incomplete count.
    //
    // `COALESCE(..., 0)` is not decoration. SQL's `AND` is three-valued, so a
    // row with a `NULL` in it evaluates to `NULL` rather than to false — and
    // `COUNT(*) FILTER (WHERE NULL)` excludes it from *both* counts, dropping
    // the request out of the statement silently. That is the one failure this
    // module exists to prevent.
    //
    // The rules, in the order `crate::billing::pricing` applies them:
    //  * input and output tokens reported, and not negative;
    //  * a price snapshot in force — all three columns;
    //  * a cached count, unless the cached price equals the input price, in
    //    which case an unreported count costs exactly what zero costs (the rule
    //    `needs_cached_count` encodes in Rust);
    //  * cached tokens not exceeding input tokens.
    let mut stmt = conn.prepare(
        "WITH classified AS ( \
             SELECT model, input_price_snapshot, cached_input_price_snapshot, \
                    output_price_snapshot, input_tokens, cached_tokens, output_tokens, \
                    COALESCE( \
                        input_tokens IS NOT NULL AND input_tokens >= 0 \
                        AND output_tokens IS NOT NULL AND output_tokens >= 0 \
                        AND input_price_snapshot IS NOT NULL \
                        AND cached_input_price_snapshot IS NOT NULL \
                        AND output_price_snapshot IS NOT NULL \
                        AND (cached_input_price_snapshot = input_price_snapshot \
                             OR cached_tokens IS NOT NULL) \
                        AND COALESCE(cached_tokens, 0) >= 0 \
                        AND COALESCE(cached_tokens, 0) <= input_tokens, 0) AS billable \
             FROM usage_records \
             WHERE consumer_id = ?1 AND created_at >= ?2 AND created_at < ?3 \
         ) \
         SELECT model, input_price_snapshot, cached_input_price_snapshot, \
                output_price_snapshot, \
                COUNT(*) FILTER (WHERE billable), \
                SUM(input_tokens) FILTER (WHERE billable), \
                SUM(cached_tokens) FILTER (WHERE billable), \
                SUM(output_tokens) FILTER (WHERE billable), \
                COUNT(*) FILTER (WHERE NOT billable) \
         FROM classified \
         GROUP BY model, input_price_snapshot, cached_input_price_snapshot, \
                  output_price_snapshot \
         ORDER BY model, input_price_snapshot, cached_input_price_snapshot, \
                  output_price_snapshot",
    )?;
    let rows = stmt.query_map(params![consumer_id, period_start, period_end], |row| {
        Ok(UsageGroup {
            model: row.get(0)?,
            prices: PricingSnapshot::from_columns(
                row.get::<_, Option<i64>>(1)?,
                row.get::<_, Option<i64>>(2)?,
                row.get::<_, Option<i64>>(3)?,
            )
            .ok(),
            request_count: row.get(4)?,
            input_tokens: row.get(5)?,
            cached_tokens: row.get(6)?,
            output_tokens: row.get(7)?,
            incomplete_count: row.get(8)?,
        })
    })?;
    rows.collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::partner::BillingMode;
    use crate::billing::pricing::PricePerMillion;
    use crate::ledger::LedgerPool;

    /// A pool with the schema applied, and the store pointed at it.
    fn store() -> (tempfile::TempDir, BillingStore) {
        let dir = tempfile::TempDir::new().unwrap();
        let pool = Arc::new(LedgerPool::new(dir.path().join("ledger.db")).unwrap());
        (dir, BillingStore::new(pool))
    }

    fn prices(input: i64, cached: i64, output: i64) -> PricingSnapshot {
        PricingSnapshot::new(
            PricePerMillion::new(input),
            PricePerMillion::new(cached),
            PricePerMillion::new(output),
        )
    }

    fn price(model: &str, input: i64, cached: i64, output: i64) -> ModelPrice {
        ModelPrice {
            model: model.to_string(),
            prices: prices(input, cached, output),
        }
    }

    fn new_partner(consumer_id: &str) -> NewPartner {
        NewPartner {
            consumer_id: consumer_id.to_string(),
            name: "Acme".to_string(),
            billing_email: "billing@acme.test".to_string(),
            billing_mode: BillingMode::Invoice,
            payment_terms_minutes: 720,
        }
    }

    /// A blank model name in a price list is a typo that would grant access to
    /// nothing, or to something nobody meant to name, and it is refused.
    ///
    /// This rule used to live on the key's `allowed_models` and before that in
    /// `config.yaml`. The list moved to `partner_models` (ADR 0015); the rule
    /// moved with it, and this is the assertion that it did not get left behind
    /// when the config keys were deleted.
    ///
    /// Model names are not credentials, so the message may name the offending
    /// entry without echoing a secret — and it does.
    #[test]
    fn test_a_blank_model_name_is_refused_when_pricing_a_partner() {
        let (_dir, store) = store();
        store.create_partner(new_partner("acme")).unwrap();

        for (label, models) in [
            ("empty", vec![price("", 95_000, 95_000, 475_000)]),
            ("whitespace", vec![price("   ", 95_000, 95_000, 475_000)]),
            (
                "empty-after-a-good-one",
                vec![
                    price("gpt-4o", 95_000, 95_000, 475_000),
                    price("\t", 95_000, 95_000, 475_000),
                ],
            ),
        ] {
            let err = store
                .replace_models("acme", &models)
                .expect_err(&format!("a {label} model name must be refused"));
            assert!(
                err.to_string().contains("model name"),
                "the error must name the offending field: {err}"
            );
        }

        // And a refused replacement wrote nothing: a price list that is
        // half-applied is a partner paying one price here and another there.
        assert!(
            store.models("acme").unwrap().is_empty(),
            "a refused replacement must not have written anything"
        );
    }

    /// One model has one price, so naming it twice in one list is a request
    /// that cannot be honoured — the second price would either lose silently or
    /// win arbitrarily.
    #[test]
    fn test_a_duplicate_model_in_one_price_list_is_refused() {
        let (_dir, store) = store();
        store.create_partner(new_partner("acme")).unwrap();

        let err = store
            .replace_models(
                "acme",
                &[
                    price("gpt-4o", 95_000, 95_000, 475_000),
                    price("gpt-4o", 15_000, 15_000, 60_000),
                ],
            )
            .expect_err("two prices for one model must be refused");
        assert!(
            err.to_string().contains("listed twice"),
            "the error must say what is wrong, not just that something is: {err}"
        );
        assert!(store.models("acme").unwrap().is_empty());
    }

    /// The permission list is the price list, and an empty one means *no*
    /// model — never "everything".
    ///
    /// The dangerous direction is the empty list read as permissive, so this is
    /// asserted through a fresh read of the database rather than through the
    /// value the test handed in: what matters is what a restarted process would
    /// load, not what this call already had in hand.
    #[test]
    fn test_an_empty_price_list_admits_nothing_and_is_not_read_as_permissive() {
        let (_dir, store) = store();
        store.create_partner(new_partner("acme")).unwrap();

        // A partner with no rows at all.
        assert!(store.models("acme").unwrap().is_empty());

        // A partner whose list was set and then cleared.
        store
            .replace_models("acme", &[price("gpt-4o", 95_000, 95_000, 475_000)])
            .unwrap();
        assert_eq!(store.models("acme").unwrap().len(), 1);
        store.replace_models("acme", &[]).unwrap();

        let after = store.models("acme").unwrap();
        assert!(
            after.is_empty(),
            "an empty price list must come back empty, not as a wildcard: {after:?}"
        );
    }

    /// Replacing a list is a replacement, not a merge: a model that is dropped
    /// stops being callable *and* stops being priced in the same transaction.
    ///
    /// The quiet failure this guards is a merge, where withdrawing a model
    /// leaves its price behind for the statement generator to find.
    #[test]
    fn test_replacing_a_price_list_removes_what_it_does_not_repeat() {
        let (_dir, store) = store();
        store.create_partner(new_partner("acme")).unwrap();

        store
            .replace_models(
                "acme",
                &[
                    price("gpt-4o", 95_000, 95_000, 475_000),
                    price("gpt-4o-mini", 15_000, 7_500, 60_000),
                ],
            )
            .unwrap();

        store
            .replace_models("acme", &[price("gpt-5", 125_000, 12_500, 1_000_000)])
            .unwrap();

        let models = store.models("acme").unwrap();
        assert_eq!(models.len(), 1, "the old entries must be gone: {models:?}");
        assert_eq!(models[0].model, "gpt-5");
        assert_eq!(models[0].prices.input.as_i64(), 125_000);
        assert_eq!(models[0].prices.cached_input.as_i64(), 12_500);
        assert_eq!(models[0].prices.output.as_i64(), 1_000_000);
    }

    /// A model name is stored and read back byte for byte, including the
    /// awkward ones. The list used to be a JSON array in a column, where a
    /// quote, a backslash or a comma needed escaping; it is now a row per
    /// model, and the values that broke the old encoding are exactly the ones
    /// worth re-checking against the new one.
    #[test]
    fn test_awkward_model_names_survive_storage_unchanged() {
        let (_dir, store) = store();
        store.create_partner(new_partner("acme")).unwrap();

        let awkward: Vec<String> = vec![
            "gpt-4o".to_string(),
            "vendor/model:free".to_string(),
            "a,\"quoted\",name".to_string(),
            r"back\slash".to_string(),
            "gpt-4ö".to_string(),
            "日本語モデル".to_string(),
            "  spaced  ".to_string(),
        ]
        .into_iter()
        // The list is stored in name order so that two reads agree; sorting
        // here rather than assuming the input order keeps the assertion about
        // the values, not about the ordering rule.
        .collect::<std::collections::BTreeSet<_>>()
        .into_iter()
        .collect();

        let priced: Vec<ModelPrice> = awkward
            .iter()
            .map(|model| price(model, 95_000, 95_000, 475_000))
            .collect();
        store.replace_models("acme", &priced).unwrap();

        let read_back: Vec<String> = store
            .models("acme")
            .unwrap()
            .into_iter()
            .map(|entry| entry.model)
            .collect();
        assert_eq!(
            read_back, awkward,
            "a model name must survive storage exactly"
        );
    }

    /// Pricing a partner that does not exist is refused as that, rather than as
    /// a constraint failure: the foreign key is the enforcement and the message
    /// is the diagnosis.
    #[test]
    fn test_pricing_an_unknown_partner_is_reported_as_an_unknown_partner() {
        let (_dir, store) = store();
        let err = store
            .replace_models("nobody", &[price("gpt-4o", 95_000, 95_000, 475_000)])
            .expect_err("a price for a partner that does not exist must be refused");
        assert!(
            matches!(err, BillingError::PartnerNotFound(ref id) if id == "nobody"),
            "expected PartnerNotFound(nobody), got {err:?}"
        );
    }

    /// A partner rounded back to zero is a price list that costs nothing, which
    /// is the one number the product must never invent. Documented here as a
    /// test because `replace_models` does *not* refuse it — zero is a price an
    /// operator may genuinely set — and the protection against it being
    /// mistaken for "unpriced" lives in `PricingSnapshot`, which stores it as a
    /// value rather than as `NULL`.
    #[test]
    fn test_a_zero_price_is_stored_as_a_price_not_as_an_absence() {
        let (_dir, store) = store();
        store.create_partner(new_partner("acme")).unwrap();
        store
            .replace_models("acme", &[price("free-model", 0, 0, 0)])
            .unwrap();

        let stored = store.models("acme").unwrap();
        let snapshot = stored[0].prices;
        assert_eq!(snapshot.input.as_i64(), 0);
        assert_eq!(snapshot.cached_input.as_i64(), 0);
        assert_eq!(snapshot.output.as_i64(), 0);
        // Round-tripping a zero must not produce the "no snapshot" answer.
        assert_eq!(
            PricingSnapshot::from_columns(Some(0), Some(0), Some(0)).unwrap(),
            snapshot
        );
    }

    /// One priced line for a model, enough to make a statement non-empty.
    fn a_line(model: &str, tokens: i64) -> StatementLineDraft {
        StatementLineDraft {
            model: model.to_string(),
            prices: prices(1_000_000, 100_000, 4_000_000),
            request_count: 1,
            input_tokens: tokens,
            cached_input_tokens: 0,
            output_tokens: 0,
            cost: LineCost {
                uncached_input_tokens: tokens as u64,
                input_cost: MicroUsd::from_i64(tokens),
                cached_input_cost: MicroUsd::from_i64(0),
                output_cost: MicroUsd::from_i64(0),
                total: MicroUsd::from_i64(tokens),
            },
        }
    }

    fn a_draft(consumer_id: &str, day: &str) -> StatementDraft {
        StatementDraft {
            consumer_id: consumer_id.to_string(),
            billing_date: BillingDay::parse(day).expect("a parseable day"),
            billing_mode: BillingMode::Invoice,
            period_start: format!("{day}T00:00:00.000000000Z"),
            period_end: format!("{day}T00:00:00.000000000Z"),
            billing_cutoff_at: format!("{day}T00:00:00.000000000Z"),
            // An invoice statement has to carry a due date — the store refuses
            // one without, which is the same check a real write goes through.
            due_at: Some(time::macros::datetime!(2026-10-02 00:00 UTC)),
            incomplete_usage_count: 0,
            lines: vec![a_line("gpt-4o", 1_000)],
        }
    }

    /// A partner's statement lines are filtered by the scope *in the query*.
    ///
    /// `statement_lines` has no `consumer_id` column of its own, so for a while
    /// the only thing keeping beta's per-model prices and token counts out of
    /// acme's response was that the HTTP handler fetched the statement first
    /// and returned early when it was not theirs. That is a filter held in
    /// control flow rather than in SQL: reorder those two lines, or fetch the
    /// lines first to render a count on a list endpoint, and the guard is gone
    /// with nothing left to make it look wrong. The join puts the scope in the
    /// query, and this is the test that goes red if it is ever taken back out.
    #[test]
    fn test_statement_lines_are_filtered_by_scope_and_not_by_caller_ordering() {
        let (_dir, store) = store();
        store.create_partner(new_partner("acme")).unwrap();
        store.create_partner(new_partner("beta")).unwrap();

        let acme_id = store
            .write_statement(&a_draft("acme", "2026-09-25"))
            .expect("acme's statement is written")
            .statement()
            .id;
        let beta_id = store
            .write_statement(&a_draft("beta", "2026-09-25"))
            .expect("beta's statement is written")
            .statement()
            .id;
        assert_ne!(acme_id, beta_id, "the two fixtures must be distinct rows");

        // Asked directly, with no statement lookup in front of it at all.
        let foreign = store
            .statement_lines(beta_id, &Scope::One("acme".to_string()))
            .expect("the read is answered");
        assert!(
            foreign.is_empty(),
            "another consumer's lines must not come back, and this asks for them by \
             id with nothing else having authorised it: {foreign:?}"
        );

        // Their own lines, and a scope that names them, still work — the join is
        // a filter, not a way of dropping rows a legitimate caller wanted.
        assert_eq!(
            store
                .statement_lines(acme_id, &Scope::One("acme".to_string()))
                .expect("the read is answered")
                .len(),
            1,
            "a partner reads their own line"
        );
        assert_eq!(
            store
                .statement_lines(acme_id, &Scope::All)
                .expect("the read is answered")
                .len(),
            1,
            "a manager reads any line"
        );
        assert_eq!(
            store
                .statement_lines(beta_id, &Scope::List(vec!["acme".into(), "beta".into()]))
                .expect("the read is answered")
                .len(),
            1,
            "a manager list that includes the owner still reads it"
        );
    }
}
