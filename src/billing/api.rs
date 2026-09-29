//! Statements over HTTP, scoped exactly like every other consumer read.
//!
//! # Who sees what
//!
//! A partner key sees its own statements and nothing else. The scope is resolved
//! by [`crate::auth::resolve_scope`] and rendered by
//! [`crate::auth::scope_clause`] — the same two functions the dashboard's ledger
//! views use — and it reaches SQL as a `WHERE`, not as a check afterwards. A
//! statement id is a rowid and therefore guessable, so `GET /api/billing/statements/7`
//! from the wrong partner has to be answered by a query that cannot return the
//! row: it is a 404, and it is the same 404 the partner would get for an id that
//! does not exist anywhere. Distinguishing the two would tell one partner that
//! another partner's statement id is real.
//!
//! A manager sees every statement, and the `consumers` parameter can only narrow
//! that (ADR 0013). The manager's list lives on
//! [`crate::admin::billing`], because marking a statement paid is an operator
//! action and the two belong on one surface; the views below are shared, so the
//! two surfaces cannot render the same row two different ways.
//!
//! # Why the partner's copy of a statement omits the email bookkeeping
//!
//! `email_attempts`, `email_last_error` and the payment reference describe how
//! *we* operate the account: which relay refused, and what it said. That is an
//! operational detail a partner has no use for and no business reading, and a
//! view that included it by default would leak it the first time someone added a
//! route that forgot to strip it. So the manager's constructor is the one that
//! fills those fields, and the partner's sets them to `None` — where they are
//! omitted from the JSON rather than serialised as `0`, because "we never tried"
//! and "we do not tell you how many times we tried" are different answers.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::{Authenticated, Scope, resolve_scope};
use crate::billing::pricing::MicroUsd;
use crate::billing::status::{ServiceStatus, SuspensionReason, status_for};
use crate::billing::store::{BillingStore, Statement, StatementLine};
use crate::dashboard::DashboardError;
use crate::proxy::handler::AppState;

/// A billing-store failure, as this surface reports it.
///
/// The `/api/*` surfaces answer with one error shape, so the SPA has one branch
/// and a client cannot learn from the shape of an error which table it came
/// from. A missing partner or statement is a 404 and not a 500: the request was
/// answerable, and the answer is that there is nothing there.
impl From<crate::billing::store::BillingError> for DashboardError {
    fn from(e: crate::billing::store::BillingError) -> Self {
        use crate::billing::store::BillingError;
        match e {
            BillingError::PartnerNotFound(id) => {
                DashboardError::NotFound(format!("no partner with consumer_id {id}"))
            }
            BillingError::PartnerExists(id) => {
                DashboardError::BadRequest(format!("existing partner {id} cannot be created"))
            }
            BillingError::StatementNotPayable { id, .. } => {
                DashboardError::BadRequest(format!("statement {id} carries no payment obligation"))
            }
            BillingError::Invalid(msg) => DashboardError::BadRequest(msg),
            BillingError::Database(e) => DashboardError::Database(e),
        }
    }
}

/// Default page size for a statement list.
const DEFAULT_LIMIT: i64 = 50;
/// Hard ceiling on page size, so one request cannot pull every statement ever.
const MAX_LIMIT: i64 = 200;

/// Partner-facing statement router. Mounted at the root by the composition
/// root, beside the admin and dashboard routers.
pub fn create_billing_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/billing/statements", get(list_statements))
        .route("/api/billing/statements/{id}", get(get_statement))
        .route("/api/billing/status", get(get_status))
}

/// Query parameters for a statement list.
///
/// `consumers` is here for the same reason it is on the dashboard: the SPA has
/// one query builder and two roles. It is inert for a partner key — the scope
/// resolver does not read it — and narrowing for a manager.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct StatementsQuery {
    /// Comma-separated consumer filter, **narrowing only**.
    pub consumers: String,
    /// Only statements that are still owed: invoices with no payment recorded.
    pub unpaid: bool,
    pub limit: i64,
    pub offset: i64,
}

impl Default for StatementsQuery {
    fn default() -> Self {
        Self {
            consumers: String::new(),
            unpaid: false,
            limit: DEFAULT_LIMIT,
            offset: 0,
        }
    }
}

impl StatementsQuery {
    /// The page bounds this query asks for, clamped to something answerable.
    ///
    /// A negative limit is zero rows rather than every row, and an offset below
    /// zero is no offset rather than a SQL error: both are a client's arithmetic
    /// going wrong, and neither is worth a 400 when the answer is unambiguous.
    pub fn page(&self) -> (i64, i64) {
        (self.limit.clamp(0, MAX_LIMIT), self.offset.max(0))
    }
}

/// One model's line of a statement, as the provider's usage was measured and as
/// it was priced at the time.
#[derive(Debug, Serialize)]
pub struct StatementLineView {
    pub model: String,
    /// The three prices this line was priced at, as decimal dollars per million.
    /// A snapshot from accept time, not today's configuration: a statement that
    /// repriced itself when an operator changed a price would be a bill that
    /// changed after it was sent.
    pub input_per_million: String,
    pub cached_input_per_million: String,
    pub output_per_million: String,
    pub request_count: i64,
    pub input_tokens: i64,
    pub cached_input_tokens: i64,
    /// `input - cached`, which is what the input price is charged on: the cached
    /// tokens are billed at the cached price and are not charged twice.
    pub uncached_input_tokens: i64,
    pub output_tokens: i64,
    pub input_cost_micro_usd: i64,
    pub cached_input_cost_micro_usd: i64,
    pub output_cost_micro_usd: i64,
    pub total_cost_micro_usd: i64,
    pub total_cost: String,
}

impl StatementLineView {
    pub(crate) fn from_row(line: &StatementLine) -> Self {
        Self {
            model: line.model.clone(),
            input_per_million: line.prices.input.to_decimal_string(),
            cached_input_per_million: line.prices.cached_input.to_decimal_string(),
            output_per_million: line.prices.output.to_decimal_string(),
            request_count: line.request_count,
            input_tokens: line.input_tokens,
            cached_input_tokens: line.cached_input_tokens,
            uncached_input_tokens: line.uncached_input_tokens,
            output_tokens: line.output_tokens,
            input_cost_micro_usd: line.input_cost_micro_usd,
            cached_input_cost_micro_usd: line.cached_input_cost_micro_usd,
            output_cost_micro_usd: line.output_cost_micro_usd,
            total_cost_micro_usd: line.total_cost_micro_usd,
            total_cost: MicroUsd::from_i64(line.total_cost_micro_usd).to_string(),
        }
    }
}

/// A statement, as a client reads it.
#[derive(Debug, Serialize)]
pub struct StatementView {
    pub id: i64,
    /// The partner this statement is for. Present on a partner's own view too:
    /// a statement that did not say whose it was would be unusable as a record.
    pub consumer_id: String,
    pub billing_date: String,
    /// `invoice` or `reconciliation`. The two are not interchangeable: one
    /// carries an obligation and a deadline, the other is a settlement record.
    pub billing_mode: String,
    pub currency: String,
    pub period_start: String,
    pub period_end: String,
    /// The instant this statement was decided to be final. Never `period_end`:
    /// a request accepted inside the day can finalize after it, and this is when
    /// the worker judged that no more of that day's rows were coming.
    pub billing_cutoff_at: String,
    pub total_amount_micro_usd: i64,
    pub total_amount: String,
    /// Requests in the period whose usage could not be priced — the provider did
    /// not report it in full, or no price was in force when they were accepted.
    /// They contribute no money to the statement, and they are never a zero:
    /// this count is what says so.
    pub incomplete_usage_count: i64,
    pub has_incomplete_usage: bool,
    pub due_at: Option<String>,
    pub paid_at: Option<String>,
    /// Whether this statement is still owed on. A reconciliation statement is
    /// never outstanding, whatever its amount.
    pub outstanding: bool,
    /// Whether this statement is a reason the partner's service is suspended.
    /// False for a zero statement, an incomplete one, and every reconciliation
    /// statement — the three cases in which there is no debt to enforce.
    ///
    /// Deliberately not a claim that the statement is *overdue*: overdue is
    /// decided against a clock by [`crate::billing::status::status_for`], and a
    /// second comparison here would be a second answer to "is this partner
    /// suspended" — one the dashboard could show while the request path was
    /// answering something else. That answer is on `/api/billing/status`, which
    /// derives it the same way the request path does.
    pub can_suspend: bool,
    /// Who recorded the payment, and against what. Manager-only.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub paid_by: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment_reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payment_note: Option<String>,
    /// Delivery bookkeeping. Manager-only, and `None` — not `0` — on a partner's
    /// view, because how many times our relay refused is not a fact about them.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_sent_at: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_attempts: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_last_error: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub email_next_retry_at: Option<String>,
    pub created_at: String,
    pub updated_at: String,
    /// The priced lines behind the total. Absent on a list, filled on a
    /// single-statement read: a page of fifty statements does not need fifty
    /// line-sets, and fetching them would be fifty queries.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub lines: Option<Vec<StatementLineView>>,
}

impl StatementView {
    /// The manager's view: every column the row holds.
    pub fn for_manager(statement: &Statement, lines: Option<Vec<StatementLineView>>) -> Self {
        Self {
            paid_by: statement.paid_by.clone(),
            payment_reference: statement.payment_reference.clone(),
            payment_note: statement.payment_note.clone(),
            email_sent_at: statement.email_sent_at.clone(),
            email_attempts: Some(statement.email_attempts),
            email_last_error: statement.email_last_error.clone(),
            email_next_retry_at: statement.email_next_retry_at.clone(),
            lines,
            ..Self::common(statement)
        }
    }

    /// The partner's view of their own statement: everything commercial, nothing
    /// operational. See the module docs for why the two differ.
    pub fn for_partner(statement: &Statement, lines: Option<Vec<StatementLineView>>) -> Self {
        Self {
            lines,
            ..Self::common(statement)
        }
    }

    /// The fields both views carry.
    fn common(statement: &Statement) -> Self {
        Self {
            id: statement.id,
            consumer_id: statement.consumer_id.clone(),
            billing_date: statement.billing_date.clone(),
            billing_mode: statement.billing_mode.clone(),
            currency: statement.currency.clone(),
            period_start: statement.period_start.clone(),
            period_end: statement.period_end.clone(),
            billing_cutoff_at: statement.billing_cutoff_at.clone(),
            total_amount_micro_usd: statement.total_amount_micro_usd,
            total_amount: statement.total().to_string(),
            incomplete_usage_count: statement.incomplete_usage_count,
            has_incomplete_usage: statement.has_incomplete_usage(),
            due_at: statement.due_at.clone(),
            paid_at: statement.paid_at.clone(),
            outstanding: statement.is_outstanding(),
            can_suspend: statement.can_suspend(),
            paid_by: None,
            payment_reference: None,
            payment_note: None,
            email_sent_at: None,
            email_attempts: None,
            email_last_error: None,
            email_next_retry_at: None,
            created_at: statement.created_at.clone(),
            updated_at: statement.updated_at.clone(),
            lines: None,
        }
    }
}

/// `GET /api/billing/statements` — this credential's statements, newest first.
async fn list_statements(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<StatementsQuery>,
) -> Result<Response, DashboardError> {
    let scope = resolve_scope(&consumer, &query.consumers);
    let (limit, offset) = query.page();
    let unpaid = query.unpaid;
    let store = BillingStore::new(state.pool.clone());

    let (rows, total, (unpaid_count, unpaid_total)) = tokio::task::spawn_blocking(move || {
        let rows = store.list_statements(&scope, limit, offset, unpaid)?;
        let total = store.count_statements(&scope)?;
        let totals = store.outstanding_totals(&scope)?;
        Ok::<_, crate::billing::store::BillingError>((rows, total, totals))
    })
    .await
    .map_err(|e| DashboardError::Internal(format!("statement list task failed: {e}")))??;

    // The manager's copy, which is the wider one; a partner key's scope yields
    // its own statements and the extra fields are the operator's, so a partner
    // gets `for_partner` for the same row it just read.
    let views: Vec<StatementView> = rows
        .iter()
        .map(|statement| {
            if consumer.is_manager() {
                StatementView::for_manager(statement, None)
            } else {
                StatementView::for_partner(statement, None)
            }
        })
        .collect();

    Ok(Json(StatementList {
        statements: views,
        total,
        limit,
        offset,
        unpaid_count,
        unpaid_total_micro_usd: unpaid_total.as_i64(),
        unpaid_total: unpaid_total.to_string(),
    })
    .into_response())
}

/// A page of statements, with the totals the scope holds.
#[derive(Debug, Serialize)]
pub struct StatementList {
    pub statements: Vec<StatementView>,
    /// How many statements this credential can see, so a pager knows when to
    /// stop. Counted under the same scope as the page, because a total wider
    /// than the page is how a paginator starts offering empty pages.
    pub total: i64,
    pub limit: i64,
    pub offset: i64,
    /// Unpaid invoices in scope, and what they total.
    ///
    /// Aggregated over the whole scope and not over this page: a partner reading
    /// "what do I owe" must get the same answer whether they are looking at page
    /// one or page five, and a reconciliation statement is never in it — there is
    /// nothing owed on a settlement record.
    pub unpaid_count: i64,
    pub unpaid_total_micro_usd: i64,
    pub unpaid_total: String,
}

/// `GET /api/billing/statements/{id}` — one statement, with its lines.
///
/// A statement outside this credential's scope is a 404, from SQL, with no way
/// to tell it apart from an id that does not exist.
async fn get_statement(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Path(id): Path<i64>,
    Query(query): Query<StatementsQuery>,
) -> Result<Response, DashboardError> {
    let scope = resolve_scope(&consumer, &query.consumers);
    let store = BillingStore::new(state.pool.clone());
    let manager = consumer.is_manager();

    let found = tokio::task::spawn_blocking(move || {
        let Some(statement) = store.get_statement_by_id(id, &scope)? else {
            return Ok::<_, crate::billing::store::BillingError>(None);
        };
        let lines: Vec<StatementLineView> = store
            .statement_lines(id)?
            .iter()
            .map(StatementLineView::from_row)
            .collect();
        let view = if manager {
            StatementView::for_manager(&statement, Some(lines))
        } else {
            StatementView::for_partner(&statement, Some(lines))
        };
        Ok(Some(view))
    })
    .await
    .map_err(|e| DashboardError::Internal(format!("statement read task failed: {e}")))??;

    found.map_or_else(
        // No id in the message. The sentence is identical for "not yours" and
        // "no such row", which is what makes the two indistinguishable; echoing
        // the id back would hand a partner an oracle — ids are rowids, so
        // iterating 1, 2, 3, … against someone else's statement is enough to
        // learn the size of the table and the day each partner billed. A 404
        // that says which ids exist is the leak the scope is there to prevent.
        || {
            Err(DashboardError::NotFound(
                "no statement with that id".to_string(),
            ))
        },
        |view| Ok(Json(view).into_response()),
    )
}

/// The scope a statement read may use, for a caller that already has a context.
///
/// Exposed rather than duplicated so a future surface scopes a statement read
/// the one way there is.
pub fn statement_scope(consumer: &crate::auth::ConsumerContext, consumers: &str) -> Scope {
    resolve_scope(consumer, consumers)
}

// ---------------------------------------------------------------------------
// Service status
// ---------------------------------------------------------------------------

/// Why a partner's service is suspended, in the terms a human needs.
#[derive(Debug, Serialize)]
pub struct SuspensionReasonView {
    /// The kind of reason. One value today, and named as data rather than
    /// inferred from the presence of the fields, so a second kind is an addition
    /// rather than a reinterpretation.
    pub code: &'static str,
    pub statement_id: i64,
    pub billing_date: String,
    pub due_at: String,
    pub amount_micro_usd: i64,
    pub amount: String,
}

/// A partner's derived service status.
#[derive(Debug, Serialize)]
pub struct ServiceStatusView {
    pub consumer_id: String,
    /// `active` or `suspended`.
    pub status: &'static str,
    pub suspended: bool,
    /// The message a suspended request is refused with, verbatim, so the
    /// dashboard and the 403 body cannot say different things.
    pub message: Option<&'static str>,
    pub reason: Option<SuspensionReasonView>,
    /// How many of this partner's statements are past their deadline and unpaid.
    /// A count of *statements*, not of the money: an incomplete statement is in
    /// it and does not suspend anyone, which is why the two numbers are separate
    /// fields rather than one.
    pub overdue_statements: i64,
    /// What those statements total. Never the money that could suspend:
    /// see `overdue_statements`.
    pub overdue_amount_micro_usd: i64,
    pub overdue_amount: String,
}

/// Read the derived status of every consumer in `scope`.
///
/// The composition is deliberate: read the facts (the partners, their statements
/// past their deadline) and then hand them to
/// [`status_for`](crate::billing::status::status_for), which is the one place the
/// decision is made. Re-deciding it here with a second comparison is exactly the
/// failure the summary above warns about, so nothing in this function looks at a
/// clock except to pass the instant to `status_for`.
///
/// A partner key scopes this to one `get_partner` read, so the narrow case never
/// reads another consumer's row even transiently. A manager's view is every
/// partner — which the manager may see anyway — narrowed in memory when it named
/// consumers explicitly.
pub(crate) fn statuses_in_scope(
    store: &BillingStore,
    scope: &Scope,
    now: time::OffsetDateTime,
) -> Result<Vec<ServiceStatusView>, crate::billing::store::BillingError> {
    let partners = match scope {
        Scope::One(id) => store.get_partner(id)?.into_iter().collect(),
        Scope::List(ids) => store
            .list_partners()?
            .into_iter()
            .filter(|p| ids.iter().any(|id| id == &p.consumer_id))
            .collect(),
        Scope::All => store.list_partners()?,
    };

    // One read for every partner's overdue rows, grouped here rather than queried
    // per partner: the number of partners is small and grows slowly, and a query
    // per partner is a query per partner.
    let mut overdue: std::collections::HashMap<String, Vec<crate::billing::status::OverdueRow>> =
        std::collections::HashMap::new();
    for statement in store.overdue_statements(None)? {
        overdue
            .entry(statement.consumer_id.clone())
            .or_default()
            .push(statement.to_overdue_row());
    }

    let mut out = Vec::with_capacity(partners.len());
    for partner in partners {
        let rows = overdue.remove(&partner.consumer_id).unwrap_or_default();
        let status = status_for(partner.billing_mode.as_str(), now, &rows);
        out.push(view_status(&partner.consumer_id, &status, &rows));
    }
    Ok(out)
}

/// Render a derived status and the rows behind it.
fn view_status(
    consumer_id: &str,
    status: &ServiceStatus,
    rows: &[crate::billing::status::OverdueRow],
) -> ServiceStatusView {
    // One suspension reason exists, so this is a mapping rather than a choice:
    // the day a second one is added, the compiler points here, and the arm has to
    // say how it renders.
    let reason = status.suspension_reason().map(|reason| {
        let SuspensionReason::InvoiceOverdue(overdue) = reason;
        SuspensionReasonView {
            code: "invoice_overdue",
            statement_id: overdue.id,
            billing_date: overdue.billing_date.clone(),
            due_at: crate::ledger::timefmt::format_ts(overdue.due_at),
            amount_micro_usd: overdue.amount.as_i64(),
            amount: overdue.amount.to_string(),
        }
    });

    let overdue_amount = rows.iter().fold(MicroUsd::ZERO, |sum, row| {
        sum.checked_add(MicroUsd::from_i64(row.total_amount_micro_usd))
            .unwrap_or(sum)
    });

    ServiceStatusView {
        consumer_id: consumer_id.to_string(),
        status: if status.is_suspended() {
            "suspended"
        } else {
            "active"
        },
        suspended: status.is_suspended(),
        message: status.suspension_message(),
        reason,
        overdue_statements: rows.len() as i64,
        overdue_amount_micro_usd: overdue_amount.as_i64(),
        overdue_amount: overdue_amount.to_string(),
    }
}

#[derive(Debug, Serialize)]
pub struct ServiceStatusList {
    pub statuses: Vec<ServiceStatusView>,
}

/// `GET /api/billing/status` — the derived service status, and why.
///
/// This is the answer to "why are my requests being refused?" and it is *derived
/// at read time* from the same rows the request path uses, not stored: a stored
/// status is a second copy of the truth that a manager marking a bill paid would
/// have to remember to update, and the failure mode of forgetting is a partner
/// who stays offline after paying.
///
/// One entry per consumer in scope: exactly one for a partner key, every partner
/// for a manager.
async fn get_status(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<StatementsQuery>,
) -> Result<Response, DashboardError> {
    let scope = resolve_scope(&consumer, &query.consumers);
    let now = crate::ledger::timefmt::now();
    let store = BillingStore::new(state.pool.clone());

    let statuses = tokio::task::spawn_blocking(move || statuses_in_scope(&store, &scope, now))
        .await
        .map_err(|e| DashboardError::Internal(format!("status task failed: {e}")))??;

    Ok(Json(ServiceStatusList { statuses }).into_response())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::billing::partner::BillingMode;
    use crate::billing::pricing::{PricePerMillion, PricingSnapshot};
    use crate::billing::store::Statement;

    fn statement(mode: &str, total: i64, incomplete: i64, due: Option<&str>) -> Statement {
        Statement {
            id: 7,
            consumer_id: "acme".into(),
            billing_date: "2026-09-27".into(),
            billing_mode: mode.into(),
            currency: "USD".into(),
            period_start: "2026-09-27T00:00:00.000000000Z".into(),
            period_end: "2026-09-28T00:00:00.000000000Z".into(),
            billing_cutoff_at: "2026-09-28T00:05:00.000000000Z".into(),
            total_amount_micro_usd: total,
            incomplete_usage_count: incomplete,
            due_at: due.map(str::to_string),
            paid_at: None,
            paid_by: Some("operator@ecoma".into()),
            payment_reference: Some("bank-ref-1".into()),
            payment_note: Some("settled by transfer".into()),
            email_sent_at: Some("2026-09-28T00:06:00.000000000Z".into()),
            email_attempts: 2,
            email_last_error: Some("relay refused: 550 mailbox unavailable".into()),
            email_next_retry_at: Some("2026-09-28T00:21:00.000000000Z".into()),
            created_at: "2026-09-28T00:05:00.000000000Z".into(),
            updated_at: "2026-09-28T00:06:00.000000000Z".into(),
        }
    }

    /// The partner's view is the one that must not carry our operational detail,
    /// so it is asserted on the serialised form: a field that is safe today can
    /// be added to the struct tomorrow and leak without any test noticing, unless
    /// the test reads the JSON.
    #[test]
    fn test_a_partners_view_of_their_statement_carries_no_operational_detail() {
        let row = statement(
            "invoice",
            142_500,
            0,
            Some("2026-09-28T12:00:00.000000000Z"),
        );
        let json = serde_json::to_string(&StatementView::for_partner(&row, None)).unwrap();

        for leaked in [
            "bank-ref-1",
            "settled by transfer",
            "operator@ecoma",
            "550 mailbox unavailable",
            "email_attempts",
            "email_sent_at",
            "email_next_retry_at",
            "email_last_error",
            "payment_reference",
            "payment_note",
            "paid_by",
        ] {
            assert!(!json.contains(leaked), "{leaked} reached a partner: {json}");
        }
        // And everything commercial did reach them.
        for shown in ["142500", "$0.142500", "2026-09-27", "invoice", "acme"] {
            assert!(json.contains(shown), "{shown} is missing: {json}");
        }
    }

    /// The manager's view is the whole row, because an operator investigating a
    /// partner's complaint needs exactly the fields the partner cannot see.
    #[test]
    fn test_a_managers_view_carries_the_operational_detail() {
        let row = statement(
            "invoice",
            142_500,
            0,
            Some("2026-09-28T12:00:00.000000000Z"),
        );
        let json = serde_json::to_string(&StatementView::for_manager(&row, None)).unwrap();

        for shown in [
            "bank-ref-1",
            "550 mailbox unavailable",
            "email_attempts",
            "email_attempts\":2",
        ] {
            assert!(json.contains(shown), "{shown} is missing: {json}");
        }
    }

    /// The three facts that decide whether a statement is a reason to suspend
    /// someone, each read straight off the row.
    #[test]
    fn test_the_derived_flags_are_read_from_the_row_not_guessed() {
        let due = Some("2026-09-28T12:00:00.000000000Z");

        // A reconciliation statement is never outstanding and never suspends,
        // whatever it totals.
        let reconciliation =
            StatementView::for_manager(&statement("reconciliation", 142_500, 0, None), None);
        assert!(!reconciliation.outstanding);
        assert!(!reconciliation.can_suspend);

        // A zero invoice has nothing to enforce.
        let zero = StatementView::for_manager(&statement("invoice", 0, 0, due), None);
        assert!(zero.outstanding);
        assert!(!zero.can_suspend, "no debt is a reason to cut nobody off");

        // An invoice with usage nobody could measure does not suspend either:
        // the amount is a floor, not the whole bill.
        let incomplete = StatementView::for_manager(&statement("invoice", 142_500, 3, due), None);
        assert!(incomplete.outstanding);
        assert!(!incomplete.can_suspend);
        assert!(incomplete.has_incomplete_usage);

        // A complete, owed invoice with a deadline does.
        let payable = StatementView::for_manager(&statement("invoice", 142_500, 0, due), None);
        assert!(payable.outstanding);
        assert!(payable.can_suspend);
    }

    /// A partner's view still says whether the statement is a reason they were
    /// suspended. That is not our operational detail — it is why their requests
    /// are being refused, and they are entitled to it.
    #[test]
    fn test_a_partner_is_told_their_service_is_suspended_by_this_statement() {
        let row = statement(
            "invoice",
            142_500,
            0,
            Some("2026-09-28T12:00:00.000000000Z"),
        );
        let view = StatementView::for_partner(&row, None);
        assert!(view.can_suspend);
        assert!(view.outstanding);
        let json = serde_json::to_string(&view).unwrap();
        assert!(json.contains("\"can_suspend\":true"), "{json}");
    }

    #[test]
    fn test_a_page_is_clamped_to_something_answerable() {
        let mut q = StatementsQuery::default();
        assert_eq!(q.page(), (DEFAULT_LIMIT, 0));

        q.limit = -1;
        q.offset = -5;
        assert_eq!(q.page(), (0, 0), "a negative page is empty, not everything");

        q.limit = 10_000;
        assert_eq!(q.page().0, MAX_LIMIT, "a page has a ceiling");

        q.limit = 0;
        assert_eq!(q.page().0, 0, "zero means zero rows, not the default");
    }

    /// A line's rendered prices are the snapshot's, and the uncached count is
    /// what the input price was charged on.
    #[test]
    fn test_a_line_view_reports_the_prices_that_priced_it() {
        let line = StatementLine {
            id: 1,
            statement_id: 7,
            model: "gpt-4o".into(),
            prices: PricingSnapshot::new(
                PricePerMillion::new(95_000),
                PricePerMillion::new(47_500),
                PricePerMillion::new(475_000),
            ),
            request_count: 4,
            input_tokens: 1_000,
            cached_input_tokens: 400,
            uncached_input_tokens: 600,
            output_tokens: 200,
            input_cost_micro_usd: 57,
            cached_input_cost_micro_usd: 19,
            output_cost_micro_usd: 95,
            total_cost_micro_usd: 171,
        };
        let view = StatementLineView::from_row(&line);
        assert_eq!(view.input_per_million, "0.095");
        assert_eq!(view.cached_input_per_million, "0.0475");
        assert_eq!(view.output_per_million, "0.475");
        assert_eq!(view.uncached_input_tokens, 600);
        assert_eq!(view.total_cost, "$0.000171");
    }

    /// A mode is reported as it was at the time, not as the partner is
    /// configured today: a statement is immutable once issued.
    #[test]
    fn test_a_mode_comes_from_the_row_not_from_configuration() {
        let row = statement(BillingMode::Reconciliation.as_str(), 0, 0, None);
        let view = StatementView::for_manager(&row, None);
        assert_eq!(view.billing_mode, "reconciliation");
        assert_eq!(
            row.billing_mode_value(),
            Some(BillingMode::Reconciliation),
            "the stored value round-trips to the type"
        );
    }
}
