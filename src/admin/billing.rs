//! Admin API for statements and payment.
//!
//! # What an operator does here
//!
//! Reads the statements the daily scheduler has issued — every partner's, or one
//! partner's — and records a payment against one of them. That is the whole
//! payment process: there is no charge, no provider, no reconciliation file, and
//! no automated settlement. A human transfers money, and a human says so here.
//!
//! # Why marking paid is a transaction and not an `UPDATE`
//!
//! A payment changes two things that have to agree: the statement's payment
//! columns, and, as a consequence, whether the partner's service is suspended.
//! The second is not stored — it is derived at read time from the same rows
//! ([`crate::billing::status`]) — so the write is a single guarded `UPDATE` and
//! the resumption follows from the next snapshot refresh rather than from this
//! function doing anything. There is no half-state to get wrong: either the
//! payment is recorded and the partner resumes, or the write did not land and
//! they do not.
//!
//! # Idempotence
//!
//! Marking an already-paid statement paid again changes nothing and reports the
//! statement as it stands. The guard is in the `WHERE` (`paid_at IS NULL`), so a
//! retried request cannot overwrite the first payment's reference with the second
//! request's — the audit trail keeps the one that was first recorded, which is
//! the one a human remembers doing.

use axum::{
    Json, Router,
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::auth::{Scope, resolve_scope};
use crate::billing::api::{StatementLineView, StatementList, StatementView, StatementsQuery};
use crate::billing::store::{BillingError, BillingStore};
use crate::proxy::handler::AppState;

use super::{AdminError, ManagerOnly, no_store};

/// Admin billing router. Mounted at the root by [`super::create_admin_router`].
///
/// Every route takes [`ManagerOnly`]: a partner's own statements are on
/// `/api/billing/*`, scoped to their credential, and this surface is the
/// organisation-wide view that only the manager password reaches.
pub fn create_billing_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/admin/billing/statements", get(list_statements))
        .route("/api/admin/billing/statements/{id}", get(get_statement))
        .route(
            "/api/admin/billing/statements/{id}/mark-paid",
            post(mark_paid),
        )
        .route("/api/admin/billing/summary", get(get_summary))
}

/// `GET /api/admin/billing/statements` — every statement in scope, newest first.
///
/// `consumers` narrows a manager's view and cannot widen it (ADR 0013);
/// `unpaid=true` shows what is still owed, ordered by deadline rather than by
/// date, because the next thing that happens to an unpaid invoice is that it
/// suspends someone.
async fn list_statements(
    State(state): State<Arc<AppState>>,
    ManagerOnly(manager): ManagerOnly,
    Query(query): Query<StatementsQuery>,
) -> Result<Response, AdminError> {
    let scope = resolve_scope(&manager, &query.consumers);
    let (limit, offset) = query.page();
    let unpaid = query.unpaid;
    let store = BillingStore::new(state.pool.clone());

    let (rows, total, (unpaid_count, unpaid_total)) = tokio::task::spawn_blocking(move || {
        let rows = store.list_statements(&scope, limit, offset, unpaid)?;
        let total = store.count_statements(&scope)?;
        let totals = store.outstanding_totals(&scope)?;
        Ok::<_, BillingError>((rows, total, totals))
    })
    .await
    .map_err(AdminError::Join)??;

    let statements: Vec<StatementView> = rows
        .iter()
        .map(|statement| StatementView::for_manager(statement, None))
        .collect();

    Ok(no_store(
        StatusCode::OK,
        Json(StatementList {
            statements,
            total,
            limit,
            offset,
            unpaid_count,
            unpaid_total_micro_usd: unpaid_total.as_i64(),
            unpaid_total: unpaid_total.to_string(),
        }),
    )
    .into_response())
}

/// `GET /api/admin/billing/statements/{id}` — one statement, with its lines.
async fn get_statement(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(id): Path<i64>,
) -> Result<Response, AdminError> {
    let store = BillingStore::new(state.pool.clone());
    let view = tokio::task::spawn_blocking(move || {
        let Some(statement) = store.get_statement_by_id(id, &Scope::All)? else {
            return Ok::<_, BillingError>(None);
        };
        let lines: Vec<StatementLineView> = store
            .statement_lines(id, &Scope::All)?
            .iter()
            .map(StatementLineView::from_row)
            .collect();
        Ok(Some(StatementView::for_manager(&statement, Some(lines))))
    })
    .await
    .map_err(AdminError::Join)??;

    match view {
        Some(view) => Ok(no_store(StatusCode::OK, Json(view)).into_response()),
        None => Err(AdminError::StatementNotFound(id)),
    }
}

#[derive(Debug, Deserialize)]
pub struct MarkPaidRequest {
    /// Who recorded the payment. Defaults to `"manager"`, which is the truth
    /// when the shared manager password is what made the call: there is no
    /// per-operator identity to derive it from, and inventing one would be a
    /// worse audit trail than an honest generic.
    #[serde(default)]
    pub paid_by: Option<String>,
    /// The bank reference, transfer id or cheque number. Free text: this product
    /// does not know what the money moved through, and a field it need not
    /// interpret is a field it must not constrain.
    #[serde(default)]
    pub reference: Option<String>,
    #[serde(default)]
    pub note: Option<String>,
}

/// `POST /api/admin/billing/statements/{id}/mark-paid` — record a payment.
///
/// Refuses a reconciliation statement with 409: it carries no payment
/// obligation, and recording one against it would make the ledger say money
/// changed hands for a bill that was never owed.
async fn mark_paid(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(id): Path<i64>,
    Json(req): Json<MarkPaidRequest>,
) -> Result<Response, AdminError> {
    let paid_by = req
        .paid_by
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .unwrap_or("manager")
        .to_string();
    let reference = non_empty(req.reference);
    let note = non_empty(req.note);

    let store = BillingStore::new(state.pool.clone());
    let recorder = paid_by.clone();
    let paid = tokio::task::spawn_blocking(move || {
        // Read the row first, so a missing id and a reconciliation statement are
        // told apart from a database failure by their own 404 and 409 rather
        // than by matching on an error message.
        let Some(existing) = store.get_statement_by_id(id, &Scope::All)? else {
            return Ok::<_, BillingError>(None);
        };
        if !existing
            .billing_mode_value()
            .is_some_and(crate::billing::partner::BillingMode::owes_payment)
        {
            return Err(BillingError::StatementNotPayable {
                id,
                billing_mode: existing.billing_mode.clone(),
            });
        }
        // Already paid: report it as it stands, change nothing. The second
        // request is answered, not refused — the caller's intent is already
        // satisfied, and a 409 here would make an operator wonder whether the
        // first one worked.
        if existing.is_paid() {
            return Ok(Some((StatementView::for_manager(&existing, None), false)));
        }

        let statement = store.mark_paid(id, &recorder, reference.as_deref(), note.as_deref())?;
        Ok(Some((StatementView::for_manager(&statement, None), true)))
    })
    .await
    .map_err(AdminError::Join)??;

    let Some((view, recorded)) = paid else {
        return Err(AdminError::StatementNotFound(id));
    };

    if recorded {
        // The event the operator's actions are audited by: who, which statement,
        // how much, and what the partner referred to it as.
        tracing::info!(
            statement_id = view.id,
            consumer_id = %view.consumer_id,
            billing_date = %view.billing_date,
            amount_micro_usd = view.total_amount_micro_usd,
            paid_by = %paid_by,
            has_reference = view.payment_reference.is_some(),
            "payment_marked_paid"
        );
    } else {
        tracing::info!(
            statement_id = view.id,
            consumer_id = %view.consumer_id,
            "payment_already_recorded"
        );
    }

    Ok(no_store(StatusCode::OK, Json(view)).into_response())
}

/// `GET /api/admin/billing/summary` — what is owed across the scope.
///
/// Read from the statements that exist, never recomputed from usage: retention
/// prunes usage, and a lifetime total that shrank when a sweep ran would be worse
/// than no total at all.
async fn get_summary(
    State(state): State<Arc<AppState>>,
    ManagerOnly(manager): ManagerOnly,
    Query(query): Query<StatementsQuery>,
) -> Result<Response, AdminError> {
    let scope = resolve_scope(&manager, &query.consumers);
    let store = BillingStore::new(state.pool.clone());
    let now = crate::ledger::timefmt::now();

    let (statements, (unpaid_count, unpaid_total), statuses) =
        tokio::task::spawn_blocking(move || {
            let statements = store.count_statements(&scope)?;
            let totals = store.outstanding_totals(&scope)?;
            // The suspended count comes from the same derivation the request path
            // uses, not from a count of overdue rows: an invoice that is incomplete
            // or zero is overdue by date and suspends nobody.
            let statuses = crate::billing::api::statuses_in_scope(&store, &scope, now)?;
            Ok::<_, BillingError>((statements, totals, statuses))
        })
        .await
        .map_err(AdminError::Join)??;

    let suspended = statuses.iter().filter(|s| s.suspended).count() as i64;
    let partners = statuses.len() as i64;

    Ok(no_store(
        StatusCode::OK,
        Json(BillingSummary {
            partners,
            suspended_partners: suspended,
            statements,
            unpaid_statements: unpaid_count,
            unpaid_total_micro_usd: unpaid_total.as_i64(),
            unpaid_total: unpaid_total.to_string(),
        }),
    )
    .into_response())
}

#[derive(Debug, Serialize)]
pub struct BillingSummary {
    pub partners: i64,
    /// Partners whose *derived* status is suspended right now. Derived, because
    /// suspension is: a number stored here would be one a manager's payment had
    /// to remember to decrement, and a stale count of suspended partners is a
    /// count someone acts on.
    pub suspended_partners: i64,
    pub statements: i64,
    pub unpaid_statements: i64,
    pub unpaid_total_micro_usd: i64,
    pub unpaid_total: String,
}

/// Trim a free-text field, treating whitespace as absent.
///
/// An empty string is not a reference: storing one would put a payment reference
/// in the audit trail that says nothing, and a reader could not tell it from one
/// that was never supplied.
fn non_empty(raw: Option<String>) -> Option<String> {
    raw.map(|s| s.trim().to_string()).filter(|s| !s.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_blank_field_is_absent_rather_than_empty() {
        assert_eq!(non_empty(None), None);
        assert_eq!(non_empty(Some(String::new())), None);
        assert_eq!(non_empty(Some("   ".to_string())), None);
        assert_eq!(
            non_empty(Some("  bank-ref-1 ".to_string())),
            Some("bank-ref-1".to_string())
        );
    }
}
