//! Admin API for the commercial side of a partner.
//!
//! # What a partner is here
//!
//! The commercial unit: a name, an address to send statements to, a billing
//! mode, payment terms, and the list of models they may call with the prices
//! they pay for them. `consumer_id` remains the identity boundary — this surface
//! does not introduce a second one, and every row it writes is keyed by the same
//! `consumer_id` the ledger, the API keys and the dashboard use.
//!
//! # Why the model list is a `PUT` and not a `PATCH`
//!
//! `PUT /api/admin/partners/{consumer_id}/models` replaces the whole list,
//! atomically, in one transaction. A merge cannot express *removing* a model —
//! the operation an operator most needs when they find a partner calling
//! something they should not. The request is "this is what this partner may
//! call", and replacing is the only operation that says it.
//!
//! This list is the **single** source of truth for both access and price. The
//! request path resolves one lookup per model, so there is no state in which a
//! model is allowed but unpriced, or priced but not allowed (ADR 0015).
//!
//! # Prices
//!
//! Every price is a decimal string in US dollars per million tokens —
//! `"0.095"` for $0.095/M — converted to integer micro-USD at the edge, here.
//! The wire format is a string and not a float: `0.1` has no exact binary
//! representation, and a price that arrives one ulp low is a partner's bill that
//! is wrong in the last digit forever. See [`parse_price`].
//!
//! # Blocking work
//!
//! Every handler opens a SQLite connection on the same writer the metering
//! pipeline uses, so every one runs on `spawn_blocking` and none of them runs on
//! the request path.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, put},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::billing::partner::{BillingMode, ModelPrice, Partner};
use crate::billing::pricing::{PricePerMillion, PricingSnapshot};
use crate::billing::store::{BillingStore, NewPartner, PartnerPatch};
use crate::proxy::handler::AppState;

use super::{AdminError, ManagerOnly, no_store};

/// Admin partner router. Mounted at the root by [`super::create_admin_router`].
pub fn create_partner_router() -> Router<Arc<AppState>> {
    Router::new()
        .route(
            "/api/admin/partners",
            get(list_partners).post(create_partner),
        )
        .route(
            "/api/admin/partners/{consumer_id}",
            get(get_partner)
                .patch(update_partner)
                .delete(delete_partner),
        )
        .route(
            "/api/admin/partners/{consumer_id}/models",
            put(replace_models).get(get_models),
        )
}

/// A partner as an operator sees them: the commercial facts, and what they pay.
#[derive(Debug, Serialize)]
pub struct PartnerView {
    pub consumer_id: String,
    pub name: String,
    pub billing_email: String,
    pub billing_mode: &'static str,
    pub payment_terms_minutes: i64,
    /// Whether an invoice statement for this partner is emailed to a human.
    /// Derived, and reported because "we never send anything to this partner" is
    /// the fact an operator is looking for when a partner says they heard
    /// nothing.
    pub emails_statements: bool,
    pub created_at: String,
    pub updated_at: String,
    /// Rolled up from the statements that exist, never recomputed from usage:
    /// retention prunes usage, and a lifetime total that shrank when a sweep ran
    /// would be worse than no total at all.
    pub total_billed_micro_usd: i64,
    pub total_billed: String,
    /// How many statements this partner has, for a pager.
    pub statement_count: i64,
    pub models: Vec<ModelPriceView>,
}

/// One model's prices, as configured: both the integer form the database holds
/// and the decimal string an operator typed, so a round trip through this API
/// shows what was entered rather than what it was converted to.
#[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq)]
pub struct ModelPriceView {
    pub model: String,
    /// US dollars per million tokens, decimal string.
    pub input_per_million: String,
    pub cached_input_per_million: String,
    pub output_per_million: String,
}

impl ModelPriceView {
    fn from_row(row: &ModelPrice) -> Self {
        let (input, cached, output) = row.prices.as_tuple();
        Self {
            model: row.model.clone(),
            input_per_million: PricePerMillion::new(input).to_decimal_string(),
            cached_input_per_million: PricePerMillion::new(cached).to_decimal_string(),
            output_per_million: PricePerMillion::new(output).to_decimal_string(),
        }
    }
}

impl PartnerView {
    fn build(
        partner: &Partner,
        total_billed: i64,
        statement_count: i64,
        models: Vec<ModelPrice>,
    ) -> Self {
        Self {
            consumer_id: partner.consumer_id.clone(),
            name: partner.name.clone(),
            billing_email: partner.billing_email.clone(),
            billing_mode: partner.billing_mode.as_str(),
            payment_terms_minutes: partner.payment_terms_minutes,
            emails_statements: partner.billing_mode.owes_payment()
                && !partner.billing_email.trim().is_empty(),
            created_at: partner.created_at.clone(),
            updated_at: partner.updated_at.clone(),
            total_billed_micro_usd: total_billed,
            total_billed: crate::billing::pricing::MicroUsd::from_i64(total_billed).to_string(),
            statement_count,
            models: models.iter().map(ModelPriceView::from_row).collect(),
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct CreatePartnerRequest {
    pub consumer_id: String,
    pub name: String,
    /// Absent or empty means "no address on file": statements are still written
    /// and simply not sent, which is a legitimate way to run a partner.
    #[serde(default)]
    pub billing_email: String,
    /// `invoice` or `reconciliation`.
    pub billing_mode: String,
    /// Defaults to the product default (see `DEFAULT_PAYMENT_TERMS_MINUTES`)
    /// rather than to zero, because zero terms mean an invoice is overdue the
    /// instant it is issued.
    #[serde(default)]
    pub payment_terms_minutes: Option<i64>,
    /// The partner's models and prices, if they are known at creation. Both can
    /// be set later with `PUT .../models`; a partner with none configured can
    /// call nothing, which is the only safe reading of "not configured".
    #[serde(default)]
    pub models: Vec<ModelPriceView>,
}

#[derive(Debug, Deserialize)]
pub struct UpdatePartnerRequest {
    pub name: Option<String>,
    pub billing_email: Option<String>,
    pub billing_mode: Option<String>,
    pub payment_terms_minutes: Option<i64>,
}

/// `POST /api/admin/partners` — open an account.
///
/// Fails if the partner already exists. A create that silently reset an existing
/// partner's billing mode or payment terms is how a contract changes by accident.
async fn create_partner(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Json(req): Json<CreatePartnerRequest>,
) -> Result<Response, AdminError> {
    let mode = parse_mode(&req.billing_mode)?;
    let models = parse_models(&req.models)?;
    let new = NewPartner {
        consumer_id: req.consumer_id.clone(),
        name: req.name.clone(),
        billing_email: req.billing_email.clone(),
        billing_mode: mode,
        payment_terms_minutes: req
            .payment_terms_minutes
            .unwrap_or(crate::billing::DEFAULT_PAYMENT_TERMS_MINUTES),
    };

    let store = BillingStore::new(state.pool.clone());
    let (partner, billed, count) = tokio::task::spawn_blocking(move || {
        let partner = store.create_partner(new)?;
        if !models.is_empty() {
            store.replace_models(&partner.consumer_id, &models)?;
        }
        let billed = store.total_billed(&partner.consumer_id)?.as_i64();
        let count =
            store.count_statements(&crate::auth::Scope::One(partner.consumer_id.clone()))?;
        Ok::<_, crate::billing::store::BillingError>((partner, billed, count))
    })
    .await
    .map_err(AdminError::Join)??;

    // The partner and its prices are source data for the credential snapshot.
    // A key issued after this write normally refreshes it too, but an already
    // issued key must see a partner created around it without waiting for the
    // periodic refresher.
    refresh_snapshot(&state).await;
    let models = store_models(&state, &partner.consumer_id).await?;

    tracing::info!(
        consumer = %partner.consumer_id,
        mode = partner.billing_mode.as_str(),
        models = models.len(),
        "partner created"
    );

    Ok(no_store(
        StatusCode::CREATED,
        Json(PartnerView::build(&partner, billed, count, models)),
    )
    .into_response())
}

/// `GET /api/admin/partners` — every partner, in `consumer_id` order.
async fn list_partners(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
) -> Result<Response, AdminError> {
    let store = BillingStore::new(state.pool.clone());
    let rows = tokio::task::spawn_blocking(move || {
        let mut out = Vec::new();
        for partner in store.list_partners()? {
            let billed = store.total_billed(&partner.consumer_id)?.as_i64();
            let count =
                store.count_statements(&crate::auth::Scope::One(partner.consumer_id.clone()))?;
            let models = store.models(&partner.consumer_id)?;
            out.push(PartnerView::build(&partner, billed, count, models));
        }
        Ok::<_, crate::billing::store::BillingError>(out)
    })
    .await
    .map_err(AdminError::Join)??;

    Ok(no_store(StatusCode::OK, Json(rows)).into_response())
}

/// `GET /api/admin/partners/{consumer_id}` — one partner.
async fn get_partner(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(consumer_id): Path<String>,
) -> Result<Response, AdminError> {
    let store = BillingStore::new(state.pool.clone());
    let id = consumer_id.clone();
    let view = tokio::task::spawn_blocking(move || {
        let partner = store
            .get_partner(&id)?
            .ok_or_else(|| crate::billing::store::BillingError::PartnerNotFound(id.clone()))?;
        let billed = store.total_billed(&id)?.as_i64();
        let count = store.count_statements(&crate::auth::Scope::One(id.clone()))?;
        let models = store.models(&id)?;
        Ok::<_, crate::billing::store::BillingError>(PartnerView::build(
            &partner, billed, count, models,
        ))
    })
    .await
    .map_err(AdminError::Join)??;

    Ok(no_store(StatusCode::OK, Json(view)).into_response())
}

/// `PATCH /api/admin/partners/{consumer_id}` — change the commercial facts.
///
/// Absent fields are left as they are: `None` means "unchanged", which is what
/// makes a patch with one field in it safe. Changing the billing mode changes
/// whether the partner owes money at all, so it is logged explicitly.
async fn update_partner(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(consumer_id): Path<String>,
    Json(req): Json<UpdatePartnerRequest>,
) -> Result<Response, AdminError> {
    let patch = PartnerPatch {
        name: req.name,
        billing_email: req.billing_email,
        billing_mode: req.billing_mode.as_deref().map(parse_mode).transpose()?,
        payment_terms_minutes: req.payment_terms_minutes,
    };
    if let Some(terms) = patch.payment_terms_minutes {
        if terms < 0 {
            return Err(AdminError::BadRequest(
                "payment_terms_minutes must not be negative".to_string(),
            ));
        }
    }

    let store = BillingStore::new(state.pool.clone());
    let id = consumer_id.clone();
    let (partner, billed, count, models) = tokio::task::spawn_blocking(move || {
        let partner = store.update_partner(&id, patch)?;
        let billed = store.total_billed(&id)?.as_i64();
        let count = store.count_statements(&crate::auth::Scope::One(id.clone()))?;
        let models = store.models(&id)?;
        Ok::<_, crate::billing::store::BillingError>((partner, billed, count, models))
    })
    .await
    .map_err(AdminError::Join)??;

    refresh_snapshot(&state).await;
    tracing::info!(
        consumer = %partner.consumer_id,
        mode = partner.billing_mode.as_str(),
        "partner updated"
    );

    Ok(no_store(
        StatusCode::OK,
        Json(PartnerView::build(&partner, billed, count, models)),
    )
    .into_response())
}

/// `DELETE /api/admin/partners/{consumer_id}` — remove a partner that has never
/// been billed.
///
/// Refused once any statement exists, by the foreign key: statements are
/// financial records and the partner they belong to is part of what they say. The
/// request path treats a partner with no row as a partner that may call nothing,
/// so a deletion is a suspension of access as well as of billing.
async fn delete_partner(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(consumer_id): Path<String>,
) -> Result<Response, AdminError> {
    let store = BillingStore::new(state.pool.clone());
    let id = consumer_id.clone();
    tokio::task::spawn_blocking(move || {
        let statements = store.count_statements(&crate::auth::Scope::One(id.clone()))?;
        if statements > 0 {
            return Err(crate::billing::store::BillingError::Invalid(format!(
                "partner {id} has {statements} statement(s) and cannot be deleted; \
                 their billing history is a financial record"
            )));
        }
        store.delete_partner(&id)
    })
    .await
    .map_err(AdminError::Join)??;

    refresh_snapshot(&state).await;
    tracing::warn!(consumer = %consumer_id, "partner deleted");
    Ok(no_store(
        StatusCode::OK,
        Json(serde_json::json!({ "deleted": true, "consumer_id": consumer_id })),
    )
    .into_response())
}

/// `GET /api/admin/partners/{consumer_id}/models` — what this partner may call.
async fn get_models(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(consumer_id): Path<String>,
) -> Result<Response, AdminError> {
    // Confirmed to exist first: a model list for a partner that does not exist is
    // an empty list, and an empty list is also what a partner with nothing
    // configured has. Answering "no such partner" for one and `[]` for the other
    // is the difference between a typo and a configuration.
    let store = BillingStore::new(state.pool.clone());
    let id = consumer_id.clone();
    let models = tokio::task::spawn_blocking(move || {
        if store.get_partner(&id)?.is_none() {
            return Err(crate::billing::store::BillingError::PartnerNotFound(id));
        }
        store.models(&id)
    })
    .await
    .map_err(AdminError::Join)??;

    let views: Vec<ModelPriceView> = models.iter().map(ModelPriceView::from_row).collect();
    Ok(no_store(StatusCode::OK, Json(views)).into_response())
}

/// `PUT /api/admin/partners/{consumer_id}/models` — replace what this partner
/// may call, and what they pay for it.
///
/// Both at once and atomically, because they are one fact: a model a partner may
/// call with no price is a request that cannot be metered, and a price for a
/// model they may not call is a price nobody can reach.
#[derive(Debug, Deserialize)]
pub struct ReplaceModelsRequest {
    pub models: Vec<ModelPriceView>,
}

async fn replace_models(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(consumer_id): Path<String>,
    Json(req): Json<ReplaceModelsRequest>,
) -> Result<Response, AdminError> {
    let models = parse_models(&req.models)?;

    let store = BillingStore::new(state.pool.clone());
    let id = consumer_id.clone();
    let count = models.len();
    tokio::task::spawn_blocking(move || store.replace_models(&id, &models))
        .await
        .map_err(AdminError::Join)??;

    refresh_snapshot(&state).await;
    let models = store_models(&state, &consumer_id).await?;
    tracing::info!(consumer = %consumer_id, models = count, "partner models replaced");

    let views: Vec<ModelPriceView> = models.iter().map(ModelPriceView::from_row).collect();
    Ok(no_store(StatusCode::OK, Json(views)).into_response())
}

/// Refresh the request-path snapshot after an operator changed the source rows.
///
/// A refresh failure cannot roll the committed edit back, and serving the last
/// coherent snapshot is safer than making the admin mutation look failed after
/// its durable state changed. The per-second refresher retries it; the warning
/// names the only time the two views may temporarily diverge.
async fn refresh_snapshot(state: &Arc<AppState>) {
    let store = state.api_keys.clone();
    match tokio::task::spawn_blocking(move || store.refresh()).await {
        Ok(Ok(_)) => {}
        Ok(Err(error)) => {
            tracing::warn!(error = %error, "partner snapshot refresh failed after admin mutation")
        }
        Err(error) => {
            tracing::warn!(error = %error, "partner snapshot refresh task failed after admin mutation")
        }
    }
}

/// Read a partner's models back, off the runtime.
async fn store_models(
    state: &Arc<AppState>,
    consumer_id: &str,
) -> Result<Vec<ModelPrice>, AdminError> {
    let store = BillingStore::new(state.pool.clone());
    let id = consumer_id.to_string();
    Ok(tokio::task::spawn_blocking(move || store.models(&id))
        .await
        .map_err(AdminError::Join)??)
}

/// Parse a billing mode from the wire.
fn parse_mode(raw: &str) -> Result<BillingMode, AdminError> {
    BillingMode::parse(raw.trim()).ok_or_else(|| {
        AdminError::BadRequest(format!(
            "`billing_mode` must be `invoice` or `reconciliation`, not {raw:?}"
        ))
    })
}

/// Convert the wire's model list into the store's, refusing a duplicate.
///
/// The duplicate check is here as well as in the store because the two refuse
/// different things: this one can name the position in the request that is
/// wrong, and the store's guards the invariant for every caller.
fn parse_models(views: &[ModelPriceView]) -> Result<Vec<ModelPrice>, AdminError> {
    let mut seen: BTreeMap<&str, ()> = BTreeMap::new();
    let mut out = Vec::with_capacity(views.len());
    for view in views {
        let model = view.model.trim();
        if model.is_empty() {
            return Err(AdminError::BadRequest(
                "a model name must not be blank".to_string(),
            ));
        }
        if seen.insert(model, ()).is_some() {
            return Err(AdminError::BadRequest(format!(
                "{model:?} is listed twice; a model has one price"
            )));
        }
        out.push(ModelPrice {
            // Stored verbatim, not trimmed: the check above trims because a
            // whitespace-only name is a typo, but *storing* the trimmed form
            // would silently rename a model the operator configured, and the
            // request path compares model names literally.
            model: view.model.clone(),
            prices: PricingSnapshot::new(
                parse_price(&view.model, "input_per_million", &view.input_per_million)?,
                parse_price(
                    &view.model,
                    "cached_input_per_million",
                    &view.cached_input_per_million,
                )?,
                parse_price(&view.model, "output_per_million", &view.output_per_million)?,
            ),
        });
    }
    Ok(out)
}

/// Parse a decimal dollar price into integer micro-USD per million tokens.
///
/// The conversion itself is [`PricePerMillion::parse`]'s, deliberately and
/// without a second implementation here: it is the same value the config file,
/// the statement arithmetic and the frozen usage row all use, and a second
/// parser at the HTTP edge would be a second rounding rule — the one thing that
/// would make a price in a statement disagree with the price that produced it.
/// What this wrapper adds is the context a 400 needs: which model, which field.
///
/// The wire carries a decimal *string* — `"0.095"` — and never a JSON number.
/// `0.1` has no exact binary representation, and a price that arrives one ulp low
/// is a partner's bill that is wrong in the last digit that shows.
fn parse_price(model: &str, field: &str, raw: &str) -> Result<PricePerMillion, AdminError> {
    PricePerMillion::parse(raw.trim())
        .map_err(|e| AdminError::BadRequest(format!("{model:?}: `{field}`: {e}")))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_decimal_price_becomes_micro_dollars_exactly() {
        // The four prices the specification names, each of which is exact in
        // micro-USD and none of which is exact in binary floating point.
        for (raw, expected) in [
            ("0.095", 95_000),
            ("0.0475", 47_500),
            ("0.475", 475_000),
            ("0.002375", 2_375),
            ("0", 0),
            ("1", 1_000_000),
            ("0.000001", 1),
            ("12.5", 12_500_000),
            // The surrounding whitespace an operator's copy-paste leaves.
            (" 0.095 ", 95_000),
            ("2.5", 2_500_000),
        ] {
            let got = parse_price("gpt-4o", "input_per_million", raw).unwrap();
            assert_eq!(got.as_i64(), expected, "{raw:?}");
        }
    }

    #[test]
    fn test_a_price_with_more_precision_than_the_unit_rounds_half_away_from_zero() {
        // A seventh digit decides the sixth: truncating instead would make every
        // price quietly cheaper than the one that was quoted.
        for (raw, expected) in [
            ("0.0950005", 95_001),
            ("0.0950004", 95_000),
            ("0.0950004999", 95_000),
            ("0.095000000", 95_000),
        ] {
            let got = parse_price("m", "f", raw).unwrap();
            assert_eq!(got.as_i64(), expected, "{raw:?}");
        }
    }

    #[test]
    fn test_a_price_that_is_not_a_price_is_refused_not_guessed() {
        for raw in [
            "",
            "   ",
            // A refund is not something a partner is configured into.
            "-0.095",
            "free",
            "$0.095",
            "0.09.5",
            // A number that stopped typing.
            "0.095.",
            "0.095e3",
            "nan",
            "inf",
            "0x10",
            "1,5",
            // Out of range for the storage type, not silently wrapped.
            "92233720368547758.08",
        ] {
            assert!(
                parse_price("m", "f", raw).is_err(),
                "{raw:?} must not parse as a price"
            );
        }
        // The short way of writing it is a price, and reads unambiguously.
        assert_eq!(
            parse_price("m", "f", ".095").unwrap().as_i64(),
            95_000,
            "\".095\" is the same number"
        );
    }

    /// The error a client reads names the model and the field, so an operator
    /// editing a list of twenty models knows which line to fix.
    #[test]
    fn test_a_refused_price_says_which_model_and_which_field() {
        let err = parse_price("gpt-4o", "output_per_million", "free").unwrap_err();
        let AdminError::BadRequest(message) = err else {
            panic!("a bad price is a bad request");
        };
        assert!(message.contains("gpt-4o"), "{message}");
        assert!(message.contains("output_per_million"), "{message}");
    }

    #[test]
    fn test_a_model_list_that_repeats_a_model_is_refused() {
        let view = ModelPriceView {
            model: "gpt-4o".into(),
            input_per_million: "0.095".into(),
            cached_input_per_million: "0.0475".into(),
            output_per_million: "0.475".into(),
        };
        assert!(parse_models(std::slice::from_ref(&view)).is_ok());
        assert!(parse_models(&[view.clone(), view.clone()]).is_err());

        let blank = ModelPriceView {
            model: "   ".into(),
            ..view.clone()
        };
        assert!(parse_models(&[blank]).is_err());

        // A model name is stored verbatim once it is not blank: trimming it
        // would rename a model the request path compares literally.
        let padded = ModelPriceView {
            model: " gpt-4o ".into(),
            ..view.clone()
        };
        let parsed = parse_models(&[padded]).unwrap();
        assert_eq!(parsed[0].model, " gpt-4o ");
    }

    #[test]
    fn test_a_mode_is_parsed_or_refused_and_never_defaulted() {
        assert_eq!(parse_mode("invoice").unwrap(), BillingMode::Invoice);
        assert_eq!(
            parse_mode(" reconciliation ").unwrap(),
            BillingMode::Reconciliation
        );
        // Guessing a mode is guessing whether a partner owes money.
        assert!(parse_mode("").is_err());
        assert!(parse_mode("prepaid").is_err());
        assert!(parse_mode("Invoice").is_err());
    }

    #[test]
    fn test_a_model_view_round_trips_through_the_decimal_string() {
        let row = ModelPrice {
            model: "gpt-4o".into(),
            prices: PricingSnapshot::new(
                PricePerMillion::new(95_000),
                PricePerMillion::new(47_500),
                PricePerMillion::new(475_000),
            ),
        };
        let view = ModelPriceView::from_row(&row);
        assert_eq!(view.input_per_million, "0.095");
        assert_eq!(view.cached_input_per_million, "0.0475");
        assert_eq!(view.output_per_million, "0.475");

        let back = parse_models(std::slice::from_ref(&view)).unwrap();
        assert_eq!(back[0], row);
    }
}
