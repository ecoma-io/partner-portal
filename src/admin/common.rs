//! What every `/api/admin` route shares: who may call it, and how it fails.
//!
//! # Why one module
//!
//! There are three admin surfaces — API keys, partners and billing — and the two
//! answers above have to be *the same* on all three. A second copy of
//! [`no_store`] is a response that forgets it; a second error renderer is a
//! database failure some surface renders as a 400. Both are failures that no
//! single surface's tests would catch, so the mechanisms live here once and each
//! surface is about its own domain.
//!
//! # Who may call this
//!
//! The manager password, and nothing else. A partner key is scoped to one
//! consumer, and letting it mint keys, edit prices or mark its own invoice paid
//! would make one partner's credential a path to commercial state it does not
//! own. The manager password is already refused on `/v1/*` (ADR 0013); this is
//! the mirror of that.
//!
//! A partner key gets 403 and no credential gets 401, so a client being debugged
//! can tell "you presented nothing usable" from "you presented something usable
//! that is not allowed here".

use axum::{
    Json,
    extract::FromRequestParts,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use std::sync::Arc;

use crate::apikeys::store::ApiKeyError;
use crate::auth::{AuthError as CredentialError, Authenticated, ConsumerContext};
use crate::billing::store::BillingError;
use crate::proxy::handler::AppState;

/// A credential that is allowed to administer the product.
pub struct ManagerOnly(pub ConsumerContext);

impl FromRequestParts<Arc<AppState>> for ManagerOnly {
    type Rejection = ManagerOnlyError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &Arc<AppState>,
    ) -> Result<Self, Self::Rejection> {
        let Authenticated(consumer) = Authenticated::from_request_parts(parts, state).await?;
        if !consumer.is_manager() {
            return Err(ManagerOnlyError::NotManager);
        }
        Ok(ManagerOnly(consumer))
    }
}

/// Refusal from a manager-only route.
#[derive(Debug)]
pub enum ManagerOnlyError {
    /// No usable credential. Rendered by `Authenticated`, reused here so the two
    /// surfaces answer identically.
    Unauthenticated(CredentialError),
    /// A valid credential that is scoped to a single consumer.
    NotManager,
}

impl From<CredentialError> for ManagerOnlyError {
    fn from(e: CredentialError) -> Self {
        ManagerOnlyError::Unauthenticated(e)
    }
}

impl IntoResponse for ManagerOnlyError {
    fn into_response(self) -> Response {
        match self {
            ManagerOnlyError::Unauthenticated(e) => e.into_response(),
            ManagerOnlyError::NotManager => {
                let body = serde_json::json!({
                    "error": {
                        "message": "This credential is scoped to one consumer and \
                                    cannot administer the product; use the manager password",
                        "type": "permission_error",
                        "code": "manager_required",
                    }
                });
                no_store(StatusCode::FORBIDDEN, Json(body)).into_response()
            }
        }
    }
}

/// Attach the headers every response on this surface carries.
///
/// `no-store` because these bodies describe credentials or commercial state that
/// a shared cache must never replay: a create or rotate body *is* a credential,
/// and a partner's billing email is not a public document. `Pragma: no-cache` is
/// belt-and-braces for HTTP/1.0 intermediaries, which ignore `Cache-Control`.
///
/// The status is a parameter rather than fixed at 200 because this wraps errors
/// too — an error body that rendered 200 would be the loudest possible way to
/// fail.
pub fn no_store<B>(
    status: StatusCode,
    body: B,
) -> (StatusCode, [(header::HeaderName, &'static str); 2], B) {
    (
        status,
        [
            (header::CACHE_CONTROL, "no-store"),
            (header::PRAGMA, "no-cache"),
        ],
        body,
    )
}

/// Admin error, shaped like [`crate::dashboard::api::DashboardError`].
///
/// One variant per *outcome a client acts on*, not one per module: two surfaces
/// that both mean "no such thing" render the same 404 with the same code, and
/// the status is decided in the same match as the body. Deriving the status from
/// the code in a second match over the same value would be two places to keep in
/// step, and a new variant would compile with only one of them filled in.
#[derive(Debug)]
pub enum AdminError {
    /// The request was malformed, or asked for something the product refuses.
    BadRequest(String),
    /// No API key has that id.
    KeyNotFound(i64),
    /// No partner has that `consumer_id`.
    PartnerNotFound(String),
    /// A partner with that `consumer_id` already exists.
    PartnerExists(String),
    /// No statement has that id.
    StatementNotFound(i64),
    /// The statement exists and carries no payment obligation — it is a
    /// reconciliation statement. A payment recorded against it would be a
    /// payment against a bill that was never owed.
    StatementNotPayable { id: i64, billing_mode: String },
    /// A key-store failure, including the two conflicts a key can be in: a
    /// revoked key cannot be edited, and a consumer that already holds an active
    /// key cannot be issued a second one. Both are rendered from the store's own
    /// variants rather than restated here — a second way to say "this key is
    /// revoked" is a second thing to keep in step.
    Key(ApiKeyError),
    /// A billing-store failure.
    Billing(BillingError),
    /// The blocking task did not finish. Distinct from a store error because it
    /// means the request was not processed at all, which a client may retry.
    Join(tokio::task::JoinError),
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        let (status, code, message) = match self {
            AdminError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "invalid_request", msg),
            AdminError::KeyNotFound(id) => (
                StatusCode::NOT_FOUND,
                "not_found",
                format!("no api key with id {id}"),
            ),
            AdminError::PartnerNotFound(consumer_id) => (
                StatusCode::NOT_FOUND,
                "not_found",
                format!("no partner with consumer_id {consumer_id}"),
            ),
            AdminError::PartnerExists(consumer_id) => (
                StatusCode::CONFLICT,
                "partner_exists",
                format!(
                    "a partner with consumer_id {consumer_id} already exists; use \
                     PATCH /api/admin/partners/{consumer_id} to change it"
                ),
            ),
            AdminError::StatementNotFound(id) => (
                StatusCode::NOT_FOUND,
                "not_found",
                format!("no statement with id {id}"),
            ),
            AdminError::StatementNotPayable { id, billing_mode } => (
                StatusCode::CONFLICT,
                "statement_not_payable",
                format!(
                    "statement {id} is a {billing_mode} statement and carries no \
                     payment obligation"
                ),
            ),
            AdminError::Key(ApiKeyError::Invalid(msg)) => {
                (StatusCode::BAD_REQUEST, "invalid_request", msg)
            }
            AdminError::Key(ApiKeyError::NotFound(id)) => (
                StatusCode::NOT_FOUND,
                "not_found",
                format!("no api key with id {id}"),
            ),
            // A revoked key is a conflict with its current state, not a bad
            // request: the request was well-formed and the resource is simply
            // no longer editable.
            AdminError::Key(ApiKeyError::NotActive(id)) => (
                StatusCode::CONFLICT,
                "key_not_active",
                format!("api key {id} is revoked and cannot be changed"),
            ),
            // A partner has one credential. Issuing a second is a well-formed
            // request against a state that already exists, which is what a 409
            // is for — and the message says what to do instead, because
            // "already has an active key" without a next step is a dead end for
            // whoever is holding the admin token.
            AdminError::Key(ApiKeyError::AlreadyActive(exists)) => (
                StatusCode::CONFLICT,
                "key_already_active",
                format!(
                    "consumer {} already has an active api key; rotate that key \
                     instead of creating a second one",
                    exists.consumer_id
                ),
            ),
            AdminError::Key(ApiKeyError::Database(e)) => {
                tracing::error!(error = %e, "api key database error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "database error".to_string(),
                )
            }
            AdminError::Billing(BillingError::Invalid(msg)) => {
                (StatusCode::BAD_REQUEST, "invalid_request", msg)
            }
            AdminError::Billing(BillingError::PartnerNotFound(consumer_id)) => (
                StatusCode::NOT_FOUND,
                "not_found",
                format!("no partner with consumer_id {consumer_id}"),
            ),
            AdminError::Billing(BillingError::PartnerExists(consumer_id)) => (
                StatusCode::CONFLICT,
                "partner_exists",
                format!("a partner with consumer_id {consumer_id} already exists"),
            ),
            AdminError::Billing(BillingError::StatementNotPayable { id, billing_mode }) => (
                StatusCode::CONFLICT,
                "statement_not_payable",
                format!(
                    "statement {id} is a {billing_mode} statement and carries no \
                     payment obligation"
                ),
            ),
            AdminError::Billing(BillingError::Database(e)) => {
                tracing::error!(error = %e, "billing database error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "database error".to_string(),
                )
            }
            AdminError::Join(e) => {
                tracing::error!(error = %e, "admin task did not finish");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "internal error".to_string(),
                )
            }
        };

        let body = serde_json::json!({
            "error": { "message": message, "type": "admin_error", "code": code }
        });
        no_store(status, Json(body)).into_response()
    }
}

impl From<ApiKeyError> for AdminError {
    fn from(e: ApiKeyError) -> Self {
        AdminError::Key(e)
    }
}

/// A billing failure, mapped to the outcome the client acts on.
///
/// `PartnerNotFound`/`PartnerExists` become their own variants rather than a
/// generic conflict so a caller that asked about a partner that does not exist
/// is told *that*, instead of "constraint failed". `Invalid` becomes a 400 — it
/// is raised before SQL for a name that is blank, terms that are negative or a
/// model listed twice, which is a malformed request and not a state conflict.
impl From<BillingError> for AdminError {
    fn from(e: BillingError) -> Self {
        match e {
            BillingError::PartnerNotFound(consumer_id) => AdminError::PartnerNotFound(consumer_id),
            BillingError::PartnerExists(consumer_id) => AdminError::PartnerExists(consumer_id),
            BillingError::StatementNotPayable { id, billing_mode } => {
                AdminError::StatementNotPayable { id, billing_mode }
            }
            BillingError::Invalid(msg) => AdminError::BadRequest(msg),
            BillingError::Database(e) => AdminError::Billing(BillingError::Database(e)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::apikeys::store::PartnerKeyExists;
    use crate::billing::store::BillingError;

    /// The status is part of the decision, and it is decided in the same match
    /// as the body — a body that rendered 200 would be the loudest possible way
    /// to fail, and nothing else would catch it.
    #[tokio::test]
    async fn test_every_admin_error_renders_its_own_status() {
        for (error, expected, code) in [
            (
                AdminError::BadRequest("`status` must be `active` or `revoked`".into()),
                StatusCode::BAD_REQUEST,
                "invalid_request",
            ),
            (
                AdminError::KeyNotFound(9),
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            (
                AdminError::Key(ApiKeyError::NotFound(9)),
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            (
                AdminError::Key(ApiKeyError::NotActive(4)),
                StatusCode::CONFLICT,
                "key_not_active",
            ),
            (
                AdminError::Key(ApiKeyError::AlreadyActive(PartnerKeyExists {
                    consumer_id: "acme".to_string(),
                })),
                StatusCode::CONFLICT,
                "key_already_active",
            ),
            (
                AdminError::Key(ApiKeyError::Invalid("name must not be empty".into())),
                StatusCode::BAD_REQUEST,
                "invalid_request",
            ),
            (
                AdminError::Key(ApiKeyError::Database(rusqlite::Error::QueryReturnedNoRows)),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ),
            (
                AdminError::PartnerNotFound("acme".into()),
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            (
                AdminError::PartnerExists("acme".into()),
                StatusCode::CONFLICT,
                "partner_exists",
            ),
            (
                AdminError::StatementNotFound(7),
                StatusCode::NOT_FOUND,
                "not_found",
            ),
            (
                AdminError::StatementNotPayable {
                    id: 7,
                    billing_mode: "reconciliation".into(),
                },
                StatusCode::CONFLICT,
                "statement_not_payable",
            ),
            (
                AdminError::Billing(BillingError::Database(rusqlite::Error::QueryReturnedNoRows)),
                StatusCode::INTERNAL_SERVER_ERROR,
                "internal_error",
            ),
        ] {
            let label = format!("{error:?}");
            let response = error.into_response();
            assert_eq!(
                response.status(),
                expected,
                "{label} must not render as a different status"
            );
            // Even a database error, whose detail is logged rather than shown,
            // must not leak the SQLite error text to the client.
            assert_eq!(
                response.headers().get(header::CACHE_CONTROL).unwrap(),
                "no-store"
            );
            let body = axum::body::to_bytes(response.into_body(), 8192)
                .await
                .expect("an error body is small enough to read");
            let body = String::from_utf8(body.to_vec()).unwrap();
            assert!(
                body.contains(code),
                "{label} must carry code {code}: {body}"
            );
            assert!(!body.contains("QueryReturnedNoRows"), "{body}");
        }
    }

    /// A billing failure keeps its meaning on the way through.
    #[test]
    fn test_a_billing_failure_maps_to_the_outcome_a_client_acts_on() {
        assert!(matches!(
            AdminError::from(BillingError::PartnerNotFound("acme".into())),
            AdminError::PartnerNotFound(id) if id == "acme"
        ));
        assert!(matches!(
            AdminError::from(BillingError::PartnerExists("acme".into())),
            AdminError::PartnerExists(id) if id == "acme"
        ));
        assert!(matches!(
            AdminError::from(BillingError::Invalid("blank name".into())),
            AdminError::BadRequest(msg) if msg == "blank name"
        ));
        assert!(matches!(
            AdminError::from(BillingError::Database(rusqlite::Error::QueryReturnedNoRows)),
            AdminError::Billing(BillingError::Database(_))
        ));
    }

    #[tokio::test]
    async fn test_a_manager_credential_is_refused_by_a_partner_key() {
        use crate::auth::ConsumerContext;
        use crate::billing::partner::{BillingMode, PartnerRuntimeConfig};
        use crate::billing::status::ServiceStatus;

        // The two refusals are deliberately different answers: a client being
        // debugged needs to know whether it presented nothing or presented
        // something that is not allowed here.
        let partner = ConsumerContext::new(Arc::new(PartnerRuntimeConfig::empty(
            "acme".to_string(),
            "primary".to_string(),
            BillingMode::Invoice,
            ServiceStatus::Active,
        )));
        assert!(!partner.is_manager(), "a partner key is not the manager");

        let response = ManagerOnlyError::NotManager.into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        assert_eq!(
            response.headers().get(header::CACHE_CONTROL).unwrap(),
            "no-store"
        );

        // And the unauthenticated case still renders the 401 body, unchanged.
        let unauth = ManagerOnlyError::Unauthenticated(CredentialError::InvalidKey).into_response();
        assert_eq!(unauth.status(), StatusCode::UNAUTHORIZED);
    }
}
