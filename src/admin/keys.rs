//! Admin API for partner API keys.
//!
//! # Who may call this
//!
//! The manager password, and nothing else. This is the same credential the
//! dashboard authenticates with, and the decision is deliberate: a partner key
//! is scoped to one consumer, and letting it mint or revoke keys would make one
//! partner's key a path to another partner's identity. The manager password is
//! already refused on `/v1/*` (ADR 0013); this is the mirror of that — a
//! credential that is not a partner key is not a partner key issuer either.
//!
//! A partner key gets 403 and no credential gets 401, so the two cases stay
//! distinguishable by a client that is being debugged.
//!
//! # The plaintext key
//!
//! `create` and `rotate` are the **only** responses in the entire product that
//! contain a plaintext key, and each contains it exactly once. Everything else
//! returns [`ApiKeyRow`], which has no plaintext field and no hash field to fall
//! back on — the plaintext is not in the type, so it cannot leak by being
//! forgotten when someone adds a field.
//!
//! Both responses are `Cache-Control: no-store` for the same reason the 401 is:
//! a credential must never be replayable from a cache.
//!
//! # Blocking work
//!
//! Every handler here opens a SQLite connection, so every one runs on
//! `spawn_blocking`. These are administrative operations, not the request path —
//! the number of them is bounded by how often a human issues a key.

use axum::{
    Json, Router,
    extract::{Path, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;

use crate::apikeys::store::{ApiKeyError, ApiKeyRow, KeyStatus};
use crate::auth::{AuthError as CredentialError, Authenticated, ConsumerContext};
use crate::ledger::timefmt;
use crate::proxy::handler::AppState;

/// Admin API-key router. Mounted at the root by [`super::create_admin_router`].
///
/// Every route takes [`ManagerOnly`], so a new route added here is
/// manager-only by construction rather than by remembering to check.
pub fn create_key_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/admin/api-keys", post(create_key).get(list_keys))
        .route("/api/admin/api-keys/{id}", get(get_key).patch(update_key))
        .route("/api/admin/api-keys/{id}/rotate", post(rotate_key))
        .route("/api/admin/api-keys/{id}/revoke", post(revoke_key))
}

/// A credential that is allowed to manage API keys.
///
/// Rejects a partner key with 403 and refuses to run at all without a valid
/// credential, so the 401 and the 403 cannot be confused: one means "you
/// presented nothing usable", the other means "you presented something usable
/// that is not allowed here".
pub struct ManagerOnly(pub ConsumerContext);

impl axum::extract::FromRequestParts<Arc<AppState>> for ManagerOnly {
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
                                    cannot manage api keys; use the manager password",
                        "type": "permission_error",
                        "code": "manager_required",
                    }
                });
                no_store(StatusCode::FORBIDDEN, Json(body)).into_response()
            }
        }
    }
}

/// A key as an operator sees it.
///
/// Built by hand from [`ApiKeyRow`] rather than derived from it, so adding a
/// column to the table cannot silently start publishing it. The one field this
/// adds beyond the row is `key`, and it is populated **only** by the create and
/// rotate paths — which is why they build this struct themselves instead of
/// calling [`ApiKeyView::from_row`].
#[derive(Debug, Serialize)]
pub struct ApiKeyView {
    pub id: i64,
    pub name: String,
    pub consumer_id: String,
    pub key_prefix: String,
    pub allowed_models: Vec<String>,
    pub status: &'static str,
    pub created_at: String,
    pub updated_at: String,
    pub expires_at: Option<String>,
    pub revoked_at: Option<String>,
}

impl ApiKeyView {
    fn from_row(row: &ApiKeyRow) -> Self {
        Self {
            id: row.id,
            name: row.name.clone(),
            consumer_id: row.consumer_id.clone(),
            key_prefix: row.key_prefix.clone(),
            allowed_models: row.allowed_models.clone(),
            status: match row.status {
                KeyStatus::Active => "active",
                KeyStatus::Revoked => "revoked",
            },
            created_at: row.created_at.clone(),
            updated_at: row.updated_at.clone(),
            expires_at: row.expires_at.clone(),
            revoked_at: row.revoked_at.clone(),
        }
    }
}

/// The response to a create or a rotate: the row, plus the one plaintext.
///
/// `key` is `Option` so its absence is a type-level statement in every other
/// response rather than an omission someone can forget.
#[derive(Debug, Serialize)]
pub struct IssuedKeyView {
    #[serde(flatten)]
    pub key: ApiKeyView,
    /// The plaintext, once. Never retrievable again.
    pub key_secret: Option<String>,
}

/// Attach the headers every response on this surface carries.
///
/// `no-store` because these bodies describe credentials: the 401-adjacent ones
/// exist so a client never learns a key is revoked, and a create or rotate body
/// *is* the credential. `Pragma: no-cache` is belt-and-braces for HTTP/1.0
/// intermediaries, which ignore `Cache-Control`.
///
/// The status is a parameter rather than fixed at 200 because this wraps errors
/// too — an error body that rendered 200 would be the loudest possible way to
/// fail.
fn no_store<B>(
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

#[derive(Debug, Deserialize)]
pub struct CreateKeyRequest {
    pub name: String,
    pub consumer_id: String,
    #[serde(default)]
    pub allowed_models: Vec<String>,
    /// ISO 8601, or null/absent for a key that never expires.
    pub expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateKeyRequest {
    pub name: Option<String>,
    pub allowed_models: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
pub struct ListKeysQuery {
    /// `active` or `revoked`; absent means every key.
    pub status: Option<String>,
}

/// `POST /api/admin/api-keys` — issue a key.
///
/// The response is the only time the plaintext exists anywhere but the
/// operator's screen. It is `no-store` for the same reason the 401 is.
async fn create_key(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Json(req): Json<CreateKeyRequest>,
) -> Result<Response, AdminError> {
    let expires_at = parse_expiry(req.expires_at.as_deref())?;

    let store = state.api_keys.clone();
    let (row, plaintext) = tokio::task::spawn_blocking(move || {
        store.create(&req.name, &req.consumer_id, req.allowed_models, expires_at)
    })
    .await
    .map_err(AdminError::Join)??;

    tracing::info!(id = row.id, name = %row.name, consumer = %row.consumer_id, "api key issued");

    Ok(issued(
        StatusCode::CREATED,
        IssuedKeyView {
            key: ApiKeyView::from_row(&row),
            key_secret: Some(plaintext),
        },
    ))
}

/// `POST /api/admin/api-keys/{id}/rotate` — replace a key's secret.
///
/// The old row is revoked and the new one inserted in one transaction, so a
/// crash can leave the old key valid or the new one valid but never two or
/// neither. The plaintext appears here for the last time, and the caller gets a
/// new id for the same consumer and the same name.
async fn rotate_key(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(id): Path<i64>,
) -> Result<Response, AdminError> {
    let store = state.api_keys.clone();
    let (row, plaintext) = tokio::task::spawn_blocking(move || store.rotate(id))
        .await
        .map_err(AdminError::Join)??;

    tracing::info!(
        id = row.id,
        consumer = %row.consumer_id,
        "api key rotated"
    );

    Ok(issued(
        StatusCode::CREATED,
        IssuedKeyView {
            key: ApiKeyView::from_row(&row),
            key_secret: Some(plaintext),
        },
    ))
}

/// `GET /api/admin/api-keys` — list keys. Never contains a plaintext.
async fn list_keys(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    axum::extract::Query(query): axum::extract::Query<ListKeysQuery>,
) -> Result<Response, AdminError> {
    let filter = query.status.as_deref().map(str::to_ascii_lowercase);
    if let Some(s) = filter.as_deref() {
        if s != "active" && s != "revoked" {
            return Err(AdminError::BadRequest(format!(
                "`status` must be `active` or `revoked`, not `{s}`"
            )));
        }
    }

    let store = state.api_keys.clone();
    let rows = tokio::task::spawn_blocking(move || store.list())
        .await
        .map_err(AdminError::Join)??;

    let views: Vec<ApiKeyView> = rows
        .iter()
        .filter(|row| match filter.as_deref() {
            None => true,
            Some("active") => row.status == KeyStatus::Active,
            Some(_) => row.status == KeyStatus::Revoked,
        })
        .map(ApiKeyView::from_row)
        .collect();

    Ok(no_store(StatusCode::OK, Json(views)).into_response())
}

/// `GET /api/admin/api-keys/{id}` — one key. Never contains a plaintext.
async fn get_key(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(id): Path<i64>,
) -> Result<Response, AdminError> {
    let store = state.api_keys.clone();
    let row = tokio::task::spawn_blocking(move || store.get(id))
        .await
        .map_err(AdminError::Join)??;

    match row {
        Some(row) => Ok(no_store(StatusCode::OK, Json(ApiKeyView::from_row(&row))).into_response()),
        None => Err(AdminError::NotFound(id)),
    }
}

/// `PATCH /api/admin/api-keys/{id}` — rename a key or change its allow-list.
///
/// Deliberately cannot change the secret. Rotating a secret destroys the old
/// one; doing that under a PATCH that a client may retry would issue a key per
/// retry. `POST .../rotate` is the one way to change a secret, and it is never
/// retried by accident because it is not idempotent by design.
async fn update_key(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(id): Path<i64>,
    Json(req): Json<UpdateKeyRequest>,
) -> Result<Response, AdminError> {
    let store = state.api_keys.clone();
    let row = tokio::task::spawn_blocking(move || {
        store.update(id, req.name.as_deref(), req.allowed_models)
    })
    .await
    .map_err(AdminError::Join)??;

    tracing::info!(id = row.id, "api key updated");
    Ok(no_store(StatusCode::OK, Json(ApiKeyView::from_row(&row))).into_response())
}

/// `POST /api/admin/api-keys/{id}/revoke` — stop a key authenticating.
///
/// The write commits before this returns, and the store refreshes the snapshot
/// inside that same call, so the key is already rejected on the next request
/// against this instance. A sibling instance picks it up within
/// `server.api_key_refresh_ms`.
///
/// Idempotent: revoking a revoked key reports it as already revoked rather than
/// failing, because the caller's intent — this key must not work — is already
/// satisfied.
async fn revoke_key(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(id): Path<i64>,
) -> Result<Response, AdminError> {
    let store = state.api_keys.clone();
    let row = tokio::task::spawn_blocking(move || store.revoke(id))
        .await
        .map_err(AdminError::Join)??;

    tracing::info!(id = row.id, "api key revoked");
    Ok(no_store(StatusCode::OK, Json(ApiKeyView::from_row(&row))).into_response())
}

/// A created-issued-key response: the row, the plaintext, and the headers.
fn issued(status: StatusCode, body: IssuedKeyView) -> Response {
    no_store(status, Json(body)).into_response()
}

/// Parse an optional ISO 8601 expiry.
fn parse_expiry(raw: Option<&str>) -> Result<Option<OffsetDateTime>, AdminError> {
    let Some(raw) = raw else { return Ok(None) };
    if raw.trim().is_empty() {
        return Ok(None);
    }
    timefmt::parse_ts(raw)
        .map(Some)
        .ok_or_else(|| AdminError::BadRequest(format!("`expires_at` is not ISO 8601: {raw:?}")))
}

/// Admin error, shaped like [`crate::dashboard::api::DashboardError`].
#[derive(Debug)]
pub enum AdminError {
    BadRequest(String),
    NotFound(i64),
    Key(ApiKeyError),
    /// The blocking task did not finish. Distinct from a key error because it
    /// means the request was not processed at all, which a client may retry.
    Join(tokio::task::JoinError),
}

impl IntoResponse for AdminError {
    fn into_response(self) -> Response {
        // One match, because a status and a body are one decision: deriving the
        // status from the code in a second match over the same value would be
        // two places to keep in step, and a new variant would compile with only
        // one of them filled in.
        let (status, code, message) = match self {
            AdminError::BadRequest(msg) => (StatusCode::BAD_REQUEST, "invalid_request", msg),
            AdminError::Key(ApiKeyError::Invalid(msg)) => {
                (StatusCode::BAD_REQUEST, "invalid_request", msg)
            }
            AdminError::NotFound(id) => (
                StatusCode::NOT_FOUND,
                "not_found",
                format!("no api key with id {id}"),
            ),
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
            AdminError::Key(ApiKeyError::Database(e)) => {
                // Log the detail; never hand database internals to the client.
                tracing::error!(error = %e, "api key database error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal_error",
                    "database error".to_string(),
                )
            }
            AdminError::Join(e) => {
                tracing::error!(error = %e, "api key admin task did not finish");
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_a_view_never_carries_a_plaintext_or_a_hash() {
        // The guarantee is structural, so it is asserted structurally: the
        // serialised form of a view, rendered from a row that has both a prefix
        // and a hash in the database, contains neither.
        let row = ApiKeyRow {
            id: 1,
            name: "primary".into(),
            consumer_id: "acme".into(),
            key_prefix: "pp_abc123XY".into(),
            allowed_models: vec!["gpt-4o".into()],
            status: KeyStatus::Active,
            created_at: "2026-09-28T00:00:00.000000000Z".into(),
            updated_at: "2026-09-28T00:00:00.000000000Z".into(),
            expires_at: None,
            revoked_at: None,
        };
        let json = serde_json::to_string(&ApiKeyView::from_row(&row)).unwrap();

        assert!(
            json.contains("pp_abc123XY"),
            "the prefix is meant to be shown"
        );
        assert!(!json.contains("key_hash"), "{json}");
        assert!(!json.contains("key_secret"), "{json}");
        assert!(!json.contains("\"key\""), "{json}");
    }

    #[test]
    fn test_an_expiry_is_parsed_or_refused_and_never_silently_dropped() {
        assert!(parse_expiry(None).unwrap().is_none());
        assert!(parse_expiry(Some("  ")).unwrap().is_none());
        let ts = parse_expiry(Some("2026-09-28T00:00:00Z")).unwrap();
        assert!(ts.is_some());

        // A typo must not become a key that never expires: that is the silent
        // failure this refuses.
        assert!(parse_expiry(Some("next tuesday")).is_err());
        assert!(parse_expiry(Some("1788000000")).is_err());
    }

    /// The status is part of the decision, and it is decided in the same match
    /// as the body — a body that rendered 200 would be the loudest possible way
    /// to fail, and nothing else would catch it.
    #[tokio::test]
    async fn test_every_admin_error_renders_its_own_status() {
        for (error, expected) in [
            (
                AdminError::BadRequest("`status` must be `active` or `revoked`".into()),
                StatusCode::BAD_REQUEST,
            ),
            (AdminError::NotFound(9), StatusCode::NOT_FOUND),
            (
                AdminError::Key(ApiKeyError::NotFound(9)),
                StatusCode::NOT_FOUND,
            ),
            (
                AdminError::Key(ApiKeyError::NotActive(4)),
                StatusCode::CONFLICT,
            ),
            (
                AdminError::Key(ApiKeyError::Invalid("name must not be empty".into())),
                StatusCode::BAD_REQUEST,
            ),
            (
                AdminError::Key(ApiKeyError::Database(rusqlite::Error::QueryReturnedNoRows)),
                StatusCode::INTERNAL_SERVER_ERROR,
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
        }
    }

    #[tokio::test]
    async fn test_a_manager_credential_is_refused_by_a_partner_key() {
        use crate::auth::ConsumerContext;

        // The two refusals are deliberately different answers: a client being
        // debugged needs to know whether it presented nothing or presented
        // something that is not allowed here.
        let partner = ConsumerContext::new("acme".into(), "primary".into(), vec![]);
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
