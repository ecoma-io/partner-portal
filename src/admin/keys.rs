//! Admin API for partner API keys.
//!
//! # Who may call this
//!
//! The manager password, and nothing else — why, and how the two refusals are
//! distinguished, is [`crate::admin::common`]'s story and not this module's.
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
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::OffsetDateTime;

use crate::apikeys::store::{ApiKeyRow, KeyStatus};
use crate::ledger::timefmt;
use crate::proxy::handler::AppState;

use super::{AdminError, ManagerOnly, no_store};

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

#[derive(Debug, Deserialize)]
pub struct CreateKeyRequest {
    pub name: String,
    pub consumer_id: String,
    /// ISO 8601, or null/absent for a key that never expires.
    pub expires_at: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct UpdateKeyRequest {
    pub name: Option<String>,
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
    let (row, plaintext) =
        tokio::task::spawn_blocking(move || store.create(&req.name, &req.consumer_id, expires_at))
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
        None => Err(AdminError::KeyNotFound(id)),
    }
}

/// `PATCH /api/admin/api-keys/{id}` — rename a key.
///
/// Deliberately cannot change the secret. Rotating a secret destroys the old
/// one; doing that under a PATCH that a client may retry would issue a key per
/// retry. `POST .../rotate` is the one way to change a secret, and it is never
/// retried by accident because it is not idempotent by design.
///
/// Nor can it change what the partner may call: that is configured once per
/// partner on `PUT /api/admin/partners/{consumer_id}/models` and applies to the
/// partner's one active key. A per-key allow-list was the second source of
/// truth this surface used to carry, and it is gone.
async fn update_key(
    State(state): State<Arc<AppState>>,
    ManagerOnly(_): ManagerOnly,
    Path(id): Path<i64>,
    Json(req): Json<UpdateKeyRequest>,
) -> Result<Response, AdminError> {
    let store = state.api_keys.clone();
    let row = tokio::task::spawn_blocking(move || store.update(id, req.name.as_deref()))
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
}
