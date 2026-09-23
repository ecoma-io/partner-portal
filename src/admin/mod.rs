//! Admin and health endpoints

use axum::{Json, Router, http::StatusCode, response::IntoResponse, routing::get};
use serde::Serialize;
use std::sync::Arc;

use crate::proxy::handler::AppState;

/// Admin router
pub fn create_admin_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/version", get(version))
}

/// Health check response
#[derive(Serialize)]
pub struct HealthResponse {
    pub status: String,
}

/// GET /healthz - Liveness probe
async fn healthz() -> impl IntoResponse {
    Json(HealthResponse {
        status: "ok".to_string(),
    })
}

/// Readiness check response
#[derive(Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
    pub ledger_ready: bool,
}

/// GET /readyz - Readiness probe
async fn readyz(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
) -> impl IntoResponse {
    let ledger_ready = state.ledger.is_ready();
    let ready = ledger_ready;

    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (
        status,
        Json(ReadyResponse {
            ready,
            ledger_ready,
        }),
    )
}

/// Version response
#[derive(Serialize)]
pub struct VersionResponse {
    pub version: String,
    pub commit: String,
    pub build_time: String,
}

/// GET /version
async fn version() -> impl IntoResponse {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION").to_string(),
        commit: option_env!("GIT_COMMIT").unwrap_or("unknown").to_string(),
        build_time: option_env!("BUILD_TIME").unwrap_or("unknown").to_string(),
    })
}
