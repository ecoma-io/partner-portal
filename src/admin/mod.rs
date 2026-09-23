//! Operational endpoints: liveness, readiness, version.
//!
//! These are deliberately unauthenticated so an orchestrator can probe them
//! without holding a credential. They expose no usage data — only process state
//! and build identity — so there is nothing here for an unauthenticated caller
//! to learn beyond whether the service is up.

use axum::{Json, Router, extract::State, http::StatusCode, response::IntoResponse, routing::get};
use serde::Serialize;
use std::sync::Arc;

use crate::proxy::handler::AppState;

/// Admin router. Mount at the root.
pub fn create_admin_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/healthz", get(healthz))
        .route("/readyz", get(readyz))
        .route("/version", get(version))
}

#[derive(Serialize)]
pub struct HealthResponse {
    pub status: &'static str,
}

/// `GET /healthz` — liveness.
///
/// Only answers "is this process alive and able to serve"? It deliberately does
/// **not** consult the database: a liveness probe that fails on a transient
/// dependency problem causes a restart loop, and a restart is not what fixes a
/// slow disk. Readiness carries that judgement instead.
async fn healthz() -> impl IntoResponse {
    Json(HealthResponse { status: "ok" })
}

#[derive(Serialize)]
pub struct ReadyResponse {
    pub ready: bool,
    /// The ledger writer is accepting and committing records.
    pub ledger_ready: bool,
    /// Shutdown has begun; new work should go elsewhere.
    pub shutting_down: bool,
    /// Requests currently queued for the metering writer.
    pub ledger_queue_depth: usize,
    /// Records durably committed since process start.
    pub ledger_committed: u64,
}

/// `GET /readyz` — readiness.
///
/// Fails **before** the instance starts rejecting requests: readiness tracks how
/// full the metering queue is, so a load balancer stops sending traffic while the
/// instance can still serve it correctly. This is what makes a rolling update
/// safe — the outgoing instance leaves rotation before it starts shedding work.
///
/// Returns 503, not 200-with-a-flag, because that is what orchestrators act on.
async fn readyz(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let shutting_down = state
        .shutting_down
        .load(std::sync::atomic::Ordering::Acquire);
    let ledger_ready = state.ledger.is_ready();
    let ready = !shutting_down && ledger_ready;

    let body = ReadyResponse {
        ready,
        ledger_ready,
        shutting_down,
        ledger_queue_depth: state.ledger.queue_depth(),
        ledger_committed: state.ledger.committed_total(),
    };

    let status = if ready {
        StatusCode::OK
    } else {
        StatusCode::SERVICE_UNAVAILABLE
    };

    (status, Json(body))
}

#[derive(Serialize)]
pub struct VersionResponse {
    pub version: &'static str,
    pub commit: &'static str,
    pub build_time: &'static str,
    pub schema_version: u32,
}

/// `GET /version` — build identity, for confirming what a rolling update rolled.
async fn version() -> impl IntoResponse {
    Json(VersionResponse {
        version: env!("CARGO_PKG_VERSION"),
        commit: option_env!("GIT_COMMIT").unwrap_or("unknown"),
        build_time: option_env!("BUILD_TIME").unwrap_or("unknown"),
        schema_version: crate::ledger::SCHEMA_VERSION,
    })
}
