//! Usage dashboard: REST API plus a realtime invalidation stream.

mod api;
pub mod sse;

pub use api::{DashboardQuery, create_api_router};
pub use sse::{SseBroadcaster, sse_handler};

use axum::{Router, routing::get};
use std::sync::Arc;

use crate::proxy::handler::AppState;

/// Dashboard router: the query API plus the SSE invalidation stream.
///
/// Every route requires an authenticated key, and each handler scopes its query
/// to that key's consumer. There is no administrative or cross-consumer view.
pub fn create_dashboard_router() -> Router<Arc<AppState>> {
    create_api_router().route("/api/dashboard/events", get(sse_handler))
}
