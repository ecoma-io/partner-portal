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
/// Every route requires a valid credential. A key scopes its query to the key's
/// consumer; a manager password (when configured — docs/adr/0008, ADR 0013)
/// sees every consumer, and the `consumers=` parameter only narrows that view.
/// There is no other cross-consumer view.
pub fn create_dashboard_router() -> Router<Arc<AppState>> {
    create_api_router().route("/api/dashboard/events", get(sse_handler))
}
