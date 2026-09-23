//! Dashboard API for usage inspection

mod api;
pub mod sse;

pub use api::{DashboardQuery, create_dashboard_router};
pub use sse::{SseBroadcaster, sse_response};
