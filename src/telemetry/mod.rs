//! Telemetry and tracing

use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize telemetry
pub fn init_telemetry() {
    tracing_subscriber::registry()
        .with(
            EnvFilter::from_default_env()
                .add_directive("partner_portal=info".parse().unwrap())
                .add_directive("tower_http=info".parse().unwrap()),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false),
        )
        .init();
}
