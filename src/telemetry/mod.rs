//! Telemetry and tracing

use tracing_subscriber::{EnvFilter, layer::SubscriberExt, util::SubscriberInitExt};

/// Initialize telemetry.
///
/// `RUST_LOG` wins outright when it is set; the defaults apply only when it is
/// not. Building the filter with `from_default_env()` and *then* appending
/// `partner_portal=info` — the obvious-looking spelling — silently caps this
/// crate at `info` no matter what the operator asked for, because a directive
/// added later overrides an earlier one for the same target. The same trap
/// applies to `RUST_LOG=debug`, so the fallback has to be chosen before the
/// environment is consulted, not layered on top of it.
pub fn init_telemetry() {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("partner_portal=info,tower_http=info"));

    tracing_subscriber::registry()
        .with(filter)
        .with(
            tracing_subscriber::fmt::layer()
                .with_target(false)
                .with_thread_ids(false),
        )
        .init();
}
