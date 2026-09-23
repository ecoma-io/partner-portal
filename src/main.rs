//! Partner Portal — a lightweight OpenAI-compatible reverse proxy.
//!
//! This binary is the composition root: it starts the ledger, the dashboard's
//! change poller and the HTTP server, and it owns the shutdown sequence. The
//! sequence is the part worth reading closely, because it is where the
//! accounting guarantees either hold or quietly fail:
//!
//! ```text
//!   SIGTERM / SIGINT
//!     -> shutting_down = true          (readiness fails; the balancer stops routing)
//!     -> wait the grace period         (time for the balancer to notice)
//!     -> stop accepting connections    (axum drains requests already in flight)
//!     -> drain the metering pipeline   (flush and COMMIT every queued record)
//!     -> await detached finalizers     (stream drop guards that could not await)
//!     -> close the writer task, drop connections
//! ```
//!
//! The ordering is not stylistic. The metering producer is stopped before the
//! consumer, because a writer closed underneath a live producer turns a
//! completed request into an unrecorded one — the single failure this product
//! exists to avoid.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{HeaderValue, StatusCode, header},
    response::Response,
    routing::any,
};
use tokio::signal;
use tokio::sync::watch;
use tower_http::{
    cors::{AllowOrigin, Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    sensitive_headers::SetSensitiveHeadersLayer,
    trace::TraceLayer,
};
use tracing::{error, info, warn};

use partner_portal::{
    admin::create_admin_router,
    auth::Authenticated,
    config::{ConfigLoader, HotReloader},
    dashboard::{SseBroadcaster, create_dashboard_router},
    ledger::{LedgerPool, LedgerWriter, LedgerWriterConfig, recover_in_flight, retention},
    proxy::client::ProxyClient,
    proxy::handler::AppState,
    telemetry, web,
};

/// Environment variable that overrides the configuration path.
const CONFIG_ENV: &str = "PARTNER_PORTAL_CONFIG";

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_telemetry();

    let config_path = config_path();
    let config = ConfigLoader::from_file(&config_path)
        .map_err(|e| anyhow::anyhow!("failed to load {}: {e}", config_path.display()))?;

    info!(
        path = %config_path.display(),
        keys = config.keys.len(),
        upstream = %config.upstream.base_url,
        "configuration loaded"
    );

    // --- Ledger -------------------------------------------------------------

    let db_path = PathBuf::from(&config.database.path);
    let pool = Arc::new(
        LedgerPool::new(db_path.clone())
            .map_err(|e| anyhow::anyhow!("failed to open ledger at {}: {e}", db_path.display()))?,
    );

    // Crash recovery runs before the writer starts and before the listener opens.
    // Any request that was `in_flight` when the previous process died will never
    // reach a terminal state on its own, and a record that stays `in_flight`
    // forever is indistinguishable from a request still running. Recovery resolves
    // them here, once, so `in_flight` means what it says from the first request on.
    match pool.write(recover_in_flight) {
        Ok(report) if report.recovered > 0 => {
            warn!(
                recovered = report.recovered,
                "resolved requests interrupted by a previous shutdown"
            );
        }
        Ok(_) => {}
        Err(e) => {
            // Recovery is not best-effort: starting with an unknown set of
            // orphaned records would silently corrupt every usage view until the
            // next restart. Refuse to start.
            return Err(anyhow::anyhow!("crash recovery failed: {e}"));
        }
    }

    let writer_config = LedgerWriterConfig::from(config.database.clone());
    info!(
        queue_size = writer_config.queue_size,
        batch_size = writer_config.batch_size,
        batch_timeout_ms = writer_config.batch_timeout_ms,
        "metering pipeline configured"
    );
    let ledger = Arc::new(LedgerWriter::new(pool.writer(), writer_config));

    // --- Configuration hot reload -------------------------------------------

    let reloader = HotReloader::new(config_path.clone(), config.clone());
    reloader.start();

    // --- Application state --------------------------------------------------
    //
    // `handle()` is the lock the watcher swaps. Taking it here is what makes hot
    // reload observable: constructing a separate RwLock would swap a snapshot the
    // server never reads, and every request would keep using the startup config.

    let client = Arc::new(ProxyClient::new(&config.upstream));
    let broadcaster = Arc::new(
        SseBroadcaster::new(&db_path, config.server.sse_poll_interval_ms)
            .map_err(|e| anyhow::anyhow!("failed to open the dashboard change poller: {e}"))?,
    );
    broadcaster.start();

    let shutting_down = Arc::new(AtomicBool::new(false));

    let state = Arc::new(AppState {
        config: reloader.handle(),
        client,
        ledger: ledger.clone(),
        pool: pool.clone(),
        broadcaster: broadcaster.clone(),
        shutting_down: shutting_down.clone(),
    });

    // --- Retention ----------------------------------------------------------

    let (retention_stop_tx, retention_stop_rx) = watch::channel(());
    spawn_retention(pool.clone(), config.database.clone(), retention_stop_rx);

    // --- Router -------------------------------------------------------------
    //
    // Order matters. The proxy and dashboard routes are matched first; `web`
    // owns only the fallback, so it can serve the embedded SPA without ever
    // shadowing a 404 from `/v1/*` or `/api/*`.

    let app = Router::new()
        .merge(create_admin_router())
        .merge(create_dashboard_router())
        .route("/v1/chat/completions", any(proxy_handler))
        .route("/v1/responses", any(proxy_handler))
        .route("/v1/models", any(proxy_handler))
        .fallback(web::fallback);

    // `cors_allow_origins` is read here and fixed for the process lifetime: it
    // shapes the router, which is built once. Everything a reload can change is
    // read per request from the config snapshot instead.
    let app = match cors_layer(&config.server.cors_allow_origins) {
        Some(cors) => app.layer(cors),
        None => app,
    };

    let app = app
        .layer(RequestBodyLimitLayer::new(config.server.max_body_size))
        .layer(TraceLayer::new_for_http())
        // Outermost, so the credential is marked unrenderable before anything
        // inside the stack has a chance to log it.
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(
            header::AUTHORIZATION,
        )))
        .with_state(state.clone());

    // --- Serve --------------------------------------------------------------

    let addr: SocketAddr =
        config.server.listen.parse().map_err(|e| {
            anyhow::anyhow!("invalid server.listen {:?}: {e}", config.server.listen)
        })?;
    let listener = tokio::net::TcpListener::bind(addr)
        .await
        .map_err(|e| anyhow::anyhow!("failed to bind {addr}: {e}"))?;

    info!(addr = %addr, "listening");

    let grace = if config.server.graceful_shutdown {
        Duration::from_secs(config.server.shutdown_grace_secs)
    } else {
        Duration::ZERO
    };

    let serve_result = axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal(state.clone(), grace))
        .await;

    // --- Drain --------------------------------------------------------------
    //
    // Reached once the listener has stopped and every in-flight request has
    // finished. What remains is metering work: queued records and the detached
    // finalizers written by stream drop guards, which cannot await.

    info!("listener stopped; draining the metering pipeline");
    retention_stop_tx.send(()).ok();
    ledger.shutdown().await;
    info!(
        committed = ledger.committed_total(),
        "metering pipeline drained and committed"
    );

    // Only now is it safe for the database to close. Dropping the pool while the
    // writer still had queued records would discard accepted usage.
    drop(state);

    if let Err(e) = serve_result {
        return Err(anyhow::anyhow!("server error: {e}"));
    }

    info!("shutdown complete");
    Ok(())
}

/// Configuration path: `PARTNER_PORTAL_CONFIG`, else `config.yaml`.
fn config_path() -> PathBuf {
    match std::env::var(CONFIG_ENV) {
        Ok(path) => PathBuf::from(path),
        Err(_) => PathBuf::from("config.yaml"),
    }
}

/// Build the CORS layer, or `None` when no browser origins are allowed.
///
/// An invalid origin is a startup failure rather than a silently dropped entry:
/// a typo in this list is otherwise discovered as a browser error in production.
fn cors_layer(origins: &[String]) -> Option<CorsLayer> {
    if origins.is_empty() {
        return None;
    }

    let mut allowed = Vec::with_capacity(origins.len());
    for origin in origins {
        match HeaderValue::from_str(origin) {
            Ok(value) => allowed.push(value),
            Err(_) => {
                error!(origin = %origin, "ignoring invalid CORS origin");
            }
        }
    }

    if allowed.is_empty() {
        return None;
    }

    info!(count = allowed.len(), "browser origins allowed");
    Some(
        CorsLayer::new()
            .allow_origin(AllowOrigin::list(allowed))
            .allow_methods(Any)
            .allow_headers(Any),
    )
}

/// Run retention on a timer for the life of the process.
///
/// Deletion runs off the async runtime because it is blocking I/O against
/// SQLite, and it releases the write lock between slices so a sweep never stalls
/// the metering pipeline.
fn spawn_retention(
    pool: Arc<LedgerPool>,
    db: partner_portal::config::DatabaseConfig,
    mut stop_rx: watch::Receiver<()>,
) {
    let interval = Duration::from_secs(db.retention_interval_secs.max(60));

    tokio::spawn(async move {
        loop {
            let run = tokio::task::spawn_blocking({
                let pool = pool.clone();
                let db = db.clone();
                move || {
                    retention::run_retention(
                        &pool.writer(),
                        db.retention_days,
                        db.retention_batch_size,
                        retention::DEFAULT_MAX_BATCHES,
                    )
                }
            });

            match run.await {
                Ok(Ok(stats)) => {
                    if stats.total_deleted() > 0 || stats.hit_budget {
                        retention::report(&stats, db.retention_days);
                    } else {
                        tracing::debug!("retention found nothing to delete");
                    }
                }
                Ok(Err(e)) => warn!(error = %e, "retention pass failed; will retry next interval"),
                Err(e) => warn!(error = %e, "retention task panicked"),
            }

            tokio::select! {
                _ = stop_rx.changed() => break,
                _ = tokio::time::sleep(interval) => {}
            }
        }
    });
}

/// Resolve when a termination signal arrives, after marking the process
/// unready and giving the load balancer time to notice.
async fn shutdown_signal(state: Arc<AppState>, grace: Duration) {
    let ctrl_c = async {
        if let Err(e) = signal::ctrl_c().await {
            error!(error = %e, "failed to listen for Ctrl+C");
            std::future::pending::<()>().await;
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match signal::unix::signal(signal::unix::SignalKind::terminate()) {
            Ok(mut stream) => {
                stream.recv().await;
            }
            Err(e) => {
                error!(error = %e, "failed to listen for SIGTERM");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => info!("received Ctrl+C"),
        _ = terminate => info!("received SIGTERM"),
    }

    // Readiness must fail before anything starts shedding load. An instance that
    // closes its listener at the same moment its readiness flips produces
    // connection errors at the balancer; this window is what prevents that.
    state.shutting_down.store(true, Ordering::Release);
    info!(
        grace_secs = grace.as_secs(),
        "shutting down: readiness now fails, waiting for the balancer to notice"
    );

    if !grace.is_zero() {
        tokio::time::sleep(grace).await;
    }
}

/// Proxy handler for the `/v1/*` routes.
async fn proxy_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    request: Request,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let headers = request.headers().clone();

    // The body is read in full, bounded by `server.max_body_size` which
    // `RequestBodyLimitLayer` has already enforced. Inference requests are small
    // JSON documents; the response, which is where the size actually is, is
    // streamed and never buffered.
    let limit = state.max_body_size();
    let body = match axum::body::to_bytes(request.into_body(), limit).await {
        Ok(body) => body,
        Err(e) => {
            warn!(error = %e, %path, "failed to read the request body");
            return error_response(
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body could not be read",
                "invalid_request_error",
            );
        }
    };

    partner_portal::proxy::handler::handle_proxy(state, consumer, method, path, headers, body).await
}

/// Standard OpenAI-style error response.
fn error_response(status: StatusCode, message: &str, error_type: &str) -> Response {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": error_type,
        }
    });

    let json = serde_json::to_string(&body).unwrap_or_else(|_| r#"{"error":{}}"#.to_string());

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(json))
        .unwrap_or_else(|_| Response::new(Body::empty()))
}
