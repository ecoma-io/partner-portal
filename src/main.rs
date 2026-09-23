//! Partner Portal - Lightweight OpenAI-compatible reverse proxy

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use axum::{
    Router,
    body::Body,
    extract::Request,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
    routing::{any, get},
};
use parking_lot::RwLock;
use tokio::signal;
use tower_http::{
    cors::{Any, CorsLayer},
    limit::RequestBodyLimitLayer,
    sensitive_headers::SetSensitiveHeadersLayer,
    trace::TraceLayer,
};
use tracing::info;

use partner_portal::{
    admin::create_admin_router,
    auth::Authenticated,
    config::{ConfigLoader, ConfigSnapshot, HotReloader},
    dashboard::{SseBroadcaster, create_dashboard_router},
    ledger::{LedgerPool, LedgerWriter, LedgerWriterConfig},
    proxy::client::ProxyClient,
    proxy::handler::AppState,
    telemetry,
};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // Initialize telemetry
    telemetry::init_telemetry();

    // Load configuration
    let config_path = PathBuf::from("config.yaml");
    let config = ConfigLoader::from_file(&config_path)
        .map_err(|e| anyhow::anyhow!("Failed to load config: {}", e))?;

    info!(path = %config_path.display(), "Configuration loaded");

    // Initialize database
    let db_path = config.database.path.clone();
    let pool = Arc::new(LedgerPool::new(PathBuf::from(&db_path))?);
    info!(path = %db_path, "Database initialized");

    // Initialize ledger writer
    let writer_config = LedgerWriterConfig::from(config.database.clone());
    let ledger = Arc::new(LedgerWriter::new(pool.writer(), writer_config));

    // Initialize hot reloader
    let snapshot = ConfigSnapshot::new(config.clone());
    let config_guard = Arc::new(RwLock::new(snapshot));
    let reloader = HotReloader::new(config_path, config.clone());
    reloader.start();

    // Initialize proxy client
    let client = Arc::new(ProxyClient::new(&config.upstream));

    // Initialize SSE broadcaster
    let broadcaster = Arc::new(SseBroadcaster::new(&PathBuf::from(&db_path), 500)?);
    broadcaster.start();

    // Build app state
    let app_state = Arc::new(AppState {
        config: config_guard.clone(),
        client,
        ledger: ledger.clone(),
        pool: pool.clone(),
        broadcaster: broadcaster.clone(),
    });

    // Build router
    let app = Router::new()
        // Admin routes (no auth)
        .merge(create_admin_router())
        // Dashboard routes (auth required via extractor in handlers)
        .merge(create_dashboard_router())
        // Proxy routes (auth required)
        .route("/v1/chat/completions", any(proxy_handler))
        .route("/v1/responses", any(proxy_handler))
        .route("/v1/models", any(proxy_handler))
        .route("/*path", any(catch_all_handler))
        .route("/api/dashboard/events", get(sse_handler))
        // Layers
        .layer(SetSensitiveHeadersLayer::new(std::iter::once(
            header::AUTHORIZATION,
        )))
        .layer(RequestBodyLimitLayer::new(config.server.max_body_size))
        .layer(TraceLayer::new_for_http())
        .layer(
            CorsLayer::new()
                .allow_origin(Any)
                .allow_methods(Any)
                .allow_headers(Any),
        )
        .with_state(app_state.clone());

    // Bind address
    let addr: SocketAddr = config.server.listen.parse()?;
    let listener = tokio::net::TcpListener::bind(addr).await?;
    info!(addr = %addr, "Server starting");

    // Run with graceful shutdown
    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    info!("Server shutdown complete");
    Ok(())
}

/// SSE handler for realtime dashboard updates
async fn sse_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
) -> impl IntoResponse {
    partner_portal::dashboard::sse::sse_response(
        state.broadcaster.clone(),
        consumer.consumer_id().to_string(),
    )
}

/// Proxy handler for all /v1/* routes
async fn proxy_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    request: Request,
) -> Response {
    let method = request.method().clone();
    let path = request.uri().path().to_string();
    let headers = request.headers().clone();

    // Collect body
    let body = match axum::body::to_bytes(request.into_body(), 10 * 1024 * 1024).await {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, "Failed to read request body");
            return error_response(
                StatusCode::BAD_REQUEST,
                "Failed to read request body",
                "invalid_request_error",
            );
        }
    };

    partner_portal::proxy::handler::handle_proxy(state, consumer, method, path, headers, body).await
}

/// Catch-all handler for unmatched routes
async fn catch_all_handler() -> Response {
    error_response(StatusCode::NOT_FOUND, "Not Found", "invalid_request_error")
}

/// Standard error response
fn error_response(status: StatusCode, message: &str, error_type: &str) -> Response {
    let body = serde_json::json!({
        "error": {
            "message": message,
            "type": error_type,
        }
    });

    Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_string(&body).unwrap()))
        .unwrap()
}

/// Graceful shutdown signal
async fn shutdown_signal() {
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to install Ctrl+C handler");
    };

    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to install signal handler")
            .recv()
            .await;
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {},
        _ = terminate => {},
    }

    info!("Shutdown signal received");
}
