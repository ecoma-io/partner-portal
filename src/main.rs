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
//!     -> close the dashboard streams    (SSE bodies never end on their own)
//!     -> stop accepting connections    (axum drains requests already in flight)
//!     -> drain the metering pipeline   (flush and COMMIT every queued record)
//!     -> await detached finalizers     (stream drop guards that could not await)
//!     -> release the instance           (registration + advisory lock)
//!     -> close the writer task, drop connections
//! ```
//!
//! The ordering is not stylistic. The metering producer is stopped before the
//! consumer, because a writer closed underneath a live producer turns a
//! completed request into an unrecorded one — the single failure this product
//! exists to avoid.
//!
//! Two bounds exist because the drain must *always* run. The listener stops on
//! its own only when every response body has finished, and an SSE stream never
//! finishes, so the dashboard streams are closed explicitly. That still leaves a
//! client that stops reading a proxied stream, so the serve future is bounded in
//! time as well — but only *after* the signal, never while the server is
//! supposed to be serving. Without these, a single stalled connection holds the
//! process past its termination deadline, the orchestrator sends SIGKILL, and
//! every queued record dies with it.

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use axum::{
    Router,
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
    ledger::{
        InstanceGuard, LedgerPool, LedgerWriter, LedgerWriterConfig, RecoveryContext,
        recover_in_flight, retention,
    },
    proxy::client::ProxyClient,
    proxy::handler::{AppState, error_response},
    telemetry, web,
};

/// Environment variable that overrides the configuration path.
const CONFIG_ENV: &str = "PARTNER_PORTAL_CONFIG";

/// How long shutdown waits for in-flight requests **after the shutdown signal**,
/// before it gives up on them and runs the metering drain anyway.
///
/// A floor rather than a fixed value: an operator who configured a longer grace
/// period is asking for more patience, not less. Requests abandoned at this bound
/// stay `in_flight` and are resolved to `interrupted` by the next start's
/// recovery — recorded as failures, never silently dropped.
///
/// The bound governs the *drain*, and nothing else: it must never be applied to
/// a server that has not been asked to stop. See the `select!` in `run` for what
/// applying it to the whole serve future costs.
const MIN_DRAIN_BOUND: Duration = Duration::from_secs(30);

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    telemetry::init_telemetry();

    let config_path = config_path();
    let config = ConfigLoader::from_file(&config_path)
        .map_err(|e| anyhow::anyhow!("failed to load {}: {e}", config_path.display()))?;

    info!(
        path = %config_path.display(),
        keys = config.keys.len(),
        upstream = %config.upstream.redacted_base_url(),
        "configuration loaded"
    );

    // --- Ledger -------------------------------------------------------------

    let db_path = PathBuf::from(&config.database.path);
    let pool = Arc::new(
        LedgerPool::new(db_path.clone())
            .map_err(|e| anyhow::anyhow!("failed to open ledger at {}: {e}", db_path.display()))?,
    );

    // --- Instance ownership -------------------------------------------------
    //
    // Claim an identity *before* recovery runs, and hold it for the life of the
    // process. The claim is an advisory lock beside the database, which the
    // kernel releases even on SIGKILL, so another instance — a rolling update's
    // sibling sharing this file — can tell that our rows are ours and that we
    // are still here. Without it, a starting instance has no way to distinguish
    // a stranded request from one its sibling is still serving.
    let instance = {
        let writer_conn = pool.writer();
        let conn = writer_conn.lock();
        InstanceGuard::acquire(&conn, &db_path).map_err(|e| {
            anyhow::anyhow!(
                "failed to claim an instance identity beside {}: {e}",
                db_path.display()
            )
        })?
    };
    info!(
        instance_id = %instance.id(),
        lock = %instance.lock_path().display(),
        "instance identity claimed"
    );

    // Crash recovery runs before the writer starts and before the listener opens.
    // Any request that was `in_flight` when its owner died will never reach a
    // terminal state on its own, and a record that stays `in_flight` forever is
    // indistinguishable from a request still running. Recovery resolves them
    // here, once, so `in_flight` means what it says from the first request on —
    // and it resolves *only* rows whose owner is gone, so a live sibling's
    // in-flight requests are left alone.
    let recovery_ctx = RecoveryContext::new(instance.id().to_string(), &db_path);
    match pool.write(|conn| recover_in_flight(conn, &recovery_ctx)) {
        Ok(report) if !report.is_empty() => {
            warn!(
                recovered = report.recovered,
                instances_swept = report.instances_swept,
                "resolved requests stranded by a process that is gone"
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

    let writer_config = LedgerWriterConfig {
        instance_id: Some(instance.id().to_string()),
        ..LedgerWriterConfig::from(config.database.clone())
    };
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

    // --- Recovery sweeper ---------------------------------------------------
    //
    // Recovery at startup is not enough. A row can become stranded *later*: a
    // second instance that was alive when this one started can die at any time,
    // and a row owned by an unprobeable owner only becomes recoverable once it
    // is older than the grace period. Neither is visible to a pass that ran
    // once, at boot. The sweep is idempotent and probes liveness before touching
    // anything, so running it against a live sibling is safe by construction.
    let (sweeper_stop_tx, sweeper_stop_rx) = watch::channel(());
    spawn_recovery_sweeper(
        pool.clone(),
        instance.id().to_string(),
        db_path.clone(),
        sweeper_stop_rx,
    );

    // The raw-vs-rollup audit is a full scan of the ledger, so it runs once, off
    // the startup path, after the listener is already serving.
    spawn_consistency_audit(pool.clone());

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

    // The drain bound is armed by the shutdown signal, not by startup.
    //
    // `tokio::time::timeout(drain_bound, serve)` is the obvious spelling and it
    // is wrong: it starts counting the moment the process begins listening, so
    // an idle server abandons every in-flight request and exits — cleanly,
    // status 0 — thirty seconds after it started, having never been asked to
    // stop. Under `restart: unless-stopped` that is a crash loop with a thirty
    // second period. Observed, not deduced: a container built from that code
    // exited 0 at 30.00 s uptime with no signal sent, repeatedly, and logged
    // "in-flight requests did not finish within the drain bound" for a server
    // that had nothing in flight.
    let (shutdown_started_tx, shutdown_started_rx) = tokio::sync::oneshot::channel();
    let shutdown_state = state.clone();
    let shutdown = async move {
        shutdown_signal(shutdown_state, grace).await;
        // Failing to send means nobody is waiting — the server has already
        // stopped, which is not an error.
        let _ = shutdown_started_tx.send(());
    };

    let serve = axum::serve(listener, app).with_graceful_shutdown(shutdown);

    let drain_bound = grace.max(MIN_DRAIN_BOUND);
    let mut drain_expired = false;
    // `biased` so the server is always polled first: if it stops at the same
    // instant the bound expires, the honest answer is that it stopped.
    let serve_result = tokio::select! {
        biased;
        result = serve => result,
        () = async {
            // Nothing to bound until shutdown has begun. This arm is what makes
            // the two phases — serving, then draining — separate.
            let _ = shutdown_started_rx.await;
            tokio::time::sleep(drain_bound).await;
        } => {
            drain_expired = true;
            Ok(())
        }
    };

    if drain_expired {
        // A connection outlived the bound — typically a client that stopped
        // reading a proxied stream. Waiting longer would risk the process being
        // killed before it commits what it already accepted, so the remaining
        // requests are abandoned here: their records are still `in_flight`, and
        // the next start's recovery resolves them to `interrupted`. Recorded as
        // failures, never lost.
        error!(
            bound_secs = drain_bound.as_secs(),
            "in-flight requests did not finish within the drain bound; \
             abandoning them to be resolved by recovery on the next start"
        );
    }

    // --- Drain --------------------------------------------------------------
    //
    // Reached once the listener has stopped and every in-flight request has
    // finished. What remains is metering work: queued records and the detached
    // finalizers written by stream drop guards, which cannot await.

    info!("listener stopped; draining the metering pipeline");
    retention_stop_tx.send(()).ok();
    sweeper_stop_tx.send(()).ok();
    ledger.shutdown().await;
    info!(
        committed = ledger.committed_total(),
        "metering pipeline drained and committed"
    );

    // --- Release the instance -----------------------------------------------
    //
    // After the drain, so no `in_flight` row this instance owns is still
    // expected to be finalized — and before the database closes, so the
    // registration and the lock file are gone by the time the next instance
    // starts. That ordering is what makes a graceful restart leave nothing to
    // recover: the successor finds no dead instance, because there is none.
    {
        let writer_conn = pool.writer();
        let conn = writer_conn.lock();
        instance.release(&conn);
    }
    drop(instance);

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

/// Re-run recovery on a timer for the life of the process.
///
/// The interval is the same as the grace period that governs unprobeable owners,
/// which keeps the worst-case age of a stranded row at two grace periods rather
/// than an interval plus a grace. Lock files of instances found dead are removed
/// by recovery itself, after its transaction commits.
fn spawn_recovery_sweeper(
    pool: Arc<LedgerPool>,
    instance_id: String,
    db_path: PathBuf,
    mut stop_rx: watch::Receiver<()>,
) {
    let interval = partner_portal::ledger::instance::DEFAULT_UNKNOWN_OWNER_GRACE;

    tokio::spawn(async move {
        loop {
            tokio::select! {
                _ = stop_rx.changed() => return,
                _ = tokio::time::sleep(interval) => {}
            }

            let run = tokio::task::spawn_blocking({
                let pool = pool.clone();
                let db_path = db_path.clone();
                let instance_id = instance_id.clone();
                move || {
                    let ctx = RecoveryContext::new(instance_id, &db_path);
                    pool.write(|conn| recover_in_flight(conn, &ctx))
                }
            });

            match run.await {
                Ok(Ok(report)) if report.recovered > 0 || report.instances_swept > 0 => warn!(
                    recovered = report.recovered,
                    instances_swept = report.instances_swept,
                    "sweep resolved requests stranded by a process that is gone"
                ),
                Ok(Ok(_)) => tracing::debug!("sweep found nothing stranded"),
                Ok(Err(e)) => warn!(error = %e, "recovery sweep failed; will retry next interval"),
                Err(e) => warn!(error = %e, "recovery sweep task panicked"),
            }
        }
    });
}

/// Check the raw ledger against the hourly rollup once, in the background.
///
/// A diagnostic, not a repair: recovery and retention both maintain the
/// invariant, and a drift here means one of them failed, which is worth a loud
/// log line and nothing more. Deferring it off the startup path keeps a full
/// scan of a retention-limited ledger from delaying a listener that is already
/// correct.
fn spawn_consistency_audit(pool: Arc<LedgerPool>) {
    tokio::task::spawn_blocking(move || {
        if let Err(e) = pool.read(|conn| {
            partner_portal::ledger::recovery::audit_consistency(conn);
            Ok(())
        }) {
            warn!(error = %e, "could not open a connection for the consistency audit");
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

    // The listener stops accepting the moment this future resolves, and axum
    // then waits for open response bodies to finish. An SSE body never finishes
    // on its own, so it is ended here — exactly when the instance stops serving —
    // rather than at the very end, when the process is already tearing down.
    state.broadcaster.shutdown();
    info!("dashboard streams closed; waiting for in-flight requests");
}

/// Proxy handler for the `/v1/*` routes.
async fn proxy_handler(
    axum::extract::State(state): axum::extract::State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    request: Request,
) -> Response {
    let method = request.method().clone();
    // Path *and* query. `uri().path()` alone drops the query string, which
    // silently changes the request the upstream sees: `?beta=true`, a cache
    // buster, or any provider-specific parameter would be forwarded as if it had
    // never been sent. The router matches on the path only, so this is safe to
    // reconstruct — and it is the same string used for endpoint classification,
    // which parses the path component and ignores what follows it.
    let path = request
        .uri()
        .path_and_query()
        .map(|pq| pq.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());
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
            // The request never reaches `handle_proxy`, so no identity exists
            // yet — mint one here rather than answer an untraceable error.
            return error_response(
                &uuid::Uuid::now_v7().to_string(),
                StatusCode::PAYLOAD_TOO_LARGE,
                "Request body could not be read",
                "invalid_request_error",
            );
        }
    };

    partner_portal::proxy::handler::handle_proxy(state, consumer, method, path, headers, body).await
}
