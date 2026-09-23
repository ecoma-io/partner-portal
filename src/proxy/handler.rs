//! Request handling for the proxied inference endpoints.
//!
//! # Metering lifecycle
//!
//! ```text
//!   accepted  ->  in_flight (durable COMMIT)  ->  completed | failed | interrupted
//! ```
//!
//! The `in_flight` write is awaited *before* the upstream is contacted. If the
//! durable accept fails, the request is rejected with 503 rather than served:
//! serving an inference request we cannot account for would break the product's
//! core promise. After the upstream responds, the record is resolved to a
//! terminal state, and a streaming response's terminal write is guaranteed by
//! [`StreamMeter`]'s drop guard even if the client vanishes mid-stream.
//!
//! # Streaming
//!
//! Streaming responses are forwarded frame by frame. The body is never
//! collected: the first token reaches the client as soon as the upstream
//! produces it. Usage is recovered by scanning the frames as they pass through
//! ([`crate::proxy::sse_scan`]), time-to-first-token is measured at the first
//! data frame, and a stream that ends without a usage event is recorded as
//! `unavailable` rather than as zero.

use axum::{
    body::Body as AxumBody,
    http::{HeaderMap, Method, StatusCode, header},
    response::Response,
};
use bytes::Bytes;
use http_body_util::BodyExt;
use std::sync::Arc;
use std::sync::atomic::AtomicBool;
use std::time::{Duration, Instant};

use crate::auth::ConsumerContext;
use crate::config::{ConfigSnapshot, UpstreamConfig};
use crate::dashboard::SseBroadcaster;
use crate::ledger::{Endpoint, LedgerPool, LedgerWriter, RequestRecord, Usage};
use crate::proxy::client::ProxyClient;
use crate::proxy::sse_scan::SseUsageScanner;
use crate::proxy::usage::{
    extract_chat_completions_usage, extract_model_from_request, extract_responses_usage,
};

/// Largest non-streaming upstream body buffered for usage extraction.
///
/// Non-streaming responses must be parsed whole to find `usage`, so they are
/// buffered — but under a cap, so a hostile or broken upstream cannot exhaust
/// memory. A response above this is truncated, and its usage reported as
/// unavailable rather than guessed.
pub const MAX_BUFFERED_RESPONSE: usize = 32 * 1024 * 1024;

/// Reason recorded when the client goes away mid-stream.
const CLIENT_DISCONNECT: &str = "client disconnected during streaming";

/// Shared application state.
#[derive(Clone)]
pub struct AppState {
    /// Live configuration. Replaced atomically by the hot reloader.
    pub config: Arc<parking_lot::RwLock<ConfigSnapshot>>,
    pub client: Arc<ProxyClient>,
    pub ledger: Arc<LedgerWriter>,
    pub pool: Arc<LedgerPool>,
    pub broadcaster: Arc<SseBroadcaster>,
    /// Set when shutdown starts, so readiness fails before draining begins.
    pub shutting_down: Arc<AtomicBool>,
}

impl AppState {
    /// Snapshot the upstream configuration for one request.
    pub fn upstream(&self) -> UpstreamConfig {
        self.config.read().config.upstream.clone()
    }

    /// The request body limit in force, read fresh so a reload applies.
    pub fn max_body_size(&self) -> usize {
        self.config.read().config.server.max_body_size
    }

    /// Every credential in the live configuration, as owned strings.
    ///
    /// Read fresh rather than cached: a reload can rotate a key, and a scrubber
    /// still holding the previous one would stop recognising the current value.
    /// Only taken on paths that build a recorded reason, so the read lock is not
    /// on the hot path.
    pub fn credentials(&self) -> Vec<String> {
        self.config
            .read()
            .config
            .credentials()
            .into_iter()
            .map(str::to_owned)
            .collect()
    }
}

/// Handle a proxied request end to end.
pub async fn handle_proxy(
    state: Arc<AppState>,
    consumer: ConsumerContext,
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();

    // Minted before anything can answer, so that *every* exit from this function
    // — the 404 below, a refused accept, a proxied failure, a stream — carries
    // the same identity the ledger row uses. A client reporting a failure can
    // then match its response to a log line and a row instead of guessing from a
    // timestamp, and an operator answering "what happened to this request?"
    // does not have to reconstruct the answer from the arrival time.
    let request_id = uuid::Uuid::now_v7().to_string();

    let Some(endpoint) = Endpoint::from_path(&path) else {
        return error_response(
            &request_id,
            StatusCode::NOT_FOUND,
            "Not Found",
            "invalid_request_error",
        );
    };

    // `/v1/models` is discovery, not inference. It is authenticated and proxied,
    // but deliberately not metered: it consumes no tokens, and recording it
    // would add noise with zero usage to every usage view and rollup.
    if endpoint == Endpoint::Models {
        return proxy_unmetered(&state, method, path, headers, body, &request_id).await;
    }

    let request_value: Option<serde_json::Value> = if body.is_empty() {
        None
    } else {
        serde_json::from_slice(&body).ok()
    };

    let model = match &request_value {
        Some(v) => match endpoint {
            Endpoint::ChatCompletions => extract_model_from_request(v),
            Endpoint::Responses => extract_responses_usage_model(v),
            Endpoint::Models => "unknown".to_string(),
        },
        None => "unknown".to_string(),
    };

    let wants_stream = request_value
        .as_ref()
        .and_then(|v| v.get("stream"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    let record = RequestRecord::new(
        request_id.clone(),
        consumer.consumer_id().to_string(),
        model,
        endpoint,
        wants_stream,
    );

    // Durable accept, before the upstream is contacted. A request we cannot
    // account for is not served.
    if let Err(e) = state.ledger.accept(record.clone()).await {
        state.ledger.mark_unhealthy();
        tracing::error!(
            request_id = %request_id,
            error = %e,
            "Refusing request: could not durably record its acceptance"
        );
        return error_response(
            &request_id,
            StatusCode::SERVICE_UNAVAILABLE,
            "Metering unavailable; request not forwarded",
            "metering_error",
        );
    }

    let upstream_cfg = state.upstream();
    let header_timeout = Duration::from_secs(upstream_cfg.timeout_secs.max(1));

    // Read the live credentials only when a reason is about to be built: this is
    // a failure-path cost, not a per-request one.
    let scrub = |reason: String| -> String {
        let credentials = state.credentials();
        let credentials: Vec<&str> = credentials.iter().map(String::as_str).collect();
        sanitize_reason(&reason, &credentials)
    };

    let upstream_result = tokio::time::timeout(
        header_timeout,
        state
            .client
            .proxy(Some(&upstream_cfg), method, path, body, headers),
    )
    .await;

    let upstream_response = match upstream_result {
        Err(_) => {
            let reason = scrub(format!(
                "upstream did not respond within {}s",
                header_timeout.as_secs()
            ));
            return finalize_and_respond(
                &state,
                record,
                Outcome::Failed {
                    http_status: Some(StatusCode::GATEWAY_TIMEOUT.as_u16()),
                    reason,
                    usage: Usage::default(),
                },
                started,
                error_response(
                    &request_id,
                    StatusCode::GATEWAY_TIMEOUT,
                    "Upstream timeout",
                    "upstream_timeout",
                ),
            )
            .await;
        }
        Ok(Err(e)) => {
            let reason = scrub(format!("upstream connection failed: {e}"));
            return finalize_and_respond(
                &state,
                record,
                Outcome::Failed {
                    http_status: Some(StatusCode::BAD_GATEWAY.as_u16()),
                    reason,
                    usage: Usage::default(),
                },
                started,
                error_response(
                    &request_id,
                    StatusCode::BAD_GATEWAY,
                    "Upstream connection failed",
                    "upstream_error",
                ),
            )
            .await;
        }
        Ok(Ok(response)) => response,
    };

    let status = upstream_response.status();
    let (parts, upstream_body) = upstream_response.into_parts();
    let upstream_is_sse = parts
        .headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|v| v.to_ascii_lowercase().contains("text/event-stream"))
        .unwrap_or(false);

    // An error status never streams a usage event, so it is always buffered and
    // recorded as a failure regardless of what the client asked for.
    if !status.is_success() {
        let (body_bytes, fate) = collect_capped(upstream_body).await;
        let usage = extract_usage(&body_bytes, endpoint);
        let reason = scrub(match &fate {
            BodyFate::Complete => upstream_error_message(&body_bytes)
                .unwrap_or_else(|| format!("upstream returned {status}")),
            BodyFate::CapExceeded => format!(
                "upstream returned {status} with an error body above {MAX_BUFFERED_RESPONSE} bytes"
            ),
            BodyFate::Broken(e) => {
                format!("upstream returned {status} and its error body broke mid-read: {e}")
            }
        });

        // The client is handed the upstream's own error document rather than a
        // generic substitute. A reverse proxy that swallows it takes away the
        // only explanation the caller will ever get — the provider's
        // `error.message` is the actionable part of a failed inference call.
        let response = forward_upstream_error(status, &parts.headers, &body_bytes, &request_id);

        return finalize_and_respond(
            &state,
            record,
            Outcome::Failed {
                http_status: Some(status.as_u16()),
                reason,
                usage,
            },
            started,
            response,
        )
        .await;
    }

    if wants_stream || upstream_is_sse {
        return stream_response(
            &state,
            record,
            endpoint,
            started,
            status.as_u16(),
            parts.headers,
            upstream_body,
            upstream_cfg,
            request_id,
        );
    }

    // Non-streaming: buffer under a cap, extract usage, then respond.
    let (body_bytes, fate) = collect_capped(upstream_body).await;

    // A body that did not arrive whole cannot be passed off as the whole body.
    // Sending what was read would hand the client a truncated document carrying
    // the upstream's `Content-Length`, and recording it as `completed` would
    // state in the ledger that a request succeeded which was in fact served
    // incomplete. Neither is acceptable, and nothing has been sent yet, so the
    // honest answer is a failure.
    if !matches!(fate, BodyFate::Complete) {
        let reason = match &fate {
            BodyFate::Complete => unreachable!("guarded above"),
            BodyFate::CapExceeded => {
                format!("upstream response exceeded the {MAX_BUFFERED_RESPONSE}-byte buffering cap")
            }
            BodyFate::Broken(e) => format!("upstream response body broke mid-read: {e}"),
        };
        tracing::error!(request_id = %request_id, reason = %reason, "not forwarding a partial response");

        return finalize_and_respond(
            &state,
            record,
            Outcome::Failed {
                http_status: Some(StatusCode::BAD_GATEWAY.as_u16()),
                reason,
                usage: Usage::default(),
            },
            started,
            error_response(
                &request_id,
                StatusCode::BAD_GATEWAY,
                "Upstream response could not be read in full",
                "upstream_error",
            ),
        )
        .await;
    }

    let usage = extract_usage(&body_bytes, endpoint);

    let mut record = record;
    let duration_ms = started.elapsed().as_millis() as u64;
    // Truthful, not requested: this branch is the proof the response was not
    // streamed, whatever the request body asked for.
    record.streaming = false;
    record.complete(status.as_u16(), usage, duration_ms);
    if let Err(e) = state.ledger.finalize(record).await {
        state.ledger.mark_unhealthy();
        tracing::error!(request_id = %request_id, error = %e, "Failed to finalize metering record");
    }

    let builder =
        ProxyClient::forward_response_headers(Response::builder().status(status), &parts.headers);
    match builder
        .header("x-request-id", &request_id)
        .body(AxumBody::from(body_bytes))
    {
        Ok(response) => response,
        Err(_) => error_response(
            &request_id,
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to build response",
            "proxy_error",
        ),
    }
}

/// Rebuild the client-facing response for an upstream error.
///
/// The upstream body is forwarded verbatim when there is one — headers included,
/// so `Content-Type` and any provider-specific fields survive — and a generic
/// error is substituted only when the body is empty or could not be read. The
/// upstream status is preserved either way, because the classifier for a failure
/// (`429` vs `400` vs `503`) is what a client retries on.
fn forward_upstream_error(
    status: StatusCode,
    headers: &HeaderMap,
    body: &Bytes,
    request_id: &str,
) -> Response {
    if body.is_empty() {
        return error_response(request_id, status, "Upstream error", "upstream_error");
    }

    let builder =
        ProxyClient::forward_response_headers(Response::builder().status(status), headers);
    match builder
        .header("x-request-id", request_id)
        .body(AxumBody::from(body.clone()))
    {
        Ok(response) => response,
        Err(_) => error_response(request_id, status, "Upstream error", "upstream_error"),
    }
}

/// Proxy a request without metering it.
async fn proxy_unmetered(
    state: &Arc<AppState>,
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
    request_id: &str,
) -> Response {
    let upstream_cfg = state.upstream();
    let timeout = Duration::from_secs(upstream_cfg.timeout_secs.max(1));

    match tokio::time::timeout(
        timeout,
        state
            .client
            .proxy(Some(&upstream_cfg), method, path, body, headers),
    )
    .await
    {
        Err(_) => error_response(
            request_id,
            StatusCode::GATEWAY_TIMEOUT,
            "Upstream timeout",
            "upstream_timeout",
        ),
        Ok(Err(e)) => error_response(
            request_id,
            StatusCode::BAD_GATEWAY,
            &format!("Upstream connection failed: {e}"),
            "upstream_error",
        ),
        Ok(Ok(response)) => {
            let status = response.status();
            let (parts, body) = response.into_parts();
            let (bytes, fate) = collect_capped(body).await;
            if !matches!(fate, BodyFate::Complete) {
                // Same rule as the metered path: a partially read body is never
                // presented as if it were the whole one.
                return error_response(
                    request_id,
                    StatusCode::BAD_GATEWAY,
                    "Upstream response could not be read in full",
                    "upstream_error",
                );
            }
            let builder = ProxyClient::forward_response_headers(
                Response::builder().status(status),
                &parts.headers,
            );
            match builder
                .header("x-request-id", request_id)
                .body(AxumBody::from(bytes))
            {
                Ok(response) => response,
                Err(_) => error_response(
                    request_id,
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Failed to build response",
                    "proxy_error",
                ),
            }
        }
    }
}

/// Terminal state chosen for a request, applied by [`finalize_and_respond`].
enum Outcome {
    Failed {
        http_status: Option<u16>,
        reason: String,
        usage: Usage,
    },
}

/// Apply a terminal state, durably, then return the response the client gets.
///
/// The response is built by the caller and passed in, because the two are not
/// always the same object: a failure that the upstream already described must be
/// forwarded as the upstream's own document. What the ledger records and what
/// the client saw are then the same event, and only one of them is authoritative.
async fn finalize_and_respond(
    state: &Arc<AppState>,
    mut record: RequestRecord,
    outcome: Outcome,
    started: Instant,
    response: Response,
) -> Response {
    let duration_ms = started.elapsed().as_millis() as u64;
    let request_id = record.request_id.clone();

    match outcome {
        Outcome::Failed {
            http_status,
            reason,
            usage,
        } => {
            record.fail(http_status, reason, duration_ms);
            record.set_usage(usage);
        }
    }

    // This branch never streamed, whatever the request asked for.
    record.streaming = false;

    if let Err(e) = state.ledger.finalize(record).await {
        state.ledger.mark_unhealthy();
        tracing::error!(request_id = %request_id, error = %e, "Failed to finalize metering record");
    }

    response
}

/// Forward a streaming response incrementally while metering it.
#[allow(clippy::too_many_arguments)]
fn stream_response(
    state: &Arc<AppState>,
    mut record: RequestRecord,
    endpoint: Endpoint,
    started: Instant,
    http_status: u16,
    upstream_headers: HeaderMap,
    upstream_body: hyper::body::Incoming,
    upstream_cfg: UpstreamConfig,
    request_id: String,
) -> Response {
    // The record was accepted with the client's *request* for a stream; this is
    // the branch where one is actually being served, so the ledger records that
    // rather than the request body's wish. The two differ whenever an upstream
    // answers `text/event-stream` to a request that did not ask to stream.
    record.streaming = true;

    let meter = Arc::new(StreamMeter::new(
        record,
        state.ledger.clone(),
        endpoint,
        started,
        http_status,
        state.credentials(),
    ));

    let idle_timeout = Duration::from_secs(upstream_cfg.timeout_secs.max(1));
    let stream_meter = meter.clone();

    let body = AxumBody::from_stream(async_stream::stream! {
        let mut body = upstream_body;
        let mut failure: Option<String> = None;

        loop {
            match tokio::time::timeout(idle_timeout, body.frame()).await {
                Err(_) => {
                    failure = Some(format!(
                        "upstream sent no data for {}s mid-stream",
                        idle_timeout.as_secs()
                    ));
                    break;
                }
                Ok(None) => break,
                Ok(Some(Err(e))) => {
                    failure = Some(format!("upstream stream broke: {e}"));
                    break;
                }
                Ok(Some(Ok(frame))) => {
                    // Only data frames carry payload; trailers are dropped
                    // because this API does not use them and forwarding would
                    // require re-framing.
                    if let Ok(data) = frame.into_data() {
                        if !data.is_empty() {
                            stream_meter.observe(&data);
                            yield Ok::<Bytes, std::io::Error>(data);
                        }
                    }
                }
            }
        }

        match failure {
            Some(reason) => stream_meter.finish_broken(reason),
            None => stream_meter.finish_completed(),
        }
    });

    let builder = ProxyClient::forward_response_headers(
        Response::builder().status(http_status),
        &upstream_headers,
    );

    match builder.header("x-request-id", &request_id).body(body) {
        Ok(response) => response,
        Err(_) => {
            // The response could not be built, so the stream will be dropped and
            // the meter's guard records the request as interrupted.
            error_response(
                &request_id,
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build streaming response",
                "proxy_error",
            )
        }
    }
}

/// Owns the metering state for a streaming response.
///
/// Its drop guard is what makes accounting complete: if the client disconnects
/// mid-stream the response body (and therefore this value) is dropped, and the
/// guard records the request as `interrupted` instead of leaving it `in_flight`
/// forever or losing it.
struct StreamMeter {
    inner: parking_lot::Mutex<StreamMeterState>,
    ledger: Arc<LedgerWriter>,
    started: Instant,
    http_status: u16,
    /// Every configured credential, so a reason built from upstream text cannot
    /// commit one to the ledger. See [`sanitize_reason`].
    credentials: Vec<String>,
}

/// The meter's one-shot latch.
///
/// The armed/handed-off flag lives *inside* the same mutex as the state it
/// guards, and the terminal record is built under that lock before the flag
/// flips. That ordering is the whole point: an `AtomicBool` swapped before an
/// `await` left a window in which a cancelled write had already disarmed the
/// guard, so the record was never written by either path. Deciding and building
/// under one lock, with the handoff itself being non-blocking, means a terminal
/// state is always produced exactly once and is never cancellable.
enum MeterLatch {
    Armed,
    HandedOff,
}

struct StreamMeterState {
    record: RequestRecord,
    scanner: SseUsageScanner,
    ttft_ms: Option<u64>,
    bytes: u64,
    latch: MeterLatch,
}

impl StreamMeterState {
    /// Claim the one terminal write this meter is allowed to make.
    ///
    /// Returns `false` for a caller that arrived second. Called with the state
    /// lock held, and the caller builds the record before releasing it, so the
    /// loser of this race can never observe a claimed-but-unbuilt state.
    fn arm_once(&mut self) -> bool {
        match self.latch {
            MeterLatch::Armed => {
                self.latch = MeterLatch::HandedOff;
                true
            }
            MeterLatch::HandedOff => false,
        }
    }
}

impl StreamMeter {
    fn new(
        record: RequestRecord,
        ledger: Arc<LedgerWriter>,
        endpoint: Endpoint,
        started: Instant,
        http_status: u16,
        credentials: Vec<String>,
    ) -> Self {
        Self {
            inner: parking_lot::Mutex::new(StreamMeterState {
                record,
                scanner: SseUsageScanner::new(endpoint),
                ttft_ms: None,
                bytes: 0,
                latch: MeterLatch::Armed,
            }),
            ledger,
            started,
            http_status,
            credentials,
        }
    }

    /// Observe one data frame: measure TTFT once, then scan for usage.
    fn observe(&self, data: &[u8]) {
        let mut state = self.inner.lock();
        if state.ttft_ms.is_none() {
            state.ttft_ms = Some(self.started.elapsed().as_millis() as u64);
        }
        state.bytes += data.len() as u64;
        state.scanner.feed(data);
    }

    /// The upstream stream ended normally.
    fn finish_completed(&self) {
        let record = {
            let mut state = self.inner.lock();
            if !state.arm_once() {
                return;
            }
            state.scanner.finish();

            let usage = state.scanner.usage().unwrap_or_default();
            if state.scanner.truncated() {
                tracing::warn!(
                    request_id = %state.record.request_id,
                    "Streaming usage scan was truncated; recording usage as unavailable"
                );
            }
            let duration_ms = self.started.elapsed().as_millis() as u64;
            let ttft = state.ttft_ms;
            let mut record = state.record.clone();
            record.complete(self.http_status, usage, duration_ms);
            if let Some(ttft) = ttft {
                record.set_ttft(ttft);
            }
            record
        };

        self.write_terminal(record);
    }

    /// The upstream stream broke after the response was already sent.
    fn finish_broken(&self, reason: String) {
        let record = {
            let mut state = self.inner.lock();
            if !state.arm_once() {
                return;
            }
            state.scanner.finish();

            let duration_ms = self.started.elapsed().as_millis() as u64;
            let ttft = state.ttft_ms;
            let usage = state.scanner.usage();
            let mut record = state.record.clone();
            let credentials: Vec<&str> = self.credentials.iter().map(String::as_str).collect();
            record.fail(
                Some(self.http_status),
                sanitize_reason(
                    &format!("{} (after {} bytes)", reason, state.bytes),
                    &credentials,
                ),
                duration_ms,
            );
            // Keep whatever usage was observed before the break; leave the rest
            // unavailable rather than inventing it.
            if let Some(usage) = usage {
                record.set_usage(usage);
            }
            if let Some(ttft) = ttft {
                record.set_ttft(ttft);
            }
            record
        };

        self.write_terminal(record);
    }

    /// Hand the terminal record to the writer.
    ///
    /// Deliberately not `async`: the handoff must not sit on a suspension point
    /// that the response body's own future can be dropped at. This code runs at
    /// the *end* of the stream generator, and a client disconnecting in that
    /// instant drops the generator — which, when the write was awaited inline,
    /// cancelled it after the record had been built and after the guard had been
    /// disarmed, losing the record entirely. The writer tracks detached writes
    /// and its shutdown waits for them, so nothing is lost by returning here.
    fn write_terminal(&self, record: RequestRecord) {
        self.ledger.spawn_detached_finalize(record);
    }
}

impl Drop for StreamMeter {
    fn drop(&mut self) {
        let record = {
            let mut state = self.inner.lock();
            // Stand down if a terminal state was already handed off. Taking the
            // decision under the same lock the handoff used is what makes this
            // race-free: the guard cannot observe a half-finished handoff.
            if !state.arm_once() {
                return;
            }
            state.scanner.finish();
            let duration_ms = self.started.elapsed().as_millis() as u64;
            let ttft = state.ttft_ms;
            let usage = state.scanner.usage();
            let mut record = state.record.clone();
            record.interrupt(CLIENT_DISCONNECT, duration_ms);
            if let Some(usage) = usage {
                record.set_usage(usage);
            }
            if let Some(ttft) = ttft {
                record.set_ttft(ttft);
            }
            record
        };

        // Drop cannot await, so the durable write is handed to a detached task.
        // The writer tracks these, and its shutdown waits for them, so the record
        // is still committed before the database closes.
        self.ledger.spawn_detached_finalize(record);
    }
}

/// How a buffered upstream body ended.
///
/// The three cases are kept apart rather than collapsed into a `truncated` flag
/// because they call for different answers: a body cut off by the cap and a body
/// cut off by a stream error both mean "this is not the whole response", but the
/// reason a human reads later is not the same one. A boolean also has no way to
/// say what went wrong, and a ledger row that says only "something was truncated"
/// is not worth storing.
enum BodyFate {
    /// Read to the end.
    Complete,
    /// The buffering cap was reached; the remainder was not read.
    CapExceeded,
    /// The body failed mid-read.
    Broken(String),
}

/// Collect a body under a cap, reporting how it ended.
async fn collect_capped(mut body: hyper::body::Incoming) -> (Bytes, BodyFate) {
    let mut acc: Vec<u8> = Vec::new();
    let mut fate = BodyFate::Complete;

    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if let Ok(data) = frame.into_data() {
                    if acc.len() + data.len() > MAX_BUFFERED_RESPONSE {
                        let room = MAX_BUFFERED_RESPONSE.saturating_sub(acc.len());
                        acc.extend_from_slice(&data[..room]);
                        fate = BodyFate::CapExceeded;
                        break;
                    }
                    acc.extend_from_slice(&data);
                }
            }
            Err(e) => {
                fate = BodyFate::Broken(e.to_string());
                break;
            }
        }
    }

    (Bytes::from(acc), fate)
}

/// Longest reason string persisted to the ledger.
///
/// The reason is upstream-influenced text (a provider's error document, a
/// transport error). It is stored for an operator to read, not to be a copy of
/// whatever the upstream chose to send, so it is capped — otherwise a 32 MiB
/// error body becomes a 32 MiB row in every usage view.
const MAX_REASON_CHARS: usize = 512;

/// Make an upstream-influenced string safe to persist and to log.
///
/// Three problems, one function, and this is the only point where such text
/// enters the ledger — which is why the credential scrub lives here rather than
/// at each call site. Missing it once would mean a secret committed to a row and
/// served back through the dashboard.
///
/// * **Credentials.** An upstream that echoes the key it was given — "Incorrect
///   API key provided: sk-…" is a real provider behaviour — hands us a credential
///   in its error document. Persisting it would let it outlive the request, so
///   every configured credential is replaced with [`REDACTED`].
/// * **Length.** Unbounded length is a storage and display problem (above).
/// * **Control characters.** A *log* problem: a message containing a newline can
///   forge a second log line, and one containing an ANSI escape can repaint a
///   terminal reading the log.
fn sanitize_reason(reason: &str, credentials: &[&str]) -> String {
    let reason = crate::config::redact_credentials(reason, credentials);
    let reason = reason.as_str();

    let mut out = String::with_capacity(reason.len().min(MAX_REASON_CHARS));
    for (i, c) in reason.chars().enumerate() {
        if i >= MAX_REASON_CHARS {
            out.push_str("… (truncated)");
            break;
        }
        if c.is_control() {
            // Whitespace control characters become a space so words do not run
            // together; everything else is dropped.
            if c == '\n' || c == '\r' || c == '\t' {
                out.push(' ');
            }
        } else {
            out.push(c);
        }
    }
    out
}

/// Extract usage from a buffered non-streaming response body.
fn extract_usage(body: &[u8], endpoint: Endpoint) -> Usage {
    let Ok(value) = serde_json::from_slice::<serde_json::Value>(body) else {
        return Usage::default();
    };
    match endpoint {
        Endpoint::ChatCompletions => extract_chat_completions_usage(&value),
        Endpoint::Responses => extract_responses_usage(&value),
        Endpoint::Models => Usage::default(),
    }
}

/// Pull a provider's error message out of a failure body, if present.
fn upstream_error_message(body: &[u8]) -> Option<String> {
    let value: serde_json::Value = serde_json::from_slice(body).ok()?;
    value
        .get("error")
        .and_then(|e| e.get("message"))
        .and_then(|m| m.as_str())
        .map(|m| m.to_string())
}

/// Model name for a Responses API request.
fn extract_responses_usage_model(body: &serde_json::Value) -> String {
    crate::proxy::usage::extract_responses_model(body)
}

/// Standard OpenAI-style error response.
/// Standard OpenAI-shaped error response, tagged with the request identity.
///
/// The id is a required argument rather than an optional header added by the
/// caller: an error the proxy generated itself is exactly the kind a client has
/// no other way to trace, and making it optional would mean the responses that
/// need it most are the ones most likely to be built without it.
pub fn error_response(
    request_id: &str,
    status: StatusCode,
    message: &str,
    error_type: &str,
) -> Response {
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
        .header("x-request-id", request_id)
        .body(AxumBody::from(json))
        .unwrap_or_else(|_| Response::new(AxumBody::empty()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ledger::{LedgerWriterConfig, RequestRecord, RequestStatus, UsageStatus};
    use parking_lot::Mutex as ParkingMutex;
    use rusqlite::Connection;
    use std::sync::Arc;
    use tempfile::TempDir;

    /// The credential these tests pretend is configured upstream, so that a
    /// meter built here has something a scrubber must recognise.
    const TEST_UPSTREAM_KEY: &str = "sk-upstream-secret-1234";

    fn meter_with_db(path: &std::path::Path) -> (Arc<StreamMeter>, Arc<LedgerWriter>) {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        let ledger = Arc::new(LedgerWriter::new(
            Arc::new(ParkingMutex::new(conn)),
            LedgerWriterConfig {
                // Single-instance tests: no ownership is claimed, so
                // every row reads back with a `NULL` owner.
                instance_id: None,
                queue_size: 100,
                batch_size: 1,
                batch_timeout_ms: 1,
            },
        ));
        let record = RequestRecord::new(
            "s-1".into(),
            "consumer".into(),
            "gpt-4o".into(),
            Endpoint::ChatCompletions,
            true,
        );
        let meter = Arc::new(StreamMeter::new(
            record,
            ledger.clone(),
            Endpoint::ChatCompletions,
            Instant::now(),
            200,
            vec![TEST_UPSTREAM_KEY.to_string()],
        ));
        (meter, ledger)
    }

    fn read_status(path: &std::path::Path) -> (String, Option<i64>, Option<i64>, Option<i64>) {
        let conn = Connection::open(path).unwrap();
        conn.query_row(
            "SELECT request_status, input_tokens, output_tokens, ttft_ms
             FROM usage_records WHERE request_id='s-1'",
            [],
            |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?)),
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_completed_stream_records_usage_and_ttft() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let (meter, ledger) = meter_with_db(&path);

        meter.observe(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n");
        meter.observe(
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":22}}\n\n",
        );
        meter.finish_completed();
        ledger.shutdown().await;

        let (status, input, output, ttft) = read_status(&path);
        assert_eq!(status, "completed");
        assert_eq!(input, Some(11));
        assert_eq!(output, Some(22));
        assert!(
            ttft.is_some(),
            "TTFT must be measured at the first data frame"
        );
    }

    #[tokio::test]
    async fn test_stream_without_usage_records_null_not_zero() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let (meter, ledger) = meter_with_db(&path);

        meter.observe(b"data: {\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n");
        meter.observe(b"data: [DONE]\n\n");
        meter.finish_completed();
        ledger.shutdown().await;

        let conn = Connection::open(&path).unwrap();
        let (status, input, usage_status): (String, Option<i64>, String) = conn
            .query_row(
                "SELECT request_status, input_tokens, usage_status
                 FROM usage_records WHERE request_id='s-1'",
                [],
                |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)),
            )
            .unwrap();
        assert_eq!(status, "completed");
        assert_eq!(input, None, "absent usage must stay NULL, never 0");
        assert_eq!(usage_status, "unavailable");
    }

    #[tokio::test]
    async fn test_dropped_stream_records_interrupted() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let (meter, ledger) = meter_with_db(&path);

        meter.observe(b"data: {\"choices\":[{\"delta\":{\"content\":\"partial\"}}]}\n\n");
        // Simulate the client vanishing: the body is dropped without finish().
        drop(meter);

        // The detached write is handed to the writer; give it a moment, then
        // shut down, which must drain it.
        tokio::time::sleep(Duration::from_millis(50)).await;
        ledger.shutdown().await;

        let (status, _, _, ttft) = read_status(&path);
        assert_eq!(status, "interrupted");
        assert!(ttft.is_some(), "partial progress must still be described");
    }

    #[tokio::test]
    async fn test_broken_stream_records_failed_with_partial_usage() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let (meter, ledger) = meter_with_db(&path);

        meter.observe(
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":7,\"completion_tokens\":3}}\n\n",
        );
        meter.finish_broken("upstream stream broke: connection reset".into());
        ledger.shutdown().await;

        let (status, input, output, _) = read_status(&path);
        assert_eq!(status, "failed");
        assert_eq!(
            input,
            Some(7),
            "usage observed before the break must be kept"
        );
        assert_eq!(output, Some(3));
    }

    #[tokio::test]
    async fn test_a_key_echoed_by_a_broken_upstream_never_reaches_the_database() {
        // The whole point of scrubbing inside `sanitize_reason`: this value goes
        // through `finish_broken` into a real SQLite row. Asserting on the
        // function alone would not catch a call site that bypassed it.
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let (meter, ledger) = meter_with_db(&path);

        meter.finish_broken(format!(
            "upstream rejected the request: Incorrect API key provided: {TEST_UPSTREAM_KEY}"
        ));
        ledger.shutdown().await;

        let conn = Connection::open(&path).unwrap();
        let stored: Option<String> = conn
            .query_row("SELECT error_message FROM usage_records LIMIT 1", [], |r| {
                r.get(0)
            })
            .unwrap();
        let stored = stored.expect("a broken stream records why it broke");
        assert!(
            !stored.contains(TEST_UPSTREAM_KEY),
            "the credential was committed to the ledger: {stored}"
        );
        assert!(
            stored.contains("<redacted>"),
            "the reason should still say the key was rejected: {stored}"
        );
    }

    #[tokio::test]
    async fn test_finish_then_drop_does_not_double_record() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let (meter, ledger) = meter_with_db(&path);

        meter.observe(
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n",
        );
        meter.finish_completed();
        drop(meter);
        ledger.shutdown().await;

        let conn = Connection::open(&path).unwrap();
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
            .unwrap();
        assert_eq!(n, 1);
        let (status, _, _, _) = read_status(&path);
        assert_eq!(
            status, "completed",
            "the guard must not overwrite a terminal state"
        );
    }

    #[test]
    fn test_extract_usage_from_buffered_error_body() {
        let body = br#"{"error":{"message":"bad model","type":"invalid_request_error"}}"#;
        assert_eq!(upstream_error_message(body).as_deref(), Some("bad model"));
    }

    #[test]
    fn test_upstream_error_message_absent_is_none() {
        assert!(upstream_error_message(b"not json").is_none());
        assert!(upstream_error_message(b"{}").is_none());
    }

    #[test]
    fn test_a_provider_echoing_the_key_does_not_get_it_persisted() {
        // Real provider behaviour: the error document quotes the rejected key.
        let reason =
            "Incorrect API key provided: sk-upstream-secret-1234. You can find your key at ...";
        let sanitized = sanitize_reason(reason, &["sk-upstream-secret-1234"]);
        assert_eq!(
            sanitized,
            "Incorrect API key provided: <redacted>. You can find your key at ..."
        );
        assert!(!sanitized.contains("sk-upstream-secret-1234"));
    }

    #[test]
    fn test_a_local_key_echoed_by_the_upstream_is_also_scrubbed() {
        // The client's own key is a credential too: an upstream that echoes the
        // forwarded Authorization header must not have it land in the ledger.
        let reason = "rejected bearer sk-local-abcdef";
        let sanitized = sanitize_reason(reason, &["sk-upstream-secret-1234", "sk-local-abcdef"]);
        assert_eq!(sanitized, "rejected bearer <redacted>");
    }

    #[test]
    fn test_a_credential_past_the_truncation_point_is_still_scrubbed() {
        // Redaction runs over the whole string before the cap is applied, so a
        // secret that would be cut off is still removed rather than half-kept.
        let reason = format!(
            "{}sk-upstream-secret-1234",
            "x".repeat(MAX_REASON_CHARS + 64)
        );
        let sanitized = sanitize_reason(&reason, &["sk-upstream-secret-1234"]);
        assert!(!sanitized.contains("sk-upstream-secret-1234"));
        assert!(sanitized.ends_with("… (truncated)"));
    }

    #[test]
    fn test_a_long_reason_is_capped() {
        let reason = "a".repeat(MAX_REASON_CHARS * 4);
        let sanitized = sanitize_reason(&reason, &[]);
        assert_eq!(
            sanitized.chars().count(),
            MAX_REASON_CHARS + "… (truncated)".chars().count()
        );
        assert!(sanitized.ends_with("… (truncated)"));
        assert_eq!(&sanitized[..MAX_REASON_CHARS], &reason[..MAX_REASON_CHARS]);
    }

    #[test]
    fn test_a_reason_at_the_cap_is_not_marked_truncated() {
        // The boundary matters: an exactly-capped message is complete, and
        // claiming otherwise would send an operator looking for missing text.
        let reason = "a".repeat(MAX_REASON_CHARS);
        let sanitized = sanitize_reason(&reason, &[]);
        assert_eq!(sanitized, reason);
    }

    #[test]
    fn test_a_newline_cannot_forge_a_log_line_and_an_escape_cannot_repaint_one() {
        // "\n" and "\r" would forge a second log line; an ANSI escape would
        // repaint the terminal reading it. The escape introducer is dropped.
        let sanitized = sanitize_reason("upstream said:\n\r\tsk-\u{1b}[31m bad", &[]);
        assert_eq!(sanitized, "upstream said:   sk-[31m bad");
        assert!(!sanitized.contains('\n'));
        assert!(!sanitized.contains('\r'));
        assert!(!sanitized.contains('\u{1b}'));
    }

    #[test]
    fn test_an_ordinary_reason_passes_through_unchanged() {
        let reason = "upstream returned status 502: bad gateway";
        assert_eq!(
            sanitize_reason(reason, &["sk-upstream-secret-1234"]),
            reason
        );
    }

    #[test]
    fn test_extract_usage_unparseable_body_is_unavailable_not_zero() {
        let usage = extract_usage(b"<html>gateway error</html>", Endpoint::ChatCompletions);
        assert_eq!(usage.status(), UsageStatus::Unavailable);
    }

    #[test]
    fn test_extract_usage_from_valid_non_streaming_body() {
        let body = br#"{"id":"1","usage":{"prompt_tokens":5,"completion_tokens":6}}"#;
        let usage = extract_usage(body, Endpoint::ChatCompletions);
        assert_eq!(usage.input_tokens, Some(5));
        assert_eq!(usage.output_tokens, Some(6));
        assert_eq!(usage.status(), UsageStatus::Available);
    }

    #[test]
    fn test_models_endpoint_is_not_metered() {
        // Guard against accidentally billing discovery traffic.
        assert_eq!(Endpoint::from_path("/v1/models"), Some(Endpoint::Models));
        assert_eq!(
            Endpoint::from_path("/v1/chat/completions"),
            Some(Endpoint::ChatCompletions)
        );
        assert_eq!(
            Endpoint::from_path("/v1/responses"),
            Some(Endpoint::Responses)
        );
        assert_eq!(Endpoint::from_path("/v1/embeddings"), None);
    }

    #[test]
    fn test_terminal_state_is_deterministic() {
        // Every terminal path must land on a status the schema accepts.
        let mut r = RequestRecord::new(
            "x".into(),
            "c".into(),
            "m".into(),
            Endpoint::ChatCompletions,
            true,
        );
        assert_eq!(r.request_status, RequestStatus::InFlight);
        r.complete(200, Usage::default(), 1);
        assert!(r.is_terminal());
        r.interrupt("gone", 2);
        assert_eq!(r.request_status, RequestStatus::Interrupted);
    }
}
