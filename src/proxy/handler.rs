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
use std::sync::atomic::{AtomicBool, Ordering};
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

    let Some(endpoint) = Endpoint::from_path(&path) else {
        return error_response(StatusCode::NOT_FOUND, "Not Found", "invalid_request_error");
    };

    // `/v1/models` is discovery, not inference. It is authenticated and proxied,
    // but deliberately not metered: it consumes no tokens, and recording it
    // would add noise with zero usage to every usage view and rollup.
    if endpoint == Endpoint::Models {
        return proxy_unmetered(&state, method, path, headers, body).await;
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

    let request_id = uuid::Uuid::now_v7().to_string();
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
            StatusCode::SERVICE_UNAVAILABLE,
            "Metering unavailable; request not forwarded",
            "metering_error",
        );
    }

    let upstream_cfg = state.upstream();
    let header_timeout = Duration::from_secs(upstream_cfg.timeout_secs.max(1));

    let upstream_result = tokio::time::timeout(
        header_timeout,
        state
            .client
            .proxy(Some(&upstream_cfg), method, path, body, headers),
    )
    .await;

    let upstream_response = match upstream_result {
        Err(_) => {
            let reason = format!(
                "upstream did not respond within {}s",
                header_timeout.as_secs()
            );
            return finalize_and_respond(
                &state,
                record,
                Outcome::Failed {
                    http_status: Some(StatusCode::GATEWAY_TIMEOUT.as_u16()),
                    reason: reason.clone(),
                    usage: Usage::default(),
                },
                started,
                StatusCode::GATEWAY_TIMEOUT,
                "Upstream timeout",
                "upstream_timeout",
            )
            .await;
        }
        Ok(Err(e)) => {
            let reason = format!("upstream connection failed: {e}");
            return finalize_and_respond(
                &state,
                record,
                Outcome::Failed {
                    http_status: Some(StatusCode::BAD_GATEWAY.as_u16()),
                    reason: reason.clone(),
                    usage: Usage::default(),
                },
                started,
                StatusCode::BAD_GATEWAY,
                "Upstream connection failed",
                "upstream_error",
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
        let (body_bytes, truncated) = collect_capped(upstream_body).await;
        let usage = extract_usage(&body_bytes, endpoint);
        let reason = if truncated {
            format!("upstream returned {status} (error body truncated)")
        } else {
            upstream_error_message(&body_bytes)
                .unwrap_or_else(|| format!("upstream returned {status}"))
        };

        return finalize_and_respond(
            &state,
            record,
            Outcome::Failed {
                http_status: Some(status.as_u16()),
                reason,
                usage,
            },
            started,
            status,
            "Upstream error",
            "upstream_error",
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
    let (body_bytes, truncated) = collect_capped(upstream_body).await;
    if truncated {
        tracing::warn!(
            request_id = %request_id,
            limit = MAX_BUFFERED_RESPONSE,
            "Upstream response exceeded the buffering cap; usage recorded as unavailable"
        );
    }
    let usage = extract_usage(&body_bytes, endpoint);

    let mut record = record;
    let duration_ms = started.elapsed().as_millis() as u64;
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
            StatusCode::INTERNAL_SERVER_ERROR,
            "Failed to build response",
            "proxy_error",
        ),
    }
}

/// Proxy a request without metering it.
async fn proxy_unmetered(
    state: &Arc<AppState>,
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
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
            StatusCode::GATEWAY_TIMEOUT,
            "Upstream timeout",
            "upstream_timeout",
        ),
        Ok(Err(e)) => error_response(
            StatusCode::BAD_GATEWAY,
            &format!("Upstream connection failed: {e}"),
            "upstream_error",
        ),
        Ok(Ok(response)) => {
            let status = response.status();
            let (parts, body) = response.into_parts();
            let (bytes, _) = collect_capped(body).await;
            let builder = ProxyClient::forward_response_headers(
                Response::builder().status(status),
                &parts.headers,
            );
            match builder.body(AxumBody::from(bytes)) {
                Ok(response) => response,
                Err(_) => error_response(
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

/// Apply a terminal state, durably, then build a local error response.
async fn finalize_and_respond(
    state: &Arc<AppState>,
    mut record: RequestRecord,
    outcome: Outcome,
    started: Instant,
    status: StatusCode,
    message: &str,
    error_type: &str,
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

    if let Err(e) = state.ledger.finalize(record).await {
        state.ledger.mark_unhealthy();
        tracing::error!(request_id = %request_id, error = %e, "Failed to finalize metering record");
    }

    error_response(status, message, error_type)
}

/// Forward a streaming response incrementally while metering it.
#[allow(clippy::too_many_arguments)]
fn stream_response(
    state: &Arc<AppState>,
    record: RequestRecord,
    endpoint: Endpoint,
    started: Instant,
    http_status: u16,
    upstream_headers: HeaderMap,
    upstream_body: hyper::body::Incoming,
    upstream_cfg: UpstreamConfig,
    request_id: String,
) -> Response {
    let meter = Arc::new(StreamMeter::new(
        record,
        state.ledger.clone(),
        endpoint,
        started,
        http_status,
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
            Some(reason) => stream_meter.finish_broken(reason).await,
            None => stream_meter.finish_completed().await,
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
    /// False once a terminal state has been written, so the guard stands down.
    armed: AtomicBool,
}

struct StreamMeterState {
    record: RequestRecord,
    scanner: SseUsageScanner,
    ttft_ms: Option<u64>,
    bytes: u64,
}

impl StreamMeter {
    fn new(
        record: RequestRecord,
        ledger: Arc<LedgerWriter>,
        endpoint: Endpoint,
        started: Instant,
        http_status: u16,
    ) -> Self {
        Self {
            inner: parking_lot::Mutex::new(StreamMeterState {
                record,
                scanner: SseUsageScanner::new(endpoint),
                ttft_ms: None,
                bytes: 0,
            }),
            ledger,
            started,
            http_status,
            armed: AtomicBool::new(true),
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
    async fn finish_completed(&self) {
        let record = {
            let mut state = self.inner.lock();
            if !self.armed.swap(false, Ordering::AcqRel) {
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

        self.write_terminal(record).await;
    }

    /// The upstream stream broke after the response was already sent.
    async fn finish_broken(&self, reason: String) {
        let record = {
            let mut state = self.inner.lock();
            if !self.armed.swap(false, Ordering::AcqRel) {
                return;
            }
            state.scanner.finish();

            let duration_ms = self.started.elapsed().as_millis() as u64;
            let ttft = state.ttft_ms;
            let usage = state.scanner.usage();
            let mut record = state.record.clone();
            record.fail(
                Some(self.http_status),
                format!("{} (after {} bytes)", reason, state.bytes),
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

        self.write_terminal(record).await;
    }

    async fn write_terminal(&self, record: RequestRecord) {
        let request_id = record.request_id.clone();
        if let Err(e) = self.ledger.finalize(record).await {
            self.ledger.mark_unhealthy();
            tracing::error!(
                request_id = %request_id,
                error = %e,
                "Failed to finalize streaming metering record"
            );
        }
    }
}

impl Drop for StreamMeter {
    fn drop(&mut self) {
        // Stand down if a terminal state was already written.
        if !self.armed.swap(false, Ordering::AcqRel) {
            return;
        }

        let record = {
            let mut state = self.inner.lock();
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

/// Collect a body under a cap. Returns `(bytes, truncated)`.
async fn collect_capped(mut body: hyper::body::Incoming) -> (Bytes, bool) {
    let mut acc: Vec<u8> = Vec::new();
    let mut truncated = false;

    while let Some(frame) = body.frame().await {
        match frame {
            Ok(frame) => {
                if let Ok(data) = frame.into_data() {
                    if acc.len() + data.len() > MAX_BUFFERED_RESPONSE {
                        let room = MAX_BUFFERED_RESPONSE.saturating_sub(acc.len());
                        acc.extend_from_slice(&data[..room]);
                        truncated = true;
                        break;
                    }
                    acc.extend_from_slice(&data);
                }
            }
            Err(_) => {
                truncated = true;
                break;
            }
        }
    }

    (Bytes::from(acc), truncated)
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
pub fn error_response(status: StatusCode, message: &str, error_type: &str) -> Response {
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

    fn meter_with_db(path: &std::path::Path) -> (Arc<StreamMeter>, Arc<LedgerWriter>) {
        let conn = Connection::open(path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();
        let ledger = Arc::new(LedgerWriter::new(
            Arc::new(ParkingMutex::new(conn)),
            LedgerWriterConfig {
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
        meter.finish_completed().await;
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
        meter.finish_completed().await;
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
        meter
            .finish_broken("upstream stream broke: connection reset".into())
            .await;
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
    async fn test_finish_then_drop_does_not_double_record() {
        let dir = TempDir::new().unwrap();
        let path = dir.path().join("t.db");
        let (meter, ledger) = meter_with_db(&path);

        meter.observe(
            b"data: {\"choices\":[],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n",
        );
        meter.finish_completed().await;
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
