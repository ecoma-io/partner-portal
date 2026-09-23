//! Request handler for the proxy endpoints

use axum::{
    body::Body as AxumBody,
    http::{HeaderMap, Method, StatusCode, header},
    response::Response,
};
use bytes::Bytes;
use http_body_util::BodyExt;
use std::sync::Arc;
use std::time::Instant;

use crate::auth::ConsumerContext;
use crate::ledger::{Endpoint, LedgerWriter, RequestRecord, Usage};
use crate::proxy::client::ProxyClient;
use crate::proxy::usage::{
    extract_chat_completions_usage, extract_model_from_request, extract_responses_model,
    extract_responses_usage,
};

/// Shared application state
#[derive(Clone)]
pub struct AppState {
    pub config: Arc<parking_lot::RwLock<crate::config::ConfigSnapshot>>,
    pub client: Arc<ProxyClient>,
    pub ledger: Arc<LedgerWriter>,
    pub pool: Arc<crate::ledger::LedgerPool>,
    pub broadcaster: Arc<crate::dashboard::SseBroadcaster>,
}

/// Handle proxying of a request (async, returns response)
pub async fn handle_proxy(
    state: Arc<AppState>,
    consumer: ConsumerContext,
    method: Method,
    path: String,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let started = Instant::now();
    let endpoint = match Endpoint::from_path(&path) {
        Some(e) => e,
        None => {
            return error_response(StatusCode::NOT_FOUND, "Not Found", "invalid_request_error");
        }
    };

    // Parse request body for model extraction
    let request_value: Option<serde_json::Value> = if body.is_empty() {
        None
    } else {
        serde_json::from_slice(&body).ok()
    };

    let model = match &request_value {
        Some(v) => match endpoint {
            Endpoint::ChatCompletions => extract_model_from_request(v),
            Endpoint::Responses => extract_responses_model(v),
            _ => "unknown".to_string(),
        },
        None => "unknown".to_string(),
    };

    let streaming = request_value
        .as_ref()
        .and_then(|v| v.get("stream"))
        .and_then(|v| v.as_bool())
        .unwrap_or(false);

    // Create request record
    let request_id = uuid::Uuid::now_v7().to_string();
    let mut record = RequestRecord::new(
        request_id,
        consumer.consumer_id().to_string(),
        model,
        endpoint,
        streaming,
    );

    // Proxy to upstream
    let upstream_result = state.client.proxy(method, path, body, headers).await;

    match upstream_result {
        Ok(upstream_response) => {
            let status = upstream_response.status();
            let is_success = status.is_success();

            // Collect body
            let body = upstream_response.into_body();
            let full_body = match body.collect().await {
                Ok(collected) => collected.to_bytes(),
                Err(e) => {
                    record.fail(
                        Some(500),
                        format!("Failed to read upstream body: {}", e),
                        started.elapsed().as_millis() as u64,
                    );
                    let _ = state.ledger.write(record).await;
                    return error_response(
                        StatusCode::BAD_GATEWAY,
                        "Upstream error",
                        "upstream_error",
                    );
                }
            };

            // Parse body for usage
            let usage_value: Option<serde_json::Value> = serde_json::from_slice(&full_body).ok();
            let usage = match (&usage_value, endpoint) {
                (Some(v), Endpoint::ChatCompletions) => extract_chat_completions_usage(v),
                (Some(v), Endpoint::Responses) => extract_responses_usage(v),
                _ => Usage::default(),
            };

            let duration_ms = started.elapsed().as_millis() as u64;

            if is_success {
                record.complete(status.as_u16(), usage, duration_ms);
            } else {
                let error_msg = format!("Upstream returned {}", status);
                record.fail(Some(status.as_u16()), error_msg, duration_ms);
            }

            // Write to ledger (durable, never drops)
            if state.ledger.write(record).await.is_err() {
                state.ledger.set_ready(false);
                tracing::warn!("Ledger write failed, marked not ready");
            }

            // Build response
            let mut builder = Response::builder().status(status);

            // Forward headers from upstream
            builder = builder.header(header::CONTENT_TYPE, "application/json");

            match builder.body(AxumBody::from(full_body)) {
                Ok(response) => response,
                Err(_) => error_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Proxy error",
                    "proxy_error",
                ),
            }
        }
        Err(e) => {
            let duration_ms = started.elapsed().as_millis() as u64;
            record.fail(
                None,
                format!("Upstream connection failed: {}", e),
                duration_ms,
            );
            let _ = state.ledger.write(record).await;
            error_response(
                StatusCode::BAD_GATEWAY,
                &format!("Upstream error: {}", e),
                "upstream_error",
            )
        }
    }
}

/// Standard OpenAI-style error response
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
        .unwrap()
}
