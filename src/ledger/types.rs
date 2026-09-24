//! Ledger types

use serde::{Deserialize, Serialize};
use time::OffsetDateTime;

/// Endpoint types
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Endpoint {
    ChatCompletions,
    Responses,
    Models,
}

impl Endpoint {
    pub fn as_str(&self) -> &'static str {
        match self {
            Endpoint::ChatCompletions => "chat_completions",
            Endpoint::Responses => "responses",
            Endpoint::Models => "models",
        }
    }

    /// Classify a request target, tolerating a query string.
    ///
    /// The caller hands over `uri().path_and_query()` so the query is forwarded
    /// to the upstream verbatim, which means the string arriving here may be
    /// `/v1/chat/completions?beta=true`. Comparing the whole target against three
    /// exact paths would answer every such request 404 — never forwarded, never
    /// metered — so the query is stripped before matching. Only the path is
    /// classified; the query stays request data, and is never read as identity.
    pub fn from_path(path: &str) -> Option<Self> {
        let path = match path.find(['?', '#']) {
            Some(cut) => &path[..cut],
            None => path,
        };
        match path {
            "/v1/chat/completions" => Some(Endpoint::ChatCompletions),
            "/v1/responses" => Some(Endpoint::Responses),
            "/v1/models" => Some(Endpoint::Models),
            _ => None,
        }
    }
}

impl std::fmt::Display for Endpoint {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Request status in the ledger.
///
/// `InFlight` is the *accepted* state: the record is durable before the proxy
/// contacts the upstream, so a crash mid-request leaves a recoverable trace
/// instead of a silently missing record. Only the three terminal states are
/// ever rolled up into `usage_hourly`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestStatus {
    InFlight,
    Completed,
    Failed,
    Interrupted,
}

impl RequestStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RequestStatus::InFlight => "in_flight",
            RequestStatus::Completed => "completed",
            RequestStatus::Failed => "failed",
            RequestStatus::Interrupted => "interrupted",
        }
    }

    /// Whether this state is terminal (no further transition is possible).
    pub fn is_terminal(&self) -> bool {
        !matches!(self, RequestStatus::InFlight)
    }

    /// Parse a persisted status string. Unknown values are treated as
    /// `Interrupted`, the conservative terminal state, rather than silently
    /// mapping to a success-shaped value.
    pub fn from_str_lossy(s: &str) -> Self {
        match s {
            "in_flight" => RequestStatus::InFlight,
            "completed" => RequestStatus::Completed,
            "failed" => RequestStatus::Failed,
            _ => RequestStatus::Interrupted,
        }
    }
}

impl std::fmt::Display for RequestStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Usage availability status
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum UsageStatus {
    Available,
    Unavailable,
    Partial,
}

impl UsageStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            UsageStatus::Available => "available",
            UsageStatus::Unavailable => "unavailable",
            UsageStatus::Partial => "partial",
        }
    }
}

impl std::fmt::Display for UsageStatus {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.as_str())
    }
}

/// Token usage information
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct Usage {
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
}

impl Usage {
    pub fn new(input: Option<u64>, output: Option<u64>, cached: Option<u64>) -> Self {
        Self {
            input_tokens: input,
            output_tokens: output,
            cached_tokens: cached,
        }
    }

    pub fn status(&self) -> UsageStatus {
        if self.input_tokens.is_some() && self.output_tokens.is_some() {
            UsageStatus::Available
        } else if self.input_tokens.is_some() || self.output_tokens.is_some() {
            UsageStatus::Partial
        } else {
            UsageStatus::Unavailable
        }
    }

    pub fn total(&self) -> Option<u64> {
        match (self.input_tokens, self.output_tokens) {
            (Some(i), Some(o)) => Some(i + o),
            _ => None,
        }
    }
}

/// Request record for the ledger
#[derive(Debug, Clone)]
pub struct RequestRecord {
    pub request_id: String,
    pub created_at: OffsetDateTime,
    pub consumer_id: String,
    pub model: String,
    pub endpoint: Endpoint,
    /// Whether the response was actually streamed to the client.
    ///
    /// Set from the request's `stream` flag at accept, and corrected at finalize
    /// to what really happened: a request that asked to stream but whose upstream
    /// answered with a plain JSON body was not served as a stream, and a request
    /// that did not ask but whose upstream answered `text/event-stream` was. The
    /// hourly rollup buckets by this column, so `streaming = true` must mean
    /// "there is a stream", not "there was a request for one".
    pub streaming: bool,
    pub http_status: Option<u16>,
    pub request_status: RequestStatus,
    pub usage: Usage,
    pub ttft_ms: Option<u64>,
    pub duration_ms: u64,
    pub error_message: Option<String>,
    /// Bounded, lossy UTF-8 text from a non-streaming non-2xx upstream response.
    /// Never set from request, successful, or streaming bodies.
    pub error_body: Option<String>,
}

impl RequestRecord {
    /// Create a new request record
    pub fn new(
        request_id: String,
        consumer_id: String,
        model: String,
        endpoint: Endpoint,
        streaming: bool,
    ) -> Self {
        Self {
            request_id,
            created_at: OffsetDateTime::now_utc(),
            consumer_id,
            model,
            endpoint,
            streaming,
            http_status: None,
            request_status: RequestStatus::InFlight,
            usage: Usage::default(),
            ttft_ms: None,
            duration_ms: 0,
            error_message: None,
            error_body: None,
        }
    }

    /// Mark as completed with usage
    pub fn complete(&mut self, http_status: u16, usage: Usage, duration_ms: u64) {
        self.http_status = Some(http_status);
        self.request_status = RequestStatus::Completed;
        self.usage = usage;
        self.duration_ms = duration_ms;
    }

    /// Mark as failed
    pub fn fail(&mut self, http_status: Option<u16>, error: String, duration_ms: u64) {
        self.http_status = http_status;
        self.request_status = RequestStatus::Failed;
        self.error_message = Some(error);
        self.duration_ms = duration_ms;
    }

    /// Mark as interrupted (client disconnected, upstream stream broke, or the
    /// process died while the request was in flight).
    pub fn interrupt(&mut self, reason: &str, duration_ms: u64) {
        self.request_status = RequestStatus::Interrupted;
        self.duration_ms = duration_ms;
        if self.error_message.is_none() {
            self.error_message = Some(reason.to_string());
        }
    }

    /// Whether this record has already reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        self.request_status.is_terminal()
    }

    /// Set TTFT for streaming requests
    pub fn set_ttft(&mut self, ttft_ms: u64) {
        self.ttft_ms = Some(ttft_ms);
    }

    /// Get usage status
    pub fn usage_status(&self) -> UsageStatus {
        self.usage.status()
    }

    /// Update usage without changing lifecycle state (streaming path).
    pub fn set_usage(&mut self, usage: Usage) {
        self.usage = usage;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_usage_status() {
        let full = Usage::new(Some(100), Some(50), Some(10));
        assert_eq!(full.status(), UsageStatus::Available);
        assert_eq!(full.total(), Some(150));

        let partial = Usage::new(Some(100), None, None);
        assert_eq!(partial.status(), UsageStatus::Partial);

        let none = Usage::default();
        assert_eq!(none.status(), UsageStatus::Unavailable);
    }

    #[test]
    fn test_endpoint_from_path() {
        assert_eq!(
            Endpoint::from_path("/v1/chat/completions"),
            Some(Endpoint::ChatCompletions)
        );
        assert_eq!(
            Endpoint::from_path("/v1/responses"),
            Some(Endpoint::Responses)
        );
        assert_eq!(Endpoint::from_path("/v1/models"), Some(Endpoint::Models));
        assert_eq!(Endpoint::from_path("/v1/unknown"), None);

        // The caller passes `path_and_query`, so a query must not make a known
        // endpoint unknown — that would turn a forwarded request into a 404.
        assert_eq!(
            Endpoint::from_path("/v1/chat/completions?beta=true"),
            Some(Endpoint::ChatCompletions)
        );
        assert_eq!(
            Endpoint::from_path("/v1/models?x=1"),
            Some(Endpoint::Models)
        );
        assert_eq!(
            Endpoint::from_path("/v1/responses?stream=true&trace=1"),
            Some(Endpoint::Responses)
        );
        // A fragment is not sent by a client, but stripping it keeps the
        // classification honest for any caller that hands over a full URI.
        assert_eq!(
            Endpoint::from_path("/v1/models#frag"),
            Some(Endpoint::Models)
        );
        // An unknown path stays unknown, with or without a query.
        assert_eq!(Endpoint::from_path("/v1/embeddings?x=1"), None);
        // A prefix match on a longer path must not sneak through.
        assert_eq!(Endpoint::from_path("/v1/chat/completions/x"), None);
    }

    #[test]
    fn test_request_record_lifecycle() {
        let mut record = RequestRecord::new(
            "req-123".to_string(),
            "consumer-1".to_string(),
            "gpt-4".to_string(),
            Endpoint::ChatCompletions,
            true,
        );

        // A freshly accepted request is in flight, not completed.
        assert_eq!(record.request_status, RequestStatus::InFlight);
        assert!(!record.is_terminal());

        record.complete(200, Usage::new(Some(100), Some(50), None), 1500);
        record.set_ttft(200);

        assert_eq!(record.request_status, RequestStatus::Completed);
        assert!(record.is_terminal());
        assert_eq!(record.http_status, Some(200));
        assert_eq!(record.ttft_ms, Some(200));
    }

    #[test]
    fn test_interrupt_preserves_first_reason() {
        let mut record = RequestRecord::new(
            "req-9".to_string(),
            "c".to_string(),
            "m".to_string(),
            Endpoint::Responses,
            true,
        );
        record.interrupt("client disconnected", 400);
        record.interrupt("second reason", 500);
        assert_eq!(record.request_status, RequestStatus::Interrupted);
        assert_eq!(
            record.error_message.as_deref(),
            Some("client disconnected"),
            "the first (root cause) reason must not be overwritten"
        );
        assert_eq!(record.duration_ms, 500);
    }

    #[test]
    fn test_all_statuses_round_trip_as_str() {
        for status in [
            RequestStatus::InFlight,
            RequestStatus::Completed,
            RequestStatus::Failed,
            RequestStatus::Interrupted,
        ] {
            assert_eq!(RequestStatus::from_str_lossy(status.as_str()), status);
        }
    }

    #[test]
    fn test_unknown_status_reads_as_interrupted_not_completed() {
        // A status written by a newer schema must not be mistaken for success.
        assert_eq!(
            RequestStatus::from_str_lossy("weird"),
            RequestStatus::Interrupted
        );
        assert_eq!(
            RequestStatus::from_str_lossy(""),
            RequestStatus::Interrupted
        );
    }
}
