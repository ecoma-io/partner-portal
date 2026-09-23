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

    pub fn from_path(path: &str) -> Option<Self> {
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

/// Request status in the ledger
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RequestStatus {
    Completed,
    Failed,
    Interrupted,
}

impl RequestStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            RequestStatus::Completed => "completed",
            RequestStatus::Failed => "failed",
            RequestStatus::Interrupted => "interrupted",
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
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
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
    pub streaming: bool,
    pub http_status: Option<u16>,
    pub request_status: RequestStatus,
    pub usage: Usage,
    pub ttft_ms: Option<u64>,
    pub duration_ms: u64,
    pub error_message: Option<String>,
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
            request_status: RequestStatus::Completed,
            usage: Usage::default(),
            ttft_ms: None,
            duration_ms: 0,
            error_message: None,
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

    /// Mark as interrupted
    pub fn interrupt(&mut self, duration_ms: u64) {
        self.request_status = RequestStatus::Interrupted;
        self.duration_ms = duration_ms;
    }

    /// Set TTFT for streaming requests
    pub fn set_ttft(&mut self, ttft_ms: u64) {
        self.ttft_ms = Some(ttft_ms);
    }

    /// Get usage status
    pub fn usage_status(&self) -> UsageStatus {
        self.usage.status()
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

        record.complete(200, Usage::new(Some(100), Some(50), None), 1500);
        record.set_ttft(200);

        assert_eq!(record.request_status, RequestStatus::Completed);
        assert_eq!(record.http_status, Some(200));
        assert_eq!(record.ttft_ms, Some(200));
    }
}
