//! Incremental usage extraction from a streaming response.
//!
//! # Why not buffer
//!
//! The product contract requires streaming responses to stay incremental: the
//! first token must reach the client immediately, so the proxy cannot collect
//! the whole body and parse it afterwards. This scanner is fed each chunk as it
//! passes through and never retains more than one SSE event.
//!
//! # Bounding
//!
//! Bytes that do not yet form a complete event are held in a buffer, because a
//! chunk boundary can split an event. That buffer is capped: an upstream that
//! never sends an event terminator (or sends one enormous event) is treated as
//! unparseable rather than being allowed to grow the proxy's memory. Truncation
//! is recorded so the request's usage is reported as unavailable rather than
//! silently guessed.

use crate::ledger::{Endpoint, Usage};
use crate::proxy::usage::{extract_stream_chat_completions_usage, extract_stream_responses_usage};

/// Maximum bytes held for a single not-yet-complete SSE event.
///
/// OpenAI-compatible usage-bearing events are a few hundred bytes. 256 KiB is
/// far above any legitimate event and far below anything that could exhaust
/// memory under concurrency.
pub const MAX_EVENT_BYTES: usize = 256 * 1024;

/// What the scanner observed over the life of a stream.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ScanOutcome {
    pub usage: Option<Usage>,
    /// Number of complete SSE events processed.
    pub events: u64,
    /// Number of events whose payload was JSON we could parse.
    pub json_events: u64,
    /// True when the buffer cap was hit and scanning was abandoned. Usage will
    /// therefore be reported as unavailable.
    pub truncated: bool,
}

/// Incremental SSE scanner that recovers usage from a streaming response.
pub struct SseUsageScanner {
    endpoint: Endpoint,
    /// Bytes received but not yet forming complete lines.
    buf: Vec<u8>,
    /// Accumulated `data:` payload of the event currently being read. SSE allows
    /// an event's data to span multiple lines and multiple chunks.
    pending_data: String,
    /// Name of the current SSE event, used to disambiguate usage shapes.
    outcome: ScanOutcome,
}

impl SseUsageScanner {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            buf: Vec::with_capacity(8 * 1024),
            pending_data: String::new(),
            outcome: ScanOutcome::default(),
        }
    }

    /// Feed one chunk. Chunk boundaries are arbitrary; events may be split.
    pub fn feed(&mut self, chunk: &[u8]) {
        if self.outcome.truncated {
            return;
        }

        // Move the buffer out so parsing can borrow chunk data without fighting
        // the borrow checker over `self`.
        let mut buf = std::mem::take(&mut self.buf);
        buf.extend_from_slice(chunk);

        let mut consumed = 0usize;
        loop {
            let rel = memchr::memchr(b'\n', &buf[consumed..]);
            let Some(rel) = rel else { break };

            let line_end = consumed + rel;
            // Line content without the newline, and without a trailing CR.
            let mut line = &buf[consumed..line_end];
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            consumed = line_end + 1;

            self.absorb_line(line);
        }

        if consumed > 0 {
            buf.drain(..consumed);
        }

        if buf.len() > MAX_EVENT_BYTES || self.pending_data.len() > MAX_EVENT_BYTES {
            // Give up rather than grow without bound. The request is still
            // forwarded and metered; only usage is reported unavailable.
            buf.clear();
            self.pending_data.clear();
            self.outcome.truncated = true;
        }

        self.buf = buf;
    }

    /// Finish the stream: parse whatever is left over.
    ///
    /// Two things can be incomplete at the end, and both must still be read: the
    /// event's terminating blank line (some upstreams end right after a `data:`
    /// line) and the final line's newline (a stream can end mid-line). Dropping
    /// either would lose the usage event that carries the token counts — the
    /// last event is the one that matters most, because it is the one that
    /// reports totals.
    pub fn finish(&mut self) {
        if self.outcome.truncated {
            self.buf.clear();
            return;
        }

        if !self.buf.is_empty() {
            let buf = std::mem::take(&mut self.buf);
            let mut line = buf.as_slice();
            if line.last() == Some(&b'\r') {
                line = &line[..line.len() - 1];
            }
            self.absorb_line(line);
        }

        self.flush_event();
        self.buf.clear();
    }

    /// Apply one complete line of the event stream.
    ///
    /// Shared by [`Self::feed`] and [`Self::finish`] so a line that arrives
    /// without a terminator is parsed exactly like one that has it.
    fn absorb_line(&mut self, line: &[u8]) {
        if line.is_empty() {
            // Blank line terminates the event.
            self.flush_event();
            return;
        }

        if let Some(rest) = line.strip_prefix(b"data:") {
            // A single optional space after the colon is part of the framing.
            let rest = rest.strip_prefix(b" ").unwrap_or(rest);
            if !self.pending_data.is_empty() {
                self.pending_data.push('\n');
            }
            self.pending_data.push_str(&String::from_utf8_lossy(rest));
        }
        // `event:`, `id:`, `retry:` and comment lines carry no usage.
    }

    pub fn outcome(&self) -> &ScanOutcome {
        &self.outcome
    }

    /// Usage observed so far. `None` means no usage event was seen — the caller
    /// must record that as unavailable, never as zero.
    pub fn usage(&self) -> Option<Usage> {
        self.outcome.usage.clone()
    }

    pub fn truncated(&self) -> bool {
        self.outcome.truncated
    }

    fn flush_event(&mut self) {
        if self.pending_data.is_empty() {
            return;
        }
        let data = std::mem::take(&mut self.pending_data);
        self.outcome.events += 1;

        // `[DONE]` is a sentinel, not JSON.
        if data.trim() == "[DONE]" {
            return;
        }

        let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
            return;
        };
        self.outcome.json_events += 1;

        let found = match self.endpoint {
            Endpoint::ChatCompletions => extract_stream_chat_completions_usage(&value),
            Endpoint::Responses => extract_stream_responses_usage(&value),
            Endpoint::Models => None,
        };

        // Later usage supersedes earlier: the final chunk carries the totals.
        if let Some(usage) = found {
            self.outcome.usage = Some(usage);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn chat(endpoint: Endpoint) -> SseUsageScanner {
        SseUsageScanner::new(endpoint)
    }

    #[test]
    fn test_usage_recovered_from_final_chunk() {
        let mut s = chat(Endpoint::ChatCompletions);
        let body = concat!(
            "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
            "data: {\"id\":\"c1\",\"choices\":[],\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\n\n",
            "data: [DONE]\n\n"
        );
        s.feed(body.as_bytes());
        s.finish();

        let u = s.usage().expect("usage must be recovered");
        assert_eq!(u.input_tokens, Some(12));
        assert_eq!(u.output_tokens, Some(7));
        assert_eq!(s.outcome().events, 3);
        assert!(!s.truncated());
    }

    #[test]
    fn test_no_usage_event_yields_none_not_zero() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"a\"}}]}\n\n");
        s.feed(b"data: [DONE]\n\n");
        s.finish();
        assert!(
            s.usage().is_none(),
            "absent usage must stay None so it is recorded as unavailable"
        );
    }

    #[test]
    fn test_event_split_across_chunks_is_reassembled() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {\"id\":\"c1\",\"usa");
        s.feed(b"ge\":{\"prompt_tokens\":5,");
        s.feed(b"\"completion_tokens\":9}}\n\n");
        s.finish();

        let u = s.usage().expect("split event must still be parsed");
        assert_eq!(u.input_tokens, Some(5));
        assert_eq!(u.output_tokens, Some(9));
    }

    #[test]
    fn test_crlf_line_endings() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4}}\r\n\r\n");
        s.finish();
        let u = s.usage().expect("CRLF framing must parse");
        assert_eq!(u.input_tokens, Some(3));
        assert_eq!(u.output_tokens, Some(4));
    }

    #[test]
    fn test_multi_line_data_is_joined() {
        // SSE permits one event's data to be split over several `data:` lines,
        // which the receiver rejoins with newlines. JSON tolerates that
        // whitespace, so the payload still parses.
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"event: chunk\ndata: {\"usage\":\ndata: {\"prompt_tokens\":1,\"completion_tokens\":2}}\n\n");
        s.finish();
        let u = s.usage().expect("multi-line data must parse");
        assert_eq!(u.input_tokens, Some(1));
        assert_eq!(u.output_tokens, Some(2));
    }

    #[test]
    fn test_responses_api_nested_usage() {
        let mut s = chat(Endpoint::Responses);
        s.feed(
            b"event: response.completed\ndata: {\"type\":\"response.completed\",\"response\":{\"usage\":{\"input_tokens\":40,\"output_tokens\":11}}}\n\n",
        );
        s.finish();
        let u = s.usage().expect("nested response.usage must be found");
        assert_eq!(u.input_tokens, Some(40));
        assert_eq!(u.output_tokens, Some(11));
    }

    #[test]
    fn test_responses_api_top_level_usage() {
        let mut s = chat(Endpoint::Responses);
        s.feed(b"data: {\"type\":\"response.completed\",\"usage\":{\"input_tokens\":8,\"output_tokens\":9}}\n\n");
        s.finish();
        let u = s.usage().expect("top-level usage must be found");
        assert_eq!(u.input_tokens, Some(8));
    }

    #[test]
    fn test_cached_tokens_recovered() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(
            b"data: {\"usage\":{\"prompt_tokens\":100,\"completion_tokens\":10,\"prompt_tokens_details\":{\"cached_tokens\":64}}}\n\n",
        );
        s.finish();
        let u = s.usage().unwrap();
        assert_eq!(u.cached_tokens, Some(64));
    }

    #[test]
    fn test_malformed_json_is_skipped_not_fatal() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {this is not json}\n\n");
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":2,\"completion_tokens\":3}}\n\n");
        s.finish();
        let u = s.usage().expect("a bad event must not stop later parsing");
        assert_eq!(u.input_tokens, Some(2));
        assert_eq!(s.outcome().json_events, 1);
    }

    #[test]
    fn test_trailing_event_without_blank_line() {
        // Some upstreams end the body right after a data line.
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":6,\"completion_tokens\":7}}");
        assert!(s.usage().is_none(), "not parsed until the stream ends");
        s.finish();
        let u = s.usage().expect("finish() must parse the trailing event");
        assert_eq!(u.input_tokens, Some(6));
    }

    #[test]
    fn test_oversized_event_is_truncated_not_buffered_forever() {
        let mut s = chat(Endpoint::ChatCompletions);
        // No event terminator, ever.
        let junk = vec![b'x'; 64 * 1024];
        for _ in 0..8 {
            s.feed(&junk);
        }
        assert!(
            s.truncated(),
            "must abandon scanning rather than grow unbounded"
        );
        assert!(s.buf.len() <= MAX_EVENT_BYTES);
        // Further input is ignored once truncated, so parsing cannot resume on
        // a partial payload and produce a wrong number.
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":999,\"completion_tokens\":999}}\n\n");
        s.finish();
        assert!(s.usage().is_none());
    }

    #[test]
    fn test_memory_stays_bounded_across_many_events() {
        let mut s = chat(Endpoint::ChatCompletions);
        for i in 0..2_000 {
            let event = format!(
                "data: {{\"id\":\"c{i}\",\"choices\":[{{\"delta\":{{\"content\":\"tok{i}\"}}}}]}}\n\n"
            );
            s.feed(event.as_bytes());
            assert!(
                s.buf.len() < 4 * 1024,
                "processed events must be released, buffer was {}",
                s.buf.len()
            );
        }
        s.finish();
        assert_eq!(s.outcome().events, 2_000);
    }

    #[test]
    fn test_later_usage_supersedes_earlier() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1}}\n\n");
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":50,\"completion_tokens\":60}}\n\n");
        s.finish();
        let u = s.usage().unwrap();
        assert_eq!(u.input_tokens, Some(50), "final totals must win");
        assert_eq!(u.output_tokens, Some(60));
    }

    #[test]
    fn test_comments_and_other_fields_ignored() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b": keep-alive\n\nretry: 1000\n\nid: 42\n\ndata: {\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":5}}\n\n");
        s.finish();
        assert_eq!(s.usage().unwrap().input_tokens, Some(4));
    }
}
