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
//!
//! The cap is charged against the event as a whole, not against whichever
//! prefix of it happens to be sitting in a buffer, so the decision does not
//! depend on where the upstream's writes landed: the same bytes fed as one
//! chunk, byte by byte, or split at any boundary produce the same
//! [`ScanOutcome`].

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
    /// Usage reported by the last event that carried usable counts. `None` means
    /// no usable usage was seen — the caller must record that as unavailable,
    /// never as zero.
    pub usage: Option<Usage>,
    /// Number of events whose payload was JSON we could parse.
    pub json_events: usize,
    /// True when the buffer cap was hit and scanning was abandoned. Usage will
    /// therefore be reported as unavailable.
    pub truncated: bool,
    /// Number of events whose payload did not parse as JSON. SSE requires an
    /// event's `data:` lines to be rejoined, so a payload that is only valid
    /// while its lines stay apart lands here; counted so a dropped event is
    /// diagnosable instead of invisible.
    pub unparsable_events: usize,
}

impl ScanOutcome {
    /// True when the scanner abandoned the stream mid-event, so usage is
    /// reported as unavailable. Kept observable so the caller can log the gap.
    pub fn truncated(&self) -> bool {
        self.truncated
    }

    /// Usage observed, or `None` when the stream never reported a usable count.
    pub fn usage(&self) -> Option<&Usage> {
        self.usage.as_ref()
    }

    /// Number of complete events whose payload parsed as JSON.
    pub fn json_events(&self) -> usize {
        self.json_events
    }

    /// Number of events whose joined payload was not JSON.
    pub fn unparsable_events(&self) -> usize {
        self.unparsable_events
    }
}

/// Incremental SSE scanner that recovers usage from a streaming response.
pub struct SseUsageScanner {
    endpoint: Endpoint,
    /// Bytes of the current line received without a terminator yet.
    line_buf: Vec<u8>,
    /// Accumulated `data:` payload of the event currently being read. SSE allows
    /// an event's data to span multiple lines and multiple chunks.
    pending_data: String,
    /// Bytes of the current event seen so far, terminators included; reset when
    /// a blank line ends it. Tracked apart from the buffers because the cap
    /// bounds the event, not the prefixes of it that are currently retained.
    event_bytes: usize,
    /// Set when the previous byte was a CR terminator. The LF of a CRLF pair is
    /// framing, not the start of a new (empty) line, so it is skipped.
    after_cr: bool,
    outcome: ScanOutcome,
}

impl SseUsageScanner {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            line_buf: Vec::with_capacity(8 * 1024),
            pending_data: String::new(),
            event_bytes: 0,
            after_cr: false,
            outcome: ScanOutcome::default(),
        }
    }

    /// Feed one chunk. Chunk boundaries are arbitrary; events may be split.
    pub fn feed(&mut self, chunk: &[u8]) {
        if self.outcome.truncated {
            return;
        }

        let mut rest = chunk;
        while !rest.is_empty() {
            // SSE terminates a line with CR, LF or CRLF. When a CR ended the
            // previous line, an LF at the head of what remains is the second
            // half of that terminator, not an empty line of its own.
            if self.after_cr {
                self.after_cr = false;
                if rest[0] == b'\n' {
                    rest = &rest[1..];
                    continue;
                }
            }

            let Some(idx) = memchr::memchr2(b'\n', b'\r', rest) else {
                // No terminator in the rest of this chunk: hold the tail as a
                // partial line, paying for it against the event cap first so an
                // oversized line is never buffered.
                if !self.reserve(rest.len()) {
                    return;
                }
                self.line_buf.extend_from_slice(rest);
                return;
            };

            let terminator = rest[idx];
            let line = &rest[..idx];
            // The terminator is part of the event too; a CRLF counts once, as
            // its CR — the LF is consumed by the branch above.
            if !self.reserve(line.len() + 1) {
                return;
            }
            rest = &rest[idx + 1..];
            self.after_cr = terminator == b'\r';

            if self.line_buf.is_empty() {
                self.absorb_line(line);
            } else {
                let mut joined = std::mem::take(&mut self.line_buf);
                joined.extend_from_slice(line);
                self.absorb_line(&joined);
            }
        }
    }

    /// Finish the stream, parse whatever is left over, and report the outcome.
    ///
    /// Two things can be incomplete at the end, and both must still be read: the
    /// event's terminating blank line (some upstreams end right after a `data:`
    /// line) and the final line's newline (a stream can end mid-line). Dropping
    /// either would lose the usage event that carries the token counts — the
    /// last event is the one that matters most, because it is the one that
    /// reports totals.
    pub fn finish(&mut self) -> ScanOutcome {
        if !self.outcome.truncated {
            if !self.line_buf.is_empty() {
                let line = std::mem::take(&mut self.line_buf);
                self.absorb_line(&line);
            }
            self.flush_event();
        }

        // A truncated scan is not evidence of anything: whatever was read before
        // the overflow is discarded, so a stale count is never written as
        // `available`. Stated here as well as in `truncate` so the contract
        // holds for every path out of this type.
        if self.outcome.truncated {
            self.outcome.usage = None;
        }

        self.line_buf.clear();
        self.pending_data.clear();
        self.after_cr = false;
        self.event_bytes = 0;
        self.outcome.clone()
    }

    /// Account `n` more bytes to the event being read, giving up once the cap is
    /// exceeded. Returns whether the bytes were accepted.
    fn reserve(&mut self, n: usize) -> bool {
        self.event_bytes = self.event_bytes.saturating_add(n);
        if self.event_bytes > MAX_EVENT_BYTES {
            self.truncate();
            return false;
        }
        true
    }

    /// Abandon scanning, dropping what was buffered and any usage already seen.
    ///
    /// The request is still forwarded and metered; only usage is reported
    /// unavailable, because a stream that was read in part cannot say what it
    /// reported.
    fn truncate(&mut self) {
        self.outcome.usage = None;
        self.outcome.truncated = true;
        self.line_buf.clear();
        self.pending_data.clear();
        self.after_cr = false;
        self.event_bytes = 0;
    }

    /// Apply one complete line of the event stream.
    ///
    /// Shared by [`Self::feed`] and [`Self::finish`] so a line that arrives
    /// without a terminator is parsed exactly like one that has it.
    fn absorb_line(&mut self, line: &[u8]) {
        if line.is_empty() {
            // Blank line terminates the event; the next one counts from zero.
            self.flush_event();
            self.event_bytes = 0;
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

        // `[DONE]` is a sentinel, not JSON.
        if data.trim() == "[DONE]" {
            return;
        }

        let Ok(value) = serde_json::from_str::<serde_json::Value>(&data) else {
            // Not JSON: either a malformed frame, or an event whose `data:`
            // lines — rejoined as SSE requires — only parse when kept apart.
            // Either way the payload is dropped, so count it: a silent drop is
            // a usage gap nobody can see.
            self.outcome.unparsable_events += 1;
            return;
        };
        self.outcome.json_events += 1;

        let found = match self.endpoint {
            Endpoint::ChatCompletions => extract_stream_chat_completions_usage(&value),
            Endpoint::Responses => extract_stream_responses_usage(&value),
            Endpoint::Models => None,
        };

        // Later usage supersedes earlier: the final chunk carries the totals.
        // An event that yields no usable count is not a new observation and
        // must not erase a count already recovered.
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

    /// Feed `bytes` in the given cuts and report the outcome.
    fn scan_in_pieces(bytes: &[u8], cuts: &[usize]) -> ScanOutcome {
        let mut s = chat(Endpoint::ChatCompletions);
        let mut start = 0;
        for &cut in cuts {
            s.feed(&bytes[start..cut]);
            start = cut;
        }
        s.feed(&bytes[start..]);
        s.finish()
    }

    /// Feed `bytes` one byte at a time and report the outcome.
    fn scan_byte_at_a_time(bytes: &[u8]) -> ScanOutcome {
        let mut s = chat(Endpoint::ChatCompletions);
        for byte in bytes {
            s.feed(std::slice::from_ref(byte));
        }
        s.finish()
    }

    /// Every way of cutting `bytes` into two chunks, plus one byte at a time.
    ///
    /// Only for inputs small enough that quadratic re-scanning is free; the
    /// over-cap cases rely on [`scan_byte_at_a_time`] instead.
    fn assert_frame_independent(bytes: &[u8]) -> ScanOutcome {
        let whole = scan_in_pieces(bytes, &[]);
        for split in 1..bytes.len() {
            assert_eq!(
                scan_in_pieces(bytes, &[split]),
                whole,
                "splitting at {split} changed the outcome"
            );
        }
        assert_eq!(
            scan_byte_at_a_time(bytes),
            whole,
            "byte-at-a-time changed the outcome"
        );
        whole
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
        assert_eq!(s.outcome().json_events, 2);
        assert_eq!(s.outcome().unparsable_events, 0);
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
    fn test_crlf_split_across_chunks() {
        // The LF of a CRLF can land in the next chunk; it is framing, not an
        // empty line, so it must not terminate the event early.
        let body = b"data: {\"usage\":{\"prompt_tokens\":3,\"completion_tokens\":4}}\r\n\r\n";
        let outcome = assert_frame_independent(body);
        assert_eq!(outcome.usage.expect("usage").input_tokens, Some(3));
        assert_eq!(outcome.json_events, 1);
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
        assert_eq!(
            s.outcome().unparsable_events,
            1,
            "an unparseable payload must be counted, not silently dropped"
        );
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
        assert!(s.line_buf.len() <= MAX_EVENT_BYTES);
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
                s.line_buf.len() < 4 * 1024 && s.pending_data.len() < 4 * 1024,
                "processed events must be released, line={} data={}",
                s.line_buf.len(),
                s.pending_data.len()
            );
        }
        s.finish();
        assert_eq!(s.outcome().json_events, 2_000);
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
    fn test_usage_event_without_usable_tokens_does_not_erase_the_last_count() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\n\n");
        // Later events that mention usage but carry no usable number: a null
        // placeholder, an empty object, and a string-valued count.
        s.feed(b"data: {\"usage\":null}\n\n");
        s.feed(b"data: {\"usage\":{}}\n\n");
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":\"twelve\"}}\n\n");
        s.finish();
        let u = s
            .usage()
            .expect("a token-less usage event must not erase a real count");
        assert_eq!(u.input_tokens, Some(12));
        assert_eq!(u.output_tokens, Some(7));
    }

    #[test]
    fn test_truncation_discards_usage_seen_before_the_oversized_event() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\n\n");
        // An event that never terminates: the scan is abandoned part-way, so
        // the numbers above describe a prefix of a stream we could not read.
        let junk = vec![b'x'; 64 * 1024];
        for _ in 0..8 {
            s.feed(&junk);
        }
        assert!(s.truncated(), "the cap must be enforced");
        assert!(
            s.usage().is_none(),
            "a truncated scan must not report the stale count as available"
        );
        assert!(s.finish().usage.is_none());
    }

    #[test]
    fn test_comments_and_other_fields_ignored() {
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b": keep-alive\n\nretry: 1000\n\nid: 42\n\ndata: {\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":5}}\n\n");
        s.finish();
        assert_eq!(s.usage().unwrap().input_tokens, Some(4));
    }

    #[test]
    fn test_bare_cr_framing_is_extracted() {
        // The SSE spec accepts a bare CR terminator. Splitting only on LF
        // collapses this whole stream into one line and loses the usage.
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(b"data: {\"id\":\"c1\"}\r\rdata: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\r\rdata: [DONE]\r\r");
        s.finish();
        let u = s.usage().expect("bare-CR framing must still expose usage");
        assert_eq!(u.input_tokens, Some(12));
        assert_eq!(u.output_tokens, Some(7));
        assert_eq!(s.outcome().json_events, 2);
        assert!(!s.truncated(), "CR framing is not a truncation");
    }

    #[test]
    fn test_unparsable_joined_payload_is_counted() {
        // Spec-correct joining makes this payload non-JSON: `[DONE]` is glued
        // to the usage object. The drop must be visible to the caller.
        let mut s = chat(Endpoint::ChatCompletions);
        s.feed(
            b"data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\ndata: [DONE]\n\n",
        );
        s.finish();
        assert!(s.usage().is_none(), "the joined payload is not JSON");
        assert_eq!(s.outcome().unparsable_events, 1);
        assert_eq!(s.outcome().json_events, 0);
    }

    #[test]
    fn test_outcome_is_frame_independent_for_a_small_stream() {
        let body = concat!(
            "data: {\"id\":\"c1\",\"choices\":[{\"delta\":{\"content\":\"hi\"}}]}\n\n",
            "data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\n\n",
            "data: [DONE]\n\n"
        )
        .as_bytes();
        let outcome = assert_frame_independent(body);
        assert_eq!(outcome.usage.expect("usage").input_tokens, Some(12));
        assert_eq!(outcome.json_events, 2);
        assert!(!outcome.truncated);
    }

    #[test]
    fn test_oversized_event_outcome_is_frame_independent() {
        // An over-cap event that arrives complete in one chunk used to be
        // processed, while the same bytes split across frames truncated. The
        // decision must not depend on how the upstream happened to write.
        let mut body =
            String::from("data: {\"usage\":{\"prompt_tokens\":12,\"completion_tokens\":7}}\n\n");
        body.push_str("data: {\"note\":\"");
        body.push_str(&"x".repeat(MAX_EVENT_BYTES + 1));
        body.push_str("\"}\n\n");
        body.push_str("data: [DONE]\n\n");
        let bytes = body.as_bytes();

        let mut one_chunk = chat(Endpoint::ChatCompletions);
        one_chunk.feed(bytes);
        let outcome = one_chunk.finish();

        assert!(outcome.truncated, "an over-cap event must truncate");
        assert!(
            outcome.usage.is_none(),
            "an over-cap event must not leave a stale count"
        );
        assert_eq!(
            scan_byte_at_a_time(bytes),
            outcome,
            "byte-at-a-time changed the outcome of an over-cap stream"
        );
        // The cap bounds what is retained, not just what is accepted.
        assert!(one_chunk.line_buf.len() <= MAX_EVENT_BYTES);
        assert!(one_chunk.pending_data.len() <= MAX_EVENT_BYTES);
    }
}
