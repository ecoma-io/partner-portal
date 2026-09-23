# 0009 — Streaming stays incremental: chunk scanning, bounded buffers

Status: accepted

## Context

Usage arrives at the end of a stream. The straightforward implementation —
collect the body, parse it, forward it — is incompatible with what the endpoint is
for: time-to-first-token is the latency a consumer feels, and buffering converts it
into time-to-last-token.

But incremental forwarding puts a scanner on the hot path, and the hot path has an
adversary: an upstream that never terminates an event, sends one enormous event, or
simply stalls mid-stream. A scanner that buffers until it finds a terminator is a
memory-exhaustion primitive.

## Decision

* Response frames are forwarded to the client **as they arrive**; the body is never
  collected to recover usage. The upstream response body is a
  `hyper::body::Incoming` stream that is consumed frame by frame and yielded
  onwards.
* Usage is recovered by feeding each data frame to an `SseUsageScanner`, which:
  * reassembles an event whose `data:` lines are split across chunk boundaries;
  * ignores `event:`/`id:`/`retry:`/comment lines and `[DONE]`;
  * keeps the **last** usage it sees, because the terminal event carries the
    totals;
  * caps the partial-event buffer at 256 KiB, and on overflow *abandons* scanning
    and marks the outcome truncated — the request continues and its usage is
    recorded as `unavailable`, never guessed;
  * parses whatever is left at end-of-stream, including a final event with no
    terminating blank line, because that event is the one carrying the totals.
* TTFT is measured at the **first data frame**, not at the first byte of the
  response (headers are not a token).
* Mid-stream stalls are bounded by `upstream.timeout_secs` as an **idle** timeout
  between frames, not as a total-duration cap — a long generation is legitimate.
* Non-streaming responses *are* buffered (they must be parsed whole), under a hard
 32 MiB cap. Over the cap the body is truncated and usage is recorded as
  unavailable.
* `Accept-Encoding` is dropped on the upstream request. A compressed body cannot be
  scanned, and every streaming request would then be metered as unavailable.
* Trailer frames are dropped rather than forwarded: this API does not use them and
  forwarding would require re-framing.

## Alternatives considered

* **Buffer the stream, then forward** — rejected: it destroys the product's reason
  to exist for streaming consumers.
* **A second pass over a tee'd copy** — rejected: same latency and memory cost as
  buffering, plus a lifetime problem.
* **Parse incrementally with a streaming JSON parser over the whole response** —
  rejected: SSE framing is the actual structure; parsing events as they complete is
  simpler and cheaper.
* **Cap the whole stream instead of a single event** — rejected: legitimate long
  generations would be cut off; the risk is one unbounded event, not total volume
  (total volume is the client's own consumption).
* **Total-duration timeout on streams** — rejected: it fails long, healthy
  generations. The idle timeout catches the failure that matters (a stalled
  upstream).
* **Forwarding `Accept-Encoding` and giving up on usage when compressed** — trade
  explicitly made the other way: correct accounting beats a bandwidth saving on an
  API whose responses are already streamed as text.

## Consequences

* Memory per in-flight streaming request is bounded by the event cap plus
  per-frame overhead, independent of response length.
* A stream whose usage cannot be read is still forwarded and still metered — as
  `completed` with `usage_status='unavailable'` — so the request is visible and its
  gap is visible with it.
* Compression is lost between proxy and upstream. The consumer's own connection to
  the proxy is uncompressed regardless, since the body is re-framed by hyper.
* The scanner is the most test-dense module in the repository for a reason: every
  boundary case it mishandles becomes a wrong number in the ledger.

## Evidence

* `src/proxy/handler.rs` — module docs, `stream_response` (the frame loop, idle
  timeout, TTFT measurement), `StreamMeter::observe`, `MAX_BUFFERED_RESPONSE`,
  `collect_capped`.
* `src/proxy/sse_scan.rs` — module docs, `MAX_EVENT_BYTES`, `feed`, `finish`,
  `absorb_line`, `flush_event`, and the bounded-memory test
  `test_memory_stays_bounded_across_many_events`.
* `src/proxy/client.rs` — module docs ("`Accept-Encoding` is dropped"),
  `headers.remove(header::ACCEPT_ENCODING)`, and `proxy` returning
  `hyper::Response<hyper::body::Incoming>` without collecting.
* Tests: `test_event_split_across_chunks_is_reassembled`,
  `test_trailing_event_without_blank_line`,
  `test_oversized_event_is_truncated_not_buffered_forever`,
  `test_completed_stream_records_usage_and_ttft`.
