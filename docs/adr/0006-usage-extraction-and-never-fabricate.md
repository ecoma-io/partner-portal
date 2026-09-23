# 0006 — Per-endpoint usage extraction, and never fabricating usage

Status: accepted

## Context

Token counts come from the provider, in a JSON shape that differs per endpoint and
per provider. They are also frequently absent: a stream that ends without its
terminal usage event, a provider that only reports usage on request, an error
response, a body that is not JSON at all.

The tempting normalisation is to write `0` — the columns are integers, `0` sums
cleanly, and dashboards render it without a null check. It is also wrong in the
one way that matters: `0` and "the provider told us nothing" are different facts,
and a total that silently mixes them is a number that cannot be defended in a
billing conversation.

There are three endpoint shapes to read, plus two streaming shapes that differ
from their non-streaming counterparts: Chat Completions reports `prompt_tokens` /
`completion_tokens` with cache under `prompt_tokens_details.cached_tokens`; the
Responses API reports `input_tokens` / `output_tokens` with cache under
`input_tokens_details.cached_tokens`, and wraps its terminal event's payload under
`response` in some providers but not others.

## Decision

* Extraction is **per endpoint**, in one module, with an explicit shape for each:
  `extract_chat_completions_usage`, `extract_responses_usage`, and the streaming
  variants `extract_stream_chat_completions_usage` /
  `extract_stream_responses_usage`.
* Missing usage is represented as `Options` all the way down: `Usage { input_tokens:
  Option<u64>, output_tokens: Option<u64>, cached_tokens: Option<u64> }`.
* The persisted `usage_status` is **derived**, not asserted: both token counts
  present ⇒ `available`; exactly one ⇒ `partial`; neither ⇒ `unavailable`. No code
  path writes a status independently of the values it describes.
* A streaming event that contains no usage field yields `None`, not a zeroed
  `Usage` — the distinction reaches the ledger as `NULL` columns.
* The schema enforces the shape: token columns are `NULL`-able and `CHECK`ed
  non-negative. Negative tokens are rejected by the database, not merely by the
  extractor.
* The rollup sums what was reported: `COALESCE(?, 0)` into
  `total_input_tokens` etc., while the raw row keeps `NULL`. A total is therefore a
  sum of reported usage, and the count of requests whose usage was unavailable is a
  separate, visible number (`unavailable_usage_count` in the summary, computed
  from the raw ledger where the `NULL`/`0` distinction lives).
* Nothing estimates. There is no tokenizer in this binary, and a request that
  fails before usage is observed is recorded with `NULL` tokens and a failure
  status.
* A partial stream keeps what was observed and leaves the rest unavailable; a
  truncated scan (over the 256 KiB event cap) discards usage entirely and logs a
  warning, rather than reporting a count it might have mis-parsed.

## Alternatives considered

* **Write `0` for missing usage** — rejected: it is indistinguishable from a
  genuinely zero-token request and corrupts every total that includes it.
* **Estimate with a tokenizer** — rejected: an estimate that looks like a
  measurement is worse than an admitted gap, and it would make the ledger's numbers
  unauditable.
* **A single permissive extractor that walks the JSON for any `*_tokens` key** —
  rejected: it silently accepts fields with different meanings across providers and
  endpoints (input vs. prompt, cached vs. reasoning), producing plausible wrong
  numbers.
* **A non-nullable token column defaulting to `0`** — rejected: schema-level
  pressure to fabricate.
* **Failing the request when usage is absent** — rejected: the provider's answer is
  what the consumer paid for; refusing to relay it because metering is incomplete
  would be a worse outcome than recording the gap honestly.
* **Reporting the truncated scan's partial parse** — rejected: a mis-parsed count
  is a fabricated one.

## Consequences

* Every consumer of usage must handle "unknown", which is deliberate: the dashboard
  renders `NULL` tokens as an empty value and surfaces the unavailable count.
* Operators can see metering coverage — `unavailable_usage_count` and the
  `usage_status` column — instead of inferring it.
* `Usage::total()` returns `None` unless *both* counts are present, so a partial
  usage can never be summed into a total that looks complete.

## Evidence

* `src/proxy/usage.rs` — the four extractors, `merge_usage`,
  `usage_status_never_fabricate`, and tests including
  `test_usage_ignores_non_numeric_values` (a provider echoing strings must not be
  coerced) and `test_extract_responses_partial_usage_is_not_fabricated`.
* `src/ledger/types.rs::Usage::status` / `total`.
* `src/ledger/schema.sql` — `input_tokens INTEGER` (nullable), the
  `CHECK (input_tokens IS NULL OR input_tokens >= 0)` constraints, the
  `usage_status IN ('available','unavailable','partial')` constraint, and the
  comment "never a fabricated zero standing in for 'unknown'".
* `src/ledger/writer.rs::upsert_hourly` — `COALESCE`-style sums with the raw row
  keeping `NULL`.
* `src/proxy/handler.rs` — `extract_usage` returns `Usage::default()` for an
  unparseable body; `test_extract_usage_unparseable_body_is_unavailable_not_zero`.
* `src/dashboard/api.rs` — `unavailable_usage_count`, computed as
  `usage_status <> 'available' AND request_status <> 'in_flight'`.
* `src/proxy/sse_scan.rs` — `usage() -> Option<Usage>`, truncation handling.
