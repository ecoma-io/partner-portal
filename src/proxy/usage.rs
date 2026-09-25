//! Usage extraction from OpenAI-compatible responses

use crate::ledger::{Usage, UsageStatus};
use serde_json::Value;

/// Extract usage from a chat completions response body
pub fn extract_chat_completions_usage(body: &Value) -> Usage {
    let usage = body.get("usage");

    match usage {
        Some(usage_obj) => {
            let input_tokens = usage_obj.get("prompt_tokens").and_then(|v| v.as_u64());
            let output_tokens = usage_obj.get("completion_tokens").and_then(|v| v.as_u64());

            // Cache information can be in prompt_tokens_details
            let cached_tokens = usage_obj
                .get("prompt_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|v| v.as_u64());

            Usage::new(input_tokens, output_tokens, cached_tokens)
        }
        None => Usage::default(),
    }
}

/// Extract usage from a responses API body
pub fn extract_responses_usage(body: &Value) -> Usage {
    let usage = body.get("usage");

    match usage {
        Some(usage_obj) => {
            let input_tokens = usage_obj
                .get("input_tokens")
                .or_else(|| usage_obj.get("prompt_tokens"))
                .and_then(|v| v.as_u64());
            let output_tokens = usage_obj
                .get("output_tokens")
                .or_else(|| usage_obj.get("completion_tokens"))
                .and_then(|v| v.as_u64());

            // Cache in input_tokens_details
            let cached_tokens = usage_obj
                .get("input_tokens_details")
                .and_then(|d| d.get("cached_tokens"))
                .and_then(|v| v.as_u64());

            Usage::new(input_tokens, output_tokens, cached_tokens)
        }
        None => Usage::default(),
    }
}

/// The places a streaming event may report usage, in the order they are tried.
///
/// Both the top level and `response.usage` occur across providers, so both are
/// candidates for either endpoint. Only objects are yielded: an absent, `null`
/// or non-object value falls through to the next candidate rather than
/// short-circuiting it — a provider that emits `"usage": null` at the top level
/// alongside the real numbers nested under `response` must still be metered.
fn stream_usage_objects(event: &Value) -> impl Iterator<Item = &serde_json::Map<String, Value>> {
    [
        event.get("usage"),
        event
            .get("response")
            .and_then(|response| response.get("usage")),
    ]
    .into_iter()
    .flatten()
    .filter_map(Value::as_object)
}

/// A usage object counts as observed only when at least one token count is a
/// number.
///
/// An object that carries nothing usable — empty, or all values non-numeric —
/// is not evidence and yields `None`, so it can neither be recorded as a real
/// count nor replace one already recovered (invariant 3).
fn observed_usage(
    input_tokens: Option<u64>,
    output_tokens: Option<u64>,
    cached_tokens: Option<u64>,
) -> Option<Usage> {
    if input_tokens.is_some() || output_tokens.is_some() || cached_tokens.is_some() {
        Some(Usage::new(input_tokens, output_tokens, cached_tokens))
    } else {
        None
    }
}

/// Extract usage from a streaming chunk (Chat Completions SSE).
///
/// Chat Completions reports usage in the final chunk at the top level; some
/// OpenAI-compatible providers wrap the same object under `response`, so both
/// shapes are tried. A candidate is skipped unless it carries a usable count.
pub fn extract_stream_chat_completions_usage(chunk: &Value) -> Option<Usage> {
    stream_usage_objects(chunk).find_map(|usage| {
        observed_usage(
            usage.get("prompt_tokens").and_then(Value::as_u64),
            usage.get("completion_tokens").and_then(Value::as_u64),
            usage
                .get("prompt_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
                .and_then(Value::as_u64),
        )
    })
}

/// Extract usage from a streaming event (Responses API SSE).
///
/// The Responses API reports usage in the terminal `response.completed` /
/// `response.incomplete` event, either at the top level or nested under
/// `response`. Both shapes occur across providers, so both are accepted; a
/// candidate that yields no usable count falls through to the next one.
pub fn extract_stream_responses_usage(event: &Value) -> Option<Usage> {
    stream_usage_objects(event).find_map(|usage| {
        observed_usage(
            usage
                .get("input_tokens")
                .or_else(|| usage.get("prompt_tokens"))
                .and_then(Value::as_u64),
            usage
                .get("output_tokens")
                .or_else(|| usage.get("completion_tokens"))
                .and_then(Value::as_u64),
            usage
                .get("input_tokens_details")
                .and_then(|details| details.get("cached_tokens"))
                .and_then(Value::as_u64),
        )
    })
}

/// Extract model from a chat completions request
pub fn extract_model_from_request(body: &Value) -> String {
    body.get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string()
}

/// Merge two usage objects (for streaming where usage comes in final chunk)
pub fn merge_usage(base: &Usage, incoming: &Usage) -> Usage {
    Usage::new(
        incoming.input_tokens.or(base.input_tokens),
        incoming.output_tokens.or(base.output_tokens),
        incoming.cached_tokens.or(base.cached_tokens),
    )
}

/// Determine model from a responses request
pub fn extract_responses_model(body: &Value) -> String {
    body.get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string()
}

/// Deterministic status when usage is unavailable (never fabricates)
pub fn usage_status_never_fabricate(usage: &Usage) -> UsageStatus {
    usage.status()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_extract_chat_completions_usage() {
        let body = json!({
            "id": "chatcmpl-123",
            "usage": {
                "prompt_tokens": 100,
                "completion_tokens": 50,
                "total_tokens": 150,
                "prompt_tokens_details": {
                    "cached_tokens": 20
                }
            }
        });

        let usage = extract_chat_completions_usage(&body);
        assert_eq!(usage.input_tokens, Some(100));
        assert_eq!(usage.output_tokens, Some(50));
        assert_eq!(usage.cached_tokens, Some(20));
        assert_eq!(usage.status(), UsageStatus::Available);
    }

    #[test]
    fn test_extract_chat_completions_no_usage() {
        let body = json!({
            "id": "chatcmpl-123",
            "choices": []
        });

        let usage = extract_chat_completions_usage(&body);
        assert_eq!(usage.input_tokens, None);
        assert_eq!(usage.output_tokens, None);
        assert_eq!(usage.status(), UsageStatus::Unavailable);
    }

    #[test]
    fn test_extract_responses_usage() {
        let body = json!({
            "id": "resp-123",
            "usage": {
                "input_tokens": 80,
                "output_tokens": 30,
                "input_tokens_details": {
                    "cached_tokens": 5
                }
            }
        });

        let usage = extract_responses_usage(&body);
        assert_eq!(usage.input_tokens, Some(80));
        assert_eq!(usage.output_tokens, Some(30));
        assert_eq!(usage.cached_tokens, Some(5));
    }

    #[test]
    fn test_extract_stream_chat_completions_usage() {
        let chunk = json!({
            "id": "chatcmpl-123",
            "choices": [],
            "usage": {
                "prompt_tokens": 50,
                "completion_tokens": 25
            }
        });

        let usage = extract_stream_chat_completions_usage(&chunk).unwrap();
        assert_eq!(usage.input_tokens, Some(50));
        assert_eq!(usage.output_tokens, Some(25));
    }

    #[test]
    fn test_extract_stream_responses_usage() {
        let event = json!({
            "type": "response.completed",
            "usage": {
                "input_tokens": 60,
                "output_tokens": 40
            }
        });

        let usage = extract_stream_responses_usage(&event).unwrap();
        assert_eq!(usage.input_tokens, Some(60));
        assert_eq!(usage.output_tokens, Some(40));
    }

    #[test]
    fn test_extract_stream_responses_nested_usage() {
        // The Responses API wraps the terminal event's payload under `response`.
        let event = json!({
            "type": "response.completed",
            "response": {
                "id": "resp-1",
                "usage": { "input_tokens": 12, "output_tokens": 34 }
            }
        });
        let usage = extract_stream_responses_usage(&event).unwrap();
        assert_eq!(usage.input_tokens, Some(12));
        assert_eq!(usage.output_tokens, Some(34));
    }

    #[test]
    fn test_extract_stream_responses_null_usage_does_not_mask_nested() {
        // A present-but-null top-level key must fall through to the nested
        // shape, not short-circuit it into `None`.
        let event = json!({
            "type": "response.completed",
            "usage": null,
            "response": {
                "usage": { "input_tokens": 40, "output_tokens": 11 }
            }
        });
        let usage =
            extract_stream_responses_usage(&event).expect("nested usage must still be read");
        assert_eq!(usage.input_tokens, Some(40));
        assert_eq!(usage.output_tokens, Some(11));
    }

    #[test]
    fn test_extract_stream_responses_non_object_usage_falls_through() {
        // Same for any non-object: a string, an array, a number.
        for masked in [json!("n/a"), json!([]), json!(0)] {
            let event = json!({
                "type": "response.completed",
                "usage": masked,
                "response": {
                    "usage": { "input_tokens": 7, "output_tokens": 3 }
                }
            });
            let usage = extract_stream_responses_usage(&event)
                .expect("a non-object usage must not mask the nested one");
            assert_eq!(usage.input_tokens, Some(7));
            assert_eq!(usage.output_tokens, Some(3));
        }
    }

    #[test]
    fn test_extract_stream_responses_empty_usage_falls_through() {
        // An object with nothing usable is not evidence either: fall through.
        let event = json!({
            "type": "response.completed",
            "usage": {},
            "response": {
                "usage": { "input_tokens": 21, "output_tokens": 5 }
            }
        });
        let usage = extract_stream_responses_usage(&event).expect("empty usage must fall through");
        assert_eq!(usage.input_tokens, Some(21));
        assert_eq!(usage.output_tokens, Some(5));
    }

    #[test]
    fn test_extract_stream_chat_nested_usage() {
        // Chat Completions normally reports usage at the top level, but the
        // wrapped shape occurs too and must not be missed.
        let chunk = json!({
            "id": "chatcmpl-123",
            "choices": [],
            "response": {
                "usage": { "prompt_tokens": 30, "completion_tokens": 12 }
            }
        });
        let usage =
            extract_stream_chat_completions_usage(&chunk).expect("nested usage must be read");
        assert_eq!(usage.input_tokens, Some(30));
        assert_eq!(usage.output_tokens, Some(12));
    }

    #[test]
    fn test_extract_stream_chat_null_usage_does_not_mask_nested() {
        let chunk = json!({
            "id": "chatcmpl-123",
            "choices": [],
            "usage": null,
            "response": {
                "usage": { "prompt_tokens": 30, "completion_tokens": 12 }
            }
        });
        let usage = extract_stream_chat_completions_usage(&chunk)
            .expect("null usage must not mask the nested one");
        assert_eq!(usage.input_tokens, Some(30));
        assert_eq!(usage.output_tokens, Some(12));
    }

    #[test]
    fn test_extract_stream_usage_with_no_usable_count_is_none() {
        // Nothing usable at any level: `None`, so the caller keeps whatever it
        // already had instead of overwriting it with nothing.
        for masked in [
            json!(null),
            json!({}),
            json!("none"),
            json!({"prompt_tokens": "many"}),
        ] {
            let chat_event = json!({ "choices": [], "usage": masked });
            assert!(extract_stream_chat_completions_usage(&chat_event).is_none());
            let responses_event = json!({ "type": "response.completed", "usage": masked });
            assert!(extract_stream_responses_usage(&responses_event).is_none());
        }
    }

    #[test]
    fn test_extract_stream_responses_no_usage_is_none() {
        let event = json!({"type": "response.output_text.delta", "delta": "hi"});
        assert!(extract_stream_responses_usage(&event).is_none());
    }

    #[test]
    fn test_extract_responses_partial_usage_is_not_fabricated() {
        // Only input tokens known: output must stay None (Partial), not 0.
        let body = json!({"usage": {"input_tokens": 9}});
        let usage = extract_responses_usage(&body);
        assert_eq!(usage.input_tokens, Some(9));
        assert_eq!(usage.output_tokens, None);
        assert_eq!(usage.status(), UsageStatus::Partial);
    }

    #[test]
    fn test_usage_ignores_non_numeric_values() {
        // A provider echoing strings must not be coerced into a number.
        let body = json!({"usage": {"prompt_tokens": "100", "completion_tokens": null}});
        let usage = extract_chat_completions_usage(&body);
        assert_eq!(usage.input_tokens, None);
        assert_eq!(usage.output_tokens, None);
        assert_eq!(usage.status(), UsageStatus::Unavailable);
    }

    #[test]
    fn test_merge_usage_prefers_incoming_and_keeps_base() {
        let base = Usage::new(Some(10), None, Some(3));
        let incoming = Usage::new(None, Some(20), None);
        let merged = merge_usage(&base, &incoming);
        assert_eq!(merged.input_tokens, Some(10));
        assert_eq!(merged.output_tokens, Some(20));
        assert_eq!(merged.cached_tokens, Some(3));
    }

    #[test]
    fn test_extract_stream_chunk_no_usage() {
        let chunk = json!({
            "id": "chatcmpl-123",
            "choices": [{
                "delta": {"content": "hello"}
            }]
        });

        assert!(extract_stream_chat_completions_usage(&chunk).is_none());
    }

    #[test]
    fn test_usage_never_fabricated() {
        // Missing usage must not be fabricated as 0
        let empty = Usage::default();
        assert_eq!(
            usage_status_never_fabricate(&empty),
            UsageStatus::Unavailable
        );
        assert_eq!(empty.input_tokens, None);
        assert_eq!(empty.output_tokens, None);
    }
}
