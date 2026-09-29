//! Shared harness for the integration and fault suites.
//!
//! Each file under `tests/` is its own crate, so this module is included with
//! `#[path = "../common/mod.rs"] mod common;` rather than being a test target
//! itself (`tests/common/mod.rs` has no `main.rs`, so Cargo does not treat it as
//! one).
//!
//! The harness provides four things:
//!
//! 1. [`MockUpstream`] — a real HTTP/1.1 server on an ephemeral port that speaks
//!    enough of the OpenAI wire format to exercise every branch of the proxy,
//!    including the misbehaviours (hangs, aborts, malformed JSON, no usage).
//!    Every request it receives is recorded so tests can assert what the proxy
//!    forwarded, with which headers and which credential.
//! 2. [`TestServer`] — the real compiled binary as a child process, configured
//!    through a temp-dir `config.yaml` and a temp-dir SQLite file, with
//!    readiness-gated startup and signal helpers. Its API keys are *seeded rows*
//    written by [`seed_keys`] before the child starts, not configuration: the
//!    harness hands the child the same [`TEST_SECRET`] the store hashes with, so
//!    a plaintext issued here authenticates there.
//! 3. [`TestClient`] — a hyper client with buffered and streaming reads.
//! 4. Ledger readers — the tests inspect SQLite directly, because the ledger is
//!    the thing under test and the dashboard API is not a faithful proxy for
//!    every column (`NULL` versus `0`, `in_flight` rows, duplicate ids).
//!
//! Everything here is deterministic: no fixed sleeps to "wait for the server",
//! no bound port numbers, no reliance on wall-clock alignment.

// A single test crate uses a subset of the harness; unused helpers must not
// turn into `-D warnings` failures in the crates that do not exercise them.
#![allow(dead_code)]

use std::fs::File;
use std::net::TcpListener;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitStatus, Stdio};
use std::sync::atomic::AtomicUsize;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::Router;
use axum::body::Body;
use axum::response::Response;
use axum::routing::any;
use bytes::Bytes;
use http::{HeaderMap, Method, StatusCode};
use http_body_util::{BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::Client;
use hyper_util::client::legacy::connect::HttpConnector;
use hyper_util::rt::TokioExecutor;
use rusqlite::Connection;
use serde_json::{Value, json};

/// The compiled binary under test.
pub const BIN: &str = env!("CARGO_BIN_EXE_partner-portal");

/// Models the default test key may call — everything the mock upstream
/// accepts, so ordinary tests never trip the strict-per-key allow-list.
const DEFAULT_ALLOWED_MODELS: &[&str] = &[
    "gpt-4o",
    "gpt-4o-mini",
    "gpt-5",
    "mock-model",
    "mock-model-mini",
];

/// Default name of the seeded key.
pub const CLIENT_KEY: &str = "primary";
/// Default consumer identity for that key.
pub const CONSUMER: &str = "test-consumer";
/// Credential the proxy is configured to present upstream.
pub const UPSTREAM_KEY: &str = "sk-upstream-secret";

/// The HMAC secret every spawned instance is given.
///
/// A test credential, not a production one, and not a real-looking one: it
/// never leaves this file and protects nothing. The value matters only because
/// the *same* secret must be used by both sides — the harness seeds the rows
/// and the child process hashes the presented token with it — so a child
/// spawned with a different value authenticates nothing, which is exactly what
/// `auth::test_a_different_secret_authenticates_nothing` asserts.
pub const TEST_SECRET: &[u8] = b"a-test-only-hmac-secret-of-32-bytes";

/// How long to wait for a child process to become ready.
pub const READY_TIMEOUT: Duration = Duration::from_secs(25);
/// How long a polling helper waits by default.
pub const WAIT_TIMEOUT: Duration = Duration::from_secs(10);

// ---------------------------------------------------------------------------
// Polling
// ---------------------------------------------------------------------------

/// Poll `f` until it returns true or the deadline passes. Returns whether it
/// became true. This is how tests wait for asynchronous durability: an explicit
/// condition with a bound, never a sleep that "should be long enough".
pub async fn wait_until<F: FnMut() -> bool>(timeout: Duration, mut f: F) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if f() {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

/// As [`wait_until`], but panics with `what` on timeout.
pub async fn await_until<F: FnMut() -> bool>(timeout: Duration, what: &str, f: F) {
    assert!(wait_until(timeout, f).await, "timed out waiting for {what}");
}

/// A free TCP port on the loopback interface.
///
/// The listener is closed before returning, so there is a small window in which
/// another process could take the port. The harness treats that as a startup
/// failure (with the child's log) rather than a flake, and callers can retry.
pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind an ephemeral port");
    listener.local_addr().expect("local addr").port()
}

// ---------------------------------------------------------------------------
// Mock upstream
// ---------------------------------------------------------------------------

/// Terminal shapes the mock can serve for the inference endpoints.
#[derive(Clone, Debug)]
pub enum Behaviour {
    /// Non-streaming chat completion with a full `usage` object.
    ChatJson {
        prompt: u64,
        completion: u64,
        cached: u64,
    },
    /// Non-streaming chat completion with no `usage` key at all.
    ChatJsonWithoutUsage,
    /// Streaming chat completion ending with a usage chunk and `data: [DONE]`.
    ChatStream {
        prompt: u64,
        completion: u64,
        cached: u64,
        events: usize,
        delay_ms: u64,
    },
    /// Streaming chat completion that ends without usage and without `[DONE]`.
    ChatStreamWithoutUsage,
    /// Chat stream that reports usage and then aborts mid-body, after `after`
    /// content events. Used to check that usage observed before a break is kept
    /// and the request is recorded as failed rather than completed.
    StreamAbort { after: usize },
    /// Streaming response that emits one event and then never finishes.
    StreamHang,
    /// Responses API, non-streaming, with usage.
    ResponsesJson {
        input: u64,
        output: u64,
        cached: u64,
    },
    /// Responses API, streaming, ending with a `response.completed` usage event.
    ResponsesStream {
        input: u64,
        output: u64,
        cached: u64,
        events: usize,
        delay_ms: u64,
    },
    /// HTTP 200 with a body that is not JSON.
    MalformedJson,
    /// An error status with an OpenAI-shaped error body.
    Error { status: u16, message: String },
    /// An error status with a body that is not JSON at all.
    ErrorText { status: u16, body: String },
    /// An HTTP 200 body larger than the proxy's buffering cap.
    OversizedResponse { bytes: usize },
}

impl Behaviour {
    /// A streaming completion with no delay, for tests that only care about the
    /// recorded outcome.
    pub fn chat_stream(prompt: u64, completion: u64) -> Self {
        Behaviour::ChatStream {
            prompt,
            completion,
            cached: 0,
            events: 3,
            delay_ms: 0,
        }
    }
}

/// One request as the mock saw it.
#[derive(Clone, Debug)]
pub struct RecordedRequest {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: Bytes,
}

impl RecordedRequest {
    /// Case-insensitive header lookup.
    pub fn header(&self, name: &str) -> Option<&str> {
        let name = name.to_ascii_lowercase();
        self.headers
            .iter()
            .find(|(n, _)| *n == name)
            .map(|(_, v)| v.as_str())
    }

    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }
}

struct MockState {
    behaviour: Mutex<Behaviour>,
    recorded: Mutex<Vec<RecordedRequest>>,
    /// Requests that failed to reach the handler at the transport level.
    transport_errors: AtomicUsize,
}

/// An OpenAI-compatible upstream with deliberately configurable misbehaviour.
pub struct MockUpstream {
    pub addr: std::net::SocketAddr,
    state: Arc<MockState>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    task: Option<tokio::task::JoinHandle<()>>,
}

impl MockUpstream {
    /// Start the mock on an ephemeral port with an initial behaviour.
    pub async fn start(behaviour: Behaviour) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
            .await
            .expect("bind mock upstream");
        let addr = listener.local_addr().expect("mock addr");

        let state = Arc::new(MockState {
            behaviour: Mutex::new(behaviour),
            recorded: Mutex::new(Vec::new()),
            transport_errors: AtomicUsize::new(0),
        });

        let app = Router::new()
            .fallback(any(mock_handler))
            .with_state(state.clone());

        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let handle = tokio::spawn(async move {
            let _ = axum::serve(listener, app)
                .with_graceful_shutdown(async {
                    let _ = rx.await;
                })
                .await;
        });

        Self {
            addr,
            state,
            shutdown: Some(tx),
            task: Some(handle),
        }
    }

    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    pub fn set_behaviour(&self, behaviour: Behaviour) {
        *self.state.behaviour.lock().expect("mock behaviour lock") = behaviour;
    }

    pub fn requests(&self) -> Vec<RecordedRequest> {
        self.state.recorded.lock().expect("mock log lock").clone()
    }

    pub fn request_count(&self) -> usize {
        self.state.recorded.lock().expect("mock log lock").len()
    }

    /// Requests to a path, in arrival order.
    pub fn requests_to(&self, path: &str) -> Vec<RecordedRequest> {
        self.requests()
            .into_iter()
            .filter(|r| r.path == path)
            .collect()
    }

    /// Wait until at least `n` requests have been received.
    pub async fn wait_for_requests(&self, n: usize, timeout: Duration) -> Vec<RecordedRequest> {
        let mut seen = self.requests();
        let ok = wait_until(timeout, || {
            seen = self.requests();
            seen.len() >= n
        })
        .await;
        assert!(
            ok,
            "mock upstream saw {} of {n} expected requests",
            seen.len()
        );
        seen
    }

    /// Stop serving. In-flight responses are dropped when the task aborts.
    pub async fn stop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            let _ = tokio::time::timeout(Duration::from_secs(2), task).await;
        }
    }
}

impl Drop for MockUpstream {
    fn drop(&mut self) {
        if let Some(tx) = self.shutdown.take() {
            let _ = tx.send(());
        }
        if let Some(task) = self.task.take() {
            task.abort();
        }
    }
}

async fn mock_handler(
    axum::extract::State(state): axum::extract::State<Arc<MockState>>,
    method: Method,
    uri: http::Uri,
    headers: HeaderMap,
    body: Bytes,
) -> Response {
    let behaviour = state.behaviour.lock().expect("mock behaviour lock").clone();

    state
        .recorded
        .lock()
        .expect("mock log lock")
        .push(RecordedRequest {
            method: method.to_string(),
            path: uri.path().to_string(),
            headers: headers
                .iter()
                .map(|(name, value)| {
                    (
                        name.as_str().to_string(),
                        value.to_str().unwrap_or("<non-utf8>").to_string(),
                    )
                })
                .collect(),
            body,
        });

    if uri.path() == "/v1/models" {
        return json_response(json!({
            "object": "list",
            "data": [
                {"id": "mock-model", "object": "model", "owned_by": "mock"},
                {"id": "mock-model-mini", "object": "model", "owned_by": "mock"},
            ],
        }));
    }

    dispatch(&behaviour, uri.path())
}

/// Build the response a behaviour asks for.
fn dispatch(behaviour: &Behaviour, path: &str) -> Response {
    let responses_api = path == "/v1/responses";

    match behaviour {
        Behaviour::ChatJson {
            prompt,
            completion,
            cached,
        } if !responses_api => json_response(chat_completion_body(*prompt, *completion, *cached)),
        Behaviour::ChatJsonWithoutUsage if !responses_api => json_response(json!({
            "id": "chatcmpl-mock",
            "object": "chat.completion",
            "model": "mock-model",
            "choices": [{"index": 0, "message": {"role": "assistant", "content": "hi"}, "finish_reason": "stop"}],
        })),
        Behaviour::ChatStream {
            prompt,
            completion,
            cached,
            events,
            delay_ms,
        } if !responses_api => chat_stream(*prompt, *completion, *cached, *events, *delay_ms),
        Behaviour::ChatStreamWithoutUsage if !responses_api => chat_stream_without_usage(),
        Behaviour::StreamAbort { after } => aborting_stream(*after),
        Behaviour::StreamHang => hanging_stream(),
        Behaviour::ResponsesJson {
            input,
            output,
            cached,
        } => json_response(json!({
            "id": "resp-mock",
            "object": "response",
            "model": "mock-model",
            "output": [],
            "usage": {
                "input_tokens": input,
                "output_tokens": output,
                "input_tokens_details": {"cached_tokens": cached},
            },
        })),
        Behaviour::ResponsesStream { .. } if responses_api => {
            let Behaviour::ResponsesStream {
                input,
                output,
                cached,
                events,
                delay_ms,
            } = behaviour
            else {
                unreachable!()
            };
            responses_stream(*input, *output, *cached, *events, *delay_ms)
        }
        Behaviour::ChatStream { .. } if responses_api => {
            // A chat-shaped SSE body delivered on the Responses route: the proxy
            // must scan it as a Responses stream and find no usage.
            let Behaviour::ChatStream { events, .. } = behaviour else {
                unreachable!()
            };
            generic_event_stream(*events, 0)
        }
        Behaviour::MalformedJson => Response::builder()
            .status(StatusCode::OK)
            .header("content-type", "application/json")
            .header("x-mock-upstream", "1")
            .body(Body::from("{ this is not json"))
            .expect("malformed response"),
        Behaviour::Error { status, message } => Response::builder()
            .status(*status)
            .header("content-type", "application/json")
            .header("x-mock-upstream", "1")
            .body(Body::from(
                serde_json::to_vec(&json!({
                    "error": {"message": message, "type": "invalid_request_error"},
                }))
                .expect("serialize error body"),
            ))
            .expect("error response"),
        Behaviour::ErrorText { status, body } => Response::builder()
            .status(*status)
            .header("content-type", "text/plain")
            .header("x-mock-upstream", "1")
            .body(Body::from(body.clone()))
            .expect("error text response"),
        Behaviour::OversizedResponse { bytes } => oversized_response(*bytes),
        // Any behaviour that does not apply to this route degrades to a plain
        // success so a mismatch shows up as a wrong ledger row, not a hang.
        _ => json_response(chat_completion_body(1, 1, 0)),
    }
}

fn chat_completion_body(prompt: u64, completion: u64, cached: u64) -> Value {
    json!({
        "id": "chatcmpl-mock",
        "object": "chat.completion",
        "model": "mock-model",
        "choices": [{
            "index": 0,
            "message": {"role": "assistant", "content": "hello"},
            "finish_reason": "stop",
        }],
        "usage": {
            "prompt_tokens": prompt,
            "completion_tokens": completion,
            "total_tokens": prompt + completion,
            "prompt_tokens_details": {"cached_tokens": cached},
        },
    })
}

fn json_response(value: Value) -> Response {
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .header("x-mock-upstream", "1")
        .body(Body::from(
            serde_json::to_vec(&value).expect("serialize mock body"),
        ))
        .expect("mock json response")
}

fn sse_response<S>(stream: S) -> Response
where
    S: futures::Stream<Item = Result<Bytes, std::io::Error>> + Send + 'static,
{
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "text/event-stream")
        .header("cache-control", "no-cache")
        .header("x-mock-upstream", "1")
        .body(Body::from_stream(stream))
        .expect("mock sse response")
}

fn sse_data(payload: &str) -> Bytes {
    Bytes::from(format!("data: {payload}\n\n"))
}

fn chat_stream(
    prompt: u64,
    completion: u64,
    cached: u64,
    events: usize,
    delay_ms: u64,
) -> Response {
    let stream = async_stream::stream! {
        for i in 0..events {
            if i > 0 && delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            let chunk = json!({
                "id": "chatcmpl-mock",
                "object": "chat.completion.chunk",
                "model": "mock-model",
                "choices": [{"index": 0, "delta": {"content": format!("token-{i}")}}],
            });
            yield Ok::<Bytes, std::io::Error>(sse_data(&chunk.to_string()));
        }

        // The totals arrive in the last chunk, which is what the scanner exists
        // to find. Note there is no usage in any earlier chunk.
        let final_chunk = json!({
            "id": "chatcmpl-mock",
            "object": "chat.completion.chunk",
            "model": "mock-model",
            "choices": [],
            "usage": {
                "prompt_tokens": prompt,
                "completion_tokens": completion,
                "total_tokens": prompt + completion,
                "prompt_tokens_details": {"cached_tokens": cached},
            },
        });
        yield Ok(sse_data(&final_chunk.to_string()));
        yield Ok(Bytes::from_static(b"data: [DONE]\n\n"));
    };

    sse_response(stream)
}

fn chat_stream_without_usage() -> Response {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(sse_data(
            &json!({
                "id": "chatcmpl-mock",
                "choices": [{"index": 0, "delta": {"content": "token"}}],
            })
            .to_string(),
        ));
        // A clean end with no usage event and no [DONE] sentinel.
    };

    sse_response(stream)
}

fn responses_stream(
    input: u64,
    output: u64,
    cached: u64,
    events: usize,
    delay_ms: u64,
) -> Response {
    let stream = async_stream::stream! {
        for i in 0..events {
            if i > 0 && delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            yield Ok::<Bytes, std::io::Error>(Bytes::from(format!(
                "event: response.output_text.delta\ndata: {}\n\n",
                json!({"type": "response.output_text.delta", "delta": format!("token-{i}")})
            )));
        }

        yield Ok::<Bytes, std::io::Error>(Bytes::from(format!(
            "event: response.completed\ndata: {}\n\n",
            json!({
                "type": "response.completed",
                "response": {
                    "id": "resp-mock",
                    "object": "response",
                    "output": [],
                    "usage": {
                        "input_tokens": input,
                        "output_tokens": output,
                        "input_tokens_details": {"cached_tokens": cached},
                    },
                },
            })
        )));
    };

    sse_response(stream)
}

/// A stream of `events` data events with no usage anywhere.
fn generic_event_stream(events: usize, delay_ms: u64) -> Response {
    let stream = async_stream::stream! {
        for i in 0..events {
            if i > 0 && delay_ms > 0 {
                tokio::time::sleep(Duration::from_millis(delay_ms)).await;
            }
            yield Ok::<Bytes, std::io::Error>(sse_data(&format!("{{\"n\":{i}}}")));
        }
    };

    sse_response(stream)
}

/// Usage the aborting stream reports before it breaks.
pub const ABORT_PROMPT_TOKENS: i64 = 7;
/// See [`ABORT_PROMPT_TOKENS`].
pub const ABORT_COMPLETION_TOKENS: i64 = 3;

/// A stream that breaks after `after` events: the body errors and the connection
/// is torn down without a terminating chunk, which is what a proxy sees when an
/// upstream dies mid-response.
fn aborting_stream(after: usize) -> Response {
    let stream = async_stream::stream! {
        for i in 0..after {
            let chunk = json!({
                "id": "chatcmpl-mock",
                "choices": [{"index": 0, "delta": {"content": format!("token-{i}")}}],
            });
            yield Ok::<Bytes, std::io::Error>(sse_data(&chunk.to_string()));
        }
        // Usage observed before the break must survive the failure.
        yield Ok::<Bytes, std::io::Error>(sse_data(
            &json!({
                "id": "chatcmpl-mock",
                "choices": [],
                "usage": {
                    "prompt_tokens": ABORT_PROMPT_TOKENS,
                    "completion_tokens": ABORT_COMPLETION_TOKENS,
                },
            })
            .to_string(),
        ));
        // A real upstream never breaks in the same poll as its last byte: the
        // socket has already flushed by the time the connection dies. Without
        // this suspension hyper aborts the whole response before writing the
        // head, and the client sees a connection error instead of a broken body.
        tokio::time::sleep(Duration::from_millis(25)).await;
        yield Err::<Bytes, std::io::Error>(std::io::Error::other(
            "simulated upstream abort mid-stream",
        ));
    };

    sse_response(stream)
}

/// One event, then silence: the response stays open without ever finishing.
fn hanging_stream() -> Response {
    let stream = async_stream::stream! {
        yield Ok::<Bytes, std::io::Error>(sse_data(
            &json!({
                "id": "chatcmpl-mock",
                "choices": [{"index": 0, "delta": {"content": "token-0"}}],
            })
            .to_string(),
        ));

        loop {
            tokio::time::sleep(Duration::from_secs(3600)).await;
        }
    };

    sse_response(stream)
}

fn oversized_response(bytes: usize) -> Response {
    let stream = async_stream::stream! {
        // Padding after the opening brace keeps the body invalid JSON once the
        // proxy truncates it, which is exactly the case being tested.
        let chunk = vec![b'x'; 64 * 1024];
        let mut written = 1usize;
        yield Ok::<Bytes, std::io::Error>(Bytes::from_static(b"{\"padding\":\""));
        while written < bytes {
            let take = chunk.len().min(bytes - written);
            written += take;
            yield Ok(Bytes::from(chunk[..take].to_vec()));
        }
        yield Ok(Bytes::from_static(b"\"}"));
    };

    // Deliberately *not* `text/event-stream`: an SSE response would take the
    // proxy's uncapped streaming path, and the cap would never be exercised.
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/json")
        .header("x-mock-upstream", "1")
        .body(Body::from_stream(stream))
        .expect("oversized response")
}

// ---------------------------------------------------------------------------
// Server under test
// ---------------------------------------------------------------------------

/// One API key to seed into `api_keys` before the instance starts.
///
/// There is no `key` field and that is the point: the harness derives its
/// plaintext from the fixture identity, so tests do not pin credentials in
/// source the way the old `keys:` fixtures did. The derivation makes a restart
/// seed the same credential without storing or recovering a plaintext from the
/// database. What a test still controls is the row's identity — its name and
/// its consumer — and those are what the assertions are about.
///
/// `Spec::new` seeds exactly one key so that the ~60 tests which call
/// `Spec::new` and then `server.key()` keep working without knowing any of
/// this happened; a test about more than one key calls `with_keys`.
///
/// # A key is not a partner
///
/// A key row no longer carries a model list. Which models the consumer may call
/// — and what each costs — is configured once, on the *partner*, in
/// `partner_models` (ADR 0015), and one partner holds exactly one active key.
/// So the models a [`KeySpec`] lists are written to that partner's price list
/// during seeding, and `allowed_models` is now a request-path property of a
/// partner rather than a column on a credential. The name survives the move
/// because the alternative — threading `partner_models` through every
/// `Spec` that only wants "this consumer can call `gpt-4o`" — is noise in
/// every test that never asks about billing.
#[derive(Clone, Debug)]
pub struct KeySpec {
    pub name: String,
    pub consumer_id: Option<String>,
    /// Models this key may call. Empty means *no* models (strict default).
    pub allowed_models: Vec<String>,
    /// How this consumer is billed, when the test is declaring a ledger rather
    /// than discovering one. `None` is the invoice default every other key gets.
    pub billing_mode: Option<String>,
    /// Payment terms in minutes, when the test is declaring a ledger.
    pub payment_terms_minutes: Option<i64>,
}

impl KeySpec {
    /// A key that may call every model the mock upstream serves. `Spec::new`
    /// builds its default key through this, so ordinary tests keep exercising
    /// the proxy rather than the allow-list; a test about restrictions calls
    /// `with_allowed_models` explicitly.
    pub fn new(name: &str) -> Self {
        Self {
            name: name.to_string(),
            consumer_id: None,
            allowed_models: DEFAULT_ALLOWED_MODELS
                .iter()
                .map(|s| s.to_string())
                .collect(),
            billing_mode: None,
            payment_terms_minutes: None,
        }
    }

    pub fn with_consumer(mut self, consumer_id: &str) -> Self {
        self.consumer_id = Some(consumer_id.to_string());
        self
    }

    /// Bill this consumer on reconciliation terms: a settlement record rather
    /// than an obligation. Only honoured by a spec that declares a starting
    /// ledger, because otherwise the scheduler closes the first day before the
    /// test has said anything.
    pub fn on_reconciliation(mut self) -> Self {
        self.billing_mode = Some("reconciliation".to_string());
        self
    }

    /// Payment terms in minutes, for a spec that declares a starting ledger.
    pub fn with_payment_terms(mut self, minutes: i64) -> Self {
        self.payment_terms_minutes = Some(minutes);
        self
    }

    /// Restrict this key to exactly the listed models. Passing `&[]` models
    /// the strict default: no model is allowed.
    pub fn with_allowed_models(mut self, models: &[&str]) -> Self {
        self.allowed_models = models.iter().map(|s| s.to_string()).collect();
        self
    }

    /// The identity the ledger will record for this key.
    pub fn effective_consumer_id(&self) -> &str {
        self.consumer_id.as_deref().unwrap_or(&self.name)
    }
}

/// A configured manager password, mapping onto the `manager` config block.
/// A manager sees every consumer; there is no allow-list to configure (ADR 0013).
#[derive(Clone, Debug)]
pub struct ManagerSpec {
    pub password: String,
}

impl ManagerSpec {
    pub fn new(password: &str) -> Self {
        Self {
            password: password.to_string(),
        }
    }
}

/// Everything the child process is configured with. Fields map one-to-one onto
/// `config.yaml`, so a test only sets what it is actually about.
#[derive(Clone, Debug)]
pub struct Spec {
    pub upstream_url: String,
    pub upstream_key: String,
    pub upstream_timeout_secs: u64,
    pub keys: Vec<KeySpec>,
    /// Optional manager credential. `None` means no `manager` block in the file.
    pub manager: Option<ManagerSpec>,
    /// Ledger path. Relative names are resolved inside the server's directory,
    /// so a restart against the same directory reuses the same database.
    pub db_name: PathBuf,
    pub port: u16,
    pub queue_size: usize,
    pub batch_size: usize,
    pub batch_timeout_ms: u64,
    pub max_body_size: usize,
    pub shutdown_grace_secs: u64,
    pub sse_poll_interval_ms: u64,
    pub retention_days: u32,
    /// Absent means no `billing:` block, and the server uses its defaults.
    pub billing: Option<BillingSpec>,
    /// Whether [`backdate_partners`] should pre-anchor the seeded partners.
    /// See [`Spec::with_backdated_billing`]; default is `false` so a suite that
    /// does not care about billing is not quietly anchored.
    pub billing_backdates_partners: bool,
    /// Writes to the ledger before the child process exists.
    pub billing_start_ledger: Option<LedgerHook>,
    /// Writes to the ledger once the child is ready.
    pub billing_inject_usage: Option<LedgerHook>,
}

/// A test-supplied write to the ledger, taken by the harness at a point the
/// test chooses.
///
/// `Arc` rather than `Box` so [`Spec`] keeps its `Clone`, and a hand-written
/// `Debug` so a hook — which may close over anything — is never printed into a
/// panic message. A hook must not consume its captures: it is `Fn`, because
/// [`Spec`] is cloned when a test restarts an instance, and a hook that had
/// already been run must still be runnable on the next process.
#[derive(Clone)]
pub struct LedgerHook(Arc<dyn Fn(&Path) + Send + Sync>);

impl LedgerHook {
    /// Run the hook against the ledger at `path`.
    pub fn run(&self, path: &Path) {
        (self.0)(path);
    }
}

impl std::fmt::Debug for LedgerHook {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("LedgerHook")
    }
}

/// The `billing:` block, for the tests that are about when a day closes.
///
/// Every field is optional because the *defaults* are themselves behaviour under
/// test: a suite that set `close_delay_minutes` everywhere would never notice
/// that the default is wrong. `None` renders the field as absent, so
/// `config.example.yaml`'s values are what an unspecified test runs with.
#[derive(Clone, Debug, Default)]
pub struct BillingSpec {
    /// The billing calendar's distance from UTC. `None` leaves the server default.
    pub timezone_offset_minutes: Option<i32>,
    /// How long after a day ends it may close. `Some(0)` is the interesting
    /// case: it makes a day closable the instant it ends, so a test does not
    /// have to wait out a real delay.
    pub close_delay_minutes: Option<i64>,
    /// How often the scheduler looks. A test that waits for a day to close
    /// sets this to 1 rather than sleeping 30 seconds.
    pub scheduler_interval_secs: Option<u64>,
    /// Whether statements are emailed. Off by default: the harness configures
    /// no relay, and a suite that needs the send path names a sink explicitly.
    pub email_enabled: Option<bool>,
}

impl Spec {
    /// The billing block, or an empty string when the test does not set one.
    fn billing_yaml(&self) -> String {
        let Some(b) = &self.billing else {
            return String::new();
        };
        let mut out = String::from("billing:\n");
        if let Some(tz) = b.timezone_offset_minutes {
            out.push_str(&format!("  timezone_offset_minutes: {tz}\n"));
        }
        if let Some(delay) = b.close_delay_minutes {
            out.push_str(&format!("  close_delay_minutes: {delay}\n"));
        }
        if let Some(interval) = b.scheduler_interval_secs {
            out.push_str(&format!("  scheduler_interval_secs: {interval}\n"));
        }
        if let Some(enabled) = b.email_enabled {
            out.push_str(&format!("  email:\n    enabled: {enabled}\n"));
        }
        out
    }
}

impl Spec {
    /// A spec pointed at `upstream`, with its ledger inside the server's own
    /// directory.
    pub fn new(upstream: &MockUpstream) -> Self {
        Self {
            upstream_url: upstream.url(),
            upstream_key: UPSTREAM_KEY.to_string(),
            upstream_timeout_secs: 10,
            keys: vec![KeySpec::new(CONSUMER)],
            manager: None,
            db_name: PathBuf::from("ledger.db"),
            port: free_port(),
            queue_size: 10_000,
            batch_size: 100,
            batch_timeout_ms: 10,
            max_body_size: 10 * 1024 * 1024,
            shutdown_grace_secs: 1,
            sse_poll_interval_ms: 100,
            retention_days: 60,
            billing: None,
            billing_backdates_partners: false,
            billing_start_ledger: None,
            billing_inject_usage: None,
        }
    }

    pub fn with_keys(mut self, keys: Vec<KeySpec>) -> Self {
        self.keys = keys;
        self
    }

    pub fn with_port(mut self, port: u16) -> Self {
        self.port = port;
        self
    }

    pub fn with_db_name(mut self, db_name: &str) -> Self {
        self.db_name = PathBuf::from(db_name);
        self
    }

    pub fn with_upstream_url(mut self, url: &str) -> Self {
        self.upstream_url = url.to_string();
        self
    }

    pub fn with_upstream_key(mut self, key: &str) -> Self {
        self.upstream_key = key.to_string();
        self
    }

    pub fn with_upstream_timeout(mut self, secs: u64) -> Self {
        self.upstream_timeout_secs = secs;
        self
    }

    pub fn with_shutdown_grace(mut self, secs: u64) -> Self {
        self.shutdown_grace_secs = secs;
        self
    }

    pub fn with_max_body_size(mut self, bytes: usize) -> Self {
        self.max_body_size = bytes;
        self
    }

    pub fn with_queue(
        mut self,
        queue_size: usize,
        batch_size: usize,
        batch_timeout_ms: u64,
    ) -> Self {
        self.queue_size = queue_size;
        self.batch_size = batch_size;
        self.batch_timeout_ms = batch_timeout_ms;
        self
    }

    pub fn with_sse_poll_interval(mut self, ms: u64) -> Self {
        self.sse_poll_interval_ms = ms;
        self
    }

    /// Write a `billing:` block, so the test controls when a day closes.
    pub fn with_billing(mut self, billing: BillingSpec) -> Self {
        self.billing = Some(billing);
        self
    }

    /// A billing block that closes a day the instant it ends, and looks every
    /// second. What a test about the statement lifecycle almost always wants:
    /// the alternative is waiting out a real five-minute close delay.
    pub fn with_fast_billing(self) -> Self {
        self.with_billing(BillingSpec {
            close_delay_minutes: Some(0),
            scheduler_interval_secs: Some(1),
            ..BillingSpec::default()
        })
    }

    /// Fast billing **and** a backdated partner, which is what a billing test
    /// almost always needs.
    ///
    /// A partner is anchored at the day before it was created, so a partner the
    /// harness creates moments ago is only ever walked from today — a ledger
    /// with a month of usage under it would produce a statement dated in the
    /// future, and the API's own `billing_date <= today` filter would hide it.
    /// The fixture therefore pre-anchors the partner by writing a zero-amount
    /// statement for the day before yesterday, which is a row a deployment
    /// produces on its own and which the walk would have written anyway. What it
    /// removes is a dependency on the wall clock, not a behaviour: the scheduler
    /// still decides every day after the anchor, and the statement under test is
    /// still generated by the scheduler from real usage rows.
    /// Run a closure against the seeded ledger *before* the process starts, and
    /// run a request-driven rest against the ledger that process leaves behind.
    ///
    /// Two hooks rather than one, because they solve two different problems and
    /// the order matters:
    ///
    /// - `start_ledger` gets to write **usage on a day the scheduler will
    ///   close**. A test cannot arrange that from the outside, because the
    ///   scheduler's first tick runs before the test's first line of code — by
    ///   which time yesterday is already a closed, empty day, and a statement
    ///   the walk will never revisit.
    /// - `inject` then acts on a process that is already running, and so
    ///   exercises the real request path: the price snapshots the metering
    ///   write actually took, the terminal states recovery can see, and the
    ///   suspension check reading a derived status.
    ///
    /// A test that wants both chains one statement — a real request metered at
    /// the seeded prices, closed into a statement — has no ordering that
    /// produces it, because a day closes once. So the two are separate: usage
    /// written by a request belongs to *today*, which is not yet closeable, and
    /// the statement under test is built by the generator from a day the
    /// scheduler closed. What each half proves is stated where it is used.
    pub fn starting_ledger<F>(mut self, prepare: F) -> Self
    where
        F: Fn(&Path) + Send + Sync + 'static,
    {
        self.billing_backdates_partners = true;
        self.billing_start_ledger = Some(LedgerHook(Arc::new(prepare)));
        self
    }

    pub fn inject_usage<F>(mut self, inject: F) -> Self
    where
        F: Fn(&Path) + Send + Sync + 'static,
    {
        self.billing_inject_usage = Some(LedgerHook(Arc::new(inject)));
        self
    }

    pub fn with_backdated_billing(self) -> Self {
        let mut spec = self.with_fast_billing();
        spec.billing_backdates_partners = true;
        spec
    }

    pub fn with_manager(mut self, manager: ManagerSpec) -> Self {
        self.manager = Some(manager);
        self
    }

    /// The configuration file, which no longer describes any API key.
    ///
    /// `self.keys` is deliberately absent from the output: those keys are
    /// seeded into the database by [`seed_keys`] before the child starts, and
    /// writing them here as well would reintroduce the two-sources problem
    /// this change exists to remove.
    pub fn yaml(&self) -> String {
        let manager = match &self.manager {
            None => String::new(),
            Some(m) => format!("manager:\n  password: {}\n", yaml_str(&m.password)),
        };
        let billing = self.billing_yaml();

        // The listen address is not in the file: `spawn_in` passes it as
        // PARTNER_PORTAL_LISTEN, like a real deployment does.
        format!(
            "server:\n  \
               graceful_shutdown: true\n  \
               shutdown_grace_secs: {grace}\n  \
               max_body_size: {max_body}\n  \
               cors_allow_origins: []\n  \
               sse_poll_interval_ms: {sse}\n\
             upstream:\n  \
               base_url: {upstream}\n  \
               api_key: {upstream_key}\n  \
               timeout_secs: {timeout}\n  \
               connect_timeout_secs: 2\n\
             {manager}\
             {billing}\
             database:\n  \
               path: {db}\n  \
               retention_days: {retention}\n  \
               queue_size: {queue}\n  \
               batch_size: {batch}\n  \
               batch_timeout_ms: {batch_timeout}\n  \
               retention_interval_secs: 3600\n  \
               retention_batch_size: 2000\n",
            grace = self.shutdown_grace_secs,
            max_body = self.max_body_size,
            sse = self.sse_poll_interval_ms,
            upstream = yaml_str(&self.upstream_url),
            upstream_key = yaml_str(&self.upstream_key),
            timeout = self.upstream_timeout_secs,
            manager = manager,
            billing = billing,
            db = yaml_str(&self.db_name.display().to_string()),
            retention = self.retention_days,
            queue = self.queue_size,
            batch = self.batch_size,
            batch_timeout = self.batch_timeout_ms,
        )
    }
}

/// Quote a scalar for YAML. Test values are ordinary strings, but a URL or a
/// path can contain characters that would otherwise change the document.
fn yaml_str(value: &str) -> String {
    let escaped = value.replace('\\', "\\\\").replace('"', "\\\"");
    format!("\"{escaped}\"")
}

/// The compiled binary, running against a temp directory.
pub struct TestServer {
    child: Option<Child>,
    /// Held only when the harness created the directory itself.
    _dir: Option<tempfile::TempDir>,
    pub root: PathBuf,
    pub config_path: PathBuf,
    pub db_path: PathBuf,
    pub log_path: PathBuf,
    pub addr: String,
    pub base_url: String,
    pub spec: Spec,
    /// What [`seed_keys`] inserted, in the order the spec listed it. The
    /// plaintexts live here rather than on [`Spec`] because they did not exist
    /// until the row was written.
    pub seeded: Vec<SeededKey>,
}

impl TestServer {
    /// Start in a fresh temp directory owned by the returned value.
    pub async fn start(spec: Spec) -> Self {
        let dir = tempfile::TempDir::new().expect("temp dir");
        let mut server = Self::start_in(dir.path(), spec).await;
        server._dir = Some(dir);
        server
    }

    /// Start in a caller-owned directory.
    ///
    /// `spec.db_path` must already point inside it; this is the form used to
    /// restart a process against the database a previous process left behind.
    pub async fn start_in(dir: &Path, spec: Spec) -> Self {
        let mut server = Self::spawn_in(dir, spec).await;
        server.await_ready().await;
        server
    }

    /// Start in a caller-owned directory, without waiting for readiness.
    ///
    /// [`Self::start_in`] panics on an instance that never becomes ready, which
    /// is precisely the state a readiness test has to observe. Such a test waits
    /// for `/healthz` instead — liveness is a different signal, and the gap
    /// between the two is the thing under test.
    pub async fn spawn_in(dir: &Path, spec: Spec) -> Self {
        let config_path = dir.join("config.yaml");
        let log_path = dir.join("server.log");
        let db_path = if spec.db_name.is_absolute() {
            spec.db_name.clone()
        } else {
            dir.join(&spec.db_name)
        };
        std::fs::write(&config_path, spec.yaml()).expect("write config");

        // The keys go in *before* the child exists. Startup loads the active
        // key set and refuses to serve without one, so a test that seeded after
        // the spawn would race the load — and the readiness gate would paper
        // over it rather than fail, which is the failure mode the harness
        // exists to make loud.
        let seeded = seed_keys(&db_path, &spec.keys);

        // Last, and only when a billing test asked for it: pre-anchor every
        // seeded partner, then let the test declare its own starting ledger.
        // Both run *before* the child exists, so the first tick the process runs
        // already has an anchor and a day of usage to close.
        if spec.billing_backdates_partners {
            backdate_partners(&db_path, &spec.keys);
        }
        if let Some(prepare) = &spec.billing_start_ledger {
            prepare.run(&db_path);
        }

        let log = File::create(&log_path).expect("create log file");
        let child = Command::new(BIN)
            .env("PARTNER_PORTAL_CONFIG", &config_path)
            .env("PARTNER_PORTAL_LISTEN", format!("127.0.0.1:{}", spec.port))
            .env(
                "PARTNER_PORTAL_API_KEY_SECRET",
                std::str::from_utf8(TEST_SECRET).expect("the test secret is ASCII"),
            )
            .env("RUST_LOG", "info")
            .current_dir(dir)
            .stdin(Stdio::null())
            .stdout(Stdio::from(log.try_clone().expect("clone log handle")))
            .stderr(Stdio::from(log))
            .spawn()
            .expect("spawn partner-portal");

        let addr = format!("127.0.0.1:{}", spec.port);
        let base_url = format!("http://{addr}");

        Self {
            child: Some(child),
            _dir: None,
            root: dir.to_path_buf(),
            config_path,
            db_path,
            log_path,
            addr,
            base_url,
            spec,
            seeded,
        }
    }

    /// Poll `/healthz` until the process is alive, or fail with its log.
    ///
    /// The liveness counterpart to [`Self::await_ready`], for tests that start a
    /// process with [`Self::spawn_in`] and then judge its readiness themselves.
    pub async fn await_healthz(&self, timeout: Duration) {
        let client = TestClient::new();
        let deadline = Instant::now() + timeout;
        loop {
            match client
                .get(&format!("{}/healthz", self.base_url), None)
                .await
            {
                Ok(response) if response.status == StatusCode::OK => return,
                Ok(_) | Err(_) => {}
            }
            assert!(
                Instant::now() < deadline,
                "server was not alive within {timeout:?}:\n{}",
                self.logs()
            );
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    /// Poll `/readyz` until the process reports ready, or fail with its log.
    async fn await_ready(&mut self) {
        let client = TestClient::new();
        let deadline = Instant::now() + READY_TIMEOUT;

        loop {
            if let Some(status) = self
                .child
                .as_mut()
                .and_then(|c| c.try_wait().ok().flatten())
            {
                panic!(
                    "server exited before becoming ready ({status}):\n{}",
                    self.logs()
                );
            }

            match client.get(&format!("{}/readyz", self.base_url), None).await {
                Ok(response) if response.status == StatusCode::OK => {
                    // Ready, so a test-declared write to the running instance's
                    // ledger can happen now. Before this the process may not
                    // have the database open at all, and a write against a file
                    // another handle holds mid-startup is a race the fixture
                    // cannot win by waiting longer.
                    if let Some(inject) = &self.spec.billing_inject_usage {
                        inject.run(&self.db_path);
                    }
                    return;
                }
                Ok(_) | Err(_) => {}
            }

            if Instant::now() >= deadline {
                panic!(
                    "server was not ready within {READY_TIMEOUT:?}:\n{}",
                    self.logs()
                );
            }
            tokio::time::sleep(Duration::from_millis(25)).await;
        }
    }

    pub fn pid(&self) -> u32 {
        self.child.as_ref().expect("child running").id()
    }

    pub fn is_running(&mut self) -> bool {
        matches!(self.child.as_mut().map(|c| c.try_wait()), Some(Ok(None)))
    }

    /// Deliver a signal to the child.
    pub fn signal(&self, signal: i32) {
        let pid = self.pid() as libc::pid_t;
        // SAFETY: `kill` with a valid pid and a standard signal number.
        let rc = unsafe { libc::kill(pid, signal) };
        assert_eq!(rc, 0, "kill({pid}, {signal}) failed");
    }

    pub fn sigterm(&self) {
        self.signal(libc::SIGTERM);
    }

    /// SIGKILL: no handler runs, no drain happens. This is the crash the ledger
    /// has to survive.
    pub fn sigkill(&self) {
        self.signal(libc::SIGKILL);
    }

    /// Wait for the process to exit, returning its status.
    pub async fn wait_exit(&mut self, timeout: Duration) -> ExitStatus {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.as_mut().expect("child").try_wait() {
                Ok(Some(status)) => return status,
                Ok(None) => {
                    assert!(
                        Instant::now() < deadline,
                        "server did not exit within {timeout:?}:\n{}",
                        self.logs()
                    );
                    tokio::time::sleep(Duration::from_millis(20)).await;
                }
                Err(e) => panic!("waitpid failed: {e}"),
            }
        }
    }

    /// Overwrite the config file in place (truncating first, like a shell
    /// redirect). Deliberately not atomic: the truncation window is one of the
    /// cases hot reload must survive.
    pub fn write_config_raw(&self, contents: &str) {
        std::fs::write(&self.config_path, contents).expect("write config");
    }

    /// Write a valid config built from a spec (with this server's paths).
    pub fn write_config(&self, spec: &Spec) {
        self.write_config_raw(&spec.yaml());
    }

    pub fn read_config(&self) -> String {
        std::fs::read_to_string(&self.config_path).expect("read config")
    }

    pub fn logs(&self) -> String {
        std::fs::read_to_string(&self.log_path).unwrap_or_default()
    }

    /// Open the ledger directly. Independent of the server's own connections.
    pub fn open_db(&self) -> Connection {
        open_db(&self.db_path)
    }

    pub fn url(&self, path: &str) -> String {
        format!("{}{path}", self.base_url)
    }

    /// The plaintext of the first seeded key — the one `Spec::new` creates, so
    /// a test that never calls `with_keys` needs to know nothing about seeding.
    ///
    /// Panics when the spec seeded nothing. A test with no key has no request
    /// to make, and an empty `&str` would 401 with a message that points at
    /// the credential rather than at the fixture.
    pub fn key(&self) -> &str {
        &self
            .seeded
            .first()
            .expect("the spec seeds at least one key")
            .plaintext
    }

    /// The plaintext of the `index`-th seeded key.
    pub fn key_at(&self, index: usize) -> &str {
        &self.seeded[index].plaintext
    }

    /// The id of the first seeded key, for the admin API.
    pub fn key_id(&self) -> i64 {
        self.seeded
            .first()
            .expect("the spec seeds at least one key")
            .id
    }

    pub fn consumer(&self) -> &str {
        &self
            .seeded
            .first()
            .expect("the spec seeds at least one key")
            .consumer_id
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        if let Some(mut child) = self.child.take() {
            // Kill rather than wait: a test that panicked leaves the process in
            // an unknown state, and a leaked child would hold the port.
            let _ = child.kill();
            let _ = child.wait();
        }
    }
}

// ---------------------------------------------------------------------------
// HTTP client
// ---------------------------------------------------------------------------

/// A buffered HTTP response.
#[derive(Clone, Debug)]
pub struct HttpResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    pub body: Bytes,
}

impl HttpResponse {
    pub fn json(&self) -> Value {
        serde_json::from_slice(&self.body).unwrap_or(Value::Null)
    }

    pub fn text(&self) -> String {
        String::from_utf8_lossy(&self.body).to_string()
    }

    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }

    pub fn request_id(&self) -> Option<String> {
        self.header("x-request-id")
    }
}

/// A hyper client over the loopback interface.
pub struct TestClient {
    client: Client<HttpConnector, Full<Bytes>>,
}

impl Default for TestClient {
    fn default() -> Self {
        Self::new()
    }
}

impl TestClient {
    pub fn new() -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(Some(Duration::from_secs(5)));
        let client = Client::builder(TokioExecutor::new())
            .pool_idle_timeout(Duration::from_secs(10))
            .build(connector);
        Self { client }
    }

    /// Send a request and hand back the streaming response.
    pub async fn send(
        &self,
        method: Method,
        url: &str,
        bearer: Option<&str>,
        body: Bytes,
        extra: &[(&str, &str)],
    ) -> Result<http::Response<Incoming>, String> {
        let mut builder = http::Request::builder().method(method).uri(url);
        if let Some(bearer) = bearer {
            builder = builder.header(http::header::AUTHORIZATION, format!("Bearer {bearer}"));
        }
        let mut has_content_type = false;
        for (name, value) in extra {
            if name.eq_ignore_ascii_case("content-type") {
                has_content_type = true;
            }
            builder = builder.header(*name, *value);
        }
        if !body.is_empty() && !has_content_type {
            builder = builder.header(http::header::CONTENT_TYPE, "application/json");
        }

        let request = builder
            .body(Full::new(body))
            .map_err(|e| format!("build request: {e}"))?;

        self.client
            .request(request)
            .await
            .map_err(|e| format!("send request: {e}"))
    }

    /// Send and buffer the whole body.
    pub async fn call(
        &self,
        method: Method,
        url: &str,
        bearer: Option<&str>,
        body: Bytes,
        extra: &[(&str, &str)],
    ) -> HttpResponse {
        let response = self
            .send(method, url, bearer, body, extra)
            .await
            .expect("request must reach the server");
        buffer(response).await
    }

    pub async fn get(&self, url: &str, bearer: Option<&str>) -> Result<HttpResponse, String> {
        let response = self
            .send(Method::GET, url, bearer, Bytes::new(), &[])
            .await?;
        Ok(buffer(response).await)
    }

    pub async fn get_json(&self, url: &str, bearer: Option<&str>) -> HttpResponse {
        self.call(Method::GET, url, bearer, Bytes::new(), &[]).await
    }

    pub async fn post_json(&self, url: &str, bearer: Option<&str>, body: Value) -> HttpResponse {
        self.call(
            Method::POST,
            url,
            bearer,
            Bytes::from(serde_json::to_vec(&body).expect("serialize body")),
            &[],
        )
        .await
    }

    /// A chat-completions request body.
    pub fn chat_body(model: &str, stream: bool) -> Value {
        json!({
            "model": model,
            "stream": stream,
            "messages": [{"role": "user", "content": "hello"}],
        })
    }
}

/// A plain chat-completions request body: one model, one user message.
pub fn chat_request(model: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&json!({
            "model": model,
            "messages": [{"role": "user", "content": "hello"}],
        }))
        .expect("serialize chat request"),
    )
}

/// A streaming chat-completions request body.
pub fn chat_stream_request(model: &str) -> Bytes {
    Bytes::from(
        serde_json::to_vec(&json!({
            "model": model,
            "stream": true,
            "messages": [{"role": "user", "content": "hello"}],
        }))
        .expect("serialize chat request"),
    )
}

/// Read a response to completion.
pub async fn buffer(response: http::Response<Incoming>) -> HttpResponse {
    let status = response.status();
    let headers = response.headers().clone();
    let body = response
        .into_body()
        .collect()
        .await
        .map(|c| c.to_bytes())
        .unwrap_or_default();
    HttpResponse {
        status,
        headers,
        body,
    }
}

/// Whether a raw response buffer holds a complete message.
///
/// Used only to stop reading early: the caller wants the status line and headers,
/// and a closed or length-delimited response should not cost a timeout wait.
fn raw_response_is_complete(buf: &[u8]) -> bool {
    let Some(split) = buf.windows(4).position(|w| w == b"\r\n\r\n") else {
        return false;
    };
    let head = String::from_utf8_lossy(&buf[..split]);
    let body = &buf[split + 4..];
    for line in head.split("\r\n") {
        if let Some((name, value)) = line.split_once(':') {
            if name.trim().eq_ignore_ascii_case("content-length") {
                if let Ok(len) = value.trim().parse::<usize>() {
                    return body.len() >= len;
                }
            }
            if name.trim().eq_ignore_ascii_case("transfer-encoding")
                && value.trim().eq_ignore_ascii_case("chunked")
            {
                return body.ends_with(b"\r\n0\r\n\r\n");
            }
        }
    }
    false
}

/// A response read off a raw socket, kept in both parsed and raw form.
#[derive(Clone, Debug)]
pub struct RawResponse {
    pub status: StatusCode,
    pub headers: HeaderMap,
    /// Everything after the header block, exactly as it arrived (so chunked
    /// framing is still in there if the server chose chunked).
    pub body: Bytes,
    pub raw: String,
}

impl RawResponse {
    pub fn header(&self, name: &str) -> Option<String> {
        self.headers
            .get(name)
            .and_then(|v| v.to_str().ok())
            .map(str::to_string)
    }
}

/// Speak HTTP/1.1 by hand over a fresh TCP connection.
///
/// A well-behaved client normalises framing headers away (hyper's client will
/// not let a caller put an arbitrary token in `Connection:`), so the only way to
/// test what the proxy does with such a header is to write the bytes oneself.
/// `address` is `host:port`; `request` is the complete request head and body,
/// using CRLF line endings.
pub async fn raw_request(address: &str, request: &str) -> RawResponse {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let mut stream = tokio::net::TcpStream::connect(address)
        .await
        .unwrap_or_else(|e| panic!("connect to {address}: {e}"));
    stream
        .write_all(request.as_bytes())
        .await
        .expect("write raw request");
    stream.flush().await.expect("flush raw request");

    // Read until the response is complete: EOF, a satisfied `Content-Length`, or
    // the end of a chunked body. Reading to EOF alone would stall on a keep-alive
    // connection, and a flat timeout would make a passing case wait for it.
    let mut buf = Vec::new();
    let deadline = Instant::now() + Duration::from_secs(10);
    let mut chunk = [0u8; 8192];
    loop {
        let remaining = deadline.saturating_duration_since(Instant::now());
        assert!(
            !remaining.is_zero(),
            "raw request timed out; partial response: {:?}",
            String::from_utf8_lossy(&buf)
        );
        let read = tokio::time::timeout(remaining, stream.read(&mut chunk)).await;
        match read {
            Ok(Ok(0)) => break,
            Ok(Ok(n)) => buf.extend_from_slice(&chunk[..n]),
            Ok(Err(e)) => panic!("raw read failed: {e}"),
            Err(_) => panic!(
                "raw request timed out; partial response: {:?}",
                String::from_utf8_lossy(&buf)
            ),
        }
        if raw_response_is_complete(&buf) {
            break;
        }
    }

    let split = buf
        .windows(4)
        .position(|w| w == b"\r\n\r\n")
        .expect("raw response must contain a header block");
    let head = String::from_utf8_lossy(&buf[..split]).to_string();
    let body = Bytes::copy_from_slice(&buf[split + 4..]);

    let mut lines = head.split("\r\n");
    let status_line = lines.next().unwrap_or_default();
    let code: u16 = status_line
        .split(' ')
        .nth(1)
        .and_then(|c| c.parse().ok())
        .unwrap_or_else(|| panic!("unparseable status line: {status_line:?}"));
    let mut headers = HeaderMap::new();
    for line in lines {
        if let Some((name, value)) = line.split_once(':') {
            if let (Ok(name), Ok(value)) = (
                http::header::HeaderName::try_from(name.trim()),
                http::header::HeaderValue::try_from(value.trim()),
            ) {
                headers.append(name, value);
            }
        }
    }

    RawResponse {
        status: StatusCode::from_u16(code).expect("valid status code"),
        headers,
        body,
        raw: String::from_utf8_lossy(&buf).to_string(),
    }
}

/// Incremental reader over a response body, with per-read deadlines.
pub struct BodyReader {
    body: Incoming,
    buffer: Vec<u8>,
}

impl BodyReader {
    pub fn new(body: Incoming) -> Self {
        Self {
            body,
            buffer: Vec::new(),
        }
    }

    /// The next frame, or `None` at end of body.
    pub async fn next_frame(&mut self, timeout: Duration) -> Result<Option<Bytes>, String> {
        match tokio::time::timeout(timeout, self.body.frame()).await {
            Err(_) => Err("timed out reading the response body".to_string()),
            Ok(None) => Ok(None),
            Ok(Some(Err(e))) => Err(format!("body error: {e}")),
            Ok(Some(Ok(frame))) => Ok(frame.into_data().ok()),
        }
    }

    /// Read until the accumulated body contains `needle`.
    ///
    /// SSE arrives in arbitrarily sized chunks and a single event can be split
    /// across them, so matching on the accumulated buffer is the only correct
    /// way to look for an event.
    pub async fn read_until(&mut self, needle: &str, timeout: Duration) -> Result<String, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let text = String::from_utf8_lossy(&self.buffer).to_string();
            if text.contains(needle) {
                return Ok(text);
            }
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err(format!(
                    "never saw {needle:?} in the response body; got so far:\n{text}"
                ));
            }
            match self.next_frame(remaining).await? {
                Some(frame) => self.buffer.extend_from_slice(&frame),
                None => {
                    return Err(format!(
                        "response body ended before {needle:?} arrived:\n{}",
                        String::from_utf8_lossy(&self.buffer)
                    ));
                }
            }
        }
    }

    /// Read everything until end of body.
    pub async fn read_to_end(&mut self, timeout: Duration) -> Result<Bytes, String> {
        let deadline = Instant::now() + timeout;
        loop {
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                return Err("timed out reading the response body to the end".to_string());
            }
            match self.next_frame(remaining).await? {
                Some(frame) => self.buffer.extend_from_slice(&frame),
                None => return Ok(Bytes::from(std::mem::take(&mut self.buffer))),
            }
        }
    }

    pub fn so_far(&self) -> String {
        String::from_utf8_lossy(&self.buffer).to_string()
    }
}

// ---------------------------------------------------------------------------
// Ledger readers
// ---------------------------------------------------------------------------

/// Open the ledger for inspection. Deliberately a plain read-write handle, the
/// same way the server opens its reader connections, so WAL recovery is not an
/// issue.
/// A key the harness inserted, with the plaintext that was issued for it.
#[derive(Clone, Debug)]
pub struct SeededKey {
    pub id: i64,
    pub plaintext: String,
    pub name: String,
    pub consumer_id: String,
    /// What the consumer may call, mirrored from `partner_models` for the same
    /// reason [`KeySpec::allowed_models`] survives: the request path filters
    /// `/v1/models` by it, and a test that asserts on the filtered list should
    /// not have to re-read the price table to know what it configured.
    pub allowed_models: Vec<String>,
}

/// The prices a seeded partner's models are metered at.
///
/// One price for every model, chosen so that a statement built from it is
/// obviously a statement: `$0.095 / M` input, `$0.002375 / M` cached input and
/// `$0.475 / M` output are the prices the specification names, and a
/// 1 000 000-token request at them costs `$0.095 + $0.000002375 + $0.475`.
const SEEDED_INPUT_PER_MILLION: &str = "0.095";
const SEEDED_CACHED_PER_MILLION: &str = "0.002375";
const SEEDED_OUTPUT_PER_MILLION: &str = "0.475";

/// Insert `specs` into `api_keys` — and each one's partner and prices — and
/// return what was issued.
///
/// The rows go in through the product's own [`ApiKeyStore`] and
/// [`BillingStore`], not through a hand-written `INSERT`, for two reasons: the
/// schema is applied by the same call the product makes, so a harness can never
/// seed a column that does not exist; and the plaintext is *generated* here and
/// handed back, so no test pins a credential in source.
///
/// # The partner comes first
///
/// A key is only a credential *for* a partner, and since ADR 0015 the partner
/// is what carries model capability and price. So the partner row and its
/// `partner_models` rows are written before the key. That ordering is not a
/// convenience: the snapshot the process loads on startup joins the two, and a
/// key with no partner row would authenticate and be able to call nothing —
/// which would make every seeded test fail at the model gate rather than at the
/// thing it is about.
///
/// # Idempotent, because restarts are ordinary
///
/// A test that restarts a process against the directory a previous process
/// used calls this again on a database that already holds the rows. Partner
/// rows and prices are therefore upserted — a re-seed replaces the price list
/// with the same one. The fixture derives the same test-only plaintext from the
/// same name and consumer, then registers it with `create_with_plaintext`; the
/// unique hash constraint makes this one repeatable seed idempotent. This is
/// fixture idempotence only: concurrent operator issuance remains guarded by
/// the database's partial unique index.
///
/// # Pre-flight, not a race
///
/// Every spawned instance is also handed [`TEST_SECRET`], so its startup load
/// and this insert agree on the hash. The process starts *after* this returns,
/// so a test never has to wait for a key to appear.
pub fn seed_keys(db_path: &Path, specs: &[KeySpec]) -> Vec<SeededKey> {
    use partner_portal::apikeys::ApiKeyStore;
    use partner_portal::ledger::LedgerPool;
    use std::sync::Arc;

    let pool = Arc::new(LedgerPool::new(db_path.to_path_buf()).expect("open ledger for seeding"));
    let store = ApiKeyStore::new(pool.clone(), TEST_SECRET.to_vec());
    let billing = partner_portal::billing::store::BillingStore::new(pool);

    // One partner per *consumer*, not per key: a restart re-seeds every spec in
    // the same order, and a spec that names a consumer another spec already
    // named must not reopen (and blank) the partner that owns the models the
    // first one works through. Collected first so the partner is created once
    // regardless of how many keys point at it.
    let mut partners: Vec<String> = Vec::new();
    for spec in specs {
        let consumer_id = spec.effective_consumer_id().to_string();
        if !partners.contains(&consumer_id) {
            partners.push(consumer_id);
        }
    }

    for consumer_id in &partners {
        open_partner(&billing, consumer_id);
    }

    // Commercial terms the test declared, applied before the process exists so
    // the first tick already sees them.
    for spec in specs {
        declare_partner_terms(&billing, spec);
    }

    specs
        .iter()
        .map(|spec| {
            let name = spec.name.clone();
            let consumer_id = spec.effective_consumer_id().to_string();
            let models = spec.allowed_models.clone();

            if !models.is_empty() {
                set_models(&billing, &consumer_id, &models);
            }

            // Deliberate test-only determinism: every fixture name and
            // consumer maps to one opaque credential. It never reaches product
            // code or a production database; its only job is to let a restart
            // present the same test credential without recovering plaintext
            // from SQLite (which must be impossible in production).
            let plaintext = seeded_plaintext(&name, &consumer_id);
            let existing = || {
                store
                    .list()
                    .unwrap_or_else(|e| {
                        panic!(
                            "could not inspect existing test keys in {}: {e}",
                            db_path.display()
                        )
                    })
                    .into_iter()
                    .find(|row| {
                        row.name == name
                            && row.consumer_id == consumer_id
                            && row.status == partner_portal::apikeys::KeyStatus::Active
                    })
                    .unwrap_or_else(|| {
                        panic!(
                            "seeding {name} into {} found an unrelated duplicate key hash",
                            db_path.display()
                        )
                    })
            };
            let row = match store.create_with_plaintext(&name, &consumer_id, None, &plaintext) {
                Ok(row) => row,
                // The store names the partial-index violation after it has
                // checked that the partner already owns a live key. In a
                // re-seed, that key must be this fixture's exact row.
                Err(partner_portal::apikeys::ApiKeyError::AlreadyActive(_)) => existing(),
                Err(partner_portal::apikeys::ApiKeyError::Database(error))
                    if is_unique_constraint(&error) =>
                {
                    existing()
                }
                Err(e) => panic!("seeding {name} into {} failed: {e}", db_path.display()),
            };

            SeededKey {
                id: row.id,
                plaintext,
                name: row.name,
                consumer_id: row.consumer_id,
                allowed_models: models,
            }
        })
        .collect()
}

/// Derive the opaque credential used only by this test fixture.
///
/// The product generates keys from OS entropy. Tests need deterministic restart
/// seeding instead, so they use a HMAC under the already test-only hash secret;
/// the resulting text never leaves this process or test database.
fn seeded_plaintext(name: &str, consumer_id: &str) -> String {
    let identity = format!("partner-portal-test-fixture:{consumer_id}:{name}");
    format!(
        "pp_test_{}",
        partner_portal::apikeys::derive_key_hash(TEST_SECRET, &identity)
    )
}

/// Is this the ordinary uniqueness response from a fixture re-seed?
fn is_unique_constraint(error: &rusqlite::Error) -> bool {
    matches!(
        error,
        rusqlite::Error::SqliteFailure(code, _) if code.extended_code == 2067
    )
}

/// Open an account for `consumer_id`, or leave the one that is already there.
///
/// `create_partner` is refused for a partner that exists, which is the right
/// answer for the admin API and the wrong one for a fixture: a re-seed is not
/// trying to create a second account, it is making sure the first one is still
/// there. A PATCH afterwards is what keeps a re-seed from silently changing the
/// billing mode a test configured.
fn open_partner(billing: &partner_portal::billing::store::BillingStore, consumer_id: &str) {
    if billing
        .get_partner(consumer_id)
        .expect("read partner")
        .is_some()
    {
        return;
    }
    billing
        .create_partner(partner_portal::billing::store::NewPartner {
            consumer_id: consumer_id.to_string(),
            name: consumer_id.to_string(),
            // No address: statements are still written, and a harness that
            // never configures SMTP must not be the reason a test fails.
            billing_email: String::new(),
            billing_mode: partner_portal::billing::partner::BillingMode::Invoice,
            payment_terms_minutes: partner_portal::billing::DEFAULT_PAYMENT_TERMS_MINUTES,
        })
        .expect("seed the partner row");
}

/// Apply a key's declared commercial terms, if it declared any.
///
/// Only called for a spec that declares a starting ledger, because the terms
/// have to be in force *before* the first tick: a reconciliation mode applied
/// after the scheduler has closed the first day would leave that day an invoice,
/// and the test would be asserting on a fixture mistake rather than on the
/// product.
fn declare_partner_terms(billing: &partner_portal::billing::store::BillingStore, spec: &KeySpec) {
    use partner_portal::billing::store::PartnerPatch;

    let consumer_id = spec.effective_consumer_id();
    if spec.billing_mode.is_none() && spec.payment_terms_minutes.is_none() {
        return;
    }
    let mode = spec.billing_mode.as_deref().map(|text| match text {
        "invoice" => partner_portal::billing::partner::BillingMode::Invoice,
        "reconciliation" => partner_portal::billing::partner::BillingMode::Reconciliation,
        other => panic!("{other:?} is not a billing mode"),
    });
    billing
        .update_partner(
            consumer_id,
            PartnerPatch {
                name: None,
                billing_email: None,
                billing_mode: mode,
                payment_terms_minutes: spec.payment_terms_minutes,
            },
        )
        .unwrap_or_else(|e| panic!("declare the terms for {consumer_id}: {e}"));
}

/// Write `models` as the partner's price list, replacing whatever was there.
fn set_models(
    billing: &partner_portal::billing::store::BillingStore,
    consumer_id: &str,
    models: &[String],
) {
    let priced = models
        .iter()
        .map(|model| partner_portal::billing::partner::ModelPrice {
            model: model.clone(),
            prices: partner_portal::billing::pricing::PricingSnapshot::new(
                price(SEEDED_INPUT_PER_MILLION),
                price(SEEDED_CACHED_PER_MILLION),
                price(SEEDED_OUTPUT_PER_MILLION),
            ),
        })
        .collect::<Vec<_>>();
    billing
        .replace_models(consumer_id, &priced)
        .expect("seed the partner's models and prices");
}

/// Parse one of the harness's own price constants. Panics by name so a bad
/// constant reads as "the fixture is wrong", not as a request that was refused.
fn price(raw: &str) -> partner_portal::billing::pricing::PricePerMillion {
    partner_portal::billing::pricing::PricePerMillion::parse(raw)
        .unwrap_or_else(|e| panic!("{raw:?} is not a price: {e}"))
}

/// Give every seeded partner a statement anchor from before the test began.
///
/// The walk anchors on `MAX(billing_date)` per partner, and a partner the
/// harness created seconds ago has none — so the first thing the scheduler can
/// close is the *day it was created*, which is a day whose statement the API
/// will not return (`billing_date <= today`). A billing test would then sit
/// waiting for a row that exists and is deliberately hidden, and would learn
/// nothing from the wait.
///
/// So the fixture writes the one row a deployment writes on its own: a
/// zero-amount statement for the day before the day before yesterday. It is a
/// real statement the generator would have written, it costs nothing and
/// suspends nobody, and it means the walk that produces the statement under test
/// starts from a fixed point rather than from the wall clock.
///
/// Deliberately not used by default. A suite that is not about billing should
/// not be anchored, and a suite that wants to prove the anchor behaviour — a
/// new partner is not billed for days it did not exist — must not have it done
/// for it.
pub fn backdate_partners(db_path: &Path, specs: &[KeySpec]) {
    use partner_portal::billing::store::BillingStore;
    use partner_portal::billing::{BillingDay, BillingTimezone, Generator};
    use partner_portal::ledger::LedgerPool;

    let timezone = BillingTimezone::default();
    let pool = Arc::new(LedgerPool::new(db_path.to_path_buf()).expect("open ledger for anchoring"));
    let store = BillingStore::new(pool.clone());
    let today = BillingDay::of(partner_portal::ledger::timefmt::now(), timezone);
    let anchor = today.previous().previous();

    let consumers = {
        let mut seen: Vec<String> = Vec::new();
        for spec in specs {
            let consumer = spec.effective_consumer_id().to_string();
            if !seen.contains(&consumer) {
                seen.push(consumer);
            }
        }
        seen
    };

    for consumer in consumers {
        let Some(partner) = store
            .get_partner(&consumer)
            .expect("read the seeded partner")
        else {
            continue;
        };
        // The *product's* generator, not a hand-written row: the anchor is a
        // statement like any other, and a fixture that built one by hand would
        // be asserting against a shape the product does not produce.
        let generator = Generator::new(timezone, 0);
        let conn = pool.reader().expect("open a reader connection");
        let draft = generator
            .statement_for_day(&conn, &partner, anchor, generator.cutoff_for(anchor))
            .expect("build the anchoring statement");
        store
            .write_statement(&draft)
            .expect("anchor the seeded partner");
    }
}

pub fn open_db(path: &Path) -> Connection {
    let conn = Connection::open(path).expect("open ledger");
    conn.busy_timeout(Duration::from_secs(5))
        .expect("busy timeout");
    conn
}

/// One row of `usage_records`, with the distinctions the tests care about kept
/// intact (`Option` tokens, raw status strings).
#[derive(Clone, Debug)]
pub struct LedgerRow {
    pub id: i64,
    pub request_id: String,
    pub created_at: String,
    pub consumer_id: String,
    pub model: String,
    pub endpoint: String,
    pub streaming: bool,
    pub http_status: Option<i64>,
    pub request_status: String,
    pub usage_status: String,
    pub input_tokens: Option<i64>,
    pub output_tokens: Option<i64>,
    pub cached_tokens: Option<i64>,
    pub ttft_ms: Option<i64>,
    pub duration_ms: i64,
    pub error_message: Option<String>,
    /// Bounded raw text from a non-streaming non-2xx upstream response, if any.
    pub error_body: Option<String>,
}

impl LedgerRow {
    pub fn is_terminal(&self) -> bool {
        self.request_status != "in_flight"
    }

    fn from_row(row: &rusqlite::Row) -> rusqlite::Result<Self> {
        Ok(Self {
            id: row.get(0)?,
            request_id: row.get(1)?,
            created_at: row.get(2)?,
            consumer_id: row.get(3)?,
            model: row.get(4)?,
            endpoint: row.get(5)?,
            streaming: row.get::<_, i64>(6)? != 0,
            http_status: row.get(7)?,
            request_status: row.get(8)?,
            usage_status: row.get(9)?,
            input_tokens: row.get(10)?,
            output_tokens: row.get(11)?,
            cached_tokens: row.get(12)?,
            ttft_ms: row.get(13)?,
            duration_ms: row.get(14)?,
            error_message: row.get(15)?,
            error_body: row.get(16)?,
        })
    }
}

const ROW_COLUMNS: &str = "id, request_id, created_at, consumer_id, model, endpoint, streaming, \
                           http_status, request_status, usage_status, input_tokens, output_tokens, \
                           cached_tokens, ttft_ms, duration_ms, error_message, error_body";

/// Every row, oldest first.
pub fn all_rows(conn: &Connection) -> Vec<LedgerRow> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {ROW_COLUMNS} FROM usage_records ORDER BY id ASC"
        ))
        .expect("prepare ledger query");
    stmt.query_map([], LedgerRow::from_row)
        .expect("query ledger")
        .collect::<Result<Vec<_>, _>>()
        .expect("read ledger rows")
}

pub fn row_count(conn: &Connection) -> i64 {
    conn.query_row("SELECT COUNT(*) FROM usage_records", [], |r| r.get(0))
        .expect("count ledger rows")
}

pub fn find_row(conn: &Connection, request_id: &str) -> Option<LedgerRow> {
    let mut stmt = conn
        .prepare(&format!(
            "SELECT {ROW_COLUMNS} FROM usage_records WHERE request_id = ?1"
        ))
        .expect("prepare ledger lookup");
    let mut rows = stmt
        .query_map([request_id], LedgerRow::from_row)
        .expect("query ledger row");
    rows.next().transpose().expect("read ledger row")
}

pub fn rows_for_consumer(conn: &Connection, consumer_id: &str) -> Vec<LedgerRow> {
    all_rows(conn)
        .into_iter()
        .filter(|r| r.consumer_id == consumer_id)
        .collect()
}

/// `(terminal raw rows, rolled-up request count)`. Both must agree, always.
pub fn raw_rollup_totals(conn: &Connection) -> (i64, i64) {
    let terminal: i64 = conn
        .query_row(
            "SELECT COUNT(*) FROM usage_records WHERE request_status <> 'in_flight'",
            [],
            |r| r.get(0),
        )
        .expect("count terminal rows");
    let rolled: i64 = conn
        .query_row(
            "SELECT COALESCE(SUM(request_count), 0) FROM usage_hourly",
            [],
            |r| r.get(0),
        )
        .expect("sum rollup");
    (terminal, rolled)
}

pub fn in_flight_count(conn: &Connection) -> i64 {
    conn.query_row(
        "SELECT COUNT(*) FROM usage_records WHERE request_status = 'in_flight'",
        [],
        |r| r.get(0),
    )
    .expect("count in-flight rows")
}

/// Wait for a request id to reach a terminal state, then return its row.
pub async fn wait_for_terminal(path: &Path, request_id: &str, timeout: Duration) -> LedgerRow {
    let conn = open_db(path);
    let mut found: Option<LedgerRow> = None;
    let ok = wait_until(timeout, || {
        found = find_row(&conn, request_id).filter(LedgerRow::is_terminal);
        found.is_some()
    })
    .await;
    match found {
        Some(row) if ok => row,
        _ => {
            let rows = all_rows(&conn);
            panic!("request {request_id} never reached a terminal state; ledger: {rows:#?}")
        }
    }
}

/// The one terminal row a test expects, found without knowing its request id.
///
/// The proxy mints the id and only echoes it as `x-request-id` on the paths that
/// forward an upstream response; locally generated errors carry no id. Failure
/// paths therefore identify their record as "the ledger holds exactly this one".
pub async fn wait_for_single_terminal(path: &Path, timeout: Duration) -> LedgerRow {
    let mut rows = wait_for_terminal_count(path, 1, timeout).await;
    rows.pop()
        .expect("wait_for_terminal_count returned one row")
}

/// Wait for the ledger to hold exactly `n` terminal rows.
pub async fn wait_for_terminal_count(path: &Path, n: i64, timeout: Duration) -> Vec<LedgerRow> {
    let conn = open_db(path);
    let mut rows = Vec::new();
    let ok = wait_until(timeout, || {
        rows = all_rows(&conn);
        rows.len() as i64 == n && rows.iter().all(LedgerRow::is_terminal)
    })
    .await;
    assert!(
        ok,
        "expected {n} terminal ledger rows, saw {rows:#?} after {timeout:?}"
    );
    rows
}

/// The bearer token a recorded upstream request carried.
pub fn upstream_bearer(request: &RecordedRequest) -> Option<String> {
    request
        .header("authorization")
        .map(|v| v.trim_start_matches("Bearer ").to_string())
}
