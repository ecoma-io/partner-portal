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
//!    readiness-gated startup and signal helpers.
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

/// Default credential for the first configured key.
pub const CLIENT_KEY: &str = "sk-local-test-key";
/// Default consumer identity for that key.
pub const CONSUMER: &str = "test-consumer";
/// Credential the proxy is configured to present upstream.
pub const UPSTREAM_KEY: &str = "sk-upstream-secret";

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

/// One configured key.
#[derive(Clone, Debug)]
pub struct KeySpec {
    pub key: String,
    pub name: String,
    pub consumer_id: Option<String>,
}

impl KeySpec {
    pub fn new(key: &str, name: &str) -> Self {
        Self {
            key: key.to_string(),
            name: name.to_string(),
            consumer_id: None,
        }
    }

    pub fn with_consumer(mut self, consumer_id: &str) -> Self {
        self.consumer_id = Some(consumer_id.to_string());
        self
    }

    /// The identity the ledger will record for this key.
    pub fn effective_consumer_id(&self) -> &str {
        self.consumer_id.as_deref().unwrap_or(&self.name)
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
}

impl Spec {
    /// A spec pointed at `upstream`, with its ledger inside the server's own
    /// directory.
    pub fn new(upstream: &MockUpstream) -> Self {
        Self {
            upstream_url: upstream.url(),
            upstream_key: UPSTREAM_KEY.to_string(),
            upstream_timeout_secs: 10,
            keys: vec![KeySpec::new(CLIENT_KEY, CONSUMER)],
            db_name: PathBuf::from("ledger.db"),
            port: free_port(),
            queue_size: 10_000,
            batch_size: 100,
            batch_timeout_ms: 10,
            max_body_size: 10 * 1024 * 1024,
            shutdown_grace_secs: 1,
            sse_poll_interval_ms: 100,
            retention_days: 60,
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

    pub fn yaml(&self) -> String {
        let mut keys = String::new();
        for key in &self.keys {
            keys.push_str(&format!(
                "  - key: {}\n    name: {}\n",
                yaml_str(&key.key),
                yaml_str(&key.name),
            ));
            if let Some(consumer_id) = &key.consumer_id {
                keys.push_str(&format!("    consumer_id: {}\n", yaml_str(consumer_id)));
            }
        }

        format!(
            "server:\n  \
               listen: {listen}\n  \
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
             keys:\n{keys}\
             database:\n  \
               path: {db}\n  \
               retention_days: {retention}\n  \
               queue_size: {queue}\n  \
               batch_size: {batch}\n  \
               batch_timeout_ms: {batch_timeout}\n  \
               retention_interval_secs: 3600\n  \
               retention_batch_size: 2000\n",
            listen = yaml_str(&format!("127.0.0.1:{}", self.port)),
            grace = self.shutdown_grace_secs,
            max_body = self.max_body_size,
            sse = self.sse_poll_interval_ms,
            upstream = yaml_str(&self.upstream_url),
            upstream_key = yaml_str(&self.upstream_key),
            timeout = self.upstream_timeout_secs,
            db = yaml_str(&self.db_name.display().to_string()),
            retention = self.retention_days,
            queue = self.queue_size,
            batch = self.batch_size,
            batch_timeout = self.batch_timeout_ms,
        )
    }
}

/// Quote a scalar for YAML. Test values are ordinary strings, but a key or a
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

        let log = File::create(&log_path).expect("create log file");
        let child = Command::new(BIN)
            .env("PARTNER_PORTAL_CONFIG", &config_path)
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
                Ok(response) if response.status == StatusCode::OK => return,
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

    /// The key the harness configures first.
    pub fn key(&self) -> &str {
        &self.spec.keys[0].key
    }

    pub fn consumer(&self) -> &str {
        self.spec.keys[0].effective_consumer_id()
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
