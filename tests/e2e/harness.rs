//! Shared harness for the end-to-end suite.
//!
//! Everything here drives the **real binary** as a child process: real signals,
//! a real SQLite file, real HTTP. The end-to-end tests exist because the
//! behaviour they cover — draining on shutdown, two writers on one database,
//! crash recovery, reloading a config file — is defined by what the process does
//! at the operating-system boundary, not by what a function returns when called
//! in-process.
//!
//! The traffic is shaped so results are *checkable rather than plausible*: every
//! request carries a unique model name, so the set of model names the mock
//! upstream saw can be compared for exact equality against the set the ledger
//! recorded. One lost record and one duplicated record cannot cancel out.

#![allow(dead_code)] // each test file uses a different subset

pub use std::io::Write;
pub use std::net::TcpListener;
pub use std::process::{Child, Command, Stdio};
pub use std::sync::Arc;
pub use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

// Re-exported so a test file only needs `use crate::harness::*`.
pub use std::path::{Path, PathBuf};
pub use std::time::{Duration, Instant};

pub use rusqlite::Connection;
pub use serde_json::Value;

pub use axum::{Json, Router, extract::State, routing::post};
pub use bytes::Bytes;
pub use http_body_util::{BodyExt, Full};
pub use hyper_util::client::legacy::Client;
pub use hyper_util::client::legacy::connect::HttpConnector;
pub use hyper_util::rt::TokioExecutor;
pub use serde_json::json;

pub const LOCAL_KEY: &str = "local-test-key";
pub const UPSTREAM_KEY: &str = "upstream-secret";

/// How long any single wait may take before the test gives up.
pub const WAIT: Duration = Duration::from_secs(30);

// ---------------------------------------------------------------------------
// Mock upstream
// ---------------------------------------------------------------------------

/// The upstream: answers chat completions and records every model name it saw.
#[derive(Clone)]
pub struct MockUpstream {
    inner: Arc<MockInner>,
}

pub struct MockInner {
    models_seen: parking_lot::Mutex<Vec<String>>,
    /// Every `Authorization` value the upstream received, in order. This is how
    /// credential replacement is observed from the upstream's side.
    auth_seen: parking_lot::Mutex<Vec<String>>,
    /// Stalls the response, so a request is still in flight when a signal lands.
    hang: AtomicBool,
}

impl MockUpstream {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(MockInner {
                models_seen: parking_lot::Mutex::new(Vec::new()),
                auth_seen: parking_lot::Mutex::new(Vec::new()),
                hang: AtomicBool::new(false),
            }),
        }
    }

    pub fn models_seen(&self) -> Vec<String> {
        self.inner.models_seen.lock().clone()
    }

    pub fn set_hang(&self, hang: bool) {
        self.inner.hang.store(hang, Ordering::Release);
    }

    /// The most recent `Authorization` the upstream saw, if any.
    pub fn last_auth(&self) -> Option<String> {
        self.inner.auth_seen.lock().last().cloned()
    }
}

pub async fn mock_chat_completions(
    State(mock): State<MockUpstream>,
    headers: axum::http::HeaderMap,
    body: Bytes,
) -> Json<Value> {
    if let Some(auth) = headers.get("authorization").and_then(|v| v.to_str().ok()) {
        mock.inner.auth_seen.lock().push(auth.to_string());
    }

    let request: Value = serde_json::from_slice(&body).unwrap_or(json!({}));
    let model = request
        .get("model")
        .and_then(|v| v.as_str())
        .unwrap_or("unknown")
        .to_string();

    // Recorded before any stall: the request genuinely reached the upstream.
    mock.inner.models_seen.lock().push(model.clone());

    if mock.inner.hang.load(Ordering::Acquire) {
        // Long enough that a signal always lands while this is in flight, short
        // enough that a test that waits for the timeout still finishes.
        tokio::time::sleep(Duration::from_secs(20)).await;
    }

    Json(json!({
        "id": "chatcmpl-test",
        "object": "chat.completion",
        "model": model,
        "choices": [{
            "index": 0,
            "message": { "role": "assistant", "content": "hi" },
            "finish_reason": "stop"
        }],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 5,
            "total_tokens": 15,
            "prompt_tokens_details": { "cached_tokens": 2 }
        }
    }))
}

/// Start the mock upstream on an ephemeral port; returns it and its base URL.
pub async fn start_mock_upstream() -> (MockUpstream, String) {
    let mock = MockUpstream::new();
    let app = Router::new()
        .route("/v1/chat/completions", post(mock_chat_completions))
        .with_state(mock.clone());

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    (mock, format!("http://{addr}"))
}

// ---------------------------------------------------------------------------
// Instance under test
// ---------------------------------------------------------------------------

/// A `partner-portal` child process.
pub struct Instance {
    pub child: Child,
    pub port: u16,
    /// Retained so a test can publish a new configuration and watch it land.
    pub config_path: PathBuf,
}

impl Instance {
    /// Start an instance listening on an ephemeral port, sharing `db_path`.
    pub fn start(name: &str, dir: &Path, db_path: &Path, upstream: &str) -> Self {
        let port = free_port();
        let config_path = dir.join(format!("config-{name}.yaml"));
        write_config(&config_path, port, db_path, upstream);

        let child = Command::new(binary_path())
            .env("PARTNER_PORTAL_CONFIG", &config_path)
            .env("RUST_LOG", "warn")
            .current_dir(dir)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("failed to spawn partner-portal");

        Self {
            child,
            port,
            config_path,
        }
    }

    pub fn base_url(&self) -> String {
        format!("http://127.0.0.1:{}", self.port)
    }

    pub fn pid(&self) -> u32 {
        self.child.id()
    }

    /// Wait until `/readyz` answers 200.
    pub async fn wait_ready(&self, timeout: Duration) -> bool {
        wait_for_status(&self.base_url(), "/readyz", 200, timeout).await
    }

    /// Wait until `/healthz` answers 200.
    pub async fn wait_healthy(&self, timeout: Duration) -> bool {
        wait_for_status(&self.base_url(), "/healthz", 200, timeout).await
    }

    /// Wait until `/readyz` answers 503 — the state a draining instance must
    /// reach before the balancer takes it out of rotation.
    pub async fn wait_unready(&self, timeout: Duration) -> bool {
        wait_for_status(&self.base_url(), "/readyz", 503, timeout).await
    }

    /// Deliver a signal with `kill(2)` directly.
    ///
    /// Not the `kill(1)` binary: the test must not depend on a coreutils or
    /// procps utility being present in the environment it runs in.
    pub fn signal(&self, signal: libc::c_int) {
        // SAFETY: `kill` is async-signal-safe and takes no pointers; the pid
        // belongs to a child this process spawned and has not reaped.
        let rc = unsafe { libc::kill(self.pid() as libc::pid_t, signal) };
        assert_eq!(
            rc,
            0,
            "kill({}, {signal}) failed: {}",
            self.pid(),
            std::io::Error::last_os_error()
        );
    }

    /// Wait for the process to exit; `None` on timeout.
    pub async fn wait_exit(&mut self, timeout: Duration) -> Option<std::process::ExitStatus> {
        let deadline = Instant::now() + timeout;
        loop {
            match self.child.try_wait() {
                Ok(Some(status)) => return Some(status),
                Ok(None) if Instant::now() < deadline => {
                    tokio::time::sleep(Duration::from_millis(25)).await;
                }
                Ok(None) => return None,
                Err(_) => return None,
            }
        }
    }

    /// SIGTERM and wait for exit.
    pub async fn terminate_and_wait(
        &mut self,
        timeout: Duration,
    ) -> Option<std::process::ExitStatus> {
        self.signal(libc::SIGTERM);
        self.wait_exit(timeout).await
    }
}

impl Drop for Instance {
    fn drop(&mut self) {
        let _ = self.child.kill();
        let _ = self.child.wait();
    }
}

pub fn binary_path() -> PathBuf {
    PathBuf::from(env!("CARGO_BIN_EXE_partner-portal"))
}

pub fn free_port() -> u16 {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

pub fn write_config(path: &Path, port: u16, db_path: &Path, upstream: &str) {
    write_config_full(
        path,
        port,
        db_path,
        upstream,
        UPSTREAM_KEY,
        LOCAL_KEY,
        "tester",
    );
}

/// Write a configuration file, varying the fields a reload test needs to move.
pub fn write_config_full(
    path: &Path,
    port: u16,
    db_path: &Path,
    upstream: &str,
    upstream_key: &str,
    local_key: &str,
    local_name: &str,
) {
    let yaml = format!(
        r#"server:
  listen: "127.0.0.1:{port}"
  shutdown_grace_secs: 1
  sse_poll_interval_ms: 100
  max_body_size: 1048576
upstream:
  base_url: "{upstream}"
  api_key: "{upstream_key}"
  timeout_secs: 15
  connect_timeout_secs: 2
keys:
  - key: "{local_key}"
    name: "{local_name}"
database:
  path: "{db}"
  queue_size: 5000
  batch_size: 50
  batch_timeout_ms: 20
  retention_interval_secs: 3600
"#,
        db = db_path.display(),
    );

    write_raw(path, &yaml);
}

/// Write a configuration with several local keys, each with its own consumer.
pub fn write_config_multi(
    path: &Path,
    port: u16,
    db_path: &Path,
    upstream: &str,
    keys: &[(&str, &str, &str)],
) {
    let mut list = String::new();
    for (key, name, consumer) in keys {
        list.push_str(&format!(
            "  - key: \"{key}\"\n    name: \"{name}\"\n    consumer_id: \"{consumer}\"\n"
        ));
    }

    let yaml = format!(
        r#"server:
  listen: "127.0.0.1:{port}"
  shutdown_grace_secs: 1
  sse_poll_interval_ms: 100
upstream:
  base_url: "{upstream}"
  api_key: "{UPSTREAM_KEY}"
  timeout_secs: 15
keys:
{list}database:
  path: "{db}"
  queue_size: 5000
  batch_size: 50
  batch_timeout_ms: 20
  retention_interval_secs: 3600
"#,
        db = db_path.display(),
    );

    write_raw(path, &yaml);
}

/// Write arbitrary bytes to a path atomically.
///
/// Via a temp file + rename so a watcher never observes a partial file, which is
/// also how an operator's config management would publish a change.
pub fn write_raw(path: &Path, contents: &str) {
    let tmp = path.with_extension("yaml.tmp");
    let mut file = std::fs::File::create(&tmp).unwrap();
    file.write_all(contents.as_bytes()).unwrap();
    file.sync_all().unwrap();
    drop(file);
    std::fs::rename(&tmp, path).unwrap();
}

/// Poll a path on an instance until it returns `want`.
pub async fn wait_for_status(base: &str, path: &str, want: u16, timeout: Duration) -> bool {
    let deadline = Instant::now() + timeout;
    loop {
        if raw_get(base, path).await == Some(want) {
            return true;
        }
        if Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(25)).await;
    }
}

/// Minimal HTTP/1.1 GET returning only the status code.
///
/// The probe path is deliberately independent of the JSON client: a readiness
/// check must work even while the instance is busy or refusing request bodies.
pub async fn raw_get(base: &str, path: &str) -> Option<u16> {
    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};

    let addr = base.trim_start_matches("http://");
    let stream = tokio::time::timeout(Duration::from_secs(2), tokio::net::TcpStream::connect(addr))
        .await
        .ok()?
        .ok()?;

    let mut stream = BufReader::new(stream);
    stream
        .get_mut()
        .write_all(
            format!("GET {path} HTTP/1.1\r\nHost: {addr}\r\nConnection: close\r\n\r\n").as_bytes(),
        )
        .await
        .ok()?;

    let mut line = String::new();
    stream.read_line(&mut line).await.ok()?;

    line.split_whitespace().nth(1)?.parse().ok()
}

// ---------------------------------------------------------------------------
// Client
// ---------------------------------------------------------------------------

/// A JSON POST client built on the hyper stack the crate already uses.
#[derive(Clone)]
pub struct ProxyClient {
    pub client: Client<HttpConnector, http_body_util::combinators::BoxBody<Bytes, hyper::Error>>,
}

impl ProxyClient {
    pub fn new() -> Self {
        Self {
            client: Client::builder(TokioExecutor::new()).build_http(),
        }
    }

    /// POST a chat completion carrying a unique model name.
    pub async fn chat(&self, base: &str, model: &str) -> Result<u16, String> {
        self.chat_with_key(base, model, LOCAL_KEY).await
    }

    /// GET a path with an optional bearer key; returns `(status, body)`.
    pub async fn get(&self, base: &str, path: &str, key: Option<&str>) -> (u16, String) {
        let mut builder = hyper::Request::builder()
            .method("GET")
            .uri(format!("{base}{path}"));
        if let Some(key) = key {
            builder = builder.header("authorization", format!("Bearer {key}"));
        }

        let request = builder
            .body(
                Full::new(Bytes::new())
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .expect("failed to build request");

        match self.client.request(request).await {
            Ok(response) => {
                let status = response.status().as_u16();
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                    .unwrap_or_default();
                (status, body)
            }
            Err(e) => (0, e.to_string()),
        }
    }

    /// GET with extra headers, for probing that a client-supplied identity is
    /// ignored.
    pub async fn get_with_headers(
        &self,
        base: &str,
        path: &str,
        key: Option<&str>,
        extra: &[(&str, &str)],
    ) -> (u16, String) {
        let mut builder = hyper::Request::builder()
            .method("GET")
            .uri(format!("{base}{path}"));
        if let Some(key) = key {
            builder = builder.header("authorization", format!("Bearer {key}"));
        }
        for (name, value) in extra {
            builder = builder.header(*name, *value);
        }

        let request = builder
            .body(
                Full::new(Bytes::new())
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .expect("failed to build request");

        match self.client.request(request).await {
            Ok(response) => {
                let status = response.status().as_u16();
                let body = response
                    .into_body()
                    .collect()
                    .await
                    .map(|c| String::from_utf8_lossy(&c.to_bytes()).into_owned())
                    .unwrap_or_default();
                (status, body)
            }
            Err(e) => (0, e.to_string()),
        }
    }

    /// Read an SSE stream until `needle` appears in the accumulated text.
    ///
    /// Returns the text seen and how long it took; `None` on timeout.
    pub async fn read_sse_until(
        &self,
        base: &str,
        path: &str,
        key: &str,
        needle: &str,
        timeout: Duration,
    ) -> Option<(String, Duration)> {
        use futures::StreamExt;

        let request = hyper::Request::builder()
            .method("GET")
            .uri(format!("{base}{path}"))
            .header("authorization", format!("Bearer {key}"))
            .body(
                Full::new(Bytes::new())
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .expect("failed to build request");

        let started = Instant::now();
        let response = self.client.request(request).await.ok()?;
        let mut stream = response.into_body().into_data_stream();
        let mut seen = String::new();

        while started.elapsed() < timeout {
            let remaining = timeout.saturating_sub(started.elapsed());
            match tokio::time::timeout(remaining, stream.next()).await {
                Ok(Some(Ok(chunk))) => {
                    seen.push_str(&String::from_utf8_lossy(&chunk));
                    if seen.contains(needle) {
                        return Some((seen, started.elapsed()));
                    }
                }
                Ok(Some(Err(_))) | Ok(None) => break,
                Err(_) => break,
            }
        }

        if seen.contains(needle) {
            Some((seen, started.elapsed()))
        } else {
            None
        }
    }

    /// POST with an explicit local credential, so a reload that rotates the key
    /// list can be observed from the client's side.
    pub async fn chat_with_key(&self, base: &str, model: &str, key: &str) -> Result<u16, String> {
        let body = json!({
            "model": model,
            "messages": [{ "role": "user", "content": "hello" }],
            "stream": false,
        })
        .to_string();

        let request = hyper::Request::builder()
            .method("POST")
            .uri(format!("{base}/v1/chat/completions"))
            .header("content-type", "application/json")
            .header("authorization", format!("Bearer {key}"))
            .body(
                Full::new(Bytes::from(body))
                    .map_err(|never| match never {})
                    .boxed(),
            )
            .map_err(|e| e.to_string())?;

        let response = self
            .client
            .request(request)
            .await
            .map_err(|e| e.to_string())?;

        Ok(response.status().as_u16())
    }
}

// ---------------------------------------------------------------------------
// Traffic driver
// ---------------------------------------------------------------------------

/// The set of instances currently in rotation.
///
/// A worker picks from this list each iteration, so widening it to two URLs is
/// exactly what "both instances are serving" means, and narrowing it back is
/// what "A left rotation" means.
#[derive(Clone)]
pub struct Rotation {
    targets: Arc<parking_lot::RwLock<Vec<String>>>,
    stop: Arc<AtomicBool>,
    counter: Arc<AtomicUsize>,
    accepted: Arc<AtomicUsize>,
}

impl Rotation {
    pub fn new() -> Self {
        Self {
            targets: Arc::new(parking_lot::RwLock::new(Vec::new())),
            stop: Arc::new(AtomicBool::new(false)),
            counter: Arc::new(AtomicUsize::new(0)),
            accepted: Arc::new(AtomicUsize::new(0)),
        }
    }

    pub fn add(&self, url: String) {
        self.targets.write().push(url);
    }

    pub fn remove(&self, url: &str) {
        self.targets.write().retain(|t| t != url);
    }

    pub fn accepted(&self) -> usize {
        self.accepted.load(Ordering::Relaxed)
    }

    /// Stop every worker at the top of its next iteration.
    pub fn stop(&self) {
        self.stop.store(true, Ordering::Release);
    }

    /// Run `workers` request loops until `stop` is set.
    pub fn run(&self, workers: usize, client: ProxyClient) -> Vec<tokio::task::JoinHandle<()>> {
        (0..workers)
            .map(|_| {
                let rotation = self.clone();
                let client = client.clone();
                tokio::spawn(async move {
                    while !rotation.stop.load(Ordering::Acquire) {
                        // Clone the target list out of the lock rather than
                        // holding the guard across the await.
                        let targets = rotation.targets.read().clone();
                        if targets.is_empty() {
                            tokio::time::sleep(Duration::from_millis(5)).await;
                            continue;
                        }

                        let n = rotation.counter.fetch_add(1, Ordering::Relaxed);
                        let target = &targets[n % targets.len()];
                        let model = format!("model-{n}");

                        if let Ok(200) = client.chat(target, &model).await {
                            rotation.accepted.fetch_add(1, Ordering::Relaxed);
                        }

                        tokio::time::sleep(Duration::from_millis(2)).await;
                    }
                })
            })
            .collect()
    }
}

// ---------------------------------------------------------------------------
// Ledger inspection
// ---------------------------------------------------------------------------

// Ledger inspection
// ---------------------------------------------------------------------------

pub struct LedgerSnapshot {
    pub models: Vec<String>,
    pub request_ids: Vec<String>,
    pub in_flight: i64,
    pub non_terminal: i64,
    pub integrity: String,
}

pub fn read_ledger(db_path: &Path) -> LedgerSnapshot {
    let conn = Connection::open(db_path).expect("failed to open ledger for inspection");

    let models = column(&conn, "SELECT model FROM usage_records ORDER BY id");
    let request_ids = column(&conn, "SELECT request_id FROM usage_records ORDER BY id");

    let count = |sql: &str| -> i64 { conn.query_row(sql, [], |row| row.get(0)).unwrap() };

    LedgerSnapshot {
        models,
        request_ids,
        in_flight: count("SELECT COUNT(*) FROM usage_records WHERE request_status = 'in_flight'"),
        non_terminal: count(
            "SELECT COUNT(*) FROM usage_records \
             WHERE request_status NOT IN ('completed','failed','interrupted')",
        ),
        integrity: conn
            .query_row("PRAGMA integrity_check", [], |row| row.get(0))
            .unwrap(),
    }
}

/// Per-status counts, so a run reports what actually happened rather than just
/// "ok".
pub fn status_counts(db_path: &Path) -> Vec<(String, i64)> {
    let conn = Connection::open(db_path).unwrap();
    let mut stmt = conn
        .prepare(
            "SELECT request_status, COUNT(*) FROM usage_records \
             GROUP BY request_status ORDER BY request_status",
        )
        .unwrap();
    let rows = stmt
        .query_map([], |row| Ok((row.get(0)?, row.get(1)?)))
        .unwrap();
    rows.map(|r| r.unwrap()).collect()
}

pub fn column(conn: &Connection, sql: &str) -> Vec<String> {
    let mut stmt = conn.prepare(sql).unwrap();
    let rows = stmt.query_map([], |row| row.get::<_, String>(0)).unwrap();
    rows.map(|r| r.unwrap()).collect()
}

/// The ledger must satisfy these after any rolling update, so they are asserted
/// together rather than being repeated per test.
pub fn assert_ledger_sound(ledger: &LedgerSnapshot) {
    assert_eq!(
        ledger.integrity, "ok",
        "integrity check failed: the database was corrupted by concurrent writers"
    );
    assert_eq!(
        ledger.non_terminal, 0,
        "every accepted request must reach a terminal state"
    );
    assert_eq!(
        ledger.in_flight, 0,
        "no record may be left in_flight after a graceful rolling update"
    );

    let mut ids = ledger.request_ids.clone();
    ids.sort();
    let total = ids.len();
    ids.dedup();
    assert_eq!(
        ids.len(),
        total,
        "duplicate request_id in the ledger: {total} rows collapsed to {} identities",
        ids.len()
    );
}

/// The exact check: the set of requests the upstream served must equal the set
/// of requests the ledger recorded.
///
/// Set equality rather than a count, because one lost record and one duplicated
/// record would cancel out in a count and leave the bug invisible.
pub fn assert_no_divergence(mock: &MockUpstream, ledger: &LedgerSnapshot) {
    let upstream: std::collections::HashSet<String> = mock.models_seen().into_iter().collect();
    let recorded: std::collections::HashSet<String> = ledger.models.iter().cloned().collect();

    let missing: Vec<_> = upstream.difference(&recorded).take(5).cloned().collect();
    let extra: Vec<_> = recorded.difference(&upstream).take(5).cloned().collect();

    assert!(
        missing.is_empty(),
        "{} request(s) reached the upstream but have no ledger record, e.g. {missing:?}",
        upstream.difference(&recorded).count()
    );
    assert!(
        extra.is_empty(),
        "{} ledger record(s) have no corresponding upstream request, e.g. {extra:?}",
        recorded.difference(&upstream).count()
    );
    assert_eq!(
        ledger.models.len(),
        upstream.len(),
        "ledger row count must match the upstream request count exactly"
    );
}
