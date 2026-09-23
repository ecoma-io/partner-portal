//! The dashboard's realtime invalidation stream.
//!
//! The contract under test is deliberately narrow: the stream says *that*
//! something changed, never *what*. Every assertion about data therefore has to
//! go through the REST API on a separate request, and the event payload must
//! carry nothing that could belong to another consumer.

use std::time::{Duration, Instant};

use crate::common::{
    Behaviour, BodyReader, MockUpstream, Spec, TestClient, TestServer, WAIT_TIMEOUT, chat_request,
    wait_for_terminal_count,
};
use http::{Method, StatusCode};

/// How long to wait for an invalidation event.
const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

#[tokio::test]
async fn the_stream_requires_a_credential() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let anonymous = client
        .get(&server.url("/api/dashboard/events"), None)
        .await
        .expect("request reaches the server");
    assert_eq!(anonymous.status, StatusCode::UNAUTHORIZED);
    assert_eq!(
        anonymous.header("cache-control").as_deref(),
        Some("no-store")
    );
}

#[tokio::test]
async fn a_write_produces_an_invalidation_event() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 4,
        completion: 4,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .send(
            Method::GET,
            &server.url("/api/dashboard/events"),
            Some(server.key()),
            bytes::Bytes::new(),
            &[],
        )
        .await
        .expect("open the event stream");
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response
            .headers()
            .get("content-type")
            .and_then(|v| v.to_str().ok())
            .unwrap_or_default(),
        "text/event-stream"
    );

    let mut reader = BodyReader::new(response.into_body());

    // The first event tells the client the stream is live. Waiting for it also
    // proves the subscriber existed before the write below.
    let connected = reader
        .read_until("\"type\":\"connected\"", EVENT_TIMEOUT)
        .await
        .expect("connected event");
    assert!(
        connected.contains("event: connected"),
        "the first event must be named `connected`, got {connected:?}"
    );
    assert!(
        connected.contains("retry:"),
        "the stream must suggest a reconnect delay, got {connected:?}"
    );

    // A metered request is a write to the ledger, which is what the stream
    // announces.
    let metered = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(server.key()),
            chat_request("gpt-4o"),
            &[],
        )
        .await;
    assert_eq!(metered.status, StatusCode::OK);
    wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;

    let event = reader
        .read_until("\"type\":\"data_changed\"", EVENT_TIMEOUT)
        .await
        .expect("data_changed event");
    assert!(
        event.contains("data: {\"type\":\"data_changed\"}"),
        "an invalidation event carries no data, got {event:?}"
    );
    // Nothing that could belong to a consumer may appear on a shared stream.
    for forbidden in [
        "test-consumer",
        "gpt-4o",
        "input_tokens",
        "output_tokens",
        "request_id",
    ] {
        assert!(
            !event.contains(forbidden),
            "the shared stream must not carry {forbidden:?}: {event:?}"
        );
    }
}

/// An idle ledger announces nothing — not even while it is starting up.
///
/// A regression guard, and the guard has teeth: this instance used to emit a
/// `data_changed` about one poll interval after the stream opened, with an empty
/// ledger and no traffic at all. What moved the signal was the startup work on
/// the file, not usage — the reader connections opened while the process came up
/// ran `PRAGMA auto_vacuum = INCREMENTAL` on every open (`configure_sqlite`,
/// `src/ledger/mod.rs`), and that pragma is a write: one page-1 WAL frame per
/// open. The statement is now issued only where it can still take effect, a
/// database with no schema yet. A second source was the poller's own baseline,
/// taken inside `SseBroadcaster::start` (`src/dashboard/sse.rs`) on the very
/// connection that will compare against it: a fresh connection's first reading of
/// `data_version` is its own not-yet-read view of the file and moves on its first
/// real read, so a baseline read any earlier than that moves under the poller's
/// feet. Either regression tells every dashboard attached at boot that something
/// changed, and a dashboard refetches on every event.
#[tokio::test]
async fn an_idle_ledger_produces_no_events() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    // A slow poll makes the race that hides a regression impossible rather than
    // unlikely: the subscriber is connected a few milliseconds after readiness,
    // and a startup artifact is not emitted until the first tick after it. The
    // window below is several ticks wide.
    let spec = Spec::new(&upstream).with_sse_poll_interval(500);
    let server = TestServer::start(spec).await;
    let client = TestClient::new();

    let response = client
        .send(
            Method::GET,
            &server.url("/api/dashboard/events"),
            Some(server.key()),
            bytes::Bytes::new(),
            &[],
        )
        .await
        .expect("open the event stream");
    let mut reader = BodyReader::new(response.into_body());
    reader
        .read_until("\"type\":\"connected\"", EVENT_TIMEOUT)
        .await
        .expect("connected event");

    // Nothing writes, so nothing may be announced. A stream that emitted events
    // on a timer would make every dashboard poll continuously.
    let quiet = reader
        .read_until("\"data_changed\"", Duration::from_millis(1_500))
        .await;
    assert!(
        quiet.is_err(),
        "an idle ledger must not produce change events, got {quiet:?}"
    );
    assert!(
        !reader.so_far().contains("data_changed"),
        "no change event may arrive while nothing writes"
    );
}

/// A read-only dashboard query must not announce a change.
///
/// This is what keeps a dashboard idle: an invalidation makes every subscriber
/// refetch, and the refetch goes back through this same query API. If reading
/// were what moved the change signal, every refetch would cause the next
/// invalidation and a dashboard left open would reload in a loop, with the
/// polling rate set by the poll interval rather than by the traffic.
///
/// That loop existed. Every dashboard query opens its own reader connection,
/// and each one used to run `PRAGMA auto_vacuum = INCREMENTAL`
/// (`configure_sqlite`, `src/ledger/mod.rs`), which committed a page and moved
/// the `PRAGMA data_version` the poller compares: three `GET
/// /api/dashboard/summary` calls with nothing writing anywhere moved the
/// counter 4 → 5 → 6 → 7, each followed by a `data_changed` one poll interval
/// later. The statement is now issued only where it can take effect — a
/// database with no schema yet — and two witnesses below hold the line: the
/// ledger's own change counter, and the stream.
#[tokio::test]
async fn a_read_only_dashboard_query_announces_nothing() {
    let upstream = MockUpstream::start(Behaviour::chat_stream(1, 1)).await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let client = TestClient::new();

    let response = client
        .send(
            Method::GET,
            &server.url("/api/dashboard/events"),
            Some(server.key()),
            bytes::Bytes::new(),
            &[],
        )
        .await
        .expect("open the event stream");
    let mut reader = BodyReader::new(response.into_body());
    reader
        .read_until("\"type\":\"connected\"", EVENT_TIMEOUT)
        .await
        .expect("connected event");

    // Let any startup announcement arrive first, so that a failure below can
    // only mean the read path.
    let settle = Instant::now() + Duration::from_millis(700);
    while change_events(&reader) == 0 && Instant::now() < settle {
        if let Ok(None) = reader.next_frame(Duration::from_millis(100)).await {
            break;
        }
    }
    assert!(
        change_events(&reader) == 0,
        "an idle instance announced {} change(s) before any client read \
         anything; see `an_idle_ledger_produces_no_events`",
        change_events(&reader)
    );

    // A second witness, independent of the stream: a connection of our own that
    // watches the ledger's change counter. A fresh connection's first reading is
    // its own not-yet-read view of the file and moves on its first real read, so
    // it is settled here the same way the poller settles its own before the
    // value is trusted.
    let watcher = crate::common::open_db(&server.db_path);
    let version = |conn: &rusqlite::Connection| -> i64 {
        conn.query_row("PRAGMA data_version", [], |row| row.get(0))
            .expect("read data_version")
    };
    let mut settled = version(&watcher);
    for _ in 0..8 {
        watcher
            .query_row("SELECT count(*) FROM sqlite_master", [], |row| {
                row.get::<_, i64>(0)
            })
            .expect("touch the ledger");
        let next = version(&watcher);
        if next == settled {
            break;
        }
        settled = next;
    }
    assert_eq!(
        version(&watcher),
        settled,
        "the witness connection never settled, so it cannot witness anything"
    );

    // Two reads, one after the other. Neither may be visible to a subscriber.
    for path in ["/api/dashboard/summary", "/api/dashboard/requests?limit=5"] {
        let before = change_events(&reader);
        let db_before = version(&watcher);
        let page = client.get_json(&server.url(path), Some(server.key())).await;
        assert_eq!(
            page.status,
            StatusCode::OK,
            "the read itself must succeed: {path}"
        );

        // Several poll intervals, so a change the poller noticed after the
        // query came back is still counted.
        let ended = observe(&mut reader, Duration::from_millis(600)).await;

        assert_eq!(
            change_events(&reader),
            before,
            "reading {path} is not a change to the ledger, so it must not \
             invalidate subscribers; got {} event(s) (stream ended: {ended}) — \
             a query that moves `PRAGMA data_version` makes every subscriber \
             refetch, and the refetch is another query, so the dashboard feeds \
             itself at the poll interval",
            change_events(&reader)
        );
        assert_eq!(
            version(&watcher),
            db_before,
            "reading {path} changed the ledger's `PRAGMA data_version` — the \
             query is not read-only. A dashboard read opens a fresh reader \
             connection (`LedgerPool::read`, src/ledger/pool.rs), so anything \
             its setup writes is written once per query"
        );
    }
}

/// Read frames for `window`. Returns true if the body ended.
async fn observe(reader: &mut BodyReader, window: Duration) -> bool {
    let deadline = Instant::now() + window;
    while Instant::now() < deadline {
        if let Ok(None) = reader.next_frame(Duration::from_millis(100)).await {
            return true;
        }
    }
    false
}

/// How many change events the stream has delivered so far.
fn change_events(reader: &BodyReader) -> usize {
    reader.so_far().matches("\"type\":\"data_changed\"").count()
}

#[tokio::test]
async fn every_connected_client_is_invalidated() {
    let upstream = MockUpstream::start(Behaviour::ChatJson {
        prompt: 2,
        completion: 2,
        cached: 0,
    })
    .await;
    let server = TestServer::start(Spec::new(&upstream)).await;
    let key = server.key().to_string();

    let mut readers = Vec::new();
    for _ in 0..3 {
        let client = TestClient::new();
        let response = client
            .send(
                Method::GET,
                &server.url("/api/dashboard/events"),
                Some(&key),
                bytes::Bytes::new(),
                &[],
            )
            .await
            .expect("open the event stream");
        let mut reader = BodyReader::new(response.into_body());
        reader
            .read_until("\"type\":\"connected\"", EVENT_TIMEOUT)
            .await
            .expect("connected event");
        readers.push(reader);
    }

    let client = TestClient::new();
    let metered = client
        .call(
            Method::POST,
            &server.url("/v1/chat/completions"),
            Some(&key),
            chat_request("gpt-4o"),
            &[],
        )
        .await;
    assert_eq!(metered.status, StatusCode::OK);
    wait_for_terminal_count(&server.db_path, 1, WAIT_TIMEOUT).await;

    for (i, reader) in readers.iter_mut().enumerate() {
        reader
            .read_until("\"type\":\"data_changed\"", EVENT_TIMEOUT)
            .await
            .unwrap_or_else(|e| panic!("subscriber {i} must be invalidated: {e}"));
    }
}
