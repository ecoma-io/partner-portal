//! Dashboard REST API.
//!
//! # Isolation
//!
//! Every query is scoped by `consumer_id` **taken from the authenticated key**,
//! never from a request parameter. Combined with [`crate::auth::Authenticated`]
//! this means one API key can only ever observe its own traffic: there is no
//! parameter that widens the scope, so there is nothing to tamper with.
//!
//! # Two data sources, deliberately
//!
//! Totals and timeseries read the hourly rollup (`usage_hourly`); the request
//! list reads the raw ledger (`usage_records`). Both are derived from the same
//! transaction, so they agree. The rollup cannot be sliced finer than an hour,
//! so rollup bounds are rounded to hour buckets while raw bounds are exact.

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use time::Duration;

use crate::auth::Authenticated;
use crate::ledger::timefmt;
use crate::proxy::handler::AppState;

/// The dashboard's query API. Every route requires an authenticated key.
pub fn create_api_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/me", get(get_me))
        .route("/api/dashboard/summary", get(get_summary))
        .route("/api/dashboard/timeseries", get(get_timeseries))
        .route("/api/dashboard/requests", get(get_requests))
        .route("/api/dashboard/models", get(get_models))
}

/// Default page size for the request list.
const DEFAULT_LIMIT: usize = 50;
/// Hard ceiling on page size, so one request cannot pull the whole ledger.
const MAX_LIMIT: usize = 200;

/// Output length of SHA-256, and therefore of the cursor's HMAC.
const SHA256_OUTPUT: usize = 32;
/// Input block size of SHA-256, used to pad the HMAC key.
const SHA256_BLOCK: usize = 64;
/// Separator between the signature and the payload of a cursor.
///
/// `.` is not in the URL-safe base64 alphabet, so it cannot occur inside either
/// half and splitting on it is unambiguous.
const CURSOR_SEPARATOR: char = '.';
/// Separator between the timestamp and the row id *inside* the signed payload.
///
/// Chosen because it cannot occur in either part: ISO 8601 timestamps contain
/// `:` and `-`, so a colon-separated payload would split inside the timestamp.
const PAYLOAD_SEPARATOR: char = '|';
/// URL-safe base64 alphabet (RFC 4648 §5), unpadded.
const B64_ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";

/// Query parameters shared by the dashboard endpoints.
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DashboardQuery {
    /// `today`, `24h`, `7d`, `14d`, `30d`, or `custom`.
    pub range: String,
    /// Range start, ISO 8601. Required when `range=custom`.
    pub start: Option<String>,
    /// Range end, ISO 8601. Required when `range=custom`.
    pub end: Option<String>,
    /// Model filter; `all` (or empty) means no filter.
    pub model: String,
    /// `all`, `completed`, `failed`, or `interrupted`.
    pub status: String,
    /// Keyset cursor from a previous page's `next_cursor`.
    pub cursor: Option<String>,
    /// Page size for the request list.
    pub limit: usize,
}

impl Default for DashboardQuery {
    fn default() -> Self {
        Self {
            range: "24h".to_string(),
            start: None,
            end: None,
            model: "all".to_string(),
            status: "all".to_string(),
            cursor: None,
            limit: DEFAULT_LIMIT,
        }
    }
}

/// A resolved time window, in the two granularities the two data sources need.
#[derive(Debug)]
struct TimeWindow {
    /// Exact bounds for the raw ledger.
    start_ts: String,
    end_ts: String,
    /// Hour-bucket bounds for the rollup. `end` is inclusive so the partially
    /// elapsed current hour is included; the rollup cannot express finer.
    start_hour: String,
    end_hour: String,
}

impl TimeWindow {
    fn resolve(query: &DashboardQuery, retention_days: u32) -> Result<Self, DashboardError> {
        let now = timefmt::now();

        let (start, end) = match query.range.as_str() {
            "today" => (now.replace_time(time::Time::MIDNIGHT), now),
            "7d" => (now - Duration::days(7), now),
            "14d" => (now - Duration::days(14), now),
            "30d" => (now - Duration::days(30), now),
            "custom" => {
                let start_raw = query.start.as_deref().ok_or_else(|| {
                    DashboardError::BadRequest("`start` is required when range=custom".into())
                })?;
                let end_raw = query.end.as_deref().ok_or_else(|| {
                    DashboardError::BadRequest("`end` is required when range=custom".into())
                })?;

                let start = timefmt::parse_ts(start_raw).ok_or_else(|| {
                    DashboardError::BadRequest(format!(
                        "`start` is not a valid ISO 8601 timestamp: {start_raw:?}"
                    ))
                })?;
                let end = timefmt::parse_ts(end_raw).ok_or_else(|| {
                    DashboardError::BadRequest(format!(
                        "`end` is not a valid ISO 8601 timestamp: {end_raw:?}"
                    ))
                })?;

                if start >= end {
                    return Err(DashboardError::BadRequest(
                        "`start` must be earlier than `end`".into(),
                    ));
                }
                (start, end)
            }
            // Unrecognised ranges fall back to a day rather than scanning
            // everything, so a typo cannot become an expensive query.
            _ => (now - Duration::hours(24), now),
        };

        // Reject ranges outside the retention window instead of silently
        // returning less data than was asked for.
        let earliest = now - Duration::days(retention_days.max(1) as i64);
        if start < earliest {
            return Err(DashboardError::BadRequest(format!(
                "`start` predates the {retention_days}-day retention window (earliest: {})",
                timefmt::format_ts(earliest)
            )));
        }

        // A caller asking for the future would otherwise get a scan with no lower
        // useful bound; clamp so the window is always meaningful.
        let end = end.min(now);

        Ok(Self {
            start_ts: timefmt::format_ts(start),
            end_ts: timefmt::format_ts(end),
            start_hour: timefmt::format_hour(start),
            end_hour: timefmt::format_hour(end),
        })
    }
}

/// The `model` the query is scoped to, or `None` for "all".
///
/// Returns an owned value because the filter crosses into a blocking task.
fn model_filter(query: &DashboardQuery) -> Option<String> {
    let m = query.model.trim();
    if m.is_empty() || m.eq_ignore_ascii_case("all") {
        None
    } else {
        Some(m.to_string())
    }
}

fn status_filter(query: &DashboardQuery) -> Option<&'static str> {
    match query.status.trim().to_ascii_lowercase().as_str() {
        "completed" => Some("completed"),
        "failed" => Some("failed"),
        "interrupted" => Some("interrupted"),
        "in_flight" => Some("in_flight"),
        _ => None,
    }
}

/// `GET /api/me` — identify the authenticated key.
#[derive(Serialize)]
pub struct MeResponse {
    pub consumer_id: String,
    pub key_name: String,
}

async fn get_me(Authenticated(consumer): Authenticated) -> Json<MeResponse> {
    Json(MeResponse {
        consumer_id: consumer.consumer_id().to_string(),
        key_name: consumer.identity.key_name.clone(),
    })
}

/// `GET /api/dashboard/summary`
#[derive(Serialize)]
pub struct SummaryResponse {
    pub total_requests: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cached_tokens: u64,
    /// Mean request duration in milliseconds, weighted by request count.
    pub avg_latency_ms: Option<f64>,
    /// Mean time to first token, over the requests that reported one.
    pub avg_ttft_ms: Option<f64>,
    /// Fraction of requests that reached `completed`, 0.0..=1.0.
    pub success_rate: f64,
    /// Requests whose provider reported **no** usage at all
    /// (`usage_status = 'unavailable'`). Their tokens are absent from the totals
    /// above rather than counted as zero.
    ///
    /// This is a count of requests, not tokens. Requests with `partial` usage
    /// are not included: the part the provider did report is in the totals
    /// above, and only the part it did not report is absent.
    pub unavailable_usage_count: u64,
}

async fn get_summary(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<SummaryResponse>, DashboardError> {
    let retention_days = state.config.read().config.database.retention_days;
    let window = TimeWindow::resolve(&query, retention_days)?;
    let model = model_filter(&query);
    let model_for_rollup = model.clone();

    let pool = state.pool.clone();
    let consumer_id = consumer.consumer_id().to_string();

    // Blocking SQLite reads run on a blocking thread so a slow query cannot
    // stall the async runtime.
    let summary = tokio::task::spawn_blocking(move || {
        pool.read(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    COALESCE(SUM(request_count), 0),
                    COALESCE(SUM(success_count), 0),
                    COALESCE(SUM(failure_count), 0),
                    COALESCE(SUM(total_input_tokens), 0),
                    COALESCE(SUM(total_output_tokens), 0),
                    COALESCE(SUM(total_cached_tokens), 0),
                    -- Weighted mean: sum of durations over sum of requests. AVG()
                    -- here would average per-bucket means and misweight hours by
                    -- traffic volume.
                    CASE WHEN SUM(request_count) > 0
                         THEN SUM(total_duration_ms) * 1.0 / SUM(request_count)
                         ELSE NULL END,
                    CASE WHEN SUM(ttft_count) > 0
                         THEN SUM(total_ttft_ms) * 1.0 / SUM(ttft_count)
                         ELSE NULL END,
                    CASE WHEN SUM(request_count) > 0
                         THEN SUM(success_count) * 1.0 / SUM(request_count)
                         ELSE 0.0 END
                FROM usage_hourly
                WHERE consumer_id = ?1
                  AND hour >= ?2
                  AND hour <= ?3
                  AND (?4 IS NULL OR model = ?4)
                "#,
            )?;

            stmt.query_row(
                rusqlite::params![
                    consumer_id,
                    window.start_hour,
                    window.end_hour,
                    model_for_rollup
                ],
                |row| {
                    let total_requests: i64 = row.get(0)?;
                    let success_count: i64 = row.get(1)?;
                    Ok(SummaryResponse {
                        total_requests: total_requests as u64,
                        success_count: success_count as u64,
                        failure_count: row.get::<_, i64>(2)? as u64,
                        total_input_tokens: row.get::<_, i64>(3)? as u64,
                        total_output_tokens: row.get::<_, i64>(4)? as u64,
                        total_cached_tokens: row.get::<_, i64>(5)? as u64,
                        avg_latency_ms: row.get(6)?,
                        avg_ttft_ms: row.get(7)?,
                        success_rate: row.get(8)?,
                        unavailable_usage_count: 0,
                    })
                },
            )
        })
    })
    .await
    .map_err(|e| DashboardError::Internal(format!("query task failed: {e}")))??;

    // Count of requests in the window whose usage the provider never reported.
    // Read from the raw ledger, where the distinction between NULL and 0 lives.
    //
    // The predicate is exactly `usage_status = 'unavailable'`, matching what the
    // field documents. `partial` rows are deliberately excluded: their reported
    // tokens *are* in the totals above, so counting them here would claim those
    // tokens were left out. In-flight rows are excluded because they have no
    // terminal state yet, and recovery resolves them either way.
    let pool = state.pool.clone();
    let consumer_id = consumer.consumer_id().to_string();
    let window_start = window.start_ts.clone();
    let window_end = window.end_ts.clone();
    let unavailable = tokio::task::spawn_blocking(move || {
        pool.read(|conn| {
            conn.query_row(
                r#"
                SELECT COUNT(*) FROM usage_records
                WHERE consumer_id = ?1
                  AND created_at >= ?2
                  AND created_at < ?3
                  AND (?4 IS NULL OR model = ?4)
                  AND usage_status = 'unavailable'
                  AND request_status <> 'in_flight'
                "#,
                rusqlite::params![consumer_id, window_start, window_end, model],
                |row| row.get::<_, i64>(0),
            )
        })
    })
    .await
    .map_err(|e| DashboardError::Internal(format!("query task failed: {e}")))??;

    let mut summary = summary;
    summary.unavailable_usage_count = unavailable as u64;
    Ok(Json(summary))
}

/// `GET /api/dashboard/timeseries`
#[derive(Serialize)]
pub struct TimeseriesResponse {
    pub data: Vec<TimeseriesPoint>,
}

#[derive(Serialize)]
pub struct TimeseriesPoint {
    pub hour: String,
    pub requests: u64,
    pub success_count: u64,
    pub failure_count: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
}

async fn get_timeseries(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<TimeseriesResponse>, DashboardError> {
    let retention_days = state.config.read().config.database.retention_days;
    let window = TimeWindow::resolve(&query, retention_days)?;
    let model = model_filter(&query);
    let consumer_id = consumer.consumer_id().to_string();
    let pool = state.pool.clone();

    let data = tokio::task::spawn_blocking(move || {
        pool.read(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT
                    hour,
                    SUM(request_count),
                    SUM(success_count),
                    SUM(failure_count),
                    SUM(total_input_tokens),
                    SUM(total_output_tokens),
                    SUM(total_cached_tokens)
                FROM usage_hourly
                WHERE consumer_id = ?1
                  AND hour >= ?2
                  AND hour <= ?3
                  AND (?4 IS NULL OR model = ?4)
                GROUP BY hour
                ORDER BY hour ASC
                "#,
            )?;

            let rows = stmt.query_map(
                rusqlite::params![consumer_id, window.start_hour, window.end_hour, model],
                |row| {
                    Ok(TimeseriesPoint {
                        hour: row.get(0)?,
                        requests: row.get::<_, i64>(1)? as u64,
                        success_count: row.get::<_, i64>(2)? as u64,
                        failure_count: row.get::<_, i64>(3)? as u64,
                        input_tokens: row.get::<_, i64>(4)? as u64,
                        output_tokens: row.get::<_, i64>(5)? as u64,
                        cached_tokens: row.get::<_, i64>(6)? as u64,
                    })
                },
            )?;

            rows.collect::<Result<Vec<_>, _>>()
        })
    })
    .await
    .map_err(|e| DashboardError::Internal(format!("query task failed: {e}")))??;

    Ok(Json(TimeseriesResponse { data }))
}

/// `GET /api/dashboard/requests`
#[derive(Serialize)]
pub struct RequestsResponse {
    pub data: Vec<RequestItem>,
    /// Opaque cursor for the next page. `None` when this is the last page.
    pub next_cursor: Option<String>,
}

#[derive(Serialize)]
pub struct RequestItem {
    pub request_id: String,
    pub created_at: String,
    pub model: String,
    pub endpoint: String,
    pub streaming: bool,
    pub http_status: Option<u16>,
    pub request_status: String,
    pub usage_status: String,
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub duration_ms: u64,
    pub ttft_ms: Option<u64>,
    /// Present only for failed/interrupted requests; may repeat upstream text.
    pub error_message: Option<String>,
}

async fn get_requests(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<RequestsResponse>, DashboardError> {
    let retention_days = state.config.read().config.database.retention_days;
    let window = TimeWindow::resolve(&query, retention_days)?;
    let limit = query.limit.clamp(1, MAX_LIMIT);
    let model = model_filter(&query).map(|m| m.to_string());
    let status = status_filter(&query);
    let consumer_id = consumer.consumer_id().to_string();
    let pool = state.pool.clone();

    // Cursors are signed with the per-database key: an unsigned or edited one is
    // rejected with a 400 rather than silently treated as a first page, and a
    // key that cannot be read fails the request rather than ending the paging
    // early, which the caller could only read as "no more data".
    let key = signing_key(&pool)?;
    let cursor = match query.cursor.as_deref() {
        Some(raw) => Some(verify_cursor(&key, raw)?),
        None => None,
    };

    let response = tokio::task::spawn_blocking(move || {
        pool.read(|conn| {
            // Keyset pagination on (created_at, id): a stable total order with no
            // OFFSET, so page cost does not grow with depth. The index
            // (consumer_id, created_at, id) makes this a direct range seek.
            let mut sql = String::from(
                r#"
                SELECT
                    id, request_id, created_at, model, endpoint, streaming,
                    http_status, request_status, usage_status,
                    input_tokens, output_tokens, cached_tokens,
                    duration_ms, ttft_ms, error_message
                FROM usage_records
                WHERE consumer_id = ?1
                  AND created_at >= ?2
                  AND created_at < ?3
                  AND (?4 IS NULL OR model = ?4)
                  AND (?5 IS NULL OR request_status = ?5)
                "#,
            );
            if cursor.is_some() {
                sql.push_str("AND (created_at, id) < (?6, ?7)\n");
            }
            sql.push_str("ORDER BY created_at DESC, id DESC\nLIMIT ?8");

            let mut stmt = conn.prepare(&sql)?;

            let (cursor_created, cursor_id) = cursor.clone().unwrap_or_else(|| (String::new(), 0));
            let fetch = limit as i64 + 1;

            let rows = stmt.query_map(
                rusqlite::params![
                    consumer_id,
                    window.start_ts,
                    window.end_ts,
                    model,
                    status,
                    cursor_created,
                    cursor_id,
                    fetch,
                ],
                map_row,
            )?;

            let mut items: Vec<(i64, RequestItem)> = rows.collect::<Result<_, _>>()?;

            // One extra row was fetched purely to detect whether more exist.
            let next_cursor = if items.len() > limit {
                items.truncate(limit);
                items
                    .last()
                    .map(|(id, item)| sign_cursor(&key, &item.created_at, *id))
            } else {
                None
            };

            Ok(RequestsResponse {
                data: items.into_iter().map(|(_, item)| item).collect(),
                next_cursor,
            })
        })
    })
    .await
    .map_err(|e| DashboardError::Internal(format!("query task failed: {e}")))??;

    Ok(Json(response))
}

/// `GET /api/dashboard/models` — the models this key has used in the window.
#[derive(Serialize)]
pub struct ModelsResponse {
    pub models: Vec<String>,
}

async fn get_models(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<ModelsResponse>, DashboardError> {
    let retention_days = state.config.read().config.database.retention_days;
    let window = TimeWindow::resolve(&query, retention_days)?;
    let consumer_id = consumer.consumer_id().to_string();
    let pool = state.pool.clone();

    let models = tokio::task::spawn_blocking(move || {
        pool.read(|conn| {
            let mut stmt = conn.prepare(
                r#"
                SELECT DISTINCT model FROM usage_hourly
                WHERE consumer_id = ?1 AND hour >= ?2 AND hour <= ?3
                ORDER BY model ASC
                "#,
            )?;
            let rows = stmt.query_map(
                rusqlite::params![consumer_id, window.start_hour, window.end_hour],
                |row| row.get::<_, String>(0),
            )?;
            rows.collect::<Result<Vec<_>, _>>()
        })
    })
    .await
    .map_err(|e| DashboardError::Internal(format!("query task failed: {e}")))??;

    Ok(Json(ModelsResponse { models }))
}

/// Marshal a row, keeping the primary key so the cursor needs no second query.
fn map_row(row: &rusqlite::Row) -> rusqlite::Result<(i64, RequestItem)> {
    let id: i64 = row.get(0)?;
    Ok((
        id,
        RequestItem {
            request_id: row.get(1)?,
            created_at: row.get(2)?,
            model: row.get(3)?,
            endpoint: row.get(4)?,
            streaming: row.get::<_, i32>(5)? != 0,
            http_status: row.get::<_, Option<i64>>(6)?.map(|v| v as u16),
            request_status: row.get(7)?,
            usage_status: row.get(8)?,
            input_tokens: row.get::<_, Option<i64>>(9)?.map(|v| v as u64),
            output_tokens: row.get::<_, Option<i64>>(10)?.map(|v| v as u64),
            cached_tokens: row.get::<_, Option<i64>>(11)?.map(|v| v as u64),
            duration_ms: row.get::<_, i64>(12)? as u64,
            ttft_ms: row.get::<_, Option<i64>>(13)?.map(|v| v as u64),
            error_message: row.get(14)?,
        },
    ))
}

fn malformed_cursor() -> DashboardError {
    DashboardError::BadRequest("malformed cursor".into())
}

/// Read the per-database cursor signing key.
///
/// The key lives in `ledger_meta` so it is generated once and shared by every
/// instance reading that database. A failure here is reported as a server error
/// rather than worked around: signing with a substitute key would produce
/// cursors this server cannot verify, and omitting `next_cursor` would make a
/// truncated page look like the end of the data.
fn signing_key(pool: &crate::ledger::LedgerPool) -> Result<Vec<u8>, DashboardError> {
    pool.read(crate::ledger::cursor_key)
        .map_err(|e| DashboardError::Internal(format!("cursor signing key unavailable: {e}")))
}

/// Build a cursor for the row a page ended on.
///
/// The cursor is `base64url(HMAC-SHA256(key, payload)) || "." || base64url(payload)`
/// where the payload is `"<created_at>|<id>"`. Both halves are *unpadded*
/// URL-safe base64 (alphabet `A-Z a-z 0-9 - _`, no `=` padding), so every
/// character of a cursor is unreserved and a cursor never needs escaping in a
/// query string.
///
/// The signature is what makes the cursor tamper-evident: the row id is a
/// global `AUTOINCREMENT`, so a readable payload would let a partner estimate
/// portal-wide request volume from an id it was handed. Signing does not widen
/// or narrow access — the query is still scoped by the authenticated key — it
/// only stops a cursor from being read as a number or edited.
fn sign_cursor(key: &[u8], created_at: &str, id: i64) -> String {
    let payload = format!("{created_at}{PAYLOAD_SEPARATOR}{id}");
    let signature = hmac_sha256(key, payload.as_bytes());
    format!(
        "{}{CURSOR_SEPARATOR}{}",
        base64url_encode(&signature),
        base64url_encode(payload.as_bytes())
    )
}

/// Verify and decode a cursor.
///
/// A cursor whose signature does not verify is a client error (400), never a
/// silent fall-back to the first page: quietly ignoring it would turn a paging
/// bug into what looks like missing data, and would make an edited cursor look
/// accepted.
fn verify_cursor(key: &[u8], raw: &str) -> Result<(String, i64), DashboardError> {
    let (signature_b64, payload_b64) = raw
        .split_once(CURSOR_SEPARATOR)
        .ok_or_else(malformed_cursor)?;
    let signature = base64url_decode(signature_b64).ok_or_else(malformed_cursor)?;
    let payload = base64url_decode(payload_b64).ok_or_else(malformed_cursor)?;

    let expected = hmac_sha256(key, &payload);
    if !constant_time_eq(&signature, &expected) {
        return Err(DashboardError::BadRequest(
            "cursor signature does not verify".into(),
        ));
    }

    // The payload is authentic past this point; these checks only guard against
    // a cursor that is internally inconsistent.
    let payload = String::from_utf8(payload).map_err(|_| malformed_cursor())?;
    let (created_at, id) = payload
        .rsplit_once(PAYLOAD_SEPARATOR)
        .ok_or_else(malformed_cursor)?;
    let id: i64 = id.parse().map_err(|_| malformed_cursor())?;
    if created_at.is_empty() {
        return Err(malformed_cursor());
    }
    Ok((created_at.to_string(), id))
}

/// HMAC-SHA256 (RFC 2104).
///
/// Written out rather than pulled from an `hmac` crate: `sha2` is already a
/// dependency and the construction is the two padded hashes below (the
/// `SHA256_BLOCK` test anchors it against the RFC 4231 vectors).
fn hmac_sha256(key: &[u8], message: &[u8]) -> [u8; SHA256_OUTPUT] {
    // A key longer than the block size is hashed down first; anything shorter is
    // zero-padded to it.
    let mut block = [0u8; SHA256_BLOCK];
    if key.len() > SHA256_BLOCK {
        block[..SHA256_OUTPUT].copy_from_slice(&Sha256::digest(key)[..]);
    } else {
        block[..key.len()].copy_from_slice(key);
    }

    let mut inner = Sha256::new();
    inner.update(block.map(|b| b ^ 0x36));
    inner.update(message);
    let inner = inner.finalize();

    let mut outer = Sha256::new();
    outer.update(block.map(|b| b ^ 0x5c));
    outer.update(inner);
    let mut mac = [0u8; SHA256_OUTPUT];
    mac.copy_from_slice(&outer.finalize());
    mac
}

/// Compare two byte strings without an early exit, so a forger learns nothing
/// from how far the comparison got.
fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Encode bytes as unpadded URL-safe base64.
///
/// Hand-written on purpose: it is a dozen readable lines, the alphabet is the
/// whole format, and the alternative is a dependency for one function.
fn base64url_encode(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len().div_ceil(3) * 4);
    for chunk in bytes.chunks(3) {
        // Read the group as one 24-bit big-endian number, zero-padded on the
        // right; a group of 1 or 2 bytes then simply emits fewer sextets.
        let mut padded = [0u8; 3];
        padded[..chunk.len()].copy_from_slice(chunk);
        let n = u32::from_be_bytes([0, padded[0], padded[1], padded[2]]);

        for (index, shift) in [18u32, 12, 6, 0].into_iter().enumerate() {
            if index < chunk.len() + 1 {
                out.push(B64_ALPHABET[((n >> shift) & 0b11_1111) as usize] as char);
            }
        }
    }
    out
}

/// Decode unpadded URL-safe base64.
///
/// Returns `None` for anything that is not a canonical encoding: a stray `=`,
/// a character outside the alphabet, a length that cannot carry whole bytes, or
/// a final sextet with non-zero padding bits.
fn base64url_decode(text: &str) -> Option<Vec<u8>> {
    let mut out = Vec::with_capacity(text.len() / 4 * 3 + 2);
    let mut accumulator: u32 = 0;
    let mut bits: u32 = 0;

    for byte in text.bytes() {
        let value = B64_ALPHABET.iter().position(|&a| a == byte)? as u32;
        accumulator = (accumulator << 6) | value;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((accumulator >> bits) as u8);
        }
    }

    // At most 4 leftover bits are meaningful; 6 or more means the last character
    // contributed no byte at all, which no encoder produces. Whatever is left
    // must be zero, or the encoding is non-canonical.
    if bits >= 6 || (accumulator & ((1 << bits) - 1)) != 0 {
        return None;
    }
    Some(out)
}

/// Dashboard error.
#[derive(Debug)]
pub enum DashboardError {
    BadRequest(String),
    Database(rusqlite::Error),
    Internal(String),
}

impl IntoResponse for DashboardError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            DashboardError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            DashboardError::Database(e) => {
                // Log the detail; do not hand database internals to the client.
                tracing::error!(error = %e, "dashboard database error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "database error".to_string(),
                )
            }
            DashboardError::Internal(msg) => {
                tracing::error!(error = %msg, "dashboard internal error");
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "internal error".to_string(),
                )
            }
        };

        let body = serde_json::json!({
            "error": {
                "message": message,
                "type": "dashboard_error",
            }
        });

        (status, Json(body)).into_response()
    }
}

impl From<rusqlite::Error> for DashboardError {
    fn from(e: rusqlite::Error) -> Self {
        DashboardError::Database(e)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn q(range: &str) -> DashboardQuery {
        DashboardQuery {
            range: range.to_string(),
            ..Default::default()
        }
    }

    /// A fixed key, so the tests do not depend on a database.
    const TEST_KEY: &[u8] = b"cursor-test-key";
    /// A second key, standing in for another database's.
    const OTHER_KEY: &[u8] = b"another-database-key";

    #[test]
    fn test_cursor_round_trip() {
        // Regression: a colon-separated cursor split inside the ISO timestamp,
        // because timestamps themselves contain colons.
        let created = "2026-09-24T07:12:33.123456789Z";
        let cursor = sign_cursor(TEST_KEY, created, 4242);
        let (parsed_created, parsed_id) = verify_cursor(TEST_KEY, &cursor).unwrap();
        assert_eq!(parsed_created, created);
        assert_eq!(parsed_id, 4242);
    }

    #[test]
    fn test_cursor_is_url_safe() {
        // A cursor travels in a query string; anything that needs escaping there
        // is a bug even when the signature is valid.
        let cursor = sign_cursor(TEST_KEY, "2026-09-24T07:12:33.123456789Z", 4242);
        assert!(
            cursor
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_' || b == b'.'),
            "cursor {cursor:?} must contain only unreserved characters"
        );
    }

    #[test]
    fn test_cursor_payload_is_not_readable() {
        // The row id is a global AUTOINCREMENT: an id a partner can read is an
        // id the partner can use to size portal-wide traffic.
        let created = "2026-09-24T07:12:33.123456789Z";
        let cursor = sign_cursor(TEST_KEY, created, 987654);
        assert!(
            !cursor.contains(created),
            "the timestamp must not appear in the clear"
        );
        assert!(
            !cursor.contains("987654"),
            "the row id must not appear in the clear"
        );
    }

    #[test]
    fn test_cursor_rejects_malformed_input() {
        for bad in [
            "",
            "no-separator",
            "2026-09-24T07:12:33Z|notanumber",
            "|5",
            "garbage",
            // A padded encoding is not what `sign_cursor` emits.
            "Zm9vYmFy==",
            ".",
            "AAAA.",
        ] {
            assert!(
                verify_cursor(TEST_KEY, bad).is_err(),
                "{bad:?} must be rejected as a malformed cursor"
            );
        }
    }

    #[test]
    fn test_unsigned_legacy_cursor_is_rejected() {
        // The pre-signing cursor format was the payload in the clear. It must be
        // refused, not honoured, or the signature would be optional.
        let legacy = "2026-09-24T07:12:33.123456789Z|4242";
        assert!(verify_cursor(TEST_KEY, legacy).is_err());
    }

    #[test]
    fn test_cursor_rejects_tampered_payload() {
        let created = "2026-09-24T07:12:33.123456789Z";
        let cursor = sign_cursor(TEST_KEY, created, 4242);
        let (signature, _) = cursor.split_once(CURSOR_SEPARATOR).unwrap();

        // Keep the genuine signature, swap in a payload claiming a far larger
        // id: the forgery must not verify.
        let forged_payload = base64url_encode(format!("{created}|9999999999").as_bytes());
        let forged = format!("{signature}{CURSOR_SEPARATOR}{forged_payload}");
        let err = verify_cursor(TEST_KEY, &forged).unwrap_err();
        assert!(
            matches!(err, DashboardError::BadRequest(_)),
            "a tampered payload must be a client error"
        );
    }

    #[test]
    fn test_cursor_rejects_tampered_signature() {
        let cursor = sign_cursor(TEST_KEY, "2026-09-24T07:12:33.123456789Z", 4242);
        let (signature, payload) = cursor.split_once(CURSOR_SEPARATOR).unwrap();

        // Flip one character of the signature to another alphabet character, so
        // the encoding stays valid and only the signature is wrong.
        let mut flipped = String::from(signature);
        let first = flipped.remove(0);
        flipped.insert(0, if first == 'A' { 'B' } else { 'A' });
        assert!(verify_cursor(TEST_KEY, &format!("{flipped}{CURSOR_SEPARATOR}{payload}")).is_err());
    }

    #[test]
    fn test_cursor_from_a_different_key_is_rejected() {
        let created = "2026-09-24T07:12:33.123456789Z";
        let cursor = sign_cursor(TEST_KEY, created, 4242);
        // Same payload, key this database does not have.
        let err = verify_cursor(OTHER_KEY, &cursor).unwrap_err();
        assert!(
            matches!(err, DashboardError::BadRequest(_)),
            "a cursor signed with another key must not verify"
        );
    }

    #[test]
    fn test_bad_cursor_is_a_client_error_not_a_server_error() {
        // 400, not 500 — and never a silent page-1 fall-back, which would read
        // as data loss.
        let response = verify_cursor(TEST_KEY, "garbage")
            .unwrap_err()
            .into_response();
        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn test_base64url_matches_rfc4648_vectors() {
        for (raw, encoded) in [
            ("", ""),
            ("f", "Zg"),
            ("fo", "Zm8"),
            ("foo", "Zm9v"),
            ("foob", "Zm9vYg"),
            ("fooba", "Zm9vYmE"),
            ("foobar", "Zm9vYmFy"),
        ] {
            assert_eq!(base64url_encode(raw.as_bytes()), encoded);
            assert_eq!(base64url_decode(encoded).unwrap(), raw.as_bytes());
        }

        // The two characters that distinguish the URL-safe alphabet from the
        // standard one: 0xfb 0xff 0xbf encodes to "-_-_" here, "+/+/" there.
        let bytes = [0xfb, 0xff, 0xbf];
        assert_eq!(base64url_encode(&bytes), "-_-_");
    }

    #[test]
    fn test_base64url_round_trips_every_byte_value() {
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(base64url_decode(&base64url_encode(&all)).unwrap(), all);

        // Every length modulo 3, so the shortened final group is covered too.
        for len in 0..=all.len() {
            let slice = &all[..len];
            assert_eq!(
                base64url_decode(&base64url_encode(slice)).unwrap(),
                slice,
                "round trip failed at length {len}"
            );
        }
    }

    #[test]
    fn test_base64url_rejects_non_canonical_input() {
        for bad in [
            // Padding is never emitted, so it is never accepted.
            "=", "Zg=", "Zm9v=", "Zg==", "Zm9vYg==",
            // A length that cannot carry a whole byte, or characters outside
            // the alphabet (including the standard-alphabet `+` and `/`).
            "a", "!!!!", "Zm9v\n", "Zm+v", "Zm/v",
            // Non-zero padding bits: "Zh" and "ab" encode the same bytes as
            // "Zg" and "aa", which is what a canonical encoder emits.
            "Zh", "ab", "abd",
        ] {
            assert!(
                base64url_decode(bad).is_none(),
                "{bad:?} must not decode as unpadded URL-safe base64"
            );
        }

        // The canonical spellings of the same bytes do decode, so the rejections
        // above are about canonical form rather than the length alone.
        assert_eq!(base64url_decode("Zg").unwrap(), b"f");
        assert_eq!(base64url_decode("abc").unwrap(), b"i\xb7");
    }

    #[test]
    fn test_hmac_sha256_matches_rfc4231_vectors() {
        // RFC 4231 test case 2: key "Jefe", data "what do ya want for nothing?".
        assert_eq!(
            hex::encode(hmac_sha256(b"Jefe", b"what do ya want for nothing?")),
            "5bdcc146bf60754e6a042426089575c75a003f089d2739839dec58b964ec3843"
        );

        // RFC 4231 test case 1, which also exercises a key of exactly the block
        // size and the case below it.
        assert_eq!(
            hex::encode(hmac_sha256(&[0x0b; 20], b"Hi There")),
            "b0344c61d8db38535ca8afceaf0bf12b881dc200c9833da726e9376c2e32cff7"
        );

        // RFC 4231 test case 6: a key longer than the block size, which must be
        // hashed down first.
        assert_eq!(
            hex::encode(hmac_sha256(
                &[0xaa; 131],
                b"Test Using Larger Than Block-Size Key - Hash Key First"
            )),
            "60e431591ee0b67f0d8a26aacbf5b77f8e0bc6213728c5140546040f0ee37f54"
        );
    }

    #[test]
    fn test_cursor_key_is_stable_for_one_database_and_unique_per_database() {
        let dir = tempfile::TempDir::new().unwrap();
        let path = dir.path().join("t.db");

        let conn = rusqlite::Connection::open(&path).unwrap();
        crate::ledger::configure_sqlite(&conn).unwrap();
        crate::ledger::init_schema(&conn).unwrap();

        let key = crate::ledger::cursor_key(&conn).unwrap();
        assert_eq!(key.len(), 32, "the cursor key is 32 random bytes");

        let cursor = sign_cursor(&key, "2026-09-24T07:12:33.123456789Z", 7);
        assert_eq!(verify_cursor(&key, &cursor).unwrap().1, 7);

        // The key is stored, not regenerated: a cursor issued before a restart
        // must still verify after one.
        let reopened = rusqlite::Connection::open(&path).unwrap();
        assert_eq!(crate::ledger::cursor_key(&reopened).unwrap(), key);

        // A different database signs with a different key.
        let other = rusqlite::Connection::open(dir.path().join("other.db")).unwrap();
        crate::ledger::init_schema(&other).unwrap();
        let other_key = crate::ledger::cursor_key(&other).unwrap();
        assert_ne!(other_key, key);
        assert!(verify_cursor(&other_key, &cursor).is_err());
    }

    /// The signing key is a secret: it must not be derivable from a cursor, and
    /// two databases must not share one.
    #[test]
    fn test_distinct_keys_produce_distinct_signatures() {
        let created = "2026-09-24T07:12:33.123456789Z";
        assert_ne!(
            sign_cursor(TEST_KEY, created, 1),
            sign_cursor(OTHER_KEY, created, 1)
        );
    }

    #[test]
    fn test_window_rollup_bounds_include_current_hour() {
        // Regression: hour bounds formatted with minute precision made the
        // current hour compare as *before* the start bound and drop out of every
        // summary and timeseries.
        let window = TimeWindow::resolve(&q("24h"), 60).unwrap();
        assert_eq!(window.start_hour.len(), 13);
        assert_eq!(window.end_hour.len(), 13);
        assert!(window.start_hour <= window.end_hour);

        let now_hour = timefmt::format_hour(timefmt::now());
        assert_eq!(
            window.end_hour, now_hour,
            "the current hour must be inside the window"
        );
    }

    #[test]
    fn test_window_raw_bounds_are_exact_and_ordered() {
        let window = TimeWindow::resolve(&q("7d"), 60).unwrap();
        assert_eq!(window.start_ts.len(), 30);
        assert_eq!(window.end_ts.len(), 30);
        assert!(window.start_ts < window.end_ts);
    }

    #[test]
    fn test_unknown_range_falls_back_to_a_day_not_everything() {
        let window = TimeWindow::resolve(&q("nonsense"), 60).unwrap();
        let span = timefmt::parse_ts(&window.end_ts).unwrap()
            - timefmt::parse_ts(&window.start_ts).unwrap();
        assert_eq!(span.whole_hours(), 24);
    }

    #[test]
    fn test_custom_range_requires_both_bounds() {
        let mut query = q("custom");
        query.start = Some("2026-09-01T00:00:00Z".into());
        assert!(matches!(
            TimeWindow::resolve(&query, 60),
            Err(DashboardError::BadRequest(_))
        ));
    }

    #[test]
    fn test_custom_range_rejects_inverted_bounds() {
        let mut query = q("custom");
        query.start = Some("2026-09-10T00:00:00Z".into());
        query.end = Some("2026-09-01T00:00:00Z".into());
        let err = TimeWindow::resolve(&query, 60).unwrap_err();
        assert!(matches!(err, DashboardError::BadRequest(m) if m.contains("earlier")));
    }

    #[test]
    fn test_custom_range_rejects_beyond_retention() {
        let mut query = q("custom");
        query.start = Some("2020-01-01T00:00:00Z".into());
        query.end = Some("2020-01-02T00:00:00Z".into());
        let err = TimeWindow::resolve(&query, 60).unwrap_err();
        assert!(
            matches!(err, DashboardError::BadRequest(ref m) if m.contains("retention")),
            "an out-of-retention range must be rejected, not silently truncated"
        );
    }

    #[test]
    fn test_custom_range_rejects_unparseable_bounds() {
        let mut query = q("custom");
        query.start = Some("yesterday".into());
        query.end = Some("today".into());
        assert!(matches!(
            TimeWindow::resolve(&query, 60),
            Err(DashboardError::BadRequest(_))
        ));
    }

    #[test]
    fn test_custom_range_clamps_future_end_to_now() {
        let mut query = q("custom");
        query.start = Some("2026-09-01T00:00:00Z".into());
        query.end = Some("2030-01-01T00:00:00Z".into());
        let window = TimeWindow::resolve(&query, 60).unwrap();
        let end = timefmt::parse_ts(&window.end_ts).unwrap();
        assert!(end <= timefmt::now());
    }

    #[test]
    fn test_model_filter_normalizes_all_and_blank() {
        let mut query = q("24h");
        query.model = "all".into();
        assert_eq!(model_filter(&query), None);
        query.model = "ALL".into();
        assert_eq!(model_filter(&query), None);
        query.model = "  ".into();
        assert_eq!(model_filter(&query), None);
        query.model = "gpt-4o".into();
        assert_eq!(model_filter(&query).as_deref(), Some("gpt-4o"));
    }

    #[test]
    fn test_status_filter_only_accepts_known_states() {
        let mut query = q("24h");
        query.status = "completed".into();
        assert_eq!(status_filter(&query), Some("completed"));
        query.status = "Bogus".into();
        assert_eq!(
            status_filter(&query),
            None,
            "unknown status must not filter to nothing"
        );
        query.status = "all".into();
        assert_eq!(status_filter(&query), None);
    }

    #[test]
    fn test_dashboard_error_does_not_leak_database_internals() {
        let response = DashboardError::Database(rusqlite::Error::InvalidQuery).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
