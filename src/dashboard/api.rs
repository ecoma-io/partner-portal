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
    /// Requests whose provider reported no usage; their tokens are excluded from
    /// the totals above rather than counted as zero.
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
                  AND usage_status <> 'available'
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

    // Parse the cursor before touching the database so a malformed one is a
    // clean 400 rather than an empty page.
    let cursor = match query.cursor.as_deref() {
        Some(raw) => Some(parse_cursor(raw)?),
        None => None,
    };

    let pool = state.pool.clone();

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
                    .map(|(id, item)| format_cursor(&item.created_at, *id))
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

/// Separator between timestamp and row id in a cursor.
///
/// Chosen because it cannot occur in either part: ISO 8601 timestamps contain
/// `:` and `-`, so a colon-separated cursor would split inside the timestamp.
const CURSOR_SEPARATOR: char = '|';

fn format_cursor(created_at: &str, id: i64) -> String {
    format!("{created_at}{CURSOR_SEPARATOR}{id}")
}

fn parse_cursor(raw: &str) -> Result<(String, i64), DashboardError> {
    let (created_at, id) = raw
        .rsplit_once(CURSOR_SEPARATOR)
        .ok_or_else(|| DashboardError::BadRequest("malformed cursor".into()))?;
    let id: i64 = id
        .parse()
        .map_err(|_| DashboardError::BadRequest("malformed cursor".into()))?;
    if created_at.is_empty() {
        return Err(DashboardError::BadRequest("malformed cursor".into()));
    }
    Ok((created_at.to_string(), id))
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

    #[test]
    fn test_cursor_round_trip() {
        // Regression: a colon-separated cursor split inside the ISO timestamp,
        // because timestamps themselves contain colons.
        let created = "2026-09-24T07:12:33.123456789Z";
        let cursor = format_cursor(created, 4242);
        let (parsed_created, parsed_id) = parse_cursor(&cursor).unwrap();
        assert_eq!(parsed_created, created);
        assert_eq!(parsed_id, 4242);
    }

    #[test]
    fn test_cursor_rejects_malformed_input() {
        for bad in ["", "no-separator", "2026-09-24T07:12:33Z|notanumber", "|5"] {
            assert!(
                parse_cursor(bad).is_err(),
                "{bad:?} must be rejected as a malformed cursor"
            );
        }
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
