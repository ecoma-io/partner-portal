//! Dashboard REST API endpoints

use axum::{
    Json, Router,
    extract::{Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::get,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use time::{Duration, OffsetDateTime};

use crate::auth::Authenticated;
use crate::ledger::LedgerPool;
use crate::proxy::handler::AppState;

/// Dashboard router
pub fn create_dashboard_router() -> Router<Arc<AppState>> {
    Router::new()
        .route("/api/me", get(get_me))
        .route("/api/dashboard/summary", get(get_summary))
        .route("/api/dashboard/timeseries", get(get_timeseries))
        .route("/api/dashboard/requests", get(get_requests))
}

/// Resolve the ledger pool from app state.
fn pool_from_state(state: &Arc<AppState>) -> Arc<LedgerPool> {
    state.pool.clone()
}

/// Query parameters for dashboard endpoints
#[derive(Debug, Deserialize)]
#[serde(default)]
pub struct DashboardQuery {
    /// Time range: today, 24h, 7d, 14d, 30d, or custom
    pub range: String,

    /// Start date for custom range (ISO 8601)
    pub start: Option<String>,

    /// End date for custom range (ISO 8601)
    pub end: Option<String>,

    /// Model filter (all or specific model)
    pub model: String,

    /// Page cursor for keyset pagination
    pub cursor: Option<String>,

    /// Page size
    pub limit: usize,
}

impl Default for DashboardQuery {
    fn default() -> Self {
        Self {
            range: "24h".to_string(),
            start: None,
            end: None,
            model: "all".to_string(),
            cursor: None,
            limit: 50,
        }
    }
}

/// Response for /api/me
#[derive(Serialize)]
pub struct MeResponse {
    pub consumer_id: String,
    pub key_name: String,
}

/// GET /api/me - Get authenticated consumer info
async fn get_me(Authenticated(consumer): Authenticated) -> Json<MeResponse> {
    Json(MeResponse {
        consumer_id: consumer.consumer_id().to_string(),
        key_name: consumer.identity.key_name.clone(),
    })
}

/// Response for /api/dashboard/summary
#[derive(Serialize)]
pub struct SummaryResponse {
    pub total_requests: u64,
    pub total_input_tokens: u64,
    pub total_output_tokens: u64,
    pub total_cached_tokens: u64,
    pub avg_latency_ms: f64,
    pub avg_ttft_ms: Option<f64>,
    pub success_rate: f64,
}

/// GET /api/dashboard/summary
async fn get_summary(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<SummaryResponse>, DashboardError> {
    let (start, end) = parse_time_range(&query)?;
    let model_filter = if query.model == "all" {
        None
    } else {
        Some(query.model.as_str())
    };

    let pool = pool_from_state(&state);
    let summary = pool.read(|conn| {
        let mut stmt = conn.prepare(
            r#"
            SELECT
                COALESCE(SUM(request_count), 0) as total_requests,
                COALESCE(SUM(total_input_tokens), 0) as total_input_tokens,
                COALESCE(SUM(total_output_tokens), 0) as total_output_tokens,
                COALESCE(SUM(total_cached_tokens), 0) as total_cached_tokens,
                COALESCE(AVG(total_duration_ms), 0) as avg_latency_ms,
                CASE WHEN SUM(ttft_count) > 0 THEN SUM(total_ttft_ms) * 1.0 / SUM(ttft_count) ELSE NULL END as avg_ttft_ms,
                CASE WHEN SUM(request_count) > 0 THEN SUM(success_count) * 1.0 / SUM(request_count) ELSE 0 END as success_rate
            FROM usage_hourly
            WHERE consumer_id = ?1
              AND hour >= ?2
              AND hour < ?3
              AND (?4 IS NULL OR model = ?4)
            "#,
        )?;

        let row = stmt.query_row(
            rusqlite::params![consumer.consumer_id(), start, end, model_filter],
            |row| {
                Ok(SummaryResponse {
                    total_requests: row.get::<_, i64>(0)? as u64,
                    total_input_tokens: row.get::<_, i64>(1)? as u64,
                    total_output_tokens: row.get::<_, i64>(2)? as u64,
                    total_cached_tokens: row.get::<_, i64>(3)? as u64,
                    avg_latency_ms: row.get(4)?,
                    avg_ttft_ms: row.get(5)?,
                    success_rate: row.get(6)?,
                })
            },
        )?;
        Ok(row)
    })?;

    Ok(Json(summary))
}

/// Response for /api/dashboard/timeseries
#[derive(Serialize)]
pub struct TimeseriesResponse {
    pub data: Vec<TimeseriesPoint>,
}

#[derive(Serialize)]
pub struct TimeseriesPoint {
    pub hour: String,
    pub requests: u64,
    pub input_tokens: u64,
    pub output_tokens: u64,
    pub cached_tokens: u64,
}

/// GET /api/dashboard/timeseries
async fn get_timeseries(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<TimeseriesResponse>, DashboardError> {
    let (start, end) = parse_time_range(&query)?;
    let model_filter = if query.model == "all" {
        None
    } else {
        Some(query.model.as_str())
    };

    let pool = pool_from_state(&state);
    let data = pool.read(|conn| {
        let mut stmt = conn.prepare(
            r#"
            SELECT
                hour,
                SUM(request_count) as requests,
                SUM(total_input_tokens) as input_tokens,
                SUM(total_output_tokens) as output_tokens,
                SUM(total_cached_tokens) as cached_tokens
            FROM usage_hourly
            WHERE consumer_id = ?1
              AND hour >= ?2
              AND hour < ?3
              AND (?4 IS NULL OR model = ?4)
            GROUP BY hour
            ORDER BY hour ASC
            "#,
        )?;

        let rows = stmt.query_map(
            rusqlite::params![consumer.consumer_id(), start, end, model_filter],
            |row| {
                Ok(TimeseriesPoint {
                    hour: row.get(0)?,
                    requests: row.get::<_, i64>(1)? as u64,
                    input_tokens: row.get::<_, i64>(2)? as u64,
                    output_tokens: row.get::<_, i64>(3)? as u64,
                    cached_tokens: row.get::<_, i64>(4)? as u64,
                })
            },
        )?;

        rows.collect::<Result<Vec<_>, _>>()
    })?;

    Ok(Json(TimeseriesResponse { data }))
}

/// Response for /api/dashboard/requests
#[derive(Serialize)]
pub struct RequestsResponse {
    pub data: Vec<RequestItem>,
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
    pub input_tokens: Option<u64>,
    pub output_tokens: Option<u64>,
    pub cached_tokens: Option<u64>,
    pub duration_ms: u64,
    pub ttft_ms: Option<u64>,
}

/// GET /api/dashboard/requests
async fn get_requests(
    State(state): State<Arc<AppState>>,
    Authenticated(consumer): Authenticated,
    Query(query): Query<DashboardQuery>,
) -> Result<Json<RequestsResponse>, DashboardError> {
    let (start, end) = parse_time_range(&query)?;
    let limit = query.limit.clamp(1, 100);
    let model_filter = if query.model == "all" {
        None
    } else {
        Some(query.model.as_str())
    };

    let pool = pool_from_state(&state);
    let result = pool.read(|conn| {
        // Keyset pagination: (created_at, id) < (?5, ?6) for the "next page"
        let cursor_filter = if query.cursor.is_some() {
            "AND (created_at, id) < (?5, ?6)"
        } else {
            ""
        };

        let sql = format!(
            r#"
            SELECT
                request_id, created_at, model, endpoint, streaming,
                http_status, request_status, input_tokens, output_tokens,
                cached_tokens, duration_ms, ttft_ms
            FROM usage_records
            WHERE consumer_id = ?1
              AND created_at >= ?2
              AND created_at < ?3
              AND (?4 IS NULL OR model = ?4)
            {}
            ORDER BY created_at DESC, id DESC
            LIMIT ?
            "#,
            cursor_filter
        );

        let mut stmt = conn.prepare(&sql)?;

        let cursor_parts: Option<(String, i64)> = query.cursor.as_ref().map(|c| {
            let mut it = c.splitn(2, ':');
            let created = it.next().unwrap_or("").to_string();
            let id = it.next().and_then(|s| s.parse().ok()).unwrap_or(0);
            (created, id)
        });

        let rows = match cursor_parts {
            Some((created_at, id)) => stmt.query_map(
                rusqlite::params![
                    consumer.consumer_id(),
                    start,
                    end,
                    model_filter,
                    created_at,
                    id,
                    limit as i64 + 1,
                ],
                map_row,
            )?,
            None => stmt.query_map(
                rusqlite::params![
                    consumer.consumer_id(),
                    start,
                    end,
                    model_filter,
                    limit as i64 + 1,
                ],
                map_row,
            )?,
        };

        let mut items: Vec<RequestItem> = rows.collect::<Result<_, _>>()?;

        // Proper keyset cursor: use the last returned row's (created_at, id).
        let next_cursor = if items.len() > limit {
            items.truncate(limit);
            let last = items.last().unwrap();
            let id: i64 = conn.query_row(
                "SELECT id FROM usage_records WHERE request_id = ?1 AND consumer_id = ?2",
                rusqlite::params![last.request_id, consumer.consumer_id()],
                |r| r.get(0),
            )?;
            Some(format!("{}:{}", last.created_at, id))
        } else {
            items.truncate(limit);
            None
        };

        Ok(RequestsResponse {
            data: items,
            next_cursor,
        })
    })?;

    Ok(Json(result))
}

/// Map a row to a RequestItem
fn map_row(row: &rusqlite::Row) -> rusqlite::Result<RequestItem> {
    Ok(RequestItem {
        request_id: row.get(0)?,
        created_at: row.get(1)?,
        model: row.get(2)?,
        endpoint: row.get(3)?,
        streaming: row.get::<_, i32>(4)? != 0,
        http_status: row.get::<_, Option<i64>>(5)?.map(|v| v as u16),
        request_status: row.get(6)?,
        input_tokens: row.get::<_, Option<i64>>(7)?.map(|v| v as u64),
        output_tokens: row.get::<_, Option<i64>>(8)?.map(|v| v as u64),
        cached_tokens: row.get::<_, Option<i64>>(9)?.map(|v| v as u64),
        duration_ms: row.get::<_, i64>(10)? as u64,
        ttft_ms: row.get::<_, Option<i64>>(11)?.map(|v| v as u64),
    })
}

/// Parse time range query parameter
fn parse_time_range(query: &DashboardQuery) -> Result<(String, String), DashboardError> {
    let now = OffsetDateTime::now_utc();
    let fmt = time::format_description::parse("[year]-[month]-[day]T[hour]:[minute]")
        .expect("valid format");

    let (start, end) = match query.range.as_str() {
        "today" => {
            let today = now.replace_time(time::Time::MIDNIGHT);
            (today, now)
        }
        "24h" => (now - Duration::hours(24), now),
        "7d" => (now - Duration::days(7), now),
        "14d" => (now - Duration::days(14), now),
        "30d" => (now - Duration::days(30), now),
        "custom" => {
            let start = query.start.as_ref().ok_or_else(|| {
                DashboardError::BadRequest("start parameter required for custom range".into())
            })?;
            let end = query.end.as_ref().ok_or_else(|| {
                DashboardError::BadRequest("end parameter required for custom range".into())
            })?;
            return Ok((start.clone(), end.clone()));
        }
        _ => (now - Duration::hours(24), now),
    };

    let start_str = start.format(&fmt).map_err(DashboardError::from)?;
    let end_str = end.format(&fmt).map_err(DashboardError::from)?;

    Ok((start_str, end_str))
}

/// Dashboard error
#[derive(Debug)]
pub enum DashboardError {
    BadRequest(String),
    Database(rusqlite::Error),
    Time(time::error::Format),
}

impl IntoResponse for DashboardError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            DashboardError::BadRequest(msg) => (StatusCode::BAD_REQUEST, msg),
            DashboardError::Database(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
            DashboardError::Time(e) => (StatusCode::INTERNAL_SERVER_ERROR, e.to_string()),
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

impl From<time::error::Format> for DashboardError {
    fn from(e: time::error::Format) -> Self {
        DashboardError::Time(e)
    }
}
