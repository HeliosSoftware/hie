use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rusqlite::params;
use serde::Deserialize;
use serde_json::{Value, json};

use crate::config::FeedConfig;
use crate::db::{Db, now_ms};
use crate::pipeline::human;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Db>,
    pub feeds: Arc<Vec<FeedConfig>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/api/feeds", get(list_feeds))
        .route("/api/feeds/{id}/files", get(list_files))
        .route("/api/records", get(list_records))
        .route("/api/records/{id}", get(get_record))
        .route("/api/records/{id}/reprocess", post(reprocess))
        .route("/api/alerts", get(list_alerts))
        .route("/metrics", get(metrics))
        .with_state(state)
}

const ISO: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', {}/1000.0, 'unixepoch')";

fn iso(col: &str) -> String {
    ISO.replace("{}", col)
}

struct ApiError(StatusCode, String);

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (self.0, Json(json!({"error": self.1}))).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError(StatusCode::INTERNAL_SERVER_ERROR, format!("{e:#}"))
    }
}

type ApiResult = Result<Json<Value>, ApiError>;

fn feed_status(db: &Db, feed: &FeedConfig) -> anyhow::Result<Value> {
    let last = db.last_received_at(&feed.id)?;
    let freshness = match (feed.expect_every, last) {
        (None, _) => "not_monitored",
        (Some(_), None) => "no_data_yet",
        (Some(w), Some(t)) if now_ms() - t > w.as_millis() as i64 => "silent",
        _ => "ok",
    };
    let last_alert = db.query_json(
        &format!("SELECT kind, message, {} AS created_at FROM alerts WHERE feed_id = ?1 ORDER BY id DESC LIMIT 1", iso("created_at")),
        params![feed.id],
    )?;
    Ok(json!({
        "id": feed.id,
        "name": feed.name,
        "source": feed.source_dir().display().to_string(),
        "expect_every": feed.expect_every.map(human),
        "freshness": freshness,
        "last_received_at": last.and_then(chrono::DateTime::from_timestamp_millis).map(|t| t.to_rfc3339()),
        "records": db.feed_counts(&feed.id)?,
        "last_alert": last_alert.into_iter().next(),
    }))
}

async fn list_feeds(State(s): State<AppState>) -> ApiResult {
    let feeds: anyhow::Result<Vec<Value>> = s.feeds.iter().map(|f| feed_status(&s.db, f)).collect();
    Ok(Json(json!({"feeds": feeds?})))
}

#[derive(Deserialize)]
struct Limit {
    limit: Option<i64>,
}

async fn list_files(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<Limit>,
) -> ApiResult {
    let rows = s.db.query_json(
        &format!(
            "SELECT f.id, f.name, f.status, f.duplicate_of, f.size, f.error,
                    {} AS received_at, {} AS finished_at,
                    COUNT(r.id) AS records,
                    COALESCE(SUM(r.status = 'sent'), 0) AS sent,
                    COALESCE(SUM(r.status = 'rejected'), 0) AS rejected,
                    COALESCE(SUM(r.status IN ('received', 'pending')), 0) AS in_progress
             FROM files f LEFT JOIN records r ON r.file_id = f.id
             WHERE f.feed_id = ?1 GROUP BY f.id ORDER BY f.id DESC LIMIT ?2",
            iso("f.received_at"),
            iso("f.finished_at")
        ),
        params![id, q.limit.unwrap_or(50)],
    )?;
    Ok(Json(json!({"files": rows})))
}

#[derive(Deserialize)]
struct RecordQuery {
    feed: Option<String>,
    status: Option<String>,
    file_id: Option<i64>,
    limit: Option<i64>,
}

async fn list_records(State(s): State<AppState>, Query(q): Query<RecordQuery>) -> ApiResult {
    let rows = s.db.query_json(
        &format!(
            "SELECT r.id, r.feed_id, r.file_id, f.name AS file, r.line, r.status, r.error_kind, r.error,
                    r.attempts, {} AS updated_at
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE (?1 IS NULL OR r.feed_id = ?1) AND (?2 IS NULL OR r.status = ?2) AND (?3 IS NULL OR r.file_id = ?3)
             ORDER BY r.id DESC LIMIT ?4",
            iso("r.updated_at")
        ),
        params![q.feed, q.status, q.file_id, q.limit.unwrap_or(100)],
    )?;
    Ok(Json(json!({"records": rows})))
}

async fn get_record(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    let row = s
        .db
        .query_json(
            &format!(
                "SELECT r.id, r.feed_id, r.file_id, f.name AS file, f.sha256 AS file_sha256, r.line, r.status,
                        r.raw, r.original_raw, r.resources, r.response, r.error_kind, r.error, r.attempts,
                        {} AS received_at, {} AS updated_at
                 FROM records r JOIN files f ON f.id = r.file_id WHERE r.id = ?1",
                iso("f.received_at"),
                iso("r.updated_at")
            ),
            params![id],
        )?
        .into_iter()
        .next()
        .ok_or(ApiError(StatusCode::NOT_FOUND, format!("record {id} not found")))?;
    Ok(Json(row))
}

#[derive(Deserialize, Default)]
struct ReprocessBody {
    raw: Option<Value>,
}

async fn reprocess(
    State(s): State<AppState>,
    Path(id): Path<i64>,
    body: Option<Json<ReprocessBody>>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let raw = body.and_then(|Json(b)| b.raw);
    if !s.db.reprocess(id, raw.as_ref())? {
        return Err(ApiError(
            StatusCode::NOT_FOUND,
            format!("record {id} not found"),
        ));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id": id, "status": "received", "corrected": raw.is_some()})),
    ))
}

#[derive(Deserialize)]
struct AlertQuery {
    feed: Option<String>,
    limit: Option<i64>,
}

async fn list_alerts(State(s): State<AppState>, Query(q): Query<AlertQuery>) -> ApiResult {
    let rows = s.db.query_json(
        &format!(
            "SELECT id, feed_id, kind, message, {} AS created_at FROM alerts
             WHERE (?1 IS NULL OR feed_id = ?1) ORDER BY id DESC LIMIT ?2",
            iso("created_at")
        ),
        params![q.feed, q.limit.unwrap_or(100)],
    )?;
    Ok(Json(json!({"alerts": rows})))
}

/// Prometheus text format, so the numbers can live in the same monitoring stack as HFS.
async fn metrics(State(s): State<AppState>) -> Result<Response, ApiError> {
    let mut out = String::new();
    out.push_str("# TYPE hie_records gauge\n");
    for f in s.feeds.iter() {
        if let Value::Object(counts) = s.db.feed_counts(&f.id)? {
            for status in ["received", "pending", "sent", "rejected"] {
                let n = counts.get(status).and_then(Value::as_i64).unwrap_or(0);
                out.push_str(&format!(
                    "hie_records{{feed=\"{}\",status=\"{status}\"}} {n}\n",
                    f.id
                ));
            }
        }
    }
    out.push_str("# TYPE hie_feed_last_received_timestamp_seconds gauge\n");
    for f in s.feeds.iter() {
        if let Some(t) = s.db.last_received_at(&f.id)? {
            out.push_str(&format!(
                "hie_feed_last_received_timestamp_seconds{{feed=\"{}\"}} {}\n",
                f.id,
                t / 1000
            ));
        }
    }
    out.push_str("# TYPE hie_alerts_total counter\n");
    for row in s.db.query_json("SELECT COALESCE(feed_id, '') AS feed, kind, COUNT(*) AS n FROM alerts GROUP BY feed_id, kind", [])? {
        out.push_str(&format!(
            "hie_alerts_total{{feed=\"{}\",kind=\"{}\"}} {}\n",
            row["feed"].as_str().unwrap_or_default(),
            row["kind"].as_str().unwrap_or_default(),
            row["n"]
        ));
    }
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response())
}
