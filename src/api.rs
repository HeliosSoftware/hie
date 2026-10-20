use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};
use axum::routing::{get, post};
use axum::{Json, Router};
use rusqlite::params;
use serde::Deserialize;
use serde_json::{Map, Value, json};

use crate::config::FeedConfig;
use crate::db::{Db, now_ms};
use crate::manager::{ManagerError, SourceManager};
use crate::pipeline::human;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<Db>,
    pub manager: Arc<SourceManager>,
    pub auth: crate::auth::Auth,
}

/// Everything under `/api` needs a bearer token when auth is on; `/health` and
/// `/metrics` stay open for probes and the metrics scraper.
pub fn router(state: AppState) -> Router {
    let api = Router::new()
        .route("/api/sources", get(list_sources).post(create_source))
        .route("/api/sources/test", post(test_source))
        .route(
            "/api/sources/{id}",
            get(get_source).put(replace_source).delete(delete_source),
        )
        .route("/api/sources/{id}/pause", post(pause_source))
        .route("/api/sources/{id}/resume", post(resume_source))
        .route("/api/sources/{id}/check-now", post(check_now))
        .route("/api/sources/{id}/files", get(list_files))
        .route("/api/sources/{id}/stats", get(source_stats))
        .route("/api/transforms", get(list_transforms))
        .route(
            "/api/transforms/{name}",
            get(get_transform).put(put_transform),
        )
        .route("/api/transforms/{name}/try", post(try_transform))
        .route("/api/monitoring/status", get(monitoring_status))
        .route("/api/records", get(list_records))
        .route("/api/records/{id}", get(get_record))
        .route("/api/records/{id}/reprocess", post(reprocess))
        .route("/api/alerts", get(list_alerts))
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            crate::auth::require_bearer,
        ));
    Router::new()
        .route("/health", get(|| async { Json(json!({"status": "ok"})) }))
        .route("/metrics", get(metrics))
        .merge(api)
        .with_state(state)
}

const ISO: &str = "strftime('%Y-%m-%dT%H:%M:%fZ', {}/1000.0, 'unixepoch')";
const DAY_MS: i64 = 86_400_000;

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

impl From<ManagerError> for ApiError {
    fn from(e: ManagerError) -> Self {
        match e {
            ManagerError::Invalid(m) => ApiError(StatusCode::BAD_REQUEST, m),
            ManagerError::NotFound(m) => ApiError(StatusCode::NOT_FOUND, m),
            ManagerError::Conflict(m) => ApiError(StatusCode::CONFLICT, m),
            ManagerError::Internal(e) => e.into(),
        }
    }
}

type ApiResult = Result<Json<Value>, ApiError>;

fn not_found(what: &str) -> ApiError {
    ApiError(StatusCode::NOT_FOUND, format!("{what} not found"))
}

fn iso_ms(ms: i64) -> Option<String> {
    chrono::DateTime::from_timestamp_millis(ms).map(|t| t.to_rfc3339())
}

fn count(db: &Db, sql: &str, p: impl rusqlite::Params) -> anyhow::Result<i64> {
    Ok(db
        .query_json(sql, p)?
        .first()
        .and_then(|r| r.get("n"))
        .and_then(Value::as_i64)
        .unwrap_or(0))
}

/// A source's definition and its state, as one object.
fn source_status(s: &AppState, feed: &FeedConfig) -> anyhow::Result<Value> {
    let db = &*s.db;
    let last = db.last_received_at(&feed.id)?;
    let paused = db.paused_state(&feed.id)?;
    let started = s
        .manager
        .runner(&feed.id)
        .map(|r| r.started_at())
        .unwrap_or_else(now_ms);
    let freshness = match (feed.expect_every, last) {
        (None, _) => "not_monitored",
        (Some(w), _) if now_ms() - last.unwrap_or(started) > w.as_millis() as i64 => "silent",
        (Some(_), None) => "no_data_yet",
        _ => "ok",
    };
    let state = match (&paused, freshness) {
        (Some(_), _) => "paused",
        (None, "silent") => "late",
        (None, "no_data_yet") => "waiting",
        _ => "receiving",
    };
    let last_alert = db.query_json(
        &format!("SELECT kind, message, {} AS created_at FROM alerts WHERE feed_id = ?1 ORDER BY id DESC LIMIT 1", iso("created_at")),
        params![feed.id],
    )?;
    let since = now_ms() - DAY_MS;
    let rejected_24h = count(
        db,
        "SELECT COUNT(*) AS n FROM records WHERE feed_id = ?1 AND status = 'rejected' AND updated_at >= ?2",
        params![feed.id, since],
    )?;
    Ok(json!({
        "id": feed.id,
        "name": feed.name,
        "source": feed.source.describe(),
        "format": feed.format,
        "transform": feed.transform.display().to_string(),
        "poll_seconds": feed.source.poll_interval().as_secs(),
        "expect_every": feed.expect_every.map(human),
        "state": state,
        "freshness": freshness,
        "paused": paused.map(|(at, by)| json!({"since": iso_ms(at), "by": by})),
        "last_received_at": last.and_then(iso_ms),
        "records": db.feed_counts(&feed.id)?,
        "rejected_last_24h": rejected_24h,
        "last_alert": last_alert.into_iter().next(),
        "definition": s.manager.definition(&feed.id)?,
    }))
}

async fn list_sources(State(s): State<AppState>) -> ApiResult {
    let sources: anyhow::Result<Vec<Value>> = s
        .manager
        .sources()
        .iter()
        .map(|f| source_status(&s, f))
        .collect();
    Ok(Json(json!({"sources": sources?})))
}

async fn get_source(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    let feed = s
        .manager
        .source(&id)
        .ok_or_else(|| not_found(&format!("source {id}")))?;
    Ok(Json(source_status(&s, &feed)?))
}

async fn create_source(
    State(s): State<AppState>,
    Json(body): Json<Value>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let feed = s.manager.create(&body)?;
    Ok((StatusCode::CREATED, Json(source_status(&s, &feed)?)))
}

async fn replace_source(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Json(body): Json<Value>,
) -> ApiResult {
    let feed = s.manager.replace(&id, &body)?;
    Ok(Json(source_status(&s, &feed)?))
}

async fn delete_source(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<StatusCode, ApiError> {
    s.manager.delete(&id)?;
    Ok(StatusCode::NO_CONTENT)
}

#[derive(Deserialize, Default)]
struct PauseBody {
    by: Option<String>,
}

async fn pause_source(
    State(s): State<AppState>,
    Path(id): Path<String>,
    principal: Option<axum::Extension<Arc<helios_auth::Principal>>>,
    body: Option<Json<PauseBody>>,
) -> ApiResult {
    // Who paused it: the body's name, else the authenticated subject.
    let by = body
        .and_then(|Json(b)| b.by)
        .or_else(|| principal.map(|axum::Extension(p)| p.subject().to_string()));
    s.manager.pause(&id, by.as_deref())?;
    get_source(State(s), Path(id)).await
}

async fn resume_source(State(s): State<AppState>, Path(id): Path<String>) -> ApiResult {
    s.manager.resume(&id)?;
    get_source(State(s), Path(id)).await
}

async fn check_now(
    State(s): State<AppState>,
    Path(id): Path<String>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    s.manager.check_now(&id)?;
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id": id, "checking": true})),
    ))
}

async fn test_source(State(s): State<AppState>, Json(body): Json<Value>) -> ApiResult {
    let probe = s.manager.test(&body).await?;
    Ok(Json(
        serde_json::to_value(probe).map_err(anyhow::Error::from)?,
    ))
}

#[derive(Deserialize)]
struct StatsQuery {
    days: Option<i64>,
}

/// What a source's page draws: today's numbers and one row per day for the last N days.
async fn source_stats(
    State(s): State<AppState>,
    Path(id): Path<String>,
    Query(q): Query<StatsQuery>,
) -> ApiResult {
    s.manager
        .source(&id)
        .ok_or_else(|| not_found(&format!("source {id}")))?;
    let days = q.days.unwrap_or(14).clamp(1, 366);
    let today = chrono::Utc::now().date_naive();
    let first = today - chrono::Days::new(days as u64 - 1);
    let since = first
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();

    let mut by_day: Map<String, Value> = (0..days)
        .map(|i| {
            let day = (first + chrono::Days::new(i as u64)).to_string();
            (day, json!({"files": 0, "sent": 0, "rejected": 0}))
        })
        .collect();
    for row in s.db.query_json(
        "SELECT date(received_at / 1000, 'unixepoch') AS day, COUNT(*) AS n FROM files
         WHERE feed_id = ?1 AND status <> 'duplicate' AND received_at >= ?2 GROUP BY day",
        params![id, since],
    )? {
        if let Some(entry) = row["day"].as_str().and_then(|d| by_day.get_mut(d)) {
            entry["files"] = row["n"].clone();
        }
    }
    for row in s.db.query_json(
        "SELECT date(updated_at / 1000, 'unixepoch') AS day, status, COUNT(*) AS n FROM records
         WHERE feed_id = ?1 AND status IN ('sent', 'rejected') AND updated_at >= ?2 GROUP BY day, status",
        params![id, since],
    )? {
        if let (Some(entry), Some(status)) = (
            row["day"].as_str().and_then(|d| by_day.get_mut(d)),
            row["status"].as_str(),
        ) {
            entry[status] = row["n"].clone();
        }
    }
    let rows: Vec<Value> = by_day
        .into_iter()
        .map(|(day, v)| json!({"day": day, "files": v["files"], "sent": v["sent"], "rejected": v["rejected"]}))
        .collect();

    let today_since = today
        .and_hms_opt(0, 0, 0)
        .unwrap()
        .and_utc()
        .timestamp_millis();
    let today_json = json!({
        "files": count(&s.db, "SELECT COUNT(*) AS n FROM files WHERE feed_id = ?1 AND status <> 'duplicate' AND received_at >= ?2", params![id, today_since])?,
        "records": count(&s.db, "SELECT COUNT(r.id) AS n FROM records r JOIN files f ON f.id = r.file_id WHERE r.feed_id = ?1 AND f.received_at >= ?2", params![id, today_since])?,
        "sent": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = ?1 AND status = 'sent' AND updated_at >= ?2", params![id, today_since])?,
        "rejected": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = ?1 AND status = 'rejected' AND updated_at >= ?2", params![id, today_since])?,
        "pending": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE feed_id = ?1 AND status IN ('received', 'pending')", params![id])?,
    });
    Ok(Json(
        json!({"source": id, "days": days, "today": today_json, "by_day": rows}),
    ))
}

async fn list_transforms(State(s): State<AppState>) -> ApiResult {
    Ok(Json(json!({"transforms": s.manager.transforms()?})))
}

async fn get_transform(State(s): State<AppState>, Path(name): Path<String>) -> ApiResult {
    let code = s
        .manager
        .transform_code(&name)?
        .ok_or_else(|| not_found(&format!("transform {name}")))?;
    Ok(Json(json!({"name": name, "code": code})))
}

#[derive(Deserialize)]
struct TransformBody {
    code: String,
}

async fn put_transform(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<TransformBody>,
) -> Result<(StatusCode, Json<Value>), ApiError> {
    let existed = s.manager.save_transform(&name, &body.code)?;
    let status = if existed {
        StatusCode::OK
    } else {
        StatusCode::CREATED
    };
    Ok((status, Json(json!({"name": name, "saved": true}))))
}

#[derive(Deserialize)]
struct TryBody {
    records: Vec<Value>,
}

/// Runs a transform on the supplied records without loading anything.
async fn try_transform(
    State(s): State<AppState>,
    Path(name): Path<String>,
    Json(body): Json<TryBody>,
) -> ApiResult {
    if body.records.len() > 100 {
        return Err(ApiError(
            StatusCode::BAD_REQUEST,
            "try at most 100 records at a time".into(),
        ));
    }
    let results = s.manager.try_transform(&name, &body.records).await?;
    Ok(Json(json!({"name": name, "results": results})))
}

/// What the monitoring page opens on: what needs a person, the last 24 hours, every source.
async fn monitoring_status(State(s): State<AppState>) -> ApiResult {
    let mut sources = Vec::new();
    let mut needs_you = Vec::new();
    for feed in s.manager.sources() {
        let status = source_status(&s, &feed)?;
        let rejected = status["rejected_last_24h"].as_i64().unwrap_or(0);
        let reason = match status["state"].as_str() {
            Some("late") => Some(format!(
                "nothing received since {}, expected at least every {}",
                status["last_received_at"].as_str().unwrap_or("the start"),
                status["expect_every"].as_str().unwrap_or("?")
            )),
            Some("paused") => Some(format!(
                "paused{} since {}",
                status["paused"]["by"]
                    .as_str()
                    .map(|b| format!(" by {b}"))
                    .unwrap_or_default(),
                status["paused"]["since"].as_str().unwrap_or("?")
            )),
            _ if rejected > 0 => Some(format!("{rejected} records rejected in the last 24 hours")),
            _ => None,
        };
        if let Some(reason) = reason {
            needs_you.push(json!({"id": feed.id, "name": feed.name, "state": status["state"], "reason": reason}));
        }
        sources.push(status);
    }
    let since = now_ms() - DAY_MS;
    let last_24h = json!({
        "files": count(&s.db, "SELECT COUNT(*) AS n FROM files WHERE status <> 'duplicate' AND received_at >= ?1", params![since])?,
        "sent": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE status = 'sent' AND updated_at >= ?1", params![since])?,
        "rejected": count(&s.db, "SELECT COUNT(*) AS n FROM records WHERE status = 'rejected' AND updated_at >= ?1", params![since])?,
        "alerts": count(&s.db, "SELECT COUNT(*) AS n FROM alerts WHERE created_at >= ?1", params![since])?,
    });
    Ok(Json(json!({
        "state": if needs_you.is_empty() { "all_receiving" } else { "needs_you" },
        "needs_you": needs_you,
        "last_24h": last_24h,
        "sources": sources,
    })))
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
    source: Option<String>,
    status: Option<String>,
    file_id: Option<i64>,
    limit: Option<i64>,
}

async fn list_records(State(s): State<AppState>, Query(q): Query<RecordQuery>) -> ApiResult {
    let rows = s.db.query_json(
        &format!(
            "SELECT r.id, r.feed_id AS source_id, r.file_id, f.name AS file, r.line, r.status, r.error_kind, r.error,
                    r.attempts, {} AS updated_at
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE (?1 IS NULL OR r.feed_id = ?1) AND (?2 IS NULL OR r.status = ?2) AND (?3 IS NULL OR r.file_id = ?3)
             ORDER BY r.id DESC LIMIT ?4",
            iso("r.updated_at")
        ),
        params![q.source, q.status, q.file_id, q.limit.unwrap_or(100)],
    )?;
    Ok(Json(json!({"records": rows})))
}

async fn get_record(State(s): State<AppState>, Path(id): Path<i64>) -> ApiResult {
    let row = s
        .db
        .query_json(
            &format!(
                "SELECT r.id, r.feed_id AS source_id, r.file_id, f.name AS file, f.sha256 AS file_sha256, r.line, r.status,
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
        .ok_or_else(|| not_found(&format!("record {id}")))?;
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
        return Err(not_found(&format!("record {id}")));
    }
    Ok((
        StatusCode::ACCEPTED,
        Json(json!({"id": id, "status": "received", "corrected": raw.is_some()})),
    ))
}

#[derive(Deserialize)]
struct AlertQuery {
    source: Option<String>,
    limit: Option<i64>,
}

async fn list_alerts(State(s): State<AppState>, Query(q): Query<AlertQuery>) -> ApiResult {
    let rows = s.db.query_json(
        &format!(
            "SELECT id, feed_id AS source_id, kind, message, {} AS created_at FROM alerts
             WHERE (?1 IS NULL OR feed_id = ?1) ORDER BY id DESC LIMIT ?2",
            iso("created_at")
        ),
        params![q.source, q.limit.unwrap_or(100)],
    )?;
    Ok(Json(json!({"alerts": rows})))
}

/// Prometheus text format, so the numbers can live in the same monitoring stack as HFS.
async fn metrics(State(s): State<AppState>) -> Result<Response, ApiError> {
    let sources = s.manager.sources();
    let mut out = String::new();
    out.push_str("# TYPE hie_records gauge\n");
    for f in &sources {
        if let Value::Object(counts) = s.db.feed_counts(&f.id)? {
            for status in ["received", "pending", "sent", "rejected"] {
                let n = counts.get(status).and_then(Value::as_i64).unwrap_or(0);
                out.push_str(&format!(
                    "hie_records{{source=\"{}\",status=\"{status}\"}} {n}\n",
                    f.id
                ));
            }
        }
    }
    out.push_str("# TYPE hie_source_last_received_timestamp_seconds gauge\n");
    for f in &sources {
        if let Some(t) = s.db.last_received_at(&f.id)? {
            out.push_str(&format!(
                "hie_source_last_received_timestamp_seconds{{source=\"{}\"}} {}\n",
                f.id,
                t / 1000
            ));
        }
    }
    out.push_str("# TYPE hie_source_paused gauge\n");
    for f in &sources {
        out.push_str(&format!(
            "hie_source_paused{{source=\"{}\"}} {}\n",
            f.id,
            u8::from(s.db.is_paused(&f.id)?)
        ));
    }
    out.push_str("# TYPE hie_alerts_total counter\n");
    for row in s.db.query_json("SELECT COALESCE(feed_id, '') AS source, kind, COUNT(*) AS n FROM alerts GROUP BY feed_id, kind", [])? {
        out.push_str(&format!(
            "hie_alerts_total{{source=\"{}\",kind=\"{}\"}} {}\n",
            row["source"].as_str().unwrap_or_default(),
            row["kind"].as_str().unwrap_or_default(),
            row["n"]
        ));
    }
    Ok(([(header::CONTENT_TYPE, "text/plain; version=0.0.4")], out).into_response())
}
