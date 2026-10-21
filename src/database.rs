//! A SQL database as a source: a query that follows a cursor column. Each pass
//! binds the cursor where the query says `{cursor}`, takes the rows past it, and
//! hands them on as one NDJSON load; the cursor becomes the largest value of the
//! cursor column once the records are stored.

use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use anyhow::{Context, Result, bail};
use chrono::{DateTime, NaiveDate, NaiveDateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use sqlx::mysql::{MySqlPool, MySqlPoolOptions, MySqlRow};
use sqlx::postgres::{PgPool, PgPoolOptions, PgRow};
use sqlx::sqlite::{SqlitePool, SqlitePoolOptions, SqliteRow};
use sqlx::{Column, Row, TypeInfo};

use crate::db::Db;
use crate::source::{Incoming, Probe};

/// The one key a database source has: its query. The cursor is stored under the
/// cursor column's name instead, so changing the column starts over from
/// `cursor_start` rather than binding a value of the wrong kind.
pub const KEY: &str = "query";

pub struct DatabaseSource {
    feed_id: String,
    url: String,
    query: String,
    cursor_column: String,
    cursor_start: Option<String>,
    max_runs: usize,
    pool: tokio::sync::OnceCell<Pool>,
    pub(crate) db: Option<Arc<Db>>,
    pulled: Mutex<Option<Pulled>>,
}

enum Pool {
    Postgres(PgPool),
    MySql(MySqlPool),
    Sqlite(SqlitePool),
}

struct Pulled {
    bytes: Vec<u8>,
    count: usize,
    newest: Cursor,
}

/// A cursor value with the kind it binds as, kept as JSON in the store.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum Cursor {
    Int(i64),
    Float(f64),
    /// A date-time as the driver's decode wrote it (RFC 3339 for Postgres and
    /// MySQL; whatever the column holds for SQLite).
    Timestamp(String),
    Date(String),
    Text(String),
}

impl Cursor {
    /// What a person typed as `cursor_start`: a number, an instant, a date, or text.
    fn parse(text: &str) -> Self {
        let t = text.trim();
        if let Ok(i) = t.parse::<i64>() {
            return Cursor::Int(i);
        }
        if let Ok(f) = t.parse::<f64>() {
            return Cursor::Float(f);
        }
        if DateTime::parse_from_rfc3339(t).is_ok() || naive(t).is_some() {
            return Cursor::Timestamp(t.to_string());
        }
        if NaiveDate::parse_from_str(t, "%Y-%m-%d").is_ok() {
            return Cursor::Date(t.to_string());
        }
        Cursor::Text(t.to_string())
    }

    /// The cursor a row's cursor column gives.
    fn from_value(v: &Value) -> Option<Self> {
        match v {
            Value::Number(n) => n
                .as_i64()
                .map(Cursor::Int)
                .or_else(|| n.as_f64().map(Cursor::Float)),
            Value::String(s) => Some(Cursor::parse(s)),
            Value::Bool(b) => Some(Cursor::Int(*b as i64)),
            _ => None,
        }
    }

    fn later_than(&self, other: &Cursor) -> bool {
        match (self, other) {
            (Cursor::Int(a), Cursor::Int(b)) => a > b,
            (Cursor::Float(a), Cursor::Float(b)) => a > b,
            (Cursor::Int(a), Cursor::Float(b)) => (*a as f64) > *b,
            (Cursor::Float(a), Cursor::Int(b)) => *a > (*b as f64),
            (Cursor::Timestamp(a), Cursor::Timestamp(b)) => match (instant(a), instant(b)) {
                (Some(a), Some(b)) => a > b,
                _ => a > b,
            },
            (a, b) => a.text() > b.text(),
        }
    }

    fn text(&self) -> String {
        match self {
            Cursor::Int(i) => i.to_string(),
            Cursor::Float(f) => f.to_string(),
            Cursor::Timestamp(s) | Cursor::Date(s) | Cursor::Text(s) => s.clone(),
        }
    }
}

fn naive(t: &str) -> Option<NaiveDateTime> {
    ["%Y-%m-%dT%H:%M:%S%.f", "%Y-%m-%d %H:%M:%S%.f"]
        .iter()
        .find_map(|f| NaiveDateTime::parse_from_str(t, f).ok())
}

fn instant(t: &str) -> Option<DateTime<Utc>> {
    DateTime::parse_from_rfc3339(t)
        .map(|d| d.with_timezone(&Utc))
        .ok()
        .or_else(|| naive(t).map(|n| n.and_utc()))
}

impl DatabaseSource {
    pub fn new(
        feed_id: &str,
        url_env: &str,
        query: &str,
        cursor_column: &str,
        cursor_start: Option<String>,
        max_runs: usize,
    ) -> Result<Self> {
        let url = std::env::var(url_env)
            .ok()
            .filter(|v| !v.trim().is_empty())
            .with_context(|| {
                format!("environment variable {url_env} (the database URL named by the source) is not set")
            })?;
        if !(url.starts_with("postgres://")
            || url.starts_with("postgresql://")
            || url.starts_with("mysql://")
            || url.starts_with("sqlite:"))
        {
            bail!("{url_env} must be a postgres://, mysql:// or sqlite: URL");
        }
        Ok(Self {
            feed_id: feed_id.to_string(),
            url,
            query: query.to_string(),
            cursor_column: cursor_column.to_string(),
            cursor_start,
            max_runs,
            pool: tokio::sync::OnceCell::new(),
            db: None,
            pulled: Mutex::new(None),
        })
    }

    async fn pool(&self) -> Result<&Pool> {
        self.pool
            .get_or_try_init(|| async {
                let timeout = Duration::from_secs(30);
                let pool = if self.url.starts_with("mysql://") {
                    Pool::MySql(
                        MySqlPoolOptions::new()
                            .max_connections(2)
                            .acquire_timeout(timeout)
                            .connect(&self.url)
                            .await?,
                    )
                } else if self.url.starts_with("sqlite:") {
                    Pool::Sqlite(
                        SqlitePoolOptions::new()
                            .max_connections(1)
                            .acquire_timeout(timeout)
                            .connect(&self.url)
                            .await?,
                    )
                } else {
                    Pool::Postgres(
                        PgPoolOptions::new()
                            .max_connections(2)
                            .acquire_timeout(timeout)
                            .connect(&self.url)
                            .await?,
                    )
                };
                Ok::<_, anyhow::Error>(pool)
            })
            .await
            .context("connecting to the database")
    }

    fn cursor(&self) -> Result<Option<Cursor>> {
        if let Some(db) = &self.db
            && let Some(stored) = db.cursor(&self.feed_id, &self.cursor_column)?
        {
            return Ok(Some(
                serde_json::from_str(&stored).context("stored cursor")?,
            ));
        }
        Ok(self.cursor_start.as_deref().map(Cursor::parse))
    }

    /// The query with `{cursor}` replaced by the driver's placeholder, and how many
    /// times the value must be bound.
    fn sql(&self, pool: &Pool) -> (String, usize) {
        let occurrences = self.query.matches("{cursor}").count();
        match pool {
            Pool::Postgres(_) => (self.query.replace("{cursor}", "$1"), 1),
            Pool::Sqlite(_) => (self.query.replace("{cursor}", "?1"), 1),
            Pool::MySql(_) => (self.query.replace("{cursor}", "?"), occurrences),
        }
    }

    /// One run of the query past `cursor`, as JSON rows. Without a cursor the
    /// `{cursor}` is bound to the lowest value of its kind, so the query still parses.
    /// The SQL is the operator's own, from the source definition; the only value
    /// that comes from data, the cursor, is bound, never spliced.
    async fn run(&self, cursor: Option<&Cursor>) -> Result<Vec<Map<String, Value>>> {
        let pool = self.pool().await?;
        let (sql, binds) = self.sql(pool);
        let first = cursor.is_none();
        let cursor = cursor.cloned().unwrap_or(Cursor::Int(i64::MIN));
        let rows = match pool {
            Pool::Postgres(p) => {
                let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));
                for _ in 0..binds {
                    q = match &cursor {
                        Cursor::Int(i) => q.bind(*i),
                        Cursor::Float(f) => q.bind(*f),
                        Cursor::Timestamp(t) => match instant(t) {
                            Some(at) => q.bind(at),
                            None => q.bind(t.clone()),
                        },
                        Cursor::Date(d) => match NaiveDate::parse_from_str(d, "%Y-%m-%d") {
                            Ok(d) => q.bind(d),
                            Err(_) => q.bind(d.clone()),
                        },
                        Cursor::Text(s) => q.bind(s.clone()),
                    };
                }
                q.fetch_all(p)
                    .await
                    .with_context(|| query_failed(&sql, first))?
                    .iter()
                    .map(pg_row)
                    .collect()
            }
            Pool::MySql(p) => {
                let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));
                for _ in 0..binds {
                    q = match &cursor {
                        Cursor::Int(i) => q.bind(*i),
                        Cursor::Float(f) => q.bind(*f),
                        Cursor::Timestamp(t) => match instant(t) {
                            Some(at) => q.bind(at.naive_utc()),
                            None => q.bind(t.clone()),
                        },
                        Cursor::Date(d) => match NaiveDate::parse_from_str(d, "%Y-%m-%d") {
                            Ok(d) => q.bind(d),
                            Err(_) => q.bind(d.clone()),
                        },
                        Cursor::Text(s) => q.bind(s.clone()),
                    };
                }
                q.fetch_all(p)
                    .await
                    .with_context(|| query_failed(&sql, first))?
                    .iter()
                    .map(mysql_row)
                    .collect()
            }
            Pool::Sqlite(p) => {
                let mut q = sqlx::query(sqlx::AssertSqlSafe(sql.clone()));
                for _ in 0..binds {
                    q = match &cursor {
                        Cursor::Int(i) => q.bind(*i),
                        Cursor::Float(f) => q.bind(*f),
                        Cursor::Timestamp(t) | Cursor::Date(t) | Cursor::Text(t) => {
                            q.bind(t.clone())
                        }
                    };
                }
                q.fetch_all(p)
                    .await
                    .with_context(|| query_failed(&sql, first))?
                    .iter()
                    .map(sqlite_row)
                    .collect()
            }
        };
        Ok(rows)
    }

    /// Runs the query past the cursor, moving an in-memory cursor between runs,
    /// until a run brings nothing new or `max_runs` is reached.
    async fn pull(&self) -> Result<Pulled> {
        let mut cursor = self.cursor()?;
        let mut bytes = Vec::new();
        let mut count = 0;
        for _ in 0..self.max_runs {
            let rows = self.run(cursor.as_ref()).await?;
            if rows.is_empty() {
                break;
            }
            let mut newest = cursor.clone();
            for row in &rows {
                let Some(value) = row.get(&self.cursor_column) else {
                    bail!(
                        "the query does not return the cursor column {:?} (columns: {})",
                        self.cursor_column,
                        row.keys().cloned().collect::<Vec<_>>().join(", ")
                    );
                };
                if let Some(c) = Cursor::from_value(value)
                    && newest.as_ref().is_none_or(|n| c.later_than(n))
                {
                    newest = Some(c);
                }
                bytes.extend_from_slice(Value::Object(row.clone()).to_string().as_bytes());
                bytes.push(b'\n');
                count += 1;
            }
            if newest == cursor {
                // Every row sits at the cursor: the column is not moving, so
                // another run would bring the same rows again.
                break;
            }
            if rows.len() >= 2
                && rows.iter().all(|r| {
                    r.get(&self.cursor_column)
                        .and_then(Cursor::from_value)
                        .as_ref()
                        == newest.as_ref()
                })
            {
                // A run whose rows all share one cursor value may have been cut by
                // LIMIT in the middle of that value; the rest would never be read.
                tracing::warn!(
                    feed = %self.feed_id,
                    cursor = %newest.as_ref().map(Cursor::text).unwrap_or_default(),
                    "every row of a run shares the cursor value; rows beyond LIMIT with that value are skipped. Use a unique cursor column (an id) or a finer timestamp"
                );
            }
            cursor = newest;
        }
        Ok(Pulled {
            bytes,
            count,
            newest: cursor.unwrap_or(Cursor::Int(i64::MIN)),
        })
    }

    pub async fn list(&self) -> Result<Vec<String>> {
        let pulled = self.pull().await?;
        if pulled.count == 0 {
            return Ok(vec![]);
        }
        tracing::debug!(feed = %self.feed_id, rows = pulled.count, newest = %pulled.newest.text(), "pulled");
        *self.pulled.lock().expect("pulled mutex") = Some(pulled);
        Ok(vec![KEY.to_string()])
    }

    pub fn fetch(&self, key: &str) -> Result<Incoming> {
        let pulled = self.pulled.lock().expect("pulled mutex");
        let p = pulled
            .as_ref()
            .filter(|_| key == KEY)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        Ok(Incoming {
            key: KEY.to_string(),
            name: format!(
                "{}-{}.ndjson",
                self.feed_id,
                Utc::now().format("%Y%m%dT%H%M%SZ")
            ),
            bytes: p.bytes.clone(),
        })
    }

    /// The records are stored: the cursor may move past them.
    pub fn acknowledge(&self, key: &str) -> Result<String> {
        let newest = self
            .pulled
            .lock()
            .expect("pulled mutex")
            .take()
            .filter(|_| key == KEY)
            .map(|p| p.newest)
            .with_context(|| format!("{key} was not pulled in this pass"))?;
        if let Some(db) = &self.db {
            db.set_cursor(
                &self.feed_id,
                &self.cursor_column,
                &serde_json::to_string(&newest)?,
            )?;
        }
        Ok(format!("cursor {}", newest.text()))
    }

    /// Connects and runs the query once past the cursor: what waits, and the
    /// newest cursor value among it.
    pub async fn probe(&self, started: Instant) -> Result<Probe> {
        let cursor = self.cursor()?;
        let rows = self.run(cursor.as_ref()).await?;
        let newest = rows
            .iter()
            .filter_map(|r| r.get(&self.cursor_column))
            .filter_map(Cursor::from_value)
            .fold(None::<Cursor>, |best, c| match best {
                Some(b) if !c.later_than(&b) => Some(b),
                _ => Some(c),
            });
        Ok(Probe {
            reachable: true,
            elapsed_ms: started.elapsed().as_millis() as u64,
            objects: rows.len(),
            newest: newest
                .as_ref()
                .map(|c| format!("{} = {}", self.cursor_column, c.text())),
            newest_at: newest.and_then(|c| match c {
                Cursor::Timestamp(t) => instant(&t).map(|i| i.to_rfc3339()),
                Cursor::Date(d) => Some(d),
                _ => None,
            }),
        })
    }
}

/// Why a run failed. Before the first cursor the placeholder is bound as an
/// integer, which a timestamp or text column refuses; the fix is a cursor_start.
fn query_failed(sql: &str, first: bool) -> String {
    if first {
        format!(
            "running the query (no cursor yet, so {{cursor}} was bound as an integer; if the cursor column is not one, set cursor_start to a value of its kind, e.g. 1970-01-01T00:00:00Z): {sql}"
        )
    } else {
        format!("running the query: {sql}")
    }
}

/// `try_get` of an optional value, as JSON through `f`.
macro_rules! get {
    ($row:expr, $i:expr, $t:ty, $f:expr) => {
        $row.try_get::<Option<$t>, _>($i)
            .ok()
            .map(|v| v.map_or(Value::Null, $f))
    };
}

fn pg_row(row: &PgRow) -> Map<String, Value> {
    let mut out = Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.type_info().name();
        let value = match name {
            "BOOL" => get!(row, i, bool, Value::Bool),
            "INT2" => get!(row, i, i16, |v| Value::from(v as i64)),
            "INT4" => get!(row, i, i32, |v| Value::from(v as i64)),
            "INT8" => get!(row, i, i64, Value::from),
            "FLOAT4" => get!(row, i, f32, |v| Value::from(v as f64)),
            "FLOAT8" => get!(row, i, f64, Value::from),
            "NUMERIC" => get!(row, i, sqlx::types::Decimal, |d| number(&d.to_string())),
            "UUID" => get!(row, i, sqlx::types::Uuid, |u| Value::String(u.to_string())),
            "DATE" => get!(row, i, NaiveDate, |d| Value::String(d.to_string())),
            "TIME" => get!(row, i, chrono::NaiveTime, |t| Value::String(t.to_string())),
            "TIMESTAMP" => get!(row, i, NaiveDateTime, |t| Value::String(
                t.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
            )),
            "TIMESTAMPTZ" => get!(row, i, DateTime<Utc>, |t| Value::String(t.to_rfc3339())),
            "JSON" | "JSONB" => get!(row, i, Value, |v| v),
            "BYTEA" => get!(row, i, Vec<u8>, |b| Value::String(hex::encode(b))),
            _ => get!(row, i, String, Value::String),
        };
        out.insert(
            column.name().to_string(),
            value.unwrap_or_else(|| {
                tracing::warn!(
                    column = column.name(),
                    kind = name,
                    "column type not decoded; null"
                );
                Value::Null
            }),
        );
    }
    out
}

fn mysql_row(row: &MySqlRow) -> Map<String, Value> {
    let mut out = Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        let name = column.type_info().name();
        let value = match name {
            "BOOLEAN" => get!(row, i, bool, Value::Bool),
            "TINYINT" | "SMALLINT" | "MEDIUMINT" | "INT" | "BIGINT" => {
                get!(row, i, i64, Value::from)
            }
            "TINYINT UNSIGNED" | "SMALLINT UNSIGNED" | "MEDIUMINT UNSIGNED" | "INT UNSIGNED"
            | "BIGINT UNSIGNED" => get!(row, i, u64, Value::from),
            "FLOAT" | "DOUBLE" => get!(row, i, f64, Value::from),
            "DECIMAL" => get!(row, i, sqlx::types::Decimal, |d| number(&d.to_string())),
            "DATE" => get!(row, i, NaiveDate, |d| Value::String(d.to_string())),
            "TIME" => get!(row, i, chrono::NaiveTime, |t| Value::String(t.to_string())),
            "DATETIME" => get!(row, i, NaiveDateTime, |t| Value::String(
                t.format("%Y-%m-%dT%H:%M:%S%.f").to_string()
            )),
            "TIMESTAMP" => get!(row, i, DateTime<Utc>, |t| Value::String(t.to_rfc3339())),
            "JSON" => get!(row, i, Value, |v| v),
            "BLOB" | "TINYBLOB" | "MEDIUMBLOB" | "LONGBLOB" | "BINARY" | "VARBINARY" => {
                get!(row, i, Vec<u8>, |b| Value::String(hex::encode(b)))
            }
            _ => get!(row, i, String, Value::String),
        };
        out.insert(
            column.name().to_string(),
            value.unwrap_or_else(|| {
                tracing::warn!(
                    column = column.name(),
                    kind = name,
                    "column type not decoded; null"
                );
                Value::Null
            }),
        );
    }
    out
}

fn sqlite_row(row: &SqliteRow) -> Map<String, Value> {
    let mut out = Map::new();
    for (i, column) in row.columns().iter().enumerate() {
        // SQLite types are per value, not per column: try the kinds in turn.
        let value = get!(row, i, i64, Value::from)
            .filter(|v| !v.is_null())
            .or_else(|| get!(row, i, f64, Value::from).filter(|v| !v.is_null()))
            .or_else(|| get!(row, i, String, Value::String).filter(|v| !v.is_null()))
            .or_else(|| get!(row, i, Vec<u8>, |b| Value::String(hex::encode(b))))
            .unwrap_or(Value::Null);
        out.insert(column.name().to_string(), value);
    }
    out
}

/// A decimal as a JSON number when it fits one, as text otherwise.
fn number(text: &str) -> Value {
    serde_json::from_str::<Value>(text)
        .ok()
        .filter(Value::is_number)
        .unwrap_or_else(|| Value::String(text.to_string()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cursor_starts_are_typed() {
        assert_eq!(Cursor::parse("42"), Cursor::Int(42));
        assert_eq!(Cursor::parse("4.5"), Cursor::Float(4.5));
        assert_eq!(
            Cursor::parse("2026-10-01T10:00:00Z"),
            Cursor::Timestamp("2026-10-01T10:00:00Z".into())
        );
        assert_eq!(
            Cursor::parse("2026-10-01 10:00:00"),
            Cursor::Timestamp("2026-10-01 10:00:00".into())
        );
        assert_eq!(
            Cursor::parse("2026-10-01"),
            Cursor::Date("2026-10-01".into())
        );
        assert_eq!(Cursor::parse("abc"), Cursor::Text("abc".into()));
    }

    #[test]
    fn cursors_order_within_their_kind() {
        assert!(Cursor::Int(3).later_than(&Cursor::Int(2)));
        assert!(!Cursor::Int(2).later_than(&Cursor::Int(2)));
        assert!(
            Cursor::Timestamp("2026-10-01T10:00:00+02:00".into())
                .later_than(&Cursor::Timestamp("2026-10-01T07:00:00Z".into()))
        );
        assert!(Cursor::Text("b".into()).later_than(&Cursor::Text("a".into())));
    }
}
