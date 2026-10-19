use std::path::Path;
use std::sync::Mutex;

use anyhow::{Context, Result};
use rusqlite::types::ValueRef;
use rusqlite::{Connection, OptionalExtension, Params, params};
use serde_json::{Map, Value, json};

const SCHEMA: &str = "
CREATE TABLE IF NOT EXISTS files (
  id INTEGER PRIMARY KEY,
  feed_id TEXT NOT NULL,
  name TEXT NOT NULL,
  sha256 TEXT NOT NULL,
  size INTEGER NOT NULL,
  status TEXT NOT NULL,
  duplicate_of INTEGER,
  received_at INTEGER NOT NULL,
  finished_at INTEGER,
  error TEXT,
  rejected_reported INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX IF NOT EXISTS files_feed_sha ON files(feed_id, sha256);
CREATE INDEX IF NOT EXISTS files_feed_received ON files(feed_id, received_at);
CREATE TABLE IF NOT EXISTS records (
  id INTEGER PRIMARY KEY,
  file_id INTEGER NOT NULL REFERENCES files(id),
  feed_id TEXT NOT NULL,
  line INTEGER NOT NULL,
  raw TEXT NOT NULL,
  original_raw TEXT,
  status TEXT NOT NULL,
  resources TEXT,
  error_kind TEXT,
  error TEXT,
  response TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS records_feed_status ON records(feed_id, status, next_attempt_at);
CREATE INDEX IF NOT EXISTS records_file ON records(file_id);
CREATE TABLE IF NOT EXISTS alerts (
  id INTEGER PRIMARY KEY,
  feed_id TEXT,
  kind TEXT NOT NULL,
  message TEXT NOT NULL,
  created_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS alerts_feed_kind ON alerts(feed_id, kind, created_at);
";

pub fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

pub struct Db {
    conn: Mutex<Connection>,
}

#[derive(Debug, Clone)]
pub struct WorkItem {
    pub id: i64,
    pub file_id: i64,
    pub line: i64,
    pub file_name: String,
    pub raw: Value,
    pub resources: Option<Value>,
    pub attempts: u32,
}

#[derive(Debug, Clone)]
pub struct FinishedFile {
    pub id: i64,
    pub name: String,
    pub total: i64,
    pub sent: i64,
    pub rejected: i64,
    pub newly_rejected: i64,
    pub first_rejection: Option<(i64, String)>,
}

impl Db {
    pub fn open(path: &Path) -> Result<Self> {
        if let Some(dir) = path.parent() {
            std::fs::create_dir_all(dir)?;
        }
        let conn = Connection::open(path).with_context(|| format!("opening {}", path.display()))?;
        Self::init(conn)
    }

    pub fn in_memory() -> Result<Self> {
        Self::init(Connection::open_in_memory()?)
    }

    fn init(conn: Connection) -> Result<Self> {
        conn.pragma_update(None, "journal_mode", "WAL")?;
        conn.pragma_update(None, "foreign_keys", "ON")?;
        conn.pragma_update(None, "busy_timeout", 5000)?;
        conn.execute_batch(SCHEMA)?;
        Ok(Self {
            conn: Mutex::new(conn),
        })
    }

    fn with<T>(&self, f: impl FnOnce(&Connection) -> rusqlite::Result<T>) -> Result<T> {
        let conn = self.conn.lock().expect("database mutex poisoned");
        Ok(f(&conn)?)
    }

    pub fn find_file_by_hash(&self, feed: &str, sha: &str) -> Result<Option<i64>> {
        self.with(|c| {
            c.query_row(
                "SELECT id FROM files WHERE feed_id = ?1 AND sha256 = ?2 AND status <> 'duplicate' ORDER BY id LIMIT 1",
                params![feed, sha],
                |r| r.get(0),
            )
            .optional()
        })
    }

    pub fn insert_file(
        &self,
        feed: &str,
        name: &str,
        sha: &str,
        size: u64,
        status: &str,
        duplicate_of: Option<i64>,
    ) -> Result<i64> {
        self.with(|c| {
            c.execute(
                "INSERT INTO files (feed_id, name, sha256, size, status, duplicate_of, received_at, finished_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, CASE WHEN ?5 = 'processing' THEN NULL ELSE ?7 END)",
                params![feed, name, sha, size as i64, status, duplicate_of, now_ms()],
            )?;
            Ok(c.last_insert_rowid())
        })
    }

    pub fn fail_file(&self, file_id: i64, error: &str) -> Result<()> {
        self.with(|c| {
            c.execute(
                "UPDATE files SET status = 'unreadable', error = ?2, finished_at = ?3 WHERE id = ?1",
                params![file_id, error, now_ms()],
            )
            .map(drop)
        })
    }

    pub fn insert_records(
        &self,
        file_id: i64,
        feed: &str,
        rows: &[(i64, Value, Option<String>)],
    ) -> Result<()> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let tx = conn.transaction()?;
        {
            let mut stmt = tx.prepare(
                "INSERT INTO records (file_id, feed_id, line, raw, status, error_kind, error, updated_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            )?;
            let now = now_ms();
            for (line, raw, parse_error) in rows {
                let (status, kind) = match parse_error {
                    Some(_) => ("rejected", Some("parse")),
                    None => ("received", None),
                };
                stmt.execute(params![
                    file_id,
                    feed,
                    line,
                    raw.to_string(),
                    status,
                    kind,
                    parse_error,
                    now
                ])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn work_items(&self, sql: &str, p: impl Params) -> Result<Vec<WorkItem>> {
        self.with(|c| {
            let mut stmt = c.prepare(sql)?;
            let rows = stmt.query_map(p, |r| {
                let raw: String = r.get(4)?;
                let resources: Option<String> = r.get(5)?;
                Ok(WorkItem {
                    id: r.get(0)?,
                    file_id: r.get(1)?,
                    line: r.get(2)?,
                    file_name: r.get(3)?,
                    raw: serde_json::from_str(&raw).unwrap_or(Value::Null),
                    resources: resources.and_then(|s| serde_json::from_str(&s).ok()),
                    attempts: r.get::<_, i64>(6)? as u32,
                })
            })?;
            rows.collect()
        })
    }

    pub fn received_records(&self, feed: &str, limit: usize) -> Result<Vec<WorkItem>> {
        self.work_items(
            "SELECT r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = ?1 AND r.status = 'received' ORDER BY r.id LIMIT ?2",
            params![feed, limit as i64],
        )
    }

    pub fn due_records(&self, feed: &str, limit: usize) -> Result<Vec<WorkItem>> {
        self.work_items(
            "SELECT r.id, r.file_id, r.line, f.name, r.raw, r.resources, r.attempts
             FROM records r JOIN files f ON f.id = r.file_id
             WHERE r.feed_id = ?1 AND r.status = 'pending' AND r.next_attempt_at <= ?2
             ORDER BY r.id LIMIT ?3",
            params![feed, now_ms(), limit as i64],
        )
    }

    pub fn mark_transformed(&self, id: i64, resources: &Value) -> Result<()> {
        self.with(|c| {
            c.execute(
                "UPDATE records SET status = 'pending', resources = ?2, error_kind = NULL, error = NULL,
                        attempts = 0, next_attempt_at = 0, updated_at = ?3 WHERE id = ?1",
                params![id, resources.to_string(), now_ms()],
            )
            .map(drop)
        })
    }

    pub fn mark_rejected(
        &self,
        id: i64,
        kind: &str,
        error: &str,
        response: Option<&Value>,
    ) -> Result<()> {
        self.with(|c| {
            c.execute(
                "UPDATE records SET status = 'rejected', error_kind = ?2, error = ?3, response = ?4, updated_at = ?5 WHERE id = ?1",
                params![id, kind, error, response.map(Value::to_string), now_ms()],
            )
            .map(drop)
        })
    }

    pub fn mark_sent(&self, id: i64, response: &Value) -> Result<()> {
        self.with(|c| {
            c.execute(
                "UPDATE records SET status = 'sent', response = ?2, error_kind = NULL, error = NULL, updated_at = ?3 WHERE id = ?1",
                params![id, response.to_string(), now_ms()],
            )
            .map(drop)
        })
    }

    pub fn defer(&self, id: i64, attempts: u32, next_attempt_at: i64, error: &str) -> Result<()> {
        self.with(|c| {
            c.execute(
                "UPDATE records SET attempts = ?2, next_attempt_at = ?3, error_kind = 'transient', error = ?4, updated_at = ?5 WHERE id = ?1",
                params![id, attempts as i64, next_attempt_at, error, now_ms()],
            )
            .map(drop)
        })
    }

    /// Puts a record back at the start of the pipeline, optionally with corrected source data.
    pub fn reprocess(&self, id: i64, raw: Option<&Value>) -> Result<bool> {
        self.with(|c| {
            let n = match raw {
                Some(raw) => c.execute(
                    "UPDATE records SET status = 'received', original_raw = COALESCE(original_raw, raw), raw = ?2, resources = NULL, error_kind = NULL, error = NULL,
                            response = NULL, attempts = 0, next_attempt_at = 0, updated_at = ?3 WHERE id = ?1",
                    params![id, raw.to_string(), now_ms()],
                )?,
                None => c.execute(
                    "UPDATE records SET status = 'received', resources = NULL, error_kind = NULL, error = NULL,
                            response = NULL, attempts = 0, next_attempt_at = 0, updated_at = ?2 WHERE id = ?1",
                    params![id, now_ms()],
                )?,
            };
            if n > 0 {
                c.execute(
                    "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = (SELECT file_id FROM records WHERE id = ?1)",
                    params![id],
                )?;
            }
            Ok(n > 0)
        })
    }

    /// Marks files whose records have all reached `sent` or `rejected` as done and returns them.
    pub fn finish_files(&self, feed: &str) -> Result<Vec<FinishedFile>> {
        self.with(|c| {
            let mut stmt = c.prepare(
                "SELECT f.id, f.name,
                        COUNT(r.id),
                        COALESCE(SUM(r.status = 'sent'), 0),
                        COALESCE(SUM(r.status = 'rejected'), 0),
                        f.rejected_reported
                 FROM files f LEFT JOIN records r ON r.file_id = f.id
                 WHERE f.feed_id = ?1 AND f.status = 'processing'
                 GROUP BY f.id
                 HAVING COALESCE(SUM(r.status IN ('received', 'pending')), 0) = 0",
            )?;
            let mut done: Vec<FinishedFile> = stmt
                .query_map(params![feed], |r| {
                    Ok(FinishedFile {
                        id: r.get(0)?,
                        name: r.get(1)?,
                        total: r.get(2)?,
                        sent: r.get(3)?,
                        rejected: r.get(4)?,
                        newly_rejected: r.get::<_, i64>(4)? - r.get::<_, i64>(5)?,
                        first_rejection: None,
                    })
                })?
                .collect::<rusqlite::Result<_>>()?;
            for f in &mut done {
                c.execute(
                    "UPDATE files SET status = 'done', finished_at = ?2, rejected_reported = MAX(rejected_reported, ?3) WHERE id = ?1",
                    params![f.id, now_ms(), f.rejected],
                )?;
                f.first_rejection = c
                    .query_row(
                        "SELECT line, error FROM records WHERE file_id = ?1 AND status = 'rejected' ORDER BY line LIMIT 1",
                        params![f.id],
                        |r| Ok((r.get(0)?, r.get::<_, Option<String>>(1)?.unwrap_or_default())),
                    )
                    .optional()?;
            }
            Ok(done)
        })
    }

    pub fn last_received_at(&self, feed: &str) -> Result<Option<i64>> {
        self.with(|c| {
            c.query_row(
                "SELECT MAX(received_at) FROM files WHERE feed_id = ?1 AND status <> 'duplicate'",
                params![feed],
                |r| r.get(0),
            )
        })
    }

    pub fn last_alert_at(&self, feed: &str, kind: &str) -> Result<Option<i64>> {
        self.with(|c| {
            c.query_row(
                "SELECT MAX(created_at) FROM alerts WHERE feed_id = ?1 AND kind = ?2",
                params![feed, kind],
                |r| r.get(0),
            )
        })
    }

    pub fn insert_alert(&self, feed: Option<&str>, kind: &str, message: &str) -> Result<i64> {
        self.with(|c| {
            c.execute(
                "INSERT INTO alerts (feed_id, kind, message, created_at) VALUES (?1, ?2, ?3, ?4)",
                params![feed, kind, message, now_ms()],
            )?;
            Ok(c.last_insert_rowid())
        })
    }

    pub fn feed_counts(&self, feed: &str) -> Result<Value> {
        self.with(|c| {
            let mut counts = Map::new();
            for status in ["received", "pending", "sent", "rejected"] {
                counts.insert(status.into(), json!(0));
            }
            let mut stmt = c.prepare(
                "SELECT status, COUNT(*) FROM records WHERE feed_id = ?1 GROUP BY status",
            )?;
            let rows = stmt.query_map(params![feed], |r| {
                Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
            })?;
            for row in rows {
                let (status, n) = row?;
                counts.insert(status, json!(n));
            }
            let files: i64 = c.query_row(
                "SELECT COUNT(*) FROM files WHERE feed_id = ?1 AND status <> 'duplicate'",
                params![feed],
                |r| r.get(0),
            )?;
            let duplicates: i64 = c.query_row(
                "SELECT COUNT(*) FROM files WHERE feed_id = ?1 AND status = 'duplicate'",
                params![feed],
                |r| r.get(0),
            )?;
            counts.insert("files".into(), json!(files));
            counts.insert("duplicate_files".into(), json!(duplicates));
            Ok(Value::Object(counts))
        })
    }

    /// Runs a read-only query and returns each row as a JSON object keyed by column name.
    pub fn query_json(&self, sql: &str, p: impl Params) -> Result<Vec<Value>> {
        self.with(|c| {
            let mut stmt = c.prepare(sql)?;
            let names: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
            let rows = stmt.query_map(p, |r| {
                let mut obj = Map::new();
                for (i, name) in names.iter().enumerate() {
                    let v = match r.get_ref(i)? {
                        ValueRef::Null => Value::Null,
                        ValueRef::Integer(n) => json!(n),
                        ValueRef::Real(f) => json!(f),
                        ValueRef::Text(t) => {
                            let s = String::from_utf8_lossy(t);
                            if matches!(
                                name.as_str(),
                                "raw" | "original_raw" | "resources" | "response"
                            ) {
                                serde_json::from_str(&s).unwrap_or(Value::String(s.into_owned()))
                            } else {
                                Value::String(s.into_owned())
                            }
                        }
                        ValueRef::Blob(_) => Value::Null,
                    };
                    obj.insert(name.clone(), v);
                }
                Ok(Value::Object(obj))
            })?;
            rows.collect()
        })
    }
}
