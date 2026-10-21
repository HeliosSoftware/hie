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
CREATE TABLE IF NOT EXISTS source_state (
  feed_id TEXT PRIMARY KEY,
  paused INTEGER NOT NULL DEFAULT 0,
  paused_at INTEGER,
  paused_by TEXT
);
CREATE TABLE IF NOT EXISTS source_cursors (
  feed_id TEXT NOT NULL,
  key TEXT NOT NULL,
  cursor TEXT NOT NULL,
  updated_at INTEGER NOT NULL,
  PRIMARY KEY (feed_id, key)
);
CREATE TABLE IF NOT EXISTS submissions (
  token TEXT PRIMARY KEY,
  feed_id TEXT NOT NULL,
  submission_id TEXT NOT NULL,
  status TEXT NOT NULL,
  manifest TEXT NOT NULL,
  status_url TEXT,
  attempts INTEGER NOT NULL DEFAULT 0,
  next_attempt_at INTEGER NOT NULL DEFAULT 0,
  error TEXT,
  created_at INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS submissions_feed_status ON submissions(feed_id, status, next_attempt_at);
CREATE TABLE IF NOT EXISTS alert_rules (
  id INTEGER PRIMARY KEY,
  source_id TEXT,
  condition TEXT NOT NULL,
  threshold TEXT NOT NULL,
  notify TEXT NOT NULL DEFAULT '[]',
  channels TEXT NOT NULL DEFAULT '[\"app\"]',
  enabled INTEGER NOT NULL DEFAULT 1,
  created_by TEXT,
  created_at INTEGER NOT NULL
);
";

/// Columns added after the first schema; each is added when missing.
const ADDED_COLUMNS: &[(&str, &str, &str)] = &[
    ("files", "source_key", "TEXT"),
    ("files", "archived", "INTEGER NOT NULL DEFAULT 0"),
    ("records", "submission", "TEXT"),
    ("alerts", "rule_id", "INTEGER"),
    ("alerts", "cleared_at", "INTEGER"),
    ("alerts", "cleared_reason", "TEXT"),
    ("alerts", "notify", "TEXT"),
    ("alerts", "channels", "TEXT"),
];

/// A rule in words: "nothing arrives for 2 hours", "more than 1% of a load's rows
/// fail", "the file is not there by 06:00", "HFS cannot be reached after 5 attempts".
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Rule {
    #[serde(default)]
    pub id: i64,
    /// `None` applies to every source.
    #[serde(default)]
    pub source_id: Option<String>,
    pub condition: String,
    pub threshold: String,
    #[serde(default)]
    pub notify: Vec<String>,
    #[serde(default = "default_channels")]
    pub channels: Vec<String>,
    #[serde(default = "default_enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub created_by: Option<String>,
}

fn default_channels() -> Vec<String> {
    vec!["app".into(), "webhook".into()]
}
fn default_enabled() -> bool {
    true
}

/// A `$bulk-submit` submission the engine handed to HFS and is waiting on.
#[derive(Debug, Clone)]
pub struct Submission {
    pub token: String,
    pub feed_id: String,
    pub submission_id: String,
    pub status: String,
    pub manifest: Value,
    pub status_url: Option<String>,
    pub attempts: u32,
}

/// A file with records waiting to be submitted, and what they are.
#[derive(Debug, Clone)]
pub struct SubmittableFile {
    pub file_id: i64,
    pub name: String,
    pub source_key: Option<String>,
    /// (record id, resource type) for every pending record of the file.
    pub records: Vec<(i64, String)>,
}

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
        for (table, column, kind) in ADDED_COLUMNS {
            let exists = conn
                .prepare(&format!("PRAGMA table_info({table})"))?
                .query_map([], |r| r.get::<_, String>(1))?
                .filter_map(Result::ok)
                .any(|c| c == *column);
            if !exists {
                conn.execute(
                    &format!("ALTER TABLE {table} ADD COLUMN {column} {kind}"),
                    [],
                )?;
            }
        }
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

    /// Records a file. `source_key` is kept when the object must stay at its source
    /// until HFS has fetched it (the `$bulk-submit` path).
    #[allow(clippy::too_many_arguments)]
    pub fn insert_file(
        &self,
        feed: &str,
        name: &str,
        sha: &str,
        size: u64,
        status: &str,
        duplicate_of: Option<i64>,
        source_key: Option<&str>,
    ) -> Result<i64> {
        self.with(|c| {
            c.execute(
                "INSERT INTO files (feed_id, name, sha256, size, status, duplicate_of, received_at, finished_at, source_key)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, CASE WHEN ?5 = 'processing' THEN NULL ELSE ?7 END, ?8)",
                params![feed, name, sha, size as i64, status, duplicate_of, now_ms(), source_key],
            )?;
            Ok(c.last_insert_rowid())
        })
    }

    /// The key and name of a file still waiting at its source (not archived yet).
    pub fn file_source(&self, file_id: i64) -> Result<Option<(String, String)>> {
        self.with(|c| {
            c.query_row(
                "SELECT source_key, name FROM files WHERE id = ?1 AND source_key IS NOT NULL AND archived = 0",
                params![file_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
        })
    }

    /// The file was moved out of the way; `key` is where it lives now, so a retry
    /// can still hand it to HFS.
    pub fn mark_file_archived(&self, file_id: i64, key: &str) -> Result<()> {
        self.with(|c| {
            c.execute(
                "UPDATE files SET source_key = ?2, archived = 1 WHERE id = ?1",
                params![file_id, key],
            )
            .map(drop)
        })
    }

    /// Rejected records of a file, grouped by reason, each with a few sample rows.
    pub fn failure_groups(&self, file_id: i64) -> Result<Vec<Value>> {
        let groups = self.query_json(
            "SELECT error_kind AS kind, error AS reason, COUNT(*) AS n, MIN(line) AS first_line
             FROM records WHERE file_id = ?1 AND status = 'rejected'
             GROUP BY error_kind, error ORDER BY n DESC, first_line",
            params![file_id],
        )?;
        let mut out = Vec::with_capacity(groups.len());
        for g in groups {
            let samples = self.query_json(
                "SELECT id, line, raw FROM records
                 WHERE file_id = ?1 AND status = 'rejected' AND error_kind IS ?2 AND error IS ?3
                 ORDER BY line LIMIT 3",
                params![file_id, g["kind"].as_str(), g["reason"].as_str()],
            )?;
            out.push(json!({
                "kind": g["kind"],
                "reason": g["reason"],
                "count": g["n"],
                "first_line": g["first_line"],
                "samples": samples,
            }));
        }
        Ok(out)
    }

    /// Puts a file's rejected records back at the start, all of them or those with
    /// one reason. Returns how many.
    pub fn reprocess_rejected(&self, file_id: i64, reason: Option<&str>) -> Result<usize> {
        self.with(|c| {
            let n = c.execute(
                "UPDATE records SET status = 'received', resources = NULL, error_kind = NULL, error = NULL,
                        response = NULL, attempts = 0, next_attempt_at = 0, submission = NULL, updated_at = ?3
                 WHERE file_id = ?1 AND status = 'rejected' AND (?2 IS NULL OR error = ?2)",
                params![file_id, reason, now_ms()],
            )?;
            if n > 0 {
                c.execute(
                    "UPDATE files SET status = 'processing', finished_at = NULL WHERE id = ?1",
                    params![file_id],
                )?;
            }
            Ok(n)
        })
    }

    /// Files of a feed with pending records that no submission holds yet.
    pub fn submittable_files(&self, feed: &str, limit: usize) -> Result<Vec<SubmittableFile>> {
        self.with(|c| {
            let mut stmt = c.prepare(
                "SELECT r.id, r.file_id, f.name, f.source_key, r.resources
                 FROM records r JOIN files f ON f.id = r.file_id
                 WHERE r.feed_id = ?1 AND r.status = 'pending' AND r.submission IS NULL
                 ORDER BY r.file_id, r.id",
            )?;
            let mut files: Vec<SubmittableFile> = Vec::new();
            let rows = stmt.query_map(params![feed], |r| {
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    r.get::<_, String>(2)?,
                    r.get::<_, Option<String>>(3)?,
                    r.get::<_, Option<String>>(4)?,
                ))
            })?;
            for row in rows {
                let (record_id, file_id, name, source_key, resources) = row?;
                let resource_type = resources
                    .and_then(|s| serde_json::from_str::<Value>(&s).ok())
                    .and_then(|v| {
                        v.get(0)
                            .and_then(|r| r.get("resourceType"))
                            .and_then(Value::as_str)
                            .map(str::to_string)
                    })
                    .unwrap_or_default();
                match files.last_mut() {
                    Some(f) if f.file_id == file_id => f.records.push((record_id, resource_type)),
                    _ => {
                        if files.len() >= limit {
                            break;
                        }
                        files.push(SubmittableFile {
                            file_id,
                            name,
                            source_key,
                            records: vec![(record_id, resource_type)],
                        });
                    }
                }
            }
            Ok(files)
        })
    }

    /// Opens a submission over the given records, which move to `submitted`.
    pub fn create_submission(
        &self,
        token: &str,
        feed: &str,
        submission_id: &str,
        manifest: &Value,
        record_ids: &[i64],
    ) -> Result<()> {
        let mut conn = self.conn.lock().expect("database mutex poisoned");
        let tx = conn.transaction()?;
        let now = now_ms();
        tx.execute(
            "INSERT INTO submissions (token, feed_id, submission_id, status, manifest, created_at, updated_at)
             VALUES (?1, ?2, ?3, 'kickoff', ?4, ?5, ?5)",
            params![token, feed, submission_id, manifest.to_string(), now],
        )?;
        {
            let mut stmt = tx.prepare(
                "UPDATE records SET status = 'submitted', submission = ?2, updated_at = ?3 WHERE id = ?1",
            )?;
            for id in record_ids {
                stmt.execute(params![id, token, now])?;
            }
        }
        tx.commit()?;
        Ok(())
    }

    fn submissions(&self, sql: &str, p: impl Params) -> Result<Vec<Submission>> {
        self.with(|c| {
            let mut stmt = c.prepare(sql)?;
            let rows = stmt.query_map(p, |r| {
                let manifest: String = r.get(4)?;
                Ok(Submission {
                    token: r.get(0)?,
                    feed_id: r.get(1)?,
                    submission_id: r.get(2)?,
                    status: r.get(3)?,
                    manifest: serde_json::from_str(&manifest).unwrap_or(Value::Null),
                    status_url: r.get(5)?,
                    attempts: r.get::<_, i64>(6)? as u32,
                })
            })?;
            rows.collect()
        })
    }

    pub fn submission(&self, token: &str) -> Result<Option<Submission>> {
        Ok(self
            .submissions(
                "SELECT token, feed_id, submission_id, status, manifest, status_url, attempts FROM submissions WHERE token = ?1",
                params![token],
            )?
            .into_iter()
            .next())
    }

    /// Open submissions of a feed whose next attempt is due.
    pub fn submissions_due(&self, feed: &str) -> Result<Vec<Submission>> {
        self.submissions(
            "SELECT token, feed_id, submission_id, status, manifest, status_url, attempts FROM submissions
             WHERE feed_id = ?1 AND status IN ('kickoff', 'running') AND next_attempt_at <= ?2 ORDER BY created_at",
            params![feed, now_ms()],
        )
    }

    pub fn update_submission(
        &self,
        token: &str,
        status: &str,
        status_url: Option<&str>,
        attempts: u32,
        next_attempt_at: i64,
        error: Option<&str>,
    ) -> Result<()> {
        self.with(|c| {
            c.execute(
                "UPDATE submissions SET status = ?2, status_url = COALESCE(?3, status_url), attempts = ?4,
                        next_attempt_at = ?5, error = ?6, updated_at = ?7 WHERE token = ?1",
                params![token, status, status_url, attempts as i64, next_attempt_at, error, now_ms()],
            )
            .map(drop)
        })
    }

    /// The records a submission holds: (record id, file id, resources).
    pub fn submitted_records(&self, token: &str) -> Result<Vec<(i64, i64, Value)>> {
        self.with(|c| {
            let mut stmt = c.prepare(
                "SELECT id, file_id, resources FROM records WHERE submission = ?1 AND status = 'submitted' ORDER BY id",
            )?;
            let rows = stmt.query_map(params![token], |r| {
                let resources: Option<String> = r.get(2)?;
                Ok((
                    r.get::<_, i64>(0)?,
                    r.get::<_, i64>(1)?,
                    resources.and_then(|s| serde_json::from_str(&s).ok()).unwrap_or(Value::Null),
                ))
            })?;
            rows.collect()
        })
    }

    /// The source key of a file, only when that file is part of the submission.
    pub fn submission_file(&self, token: &str, file_id: i64) -> Result<Option<String>> {
        self.with(|c| {
            c.query_row(
                "SELECT f.source_key FROM files f
                 WHERE f.id = ?2 AND f.source_key IS NOT NULL
                   AND EXISTS (SELECT 1 FROM records r WHERE r.file_id = f.id AND r.submission = ?1)",
                params![token, file_id],
                |r| r.get(0),
            )
            .optional()
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
                 HAVING COALESCE(SUM(r.status IN ('received', 'pending', 'submitted')), 0) = 0",
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

    /// Pauses or resumes a source. Paused is an operational state kept here, not
    /// in the source's definition file.
    pub fn set_paused(&self, feed: &str, paused: bool, by: Option<&str>) -> Result<()> {
        self.with(|c| {
            c.execute(
                "INSERT INTO source_state (feed_id, paused, paused_at, paused_by) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(feed_id) DO UPDATE SET paused = excluded.paused, paused_at = excluded.paused_at, paused_by = excluded.paused_by",
                params![feed, paused, paused.then(now_ms), if paused { by } else { None }],
            )
            .map(drop)
        })
    }

    /// When and by whom a source was paused, if it is.
    pub fn paused_state(&self, feed: &str) -> Result<Option<(i64, Option<String>)>> {
        self.with(|c| {
            c.query_row(
                "SELECT paused_at, paused_by FROM source_state WHERE feed_id = ?1 AND paused = 1",
                params![feed],
                |r| Ok((r.get::<_, Option<i64>>(0)?.unwrap_or_default(), r.get(1)?)),
            )
            .optional()
        })
    }

    pub fn is_paused(&self, feed: &str) -> Result<bool> {
        Ok(self.paused_state(feed)?.is_some())
    }

    /// Where a pulling source left off, per key (a resource type for a FHIR server).
    pub fn cursor(&self, feed: &str, key: &str) -> Result<Option<String>> {
        self.with(|c| {
            c.query_row(
                "SELECT cursor FROM source_cursors WHERE feed_id = ?1 AND key = ?2",
                params![feed, key],
                |r| r.get(0),
            )
            .optional()
        })
    }

    pub fn set_cursor(&self, feed: &str, key: &str, cursor: &str) -> Result<()> {
        self.with(|c| {
            c.execute(
                "INSERT INTO source_cursors (feed_id, key, cursor, updated_at) VALUES (?1, ?2, ?3, ?4)
                 ON CONFLICT(feed_id, key) DO UPDATE SET cursor = excluded.cursor, updated_at = excluded.updated_at",
                params![feed, key, cursor, now_ms()],
            )?;
            Ok(())
        })
    }

    pub fn cursors(&self, feed: &str) -> Result<Vec<(String, String)>> {
        self.with(|c| {
            c.prepare("SELECT key, cursor FROM source_cursors WHERE feed_id = ?1 ORDER BY key")?
                .query_map(params![feed], |r| Ok((r.get(0)?, r.get(1)?)))?
                .collect()
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

    pub fn insert_alert(
        &self,
        feed: Option<&str>,
        kind: &str,
        message: &str,
        rule: Option<&Rule>,
    ) -> Result<i64> {
        self.with(|c| {
            c.execute(
                "INSERT INTO alerts (feed_id, kind, message, created_at, rule_id, notify, channels)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7)",
                params![
                    feed,
                    kind,
                    message,
                    now_ms(),
                    rule.map(|r| r.id),
                    rule.map(|r| serde_json::to_string(&r.notify).unwrap_or_default()),
                    rule.map(|r| serde_json::to_string(&r.channels).unwrap_or_default()),
                ],
            )?;
            Ok(c.last_insert_rowid())
        })
    }

    /// The open alert (not cleared) of this kind for a source and rule, if any.
    pub fn open_alert(
        &self,
        feed: &str,
        kind: &str,
        rule_id: Option<i64>,
    ) -> Result<Option<(i64, i64)>> {
        self.with(|c| {
            c.query_row(
                "SELECT id, created_at FROM alerts
                 WHERE feed_id = ?1 AND kind = ?2 AND cleared_at IS NULL AND (rule_id IS ?3)
                 ORDER BY id DESC LIMIT 1",
                params![feed, kind, rule_id],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .optional()
        })
    }

    pub fn clear_alert(&self, id: i64, reason: &str) -> Result<()> {
        self.with(|c| {
            c.execute(
                "UPDATE alerts SET cleared_at = ?2, cleared_reason = ?3 WHERE id = ?1 AND cleared_at IS NULL",
                params![id, now_ms(), reason],
            )
            .map(drop)
        })
    }

    // ---- alert rules -------------------------------------------------------

    fn rule_rows(&self, sql: &str, p: impl Params) -> Result<Vec<Rule>> {
        self.with(|c| {
            let mut stmt = c.prepare(sql)?;
            let rows = stmt.query_map(p, |r| {
                let notify: String = r.get(4)?;
                let channels: String = r.get(5)?;
                Ok(Rule {
                    id: r.get(0)?,
                    source_id: r.get(1)?,
                    condition: r.get(2)?,
                    threshold: r.get(3)?,
                    notify: serde_json::from_str(&notify).unwrap_or_default(),
                    channels: serde_json::from_str(&channels).unwrap_or_default(),
                    enabled: r.get::<_, i64>(6)? != 0,
                    created_by: r.get(7)?,
                })
            })?;
            rows.collect()
        })
    }

    const RULE_SELECT: &'static str = "SELECT id, source_id, condition, threshold, notify, channels, enabled, created_by FROM alert_rules";

    pub fn rules(&self) -> Result<Vec<Rule>> {
        self.rule_rows(&format!("{} ORDER BY id", Self::RULE_SELECT), [])
    }

    pub fn rule(&self, id: i64) -> Result<Option<Rule>> {
        Ok(self
            .rule_rows(&format!("{} WHERE id = ?1", Self::RULE_SELECT), params![id])?
            .into_iter()
            .next())
    }

    /// The enabled rules that watch a source: its own and the ones for every source.
    pub fn rules_for(&self, feed: &str) -> Result<Vec<Rule>> {
        self.rule_rows(
            &format!(
                "{} WHERE enabled = 1 AND (source_id IS NULL OR source_id = ?1) ORDER BY id",
                Self::RULE_SELECT
            ),
            params![feed],
        )
    }

    pub fn has_rule(&self, feed: &str, condition: &str) -> Result<bool> {
        self.with(|c| {
            c.query_row(
                "SELECT 1 FROM alert_rules WHERE source_id = ?1 AND condition = ?2 LIMIT 1",
                params![feed, condition],
                |_| Ok(()),
            )
            .optional()
            .map(|r| r.is_some())
        })
    }

    pub fn insert_rule(&self, rule: &Rule) -> Result<i64> {
        self.with(|c| {
            c.execute(
                "INSERT INTO alert_rules (source_id, condition, threshold, notify, channels, enabled, created_by, created_at)
                 VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                params![
                    rule.source_id,
                    rule.condition,
                    rule.threshold,
                    serde_json::to_string(&rule.notify).unwrap_or_default(),
                    serde_json::to_string(&rule.channels).unwrap_or_default(),
                    rule.enabled,
                    rule.created_by,
                    now_ms()
                ],
            )?;
            Ok(c.last_insert_rowid())
        })
    }

    pub fn update_rule(&self, id: i64, rule: &Rule) -> Result<bool> {
        self.with(|c| {
            let n = c.execute(
                "UPDATE alert_rules SET source_id = ?2, condition = ?3, threshold = ?4, notify = ?5, channels = ?6, enabled = ?7
                 WHERE id = ?1",
                params![
                    id,
                    rule.source_id,
                    rule.condition,
                    rule.threshold,
                    serde_json::to_string(&rule.notify).unwrap_or_default(),
                    serde_json::to_string(&rule.channels).unwrap_or_default(),
                    rule.enabled
                ],
            )?;
            Ok(n > 0)
        })
    }

    pub fn delete_rule(&self, id: i64) -> Result<bool> {
        self.with(|c| Ok(c.execute("DELETE FROM alert_rules WHERE id = ?1", params![id])? > 0))
    }

    pub fn feed_counts(&self, feed: &str) -> Result<Value> {
        self.with(|c| {
            let mut counts = Map::new();
            for status in ["received", "pending", "submitted", "sent", "rejected"] {
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
