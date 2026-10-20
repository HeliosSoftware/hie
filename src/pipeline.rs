use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use serde_json::{Value, json};

use crate::alerts::Alerter;
use crate::config::FeedConfig;
use crate::db::{Db, WorkItem, now_ms};
use crate::fhir::{self, EntryResult, FhirClient, SendError};
use crate::reader;
use crate::source::{Incoming, Source};
use crate::transform::{self, Outcome, Transformer};

const TRANSFORM_CHUNK: usize = 200;

/// How the manager drives a running feed: `poke` makes it pass now, `stopped` ends it.
#[derive(Default)]
pub struct RunnerControl {
    pub poke: tokio::sync::Notify,
    pub stopped: AtomicBool,
}

/// Moves one feed's data through: source -> stored rows -> transform -> HFS.
/// Every row is in the database before its object leaves the source, so a crash or an
/// HFS outage never loses data, and every row keeps its object name and line number.
pub struct FeedRunner {
    pub feed: FeedConfig,
    source: Source,
    db: Arc<Db>,
    fhir: Arc<FhirClient>,
    transformer: Transformer,
    alerts: Arc<Alerter>,
    started_at: i64,
    outage_alerted: AtomicBool,
}

impl FeedRunner {
    pub fn new(
        feed: FeedConfig,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
        python: &str,
        runtime_dir: &Path,
    ) -> Result<Self> {
        let source = Source::from_config(&feed)?;
        Ok(Self::with_source(
            feed,
            source,
            db,
            fhir,
            alerts,
            python,
            runtime_dir,
        ))
    }

    /// Builds a runner over a given source, for tests and for sources that are
    /// not configured from a file.
    pub fn with_source(
        feed: FeedConfig,
        source: Source,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
        python: &str,
        runtime_dir: &Path,
    ) -> Self {
        let transformer = Transformer::new(python, runtime_dir, &feed.transform);
        Self {
            feed,
            source,
            db,
            fhir,
            transformer,
            alerts,
            started_at: now_ms(),
            outage_alerted: AtomicBool::new(false),
        }
    }

    /// The feed's loop: a pass every poll interval, or sooner when poked; nothing
    /// while the source is paused; until stopped.
    pub async fn run(self: Arc<Self>, control: Arc<RunnerControl>) {
        tracing::info!(feed = %self.feed.id, source = %self.feed.source.describe(), "watching");
        loop {
            if control.stopped.load(Ordering::Acquire) {
                tracing::info!(feed = %self.feed.id, "stopped");
                return;
            }
            match self.db.is_paused(&self.feed.id) {
                Ok(true) => {}
                Ok(false) => {
                    if let Err(e) = self.tick().await {
                        tracing::error!(feed = %self.feed.id, "{e:#}");
                    }
                }
                Err(e) => tracing::error!(feed = %self.feed.id, "{e:#}"),
            }
            tokio::select! {
                _ = tokio::time::sleep(self.feed.source.poll_interval()) => {}
                _ = control.poke.notified() => {}
            }
        }
    }

    /// One pass of the whole pipeline. Each step picks up whatever the previous
    /// passes left, so a step that fails is simply retried on the next pass.
    pub async fn tick(&self) -> Result<()> {
        self.scan().await?;
        self.transform_received().await?;
        self.deliver_due().await?;
        self.finish()?;
        self.check_silence()?;
        Ok(())
    }

    async fn scan(&self) -> Result<()> {
        for key in self.source.list().await? {
            let incoming = self.source.fetch(&key).await?;
            self.ingest(incoming).await?;
        }
        Ok(())
    }

    async fn ingest(&self, incoming: Incoming) -> Result<()> {
        let feed = &self.feed.id;
        let Incoming { key, name, bytes } = incoming;
        let sha = reader::sha256_hex(&bytes);

        if let Some(original) = self.db.find_file_by_hash(feed, &sha)? {
            let id = self.db.insert_file(
                feed,
                &name,
                &sha,
                bytes.len() as u64,
                "duplicate",
                Some(original),
            )?;
            tracing::info!(feed, file = %name, duplicate_of = original, "same content already received; not reprocessed");
            return self.source.acknowledge(&key, &format!("{id}-{name}")).await;
        }

        let file_id =
            self.db
                .insert_file(feed, &name, &sha, bytes.len() as u64, "processing", None)?;
        match reader::read(self.feed.format, &bytes) {
            Ok(records) => {
                let rows: Vec<_> = records
                    .into_iter()
                    .map(|r| (r.line, r.raw, r.parse_error))
                    .collect();
                self.db.insert_records(file_id, feed, &rows)?;
                tracing::info!(feed, file = %name, records = rows.len(), "received");
            }
            Err(e) => {
                self.db.fail_file(file_id, &format!("{e:#}"))?;
                self.alerts.raise(
                    Some(feed),
                    "unreadable_file",
                    &format!("{}: cannot read {name}: {e:#}", self.feed.name),
                )?;
            }
        }
        self.source
            .acknowledge(&key, &format!("{file_id}-{name}"))
            .await
    }

    async fn transform_received(&self) -> Result<()> {
        loop {
            let items = self.db.received_records(&self.feed.id, TRANSFORM_CHUNK)?;
            if items.is_empty() {
                return Ok(());
            }
            for item in &items {
                let context =
                    json!({"feed": self.feed.id, "file": item.file_name, "line": item.line});
                let outcome = match self.transformer.transform(&item.raw, &context).await {
                    Ok(o) => o,
                    Err(e) => {
                        tracing::error!(feed = %self.feed.id, "transform process unavailable, will retry: {e:#}");
                        return Ok(());
                    }
                };
                match outcome {
                    Outcome::Rejected { kind, message } => {
                        self.db.mark_rejected(item.id, &kind, &message, None)?
                    }
                    Outcome::Resources(resources) if resources.is_empty() => {
                        self.db.mark_rejected(
                            item.id,
                            "filtered",
                            "transform returned no resources",
                            None,
                        )?
                    }
                    Outcome::Resources(resources) => {
                        match resources
                            .iter()
                            .map(transform::check_resource)
                            .collect::<Result<Vec<_>, _>>()
                        {
                            Ok(_) => self
                                .db
                                .mark_transformed(item.id, &Value::Array(resources))?,
                            Err(msg) => self.db.mark_rejected(item.id, "validation", &msg, None)?,
                        }
                    }
                }
            }
        }
    }

    async fn deliver_due(&self) -> Result<()> {
        loop {
            let items = self
                .db
                .due_records(&self.feed.id, self.feed.delivery.batch_size)?;
            if items.is_empty() {
                return Ok(());
            }
            if !self.deliver(&items).await? {
                return Ok(());
            }
        }
    }

    /// Sends `items` in one batch Bundle. Returns false when HFS is unavailable,
    /// so the caller stops until the next pass instead of hammering it.
    async fn deliver(&self, items: &[WorkItem]) -> Result<bool> {
        let mut entries = Vec::new();
        let mut owner = Vec::new();
        for (i, item) in items.iter().enumerate() {
            for resource in item
                .resources
                .as_ref()
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                let key = transform::check_resource(resource).map_err(anyhow::Error::msg)?;
                entries.push((fhir::request_url(&key), resource.clone()));
                owner.push(i);
            }
        }
        match self.fhir.post_bundle(&fhir::batch_bundle(&entries)).await {
            Err(SendError::Transient(reason)) => {
                for item in items {
                    self.defer(item, &reason)?;
                }
                let worst = items.iter().map(|i| i.attempts + 1).max().unwrap_or(0);
                if worst >= self.feed.delivery.alert_after_attempts
                    && !self.outage_alerted.swap(true, Ordering::SeqCst)
                {
                    self.alerts.raise(
                        Some(&self.feed.id),
                        "delivery",
                        &format!("{}: cannot deliver to HFS after {worst} attempts ({reason}); data is kept and retried", self.feed.name),
                    )?;
                }
                Ok(false)
            }
            Err(SendError::Refused(status, _)) if items.len() > 1 => {
                tracing::warn!(feed = %self.feed.id, status, "batch refused as a whole; sending records one by one");
                for item in items {
                    if !Box::pin(self.deliver(std::slice::from_ref(item))).await? {
                        return Ok(false);
                    }
                }
                Ok(true)
            }
            Err(SendError::Refused(status, body)) => {
                let reason = format!("HTTP {status}: {}", fhir::outcome_text(&body));
                self.db
                    .mark_rejected(items[0].id, "server", &reason, Some(&body))?;
                Ok(true)
            }
            Ok(response) => {
                if self.outage_alerted.swap(false, Ordering::SeqCst) {
                    self.alerts.raise(
                        Some(&self.feed.id),
                        "delivery_recovered",
                        &format!("{}: delivery to HFS recovered", self.feed.name),
                    )?;
                }
                let results = fhir::entry_results(&response, entries.len());
                for (i, item) in items.iter().enumerate() {
                    let mine: Vec<&EntryResult> = results
                        .iter()
                        .zip(&owner)
                        .filter(|(_, o)| **o == i)
                        .map(|(r, _)| r)
                        .collect();
                    let responses: Vec<Value> = mine
                        .iter()
                        .map(|r| match r {
                            EntryResult::Ok(v)
                            | EntryResult::Rejected(_, v)
                            | EntryResult::Transient(_, v) => v.clone(),
                        })
                        .collect();
                    if let Some(EntryResult::Rejected(status, v)) =
                        mine.iter().find(|r| matches!(r, EntryResult::Rejected(..)))
                    {
                        let reason = format!("HTTP {status}: {}", fhir::outcome_text(v));
                        self.db.mark_rejected(
                            item.id,
                            "server",
                            &reason,
                            Some(&Value::Array(responses)),
                        )?;
                    } else if let Some(EntryResult::Transient(status, _)) = mine
                        .iter()
                        .find(|r| matches!(r, EntryResult::Transient(..)))
                    {
                        self.defer(item, &format!("HTTP {status} for this entry"))?;
                    } else {
                        self.db.mark_sent(item.id, &Value::Array(responses))?;
                    }
                }
                Ok(true)
            }
        }
    }

    fn defer(&self, item: &WorkItem, reason: &str) -> Result<()> {
        let attempts = item.attempts + 1;
        let d = &self.feed.delivery;
        let backoff = d
            .initial_backoff_ms
            .saturating_mul(1u64 << (attempts - 1).min(20))
            .min(d.max_backoff_ms);
        self.db
            .defer(item.id, attempts, now_ms() + backoff as i64, reason)
    }

    fn finish(&self) -> Result<()> {
        for f in self.db.finish_files(&self.feed.id)? {
            tracing::info!(feed = %self.feed.id, file = %f.name, total = f.total, sent = f.sent, rejected = f.rejected, "file done");
            if f.newly_rejected > 0 {
                let first = f
                    .first_rejection
                    .as_ref()
                    .map(|(line, err)| {
                        format!(
                            " First: line {line}: {}",
                            err.lines().next().unwrap_or_default()
                        )
                    })
                    .unwrap_or_default();
                self.alerts.raise(
                    Some(&self.feed.id),
                    "rejected_records",
                    &format!(
                        "{}: {} of {} records rejected in {}.{first}",
                        self.feed.name, f.rejected, f.total, f.name
                    ),
                )?;
            }
        }
        Ok(())
    }

    fn check_silence(&self) -> Result<()> {
        let Some(window) = self.feed.expect_every else {
            return Ok(());
        };
        let last = self
            .db
            .last_received_at(&self.feed.id)?
            .unwrap_or(self.started_at);
        let silent_for = now_ms() - last;
        if silent_for <= window.as_millis() as i64 {
            return Ok(());
        }
        if self
            .db
            .last_alert_at(&self.feed.id, "silence")?
            .is_some_and(|t| t >= last)
        {
            return Ok(());
        }
        self.alerts.raise(
            Some(&self.feed.id),
            "silence",
            &format!(
                "{}: nothing received for {} (expected at least every {})",
                self.feed.name,
                human(Duration::from_millis(silent_for as u64)),
                human(window)
            ),
        )
    }
}

pub fn human(d: Duration) -> String {
    let s = d.as_secs();
    match s {
        0..=119 => format!("{s}s"),
        120..=7199 => format!("{}m", s / 60),
        7200..=172_799 => format!("{}h", s / 3600),
        _ => format!("{}d", s / 86_400),
    }
}
