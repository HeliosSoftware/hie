use std::path::Path;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use anyhow::Result;
use serde_json::{Value, json};

use chrono::Timelike;

use crate::alerts::{Alerter, parse_clock};
use crate::bulk::{self, BulkSubmitter, Poll};
use crate::config::{DeliveryMethod, FeedConfig, parse_duration};
use crate::db::{Db, Rule, WorkItem, now_ms};
use crate::fhir::{self, DeliveryKey, EntryResult, FhirClient, SendError};
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
    bulk: Option<BulkSubmitter>,
    transformer: Transformer,
    alerts: Arc<Alerter>,
    started_at: i64,
    outage_alerted: AtomicBool,
}

impl FeedRunner {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        feed: FeedConfig,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
        python: &str,
        runtime_dir: &Path,
        fhir_base_url: &str,
        public_url: &str,
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
            fhir_base_url,
            public_url,
        ))
    }

    /// Builds a runner over a given source, for tests and for sources that are
    /// not configured from a file.
    #[allow(clippy::too_many_arguments)]
    pub fn with_source(
        feed: FeedConfig,
        source: Source,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
        python: &str,
        runtime_dir: &Path,
        fhir_base_url: &str,
        public_url: &str,
    ) -> Self {
        let transformer = Transformer::new(python, runtime_dir, &feed.transform);
        let bulk = (feed.delivery.method == DeliveryMethod::BulkSubmit)
            .then(|| BulkSubmitter::new(fhir_base_url, public_url));
        Self {
            feed,
            source,
            db,
            fhir,
            bulk,
            transformer,
            alerts,
            started_at: now_ms(),
            outage_alerted: AtomicBool::new(false),
        }
    }

    /// When this runner started: the reference for silence until data arrives.
    pub fn started_at(&self) -> i64 {
        self.started_at
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
        if self.bulk.is_some() {
            self.preflight_received()?;
            self.submit_pending().await?;
            self.poll_submissions().await?;
        } else {
            self.transform_received().await?;
            self.deliver_due().await?;
        }
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
            if self.bulk.is_some() && self.db.file_source(original)?.is_some() {
                // The same object, still waiting for HFS to fetch it: leave it alone.
                return Ok(());
            }
            let id = self.db.insert_file(
                feed,
                &name,
                &sha,
                bytes.len() as u64,
                "duplicate",
                Some(original),
                None,
            )?;
            tracing::info!(feed, file = %name, duplicate_of = original, "same content already received; not reprocessed");
            let archived = self
                .source
                .acknowledge(&key, &format!("{id}-{name}"))
                .await?;
            return self.db.mark_file_archived(id, &archived);
        }

        // On the bulk path the object stays at its source until HFS has read it.
        let keep = self.bulk.is_some();
        let file_id = self.db.insert_file(
            feed,
            &name,
            &sha,
            bytes.len() as u64,
            "processing",
            None,
            keep.then_some(key.as_str()),
        )?;
        let readable = match reader::read(self.feed.format, &bytes) {
            Ok(records) => {
                let rows: Vec<_> = records
                    .into_iter()
                    .map(|r| (r.line, r.raw, r.parse_error))
                    .collect();
                self.db.insert_records(file_id, feed, &rows)?;
                tracing::info!(feed, file = %name, records = rows.len(), "received");
                true
            }
            Err(e) => {
                self.db.fail_file(file_id, &format!("{e:#}"))?;
                self.alerts.raise(
                    Some(feed),
                    "unreadable_file",
                    &format!("{}: cannot read {name}: {e:#}", self.feed.name),
                )?;
                false
            }
        };
        if keep && readable {
            return Ok(());
        }
        let archived = self
            .source
            .acknowledge(&key, &format!("{file_id}-{name}"))
            .await?;
        self.db.mark_file_archived(file_id, &archived)
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

    /// The bulk path has no transform: a record is the resource HFS will read. The
    /// engine only predicts what HFS will refuse (no resourceType, no id); the receipt
    /// is what decides.
    fn preflight_received(&self) -> Result<()> {
        loop {
            let items = self.db.received_records(&self.feed.id, TRANSFORM_CHUNK)?;
            if items.is_empty() {
                return Ok(());
            }
            for item in &items {
                match transform::check_resource(&item.raw) {
                    Ok(DeliveryKey::Id { .. }) => self
                        .db
                        .mark_transformed(item.id, &Value::Array(vec![item.raw.clone()]))?,
                    Ok(DeliveryKey::Identifier { resource_type, .. }) => self.db.mark_rejected(
                        item.id,
                        "validation",
                        &format!("{resource_type} has no id; $bulk-submit stores resources by id"),
                        None,
                    )?,
                    Err(msg) => self.db.mark_rejected(item.id, "validation", &msg, None)?,
                }
            }
        }
    }

    /// Opens one submission per drop: the files with pending records, up to the
    /// manifest cap, handed to HFS through a manifest the engine serves.
    async fn submit_pending(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        loop {
            let files = self
                .db
                .submittable_files(&self.feed.id, self.feed.delivery.manifest_objects_max)?;
            if files.is_empty() {
                return Ok(());
            }
            let token = bulk::token();
            let ttl = Duration::from_secs(self.feed.delivery.url_ttl_seconds);
            let mut entries = Vec::new();
            let mut record_ids = Vec::new();
            let mut hashes = Vec::new();
            for file in &files {
                let url = match &file.source_key {
                    Some(key) => match self.source.share(key, ttl).await? {
                        Some(signed) => signed,
                        None => bulk.file_url(&token, file.file_id),
                    },
                    None => bulk.file_url(&token, file.file_id),
                };
                let resource_type = file
                    .records
                    .iter()
                    .map(|(_, rt)| rt.as_str())
                    .find(|rt| !rt.is_empty())
                    .unwrap_or("Resource")
                    .to_string();
                entries.push((resource_type, url, file.records.len()));
                record_ids.extend(file.records.iter().map(|(id, _)| *id));
                hashes.push(file.name.clone());
            }
            let hash_refs: Vec<&str> = hashes.iter().map(String::as_str).collect();
            let submission_id = bulk::submission_id(&self.feed.id, &hash_refs);
            let manifest = bulk.manifest(&entries);
            self.db.create_submission(
                &token,
                &self.feed.id,
                &submission_id,
                &manifest,
                &record_ids,
            )?;
            tracing::info!(feed = %self.feed.id, submission = %submission_id, files = files.len(), records = record_ids.len(), "submission opened");
        }
    }

    /// Moves every open submission one step: kick-off, then status polls until the
    /// receipts arrive, then the records' dispositions and the objects' archive.
    async fn poll_submissions(&self) -> Result<()> {
        let Some(bulk) = &self.bulk else {
            return Ok(());
        };
        for sub in self.db.submissions_due(&self.feed.id)? {
            let step = match sub.status.as_str() {
                "kickoff" => {
                    match bulk
                        .kickoff(
                            &self.feed.id,
                            &sub.submission_id,
                            &bulk.manifest_url(&sub.token),
                        )
                        .await
                    {
                        Ok(()) => match bulk.status_url(&self.feed.id, &sub.submission_id).await {
                            Ok(url) => Ok(Some(("running", Some(url)))),
                            Err(e) => Err(e),
                        },
                        // HFS already holds this drop: a repeat of the same files.
                        Err(SendError::Refused(409, _)) => {
                            match bulk.status_url(&self.feed.id, &sub.submission_id).await {
                                Ok(url) => Ok(Some(("running", Some(url)))),
                                Err(e) => Err(e),
                            }
                        }
                        Err(e) => Err(e),
                    }
                }
                _ => match sub.status_url.as_deref() {
                    None => Ok(Some(("kickoff", None))),
                    Some(url) => match bulk.poll(url).await {
                        Ok(Poll::InProgress) => Ok(None),
                        Ok(Poll::Done { references, errors }) => {
                            self.settle(&sub.token, &references, &errors).await?;
                            Ok(Some(("done", None)))
                        }
                        Err(e) => Err(e),
                    },
                },
            };
            match step {
                Ok(None) => {
                    self.db.update_submission(
                        &sub.token,
                        &sub.status,
                        None,
                        sub.attempts,
                        now_ms() + 2000,
                        None,
                    )?;
                }
                Ok(Some((status, url))) => {
                    self.db
                        .update_submission(&sub.token, status, url.as_deref(), 0, 0, None)?;
                    if status == "done" {
                        self.recovered()?;
                    }
                }
                Err(SendError::Refused(status, body)) => {
                    let reason = format!("HTTP {status}: {}", fhir::outcome_text(&body));
                    tracing::warn!(feed = %self.feed.id, submission = %sub.submission_id, "refused: {reason}");
                    for (record_id, _, _) in self.db.submitted_records(&sub.token)? {
                        self.db
                            .mark_rejected(record_id, "server", &reason, Some(&body))?;
                    }
                    self.db.update_submission(
                        &sub.token,
                        "failed",
                        None,
                        sub.attempts,
                        0,
                        Some(&reason),
                    )?;
                    self.archive_submission(&sub.token).await?;
                }
                Err(SendError::Transient(reason)) => {
                    let attempts = sub.attempts + 1;
                    let d = &self.feed.delivery;
                    let backoff = d
                        .initial_backoff_ms
                        .saturating_mul(1u64 << (attempts - 1).min(20))
                        .min(d.max_backoff_ms);
                    self.db.update_submission(
                        &sub.token,
                        &sub.status,
                        None,
                        attempts,
                        now_ms() + backoff as i64,
                        Some(&reason),
                    )?;
                    self.outage(attempts, &reason)?;
                }
            }
        }
        Ok(())
    }

    /// Gives every submitted record its disposition from the receipts, then archives
    /// the objects HFS has read.
    async fn settle(
        &self,
        token: &str,
        references: &std::collections::HashSet<String>,
        errors: &[String],
    ) -> Result<()> {
        let hint = match errors.len() {
            0 => String::new(),
            n => format!(" HFS reported {n} error(s); the first: {}", errors[0]),
        };
        for (record_id, _, resources) in self.db.submitted_records(token)? {
            let reference = resources
                .get(0)
                .and_then(|r| transform::check_resource(r).ok())
                .map(|key| fhir::request_url(&key));
            match reference {
                Some(r) if references.contains(&r) => {
                    self.db
                        .mark_sent(record_id, &json!([{"reference": r, "via": "$bulk-submit"}]))?;
                }
                _ => self.db.mark_rejected(
                    record_id,
                    "server",
                    &format!("no receipt from HFS for this resource.{hint}"),
                    None,
                )?,
            }
        }
        self.archive_submission(token).await
    }

    async fn archive_submission(&self, token: &str) -> Result<()> {
        let file_ids: Vec<i64> = {
            let rows = self.db.query_json(
                "SELECT DISTINCT file_id FROM records WHERE submission = ?1",
                rusqlite::params![token],
            )?;
            rows.iter().filter_map(|r| r["file_id"].as_i64()).collect()
        };
        for file_id in file_ids {
            if let Some((key, name)) = self.db.file_source(file_id)? {
                let archived = self
                    .source
                    .acknowledge(&key, &format!("{file_id}-{name}"))
                    .await?;
                self.db.mark_file_archived(file_id, &archived)?;
            }
        }
        Ok(())
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
                self.outage(worst, &reason)?;
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
                self.recovered()?;
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
            // The rules say whether a few rejected rows are worth an alert; without
            // one, every rejection is.
            let share_rules = self.rules("rejected_share")?;
            if f.newly_rejected > 0 && share_rules.is_empty() {
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
            // "More than N% of the rows in a load fail": fires on this load, clears on
            // the next load within the limit.
            let share = if f.total > 0 {
                f.rejected as f64 * 100.0 / f.total as f64
            } else {
                0.0
            };
            for rule in share_rules {
                let limit: f64 = rule.threshold.trim_end_matches('%').parse().unwrap_or(1.0);
                if share > limit {
                    self.alerts.fire_once(
                        &self.feed.id,
                        "rejected_share",
                        &rule,
                        &format!(
                            "{}: {share:.1}% of the rows in {} failed, more than the {limit}% allowed ({} of {})",
                            self.feed.name, f.name, f.rejected, f.total
                        ),
                    )?;
                } else {
                    self.alerts.clear_open(
                        &self.feed.id,
                        "rejected_share",
                        Some(rule.id),
                        &format!(
                            "{}: {} loaded with {share:.1}% failed, within the limit",
                            self.feed.name, f.name
                        ),
                    )?;
                }
            }
        }
        Ok(())
    }

    fn rules(&self, condition: &str) -> Result<Vec<Rule>> {
        Ok(self
            .db
            .rules_for(&self.feed.id)?
            .into_iter()
            .filter(|r| r.condition == condition)
            .collect())
    }

    /// Delivery failed `attempts` times in a row. The unreachable rules say when to
    /// tell someone; without one, the source's `alert_after_attempts` does.
    fn outage(&self, attempts: u32, reason: &str) -> Result<()> {
        let message = format!(
            "{}: cannot deliver to HFS after {attempts} attempts ({reason}); data is kept and retried",
            self.feed.name
        );
        let rules = self.rules("unreachable")?;
        if rules.is_empty() {
            if attempts >= self.feed.delivery.alert_after_attempts
                && !self.outage_alerted.swap(true, Ordering::SeqCst)
            {
                self.alerts
                    .raise(Some(&self.feed.id), "delivery", &message)?;
            }
            return Ok(());
        }
        for rule in &rules {
            if attempts >= rule.threshold.parse().unwrap_or(5) {
                self.alerts
                    .fire_once(&self.feed.id, "delivery", rule, &message)?;
            }
        }
        Ok(())
    }

    fn recovered(&self) -> Result<()> {
        let message = format!("{}: delivery to HFS recovered", self.feed.name);
        if self.outage_alerted.swap(false, Ordering::SeqCst) {
            self.alerts
                .raise(Some(&self.feed.id), "delivery_recovered", &message)?;
        }
        for rule in self.rules("unreachable")? {
            self.alerts
                .clear_open(&self.feed.id, "delivery", Some(rule.id), &message)?;
        }
        Ok(())
    }

    /// The rules that watch time rather than loads: silence, and a file expected by
    /// a time of day. Each fires once and clears itself when data arrives.
    fn check_silence(&self) -> Result<()> {
        let last = self.db.last_received_at(&self.feed.id)?;
        let since = last.unwrap_or(self.started_at);
        let now = now_ms();
        for rule in self.db.rules_for(&self.feed.id)? {
            match rule.condition.as_str() {
                "silence" => {
                    let Ok(window) = parse_duration(&rule.threshold) else {
                        continue;
                    };
                    let silent_for = now - since;
                    if silent_for > window.as_millis() as i64 {
                        self.alerts.fire_once(
                            &self.feed.id,
                            "silence",
                            &rule,
                            &format!(
                                "{}: nothing received for {} (expected at least every {})",
                                self.feed.name,
                                human(Duration::from_millis(silent_for as u64)),
                                human(window)
                            ),
                        )?;
                    } else {
                        self.alerts.clear_open(
                            &self.feed.id,
                            "silence",
                            Some(rule.id),
                            &format!("{}: data arrived", self.feed.name),
                        )?;
                    }
                }
                "missing_by" => {
                    let Some(due_minute) = parse_clock(&rule.threshold) else {
                        continue;
                    };
                    let now_utc = chrono::Utc::now();
                    let today = now_utc
                        .date_naive()
                        .and_hms_opt(0, 0, 0)
                        .expect("midnight")
                        .and_utc()
                        .timestamp_millis();
                    let received_today = last.is_some_and(|t| t >= today);
                    let due = now_utc.hour() * 60 + now_utc.minute() >= due_minute;
                    if received_today {
                        self.alerts.clear_open(
                            &self.feed.id,
                            "missing",
                            Some(rule.id),
                            &format!("{}: the file arrived", self.feed.name),
                        )?;
                    } else if due {
                        self.alerts.fire_once(
                            &self.feed.id,
                            "missing",
                            &rule,
                            &format!(
                                "{}: nothing has arrived today and the file was expected by {} UTC",
                                self.feed.name, rule.threshold
                            ),
                        )?;
                    }
                }
                _ => {}
            }
        }
        Ok(())
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
