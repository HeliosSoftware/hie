//! Sources as resources: created, replaced, paused and tested through the API,
//! persisted as the YAML files `config::load` reads, and run without a restart.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};

use anyhow::{Context, Result};
use serde_json::Value;

use crate::alerts::Alerter;
use crate::config::{Config, FeedConfig, SourceConfig};
use crate::db::Db;
use crate::fhir::FhirClient;
use crate::pipeline::{FeedRunner, RunnerControl};
use crate::source::{Probe, Source};

#[derive(Debug)]
pub enum ManagerError {
    Invalid(String),
    NotFound(String),
    Conflict(String),
    Internal(anyhow::Error),
}

impl From<anyhow::Error> for ManagerError {
    fn from(e: anyhow::Error) -> Self {
        ManagerError::Internal(e)
    }
}

struct Managed {
    feed: FeedConfig,
    runner: Arc<FeedRunner>,
    control: Arc<RunnerControl>,
}

pub struct SourceManager {
    python: String,
    runtime_dir: PathBuf,
    sources_dir: PathBuf,
    base_dir: PathBuf,
    db: Arc<Db>,
    fhir: Arc<FhirClient>,
    alerts: Arc<Alerter>,
    sources: Mutex<BTreeMap<String, Managed>>,
}

impl SourceManager {
    pub fn new(
        config: &Config,
        runtime_dir: &Path,
        db: Arc<Db>,
        fhir: Arc<FhirClient>,
        alerts: Arc<Alerter>,
    ) -> Arc<Self> {
        Arc::new(Self {
            python: config.python.clone(),
            runtime_dir: runtime_dir.to_path_buf(),
            sources_dir: config.sources_dir.clone(),
            base_dir: config.base_dir.clone(),
            db,
            fhir,
            alerts,
            sources: Mutex::new(BTreeMap::new()),
        })
    }

    /// Builds and spawns the runner for a resolved definition, replacing a running one.
    pub fn start(&self, feed: FeedConfig) -> Result<()> {
        let runner = Arc::new(FeedRunner::new(
            feed.clone(),
            self.db.clone(),
            self.fhir.clone(),
            self.alerts.clone(),
            &self.python,
            &self.runtime_dir,
        )?);
        let control = Arc::new(RunnerControl::default());
        tokio::spawn(runner.clone().run(control.clone()));
        let previous = self.sources.lock().expect("manager mutex").insert(
            feed.id.clone(),
            Managed {
                feed,
                runner,
                control,
            },
        );
        if let Some(p) = previous {
            Self::halt(&p);
        }
        Ok(())
    }

    fn halt(managed: &Managed) {
        managed.control.stopped.store(true, Ordering::Release);
        managed.control.poke.notify_one();
    }

    pub fn start_all(&self, feeds: Vec<FeedConfig>) -> Result<()> {
        for feed in feeds {
            self.start(feed)?;
        }
        Ok(())
    }

    pub fn sources(&self) -> Vec<FeedConfig> {
        self.sources
            .lock()
            .expect("manager mutex")
            .values()
            .map(|m| m.feed.clone())
            .collect()
    }

    pub fn source(&self, id: &str) -> Option<FeedConfig> {
        self.sources
            .lock()
            .expect("manager mutex")
            .get(id)
            .map(|m| m.feed.clone())
    }

    pub fn runner(&self, id: &str) -> Option<Arc<FeedRunner>> {
        self.sources
            .lock()
            .expect("manager mutex")
            .get(id)
            .map(|m| m.runner.clone())
    }

    fn file_for(&self, id: &str) -> PathBuf {
        self.sources_dir.join(format!("{id}.yaml"))
    }

    /// The source's definition as its file holds it: paths as written, nothing resolved.
    pub fn definition(&self, id: &str) -> Result<Option<Value>> {
        let path = self.file_for(id);
        if !path.exists() {
            return Ok(None);
        }
        let text = std::fs::read_to_string(&path)
            .with_context(|| format!("reading {}", path.display()))?;
        let yaml: serde_yaml::Value = serde_yaml::from_str(&text)?;
        Ok(Some(serde_json::to_value(yaml)?))
    }

    /// Parses a submitted definition, validates it against the base directory and
    /// returns both the form to write (as submitted) and the resolved form to run.
    fn parse(&self, body: &Value) -> Result<(FeedConfig, FeedConfig), ManagerError> {
        let submitted: FeedConfig = serde_json::from_value(body.clone())
            .map_err(|e| ManagerError::Invalid(e.to_string()))?;
        let mut resolved = submitted.clone();
        resolved
            .resolve(&self.base_dir)
            .map_err(|e| ManagerError::Invalid(format!("{e:#}")))?;
        Source::from_config(&resolved).map_err(|e| ManagerError::Invalid(format!("{e:#}")))?;
        Ok((submitted, resolved))
    }

    fn write(&self, submitted: &FeedConfig) -> Result<()> {
        std::fs::create_dir_all(&self.sources_dir)?;
        let path = self.file_for(&submitted.id);
        std::fs::write(&path, submitted.to_yaml()?)
            .with_context(|| format!("writing {}", path.display()))
    }

    pub fn create(&self, body: &Value) -> Result<FeedConfig, ManagerError> {
        let (submitted, resolved) = self.parse(body)?;
        if self.source(&submitted.id).is_some() || self.file_for(&submitted.id).exists() {
            return Err(ManagerError::Conflict(format!(
                "source {} already exists",
                submitted.id
            )));
        }
        self.write(&submitted)?;
        self.start(resolved.clone())?;
        Ok(resolved)
    }

    pub fn replace(&self, id: &str, body: &Value) -> Result<FeedConfig, ManagerError> {
        if self.source(id).is_none() {
            return Err(ManagerError::NotFound(format!("source {id} not found")));
        }
        let (submitted, resolved) = self.parse(body)?;
        if submitted.id != id {
            return Err(ManagerError::Invalid(format!(
                "the body's id {:?} does not match {id:?}",
                submitted.id
            )));
        }
        self.write(&submitted)?;
        self.start(resolved.clone())?;
        Ok(resolved)
    }

    /// Stops the source and removes its definition. Its records and alerts stay.
    pub fn delete(&self, id: &str) -> Result<(), ManagerError> {
        let removed = self.sources.lock().expect("manager mutex").remove(id);
        let Some(managed) = removed else {
            return Err(ManagerError::NotFound(format!("source {id} not found")));
        };
        Self::halt(&managed);
        let path = self.file_for(id);
        if path.exists() {
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        }
        Ok(())
    }

    pub fn pause(&self, id: &str, by: Option<&str>) -> Result<(), ManagerError> {
        self.require(id)?;
        self.db.set_paused(id, true, by)?;
        Ok(())
    }

    pub fn resume(&self, id: &str) -> Result<(), ManagerError> {
        self.require(id)?;
        self.db.set_paused(id, false, None)?;
        self.check_now(id)
    }

    /// Makes the source pass now instead of at its next poll.
    pub fn check_now(&self, id: &str) -> Result<(), ManagerError> {
        let sources = self.sources.lock().expect("manager mutex");
        let managed = sources
            .get(id)
            .ok_or_else(|| ManagerError::NotFound(format!("source {id} not found")))?;
        managed.control.poke.notify_one();
        Ok(())
    }

    fn require(&self, id: &str) -> Result<(), ManagerError> {
        if self.source(id).is_none() {
            return Err(ManagerError::NotFound(format!("source {id} not found")));
        }
        Ok(())
    }

    /// Tests a connection before a source exists: the body is a full definition or
    /// just `{"source": {...}}`. Nothing is taken from the source.
    pub async fn test(&self, body: &Value) -> Result<Probe, ManagerError> {
        let source_value = body.get("source").unwrap_or(body);
        let mut source: SourceConfig = serde_json::from_value(source_value.clone())
            .map_err(|e| ManagerError::Invalid(e.to_string()))?;
        source.resolve(&self.base_dir);
        let feed = FeedConfig {
            id: "test".into(),
            name: "test".into(),
            source,
            format: Default::default(),
            transform: PathBuf::new(),
            delivery: Default::default(),
            expect_every_text: None,
            expect_every: None,
        };
        let source =
            Source::from_config(&feed).map_err(|e| ManagerError::Invalid(format!("{e:#}")))?;
        match source.probe().await {
            Ok(probe) => Ok(probe),
            Err(e) => Ok(Probe {
                reachable: false,
                elapsed_ms: 0,
                objects: 0,
                newest: None,
                newest_at: Some(format!("{e:#}")),
            }),
        }
    }
}
