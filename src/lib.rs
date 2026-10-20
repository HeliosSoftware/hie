//! Helios Integration Engine: gets data from source feeds into HFS,
//! with per-record traceability, idempotent delivery, retries and alerts.

pub mod alerts;
pub mod api;
pub mod config;
pub mod db;
pub mod fhir;
pub mod manager;
pub mod pipeline;
pub mod reader;
pub mod source;
pub mod transform;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

/// Opens the store and the HFS client, starts every configured source, and returns
/// the manager (which owns the runners) and the API state.
pub fn build(
    config: &config::Config,
    feeds: Vec<config::FeedConfig>,
    runtime_dir: &Path,
) -> Result<(Arc<manager::SourceManager>, api::AppState)> {
    let db = Arc::new(db::Db::open(&config.database)?);
    let fhir = Arc::new(fhir::FhirClient::new(&config.fhir_base_url));
    let alerts = Arc::new(alerts::Alerter::new(
        db.clone(),
        config.alerts.webhook_url.clone(),
    ));
    let manager = manager::SourceManager::new(config, runtime_dir, db.clone(), fhir, alerts);
    manager.start_all(feeds)?;
    Ok((manager.clone(), api::AppState { db, manager }))
}
