//! Helios Integration Engine: gets data from source feeds into HFS,
//! with per-record traceability, idempotent delivery, retries and alerts.

pub mod alerts;
pub mod api;
pub mod config;
pub mod db;
pub mod fhir;
pub mod pipeline;
pub mod reader;
pub mod source;
pub mod transform;

use std::path::Path;
use std::sync::Arc;

use anyhow::Result;

/// Builds one runner per feed plus the API state, sharing a database and HFS client.
pub fn build(
    config: &config::Config,
    feeds: Vec<config::FeedConfig>,
    runtime_dir: &Path,
) -> Result<(Vec<Arc<pipeline::FeedRunner>>, api::AppState)> {
    let db = Arc::new(db::Db::open(&config.database)?);
    let fhir = Arc::new(fhir::FhirClient::new(&config.fhir_base_url));
    let alerts = Arc::new(alerts::Alerter::new(
        db.clone(),
        config.alerts.webhook_url.clone(),
    ));
    let mut runners = Vec::with_capacity(feeds.len());
    for feed in feeds.iter().cloned() {
        runners.push(Arc::new(pipeline::FeedRunner::new(
            feed,
            db.clone(),
            fhir.clone(),
            alerts.clone(),
            &config.python,
            runtime_dir,
        )?));
    }
    Ok((
        runners,
        api::AppState {
            db,
            feeds: Arc::new(feeds),
        },
    ))
}
