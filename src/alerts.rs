use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use serde_json::json;

use crate::db::Db;

/// Records every alert and, when configured, posts it to a webhook. The payload's
/// `text` field is what Slack and Teams incoming webhooks display.
pub struct Alerter {
    db: Arc<Db>,
    webhook_url: Option<String>,
    http: reqwest::Client,
}

impl Alerter {
    pub fn new(db: Arc<Db>, webhook_url: Option<String>) -> Self {
        let http = reqwest::Client::builder()
            .timeout(Duration::from_secs(10))
            .build()
            .expect("HTTP client");
        Self {
            db,
            webhook_url,
            http,
        }
    }

    pub fn raise(&self, feed: Option<&str>, kind: &str, message: &str) -> Result<()> {
        let id = self.db.insert_alert(feed, kind, message)?;
        tracing::warn!(alert = id, feed, kind, "{message}");
        if let Some(url) = self.webhook_url.clone() {
            let http = self.http.clone();
            let payload = json!({"text": format!("[HIE] {message}"), "feed": feed, "kind": kind, "alert_id": id});
            tokio::spawn(async move {
                if let Err(e) = http
                    .post(&url)
                    .json(&payload)
                    .send()
                    .await
                    .and_then(|r| r.error_for_status())
                {
                    tracing::error!("alert webhook failed: {e}");
                }
            });
        }
        Ok(())
    }
}
