//! The shared Python runtime as a transform backend (`runtime: <url>` in
//! `hie.yaml`): python-runtime's HIE facade stores a transform's code under a
//! name (`PUT /transforms/{name}`), runs it over a batch of requests
//! (`POST /transforms/{name}/run`) and runs inline code once
//! (`POST /transforms/try`). Replies are the worker's own lines, one per
//! request in order, so what comes back is parsed exactly as the local
//! worker's output.

use std::path::Path;
use std::sync::Mutex;
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use reqwest::StatusCode;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

/// The facade's own ceiling on requests per call.
pub const MAX_REQUESTS_PER_CALL: usize = 1000;
/// The facade's ceiling on requests per try.
pub const MAX_TRY_REQUESTS: usize = 100;
/// Longer than the facade's run budget (120 s by default), so a slow batch is
/// answered rather than cut on this side.
const CALL_TIMEOUT: Duration = Duration::from_secs(180);

#[derive(Debug, Clone)]
pub struct RuntimeSettings {
    pub url: String,
    pub token: Option<String>,
    /// Requests per run call; the facade partitions them further itself.
    pub batch_size: usize,
}

/// One transform as the runtime knows it.
pub struct RemoteRuntime {
    settings: RuntimeSettings,
    client: reqwest::Client,
    name: String,
    /// The sha256 of the code the runtime holds under `name`, once stored.
    stored: Mutex<Option<String>>,
}

impl RemoteRuntime {
    pub fn new(settings: RuntimeSettings, transform_script: &Path) -> Self {
        let client = reqwest::Client::builder()
            .timeout(CALL_TIMEOUT)
            .user_agent(concat!("hie/", env!("CARGO_PKG_VERSION")))
            .build()
            .expect("reqwest client");
        Self {
            name: transform_name(transform_script),
            settings,
            client,
            stored: Mutex::new(None),
        }
    }

    /// The name the transform is stored under at the runtime.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Runs the stored transform over `requests` (record, context pairs), storing
    /// its code first when the runtime does not hold this version. One reply per
    /// request, in order.
    pub async fn run(&self, script: &Path, requests: &[(Value, Value)]) -> Result<Vec<Value>> {
        let code = std::fs::read_to_string(script)
            .with_context(|| format!("reading {}", script.display()))?;
        self.ensure_stored(&code).await?;
        let mut replies = Vec::with_capacity(requests.len());
        let per_call = self.settings.batch_size.clamp(1, MAX_REQUESTS_PER_CALL);
        for chunk in requests.chunks(per_call) {
            let body = envelope(chunk, None);
            let url = format!("{}/transforms/{}/run", self.base(), self.name);
            let mut answer = self.post(&url, &body).await?;
            if answer.0 == StatusCode::NOT_FOUND && error_code(&answer.1) == "transform_unknown" {
                // The runtime lost or never had it (a restart without its store):
                // stored again, run again.
                *self.stored.lock().expect("stored") = None;
                self.ensure_stored(&code).await?;
                answer = self.post(&url, &body).await?;
            }
            replies.extend(Self::replies(answer, chunk.len())?);
        }
        Ok(replies)
    }

    /// Runs `code` inline over at most [`MAX_TRY_REQUESTS`] requests; nothing is stored.
    pub async fn try_code(&self, code: &str, requests: &[(Value, Value)]) -> Result<Vec<Value>> {
        if requests.len() > MAX_TRY_REQUESTS {
            bail!("a try takes at most {MAX_TRY_REQUESTS} records");
        }
        let url = format!("{}/transforms/try", self.base());
        let answer = self.post(&url, &envelope(requests, Some(code))).await?;
        Self::replies(answer, requests.len())
    }

    fn base(&self) -> &str {
        self.settings.url.trim_end_matches('/')
    }

    async fn ensure_stored(&self, code: &str) -> Result<()> {
        let sha = hex::encode(Sha256::digest(code.as_bytes()));
        if self.stored.lock().expect("stored").as_deref() == Some(sha.as_str()) {
            return Ok(());
        }
        let url = format!("{}/transforms/{}", self.base(), self.name);
        let (status, body) = self
            .send(self.client.put(&url), &json!({"code": code}))
            .await?;
        if !(status == StatusCode::OK || status == StatusCode::CREATED) {
            bail!(
                "the runtime did not store transform {}: {}",
                self.name,
                describe(status, &body)
            );
        }
        let theirs = body
            .get("sha256")
            .and_then(Value::as_str)
            .unwrap_or_default();
        if theirs != sha {
            bail!(
                "the runtime stored transform {} with sha256 {theirs}, expected {sha}",
                self.name
            );
        }
        *self.stored.lock().expect("stored") = Some(sha);
        Ok(())
    }

    async fn post(&self, url: &str, body: &Value) -> Result<(StatusCode, Value)> {
        self.send(self.client.post(url), body).await
    }

    async fn send(
        &self,
        request: reqwest::RequestBuilder,
        body: &Value,
    ) -> Result<(StatusCode, Value)> {
        let mut request = request.json(body);
        if let Some(token) = &self.settings.token {
            request = request.bearer_auth(token);
        }
        let response = request.send().await.map_err(|e| {
            anyhow!(
                "the runtime at {} cannot be reached: {e}",
                self.settings.url
            )
        })?;
        let status = response.status();
        let text = response.text().await.unwrap_or_default();
        let body = serde_json::from_str(&text).unwrap_or(Value::Null);
        Ok((status, body))
    }

    fn replies((status, body): (StatusCode, Value), expected: usize) -> Result<Vec<Value>> {
        if status != StatusCode::OK {
            bail!(
                "the runtime did not run the batch: {}",
                describe(status, &body)
            );
        }
        let replies = body
            .get("replies")
            .and_then(Value::as_array)
            .cloned()
            .ok_or_else(|| anyhow!("the runtime answered without replies"))?;
        if replies.len() != expected {
            bail!(
                "the runtime answered {} replies for {expected} requests",
                replies.len()
            );
        }
        Ok(replies)
    }
}

fn envelope(requests: &[(Value, Value)], code: Option<&str>) -> Value {
    let requests: Vec<Value> = requests
        .iter()
        .enumerate()
        .map(|(i, (record, context))| json!({"id": i, "record": record, "context": context}))
        .collect();
    match code {
        Some(code) => json!({"code": code, "requests": requests}),
        None => json!({"requests": requests}),
    }
}

fn error_code(body: &Value) -> &str {
    body.get("error")
        .and_then(Value::as_str)
        .unwrap_or_default()
}

/// What a refusal means for the operator, by the facade's status mapping.
fn describe(status: StatusCode, body: &Value) -> String {
    let code = error_code(body);
    match status.as_u16() {
        422 => {
            let detail = body
                .get("load_error")
                .and_then(|e| e.get("message"))
                .and_then(Value::as_str)
                .unwrap_or("no detail");
            format!("the transform failed to load ({detail})")
        }
        503 => {
            format!("the runtime is busy or not ready ({code}); the records wait for the next pass")
        }
        408 | 500 | 502 => format!("HTTP {status} {code}; the records wait for the next pass"),
        401 | 403 => format!("HTTP {status} {code}; check the runtime token and consumer profile"),
        400 | 405 | 409 | 413 | 415 => {
            format!("HTTP {status} {code}; check the runtime configuration")
        }
        _ => format!("HTTP {status} {code}"),
    }
}

/// The name a transform file is stored under: its stem, with anything outside
/// `[A-Za-z0-9_-]` replaced, at most 64 characters, never the reserved `try`.
pub fn transform_name(script: &Path) -> String {
    let stem = script
        .file_stem()
        .and_then(|s| s.to_str())
        .unwrap_or("transform");
    let mut name: String = stem
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
                c
            } else {
                '_'
            }
        })
        .take(64)
        .collect();
    if name.is_empty() {
        name = "transform".into();
    }
    if name == "try" {
        name = "try_".into();
    }
    name
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_follow_the_facade_rules() {
        assert_eq!(
            transform_name(Path::new("transforms/lab_results.py")),
            "lab_results"
        );
        assert_eq!(transform_name(Path::new("a b.c.py")), "a_b_c");
        assert_eq!(transform_name(Path::new("try.py")), "try_");
        assert_eq!(transform_name(Path::new(&"x".repeat(80))).len(), 64);
    }
}
