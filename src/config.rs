use std::collections::HashSet;
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    pub fhir_base_url: String,
    #[serde(default = "default_database")]
    pub database: PathBuf,
    #[serde(default = "default_listen")]
    pub listen: String,
    #[serde(default = "default_python")]
    pub python: String,
    /// One YAML file per source. The engine writes here when a source is created
    /// through the API, so the definitions stay files under version control.
    #[serde(default = "default_sources_dir", alias = "feeds_dir")]
    pub sources_dir: PathBuf,
    #[serde(default)]
    pub alerts: AlertConfig,
    /// The directory holding `hie.yaml`; relative paths in source files resolve against it.
    #[serde(skip)]
    pub base_dir: PathBuf,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AlertConfig {
    pub webhook_url: Option<String>,
}

/// A source: one connection, its format, its transform, how it is delivered and
/// how often data is expected. Serialized as the source's YAML file.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedConfig {
    pub id: String,
    pub name: String,
    pub source: SourceConfig,
    #[serde(default)]
    pub format: Format,
    pub transform: PathBuf,
    #[serde(default)]
    pub delivery: DeliveryConfig,
    #[serde(
        default,
        rename = "expect_every",
        skip_serializing_if = "Option::is_none"
    )]
    pub(crate) expect_every_text: Option<String>,
    #[serde(skip)]
    pub expect_every: Option<Duration>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SourceConfig {
    Folder {
        path: PathBuf,
        #[serde(default = "default_pattern")]
        pattern: String,
        #[serde(default = "default_poll_seconds")]
        poll_seconds: u64,
        #[serde(default = "default_min_file_age_ms")]
        min_file_age_ms: u64,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        archive: Option<PathBuf>,
    },
    /// An S3 bucket. Credentials come from the usual AWS environment
    /// (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION`, profiles).
    S3 {
        bucket: String,
        #[serde(default)]
        prefix: String,
        #[serde(default, skip_serializing_if = "Option::is_none")]
        region: Option<String>,
        /// For S3-compatible stores such as MinIO.
        #[serde(default, skip_serializing_if = "Option::is_none")]
        endpoint: Option<String>,
        #[serde(default = "default_pattern")]
        pattern: String,
        #[serde(default = "default_poll_seconds")]
        poll_seconds: u64,
        #[serde(default = "default_min_file_age_ms")]
        min_file_age_ms: u64,
        #[serde(default = "default_archive_prefix")]
        archive_prefix: String,
    },
}

#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum Format {
    #[default]
    Csv,
    Ndjson,
    FhirBundle,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeliveryConfig {
    #[serde(default = "default_batch_size")]
    pub batch_size: usize,
    #[serde(default = "default_initial_backoff_ms")]
    pub initial_backoff_ms: u64,
    #[serde(default = "default_max_backoff_ms")]
    pub max_backoff_ms: u64,
    #[serde(default = "default_alert_after_attempts")]
    pub alert_after_attempts: u32,
}

impl Default for DeliveryConfig {
    fn default() -> Self {
        Self {
            batch_size: default_batch_size(),
            initial_backoff_ms: default_initial_backoff_ms(),
            max_backoff_ms: default_max_backoff_ms(),
            alert_after_attempts: default_alert_after_attempts(),
        }
    }
}

fn default_database() -> PathBuf {
    PathBuf::from("data/hie.db")
}
fn default_listen() -> String {
    "127.0.0.1:8090".into()
}
fn default_python() -> String {
    if cfg!(windows) {
        "py".into()
    } else {
        "python3".into()
    }
}
fn default_sources_dir() -> PathBuf {
    PathBuf::from("sources")
}
fn default_pattern() -> String {
    "*".into()
}
fn default_poll_seconds() -> u64 {
    5
}
fn default_min_file_age_ms() -> u64 {
    1000
}
fn default_archive_prefix() -> String {
    "processed".into()
}
fn default_batch_size() -> usize {
    100
}
fn default_initial_backoff_ms() -> u64 {
    2000
}
fn default_max_backoff_ms() -> u64 {
    300_000
}
fn default_alert_after_attempts() -> u32 {
    5
}

impl SourceConfig {
    /// Where the source reads from, for logs and the API.
    pub fn describe(&self) -> String {
        match self {
            SourceConfig::Folder { path, .. } => path.display().to_string(),
            SourceConfig::S3 { bucket, prefix, .. } => format!("s3://{bucket}/{prefix}"),
        }
    }

    pub fn poll_interval(&self) -> Duration {
        match self {
            SourceConfig::Folder { poll_seconds, .. } | SourceConfig::S3 { poll_seconds, .. } => {
                Duration::from_secs(*poll_seconds)
            }
        }
    }

    /// Makes a folder's paths absolute against the directory holding `hie.yaml`.
    pub fn resolve(&mut self, base: &Path) {
        if let SourceConfig::Folder { path, archive, .. } = self {
            *path = base.join(&*path);
            if let Some(a) = archive {
                *a = base.join(&*a);
            }
        }
    }
}

impl FeedConfig {
    /// Makes the paths absolute, checks the transform exists and parses `expect_every`.
    pub fn resolve(&mut self, base: &Path) -> Result<()> {
        if self.id.is_empty()
            || !self
                .id
                .chars()
                .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '-')
        {
            bail!(
                "source id {:?} must be lowercase letters, digits and dashes",
                self.id
            );
        }
        if self.name.trim().is_empty() {
            bail!("source {}: name is required", self.id);
        }
        self.source.resolve(base);
        self.transform = base.join(&self.transform);
        if !self.transform.is_file() {
            bail!(
                "source {}: transform {} not found",
                self.id,
                self.transform.display()
            );
        }
        self.expect_every = self
            .expect_every_text
            .as_deref()
            .map(parse_duration)
            .transpose()
            .with_context(|| format!("source {}: expect_every", self.id))?;
        Ok(())
    }

    /// The source's YAML file, as submitted (paths as given, not resolved).
    pub fn to_yaml(&self) -> Result<String> {
        Ok(serde_yaml::to_string(self)?)
    }
}

/// Loads `hie.yaml` and every source definition in its `sources_dir`.
/// Relative paths resolve against the directory holding `hie.yaml`.
pub fn load(path: &Path) -> Result<(Config, Vec<FeedConfig>)> {
    let text =
        std::fs::read_to_string(path).with_context(|| format!("reading {}", path.display()))?;
    let mut config: Config =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", path.display()))?;
    let base = path.parent().map(Path::to_path_buf).unwrap_or_default();
    config.base_dir = base.clone();
    config.database = base.join(&config.database);
    config.sources_dir = base.join(&config.sources_dir);
    std::fs::create_dir_all(&config.sources_dir)
        .with_context(|| format!("creating {}", config.sources_dir.display()))?;

    let mut feeds = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(&config.sources_dir)
        .with_context(|| format!("reading {}", config.sources_dir.display()))?
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| matches!(p.extension().and_then(|e| e.to_str()), Some("yaml" | "yml")))
        .collect();
    entries.sort();
    let mut ids = HashSet::new();
    for feed_path in entries {
        let text = std::fs::read_to_string(&feed_path)?;
        let mut feed: FeedConfig = serde_yaml::from_str(&text)
            .with_context(|| format!("parsing {}", feed_path.display()))?;
        feed.resolve(&base)?;
        if !ids.insert(feed.id.clone()) {
            bail!("duplicate source id {}", feed.id);
        }
        feeds.push(feed);
    }
    Ok((config, feeds))
}

/// Parses `90s`, `30m`, `24h` or `7d`.
pub fn parse_duration(text: &str) -> Result<Duration> {
    let text = text.trim();
    let (number, unit) = text.split_at(
        text.find(|c: char| !c.is_ascii_digit())
            .unwrap_or(text.len()),
    );
    let n: u64 = number
        .parse()
        .with_context(|| format!("invalid duration {text:?}"))?;
    let secs = match unit {
        "s" => n,
        "m" => n * 60,
        "h" => n * 3600,
        "d" => n * 86_400,
        _ => bail!("invalid duration unit in {text:?} (use s, m, h or d)"),
    };
    Ok(Duration::from_secs(secs))
}

/// Matches a file name against a pattern with at most one `*`, ignoring case.
pub fn glob_match(pattern: &str, name: &str) -> bool {
    match pattern.split_once('*') {
        None => pattern.eq_ignore_ascii_case(name),
        Some((prefix, suffix)) => {
            let (n, p, s) = (
                name.to_ascii_lowercase(),
                prefix.to_ascii_lowercase(),
                suffix.to_ascii_lowercase(),
            );
            n.len() >= p.len() + s.len() && n.starts_with(&p) && n.ends_with(&s)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn durations_parse_with_units() {
        assert_eq!(parse_duration("90s").unwrap(), Duration::from_secs(90));
        assert_eq!(parse_duration("24h").unwrap(), Duration::from_secs(86_400));
        assert!(parse_duration("5 weeks").is_err());
    }

    #[test]
    fn patterns_match_by_prefix_and_suffix() {
        assert!(glob_match("*.csv", "labs.CSV"));
        assert!(glob_match("lab_*.csv", "lab_2026-10-18.csv"));
        assert!(!glob_match("*.csv", "labs.csv.tmp"));
        assert!(glob_match("*", "anything"));
    }

    #[test]
    fn an_s3_source_parses_with_defaults_and_round_trips() {
        let feed: FeedConfig = serde_yaml::from_str(
            "id: x\nname: X\nsource:\n  type: s3\n  bucket: lab\n  prefix: results/\nformat: ndjson\ntransform: t.py\nexpect_every: 24h\n",
        )
        .unwrap();
        assert_eq!(feed.format, Format::Ndjson);
        assert_eq!(feed.source.describe(), "s3://lab/results/");
        let yaml = feed.to_yaml().unwrap();
        let again: FeedConfig = serde_yaml::from_str(&yaml).unwrap();
        assert_eq!(again.source.describe(), "s3://lab/results/");
        assert!(yaml.contains("expect_every: 24h"));
        assert!(!yaml.contains("region"));
        match feed.source {
            SourceConfig::S3 {
                pattern,
                archive_prefix,
                ..
            } => assert_eq!(
                (pattern.as_str(), archive_prefix.as_str()),
                ("*", "processed")
            ),
            _ => panic!("expected s3"),
        }
    }

    #[test]
    fn source_ids_are_checked_on_resolve() {
        let dir = tempfile::tempdir().unwrap();
        std::fs::write(dir.path().join("t.py"), "def transform(r, c): return r\n").unwrap();
        let mut feed: FeedConfig = serde_yaml::from_str(
            "id: Bad Id\nname: X\nsource:\n  type: folder\n  path: inbox\ntransform: t.py\n",
        )
        .unwrap();
        assert!(
            feed.resolve(dir.path())
                .unwrap_err()
                .to_string()
                .contains("lowercase")
        );
        feed.id = "good-id".into();
        feed.resolve(dir.path()).unwrap();
        assert!(feed.transform.is_absolute());
    }
}
