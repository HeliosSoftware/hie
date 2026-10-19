use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, SystemTime};

use anyhow::{Context, Result};
use chrono::{TimeDelta, Utc};
use futures::StreamExt;
use object_store::ObjectStore;
use object_store::aws::AmazonS3Builder;
use object_store::path::Path as ObjectPath;

use crate::config::{FeedConfig, SourceConfig, glob_match};

/// One object fetched from a source: a file, an S3 object.
pub struct Incoming {
    /// The source's own handle: a file path, an object key.
    pub key: String,
    /// The human-readable name kept with every record.
    pub name: String,
    pub bytes: Vec<u8>,
}

/// Where a feed's objects come from. `list` names what is waiting, `fetch` reads
/// one object, and `acknowledge` moves it out of the way once its records are stored.
pub enum Source {
    Folder(FolderSource),
    Object(ObjectSource),
}

impl Source {
    pub fn from_config(feed: &FeedConfig) -> Result<Self> {
        Ok(match &feed.source {
            SourceConfig::Folder {
                path,
                pattern,
                min_file_age_ms,
                archive,
                ..
            } => Source::Folder(FolderSource {
                dir: path.clone(),
                pattern: pattern.clone(),
                min_age: Duration::from_millis(*min_file_age_ms),
                archive: archive.clone().unwrap_or_else(|| path.join("processed")),
            }),
            SourceConfig::S3 {
                bucket,
                prefix,
                region,
                endpoint,
                pattern,
                min_file_age_ms,
                archive_prefix,
                ..
            } => {
                let mut builder = AmazonS3Builder::from_env().with_bucket_name(bucket);
                if let Some(region) = region {
                    builder = builder.with_region(region);
                }
                if let Some(endpoint) = endpoint {
                    builder = builder
                        .with_endpoint(endpoint)
                        .with_allow_http(true)
                        .with_virtual_hosted_style_request(false);
                }
                let store = builder
                    .build()
                    .with_context(|| format!("feed {}: S3 source", feed.id))?;
                Source::Object(ObjectSource::new(
                    Arc::new(store),
                    prefix,
                    archive_prefix,
                    pattern,
                    Duration::from_millis(*min_file_age_ms),
                ))
            }
        })
    }

    pub async fn list(&self) -> Result<Vec<String>> {
        match self {
            Source::Folder(f) => f.list(),
            Source::Object(o) => o.list().await,
        }
    }

    pub async fn fetch(&self, key: &str) -> Result<Incoming> {
        match self {
            Source::Folder(f) => f.fetch(key),
            Source::Object(o) => o.fetch(key).await,
        }
    }

    pub async fn acknowledge(&self, key: &str, archived_name: &str) -> Result<()> {
        match self {
            Source::Folder(f) => f.acknowledge(key, archived_name),
            Source::Object(o) => o.acknowledge(key, archived_name).await,
        }
    }
}

/// A local folder, polled. A file is taken once it has not changed for `min_age`,
/// so a file still being written is left alone.
pub struct FolderSource {
    dir: PathBuf,
    pattern: String,
    min_age: Duration,
    archive: PathBuf,
}

impl FolderSource {
    fn list(&self) -> Result<Vec<String>> {
        std::fs::create_dir_all(&self.dir)
            .with_context(|| format!("creating {}", self.dir.display()))?;
        let mut keys = Vec::new();
        for entry in std::fs::read_dir(&self.dir)? {
            let path = entry?.path();
            if !path.is_file() {
                continue;
            }
            let Some(name) = path.file_name().and_then(|n| n.to_str()) else {
                continue;
            };
            if !glob_match(&self.pattern, name) {
                continue;
            }
            let age = std::fs::metadata(&path)?
                .modified()
                .ok()
                .and_then(|m| SystemTime::now().duration_since(m).ok())
                .unwrap_or_default();
            if age < self.min_age {
                continue;
            }
            keys.push(path.to_string_lossy().into_owned());
        }
        keys.sort();
        Ok(keys)
    }

    fn fetch(&self, key: &str) -> Result<Incoming> {
        let path = Path::new(key);
        let name = path
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("file")
            .to_string();
        let bytes = std::fs::read(path).with_context(|| format!("reading {key}"))?;
        Ok(Incoming {
            key: key.to_string(),
            name,
            bytes,
        })
    }

    fn acknowledge(&self, key: &str, archived_name: &str) -> Result<()> {
        std::fs::create_dir_all(&self.archive)?;
        let target = self.archive.join(archived_name);
        if std::fs::rename(key, &target).is_err() {
            std::fs::copy(key, &target)?;
            std::fs::remove_file(key)?;
        }
        Ok(())
    }
}

/// An object store (S3 in production, in-memory in tests), polled by listing a prefix.
/// Acknowledged objects are renamed under the archive prefix.
pub struct ObjectSource {
    store: Arc<dyn ObjectStore>,
    prefix: Option<ObjectPath>,
    archive: ObjectPath,
    pattern: String,
    min_age: TimeDelta,
}

impl ObjectSource {
    pub fn new(
        store: Arc<dyn ObjectStore>,
        prefix: &str,
        archive_prefix: &str,
        pattern: &str,
        min_age: Duration,
    ) -> Self {
        let prefix = prefix.trim_matches('/');
        Self {
            store,
            prefix: (!prefix.is_empty()).then(|| ObjectPath::from(prefix)),
            archive: ObjectPath::from(archive_prefix.trim_matches('/')),
            pattern: pattern.to_string(),
            min_age: TimeDelta::from_std(min_age).unwrap_or_default(),
        }
    }

    async fn list(&self) -> Result<Vec<String>> {
        let archive_prefix = format!("{}/", self.archive);
        let mut stream = self.store.list(self.prefix.as_ref());
        let mut keys = Vec::new();
        while let Some(meta) = stream.next().await {
            let meta = meta.context("listing objects")?;
            let key = meta.location.to_string();
            if key.starts_with(&archive_prefix) {
                continue;
            }
            let Some(name) = meta.location.filename() else {
                continue;
            };
            if !glob_match(&self.pattern, name) {
                continue;
            }
            if Utc::now().signed_duration_since(meta.last_modified) < self.min_age {
                continue;
            }
            keys.push(key);
        }
        keys.sort();
        Ok(keys)
    }

    async fn fetch(&self, key: &str) -> Result<Incoming> {
        let path = ObjectPath::from(key);
        let bytes = self
            .store
            .get(&path)
            .await
            .with_context(|| format!("fetching {key}"))?
            .bytes()
            .await?
            .to_vec();
        Ok(Incoming {
            key: key.to_string(),
            name: path.filename().unwrap_or("object").to_string(),
            bytes,
        })
    }

    async fn acknowledge(&self, key: &str, archived_name: &str) -> Result<()> {
        let to = ObjectPath::from(format!("{}/{archived_name}", self.archive));
        self.store
            .rename(&ObjectPath::from(key), &to)
            .await
            .with_context(|| format!("archiving {key}"))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use object_store::PutPayload;
    use object_store::memory::InMemory;

    #[tokio::test]
    async fn objects_are_listed_fetched_and_archived() {
        let store = Arc::new(InMemory::new());
        for key in [
            "in/b.csv",
            "in/a.csv",
            "in/notes.txt",
            "in/processed/old.csv",
            "elsewhere/c.csv",
        ] {
            store
                .put(&ObjectPath::from(key), PutPayload::from_static(b"x"))
                .await
                .unwrap();
        }
        let source =
            ObjectSource::new(store.clone(), "in", "in/processed", "*.csv", Duration::ZERO);
        assert_eq!(source.list().await.unwrap(), ["in/a.csv", "in/b.csv"]);

        let got = source.fetch("in/a.csv").await.unwrap();
        assert_eq!(
            (got.name.as_str(), got.bytes.as_slice()),
            ("a.csv", &b"x"[..])
        );

        source.acknowledge("in/a.csv", "7-a.csv").await.unwrap();
        assert_eq!(source.list().await.unwrap(), ["in/b.csv"]);
        assert!(
            store
                .head(&ObjectPath::from("in/processed/7-a.csv"))
                .await
                .is_ok()
        );
    }

    #[tokio::test]
    async fn a_fresh_object_waits_for_the_age_guard() {
        let store = Arc::new(InMemory::new());
        store
            .put(&ObjectPath::from("x.csv"), PutPayload::from_static(b"x"))
            .await
            .unwrap();
        let waits = ObjectSource::new(store.clone(), "", "processed", "*", Duration::from_secs(60));
        assert!(waits.list().await.unwrap().is_empty());
        let takes = ObjectSource::new(store, "", "processed", "*", Duration::ZERO);
        assert_eq!(takes.list().await.unwrap(), ["x.csv"]);
    }
}
