//! The API end to end over a real listener: a source created, tested, fed, paused,
//! resumed and deleted, with a stand-in FHIR server behind it.

use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use serde_json::{Value, json};

type Urls = Arc<Mutex<Vec<String>>>;

async fn batch(State(urls): State<Urls>, Json(bundle): Json<Value>) -> Json<Value> {
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        urls.lock()
            .unwrap()
            .push(e["request"]["url"].as_str().unwrap().to_string());
        entries.push(json!({"response": {"status": "201 Created"}}));
    }
    Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries}))
}

async fn serve(router: Router) -> String {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, router).await.unwrap() });
    format!("http://{addr}")
}

fn python() -> Option<String> {
    let candidates = match std::env::var("HIE_TEST_PYTHON") {
        Ok(p) => vec![p],
        Err(_) => vec!["py".into(), "python3".into(), "python".into()],
    };
    candidates.into_iter().find(|p| {
        std::process::Command::new(p)
            .arg("-c")
            .arg("import sys; sys.exit(0 if sys.version_info >= (3, 8) else 1)")
            .output()
            .is_ok_and(|o| o.status.success())
    })
}

fn repo() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
}

async fn wait_until(
    mut f: impl FnMut() -> std::pin::Pin<Box<dyn std::future::Future<Output = bool>>>,
    what: &str,
) {
    let started = Instant::now();
    while !f().await {
        assert!(
            started.elapsed() < Duration::from_secs(20),
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

#[tokio::test]
async fn a_source_lives_through_the_api() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let urls: Urls = Arc::default();
    let fhir = serve(
        Router::new()
            .route("/", post(batch))
            .with_state(urls.clone()),
    )
    .await;

    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {fhir}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    assert!(feeds.is_empty());
    let (_manager, state) = hie::build(&config, feeds, &repo().join("hie_runtime")).unwrap();
    let api = serve(hie::api::router(state)).await;
    let http = reqwest::Client::new();

    // Nothing yet.
    let listed: Value = http
        .get(format!("{api}/api/sources"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(listed["sources"].as_array().unwrap().len(), 0);

    // Test the connection before creating anything: the inbox is empty.
    std::fs::create_dir_all(root.join("inbox")).unwrap();
    let probe: Value = http
        .post(format!("{api}/api/sources/test"))
        .json(&json!({"source": {"type": "folder", "path": "inbox", "pattern": "*.csv"}}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (probe["reachable"].as_bool(), probe["objects"].as_u64()),
        (Some(true), Some(0))
    );

    // A bad definition is refused with the reason.
    let bad = http
        .post(format!("{api}/api/sources"))
        .json(&json!({"id": "Bad", "name": "x", "source": {"type": "folder", "path": "inbox"}, "transform": "transforms/lab_results.py"}))
        .send()
        .await
        .unwrap();
    assert_eq!(bad.status(), 400);
    assert!(
        bad.json::<Value>().await.unwrap()["error"]
            .as_str()
            .unwrap()
            .contains("lowercase")
    );

    // Create it: the definition is written as a file and the source starts.
    let definition = json!({
        "id": "lab-results", "name": "Lab results",
        "source": {"type": "folder", "path": "inbox", "pattern": "*.csv", "poll_seconds": 60, "min_file_age_ms": 0},
        "transform": "transforms/lab_results.py",
        "expect_every": "24h"
    });
    let created = http
        .post(format!("{api}/api/sources"))
        .json(&definition)
        .send()
        .await
        .unwrap();
    assert_eq!(created.status(), 201);
    let created: Value = created.json().await.unwrap();
    assert_eq!(created["state"], "waiting");
    assert_eq!(created["definition"]["source"]["path"], "inbox");
    let file = std::fs::read_to_string(root.join("sources/lab-results.yaml")).unwrap();
    assert!(file.contains("id: lab-results") && file.contains("expect_every: 24h"));
    let again = http
        .post(format!("{api}/api/sources"))
        .json(&definition)
        .send()
        .await
        .unwrap();
    assert_eq!(again.status(), 409);

    // Drop a file and ask for a pass now, instead of waiting for the 60 s poll.
    std::fs::copy(
        repo().join("examples/data/labs.csv"),
        root.join("inbox/labs.csv"),
    )
    .unwrap();
    let probe: Value = http
        .post(format!("{api}/api/sources/test"))
        .json(&definition)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (probe["objects"].as_u64(), probe["newest"].as_str()),
        (Some(1), Some("labs.csv"))
    );
    let checking = http
        .post(format!("{api}/api/sources/lab-results/check-now"))
        .send()
        .await
        .unwrap();
    assert_eq!(checking.status(), 202);
    let api2 = api.clone();
    let http2 = http.clone();
    wait_until(
        move || {
            let (api, http) = (api2.clone(), http2.clone());
            Box::pin(async move {
                let s: Value = http
                    .get(format!("{api}/api/sources/lab-results"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                s["records"]["sent"] == 3
            })
        },
        "3 records sent",
    )
    .await;
    assert_eq!(urls.lock().unwrap().len(), 3);

    // Pause: a new file is left where it is.
    let paused: Value = http
        .post(format!("{api}/api/sources/lab-results/pause"))
        .json(&json!({"by": "sam"}))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(
        (paused["state"].as_str(), paused["paused"]["by"].as_str()),
        (Some("paused"), Some("sam"))
    );
    std::fs::copy(
        repo().join("examples/data/labs.csv"),
        root.join("inbox/labs-2.csv"),
    )
    .unwrap();
    http.post(format!("{api}/api/sources/lab-results/check-now"))
        .send()
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(400)).await;
    assert!(root.join("inbox/labs-2.csv").exists());
    assert_eq!(urls.lock().unwrap().len(), 3);

    // Resume: it is taken on the next pass, and recognized as the same content.
    let resumed: Value = http
        .post(format!("{api}/api/sources/lab-results/resume"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(resumed["state"], "receiving");
    let (api3, http3) = (api.clone(), http.clone());
    wait_until(
        move || {
            let (api, http) = (api3.clone(), http3.clone());
            Box::pin(async move {
                let s: Value = http
                    .get(format!("{api}/api/sources/lab-results"))
                    .send()
                    .await
                    .unwrap()
                    .json()
                    .await
                    .unwrap();
                s["records"]["duplicate_files"] == 1
            })
        },
        "the duplicate file",
    )
    .await;
    assert_eq!(urls.lock().unwrap().len(), 3);

    // Replace the definition: the name changes, the id cannot.
    let mut renamed = definition.clone();
    renamed["name"] = json!("Lab results (Quest)");
    let replaced: Value = http
        .put(format!("{api}/api/sources/lab-results"))
        .json(&renamed)
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(replaced["name"], "Lab results (Quest)");
    renamed["id"] = json!("other");
    let mismatch = http
        .put(format!("{api}/api/sources/lab-results"))
        .json(&renamed)
        .send()
        .await
        .unwrap();
    assert_eq!(mismatch.status(), 400);

    // Delete: the definition goes, the history stays.
    let deleted = http
        .delete(format!("{api}/api/sources/lab-results"))
        .send()
        .await
        .unwrap();
    assert_eq!(deleted.status(), 204);
    assert!(!root.join("sources/lab-results.yaml").exists());
    assert_eq!(
        http.get(format!("{api}/api/sources/lab-results"))
            .send()
            .await
            .unwrap()
            .status(),
        404
    );
    let records: Value = http
        .get(format!("{api}/api/records?source=lab-results"))
        .send()
        .await
        .unwrap()
        .json()
        .await
        .unwrap();
    assert_eq!(records["records"].as_array().unwrap().len(), 3);
}
