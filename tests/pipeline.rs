//! End-to-end: a folder feed, the real Python transform, and a stand-in FHIR server
//! that can be taken down and that refuses some resources.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use axum::extract::State;
use axum::routing::post;
use axum::{Json, Router};
use hie::pipeline::FeedRunner;
use serde_json::{Value, json};

#[derive(Default)]
struct Fhir {
    down: bool,
    versions: HashMap<String, u32>,
    requests: u32,
}

type Shared = Arc<Mutex<Fhir>>;

async fn batch(
    State(s): State<Shared>,
    Json(bundle): Json<Value>,
) -> (axum::http::StatusCode, Json<Value>) {
    let mut s = s.lock().unwrap();
    s.requests += 1;
    if s.down {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"resourceType": "OperationOutcome"})),
        );
    }
    let mut entries = Vec::new();
    for e in bundle["entry"].as_array().unwrap() {
        let url = e["request"]["url"].as_str().unwrap().to_string();
        if e["resource"]["subject"]["reference"] == "Patient/999" {
            entries.push(json!({"response": {"status": "400 Bad Request", "outcome": {
                "resourceType": "OperationOutcome",
                "issue": [{"severity": "error", "code": "invalid", "details": {"text": "subject Patient/999 does not exist"}}]}}}));
            continue;
        }
        let v = s.versions.entry(url).or_insert(0);
        *v += 1;
        let status = if *v == 1 { "201 Created" } else { "200 OK" };
        entries.push(json!({"response": {"status": status, "etag": format!("W/\"{v}\"")}}));
    }
    (
        axum::http::StatusCode::OK,
        Json(json!({"resourceType": "Bundle", "type": "batch-response", "entry": entries})),
    )
}

async fn fhir_server() -> (String, Shared) {
    let state: Shared = Arc::default();
    let app = Router::new()
        .route("/", post(batch))
        .with_state(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move { axum::serve(listener, app).await.unwrap() });
    (format!("http://{addr}"), state)
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

fn drop_file(inbox: &Path, name: &str, content: &str) {
    std::fs::create_dir_all(inbox).unwrap();
    std::fs::write(inbox.join(name), content).unwrap();
}

fn count(state: &hie::api::AppState, status: &str) -> i64 {
    state.db.feed_counts("lab-results").unwrap()[status]
        .as_i64()
        .unwrap()
}

fn alert_kinds(state: &hie::api::AppState) -> Vec<String> {
    state
        .db
        .query_json("SELECT kind FROM alerts ORDER BY id", [])
        .unwrap()
        .into_iter()
        .map(|r| r["kind"].as_str().unwrap().to_string())
        .collect()
}

#[tokio::test]
async fn a_feed_survives_an_outage_bad_rows_and_resends() {
    let Some(python) = python() else {
        eprintln!("skipping: no Python 3.8+ found (set HIE_TEST_PYTHON)");
        return;
    };
    let (base, fhir) = fhir_server().await;
    let dir = tempfile::tempdir().unwrap();
    let root = dir.path();
    std::fs::create_dir_all(root.join("feeds")).unwrap();
    std::fs::create_dir_all(root.join("transforms")).unwrap();
    std::fs::copy(
        repo().join("transforms/lab_results.py"),
        root.join("transforms/lab_results.py"),
    )
    .unwrap();
    std::fs::write(
        root.join("hie.yaml"),
        format!("fhir_base_url: {base}\ndatabase: data/hie.db\npython: {python}\n"),
    )
    .unwrap();
    std::fs::write(
        root.join("feeds/lab-results.yaml"),
        "id: lab-results\nname: Lab results\nsource:\n  type: folder\n  path: inbox\n  pattern: \"*.csv\"\n  min_file_age_ms: 0\n\
         transform: transforms/lab_results.py\ndelivery:\n  initial_backoff_ms: 10\n  alert_after_attempts: 2\nexpect_every: 1s\n",
    )
    .unwrap();
    let (config, feeds) = hie::config::load(&root.join("hie.yaml")).unwrap();
    let (runners, state) = hie::build(&config, feeds, &repo().join("hie_runtime")).unwrap();
    let runner: Arc<FeedRunner> = runners[0].clone();
    let inbox = root.join("inbox");
    let labs = std::fs::read_to_string(repo().join("examples/data/labs.csv")).unwrap();

    // HFS is down when the file arrives: rows are stored, the file leaves the inbox, nothing is lost.
    fhir.lock().unwrap().down = true;
    drop_file(&inbox, "labs.csv", &labs);
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "pending"), 3);
    assert!(!inbox.join("labs.csv").exists());
    tokio::time::sleep(Duration::from_millis(30)).await;
    runner.tick().await.unwrap();
    assert_eq!(alert_kinds(&state), ["delivery"]);

    // HFS comes back: the same rows are delivered on the next retry.
    fhir.lock().unwrap().down = false;
    tokio::time::sleep(Duration::from_millis(60)).await;
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 3);
    assert_eq!(fhir.lock().unwrap().versions.len(), 3);
    assert_eq!(alert_kinds(&state), ["delivery", "delivery_recovered"]);

    // The identical file again: recognized by its hash and not reprocessed.
    let before = fhir.lock().unwrap().requests;
    drop_file(&inbox, "labs.csv", &labs);
    runner.tick().await.unwrap();
    assert_eq!(fhir.lock().unwrap().requests, before);
    assert_eq!(
        state.db.feed_counts("lab-results").unwrap()["duplicate_files"],
        1
    );

    // The same rows in a different file: delivered again, but as updates, not new resources.
    drop_file(&inbox, "labs-resent.csv", &format!("{labs}\n"));
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 6);
    {
        let f = fhir.lock().unwrap();
        assert_eq!(f.versions.len(), 3);
        assert!(f.versions.values().all(|v| *v == 2));
    }

    // A messy file: each bad row is rejected on its own with its reason; good rows go through.
    let messy = std::fs::read_to_string(repo().join("examples/data/messy.csv")).unwrap();
    drop_file(
        &inbox,
        "messy.csv",
        &format!("{messy}Patient/999,2345-7,Glucose,77,mg/dL,2026-10-16\n"),
    );
    runner.tick().await.unwrap();
    let rejected = state
        .db
        .query_json(
            "SELECT line, error_kind, error FROM records WHERE status = 'rejected' ORDER BY line",
            [],
        )
        .unwrap();
    let summary: Vec<(i64, String)> = rejected
        .iter()
        .map(|r| {
            (
                r["line"].as_i64().unwrap(),
                r["error_kind"].as_str().unwrap().to_string(),
            )
        })
        .collect();
    assert_eq!(
        summary,
        [
            (3, "data".into()),
            (4, "parse".into()),
            (5, "data".into()),
            (6, "data".into()),
            (7, "data".into()),
            (8, "server".into())
        ]
    );
    assert!(
        rejected[0]["error"]
            .as_str()
            .unwrap()
            .contains("'alto' is not a number")
    );
    assert!(
        rejected[5]["error"]
            .as_str()
            .unwrap()
            .contains("Patient/999 does not exist")
    );
    assert_eq!(
        count(&state, "sent"),
        7,
        "the GLU local code row is mapped to LOINC and sent"
    );
    assert_eq!(alert_kinds(&state).last().unwrap(), "rejected_records");

    // Someone fixes the value and reprocesses that one row.
    let alto = state
        .db
        .query_json(
            "SELECT id, raw FROM records WHERE line = 3 AND status = 'rejected'",
            [],
        )
        .unwrap();
    let mut raw = alto[0]["raw"].clone();
    raw["value"] = json!("101");
    assert!(
        state
            .db
            .reprocess(alto[0]["id"].as_i64().unwrap(), Some(&raw))
            .unwrap()
    );
    runner.tick().await.unwrap();
    assert_eq!(count(&state, "sent"), 8);
    assert_eq!(count(&state, "rejected"), 5);
    let fixed = state
        .db
        .query_json(
            "SELECT raw, original_raw FROM records WHERE id = ?1",
            [alto[0]["id"].as_i64().unwrap()],
        )
        .unwrap();
    assert_eq!(fixed[0]["raw"]["value"], "101");
    assert_eq!(
        fixed[0]["original_raw"]["value"], "alto",
        "the data as received is kept"
    );
    assert_eq!(
        alert_kinds(&state)
            .iter()
            .filter(|k| *k == "rejected_records")
            .count(),
        1,
        "reprocessing a row does not repeat the file's alert"
    );

    // Nothing arrives for longer than `expect_every`: one silence alert, not one per pass.
    tokio::time::sleep(Duration::from_millis(1100)).await;
    runner.tick().await.unwrap();
    runner.tick().await.unwrap();
    assert_eq!(
        alert_kinds(&state)
            .iter()
            .filter(|k| *k == "silence")
            .count(),
        1
    );
}
