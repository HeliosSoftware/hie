//! The database source on SQL Server, when one is at hand: set
//! `HIE_TEST_MSSQL_URL` (e.g. `sqlserver://sa:pass@localhost:14333/lab?trust_cert=true`)
//! to a server holding the `lab_results` table of `sources/lab-database.yaml.example`
//! with at least three rows; the test is skipped otherwise.

use std::path::PathBuf;

use hie::source::Source;
use serde_json::Value;

const QUERY: &str = "SELECT TOP 2 id, patient_mrn AS patient, loinc AS code, display, value, unit, taken_at, updated_at, ok, note FROM lab_results WHERE id > {cursor} ORDER BY id";

#[tokio::test]
async fn sql_server_rows_are_read_past_a_cursor() {
    let Ok(url) = std::env::var("HIE_TEST_MSSQL_URL") else {
        eprintln!("skipping: HIE_TEST_MSSQL_URL is not set");
        return;
    };
    unsafe { std::env::set_var("HIE_TEST_MSSQL_URL_COPY", &url) };
    let repo = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let mut feed: hie::config::FeedConfig = serde_yaml::from_str(&format!(
        "id: mssql\nname: SQL Server\nsource:\n  type: database\n  url_env: HIE_TEST_MSSQL_URL_COPY\n  query: \"{QUERY}\"\n  cursor_column: id\n  cursor_start: \"0\"\n  max_runs: 1\ntransform: transforms/sql_lab_results.py\n"
    ))
    .unwrap();
    feed.resolve(&repo).unwrap();

    // One run of TOP 2 past id 0: the first two rows, typed.
    let source = Source::from_config(&feed).unwrap();
    assert_eq!(source.list().await.unwrap(), ["query"]);
    let incoming = source.fetch("query").await.unwrap();
    let rows: Vec<Value> = String::from_utf8(incoming.bytes)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), 2, "{rows:?}");
    let first = &rows[0];
    assert!(first["id"].is_i64(), "{first}");
    assert!(first["value"].is_number(), "{first}");
    assert!(
        first["taken_at"].as_str().unwrap().starts_with("20"),
        "{first}"
    );
    assert!(
        first["updated_at"].as_str().unwrap().contains('+')
            || first["updated_at"].as_str().unwrap().ends_with('Z'),
        "{first}"
    );
    assert_eq!(first["ok"], true);
    assert!(
        first["note"].is_null() || first["note"].is_string(),
        "{first}"
    );
    assert_eq!(
        source.acknowledge("query", "").await.unwrap(),
        format!("cursor {}", rows[1]["id"])
    );

    // The probe past the start sees the rest.
    let probe = source.probe().await.unwrap();
    assert!(probe.reachable);
    assert!(probe.objects >= 1, "{probe:?}");

    // A wrong password is refused at connection time, in the server's words.
    unsafe {
        std::env::set_var(
            "HIE_TEST_MSSQL_URL_COPY",
            url.replacen("://", "://sa:wrong@", 1),
        )
    };
    let source = Source::from_config(&feed).unwrap();
    let err = source.list().await.unwrap_err();
    assert!(format!("{err:#}").contains("SQL Server"), "{err:#}");
}
