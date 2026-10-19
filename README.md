# Helios Integration Engine

Gets data from source feeds into HFS, the Helios FHIR Server:
- **Feeds:** folders today; S3, databases and other FHIR servers later.
- **Transforms:** in Python.
- **Delivery:** idempotent, with retries.
- **Around it:** per-record traceability, an API for client UIs, Prometheus metrics, and alerts that include "this feed went quiet".

This is a working prototype. The reasons behind each choice are in [docs/decisions.md](docs/decisions.md).

## What the prototype does

Run against a real HFS (SQLite) with the example lab feed:

| Scenario | Result |
|---|---|
| **1.** HFS is down when `labs.csv` arrives | The 3 rows are stored as `pending` and the file leaves the inbox. Each row shows why it is waiting. |
| **2.** HFS comes back | The next retry delivers all 3. Nothing is lost. |
| **3.** The identical file arrives again | Recognized by its SHA-256 and not reprocessed. |
| **3.** The same rows arrive in a new file | Sent as conditional updates. HFS still has 3 `Observation`s; the re-sent one is at `versionId` 2. |
| **4.** `messy.csv`: one good row (local code `GLU`) and five bad ones | The good row is mapped to LOINC and delivered. The five are rejected, each with its line and reason: `value 'alto' is not a number`, `expected 6 columns ... found 7`, `unit 'mmHg' has no UCUM mapping`, `test code 'XYZ' is neither LOINC nor in the local code table`, `date '16/10/2026' is not YYYY-MM-DD`. One alert for the file. |
| **5.** The `alto` row is corrected and reprocessed through the API | Delivered (`201 Created`). The record keeps both the original and the corrected value. |
| **6.** No data within `expect_every` | One `silence` alert, not one per check (covered by the integration test). |

## Run it

You need:
- Rust 1.90 or later;
- Python 3.8 or later (`py` on Windows, `python3` elsewhere; set `python:` in `hie.yaml` to change it);
- HFS running at the URL in `hie.yaml` (default `http://localhost:8080`).

```sh
cargo run -- check          # validate hie.yaml and feeds/
cargo run                   # watch feeds, serve the API on 127.0.0.1:8090
cp examples/data/labs.csv inbox/lab-results/
curl localhost:8090/api/feeds
```

| Variable | Default | Purpose |
|---|---|---|
| `HIE_CONFIG` | `hie.yaml` | Engine configuration file |
| `HIE_RUNTIME_DIR` | `hie_runtime` next to the config | Location of the Python worker |
| `HIE_LOG` | `info` | Log filter (`debug`, `hie=debug`, ...) |

## Adding a feed

A feed is a YAML file in `feeds/` plus a transform:

```yaml
id: lab-results
name: Lab results
source:
  type: folder
  path: inbox/lab-results
  pattern: "*.csv"
  poll_seconds: 5
transform: transforms/lab_results.py
delivery:
  batch_size: 100            # entries per batch Bundle
  initial_backoff_ms: 2000   # doubles per failed attempt, up to 5 minutes
  alert_after_attempts: 5
expect_every: 24h            # alert when nothing arrives for this long
```

```python
from hie import DataError

def transform(record, context):
    # record: one CSV row as a dict of strings
    # context: {"feed", "file", "line"}
    if not record["value"]:
        raise DataError("missing value")           # rejected with this reason
    return {"resourceType": "Observation",
            "identifier": [{"system": "urn:...", "value": "..."}],  # required: makes re-sends updates
            ...}
```

- A transform returns a resource, a list of resources, or `None` (filtered out).
- `DataError` rejects the row with its message. Any other exception is recorded with its traceback as a transform bug.

## API

| Method and path | Returns |
|---|---|
| `GET /api/feeds` | Each feed: freshness (`ok`, `silent`, `no_data_yet`), record counts by status, last file, last alert |
| `GET /api/feeds/{id}/files` | Files received, with duplicates and per-file sent/rejected counts |
| `GET /api/records?feed=&status=&file_id=` | Records; `status` is `received`, `pending`, `sent` or `rejected` |
| `GET /api/records/{id}` | One record: file, line, original and corrected row, resources sent, HFS response |
| `POST /api/records/{id}/reprocess` | Runs a record again; body `{"raw": {...}}` replaces the row and keeps the original |
| `GET /api/alerts?feed=` | Alerts, newest first |
| `GET /metrics` | Prometheus text: records by status, last received time, alerts by kind |
| `GET /health` | `{"status": "ok"}` |

## Layout

```
src/            engine: config, source reading, transform worker, FHIR delivery, pipeline, API, alerts, storage
hie_runtime/    Python side of the transform protocol (worker.py, hie.DataError)
transforms/     one Python transform per feed
feeds/          one YAML file per feed
examples/data/  sample files used by the tests and the demo
tests/          end-to-end Rust test with a stand-in FHIR server; Python tests for the transform
docs/           design decisions
```

## Tests

```sh
cargo test                                 # unit tests and the end-to-end pipeline test
python -m unittest tests/test_lab_results.py
cargo clippy --all-targets -- -D warnings
```

The end-to-end test runs the real Python transform against a stand-in FHIR server that can be taken down. It covers:
- the outage and recovery;
- the duplicate file and the re-sent rows;
- each kind of bad row;
- reprocessing with a correction;
- the silence alert.

It is skipped if no Python 3.8+ is found; set `HIE_TEST_PYTHON` to point at one.
