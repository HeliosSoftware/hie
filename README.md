# Helios Integration Engine

Gets data from sources into HFS, the Helios FHIR Server:
- **Sources:** a folder or an S3 bucket today; databases and other FHIR servers later. Created and managed through the API, persisted as YAML files.
- **Formats:** CSV, NDJSON, FHIR Bundle files.
- **Transforms:** in Python.
- **Delivery:** idempotent, with retries.
- **Around it:** per-record traceability, an API for client UIs, Prometheus metrics, and alerts that include "this source went quiet".

The engine is a Rust service that runs on Linux (see the `Dockerfile`; CI builds and tests it on Ubuntu). This is a working prototype. The reasons behind each choice are in [docs/decisions.md](docs/decisions.md); the architecture is discussed in [the architectural discussion](https://github.com/HeliosSoftware/hie/discussions/1), the choice of stack in [discussion #2](https://github.com/HeliosSoftware/hie/discussions/2), and the shared runtime for user code in [discussion #3](https://github.com/HeliosSoftware/hie/discussions/3).

## What the prototype does

Run against a real HFS (SQLite) with the example lab source:

| Scenario | Result |
|---|---|
| **1.** HFS is down when `labs.csv` arrives | The 3 rows are stored as `pending` and the file leaves the inbox. Each row shows why it is waiting. |
| **2.** HFS comes back | The next retry delivers all 3. Nothing is lost. |
| **3.** The identical file arrives again | Recognized by its SHA-256 and not reprocessed. |
| **3.** The same rows arrive in a new file | Sent as conditional updates. HFS still has 3 `Observation`s; the re-sent one is at `versionId` 2. |
| **4.** `messy.csv`: one good row (local code `GLU`) and five bad ones | The good row is mapped to LOINC and delivered. The five are rejected, each with its line and reason: `value 'alto' is not a number`, `expected 6 columns ... found 7`, `unit 'mmHg' has no UCUM mapping`, `test code 'XYZ' is neither LOINC nor in the local code table`, `date '16/10/2026' is not YYYY-MM-DD`. One alert for the file. |
| **5.** The `alto` row is corrected and reprocessed through the API | Delivered (`201 Created`). The record keeps both the original and the corrected value. |
| **6.** No data within `expect_every` | One `silence` alert, not one per check (covered by the integration test). |

And with Synthea output for three patients, in both of its FHIR shapes:

| Source | Input | Result |
|---|---|---|
| `fhir_bundle` | 3 transaction Bundles (394 to 700 entries each, `urn:uuid` references between entries) | 1,775 resources delivered by id, 0 rejected. References rewritten to `Type/id`. |
| `ndjson` | 17 files, one per resource type, from Synthea's bulk-data export | 1,775 resources delivered, 0 rejected. |
| Both together | The same patients twice, in two formats | HFS holds each resource once: 3 `Patient`, 214 `Encounter`, 415 `Observation`. Both sources finished in 17 s on a laptop, Python transform included. |

## Run it

You need:
- Rust 1.90 or later;
- Python 3.8 or later (`py` on Windows, `python3` elsewhere; set `python:` in `hie.yaml` to change it);
- HFS running at the URL in `hie.yaml` (default `http://localhost:8080`).

```sh
cargo run -- check          # validate hie.yaml and sources/
cargo run                   # run every source, serve the API on 127.0.0.1:8090
cp examples/data/labs.csv inbox/lab-results/
curl localhost:8090/api/sources
```

On Linux, in a container:

```sh
docker build -t hie .
docker run --rm -p 8090:8090 -v $PWD/inbox:/hie/inbox hie
```

| Variable | Default | Purpose |
|---|---|---|
| `HIE_CONFIG` | `hie.yaml` | Engine configuration file |
| `HIE_RUNTIME_DIR` | `hie_runtime` next to the config | Location of the Python worker |
| `HIE_LOG` | `info` | Log filter (`debug`, `hie=debug`, ...) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_DEFAULT_REGION` | | Credentials for S3 sources, as for any AWS tool (profiles work too) |

## Sources

A source is one connection: where the data comes from, its format, the transform that turns it into FHIR, how it is delivered, and how often data is expected. Each source is one YAML file in `sources/`, which the engine reads at start and **writes when a source is created or replaced through the API**, so definitions stay under version control either way.

```yaml
id: lab-results
name: Lab results
source:
  type: folder               # or s3 (see sources/synthea-s3.yaml.example)
  path: inbox/lab-results
  pattern: "*.csv"
  poll_seconds: 5
format: csv                  # csv (default), ndjson, or fhir_bundle
transform: transforms/lab_results.py
delivery:
  batch_size: 100            # entries per batch Bundle
  initial_backoff_ms: 2000   # doubles per failed attempt, up to 5 minutes
  alert_after_attempts: 5
expect_every: 24h            # alert when nothing arrives for this long
```

An S3 source names a `bucket`, an optional `prefix`, and an `archive_prefix` (default `processed`) that acknowledged objects are moved under. `endpoint` points at an S3-compatible store such as MinIO.

The same document is what `POST /api/sources` takes. Before creating one, `POST /api/sources/test` with `{"source": {...}}` lists the connection once and answers with the evidence: reachable or not, in how many milliseconds, how many objects match, and the newest one.

## Transforms

```python
from hie import DataError

def transform(record, context):
    # record: one CSV row as a dict of strings, one NDJSON line, or one Bundle entry's resource
    # context: {"feed", "file", "line"}
    if not record["value"]:
        raise DataError("missing value")           # rejected with this reason
    return {"resourceType": "Observation",
            "identifier": [{"system": "urn:...", "value": "..."}],  # makes re-sends updates
            ...}
```

- A transform returns a resource, a list of resources, or `None` (filtered out).
- Every resource needs an `id` (delivered as `PUT Type/id`) or an `identifier` with system and value (delivered as `PUT Type?identifier=system|value`), so sending the same record twice updates instead of duplicating.
- `DataError` rejects the row with its message. Any other exception is recorded with its traceback as a transform bug.
- Data that already is FHIR (NDJSON exports, Bundle files) goes through `transforms/fhir_passthrough.py`. A Bundle is split into one record per entry, and references between entries (`urn:uuid:...`) are rewritten to `Type/id`.

## API

| Method and path | Does |
|---|---|
| `GET /api/sources` | Every source: definition, state (`waiting`, `receiving`, `late`, `paused`), freshness, record counts, last file, last alert |
| `POST /api/sources` | Creates a source from its definition (the YAML as JSON); writes `sources/{id}.yaml` and starts it. `409` if it exists, `400` with the reason if invalid |
| `POST /api/sources/test` | Tests a connection: `{"source": {...}}` or a full definition. Answers `reachable`, `elapsed_ms`, `objects`, `newest`, `newest_at` |
| `GET /api/sources/{id}` | One source, as above |
| `PUT /api/sources/{id}` | Replaces the definition and restarts the source |
| `DELETE /api/sources/{id}` | Stops the source and removes its file; its records and alerts stay |
| `POST /api/sources/{id}/pause` | Stops polling and delivery until resumed; body `{"by": "..."}` is kept with the time |
| `POST /api/sources/{id}/resume` | Resumes and passes at once |
| `POST /api/sources/{id}/check-now` | Passes now instead of at the next poll |
| `GET /api/sources/{id}/files` | Files received, with duplicates and per-file sent/rejected counts |
| `GET /api/records?source=&status=&file_id=` | Records; `status` is `received`, `pending`, `sent` or `rejected` |
| `GET /api/records/{id}` | One record: file, line, original and corrected row, resources sent, HFS response |
| `POST /api/records/{id}/reprocess` | Runs a record again; body `{"raw": {...}}` replaces the row and keeps the original |
| `GET /api/alerts?source=` | Alerts, newest first |
| `GET /metrics` | Prometheus text: records by status, last received time, paused flag, alerts by kind |
| `GET /health` | `{"status": "ok"}` |

## Layout

```
src/            engine: config, sources (folder, S3), readers (CSV, NDJSON, Bundle), transform worker,
                FHIR delivery, pipeline, the source manager, API, alerts, storage
hie_runtime/    Python side of the transform protocol (worker.py, hie.DataError)
transforms/     Python transforms: lab_results.py (CSV to Observation), fhir_passthrough.py
sources/        one YAML file per source; the API writes here too
examples/data/  sample files used by the tests and the demo
tests/          end-to-end Rust tests with a stand-in FHIR server; Python tests for the transform
docs/           design decisions
```

## Tests

```sh
cargo test                                 # unit tests and the end-to-end tests
python -m unittest tests/test_lab_results.py
cargo clippy --all-targets -- -D warnings
```

The end-to-end tests run the real Python transforms against a stand-in FHIR server. `tests/pipeline.rs` covers a folder source: the outage and recovery, the duplicate file and the re-sent rows, each kind of bad row, reprocessing with a correction, and the silence alert. `tests/object_source.rs` covers the object-store path the S3 source uses, with an in-memory store. `tests/api.rs` drives a source through the API: test, create, check now, pause, resume, replace, delete.

They are skipped if no Python 3.8+ is found; set `HIE_TEST_PYTHON` to point at one.
