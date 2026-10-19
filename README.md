# Helios Integration Engine

Gets data from source feeds into HFS, the Helios FHIR Server:
- **Feeds:** a folder or an S3 bucket today; databases and other FHIR servers later.
- **Formats:** CSV, NDJSON, FHIR Bundle files.
- **Transforms:** in Python.
- **Delivery:** idempotent, with retries.
- **Around it:** per-record traceability, an API for client UIs, Prometheus metrics, and alerts that include "this feed went quiet".

The engine is a .NET 10 service that runs on Linux (see the `Dockerfile`). This is a working prototype. The reasons behind each choice are in [docs/decisions.md](docs/decisions.md); the architecture is discussed in [the architectural discussion](https://github.com/HeliosSoftware/hie/discussions/1), and the choice of stack in [discussion #2](https://github.com/HeliosSoftware/hie/discussions/2).

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
| **6.** No data within `expect_every` | One `silence` alert, not one per check (covered by the end-to-end test). |

And with Synthea output for three patients, in both of its FHIR shapes:

| Feed | Input | Result |
|---|---|---|
| `fhir_bundle` | 3 transaction Bundles (394 to 700 entries each, `urn:uuid` references between entries) | 1,775 resources delivered by id, 0 rejected. References rewritten to `Type/id`. |
| `ndjson` | 17 files, one per resource type, from Synthea's bulk-data export | 1,775 resources delivered, 0 rejected. |
| Both together | The same patients twice, in two formats | HFS holds each resource once: 3 `Patient`, 214 `Encounter`, 415 `Observation`. |

## Run it

You need:
- the .NET 10 SDK;
- Python 3.8 or later (`py` on Windows, `python3` elsewhere; set `python:` in `hie.yaml` to change it);
- HFS running at the URL in `hie.yaml` (default `http://localhost:8080`).

```sh
dotnet run --project src/Hie -- check     # validate hie.yaml and feeds/
dotnet run --project src/Hie              # watch feeds, serve the API on 127.0.0.1:8090
cp examples/data/labs.csv inbox/lab-results/
curl localhost:8090/api/feeds
```

On Linux, in a container:

```sh
docker build -t hie .
docker run --rm -p 8090:8090 -v $PWD/inbox:/hie/inbox -e HIE_LOG=Information hie
```

| Variable | Default | Purpose |
|---|---|---|
| `HIE_CONFIG` | `hie.yaml` | Engine configuration file |
| `HIE_RUNTIME_DIR` | `hie_runtime` next to the config | Location of the Python worker |
| `HIE_LOG` | `Information` | Minimum log level (`Debug`, `Warning`, ...) |
| `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION` | | Credentials for S3 sources, as for any AWS tool (profiles and roles work too) |

## Adding a feed

A feed is a YAML file in `feeds/` plus a transform:

```yaml
id: lab-results
name: Lab results
source:
  type: folder               # or s3 (see feeds/synthea-s3.yaml.example)
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
src/Hie/        the engine: Config, Sources (folder, S3), Reader (CSV, NDJSON, Bundle), Transformer (Python worker),
                Fhir (delivery), Pipeline (feed runner), Api, Alerts, Db (SQLite)
tests/Hie.Tests xUnit: unit tests, and end-to-end tests against a stand-in FHIR server
hie_runtime/    Python side of the transform protocol (worker.py, hie.DataError)
transforms/     Python transforms: lab_results.py (CSV to Observation), fhir_passthrough.py
feeds/          one YAML file per feed
examples/data/  sample files used by the tests and the demo
docs/           design decisions
```

## Tests

```sh
dotnet test
python -m unittest tests/test_lab_results.py
```

The end-to-end tests run the real Python transforms against a stand-in FHIR server that can be taken down. They cover a folder feed (the outage and recovery, the duplicate file and the re-sent rows, each kind of bad row, reprocessing with a correction, the silence alert) and the object-store path the S3 source uses, with an in-memory store. They are skipped if no Python 3.8+ is found; set `HIE_TEST_PYTHON` to point at one.
