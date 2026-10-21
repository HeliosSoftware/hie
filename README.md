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
- HFS running at the URL in `hie.yaml` (default `http://localhost:8080`), with its search parameters loaded (`HFS_DATA_DIR`): a row delivered by business identifier is a conditional `PUT Type?identifier=...`, which HFS refuses with `400` when it does not know `identifier` for that type, and the engine then rejects the row as a data error.

```sh
cargo run -- check          # validate hie.yaml and sources/
cargo run                   # run every source, serve the API on 127.0.0.1:8090
cp examples/data/labs.csv inbox/lab-results/
curl localhost:8090/api/sources
```

### In Docker

Each release publishes a public Linux x86_64 image to `ghcr.io/heliossoftware/hie` (tags `:{version}`, `:{major.minor}`, `:latest`; see [Release and deploy](#release-and-deploy)). The image holds the engine, Python 3, `hie_runtime/`, and the example `transforms/` and `sources/` under `/hie`.

The `hie.yaml` baked into the image listens on `0.0.0.0:8090`, so `docker run -p 8090:8090 ghcr.io/heliossoftware/hie:latest` serves the API, but it expects HFS at `localhost:8080`, which inside a container is the container itself. To deliver anywhere, mount your own config. To run HIE and HFS side by side on one Docker network:

```sh
mkdir -p inbox/lab-results data
cat > hie.docker.yaml <<'EOF'
fhir_base_url: http://hfs:8080   # HFS as this container sees it
public_url: http://hie:8090      # HIE as HFS sees it (for $bulk-submit)
database: data/hie.db
listen: 0.0.0.0:8090             # all interfaces, so -p can reach it
sources_dir: sources
transforms_dir: transforms
EOF

docker network create hie-net
docker run -d --name hfs --network hie-net -p 8080:8080 ghcr.io/heliossoftware/hfs:latest
docker run -d --name hie --network hie-net -p 8090:8090 \
  -v $PWD/hie.docker.yaml:/hie/hie.yaml:ro \
  -v $PWD/inbox:/hie/inbox \
  -v $PWD/data:/hie/data \
  ghcr.io/heliossoftware/hie:latest

curl localhost:8090/health                        # {"status":"ok","service":"hie","version":...}
cp examples/data/labs.csv inbox/lab-results/      # picked up within 5 s
curl localhost:8090/api/sources/lab-results       # records.sent: 3
curl 'localhost:8080/Observation?_summary=count'  # total: 3
```

If HFS runs on the host instead, drop `--network`, use `fhir_base_url: http://host.docker.internal:8080`, and add `--add-host=host.docker.internal:host-gateway` on Linux.

| Mount | Holds |
|---|---|
| `/hie/hie.yaml` | the engine configuration (required, see above) |
| `/hie/inbox` | the folders the example folder sources watch; processed files move to `processed/` |
| `/hie/data` | the SQLite store: records, delivery state, alerts. Without it, history is lost with the container |
| `/hie/sources`, `/hie/transforms` | optional: your own sources and transforms, and anything created through the API, kept across upgrades |

Other commands: `docker run --rm ghcr.io/heliossoftware/hie:latest --version`, and `... check` with the same mounts to validate the config and sources. The container runs as root, so files it writes to the mounts are owned by root. Pass the `HIE_AUTH_*` variables (see [Authentication](#authentication)) and AWS credentials with `-e`.

To build the image yourself: `docker build -t hie .`

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
expect_every: 24h            # the silence rule the source is born with
```

An S3 source names a `bucket`, an optional `prefix`, and an `archive_prefix` (default `processed`) that acknowledged objects are moved under. `endpoint` points at an S3-compatible store such as MinIO.

### Delivery methods

`delivery.method` picks how a source's records reach HFS:

- **`rest`** (default): the transform runs on every record and the resources go out in `batch` Bundles, one `PUT` per record, with a status per entry. For anything that needs a transform, and for feeds of hundreds to tens of thousands of records at a time.
- **`bulk_submit`**: for data that already is FHIR with ids, such as an NDJSON export. No transform runs. The engine stores the rows for the history, predicts what HFS will refuse (no `resourceType`, no `id`), and hands the **source objects themselves** to HFS through a `$bulk-submit` manifest it serves at `{public_url}/bulk/{token}/manifest.json`: pre-signed URLs for S3 objects, the engine's own `/bulk/{token}/files/{n}` for folder files. One submission per drop (up to `manifest_objects_max` objects), with a `submissionId` derived from the files so a repeated drop is refused by HFS instead of ingested twice. The engine polls the status URL, reads HFS's receipts (`{"reference": "Type/id"}` per stored resource, an `OperationOutcome` per failure) and maps them to its records by `Type/id`. Objects stay at the source until the receipts are in, then they are archived. `url_ttl_seconds` bounds how long the URLs stay valid.

`public_url` in `hie.yaml` must be the URL HFS reaches the engine at.

The same document is what `POST /api/sources` takes. Before creating one, `POST /api/sources/test` with `{"source": {...}}` lists the connection once and answers with the evidence: reachable or not, in how many milliseconds, how many objects match, and the newest one.

### Alerts and rules

Alerts are governed by **rules**: data, created and edited over the API, each naming a source (or every source), a condition with its threshold, who to notify, and the channels (`app`, `webhook`, `email`). Every source is born with one `silence` rule set to its `expect_every` (two hours when it has none), so nobody has to remember to watch it.

| Condition | Threshold | Fires when | Clears when |
|-----------|-----------|------------|-------------|
| `silence` | a duration, `2h` | nothing has arrived for longer than that | data arrives |
| `missing_by` | a time of day in UTC, `06:00` | that time passes and nothing arrived today | the file arrives |
| `rejected_share` | a percentage, `10%` | a load has more than that share of rows rejected | a later load is within the limit |
| `unreachable` | a number of attempts, `5` | delivery to HFS has failed that many times in a row | delivery succeeds |

A condition raises **one** alert per rule and source, open until it clears itself; the alert keeps its rule, who was notified, the channels, and the clearing time and reason. `app` alerts are what the UI lists; `webhook` posts them to `alerts.webhook_url` (the `text` field is the shape Slack and Teams accept); `email` is accepted and recorded, with no mail sink wired yet. A source with no `rejected_share` rule alerts on every load that rejects rows, and one with no `unreachable` rule alerts after `delivery.alert_after_attempts`.

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
| `GET /api/sources/{id}/loads` (also `/files`) | Every load: the file, its method, records read, sent, rejected, in progress, how long it took, where it is archived |
| `GET /api/loads/{id}` | One load with its **failures grouped by reason**: kind, reason, count, first line, and up to three sample rows each |
| `POST /api/loads/{id}/retry` | Puts the load's rejected rows back at the start, all of them or those with one reason (`{"reason": "..."}`); the source passes at once |
| `GET /api/sources/{id}/stats?days=14` | What a source's page draws: today (files, records, sent, rejected, pending) and one row per day |
| `GET /api/transforms` | Every transform file: name, path, size, modified, and the sources that use it |
| `GET /api/transforms/{name}` | The transform's code |
| `PUT /api/transforms/{name}` | Saves the code (`{"code": "..."}`); the sources using it restart their Python worker. `201` when new |
| `POST /api/transforms/{name}/try` | Runs the transform on `{"records": [...]}` (up to 100) without loading: per record, the resources it became or the rejection reason |
| `GET /api/monitoring/status` | What the monitoring page opens on: `needs_you` (late, paused, or rejecting sources with the reason), `last_24h` (files, sent, rejected, alerts), every source |
| `GET /api/records?source=&status=&file_id=` | Records; `status` is `received`, `pending`, `sent` or `rejected` |
| `GET /api/records/{id}` | One record: file, line, original and corrected row, resources sent, HFS response |
| `POST /api/records/{id}/reprocess` | Runs a record again; body `{"raw": {...}}` replaces the row and keeps the original |
| `GET /api/alerts?source=&open=true` | Alerts, newest first, each with its rule, recipients, channels, and when and why it cleared; `open=true` lists only those not cleared |
| `GET /api/alert-rules?source=` | Every rule; `POST` creates one (`400` names what is wrong with it) |
| `GET` / `PUT` / `DELETE /api/alert-rules/{id}` | One rule: read, replace, remove |
| `GET /metrics` | Prometheus text: records by status, last received time, paused flag, alerts by kind |
| `GET /health` | `{"status": "ok", "service": "hie", "version", "uptime_seconds", "started_at", "timestamp"}` |

## Authentication

With `HIE_AUTH_ENABLED=true`, every request under `/api` needs a bearer token signed by the identity provider: a JWT validated against `HIE_AUTH_JWKS_URL` with the expected `HIE_AUTH_ISSUER` and, when set, `HIE_AUTH_AUDIENCE`. This is the same validation HFS performs, through its `helios-auth` crate. `/health` and `/metrics` stay open. A missing or invalid token answers `401` with `WWW-Authenticate: Bearer`. A pause without a `by` in the body records the token's subject.

| Variable | Default | Purpose |
|---|---|---|
| `HIE_AUTH_ENABLED` | `false` | Require bearer tokens under `/api` |
| `HIE_AUTH_JWKS_URL` | | The identity provider's JWKS |
| `HIE_AUTH_ISSUER` | | Expected `iss`; required when enabled |
| `HIE_AUTH_AUDIENCE` | | Expected `aud`, when the provider sets one |
| `HIE_AUTH_ALGORITHMS` | `RS256,RS384,ES256,ES384` | Accepted signing algorithms |
| `HIE_AUTH_JWKS_MIN_REFRESH_INTERVAL` | `10` | Seconds between JWKS refreshes |

What a token may do is decided by its scopes (the `scope`, `scp` or `roles` claim, as HFS reads them):

| Scope | Grants |
|---|---|
| `hie/read` | Every `GET`: sources, loads, records, transforms, alerts, rules, monitoring |
| `hie/manage` | Changing sources, rules, loads and records (`POST`, `PUT`, `DELETE`, pause, resume, retry, reprocess, try), and reading |
| `hie/transforms` | `PUT /api/transforms/{name}`: code the engine then runs; and reading |
| `hie/*` | Everything |

A valid token without the scope a request needs answers `403` with the scope in `required`. A token from the same identity provider that carries only HFS scopes (`system/*.cruds`) is authenticated but may do nothing here.

`docker/keycloak/` holds a Keycloak for local work: HFS's `fhir` realm (its `hfs-backend-client`, which the engine uses towards HFS) plus the `hie/*` client scopes and three service-account clients, `hie-admin-client` (`hie/*`), `hie-operator-client` (`hie/manage`) and `hie-monitor-client` (`hie/read`), all with audience `hie-api`. `docker compose -f docker/keycloak/docker-compose.yml up -d` serves it on 8180; `docker/keycloak/get-token.sh [client]` prints a token. The engine then runs with `HIE_AUTH_JWKS_URL=http://localhost:8180/realms/fhir/protocol/openid-connect/certs`, `HIE_AUTH_ISSUER=http://localhost:8180/realms/fhir`, `HIE_AUTH_AUDIENCE=hie-api`, and `HIE_FHIR_TOKEN_URL=http://localhost:8180/realms/fhir/protocol/openid-connect/token` with `hfs-backend-client` / `hfs-backend-secret` towards an HFS configured for the same realm.

With auth off the engine logs a warning at start: `PUT /api/transforms/{name}` writes code the engine then runs, so an open API must only be reachable from trusted clients.

### The engine's identity towards HFS

The engine calls HFS as itself, with an OAuth2 **client-credentials** grant at the identity provider's token endpoint (Keycloak first; the same providers HFS accepts). The token is cached and renewed a minute before it expires; a `401` or `403` from HFS discards it, the rows wait, and the next pass goes out with a fresh one. A token endpoint that cannot be reached or refuses the client is a transient delivery failure, so the data is kept and the outage alerts say why. A wrong endpoint or secret fails at startup.

| Variable | Default | Purpose |
|---|---|---|
| `HIE_FHIR_TOKEN_URL` | | The provider's token endpoint, e.g. `https://idp/realms/hfs/protocol/openid-connect/token` |
| `HIE_FHIR_CLIENT_ID` | | The engine's client |
| `HIE_FHIR_CLIENT_SECRET` | | Its secret |
| `HIE_FHIR_SCOPE` | | Scopes to ask for, when the provider needs them, e.g. `system/*.cruds` |

With none of them set the engine calls HFS anonymously and logs a warning at start, which is what the public sandbox does against the public HFS. Setting some but not all three is refused.

## Release and deploy

A version tag (`v0.1.0`) runs `.github/workflows/release.yml`, modelled on the HFS release chain (see [RELEASING.md](RELEASING.md) for how to cut one):

1. **Build**: the Linux x86_64 release binary, packaged with `hie_runtime/`, `transforms/`, `sources/` and `examples/` as `hie-{version}-x86_64-unknown-linux-gnu.tar.gz`.
2. **Release**: a GitHub Release with the archive and its SHA-256.
3. **Image**: `ghcr.io/heliossoftware/hie:{version}`, `:{major.minor}`, `:sha-{sha}`, and `:latest` for a non-prerelease.
4. **Deploy**: from the `Deploy` runner over SSH to the host behind `hie.heliossoftware.com`: the binary and runtime go to `/opt/hie`, a systemd unit `hie` runs it on `0.0.0.0:8090`, and the job fails unless `/health` answers. `data/` and anything created through the API in `sources/` and `transforms/` survive a deploy.

Secrets the deploy needs: `HIE_DEPLOY_HOST`, `HIE_DEPLOY_USER`, `HIE_DEPLOY_PORT` (optional) and `HIE_FHIR_BASE_URL` (defaults to the public HFS). The public sandbox at `hie.heliossoftware.com` is deployed open, like the public HFS: a place to show the work and run demos, with nothing sensitive on it. A secure environment adds `HIE_AUTH_JWKS_URL`, `HIE_AUTH_ISSUER` and `HIE_AUTH_AUDIENCE` for the API, and `HIE_FHIR_TOKEN_URL`, `HIE_FHIR_CLIENT_ID`, `HIE_FHIR_CLIENT_SECRET` (and `HIE_FHIR_SCOPE`) for the engine's calls to HFS; the deploy writes them to the root-only `/etc/hie/hie.env` on the host.

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

The end-to-end tests run the real Python transforms against a stand-in FHIR server. `tests/pipeline.rs` covers a folder source: the outage and recovery, the duplicate file and the re-sent rows, each kind of bad row, reprocessing with a correction, and the silence alert. `tests/alerts.rs` covers the rules: the one a source is born with, the CRUD, and the `rejected_share` and `silence` rules firing once and clearing. `tests/object_source.rs` covers the object-store path the S3 source uses, with an in-memory store. `tests/api.rs` drives a source through the API: test, create, check now, pause, resume, replace, delete.

They are skipped if no Python 3.8+ is found; set `HIE_TEST_PYTHON` to point at one.
