# Design decisions

Each decision states what was chosen, the evidence behind it, and what it costs. "Exercise" means the hands-on comparison of Apache Camel 4.22 and Open Integration Engine 4.6 run in October 2026: a lab CSV sent to HFS as `Observation`s, with bad rows, an HFS outage and re-sent files. "Demo" means the run in the [README](../README.md#what-the-prototype-does).

## 1. A separate service that talks to HFS only through the FHIR API

**Chosen:** HIE is its own repository and process. It writes to HFS with standard FHIR REST calls (`POST /` with a `batch` Bundle) and reads nothing from HFS internals.

**Why:**
- The analytics product that consumes the data is separate from HFS, with HIE between it and HFS/HTS.
- Talking FHIR means HIE works with any FHIR server, and HFS stays a plain FHIR server with nothing integration-specific inside it.

**Cost:** HIE cannot use HFS shortcuts (for example a direct database load). For very large feeds, `$bulk-submit` is the path (see 7).

## 2. Rust for the engine

**Chosen:** The engine (polling, storage, delivery, API, alerts) is a single Rust binary.

**Why:**
- It is the same toolchain, CI and dependency set as HFS and HTS: the crate uses the versions already in the HFS lockfile.
- It can later reuse `helios-fhir` types.
- One binary, a small memory footprint, no JVM.
- Both engines tried are Java. OIE needed a server, an embedded database and a desktop client, and the client failed three ways on Windows before it worked: launcher JVM options, missing JavaFX, classpath order.

**Cost:** Transform authors should not need Rust (see 3).

## 3. Transforms in Python, in a separate process

**Chosen:** A feed's transform is a Python file with a `transform(record, context)` function that returns FHIR resources or raises `DataError`. HIE runs it in a long-lived Python process and exchanges one JSON line per record (`hie_runtime/worker.py`).

**Why:**
- The plan is Python transforms first and FHIR Mapping Language later.
- Python is readable by the people who know the source data.
- Out of process, a transform that crashes or hangs cannot take the engine down: the worker is restarted, and the record stays queued.
- The line protocol is language-neutral, so a FHIR Mapping Language worker can be added later behind the same interface.
- The example transform uses only the standard library.

**Cost:** One inter-process round trip per record. Fine for lab volumes; a batched protocol is the next step if it shows up in measurements.

## 4. Feeds and transforms are files under version control

**Chosen:** One YAML file per feed (`feeds/*.yaml`) plus its Python transform, reviewed like code.

**Why:**
- In OIE the channel lives in the UI and its database; reviewing a change means exporting XML.
- Camel's YAML routes were easy to read and diff.
- Mapping rules are agreements with a data source and should have history and review.

**Cost:** No editing from a UI yet. A client UI could write feed definitions through an API later.

## 5. Every row is stored before delivery

**Chosen:** When a file arrives, each row is written to the database with its file name, file hash and line number. Then the file moves out of the inbox, and transformation and delivery run from the database.

**Why:**
- In the Camel exercise, with HFS down, the whole file went to the error folder, and nothing delivered it when HFS came back.
- Here the rows wait as `pending` and are delivered on the next retry. Demo §1–2: HFS down, 3 rows pending, HFS started, 3 delivered, nothing lost.

**Cost:** HIE keeps a copy of source data, which needs a retention policy.

## 6. Idempotent delivery: conditional update on a business identifier, plus file hashes

**Chosen:**
- Every resource carries an `identifier` (feed system + business key).
- Delivery uses `PUT {type}?identifier=system|value`, so a repeated record updates instead of creating.
- A file whose SHA-256 was already received is recorded as a duplicate and not reprocessed.

**Why:**
- In the exercise, re-sending a file doubled the data.
- A file failing on one row was re-read every half second, writing its good rows again each time: 20 copies in 10 seconds.
- HFS supports conditional update by identifier, including inside a Bundle: checked against HFS, 201 the first time and 200 after.
- Demo §3: the identical file is skipped, and the same rows in a new file leave HFS at 3 resources, the re-sent one at `versionId` 2.

**Cost:** Each feed needs a real business key from the source, such as an accession or order number plus the test code. The example uses `patient|code|date`, which would merge two same-day results; real feeds must provide a proper key.

## 7. `batch` Bundles, not `transaction`

**Chosen:**
- Records go to HFS in `batch` Bundles of up to `batch_size` entries.
- If HFS refuses a whole Bundle, the records are re-sent one by one to isolate the bad one.

**Why:**
- One bad row must not block the rest of a file. A batch gives a status per entry.
- Lab results do not need all-or-nothing per file.

**Cost:** No atomicity across a file. Feeds that need it (a resource and its dependents) would use `transaction`; very large feeds would use `$bulk-submit`.

## 8. Two kinds of failure, handled differently

**Chosen:**

| Kind | Examples | Handling |
|---|---|---|
| Data | Wrong column count, non-numeric value, unknown code or unit, missing identifier, a 4xx from HFS for that entry | Rejected at once. Kept with file, line, original row and reason. Reprocessable after a fix. |
| Transient | HFS unreachable, timeout, 429, 5xx | Retried with exponential backoff (2 s doubling to 5 min), never dropped. One alert after `alert_after_attempts`, one when delivery recovers. |

**Why:**
- In the exercise, retrying a bad row only multiplied writes, and treating an outage as a bad file stopped good data.
- Demo §4: 5 bad rows rejected, each with its reason, while the good row in the same file went through.

## 9. Validate in the transform; the engine checks only what it relies on

**Chosen:**
- The transform validates the source data: types, required columns, known codes and units, date format.
- The engine checks only `resourceType` and the identifier needed for idempotency.

**Why:**
- HFS accepts what it is given unless validation is enabled: an `Observation` with `status: "bogus"` was stored.
- Building FHIR by text templating (the Camel exercise) let `alto` reach the server, and could let crafted values inject fields.
- Building objects in code and failing early gives the precise reason ("value 'alto' is not a number").

**Later:** Optional profile validation before sending, through HFS `$validate` or with HFS validation enabled.

## 10. Every record is traceable, and corrections keep the original

**Chosen:** For each record HIE keeps:
- the file name, hash and line;
- the row as received;
- the corrected row, if it was fixed;
- the resources sent;
- HFS's response, with location and version.

A reprocess with corrected data keeps the original (`original_raw`).

**Why:** OIE's per-message history (status, error, source file, row) was the most useful thing either tool offered. It answers "where did this value come from, and what happened to it". Demo §5 shows a corrected row with both values and HFS's `201`.

## 11. Monitoring: an HTTP API for client UIs and Prometheus metrics

**Chosen:**
- `/api/...` serves feed status (freshness, counts, last alert), files, records and alerts for a client UI.
- `/metrics` exposes Prometheus gauges and counters.

**Why:**
- A data-sources page in a client UI needs per-feed freshness and completeness.
- Camel's counters were in memory and reset on restart. Here they come from the stored history.
- Prometheus is what HFS already exposes, so the same monitoring stack (Grafana or a hosted product) can watch both. Integrating a monitoring product stays an option, not a rewrite.

## 12. Alerts for data events in the engine; routing left to a product

**Chosen:**
- HIE raises alerts for rejected records, unreadable files, delivery outages and recovery, and **silence**: no data within the feed's `expect_every`.
- Alerts are stored, logged, and posted to a webhook (`text` field, the shape Slack and Teams accept).
- A repeated condition alerts once.

**Why:**
- OIE's alerts needed an SMTP server or a second channel, and they cannot detect a feed going quiet.
- Silence is the case the lab cares most about ("nothing has arrived since Tuesday").

**Not built:** Who gets paged, escalation and on-call schedules. Those belong to an alerting product fed by the webhook or the metrics. The recipients depend on the identity provider.

## 13. SQLite for the prototype, PostgreSQL next

**Chosen:** A single SQLite file (WAL mode).

**Why:**
- No setup, runs anywhere, enough for one engine instance.
- The schema is plain SQL with nothing SQLite-specific beyond types, so moving to PostgreSQL (the planned stack) is a driver change and a migration.

**Cost:** One instance at a time. Running several, or high availability, needs PostgreSQL.

## 14. Source robustness: encodings and malformed rows

**Chosen:**
- UTF-8, UTF-8 with a byte-order mark, and UTF-16 with a byte-order mark are decoded.
- Anything else marks the file `unreadable` and raises an alert.
- A row with the wrong number of columns is rejected alone, with the expected header in the message.

**Why:**
- In the exercise, a file written by Windows PowerShell (UTF-16) made Camel fail and retry it forever.
- A seventh column appearing in a feed was the example given for messy sources.

## Not in the prototype

- Connectors besides a folder: S3, database, another FHIR server.
- HL7v2 input.
- Code translation through HTS (`ConceptMap/$translate`) inside transforms.
- Patient matching (source identifier to `Patient` reference).
- Authentication to HFS (SMART backend services).
- `$bulk-submit` delivery (HFS fetches a manifest and NDJSON files, which can live on S3).
- PostgreSQL, multiple instances, a UI, FHIR Mapping Language.
