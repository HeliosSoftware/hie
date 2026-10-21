# Design decisions

Each decision states what was chosen, the evidence behind it, and what it costs. "Exercise" means the hands-on comparison of Apache Camel 4.22 and Open Integration Engine 4.6 run in October 2026: a lab CSV sent to HFS as `Observation`s, with bad rows, an HFS outage and re-sent files. "Demo" means the run in the [README](../README.md#what-the-prototype-does).

## 1. A separate service that talks to HFS only through the FHIR API

**Chosen:** HIE is its own repository and process. It writes to HFS with standard FHIR REST calls (`POST /` with a `batch` Bundle) and reads nothing from HFS internals.

**Why:**
- The analytics product that consumes the data is separate from HFS, with HIE between it and HFS/HTS.
- Talking FHIR means HIE works with any FHIR server, and HFS stays a plain FHIR server with nothing integration-specific inside it.

**Cost:** HIE cannot use HFS shortcuts (for example a direct database load). For very large feeds, `$bulk-submit` is the path (see 7).

## 2. Rust for the engine

**Chosen:** The engine (polling, storage, delivery, API, alerts) is a single Rust binary, built and run on Linux (see the `Dockerfile` and the CI workflow).

**Why:**
- HIE is a separate product from the applications built on HFS: the integration work is messy and error-prone, and it is kept in its own service that those applications call through an API. Because it is independent, it does not inherit the stack of any one client application. [Discussion #2](https://github.com/HeliosSoftware/hie/discussions/2) has the comparison with .NET and the record of the decision, which was taken for .NET on 2026-10-19 and returned to Rust on 2026-10-20 on that reasoning.
- It is the same toolchain, CI and dependency set as HFS and HTS: the crate uses the versions already in the HFS lockfile, and it can later reuse `helios-fhir` types and the shared auth and observability crates.
- One static binary, a small memory footprint, no runtime to install.
- Both engines tried are Java. OIE needed a server, an embedded database and a desktop client, and the client failed three ways on Windows before it worked: launcher JVM options, missing JavaFX, classpath order.

**Cost:**
- Transform authors should not need Rust (see 3).
- The client application's services run under Aspire; the engine joins that as a container, and emits OpenTelemetry so the same dashboard covers it (to be wired, as in HFS's `helios-observability`).

A complete .NET 10 port of the engine exists in the history (commit `c3272d4`), test for test, and was green on Linux; it is the fallback if the decision changes again.

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
- A repeated condition alerts once. (Superseded in part by decision 22: conditions are rules, and an alert clears itself.)

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

## 15. Two delivery keys: the resource's `id`, or a business identifier

**Chosen:** A resource that carries an `id` is written to `PUT {type}/{id}`. One without needs an `identifier` and is written as a conditional update on it. A resource with neither is rejected.

**Why:**
- Data that already is FHIR (an NDJSON export, a Synthea Bundle) comes with ids that other resources reference. Writing it under those ids keeps the references valid, and re-sending the same export updates instead of duplicating: the same three patients delivered as Bundles and again as NDJSON left HFS with each resource once.
- Data mapped from a non-FHIR source has no id to offer; the business identifier is what makes it idempotent (decision 6).

**Cost:** A transform that wants a conditional update must not set `id`.

## 16. A Bundle is split into records, with references made absolute

**Chosen:** A `fhir_bundle` file is read as one record per entry. Before that, references to another entry's `fullUrl` (`urn:uuid:...`, which Synthea writes) are rewritten to that entry's `Type/id`.

**Why:**
- Per-record traceability (decision 10) needs one record per resource, not one per file.
- Delivered independently, the entries would otherwise keep `urn:uuid` references that only make sense inside the transaction.
- Posting the Bundle as a transaction would keep the references intact but lose the per-resource history and the per-entry disposition; it stays an option for feeds that need atomicity (decision 7).

**Cost:** References to entries that carry no `id` cannot be rewritten and are passed through unchanged.

## 17. S3 through `object_store`

**Chosen:** The S3 source uses the `object_store` crate (the AWS implementation), listing a prefix, fetching objects and renaming them under an archive prefix.

**Why:**
- `object_store` is already in the HFS dependency set (SQL-on-FHIR uses it), and one trait covers S3, S3-compatible stores (MinIO, through `endpoint`), the local filesystem and an in-memory store, so the same source code is tested in memory and runs against a bucket.
- Credentials come from the standard AWS environment, like every other AWS tool an operator runs.

**Not yet verified:** The source has not been run against a real bucket; the object-store path is covered by the in-memory tests. That is the first thing to do once a bucket exists.

## 18. Sources are API resources, persisted as the same files

**Chosen:** `POST /api/sources` creates a source from its definition (the YAML document as JSON), the engine writes `sources/{id}.yaml` and starts the source without a restart; `PUT` replaces and restarts it; `DELETE` stops it and removes the file. Pause and resume are operational state kept in the database with who and when, not in the definition. `check-now` makes a source pass at once. `POST /api/sources/test` lists a connection once and returns the evidence (reachable, elapsed, objects, newest).

**Why:**
- The client UI's design configures a source in the application, in five steps, with a connection test whose result stays on the page, and pauses, resumes and re-checks a source from its page. All of that needs the engine to own the definition.
- Decision 4 still holds: the engine writes the same files it reads, so definitions stay under version control, diffable, and usable without the API.
- Pause is not part of the definition because it is a fact about operations ("paused by X since Y"), it must survive a restart, and it must not appear as a change to the source's file.

**Cost:** Two sources of truth for a definition while a process runs (the file and memory); the engine resolves it by re-reading the file when it reports a definition, and by rewriting the file before restarting a source. Concurrent edits of the same file by a person and the API are not arbitrated.

## 19. Transforms and monitoring over the API, preliminary

**Chosen:** Transforms are listed, read, saved and tried through `/api/transforms`; a save restarts the sources that use the transform. Monitoring is one aggregate (`/api/monitoring/status`) plus per-source day series (`/api/sources/{id}/stats`), both computed from the engine's store.

**Why:** The client UI's Transforms and Monitoring modules need these to render anything, and the assessment in discussion #1 lists them as the next additions after sources. "Try it" reuses the same Python worker as a real load, so the test is the real thing.

**Not yet:** transform versions and "used by since"; failures grouped by reason with the transform line; alert rules with recipients.

## 20. Bearer tokens on the API, validated as HFS validates them

**Chosen:** With `HIE_AUTH_ENABLED=true`, everything under `/api` requires a JWT validated against the identity provider's JWKS with the expected issuer and audience, through HFS's `helios-auth` crate; `/health` and `/metrics` stay open. Keycloak first, and the same providers HFS supports after it. The public sandbox follows the public HFS: open, for showing the work and for demos, with nothing sensitive on it; a secure environment sets the `HIE_AUTH_*` secrets and the deploy turns validation on.

**Why:**
- Saving a transform is running code on the engine; an open API on a public host is remote code execution for anyone who finds it.
- The same crate and the same claims as HFS mean one identity provider, one token, and the same `HFS_AUTH_*`-shaped settings an operator already knows.
- The subject of the token is what the UI's "paused by" needs, so it is recorded.

**Not yet:** per-user audit of changes. Scopes are decision 24. The engine's own credentials for calling HFS are decision 23.

## 21. `$bulk-submit` points HFS at the source objects, and receipts map by `Type/id`

**Chosen:** On `delivery.method: bulk_submit` the engine does not rewrite files. It stores the rows, runs a pre-flight in Rust instead of the transform (`resourceType` and `id` present), and serves a manifest whose entries are pre-signed URLs of the S3 objects, or the engine's own file URLs for a folder. The kick-off closes the submission (`submissionStatus: completed`); the engine polls `$bulk-submit-status`, reads the receipt files, marks a record `sent` when a `{"reference": "Type/id"}` for it appears and `rejected` otherwise, then archives the objects. One submission per drop, a `submissionId` derived from the files, `409` from HFS treated as "already held".

**Why:**
- It is the picture agreed in discussion #1 for an NDJSON export in a bucket: HFS does the ingestion, the engine keeps the history and the dispositions.
- HFS's receipts carry `Type/id` for every stored resource but **no input line number or file** on the error side (the line number exists inside HFS and is not published). Mapping by `Type/id` is what the published receipts allow; the engine's pre-flight is what names the line for the failures it can predict. HFS publishing the line in its error `OperationOutcome` would let the rest be named too; that is requested on the HFS side.
- Objects stay at the source until the receipts are in, so a retry or a replacement manifest can point at them again, and a pre-signed URL never points at an archived key.

**Cost:**
- A resource without an `id` cannot go this way; the pre-flight rejects it and says so.
- A line HFS refuses that the pre-flight did not predict is rejected with "no receipt", plus HFS's first error text, without a certain attribution.
- The engine's `public_url` must be reachable from HFS, and the folder-file route must stay open (unguessable token in the path).

## 22. Alert rules are data, and an alert clears itself

**Chosen:** A rule names a source (or all of them), one of four conditions with its threshold — `silence` (duration), `missing_by` (time of day, UTC), `rejected_share` (percentage of a load), `unreachable` (failed delivery attempts) — who to notify, and the channels (`app`, `webhook`, `email`). Rules are rows in the engine's database with a CRUD under `/api/alert-rules`. A condition raises **one** alert per rule and source, and the engine closes it when the condition goes away: data arrives, the file arrives, a later load is within the limit, delivery succeeds. The alert keeps its rule, the recipients, the channels, and when and why it cleared. Every source gets a `silence` rule at birth, set to its `expect_every` or two hours. Without a `rejected_share` or `unreachable` rule the fixed behaviour of decision 12 stays.

**Why:**
- The four conditions are the ones the monitoring brief asks the UI to offer, with the thresholds in the units a person would type.
- An alert that clears itself is what lets a list of *open* alerts mean something; an alert that never closes has to be reasoned about by its timestamp.
- Rules as data rather than fields of the source definition: a rule may cover every source, an operator may add one without touching the YAML a developer owns, and the UI edits them on their own page.
- The default rule keeps the promise of decision 12 that a quiet source is noticed without anyone configuring it.

**Cost / not yet:** `email` is recorded but not sent; the webhook is still the one outward channel. Recipients are free text; nothing resolves them against the identity provider. Rules are evaluated once per pass of the source, so a `silence` or `missing_by` alert is late by up to one poll interval.

## 23. The engine calls HFS with its own client-credentials token

**Chosen:** `HIE_FHIR_TOKEN_URL`, `HIE_FHIR_CLIENT_ID`, `HIE_FHIR_CLIENT_SECRET` and an optional `HIE_FHIR_SCOPE` name an OAuth2 client at the identity provider. One token source serves every request the engine makes to HFS: batch Bundles, `$bulk-submit` kick-offs, status polls and receipt files. The token is cached and renewed a minute before expiry; a `401`/`403` from HFS discards it and is treated as a transient failure, like an outage. Absent settings mean anonymous calls, with a warning at start; partial settings are refused.

**Why:**
- Any environment beyond the public sandbox has an HFS that requires a token; without this the engine could only deliver to the open one.
- Client credentials is what Keycloak, Entra ID and Okta all offer for a service identity, and what HFS validates today; the same token source will take a private-key JWT (SMART backend services) when HFS asks for one.
- A refused token must not reject the data. It is the same situation as HFS being down: wait, retry, and tell someone.

**Not yet:** private-key JWT instead of a shared secret; a token per tenant when one engine serves several; scopes narrowed per source.

## 24. Three scopes on the API: read, manage, transforms

**Chosen:** With auth on, a request needs a scope decided by its method and path: `hie/read` for every `GET`; `hie/transforms` for `PUT /api/transforms/{name}`; `hie/manage` for every other write. The write scopes also read; `hie/*` grants everything. Scopes are read from the `scope`, `scp` or `roles` claim through `helios-auth`, so Keycloak client scopes, Okta scopes and Entra ID app roles all work. A token without the needed scope answers `403` naming it.

**Why:**
- The monitoring brief has people who only watch; they should not hold a token that can rewrite a source.
- Saving a transform is running code on the engine. It is the one action worth its own scope, so an operator who manages sources and alert rules cannot ship code, and a developer who writes transforms cannot pause a feed.
- Three names rather than a SMART `hie/Source.cruds` grammar: the API is not FHIR resources, and the brief's roles map onto three verbs.
- A token that carries only HFS scopes is refused here on purpose: the engine and HFS are different universes, even when one identity provider serves both.

**Not yet:** per-source grants (a token for one source only); a `user/` context tied to who created the source; audit of who changed what.

## 25. A FHIR server as a source: pulled by `_lastUpdated`, one cursor per type

**Chosen:** `source.type: fhir_server` names a server, the resource types to pull, and how to authenticate (a client-credentials grant or a fixed bearer token, the secret always read from an environment variable the file names). Each pass searches every type for `_lastUpdated=gt{cursor}`, sorted by `_lastUpdated`, page by page up to `max_pages`, and hands the resources on as one NDJSON load per type. The cursor is the newest `meta.lastUpdated` seen and moves only after the engine has stored the records. The load's format is forced to `ndjson`, resources are delivered by their id, and `$bulk-submit` is refused for this source. The connection test reads `/metadata` and counts what waits past each cursor.

**Why:**
- Migrating from or mirroring another FHIR server is a connector customers ask for first, and it needs nothing new on the wire: search and paging that every server has.
- `_lastUpdated` with a cursor is the one incremental mechanism that works on any server; `_history` and Subscriptions are not everywhere, and `$export` is a different pipeline (decision 21).
- Moving the cursor after storing, not after fetching, means a crash repeats a page instead of losing it; idempotent `PUT`s make the repeat harmless.
- One cursor per type rather than one for the server, because servers page each type independently and a slow type should not hold back the others.

**Cost / not yet:** resources with equal `lastUpdated` at a page boundary rely on `_sort` being honoured; a server that ignores `_sort` must be drained in one pass (`max_pages` high enough). Deleted resources are not seen (`_history` would be needed). `since` is the only way to skip old data.

## Not in the prototype

- Connectors besides a folder, S3 and another FHIR server: a database.
- HL7v2 input.
- Code translation through HTS (`ConceptMap/$translate`) inside transforms.
- Patient matching (source identifier to `Patient` reference).
- Authentication to HFS with a private-key JWT (SMART backend services); client credentials are in.
- `$bulk-submit` delivery (HFS fetches a manifest and NDJSON files, which can live on S3).
- PostgreSQL, multiple instances, a UI, FHIR Mapping Language.
