# What the engine does, shown

Scenarios run against a real Helios FHIR Server, with what happened. Back to the [README](../README.md).

## Scenarios

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
