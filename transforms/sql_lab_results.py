"""Lab results read from a database -> FHIR R4 Observation.

The query (see sources/lab-database.yaml.example) returns one row per result with
the columns: id, patient, code, display, value, unit, taken_at. Values come typed
from the database (numbers as numbers, timestamps as ISO text), unlike a CSV.
"""

from hie import DataError

REQUIRED = ("id", "patient", "code", "value", "unit", "taken_at")


def transform(record, context):
    missing = [c for c in REQUIRED if record.get(c) in (None, "")]
    if missing:
        raise DataError(f"missing {', '.join(missing)}")
    value = record["value"]
    if isinstance(value, str):
        try:
            value = float(value)
        except ValueError:
            raise DataError(f"value {value!r} is not a number") from None
    if not isinstance(value, (int, float)):
        raise DataError(f"value {value!r} is not a number")
    patient = str(record["patient"])
    if not patient.startswith("Patient/"):
        patient = f"Patient/{patient}"
    coding = {"system": "http://loinc.org", "code": str(record["code"])}
    if record.get("display"):
        coding["display"] = record["display"]
    return {
        "resourceType": "Observation",
        "id": f"lab-{record['id']}",
        "status": "final",
        "code": {"coding": [coding]},
        "subject": {"reference": patient},
        "effectiveDateTime": str(record["taken_at"]),
        "valueQuantity": {
            "value": value,
            "unit": record["unit"],
            "system": "http://unitsofmeasure.org",
            "code": record["unit"],
        },
    }
