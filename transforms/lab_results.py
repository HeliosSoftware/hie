"""Lab results CSV -> FHIR R4 Observation.

Columns: patient, code, display, value, unit, date.
`code` may be a LOINC code or a local code listed in LOCAL_CODES.
"""

import re

from hie import DataError

IDENTIFIER_SYSTEM = "urn:helios:hie:lab-results"

LOCAL_CODES = {
    "GLU": ("2345-7", "Glucose [Mass/volume] in Serum or Plasma"),
    "HGB": ("718-7", "Hemoglobin [Mass/volume] in Blood"),
}

UCUM = {
    "mg/dL": "mg/dL",
    "g/dL": "g/dL",
    "mmol/L": "mmol/L",
}

REQUIRED = ("patient", "code", "value", "unit", "date")
LOINC = re.compile(r"^\d{1,7}-\d$")
DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def number(text):
    try:
        value = float(text)
    except ValueError:
        raise DataError(f"value {text!r} is not a number") from None
    return int(value) if "." not in text and value.is_integer() else value


def transform(record, context):
    missing = [c for c in REQUIRED if not record.get(c)]
    if missing:
        raise DataError(f"missing {', '.join(missing)}")

    code, display = record["code"], record.get("display") or None
    if code.upper() in LOCAL_CODES:
        code, display = LOCAL_CODES[code.upper()]
    elif not LOINC.match(code):
        raise DataError(f"test code {code!r} is neither LOINC nor in the local code table")

    unit = record["unit"]
    if unit not in UCUM:
        raise DataError(f"unit {unit!r} has no UCUM mapping")

    date = record["date"]
    if not DATE.match(date):
        raise DataError(f"date {date!r} is not YYYY-MM-DD")

    patient = record["patient"]
    if not patient.startswith("Patient/"):
        raise DataError(f"patient {patient!r} is not a Patient reference")

    coding = {"system": "http://loinc.org", "code": code}
    if display:
        coding["display"] = display

    return {
        "resourceType": "Observation",
        "identifier": [{"system": IDENTIFIER_SYSTEM, "value": f"{patient}|{code}|{date}"}],
        "status": "final",
        "category": [
            {
                "coding": [
                    {
                        "system": "http://terminology.hl7.org/CodeSystem/observation-category",
                        "code": "laboratory",
                    }
                ]
            }
        ],
        "code": {"coding": [coding]},
        "subject": {"reference": patient},
        "effectiveDateTime": date,
        "valueQuantity": {
            "value": number(record["value"]),
            "unit": unit,
            "system": "http://unitsofmeasure.org",
            "code": UCUM[unit],
        },
    }
