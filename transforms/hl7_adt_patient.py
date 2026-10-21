"""HL7 v2 ADT (A01, A04, A08, A28, A31) -> FHIR R4 Patient.

The record is the parsed message (see hie.hl7). The Patient is delivered as a
conditional update on the MRN in PID-3, so the same patient from any message
updates one resource.
"""

from hie import DataError, hl7, hl7_datetime

MRN_SYSTEM = "urn:helios:hie:mrn"
GENDER = {"M": "male", "F": "female", "O": "other", "U": "unknown"}


def transform(record, context):
    kind = str(record.get("message_type", ""))
    if not kind.startswith("ADT^"):
        raise DataError(f"not an ADT message: {kind or 'no MSH-9'}")
    trigger = kind.split("^")[1] if "^" in kind else ""
    if trigger not in ("A01", "A04", "A08", "A28", "A31"):
        raise DataError(f"ADT {trigger} is not a patient add/update; ignored")
    mrn = hl7(record, "PID-3.1")
    if not mrn:
        raise DataError("PID-3.1 (MRN) is empty")
    family = hl7(record, "PID-5.1")
    given = hl7(record, "PID-5.2")
    if not family:
        raise DataError("PID-5.1 (family name) is empty")
    patient = {
        "resourceType": "Patient",
        "identifier": [{"system": MRN_SYSTEM, "value": mrn}],
        "name": [{"family": family, "given": [g for g in [given] if g]}],
    }
    birth = hl7_datetime(hl7(record, "PID-7"))
    if birth:
        patient["birthDate"] = birth[:10]
    gender = GENDER.get(hl7(record, "PID-8").upper())
    if gender:
        patient["gender"] = gender
    return patient
