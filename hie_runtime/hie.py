"""Helpers available to transform scripts (`from hie import DataError, hl7`)."""


class DataError(Exception):
    """The source record is wrong. HIE rejects it with this message instead of retrying."""


# ---- HL7 v2 ---------------------------------------------------------------
#
# An HL7 v2 record (format hl7v2, or an mllp source) is a dict:
#   {"message_type": "ADT^A01", "control_id": "...", "sending_application": ...,
#    "segments": [{"name": "MSH", "fields": [...]}, {"name": "PID", "fields": [...]}, ...]}
# Fields keep their separators; these helpers split them.
#
#   hl7(record, "PID-5.1")        -> family name (field 5, component 1 of the first PID)
#   hl7(record, "PID-3[2].1")     -> second repetition of PID-3, component 1
#   hl7(record, "OBX(3)-5")       -> field 5 of the third OBX
#   hl7_segments(record, "OBX")   -> every OBX segment
#   hl7_field(segment, "5.1")     -> a field of one segment

import re

_PATH = re.compile(
    r"^(?P<seg>[A-Z][A-Z0-9]{2})(?:\((?P<occ>\d+)\))?"
    r"(?:-(?P<field>\d+)(?:\[(?P<rep>\d+)\])?(?:\.(?P<comp>\d+)(?:\.(?P<sub>\d+))?)?)?$"
)


def hl7_segments(record, name):
    """Every segment called `name`, in order."""
    return [s for s in record.get("segments", []) if s.get("name") == name]


def hl7_field(segment, spec, default=""):
    """`spec` is 'F', 'F.C', 'F.C.S' or 'F[r].C': 1-based field, repetition, component, subcomponent."""
    m = _PATH.match("XXX-" + str(spec))
    if not m:
        raise ValueError(f"bad HL7 field spec {spec!r}")
    return _pick(segment, m, default)


def hl7(record, path, default=""):
    """A value by path, 'SEG(occurrence)-field[repetition].component.subcomponent', 1-based.

    Missing segments, fields or components give `default`. The separators come
    from MSH-2 when the record has one (`^~\\&` otherwise).
    """
    m = _PATH.match(path)
    if not m:
        raise ValueError(f"bad HL7 path {path!r}")
    segments = hl7_segments(record, m.group("seg"))
    occurrence = int(m.group("occ") or 1)
    if occurrence < 1 or occurrence > len(segments):
        return default
    return _pick(segments[occurrence - 1], m, default, _separators(record))


def _separators(record):
    msh = hl7_segments(record, "MSH")
    enc = msh[0]["fields"][1] if msh and len(msh[0]["fields"]) > 1 else "^~\\&"
    return {
        "component": enc[0] if len(enc) > 0 else "^",
        "repetition": enc[1] if len(enc) > 1 else "~",
        "subcomponent": enc[3] if len(enc) > 3 else "&",
    }


def _pick(segment, m, default, sep=None):
    sep = sep or {"component": "^", "repetition": "~", "subcomponent": "&"}
    if not m.group("field"):
        return segment
    fields = segment.get("fields", [])
    i = int(m.group("field")) - 1
    if i < 0 or i >= len(fields):
        return default
    value = fields[i]
    if m.group("rep"):
        reps = value.split(sep["repetition"])
        r = int(m.group("rep")) - 1
        if r < 0 or r >= len(reps):
            return default
        value = reps[r]
    elif m.group("comp"):
        # Without an explicit repetition, the first one.
        value = value.split(sep["repetition"])[0]
    if m.group("comp"):
        comps = value.split(sep["component"])
        c = int(m.group("comp")) - 1
        if c < 0 or c >= len(comps):
            return default
        value = comps[c]
    if m.group("sub"):
        subs = value.split(sep["subcomponent"])
        s = int(m.group("sub")) - 1
        if s < 0 or s >= len(subs):
            return default
        value = subs[s]
    return value if value != "" else default


def hl7_datetime(text):
    """An HL7 TS ('20261021120000', '20261021', '202610211200+0100') as a FHIR dateTime, or '' when empty."""
    if not text:
        return ""
    text = str(text)
    body, zone = text, ""
    for sign in ("+", "-"):
        if sign in text[8:]:
            body, zone = text.split(sign, 1)
            zone = sign + zone
            break
    body = body.split(".")[0]
    parts = [body[0:4], body[4:6], body[6:8], body[8:10], body[10:12], body[12:14]]
    if not parts[0]:
        return ""
    out = parts[0]
    if parts[1]:
        out += "-" + parts[1]
    if parts[2]:
        out += "-" + parts[2]
    if parts[3]:
        out += "T" + parts[3] + ":" + (parts[4] or "00") + ":" + (parts[5] or "00")
        if zone:
            out += zone[:3] + ":" + zone[3:5]
    return out
