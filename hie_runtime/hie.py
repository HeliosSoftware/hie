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


# ---- Terminology (HTS) -----------------------------------------------------
#
# When the engine names a terminology server (HIE_TERMINOLOGY_SERVER, set from
# `terminology_url` in hie.yaml), a transform can check codes against it and
# translate local codes through a ConceptMap. Without a server every call answers
# None, so a transform must keep a fallback (a local table) or reject the row.
#
#   lookup("http://loinc.org", "2345-7")             -> {"display": "Glucose [Mass/volume] in Serum or Plasma", ...} or None
#   translate("GLU", "urn:lab:north:codes", "http://loinc.org")
#                                                     -> {"code": "2345-7", "display": "...", "system": "http://loinc.org"} or None
#
# Answers are cached for the life of the worker process.

import json as _json
import os as _os
import urllib.error as _urlerror
import urllib.parse as _urlparse
import urllib.request as _urlrequest

_cache = {}


def terminology_server():
    """The HTS base URL the engine configured, or None."""
    url = _os.environ.get("HIE_TERMINOLOGY_SERVER", "").strip()
    return url.rstrip("/") or None


def _get(path, params):
    base = terminology_server()
    if not base:
        return None
    key = (path, tuple(sorted(params.items())))
    if key in _cache:
        return _cache[key]
    url = f"{base}/{path}?{_urlparse.urlencode(params)}"
    # A named client: the default Python agent is refused by some front doors.
    request = _urlrequest.Request(
        url,
        headers={"Accept": "application/fhir+json", "User-Agent": "hie (+https://github.com/HeliosSoftware/hie)"},
    )
    token = _os.environ.get("HIE_TERMINOLOGY_TOKEN", "").strip()
    if token:
        request.add_header("Authorization", f"Bearer {token}")
    try:
        with _urlrequest.urlopen(request, timeout=10) as response:
            body = _json.loads(response.read().decode("utf-8"))
    except _urlerror.HTTPError as e:
        if e.code in (400, 404, 422):
            body = None
        else:
            raise TerminologyError(f"{url} answered {e.code}") from None
    except (_urlerror.URLError, OSError, ValueError) as e:
        raise TerminologyError(f"cannot reach the terminology server at {base}: {e}") from None
    _cache[key] = body
    return body


class TerminologyError(Exception):
    """The terminology server could not be used. Not a data error: the row is retried."""


def _parameters(body):
    out = {}
    for p in (body or {}).get("parameter", []):
        name = p.get("name")
        value = next((v for k, v in p.items() if k.startswith("value")), None)
        if value is not None and name not in out:
            out[name] = value
        if name == "match":
            out.setdefault("match", []).append(p.get("part", []))
    return out


def lookup(system, code):
    """`CodeSystem/$lookup`: the code's display and properties, or None when unknown."""
    body = _get("CodeSystem/$lookup", {"system": system, "code": code})
    if not body:
        return None
    params = _parameters(body)
    if "display" not in params and "name" not in params:
        return None
    return {"system": system, "code": code, "display": params.get("display"), "name": params.get("name")}


def translate(code, source_system, target_system, concept_map=None):
    """`ConceptMap/$translate`: the first equivalent target concept, or None."""
    params = {"system": source_system, "code": code, "targetsystem": target_system}
    if concept_map:
        params["url"] = concept_map
    body = _get("ConceptMap/$translate", params)
    if not body:
        return None
    parsed = _parameters(body)
    if parsed.get("result") is not True:
        return None
    for parts in parsed.get("match", []):
        concept = None
        relationship = None
        for part in parts:
            if part.get("name") == "concept":
                concept = part.get("valueCoding")
            if part.get("name") in ("relationship", "equivalence"):
                relationship = part.get("valueCode")
        if concept and relationship not in ("not-related-to", "unmatched", "disjoint"):
            return {"system": concept.get("system", target_system), "code": concept.get("code"), "display": concept.get("display")}
    return None


def has_code_system(system):
    """Whether the terminology server holds a code system at all (`CodeSystem?url=`).

    A server that lacks LOINC cannot say a LOINC code is wrong; a transform checks
    this before treating an unknown code as a data error.
    """
    if not terminology_server():
        return False
    body = _get("CodeSystem", {"url": system, "_summary": "count"})
    if not body:
        return False
    total = body.get("total")
    if total is None:
        return bool(body.get("entry"))
    return total > 0
