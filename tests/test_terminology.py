"""The terminology helpers against a stand-in HTS: lookups, translations, the
cache, what happens without a server, and the lab transform using them."""

import importlib.util
import io
import json
import os
import sys
import unittest
import urllib.request
from pathlib import Path
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "hie_runtime"))

import hie  # noqa: E402
from hie import DataError, TerminologyError, lookup, translate  # noqa: E402

spec = importlib.util.spec_from_file_location("lab_results", ROOT / "transforms" / "lab_results.py")
lab_results = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab_results)

HTS = "http://hts.test"


class FakeHts:
    """Answers $lookup and $translate the way HTS does, and counts the calls."""

    def __init__(self, loinc_loaded=True):
        self.calls = []
        self.loinc_loaded = loinc_loaded

    def __call__(self, request, timeout=None):
        url = request.full_url
        self.calls.append(url)
        path, _, query = url.partition("?")
        q = dict(urllib.parse.parse_qsl(query))
        if path.endswith("/CodeSystem"):
            held = q.get("url") == "http://loinc.org" and self.loinc_loaded
            body = {"resourceType": "Bundle", "type": "searchset", "total": 1 if held else 0}
        elif path.endswith("CodeSystem/$lookup"):
            if q["system"] == "http://loinc.org" and q["code"] == "2345-7":
                body = {"resourceType": "Parameters", "parameter": [
                    {"name": "name", "valueString": "LOINC"},
                    {"name": "display", "valueString": "Glucose [Mass/volume] in Serum or Plasma"}]}
            else:
                raise urllib.error.HTTPError(url, 404, "not found", {}, io.BytesIO(b"{}"))
        elif path.endswith("ConceptMap/$translate"):
            if q["code"] == "HGB":
                body = {"resourceType": "Parameters", "parameter": [
                    {"name": "result", "valueBoolean": True},
                    {"name": "match", "part": [
                        {"name": "relationship", "valueCode": "equivalent"},
                        {"name": "concept", "valueCoding": {"system": "http://loinc.org", "code": "718-7", "display": "Hemoglobin [Mass/volume] in Blood"}}]}]}
            else:
                body = {"resourceType": "Parameters", "parameter": [{"name": "result", "valueBoolean": False}]}
        else:
            raise AssertionError(url)
        return io.BytesIO(json.dumps(body).encode())


class Helpers(unittest.TestCase):
    def setUp(self):
        hie._cache.clear()
        self.hts = FakeHts()
        self.patches = [
            mock.patch.dict(os.environ, {"HIE_TERMINOLOGY_SERVER": HTS}),
            mock.patch.object(urllib.request, "urlopen", self.hts),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()

    def test_lookup_known_and_unknown(self):
        self.assertEqual(lookup("http://loinc.org", "2345-7")["display"], "Glucose [Mass/volume] in Serum or Plasma")
        self.assertIsNone(lookup("http://loinc.org", "0000-0"))

    def test_translate(self):
        self.assertEqual(translate("HGB", "urn:x", "http://loinc.org")["code"], "718-7")
        self.assertIsNone(translate("XYZ", "urn:x", "http://loinc.org"))

    def test_answers_are_cached(self):
        lookup("http://loinc.org", "2345-7")
        lookup("http://loinc.org", "2345-7")
        self.assertEqual(len(self.hts.calls), 1)

    def test_a_server_that_fails_is_not_a_data_error(self):
        def down(request, timeout=None):
            raise urllib.error.URLError("connection refused")
        with mock.patch.object(urllib.request, "urlopen", down):
            with self.assertRaises(TerminologyError):
                lookup("http://loinc.org", "2345-7")

    def test_without_a_server_everything_is_none(self):
        with mock.patch.dict(os.environ, {"HIE_TERMINOLOGY_SERVER": ""}):
            self.assertIsNone(lookup("http://loinc.org", "2345-7"))
            self.assertIsNone(translate("HGB", "urn:x", "http://loinc.org"))
            self.assertEqual(self.hts.calls, [])


class LabTransformWithHts(unittest.TestCase):
    def setUp(self):
        hie._cache.clear()
        self.hts = FakeHts()
        self.patches = [
            mock.patch.dict(os.environ, {"HIE_TERMINOLOGY_SERVER": HTS}),
            mock.patch.object(urllib.request, "urlopen", self.hts),
        ]
        for p in self.patches:
            p.start()

    def tearDown(self):
        for p in self.patches:
            p.stop()

    def run_row(self, **changes):
        row = {"patient": "Patient/1", "code": "2345-7", "display": "", "value": "95", "unit": "mg/dL", "date": "2026-10-15"}
        return lab_results.transform({**row, **changes}, {"feed": "lab-results", "file": "t.csv", "line": 2})

    def test_loinc_display_comes_from_the_server(self):
        coding = self.run_row(display="whatever the lab wrote")["code"]["coding"][0]
        self.assertEqual(coding["display"], "Glucose [Mass/volume] in Serum or Plasma")

    def test_unknown_loinc_is_rejected_when_a_server_is_there(self):
        with self.assertRaises(DataError) as e:
            self.run_row(code="0000-0")
        self.assertIn("not known to the terminology server", str(e.exception))

    def test_local_code_translated_by_the_server_beats_the_table(self):
        coding = self.run_row(code="HGB")["code"]["coding"][0]
        self.assertEqual((coding["code"], coding["display"]), ("718-7", "Hemoglobin [Mass/volume] in Blood"))
        self.assertTrue(any("$translate" in c for c in self.hts.calls))

    def test_local_code_the_server_lacks_falls_back_to_the_table(self):
        coding = self.run_row(code="GLU")["code"]["coding"][0]
        self.assertEqual(coding["code"], "2345-7")

    def test_a_server_without_loinc_cannot_reject_a_loinc_code(self):
        hie._cache.clear()
        without = FakeHts(loinc_loaded=False)
        without.__call__ = without.__call__
        with mock.patch.object(urllib.request, "urlopen", lambda request, timeout=None: self._lookup_404_only(without, request)):
            coding = self.run_row(code="0000-0", display="Something local")["code"]["coding"][0]
        self.assertEqual((coding["code"], coding["display"]), ("0000-0", "Something local"))

    @staticmethod
    def _lookup_404_only(fake, request):
        # Every $lookup is a 404 on this server; the CodeSystem search says LOINC is absent.
        if "$lookup" in request.full_url:
            raise urllib.error.HTTPError(request.full_url, 404, "not found", {}, io.BytesIO(b"{}"))
        return fake(request)


if __name__ == "__main__":
    unittest.main()
