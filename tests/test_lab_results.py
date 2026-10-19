import importlib.util
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "hie_runtime"))

from hie import DataError  # noqa: E402

spec = importlib.util.spec_from_file_location("lab_results", ROOT / "transforms" / "lab_results.py")
lab_results = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab_results)

ROW = {
    "patient": "Patient/1",
    "code": "2345-7",
    "display": "Glucose",
    "value": "95",
    "unit": "mg/dL",
    "date": "2026-10-15",
}


def run(**changes):
    return lab_results.transform({**ROW, **changes}, {"feed": "lab-results", "file": "t.csv", "line": 2})


class LabResults(unittest.TestCase):
    def test_a_row_becomes_an_observation_with_a_business_identifier(self):
        obs = run()
        self.assertEqual(obs["identifier"][0]["value"], "Patient/1|2345-7|2026-10-15")
        self.assertEqual(obs["valueQuantity"], {
            "value": 95, "unit": "mg/dL", "system": "http://unitsofmeasure.org", "code": "mg/dL"})

    def test_local_codes_map_to_loinc(self):
        coding = run(code="glu", display="")["code"]["coding"][0]
        self.assertEqual(coding["code"], "2345-7")
        self.assertIn("Glucose", coding["display"])

    def test_bad_data_is_rejected_with_a_reason(self):
        cases = {
            "value": ("alto", "not a number"),
            "unit": ("mmHg", "no UCUM mapping"),
            "code": ("XYZ", "neither LOINC"),
            "date": ("16/10/2026", "YYYY-MM-DD"),
            "patient": ("12345", "not a Patient reference"),
        }
        for column, (bad, reason) in cases.items():
            with self.subTest(column=column), self.assertRaisesRegex(DataError, reason):
                run(**{column: bad})

    def test_missing_columns_are_named(self):
        with self.assertRaisesRegex(DataError, "missing value, unit"):
            run(value="", unit="")


if __name__ == "__main__":
    unittest.main()
