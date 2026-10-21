"""The HL7 helpers transforms use: paths into a parsed message, and timestamps."""

import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "hie_runtime"))

from hie import hl7, hl7_datetime, hl7_field, hl7_segments  # noqa: E402

RECORD = {
    "message_type": "ADT^A01",
    "control_id": "MSG0001",
    "segments": [
        {"name": "MSH", "fields": ["|", "^~\\&", "LAB", "NORTH", "HIE", "HFS", "20261021120000", "", "ADT^A01", "MSG0001", "P", "2.5.1"]},
        {"name": "PID", "fields": ["1", "", "12345^^^NORTH^MR~98765^^^STATE^SS", "", "Doe^Jane^M", "", "19800101", "F"]},
        {"name": "OBX", "fields": ["1", "NM", "2345-7^Glucose^LN", "", "99", "mg/dL"]},
        {"name": "OBX", "fields": ["2", "NM", "718-7^Hemoglobin^LN", "", "13.8", "g/dL&gram per decilitre"]},
    ],
}


class Paths(unittest.TestCase):
    def test_fields_components_repetitions_and_occurrences(self):
        self.assertEqual(hl7(RECORD, "PID-5"), "Doe^Jane^M")
        self.assertEqual(hl7(RECORD, "PID-5.1"), "Doe")
        self.assertEqual(hl7(RECORD, "PID-5.2"), "Jane")
        self.assertEqual(hl7(RECORD, "PID-3.1"), "12345")
        self.assertEqual(hl7(RECORD, "PID-3[2].1"), "98765")
        self.assertEqual(hl7(RECORD, "PID-3[2].5"), "SS")
        self.assertEqual(hl7(RECORD, "OBX-5"), "99")
        self.assertEqual(hl7(RECORD, "OBX(2)-5"), "13.8")
        self.assertEqual(hl7(RECORD, "OBX(2)-6.1.2"), "gram per decilitre")
        self.assertEqual(hl7(RECORD, "MSH-9.2"), "A01")
        self.assertEqual(hl7(RECORD, "MSH-2"), "^~\\&")

    def test_missing_gives_the_default(self):
        self.assertEqual(hl7(RECORD, "PID-4"), "")
        self.assertEqual(hl7(RECORD, "PID-5.9", None), None)
        self.assertEqual(hl7(RECORD, "OBX(3)-5", "none"), "none")
        self.assertEqual(hl7(RECORD, "NK1-2"), "")
        self.assertEqual(hl7(RECORD, "PID-3[3].1", "x"), "x")

    def test_segments_and_fields(self):
        obx = hl7_segments(RECORD, "OBX")
        self.assertEqual([hl7_field(s, "3.1") for s in obx], ["2345-7", "718-7"])
        self.assertEqual(hl7_field(obx[0], "5"), "99")
        self.assertEqual(hl7_segments(RECORD, "ZZZ"), [])

    def test_bad_paths_are_errors(self):
        with self.assertRaises(ValueError):
            hl7(RECORD, "pid-5")
        with self.assertRaises(ValueError):
            hl7(RECORD, "PID-5-1")


class Timestamps(unittest.TestCase):
    def test_hl7_ts_to_fhir(self):
        self.assertEqual(hl7_datetime("20261021120000"), "2026-10-21T12:00:00")
        self.assertEqual(hl7_datetime("202610211200+0100"), "2026-10-21T12:00:00+01:00")
        self.assertEqual(hl7_datetime("20261021"), "2026-10-21")
        self.assertEqual(hl7_datetime("202610"), "2026-10")
        self.assertEqual(hl7_datetime("20261021120000.1234-0500"), "2026-10-21T12:00:00-05:00")
        self.assertEqual(hl7_datetime(""), "")


if __name__ == "__main__":
    unittest.main()
