"""The worker over its real pipes: UTF-8 in, UTF-8 out, whatever the locale."""

import json
import os
import subprocess
import sys
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


class WorkerProtocol(unittest.TestCase):
    def run_worker(self, record, env_extra=None):
        env = {**os.environ, **(env_extra or {})}
        env.pop("PYTHONIOENCODING", None)
        env.pop("PYTHONUTF8", None)
        request = json.dumps({"id": 7, "record": record, "context": {"feed": "t", "file": "t", "line": 1}}, ensure_ascii=False)
        proc = subprocess.run(
            [sys.executable, "-u", str(ROOT / "hie_runtime" / "worker.py"), str(ROOT / "transforms" / "fhir_passthrough.py")],
            input=(request + "\n").encode("utf-8"),
            capture_output=True,
            env=env,
            timeout=30,
        )
        lines = proc.stdout.decode("utf-8").splitlines()
        self.assertEqual(json.loads(lines[0]), {"ready": True}, proc.stderr.decode("utf-8", "replace"))
        return json.loads(lines[1])

    def test_accented_names_survive_the_round_trip(self):
        record = {"resourceType": "Practitioner", "id": "p1", "name": [{"family": "Beltrán", "given": ["Ángela"]}], "note": "日本語 — ñ — 😀"}
        reply = self.run_worker(record)
        self.assertEqual(reply["id"], 7)
        self.assertEqual(reply["resources"][0]["name"][0]["family"], "Beltrán")
        self.assertEqual(reply["resources"][0]["name"][0]["given"], ["Ángela"])
        self.assertEqual(reply["resources"][0]["note"], "日本語 — ñ — 😀")

    def test_a_data_error_comes_back_as_data(self):
        reply = self.run_worker({"resourceType": "Observation"})
        self.assertEqual(reply["error"]["kind"], "data")
        self.assertIn("neither id nor identifier", reply["error"]["message"])


if __name__ == "__main__":
    unittest.main()
