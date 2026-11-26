"""The worker over its real pipes: UTF-8 in, UTF-8 out, whatever the locale."""

import hashlib
import json
import os
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def ready_line(transform):
    """The ready line a worker that loaded `transform` as it is now writes."""
    return {"ready": True, "transform_sha256": hashlib.sha256(Path(transform).read_bytes()).hexdigest()}


def run_once(transform, record=None):
    """The worker's stdout lines after one request on `transform`."""
    request = json.dumps({"id": 7, "record": record or {}, "context": {"feed": "t", "file": "t", "line": 1}})
    proc = subprocess.run(
        [sys.executable, "-u", str(ROOT / "hie_runtime" / "worker.py"), str(transform)],
        input=(request + "\n").encode("utf-8"),
        capture_output=True,
        timeout=30,
    )
    lines = proc.stdout.decode("utf-8").splitlines()
    if not lines:
        raise AssertionError(proc.stderr.decode("utf-8", "replace"))
    return [json.loads(line) for line in lines], proc


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
        self.assertEqual(
            json.loads(lines[0]),
            ready_line(ROOT / "transforms" / "fhir_passthrough.py"),
            proc.stderr.decode("utf-8", "replace"),
        )
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

    def test_a_write_to_file_descriptor_1_goes_to_stderr_not_the_protocol(self):
        with tempfile.TemporaryDirectory() as run:
            transform = Path(run, "stray.py")
            transform.write_text(
                "import os\n"
                "def transform(record, context):\n"
                "    os.write(1, b'stray\\n')\n"
                "    return {'resourceType': 'Observation', 'id': 'o1'}\n"
            )
            expected = ready_line(transform)
            lines, proc = run_once(transform)
        self.assertEqual(lines[0], expected)
        self.assertEqual(lines[1], {"id": 7, "resources": [{"resourceType": "Observation", "id": "o1"}]})
        self.assertEqual(len(lines), 2, lines)
        self.assertIn(b"stray", proc.stderr)


class TheCodeThatRan(unittest.TestCase):
    """The ready line names the bytes the worker compiled, not the file as it
    is later: the engine stores that hash with every record it settles."""

    def loaded_while_replaced(self, transform, replacement):
        """`worker.load` of `transform`, with the file replaced by
        `replacement` right after the worker first reads it, as a save landing
        while the worker loads would."""
        import importlib.util as util

        spec = util.spec_from_file_location("hie_worker_under_test", ROOT / "hie_runtime" / "worker.py")
        worker = util.module_from_spec(spec)
        spec.loader.exec_module(worker)
        original = Path.read_bytes

        def read_then_replace(path):
            data = original(path)
            if Path(path) == Path(transform):
                Path(path).write_bytes(replacement)
            return data

        Path.read_bytes = read_then_replace
        try:
            return worker.load(str(transform))
        finally:
            Path.read_bytes = original

    def test_the_hash_is_of_the_bytes_run_even_if_the_file_changes_while_it_loads(self):
        first = b"def transform(record, context):\n    return 'first'\n"
        second = b"def transform(record, context):\n    return 'second'\n"
        with tempfile.TemporaryDirectory() as run:
            transform = Path(run, "saved_meanwhile.py")
            transform.write_bytes(first)
            module, sha256 = self.loaded_while_replaced(transform, second)
            self.assertEqual(transform.read_bytes(), second, "the file was replaced")
        self.assertEqual(module.transform({}, {}), "first")
        self.assertEqual(sha256, hashlib.sha256(first).hexdigest())

    def test_a_compiled_transform_is_run_from_the_bytes_it_is_named_by(self):
        import py_compile

        with tempfile.TemporaryDirectory() as run:
            compiled = {}
            for name in ("first", "second"):
                source = Path(run, f"{name}.py")
                source.write_text(f"def transform(record, context):\n    return {name!r}\n")
                target = Path(run, f"{name}.pyc")
                py_compile.compile(str(source), cfile=str(target), doraise=True)
                compiled[name] = target.read_bytes()
            transform = Path(run, "compiled.pyc")
            transform.write_bytes(compiled["first"])
            module, sha256 = self.loaded_while_replaced(transform, compiled["second"])
        self.assertEqual(module.transform({}, {}), "first")
        self.assertEqual(sha256, hashlib.sha256(compiled["first"]).hexdigest())

    def test_a_compiled_transform_still_loads_and_is_named_by_its_bytes(self):
        import py_compile

        with tempfile.TemporaryDirectory() as run:
            source = Path(run, "compiled.py")
            source.write_text("def transform(record, context):\n    return {'resourceType': 'Observation', 'id': 'c1'}\n")
            compiled = Path(py_compile.compile(str(source), cfile=str(Path(run, "compiled.pyc")), doraise=True))
            source.unlink()
            expected = ready_line(compiled)
            lines, _ = run_once(compiled)
        self.assertEqual(lines[0], expected)
        self.assertEqual(lines[1]["resources"], [{"resourceType": "Observation", "id": "c1"}])


class Interpreter(unittest.TestCase):
    def test_the_interpreter_is_recent_enough_for_the_launcher(self):
        self.assertGreaterEqual(sys.version_info, (3, 9))


@unittest.skipUnless(sys.platform.startswith("linux"), "the memory limit is enforced on Linux")
class UnderTheLauncher(unittest.TestCase):
    """The worker run as the engine runs it: a transform that allocates past
    the memory limit ends the process with the launcher's memory code."""

    def test_running_out_of_memory_ends_the_process_with_65(self):
        with tempfile.TemporaryDirectory() as run:
            work, control = Path(run, "work"), Path(run, "control")
            work.mkdir()
            control.mkdir()
            transform = Path(run, "greedy.py")
            transform.write_text("def transform(record, context):\n    return bytearray(1 << 30)\n")
            proc = subprocess.Popen(
                [sys.executable, "-I", "-B", "-u", "-X", "utf8", str(ROOT / "hie_runtime" / "limits.py"),
                 "--control", str(control), "--memory", "256", "--",
                 str(ROOT / "hie_runtime" / "worker.py"), str(transform)],
                stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                cwd=work, env={"PATH": os.environ.get("PATH", "")},
            )
            try:
                deadline = time.monotonic() + 10
                while not (control / "ready.json").exists():
                    self.assertLess(time.monotonic(), deadline, "no ready.json")
                    time.sleep(0.005)
                ready = json.loads((control / "ready.json").read_text())
                self.assertIsNone(ready["error"])
                (control / "ack").touch()
                self.assertEqual(json.loads(proc.stdout.readline()), ready_line(transform))
                request = {"id": 1, "record": {}, "context": {"feed": "t", "file": "t", "line": 1}}
                proc.stdin.write((json.dumps(request) + "\n").encode())
                proc.stdin.flush()
                self.assertEqual(proc.wait(timeout=30), 65, proc.stderr.read().decode("utf-8", "replace"))
                self.assertEqual(proc.stdout.read(), b"", "no reply for the record that ran out of memory")
            finally:
                proc.kill()
                proc.wait()
                for stream in (proc.stdin, proc.stdout, proc.stderr):
                    stream.close()


if __name__ == "__main__":
    unittest.main()
