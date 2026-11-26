"""Runs a transform script for HIE.

Protocol, one JSON object per line:
  ready    {"ready": true, "transform_sha256": "<hex>" | null}, once, when the script is loaded
  request  {"id": 1, "record": {...}, "context": {"feed": ..., "file": ..., "line": ...}}
  response {"id": 1, "resources": [...]}
        or {"id": 1, "error": {"kind": "data" | "transform" | "transient", "message": ...}}

`data` rejects the record, `transform` rejects it as a bug in the script, and
`transient` (a terminology server that cannot be reached) makes the engine try
the record again later.

The script defines `transform(record, context)` returning a resource, a list of
resources or None. It raises `hie.DataError` for bad source data.

`transform_sha256` is the SHA-256 of the script's bytes as this process read
them: the engine stores it with each record this process settles. It is null
for a script loaded some other way than from those bytes (an extension module).
"""

import errno
import hashlib
import importlib.machinery
import importlib.util
import json
import marshal
import os
import sys
import traceback
import types
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from hie import DataError, TerminologyError  # noqa: E402


def load(path):
    """The script's module, and the SHA-256 of the bytes it was made from.

    The file is read once and those bytes are both hashed and run, so the hash
    names the code that runs even if the file is replaced while it loads: a
    source file is compiled from them, a compiled `.pyc` unmarshalled from
    them. Anything else goes through its own loader and is named by no hash.
    """
    spec = importlib.util.spec_from_file_location("hie_transform", path)
    module = importlib.util.module_from_spec(spec)
    if isinstance(spec.loader, importlib.machinery.SourceFileLoader):
        data = Path(path).read_bytes()
        code = compile(data, spec.origin, "exec", dont_inherit=True)
    elif isinstance(spec.loader, importlib.machinery.SourcelessFileLoader):
        data = Path(path).read_bytes()
        # Checked as the loader checks it: a 16-byte header with the magic
        # number of this Python and known flags, then a code object.
        if len(data) < 16:
            raise EOFError(f"{path!r} is too short to be compiled Python")
        if data[:4] != importlib.util.MAGIC_NUMBER:
            raise ImportError(f"bad magic number in {path!r}")
        flags = int.from_bytes(data[4:8], "little")
        if flags & ~0b11:
            raise ImportError(f"invalid flags {flags!r} in {path!r}")
        code = marshal.loads(memoryview(data)[16:])
        if not isinstance(code, types.CodeType):
            raise ImportError(f"Non-code object in {path!r}")
    else:
        spec.loader.exec_module(module)
        data = None
    if data is not None:
        exec(code, module.__dict__)
    if not callable(getattr(module, "transform", None)):
        raise SystemExit(f"{path} does not define transform(record, context)")
    return module, (hashlib.sha256(data).hexdigest() if data is not None else None)


def main():
    # The protocol is UTF-8 whatever the locale says: on Windows Python would
    # otherwise read the engine's UTF-8 as cp1252 and turn every accented letter
    # into a lone surrogate that the engine cannot parse back.
    sys.stdin.reconfigure(encoding="utf-8", errors="strict")
    # The protocol gets a descriptor of its own and file descriptor 1 becomes
    # stderr, so nothing else can write into the protocol: not a print(), not
    # a child process that inherits stdout, not a native library.
    protocol = os.fdopen(os.dup(1), "w", encoding="utf-8", errors="strict")
    os.dup2(2, 1)
    sys.stdout = sys.stderr
    module, sha256 = load(sys.argv[1])
    protocol.write(json.dumps({"ready": True, "transform_sha256": sha256}) + "\n")
    protocol.flush()
    for line in sys.stdin:
        if not line.strip():
            continue
        request = json.loads(line)
        try:
            result = module.transform(request["record"], request["context"])
            if result is None:
                result = []
            elif isinstance(result, dict):
                result = [result]
            reply = {"id": request["id"], "resources": result}
        except DataError as e:
            reply = {"id": request["id"], "error": {"kind": "data", "message": str(e)}}
        except TerminologyError as e:  # the server, not the row: the engine retries later
            reply = {"id": request["id"], "error": {"kind": "transient", "message": str(e)}}
        except MemoryError:  # past the memory limit: the process ends and the engine says why
            raise
        except Exception as e:  # a bug in the transform, kept with its traceback
            if isinstance(e, OSError) and e.errno == errno.EFBIG:
                raise  # a file past the size limit: likewise
            reply = {
                "id": request["id"],
                "error": {
                    "kind": "transform",
                    "message": f"{type(e).__name__}: {e}",
                    "trace": traceback.format_exc(limit=5),
                },
            }
        protocol.write(json.dumps(reply) + "\n")
        protocol.flush()


if __name__ == "__main__":
    main()
