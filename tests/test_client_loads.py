#!/usr/bin/env python3
"""Every client must LOAD the way its harness loads it.

    python3 tests/test_client_loads.py

This is the test that did not exist, and its absence cost a working fleet.

Adding a tool to the OpenCode plugin, a function declaration was inserted
INSIDE the object literal the plugin returns. That is a syntax error in module
goal, so the plugin could not be imported, so OpenCode registered none of its
tools — and an agent reported "no a2a_* tools in the ~164 available", read the
plugin off disk and curled the broker by hand to answer the question the
missing tool existed to answer. Nothing in the suite noticed, for two reasons:

  the gate lied     `node --check file.js` parses in SCRIPT goal and accepts
                    module-only errors. On the exact broken source:
                    `--check broken.js` exited 0, `--check broken.mjs` exited 1.
  nothing loaded    test_envelope_contract and test_client_parity lift single
                    FUNCTIONS out and run those, so a file that cannot be
                    imported at all passes both.

So this asserts the one property those cannot: the whole artifact loads. Pure
python3 plus node; no database, no harness installed.
"""
import importlib.util
import os
import re
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
PI = PLUGIN / "pi" / "index.ts"
CLAUDE = PLUGIN / "a2a" / "server" / "a2a-channel.py"
CODEX = PLUGIN / "codex" / "a2a-codex.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def imports_as_module(path: Path) -> tuple[bool, str]:
    """Import a JS file exactly as its harness would: dynamically, as ESM."""
    res = subprocess.run(
        ["node", "--input-type=module", "-e",
         f"import({str(path)!r}).then(m => console.log('@@' + "
         f"Object.keys(m).join(','))).catch(e => {{ "
         f"console.log('!!' + e.constructor.name + ': ' + e.message); "
         f"process.exitCode = 1 }})"],
        capture_output=True, text=True, timeout=90,
    )
    out = res.stdout.strip()
    if out.startswith("@@"):
        return True, out[2:]
    return False, out.lstrip("!") or res.stderr[:200]


def parses_as_module(path: Path, strip_types: bool = False) -> tuple[bool, str]:
    """Syntax-check in MODULE goal, which is the half `node --check` on a .js
    file silently skips. A .mjs copy is what forces it."""
    tmp = Path(tempfile.mkdtemp(prefix="a2a-load-")) / "probe.mjs"
    src = path.read_text()
    if strip_types:
        # Enough for a syntax check: node strips the types itself when told to,
        # so hand it the file under a module extension and let it do that.
        tmp = tmp.with_suffix(".mts")
    tmp.write_text(src)
    cmd = ["node"]
    if strip_types:
        cmd.append("--experimental-strip-types")
    cmd += ["--check", str(tmp)]
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=90)
    return res.returncode == 0, (res.stderr or "")[:300]


def python_client(path: Path, label: str, wanted: tuple[str, ...]) -> None:
    os.environ.setdefault("A2A_URL", "http://127.0.0.1:1")
    os.environ.setdefault("A2A_TOKEN", "a2a_st_test")
    os.environ["A2A_CODEX_SOCK"] = ""
    sys.path.insert(0, str(path.parent))
    try:
        spec = importlib.util.spec_from_file_location(f"load_{label}", path)
        mod = importlib.util.module_from_spec(spec)
        sys.modules[f"load_{label}"] = mod
        spec.loader.exec_module(mod)
    except Exception as e:
        check(f"{label}: imports cleanly", False, f"{type(e).__name__}: {e}")
        return
    missing = [n for n in wanted if not hasattr(mod, n)]
    check(f"{label}: imports cleanly and still has {', '.join(wanted)}",
          not missing, f"missing {missing}")


def main() -> int:
    if not shutil.which("node"):
        print("SKIP: node is not installed", file=sys.stderr)
        return 2

    # --- OpenCode: the one that broke. It has no dependencies, so the real
    # harness load can be reproduced exactly.
    ok, detail = imports_as_module(OPENCODE)
    check("opencode: the plugin IMPORTS and exports A2A — a function declared "
          "inside the returned object literal made this fail while every "
          "other check passed",
          ok and "A2A" in detail, detail)

    # --- Pi: typebox is not installed here, so a full import cannot resolve.
    # Module-goal parsing is the part that matters and is available.
    ok, detail = parses_as_module(PI, strip_types=True)
    check("pi: parses in MODULE goal (the check that catches an object-literal "
          "function; a plain `node --check` on a .js/.ts file does not)",
          ok, detail)

    # --- and prove the gate is real, rather than trusting it -----------------
    src = OPENCODE.read_text()
    i = src.index("  // One call an agent can make to orient itself")
    j = src.index("\n  return {\n    event:", i)
    block, rest = src[i:j], src[j:]
    broken = (src[:i] + rest.replace("\n    tool: {", "\n" + block + "\n    tool: {", 1))
    tmp = Path(tempfile.mkdtemp(prefix="a2a-broken-")) / "broken.js"
    tmp.write_text(broken)
    still_ok, _ = imports_as_module(tmp)
    plain = subprocess.run(["node", "--check", str(tmp)], capture_output=True)
    check("the gate CATCHES the exact regression: re-inserting the function "
          "into the object literal fails the import…",
          not still_ok, "the broken shape imported fine")
    check("…while `node --check` on the same file still passes it, which is "
          "why that was never a gate",
          plain.returncode == 0,
          f"node --check exited {plain.returncode}")

    # --- the python clients ---------------------------------------------------
    python_client(CLAUDE, "claude", ("_handle", "_pump", "_instructions"))
    python_client(CODEX, "codex", ("handle", "pump", "BRIEF"))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — every client loads the way its harness loads it")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
