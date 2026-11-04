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
PLUGIN = HERE.parent / "deploy" / "plugin"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
KILOCODE = PLUGIN / "kilocode" / "a2a-kilocode.js"
DSH = PLUGIN / "dsh" / "a2a-dsh.js"
PI = PLUGIN / "pi" / "index.ts"
CLAUDE = PLUGIN / "a2a" / "server" / "a2a-channel.py"
CODEX = PLUGIN / "codex" / "a2a-codex.py"
HERMES = PLUGIN / "hermes" / "__init__.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def imports_as_module(path: Path) -> tuple[bool, str]:
    """Import a JS file exactly as its harness would: dynamically, as ESM.

    The detail is the export names, then what `default` holds — its id and
    the type of its two entry points — since that is all either OpenCode
    reads: V1 takes `default.server`, V2 takes `default.setup`."""
    res = subprocess.run(
        ["node", "--input-type=module", "-e",
         f"import({str(path)!r}).then(m => console.log('@@' + "
         f"Object.keys(m).join(',') + '|' + (m.default ? [m.default.id, "
         f"typeof m.default.server, typeof m.default.setup].join(',') : '')))"
         f".catch(e => {{ "
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
    check("opencode: the plugin IMPORTS — a function declared inside an "
          "object literal made this fail while every other check passed",
          ok, detail)
    check("opencode: and its ONLY export is default {id, server, setup} — V2 "
          "rejects a module without a default {id, setup} and loaded nothing "
          "from the bare `export const A2A` this used to be; V1 1.18.28+ "
          "reads default.server; a second export could start it twice",
          ok and detail == "default|a2a,function,function", detail)

    # --- Kilo Code: its OWN file, as the broker serves it into
    # ~/.config/kilo/plugins — credentials prepended. Kilo's loader
    # (readV1Plugin) reads default.server and requires default.id; nothing
    # else may be exported.
    work = Path(tempfile.mkdtemp(prefix="a2a-load-"))
    baked = ('const A2A_BAKED = {"url": "http://127.0.0.1:1", "token": '
             '"a2a_st_test", "version": "0"}\n')
    kilo = work / "a2a-kilocode.js"
    kilo.write_text(baked + KILOCODE.read_text())
    ok, detail = imports_as_module(kilo)
    check("kilocode: its own client IMPORTS, and its only export is default "
          "{id, server}",
          ok and detail == "default|a2a,function,undefined", detail)

    # --- dsh: the package's entry beside dsh's own client, as the broker
    # packs it. dsh's loader takes `module.default ?? module`: a default here
    # would hide apply, so there must be NONE — only name, inject and apply.
    pkg = work / "dsh"
    pkg.mkdir()
    shutil.copy(PLUGIN / "dsh" / "index.js", pkg / "index.js")
    shutil.copy(PLUGIN / "dsh" / "package.json", pkg / "package.json")
    (pkg / "a2a-dsh.js").write_text(baked + DSH.read_text())
    res = subprocess.run(
        ["node", "--input-type=module", "-e",
         f"import({str(pkg / 'index.js')!r}).then(m => console.log('@@' + "
         f"Object.keys(m).sort().join(',') + '|' + m.name + '|' + "
         f"typeof m.apply + '|' + JSON.stringify(m.inject)))"
         f".catch(e => {{ console.log('!!' + e.message); process.exitCode = 1 }})"],
        capture_output=True, text=True, timeout=90)
    out = res.stdout.strip()
    check("dsh: the package entry IMPORTS, with NO default export — only "
          "name, inject and apply (a default would hide apply from dsh's "
          "loader)",
          out == '@@apply,inject,name|a2a|function|["agents","tools","systemPrompt"]',
          out or res.stderr[:300])

    # --- Pi: typebox is not installed here, so a full import cannot resolve.
    # Module-goal parsing is the part that matters and is available.
    ok, detail = parses_as_module(PI, strip_types=True)
    check("pi: parses in MODULE goal (the check that catches an object-literal "
          "function; a plain `node --check` on a .js/.ts file does not)",
          ok, detail)

    # --- and prove the gate is real, rather than trusting it -----------------
    src = OPENCODE.read_text()
    i = src.index("  // One call an agent can make to orient itself")
    j = src.index("\n  /** What an event says", i)
    block, rest = src[i:j], src[j:]
    broken = (src[:i] + rest.replace("\n  const tools = {\n",
                                     "\n  const tools = {\n" + block + "\n", 1))
    assert broken != src[:i] + rest, "the tool table anchor moved"
    tmp = Path(tempfile.mkdtemp(prefix="a2a-broken-")) / "broken.js"
    tmp.write_text(broken)
    still_ok, _ = imports_as_module(tmp)
    plain = subprocess.run(["node", "--check", str(tmp)], capture_output=True)
    check("the gate CATCHES the exact regression: re-inserting the function "
          "into the object literal fails the import…",
          not still_ok, "the broken shape imported fine")
    # History, not a property of this code: the Node that shipped the break
    # PASSED it under `node --check` (exit 0), which is why the import above
    # is the gate. Newer Nodes reject it (v26.13.0 exits 1). Reported, not
    # asserted — the gate must not depend on which Node runs it.
    ver = subprocess.run(["node", "--version"], capture_output=True,
                         text=True).stdout.strip()
    print(f"note `node --check` on the broken file exits {plain.returncode} "
          f"on node {ver}" + (" — it would have passed it, as it did then"
                              if plain.returncode == 0 else
                              " — this Node catches it; older ones did not"))

    # --- the python clients ---------------------------------------------------
    python_client(CLAUDE, "claude", ("_handle", "_pump", "_instructions"))
    python_client(CODEX, "codex", ("handle", "pump", "BRIEF"))
    # Hermes imports a directory plugin's __init__.py and calls register();
    # Hermes itself is imported lazily inside it, so this needs no Hermes.
    python_client(HERMES, "hermes", ("register", "A2AHermes", "TOOLS"))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — every client loads the way its harness loads it")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
