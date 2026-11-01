#!/usr/bin/env python3
"""Run the whole test suite — no MariaDB, nothing installed outside the repo.

    python3 tests/run.py --setup        # once: makes <repo>/.venv
    python3 tests/run.py                # every tests/test_*.py
    python3 tests/run.py --only test_author test_mariadb
    python3 tests/run.py --jobs 8

--setup creates <repo>/.venv with the standard library's venv and fills it,
with that venv's own pip, from tests/requirements.txt (the broker's
requirements plus pyyaml). Nothing is written to a system Python or to pip's
cache; deleting .venv undoes it.

Every file then runs in its own process, with .venv's python, on fakemaria
(tests/dbharness.py): a test-only `pymysql` backed by sqlite3, so the
database suites need no server. A2A_TEST_DB_REAL=1 points them at a real
one instead (A2A_TEST_DB_HOST/_PORT/_USER/_PASSWORD).

Exit 1 if anything failed. A contract test with no upstream checkout to read
(HERMES_SRC, OPENCODE_SRC) is listed as needing one: it is an input, not a
package, and nothing here fetches it.
"""
import argparse
import os
import shutil
import subprocess
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
VENV = REPO / ".venv"
VPY = VENV / "bin" / "python"
FAKE = HERE / "fakemaria"
# The tree that is copied to the server. A run must leave it exactly as found.
DEPLOY = REPO / "deploy"
# An input each needs, and what the test prints when it is missing.
CHECKOUTS = {"test_hermes_contract.py": "HERMES_SRC",
             "test_opencode_contract.py": "OPENCODE_SRC"}


def setup() -> int:
    if not VPY.exists():
        print(f"creating {VENV}")
        r = subprocess.run([sys.executable, "-m", "venv", str(VENV)])
        if r.returncode:
            return r.returncode
    print(f"installing tests/requirements.txt into {VENV}")
    env = dict(os.environ, PIP_NO_CACHE_DIR="1",
               PIP_DISABLE_PIP_VERSION_CHECK="1", PYTHONDONTWRITEBYTECODE="1")
    env.pop("PIP_USER", None)
    r = subprocess.run([str(VPY), "-m", "pip", "install", "--no-cache-dir",
                        "--disable-pip-version-check", "-r",
                        str(HERE / "requirements.txt")], env=env, cwd=HERE)
    if r.returncode == 0:
        print("ready: python3 tests/run.py")
    return r.returncode


def run_one(path: Path, python: str, env: dict, timeout: float):
    t0 = time.monotonic()
    try:
        p = subprocess.run([python, str(path)], cwd=REPO, env=env,
                           capture_output=True, text=True, timeout=timeout,
                           stdin=subprocess.DEVNULL)
        code, out = p.returncode, (p.stdout or "") + (p.stderr or "")
    except subprocess.TimeoutExpired as e:
        code = "timeout"
        out = ((e.stdout or b"").decode(errors="replace")
               if isinstance(e.stdout, bytes) else (e.stdout or ""))
    return path.name, code, out, time.monotonic() - t0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--setup", action="store_true",
                    help="create/refresh <repo>/.venv, then stop")
    ap.add_argument("--only", nargs="+", metavar="NAME",
                    help="run only these (test_x, test_x.py)")
    ap.add_argument("--jobs", type=int, default=4)
    ap.add_argument("--timeout", type=float, default=900,
                    help="seconds per file (default 900)")
    a = ap.parse_args()
    if a.setup:
        return setup()
    if not VPY.exists():
        print("no .venv yet — run once:  python3 tests/run.py --setup\n"
              "(it creates <repo>/.venv; nothing outside the repo is "
              "touched)", file=sys.stderr)
        return 1

    files = sorted(HERE.glob("test_*.py"))
    if a.only:
        want = {n if n.endswith(".py") else n + ".py" for n in a.only}
        missing = want - {f.name for f in files}
        if missing:
            print(f"no such test: {', '.join(sorted(missing))}",
                  file=sys.stderr)
            return 1
        files = [f for f in files if f.name in want]

    env = dict(os.environ)
    # No bytecode: a test imports the broker and the clients from deploy/,
    # and Python would leave a __pycache__ beside each — in the tree that is
    # copied to the server.
    env["PYTHONDONTWRITEBYTECODE"] = "1"
    real = env.get("A2A_TEST_DB_REAL", "") not in ("", "0")
    data = None
    if not real:
        data = tempfile.mkdtemp(prefix="a2a-fakemaria-run-")
        env["A2A_FAKEMARIA_DIR"] = data
        paths = [p for p in env.get("PYTHONPATH", "").split(os.pathsep) if p]
        env["PYTHONPATH"] = os.pathsep.join([str(FAKE), *paths])
    print(f"{len(files)} files, {a.jobs} at a time, with {VPY.relative_to(REPO)}"
          f", on {'a REAL database' if real else 'fakemaria'}")

    passed, failed, checkout = [], [], []
    t0 = time.monotonic()
    caches_before = set(DEPLOY.rglob("__pycache__"))
    try:
        with ThreadPoolExecutor(max_workers=max(1, a.jobs)) as pool:
            futs = [pool.submit(run_one, f, str(VPY), env, a.timeout)
                    for f in files]
            for fut in as_completed(futs):
                name, code, out, secs = fut.result()
                need = CHECKOUTS.get(name)
                if code == 0:
                    passed.append(name)
                    print(f"PASS  {name}  ({secs:.1f}s)")
                elif code == 2 and need and need in out:
                    checkout.append((name, need))
                    print(f"----  {name}  needs a checkout: set {need}")
                else:
                    failed.append(name)
                    why = "timed out" if code == "timeout" else f"exit {code}"
                    print(f"FAIL  {name}  ({why}, {secs:.1f}s)")
                    tail = [l for l in out.splitlines() if l.strip()][-15:]
                    for line in tail:
                        print(f"      {line}")
    finally:
        if data:
            shutil.rmtree(data, ignore_errors=True)

    # A subprocess a test started with an environment of its own does not
    # inherit PYTHONDONTWRITEBYTECODE. What it wrote into deploy/ is removed
    # here and the run FAILS, so the test that did it gets fixed.
    leaked = sorted(set(DEPLOY.rglob("__pycache__")) - caches_before)
    for d in leaked:
        shutil.rmtree(d, ignore_errors=True)
    if leaked:
        failed.append("(bytecode written into deploy/)")
        print("FAIL  a test wrote into deploy/ (removed): "
              + ", ".join(str(d.relative_to(REPO)) for d in leaked))

    print(f"\n{len(passed)} passed, {len(failed)} failed"
          + (f", {len(checkout)} need a checkout" if checkout else "")
          + f" — {time.monotonic() - t0:.0f}s")
    for name in sorted(failed):
        print(f"  FAIL {name}")
    for name, need in sorted(checkout):
        print(f"  needs {need}: {name}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main())
