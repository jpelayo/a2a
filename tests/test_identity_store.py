#!/usr/bin/env python3
"""The identity store — where a client's name lives.

    python3 tests/test_identity_store.py

An agent has one name and the broker matches it literally, so the client is the
thing that decides what to announce. identity.py is that decision, and it is
read on every connection by the `a2a` server's headersHelper and by the channel.

Two properties carry the whole design:

  compatibility — with no entry, the id is the project directory's name. That
                  is what every client shipped so far already sends, and agents
                  are registered and pushing under those ids, so an upgrade
                  must be invisible to them.
  ownership     — once a client records a name, it is that client's. Two
                  installs in one project cannot end up as one agent, which is
                  what happened when identity was derived rather than stored.

Pure python3, no pip.
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
IDENTITY = PLUGIN / "a2a" / "server" / "identity.py"
PROJECT = "/Users/x/acme-api"
DIRNAME = "acme-api"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def run(*args: str, store: str = "", project: str = PROJECT) -> str:
    out = subprocess.run(
        [sys.executable, str(IDENTITY), *args,
         "--project", project, "--store", store],
        capture_output=True, text=True,
        # A stray A2A_AGENT in the ambient environment would override
        # everything this file is testing.
        env={k: v for k, v in os.environ.items() if not k.startswith("A2A_")},
    )
    return out.stdout.strip()


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-store-"))
    a, b = str(tmp / "install-a"), str(tmp / "install-b")

    # --- compatibility ------------------------------------------------------
    first = run("get", store=a)
    check("an empty store yields the project directory's name — an upgraded "
          "install keeps the id its agent is registered under",
          first == DIRNAME, first)
    check("reading does not create the store file",
          not (Path(a) / "identity.json").exists(),
          "the helper wrote to disk during a handshake")
    check("still the directory name on a second read (nothing drifts)",
          run("get", store=a) == DIRNAME, run("get", store=a))

    # --- the header contract ------------------------------------------------
    raw = run("header", store=a)
    try:
        parsed = json.loads(raw)
    except ValueError:
        parsed = None
    check("header prints one JSON object of string pairs",
          isinstance(parsed, dict) and list(parsed) == ["X-A2A-Agent"]
          and isinstance(parsed.get("X-A2A-Agent"), str), raw)
    check("the header carries exactly what get reports",
          parsed and parsed["X-A2A-Agent"] == run("get", store=a), raw)

    # --- ownership ----------------------------------------------------------
    check("set returns the name it recorded",
          run("set", "bozo-the-clown", store=a) == "bozo-the-clown")
    check("the name survives into later reads",
          run("get", store=a) == "bozo-the-clown", run("get", store=a))
    check("and into the header the tools connect with",
          json.loads(run("header", store=a))["X-A2A-Agent"] == "bozo-the-clown")

    check("a second install in the SAME project is unaffected — this is the "
          "collision that made two harnesses one agent",
          run("get", store=b) == DIRNAME, run("get", store=b))
    run("set", "second-client", store=b)
    check("two installs keep two names", run("get", store=a) == "bozo-the-clown"
          and run("get", store=b) == "second-client",
          f"{run('get', store=a)} / {run('get', store=b)}")

    # --- other projects, and damage -----------------------------------------
    check("another project in the same store gets its own entry",
          run("get", store=a, project="/Users/x/Ledger") == "Ledger",
          run("get", store=a, project="/Users/x/Ledger"))
    check("the first project is untouched by that",
          run("get", store=a) == "bozo-the-clown", run("get", store=a))

    (Path(a) / "identity.json").write_text("{ this is not json")
    check("a corrupt store still yields a usable id rather than failing the "
          "connection", run("get", store=a) == DIRNAME, run("get", store=a))
    check("an unwritable/absent store still answers",
          run("get", store="/proc/nonexistent/nope") == DIRNAME,
          run("get", store="/proc/nonexistent/nope"))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — clients own their names, and keep them")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
