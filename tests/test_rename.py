#!/usr/bin/env python3
"""rename_me conformance test.

    python3 tests/test_rename.py

An agent has ONE name and the broker matches it literally — no aliases, no
redirects. Two things have to hold:

  semantics — a rename moves everything that names the agent as a participant.
              A receipt left under the old id is delivered to nobody and
              collected by nobody, so it pins its message until the 365-day
              ceiling. That is the failure this file mostly exists to catch.
  finality  — the old id stops working the moment it is renamed. A leftover
              redirect would capture every OTHER client still sending it and
              quietly merge two sessions into one agent, which is the bug that
              got the alias layer deleted.

Needs python3 (with mcp/uvicorn/starlette), and a free port for the live half.
"""
import asyncio
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import dbharness

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"

STATION = "default"
KEY = "acme-api"            # what the clients send, forever
NAME = "trade-desk"          # what the agent renames itself to
OTHER = "someone-else"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def load_broker(db: Path, env: dict | None = None):
    """Import the hyphenated script as a module, against a throwaway DB.

    Pass `env` to JOIN a database that already exists — the second half of
    this suite runs a real server in a subprocess and then reaches the same
    rows in-process. Under sqlite both sides shared a path; now they have to
    be pointed at the same database explicitly, or the module quietly gets a
    fresh empty one and every lookup fails for the wrong reason.
    """
    os.environ.update(env or dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("a2a_broker", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return mod


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-rename-"))
    b = load_broker(tmp / "rename.db")
    auth = {"token_hash": "", "user": "(test)", "stations": [STATION]}

    # --- a station with two agents, a channel and an unread DM --------------
    b.AGENTS.add(STATION, KEY)
    b.AGENTS.add(STATION, OTHER)
    asyncio.run(b.CHANNELS.create(STATION, "ops", "", [KEY, OTHER]))
    asyncio.run(b.DIRECT.send(STATION, OTHER, KEY, "unread before the rename"))

    def receipts(agent: str) -> int:
        return b.CONN.execute(
            "SELECT COUNT(*) n FROM message_receipts "
            "WHERE station_id = %s AND agent_id = %s", (STATION, agent),
        ).fetchone()["n"]

    check("a DM to the key leaves it a receipt", receipts(KEY) == 1,
          f"{receipts(KEY)} receipts")

    # --- the rename ---------------------------------------------------------
    out = b.AGENTS.realm_rename(auth, KEY, NAME)
    check("rename reports the new id", out.get("agent_id") == NAME,
          json.dumps(out))

    row = b.CONN.execute(
        "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
        (STATION, NAME)).fetchone()
    check("the agent row carries the new id", row is not None)
    gone = b.CONN.execute(
        "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
        (STATION, KEY)).fetchone()
    check("the old id is not left behind as a second agent", gone is None)

    check("receipts moved with it (else the message is lost silently)",
          receipts(NAME) == 1 and receipts(KEY) == 0,
          f"new={receipts(NAME)} old={receipts(KEY)}")

    members = asyncio.run(b.CHANNELS.get(STATION, "ops"))["members"]
    check("channel membership moved", NAME in members and KEY not in members,
          json.dumps(members))
    check("the other member is untouched", OTHER in members,
          json.dumps(members))

    msgs, _ = b._fetch_for_agent(STATION, NAME, 50, replay=True)
    check("the unread DM is still deliverable after the rename",
          any("unread before the rename" in (m.get("text") or "")
              for m in msgs), json.dumps(msgs)[:200])

    # --- refusals -----------------------------------------------------------
    try:
        b.AGENTS.realm_rename(auth, NAME, OTHER)
        check("renaming onto a taken id is refused", False, "no error raised")
    except ValueError as e:
        check("renaming onto a taken id is refused", "already exists" in str(e),
              str(e))

    try:
        b.AGENTS.realm_rename(auth, "never-registered", "whatever")
        check("renaming an agent you do not have is refused", False,
              "no error raised")
    except KeyError as e:
        check("renaming an agent you do not have is refused",
              "not one of your agents" in str(e), str(e))

    check("no alias table exists to redirect anything",
          not b._has_table("agent_aliases"), "agent_aliases is still there")

    # --- the same thing over HTTP, to cover the middleware ------------------
    db2 = tmp / "live.db"
    env = dict(os.environ, **dbharness.db_env(), A2A_AUTH_DISABLED="1",
               A2A_HOST="127.0.0.1", A2A_PORT=str(free_port()))
    base = f"http://127.0.0.1:{env['A2A_PORT']}"
    for agent in (KEY, OTHER):
        subprocess.run(
            [sys.executable, str(BROKER), "agent", "add", agent,
             "--station", STATION],
            env=env, capture_output=True, text=True, check=True)

    proc = subprocess.Popen(
        [sys.executable, str(BROKER), "serve"], env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("broker did not come up")

        def api(path, body=None, method="GET", as_agent=OTHER):
            req = urllib.request.Request(
                f"{base}{path}",
                data=json.dumps(body).encode() if body is not None else None,
                headers={"Content-Type": "application/json",
                         "X-A2A-Agent": as_agent},
                method=method)
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.load(r)

        # Rename through the same registry the endpoint calls.
        b2 = load_broker(db2, env)
        b2.AGENTS.realm_rename(auth, KEY, NAME)

        api("/dms", {"to": NAME, "text": "addressed to the new name"},
            method="POST")

        pending = json.dumps(api(f"/pending?agent={NAME}"))
        check("the renamed agent receives its messages",
              "addressed to the new name" in pending, pending[:300])

        # The point of having no alias layer: the old id is simply gone.
        try:
            stale = json.dumps(api(f"/pending?agent={KEY}"))
            check("the OLD id no longer collects anything",
                  "addressed to the new name" not in stale, stale[:300])
        except urllib.error.HTTPError as e:
            check("the OLD id no longer collects anything", e.code in (401, 403),
                  f"HTTP {e.code}")
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — renames move everything and stick")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
