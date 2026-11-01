#!/usr/bin/env python3
"""What the broker keeps of a client: its doctor's last finding, and no more.

    python3 tests/test_client_state.py

Needs a MariaDB (see dbharness.py) and a free port.

A client's a2a_channel_status (its doctor) ends with POST /me/state. The
broker keeps that finding — and only that — in agent_client_state, bounded so
it can never become the broker's problem. This runs the REAL broker, the
registry in-process and then `serve` over HTTP with a real token:

  bounded     ONE row per agent however many reports (upsert, no history); a
              second report within 30 s is answered but not written; collect()
              drops rows past A2A_CLIENT_STATE_TTL
  shape       a fixed body: unknown keys (a chat id, a path), bad values and
              anything over 1 KB are refused with 400 — nothing else is stored
  who         registered agents only, as themselves; another client's agent
              is refused by the client pin
  follows     rename, move, remove and deleting the station drop the row
  live        the reply says whether a stream for this agent is open
  operator    drift flags, `doctor <agent>` and `doctor` show the finding
"""
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

import dbharness

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "deploy" / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(base: str, method: str, path: str, token: str, agent: str,
         body=None, client: str = "", raw: bytes | None = None):
    headers = {"Content-Type": "application/json",
               "Authorization": f"Bearer {token}", "X-A2A-Agent": agent}
    if client:
        headers["X-A2A-Client"] = client
    data = raw if raw is not None else (
        json.dumps(body).encode() if body is not None else None)
    req = urllib.request.Request(f"{base}{path}", method=method, data=data,
                                 headers=headers)
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            text = r.read().decode()
            return r.status, (json.loads(text) if text.strip() else {})
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(text)
        except ValueError:
            return e.code, {"raw": text[:200]}


GOOD = {"harness": "hermes", "version": "0.6.0", "switch": True,
        "route": "tui", "receives": True, "settings": "own",
        "failing": ["stream"], "fixed": []}


def main() -> int:
    dbharness.require_db()
    env = dbharness.db_env()
    os.environ.update(env)
    os.environ.pop("A2A_AUTH_DISABLED", None)
    spec = importlib.util.spec_from_file_location("state_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b._startup()
    CS = b.CLIENT_STATE

    A = b.STATIONS.create("cst-a")["station_id"]
    B = b.STATIONS.create("cst-b")["station_id"]
    for aid in ("alice", "bob", "carol", "dave", "erin", "old", "live"):
        b.AGENTS.add(A, aid)
    b.AGENTS.add(B, "zed")

    def rows(sid=None):
        q = "SELECT station_id, agent_id FROM agent_client_state"
        args = ()
        if sid:
            q += " WHERE station_id = %s"
            args = (sid,)
        return [(r["station_id"], r["agent_id"])
                for r in b.CONN.execute(q, args).fetchall()]

    # --- bounded ----------------------------------------------------------------
    now = time.time()
    first = CS.report(A, "alice", CS.validate(GOOD), now=now)
    again = CS.report(A, "alice", CS.validate(dict(GOOD, failing=[])),
                      now=now + 5)
    check("a report is stored", first is True and rows(A) == [(A, "alice")],
          str(rows(A)))
    check("a second within 30 s is not written — the throttle — and the first "
          "stays", again is False and CS.get(A, "alice")["failing"]
          == ["stream"], str(CS.get(A, "alice")))
    CS.report(A, "alice", CS.validate(dict(GOOD, failing=[])), now=now + 31)
    check("after 30 s it is written OVER the last: still one row, no history",
          rows(A) == [(A, "alice")] and CS.get(A, "alice")["failing"] == [],
          f"{rows(A)} {CS.get(A, 'alice')}")
    CS.report(A, "old", CS.validate(GOOD), now=now - 40 * 86400)
    b.collect(A)
    check("collect() drops a finding older than A2A_CLIENT_STATE_TTL, and "
          "keeps a fresh one", (A, "old") not in rows(A)
          and (A, "alice") in rows(A), str(rows(A)))

    # --- shape --------------------------------------------------------------------
    for label, body, word in (
            ("an unknown key (a chat id)", dict(GOOD, chat_id="-100"),
             "unknown key"),
            ("a path smuggled in a known key", dict(GOOD, version="/home/x"),
             "version"),
            ("a bad harness", dict(GOOD, harness="other"), "harness"),
            ("a bad route", dict(GOOD, route="telegram"), "route"),
            ("a check id that does not exist", dict(GOOD, failing=["x"]),
             "failing"),
            ("a non-boolean switch", dict(GOOD, switch="yes"), "switch")):
        try:
            CS.validate(body)
            why = ""
        except ValueError as e:
            why = str(e)
        check(f"refused: {label}", word in why, why)

    # --- drift ----------------------------------------------------------------------
    flags = CS.drift(dict(CS.get(A, "alice"), failing=["switch", "stream"],
                          version="0.5.0"))
    check("drift names the failing checks and a client older than the broker",
          flags == ["doctor fails: switch, stream",
                    f"client v0.5.0, broker v{b.VERSION}"], str(flags))
    check("a finding with nothing failing has no flag",
          CS.drift(dict(CS.get(A, "alice"), version=b.VERSION)) == [])

    # --- follows ----------------------------------------------------------------------
    for aid in ("bob", "carol", "dave"):
        CS.report(A, aid, CS.validate(GOOD), now=now)
    b.AGENTS.rename(A, "bob", "robert")
    check("rename drops the finding — the client reports again",
          (A, "bob") not in rows(A) and (A, "robert") not in rows(A),
          str(rows(A)))
    b.AGENTS.move("carol", "cst-b", "cst-a")
    check("move drops it", (A, "carol") not in rows(A)
          and (B, "carol") not in rows(B), str(rows()))
    b.AGENTS.remove("dave", A)
    check("remove drops it", (A, "dave") not in rows(A), str(rows(A)))
    tmp = b.STATIONS.create("cst-tmp")["station_id"]
    b.AGENTS.add(tmp, "tmpbot")
    CS.report(tmp, "tmpbot", CS.validate(GOOD), now=now)
    b.STATIONS.delete("cst-tmp")
    check("deleting the station drops its findings", rows(tmp) == [],
          str(rows(tmp)))

    # --- operator -------------------------------------------------------------------
    CS.report(A, "erin", CS.validate(dict(GOOD, failing=["push_host"])),
              now=now)
    one = subprocess.run([sys.executable, str(BROKER), "doctor", "erin"],
                         env=dict(os.environ, **env), capture_output=True,
                         text=True, timeout=60)
    check("doctor <agent> shows its client's last finding",
          "client: last doctor run" in one.stdout
          and "doctor fails: push_host" in one.stdout, one.stdout[-600:])
    whole = subprocess.run([sys.executable, str(BROKER), "doctor"],
                           env=dict(os.environ, **env), capture_output=True,
                           text=True, timeout=60)
    check("doctor (all) names it among the problems",
          "agent erin" in whole.stdout and "CLIENT" in whole.stdout,
          whole.stdout[-800:])

    # --- the route ----------------------------------------------------------------
    tok = b.TOKENS.create(user="state-test")
    b.STATIONS.allow("cst-a", tok["token"])
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    log = Path(tempfile.mkdtemp(prefix="a2a-state-")) / "broker.log"
    proc = subprocess.Popen(
        [sys.executable, str(BROKER), "serve"],
        env=dict(os.environ, **env, A2A_HOST="127.0.0.1", A2A_PORT=str(port)),
        stdout=log.open("a"), stderr=subprocess.STDOUT)
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=1)
                break
            except Exception:
                time.sleep(0.2)
        code, out = call(base, "POST", "/me/state", tok["token"], "alice",
                         dict(GOOD, failing=["version"]), client="key-alice")
        check("POST /me/state stores a registered agent's finding and answers "
              "with the broker's version",
              code == 200 and out.get("version") == b.VERSION
              and "stream_live" in out, f"{code} {out}")
        code, out = call(base, "POST", "/me/state", tok["token"], "ghost",
                         GOOD)
        check("an unregistered agent is refused", code == 403, f"{code} {out}")
        code, out = call(base, "POST", "/me/state", tok["token"], "alice",
                         GOOD, client="key-someone-else")
        check("another client's agent is refused by the client pin",
              code == 403, f"{code} {out}")
        code, out = call(base, "POST", "/me/state", tok["token"], "alice",
                         dict(GOOD, chat_id="-100"), client="key-alice")
        check("an unknown key is a 400", code == 400
              and "unknown key" in (out.get("error") or ""), f"{code} {out}")
        code, out = call(base, "POST", "/me/state", tok["token"], "alice",
                         client="key-alice",
                         raw=json.dumps(dict(GOOD, version="x" * 2000))
                         .encode())
        check("a body over 1 KB is a 400", code == 400
              and "1024" in (out.get("error") or ""), f"{code} {out}")

        # A live stream for `live`: the reply says so; once closed, it doesn't.
        req = urllib.request.Request(
            f"{base}/stream?agent=live&format=json",
            headers={"Authorization": f"Bearer {tok['token']}",
                     "X-A2A-Agent": "live", "X-A2A-Client": "key-live"})
        holder = {}

        def hold():
            try:
                holder["resp"] = urllib.request.urlopen(req, timeout=30)
                holder["resp"].read(1)
            except Exception as e:
                holder["err"] = repr(e)

        threading.Thread(target=hold, daemon=True).start()
        time.sleep(1.5)
        code, out = call(base, "POST", "/me/state", tok["token"], "live",
                         GOOD, client="key-live")
        check("with a stream open for the agent, stream_live is true",
              code == 200 and out.get("stream_live") is True, f"{code} {out}")
        if holder.get("resp"):
            holder["resp"].close()
        time.sleep(b.STREAM_KEEPALIVE + 2)
        code, out = call(base, "POST", "/me/state", tok["token"], "live",
                         GOOD, client="key-live")
        check("after it closes, false — and the throttled report is still "
              "answered", code == 200 and out.get("stream_live") is False
              and out.get("stored") is False, f"{code} {out}")
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — the broker keeps one finding per agent, bounded, and no more")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
