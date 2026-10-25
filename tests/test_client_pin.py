#!/usr/bin/env python3
"""An agent belongs to one harness-directory: the client pin.

    python3 tests/test_client_pin.py

A token is per USER, and one user runs many harnesses in many directories, so
pinning an agent to its first token could not tell them apart: any of them
could act as any of that user's agents. Since 0.5.0 every client sends an
X-A2A-Client key (one per harness-directory, see test_client_key.py) and the
broker pins each agent to the first key that acts as it — like the token pin,
one level down. This runs a REAL broker with real tokens and asserts:

  claim       the first key to act as an agent owns it
  refuse      any other key is refused (403) on REST, /mcp/ and /stream;
              no key at all is refused too, naming the installer
  old         an agent nobody has claimed still works without a key, and is
              not claimed by such a request
  race        two keys claiming one agent at once: exactly one wins — end to
              end, and the losing branch driven directly
  stream      /stream reads ?agent= exactly as the middleware does, so a
              crafted query cannot open another client's inbox
  realm       another client's agent cannot be renamed, rebound or moved
  visible     /me says who holds each agent (this/other) and why a request
              is refused
  propose     asking for an agent another client holds is a transfer, even
              under the same token
  release     `agent free`, unbinding and an approved transfer each let
              another client claim it

Needs a MariaDB (see dbharness.py) and a free port.
"""
import hashlib
import importlib.util
import json
import os
import shutil
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
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"

K1, K2 = "a2a_ck_first-client-key-000000000", "a2a_ck_second-client-key-11111111"
K3, K4 = "a2a_ck_racer-three-2222222222222", "a2a_ck_racer-four-33333333333333"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    dbharness.require_db()
    tmp = Path(tempfile.mkdtemp(prefix="a2a-pin-"))
    db = dbharness.db_env()

    # The registry, in-process, on the same database the served broker uses:
    # setup, and reading what the broker recorded.
    os.environ.update(db)
    os.environ.pop("A2A_AUTH_DISABLED", None)
    spec = importlib.util.spec_from_file_location("pin_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b._startup()
    sid = b.DEFAULT_STATION_ID
    tok = b.TOKENS.create(user="pin", label="laptop")
    b.STATIONS.allow(sid, tok["prefix"])
    for agent in ("pinned", "spare", "race", "moving", "decoy", "loose",
                  "asked"):
        b.AGENTS.add(sid, agent)

    def held(agent: str) -> str | None:
        row = b.CONN.execute(
            "SELECT owner_client_hash FROM agents "
            "WHERE station_id = %s AND agent_id = %s", (sid, agent)).fetchone()
        return (row or {}).get("owner_client_hash")

    def sha(key: str) -> str:
        return hashlib.sha256(key.encode()).hexdigest()

    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, **db, A2A_HOST="127.0.0.1", A2A_PORT=str(port))
    env.pop("A2A_AUTH_DISABLED", None)

    def call(method, path, agent, key=None, body=None):
        headers = {"Authorization": f"Bearer {tok['token']}",
                   "Content-Type": "application/json"}
        if agent:
            headers["X-A2A-Agent"] = agent
        if key:
            headers["X-A2A-Client"] = key
        req = urllib.request.Request(
            f"{base}{path}", method=method, headers=headers,
            data=json.dumps(body).encode() if body is not None else None)
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                raw = r.read().decode() if not path.startswith("/stream") else ""
                return r.status, (json.loads(raw) if raw.strip() else {})
        except urllib.error.HTTPError as e:
            raw = e.read().decode("utf-8", "replace")
            try:
                return e.code, json.loads(raw)
            except ValueError:
                return e.code, {"raw": raw[:200]}

    def mcp_whoami(agent, key):
        req = urllib.request.Request(
            f"{base}/mcp/", method="POST",
            data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                             "params": {"name": "whoami",
                                        "arguments": {}}}).encode(),
            headers={"Authorization": f"Bearer {tok['token']}",
                     "Content-Type": "application/json",
                     "Accept": "application/json, text/event-stream",
                     "X-A2A-Agent": agent, "X-A2A-Client": key})
        try:
            with urllib.request.urlopen(req, timeout=15) as r:
                text = r.read().decode()
        except urllib.error.HTTPError as e:
            return {"http": e.code}
        data = [ln[5:].strip() for ln in text.splitlines()
                if ln.startswith("data:")]
        return json.loads(data[-1] if data else text or "{}")

    def stream_status(agent, key, query=None, header=None):
        headers = {"Authorization": f"Bearer {tok['token']}",
                   "X-A2A-Client": key}
        if header:
            headers["X-A2A-Agent"] = header
        req = urllib.request.Request(
            f"{base}/stream?{query or f'agent={agent}'}&format=json",
            headers=headers)
        try:
            with urllib.request.urlopen(req, timeout=10) as r:
                return r.status
        except urllib.error.HTTPError as e:
            return e.code

    log = tmp / "broker.log"
    proc = subprocess.Popen([sys.executable, str(BROKER), "serve"], env=env,
                            stdout=log.open("a"), stderr=subprocess.STDOUT)
    try:
        for _ in range(150):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"broker did not come up; see {log}")

        # --- old clients, on an agent nobody has claimed ---------------------
        code, _ = call("GET", "/channels", "spare")
        check("an agent nobody has claimed still works for a client that "
              "sends no key — an old install keeps working until a new one "
              "claims it", code == 200, str(code))
        check("and such a request claims nothing", held("spare") is None,
              str(held("spare")))

        # --- claim -----------------------------------------------------------
        code, _ = call("GET", "/channels", "pinned", K1)
        check("the first client key to act as an agent claims it",
              code == 200 and held("pinned") == sha(K1),
              f"{code} {held('pinned')}")
        code, _ = call("GET", "/channels", "pinned", K1)
        check("and keeps working for it", code == 200, str(code))

        # --- refuse ----------------------------------------------------------
        code, out = call("GET", "/channels", "pinned", K2)
        check("another client is refused (403), told the agent belongs to "
              "another client and how to release it",
              code == 403 and "another client" in json.dumps(out)
              and "agent free" in json.dumps(out), f"{code} {out}")
        code, out = call("POST", "/dms", "pinned", K2,
                         {"to": "spare", "text": "sneaking"})
        check("including for writes — it cannot send as that agent",
              code == 403, f"{code} {out}")
        code, out = call("GET", "/channels", "pinned")
        check("and a client with NO key is refused too, pointed at the "
              "installer — it is older than the broker",
              code == 403 and "installer" in json.dumps(out), f"{code} {out}")
        reply = mcp_whoami("pinned", K2)
        text = json.dumps(reply)
        check("over /mcp/ as well: the tool call is an error naming another "
              "client", "another client" in text
              and ((reply.get("result") or {}).get("isError")
                   or "error" in reply or reply.get("http") == 403), text[:300])
        check("the push stream refuses another client (403)…",
              stream_status("pinned", K2) == 403, "")
        check("…and opens for the owner", stream_status("pinned", K1) == 200, "")
        check("refusals leave the pin where it was",
              held("pinned") == sha(K1), str(held("pinned")))

        # --- visible ---------------------------------------------------------
        code, me = call("GET", "/me", "pinned", K2)
        check("/me still answers a refused client, saying why, so its status "
              "can show the one thing to do",
              code == 200 and "another client" in str(me.get("refused")),
              f"{code} {me}")
        _, mine = call("GET", "/me/agents", None, K1)
        by = {a["agent_id"]: a.get("client") for a in mine.get("agents") or []}
        check("/me/agents says who holds each agent: this client, another, "
              "or nobody yet",
              by.get("pinned") == "this" and by.get("spare") is None, str(by))
        _, mine = call("GET", "/me/agents", None, K2)
        by = {a["agent_id"]: a.get("client") for a in mine.get("agents") or []}
        check("and from the other client's side, 'other' — which is what "
              "rename_me checks before adopting", by.get("pinned") == "other",
              str(by))

        # --- stream: one agent per request, however the query is spelled ----
        # The middleware and the stream route used to parse ?agent= apart: an
        # encoded key or a repeated one was checked as the decoy and served
        # as the victim.
        for label, query in (("an encoded key (?%61gent=)",
                              "%61gent=pinned"),
                             ("a repeated key", "agent=decoy&agent=pinned")):
            check(f"/stream with {label} naming another client's agent is "
                  f"refused (403)",
                  stream_status(None, K2, query=query, header="decoy") == 403,
                  "")
        check("and the decoy it hid behind was never claimed",
              held("decoy") is None, str(held("decoy")))

        # --- realm -----------------------------------------------------------
        for label, body in (("renamed", {"rename": "stolen"}),
                            ("rebound", {"bind": True}),
                            ("moved", {"station": "default"})):
            code, out = call("PATCH", "/me/agents/pinned", "spare", K2, body)
            check(f"another client's agent cannot be {label} (403)",
                  code == 403, f"{code} {out}")
        check("and it is still there, still the first client's",
              held("pinned") == sha(K1), str(held("pinned")))

        # --- race ------------------------------------------------------------
        results: list[int] = []
        gate = threading.Barrier(2)

        def racer(key):
            gate.wait()
            results.append(call("GET", "/channels", "race", key)[0])

        threads = [threading.Thread(target=racer, args=(k,)) for k in (K3, K4)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        check("two clients claiming one agent at once: exactly one wins, the "
              "other is refused",
              sorted(results) == [200, 403]
              and held("race") in (sha(K3), sha(K4)), f"{results}")
        # The middleware resolves synchronously, so the two requests above
        # may well run one after the other. The branch a true race takes —
        # the loser read the agent as unheld, then its claim found it taken
        # — is driven here directly, against the real database.
        winner = held("race")
        loser = sha(K4) if winner == sha(K3) else sha(K3)
        try:
            b._client_gate(sid, "race", None, loser, claim=True)
            check("a claim that loses the race is refused", False, "accepted")
        except b.AgentDenied as e:
            check("a claim that loses the race is refused, as another client",
                  "another client" in str(e) and held("race") == winner,
                  f"{e} {held('race')}")

        # --- propose ---------------------------------------------------------
        call("GET", "/channels", "asked", K1)
        code, out = call("POST", "/me/proposals", None, K2,
                         {"agent_id": "asked"})
        check("asking for an agent another client holds — same token, other "
              "directory — is a transfer for an operator, not a dead end",
              code == 201 and out.get("kind") == "transfer", f"{code} {out}")
        code, out = call("POST", "/me/proposals", None, K1,
                         {"agent_id": "asked"})
        check("while its own client is told it already has it (409)",
              code == 409 and "already yours" in json.dumps(out),
              f"{code} {out}")

        # --- release ---------------------------------------------------------
        freed = subprocess.run(
            [sys.executable, str(BROKER), "agent", "free", "pinned",
             "--station", "default"], env=env, capture_output=True, text=True)
        check("`agent free` releases the client pin",
              freed.returncode == 0 and held("pinned") is None,
              freed.stdout + freed.stderr)
        code, _ = call("GET", "/channels", "pinned", K2)
        check("and then another client claims it",
              code == 200 and held("pinned") == sha(K2), str(code))
        code, _ = call("GET", "/channels", "pinned", K1)
        check("while the first one is now the refused one", code == 403,
              str(code))

        call("GET", "/channels", "loose", K1)
        code, out = call("PATCH", "/me/agents/loose", None, K1, {"bind": False})
        check("unbinding your own agent releases its client pin too",
              code == 200 and held("loose") is None,
              f"{code} {out} {held('loose')}")
        code, _ = call("GET", "/channels", "loose", K2)
        check("so another client can claim it",
              code == 200 and held("loose") == sha(K2), str(code))

        # An approved transfer hands the agent to another TOKEN — and to
        # whichever client that token connects from, so the pin is released.
        code, _ = call("GET", "/channels", "moving", K1)
        other = b.TOKENS.create(user="other-laptop")
        b.STATIONS.allow(sid, other["prefix"])
        b.PROPOSALS.propose(sid, "moving", b.TOKENS._hash_of(other["prefix"]))
        out = b.PROPOSALS.approve(sid, "moving")
        check("an approved transfer releases the client pin, so the winner's "
              "client can claim it",
              out.get("kind") == "transfer" and held("moving") is None,
              f"{out} {held('moving')}")
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — one agent, one harness-directory, decided by the broker")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
