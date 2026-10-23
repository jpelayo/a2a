#!/usr/bin/env python3
"""Claude Code's a2a tools must survive a broker restart.

    python3 tests/test_mcp_restart.py

The tools Claude Code lists come from the broker's /mcp/ endpoint, through
Claude Code's own HTTP MCP client — not through our channel process, which
has always reconnected by itself. On a redeploy (2026-10-22 17:21Z) that
client logged "Connection reset - server may have crashed or restarted", made
two event-stream retries inside two seconds, both into the 502 of a broker
still starting, and then "Maximum reconnection attempts (2) exceeded". Every
a2a tool in the session was dead until it was restarted, because the broker
kept MCP sessions in process memory: the session id the client held answered
404 "Session not found" from the new process.

So /mcp/ is stateless now, and this restarts a REAL broker under a client that
never re-initialises — exactly what Claude Code does:

  stateless   no session id is issued; a call after the restart, carrying a
              stale id, answers; and each request acts as the agent IT names,
              not the one the client named at initialize.
  the gate    the same restart against a copy of the broker with
              stateless_http removed answers 404 — a gate nobody has seen
              fail is not a gate.

Needs a MariaDB (see dbharness.py) and a free port. Auth is disabled, so
every request lands in the `default` station of a throwaway database.
"""
import json
import os
import shutil
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
PLUGIN = HERE.parent / "plugin"
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"

AGENT_A = "restart-a"
AGENT_B = "restart-b"
STATELESS = "    stateless_http=True,\n"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def healthy(base: str) -> bool:
    try:
        with urllib.request.urlopen(f"{base}/healthz", timeout=2) as r:
            return bool(json.load(r).get("ok"))
    except Exception:
        return False


def start(script: Path, env: dict, base: str, log: Path) -> subprocess.Popen:
    # Output to a file, not a pipe nobody reads: a chatty broker would fill
    # the pipe and stall, and the file is there to read when this fails.
    proc = subprocess.Popen([sys.executable, str(script), "serve"], env=env,
                            stdout=log.open("a"), stderr=subprocess.STDOUT)
    for _ in range(150):                      # ~15s for uvicorn to bind
        if healthy(base):
            return proc
        time.sleep(0.1)
    proc.kill()
    raise RuntimeError(f"broker did not come up; see {log}")


def stop(proc: subprocess.Popen | None) -> None:
    if proc is None or proc.poll() is not None:
        return
    proc.terminate()
    try:
        proc.wait(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait(timeout=10)


def payload(text: str) -> dict:
    """The JSON-RPC message in a response body, SSE-framed or plain JSON."""
    text = text.strip()
    if not text:
        return {}
    if text.startswith("{"):
        return json.loads(text)
    data = [ln[5:].strip() for ln in text.splitlines() if ln.startswith("data:")]
    return json.loads(data[-1]) if data else {"raw": text[:200]}


def post(base: str, body: dict, agent: str, session: str | None = None):
    """POST one message to /mcp/ the way an MCP HTTP client does.

    Returns (status, the Mcp-Session-Id the server issued, the message)."""
    headers = {"Content-Type": "application/json",
               "Accept": "application/json, text/event-stream",
               "X-A2A-Agent": agent}
    if session:
        headers["Mcp-Session-Id"] = session
    req = urllib.request.Request(f"{base}/mcp/", data=json.dumps(body).encode(),
                                 headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return (r.status, r.headers.get("mcp-session-id"),
                    payload(r.read().decode("utf-8", "replace")))
    except urllib.error.HTTPError as e:
        return (e.code, e.headers.get("mcp-session-id"),
                {"error": e.read().decode("utf-8", "replace")[:200]})


INIT = {"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
    "protocolVersion": "2025-06-18", "capabilities": {},
    "clientInfo": {"name": "test_mcp_restart", "version": "0"}}}
INITIALIZED = {"jsonrpc": "2.0", "method": "notifications/initialized"}


def whoami(n: int) -> dict:
    return {"jsonrpc": "2.0", "id": n, "method": "tools/call",
            "params": {"name": "whoami", "arguments": {}}}


def agent_of(msg: dict) -> str | None:
    """The `agent` whoami answered with, wherever this SDK put it."""
    result = msg.get("result") or {}
    sc = result.get("structuredContent")
    if isinstance(sc, dict):
        if "agent" in sc:
            return sc["agent"]
        if isinstance(sc.get("result"), dict):
            return sc["result"].get("agent")
    for c in result.get("content") or []:
        try:
            return json.loads(c.get("text") or "").get("agent")
        except (ValueError, AttributeError):
            continue
    return None


def scenario(script: Path, tmp: Path, label: str) -> dict:
    """Initialize, call, RESTART the broker, call again without initializing."""
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, **dbharness.db_env(), A2A_AUTH_DISABLED="1",
               A2A_HOST="127.0.0.1", A2A_PORT=str(port),
               A2A_PLUGIN_SRC=str(PLUGIN))
    for agent in (AGENT_A, AGENT_B):
        subprocess.run([sys.executable, str(script), "agent", "add", agent,
                        "--station", "default"],
                       env=env, capture_output=True, text=True, check=True)
    log = tmp / f"{label}.log"
    out: dict = {}
    proc = None
    try:
        proc = start(script, env, base, log)
        code, sid, _ = post(base, INIT, AGENT_A)
        out["init"] = code
        out["session"] = sid
        post(base, INITIALIZED, AGENT_A, sid)
        code, _, msg = post(base, whoami(2), AGENT_A, sid)
        out["before_a"] = (code, agent_of(msg))
        code, _, msg = post(base, whoami(3), AGENT_B, sid)
        out["before_b"] = (code, agent_of(msg))

        stop(proc)
        proc = start(script, env, base, log)
        # What a client that never re-initialises sends next: the same
        # headers, and the session id it was given — or, given none, a stale
        # one, which must be ignored rather than refused.
        stale = sid or "a-session-from-before-the-restart"
        code, _, msg = post(base, whoami(4), AGENT_A, stale)
        out["after"] = (code, agent_of(msg), msg)
    finally:
        stop(proc)
    return out


def main() -> int:
    dbharness.require_db()
    tmp = Path(tempfile.mkdtemp(prefix="a2a-mcp-restart-"))
    try:
        src = BROKER.read_text()
        check("the broker builds its MCP server stateless",
              STATELESS in src, "stateless_http=True is not in the FastMCP call")

        got = scenario(BROKER, tmp, "stateless")
        check("initialize answers, and issues NO session id — there is no "
              "session for a restart to take away",
              got["init"] == 200 and not got["session"], str(got))
        check("a tool call answers as the agent its request names",
              got["before_a"] == (200, AGENT_A), str(got["before_a"]))
        check("and the next request, naming another agent, answers as THAT "
              "one — identity is per request, not frozen at initialize",
              got["before_b"] == (200, AGENT_B), str(got["before_b"]))
        code, agent, msg = got["after"]
        check("after a RESTART, a call with no re-initialize and a stale "
              "session id still answers — the call Claude Code makes next, "
              "which a stateful broker answered 404",
              code == 200 and agent == AGENT_A, f"{code} {agent} {msg}")

        # --- the gate: put the session back and watch the same call fail ------
        stateful = tmp / "stateful"
        stateful.mkdir()
        broken = stateful / "a2a-mcp.py"
        broken.write_text(src.replace(STATELESS, "", 1))
        old = scenario(broken, tmp, "stateful")
        check("the gate is real: a stateful copy issues a session id…",
              old["init"] == 200 and bool(old["session"]), str(old))
        check("…and after the same restart answers 404, which is the outage "
              "this test exists to keep out",
              old["after"][0] == 404, str(old["after"]))
    finally:
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — a broker restart costs Claude Code's a2a tools nothing")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
