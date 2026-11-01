#!/usr/bin/env python3
"""SIGTERM ends every /stream, so `serve` exits by itself — quickly and cleanly.

    python3 tests/test_graceful_shutdown.py

`docker compose stop` / `up --build` sends SIGTERM (tini forwards it). uvicorn
then closes the listener and waits — with no deadline unless one is set — for
every open connection to finish, and a /stream never finishes: on an active
response uvicorn only sets keep_alive=False, which the stream loop cannot see.
So every deploy sat out Docker's 10 s and ended in SIGKILL, which skips the
stream's own `finally` (release the claim, hand back what was delivered and
never acked) and the lifespan's shutdown half.

What this pins, against a REAL `serve` over HTTP:

  streams     with an agent stream and the firehose open, SIGTERM ends both
              (EOF from the server, not a reset), the process exits within
              3 s — at the DEFAULT 5 s keepalive, so an exit that waited for
              the next tick would miss it — the unacked delivery is handed
              back, the lifespan shuts down, and nothing is cancelled: no
              "Exception in ASGI application" traceback per client.
  backstop    a request that is not a stream and never finishes (a POST whose
              body never comes) is cancelled after SHUTDOWN_GRACE_S, inside
              Docker's 10 s, instead of holding the stop until SIGKILL.
"""
import json
import os
import re
import signal
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from pathlib import Path

import dbharness
import pymysql
import pymysql.cursors

BROKER = Path(__file__).resolve().parent.parent / "deploy" / "a2a_mcp" / "a2a-mcp.py"
GRACE = int(re.search(r"^SHUTDOWN_GRACE_S = (\d+)", BROKER.read_text(),
                      re.M).group(1))

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def wait(cond, seconds: float = 12.0) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.1)
    return False


class Stream:
    """One /stream connection read on a thread. `eof` means the SERVER ended
    the response properly; a reset or an error leaves it False."""

    def __init__(self, base: str, agent: str | None):
        q = f"?agent={agent}&format=json" if agent else "?format=json"
        headers = {"Authorization": "Bearer dev"}
        if agent:
            headers["X-A2A-Agent"] = agent
        self.resp = urllib.request.urlopen(
            urllib.request.Request(f"{base}/stream{q}", headers=headers),
            timeout=30)
        self.lines: list[dict] = []
        self.eof = False
        self.ended = threading.Event()
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        try:
            for raw in self.resp:
                s = raw.decode("utf-8", "replace").strip()
                if s:
                    try:
                        self.lines.append(json.loads(s))
                    except ValueError:
                        pass
            self.eof = True
        except Exception:
            pass
        self.ended.set()

    def texts(self) -> list[str]:
        return [m.get("text", "") for m in self.lines]


def start(env: dict, err: Path) -> subprocess.Popen:
    proc = subprocess.Popen([sys.executable, str(BROKER), "serve"], env=env,
                            stdout=subprocess.DEVNULL,
                            stderr=err.open("w"))
    base = f"http://127.0.0.1:{env['A2A_PORT']}"
    for _ in range(150):
        try:
            urllib.request.urlopen(f"{base}/healthz", timeout=2)
            return proc
        except Exception:
            time.sleep(0.1)
    proc.kill()
    raise RuntimeError("broker did not come up: " + err.read_text()[-400:])


def stop(proc: subprocess.Popen, seconds: float) -> float | None:
    """SIGTERM, and how long until it exited — None if it did not."""
    t0 = time.monotonic()
    proc.send_signal(signal.SIGTERM)
    try:
        proc.wait(timeout=seconds)
        return time.monotonic() - t0
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
        return None


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-shutdown-"))
    db = dbharness.db_env()
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, **db, A2A_HOST="127.0.0.1", A2A_PORT=str(port),
               A2A_AUTH_DISABLED="1")
    env.pop("A2A_STREAM_KEEPALIVE", None)      # the default 5 s, on purpose

    def cli(*args: str) -> None:
        subprocess.run([sys.executable, str(BROKER), *args], env=env,
                       capture_output=True, text=True, check=True)

    def q(sql: str, params=()):
        c = pymysql.connect(host=db["A2A_DB_HOST"], port=int(db["A2A_DB_PORT"]),
                            user=db["A2A_DB_USER"],
                            password=db["A2A_DB_PASSWORD"],
                            database=db["A2A_DB_NAME"], autocommit=True,
                            cursorclass=pymysql.cursors.DictCursor)
        try:
            with c.cursor() as cur:
                cur.execute(sql, params)
                return cur.fetchall()
        finally:
            c.close()

    cli("agent", "add", "watched", "--station", "default")
    cli("agent", "add", "peer", "--station", "default")
    cli("channel", "create", "ops", "--station", "default",
        "--members", "watched,peer")

    # --- streams open, then SIGTERM -----------------------------------------
    err = tmp / "serve.err"
    proc = start(env, err)
    agent = Stream(base, "watched")
    fire = Stream(base, None)
    time.sleep(0.5)
    req = urllib.request.Request(
        f"{base}/channels/ops/messages",
        data=json.dumps({"sender": "peer", "text": "before the stop"}).encode(),
        headers={"Authorization": "Bearer dev", "X-A2A-Agent": "peer",
                 "Content-Type": "application/json"}, method="POST")
    urllib.request.urlopen(req, timeout=10)
    got = wait(lambda: "before the stop" in agent.texts())
    stamped = q("SELECT delivered_at, acked_at FROM message_receipts "
                "WHERE agent_id = 'watched'")
    check("the fixture is honest: the stream delivered a message nobody "
          "acked, and nothing has ended yet",
          got and stamped and stamped[0]["delivered_at"] is not None
          and stamped[0]["acked_at"] is None
          and not agent.ended.is_set() and not fire.ended.is_set(),
          f"{agent.texts()} {stamped}")

    took = stop(proc, 20)
    check("SIGTERM with streams open: serve exits within 3 s — at the default "
          "5 s keepalive, so it did not wait for a tick, and nowhere near "
          "Docker's 10 s SIGKILL",
          took is not None and took < 3.0,
          "never exited" if took is None else f"{took:.1f}s")
    # Not 0, by uvicorn's design: after a graceful shutdown it restores the
    # default handler and re-raises the signal it caught (server.py
    # capture_signals), so the process ends as a SIGTERM'd one — 143 to
    # Docker — exactly as it did under uvicorn.run.
    check("…ending as a process stopped by SIGTERM does (uvicorn re-raises "
          "the signal after its graceful shutdown)",
          proc.returncode == -signal.SIGTERM, str(proc.returncode))
    check("both streams were ENDED by the server, properly — the agent stream "
          "and the firehose — so each client reconnects as after any close",
          agent.ended.wait(5) and fire.ended.wait(5)
          and agent.eof and fire.eof,
          f"agent eof={agent.eof}, firehose eof={fire.eof}")
    after = q("SELECT delivered_at, acked_at FROM message_receipts "
              "WHERE agent_id = 'watched'")
    check("the stream's `finally` ran: what it delivered and was never acked "
          "is handed back to the queue — what a SIGKILL skipped",
          after and after[0]["delivered_at"] is None
          and after[0]["acked_at"] is None, str(after))
    events = {r["event"] for r in q(
        "SELECT event FROM logs WHERE event IN ('serve.stop', "
        "'stream.unstamp')")}
    check("the broker logged the stop and the hand-back",
          events == {"serve.stop", "stream.unstamp"}, str(events))
    out = err.read_text(errors="replace")
    check("the lifespan's shutdown half ran (collector cancelled, MCP "
          "session manager closed)",
          "Application shutdown complete" in out, out[-400:])
    check("and nothing was cancelled: no 'Exception in ASGI application' "
          "traceback per client, no graceful-shutdown timeout",
          "Exception in ASGI application" not in out
          and "timeout graceful shutdown exceeded" not in out, out[-600:])

    # --- the backstop: a request that is not a stream and never ends -------
    port2 = free_port()
    env2 = dict(env, A2A_PORT=str(port2))
    err2 = tmp / "serve2.err"
    proc2 = start(env2, err2)
    hung = socket.create_connection(("127.0.0.1", port2), timeout=30)
    hung.sendall(
        b"POST /channels/ops/messages HTTP/1.1\r\nHost: 127.0.0.1\r\n"
        b"Authorization: Bearer dev\r\nX-A2A-Agent: peer\r\n"
        b"Content-Type: application/json\r\nContent-Length: 100\r\n\r\n")
    time.sleep(0.8)                  # its handler is now waiting for the body
    took2 = stop(proc2, GRACE + 15)
    hung.close()
    out2 = err2.read_text(errors="replace")
    check(f"a request that never finishes is cancelled after "
          f"SHUTDOWN_GRACE_S ({GRACE} s) and serve exits — inside Docker's "
          "10 s, instead of holding the stop until SIGKILL",
          took2 is not None and took2 < GRACE + 3,
          "never exited" if took2 is None else f"{took2:.1f}s")
    check("…and the log names that cancel, so an operator can see it",
          "timeout graceful shutdown exceeded" in out2, out2[-600:])

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — a stop ends every stream and the broker exits by itself")
    return 1 if fails else 0


if __name__ == "__main__":
    dbharness.require_db()
    sys.exit(main())
