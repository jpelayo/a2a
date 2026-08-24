#!/usr/bin/env python3
"""One agent, one live stream — the newest connection wins, and nothing is lost.

    python3 tests/test_stream_supersede.py

Delivery is a destructive read: fetching stamps delivered_at, a live stream
only fetches delivered_at IS NULL, and replay is one pass per connection.
Nothing used to stop TWO streams serving one agent — and a client that died
SILENTLY (laptop sleep, NAT drop: no FIN, so is_disconnected() stays false
while the kernel retransmits for tens of minutes) left exactly that. The
zombie raced the reconnected client for every receipt, won about half, and
wrote its winnings into a dead socket. The client reported a healthy stream —
its own keepalives flowed — while messages vanished into rows marked
delivered that nothing would ever push again.

That was "push stops working after some hours". Not config, not the client:
every sleep event minted a fresh theft window.

What this file pins, against a REAL broker over HTTP:

  newest wins    a second /stream?agent=X ends the first within a tick
  nothing stolen a message posted during the overlap reaches the LIVE
                 connection, even if the zombie fetched it first
  scoped         the firehose (no ?agent=) is not superseded — several
                 operators may watch it at once — and agent Y never evicts X

Needs a MariaDB/MySQL via dbharness, a free port, and the broker's deps.
"""
import json
import os
import socket
import subprocess
import sys
import threading
import time
import urllib.request
from pathlib import Path

import dbharness

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


class Stream:
    """One /stream connection, reading lines on a thread.

    Mimics what every client does: park on the response and collect lines.
    `closed` going true is the eviction signal — the server ended the
    response, which a real client answers by reconnecting.
    """

    def __init__(self, base: str, token: str, agent: str | None):
        q = f"?agent={agent}&format=json" if agent else "?format=json"
        req = urllib.request.Request(
            f"{base}/stream{q}",
            headers={"Authorization": f"Bearer {token}"},
        )
        self.resp = urllib.request.urlopen(req, timeout=30)
        self.lines: list[dict] = []
        self.closed = False
        threading.Thread(target=self._read, daemon=True).start()

    def _read(self) -> None:
        try:
            for raw in self.resp:
                s = raw.decode("utf-8", "replace").strip()
                if not s:
                    continue  # keepalive
                try:
                    self.lines.append(json.loads(s))
                except ValueError:
                    pass
        except Exception:
            pass
        self.closed = True

    def texts(self) -> list[str]:
        return [m.get("text", "") for m in self.lines]


def wait(cond, seconds: float = 12.0) -> bool:
    deadline = time.time() + seconds
    while time.time() < deadline:
        if cond():
            return True
        time.sleep(0.2)
    return False


def main() -> int:
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, **dbharness.db_env(),
               A2A_HOST="127.0.0.1", A2A_PORT=str(port),
               A2A_AUTH_DISABLED="1",
               A2A_STREAM_KEEPALIVE="1")   # fast ticks: eviction within ~1s

    def cli(*args: str) -> str:
        return subprocess.run(
            [sys.executable, str(BROKER), *args],
            env=env, capture_output=True, text=True, check=True).stdout

    cli("agent", "add", "watched", "--station", "default")
    cli("agent", "add", "peer", "--station", "default")
    cli("channel", "create", "ops", "--station", "default",
        "--members", "watched,peer")

    proc = subprocess.Popen([sys.executable, str(BROKER), "serve"], env=env,
                            stdout=subprocess.DEVNULL,
                            stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("broker did not come up")

        tok = "anything"  # auth disabled: every token is the default station

        def post(text: str) -> None:
            req = urllib.request.Request(
                f"{base}/channels/ops/messages",
                data=json.dumps({"sender": "peer", "text": text}).encode(),
                headers={"Authorization": f"Bearer {tok}",
                         "X-A2A-Agent": "peer",
                         "Content-Type": "application/json"},
                method="POST")
            urllib.request.urlopen(req, timeout=10)

        # --- baseline: one stream, one delivery ------------------------------
        a = Stream(base, tok, "watched")
        post("first: to the only stream")
        check("a lone stream receives what is posted",
              wait(lambda: "first: to the only stream" in a.texts()),
              str(a.texts()))

        # --- the zombie scenario ---------------------------------------------
        # `a` stays OPEN — that is the point. A silently dead client looks to
        # the broker exactly like this socket: connected, consuming nothing.
        b = Stream(base, tok, "watched")
        check("a second connection for the same agent ends the first — the "
              "broker now enforces one live stream per agent, newest wins",
              wait(lambda: a.closed), "old stream still open")

        # These readers never ack — like a client that died mid-push. So the
        # message delivered to A alone is exactly the stranded case: stamped
        # delivered, confirmed by nobody. A's eviction must give it back.
        check("what the evicted stream took and never got confirmed is "
              "redelivered to the live one — HOWEVER old it is, which is "
              "what the 600s replay window could not do",
              wait(lambda: "first: to the only stream" in b.texts()),
              str(b.texts()))

        post("second: after the takeover")
        check("and what is posted after the takeover reaches the LIVE "
              "connection — before this, the zombie won about half of "
              "these and wrote them into a dead socket",
              wait(lambda: "second: after the takeover" in b.texts()),
              str(b.texts()))
        check("while the zombie got nothing new",
              "second: after the takeover" not in a.texts(), str(a.texts()))

        # --- rapid handover: the race the second replay pass exists for ------
        c = Stream(base, tok, "watched")
        post("third: during the handover")
        check("a message posted in the instant of a handover still arrives — "
              "the successor replays what a mid-fetch predecessor may have "
              "stamped seconds earlier",
              wait(lambda: "third: during the handover" in c.texts()),
              f"c={c.texts()}")

        # --- scope -----------------------------------------------------------
        d = Stream(base, tok, "peer")
        check("agent Y's stream does not evict agent X's",
              not wait(lambda: c.closed, 3), "X was evicted by Y")
        f1 = Stream(base, tok, None)
        f2 = Stream(base, tok, None)
        check("two firehose watchers coexist — supersede is per agent, and "
              "the firehose names none",
              not wait(lambda: f1.closed or f2.closed, 3),
              "a firehose was evicted")
        del d, f2

    finally:
        proc.terminate()
        proc.wait(timeout=10)

    print()
    if fails:
        print(f"{len(fails)} failure(s):")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("PASS — one agent, one live stream, and the newest one is it")
    return 0


if __name__ == "__main__":
    dbharness.require_db()
    sys.exit(main())
