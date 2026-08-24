#!/usr/bin/env python3
"""A silent link must be noticed, not waited on forever.

    python3 tests/test_stream_recovery.py

The Claude Code channel stopped delivering after a laptop lid close and came
back only on a full session restart. The cause was one missing keyword: the
stream was opened with no read timeout, so when the link died *silently* — no
FIN, no RST, just nothing — the client blocked on a read that would never
return. It never sends anything on that socket, so it had no way to discover
the peer was gone; python does not enable TCP keepalive, and macOS's own idle
default is two hours.

Nothing failed. `_state["connected"]` stayed True, `a2a_channel_status`
reported a healthy channel, and the reconnect loop never ran.

This suite is the fixture that would have caught it: a server that sends
headers and one line and then goes quiet forever, which is exactly what a
half-open socket looks like from the client's side.

Needs python3. No broker and no database.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
CHANNEL = PLUGIN / "a2a" / "server" / "a2a-channel.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


class SilentStream:
    """Serves /stream, sends one message, then never speaks again.

    Also counts connections: the whole point is that the client comes back.
    """

    def __init__(self, go_silent: bool = True):
        self.go_silent = go_silent
        self.connections = 0
        self._sock = socket.socket()
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self.port = self._sock.getsockname()[1]
        self._stop = threading.Event()
        self._held: list[socket.socket] = []
        threading.Thread(target=self._serve, daemon=True).start()

    def _serve(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _ = self._sock.accept()
            except OSError:
                return
            threading.Thread(target=self._handle, args=(conn,),
                             daemon=True).start()

    def _handle(self, conn: socket.socket) -> None:
        try:
            req = conn.recv(65536).decode("utf-8", "replace")
            path = req.split(" ")[1] if " " in req else "/"
            if "/stream" not in path:
                # /me, /channels and friends: enough for the client to get
                # past startup and reach the stream, which is what we test.
                body = json.dumps({
                    "agent": "probe", "registered": True, "stations": ["s"],
                    "channels": [], "agents": [], "ok": True,
                }).encode()
                conn.sendall(b"HTTP/1.1 200 OK\r\nContent-Type: "
                             b"application/json\r\nContent-Length: "
                             + str(len(body)).encode() + b"\r\n\r\n" + body)
                conn.close()
                return
            self.connections += 1
            conn.sendall(b"HTTP/1.1 200 OK\r\n"
                         b"Content-Type: text/plain\r\n"
                         b"Transfer-Encoding: chunked\r\n\r\n")
            line = json.dumps({"id": f"m{self.connections}", "channel": "ops",
                               "sender": "peer", "text": "hello"}) + "\n"
            chunk = line.encode()
            conn.sendall(f"{len(chunk):x}\r\n".encode() + chunk + b"\r\n")
            if self.go_silent:
                # The failure being reproduced: the socket stays OPEN and
                # simply never carries another byte. No close, no reset —
                # indistinguishable from a live idle link, except that the
                # broker's keepalive never arrives.
                self._held.append(conn)
                while not self._stop.is_set():
                    time.sleep(0.2)
            else:
                conn.close()
        except OSError:
            pass

    def stop(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass


def run_client(script: Path, port: int, tmp: Path, timeout: str,
               seconds: float, extra_env: dict | None = None) -> tuple:
    env = dict(
        os.environ,
        A2A_URL=f"http://127.0.0.1:{port}",
        A2A_TOKEN="a2a_st_test",
        A2A_AGENT="probe",
        A2A_STREAM_TIMEOUT=timeout,
        A2A_HELLO="0",
        A2A_IDENTITY_STORE=str(tmp),
        A2A_LOG_FILE=str(tmp / "client.log"),
        A2A_AGENT_DIR=str(tmp),
        A2A_READ_ON_INIT="0",
        **(extra_env or {}),
    )
    proc = subprocess.Popen([sys.executable, str(script)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env, text=True)
    if script.name.endswith("a2a-channel.py"):
        proc.stdin.write(json.dumps(
            {"jsonrpc": "2.0", "id": 1, "method": "initialize",
             "params": {"protocolVersion": "2024-11-05"}}) + "\n")
        proc.stdin.write(json.dumps(
            {"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        proc.stdin.flush()
    time.sleep(seconds)
    proc.terminate()
    try:
        out, err = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, err = proc.communicate()
    return out, err


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-stream-"))

    # --- the bug: a silent link must be given up on and retried ------------
    srv = SilentStream()
    try:
        # Timeout of 2s, run for ~9s: without the fix the client connects once
        # and blocks forever, so `connections` stays at 1.
        _, err = run_client(CHANNEL, srv.port, tmp / "a", "2", 9.0)
        check("the channel reconnects after a silent link, instead of "
              "blocking on a read that will never return",
              srv.connections >= 2, f"{srv.connections} connection(s)\n"
                                    f"{err[-400:]}")
        check("and says why, distinctly from a server-side close",
              "silent" in err and "reconnecting now" in err, err[-400:])
    finally:
        srv.stop()

    # --- a normal close still reconnects, and is reported differently ------
    srv2 = SilentStream(go_silent=False)
    try:
        _, err = run_client(CHANNEL, srv2.port, tmp / "b", "30", 8.0)
        check("a server that closes the stream is reconnected to as before",
              srv2.connections >= 2, f"{srv2.connections} connection(s)")
        check("and is NOT reported as a timeout — the two are separable "
              "afterwards, which is what makes a log worth keeping",
              "closed by server" in err, err[-300:])
    finally:
        srv2.stop()

    # --- the client keeps its own log, capped ------------------------------
    log = tmp / "a" / "client.log"
    check("the channel writes a log file of its own, rather than only to "
          "stderr that nothing captures unless debug logging is on",
          log.is_file() and log.stat().st_size > 0,
          str(log))

    srv3 = SilentStream()
    try:
        d = tmp / "c"
        _, _ = run_client(CHANNEL, srv3.port, d, "1", 8.0,
                          {"A2A_LOG_MAX_BYTES": "2048"})
        capped = d / "client.log"
        size = capped.stat().st_size if capped.is_file() else 0
        check("and caps it, so a client left running for weeks cannot fill "
              "a disk with its own diagnostics",
              0 < size <= 2048 * 1.5, f"{size} bytes")
        body = capped.read_text(errors="replace") if capped.is_file() else ""
        check("keeping the most recent lines, which are the ones that "
              "diagnose a hang",
              "earlier lines dropped" in body and "reconnecting" in body,
              body[:200])
    finally:
        srv3.stop()

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — a link that goes quiet is noticed, not waited on")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
