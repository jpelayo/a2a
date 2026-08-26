#!/usr/bin/env python3
"""The Codex client, without Codex and without a database.

    python3 tests/test_codex_client.py

Three of this client's parts are new to the project and cannot be covered by
the shared suites, so they are covered here:

  the websocket    every other client talks HTTP. This one also speaks
                   websocket-over-AF_UNIX to a private Codex app-server,
                   framing and all, so a mock app-server answers it here.
  the ack rule     a message is acked ONLY once the turn has been accepted. If
                   injection fails the message must stay pending on the broker
                   — my_pending and the next reconnect are what recover it, and
                   a premature ack would destroy it silently.
  the target rule  the socket is shared by whichever sessions attach to that
                   server, so the client injects only when EXACTLY ONE live
                   thread's cwd is its own; several matches means any choice
                   could hit the wrong window and none is made.
  degraded mode    with no reachable app-server (a plain `codex`) the tools
                   must work and push must be off, quietly — and the broker
                   stream must never be claimed, because delivery is a
                   destructive read.

Pure python3 stdlib.
"""
import base64
import hashlib
import importlib.util
import inspect
import json
import os
import socket
import struct
import subprocess
import sys
import tempfile
import threading
from pathlib import Path

# The clients live in plugin/; the suite lives here, beside it.
CLIENT = Path(__file__).resolve().parent.parent / "plugin" / "codex" / "a2a-codex.py"
GUID = "258EAFA5-E914-47DA-95CA-C5AB0DC85B11"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load(sock: str = "", agent: str = "", cwd: str | None = None):
    """A fresh copy of the client with a chosen environment."""
    os.environ["A2A_URL"] = "https://broker.invalid"
    os.environ["A2A_TOKEN"] = "a2a_st_test"
    os.environ["A2A_CODEX_SOCK"] = sock
    if agent:
        os.environ["A2A_AGENT"] = agent
    else:
        os.environ.pop("A2A_AGENT", None)
    if cwd:
        os.chdir(cwd)
    spec = importlib.util.spec_from_file_location("a2a_codex_t", CLIENT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class MockAppServer(threading.Thread):
    """Just enough app-server: the websocket upgrade, then JSON-RPC replies."""

    def __init__(self, path, threads=("t-1",), cwds=None):
        super().__init__(daemon=True)
        self.path = path
        self.threads = list(threads)
        # thread id -> the cwd thread/read reports for it
        self.cwds = dict(cwds or {t: os.getcwd() for t in self.threads})
        self.srv = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        self.srv.bind(path)
        self.srv.listen(1)
        self.turns = []          # text of every turn/start received
        self.masked = []         # was each client frame masked?
        self.methods = []
        self.fail_turns = False

    def run(self):
        # Sequentially, forever: a failed submit closes the socket and retries,
        # so a one-shot accept would make the retry look like a second failure.
        while True:
            try:
                conn, _ = self.srv.accept()
            except OSError:
                return
            self.serve(conn)

    def serve(self, conn):
        buf = b""
        while b"\r\n\r\n" not in buf:
            chunk = conn.recv(1)
            if not chunk:
                return
            buf += chunk
        key = ""
        for line in buf.decode().split("\r\n"):
            if line.lower().startswith("sec-websocket-key:"):
                key = line.split(":", 1)[1].strip()
        accept = base64.b64encode(
            hashlib.sha1((key + GUID).encode()).digest()).decode()
        conn.sendall(
            f"HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\n"
            f"Connection: Upgrade\r\nSec-WebSocket-Accept: {accept}\r\n\r\n"
            .encode())

        data = b""
        while True:
            try:
                chunk = conn.recv(65536)
            except OSError:
                return
            if not chunk:
                return
            data += chunk
            while True:
                frame, data, was_masked = self._take(data)
                if frame is None:
                    break
                self.masked.append(was_masked)
                try:
                    msg = json.loads(frame)
                except ValueError:
                    continue
                self.methods.append(msg.get("method"))
                reply = self._reply(msg)
                if reply is not None:
                    self._send(conn, reply)

    @staticmethod
    def _take(data):
        if len(data) < 2:
            return None, data, None
        masked = bool(data[1] & 0x80)
        ln, off = data[1] & 0x7F, 2
        if ln == 126:
            if len(data) < 4:
                return None, data, None
            ln = struct.unpack(">H", data[2:4])[0]
            off = 4
        elif ln == 127:
            if len(data) < 10:
                return None, data, None
            ln = struct.unpack(">Q", data[2:10])[0]
            off = 10
        need = off + (4 if masked else 0) + ln
        if len(data) < need:
            return None, data, None
        if masked:
            mask = data[off:off + 4]
            payload = bytes(b ^ mask[i % 4] for i, b in
                            enumerate(data[off + 4:off + 4 + ln]))
        else:
            payload = data[off:off + ln]
        return payload, data[need:], masked

    def _reply(self, msg):
        method, rid = msg.get("method"), msg.get("id")
        if method == "initialize":
            return {"id": rid, "result": {"userAgent": "mock"}}
        if method == "thread/loaded/list":
            return {"id": rid, "result": {"data": self.threads,
                                          "nextCursor": None}}
        if method == "thread/read":
            tid = msg["params"]["threadId"]
            return {"id": rid, "result": {"thread": {
                "id": tid, "cwd": self.cwds.get(tid, "")}}}
        if method == "turn/start":
            if self.fail_turns:
                return {"id": rid, "error": {"code": -32600,
                                             "message": "turn in progress"}}
            self.turns.append(msg["params"]["input"][0]["text"])
            return {"id": rid, "result": {"turn": {"id": "turn-1",
                                                   "status": "inProgress"}}}
        return None if rid is None else {"id": rid, "result": {}}

    @staticmethod
    def _send(conn, obj):
        payload = json.dumps(obj).encode()
        n = len(payload)
        if n < 126:
            head = b"\x81" + bytes([n])
        elif n < 1 << 16:
            head = b"\x81\x7e" + struct.pack(">H", n)
        else:
            head = b"\x81\x7f" + struct.pack(">Q", n)
        conn.sendall(head + payload)


def main() -> int:
    # resolve(): macOS TMPDIR lives behind a /var -> /private/var symlink, and
    # the client compares os.getcwd() (already resolved) against thread cwds.
    tmp = Path(tempfile.mkdtemp(prefix="a2acx-")).resolve()

    # --- identity ladder ----------------------------------------------------
    proj = tmp / "my-project"
    proj.mkdir()
    b = load(cwd=str(proj))
    check("with no store and no env, the id is the project directory's name — "
          "what every client sent before the store existed, so an upgrade is "
          "invisible to agents already registered",
          b.KEY == "my-project" and not b.EXPLICIT, f"{b.KEY} {b.EXPLICIT}")

    b2 = load(agent="chosen-1", cwd=str(proj))
    check("A2A_AGENT wins and is EXPLICIT, so the store is never written for "
          "it: two instances in one directory would otherwise fight over the "
          "same key",
          b2.KEY == "chosen-1" and b2.EXPLICIT, f"{b2.KEY} {b2.EXPLICIT}")

    # --- the websocket layer, against a real socket -------------------------
    sock = str(tmp / "s.sock")
    mock = MockAppServer(sock, cwds={"t-1": str(proj)})
    mock.start()
    b3 = load(sock=sock, cwd=str(proj))
    srv3 = b3.get_server()
    srv3.submit("hello-1")
    check("submit completes the handshake in order — initialize, initialized, "
          "then requests; anything sent before that is rejected by a real "
          "app-server",
          mock.methods[:2] == ["initialize", "initialized"],
          str(mock.methods[:3]))
    check("it finds the session thread by matching thread/read's cwd against "
          "its own — the app-server spawns this client with cwd = the "
          "thread's project directory",
          srv3.thread_id == "t-1", str(srv3.thread_id))
    check("and the text arrives as a turn", mock.turns == ["hello-1"],
          str(mock.turns))
    check("EVERY client frame is masked — an unmasked frame is a protocol "
          "violation a real server closes the connection over",
          mock.masked and all(mock.masked), str(mock.masked))

    big = "x" * 70000
    srv3.submit(big)
    check("a payload over 64 KiB uses the 8-byte length form and survives "
          "intact — messages carry whole plans",
          mock.turns[-1] == big, f"{len(mock.turns[-1])} bytes")

    # --- resume is not used -------------------------------------------------
    check("thread/resume is never called: the TUI is already subscribed to its "
          "own thread, and resume on a thread with no rollout yet fails "
          "outright",
          "thread/resume" not in mock.methods, str(set(mock.methods)))

    # --- the target rule -----------------------------------------------------
    sockx = str(tmp / "sx.sock")
    mockx = MockAppServer(sockx, threads=("t-a", "t-b", "t-c"),
                          cwds={"t-a": str(proj), "t-b": str(proj),
                                "t-c": "/somewhere/else"})
    mockx.start()
    bx = load(sock=sockx, cwd=str(proj))
    try:
        bx.get_server().session_thread()
        check("two live threads in one directory REFUSE rather than guess — "
              "either choice could inject into the wrong window, and a "
              "refused message survives on the broker while a mis-delivered "
              "one is gone",
              False, "picked one anyway")
    except bx.WSError as e:
        check("two live threads in one directory REFUSE rather than guess — "
              "either choice could inject into the wrong window, and a "
              "refused message survives on the broker while a mis-delivered "
              "one is gone",
              "2 live threads" in str(e), str(e))
    check("and a foreign thread is never counted: only cwd matches compete",
          "t-c" not in str(bx.get_server().thread_id), "")

    socky = str(tmp / "sy.sock")
    mocky = MockAppServer(socky, threads=("t-z",),
                          cwds={"t-z": "/some/other/project"})
    mocky.start()
    by = load(sock=socky, cwd=str(proj))
    check("a socket whose only thread belongs to ANOTHER project yields no "
          "target: push stays off instead of borrowing a stranger's window",
          by.get_server().session_thread() is None
          and not by._injectable(),
          str(by.get_server().thread_id))

    # --- the ack rule -------------------------------------------------------
    sock2 = str(tmp / "s2.sock")
    mock2 = MockAppServer(sock2, cwds={"t-1": str(proj)})
    mock2.fail_turns = True
    mock2.start()
    b4 = load(sock=sock2, cwd=str(proj))
    calls = []
    b4.api = lambda method, path, body=None, timeout=30: (
        calls.append((method, path, body)) or {})
    msg = {"id": "m-1", "channel": "ops", "sender": "bob", "text": "hi",
           "audience": ["me"], "addressed": []}
    b4.emit(json.dumps(msg))
    check("a message that could NOT be injected is not acked — it stays "
          "pending on the broker, where my_pending and the next reconnect "
          "find it; acking early would destroy it silently",
          not [c for c in calls if c[1] == "/ack"], str(calls))
    check("and it is not remembered as seen, so the broker's replay redelivers "
          "it rather than being deduped away",
          "m-1" not in b4._seen, str(b4._seen))

    mock2.fail_turns = False
    b4.emit(json.dumps(msg))
    check("once the turn is accepted it is acked, in one batch",
          [c for c in calls if c[1] == "/ack"]
          and calls[-1][2] == {"ids": ["m-1"]}, str(calls[-1:]))
    check("the brief rides on the first delivery, so a session that has never "
          "called an a2a tool still learns the rules",
          mock2.turns and "ADDRESSING IS AN ARGUMENT" in mock2.turns[0],
          (mock2.turns or [""])[0][:60])
    check("and not on the second — it is a greeting, not a preamble",
          len(mock2.turns) == 1 or "ADDRESSING IS AN ARGUMENT"
          not in mock2.turns[-1])

    # --- degraded mode ------------------------------------------------------
    # A2A_CODEX_SOCK set-but-empty disables push outright (tests and opt-out);
    # UNSET means autodetect the well-known control socket under CODEX_HOME.
    b5 = load(sock="", cwd=str(proj))
    check("with push disabled there is no app-server connection at all, and "
          "deliver() refuses rather than pretending",
          b5.get_server() is None and not b5.deliver(msg))
    check("and the pump never claims the broker stream — delivery is a "
          "destructive read, so a session that cannot render must not consume "
          "its agent's inbox",
          not b5._injectable())

    # --- discovery from the parent: what makes isolation structural ----------
    b7 = load(cwd=str(proj))
    os.environ.pop("A2A_CODEX_SOCK", None)
    b7._SOCK_ENV = None
    iso = Path(tempfile.mkdtemp(prefix="iso-", dir="/private/tmp")).resolve()
    mine, theirs = str(iso / "mine.sock"), str(iso / "theirs.sock")
    m_mine = MockAppServer(mine, cwds={"t-1": str(proj)})
    m_theirs = MockAppServer(theirs, threads=("t-9",), cwds={"t-9": str(proj)})
    m_mine.start(); m_theirs.start()

    b7._parent_argv = lambda: ""
    check("a plain `codex` — in-process app-server, no --listen in the "
          "parent's argv — discovers nothing, so push is off and no other "
          "session is reachable from it",
          b7.control_socket() == "" and b7.get_server() is None,
          b7.control_socket())

    b7._parent_argv = lambda: f"codex app-server --listen unix://{mine} -c x=1"
    check("a session discovers exactly the socket ITS OWN parent app-server "
          "was started with, read from that parent's argv",
          b7.control_socket() == mine, b7.control_socket())
    check("and that is the only one it can reach: a second live server whose "
          "thread ALSO matches this cwd is never consulted, so isolation is "
          "structural rather than promised",
          b7.get_server() is not None
          and b7.get_server().session_thread() == "t-1",
          str(b7.control_socket()))

    b7.drop_server()
    b7._parent_argv = lambda: "codex app-server --listen unix:///gone/s.sock"
    check("a socket named in the parent's argv but absent from disk yields "
          "nothing rather than an exception",
          b7.control_socket() == "" and b7.get_server() is None)

    # --- the well-known socket is reachable ONLY as our own parent's ---------
    # A SHORT fake home: the well-known socket path must fit in SUN_LEN
    # (~104 bytes), which a deep TMPDIR-based path does not.
    fakehome = Path(tempfile.mkdtemp(prefix="cxh-", dir="/private/tmp")).resolve()
    (fakehome / "app-server-control").mkdir(parents=True)
    os.environ["CODEX_HOME"] = str(fakehome)
    try:
        b6 = load(cwd=str(proj))
        os.environ.pop("A2A_CODEX_SOCK", None)   # load() sets ""; undo
        b6._SOCK_ENV = None
        ctl = fakehome / "app-server-control" / "app-server-control.sock"
        mock3 = MockAppServer(str(ctl), cwds={"t-1": str(proj)})
        mock3.start()
        b6._parent_argv = lambda: "codex app-server --listen unix:// -c x=1"
        check("bare `--listen unix://` resolves to the well-known control "
              "socket — reachable because it IS our parent's socket",
              b6.control_socket() == str(ctl)
              and b6.get_server() is not None
              and b6.get_server().session_thread() == "t-1",
              b6.control_socket())
        b6.drop_server()
        b6._parent_argv = lambda: "codex"
        check("with no --listen in the parent, that same existing well-known "
              "socket is NOT used — the convenient fallback that would break "
              "isolation is absent on purpose",
              b6.control_socket() == "" and b6.get_server() is None,
              b6.control_socket())
    finally:
        os.environ.pop("CODEX_HOME", None)
    # --- the orient call, in the two states an agent is actually stuck in ----
    b5.api = lambda meth, path, body=None, timeout=30: (
        {"agent": "x", "stations": ["acme"], "registered": False}
        if path == "/me" else {"channels": []})
    unreg = json.loads(b5._status({}))
    check("an UNREGISTERED agent is told so plainly and pointed at the one "
          "call that fixes it — this is the state where an agent has least to "
          "go on and is likeliest to invent something",
          unreg["registered"] is False
          and "propose_me" in (unreg["next_step"] or ""),
          str(unreg.get("next_step")))
    check("and nothing else pretends to be fine: push is off in that state",
          unreg["push"]["enabled"] is False, str(unreg["push"]))

    b5.api = lambda meth, path, body=None, timeout=30: (
        {"agent": "x", "stations": ["acme"], "registered": True}
        if path == "/me" else
        {"channels": [{"name": "ops", "members": [b5.KEY, "other"]},
                      {"name": "advisory", "members": ["other"]}]})
    reg = json.loads(b5._status({}))
    check("channels lists ONLY the rooms this agent is a member of — the "
          "fact whose absence had an agent answering into a room where its "
          "reply reached nobody",
          reg["channels"] == ["ops"], str(reg["channels"]))
    check("with push off, next_step is the launch line, not a description "
          "of it",
          b5.LAUNCH_LINE in (reg["next_step"] or ""), str(reg.get("next_step")))
    check("the status call never reads the inbox: /pending MARKS MESSAGES "
          "READ, so counting it there would consume the very messages the "
          "agent is asking about",
          'api("GET", "/pending' not in inspect.getsource(b5._status), "")

    check("that line is a single line of plain codex — no script, no wrapper, "
          "no path to anything we ship",
          "\n" not in b5.LAUNCH_LINE
          and "codex app-server --listen" in b5.LAUNCH_LINE
          and "codex --remote" in b5.LAUNCH_LINE
          and "mktemp" not in b5.LAUNCH_LINE
          and ".sh" not in b5.LAUNCH_LINE,
          b5.LAUNCH_LINE)
    check("the tool surface is the shared vocabulary plus the diagnostic",
          len(b5.TOOLS) == 21 and "post_to_channel" in b5.TOOL_BY_NAME,
          str(len(b5.TOOLS)))

    # --- a receipt, not a copy ----------------------------------------------
    # The broker echoes the whole post back. Rendered into the session that is
    # the same body twice — once in the call the agent wrote, once in the
    # result — which reads like two messages and bills like two.
    body = "x" * 4000
    raw = json.dumps({"channel": "ops", "post": {
        "id": "p-1", "channel": "ops", "sender": "me", "text": body,
        "audience": ["alice"], "addressed": ["alice"],
        "expires_at": 1787300095.73}})
    got = b5._confirm(raw)
    check("posting returns a receipt, not the message back — id, room, who "
          "owes an ack, deadline",
          json.loads(got) == {"id": "p-1", "channel": "ops",
                              "audience": ["alice"], "addressed": ["alice"],
                              "expires": "2026-08-21T08:14:55.730Z"}, got)
    check("and the body is NOT echoed: an agent does not need read back what "
          "it just wrote, and a long post would cost its own length twice",
          body not in got and len(got) < 300, f"{len(got)} chars")
    check("a response it cannot parse is passed through untouched, so an "
          "error from the broker still reaches the agent",
          b5._confirm("not json at all") == "not json at all")

    # --- it must RUN, not merely import -------------------------------------
    # A stale reference to a removed constant crashed main() on its first log
    # line while every function-level test above still passed. Nothing here
    # exercised the process, so nothing caught it.
    env = dict(os.environ, A2A_URL="http://127.0.0.1:1",
               A2A_TOKEN="a2a_st_test", A2A_CODEX_SOCK="")
    env.pop("A2A_AGENT", None)
    proc = subprocess.run(
        [sys.executable, str(CLIENT)],
        input='{"jsonrpc":"2.0","id":1,"method":"initialize","params":{}}\n'
              '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}\n',
        capture_output=True, text=True, timeout=60, env=env, cwd=str(proj))
    check("the client RUNS as a process and survives an MCP handshake — no "
          "traceback, no non-zero exit",
          proc.returncode == 0 and "Traceback" not in proc.stderr,
          (proc.stderr or "")[-300:])
    replies = [json.loads(l) for l in proc.stdout.splitlines() if l.strip()]
    init = next((r for r in replies if "serverInfo" in (r.get("result") or {})),
                None)
    listed = next((r for r in replies if "tools" in (r.get("result") or {})),
                 None)
    check("it answers initialize with the brief as `instructions`, so a "
          "session learns the rules without a tool call",
          init and "ADDRESSING IS AN ARGUMENT"
          in (init["result"].get("instructions") or ""), str(init)[:200])
    check("and lists the whole surface over the wire, not just in a table",
          listed and len(listed["result"]["tools"]) == 21,
          str(len((listed or {}).get("result", {}).get("tools", []))))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — the socket speaks, and nothing is acked before it lands")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
