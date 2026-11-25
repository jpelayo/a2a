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
  ownership        only a server the a2a launch line started — socket
                   `a2a-*.sock` — is reaped. The reaper once SIGTERMed the
                   shared control-socket server every fifteen seconds and
                   took every Codex session on it down.
  the background   Codex's own managed server (a plain `codex`, 0.157+) is
  server           pushed into — this session's thread only, named by Codex
                   in each tool call, never a subagent's — only while a
                   Codex TUI is open in the project, and never stopped
                   (daemon_cases).

Pure python3 stdlib.
"""
import base64
import hashlib
import importlib.util
import inspect
import json
import os
import signal
import socket
import struct
import subprocess
import sys
import tempfile
import threading
import time
import types
from pathlib import Path

# The clients live in plugin/; the suite lives here, beside it.
CLIENT = Path(__file__).resolve().parent.parent / "deploy" / "plugin" / "codex" / "a2a-codex.py"
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

    def __init__(self, path, threads=("t-1",), cwds=None, extra=None):
        super().__init__(daemon=True)
        self.path = path
        self.threads = list(threads)
        # thread id -> the cwd thread/read reports for it
        self.cwds = dict(cwds or {t: os.getcwd() for t in self.threads})
        # thread id -> more fields of its thread/read record (source, …)
        self.extra = dict(extra or {})
        self.init_params = []    # every initialize's params
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
            self.init_params.append(msg.get("params"))
            return {"id": rid, "result": {"userAgent": "mock"}}
        if method == "thread/loaded/list":
            return {"id": rid, "result": {"data": self.threads,
                                          "nextCursor": None}}
        if method == "thread/read":
            tid = msg["params"]["threadId"]
            return {"id": rid, "result": {"thread": {
                "id": tid, "cwd": self.cwds.get(tid, ""),
                **self.extra.get(tid, {})}}}
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


DAEMON_ARGV = ("/x/codex app-server --listen unix:// "
               "--analytics-default-enabled --managed-daemon")


class Stop(Exception):
    pass


def daemon_cases(tmp, proj, msg, codex_home):
    """Codex's own background server (Codex 0.157+: a plain `codex` runs on
    it). Push into THIS session's thread only, never stop the server, and
    hold the stream only while a Codex TUI is open in the project."""
    home = Path(tempfile.mkdtemp(prefix="cxd-", dir="/private/tmp")).resolve()
    (home / "app-server-control").mkdir(parents=True)
    os.environ["CODEX_HOME"] = str(home)
    ctl = home / "app-server-control" / "app-server-control.sock"
    try:
        md = MockAppServer(str(ctl), threads=("t-me", "t-far"),
                           cwds={"t-me": str(proj), "t-far": "/other/proj"})
        md.start()

        def daemon_client(argv=DAEMON_ARGV):
            d = load(cwd=str(proj))
            os.environ.pop("A2A_CODEX_SOCK", None)
            d._SOCK_ENV = None
            d._parent_argv = lambda: argv
            d.send = lambda obj: None
            return d

        d = daemon_client()
        check("daemon: Codex's managed background server is recognised as "
              "the parent — its argv, its control socket",
              d.daemon_parent() and d.host_kind() == "codex-daemon"
              and d.control_socket() == str(ctl), d.control_socket())
        bare = daemon_client("codex app-server --listen unix://")
        check("daemon: a server somebody started by hand on the control "
              "socket, without --managed-daemon, is still not pushed into",
              bare.host_kind() == "" and bare.control_socket() == "",
              bare.control_socket())
        srv = d.get_server()
        check("daemon: it picks the one thread in THIS project, among the "
              "server's other sessions",
              srv.session_thread() == "t-me", str(srv.thread_id))
        check("daemon: and delivers into it as today, with turn/start",
              d.deliver(msg) and bool(md.turns) and "hi" in md.turns[-1],
              str(md.turns[-1:])[:200])
        name = ((md.init_params[-1] or {}).get("clientInfo") or {}).get("name")
        check("daemon: it introduces itself with a name the app-server treats "
              "as NOT originating — any other rewrites the server-wide "
              "originator and user agent of every session on it",
              name == "codex_app_server_daemon", str(name))
        used = set(md.methods)
        check("daemon: it only reads threads and starts turns — never "
              "resume, subscribe, the queue, or anything that changes the "
              "server",
              used <= {"initialize", "initialized", "thread/loaded/list",
                       "thread/read", "turn/start"}, str(sorted(used)))
        d.drop_server()             # the mock serves one connection at a time

        # --- presence -------------------------------------------------------
        d._present.clear()
        check("daemon: with no Codex TUI open in the project the stream is "
              "not claimed — the background server outlives the terminal",
              not d._injectable())
        d._present.set()

        ps = ("  101 /usr/local/bin/codex\n"
              "  102 codex app-server --listen unix:// --managed-daemon\n"
              "  103 node /opt/homebrew/bin/codex resume --last\n"
              "  104 codex exec fix the exec bug\n"
              "  105 codex -m gpt-x \"why does exec fail\"\n"
              "  106 /usr/bin/python3 a2a-codex.py\n"
              "  107 codex mcp list\n")
        check("presence: interactive Codex sessions are told from the "
              "background server, exec, mcp and other processes — by the "
              "first non-flag word only, since a prompt may say anything",
              d._codex_tuis(ps) == [101, 103, 105], str(d._codex_tuis(ps)))

        class FakeSub:
            def __init__(self, cwds):
                self.cwds = cwds

            def run(self, argv, **kw):
                if argv[0] == "ps":
                    out = ps
                else:
                    pid = int(argv[argv.index("-p") + 1])
                    out = (f"p{pid}\nfcwd\nn{self.cwds[pid]}\n"
                           if pid in self.cwds else "")
                return types.SimpleNamespace(stdout=out, returncode=0)

        real_sub = d.subprocess
        d.subprocess = FakeSub({101: "/elsewhere", 103: str(proj),
                                105: "/elsewhere"})
        here = d.tui_here()
        d.subprocess = FakeSub({101: "/elsewhere", 103: "/elsewhere",
                                105: "/elsewhere"})
        away = d.tui_here()
        d.subprocess = real_sub
        check("presence: a TUI counts only when its cwd is this project",
              here is True and away is False, f"{here} {away}")

        seq = iter([True, False, False, False, True])
        seen = []
        pw = daemon_client()
        pw._present.clear()
        pw.tui_here = lambda: next(seq)

        def sleep(s):
            seen.append(pw._present.is_set())
            if len(seen) >= 5:
                raise Stop()
        pw.time = types.SimpleNamespace(sleep=sleep, time=time.time,
                                        strftime=time.strftime,
                                        gmtime=time.gmtime)
        try:
            pw.presence_watch()
        except Stop:
            pass
        check("presence: seen → receiving; gone for three checks in a row → "
              "released; back → receiving again",
              seen == [True, True, True, False, True], str(seen))

        # --- the stream is released mid-way, nothing consumed --------------
        rel = daemon_client()
        rel.api = lambda *a, **k: {}
        rel.resolve_name = lambda: None
        rel.check_version = lambda: None
        rel.catch_up = lambda: None
        rel._injectable = lambda: True
        emitted = []
        rel.emit = lambda line: emitted.append(line)
        lines = [b'{"id": "m-a"}\n', b'{"id": "m-b"}\n']

        class Resp:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

            def __iter__(self):
                yield lines[0]
                rel._present.clear()          # the TUI just closed
                yield lines[1]
                rel._enabled.clear()          # end the test's pump

        real_open = rel.urllib.request.urlopen
        rel.urllib.request.urlopen = lambda req, timeout=None: Resp()
        rel.time = types.SimpleNamespace(sleep=lambda s: rel._enabled.clear(),
                                         time=time.time,
                                         strftime=time.strftime,
                                         gmtime=time.gmtime)
        rel._enabled.set()
        try:
            rel.pump()
        finally:
            rel.urllib.request.urlopen = real_open
        check("presence: a line read after the TUI closed is left on the "
              "broker, like one read after the project was turned off",
              emitted == ['{"id": "m-a"}'], str(emitted))

        # --- a subagent's own a2a server, and Codex naming the thread -------
        ms = MockAppServer(str(home / "app-server-control" / "s.sock"),
                           threads=("t-root", "t-sub"),
                           cwds={"t-root": str(proj), "t-sub": str(proj)},
                           extra={"t-root": {"threadSource": "user",
                                             "source": "vscode"},
                                  "t-sub": {"threadSource": "subagent",
                                            "parentThreadId": "t-root",
                                            "source": {"subAgent": {}}}})
        ms.start()

        def on_ms():
            c = load(sock=ms.path, cwd=str(proj))
            c.send = lambda obj: None
            return c

        def call_from(c, tid):
            c.handle({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                      "params": {"name": "no_such_tool", "arguments": {},
                                 "_meta": {"x-codex-turn-metadata": {
                                     "thread_id": tid, "turn_id": "u"}}}})

        g = on_ms()
        try:
            g.get_server().session_thread()
            guessed = "picked"
        except g.WSError as e:
            guessed = str(e)
        check("thread: before Codex names it, a session and its subagent in "
              "one directory are two threads — refused, as today",
              "2 live threads" in guessed, guessed)
        call_from(g, "t-root")
        check("thread: once a tool call names the thread, that is the "
              "thread, exactly — even with a second one in the directory",
              g.get_server().session_thread() == "t-root" and g._injectable(),
              str(g.get_server().thread_id))
        g.drop_server()             # the mock serves one connection at a time
        sub = on_ms()
        call_from(sub, "t-sub")
        try:
            sub.get_server().session_thread()
            said = "picked"
        except sub.WSError as e:
            said = str(e)
        check("thread: a subagent's own a2a server never pushes — its "
              "parent session has its own, and two would fight over the "
              "stream",
              "subagent" in said and sub._pinned["subagent"]
              and not sub._injectable(), said)
        sub.drop_server()
        first = on_ms()
        first._pinned["thread"] = None
        srvf = first.get_server()
        srvf.thread_id = "t-guess"
        call_from(first, "t-root")
        check("thread: a name from Codex replaces an earlier directory guess",
              srvf.session_thread() == "t-root", str(srvf.thread_id))

        # --- the handshake ----------------------------------------------------
        hs = daemon_client()
        got = []
        hs.send = lambda obj: got.append(obj)
        for asked in ("2025-06-18", "2025-11-25", "2026-07-28", None):
            params = {"clientInfo": {"name": "codex-mcp-client",
                                     "version": "0.172.0"}}
            if asked:
                params["protocolVersion"] = asked
            hs.handle({"jsonrpc": "2.0", "id": 9, "method": "initialize",
                       "params": params})
        versions = [o["result"]["protocolVersion"] for o in got]
        check("handshake: it answers only a protocol version it speaks — "
              "claiming 2026-07-28 could let Codex share one a2a process "
              "between several threads",
              versions == ["2025-06-18", "2025-11-25", "2025-06-18",
                           "2025-06-18"], str(versions))
        check("handshake: it remembers which Codex started it",
              hs._codex["version"] == "0.172.0", hs._codex["version"])

        # --- lazy start (Codex 0.172) -----------------------------------------
        cfg = home / "config.toml"
        for text, ver, want in (
                ("", "0.172.0", True), ("", "0.171.3", False), ("", "", None),
                ("[features]\nmcp_lazy_stdio_startup = false\n", "0.172.0",
                 False),
                ('[mcp_servers.a2a]\ncommand = "python3"\n'
                 "[features]\nmcp_lazy_stdio_startup = true\n", "0.171.0",
                 True)):
            cfg.write_text(text)
            hs._codex["version"] = ver
            check(f"lazy start: config {text.strip()[-28:]!r} on Codex "
                  f"{ver or '?'} → {want}", hs.lazy_start() is want,
                  str(hs.lazy_start()))
        cfg.unlink()

        # --- the doctor ---------------------------------------------------------
        doc = daemon_client()
        doc.api = lambda meth, path, body=None, timeout=30: (
            {"agent": doc.KEY, "stations": ["acme"], "registered": True}
            if path == "/me" else {"channels": []} if path == "/channels"
            else {})
        doc._codex["version"] = "0.172.0"
        doc._state["connected"] = True          # a healthy stream
        doc._state["last_line"] = time.time()
        rep = json.loads(doc._status({}))
        check("doctor: it says which server it pushes through, and that "
              "Codex starts a2a lazily in new chats — with the fix",
              rep["push"]["host"] == "codex-daemon"
              and rep["push"]["lazy_start"] is True
              and "mcp_lazy_stdio_startup" in (rep["next_step"] or ""),
              json.dumps(rep["push"]) + str(rep["next_step"]))
        doc._present.clear()
        rep = json.loads(doc._status({}))
        ph = next(c for c in rep["checks"] if c["check"] == "push_host")
        check("doctor: with no Codex TUI open in the project, push_host says "
              "so, for the user",
              ph["ok"] is False and ph["who"] == "user"
              and "no Codex TUI is open" in (ph["do"] or ""), str(ph))
    finally:
        os.environ["CODEX_HOME"] = str(codex_home)


def main() -> int:
    # resolve(): macOS TMPDIR lives behind a /var -> /private/var symlink, and
    # the client compares os.getcwd() (already resolved) against thread cwds.
    tmp = Path(tempfile.mkdtemp(prefix="a2acx-")).resolve()
    # The client logs and keeps its identity store under CODEX_HOME. Pointed
    # here, so running this suite writes nothing into the real ~/.codex.
    codex_home = tmp / "codex-home"
    os.environ["CODEX_HOME"] = str(codex_home)

    # --- identity ladder ----------------------------------------------------
    proj = tmp / "my-project"
    proj.mkdir()
    # Opted in, so everything below exercises a project that is on. What a
    # project that is off does is pinned in test_project_gate.py.
    (proj / ".a2a.json").write_text('{"enabled_codex": true}')
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
    mine, theirs = str(iso / "a2a-1.sock"), str(iso / "a2a-2.sock")
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
    b7._parent_argv = lambda: "codex app-server --listen unix:///gone/a2a-9.sock"
    check("a socket named in the parent's argv but absent from disk yields "
          "nothing rather than an exception",
          b7.control_socket() == "" and b7.get_server() is None)

    # --- a server the launch line did not start is never ours ---------------
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
        for argv in ("codex app-server --listen unix:// -c x=1",
                     f"codex app-server --listen unix://{ctl}"):
            b6._parent_argv = lambda argv=argv: argv
            check(f"the shared control socket is NOT pushed into, even as "
                  f"our own parent's ({argv.split('unix://')[1][:12] or 'bare'}"
                  f"…) — something else started that server and put other "
                  f"sessions on it",
                  b6.parent_socket() == str(ctl)
                  and b6.control_socket() == "" and b6.get_server() is None,
                  f"{b6.parent_socket()} -> {b6.control_socket()!r}")
        b6._parent_argv = lambda: "codex"
        check("with no --listen in the parent, that same existing well-known "
              "socket is not used either",
              b6.control_socket() == "" and b6.get_server() is None,
              b6.control_socket())
    finally:
        os.environ["CODEX_HOME"] = str(codex_home)

    # --- the reaper: only what the launch line started ---------------------
    # Every call below stubs the three things that make reaper() dangerous to
    # call in a test — sleep, the TUI probe, and os.kill/_exit — and records
    # what it WOULD have done. A regression here must fail the check, not
    # signal the shell that ran the suite.
    class Os:
        def __init__(self):
            self.killed = []

        def __getattr__(self, name):
            return getattr(os, name)

        def kill(self, pid, sig):
            self.killed.append((pid, sig))

        def _exit(self, code):
            raise SystemExit(code)

    def reap(parent_argv, sock_env=None):
        r = load(cwd=str(proj))
        if sock_env is None:
            os.environ.pop("A2A_CODEX_SOCK", None)
            r._SOCK_ENV = None
        else:
            r._SOCK_ENV = sock_env
        r._parent_argv = lambda: parent_argv
        r._tui_attached = lambda sock: False      # nobody attached, ever
        r.time = types.SimpleNamespace(sleep=lambda s: None, time=time.time,
                                       strftime=time.strftime,
                                       gmtime=time.gmtime)
        r.os = Os()
        try:
            r.reaper()
            return r.os.killed, False
        except SystemExit:
            return r.os.killed, True

    killed, exited = reap(f"codex app-server --listen unix://{ctl}")
    check("the reaper NEVER stops the shared control-socket server, even with "
          "no TUI it can see — the bug that took down every Codex session on "
          "it fifteen seconds after each opened",
          killed == [] and not exited, f"{killed} exited={exited}")
    killed, exited = reap("codex app-server --listen unix:// -c x=1")
    check("nor when that server was started with a bare `--listen unix://`",
          killed == [] and not exited, f"{killed} exited={exited}")
    other = iso / "something-else.sock"
    other.touch()
    killed, exited = reap(f"codex app-server --listen unix://{other}")
    check("nor any other socket the launch line did not name",
          killed == [] and not exited, f"{killed} exited={exited}")
    ours_sock = iso / "a2a-4242.sock"
    ours_sock.touch()
    killed, exited = reap("codex", sock_env=str(ours_sock))
    check("an A2A_CODEX_SOCK override never makes the parent a target: it "
          "names a socket, not a process",
          killed == [] and not exited, f"{killed} exited={exited}")
    killed, exited = reap(f"codex app-server --listen unix://{ours_sock}")
    check("and a server the launch line DID start is still reaped once its "
          "TUI is gone — the parent, with SIGTERM, then this client exits",
          killed == [(os.getppid(), signal.SIGTERM)] and exited,
          f"{killed} exited={exited}")
    killed, exited = reap("/x/codex app-server --listen unix:// "
                          "--analytics-default-enabled --managed-daemon")
    check("the reaper NEVER stops Codex's own background server either — "
          "every Codex session the person has lives there",
          killed == [] and not exited, f"{killed} exited={exited}")

    daemon_cases(tmp, proj, msg, codex_home)

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

    # --- the doctor ------------------------------------------------------------
    sent = []

    def rec(meth, path, body=None, timeout=30):
        sent.append((meth, path, body))
        if path == "/me":
            return {"agent": "x", "stations": ["acme"], "registered": True}
        return {"channels": []} if path == "/channels" else {"ok": True}

    b5.api = rec
    reg = json.loads(b5._status({}))
    ids = [c["check"] for c in reg.get("checks") or []]
    check("doctor: every check, in the shared order",
          ids == ["install", "identity", "settings", "switch", "broker",
                  "client", "registered", "push_host", "stream", "version"],
          str(ids))
    check("doctor: each entry is exactly {check, ok, who, do, detail}, and "
          "the first failing one is next_step",
          all(set(c) == {"check", "ok", "who", "do", "detail"}
              for c in reg["checks"])
          and reg["next_step"] == next(c["do"] for c in reg["checks"]
                                       if c["ok"] is False), str(reg)[:300])
    report = [b for m, p, b in sent if p == "/me/state"]
    check("doctor: it reports its finding with only the allowed keys",
          report and set(report[-1]) == {"harness", "version", "switch",
                                         "route", "receives", "settings",
                                         "failing", "fixed"}
          and report[-1]["harness"] == "codex"
          and "push_host" in report[-1]["failing"], str(report[-1:]))
    switch_before = b5._enabled.is_set()
    b5._state.update(connected=True, last_line=time.time() - 3600)
    with b5._ack_lock:
        b5._to_ack.add("m-pending")
    sent.clear()
    fixed = json.loads(b5._status({"fix": True}))["fixed"]
    check("doctor: fix=true with no thread looks for the socket again, asks "
          "a silent stream to reconnect, and retries pending acks",
          {f["check"] for f in fixed} == {"push_host", "stream"}
          and b5._reconnect.is_set()
          and any(p == "/ack" for m, p, b in sent), str(fixed))
    check("doctor: and never touches the project switch",
          b5._enabled.is_set() == switch_before
          and (proj / ".a2a.json").read_text() == '{"enabled_codex": true}',
          "")
    b5._reconnect.clear()
    b5._state.update(connected=False, last_line=0.0)

    # Healthy: socket, thread, live stream — fix=true changes nothing.
    b3.api = rec
    b3._state.update(connected=True, last_line=time.time())
    srv_before = b3.get_server()
    st3 = json.loads(b3._status({"fix": True}))
    check("doctor: on a healthy session fix=true repairs nothing — no "
          "reconnect, the app-server connection kept",
          st3["fixed"] == [] and not b3._reconnect.is_set()
          and b3.get_server() is srv_before and st3["next_step"] is None,
          f"{st3['fixed']} {st3['next_step']}")
    b3._state.update(connected=False, last_line=0.0)

    check("that line is a single line of plain codex — no script, no wrapper, "
          "no path to anything we ship",
          "\n" not in b5.LAUNCH_LINE
          and "codex app-server --listen" in b5.LAUNCH_LINE
          and "codex --remote" in b5.LAUNCH_LINE
          and "mktemp" not in b5.LAUNCH_LINE
          and ".sh" not in b5.LAUNCH_LINE,
          b5.LAUNCH_LINE)
    check("the tool surface is the shared vocabulary plus the diagnostic "
          "and the project switch",
          len(b5.TOOLS) == 22 and "post_to_channel" in b5.TOOL_BY_NAME
          and "enable_a2a_here" in b5.TOOL_BY_NAME,
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
               A2A_TOKEN="a2a_st_test", A2A_CODEX_SOCK="",
               CODEX_HOME=str(codex_home))
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
          listed and len(listed["result"]["tools"]) == 22,
          str(len((listed or {}).get("result", {}).get("tools", []))))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — the socket speaks, and nothing is acked before it lands")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
