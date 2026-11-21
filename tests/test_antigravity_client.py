#!/usr/bin/env python3
"""The Antigravity (agy) client, without agy and without a database.

    python3 tests/test_antigravity_client.py

Runs the REAL MCP server as agy does (cwd = the plugin folder, PWD = the
project, PLUGIN_DATA and TMPDIR of its own) against a fake broker, the REAL
waiter script it writes, and the REAL Stop hook:

  the queue    what the stream delivers is kept UNACKED until a2a_inbox hands
               it to the model; the `mail` file counts it;
  the waiter   `sh <mailbox>/wait.sh` beats while nothing is waiting, exits on
               mail (which is what wakes agy), and exits when the receiver is
               gone;
  a2a_inbox    returns the envelopes and the brief, acks exactly those ids,
               clears `mail`, follows the ROOT conversation from _meta, and
               ends with the waiter line;
  the hook     silent where a2a is not receiving, for another conversation,
               and after two nudges that changed nothing; otherwise sends the
               agent back for mail or a missing waiter;
  one receiver a second agy in the project, and the Antigravity app, only send;
  the switch   off: no stream, no mailbox.

Pure python3 stdlib.
"""
import hashlib
import importlib.util
import json
import os
import queue as _queue
import shutil
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

PLUGIN = Path(__file__).resolve().parent.parent / "deploy" / "plugin" / "antigravity"
CLIENT = PLUGIN / "a2a-antigravity.py"
HOOK = PLUGIN / "a2a-stop.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def wait_for(pred, timeout=10.0, step=0.05):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if pred():
                return True
        except Exception:
            pass
        time.sleep(step)
    return False


def msg(mid, text="hello", channel="dm", sender="alice"):
    return {"id": mid, "channel": channel, "sender": sender, "text": text,
            "audience": ["agy-proj"], "addressed": [], "expires_at": None}


class Broker:
    """Just enough broker: /me, /healthz, /stream (lines pushed by the test,
    keepalives in between), /pending/peek, /ack, /channels, /me/state."""

    def __init__(self):
        self.lines: "_queue.Queue[str]" = _queue.Queue()
        self.peek = []
        self.acked = []
        self.ack_agents = []
        self.transcript = []
        self.streams = 0
        self.requests = []
        outer = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"

            def log_message(self, *a):
                pass

            def _json(self, obj, code=200):
                body = json.dumps(obj).encode()
                self.send_response(code)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_GET(self):
                outer.requests.append(("GET", self.path,
                                       self.headers.get("X-A2A-Agent")))
                if self.path.startswith("/stream"):
                    outer.streams += 1
                    self.send_response(200)
                    self.send_header("Content-Type", "application/x-ndjson")
                    self.end_headers()
                    try:
                        while True:
                            try:
                                line = outer.lines.get(timeout=0.3)
                            except _queue.Empty:
                                line = ""
                            self.wfile.write((line + "\n").encode())
                            self.wfile.flush()
                    except (BrokenPipeError, ConnectionResetError, OSError):
                        return
                if self.path.startswith("/pending/peek"):
                    return self._json({"messages": outer.peek, "has_more": False})
                if self.path == "/me":
                    return self._json({"agent": self.headers.get("X-A2A-Agent"),
                                       "registered": True,
                                       "stations": ["default"]})
                if self.path == "/healthz":
                    return self._json({"ok": True, "clients": ""})
                if self.path == "/channels":
                    return self._json({"channels": []})
                if self.path.startswith("/channels/"):
                    return self._json({"transcript": outer.transcript})
                return self._json({"error": "nope"}, 404)

            def do_POST(self):
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}")
                outer.requests.append(("POST", self.path, body))
                if self.path == "/ack":
                    outer.acked += body.get("ids") or []
                    outer.ack_agents += [(i, self.headers.get("X-A2A-Agent"))
                                         for i in body.get("ids") or []]
                    return self._json({"acked": len(body.get("ids") or [])})
                return self._json({"ok": True})

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.srv.daemon_threads = True
        self.url = f"http://127.0.0.1:{self.srv.server_address[1]}"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def push(self, m):
        self.lines.put(json.dumps(m))

    def close(self):
        self.srv.shutdown()


def hook_mod():
    spec = importlib.util.spec_from_file_location("a2a_stop_t", HOOK)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class Client:
    """The real MCP server as a process, spoken to over stdio."""

    def __init__(self, broker, project, data, tmpdir, home):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("A2A_", "PLUGIN_"))}
        env.update(A2A_URL=broker.url, A2A_TOKEN="a2a_st_test",
                   PLUGIN_DATA=str(data), PLUGIN_ROOT=str(PLUGIN),
                   PWD=str(project), TMPDIR=str(tmpdir), HOME=str(home),
                   PYTHONDONTWRITEBYTECODE="1")
        self.proc = subprocess.Popen(
            [sys.executable, "-B", "-I", str(CLIENT)], cwd=str(PLUGIN), env=env,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, text=True, bufsize=1)
        self.n = 0

    def rpc(self, method, params=None):
        self.n += 1
        self.proc.stdin.write(json.dumps({"jsonrpc": "2.0", "id": self.n,
                                          "method": method,
                                          "params": params or {}}) + "\n")
        self.proc.stdin.flush()
        while True:
            line = self.proc.stdout.readline()
            if not line:
                raise RuntimeError("client exited: "
                                   + (self.proc.stderr.read() or "")[-400:])
            out = json.loads(line)
            if out.get("id") == self.n:
                return out

    def call(self, tool, args=None, conv=None, root=None):
        params = {"name": tool, "arguments": args or {}}
        if conv:
            params["_meta"] = {"antigravity.google/conversation_id": conv,
                               "antigravity.google/root_conversation_id":
                                   root or conv,
                               "progressToken": "x"}
        res = self.rpc("tools/call", params)["result"]
        return res["content"][0]["text"]

    def close(self):
        try:
            self.proc.stdin.close()
            self.proc.wait(timeout=15)
        except Exception:
            self.proc.kill()
        return self.proc.returncode


def run_hook(event, env):
    r = subprocess.run([sys.executable, "-B", "-I", "./a2a-stop.py"],
                       cwd=str(PLUGIN), input=json.dumps(event),
                       capture_output=True, text=True, timeout=20,
                       env=dict(os.environ, **env))
    try:
        return r.returncode, json.loads(r.stdout or "{}")
    except ValueError:
        return r.returncode, {"_raw": r.stdout}


def main() -> int:
    root = Path(tempfile.mkdtemp(prefix="a2a-agy-"))
    broker = Broker()
    clients = []
    waiters = []
    try:
        home = root / "home"
        home.mkdir()
        tmpdir = root / "tmp"
        tmpdir.mkdir()
        data = root / "home" / ".gemini" / "antigravity-cli" / "plugin_data" / "a2a"
        proj = root / "agy-proj"
        proj.mkdir()
        (proj / ".a2a.json").write_text('{"enabled_antigravity": true}')
        hk = hook_mod()
        box = tmpdir / "a2a-agy" / hk.project_hash(str(proj))
        hook_env = {"TMPDIR": str(tmpdir), "PLUGIN_DATA": str(data)}

        broker.peek = [msg("m-old", "from before")]
        c = Client(broker, proj, data, tmpdir, home)
        clients.append(c)
        init = c.rpc("initialize", {"protocolVersion": "2025-06-18"})["result"]
        check("initialize carries the brief in an enabled project, with the "
              "waiter explained",
              "HOW MESSAGES REACH YOU HERE" in (init.get("instructions") or "")
              and "wait.sh" in init["instructions"], str(init)[:200])
        tools = [t["name"] for t in c.rpc("tools/list")["result"]["tools"]]
        check("a2a_inbox is listed with the shared vocabulary",
              "a2a_inbox" in tools and "post_to_channel" in tools
              and "a2a_channel_status" in tools, str(tools))

        check("it becomes the project's receiver: `live` names its pid",
              wait_for(lambda: json.loads((box / "live").read_text())["pid"]
                       == c.proc.pid), str(list(box.glob("*")) if box.exists()
                                          else "no mailbox"))
        check("and the mailbox is where the hook will look for it — the hook's "
              "hash of the project, the server's TMPDIR",
              (box / "wait.sh").is_file())
        check("the peek's old message is queued, unacked: `mail` counts it",
              wait_for(lambda: (box / "mail").read_text().startswith("1 new")),
              (box / "mail").read_text() if (box / "mail").exists() else "-")
        check("nothing was acked before the model saw it", broker.acked == [],
              str(broker.acked))

        # --- the waiter, beating, then woken by mail ---------------------------
        # Clear mail first so the waiter has nothing to exit on: hand over the
        # peeked message through a2a_inbox.
        out = c.call("a2a_inbox", conv="c-root")
        check("a2a_inbox hands over the queued message as an envelope, brief "
              "first, and ends with the exact waiter command",
              'id="m-old"' in out and out.startswith('You are a2a agent')
              and f"sh {box / 'wait.sh'}" in out, out[-300:])
        check("and acks exactly what it handed over",
              wait_for(lambda: broker.acked == ["m-old"]), str(broker.acked))
        check("and clears `mail`", (box / "mail").read_text() == "")
        check("and follows the conversation it was called from",
              json.loads((box / "live").read_text())["route"] == "c-root")

        w = subprocess.Popen(["sh", str(box / "wait.sh")],
                             stdout=subprocess.PIPE, text=True)
        waiters.append(w)
        check("the waiter beats while nothing is waiting",
              wait_for(lambda: hk._age(str(box / "beat")) is not None
                       and hk._age(str(box / "beat")) < 5, 8))
        check("and keeps running", w.poll() is None)
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("the hook lets a turn end while the waiter runs and nothing "
              "waits", code == 0 and out == {}, str(out))
        code, out = run_hook({"conversationId": "c-root", "fullyIdle": True,
                              "workspacePaths": [str(proj)]}, hook_env)
        check("but when agy says no background command runs (fullyIdle), "
              "no waiter can, whatever the beat says",
              out.get("decision") == "continue"
              and "waiter is not running" in out.get("reason", ""), str(out))
        check("and the hook's reason names no command: a2a_inbox says what "
              "to start, or that this agy does not receive",
              "wait.sh" not in out.get("reason", ""), str(out))
        link = root / "link-proj"
        link.symlink_to(proj)
        code, out = run_hook({"conversationId": "c-root", "fullyIdle": True,
                              "workspacePaths": [str(link)]}, hook_env)
        check("the hook finds the mailbox through a symlinked project path",
              out.get("decision") == "continue", str(out))

        broker.push(msg("m-1", "are you there?"))
        try:
            got = w.communicate(timeout=15)[0]
        except subprocess.TimeoutExpired:
            got = "(still running)"
        check("a message arriving makes the waiter EXIT — that is what wakes "
              "agy — saying to call a2a_inbox",
              w.returncode == 0 and "Call a2a_inbox" in got, got)
        check("and the message is still unacked", "m-1" not in broker.acked)

        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("the hook sends the agent back while mail waits",
              out.get("decision") == "continue"
              and "a2a_inbox" in out.get("reason", ""), str(out))
        code, out = run_hook({"conversationId": "c-other",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("but never another conversation (a subagent, the app)",
              out == {}, str(out))
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(root / "elsewhere")]},
                             hook_env)
        check("and stays silent in a project with no receiver", out == {},
              str(out))
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]},
                             {"TMPDIR": str(tmpdir),
                              "PLUGIN_DATA": str(home / ".gemini" / "antigravity"
                                                 / "plugin_data" / "a2a")})
        check("and in the Antigravity app", out == {}, str(out))
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        code, out3 = run_hook({"conversationId": "c-root",
                               "workspacePaths": [str(proj)]}, hook_env)
        check("it gives up after two nudges that changed nothing, so no model "
              "is held in a loop",
              out.get("decision") == "continue" and out3 == {}
              and json.loads((box / "guard.json").read_text())["gave_up"],
              f"{out} {out3}")
        code, out = run_hook({"conversationId": "c-new", "fullyIdle": True,
                              "workspacePaths": [str(proj)]}, hook_env)
        check("another conversation IS nudged when no waiter runs and agy is "
              "fully idle (a /clear or a new chat in the same agy)",
              out.get("decision") == "continue", str(out))
        for bad in ("", "not json", "[1,2]"):
            r = subprocess.run([sys.executable, "-B", "-I", "./a2a-stop.py"],
                               cwd=str(PLUGIN), input=bad, capture_output=True,
                               text=True, env=dict(os.environ, **hook_env))
            check(f"malformed hook input {bad!r} allows the stop",
                  r.returncode == 0 and json.loads(r.stdout) == {}, r.stdout)

        out = c.call("a2a_inbox", conv="c-sub", root="c-root")
        check("a2a_inbox from a subagent hands over the new message and keeps "
              "the ROOT conversation as the route",
              'id="m-1"' in out and "You are a2a agent" not in out
              and json.loads((box / "live").read_text())["route"] == "c-root",
              out[:200])
        check("and that ack went out", wait_for(lambda: "m-1" in broker.acked))
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("after progress the hook nudges again: no waiter is running now",
              out.get("decision") == "continue"
              and "waiter is not running" in out.get("reason", ""), str(out))
        # A waiter that never starts (the command denied, or run in the
        # foreground): a2a_inbox calls alone must not reset the guard.
        c.call("a2a_inbox", conv="c-root")
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        c.call("a2a_inbox", conv="c-root")
        code, out3 = run_hook({"conversationId": "c-root",
                               "workspacePaths": [str(proj)]}, hook_env)
        check("an a2a_inbox call with no waiter started is not progress: it "
              "still gives up after two nudges",
              out.get("decision") == "continue" and out3 == {},
              f"{out} {out3}")

        broker.push(msg("m-1", "replayed"))
        broker.push(msg("m-2", "new"))
        check("a replay of a handed-over id is not queued twice",
              wait_for(lambda: (box / "mail").read_text().startswith("1 new")),
              (box / "mail").read_text())

        # --- the doctor ----------------------------------------------------------
        st = json.loads(c.call("a2a_channel_status", conv="c-root"))
        ids = [x["check"] for x in st["checks"]]
        check("doctor: every check, in the shared order",
              ids == ["install", "identity", "settings", "switch", "broker",
                      "client", "registered", "push_host", "stream",
                      "version"], str(ids))
        ph = next(x for x in st["checks"] if x["check"] == "push_host")
        check("doctor: no waiter running is the agent's to fix, with the "
              "command", ph["ok"] is False and ph["who"] == "you"
              and "wait.sh" in ph["do"], str(ph))
        check("doctor: push names the queue, route and waiter",
              st["push"]["queued"] == 1 and st["push"]["route"] == "c-root"
              and st["push"]["host"] == "cli", str(st["push"]))
        report = [b for m, p, b in broker.requests if p == "/me/state"]
        check("doctor: it reports harness antigravity with the allowed keys",
              report and report[-1]["harness"] == "antigravity"
              and set(report[-1]) == {"harness", "version", "switch", "route",
                                      "receives", "settings", "failing",
                                      "fixed"}, str(report[-1:]))
        w2 = subprocess.Popen(["sh", str(box / "wait.sh")],
                              stdout=subprocess.PIPE, text=True)
        waiters.append(w2)
        got = w2.communicate(timeout=10)[0]
        check("a waiter started while mail waits exits at once",
              "Call a2a_inbox" in got, got)
        c.call("a2a_inbox", conv="c-root")

        # --- what is read elsewhere leaves the queue ----------------------------
        broker.push(msg("m-3", "room post", channel="ops"))
        check("a channel post is queued",
              wait_for(lambda: (box / "mail").read_text().startswith("1 new")))
        broker.transcript = [msg("m-3", "room post", channel="ops")]
        c.call("read_channel", {"name": "ops"}, conv="c-root")
        check("read_channel takes what it read out of the queue",
              (box / "mail").read_text() == "", (box / "mail").read_text())
        out = c.call("a2a_inbox", conv="c-root")
        check("and a2a_inbox does not hand it over a second time",
              'id="m-3"' not in out and "No new messages" in out, out[-200:])
        broker.push(msg("m-4"))
        wait_for(lambda: (box / "mail").read_text().startswith("1 new"))
        c.call("ack_messages", {"ids": ["m-4"]}, conv="c-root")
        out = c.call("a2a_inbox", conv="c-root")
        check("ack_messages does the same", 'id="m-4"' not in out
              and (box / "mail").read_text() == "", out[-200:])

        # --- the reply is capped by size --------------------------------------
        big = "x" * 60000
        for i in range(3):
            broker.push(msg(f"m-big{i}", big))
        wait_for(lambda: (box / "mail").read_text().startswith("3 new"))
        out = c.call("a2a_inbox", conv="c-root")
        check("a2a_inbox caps its reply by size: two of three 60 KB "
              "messages, the third waits", out.count('id="m-big') == 2
              and "1 more waiting" in out, str(out.count('id="m-big')))
        check("and acks only the two it sent",
              wait_for(lambda: "m-big1" in broker.acked)
              and "m-big2" not in broker.acked, str(broker.acked[-3:]))
        out = c.call("a2a_inbox", conv="c-root")
        check("the next call hands over the third", 'id="m-big2"' in out)

        # --- an ack goes as the agent the message arrived for ------------------
        broker.push(msg("m-5"))
        wait_for(lambda: (box / "mail").read_text().startswith("1 new"))
        c.call("propose_me", {"name": "agy-new"}, conv="c-root")
        c.call("a2a_inbox", conv="c-root")
        check("a message queued before a new name is acked as the agent it "
              "arrived for", wait_for(lambda: ("m-5", "agy-proj")
                                      in broker.ack_agents),
              str(broker.ack_agents[-3:]))
        check("and the stream reconnects under the new name",
              wait_for(lambda: any(m == "GET" and p.startswith("/stream")
                                   and a == "agy-new"
                                   for m, p, a in broker.requests), 15),
              str([r for r in broker.requests if r[1].startswith("/stream")]))

        w3 = subprocess.Popen(["sh", str(box / "wait.sh")],
                              stdout=subprocess.PIPE, text=True)
        waiters.append(w3)
        wait_for(lambda: hk._age(str(box / "beat")) < 2, 8)
        before = (proj / ".a2a.json").read_text()
        st = json.loads(c.call("a2a_channel_status", {"fix": True},
                               conv="c-root"))
        check("doctor: a healthy receiver comes out of fix=true as it went in",
              st["fixed"] == [] and st["next_step"] is None
              and (proj / ".a2a.json").read_text() == before,
              f"{st['fixed']} {st['next_step']}")

        # --- one receiver per project ------------------------------------------
        c2 = Client(broker, proj, data, tmpdir, home)
        clients.append(c2)
        c2.rpc("initialize")
        time.sleep(1.0)
        out = json.loads(c2.call("a2a_inbox", conv="c-two"))
        check("a second agy in the project only sends: a2a_inbox says another "
              "receives", out["receiving"] is False
              and "another agy" in out["why"], str(out))
        check("and the mailbox still belongs to the first",
              json.loads((box / "live").read_text())["pid"] == c.proc.pid)
        c2.close()

        # --- the app only sends ---------------------------------------------------
        appdata = home / ".gemini" / "antigravity" / "plugin_data" / "a2a"
        streams = broker.streams
        c3 = Client(broker, proj, appdata, tmpdir, home)
        clients.append(c3)
        c3.rpc("initialize")
        time.sleep(1.0)
        out = json.loads(c3.call("a2a_inbox", conv="c-app"))
        check("the Antigravity app only sends: no stream, a2a_inbox says so",
              out["receiving"] is False and "app" in out["why"]
              and broker.streams == streams,
              f"{out} streams {streams}->{broker.streams}")
        c3.close()

        # --- off -----------------------------------------------------------------
        off = root / "off-proj"
        off.mkdir()
        offbox = tmpdir / "a2a-agy" / hk.project_hash(str(off))
        streams = broker.streams
        c4 = Client(broker, off, data, tmpdir, home)
        clients.append(c4)
        init = c4.rpc("initialize")["result"]
        time.sleep(1.0)
        check("off: no brief, no stream, no mailbox",
              "instructions" not in init and broker.streams == streams
              and not offbox.exists(), str(init)[:120])
        out = json.loads(c4.call("a2a_inbox"))
        check("off: a2a_inbox says a2a is off and starts nothing",
              out["receiving"] is False and "off" in out["why"], str(out))
        c4.close()

        # --- the receiver leaves ---------------------------------------------------
        code = c.close()
        check("the client exits cleanly when agy closes its stdin", code == 0,
              str(code))
        check("and takes `live` with it", not (box / "live").exists())
        try:
            got = w3.communicate(timeout=10)[0]
        except subprocess.TimeoutExpired:
            got = "(still running)"
        check("a waiter still running then says the project stopped receiving",
              "not receiving" in got, got)
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("and the hook goes silent", out == {}, str(out))
        check("nothing was written into the plugin folder",
              not any(p.name == "__pycache__" for p in PLUGIN.rglob("*")))
    finally:
        for cl in clients:
            if cl.proc.poll() is None:
                cl.proc.kill()
        for w in waiters:
            if w.poll() is None:
                w.kill()
        broker.close()
        shutil.rmtree(root, ignore_errors=True)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — queued unacked, woken by the waiter, nudged by the hook")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
