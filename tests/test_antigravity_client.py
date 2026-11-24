#!/usr/bin/env python3
"""The Antigravity (agy) client, without agy and without a database.

    python3 tests/test_antigravity_client.py

Runs the REAL MCP server as agy does (cwd = the plugin folder, PWD = the
project, PLUGIN_DATA of its own) against a fake broker, the REAL waiter
(the plugin's wait.sh) with a fake `agentapi` on PATH, and the REAL Stop hook:

  the queue    what the stream delivers is kept UNACKED until a2a_inbox hands
               it to the model; `mail` counts it and `bell` goes up;
  the waiter   finds its mailbox by the conversation agy gives it, keeps
               running, rings `agentapi send-message` once per new batch
               (never on a drain), and exits when it cannot reach agentapi
               (the sandbox), when a ring fails, when the receiver is gone,
               when the route moves, or when a newer waiter takes over. It
               never prints or stores agy's CSRF token;
  a2a_inbox    returns the envelopes and the brief, acks exactly those ids,
               clears `mail`, follows the ROOT conversation from _meta, and
               ends with the waiter command — or, while one runs, says so;
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
WAITER = PLUGIN / "wait.sh"
SECRET = "CSRF-SECRET-do-not-leak-4242"

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

    def __init__(self, broker, project, data, home):
        env = {k: v for k, v in os.environ.items()
               if not k.startswith(("A2A_", "PLUGIN_", "ANTIGRAVITY_"))}
        env.update(A2A_URL=broker.url, A2A_TOKEN="a2a_st_test",
                   PLUGIN_DATA=str(data), PLUGIN_ROOT=str(PLUGIN),
                   PWD=str(project), HOME=str(home),
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


class Agentapi:
    """A fake agy `agentapi` on PATH: records its arguments (never the
    token), fails like the sandbox while `fail` exists, and otherwise prints
    a reply carrying the token, so a waiter that let its output through would
    leak it."""

    def __init__(self, root):
        self.bin = root / "bin"
        self.bin.mkdir()
        self.calls = root / "agentapi-calls"
        self.fail = root / "agentapi-fail"
        self.slow = root / "agentapi-slow"
        exe = self.bin / "agentapi"
        exe.write_text(
            "#!/bin/sh\n"
            f'echo "$*" >> "{self.calls}"\n'
            f'if [ -f "{self.slow}" ]; then sleep 3; fi\n'
            f'if [ -f "{self.fail}" ]; then\n'
            '  echo "dial tcp [::1]:1: connect: operation not permitted"\n'
            "  exit 1\n"
            "fi\n"
            'echo "{\\"token\\": \\"$ANTIGRAVITY_CSRF_TOKEN\\"}"\n')
        exe.chmod(0o755)

    def rings(self):
        try:
            lines = self.calls.read_text().splitlines()
        except OSError:
            return []
        return [x for x in lines if x.startswith("send-message")]


def start_waiter(api, home, conv="c-root", drop=()):
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("ANTIGRAVITY_", "PLUGIN_"))}
    env.update(PATH=f"{api.bin}{os.pathsep}{os.environ.get('PATH', '')}",
               HOME=str(home), ANTIGRAVITY_CONVERSATION_ID=conv,
               ANTIGRAVITY_CSRF_TOKEN=SECRET,
               ANTIGRAVITY_LS_ADDRESS="[::1]:1",
               ANTIGRAVITY_APP_DATA_DIR=str(home / ".gemini"
                                            / "antigravity-cli"))
    for k in drop:
        env.pop(k, None)
    return subprocess.Popen(["sh", str(WAITER)], env=env,
                            stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                            text=True)


def finish(w, timeout=12):
    try:
        return w.communicate(timeout=timeout)[0]
    except subprocess.TimeoutExpired:
        return "(still running)"


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
    outputs = []
    try:
        home = root / "home"
        home.mkdir()
        data = root / "home" / ".gemini" / "antigravity-cli" / "plugin_data" / "a2a"
        proj = root / "agy-proj"
        proj.mkdir()
        (proj / ".a2a.json").write_text('{"enabled_antigravity": true}')
        hk = hook_mod()
        box = data / "boxes" / hk.project_hash(str(proj))
        hook_env = {"PLUGIN_DATA": str(data)}
        api = Agentapi(root)
        line = f"sh {WAITER}"

        broker.peek = [msg("m-old", "from before")]
        c = Client(broker, proj, data, home)
        clients.append(c)
        init = c.rpc("initialize", {"protocolVersion": "2025-06-18"})["result"]
        check("initialize carries the brief in an enabled project, with the "
              "waiter explained",
              "HOW MESSAGES REACH YOU HERE" in (init.get("instructions") or "")
              and "wait.sh" in init["instructions"]
              and "BypassSandbox: true" in init["instructions"],
              str(init)[:200])
        tools = [t["name"] for t in c.rpc("tools/list")["result"]["tools"]]
        check("a2a_inbox is listed with the shared vocabulary",
              "a2a_inbox" in tools and "post_to_channel" in tools
              and "a2a_channel_status" in tools, str(tools))

        check("it becomes the project's receiver: `live` names its pid",
              wait_for(lambda: json.loads((box / "live").read_text())["pid"]
                       == c.proc.pid), str(list(box.glob("*")) if box.exists()
                                          else "no mailbox"))
        check("and the mailbox is where the hook will look for it — the hook's "
              "hash of the project, under PLUGIN_DATA/boxes",
              (box / "bell").is_file() and (box / "route").is_file()
              and not (box / "wait.sh").exists())
        check("the peek's old message is queued, unacked: `mail` counts it",
              wait_for(lambda: (box / "mail").read_text().startswith("1 new")),
              (box / "mail").read_text() if (box / "mail").exists() else "-")
        check("nothing was acked before the model saw it", broker.acked == [],
              str(broker.acked))

        # --- a2a_inbox, then the waiter -----------------------------------------
        out = c.call("a2a_inbox", conv="c-root")
        check("a2a_inbox hands over the queued message as an envelope, brief "
              "first, and ends with the exact waiter command, outside the "
              "sandbox",
              'id="m-old"' in out and out.startswith('You are a2a agent')
              and line in out and "BypassSandbox: true" in out, out[-300:])
        check("and acks exactly what it handed over",
              wait_for(lambda: broker.acked == ["m-old"]), str(broker.acked))
        check("and clears `mail`", (box / "mail").read_text() == "")
        check("and follows the conversation it was called from, in `live` "
              "and in `route`",
              json.loads((box / "live").read_text())["route"] == "c-root"
              and (box / "route").read_text() == "c-root")

        # --- SessionStart: the first prompt of a new conversation ----------
        start_ev = {"hookEventName": "SessionStart", "conversationId": "c-new",
                    "workspacePaths": [str(proj)]}
        code, out = run_hook(start_ev, hook_env)
        steps = out.get("injectSteps") or [{}]
        text = ((steps[0].get("systemMessage") or {}).get("systemMessage")
                or "")
        check("SessionStart in an enabled project with no waiter asks for "
              "a2a_inbox in that same turn, as a SYSTEM message (a user "
              "message replaced the person's prompt)",
              code == 0 and "a2a_inbox" in text and "userMessage" not in
              json.dumps(out) and "wait.sh" not in text, str(out))
        app_env = dict(hook_env, PLUGIN_DATA=str(root / "antigravity" /
                                                 "plugin_data" / "a2a"))
        check("and says nothing in the Antigravity app",
              run_hook(start_ev, app_env)[1] == {})
        cfg = proj / ".a2a.json"
        saved = cfg.read_text()
        cfg.write_text(json.dumps(dict(json.loads(saved),
                                       enabled_antigravity=False)))
        code, out = run_hook(start_ev, hook_env)
        cfg.write_text(saved)
        check("nor where .a2a.json does not turn a2a on", out == {}, str(out))

        api.slow.write_text("")            # agentapi's check takes 3 s
        w = start_waiter(api, home)
        waiters.append(w)
        early = wait_for(lambda: (box / "beat").exists(), 1.5)
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("the waiter beats BEFORE its agentapi check, so the turn that "
              "started it ends with no nudge (a real run was sent back)",
              early and not (box / "waiter").exists() and out == {},
              f"early={early} out={out}")
        api.slow.unlink()
        check("the waiter finds its mailbox by its conversation and beats",
              wait_for(lambda: hk._age(str(box / "beat")) is not None
                       and hk._age(str(box / "beat")) < 5, 8))
        check("and keeps running, its pid in `waiter` once the check passes",
              wait_for(lambda: (box / "waiter").exists()
                       and (box / "waiter").read_text().strip() == str(w.pid),
                       8) and w.poll() is None)
        out = c.call("a2a_inbox", conv="c-root")
        check("a2a_inbox while it runs says so and names no command",
              "already running" in out and "wait.sh" not in out, out[-300:])

        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("the hook lets a turn end while the waiter runs and nothing "
              "waits", code == 0 and out == {}, str(out))
        check("and SessionStart asks for nothing while the waiter runs",
              run_hook(start_ev, hook_env)[1] == {})
        code, out = run_hook({"conversationId": "c-root", "fullyIdle": True,
                              "workspacePaths": [str(proj)]}, hook_env)
        saw = json.loads((box / "guard.json").read_text()).get("saw") or {}
        check("agy's fullyIdle does not override a running waiter (a real "
              "run said true while it ran, and was sent back twice)",
              out == {}, str(out))
        check("and the hook records what it saw for the doctor: agy's "
              "fullyIdle beside the beat and the waiter's pid",
              saw.get("fully_idle") is True and saw.get("waiter_alive") is True
              and isinstance(saw.get("beat_s"), float), str(saw))
        link = root / "link-proj"
        link.symlink_to(proj)
        (box / "guard.json").unlink()
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(link)]}, hook_env)
        check("the hook finds the mailbox through a symlinked project path",
              out == {} and (box / "guard.json").is_file(), str(out))

        # --- a message rings, and the waiter keeps running ----------------------
        broker.push(msg("m-1", "are you there?"))
        check("a message arriving rings the conversation with agentapi "
              "send-message, naming a2a_inbox",
              wait_for(lambda: len(api.rings()) == 1, 10)
              and api.rings()[0].startswith("send-message --title a2a c-root")
              and "1 new a2a message" in api.rings()[0]
              and "Call a2a_inbox" in api.rings()[0], str(api.rings()))
        time.sleep(4)
        check("once: the waiter does not ring the same batch again, and keeps "
              "running", len(api.rings()) == 1 and w.poll() is None,
              str(api.rings()))
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
                             {"PLUGIN_DATA": str(home / ".gemini" / "antigravity"
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
        time.sleep(4)
        check("a drained queue rings nothing", len(api.rings()) == 1,
              str(api.rings()))

        # --- a waiter killed outright --------------------------------------------
        w.kill()
        w.wait()
        out = c.call("a2a_inbox", conv="c-root")
        check("a waiter killed outright leaves a fresh beat, but its pid is "
              "dead: a2a_inbox names the command again",
              line in out and "already running" not in out, out[-300:])
        code, out = run_hook({"conversationId": "c-new", "fullyIdle": True,
                              "workspacePaths": [str(proj)]}, hook_env)
        check("another conversation IS nudged when no waiter runs and agy is "
              "fully idle (a /clear or a new chat in the same agy)",
              out.get("decision") == "continue", str(out))
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("the hook nudges when no waiter is running: the killed one's "
              "beat is still fresh, its pid is dead",
              (box / "beat").exists() and out.get("decision") == "continue"
              and "waiter is not running" in out.get("reason", ""), str(out))
        check("and the hook's reason names no command: a2a_inbox says what "
              "to start, or that this agy does not receive",
              "wait.sh" not in out.get("reason", ""), str(out))
        # A waiter that never starts (the command denied, or run in the
        # sandbox): a2a_inbox calls alone must not reset the guard.
        c.call("a2a_inbox", conv="c-root")
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        c.call("a2a_inbox", conv="c-root")
        code, out3 = run_hook({"conversationId": "c-root",
                               "workspacePaths": [str(proj)]}, hook_env)
        check("an a2a_inbox call with no waiter started is not progress: it "
              "still gives up after two nudges",
              out3 == {}, f"{out} {out3}")

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
              "command and the bypass", ph["ok"] is False and ph["who"] == "you"
              and "wait.sh" in ph["do"] and "BypassSandbox: true" in ph["do"],
              str(ph))
        check("doctor: push names the queue, route and waiter",
              st["push"]["queued"] == 1 and st["push"]["route"] == "c-root"
              and st["push"]["host"] == "cli"
              and st["push"]["waiter"]["running"] is False, str(st["push"]))
        report = [b for m, p, b in broker.requests if p == "/me/state"]
        check("doctor: it reports harness antigravity with the allowed keys",
              report and report[-1]["harness"] == "antigravity"
              and set(report[-1]) == {"harness", "version", "switch", "route",
                                      "receives", "settings", "failing",
                                      "fixed"}, str(report[-1:]))

        # --- mail already waiting, and a newer waiter ----------------------------
        w2 = start_waiter(api, home)
        waiters.append(w2)
        check("a waiter started while mail waits rings at once, and keeps "
              "running", wait_for(lambda: len(api.rings()) == 2, 10)
              and w2.poll() is None, str(api.rings()))
        w2b = start_waiter(api, home)
        waiters.append(w2b)
        got = finish(w2)
        outputs.append(got)
        check("a newer waiter takes over: the old one stops, saying there is "
              "nothing to do", "newer waiter took over" in got, got)
        out = c.call("a2a_inbox", conv="c-root")
        check("and a2a_inbox then says the waiter is running",
              'id="m-2"' in out and "already running" in out, out[-200:])

        # --- a ring that fails ------------------------------------------------
        api.fail.write_text("")
        broker.push(msg("m-ring", "ring fails"))
        got = finish(w2b, 15)
        outputs.append(got)
        check("a ring that fails makes the waiter EXIT, which wakes agy the "
              "old way, saying to call a2a_inbox",
              w2b.returncode == 0 and "Call a2a_inbox" in got, got)
        w5 = start_waiter(api, home)
        waiters.append(w5)
        got = finish(w5)
        outputs.append(got)
        check("a waiter that cannot reach agentapi (the sandbox) stops at "
              "once and says to start it with BypassSandbox: true",
              w5.returncode == 1 and "BypassSandbox: true" in got, got)
        check("and takes its early beat with it, so nothing reads it as a "
              "running waiter", not (box / "beat").exists())
        api.fail.unlink()
        c.call("a2a_inbox", conv="c-root")

        w6 = start_waiter(api, home, drop=("ANTIGRAVITY_CONVERSATION_ID",))
        got = finish(w6)
        check("outside agy (no conversation id) the waiter does nothing",
              "only runs inside agy" in got, got)
        w7 = start_waiter(api, home, conv="c-unknown")
        got = finish(w7)
        check("and in a conversation no mailbox answers in, it says to call "
              "a2a_inbox", "no a2a mailbox" in got, got)

        # --- the route moves -------------------------------------------------
        w8 = start_waiter(api, home)
        waiters.append(w8)
        wait_for(lambda: (box / "waiter").read_text().strip() == str(w8.pid), 8)
        api.fail.write_text("")
        w9 = start_waiter(api, home)
        got = finish(w9)
        api.fail.unlink()
        time.sleep(0.2)
        check("a sandboxed second start beside a running waiter fails without "
              "touching it: same `waiter`, its beat kept, still running",
              w9.returncode == 1 and w8.poll() is None
              and (box / "waiter").read_text().strip() == str(w8.pid)
              and wait_for(lambda: (box / "beat").exists(), 4), got)
        c.call("list_channels", conv="c-moved")
        got = finish(w8)
        outputs.append(got)
        check("when a2a moves to another conversation (a /clear, a new chat) "
              "the waiter stops", "another conversation" in got, got)
        c.call("list_channels", conv="c-root")

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

        w3 = start_waiter(api, home)
        waiters.append(w3)
        wait_for(lambda: (box / "waiter").read_text().strip() == str(w3.pid)
                 and hk._age(str(box / "beat")) < 2, 8)
        before = (proj / ".a2a.json").read_text()
        st = json.loads(c.call("a2a_channel_status", {"fix": True},
                               conv="c-root"))
        check("doctor: a healthy receiver comes out of fix=true as it went in",
              st["fixed"] == [] and st["next_step"] is None
              and (proj / ".a2a.json").read_text() == before,
              f"{st['fixed']} {st['next_step']}")

        # --- one receiver per project ------------------------------------------
        c2 = Client(broker, proj, data, home)
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
        c3 = Client(broker, proj, appdata, home)
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
        offbox = data / "boxes" / hk.project_hash(str(off))
        streams = broker.streams
        c4 = Client(broker, off, data, home)
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
        got = finish(w3)
        outputs.append(got)
        check("a waiter still running then says the project stopped receiving",
              "not receiving" in got, got)
        code, out = run_hook({"conversationId": "c-root",
                              "workspacePaths": [str(proj)]}, hook_env)
        check("and the hook goes silent", out == {}, str(out))

        # --- agy's CSRF token went nowhere ------------------------------------
        leaked = [str(p) for p in root.rglob("*") if p.is_file()
                  and SECRET in p.read_text(errors="replace")]
        check("the waiter never wrote agy's CSRF token anywhere, nor passed "
              "agentapi's reply (which carries it) through",
              leaked == [] and not any(SECRET in o for o in outputs),
              f"{leaked} {[o for o in outputs if SECRET in o]}")
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
          "PASS — queued unacked, rung by the waiter, nudged by the hook")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
