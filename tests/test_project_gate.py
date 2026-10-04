#!/usr/bin/env python3
"""a2a makes no noise in a project until <project>/.a2a.json says so.

    python3 tests/test_project_gate.py

OpenCode and Pi install into a directory their harness scans for EVERY
session in EVERY directory — ~/.config/opencode/plugins and
~/.pi/agent/extensions — and Codex registers its client with `codex mcp add`,
which puts it in every session too. None of the three harnesses offers a
per-project way out. So the switch is ours, and this is what pins it.

The switch holds back EFFECTS, never the vocabulary. Every a2a tool is
registered in every project; what a disabled project does not get is the
stream, the brief, the hello and the setup hint — anything that appears in a
session that did not ask for a2a. Keeping the tools is also what lets
enable_a2a_here connect the session you ask in, with no restart: there is
nothing to wait for, because the tools are already under it.

The OpenCode half RUNS the real plugin — imports it, stubs fetch, calls A2A()
against temp directories — because a source grep would pass a client that
reads the file and ignores it. The Codex half runs the real client as a
process against a fake broker that counts every request, and drives its pump
in-process to prove that turning a project OFF stops delivery. Pi cannot be
imported here (typebox is not installed), so its half is source-level, the
same trade test_client_loads.py makes.

  off      tools registered, and ZERO network calls and ZERO injections.
           A typo must fail CLOSED.
  on       the stream runs.
  switch   writes the file, merges into it, and connects in place.

No broker, no database, no node_modules.
"""
import importlib.util
import json
import os
import queue
import re
import subprocess
import sys
import tempfile
import threading
import time
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
PI = PLUGIN / "pi" / "index.ts"
CODEX = PLUGIN / "codex" / "a2a-codex.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


# --- the OpenCode plugin, actually run ---------------------------------------
HARNESS = r"""
import { mkdtempSync, writeFileSync, readFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

// URL_BASE and TOKEN are read at MODULE scope, so credentials have to exist
// before the import or the plugin bails for want of them and never reaches
// the gate.
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "t"
process.env.A2A_HELLO = "0"
delete process.env.A2A_AGENT

let calls = []
globalThis.fetch = async (url) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path)
  const ok = (b) => ({ ok: true, status: 200,
    text: async () => JSON.stringify(b), json: async () => b })
  if (path === "/me") return ok({ agent: "x", registered: true })
  if (path === "/channels") return ok({ channels: [] })
  if (path.startsWith("/stream"))
    return { ok: true, status: 200,
             body: { getReader: () => ({ read: () => new Promise(() => {}) }) } }
  return ok({})
}

const prompts = []
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => [{ id: "ses_x", time: { updated: 1 } }],
    promptAsync: async (a) => {
      prompts.push(a?.body?.parts?.[0]?.text || "")
      return {}
    },
    prompt: async () => new Promise(() => {}),
  },
}

const { A2A } = await import(PLUGIN_PATH)
const settle = () => new Promise((r) => setTimeout(r, 1200))

const project = (contents) => {
  const d = mkdtempSync(join(tmpdir(), "a2a-gate-"))
  if (contents !== null) writeFileSync(join(d, ".a2a.json"), JSON.stringify(contents))
  return d
}
// Every case starts from a clean slate, so `calls` and `prompts` mean
// "what THIS project did", not "what the run has done so far".
const run = async (contents) => {
  calls = []; prompts.length = 0
  const d = project(contents)
  const s = await A2A({ client, directory: d })
  await settle()
  return { dir: d, s, tools: Object.keys(s.tool || {}),
           event: "event" in s, calls: [...calls], prompts: [...prompts] }
}

const out = {}
out.absent = await run(null)
out.false_ = await run({ enabled_opencode: false })
out.stringy = await run({ enabled_opencode: "true" })
// Pi's key must not speak for OpenCode: one directory, several harnesses,
// each its own agent.
out.other = await run({ enabled_pi: true })
out.bare = await run({ enabled: true })
out.on = await run({ enabled_opencode: true })

// The switch: writes, merges, and connects in place.
const off = await run({ catchup: 42, enabled_pi: true })
calls = []
out.wrote = JSON.parse(await off.s.tool.enable_a2a_here.execute({ enabled: true }))
await settle()
out.after_enable = [...calls]
out.file = JSON.parse(readFileSync(join(off.dir, ".a2a.json"), "utf8"))
out.off_again = JSON.parse(await off.s.tool.enable_a2a_here.execute({ enabled: false }))

const strip = (r) => ({ tools: r.tools, event: r.event,
                        calls: r.calls, prompts: r.prompts })
console.log("@@" + JSON.stringify({
  absent: strip(out.absent), false_: strip(out.false_),
  stringy: strip(out.stringy), other: strip(out.other),
  bare: strip(out.bare), on: strip(out.on),
  wrote: out.wrote, after_enable: out.after_enable,
  file: out.file, off_again: out.off_again,
}))
process.exit(0)
"""


def run_opencode() -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-gate-")) / "harness.mjs"
    tmp.write_text(f"const PLUGIN_PATH = {json.dumps(str(OPENCODE))}\n" + HARNESS)
    res = subprocess.run(["node", str(tmp)], capture_output=True, text=True,
                         timeout=120, env={**os.environ})
    if "@@" not in res.stdout:
        raise SystemExit("could not run the OpenCode harness:\n"
                         f"{res.stdout}\n{res.stderr}")
    return json.loads(res.stdout.split("@@", 1)[1])


# --- the Codex client, actually run ------------------------------------------
class FakeBroker:
    """Counts every request, answers enough for the client to get going, and
    serves a /stream that can be fed lines, replays what is unacked on every
    new connection — as the real broker does — and can go quiet (no
    keepalives) so the next line on the wire is exactly the one a test sends.
    """

    def __init__(self):
        self.calls: list[str] = []
        self.acked: list[str] = []
        self.unacked: list[dict] = []
        self.live: queue.Queue = queue.Queue()
        self.quiet = threading.Event()
        self.stop = threading.Event()
        self.streams = 0
        broker = self

        class H(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.0"     # a stream runs until close

            def log_message(self, *a):
                pass

            def _json(self, obj):
                b = json.dumps(obj).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(b)))
                self.end_headers()
                self.wfile.write(b)

            def do_GET(self):
                path = self.path.split("?")[0]
                broker.calls.append(path)
                if path == "/stream":
                    return self._stream()
                self._json({"agent": "x", "registered": True,
                            "stations": ["s"], "channels": []})

            def do_POST(self):
                path = self.path.split("?")[0]
                broker.calls.append(path)
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}") if n else {}
                if path == "/ack":
                    ids = body.get("ids") or []
                    broker.acked += ids
                    broker.unacked = [m for m in broker.unacked
                                      if m["id"] not in ids]
                self._json({"ok": True})

            do_PATCH = do_DELETE = do_POST

            def _stream(self):
                broker.streams += 1
                self.send_response(200)
                self.send_header("Content-Type", "application/x-ndjson")
                self.end_headers()
                try:
                    for m in list(broker.unacked):
                        self.wfile.write((json.dumps(m) + "\n").encode())
                        self.wfile.flush()
                    while not broker.stop.is_set():
                        try:
                            m = broker.live.get(timeout=0.2)
                        except queue.Empty:
                            if not broker.quiet.is_set():
                                self.wfile.write(b"\n")
                                self.wfile.flush()
                            continue
                        broker.unacked.append(m)
                        self.wfile.write((json.dumps(m) + "\n").encode())
                        self.wfile.flush()
                except OSError:
                    pass

        self.srv = ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.srv.daemon_threads = True
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()
        self.url = f"http://127.0.0.1:{self.srv.server_port}"

    def close(self):
        self.stop.set()
        self.srv.shutdown()


def _project(contents) -> Path:
    d = Path(tempfile.mkdtemp(prefix="a2a-gate-cx-")).resolve()
    if contents is not None:
        (d / ".a2a.json").write_text(json.dumps(contents))
    return d


def run_codex(contents, follow=()) -> dict:
    """The real Codex client as Codex runs it: a process in the project
    directory, spoken to over MCP stdio. `before` is every request that
    reached the broker while the session merely sat there; `after` is what
    the follow-up requests caused."""
    fb = FakeBroker()
    proj = _project(contents)
    env = dict(os.environ, A2A_URL=fb.url, A2A_TOKEN="t", A2A_CODEX_SOCK="",
               # its log and identity store, kept out of the real ~/.codex
               CODEX_HOME=str(proj.parent / (proj.name + "-home")))
    env.pop("A2A_AGENT", None)
    proc = subprocess.Popen([sys.executable, str(CODEX)], cwd=str(proj),
                            env=env, text=True, stdin=subprocess.PIPE,
                            stdout=subprocess.PIPE, stderr=subprocess.PIPE)

    def say(obj):
        proc.stdin.write(json.dumps(obj) + "\n")
        proc.stdin.flush()

    say({"jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {}})
    say({"jsonrpc": "2.0", "method": "notifications/initialized"})
    say({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    time.sleep(1.5)
    before = list(fb.calls)
    for i, (name, args) in enumerate(follow):
        say({"jsonrpc": "2.0", "id": 10 + i, "method": "tools/call",
             "params": {"name": name, "arguments": args}})
        time.sleep(1.5)
    after = fb.calls[len(before):]
    proc.stdin.close()
    out, err = proc.communicate(timeout=30)
    fb.close()
    replies = {r.get("id"): r.get("result") or {}
               for r in (json.loads(l) for l in out.splitlines() if l.strip())}
    try:
        file = json.loads((proj / ".a2a.json").read_text())
    except Exception:
        file = None
    return {
        "before": before, "after": after, "stderr": err,
        "instructions": (replies.get(1) or {}).get("instructions"),
        "tools": [t["name"] for t in (replies.get(2) or {}).get("tools", [])],
        "results": [json.loads(((replies.get(10 + i) or {}).get("content")
                                or [{}])[0].get("text") or "null")
                    for i in range(len(follow))],
        "file": file,
    }


def codex_off_stops_the_stream() -> dict:
    """Drive the real pump in-process: on, deliver; OFF, and a line arriving
    after that must be neither injected nor acked; on again, and the broker's
    replay hands that same line over. Push is stubbed to a recorder, because
    what is under test is the switch, not the websocket."""
    fb = FakeBroker()
    proj = _project({"enabled_codex": True})
    os.environ.update(A2A_URL=fb.url, A2A_TOKEN="t", A2A_CODEX_SOCK="",
                      CODEX_HOME=str(proj.parent / (proj.name + "-home")))
    os.environ.pop("A2A_AGENT", None)
    here = os.getcwd()
    os.chdir(proj)            # the client reads its project from its cwd
    try:
        spec = importlib.util.spec_from_file_location("gate_codex", CODEX)
        cx = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cx)
    finally:
        os.chdir(here)

    turns: list[str] = []

    class Session:
        def submit(self, text):
            turns.append(text)

    cx.get_server = lambda: Session()
    cx._injectable = lambda: True
    cx.RECONNECT_S = 0.1

    def until(cond, secs=5.0):
        end = time.time() + secs
        while time.time() < end:
            if cond():
                return True
            time.sleep(0.05)
        return False

    msg = lambda i, t: {"id": i, "channel": "ops", "sender": "bob", "text": t,
                        "audience": ["x"], "addressed": []}
    out = {}
    threading.Thread(target=cx.pump_guard, daemon=True).start()
    out["connected"] = until(lambda: cx._state["connected"])
    fb.live.put(msg("m1", "first-body"))
    out["m1_in"] = until(lambda: any("first-body" in t for t in turns))
    out["m1_acked"] = until(lambda: "m1" in fb.acked)

    fb.quiet.set()                         # next line on the wire is m2
    out["switched_off"] = json.loads(cx._enable_a2a_here({"enabled": False}))
    fb.live.put(msg("m2", "second-body"))
    out["closed"] = until(lambda: not cx._state["connected"])
    time.sleep(0.5)
    out["m2_in_while_off"] = any("second-body" in t for t in turns)
    out["m2_acked_while_off"] = "m2" in fb.acked
    streams = fb.streams

    fb.quiet.clear()
    cx._enable_a2a_here({"enabled": True})
    out["m2_in_after"] = until(lambda: any("second-body" in t for t in turns))
    out["m2_acked_after"] = until(lambda: "m2" in fb.acked)
    out["reconnected"] = fb.streams == streams + 1
    out["file"] = json.loads((proj / ".a2a.json").read_text())
    fb.close()
    return out


def main() -> int:
    oc = run_opencode()

    for label, key in (("no .a2a.json at all", "absent"),
                       ('{"enabled_opencode": false}', "false_"),
                       ('{"enabled_opencode": "true"} — a STRING, which must '
                        "not count: the switch fails closed", "stringy"),
                       ('{"enabled_pi": true} — ANOTHER client\'s key, which '
                        "must not speak for this one", "other"),
                       ('{"enabled": true} — the bare key names no client, '
                        "so it enables none", "bare")):
        got = oc[key]
        check(f"opencode: {label} → NOTHING reaches the broker",
              got["calls"] == [], str(got["calls"]))
        check(f"opencode: {label} → nothing is injected into the session — "
              "no hello, no brief, no setup hint",
              got["prompts"] == [], str(got["prompts"]))
        check(f"opencode: {label} → but every tool is still registered: the "
              "switch holds back effects, not vocabulary",
              len(got["tools"]) > 1 and "enable_a2a_here" in got["tools"],
              str(got["tools"]))

    check('opencode: {"enabled_opencode": true} connects — /me and then '
          'the stream',
          "/me" in oc["on"]["calls"]
          and any(c.startswith("/stream") for c in oc["on"]["calls"]),
          str(oc["on"]["calls"]))
    check("opencode: an enabled project registers the same tools as a "
          "disabled one, so nothing about the surface depends on the switch",
          sorted(oc["on"]["tools"]) == sorted(oc["absent"]["tools"]),
          f'on: {oc["on"]["tools"]} vs off: {oc["absent"]["tools"]}')

    check("opencode: the switch writes the file it is named for",
          oc["wrote"]["enabled"] is True
          and oc["wrote"]["file"].endswith("/.a2a.json"), str(oc["wrote"]))
    check("opencode: and MERGES — an existing catchup AND another client's "
          "answer both survive, because answering for one harness must not "
          "throw away the rest of the file",
          oc["file"] == {"catchup": 42, "enabled_pi": True,
                         "enabled_opencode": True}, str(oc["file"]))
    check("opencode: enabling CONNECTS THIS SESSION — the tools were already "
          "registered, so there is nothing to wait for and no restart to ask "
          "for",
          "/me" in oc["after_enable"]
          and any(c.startswith("/stream") for c in oc["after_enable"]),
          str(oc["after_enable"]))
    check("opencode: it turns a project off as well as on",
          oc["off_again"]["enabled"] is False, str(oc["off_again"]))

    # --- Codex: the real client, as a process ---------------------------------
    for label, contents in (("no .a2a.json at all", None),
                            ('{"enabled_codex": false}',
                             {"enabled_codex": False}),
                            ('{"enabled_codex": "true"} — a STRING', 
                             {"enabled_codex": "true"}),
                            ("ONLY the other clients' keys",
                             {"enabled_opencode": True, "enabled_pi": True}),
                            ('{"enabled": true} — the bare key', 
                             {"enabled": True})):
        got = run_codex(contents)
        check(f"codex: {label} → NOTHING reaches the broker",
              got["before"] == [], str(got["before"]))
        check(f"codex: {label} → no brief in the session: the handshake "
              f"carries no instructions",
              not got["instructions"], str(got["instructions"])[:80])
        check(f"codex: {label} → but every tool is listed, the switch "
              f"included",
              len(got["tools"]) > 1 and "enable_a2a_here" in got["tools"],
              str(got["tools"]))

    off = run_codex(None, follow=[("a2a_channel_status", {})])
    step = (off["results"][0] or {}).get("next_step") or ""
    check("codex: asked what is wrong in a project that is off, it says THAT "
          "first — not 'register', not 'wait for the stream' — and leaves "
          "the decision to the user",
          "a2a is off" in step and "enable_a2a_here" in step
          and "user" in step, step)

    on = run_codex({"enabled_codex": True})
    check('codex: {"enabled_codex": true} connects — the pump asks the '
          "broker who it is",
          "/me" in on["before"], str(on["before"]))
    check("codex: and the session is briefed",
          "ADDRESSING IS AN ARGUMENT" in (on["instructions"] or ""),
          str(on["instructions"])[:80])

    sw = run_codex({"catchup": 42, "enabled_pi": True},
                   follow=[("enable_a2a_here", {"enabled": True})])
    check("codex: the switch writes the file and MERGES — catchup and Pi's "
          "answer both survive",
          sw["file"] == {"catchup": 42, "enabled_pi": True,
                         "enabled_codex": True}, str(sw["file"]))
    check("codex: and enabling connects THIS session, with no restart",
          sw["before"] == [] and "/me" in sw["after"],
          f'before={sw["before"]} after={sw["after"]}')

    st = codex_off_stops_the_stream()
    check("codex: on, a message is delivered and acked",
          st["connected"] and st["m1_in"] and st["m1_acked"], str(st))
    check("codex: turning the project OFF closes the stream",
          st["switched_off"]["enabled"] is False and st["closed"], str(st))
    check("codex: and a message arriving after that is NOT injected and NOT "
          "acked — it waits on the broker, because delivery is a destructive "
          "read",
          not st["m2_in_while_off"] and not st["m2_acked_while_off"], str(st))
    check("codex: on again, the same message arrives — held, not lost",
          st["reconnected"] and st["m2_in_after"] and st["m2_acked_after"],
          str(st))

    # --- Pi: source, because typebox is not installed here --------------------
    pi = PI.read_text()
    check("pi: reads the project file SYNCHRONOUSLY — its entry point is not "
          "async and registerTool runs before any await could resolve",
          'from "node:fs"' in pi and "readFileSync(PROJECT_FILE" in pi,
          "no synchronous read")
    check("pi: the switch fails closed, like OpenCode's",
          "projectCfg[ENABLE_KEY] === true" in pi, "not a strict === true")
    check("pi: and it reads ITS OWN key — one directory can run several "
          "harnesses, and each is a separate agent",
          'const CLIENT = "pi"' in pi
          and "const ENABLE_KEY = `enabled_${CLIENT}`" in pi,
          "pi does not scope the switch to itself")
    check("pi: nothing connects in a project that has not opted in",
          # Not [^}]* — the log line inside the block contains a literal
          # `{"enabled": true}`, and the character class stopped at its brace.
          re.search(r"if \(!ENABLED\) \{.*?\n\s*return;", pi, re.S)
          is not None
          and pi.index("if (!ENABLED)") < pi.index("pumping = true"),
          "the pump is not gated, or is gated after it starts")
    check("pi: and the tools are NOT gated — every one registers whatever the "
          "switch says, which is what lets the switch connect in place",
          pi.count("\n  pi.registerTool({\n") > 15
          and "if (ENABLED) pi.registerTool" not in pi,
          "tool registration is behind the switch")
    check("pi: the switch starts the pump when it turns a project on",
          re.search(r"ENABLED = on;\s*\n\s*if \(started\)", pi) is not None,
          "enabling does not connect")

    # --- one file, one name, or a project is 'enabled' for only half of it ----
    oc_src = OPENCODE.read_text()
    check("both clients read the SAME file at the project root — several "
          "harnesses in one directory is a supported setup, and two names "
          "would mean enabling one project twice",
          '`${directory || "."}/.a2a.json`' in oc_src
          and 'join(project, ".a2a.json")' in pi
          and 'PROJECT_FILE = Path(os.getcwd()) / ".a2a.json"'
          in CODEX.read_text(),
          "the clients name different files")
    check("and both put it on TOP of the settings chain, so .a2a.json can "
          "carry read_on_init / catchup / agent per project",
          "project[fileKey] !== undefined" in oc_src
          and "projectCfg[fileKey] !== undefined" in pi,
          "the project file is not the first layer")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — a project is off until it says otherwise")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
