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

The OpenCode half RUNS the real plugin — imports it, stubs fetch, and calls
both entry points of its default export against temp directories: server()
the way OpenCode V1 does, setup(ctx) the way V2 does — because a source grep
would pass a client that reads the file and ignores it. It also pins what V2
added: the cleanup setup() returns, and the switch, close the stream they
leave behind, and switching off and on never opens a second one. The Codex half runs the real client as a
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
import { basename, join } from "node:path"

// URL_BASE and TOKEN are read at MODULE scope, so credentials have to exist
// before the import or the plugin bails for want of them and never reaches
// the gate.
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "t"
process.env.A2A_HELLO = "0"
// Under V2 the plugin keeps its own log, in OpenCode's data directory: keep
// that out of the real $HOME.
process.env.XDG_DATA_HOME = mkdtempSync(join(tmpdir(), "a2a-gate-data-"))
delete process.env.A2A_AGENT

let calls = []
// Every /stream the plugin opened, by agent (each case's temp directory name,
// so cases cannot see each other's), and whether it is still open. A stream
// closes the way a real one does: its read rejects when the fetch's signal
// aborts. "Off closes it" and "one pump" are statements about these.
const streams = []
globalThis.fetch = async (url, init = {}) => {
  const path = String(url).replace("http://broker.invalid", "")
  // Who made the call: its X-A2A-Agent, or the stream's ?agent=. Each case's
  // agent is its own temp directory's name, so a pump from an EARLIER case
  // reconnecting on schedule is not counted against a later one. A call that
  // names no agent at all is counted against every case.
  calls.push({
    path,
    agent: init.headers?.["X-A2A-Agent"]
      || new URL(String(url)).searchParams.get("agent") || "",
    // The client key every call must carry (0.5.0): the broker pins an agent
    // to the first one that acts as it.
    client: init.headers?.["X-A2A-Client"] || "",
  })
  const ok = (b) => ({ ok: true, status: 200,
    text: async () => JSON.stringify(b), json: async () => b })
  if (path === "/me") return ok({ agent: "x", registered: true })
  if (path === "/channels") return ok({ channels: [] })
  if (path.startsWith("/stream")) {
    const s = { agent: new URL(String(url)).searchParams.get("agent"), open: true }
    streams.push(s)
    const signal = init.signal
    const read = () => new Promise((_, reject) => {
      const fail = () => {
        s.open = false
        reject(Object.assign(new Error("aborted"), { name: "AbortError" }))
      }
      if (signal?.aborted) return fail()
      signal?.addEventListener("abort", fail, { once: true })
    })
    return { ok: true, status: 200, body: { getReader: () => ({ read }) } }
  }
  return ok({})
}
const callsOf = (dir) => calls
  .filter((c) => c.agent === basename(dir) || c.agent === "")
  .map((c) => c.path)
// The client keys a project's calls carried: one key, on every call.
const keysOf = (dir) => calls
  .filter((c) => c.agent === basename(dir))
  .map((c) => c.client)
const opened = (dir) => {
  const mine = streams.filter((s) => s.agent === basename(dir))
  return { total: mine.length, open: mine.filter((s) => s.open).length }
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

const mod = await import(PLUGIN_PATH)
const plugin = mod.default
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
  const s = await plugin.server({ client, directory: d })
  await settle()
  return { dir: d, s, tools: Object.keys(s.tool || {}),
           event: "event" in s, calls: callsOf(d), prompts: [...prompts] }
}

// --- V2: the smallest ctx setup() uses, recording what it is asked -----------
const v2prompts = []
const v2ctx = (d) => {
  const added = []
  const listed = []
  const hooks = {}
  const ctx = {
    app: { name: "opencode", version: "2.1.3", channel: "latest" },
    location: { directory: d, project: { id: "p", directory: d, canonical: d } },
    options: {},
    tool: {
      transform: async (cb) => {
        cb({ add: (t) => added.push(t) })
        return { dispose: async () => {} }
      },
    },
    // An event stream that stays quiet until it is aborted, as an idle
    // server's does.
    event: {
      subscribe: ({ signal } = {}) => ({
        [Symbol.asyncIterator]() {
          return {
            next: () => new Promise((resolve) => {
              const end = () => resolve({ done: true, value: undefined })
              if (signal?.aborted) return end()
              signal?.addEventListener("abort", end, { once: true })
            }),
            return: async () => ({ done: true, value: undefined }),
          }
        },
      }),
    },
    session: {
      list: async (q) => {
        listed.push(q)
        return { data: [{ id: "ses_x", time: { updated: 1 },
                          location: { directory: d } }], cursor: {} }
      },
      prompt: async (input) => {
        prompts.push(input?.text || "")
        v2prompts.push(input)
        return { id: "msg_x" }
      },
      // V2's model hooks: a name and a callback, returning a Registration.
      hook: async (name, cb) => {
        hooks[name] = cb
        return { dispose: async () => { delete hooks[name] } }
      },
    },
  }
  return { ctx, added, listed, hooks }
}
const runV2 = async (contents) => {
  calls = []; prompts.length = 0; v2prompts.length = 0
  const d = project(contents)
  const { ctx, added, listed, hooks } = v2ctx(d)
  const cleanup = await plugin.setup(ctx)
  await settle()
  const tool = (n) => added.find((t) => t.name === n)
  // What the context hook adds to a model request's system prompt here.
  const input = { sessionID: "ses_x", system: [] }
  if (hooks.context) await hooks.context(input)
  return { dir: d, cleanup, added, tool, listed: [...listed],
           tools: added.map((t) => t.name), calls: callsOf(d),
           prompts: [...prompts], v2prompts: [...v2prompts],
           hooked: Object.keys(hooks), system: input.system }
}

const cases = async (go) => ({
  absent: await go(null),
  false_: await go({ enabled_opencode: false }),
  stringy: await go({ enabled_opencode: "true" }),
  // Pi's key must not speak for OpenCode: one directory, several harnesses,
  // each its own agent.
  other: await go({ enabled_pi: true }),
  bare: await go({ enabled: true }),
  on: await go({ enabled_opencode: true }),
})

const out = {}
const v1 = await cases(run)
out.v1_keys = keysOf(v1.on.dir)

// The switch: writes, merges, and connects in place.
const off = await run({ catchup: 42, enabled_pi: true })
calls = []
out.wrote = JSON.parse(await off.s.tool.enable_a2a_here.execute({ enabled: true }))
await settle()
out.after_enable = callsOf(off.dir)
out.file = JSON.parse(readFileSync(join(off.dir, ".a2a.json"), "utf8"))
out.off_again = JSON.parse(await off.s.tool.enable_a2a_here.execute({ enabled: false }))
await settle()
// Off closes the stream that was open — not just the next one.
out.off_closed = opened(off.dir)

// "On" twice under a live stream: still ONE stream.
const twice = await run({ enabled_opencode: true })
await twice.s.tool.enable_a2a_here.execute({ enabled: true })
// Off and straight back on: the old stream closes, exactly one is open after.
const flip = await run({ enabled_opencode: true })
await flip.s.tool.enable_a2a_here.execute({ enabled: false })
await flip.s.tool.enable_a2a_here.execute({ enabled: true })

const v2 = await cases(runV2)
out.v2_keys = keysOf(v2.on.dir)
// The instructions in V2 go into the system prompt through the session
// "context" hook, never into the conversation.
out.v2_hooked = v2.on.hooked
out.v2_system_on = v2.on.system
out.v2_system_off = v2.absent.system
out.v2_posted = v2.on.v2prompts
out.v2_listed = v2.on.listed[0] || null
out.v2_tools = v2.on.added.map((t) => ({
  name: t.name, codemode: t.options?.codemode, input: t.input,
  description: typeof t.description,
}))
out.v2_enable = JSON.parse(
  (await v2.absent.tool("enable_a2a_here").execute({ enabled: true })).content)
await settle()
out.v2_enable_opened = opened(v2.absent.dir)
await v2.absent.tool("enable_a2a_here").execute({ enabled: false })
await settle()
out.v2_off_closed = opened(v2.absent.dir)
// Unload, as V2 does on reload: what setup returned must close the stream.
out.v2_before_cleanup = opened(v2.on.dir)
await v2.on.cleanup()
await settle()

// Past RECONNECT_MS, so a pump that survived its stop would have reconnected.
await new Promise((r) => setTimeout(r, 6500))
out.v2_after_cleanup = opened(v2.on.dir)
out.off_after_wait = opened(off.dir)
out.twice = opened(twice.dir)
out.flip = opened(flip.dir)

const strip = (r) => ({ tools: r.tools, event: r.event,
                        calls: r.calls, prompts: r.prompts })
const each = (c) => Object.fromEntries(
  Object.entries(c).map(([k, r]) => [k, strip(r)]))
console.log("@@" + JSON.stringify({ v1: each(v1), v2: each(v2), ...out }))
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
        # (path, X-A2A-Agent) per request: /ack marks only the CALLER's
        # receipts, so which id an ack went out as is part of what it did.
        self.agents: list[tuple[str, str]] = []
        # (path, X-A2A-Client) per request that named an agent: the key the
        # broker pins that agent to, which every such request must carry.
        self.clients: list[tuple[str, str]] = []
        # How many upcoming /ack requests answer 500 and ack nothing.
        self.fail_acks = 0
        # The ids /me/agents reports as this token's own (rename_me adopts
        # one of these instead of renaming).
        self.me_agents: list[str] = []
        # (path, JSON body) of every POST/PATCH/DELETE: what a tool SENT.
        self.bodies: list[tuple[str, dict]] = []
        # POST paths answered 409, the way the broker refuses a request.
        self.refuse: set[str] = set()
        self.acked: list[str] = []
        self.unacked: list[dict] = []
        # Unacked but past the replay window: the stream never offers these
        # again, only GET /pending/peek returns them — what a Hermes app
        # conversation closed for hours has to recover from.
        self.aged: list[dict] = []
        # False: answer /pending/peek 404, like a broker that predates it.
        self.peek = True
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

            def _record(self, path):
                broker.calls.append(path)
                broker.agents.append(
                    (path, self.headers.get("X-A2A-Agent") or ""))
                if self.headers.get("X-A2A-Agent") or path == "/stream":
                    broker.clients.append(
                        (path, self.headers.get("X-A2A-Client") or ""))

            def do_GET(self):
                path = self.path.split("?")[0]
                self._record(path)
                if path == "/stream":
                    return self._stream()
                if path == "/pending/peek":
                    if not broker.peek:
                        self.send_response(404)
                        self.send_header("Content-Length", "0")
                        self.end_headers()
                        return
                    # Read-only, like the real one: marks nothing.
                    return self._json({"messages": broker.aged
                                       + list(broker.unacked)})
                self._json({"agent": "x", "registered": True,
                            "stations": ["s"], "channels": [],
                            "agents": [{"agent_id": a}
                                       for a in broker.me_agents]})

            def do_POST(self):
                path = self.path.split("?")[0]
                self._record(path)
                n = int(self.headers.get("Content-Length") or 0)
                body = json.loads(self.rfile.read(n) or b"{}") if n else {}
                broker.bodies.append((path, body))
                if path in broker.refuse:
                    b = json.dumps({"error": "refused by the test"}).encode()
                    self.send_response(409)
                    self.send_header("Content-Type", "application/json")
                    self.send_header("Content-Length", str(len(b)))
                    self.end_headers()
                    self.wfile.write(b)
                    return
                if path == "/ack" and broker.fail_acks > 0:
                    broker.fail_acks -= 1
                    self.send_response(500)
                    self.send_header("Content-Length", "0")
                    self.end_headers()
                    return
                if path == "/ack":
                    ids = body.get("ids") or []
                    broker.acked += ids
                    broker.unacked = [m for m in broker.unacked
                                      if m["id"] not in ids]
                    broker.aged = [m for m in broker.aged
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
        # The X-A2A-Client of every request that named an agent (0.5.0).
        "clients": [c for p, c in fb.clients],
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

    # The same gate under both OpenCodes: V1 calls default.server(), V2 calls
    # default.setup(ctx), and a project that did not opt in must look the
    # same from the broker's side either way.
    for host in ("v1", "v2"):
        got_all = oc[host]
        for label, key in (("no .a2a.json at all", "absent"),
                           ('{"enabled_opencode": false}', "false_"),
                           ('{"enabled_opencode": "true"} — a STRING, which '
                            "must not count: the switch fails closed",
                            "stringy"),
                           ('{"enabled_pi": true} — ANOTHER client\'s key, '
                            "which must not speak for this one", "other"),
                           ('{"enabled": true} — the bare key names no '
                            "client, so it enables none", "bare")):
            got = got_all[key]
            check(f"opencode {host}: {label} → NOTHING reaches the broker",
                  got["calls"] == [], str(got["calls"]))
            check(f"opencode {host}: {label} → nothing is injected into the "
                  "session — no hello, no brief, no setup hint",
                  got["prompts"] == [], str(got["prompts"]))
            check(f"opencode {host}: {label} → but every tool is still "
                  "registered: the switch holds back effects, not vocabulary",
                  len(got["tools"]) > 1 and "enable_a2a_here" in got["tools"],
                  str(got["tools"]))

        check(f'opencode {host}: {{"enabled_opencode": true}} connects — /me '
              "and then the stream",
              "/me" in got_all["on"]["calls"]
              and any(c.startswith("/stream") for c in got_all["on"]["calls"]),
              str(got_all["on"]["calls"]))
        check(f"opencode {host}: an enabled project registers the same tools "
              "as a disabled one, so nothing about the surface depends on "
              "the switch",
              sorted(got_all["on"]["tools"])
              == sorted(got_all["absent"]["tools"]),
              f'on: {got_all["on"]["tools"]} vs '
              f'off: {got_all["absent"]["tools"]}')

    for host in ("v1", "v2"):
        keys = oc[f"{host}_keys"]
        check(f"opencode {host}: every call — the stream included — carries "
              "ONE client key, the one the broker pins this agent to",
              keys and len(set(keys)) == 1
              and keys[0].startswith("a2a_ck_"), str(keys))
    check("opencode: two projects are two clients — different keys",
          oc["v1_keys"][:1] != oc["v2_keys"][:1], f'{oc["v1_keys"][:1]} '
          f'{oc["v2_keys"][:1]}')

    check("opencode: V1 and V2 register the SAME tools — one vocabulary, "
          "whichever OpenCode runs it",
          sorted(oc["v1"]["on"]["tools"]) == sorted(oc["v2"]["on"]["tools"]),
          f'v1: {oc["v1"]["on"]["tools"]} vs v2: {oc["v2"]["on"]["tools"]}')

    # --- what V2 needs of a tool, read from its source ------------------------
    v2t = oc["v2_tools"]
    check("opencode v2: every tool says codemode:false — without it V2 hides "
          "it behind one `execute` tool, and the brief's 'call "
          "post_to_channel' names a tool the model does not have",
          v2t and all(t["codemode"] is False for t in v2t),
          str([t["name"] for t in v2t if t["codemode"] is not False]))
    check("opencode v2: every input is a JSON Schema object with no type "
          "arrays — an optional arg is simply not required",
          all(t["input"].get("type") == "object"
              and all(isinstance(p.get("type"), str)
                      for p in t["input"]["properties"].values())
              for t in v2t), str(v2t)[:400])
    post = next((t for t in v2t if t["name"] == "post_to_channel"), None)
    check("opencode v2: and `required` is exactly the non-nullable args",
          post is not None
          and sorted(post["input"]["required"]) == ["name", "text"],
          str(post))

    on_sys = oc["v2_system_on"] or []
    check("opencode v2: the instructions go into the system prompt through "
          "the session context hook, as a {type: text} part — never posted "
          "into the conversation",
          "context" in (oc["v2_hooked"] or [])
          and any(p.get("type") == "text"
                  and "ADDRESSING IS AN ARGUMENT" in (p.get("text") or "")
                  for p in on_sys)
          and not any("ADDRESSING IS AN ARGUMENT" in (p.get("text") or "")
                      for p in oc["v2_posted"] or []),
          str(on_sys)[:200])
    check("opencode v2: a project with a2a off adds nothing to the system "
          "prompt either", not oc["v2_system_off"], str(oc["v2_system_off"])[:200])
    listed = oc["v2_listed"] or {}
    check("opencode v2: sessions are asked for by directory and roots only — "
          "V2 sessions carry location.directory, so V1's filter would pass "
          "every project's",
          listed.get("directory", "").startswith("/")
          and "parentID" in listed and listed["parentID"] is None,
          str(listed))
    check("opencode v2: enable_a2a_here answers through {content} and "
          "connects in place",
          oc["v2_enable"].get("enabled") is True
          and oc["v2_enable_opened"]["open"] == 1, str(oc["v2_enable_opened"]))
    check("opencode v2: and off closes that stream on the spot",
          oc["v2_off_closed"]["open"] == 0, str(oc["v2_off_closed"]))
    check("opencode v2: unloading — what setup() returned — closes the "
          "stream, and nothing reconnects after it: V2 reloads a plugin when "
          "the installer rewrites it, and a survivor would split the inbox "
          "with its replacement",
          oc["v2_before_cleanup"]["open"] == 1
          and oc["v2_after_cleanup"] == {"total": 1, "open": 0},
          f'before={oc["v2_before_cleanup"]} after={oc["v2_after_cleanup"]}')

    # --- off stops what is running; on never doubles it (V1, same core) -------
    check("opencode: turning a project OFF closes the stream that was open, "
          "and it stays closed",
          oc["off_closed"]["open"] == 0
          and oc["off_after_wait"]["open"] == 0
          and oc["off_after_wait"]["total"] == oc["off_closed"]["total"],
          f'at off={oc["off_closed"]} later={oc["off_after_wait"]}')
    check("opencode: saying ON twice under a live stream opens no second "
          "stream",
          oc["twice"] == {"total": 1, "open": 1}, str(oc["twice"]))
    check("opencode: off and straight back on leaves exactly ONE stream open",
          oc["flip"]["open"] == 1, str(oc["flip"]))

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
    check("codex: every request naming its agent carries ONE client key — the "
          "one the broker pins that agent to",
          on["clients"] and len(set(on["clients"])) == 1
          and on["clients"][0].startswith("a2a_ck_"), str(on["clients"]))

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
