#!/usr/bin/env python3
"""Pi: a session with no UI sends as this agent and never receives.

    python3 tests/test_pi_send_only.py

A helper Pi starts inside the same process (a scheduled monitor, say) loads
this extension too, in the same project, so it IS the terminal's agent. It
used to open a second /stream for it: the broker keeps one stream per agent,
newest wins, so the two evicted each other and whatever landed in the helper
was injected and acked where no human reads.

Pi says which session is which (read from earendil-works/pi v0.83.0):
`ctx.mode` is "print" unless the host binds a UI — the terminal binds "tui"
(interactive-mode.ts `bindExtensions({uiContext, mode: "tui"})`) BEFORE it
emits session_start (agent-session.ts: `_applyExtensionBindings` → setUIContext,
then `emit(session_start)`).

What this pins, in two halves:

  normal Pi   "tui" and no mode: every inbound tool makes the broker call it
              always made and returns the broker's answer; the send tools and
              enable_a2a_here behave as before; status says it receives.
              (test_pi_resume.py runs its whole scenario under "tui", "rpc"
              and no mode and requires identical results.)
  no UI       "print" / "json": no /stream — even with no terminal running —
              nothing injected, mail goes to the terminal; sends carry the
              terminal's agent and client headers; the inbound tools refuse
              without calling the broker; enable_a2a_here connects nothing.

  derived id  a second run with NO A2A_AGENT, the id read from the identity
              store as on a real install. The stored id was read only on the
              way to the stream, so a helper that never went there signed as
              the folder name — an agent the broker did not know. A run that
              sets A2A_AGENT never consults the store and could not see it.

Loads the REAL index.ts once per instance, as Pi does, against a fake broker.
No database; needs node 22+ (TypeScript type stripping).
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
PI = HERE.parent / "plugin" / "pi" / "index.ts"
INSTANCES = 16

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


HARNESS = r'''
import { writeFileSync } from "node:fs"

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const enc = (m) => new TextEncoder().encode(JSON.stringify(m) + "\n")
const aborted = () => Object.assign(new Error("aborted"), { name: "AbortError" })

// --- fake broker ---------------------------------------------------------------
const calls = []          // {method, path, headers}
const streams = []
const ok = (b) => ({ ok: true, status: 200,
  text: async () => JSON.stringify(b), json: async () => b })

function openStream(signal) {
  const s = { open: true, queue: [], waiter: null }
  const close = () => {
    s.open = false
    if (s.waiter) { const w = s.waiter; s.waiter = null; w.reject(aborted()) }
  }
  if (signal?.aborted) close()
  else signal?.addEventListener("abort", close, { once: true })
  s.feed = (m) => {
    if (s.waiter) { const w = s.waiter; s.waiter = null; w.resolve({ done: false, value: enc(m) }) }
    else s.queue.push(enc(m))
  }
  streams.push(s)
  const read = () => {
    if (!s.open) return Promise.reject(aborted())
    if (s.queue.length) return Promise.resolve({ done: false, value: s.queue.shift() })
    return new Promise((resolve, reject) => { s.waiter = { resolve, reject } })
  }
  return { ok: true, status: 200,
           body: { getReader: () => ({ read, cancel: async () => close() }) } }
}
// Delivery is a destructive read: a live message goes to ONE open stream.
const push = (m) => { const s = streams.find((x) => x.open); if (s) s.feed(m) }
const openStreams = () => streams.filter((s) => s.open).length

globalThis.fetch = async (url, init = {}) => {
  const path = String(url).replace("http://broker.invalid", "")
  const method = init.method || "GET"
  calls.push({ method, path, headers: { ...(init.headers || {}) } })
  // /me answers as whoever asked: a fixed name here would paper over a client
  // that asked as the wrong one.
  const asked = (init.headers || {})["X-A2A-Agent"] || ""
  if (path === "/me") return ok({ agent: asked, registered: true, stations: ["s"] })
  if (path === "/channels")
    return ok({ channels: [{ name: "ops", members: ["probe", "stored-id"] }] })
  if (path.startsWith("/stream")) return openStream(init.signal)
  // Every other route answers with something recognisable, so a test can
  // see the tool returned the broker's body and not something of its own.
  return ok({ route: `${method} ${path}` })
}

// --- fake Pi: one per extension instance -----------------------------------------
function makePi() {
  const st = { events: {}, tools: {}, turns: [], context: [], queued: [],
               running: false, notes: [] }
  const pi = {
    on: (e, h) => { (st.events[e] ||= []).push(h) },
    registerTool: (t) => { st.tools[t.name] = t },
    ui: { notify: (m) => { st.notes.push(String(m)) } },
    sendMessage: (msg, opts) => {
      const text = String(msg.content)
      if (st.running) { st.queued.push(text); return }
      if (opts?.triggerTurn) { st.running = true; st.turns.push(text); return }
      st.context.push(text)
    },
  }
  const emit = async (name, ev, ctx) => {
    for (const h of st.events[name] || []) await h({ type: name, ...ev }, ctx)
  }
  return { pi, st, emit }
}

let n = 0
// mode: "tui", "rpc", "print", "json", or null for a Pi that reports none.
async function instance(id, mode, reason = "startup") {
  const p = makePi()
  const mod = await import(`./i${++n}.ts`)
  mod.default(p.pi)
  const before = streams.length
  const ctx = { sessionManager: { getSessionId: () => id },
                ...(mode ? { mode } : {}) }
  await p.emit("session_start", { reason }, ctx)
  await sleep(1200)
  p.opened = streams.length - before
  return p
}
async function shutdown(p) {
  await p.emit("session_shutdown", { reason: "quit" }, {})
  await sleep(300)
}
// Run a tool as Pi does. Returns {text} or {error}, and the broker calls it made.
async function tool(p, name, params = {}) {
  const from = calls.length
  try {
    const r = await p.st.tools[name].execute("call-1", params)
    return { text: r?.content?.[0]?.text ?? null, calls: calls.slice(from) }
  } catch (e) {
    return { error: String(e?.message || e), calls: calls.slice(from) }
  }
}
const snap = (p) => ({ turns: [...p.st.turns], context: [...p.st.context],
                       queued: [...p.st.queued], notes: [...p.st.notes] })
const msg = (id, text) => ({ id, kind: "channel", channel: "ops", sender: "peer",
                             text, ts: 1, audience: ["probe"], addressed: [] })
const INBOUND = [
  ["my_pending", { limit: 5 }],
  ["read_channel", { name: "ops" }],
  ["read_dms", {}],
  ["ack_messages", { ids: ["x1"] }],
  ["ack_all", {}],
]
const inbound = async (p) => {
  const r = {}
  for (const [name, params] of INBOUND) r[name] = await tool(p, name, params)
  return r
}
const sends = async (p) => ({
  send_dm: await tool(p, "send_dm", { to: "peer", text: "hello from here" }),
  post_to_channel: await tool(p, "post_to_channel", { name: "ops", text: "room hello" }),
})
const status = async (p) => JSON.parse((await tool(p, "a2a_channel_status")).text)
const out = {}

// A DERIVED id: no A2A_AGENT, the name comes from ~/.pi/agent/a2a-identity.json
// — what a real install runs as. The terminal reads it on its way to the
// stream; the helper never goes there and must read it all the same.
if (process.env.TEST_RUN === "derived") {
  const T = await instance("ses_t", "tui")
  out.t = { opened: T.opened, status: await status(T), sends: await sends(T) }
  const H = await instance("ses_h", "print")
  out.h = { opened: H.opened, status: await status(H), sends: await sends(H),
            session: snap(H) }
  out.stream_paths = calls.filter((c) => c.path.startsWith("/stream")).map((c) => c.path)
  console.log("@@" + JSON.stringify(out))
  process.exit(0)
}

// 1. A Pi that reports no mode: today's behaviour, tool by tool.
const N = await instance("ses_n", null)
out.n = { opened: N.opened, inbound: await inbound(N), sends: await sends(N),
          status: await status(N) }
await shutdown(N)

// 2. The terminal, and a helper started inside the same process beside it.
const T = await instance("ses_t", "tui")
out.t = { opened: T.opened, inbound: await inbound(T), sends: await sends(T),
          status: await status(T), session: snap(T) }
const H = await instance("ses_h", "print")
out.h = { opened: H.opened, open_streams: openStreams() }
push(msg("m-after-helper", "for-the-terminal"))
await sleep(300)
out.h.session = snap(H)
out.t.after = snap(T)
out.h.sends = await sends(H)
out.h.inbound = await inbound(H)
out.h.status = await status(H)
await shutdown(H)
await shutdown(T)

// 3. "json", the other mode with no UI.
const J = await instance("ses_j", "json")
out.j = { opened: J.opened, session: snap(J), inbound: await inbound(J) }
await shutdown(J)

// 4. The helper starts FIRST, with no terminal running; then the terminal.
const H1 = await instance("ses_h1", "print")
out.h1 = { opened: H1.opened, open_streams: openStreams() }
const T2 = await instance("ses_t2", "tui")
out.t2 = { opened: T2.opened }
push(msg("m-t2", "terminal-still-receives"))
await sleep(300)
out.t2.session = snap(T2)
out.h1.session = snap(H1)
await shutdown(H1)
await shutdown(T2)

// 5. enable_a2a_here in a project that is off.
const off = async (id, mode) => {
  writeFileSync(".a2a.json", "{}")
  const p = await instance(id, mode)
  const before = streams.length
  const r = await tool(p, "enable_a2a_here", { enabled: true })
  await sleep(600)
  const res = { opened_at_start: p.opened, opened_by_enable: streams.length - before,
                result: r }
  await shutdown(p)
  return res
}
out.enable = {
  tui: await off("ses_et", "tui"),
  none: await off("ses_en", null),
  print: await off("ses_ep", "print"),
}
writeFileSync(".a2a.json", '{"enabled_pi": true}')

console.log("@@" + JSON.stringify(out))
process.exit(0)
'''


def run(derived: bool) -> dict | None:
    """One harness run in a fresh HOME. `derived`: no A2A_AGENT, and the id
    comes from the identity store, as on a real install."""
    work = Path(tempfile.mkdtemp(prefix="a2a-pisendonly-"))
    (work / "node_modules" / "typebox").mkdir(parents=True)
    (work / "node_modules" / "typebox" / "package.json").write_text(
        json.dumps({"name": "typebox", "version": "0.0.0", "type": "module",
                    "main": "index.js"}))
    (work / "node_modules" / "typebox" / "index.js").write_text(
        "export const Type = new Proxy({}, { get: () => (...a) => ({ a }) });\n")
    # One copy per instance: Pi re-evaluates the module for each one.
    for i in range(1, INSTANCES + 1):
        shutil.copy(PI, work / f"i{i}.ts")
    (work / ".a2a.json").write_text('{"enabled_pi": true}')
    (work / "run.mjs").write_text(HARNESS)
    env = dict(os.environ, A2A_URL="http://broker.invalid", A2A_TOKEN="tok",
               A2A_AGENT="probe", A2A_HELLO="0", HOME=str(work))
    for k in ("A2A_READ_ON_INIT", "A2A_CATCHUP", "TEST_RUN"):
        env.pop(k, None)
    if derived:
        env.pop("A2A_AGENT")
        env["TEST_RUN"] = "derived"
        # Keyed by the client's `project`, which is process.cwd() — the REAL
        # path (/private/var/… on macOS, not the /var/… tempfile hands back).
        store = work / ".pi" / "agent" / "a2a-identity.json"
        store.parent.mkdir(parents=True, exist_ok=True)
        store.write_text(json.dumps({os.path.realpath(work): "stored-id"}))
    proc = subprocess.run(["node", "run.mjs"], cwd=work, env=env,
                          capture_output=True, text=True, timeout=180)
    line = next((l for l in proc.stdout.splitlines() if l.startswith("@@")), "")
    if not line:
        print(f"FAIL the harness produced nothing (derived={derived})")
        print(proc.stdout[-1500:])
        print(proc.stderr[-1500:])
        return None
    out = json.loads(line[2:])
    out["folder"] = work.name
    return out


def ident(r: dict) -> dict:
    """The identity a tool call went out under."""
    hd = (r.get("calls") or [{}])[0].get("headers") or {}
    return {k: hd.get(k) for k in ("Authorization", "X-A2A-Agent", "X-A2A-Client")}


def verify_derived(out: dict) -> None:
    folder = out["folder"]
    t, h = out["t"], out["h"]
    check("[derived id] the terminal streams as the STORED id, not the folder "
          "name — the pump path reads the store as it always did",
          t["opened"] == 1 and out["stream_paths"]
          and all("agent=stored-id" in p for p in out["stream_paths"])
          and t["status"].get("agent") == "stored-id",
          f"streams={out['stream_paths']} status.agent={t['status'].get('agent')}")
    check("[derived id] the helper reports the stored id too, and that it does "
          "not receive",
          h["status"].get("agent") == "stored-id"
          and h["status"].get("receives") is False,
          f"agent={h['status'].get('agent')} receives={h['status'].get('receives')} "
          f"(folder is {folder!r})")
    for name in ("send_dm", "post_to_channel"):
        r, ref = h["sends"][name], t["sends"][name]
        check(f"[derived id] the helper's {name} goes out as stored-id, with the "
              "terminal's token and client key — never as the folder name, an "
              "agent the broker does not know",
              "error" not in r and ident(r)["X-A2A-Agent"] == "stored-id"
              and ident(r) == ident(ref) and bool(ident(r)["X-A2A-Client"])
              and folder not in json.dumps(r.get("calls")),
              f"helper={ident(r)} terminal={ident(ref)}")
    check("[derived id] and the helper still opens no stream",
          h["opened"] == 0 and len(out["stream_paths"]) == 1
          and not (h["session"]["turns"] or h["session"]["context"]),
          f"opened={h['opened']} streams={out['stream_paths']}")


def main() -> int:
    if not shutil.which("node"):
        print("SKIP: node is not installed", file=sys.stderr)
        return 2
    out = run(derived=False)
    derived = run(derived=True)
    if out is None or derived is None:
        return 1

    routes = {
        "my_pending": ("GET", "/pending?limit=5"),
        "read_channel": ("GET", "/channels/ops/messages"),
        "read_dms": ("GET", "/dms"),
        "ack_messages": ("POST", "/ack"),
        "ack_all": ("POST", "/ack/all"),
    }
    send_routes = {"send_dm": ("POST", "/dms"),
                   "post_to_channel": ("POST", "/channels/ops/messages")}

    def made(r: dict, method: str, path: str) -> bool:
        return [(c["method"], c["path"]) for c in r.get("calls") or []] == [(method, path)]

    # --- normal Pi: no mode, and the terminal ----------------------------------------
    for label, key in (("no mode", "n"), ("tui", "t")):
        o = out[key]
        check(f"[{label}] the session opens its stream, as always",
              o["opened"] == 1, str(o["opened"]))
        for name, (method, path) in routes.items():
            r = o["inbound"][name]
            check(f"[{label}] {name} calls {method} {path} and returns the "
                  "broker's answer, as always",
                  "error" not in r and made(r, method, path)
                  and f"{method} {path}" in (r.get("text") or ""),
                  json.dumps(r)[:300])
        for name, (method, path) in send_routes.items():
            r = o["sends"][name]
            check(f"[{label}] {name} calls {method} {path}, as always",
                  "error" not in r and made(r, method, path), json.dumps(r)[:300])
        s = o["status"]
        check(f"[{label}] status: receives, registered, connected, nothing to do",
              s.get("receives") is True and s.get("registered") is True
              and s["push"]["stream_connected"] is True and s.get("next_step") is None,
              json.dumps(s)[:300])
    t = out["t"]["session"]
    check("[tui] the terminal's launch is briefed and checks its channels",
          any('You are a2a agent "' in c for c in t["context"])
          and any("Check your channels" in x for x in t["turns"]), json.dumps(t)[:300])

    # --- the helper, beside a running terminal ---------------------------------------
    h = out["h"]
    check("a helper (mode print) beside the terminal opens NO stream — the "
          "terminal's stays the only one",
          h["opened"] == 0 and h["open_streams"] == 1,
          f"opened={h['opened']} open={h['open_streams']}")
    hs = h["session"]
    check("nothing is put into the helper's session: no brief, no check, no "
          "message, no notice",
          not (hs["turns"] or hs["context"] or hs["queued"] or hs["notes"]),
          json.dumps(hs)[:300])
    check("a message posted after the helper started reaches the TERMINAL",
          any("for-the-terminal" in x for x in
              out["t"]["after"]["turns"] + out["t"]["after"]["context"]
              + out["t"]["after"]["queued"]),
          json.dumps(out["t"]["after"])[:300])

    for name, (method, path) in send_routes.items():
        r, ref = h["sends"][name], out["t"]["sends"][name]
        check(f"the helper's {name} reaches {method} {path} as the terminal's "
              "agent, with its token and client key — the broker signs it as "
              "the same agent",
              "error" not in r and made(r, method, path)
              and ident(r) == ident(ref) and ident(r)["X-A2A-Agent"] == "probe"
              and bool(ident(r)["X-A2A-Client"]),
              f"helper={ident(r)} terminal={ident(ref)} {json.dumps(r)[:200]}")
    for name in routes:
        r = h["inbound"][name]
        check(f"the helper's {name} refuses, and calls the broker for nothing",
              "sends as probe and does not receive" in (r.get("error") or "")
              and not r.get("calls"), json.dumps(r)[:300])
    s = h["status"]
    check("the helper's status says it does not receive, and does not tell it "
          "to wait for a stream it will never open",
          s.get("receives") is False and s.get("next_step") is None,
          json.dumps(s)[:300])

    # --- json ------------------------------------------------------------------------
    j = out["j"]
    check("mode json is a session with no UI too: no stream, nothing put into "
          "it, inbound tools refused",
          j["opened"] == 0
          and not (j["session"]["turns"] or j["session"]["context"])
          and all("does not receive" in (j["inbound"][k].get("error") or "")
                  and not j["inbound"][k].get("calls") for k in routes),
          json.dumps(j)[:300])

    # --- the helper first ------------------------------------------------------------
    h1, t2 = out["h1"], out["t2"]
    check("a helper that starts FIRST, with no terminal running, still opens "
          "no stream — it never takes the terminal's",
          h1["opened"] == 0 and h1["open_streams"] == 0, json.dumps(h1)[:200])
    check("the terminal that starts after it opens the stream, is briefed and "
          "checks, and receives",
          t2["opened"] == 1
          and any('You are a2a agent "' in c for c in t2["session"]["context"])
          and any("Check your channels" in x for x in t2["session"]["turns"])
          and any("terminal-still-receives" in x for x in
                  t2["session"]["turns"] + t2["session"]["context"]
                  + t2["session"]["queued"])
          and not any("terminal-still-receives" in x for x in
                      h1["session"]["turns"] + h1["session"]["context"]),
          json.dumps(t2)[:300])

    # --- enable_a2a_here -------------------------------------------------------------
    e = out["enable"]
    for label in ("tui", "none"):
        r = e[label]
        check(f"[{label}] enable_a2a_here in a project that is off connects "
              "at once, as always",
              r["opened_at_start"] == 0 and r["opened_by_enable"] == 1
              and "error" not in r["result"], json.dumps(r)[:300])
    r = e["print"]
    check("a helper's enable_a2a_here connects nothing",
          r["opened_at_start"] == 0 and r["opened_by_enable"] == 0
          and "error" not in r["result"], json.dumps(r)[:300])

    # --- a derived id, as on a real install ------------------------------------------
    verify_derived(derived)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — a session with no UI sends as this agent and never receives; "
          "normal Pi unchanged")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
