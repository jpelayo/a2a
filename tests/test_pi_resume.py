#!/usr/bin/env python3
"""Pi: one stream per process, and the channel check at CLI startup only.

    python3 tests/test_pi_resume.py

Pi REPLACES its extensions on /new, /resume, /fork and /reload, inside one
process (read from Pi 0.83.0: `AgentSessionRuntime.teardownCurrent` emits
session_shutdown to the old instance; the loader re-imports the module through
jiti with moduleCache:false and runs the factory again with a fresh `pi`; the
old `pi` "will throw if used"). Two things follow:

  survivor   nothing listened for session_shutdown, so the old pump kept
             streaming next to its replacement — two readers of one
             destructive-read inbox — and handed its half to a dead `pi`,
             where nothing was acked and the replay window soon stopped
             offering it. That is a message lost on /resume.
  the check  with read_on_init on, the "check your channels" turn runs when
             the user starts the CLI — `pi`, `pi -c`, session_start reason
             "startup" — and at no other time. Inside a running Pi a session
             gets the instructions only if it lacks them.

This loads the REAL index.ts once per instance, as Pi does, against a fake
broker whose stream hands each message to ONE open connection, as the real
one does. No database; needs node 22+ (TypeScript type stripping).
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
INSTANCES = 7

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


HARNESS = r'''
import { readFileSync, writeFileSync } from "node:fs"
import { join } from "node:path"
import { homedir } from "node:os"

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
const enc = (m) => new TextEncoder().encode(JSON.stringify(m) + "\n")
const aborted = () => Object.assign(new Error("aborted"), { name: "AbortError" })

// --- fake broker ---------------------------------------------------------------
const broker = { replay: [] }
let calls = []
const allCalls = []
let acked = []
const streams = []
const ok = (b) => ({ ok: true, status: 200,
  text: async () => JSON.stringify(b), json: async () => b })

function openStream(signal) {
  const s = { open: true, queue: broker.replay.map(enc), waiter: null }
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

globalThis.fetch = async (url, init = {}) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path); allCalls.push(path)
  if (path === "/me") return ok({ agent: "probe", registered: true, stations: ["s"] })
  if (path === "/channels") return ok({ channels: [{ name: "ops", members: ["probe"] }] })
  if (path === "/ack") {
    acked.push(...(JSON.parse(init.body || "{}").ids || []))
    return ok({ ok: true })
  }
  if (path.startsWith("/stream")) return openStream(init.signal)
  return ok({})
}

// --- fake Pi: one per extension instance -----------------------------------------
function makePi() {
  const st = { events: {}, turns: [], context: [], queued: [], running: false,
               stale: false, staleCalls: 0 }
  const pi = {
    on: (e, h) => { (st.events[e] ||= []).push(h) },
    registerTool: () => {},
    ui: { notify: () => {} },
    // Pi's own branch, chosen synchronously: queue while running, start a
    // turn when asked to and idle, else add to the context. And what Pi does
    // to a replaced instance's `pi`: throw.
    sendMessage: (msg, opts) => {
      if (st.stale) {
        st.staleCalls++
        throw new Error("extension instance is stale after session replacement")
      }
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
const ctx = (id) => ({ sessionManager: { getSessionId: () => id } })
const snap = (st) => ({ turns: [...st.turns], context: [...st.context],
                        queued: [...st.queued], staleCalls: st.staleCalls })

let n = 0
async function instance(id, reason, ctxOverride) {
  calls = []; acked = []
  const p = makePi()
  const mod = await import(`./i${++n}.ts`)
  mod.default(p.pi)
  await p.emit("session_start", { reason }, ctxOverride ?? ctx(id))
  await sleep(1200)
  return p
}
async function shutdown(p, reason) {
  const before = streams.slice()
  await p.emit("session_shutdown", { reason }, {})
  p.st.stale = true
  await sleep(300)
  return before.every((s) => !s.open)
}
const MEMO = join(homedir(), ".pi", "agent", "a2a-briefed.json")
const msg = (id, text) => ({ id, kind: "channel", channel: "ops", sender: "peer",
                             text, ts: 1, audience: ["probe"], addressed: [] })
const out = {}

const A = await instance("ses_p", "startup")
out.a = snap(A.st)
out.memo = JSON.parse(readFileSync(MEMO, "utf8")).ses_p || null
out.a_closed = await shutdown(A, "reload")

const B = await instance("ses_p", "reload")          // /reload: same session
out.b = snap(B.st)
push(msg("m-live", "live-after-reload"))
await sleep(300)
out.b_live = snap(B.st)
out.a_stale_calls = A.st.staleCalls
await shutdown(B, "resume")

// /resume, inside Pi, into a session already briefed: nothing; the replay
// still delivers what the stream holds.
broker.replay = [msg("m-replay", "replayed-on-resume")]
const C = await instance("ses_p", "resume")
out.c = snap(C.st)
out.c_acked = [...acked]
await shutdown(C, "quit")
broker.replay = []

const E = await instance("ses_new", "new")           // /new: another session
out.e = snap(E.st)
await shutdown(E, "quit")

const F = await instance("", "startup", {})          // no session id given
out.f = snap(F.st)
await shutdown(F, "quit")

// `pi -c` from the terminal: a LAUNCH, on a session the memory already has.
const H = await instance("ses_p", "startup")
out.h = snap(H.st)
await shutdown(H, "resume")

// /resume, inside Pi, into a session this client has never briefed.
const I = await instance("ses_other", "resume")
out.i = snap(I.st)
await shutdown(I, "quit")

out.all_calls = [...new Set(allCalls)]
console.log("@@" + JSON.stringify(out))
process.exit(0)
'''


def everything(s: dict) -> list[str]:
    return s["turns"] + s["context"] + s["queued"]


def briefs(s: dict) -> list[str]:
    return [t for t in everything(s) if 'You are a2a agent "' in t]


def checks(s: dict) -> list[str]:
    return [t for t in everything(s) if "Check your channels" in t]


def main() -> int:
    if not shutil.which("node"):
        print("SKIP: node is not installed", file=sys.stderr)
        return 2
    work = Path(tempfile.mkdtemp(prefix="a2a-piresume-"))
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
    env.pop("A2A_READ_ON_INIT", None)
    env.pop("A2A_CATCHUP", None)
    proc = subprocess.run(["node", "run.mjs"], cwd=work, env=env,
                          capture_output=True, text=True, timeout=120)
    line = next((l for l in proc.stdout.splitlines() if l.startswith("@@")), "")
    if not line:
        print("FAIL the harness produced nothing")
        print(proc.stdout[-1500:])
        print(proc.stderr[-1500:])
        return 1
    out = json.loads(line[2:])

    # --- the CLI starts -----------------------------------------------------------
    a = out["a"]
    check("`pi` starts: the instructions as context and one channel check that "
          "starts a turn, as before",
          len(briefs(a)) == 1 and any('You are a2a agent "' in c for c in a["context"])
          and len(checks(a)) == 1 and checks(a)[0] in a["turns"],
          json.dumps(a)[:400])
    check("and the brief is remembered, by Pi's session id, as this agent's",
          (out["memo"] or {}).get("key") == "probe"
          and (out["memo"] or {}).get("name") == "probe", str(out["memo"]))

    # --- session_shutdown stops the instance -------------------------------------
    check("session_shutdown closes the old instance's stream — it no longer "
          "reads this agent's inbox beside its replacement", out["a_closed"])
    check("a /reload of the same session briefs nothing and starts no turn",
          everything(out["b"]) == [], json.dumps(out["b"])[:300])
    check("a message arriving after the replacement reaches the NEW instance…",
          any("live-after-reload" in t for t in everything(out["b_live"])),
          json.dumps(out["b_live"])[:300])
    check("…and the old one's stale `pi` is never touched — Pi throws on it, "
          "and a message handed there was never acked",
          out["a_stale_calls"] == 0, str(out["a_stale_calls"]))

    # --- inside a running Pi ---------------------------------------------------------
    c = out["c"]
    texts = everything(c)
    check("/resume into a session already briefed: no brief, no check — and "
          "the stream's replay still delivers what it holds, acked as always",
          not briefs(c) and not checks(c)
          and sum("replayed-on-resume" in t for t in texts) == 1
          and "m-replay" in out["c_acked"], json.dumps(texts)[:400])
    check("nothing reads the inbox behind the agent's back: no /pending, no "
          "/pending/peek", not any(x.startswith("/pending") for x in out["all_calls"]),
          str(out["all_calls"]))
    e = out["e"]
    check("/new — another session, inside Pi — gets the instructions, and no "
          "channel check: that runs when Pi is launched",
          len(briefs(e)) == 1 and not checks(e) and e["turns"] == [],
          json.dumps(e)[:300])
    h = out["h"]
    check("`pi -c` from the terminal — a LAUNCH, on a session already briefed "
          "— briefs and checks, as it always did",
          len(briefs(h)) == 1 and len(checks(h)) == 1, json.dumps(h)[:300])
    i = out["i"]
    check("/resume into a session never briefed gets the instructions, and no "
          "check", len(briefs(i)) == 1 and not checks(i), json.dumps(i)[:300])
    f = out["f"]
    check("with no session id from Pi, the brief behaves exactly as before",
          len(briefs(f)) == 1 and len(checks(f)) == 1, json.dumps(f)[:300])

    print()
    for fl in fails:
        print("FAIL", fl)
    print("FAILED" if fails else
          "PASS — one stream per process, and the channel check at CLI startup only")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
