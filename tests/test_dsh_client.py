#!/usr/bin/env python3
"""DeepSeek Harness (dsh)'s own client: one Host, one agent per project.

    python3 tests/test_dsh_client.py

One dsh Host (`dsh web`, or the Desktop app's Host) serves EVERY project
directory, while an a2a agent is one harness-directory. This RUNS the real
package the way dsh does — deploy/plugin/dsh/index.js (no default export:
dsh's loader takes `module.default ?? module`) → `apply(ctx)` → a2a-dsh.js —
under a fake Cordis context and a fake broker (stubbed fetch):

  projects    two enabled projects in one Host: two agents, two keys, two
              streams; a third project with a2a off: no stream, no text.
  delivery    the hello with inject (no turn), the channel check and each
              message with followup, into the project's newest ROOT session
              (never a subagent's); delivered messages are acked.
  tools       registered ONCE for the Host and routed by the calling agent's
              session cwd; refused when the call carries no agent.
  prompt      a system-prompt section on each root agent's own ctx, whose
              text is the instructions only in an enabled, registered
              project — and none on a subagent.
  profiles    `headless` only sends: no stream, the inbox tools refuse.
  unload      the ctx.effect disposer stops every stream; nothing reconnects.
  files       everything under $DSH_HOME/a2a; nothing inside the package.

No broker, no database; needs node.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
DSH_DIR = HERE.parent / "deploy" / "plugin" / "dsh"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


HOST = r"""
import { mkdirSync, writeFileSync } from "node:fs"
import { join, basename } from "node:path"

const job = JSON.parse(process.argv[2])
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "t"
process.env.A2A_HELLO = "1"
delete process.env.A2A_AGENT
const ROOT = job.root
const proj = (name, sw) => {
  const d = join(ROOT, name)
  mkdirSync(d, { recursive: true })
  writeFileSync(join(d, ".a2a.json"), JSON.stringify(sw))
  return d
}
const A = proj("alpha", { enabled_dsh: true })
const B = proj("beta", { enabled_dsh: true })
const C = proj("gamma", {})

// --- fake broker --------------------------------------------------------------
const calls = []
const streams = []
const ok = (b) => ({ ok: true, status: 200,
  text: async () => JSON.stringify(b), json: async () => b })
globalThis.fetch = async (url, init = {}) => {
  const u = new URL(String(url))
  const agent = init.headers?.["X-A2A-Agent"] || u.searchParams.get("agent") || ""
  calls.push({ method: init.method || "GET", path: u.pathname, agent,
               client: init.headers?.["X-A2A-Client"] || "",
               body: init.body ? JSON.parse(init.body) : null })
  if (u.pathname === "/me") return ok({ agent, registered: true, stations: ["s"] })
  if (u.pathname === "/healthz") return ok({ ok: true, clients: "0" })
  if (u.pathname === "/channels")
    return ok({ channels: [{ name: "ops", members: [basename(A), basename(B)] }] })
  if (u.pathname === "/stream") {
    const queue = []
    let wake = null
    const s = { agent, open: true, push: (m) => { queue.push(m); wake?.() } }
    streams.push(s)
    const signal = init.signal
    const enc = new TextEncoder()
    const read = () => new Promise((resolve, reject) => {
      const fail = () => { s.open = false; reject(Object.assign(new Error("aborted"), { name: "AbortError" })) }
      if (signal?.aborted) return fail()
      signal?.addEventListener("abort", fail, { once: true })
      const pull = () => {
        if (queue.length) return resolve({ done: false, value: enc.encode(JSON.stringify(queue.shift()) + "\n") })
        wake = pull
      }
      pull()
    })
    return { ok: true, status: 200, body: { getReader: () => ({ read }) } }
  }
  return ok({ ok: true })
}

// --- fake dsh -----------------------------------------------------------------
const sent = []        // every message handed to an agent
const sections = []    // every system-prompt section registered
const mkAgent = (id, cwd, extra = {}) => ({
  id,
  session: { id, header: { id, cwd, ...extra } },
  status: "idle",
  ctx: { systemPrompt: { section: (s) => {
    const rec = { agent: id, ...s, disposed: false }
    sections.push(rec)
    return () => { rec.disposed = true }
  } } },
  followup: (m) => sent.push({ agent: id, how: "followup", text: m.content[0].text,
                               kind: m.source?.kind, frozen: Object.isFrozen(m) }),
  inject: (m) => sent.push({ agent: id, how: "inject", text: m.content[0].text,
                             kind: m.source?.kind, frozen: Object.isFrozen(m) }),
})
const agents = {
  sesA: mkAgent("sesA", A),
  sesAsub: mkAgent("sesAsub", A, { parentSession: "sesA", origin: "subagent" }),
  sesB: mkAgent("sesB", B),
  sesC: mkAgent("sesC", C),
}
const summaries = [
  { sessionId: "sesAsub", cwd: A, updatedAt: 30, parentSessionId: "sesA", origin: "subagent" },
  { sessionId: "sesB", cwd: B, updatedAt: 20 },
  { sessionId: "sesA", cwd: A, updatedAt: 10 },
  { sessionId: "sesC", cwd: C, updatedAt: 5 },
]
const handlers = {}
let disposer = null
const registered = []
const services = {
  profileContext: { name: job.profile, cwd: A },
  sessionController: {
    list: async () => summaries,
    resolveAgent: async (id) => agents[id] ? { agent: agents[id] } : { error: { code: "session/not-found" } },
  },
}
const ctx = {
  get: (n) => services[n],
  agents: { list: () => [agents.sesA, agents.sesAsub] },
  tools: { register: (t) => { registered.push(t); return () => {} } },
  systemPrompt: { section: () => () => {} },
  on: (name, fn) => { (handlers[name] ||= []).push(fn); return () => {} },
  effect: (fn) => { disposer = fn() },
}
const emit = async (name, payload) => { for (const fn of handlers[name] || []) await fn(payload) }
const settle = (ms = 1500) => new Promise((r) => setTimeout(r, ms))

const entry = await import(job.entry)
const out = { exports: Object.keys(entry).sort() }
await entry.apply(ctx)
await settle()
await emit("agent/created", { agent: agents.sesB, source: "user" })
await emit("agent/created", { agent: agents.sesC, source: "user" })
await settle()

const streamAgents = () => [...new Set(streams.map((s) => s.agent))].sort()
out.stream_agents = streamAgents()
out.keys = Object.fromEntries([basename(A), basename(B)].map((a) =>
  [a, [...new Set(calls.filter((c) => c.agent === a).map((c) => c.client))]]))
out.tools = registered.map((t) => t.name).sort()
out.tool_shape = registered.find((t) => t.name === "send_dm") && {
  params: registered.find((t) => t.name === "send_dm").parameters,
  out: registered.find((t) => t.name === "send_dm").output.schema,
  render: registered.find((t) => t.name === "send_dm").output.render({}, "x"),
}

if (job.profile !== "headless") {
  // A message for alpha, pushed on alpha's stream.
  streams.find((s) => s.agent === basename(A))?.push({ id: "m1", channel: "ops",
    sender: "peer", text: "hello alpha", audience: [basename(A)], addressed: [] })
  await settle()
}
out.sent = sent
out.acks = calls.filter((c) => c.path === "/ack").map((c) => ({ agent: c.agent, ids: c.body?.ids }))

// Tools: routed by the calling agent's cwd.
const dm = registered.find((t) => t.name === "send_dm")
await dm.execute({ to: "peer", text: "from beta" }, { agent: agents.sesB })
const post = calls.filter((c) => c.path === "/dms" && c.method === "POST").at(-1)
out.dm = post ? { agent: post.agent, sender: post.body?.sender } : null
try { await dm.execute({ to: "peer", text: "x" }, {}); out.no_agent = "accepted" }
catch (e) { out.no_agent = String(e.message) }
const pending = registered.find((t) => t.name === "my_pending")
const before = calls.length
try { await pending.execute({}, { agent: agents.sesA }); out.pending = "answered" }
catch (e) { out.pending = String(e.message) }
out.pending_calls = calls.length - before

out.sections = sections.map((s) => ({ agent: s.agent, name: s.name,
  order: s.order, text: typeof s.text === "function" ? s.text() : s.text }))

out.open_before = streams.filter((s) => s.open).length
out.streams_total_before = streams.length
if (typeof disposer === "function") disposer()
await settle(6500)            // past RECONNECT_MS: a survivor would reconnect
out.open_after = streams.filter((s) => s.open).length
out.streams_total_after = streams.length
console.log("@@" + JSON.stringify(out))
process.exit(0)
"""


def run(job: dict, env: dict) -> dict:
    host = Path(job["root"]) / "host.mjs"
    host.write_text(HOST)
    res = subprocess.run(["node", str(host), json.dumps(job)], capture_output=True,
                         text=True, timeout=120, env=env)
    if "@@" not in res.stdout:
        return {"__error__": (res.stderr or res.stdout)[-800:]}
    return json.loads(res.stdout.split("@@", 1)[1])


def main() -> int:
    if not shutil.which("node"):
        print("SKIP: node is not installed", file=sys.stderr)
        return 2
    base = Path(tempfile.mkdtemp(prefix="a2a-dsh-"))
    try:
        results = {}
        for profile in ("web", "headless"):
            root = base / profile
            home = root / "home"
            pkg = root / "pkg"
            home.mkdir(parents=True)
            shutil.copytree(DSH_DIR, pkg)
            env = {"HOME": str(home), "DSH_HOME": str(home / ".dsh"),
                   "PATH": os.environ.get("PATH", "/usr/bin:/bin")}
            results[profile] = run({"root": str(root), "profile": profile,
                                    "entry": str(pkg / "index.js")}, env)
            results[profile]["_pkg_before"] = sorted(p.name for p in DSH_DIR.iterdir())
            results[profile]["_pkg_after"] = sorted(p.name for p in pkg.iterdir())
            results[profile]["_state"] = sorted(
                str(p.relative_to(home)) for p in home.rglob("*") if p.is_file())

        web = results["web"]
        if "__error__" in web:
            check("the web Host ran", False, web["__error__"])
            return 1
        check("the entry exports exactly name, inject and apply — no default",
              web["exports"] == ["apply", "inject", "name"], str(web["exports"]))
        check("two enabled projects in one Host are two agents, each streaming",
              web["stream_agents"] == ["alpha", "beta"], str(web["stream_agents"]))
        keys = web["keys"]
        check("each with its own client key, on every call",
              all(len(v) == 1 and v[0] for v in keys.values())
              and keys["alpha"] != keys["beta"], str(keys))
        check("a project with a2a off opens no stream", "gamma" not in web["stream_agents"],
              str(web["stream_agents"]))
        sent = web["sent"]
        check("the hello goes in with inject — no turn spent on it",
              any(s["how"] == "inject" and "connected as" in s["text"] for s in sent),
              json.dumps(sent)[:400])
        check("the channel check goes in with followup, naming the tools",
              any(s["how"] == "followup" and "Check your channels" in s["text"]
                  for s in sent), json.dumps(sent)[:400])
        check("into each project's newest ROOT session — never the newer subagent",
              {s["agent"] for s in sent} <= {"sesA", "sesB"}
              and "sesAsub" not in {s["agent"] for s in sent}, json.dumps(sent)[:400])
        check("a pushed message reaches alpha's session with followup, in the "
              "<channel> envelope",
              any(s["agent"] == "sesA" and s["how"] == "followup"
                  and s["text"].startswith("<channel ") and "hello alpha" in s["text"]
                  for s in sent), json.dumps(sent)[:400])
        check("every message handed over is frozen and says it is a2a's",
              sent and all(s["frozen"] and s["kind"] == "a2a" for s in sent),
              json.dumps(sent)[:300])
        check("and it is acked, as alpha", {"agent": "alpha", "ids": ["m1"]} in web["acks"],
              str(web["acks"]))
        check("the tools are registered once for the Host, the full set",
              "send_dm" in web["tools"] and "a2a_channel_status" in web["tools"]
              and len(web["tools"]) == len(set(web["tools"])), str(web["tools"]))
        shape = web.get("tool_shape") or {}
        check("as raw dsh tools: JSON Schema input, a string output, rendered as text",
              (shape.get("params") or {}).get("type") == "object"
              and shape.get("out") == {"type": "string"}
              and shape.get("render") == [{"type": "text", "text": "x"}], str(shape))
        check("a tool call acts as the agent of the CALLER's project",
              web["dm"] == {"agent": "beta", "sender": "beta"}, str(web["dm"]))
        check("and a call with no agent is refused", "no session directory"
              in str(web["no_agent"]), str(web["no_agent"]))
        check("in a receiving profile the inbox tools answer",
              web["pending"] == "answered", str(web["pending"]))
        secs = web["sections"]
        check("a system-prompt section on each root agent, none on the subagent",
              sorted({s["agent"] for s in secs}) == ["sesA", "sesB", "sesC"]
              and all(s["name"] == "a2a" for s in secs), json.dumps(secs)[:400])
        by = {s["agent"]: s["text"] for s in secs}
        check("its text is the instructions in an enabled, registered project",
              "You are a2a agent" in by.get("sesA", "")
              and "You are a2a agent" in by.get("sesB", ""), json.dumps(by)[:300])
        check("and empty where a2a is off", by.get("sesC") == "", repr(by.get("sesC")))
        check("unload stops every stream", web["open_before"] >= 2
              and web["open_after"] == 0, f"{web['open_before']} -> {web['open_after']}")
        check("and nothing reconnects after it",
              web["streams_total_after"] == web["streams_total_before"],
              f"{web['streams_total_before']} -> {web['streams_total_after']}")

        hl = results["headless"]
        if "__error__" in hl:
            check("the headless Host ran", False, hl["__error__"])
        else:
            check("headless only sends: no stream at all", hl["stream_agents"] == [],
                  str(hl["stream_agents"]))
            check("and hands nothing to any session", hl["sent"] == [],
                  json.dumps(hl["sent"])[:300])
            check("its inbox tools refuse, without calling the broker",
                  "does not receive" in str(hl["pending"]) and hl["pending_calls"] == 0,
                  f"{hl['pending']} ({hl['pending_calls']} calls)")
            check("and a send still works, as the caller's project",
                  hl["dm"] == {"agent": "beta", "sender": "beta"}, str(hl["dm"]))

        for profile, r in results.items():
            check(f"{profile}: nothing is written inside the package folder",
                  r["_pkg_before"] == r["_pkg_after"],
                  f"{r['_pkg_before']} -> {r['_pkg_after']}")
            check(f"{profile}: everything it keeps is under $DSH_HOME/a2a",
                  r["_state"] and all(p.startswith(".dsh/a2a/") for p in r["_state"]),
                  str(r["_state"]))
    finally:
        shutil.rmtree(base, ignore_errors=True)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — one dsh Host, one agent per project")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
