#!/usr/bin/env python3
"""Kilo Code's own client: only the TUI receives, and a busy turn is not cut.

    python3 tests/test_kilocode_client.py

Kilo loads its plugins in EVERY process — the TUI's server, a `kilo daemon`,
the editors' `serve`, `kilo run` — and only one of them is the session a human
reads: plain `kilo` runs its server as a Worker thread inside the TUI process
with KILO_PROCESS_ROLE=worker. This RUNS the real plugin
(deploy/plugin/kilocode/a2a-kilocode.js) the way Kilo does — imported, its
default.server() called — inside real node Worker threads and on the main
thread, against a fake broker (stubbed fetch) and a fake Kilo client:

  receives   in a Worker with KILO_PROCESS_ROLE=worker: it streams, says the
             hello, runs the channel check, delivers and acks.
  sends      on the MAIN thread with the role inherited (`??=` passes it to a
             child), and in a Worker that is the daemon: no /stream at all,
             the inbox tools refuse without calling the broker, and a send is
             signed as the STORED id, asked of /me.
  busy       a message arriving while the session runs a turn is held — Kilo
             would supersede that turn — and delivered at the idle event.
  dispose    Kilo's dispose hook closes the stream and nothing reconnects.
  terminal   after pty.created/updated (info {id: "pty_…", title, cwd}) the
             message still goes to the ses_ session, never to the terminal.
  refused    a prompt Kilo refuses — its SDK RESOLVES {error}, it does not
             throw — is not acked, stays queued, and is retried by the
             client's own timer; it lands once and is acked once.
  switch     enabled_opencode does not turn Kilo on.
  folders    Kilo's own (~/.config/kilo, $XDG_DATA_HOME/kilo); nothing is ever
             created under opencode.

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
KILOCODE = HERE.parent / "deploy" / "plugin" / "kilocode" / "a2a-kilocode.js"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


# One scenario, run either on the main thread or inside a Worker. It reports
# what the plugin did through `report` (postMessage in a Worker).
SCENARIO = r"""
import { isMainThread, parentPort, workerData } from "node:worker_threads"
import { mkdirSync, writeFileSync } from "node:fs"
import { join, basename } from "node:path"

const job = isMainThread ? JSON.parse(process.argv[2]) : workerData
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "t"
process.env.A2A_HELLO = "1"
delete process.env.A2A_AGENT
const HOME = process.env.HOME
const project = join(HOME, "proj-" + job.name)
mkdirSync(project, { recursive: true })
writeFileSync(join(project, ".a2a.json"), JSON.stringify(job.switch))
if (job.stored) {
  mkdirSync(join(HOME, ".config", "kilo"), { recursive: true })
  writeFileSync(join(HOME, ".config", "kilo", "a2a-identity.json"),
                JSON.stringify({ [project]: job.stored }))
}

// --- fake broker --------------------------------------------------------------
const calls = []
const streams = []
const ok = (b) => ({ ok: true, status: 200,
  text: async () => JSON.stringify(b), json: async () => b })
const KEY = job.stored || basename(project)
globalThis.fetch = async (url, init = {}) => {
  const u = new URL(String(url))
  const path = u.pathname + u.search
  const agent = init.headers?.["X-A2A-Agent"] || u.searchParams.get("agent") || ""
  calls.push({ method: init.method || "GET", path, agent,
               body: init.body ? JSON.parse(init.body) : null })
  if (u.pathname === "/me") {
    const me = { agent: agent || KEY, registered: true, stations: ["s"] }
    // job.collide: another client holds this id (the identity collision).
    if (job.collide && (agent || KEY) === KEY)
      Object.assign(me, { this_agent: { agent_id: KEY, client: "other",
        bound: true, bound_to_me: true },
        refused: `agent '${KEY}' belongs to another client` })
    return ok(me)
  }
  if (u.pathname === "/healthz") return ok({ ok: true, clients: "0" })
  if (u.pathname === "/channels") return ok({ channels: [{ name: "ops", members: [KEY] }] })
  if (u.pathname === "/stream") {
    const queue = []
    let wake = null
    const s = { open: true, push: (m) => { queue.push(m); wake?.() } }
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

// --- fake Kilo client ---------------------------------------------------------
// It refuses what Kilo refuses, the way Kilo's SDK does: a refused request
// RESOLVES with {error} (no throwOnError), it does not throw.
const prompts = []
const refusals = []
let refuse = job.refuse ? 1 : 0
let status = job.busy ? { ses_x: { type: "busy" } } : {}
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => ({ data: [{ id: "ses_x", directory: project, time: { updated: 1 } }] }),
    status: async () => ({ data: status }),
    promptAsync: async (a) => {
      const id = a?.path?.id
      const text = a?.body?.parts?.[0]?.text || ""
      const no = (why) => {
        refusals.push({ id, why })
        return { error: { name: "BadRequest", data: { message: why } },
                 response: { ok: false, status: 400 } }
      }
      if (!String(id).startsWith("ses")) return no("not a session id")
      if (refuse && text.includes("hello from peer")) { refuse--; return no("refused once") }
      prompts.push({ id, text, noReply: !!a?.body?.noReply })
      return {}
    },
  },
}

const settle = (ms = 1500) => new Promise((r) => setTimeout(r, ms))
const mod = await import(job.plugin)
const hooks = await mod.default.server({ client, directory: project })
await settle()
const out = { name: job.name, hooks: Object.keys(hooks).sort() }

if (job.terminal) {
  // What Kilo sends when a terminal opens: an object with an id and a title,
  // and no parent — it looks like a session (packages/schema/src/pty.ts).
  const info = { id: "pty_1", title: "zsh", command: "zsh", args: [], cwd: project,
                 status: "running", pid: 1 }
  await hooks.event({ event: { type: "pty.created", properties: { info } } })
  await hooks.event({ event: { type: "pty.updated", properties: { info } } })
  await settle(300)
}
if (job.deliver) {
  streams.at(-1)?.push({ id: "m1", channel: "ops", sender: "peer", text: "hello from peer",
                         audience: [KEY], addressed: [] })
  await settle()
  out.while_busy = prompts.map((p) => p.text)
  if (job.busy) {
    status = {}
    await hooks.event({ event: { type: "session.idle", properties: { sessionID: "ses_x" } } })
    await settle()
  }
  if (job.refuse) {
    out.acks_after_refusal = calls.filter((c) => c.path === "/ack")
      .flatMap((c) => c.body?.ids || [])
    await settle(6500)          // past RECONNECT_MS, with no event at all
  }
}
out.prompts = prompts
out.refusals = refusals
out.acks = calls.filter((c) => c.path === "/ack").flatMap((c) => c.body?.ids || [])
out.streams = streams.length
if (job.tools) {
  out.refused = []
  for (const t of ["my_pending", "read_channel", "read_dms", "ack_messages", "ack_all"]) {
    const before = calls.length
    try {
      await hooks.tool[t].execute({ name: "ops", ids: ["m1"] })
      out.refused.push({ t, refused: false })
    } catch (e) {
      out.refused.push({ t, refused: true, called: calls.length - before, msg: String(e.message) })
    }
  }
  await hooks.tool.send_dm.execute({ to: "peer", text: "hi" })
  const dm = calls.find((c) => c.path === "/dms" && c.method === "POST")
  out.dm = dm ? { agent: dm.agent, sender: dm.body?.sender } : null
  out.status = JSON.parse(await hooks.tool.a2a_channel_status.execute({}))
}
if (job.dispose) {
  out.open_before = streams.filter((s) => s.open).length
  hooks.dispose()
  await settle(6500)            // past RECONNECT_MS: a survivor would reconnect
  out.open_after = streams.filter((s) => s.open).length
  out.streams_after = streams.length
}
if (isMainThread) { console.log("@@" + JSON.stringify(out)); process.exit(0) }
parentPort.postMessage(out)
process.exit(0)
"""

# Runs every Worker scenario, each with its own environment, and prints them.
DRIVER = r"""
import { Worker } from "node:worker_threads"
const jobs = JSON.parse(process.argv[2])
const out = {}
for (const job of jobs) {
  out[job.name] = await new Promise((resolve) => {
    const w = new Worker(job.script, { workerData: job, env: job.env })
    w.once("message", resolve)
    w.once("error", (e) => resolve({ error: String(e) }))
    w.once("exit", (c) => resolve({ error: `exited ${c} without reporting` }))
  })
}
console.log("@@" + JSON.stringify(out))
process.exit(0)
"""


def run(cmd: list[str], env: dict) -> dict:
    res = subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=env)
    if "@@" not in res.stdout:
        return {"__error__": (res.stderr or res.stdout)[-600:]}
    return json.loads(res.stdout.split("@@", 1)[1])


def main() -> int:
    if not shutil.which("node"):
        print("SKIP: node is not installed", file=sys.stderr)
        return 2
    base = Path(tempfile.mkdtemp(prefix="a2a-kilocode-"))
    try:
        scenario = base / "scenario.mjs"
        scenario.write_text(SCENARIO)
        driver = base / "driver.mjs"
        driver.write_text(DRIVER)

        def env_for(name: str, extra: dict) -> dict:
            home = base / name
            home.mkdir()
            # A clean environment: only what node needs to run, and this
            # scenario's own home. Nothing Kilo would set is inherited.
            env = {"HOME": str(home), "XDG_DATA_HOME": str(home / ".local" / "share"),
                   "PATH": os.environ.get("PATH", "/usr/bin:/bin")}
            env.update(extra)
            return env

        on = {"enabled_kilocode": True}
        jobs = [
            {"name": "tui", "switch": on, "deliver": True, "dispose": True,
             "env": env_for("tui", {"KILO_PROCESS_ROLE": "worker"})},
            {"name": "busy", "switch": on, "deliver": True, "busy": True,
             "env": env_for("busy", {"KILO_PROCESS_ROLE": "worker"})},
            {"name": "daemon", "switch": on, "tools": True, "stored": "stored-id",
             "env": env_for("daemon", {"KILOCODE_FEATURE": "daemon"})},
            {"name": "opencode_key", "switch": {"enabled_opencode": True},
             "env": env_for("opencode_key", {"KILO_PROCESS_ROLE": "worker"})},
            {"name": "terminal", "switch": on, "deliver": True, "terminal": True,
             "env": env_for("terminal", {"KILO_PROCESS_ROLE": "worker"})},
            {"name": "refused", "switch": on, "deliver": True, "refuse": True,
             "env": env_for("refused", {"KILO_PROCESS_ROLE": "worker"})},
            {"name": "collide", "switch": on, "collide": True,
             "env": env_for("collide", {"KILO_PROCESS_ROLE": "worker"})},
        ]
        for j in jobs:
            j["script"] = str(scenario)
            j["plugin"] = str(KILOCODE)
        workers = run(["node", str(driver), json.dumps(jobs)], dict(jobs[0]["env"]))
        if "__error__" in workers:
            check("the Worker scenarios ran", False, workers["__error__"])
            return 1

        # The main thread, with the role INHERITED: not the TUI's server.
        main_env = env_for("main", {"KILO_PROCESS_ROLE": "worker"})
        main_job = {"name": "main", "switch": on, "tools": True,
                    "stored": "stored-id", "plugin": str(KILOCODE)}
        mainrun = run(["node", str(scenario), json.dumps(main_job)], main_env)

        tui = workers["tui"]
        coll = workers.get("collide") or {}
        notes = [p for p in coll.get("prompts", [])
                 if "already used by another client" in p["text"]]
        check("collide: an id another client holds is said at start, once, "
              "as noReply, offering the stem plus -kilo",
              len(notes) == 1 and notes[0]["noReply"]
              and '"proj-collide-kilo"' in notes[0]["text"]
              and "propose_me with no name" in notes[0]["text"],
              json.dumps(coll.get("prompts"))[:400])
        check("collide: instead of the channel check",
              not any("Check your channels" in p["text"]
                      for p in coll.get("prompts", [])),
              json.dumps(coll.get("prompts"))[:400])
        check("the scenarios report", all("error" not in v for v in workers.values())
              and "__error__" not in mainrun,
              json.dumps({**workers, "main": mainrun})[:600])
        check("its hooks are Kilo's V1 set, with dispose",
              tui.get("hooks") == sorted(["dispose", "event", "tool",
                                          "experimental.chat.system.transform"]),
              str(tui.get("hooks")))
        texts = [p["text"] for p in tui.get("prompts", [])]
        check("receives in the TUI's worker: a stream is open",
              tui.get("streams", 0) >= 1, str(tui.get("streams")))
        check("and says the hello as noReply, so no turn is spent on it",
              any(p["noReply"] and "connected as" in p["text"]
                  for p in tui.get("prompts", [])), json.dumps(tui.get("prompts"))[:300])
        check("and runs the channel check at start, naming the tools",
              any("Check your channels" in t and "my_pending" in t for t in texts),
              json.dumps(texts)[:300])
        check("and delivers a pushed message in the <channel> envelope",
              any(t.startswith("<channel ") and "hello from peer" in t for t in texts),
              json.dumps(texts)[:300])
        check("and acks what it handed over", "m1" in tui.get("acks", []),
              str(tui.get("acks")))
        check("dispose closes the stream", tui.get("open_before", 0) >= 1
              and tui.get("open_after") == 0, str(tui))
        check("and nothing reconnects after it",
              tui.get("streams_after") == tui.get("streams"), str(tui))

        busy = workers["busy"]
        btexts = [p["text"] for p in busy.get("prompts", [])]
        check("busy: nothing is handed to a session running a turn — Kilo "
              "would supersede it", busy.get("while_busy") == [],
              json.dumps(busy.get("while_busy"))[:300])
        check("and at the idle event the message is delivered",
              any("hello from peer" in t for t in btexts), json.dumps(btexts)[:300])
        check("and acked only then", "m1" in busy.get("acks", []),
              str(busy.get("acks")))

        for label, r in (("the daemon (a Worker without the role)", workers["daemon"]),
                         ("the main thread with the role inherited", mainrun)):
            check(f"{label}: no stream at all", r.get("streams") == 0,
                  str(r.get("streams")))
            check(f"{label}: nothing is injected", r.get("prompts") == [],
                  json.dumps(r.get("prompts"))[:200])
            refused = r.get("refused") or []
            check(f"{label}: every inbox tool refuses, without calling the broker",
                  len(refused) == 5 and all(x["refused"] and x["called"] == 0
                                            for x in refused),
                  json.dumps(refused)[:400])
            check(f"{label}: a send is signed as the STORED id",
                  r.get("dm") == {"agent": "stored-id", "sender": "stored-id"},
                  str(r.get("dm")))
            st = r.get("status") or {}
            by = {c["check"]: c for c in st.get("checks") or []}
            check(f"{label}: the doctor says it only sends, by design",
                  by.get("push_host", {}).get("ok") is True
                  and "only sends" in str(by["push_host"].get("detail"))
                  and (st.get("push") or {}).get("receives") is False,
                  json.dumps(by.get("push_host")))
        check("the daemon is named as the daemon",
              "the Kilo daemon" in json.dumps(workers["daemon"].get("status")),
              json.dumps(workers["daemon"].get("status"))[:300])

        ok_key = workers["opencode_key"]
        check("enabled_opencode does not turn Kilo on: no stream, nothing said",
              ok_key.get("streams") == 0 and ok_key.get("prompts") == [],
              json.dumps(ok_key)[:300])

        # A terminal opened in Kilo sends pty.created/updated with info
        # {id: "pty_…", title, cwd}: it looked like a session, became the
        # injection target, and the next message went to it.
        term = workers["terminal"]
        check("terminal: after pty.created, the message still goes to the "
              "ses_ session — a terminal id is not a session",
              [p["id"] for p in term.get("prompts", [])
               if "hello from peer" in p["text"]] == ["ses_x"],
              json.dumps(term)[:600])
        check("terminal: nothing is ever sent to pty_1",
              not any(r["id"] == "pty_1" for r in term.get("refusals", [])),
              json.dumps(term.get("refusals")))
        check("terminal: and it is acked once, after it landed",
              term.get("acks", []).count("m1") == 1, str(term.get("acks")))

        # Kilo's SDK resolves a refused prompt with {error}: read as
        # delivered, it was acked and lost.
        ref = workers["refused"]
        check("refused: a refused injection (resolves {error}) is NOT acked — "
              "it was not shown", "m1" not in ref.get("acks_after_refusal", ["m1"]),
              json.dumps(ref)[:600])
        check("refused: it stays queued and the client retries it by itself, "
              "with no event to prompt it — it lands once",
              [p["id"] for p in ref.get("prompts", [])
               if "hello from peer" in p["text"]] == ["ses_x"],
              json.dumps(ref)[:600])
        check("refused: and is acked exactly once, after it landed",
              ref.get("acks", []).count("m1") == 1, str(ref.get("acks")))

        homes = [base / n for n in ("tui", "busy", "daemon", "opencode_key",
                                    "terminal", "refused", "main")]
        made = sorted(str(p.relative_to(base)) for h in homes for p in h.rglob("*")
                      if p.name == "opencode")
        check("nothing is ever created under opencode", not made, str(made))
        check("the client secret is in Kilo's own data folder",
              (base / "tui" / ".local" / "share" / "kilo" / "a2a-client.secret").is_file(),
              str(sorted(str(p.relative_to(base)) for p in (base / "tui").rglob("*"))[:20]))
    finally:
        shutil.rmtree(base, ignore_errors=True)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — Kilo receives only in its TUI, and "
                                 "never cuts a running turn short")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
