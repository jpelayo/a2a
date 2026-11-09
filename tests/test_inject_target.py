#!/usr/bin/env python3
"""OpenCode: a message goes to the session, and a refused one is never lost.

    python3 tests/test_inject_target.py

A real run, 2026-11-09: the agent ran a shell command, OpenCode 2 sent
shell.created with info {id: "sh_…", cwd, time}, and the client took that for
a root session and made it the injection target. The next #advisory message
was refused (SchemaError: Expected a string starting with "ses") — and then
dropped from the queue while its id stayed marked as handed over, so the
broker's replay was skipped as "already injected". It sat unshown for 16
minutes, until the install reloaded the plugin. OpenCode 1 has the same trap
in pty.created/updated (info {id: "pty_…", title, cwd}), and there a refusal
was worse: its SDK resolves a refused request with {error} instead of
throwing, so it read as delivered and was ACKED — lost outright.

This RUNS the real plugin under node, through both entry points — V1
server() and V2 setup() — against fakes that refuse a non-"ses" id the way
OpenCode does (V1: resolves {error}; V2: throws), and pins:

  terminal / shell   after pty.created (V1) or shell.created (V2), the next
                     message still goes to the ses_ session;
  refused once       a refused injection is not acked, stays queued, and is
                     retried by the client's own timer with no event to
                     prompt it; it then lands once and is acked once.

Kilo Code's copy is pinned the same way in test_kilocode_client.py.
Needs node. No broker, no database.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
OPENCODE = HERE.parent / "deploy" / "plugin" / "opencode" / "a2a-opencode.js"
ROOT = "ses_root_aaa"
AGENT = "target-test"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


HARNESS = r"""
import { mkdirSync, writeFileSync } from "node:fs"
import { join } from "node:path"

const job = JSON.parse(process.argv[2])
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "dev"
process.env.A2A_HELLO = "0"
process.env.A2A_AGENT = job.agent
process.env.HOME = job.home
process.env.XDG_DATA_HOME = join(job.home, ".local", "share")
const project = join(job.home, "proj")
mkdirSync(project, { recursive: true })
writeFileSync(join(project, ".a2a.json"), JSON.stringify({ enabled_opencode: true }))
const ROOT = job.root

// --- fake broker: one /stream the test writes to, and the acks ---------------
const acks = []
let streamCtl = null
const enc = new TextEncoder()
const send = (m) => streamCtl?.enqueue(enc.encode(JSON.stringify(m) + "\n"))
const ok = (b) => ({ ok: true, status: 200,
  text: async () => JSON.stringify(b), json: async () => b })
globalThis.fetch = async (url, init = {}) => {
  const u = new URL(String(url))
  if (u.pathname === "/me") return ok({ agent: job.agent, registered: true, stations: ["s"] })
  if (u.pathname === "/healthz") return ok({ ok: true, clients: "0" })
  if (u.pathname === "/channels") return ok({ channels: [] })
  if (u.pathname === "/ack") {
    acks.push(...(JSON.parse(init.body || "{}").ids || []))
    return ok({ acked: 1 })
  }
  if (u.pathname === "/stream") {
    return { ok: true, status: 200,
             body: new ReadableStream({ start(c) { streamCtl = c } }) }
  }
  return ok({})
}

// --- fake OpenCode: refuses what OpenCode refuses -----------------------------
const prompts = []          // what reached a session
const refusals = []         // what was refused, and why
let refuse = job.case === "refused" ? 1 : 0
const accept = (id, text) => {
  if (!String(id).startsWith("ses")) {
    refusals.push({ id, why: "not a session id" })
    return 'Expected a string starting with "ses" at ["sessionID"]'
  }
  if (refuse && text.includes("PEER-MSG")) {
    refuse--
    refusals.push({ id, why: "refused once" })
    return "refused once"
  }
  prompts.push({ id, text })
  return null
}

const settle = (ms) => new Promise((r) => setTimeout(r, ms))
const mod = await import(job.plugin)
let emit
if (job.mode === "v1") {
  const hooks = await mod.default.server({
    directory: project,
    client: {
      app: { log: async () => {} },
      session: {
        list: async () => [{ id: ROOT, directory: project, time: { updated: 1 } }],
        // V1's SDK resolves a refusal with {error}: it does not throw.
        promptAsync: async ({ path, body }) => {
          const why = accept(path?.id, String(body?.parts?.[0]?.text || ""))
          return why ? { error: { name: "BadRequest", data: { message: why } },
                         response: { ok: false, status: 400 } } : {}
        },
      },
    },
  })
  emit = (type, properties) => hooks.event({ event: { type, properties } })
} else {
  const evq = []
  let wake = null
  await mod.default.setup({
    app: { name: "opencode", version: "2.1.3", channel: "latest" },
    location: { directory: project, project: { id: "p", directory: project } },
    options: {},
    tool: { transform: async (cb) => { cb({ add: () => {} })
                                       return { dispose: async () => {} } } },
    event: {
      subscribe: ({ signal } = {}) => ({
        [Symbol.asyncIterator]() {
          return {
            next: () => new Promise((resolve) => {
              if (signal?.aborted) return resolve({ done: true, value: undefined })
              signal?.addEventListener("abort",
                () => resolve({ done: true, value: undefined }), { once: true })
              const pull = () => {
                if (!evq.length) { wake = pull; return }
                wake = null
                resolve({ done: false, value: evq.shift() })
              }
              pull()
            }),
            return: async () => ({ done: true, value: undefined }),
          }
        },
      }),
    },
    session: {
      list: async () => ({ data: [{ id: ROOT, time: { updated: 1 },
                                    location: { directory: project } }], cursor: {} }),
      // V2 throws on a refusal (a SchemaError for a non-session id).
      prompt: async (input) => {
        const why = accept(input?.sessionID, String(input?.text || ""))
        if (why) throw new Error(`SchemaError(${why})`)
        return { id: "msg_x" }
      },
      hook: async () => ({ dispose: async () => {} }),
    },
  })
  emit = async (type, data) => {
    evq.push({ type, data, location: { directory: project } })
    wake?.()
  }
}

await settle(1200)              // boot: the session is found, the stream opens
const message = { id: "m1", channel: "advisory", sender: "peer", text: "PEER-MSG",
                  ts: 1, kind: "channel", audience: [job.agent], addressed: [] }
const out = { mode: job.mode, case: job.case }
if (job.case === "terminal") {
  // What OpenCode sends when the agent runs a command (V2) or a terminal
  // opens (V1): an object with an id and a time or title, and no parent.
  if (job.mode === "v1") {
    const info = { id: "pty_1", title: "zsh", command: "zsh", args: [], cwd: project,
                   status: "running", pid: 1 }
    await emit("pty.created", { info })
    await emit("pty.updated", { info })
  } else {
    await emit("shell.created", { info: { id: "sh_1", command: "ls", cwd: project,
                                          status: "running", time: { started: 1 } } })
  }
  await settle(300)
  send(message)
  await settle(1000)
} else {
  send(message)
  await settle(1000)
  out.acks_after_refusal = [...acks]
  out.prompts_after_refusal = prompts.map((p) => p.text)
  await settle(6000)            // past RECONNECT_MS, with no event at all
}
out.prompts = prompts
out.refusals = refusals
out.acks = acks
console.log("@@" + JSON.stringify(out))
process.exit(0)
"""


def drive(base: Path, mode: str, case: str) -> dict:
    home = base / f"{mode}-{case}"
    home.mkdir()
    script = base / "harness.mjs"
    if not script.exists():
        script.write_text(HARNESS)
    job = {"mode": mode, "case": case, "home": str(home), "agent": AGENT,
           "root": ROOT, "plugin": str(OPENCODE)}
    # A clean environment: node's PATH and this case's own HOME, nothing else.
    r = subprocess.run(["node", str(script), json.dumps(job)],
                       capture_output=True, text=True, timeout=120,
                       env={"PATH": os.environ.get("PATH", "/usr/bin:/bin"),
                            "HOME": str(home)})
    if "@@" not in r.stdout:
        return {"__error__": (r.stderr or r.stdout)[-800:]}
    return json.loads(r.stdout.split("@@", 1)[1])


def main() -> int:
    if not shutil.which("node"):
        print("SKIP: node is not installed", file=sys.stderr)
        return 2
    base = Path(tempfile.mkdtemp(prefix="a2a-target-"))
    try:
        for mode, other in (("v1", "pty_1"), ("v2", "sh_1")):
            event = "pty.created" if mode == "v1" else "shell.created"
            t = drive(base, mode, "terminal")
            if "__error__" in t:
                check(f"{mode} terminal: the harness ran", False, t["__error__"])
                continue
            to_msg = [p["id"] for p in t["prompts"] if "PEER-MSG" in p["text"]]
            check(f"{mode}: after {event}, the message still goes to the "
                  "ses_ session — the shell/terminal id is not a session",
                  to_msg == [ROOT], json.dumps(t)[:600])
            check(f"{mode}: nothing is ever sent to {other}",
                  not any(r["id"] == other for r in t["refusals"])
                  and not any(p["id"] == other for p in t["prompts"]),
                  json.dumps(t["refusals"]))
            check(f"{mode}: and it is acked once, after it landed",
                  t["acks"].count("m1") == 1, str(t["acks"]))

            r = drive(base, mode, "refused")
            if "__error__" in r:
                check(f"{mode} refused: the harness ran", False, r["__error__"])
                continue
            how = "resolves {error}" if mode == "v1" else "throws"
            check(f"{mode}: a refused injection ({how}) is NOT acked — it was "
                  "not shown", "m1" not in r["acks_after_refusal"]
                  and "PEER-MSG" not in r["prompts_after_refusal"],
                  json.dumps(r)[:600])
            check(f"{mode}: it stays queued and the client retries it by "
                  "itself, with no event to prompt it — it lands once",
                  [p["id"] for p in r["prompts"] if "PEER-MSG" in p["text"]]
                  == [ROOT], json.dumps(r)[:600])
            check(f"{mode}: and is acked exactly once, after it landed",
                  r["acks"].count("m1") == 1, str(r["acks"]))
    finally:
        shutil.rmtree(base, ignore_errors=True)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — messages go to the session, and a refused one is retried, "
          "never lost")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
