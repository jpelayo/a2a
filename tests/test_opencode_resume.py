#!/usr/bin/env python3
"""OpenCode: the channel check runs when the CLI starts, and at no other time.

    python3 tests/test_opencode_resume.py

The rule: with read_on_init on, the "check your channels" turn runs when the
user starts the CLI. The plugin lives in the CLI's process — OpenCode 1's own,
or OpenCode 2's per-launch server once its background service is disabled —
so "the CLI started" is "the first brief of this agent in this process".
OpenCode 2 also recycles a project's plugin instance inside one process after
an hour without session activity, and reloads it when its file changes;
neither is a start, and before this each one briefed the session again: the
hello, the instructions, and a check that starts a turn.

This runs the REAL plugin, through OpenCode 2's setup() and 1's server(),
against a fake broker, loading it again and again on one data directory, and
forges the process mark to stand for a new process. No database; needs node.
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
OPENCODE = HERE.parent / "plugin" / "opencode" / "a2a-opencode.js"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


HARNESS = r"""
import { mkdirSync, readFileSync, writeFileSync, existsSync } from "node:fs"
import { join } from "node:path"

const BASE = %(base)s
const DATA = join(BASE, "data")
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "t"
process.env.A2A_HELLO = "1"
process.env.HOME = join(BASE, "home")
process.env.XDG_DATA_HOME = DATA
delete process.env.A2A_AGENT

// The agent id is derived from the project directory's name.
const project = join(BASE, "resume-proj")
mkdirSync(project, { recursive: true })
writeFileSync(join(project, ".a2a.json"), JSON.stringify({ enabled_opencode: true }))
const KEY = "resume-proj"

// --- fake broker -------------------------------------------------------------
const broker = { name: KEY }
let calls = []
const allCalls = []
const ok = (b) => ({ ok: true, status: 200,
  text: async () => JSON.stringify(b), json: async () => b })
globalThis.fetch = async (url, init = {}) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path); allCalls.push(path)
  if (path === "/me") return ok({ agent: broker.name, registered: true, stations: ["s"] })
  if (path === "/channels")
    return ok({ channels: [{ name: "advisory", members: [KEY, broker.name] }] })
  if (path.startsWith("/stream")) {
    const signal = init.signal
    const read = () => new Promise((_, reject) => {
      const fail = () => reject(Object.assign(new Error("aborted"), { name: "AbortError" }))
      if (signal?.aborted) return fail()
      signal?.addEventListener("abort", fail, { once: true })
    })
    return { ok: true, status: 200, body: { getReader: () => ({ read }) } }
  }
  return ok({})
}

// --- fake OpenCode 2 ---------------------------------------------------------
const prompts = []
const evq = []
let evWake = null
const emitEvent = (type, data) => {
  evq.push({ type, data, location: { directory: project } })
  if (evWake) { const w = evWake; evWake = null; w() }
}
const ctx = {
  app: { name: "opencode", version: "2.1.3", channel: "latest" },
  location: { directory: project, project: { id: "p", directory: project } },
  options: {},
  tool: { transform: async (cb) => { cb({ add: () => {} }); return {} } },
  event: {
    subscribe: ({ signal } = {}) => ({
      [Symbol.asyncIterator]() {
        return {
          next: () => new Promise((resolve) => {
            const end = () => resolve({ done: true, value: undefined })
            if (signal?.aborted) return end()
            signal?.addEventListener("abort", end, { once: true })
            const pull = () => {
              if (evq.length) return resolve({ done: false, value: evq.shift() })
              evWake = pull
            }
            pull()
          }),
          return: async () => ({ done: true, value: undefined }),
        }
      },
    }),
  },
  session: {
    list: async () => ({ data: [{ id: "ses_r", time: { updated: 1 },
                                  location: { directory: project } }] }),
    prompt: async (p) => {
      prompts.push({ session: p.sessionID, text: p.text || "", resume: p.resume })
      return { id: "msg" }
    },
  },
}

const mod = await import(%(module)s)
const plugin = mod.default
const settle = () => new Promise((r) => setTimeout(r, 1500))
const MEMO = join(DATA, "opencode", "a2a-briefed.json")
const memo = () => JSON.parse(readFileSync(MEMO, "utf8"))
// Another process wrote the startup mark: what the next CLI launch finds.
const otherProcess = () => {
  const m = memo()
  m.started[KEY] = { pid: 1, started: 0 }
  writeFileSync(MEMO, JSON.stringify(m))
}

/** One instance's life, as V2 runs it: setup, a while, then cleanup. */
const life = async (during) => {
  prompts.length = 0; calls = []
  const cleanup = await plugin.setup(ctx)
  await settle()
  if (during) await during()
  const seen = { prompts: [...prompts], calls: [...calls] }
  await cleanup()
  return seen
}

const out = {}
// The file as the previous client version wrote it: a different shape.
mkdirSync(join(DATA, "opencode"), { recursive: true })
writeFileSync(MEMO, JSON.stringify({ ses_r: { key: KEY, name: KEY, boot: "x", at: Date.now() } }))

out.first = await life()                               // the CLI starts
out.memo = memo()
out.recycle = await life()                             // an hourly recycle
out.newsession = await life(async () => {              // a new session, same process
  emitEvent("session.created", { sessionID: "ses_new", title: "t",
                                 location: { directory: project } })
  await settle()
})
out.viewed = await life(async () => {                  // finished turns, seen
  emitEvent("session.viewed", { sessionID: "ses_r", idle: Date.now() })
  await settle()
  emitEvent("session.viewed", { sessionID: "ses_r", idle: Date.now() })
  await settle()
})
broker.name = "renamed-agent"                          // rename_me happened
out.renamed = await life()
otherProcess()                                         // the CLI starts again
out.restart = await life()

// --- OpenCode 1: server(), the CLI's own process --------------------------------
const v1prompts = []
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => [{ id: "ses_v1", directory: project, time: { updated: 1 } }],
    promptAsync: async (a) => {
      v1prompts.push({ text: a?.body?.parts?.[0]?.text || "", noReply: !!a?.body?.noReply })
      return {}
    },
    prompt: async () => new Promise(() => {}),
  },
}
otherProcess()
await plugin.server({ client, directory: project })
await settle()
out.v1_first = [...v1prompts]
v1prompts.length = 0
otherProcess()                                         // `opencode -c`
await plugin.server({ client, directory: project })
await settle()
out.v1_second = [...v1prompts]

out.all_calls = [...new Set(allCalls)]
out.config_memo = existsSync(join(BASE, "home", ".config", "opencode", "a2a-briefed.json"))
console.log("@@" + JSON.stringify(out))
process.exit(0)
"""


def kinds(prompts: list[dict]) -> dict:
    return {
        "hello": [p for p in prompts if p["text"].startswith("[a2a] connected as")],
        "brief": [p for p in prompts if "You are a2a agent" in p["text"]],
        "check": [p for p in prompts if "Check your channels" in p["text"]],
    }


def main() -> int:
    base = Path(tempfile.mkdtemp(prefix="a2a-resume-"))
    harness = base / "drive.mjs"
    harness.write_text(HARNESS % {"base": json.dumps(str(base)),
                                  "module": json.dumps(str(OPENCODE))})
    res = subprocess.run(["node", str(harness)], text=True,
                         capture_output=True, timeout=120)
    line = next((l for l in res.stdout.splitlines() if l.startswith("@@")), "")
    if not line:
        print("FAIL the harness produced nothing")
        print(res.stdout[-1500:])
        print(res.stderr[-1500:])
        return 1
    out = json.loads(line[2:])

    # --- the CLI starts --------------------------------------------------------------
    first = kinds(out["first"]["prompts"])
    check("the CLI starts: the hello, the instructions and the channel check, "
          "as before — a file in the old shape counts for nothing",
          len(first["hello"]) == 1 and len(first["brief"]) == 1
          and len(first["check"]) == 1, json.dumps(out["first"]["prompts"])[:400])
    check("the instructions start no turn and the check does",
          all(p["resume"] is False for p in first["brief"] + first["hello"])
          and all(p["resume"] is True for p in first["check"]),
          json.dumps([(p["text"][:30], p["resume"]) for p in out["first"]["prompts"]]))
    started = (out["memo"].get("started") or {}).get("resume-proj") or {}
    session = (out["memo"].get("sessions") or {}).get("ses_r") or {}
    check("and the process that had the startup brief is recorded, with the "
          "session it briefed",
          bool(started.get("pid")) and "started" in started
          and session.get("name") == "resume-proj", json.dumps(out["memo"])[:300])
    check("in the DATA directory — never config, which OpenCode 2 watches",
          not out["config_memo"])

    # --- not a start --------------------------------------------------------------------
    check("a recycle of the plugin in the same process says nothing at all",
          out["recycle"]["prompts"] == [], json.dumps(out["recycle"]["prompts"])[:300])
    ns = kinds(out["newsession"]["prompts"])
    check("a new session in the same process gets the instructions — and no "
          "check, no hello: that is not a start",
          len(ns["brief"]) == 1 and ns["brief"][0]["session"] == "ses_new"
          and not ns["check"] and not ns["hello"],
          json.dumps(out["newsession"]["prompts"])[:300])
    check("session.viewed — a read receipt for a finished turn — runs nothing",
          out["viewed"]["prompts"] == [], json.dumps(out["viewed"]["prompts"])[:300])
    rn = kinds(out["renamed"]["prompts"])
    check("a rename: the instructions under the new name, no check",
          len(rn["brief"]) == 1 and '"renamed-agent"' in rn["brief"][0]["text"]
          and not rn["check"], json.dumps(out["renamed"]["prompts"])[:300])

    # --- the CLI starts again -----------------------------------------------------------
    rs = kinds(out["restart"]["prompts"])
    check("the next CLI start — another process — checks again, hello included",
          len(rs["hello"]) == 1 and len(rs["brief"]) == 1 and len(rs["check"]) == 1,
          json.dumps(out["restart"]["prompts"])[:400])

    check("nothing reads the inbox behind the agent's back: no /pending, no "
          "/pending/peek", not any(c.startswith("/pending") for c in out["all_calls"]),
          str(out["all_calls"]))

    # --- OpenCode 1 ---------------------------------------------------------------------
    v1a, v1b = kinds(out["v1_first"]), kinds(out["v1_second"])
    check("OpenCode 1: a launch briefs and checks",
          len(v1a["brief"]) == 1 and len(v1a["check"]) == 1,
          json.dumps(out["v1_first"])[:300])
    check("and so does the next one, `opencode -c` included",
          len(v1b["brief"]) == 1 and len(v1b["check"]) == 1,
          json.dumps(out["v1_second"])[:300])

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — the channel check runs when the CLI starts, and at no other time")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
