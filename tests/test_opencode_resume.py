#!/usr/bin/env python3
"""OpenCode: instructions in the system prompt; the check at CLI start; no noise.

    python3 tests/test_opencode_resume.py

What a user saw, and what this pins:

  the sermon     the ~70-line instructions block was POSTED into the session —
                 at start, and whenever a session first stirred: the first
                 prompt of a new conversation (a plain `opencode` opens no
                 session until then), an old conversation reopened, a reload.
                 Now it is never posted: OpenCode 1's
                 "experimental.chat.system.transform" and OpenCode 2's session
                 "context" hook put it in the system prompt of each model
                 request, for this project's root sessions only.
  the check      with read_on_init on, the "check your channels" turn runs
                 once when the CLI starts — the first brief of this agent in
                 this process — and at no other time.
  the network    "[a2a] could not reach …: ENOTFOUND" was posted into the
                 session when the broker was unreachable at start, and the
                 client then gave up for good. Now it is logged and retried.

This runs the REAL plugin, through OpenCode 2's setup() and 1's server(),
against a fake broker, loading it again and again on one data directory, and
forges the process mark to stand for a new CLI process. No database; needs node.
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

const mkproject = (name, on) => {
  const d = join(BASE, name)
  mkdirSync(d, { recursive: true })
  writeFileSync(join(d, ".a2a.json"), JSON.stringify(on ? { enabled_opencode: true } : {}))
  return d
}
const project = mkproject("resume-proj", true)
const offProject = mkproject("off-proj", false)
const KEY = "resume-proj"

// --- fake broker -------------------------------------------------------------
const broker = { meFails: 0 }
let calls = []
const allCalls = []
const ok = (b) => ({ ok: true, status: 200,
  text: async () => JSON.stringify(b), json: async () => b })
globalThis.fetch = async (url, init = {}) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path); allCalls.push(path)
  if (path === "/me") {
    if (broker.meFails > 0) {
      broker.meFails--
      throw new TypeError("getaddrinfo ENOTFOUND broker.invalid")
    }
    return ok({ agent: KEY, registered: true, stations: ["s"] })
  }
  if (path === "/channels")
    return ok({ channels: [{ name: "advisory", members: [KEY] }] })
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
const emitEvent = (dir, type, data) => {
  evq.push({ type, data, location: { directory: dir } })
  if (evWake) { const w = evWake; evWake = null; w() }
}
// The project's ROOT sessions, newest first, as ctx.session.list returns
// them for {directory, parentID: null}.
const roots = ["ses_r"]
let hooks = {}
const mkctx = (dir) => ({
  app: { name: "opencode", version: "2.1.3", channel: "latest" },
  location: { directory: dir, project: { id: "p", directory: dir } },
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
    list: async (q) => ({ data: (q?.directory === project ? roots : []).map((id, i) =>
      ({ id, time: { updated: 100 - i }, location: { directory: q.directory } })) }),
    prompt: async (p) => {
      prompts.push({ session: p.sessionID, text: p.text || "", resume: p.resume })
      return { id: "msg" }
    },
    hook: async (name, cb) => {
      hooks[name] = cb
      return { dispose: async () => { delete hooks[name] } }
    },
  },
})

const mod = await import(%(module)s)
const plugin = mod.default
const settle = (ms = 1500) => new Promise((r) => setTimeout(r, ms))
const MEMO = join(DATA, "opencode", "a2a-briefed.json")
const otherProcess = () => {
  let m = {}
  try { m = JSON.parse(readFileSync(MEMO, "utf8")) } catch {}
  m.started = { ...(m.started || {}), [KEY]: { pid: 1, started: 0 } }
  mkdirSync(join(DATA, "opencode"), { recursive: true })
  writeFileSync(MEMO, JSON.stringify(m))
}
/** What the V2 context hook puts in one request's system prompt. */
const systemOf = async (sessionID) => {
  const input = { sessionID, system: [] }
  if (hooks.context) await hooks.context(input)
  return input.system.map((p) => p.text)
}

/** One instance's life, as V2 runs it: setup, a while, then cleanup. */
const life = async (during, { dir = project, wait } = {}) => {
  prompts.length = 0; calls = []
  const cleanup = await plugin.setup(mkctx(dir))
  await settle(wait)
  const extra = during ? await during() : undefined
  const seen = { prompts: [...prompts], calls: [...calls], extra }
  await cleanup()
  return seen
}

const out = {}
// The file as the previous client version wrote it: a different shape.
mkdirSync(join(DATA, "opencode"), { recursive: true })
writeFileSync(MEMO, JSON.stringify({ ses_r: { key: KEY, name: KEY, boot: "x", at: 1 } }))

out.first = await life(async () => ({
  root: await systemOf("ses_r"),
  // A subagent the agent started: its session object says so.
  sub: await (async () => {
    emitEvent(project, "session.created", { sessionID: "ses_sub", parentID: "ses_r",
      title: "sub", location: { directory: project } })
    await settle(300)
    return systemOf("ses_sub")
  })(),
  // A session this project's list does not hold — another project's.
  other: await systemOf("ses_elsewhere"),
}))
out.recycle = await life()
out.newsession = await life(async () => {
  roots.unshift("ses_new")              // the first prompt created it
  emitEvent(project, "session.created", { sessionID: "ses_new", title: "t",
                                          location: { directory: project } })
  await settle()
  return { system: await systemOf("ses_new") }
})
out.viewed = await life(async () => {
  emitEvent(project, "session.viewed", { sessionID: "ses_r", idle: Date.now() })
  await settle()
})
out.off = await life(async () => ({ system: await systemOf("ses_r") }), { dir: offProject })

// A new CLI process, started with the broker unreachable: twice ENOTFOUND,
// then it answers.
otherProcess()
broker.meFails = 2
out.offline = await life(null, { wait: 12500 })
out.hook_disposed = !hooks.context

// --- OpenCode 1: server(), the CLI's own process --------------------------------
const v1prompts = []
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => [
      { id: "ses_v1", directory: project, time: { updated: 1 } },
      { id: "ses_v1sub", parentID: "ses_v1", directory: project, time: { updated: 2 } },
    ],
    promptAsync: async (a) => {
      v1prompts.push({ text: a?.body?.parts?.[0]?.text || "", noReply: !!a?.body?.noReply })
      return {}
    },
    prompt: async () => new Promise(() => {}),
  },
}
otherProcess()
const v1 = await plugin.server({ client, directory: project })
await settle()
out.v1_prompts = [...v1prompts]
const v1sys = async (sessionID) => {
  const output = { system: [] }
  await v1["experimental.chat.system.transform"]?.({ sessionID, model: {} }, output)
  return output.system
}
out.v1_root = await v1sys("ses_v1")
out.v1_sub = await v1sys("ses_v1sub")

out.all_calls = [...new Set(allCalls)]
console.log("@@" + JSON.stringify(out))
process.exit(0)
"""


def kinds(prompts: list[dict]) -> dict:
    return {
        "hello": [p for p in prompts if p["text"].startswith("[a2a] connected as")],
        "brief": [p for p in prompts if "You are a2a agent" in p["text"]],
        "check": [p for p in prompts if "Check your channels" in p["text"]],
    }


def has_brief(system: list) -> bool:
    return any("You are a2a agent" in s for s in system or [])


def main() -> int:
    base = Path(tempfile.mkdtemp(prefix="a2a-resume-"))
    harness = base / "drive.mjs"
    harness.write_text(HARNESS % {"base": json.dumps(str(base)),
                                  "module": json.dumps(str(OPENCODE))})
    res = subprocess.run(["node", str(harness)], text=True,
                         capture_output=True, timeout=180)
    line = next((l for l in res.stdout.splitlines() if l.startswith("@@")), "")
    if not line:
        print("FAIL the harness produced nothing")
        print(res.stdout[-1500:])
        print(res.stderr[-1500:])
        return 1
    out = json.loads(line[2:])

    # --- the CLI starts ----------------------------------------------------------
    first = kinds(out["first"]["prompts"])
    check("the CLI starts: the hello and the channel check — and NO "
          "instructions block posted into the conversation",
          len(first["hello"]) == 1 and len(first["check"]) == 1
          and not first["brief"], json.dumps(out["first"]["prompts"])[:400])
    check("the check starts a turn; the hello does not",
          all(p["resume"] is True for p in first["check"])
          and all(p["resume"] is False for p in first["hello"]),
          json.dumps([(p["text"][:30], p["resume"]) for p in out["first"]["prompts"]]))
    ex = out["first"]["extra"]
    check("the instructions are in the SYSTEM PROMPT of the project's session "
          "(OpenCode 2's context hook)", has_brief(ex["root"]), str(ex["root"])[:200])
    check("but not in a subagent's session…", not ex["sub"], str(ex["sub"])[:200])
    check("…nor in a session that is not this project's", not ex["other"],
          str(ex["other"])[:200])

    # --- nothing else posts anything --------------------------------------------
    check("a recycle of the plugin in the same process posts nothing",
          out["recycle"]["prompts"] == [], json.dumps(out["recycle"]["prompts"])[:300])
    ns = out["newsession"]
    check("a new conversation — created by your first prompt — gets nothing "
          "posted while you type…", ns["prompts"] == [], json.dumps(ns["prompts"])[:300])
    check("…and its system prompt carries the instructions",
          has_brief(ns["extra"]["system"]), str(ns["extra"]["system"])[:200])
    check("session.viewed — a read receipt for a finished turn — posts nothing",
          out["viewed"]["prompts"] == [], json.dumps(out["viewed"]["prompts"])[:300])
    check("a project with a2a off: nothing posted, nothing in the system prompt",
          out["off"]["prompts"] == [] and not out["off"]["extra"]["system"],
          json.dumps(out["off"])[:300])

    # --- the broker unreachable at start ----------------------------------------
    off = out["offline"]
    texts = [p["text"] for p in off["prompts"]]
    check("a broker that cannot be reached at start is NEVER posted into the "
          "session", not any("could not reach" in t or "ENOTFOUND" in t for t in texts),
          json.dumps(texts)[:300])
    check("it is retried, and the client connects once it answers — then the "
          "startup check runs",
          off["calls"].count("/me") >= 3
          and any(c.startswith("/stream") for c in off["calls"])
          and len(kinds(off["prompts"])["check"]) == 1,
          f"{off['calls']} {json.dumps(texts)[:200]}")
    check("the context hook is disposed when the plugin unloads",
          out["hook_disposed"])

    check("nothing reads the inbox behind the agent's back: no /pending, no "
          "/pending/peek", not any(c.startswith("/pending") for c in out["all_calls"]),
          str(out["all_calls"]))

    # --- OpenCode 1 ---------------------------------------------------------------
    v1 = kinds([dict(p, resume=not p["noReply"]) for p in out["v1_prompts"]])
    check("OpenCode 1: a launch posts the hello and the check, and no "
          "instructions block",
          len(v1["hello"]) == 1 and len(v1["check"]) == 1 and not v1["brief"],
          json.dumps(out["v1_prompts"])[:300])
    check("and experimental.chat.system.transform puts the instructions in the "
          "system prompt of the project's root session — not a subagent's",
          has_brief(out["v1_root"]) and not out["v1_sub"],
          f"{str(out['v1_root'])[:120]} / {out['v1_sub']}")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — instructions in the system prompt, the check at CLI start, no noise")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
