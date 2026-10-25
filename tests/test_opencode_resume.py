#!/usr/bin/env python3
"""OpenCode: a reload costs nothing, and a resume misses nothing.

    python3 tests/test_opencode_resume.py

Two things a real log showed:

  every hour   OpenCode 2 recycles a project's plugin instance about hourly,
               and each new instance briefed the same session again — the
               hello, the instructions, and a check prompt that STARTS A TURN.
               Eleven times in one night, with nothing to say. The brief is
               now remembered on disk, per session, across instances.

  on resume    the stream re-sends what it already handed out only within the
               broker's replay window (ten minutes). Anything older reached the
               session only if the model thought to call my_pending. Every
               connection now first reads /pending/peek — read-only, never the
               acking /pending — and delivers it exactly as a stream line.

This runs the REAL plugin, through OpenCode 2's setup() and 1's server(),
against a fake broker, loading it again and again on one data directory.
No database; needs node.
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
// The hello is ON here: it is one of the three things a reload repeated.
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
const broker = { name: KEY, peek404: false, peek: [], stream: [] }
let calls = []
let acked = []
const ok = (b) => ({ ok: true, status: 200,
  text: async () => JSON.stringify(b), json: async () => b })
globalThis.fetch = async (url, init = {}) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path)
  if (path === "/me") return ok({ agent: broker.name, registered: true, stations: ["s"] })
  if (path === "/channels")
    return ok({ channels: [{ name: "advisory", members: [KEY, broker.name] }] })
  if (path.startsWith("/pending/peek")) {
    if (broker.peek404) return { ok: false, status: 404,
      text: async () => "Not Found", json: async () => ({}) }
    return ok({ agent_id: broker.name, has_more: false, messages: broker.peek })
  }
  if (path === "/ack") {
    acked.push(...(JSON.parse(init.body || "{}").ids || []))
    return ok({ ok: true })
  }
  if (path.startsWith("/stream")) {
    // What the broker's replay sends on connect, then nothing until the
    // connection is closed — when the read rejects, as a real one does.
    const lines = broker.stream.map((m) => JSON.stringify(m) + "\n")
    const signal = init.signal
    const read = () => {
      if (lines.length)
        return Promise.resolve({ done: false,
                                 value: new TextEncoder().encode(lines.shift()) })
      return new Promise((_, reject) => {
        const fail = () => reject(Object.assign(new Error("aborted"),
                                                { name: "AbortError" }))
        if (signal?.aborted) return fail()
        signal?.addEventListener("abort", fail, { once: true })
      })
    }
    return { ok: true, status: 200, body: { getReader: () => ({ read }) } }
  }
  return ok({})
}

// --- fake OpenCode 2 ---------------------------------------------------------
const prompts = []
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

/** One instance's life, as V2 runs it: setup, a while, then cleanup. */
const life = async () => {
  prompts.length = 0; calls = []; acked = []
  const cleanup = await plugin.setup(ctx)
  await settle()
  const seen = { prompts: [...prompts], calls: [...calls], acked: [...acked] }
  await cleanup()
  return seen
}

const out = {}
out.first = await life()
out.memo_first = memo()[`ses_r`] || null
out.second = await life()                      // the hourly recycle

const m = memo()                               // a day and an hour later
m.ses_r.at -= 25 * 3600 * 1000
writeFileSync(MEMO, JSON.stringify(m))
out.stale = await life()
out.memo_refreshed = Date.now() - memo().ses_r.at < 60_000

broker.name = "renamed-agent"                  // rename_me happened
out.renamed = await life()

const msg = (id, text) => ({ id, kind: "channel", channel: "advisory",
  sender: "peer", text, ts: 1, audience: [KEY], addressed: [] })
broker.peek = [msg("m-aged", "aged-text-only-peek"), msg("m-both", "both-text")]
broker.stream = [msg("m-both", "both-text")]
out.catchup = await life()

broker.peek = []; broker.stream = []; broker.peek404 = true
out.nopeek = await life()

// --- OpenCode 1: server(), a new process per launch ---------------------------
broker.peek404 = false
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
await plugin.server({ client, directory: project })
await settle()
out.v1_first = [...v1prompts]
v1prompts.length = 0
await plugin.server({ client, directory: project })     // `opencode -c`
await settle()
out.v1_second = [...v1prompts]

out.config_memo = existsSync(join(BASE, "home", ".config", "opencode", "a2a-briefed.json"))
console.log("@@" + JSON.stringify(out))
process.exit(0)
"""


def kinds(prompts: list[dict]) -> dict:
    """What a list of prompts said, by what each one is."""
    def is_(p, needle):
        return needle in p["text"]
    return {
        "hello": [p for p in prompts if p["text"].startswith("[a2a] connected as")],
        "brief": [p for p in prompts if is_(p, "You are a2a agent")],
        "check": [p for p in prompts if is_(p, "Check your channels")],
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

    # --- the first instance briefs, as always --------------------------------
    first = kinds(out["first"]["prompts"])
    check("a new session gets the hello, the instructions and the channel "
          "check, as before",
          len(first["hello"]) == 1 and len(first["brief"]) == 1
          and len(first["check"]) == 1, json.dumps(out["first"]["prompts"])[:400])
    check("the instructions start no turn and the check does",
          all(p["resume"] is False for p in first["brief"] + first["hello"])
          and all(p["resume"] is True for p in first["check"]),
          json.dumps([(p["text"][:30], p["resume"])
                      for p in out["first"]["prompts"]]))
    memo = out["memo_first"] or {}
    check("and the brief is remembered on disk, as this agent's, for this "
          "session", memo.get("key") == "resume-proj"
          and memo.get("name") == "resume-proj", str(memo))
    check("in the DATA directory — never config, which OpenCode 2 watches",
          not out["config_memo"])

    # --- the hourly recycle ----------------------------------------------------
    check("the NEXT instance, on the same session — OpenCode 2's hourly "
          "recycle — says nothing at all: no hello, no instructions, no turn",
          out["second"]["prompts"] == [],
          json.dumps(out["second"]["prompts"])[:400])

    # --- a day later -------------------------------------------------------------
    stale = kinds(out["stale"]["prompts"])
    check("a day later the instructions go in once more, so a compacted "
          "session does not stay without them…",
          len(stale["brief"]) == 1, json.dumps(out["stale"]["prompts"])[:400])
    check("…without a check and without a turn",
          not stale["check"]
          and all(p["resume"] is False for p in out["stale"]["prompts"]),
          json.dumps([(p["text"][:30], p["resume"])
                      for p in out["stale"]["prompts"]]))
    check("and the memory is refreshed by it", out["memo_refreshed"])

    # --- a rename ----------------------------------------------------------------
    renamed = kinds(out["renamed"]["prompts"])
    check("after a rename the session is briefed again, under the new name — "
          "the model must not keep signing as the old one",
          len(renamed["brief"]) == 1
          and '"renamed-agent"' in renamed["brief"][0]["text"]
          and len(renamed["check"]) == 1,
          json.dumps(out["renamed"]["prompts"])[:400])

    # --- the catch-up --------------------------------------------------------------
    cu = out["catchup"]
    texts = [p["text"] for p in cu["prompts"]]
    check("a message only /pending/peek still holds — the stream would never "
          "offer it again — is delivered on connect",
          sum("aged-text-only-peek" in t for t in texts) == 1, json.dumps(texts)[:400])
    check("one that the peek AND the stream's replay both return is delivered "
          "once", sum("both-text" in t for t in texts) == 1, json.dumps(texts)[:400])
    check("both are acked once delivered, like any streamed message",
          {"m-aged", "m-both"} <= set(cu["acked"]), str(cu["acked"]))
    peek_at = next((i for i, c in enumerate(cu["calls"])
                    if c.startswith("/pending/peek")), -1)
    stream_at = next((i for i, c in enumerate(cu["calls"])
                      if c.startswith("/stream")), -1)
    check("the peek comes before the stream", 0 <= peek_at < stream_at,
          str(cu["calls"]))
    every = [c for k in ("first", "second", "stale", "renamed", "catchup", "nopeek")
             for c in out[k]["calls"]]
    check("and the acking /pending is never called — reading it would retire "
          "messages with nothing on screen",
          not any(c == "/pending" or c.startswith("/pending?") for c in every),
          str(sorted(set(every))))
    check("a session already briefed hears only the messages — no brief rides "
          "along with them", not kinds(cu["prompts"])["brief"]
          and not kinds(cu["prompts"])["hello"], json.dumps(texts)[:400])

    # --- an older broker ------------------------------------------------------------
    np = out["nopeek"]
    check("a broker without /pending/peek (404) still gets the stream, and "
          "nothing is said about it in the session",
          any(c.startswith("/stream") for c in np["calls"]) and np["prompts"] == [],
          f"{np['calls']} {json.dumps(np['prompts'])[:200]}")

    # --- OpenCode 1 ------------------------------------------------------------------
    v1 = [p for p in out["v1_first"] if "You are a2a agent" in p["text"]]
    check("OpenCode 1: the first launch briefs its session", len(v1) == 1,
          json.dumps(out["v1_first"])[:300])
    check("and the next launch on that session — a resume — does not brief it "
          "again", not any("You are a2a agent" in p["text"]
                           or "Check your channels" in p["text"]
                           for p in out["v1_second"]),
          json.dumps(out["v1_second"])[:300])

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — a reload costs nothing, and a resume misses nothing")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
