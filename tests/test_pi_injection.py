#!/usr/bin/env python3
"""Pi messages must reach the session, once, without racing the human.

    python3 tests/test_pi_injection.py

The Pi session printed this on almost every burst:

    Extension "<runtime>" error: Agent is already processing a prompt.
    Use steer() or followUp() to queue messages, or wait for completion.

`pi.sendUserMessage` as handed to extensions is fire-and-forget — it returns
void and routes rejections to Pi's error channel, which is where "<runtime>"
came from. So awaiting it proved nothing, and the delivery loop drained its
whole queue into concurrent `AgentSession.prompt()` calls. Whichever lost the
race threw, and that message was gone: silently, with the catch around it dead
code that could never run.

Both Pi bugs so far survived because this client had only source greps. It does
not have to. Node runs `index.ts` directly, so the REAL extension is loaded
here, driven through its REAL delivery path — a fake broker whose /stream sends
two messages in one chunk, which is exactly what the broker does when it
replays a backlog after a reconnect.

The stub reproduces Pi's semantics rather than a convenient version of them:
sendUserMessage is void and swallows its error, and starting a turn while one
is running throws the genuine agent.js:228 string.

Needs node 22+ (TypeScript type stripping). No broker, no database.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
PI = PLUGIN / "pi" / "index.ts"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


HARNESS = r'''
import http from "node:http"

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))

const events = {}
const runtimeErrors = []
const turns = []          // one entry per turn actually STARTED
const queued = []         // handed over while a run was active
const context = []        // appended with no turn
let running = false

function startRun(text) {
  if (running) {
    // The real message, from pi-agent-core/dist/agent.js:228.
    throw new Error("Agent is already processing a prompt. Use steer() or " +
                    "followUp() to queue messages, or wait for completion.")
  }
  running = true            // set synchronously, exactly as Pi does
  turns.push(text)
}

const pi = {
  on: (e, h) => { (events[e] ||= []).push(h) },
  registerTool: () => {},
  ui: { notify: () => {} },

  // Fire and forget, error swallowed: the binding Pi actually hands out. A
  // regression back to this looks like a DROPPED MESSAGE, not an exception —
  // which is why the original bug was invisible.
  //
  // THE ASYNC GAP IS THE BUG, so it is modelled rather than smoothed over.
  // Pi checks whether it is streaming at the TOP of prompt(), then does a long
  // async preamble — emitInput, template expansion, checkAuth, the compaction
  // check, emitBeforeAgentStart — and only then marks itself busy. Two calls
  // in one tick therefore BOTH see an idle session, and the loser reaches the
  // agent with a run already active.
  sendUserMessage: (content, opts) => {
    const wasStreaming = running          // decided at the check...
    ;(async () => {
      await sleep(0)                      // ...then the preamble...
      try {                               // ...and only now the prompt.
        if (wasStreaming && opts && opts.deliverAs) {
          queued.push(String(content)); return
        }
        startRun(String(content))
      } catch (e) { runtimeErrors.push(String(e.message || e)) }
    })()
  },

  sendMessage: (msg, opts) => {
    try {
      const text = String(msg.content)
      if (opts && opts.deliverAs === "nextTurn") { context.push(text); return }
      if (running) { queued.push(text); return }        // plain enqueue
      if (opts && opts.triggerTurn) { startRun(text); return }
      context.push(text)                                 // idle, no turn
    } catch (e) { runtimeErrors.push(String(e.message || e)) }
  },
}

const emit = async (name, ev) => {
  for (const h of events[name] || []) await h(ev || { type: name }, {})
}

// --- a fake broker: /me, /channels, and a /stream that sends BOTH messages
// in one chunk, the way a replay after reconnect arrives.
const server = http.createServer((req, res) => {
  if (req.url.startsWith("/stream")) {
    res.writeHead(200, { "Content-Type": "text/plain" })
    if (HUMAN_FIRST) {
      // The human's Enter is already in flight when the message lands.
      emit("input", { type: "input", text: "hi", source: "interactive" })
        .then(() => {
          res.write(JSON.stringify({ id: "m1", channel: "ops",
                                     sender: "peer", text: "one" }) + "\n")
        })
    } else {
      res.write(JSON.stringify({ id: "m1", channel: "ops",
                                 sender: "peer", text: "one" }) + "\n" +
                JSON.stringify({ id: "m2", channel: "ops",
                                 sender: "peer", text: "two" }) + "\n" +
                JSON.stringify({ id: "m3", channel: "ops",
                                 sender: "peer", text: "three" }) + "\n")
    }
    return                                    // held open, like the real one
  }
  const body = req.url.startsWith("/channels")
    ? { channels: [] }
    : { agent: "probe", registered: true, stations: ["s"], ok: true }
  res.writeHead(200, { "Content-Type": "application/json" })
  res.end(JSON.stringify(body))
})
await new Promise((r) => server.listen(0, "127.0.0.1", r))
process.env.A2A_URL = `http://127.0.0.1:${server.address().port}`

const mod = await import("./index.ts")
mod.default(pi)
await emit("session_start")
await sleep(1500)

SCENARIO

console.log("@@" + JSON.stringify({ turns, queued, context, runtimeErrors }))
process.exit(0)
'''


def run(tmp: Path, name: str, scenario: str = "", human_first: bool = False) -> dict:
    work = tmp / name
    (work / "node_modules" / "typebox").mkdir(parents=True, exist_ok=True)
    (work / "node_modules" / "typebox" / "package.json").write_text(
        json.dumps({"name": "typebox", "version": "0.0.0", "type": "module",
                    "main": "index.js"}))
    (work / "node_modules" / "typebox" / "index.js").write_text(
        "export const Type = new Proxy({}, "
        "{ get: () => (...a) => ({ a }) });\n")
    shutil.copy(PI, work / "index.ts")
    (work / "run.mjs").write_text(
        HARNESS.replace("HUMAN_FIRST", "true" if human_first else "false")
               .replace("SCENARIO", scenario))

    env = dict(os.environ, A2A_TOKEN="tok", A2A_AGENT="probe",
               A2A_HELLO="0", A2A_READ_ON_INIT="0", HOME=str(work))
    proc = subprocess.run(["node", "run.mjs"], cwd=work, env=env,
                          capture_output=True, text=True, timeout=90)
    line = next((l for l in proc.stdout.splitlines()
                 if l.startswith("@@")), "")
    if not line:
        print(proc.stdout[-400:])
        print(proc.stderr[-800:])
        return {}
    return json.loads(line[2:])


def main() -> int:
    if not shutil.which("node"):
        print("SKIP: node is not installed", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="a2a-pi-"))

    # --- the reported bug, through the client's own delivery path ----------
    out = run(tmp, "burst")
    check("the real extension loads and delivers from the stream",
          bool(out) and (out.get("turns") or out.get("queued")),
          str(out)[:300])
    check("three messages in one chunk produce ONE turn, the rest queued — "
          "not three concurrent prompts",
          len(out.get("turns", [])) == 1 and len(out.get("queued", [])) == 2,
          str(out)[:400])
    check("and NO <runtime> error is produced, which is the whole complaint",
          out.get("runtimeErrors") == [],
          str(out.get("runtimeErrors"))[:300])
    check("every message reached the session — none dropped in the race",
          all(w in json.dumps(out) for w in ("one", "two", "three")),
          str(out)[:400])

    # --- the brief arrives without spending a turn on it -------------------
    check("the brief is in the session before anything is answered, and "
          "costs no model call",
          any("a2a agent" in c for c in out.get("context", []))
          or any("a2a agent" in t for t in out.get("turns", [])),
          str(out.get("context"))[:200])

    # --- the human's prompt must never be the one that throws --------------
    out = run(tmp, "human", human_first=True)
    check("a message arriving while the human's prompt is in flight is added "
          "as context instead of starting a turn underneath them",
          any("one" in c for c in out.get("context", [])),
          str(out)[:400])
    check("so nothing throws in that window either",
          out.get("runtimeErrors") == [],
          str(out.get("runtimeErrors"))[:300])

    # --- the stub is honest: the OLD call still fails ----------------------
    # If this passes trivially the suite proves nothing, so assert that the
    # abandoned path really does produce the production error.
    out = run(tmp, "olderror", scenario='''
      // From IDLE, which is the state the old flush() waited for before
      // draining: it required `settled` before sending anything.
      running = false
      // Two in one tick, exactly as that loop did it.
      pi.sendUserMessage("a", { deliverAs: "followUp" })
      pi.sendUserMessage("b", { deliverAs: "followUp" })
      await sleep(50)
    ''')
    check("the stub reproduces the original failure for sendUserMessage, so "
          "this suite would have caught the bug it was written for",
          any("already processing" in e for e in out.get("runtimeErrors", [])),
          str(out)[:300])

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — one turn per burst, and never under the human's feet")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
