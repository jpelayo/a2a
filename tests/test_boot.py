#!/usr/bin/env python3
"""A REGISTERED OpenCode client must read its channels at boot, on its own.

Registered is the precondition, not a detail. An id the broker does not know
has no channels and no receipts, and a client claiming one is a setup problem
for its operator — it must stay quiet rather than fill a session with traffic
it has no standing to consume.

    python3 tests/test_boot.py

For 23 consecutive sessions this client was handed 40 messages by the broker
and showed exactly one. Two causes, both asserted here:

  the deadlock — `session.prompt()` does not resolve until the model has
                 finished replying. `drain()` awaited it, so at boot, with no
                 turn running, it never returned: `injecting` latched true and
                 the queue was never drained again for the life of the process.
                 The SDK's `promptAsync` is the same call that returns on
                 accept ("start if needed and return immediately", 204).

  the trigger  — the brief ran only on `session.idle` (which fires AFTER a turn
                 completes) or `session.created` (which never fires on a
                 resume). Between them they cover none of boot or resume, so a
                 resumed session was never briefed and never caught up.

So the test feeds NO events at all. Everything asserted below has to happen
because the plugin decided to do it.

Hermetic: a fake broker inside the harness, no network, no real OpenCode.
Needs node.
"""
import json
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
AGENT = "boot-tester"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


HARNESS = """
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "dev"
process.env.A2A_AGENT = %(agent)s
process.env.A2A_HELLO = "0"
// The watchdog, shortened so the test does not sit through the real one.
process.env.A2A_PROMPT_TIMEOUT_MS = "700"

const calls = []           // every broker path the plugin asked for, in order
const prompts = []         // { method, text, at }
let sessionExists = false  // flipped on partway through
let hangNext = true        // the first promptAsync never settles

// --- fake broker ----------------------------------------------------------
globalThis.fetch = async (url, opts = {}) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path)
  const ok = (body) => ({
    ok: true, status: 200,
    text: async () => JSON.stringify(body),
    json: async () => body,
  })
  if (path === "/me")
    return ok({ agent: %(agent)s, registered: %(registered)s, stations: ["s"] })
  if (path === "/channels")
    return ok({ channels: [{ name: "advisory",
                             members: %(members)s }] })
  // Served, but nothing should ask for it: fetching transcripts is the
  // agent's job now, through its own tools, where it can be seen.
  if (path.startsWith("/channels/")) {
    return ok({ transcript: [
      { id: "m1", sender: "someone", text: "history from before boot" },
    ] })
  }
  if (path === "/ack") return ok({ ok: true })
  if (path.startsWith("/stream")) {
    // One message on connect, then the connection stays open. It arrives
    // BEFORE any session exists, which is the ordering that used to strand
    // it: push() calls drain(), drain() finds nowhere to inject, and nothing
    // ever came back for it.
    let sent = false
    return {
      ok: true, status: 200,
      body: { getReader: () => ({ read: async () => {
        if (sent) return new Promise(() => {})
        sent = true
        return { done: false, value: new TextEncoder().encode(
          JSON.stringify({ id: "m2", channel: "advisory", sender: "peer",
                           text: "posted while nobody was home" }) + "\\n") }
      } }) },
    }
  }
  return ok({})
}

// --- fake OpenCode --------------------------------------------------------
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => (sessionExists
      ? [{ id: "ses_boot", directory: "/tmp/proj", time: { updated: 1 } }]
      : []),
    prompt: async (a) => {
      prompts.push({ method: "prompt", text: a?.body?.parts?.[0]?.text || "" })
      return {}
    },
    promptAsync: async (a) => {
      prompts.push({ method: "promptAsync",
                     text: a?.body?.parts?.[0]?.text || "",
                     // noReply is the difference between context and an
                     // action: a prompt that sets it runs no turn, so
                     // nothing appears on screen.
                     noReply: !!a?.body?.noReply })
      if (hangNext) { hangNext = false; await new Promise(() => {}) }
      return {}
    },
  },
}

const mod = await import(%(module)s)
await mod.A2A({ client, directory: "/tmp/proj" })

const sleep = (ms) => new Promise((r) => setTimeout(r, ms))
await sleep(1200)
// Nothing may have been read yet: there is no session to read INTO, and the
// broker acks whatever it hands back.
const beforeSession = [...calls]

sessionExists = true
await sleep(4000)   // the retry loop is 2s; allow one full turn of it

console.log("@@" + JSON.stringify({
  beforeSession, calls, prompts,
}))
process.exit(0)
"""


def run(tmp: Path, registered: bool = True, member: bool = True) -> dict:
    harness = tmp / f"boot-{registered}-{member}.mjs"
    harness.write_text(HARNESS % {
        "agent": json.dumps(AGENT),
        "module": json.dumps(str(OPENCODE)),
        "registered": "true" if registered else "false",
        "members": json.dumps([AGENT] if member else ["someone-else"]),
    })
    proc = subprocess.run(["node", str(harness)], text=True,
                          capture_output=True, timeout=60)
    line = next((l for l in proc.stdout.splitlines() if l.startswith("@@")), "")
    if not line:
        print("FAIL harness produced nothing")
        print(proc.stdout[-800:])
        print(proc.stderr[-800:])
        return {}
    return json.loads(line[2:])


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-boot-"))
    out = run(tmp, registered=True)
    if not out:
        return 1
    calls, prompts = out["calls"], out["prompts"]
    before = out["beforeSession"]
    texts = [p["text"] for p in prompts]
    methods = {p["method"] for p in prompts}

    # --- it speaks with no events at all -----------------------------------
    check("the brief is issued with ZERO events delivered — the client briefs "
          "itself instead of waiting for session.idle, which only fires after "
          "a turn, or session.created, which never fires on a resume",
          any("a2a agent" in t for t in texts), json.dumps(texts)[:300])

    # --- and only through the call that returns ----------------------------
    check("only promptAsync is used — session.prompt does not return until the "
          "model has finished replying, which at boot is never",
          methods == {"promptAsync"}, str(methods))

    # --- the watchdog: one hung call must not wedge the queue --------------
    check("a prompt that never settles does not stop the next one — the queue "
          "latching shut is what silenced 40 messages",
          len(prompts) >= 2,
          f"only {len(prompts)} prompt(s): {json.dumps(texts)[:300]}")
    check("a message that arrived before any session existed is still "
          "delivered once one does — it used to sit in the queue with nothing "
          "left to come back for it",
          any("posted while nobody was home" in t for t in texts),
          json.dumps(texts)[:400])

    # --- the check is VISIBLE, which is the whole point --------------------
    checks = [p for p in prompts if "Check your channels" in p["text"]]
    check("the agent is asked to check its channels at boot",
          bool(checks), json.dumps(texts)[:400])
    check("and that ask STARTS A TURN — noReply would make it context the "
          "model meets on your next message, so the boot check would happen "
          "with nothing on screen and look exactly like never checking",
          bool(checks) and not checks[0]["noReply"],
          f"noReply={checks[0]['noReply'] if checks else '?'}")
    check("it names the tools, so the visible action is my_pending and "
          "read_channel rather than a paraphrase",
          bool(checks) and "my_pending" in checks[0]["text"]
          and "read_channel" in checks[0]["text"],
          json.dumps(checks[:1])[:300])
    check("the instructions still arrive silently — reference material is not "
          "worth a model call",
          any(p["noReply"] and "a2a agent" in p["text"] for p in prompts),
          json.dumps([p["noReply"] for p in prompts]))

    # --- and the client reads no messages itself ---------------------------
    check("the PLUGIN never fetches channel messages — reading is receiving, "
          "so a client-side read acks with nothing on screen to attribute it "
          "to. The only read is the agent's own tool call",
          not any("/messages" in c for c in calls),
          f"plugin read messages itself: {[c for c in calls if '/messages' in c]}")
    check("listing channels is allowed, because that route acks nothing and "
          "is how it knows whether there is anywhere to look",
          "/channels" in calls, str(calls))
    check("nothing is read before a session exists",
          not any(c.startswith("/channels") for c in before),
          f"looked too early: {before}")

    # --- an agent with no rooms costs nothing ------------------------------
    alone = run(tmp, member=False)
    check("an agent that belongs to no channel is not asked to check them — "
          "otherwise every boot of every project spends a model turn "
          "discovering an empty room",
          bool(alone) and not any("Check your channels" in p["text"]
                                  for p in alone.get("prompts", [])),
          json.dumps([p["text"][:60] for p in alone.get("prompts", [])]))

    # --- and none of it happens for an agent the broker does not know ------
    unreg = run(tmp, registered=False)
    unreg_texts = [p["text"] for p in unreg.get("prompts", [])]
    check("an UNREGISTERED agent reads no channel and is never briefed — "
          "being configured is the precondition for all of the above",
          bool(unreg)
          and not any(c.startswith("/channels") for c in unreg.get("calls", []))
          and not any("You are a2a agent" in t for t in unreg_texts),
          f"calls={unreg.get('calls')} texts={json.dumps(unreg_texts)[:300]}")
    check("but its human is still told how to register it — going silent "
          "would leave the operator with nothing to act on",
          any("not a registered agent" in t for t in unreg_texts),
          json.dumps(unreg_texts)[:300])

    # --- the source itself, so a rewrite cannot quietly regress ------------
    src = OPENCODE.read_text()
    check("no bare session.prompt( call survives in the client",
          not re.search(r"client\.session\.prompt\(", src),
          "session.prompt( is back")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — the client reads its channels and delivers on its own")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
