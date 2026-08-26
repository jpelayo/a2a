#!/usr/bin/env python3
"""A subagent is not an a2a client. Brief the root session, once.

    python3 tests/test_subagent_isolation.py

This is the regression test for the fault that made OpenCode unusable the
moment an agent spawned subagents. Three lines conspired:

  * the event handler adopted ANY session id it saw as the injection target,
  * and cleared the brief memo every time it did,
  * and `message.updated` — which fires for every streamed part of every
    reply — counted as a reason to brief.

So a parent and its subagents took turns owning the injection point, each
switch re-arming the ~50-line instructions block, and because the brief also
starts a turn, the turn it started produced more events. It did not converge:
the context filled and the terminal never came back.

What this pins:

  one brief        per session id, for the life of the process. The memo is a
                   Map now; nothing may clear it wholesale.
  roots only       a session with parentID is a tool call the agent made. It
                   never gets the brief and never gets a message — messages
                   injected there were acked where no human could see them.
  no storm         a thousand message.updated events cost zero injections.
  newest ≠ target  the subagent IS the most recently updated session while it
                   runs, which is exactly why "newest in this directory" was
                   the wrong question.

Needs node. No broker and no database: the fake client records what the plugin
tried to say, which is the only thing worth asserting here.
"""
import json
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"

ROOT = "ses_root_aaa"
CHILD = "ses_child_bbb"
AGENT = "sub-test"

failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'ok  ' if ok else 'FAIL'}  {label}")
    if not ok:
        if detail:
            print(f"      {detail}")
        failures.append(label)
    return ok


HARNESS = """
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "dev"
process.env.A2A_HELLO = "0"
process.env.A2A_AGENT = %(agent)s
process.env.HOME = %(home)s

const ROOT = %(root)s, CHILD = %(child)s
const calls = []

// One message on the stream, then park. The plugin reads res.body, so this is
// a real ReadableStream rather than a text body.
const streamBody = () => new ReadableStream({
  start(c) {
    c.enqueue(new TextEncoder().encode(JSON.stringify({
      id: "m1", channel: "advisory", sender: "peer",
      text: "does the root get this?", ts: 1, kind: "channel",
    }) + "\\n"))
    // never close: a real stream stays open
  },
})

globalThis.fetch = async (url, opts) => {
  const path = String(url).replace("http://broker.invalid", "")
  const ok = (b) => ({ ok: true, status: 200,
    text: async () => JSON.stringify(b), json: async () => b })
  if (path === "/me") return ok({ agent: %(agent)s, registered: true })
  if (path === "/healthz") return ok({ ok: true, clients: "0.1.0" })
  // A channel this agent is in, so the brief also produces its checkPrompt —
  // the message that starts a turn, and the one that fed the loop.
  if (path.startsWith("/channels")) {
    return ok({ channels: [{ name: "advisory", members: [%(agent)s] }] })
  }
  if (path.startsWith("/stream")) {
    return { ok: true, status: 200, body: streamBody() }
  }
  return ok({})
}

const sessions = [
  { id: ROOT, directory: %(project)s, time: { updated: 1 } },
  // The subagent is NEWER, which is what made "most recently updated" pick it.
  { id: CHILD, parentID: ROOT, directory: %(project)s, time: { updated: 999 } },
]

const mod = await import(%(module)s)
const plugin = await mod.A2A({
  client: {
    app: { log: async () => {} },
    session: {
      list: async () => sessions,
      promptAsync: async ({ path, body }) => {
        calls.push({
          id: path?.id,
          noReply: !!body?.noReply,
          text: String(body?.parts?.[0]?.text || "").slice(0, 200),
        })
        return { ok: true }
      },
    },
  },
  directory: %(project)s,
  $: () => {},
})

// Let the boot poll find a session and brief it.
await new Promise((r) => setTimeout(r, 900))
const afterBoot = calls.length

// Now the storm: exactly what a running subagent emits. message.updated per
// streamed part, session.updated for the child, idle for both.
for (let i = 0; i < 200; i++) {
  await plugin.event({ event: { type: "message.updated",
    properties: { info: { sessionID: CHILD, id: "msg" + i } } } })
  await plugin.event({ event: { type: "session.updated",
    properties: { info: { id: CHILD, parentID: ROOT,
                          directory: %(project)s, time: { updated: 1000 + i } } } } })
  await plugin.event({ event: { type: "session.idle",
    properties: { sessionID: CHILD } } })
  await plugin.event({ event: { type: "session.idle",
    properties: { sessionID: ROOT } } })
}
await new Promise((r) => setTimeout(r, 400))

console.log("@@" + JSON.stringify({ calls, afterBoot }))
process.exit(0)
"""


def drive() -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-sub-"))
    (tmp / ".config" / "opencode").mkdir(parents=True)
    harness = tmp / "drive.mjs"
    harness.write_text(HARNESS % {
        "home": json.dumps(str(tmp)),
        "module": json.dumps(str(OPENCODE)),
        # a2a is off in a project until <project>/.a2a.json says
        # otherwise, so the fixture opts in first.
        "project": json.dumps(str(_enabled_project(tmp))),
        "agent": json.dumps(AGENT),
        "root": json.dumps(ROOT),
        "child": json.dumps(CHILD),
    })
    res = subprocess.run(["node", str(harness)], text=True,
                         capture_output=True, timeout=120)
    for line in res.stdout.splitlines():
        if line.startswith("@@"):
            return json.loads(line[2:])
    print(res.stdout[-2000:])
    print(res.stderr[-2000:], file=sys.stderr)
    raise SystemExit("harness produced no result")



def _enabled_project(tmp: Path, name: str = "acme-api") -> Path:
    """A REAL project directory with the a2a switch turned on.

    Real, not a made-up path, because the switch is a file in it now. Named,
    because the agent id is derived from the basename.
    """
    project = tmp / name
    project.mkdir(parents=True, exist_ok=True)
    (project / ".a2a.json").write_text('{"enabled_opencode": true}')
    return project


def main() -> int:
    out = drive()
    calls = out["calls"]

    briefs = [c for c in calls if "You are a2a agent" in c["text"]]
    checks = [c for c in calls if "Session start" in c["text"]]
    to_child = [c for c in calls if c["id"] == CHILD]
    to_root = [c for c in calls if c["id"] == ROOT]
    unknown = [c for c in calls if c["id"] not in (ROOT, CHILD)]

    check("the instructions are injected exactly ONCE, after 200 rounds of "
          "the event storm a running subagent produces",
          len(briefs) == 1, f"{len(briefs)} briefs: {briefs}")
    check("and the turn-starting check prompt exactly once too — it is what "
          "used to feed itself, since the turn it starts emits more events",
          len(checks) == 1, f"{len(checks)} checks")
    check("nothing at all is said to the subagent session: it is a tool call, "
          "and instructions for somebody else are all its context is for",
          not to_child, str(to_child))
    check("everything went to the root session, even though the subagent was "
          "the most recently updated one the whole time",
          len(to_root) == len(calls) and not unknown, str(unknown or calls))
    check("the channel message was delivered to the root — a message injected "
          "into a subagent is acked where no human ever sees it",
          any("does the root get this?" in c["text"] for c in calls
              if c["id"] == ROOT),
          str(calls))
    check("the brief lands before the message, so the agent knows what the "
          "message is when it arrives",
          not calls or "You are a2a agent" in calls[0]["text"],
          str(calls[:1]))
    check("the instructions carry noReply — reference material must not spend "
          "a model turn",
          all(c["noReply"] for c in briefs), str(briefs))

    # The source contract: a blanket reset is what made this unbounded. Read
    # CODE only — the comments deliberately quote the old line to explain it.
    src = OPENCODE.read_text()
    code = "\n".join(
        ln for ln in src.splitlines()
        if not ln.lstrip().startswith(("*", "//", "/*"))
    )
    check("no code path clears the whole brief memo — a wholesale reset is "
          "the bug, however it is spelled",
          "briefed.clear()" not in code and "briefing = null" not in code,
          [ln for ln in code.splitlines() if "briefing" in ln])
    check("message.updated is not a wake: it fires per streamed part, so it "
          "made brief-then-drain run continuously",
          '"message.updated"' not in src.split("const wakes")[1][:400],
          "still in wakes")

    print()
    if failures:
        print(f"{len(failures)} failure(s):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("PASS — one brief, root only, storm-proof")
    return 0


if __name__ == "__main__":
    sys.exit(main())
