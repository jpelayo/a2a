#!/usr/bin/env python3
"""One setting, three clients, one meaning.

    python3 tests/test_read_on_init.py

Reading a channel is not free and not private: the broker acks whatever it
hands back, so a catch-up at startup CONSUMES the same messages push would have
delivered. That makes "read my channels when a session starts" a decision the
operator gets to make, not a default nobody can see.

    ~/.config/opencode/a2a.json     {"read_on_init": true,  "catchup": 10}
    ~/.pi/agent/a2a.json            {"read_on_init": true,  "catchup": 10}
    <claude plugin data>/a2a.json   {"read_on_init": false, "catchup": 10}

Claude Code alone defaults to off. Its session is the user's own rather than a
sidecar, and the MCP handshake already briefs it, so a channel read at every
launch would spend the user's context on traffic push delivers anyway.

The file normally does not exist — the install is one command — so the absent
case is the one that matters most and is asserted first for every client.

Pure python3 plus node; no broker, no harnesses.
"""
import json
import os
import re
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
PI = PLUGIN / "pi" / "index.ts"
CLAUDE = PLUGIN / "a2a" / "server" / "a2a-channel.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def claude_catchup(store: Path | None, env: dict | None = None) -> int:
    """CATCHUP as the Claude channel computes it, in a fresh interpreter."""
    code = (
        "import runpy, sys, json\n"
        "m = runpy.run_path(%r)\n"
        "print('@@' + json.dumps({'catchup': m['CATCHUP'],\n"
        "                        'read': m['READ_ON_INIT']}))\n"
    ) % str(CLAUDE)
    e = dict(os.environ)
    e.pop("A2A_READ_ON_INIT", None)
    e.pop("A2A_CATCHUP", None)
    e["A2A_IDENTITY_STORE"] = str(store) if store else ""
    e["A2A_AGENT"] = "cfg-tester"
    e.update(env or {})
    out = subprocess.run([sys.executable, "-c", code], text=True,
                         capture_output=True, env=e, timeout=30)
    line = next((l for l in out.stdout.splitlines() if l.startswith("@@")), "")
    if not line:
        return -1
    return json.loads(line[2:])["catchup"]


OC_HARNESS = """
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "dev"
process.env.A2A_AGENT = "cfg-tester"
process.env.A2A_HELLO = "0"
process.env.HOME = %(home)s
const calls = []
globalThis.fetch = async (url) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path)
  const ok = (b) => ({ ok: true, status: 200,
    text: async () => JSON.stringify(b), json: async () => b })
  if (path === "/me") return ok({ agent: "cfg-tester", registered: true })
  if (path === "/channels")
    return ok({ channels: [{ name: "advisory", members: ["cfg-tester"] }] })
  if (path.startsWith("/channels/"))
    return ok({ transcript: [{ id: "m1", sender: "p", text: "hi" }] })
  if (path.startsWith("/stream"))
    return { ok: true, status: 200,
             body: { getReader: () => ({ read: () => new Promise(() => {}) }) } }
  return ok({})
}
const prompts = []
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => [{ id: "ses_x", directory: "/tmp/proj",
                         time: { updated: 1 } }],
    promptAsync: async (a) => {
      prompts.push(a?.body?.parts?.[0]?.text || "")
      return {}
    },
    prompt: async () => new Promise(() => {}),
  },
}
const mod = await import(%(module)s)
await mod.A2A({ client, directory: "/tmp/proj" })
await new Promise((r) => setTimeout(r, 1500))
console.log("@@" + JSON.stringify({ calls, prompts }))
process.exit(0)
"""


def opencode_checks(home: Path) -> bool:
    """Is the agent asked to check its channels at startup, with this HOME?"""
    harness = home / "drive.mjs"
    harness.write_text(OC_HARNESS % {
        "home": json.dumps(str(home)),
        "module": json.dumps(str(OPENCODE)),
    })
    out = subprocess.run(["node", str(harness)], text=True,
                         capture_output=True, timeout=60)
    line = next((l for l in out.stdout.splitlines() if l.startswith("@@")), "")
    if not line:
        print(out.stderr[-500:])
        return False
    return any("Check your channels" in t
               for t in json.loads(line[2:])["prompts"])


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-cfg-"))

    # --- OpenCode: on by default, off when the file says so ----------------
    on = tmp / "home-default"
    (on / ".config" / "opencode").mkdir(parents=True)
    check("OpenCode checks its channels with NO settings file — the install "
          "is one command and the default has to be the supported setup",
          opencode_checks(on), "no channel check")

    off = tmp / "home-off"
    (off / ".config" / "opencode").mkdir(parents=True)
    (off / ".config" / "opencode" / "a2a.json").write_text(
        json.dumps({"read_on_init": False}))
    check("OpenCode asks for nothing when read_on_init is false — the "
          "switch is the point, since the check costs a model turn",
          not opencode_checks(off), "asked anyway")

    zero = tmp / "home-zero"
    (zero / ".config" / "opencode").mkdir(parents=True)
    (zero / ".config" / "opencode" / "a2a.json").write_text(
        json.dumps({"catchup": 0}))
    check("catchup: 0 means the same thing as read_on_init: false",
          not opencode_checks(zero), "asked anyway")

    # --- Claude: off by default, on when asked ------------------------------
    empty = tmp / "claude-empty"
    empty.mkdir()
    check("Claude Code does NOT check on init by default — its session is "
          "the user's own and the MCP handshake already briefs it",
          claude_catchup(empty) == 0, str(claude_catchup(empty)))

    onstore = tmp / "claude-on"
    onstore.mkdir()
    (onstore / "a2a.json").write_text(json.dumps({"read_on_init": True}))
    check("and it does when the file turns it on — the key means the same "
          "thing here as in the other two clients",
          claude_catchup(onstore) == 10, str(claude_catchup(onstore)))

    sized = tmp / "claude-sized"
    sized.mkdir()
    (sized / "a2a.json").write_text(
        json.dumps({"read_on_init": True, "catchup": 3}))
    check("catchup bounds how much history it pulls",
          claude_catchup(sized) == 3, str(claude_catchup(sized)))

    check("the environment can turn it on without touching a file",
          claude_catchup(empty, {"A2A_READ_ON_INIT": "1"}) == 10,
          str(claude_catchup(empty, {"A2A_READ_ON_INIT": "1"})))
    check("the environment sizes it too when no file states the key",
          claude_catchup(empty, {"A2A_READ_ON_INIT": "1",
                                 "A2A_CATCHUP": "99"}) == 99,
          str(claude_catchup(empty, {"A2A_READ_ON_INIT": "1",
                                     "A2A_CATCHUP": "99"})))
    check("but a settings file beats the environment — the file is the one an "
          "operator edits on purpose, so it is the one that wins",
          claude_catchup(sized, {"A2A_CATCHUP": "99"}) == 3,
          str(claude_catchup(sized, {"A2A_CATCHUP": "99"})))

    # --- Pi: the same key, read from its own directory ----------------------
    pi_src = PI.read_text()
    check("Pi reads the same file name in its own config directory",
          'join(homedir(), ".pi", "agent", "a2a.json")' in pi_src,
          "settings path missing")
    check("Pi defaults read_on_init to true, like OpenCode",
          "const READ_ON_INIT_DEFAULT = true" in pi_src, "default changed")
    check("Pi asks the agent to check rather than reading for it — a "
          "client-side read is invisible and acks with nothing on screen",
          "checkPrompt(rooms)" in pi_src
          and "triggerTurn: true" in pi_src
          and "/messages?limit=" not in pi_src,
          "Pi still reads transcripts itself, or no longer spends a visible "
          "turn on the check")

    # --- all three agree on the name ---------------------------------------
    for label, src in (("OpenCode", OPENCODE.read_text()),
                       ("Pi", pi_src), ("Claude", CLAUDE.read_text())):
        check(f"{label} spells the setting read_on_init",
              "read_on_init" in src, "not found")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — one setting, three clients, one meaning")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
