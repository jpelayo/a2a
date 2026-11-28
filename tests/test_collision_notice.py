#!/usr/bin/env python3
"""A session whose agent id belongs to another client is TOLD so at start.

    python3 tests/test_collision_notice.py

Two harnesses in one folder derive one id; the first to act holds it (the
broker's client pin) and the second gets 403 "belongs to another client" on
every call. Nothing used to say so. Now, where each client does its
start-of-session work, a collision (from /me's fields: registered, and
this_agent.client "other" or bound to another token) REPLACES the channel
check with one notice: the id is taken, ask the user whether to create a new
agent — the stem plus this harness's suffix — and do NOT name the id (that
is a transfer). Whatever read_on_init says: nothing can be read as an agent
the broker refuses.

Here: OpenCode (the real plugin under node), Claude Code, Codex and
Antigravity (the real modules, network stubbed). Kilo, dsh and Pi are in
their own client tests, Hermes in test_hermes_client.py, and what
propose_me then asks for in test_propose_station.py. No broker, no harness.
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLUGIN = HERE.parent / "deploy" / "plugin"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
CLAUDE = PLUGIN / "a2a" / "server" / "a2a-channel.py"
CODEX = PLUGIN / "codex" / "a2a-codex.py"
AGY = PLUGIN / "antigravity" / "a2a-antigravity.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


ID = "team-claudecode"
ME_OK = {"agent": ID, "registered": True, "stations": ["s"]}
ME_COLLIDES = dict(ME_OK, this_agent={"agent_id": ID, "client": "other",
                                      "bound": True, "bound_to_me": True},
                   refused=f"agent {ID!r} belongs to another client")
NOTICE = "already used by another client"


OC_HARNESS = """
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "dev"
process.env.A2A_AGENT = %(id)s
process.env.A2A_HELLO = "0"
process.env.HOME = %(home)s
process.env.XDG_DATA_HOME = %(home)s + "/.local/share"
const ME = %(me)s
const calls = []
globalThis.fetch = async (url) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path)
  const ok = (b) => ({ ok: true, status: 200,
    text: async () => JSON.stringify(b), json: async () => b })
  if (path === "/me") return ok(ME)
  if (path === "/channels")
    return ok({ channels: [{ name: "advisory", members: [ME.agent] }] })
  if (path.startsWith("/stream"))
    return { ok: true, status: 200,
             body: { getReader: () => ({ read: () => new Promise(() => {}) }) } }
  return ok({})
}
const prompts = []
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => [{ id: "ses_x", directory: %(project)s,
                         time: { updated: 1 } }],
    promptAsync: async (a) => {
      prompts.push({ text: a?.body?.parts?.[0]?.text || "",
                     noReply: !!a?.body?.noReply })
      return {}
    },
    prompt: async () => new Promise(() => {}),
  },
}
const mod = await import(%(module)s)
const hooks = await mod.default.server({ client, directory: %(project)s })
await new Promise((r) => setTimeout(r, 1500))
const sys = { system: [] }
await hooks["experimental.chat.system.transform"]?.({ sessionID: "ses_x" }, sys)
console.log("@@" + JSON.stringify({ calls, prompts, system: sys.system }))
process.exit(0)
"""


def opencode(me: dict) -> dict:
    home = Path(tempfile.mkdtemp(prefix="a2a-coll-oc-"))
    project = home / "proj"
    project.mkdir()
    (project / ".a2a.json").write_text('{"enabled_opencode": true}')
    drive = home / "drive.mjs"
    drive.write_text(OC_HARNESS % {
        "id": json.dumps(ID), "home": json.dumps(str(home)),
        "me": json.dumps(me), "module": json.dumps(str(OPENCODE)),
        "project": json.dumps(str(project))})
    out = subprocess.run(["node", str(drive)], text=True, capture_output=True,
                         timeout=60)
    shutil.rmtree(home, ignore_errors=True)
    line = next((l for l in out.stdout.splitlines() if l.startswith("@@")), "")
    return json.loads(line[2:]) if line else {"__error__": out.stderr[-600:]}


def load(path: Path, name: str):
    spec = importlib.util.spec_from_file_location(name, path)
    m = importlib.util.module_from_spec(spec)
    sys.modules[name] = m
    spec.loader.exec_module(m)
    return m


def main() -> int:
    os.environ.update(A2A_URL="http://127.0.0.1:1", A2A_TOKEN="t",
                      A2A_AGENT=ID, A2A_CODEX_SOCK="")

    # --- OpenCode: the brief loop says the notice instead of the check -----
    if shutil.which("node"):
        base = opencode(ME_OK)
        hit = opencode(ME_COLLIDES)
        check("opencode: the harness ran", "__error__" not in base
              and "__error__" not in hit, str(base.get("__error__")
                                              or hit.get("__error__")))
        check("opencode: without a collision, the channel check is asked "
              "(the baseline: this driver sees a start)",
              any("Check your channels" in p["text"]
                  for p in base.get("prompts", [])), str(base.get("prompts")))
        notes = [p for p in hit.get("prompts", []) if NOTICE in p["text"]]
        check("opencode: on a collision the session gets the notice, once, "
              "as noReply (no turn spent), offering team-opencode",
              len(notes) == 1 and notes[0]["noReply"]
              and '"team-opencode"' in notes[0]["text"]
              and "propose_me with no name" in notes[0]["text"],
              json.dumps(hit.get("prompts"))[:400])
        check("opencode: and no channel check, no channel read",
              not any("Check your channels" in p["text"]
                      for p in hit.get("prompts", []))
              and "/channels" not in hit.get("calls", []),
              f"{hit.get('prompts')} {hit.get('calls')}")
        check("opencode: without a collision the system prompt carries the "
              "a2a instructions (the baseline for the next check)",
              any("You are a2a agent" in str(s)
                  for s in base.get("system", [])),
              str(base.get("system"))[:200])
        check("opencode: and no a2a instructions in the system prompt for an "
              "agent the broker refuses",
              not any("You are a2a agent" in str(s)
                      for s in hit.get("system", [])),
              str(hit.get("system"))[:200])
    else:
        check("node is installed (OpenCode runs under it)", False, "no node")

    # --- Claude Code: _check_channels, the start-of-session notify ---------
    sys.path.insert(0, str(CLAUDE.parent))
    ch = load(CLAUDE, "coll_claude")
    said = []
    ch._notify = lambda content, meta: said.append(content)
    ch._me_view = lambda: dict(ME_COLLIDES)
    ch.CATCHUP = 0                      # read_on_init off: the default here
    ch._check_channels()
    check("claude: on a collision the notice is sent even with read_on_init "
          "off, offering team-claude",
          len(said) == 1 and NOTICE in said[0]
          and '"team-claude"' in said[0], str(said))
    said.clear()
    ch._me_view = lambda: dict(ME_OK)
    ch._check_channels()
    check("claude: without one, read_on_init off still sends nothing",
          said == [], str(said))

    # --- Codex: catch_up, the injected start turn --------------------------
    cx = load(CODEX, "coll_codex")
    sent, paths = [], []

    class Srv:
        def submit(self, text):
            sent.append(text)
    cx.get_server = lambda: Srv()
    me = {"v": ME_COLLIDES}
    cx.api = lambda m, p, body=None, timeout=30: (
        paths.append(p), dict(me["v"]) if p == "/me" else {"channels": [
            {"name": "ops"}]})[1]
    cx.READ_ON_INIT, cx.CATCHUP = True, 10
    cx.catch_up()
    check("codex: on a collision the session gets one notice turn, offering "
          "team-codex, instead of the catch-up",
          len(sent) == 1 and NOTICE in sent[0] and '"team-codex"' in sent[0]
          and "read_channel" not in sent[0], str(sent))
    check("codex: and lists no channels", "/channels" not in paths, str(paths))
    sent.clear()
    me["v"] = ME_OK
    cx.catch_up()
    check("codex: without one, the catch-up is as before",
          len(sent) == 1 and "read_channel" in sent[0]
          and NOTICE not in sent[0], str(sent))

    # --- Antigravity: the first a2a_inbox's note ---------------------------
    tmp = Path(tempfile.mkdtemp(prefix="a2a-coll-agy-"))
    for sub in ("antigravity-cli/plugin_data/a2a", "t", "proj"):
        (tmp / sub).mkdir(parents=True)
    os.environ.update(PLUGIN_DATA=str(tmp / "antigravity-cli" / "plugin_data"
                                      / "a2a"),
                      TMPDIR=str(tmp / "t"), PWD=str(tmp / "proj"))
    ax = load(AGY, "coll_agy")
    paths.clear()
    me["v"] = ME_COLLIDES
    ax.api = cx.api
    ax.READ_ON_INIT, ax.CATCHUP = True, 10
    note = ax._catchup_note()
    check("antigravity: on a collision the start note is the notice, "
          "offering team-antigravity",
          NOTICE in note and '"team-antigravity"' in note
          and "read_channel" not in note, note)
    check("antigravity: and lists no channels", "/channels" not in paths,
          str(paths))
    me["v"] = ME_OK
    note = ax._catchup_note()
    check("antigravity: without one, the read-on-init note is as before",
          "read_channel" in note and NOTICE not in note, note)
    shutil.rmtree(tmp, ignore_errors=True)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — a taken id is said at session start, with the new agent "
          "to offer, instead of a channel check")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
