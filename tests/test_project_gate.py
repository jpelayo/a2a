#!/usr/bin/env python3
"""a2a makes no noise in a project until <project>/.a2a.json says so.

    python3 tests/test_project_gate.py

Both these clients install into a directory their harness scans for EVERY
session in EVERY directory — ~/.config/opencode/plugins and
~/.pi/agent/extensions — and neither harness offers a per-project way out.
So the switch is ours, and this is what pins it.

The switch holds back EFFECTS, never the vocabulary. Every a2a tool is
registered in every project; what a disabled project does not get is the
stream, the brief, the hello and the setup hint — anything that appears in a
session that did not ask for a2a. Keeping the tools is also what lets
enable_a2a_here connect the session you ask in, with no restart: there is
nothing to wait for, because the tools are already under it.

The OpenCode half RUNS the real plugin — imports it, stubs fetch, calls A2A()
against temp directories — because a source grep would pass a client that
reads the file and ignores it. Pi cannot be imported here (typebox is not
installed), so its half is source-level, the same trade test_client_loads.py
makes.

  off      tools registered, and ZERO network calls and ZERO injections.
           A typo must fail CLOSED.
  on       the stream runs.
  switch   writes the file, merges into it, and connects in place.

No broker, no database, no node_modules.
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

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


# --- the OpenCode plugin, actually run ---------------------------------------
HARNESS = r"""
import { mkdtempSync, writeFileSync, readFileSync } from "node:fs"
import { tmpdir } from "node:os"
import { join } from "node:path"

// URL_BASE and TOKEN are read at MODULE scope, so credentials have to exist
// before the import or the plugin bails for want of them and never reaches
// the gate.
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "t"
process.env.A2A_HELLO = "0"
delete process.env.A2A_AGENT

let calls = []
globalThis.fetch = async (url) => {
  const path = String(url).replace("http://broker.invalid", "")
  calls.push(path)
  const ok = (b) => ({ ok: true, status: 200,
    text: async () => JSON.stringify(b), json: async () => b })
  if (path === "/me") return ok({ agent: "x", registered: true })
  if (path === "/channels") return ok({ channels: [] })
  if (path.startsWith("/stream"))
    return { ok: true, status: 200,
             body: { getReader: () => ({ read: () => new Promise(() => {}) }) } }
  return ok({})
}

const prompts = []
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => [{ id: "ses_x", time: { updated: 1 } }],
    promptAsync: async (a) => {
      prompts.push(a?.body?.parts?.[0]?.text || "")
      return {}
    },
    prompt: async () => new Promise(() => {}),
  },
}

const { A2A } = await import(PLUGIN_PATH)
const settle = () => new Promise((r) => setTimeout(r, 1200))

const project = (contents) => {
  const d = mkdtempSync(join(tmpdir(), "a2a-gate-"))
  if (contents !== null) writeFileSync(join(d, ".a2a.json"), JSON.stringify(contents))
  return d
}
// Every case starts from a clean slate, so `calls` and `prompts` mean
// "what THIS project did", not "what the run has done so far".
const run = async (contents) => {
  calls = []; prompts.length = 0
  const d = project(contents)
  const s = await A2A({ client, directory: d })
  await settle()
  return { dir: d, s, tools: Object.keys(s.tool || {}),
           event: "event" in s, calls: [...calls], prompts: [...prompts] }
}

const out = {}
out.absent = await run(null)
out.false_ = await run({ enabled_opencode: false })
out.stringy = await run({ enabled_opencode: "true" })
// Pi's key must not speak for OpenCode: one directory, several harnesses,
// each its own agent.
out.other = await run({ enabled_pi: true })
out.bare = await run({ enabled: true })
out.on = await run({ enabled_opencode: true })

// The switch: writes, merges, and connects in place.
const off = await run({ catchup: 42, enabled_pi: true })
calls = []
out.wrote = JSON.parse(await off.s.tool.enable_a2a_here.execute({ enabled: true }))
await settle()
out.after_enable = [...calls]
out.file = JSON.parse(readFileSync(join(off.dir, ".a2a.json"), "utf8"))
out.off_again = JSON.parse(await off.s.tool.enable_a2a_here.execute({ enabled: false }))

const strip = (r) => ({ tools: r.tools, event: r.event,
                        calls: r.calls, prompts: r.prompts })
console.log("@@" + JSON.stringify({
  absent: strip(out.absent), false_: strip(out.false_),
  stringy: strip(out.stringy), other: strip(out.other),
  bare: strip(out.bare), on: strip(out.on),
  wrote: out.wrote, after_enable: out.after_enable,
  file: out.file, off_again: out.off_again,
}))
process.exit(0)
"""


def run_opencode() -> dict:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-gate-")) / "harness.mjs"
    tmp.write_text(f"const PLUGIN_PATH = {json.dumps(str(OPENCODE))}\n" + HARNESS)
    res = subprocess.run(["node", str(tmp)], capture_output=True, text=True,
                         timeout=120, env={**os.environ})
    if "@@" not in res.stdout:
        raise SystemExit("could not run the OpenCode harness:\n"
                         f"{res.stdout}\n{res.stderr}")
    return json.loads(res.stdout.split("@@", 1)[1])


def main() -> int:
    oc = run_opencode()

    for label, key in (("no .a2a.json at all", "absent"),
                       ('{"enabled_opencode": false}', "false_"),
                       ('{"enabled_opencode": "true"} — a STRING, which must '
                        "not count: the switch fails closed", "stringy"),
                       ('{"enabled_pi": true} — ANOTHER client\'s key, which '
                        "must not speak for this one", "other"),
                       ('{"enabled": true} — the bare key names no client, '
                        "so it enables none", "bare")):
        got = oc[key]
        check(f"opencode: {label} → NOTHING reaches the broker",
              got["calls"] == [], str(got["calls"]))
        check(f"opencode: {label} → nothing is injected into the session — "
              "no hello, no brief, no setup hint",
              got["prompts"] == [], str(got["prompts"]))
        check(f"opencode: {label} → but every tool is still registered: the "
              "switch holds back effects, not vocabulary",
              len(got["tools"]) > 1 and "enable_a2a_here" in got["tools"],
              str(got["tools"]))

    check('opencode: {"enabled_opencode": true} connects — /me and then '
          'the stream',
          "/me" in oc["on"]["calls"]
          and any(c.startswith("/stream") for c in oc["on"]["calls"]),
          str(oc["on"]["calls"]))
    check("opencode: an enabled project registers the same tools as a "
          "disabled one, so nothing about the surface depends on the switch",
          sorted(oc["on"]["tools"]) == sorted(oc["absent"]["tools"]),
          f'on: {oc["on"]["tools"]} vs off: {oc["absent"]["tools"]}')

    check("opencode: the switch writes the file it is named for",
          oc["wrote"]["enabled"] is True
          and oc["wrote"]["file"].endswith("/.a2a.json"), str(oc["wrote"]))
    check("opencode: and MERGES — an existing catchup AND another client's "
          "answer both survive, because answering for one harness must not "
          "throw away the rest of the file",
          oc["file"] == {"catchup": 42, "enabled_pi": True,
                         "enabled_opencode": True}, str(oc["file"]))
    check("opencode: enabling CONNECTS THIS SESSION — the tools were already "
          "registered, so there is nothing to wait for and no restart to ask "
          "for",
          "/me" in oc["after_enable"]
          and any(c.startswith("/stream") for c in oc["after_enable"]),
          str(oc["after_enable"]))
    check("opencode: it turns a project off as well as on",
          oc["off_again"]["enabled"] is False, str(oc["off_again"]))

    # --- Pi: source, because typebox is not installed here --------------------
    pi = PI.read_text()
    check("pi: reads the project file SYNCHRONOUSLY — its entry point is not "
          "async and registerTool runs before any await could resolve",
          'from "node:fs"' in pi and "readFileSync(PROJECT_FILE" in pi,
          "no synchronous read")
    check("pi: the switch fails closed, like OpenCode's",
          "projectCfg[ENABLE_KEY] === true" in pi, "not a strict === true")
    check("pi: and it reads ITS OWN key — one directory can run several "
          "harnesses, and each is a separate agent",
          'const CLIENT = "pi"' in pi
          and "const ENABLE_KEY = `enabled_${CLIENT}`" in pi,
          "pi does not scope the switch to itself")
    check("pi: nothing connects in a project that has not opted in",
          # Not [^}]* — the log line inside the block contains a literal
          # `{"enabled": true}`, and the character class stopped at its brace.
          re.search(r"if \(!ENABLED\) \{.*?\n\s*return;", pi, re.S)
          is not None
          and pi.index("if (!ENABLED)") < pi.index("pumping = true"),
          "the pump is not gated, or is gated after it starts")
    check("pi: and the tools are NOT gated — every one registers whatever the "
          "switch says, which is what lets the switch connect in place",
          pi.count("\n  pi.registerTool({\n") > 15
          and "if (ENABLED) pi.registerTool" not in pi,
          "tool registration is behind the switch")
    check("pi: the switch starts the pump when it turns a project on",
          re.search(r"ENABLED = on;\s*\n\s*if \(started\)", pi) is not None,
          "enabling does not connect")

    # --- one file, one name, or a project is 'enabled' for only half of it ----
    oc_src = OPENCODE.read_text()
    check("both clients read the SAME file at the project root — several "
          "harnesses in one directory is a supported setup, and two names "
          "would mean enabling one project twice",
          '`${directory || "."}/.a2a.json`' in oc_src
          and 'join(project, ".a2a.json")' in pi,
          "the two clients name different files")
    check("and both put it on TOP of the settings chain, so .a2a.json can "
          "carry read_on_init / catchup / agent per project",
          "project[fileKey] !== undefined" in oc_src
          and "projectCfg[fileKey] !== undefined" in pi,
          "the project file is not the first layer")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — a project is off until it says otherwise")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
