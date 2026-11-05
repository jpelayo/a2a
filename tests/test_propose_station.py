#!/usr/bin/env python3
"""propose_me can name the station — in every client.

    python3 tests/test_propose_station.py

A token that reaches two stations cannot propose without saying which one:
the broker answers 409 "this token reaches several stations (…); name one
with "station"" (`_proposal_station`). No client could say it. Each sent
`{agent_id, note}` and nothing else, so an agent that passed station="bots"
had it dropped and got the same error back. (Pi dropped `note` too: its
execute took the tool-call id for its arguments.)

Each client's REAL propose_me runs here against one fake broker that records
what was POSTed. With a station, the body carries it; without one — absent or
null — the body is exactly what it always was, so a single-station token
behaves as before. The python clients run as processes spoken to over MCP
stdio; the JS/TS ones are imported by node. Hermes is pinned in
test_hermes_client.py, which has its fake Hermes. No database.

SIGNING UP TURNS PUSH ON. In a project that is off, a request the broker
accepts switches a2a on there, as enable_a2a_here does — merged into
.a2a.json, the pump started — so the agent receives the moment it is
approved. A refused request turns nothing on. (Claude Code has no switch.)

WHAT IS ASKED FOR. With no A2A_AGENT and nothing stored, the id is the
folder's name, which every harness in that folder derives alike; so a
propose_me with no `name` asks for `<folder>-<harness>` instead. `name` asks
for anything. Once the broker takes the request the client goes by the name
it asked for — stored, and on the very next request — and a refused request
switches nothing. The operator approves or denies the name; no client refuses
one. With A2A_AGENT set (the runs above) the default is that id, as before.
"""
import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
from pathlib import Path

from test_project_gate import FakeBroker

HERE = Path(__file__).resolve().parent
PLUGIN = HERE.parent / "deploy" / "plugin"
CHANNEL = PLUGIN / "a2a" / "server" / "a2a-channel.py"
CODEX = PLUGIN / "codex" / "a2a-codex.py"
PI = PLUGIN / "pi" / "index.ts"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
KILOCODE = PLUGIN / "kilocode" / "a2a-kilocode.js"
DSH = PLUGIN / "dsh" / "a2a-dsh.js"

# What the agent passes, in order, and the body each must produce.
CALLS = [{"note": "n1", "station": "bots"},
         {"note": "n2"},
         {"note": "n3", "station": None}]
WANT = [{"agent_id": "probe", "note": "n1", "station": "bots"},
        {"agent_id": "probe", "note": "n2"},
        {"agent_id": "probe", "note": "n3"}]
# Every project starts OFF with another key in its file, which the switch
# must keep: it merges, never truncates.
PROJECT = {"read_on_init": False}

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


# True for the runs with no A2A_AGENT, where the id is derived from the folder.
DERIVED = False


def base_env(fb: FakeBroker, tmp: Path) -> dict:
    env = dict(os.environ, A2A_URL=fb.url, A2A_TOKEN="t", A2A_AGENT="probe",
               A2A_HELLO="0", A2A_READ_ON_INIT="0", HOME=str(tmp),
               XDG_DATA_HOME=str(tmp / "data"))
    for k in ("A2A_CATCHUP", "A2A_CLIENT_VERSION"):
        env.pop(k, None)
    if DERIVED:
        env.pop("A2A_AGENT", None)
    return env


def mcp(argv, env, cwd) -> tuple[list, list]:
    """A python client as its harness runs it: a process spoken to over MCP
    stdio. (propose_me's inputSchema properties, [result text per call])."""
    proc = subprocess.Popen(argv, cwd=cwd, env=env, text=True,
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE)

    def say(obj):
        proc.stdin.write(json.dumps(obj) + "\n")
        proc.stdin.flush()

    say({"jsonrpc": "2.0", "id": 1, "method": "initialize",
         "params": {"protocolVersion": "2024-11-05"}})
    say({"jsonrpc": "2.0", "method": "notifications/initialized"})
    say({"jsonrpc": "2.0", "id": 2, "method": "tools/list", "params": {}})
    for i, args in enumerate(CALLS):
        say({"jsonrpc": "2.0", "id": 10 + i, "method": "tools/call",
             "params": {"name": "propose_me", "arguments": args}})
    time.sleep(3)
    proc.stdin.close()
    try:
        out, _ = proc.communicate(timeout=30)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    replies = {}
    for line in out.splitlines():
        try:
            r = json.loads(line)
        except ValueError:
            continue
        if isinstance(r, dict) and "id" in r:
            replies[r["id"]] = r.get("result") or {}
    tool = next((t for t in (replies.get(2) or {}).get("tools", [])
                 if t.get("name") == "propose_me"), {})
    props = list(((tool.get("inputSchema") or {}).get("properties")
                  or {}).keys())
    texts = [(((replies.get(10 + i) or {}).get("content") or [{}])[0]
              .get("text")) for i in range(len(CALLS))]
    return props, texts


def node(script: str, env, cwd) -> dict:
    res = subprocess.run(["node", script], cwd=cwd, env=env,
                         capture_output=True, text=True, timeout=120)
    line = next((l for l in res.stdout.splitlines() if l.startswith("@@")),
                "")
    if not line:
        return {"error": (res.stdout[-800:] + res.stderr[-800:])}
    return json.loads(line[2:])


PI_HARNESS = r'''
const tools = {}
const pi = { on: () => {}, registerTool: (t) => { tools[t.name] = t },
             ui: { notify: () => {} }, sendMessage: () => {} }
const mod = await import("./index.ts")
mod.default(pi)
const t = tools.propose_me
const out = { schema: Object.keys(t.parameters?.a?.[0] || {}), results: [] }
for (const params of CALLS) {
  try {
    // Pi calls execute(toolCallId, params, …), as every other tool reads it.
    const r = await t.execute("call-1", params)
    out.results.push(r?.content?.[0]?.text ?? null)
  } catch (e) { out.results.push("ERR " + (e?.message || e)) }
}
await new Promise((r) => setTimeout(r, 1500))   // let a started pump connect
console.log("@@" + JSON.stringify(out))
process.exit(0)
'''

OPENCODE_HARNESS = r'''
const client = {
  app: { log: async () => {} },
  session: {
    list: async () => [],
    promptAsync: async () => ({}),
    prompt: async () => new Promise(() => {}),
  },
}
const mod = await import(PLUGIN_PATH)
const s = await mod.default.server({ client, directory: process.cwd() })
const t = s.tool.propose_me
const out = { schema: Object.keys(t.args || {}), results: [] }
for (const params of CALLS) {
  try { out.results.push(String(await t.execute(params))) }
  catch (e) { out.results.push("ERR " + (e?.message || e)) }
}
await new Promise((r) => setTimeout(r, 1500))   // let a started pump connect
console.log("@@" + JSON.stringify(out))
process.exit(0)
'''


# Kilo Code receives only in its TUI's server, a Worker thread marked
# KILO_PROCESS_ROLE=worker, so its own client runs inside one here; the
# Worker hands its answer back to the main thread, which prints it.
KILOCODE_HARNESS = r'''
import { Worker, isMainThread, parentPort } from "node:worker_threads"
if (isMainThread) {
  const w = new Worker(new URL(import.meta.url),
                       { env: { ...process.env, KILO_PROCESS_ROLE: "worker" } })
  w.on("message", (m) => { console.log(m); process.exit(0) })
  w.on("error", (e) => { console.log("worker error " + e); process.exit(1) })
} else {
  const client = {
    app: { log: async () => {} },
    session: { list: async () => [], status: async () => ({}),
               promptAsync: async () => ({}) },
  }
  const mod = await import(PLUGIN_PATH)
  const s = await mod.default.server({ client, directory: process.cwd() })
  const t = s.tool.propose_me
  const out = { schema: Object.keys(t.args || {}), results: [] }
  for (const params of CALLS) {
    try { out.results.push(String(await t.execute(params))) }
    catch (e) { out.results.push("ERR " + (e?.message || e)) }
  }
  await new Promise((r) => setTimeout(r, 1500))   // let a started pump connect
  parentPort.postMessage("@@" + JSON.stringify(out))
}
'''

# dsh: one Host, its tools registered once and routed by the caller's cwd;
# the web profile receives.
DSH_HARNESS = r'''
const tools = {}
const ctx = {
  get: (n) => n === "profileContext" ? { name: "web", cwd: process.cwd() }
    : n === "sessionController" ? { list: async () => [], resolveAgent: async () => ({}) }
    : undefined,
  agents: { list: () => [] },
  tools: { register: (t) => { tools[t.name] = t } },
  on: () => {}, effect: () => {},
}
const mod = await import(PLUGIN_PATH)
await mod.default.dsh(ctx)
const t = tools.propose_me
const exec = { agent: { session: { header: { cwd: process.cwd() } } } }
const out = { schema: Object.keys(t.parameters?.properties || {}), results: [] }
for (const params of CALLS) {
  try { out.results.push(String(await t.execute(params, exec))) }
  catch (e) { out.results.push("ERR " + (e?.message || e)) }
}
await new Promise((r) => setTimeout(r, 1500))   // let a started pump connect
console.log("@@" + JSON.stringify(out))
process.exit(0)
'''


def project_file(d: Path) -> dict | None:
    try:
        return json.loads((d / ".a2a.json").read_text())
    except Exception:
        return None


def run_pi(fb, tmp) -> dict:
    work = tmp / "pi"
    (work / "node_modules" / "typebox").mkdir(parents=True)
    (work / ".a2a.json").write_text(json.dumps(PROJECT))
    (work / "node_modules" / "typebox" / "package.json").write_text(
        json.dumps({"name": "typebox", "version": "0.0.0", "type": "module",
                    "main": "index.js"}))
    (work / "node_modules" / "typebox" / "index.js").write_text(
        "export const Type = new Proxy({}, { get: () => (...a) => ({ a }) });\n")
    shutil.copy(PI, work / "index.ts")
    (work / "run.mjs").write_text(
        f"const CALLS = {json.dumps(CALLS)}\n" + PI_HARNESS)
    return dict(node("run.mjs", base_env(fb, tmp), work), project=work)


def run_opencode(fb, tmp) -> dict:
    work = tmp / "opencode"
    work.mkdir()
    (work / ".a2a.json").write_text(json.dumps(PROJECT))
    (work / "run.mjs").write_text(
        f"const CALLS = {json.dumps(CALLS)}\n"
        f"const PLUGIN_PATH = {json.dumps(str(OPENCODE))}\n" + OPENCODE_HARNESS)
    return dict(node("run.mjs", base_env(fb, tmp), work), project=work)


def run_own_js(label: str, plugin: Path, harness: str):
    """Kilo Code's or dsh's own client, under its harness above."""
    def go(fb, tmp) -> dict:
        work = tmp / label
        work.mkdir()
        (work / ".a2a.json").write_text(json.dumps(PROJECT))
        (work / "run.mjs").write_text(
            f"const CALLS = {json.dumps(CALLS)}\n"
            f"const PLUGIN_PATH = {json.dumps(str(plugin))}\n" + harness)
        env = dict(base_env(fb, tmp), DSH_HOME=str(tmp / "dsh-home"))
        return dict(node("run.mjs", env, work), project=work)
    return go


def run_channel(fb, tmp) -> dict:
    store = tmp / "cc"
    store.mkdir()
    env = dict(base_env(fb, tmp), A2A_IDENTITY_STORE=str(store),
               A2A_AGENT_DIR=str(store), A2A_LOG_FILE=str(store / "c.log"),
               A2A_STREAM_TIMEOUT="3")
    props, texts = mcp([sys.executable, str(CHANNEL)], env, store)
    return {"schema": props, "results": texts}


def run_codex(fb, tmp) -> dict:
    proj = tmp / "codex-proj"
    proj.mkdir()
    (proj / ".a2a.json").write_text(json.dumps(PROJECT))
    env = dict(base_env(fb, tmp), A2A_CODEX_SOCK="",
               CODEX_HOME=str(tmp / "codex-home"))
    props, texts = mcp([sys.executable, str(CODEX)], env, proj)
    return {"schema": props, "results": texts, "project": proj}


# The clients with a project switch, and the key each writes.
SWITCH = {"Codex": "enabled_codex", "Pi": "enabled_pi",
          "OpenCode": "enabled_opencode", "Kilo Code": "enabled_kilocode",
          "DeepSeek Harness": "enabled_dsh"}
# What a pump that started asks first. Codex opens /stream only with the
# launch line's socket — a session that cannot render must not take the
# inbox (test_codex_client) — so there it is the pump's /me; and an OFF
# Codex asks the broker nothing at all (test_project_gate).
STARTED = {"Codex": "/me", "Pi": "/stream", "OpenCode": "/stream",
           "Kilo Code": "/stream", "DeepSeek Harness": "/stream"}


def run(label, fn, refuse=False):
    fb = FakeBroker()
    if refuse:
        fb.refuse.add("/me/proposals")
    tmp = Path(tempfile.mkdtemp(prefix="a2a-propose-")).resolve()
    try:
        out = dict(fn(fb, tmp), tmp=str(tmp))
    finally:
        fb.close()
    return out, fb


# --- what propose_me asks for, with the id derived from the folder ---------
# The folder each driver above runs the client in, and the harness suffix.
FOLDER = {"Claude Code": "cc", "Codex": "codex-proj", "Pi": "pi",
          "OpenCode": "opencode", "Kilo Code": "kilocode",
          "DeepSeek Harness": "dsh"}
SUFFIX = {"Claude Code": "claude", "Codex": "codex", "Pi": "pi",
          "OpenCode": "opencode", "Kilo Code": "kilo",
          "DeepSeek Harness": "dsh"}


def asked(fb) -> list[tuple[str, str]]:
    """(X-A2A-Agent it went out as, agent_id it asked for) per proposal."""
    callers = [a for p, a in fb.agents if p == "/me/proposals"]
    bodies = [b.get("agent_id") for p, b in fb.bodies if p == "/me/proposals"]
    return list(zip(callers, bodies))


def stored(out: dict) -> set[str]:
    """Every id any identity store under the run's home holds."""
    names: set[str] = set()
    tmp = Path(out.get("tmp") or "/nonexistent")
    for p in [*tmp.rglob("a2a-identity.json"), *tmp.rglob("identity.json")]:
        try:
            names |= {v for v in json.loads(p.read_text()).values()
                      if isinstance(v, str)}
        except Exception:
            pass
    return names


def derived_run(label, fn, calls, refuse=False):
    global CALLS, DERIVED
    CALLS, DERIVED = calls, True
    try:
        return run(label, fn, refuse)
    finally:
        CALLS, DERIVED = ORIGINAL_CALLS, False


ORIGINAL_CALLS = CALLS


def check_naming(label, fn) -> None:
    folder = FOLDER[label]
    default = f"{folder}-{SUFFIX[label]}"

    out, fb = derived_run(label, fn, [{"note": "d1"}, {"note": "d2"}])
    if "error" in out:
        check(f"{label} (derived id): the client ran", False, out["error"])
        return
    got = asked(fb)
    check(f"{label}: propose_me offers a name argument",
          "name" in (out.get("schema") or []), str(out.get("schema")))
    check(f"{label}: with no name, an id that is only the folder's name asks "
          f"for {default!r}, not {folder!r}",
          got[:1] == [(folder, default)], str(got))
    check(f"{label}: and once the broker took it, the client goes by it — "
          "the next request is sent as it, and asks for it again",
          got[1:2] == [(default, default)], str(got))
    check(f"{label}: the name is stored for this project, as rename_me "
          "stores one", default in stored(out), str(stored(out)))

    out, fb = derived_run(label, fn, [{"name": "picked", "note": "p1"},
                                      {"note": "p2"}])
    got = asked(fb)
    check(f"{label}: name asks for exactly that name",
          got[:1] == [(folder, "picked")], str(got))
    check(f"{label}: and the client goes by it from the next request on, "
          "stored", got[1:2] == [("picked", "picked")]
          and "picked" in stored(out), f"{got} {stored(out)}")

    out, fb = derived_run(label, fn, [{"name": "picked", "note": "r1"},
                                      {"note": "r2"}], refuse=True)
    got = asked(fb)
    check(f"{label}: a request the broker REFUSES switches nothing — the "
          "next one goes out under the folder's name, asking for the default",
          got[1:2] == [(folder, default)] and "picked" not in stored(out),
          f"{got} {stored(out)}")


def main() -> int:
    for label, fn in (("Claude Code", run_channel), ("Codex", run_codex),
                      ("Pi", run_pi), ("OpenCode", run_opencode),
                      ("Kilo Code", run_own_js("kilocode", KILOCODE,
                                               KILOCODE_HARNESS)),
                      ("DeepSeek Harness", run_own_js("dsh", DSH, DSH_HARNESS))):
        out, fb = run(label, fn)
        sent = [b for p, b in fb.bodies if p == "/me/proposals"]
        if "error" in out:
            check(f"{label}: the client ran", False, out["error"])
            continue
        if label in SWITCH:
            got = project_file(out["project"])
            check(f"{label}: an accepted request turns a2a ON for the "
                  "project — merged, the other keys kept",
                  got == dict(PROJECT, **{SWITCH[label]: True}), str(got))
            check(f"{label}: and connects at once: the pump starts "
                  f"({STARTED[label]})", STARTED[label] in fb.calls,
                  str(fb.calls))
            out2, fb2 = run(label, fn, refuse=True)
            got = project_file(out2.get("project") or Path("/nonexistent"))
            check(f"{label}: a REFUSED request turns nothing on — the file "
                  "is untouched and no pump starts",
                  got == PROJECT and STARTED[label] not in fb2.calls,
                  f"{got} {fb2.calls}")
        check(f"{label}: propose_me offers a station argument",
              "station" in (out.get("schema") or []), str(out.get("schema")))
        check(f"{label}: each call reached the broker", len(sent) == 3,
              f"{sent} / results {out.get('results')}")
        check(f"{label}: with a station, the body carries it",
              sent[:1] == WANT[:1], str(sent[:1]))
        check(f"{label}: without one — absent or null — the body is exactly "
              "{agent_id, note}, as before",
              sent[1:3] == WANT[1:3], str(sent[1:3]))
        check_naming(label, fn)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — every client's propose_me can name the station, sends what "
          "it always sent when it does not, an accepted request turns push "
          "on, and it asks for <folder>-<harness> or the name it is given")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
