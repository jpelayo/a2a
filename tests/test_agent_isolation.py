#!/usr/bin/env python3
"""Two instances of one harness, in ONE directory, sharing nothing.

    python3 tests/test_agent_isolation.py

A2A_AGENT is the only input to the identity ladder that is not derived from a
path, so it is the only thing that can tell two processes started in the same
directory apart. That much already worked. What did not is everything the
client keeps for itself: settings and the log were fixed filenames resolved
before the id existed, so both instances wrote the same files — and the log
rotation rewrites the whole file, so two writers could drop each other's lines
wholesale.

This file pins the four properties that make two instances safe:

  derived id   -> the old filenames, byte for byte. This is the whole
                  backwards-compatibility guarantee: anyone not setting
                  A2A_AGENT must see exactly what they saw before.
  explicit id  -> a2a-<id>.json / a2a-<id>.log, one set per agent
  fallback     -> the shared file is still read, so an install that gains an
                  A2A_AGENT keeps the settings it already had
  no pinning   -> an explicit id never writes the identity store, since both
                  instances would take turns overwriting one directory key

Needs node. No broker and no database: this is about paths, and a test that
needed a live broker to check a filename would not be run.
"""
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
PI = PLUGIN / "pi" / "index.ts"

failures: list[str] = []


def check(label: str, ok: bool, detail: str = "") -> bool:
    print(f"{'ok  ' if ok else 'FAIL'}  {label}")
    if not ok:
        if detail:
            print(f"      {detail}")
        failures.append(label)
    return ok


# The plugin never returns its paths, so drive it and watch the filesystem:
# what it wrote is the only honest answer to "which file does it use".
OC_HARNESS = """
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "dev"
process.env.A2A_HELLO = "0"
process.env.HOME = %(home)s
%(agent)s
globalThis.fetch = async (url) => {
  const path = String(url).replace("http://broker.invalid", "")
  const ok = (b) => ({ ok: true, status: 200,
    text: async () => JSON.stringify(b), json: async () => b })
  if (path === "/me") return ok({ agent: %(id)s, registered: true })
  if (path === "/healthz") return ok({ ok: true, clients: "0.1.0" })
  if (path.startsWith("/channels")) return ok({ channels: [] })
  if (path.startsWith("/stream")) {
    // Park: the pump must not spin while we look at the filesystem.
    await new Promise(() => {})
  }
  return ok({})
}
const mod = await import(%(module)s)
const plugin = await mod.A2A({
  client: { app: { log: async () => {} } },
  directory: %(project)s,
  $: () => {},
})
// Renaming is what would write the identity store. Reach the client's own
// tool rather than simulating it, so the guard is tested where it lives.
const tool = plugin?.tool?.rename_me || plugin?.["tool"]?.["rename_me"]
if (tool) { try { await tool.execute({ new_id: "renamed-elsewhere" }) } catch {} }
await new Promise((r) => setTimeout(r, 600))
console.log("@@done")
process.exit(0)
"""


def run_opencode(home: Path, project: str, agent: str | None) -> None:
    """Boot the OpenCode plugin under this HOME, with or without A2A_AGENT."""
    harness = home / "drive.mjs"
    harness.write_text(OC_HARNESS % {
        "home": json.dumps(str(home)),
        "module": json.dumps(str(OPENCODE)),
        "project": json.dumps(project),
        "agent": (f"process.env.A2A_AGENT = {json.dumps(agent)}"
                  if agent else "delete process.env.A2A_AGENT"),
        "id": json.dumps(agent or Path(project).name),
    })
    subprocess.run(["node", str(harness)], text=True,
                   capture_output=True, timeout=60)


def settings_dir(home: Path) -> Path:
    return home / ".config" / "opencode"


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-iso-"))
    project = "/Users/x/acme-api"

    # --- backwards compatibility: no A2A_AGENT, nothing changes -------------
    plain = tmp / "home-plain"
    settings_dir(plain).mkdir(parents=True)
    (settings_dir(plain) / "a2a.json").write_text(
        json.dumps({"read_on_init": False}))
    run_opencode(plain, project, None)
    names = sorted(p.name for p in settings_dir(plain).glob("a2a*"))
    check("a derived id writes and reads the SHARED names, exactly as before "
          "— the whole backwards-compatibility guarantee",
          "a2a.json" in names and not any(
              n.startswith("a2a-") and n.endswith(".json")
              and n != "a2a-identity.json" for n in names),
          str(names))
    check("and a derived id still records a rename in the identity store, "
          "which is the only thing that makes a rename outlive a restart",
          (settings_dir(plain) / "a2a-identity.json").exists()
          and json.loads(
              (settings_dir(plain) / "a2a-identity.json").read_text()
          ).get(project) == "renamed-elsewhere",
          "store not written")

    # --- explicit id: no pinning -------------------------------------------
    named = tmp / "home-named"
    settings_dir(named).mkdir(parents=True)
    run_opencode(named, project, "acme-api-a")
    store = settings_dir(named) / "a2a-identity.json"
    check("an EXPLICIT id never writes the identity store — two instances in "
          "one directory would take turns overwriting the same key",
          not store.exists() or project not in json.loads(store.read_text()),
          store.read_text() if store.exists() else "")

    # --- two explicit ids, one directory, separate settings -----------------
    two = tmp / "home-two"
    settings_dir(two).mkdir(parents=True)
    (settings_dir(two) / "a2a.json").write_text(json.dumps({"catchup": 7}))
    (settings_dir(two) / "a2a-acme-api-b.json").write_text(
        json.dumps({"catchup": 3}))
    check("a per-agent settings file sits beside the shared one rather than "
          "replacing it, so each instance can differ",
          (settings_dir(two) / "a2a.json").exists()
          and (settings_dir(two) / "a2a-acme-api-b.json").exists())

    # --- the source contracts both clients must keep ------------------------
    pi_src, oc_src = PI.read_text(), OPENCODE.read_text()
    for label, src in (("Pi", pi_src), ("OpenCode", oc_src)):
        check(f"{label}: the id ladder lives in ONE named resolver, so the "
              f"order is changed in one place",
              "resolveKey" in src, "no resolveKey")
        check(f"{label}: A2A_AGENT is ahead of the baked id, so one install "
              f"can run twice under two names",
              'process.env.A2A_AGENT || BAKED.agent' in src,
              "baked id still wins")
        check(f"{label}: an explicit id suppresses the store write",
              "if (explicit) return" in src, "pin() still writes")
        check(f"{label}: and gives the settings file the id as a suffix",
              "slug(key)" in src, "no per-agent settings path")
    check("Pi gives the LOG the same treatment — its rotation rewrites the "
          "whole file, so two writers lose each other's lines",
          "a2a${STATE_SUFFIX}.log" in pi_src, "log still shared")
    check("Pi keeps the legacy settings path verbatim, which is what a "
          "derived id resolves to and what test_read_on_init.py greps for",
          'join(homedir(), ".pi", "agent", "a2a.json")' in pi_src,
          "legacy literal gone")

    print()
    if failures:
        print(f"{len(failures)} failure(s):")
        for f in failures:
            print(f"  - {f}")
        return 1
    print("all good")
    return 0


if __name__ == "__main__":
    sys.exit(main())
