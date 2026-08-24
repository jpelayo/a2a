#!/usr/bin/env python3
"""Tests for hooks/forget-config.py — clearing the stored config on disable.

    python3 tests/test_forget_config.py

The hook runs on every ConfigChange, so the dangerous direction is not
"fails to delete" but "deletes when it shouldn't": quitting Claude Code, a
`/reload-plugins`, or an unrelated settings edit must all leave the token
alone. Those cases carry the most assertions here.

Runs against a throwaway HOME. Nothing touches your real settings.
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
HOOK = PLUGIN / "a2a" / "hooks" / "forget-config.py"
PLUGIN_ROOT = PLUGIN / "a2a"
KEY = "a2a@skills-dir"
TOKEN = "a2a_st_secret"
AGENT = "acme-api-claudecode-1"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def run(settings, *, enabled=None, raw: str | None = None, cwd=None):
    """Run the hook against a temp HOME. Returns the settings file after."""
    home = Path(tempfile.mkdtemp(prefix="a2a-hook-"))
    (home / ".claude").mkdir()
    path = home / ".claude" / "settings.json"
    if raw is not None:
        path.write_text(raw)
    else:
        s = dict(settings)
        if enabled is not None:
            s["enabledPlugins"] = enabled
        path.write_text(json.dumps(s, indent=2))
    env = dict(os.environ, HOME=str(home),
               CLAUDE_PLUGIN_ROOT=str(PLUGIN_ROOT))
    p = subprocess.run([sys.executable, str(HOOK)], env=env,
                       cwd=str(cwd or home), capture_output=True, text=True)
    after = path.read_text()
    try:
        parsed = json.loads(after)
    except Exception:
        parsed = None
    return parsed, after, p, home


# `agent` is no longer a declared option — 1.4.0 dropped it, because plugin
# config is per USER and a typed id made every project the same agent. It stays
# in this fixture as the leftover an upgraded install still carries: the hook
# clears the whole entry, not named keys, so the stale value must go too. A
# token-only fixture would let a selective regression pass.
BASE = {"pluginConfigs": {KEY: {"options": {"token": TOKEN, "agent": AGENT}}},
        "permissions": {"allow": ["Bash"]}}


def stored(after):
    """The options still on disk, or {} if the entry is gone."""
    return (after.get("pluginConfigs") or {}).get(KEY, {}).get("options", {})


# --- it deletes when disabled -----------------------------------------------
after, _, p, home = run(BASE, enabled={KEY: False})
check("disabled -> token removed", "token" not in stored(after), json.dumps(after))
check("disabled -> legacy agent id removed too", "agent" not in stored(after),
      json.dumps(after))
check("disabled -> whole entry removed", "pluginConfigs" not in after,
      json.dumps(after))
check("disabled -> unrelated settings preserved",
      after.get("permissions") == {"allow": ["Bash"]}, json.dumps(after))
check("disabled -> a backup was written",
      any((home / ".claude" / "backups").glob("settings.json.*")), "no backup")
check("disabled -> says what it did", "cleared stored config" in p.stderr,
      p.stderr)

# A fresh 1.4.0 install stores only the token; clearing it must still empty
# pluginConfigs rather than leaving `{"a2a@mkt": {"options": {}}}` behind.
after, _, _, _ = run(
    {"pluginConfigs": {KEY: {"options": {"token": TOKEN}}}},
    enabled={KEY: False},
)
check("token-only entry -> pluginConfigs removed entirely",
      "pluginConfigs" not in after, json.dumps(after))

# --- it must NOT delete otherwise -------------------------------------------
after, _, _, _ = run(BASE, enabled={KEY: True})
check("enabled -> token kept", stored(after).get("token") == TOKEN, json.dumps(after))
check("enabled -> agent id kept", stored(after).get("agent") == AGENT, json.dumps(after))

after, _, _, _ = run(BASE)          # no enabledPlugins key at all
check("not mentioned in enabledPlugins -> both kept (defaultEnabled)",
      stored(after).get("token") == TOKEN and stored(after).get("agent") == AGENT,
      json.dumps(after))

after, _, _, _ = run(BASE, enabled={"other-plugin@mkt": False})
check("a DIFFERENT plugin disabled -> both kept",
      stored(after).get("token") == TOKEN and stored(after).get("agent") == AGENT,
      json.dumps(after))

after, _, _, _ = run(BASE, enabled={KEY: "false"})
check("string 'false' is not False -> token kept",
      "pluginConfigs" in after, json.dumps(after))

# --- higher-precedence scope wins -------------------------------------------
proj = Path(tempfile.mkdtemp(prefix="a2a-proj-"))
(proj / ".claude").mkdir()
(proj / ".claude" / "settings.json").write_text(
    json.dumps({"enabledPlugins": {KEY: True}}))
after, _, _, _ = run(BASE, enabled={KEY: False}, cwd=proj)
check("project scope says enabled -> user-scope false does NOT delete",
      "pluginConfigs" in after, json.dumps(after))

# --- robustness --------------------------------------------------------------
after, raw, p, _ = run(None, raw="{ this is not json")
check("malformed settings -> writes nothing", raw == "{ this is not json", raw)
check("malformed settings -> still exits 0", p.returncode == 0, str(p.returncode))

after, _, p, _ = run({"pluginConfigs": {}}, enabled={KEY: False})
check("nothing stored -> no-op, exits 0", p.returncode == 0, str(p.returncode))

after, _, p, _ = run({}, enabled={KEY: False})
check("no pluginConfigs key -> no-op", p.returncode == 0, str(p.returncode))

# idempotent: running twice is the same as running once
home = Path(tempfile.mkdtemp(prefix="a2a-twice-"))
(home / ".claude").mkdir()
sp = home / ".claude" / "settings.json"
sp.write_text(json.dumps(dict(BASE, enabledPlugins={KEY: False}), indent=2))
env = dict(os.environ, HOME=str(home), CLAUDE_PLUGIN_ROOT=str(PLUGIN_ROOT))
for _ in range(2):
    subprocess.run([sys.executable, str(HOOK)], env=env, cwd=str(home),
                   capture_output=True, text=True)
check("running twice is idempotent",
      "pluginConfigs" not in json.loads(sp.read_text()), sp.read_text())

# --- the manifest must agree -------------------------------------------------
manifest = json.loads((PLUGIN_ROOT / ".claude-plugin" / "plugin.json").read_text())
check("token is NOT sensitive (else it is in the keychain, not settings.json)",
      "sensitive" not in manifest["userConfig"]["token"],
      json.dumps(manifest["userConfig"]["token"]))
check("the setup screen asks for the token and NOTHING else "
      "(an agent id there would be one identity for every project)",
      set(manifest["userConfig"]) == {"token"},
      str(list(manifest["userConfig"])))
hooks = json.loads((PLUGIN_ROOT / "hooks" / "hooks.json").read_text())["hooks"]
check("hooks.json wires both ConfigChange and SessionStart",
      set(hooks) == {"ConfigChange", "SessionStart"}, str(list(hooks)))

print()
for f in fails:
    print("FAIL", f)
print("FAILED" if fails else "PASS")
sys.exit(1 if fails else 0)
