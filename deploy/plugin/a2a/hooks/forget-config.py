#!/usr/bin/env python3
"""Forget this plugin's stored config once the plugin is disabled.

Wired to ConfigChange (fires when /plugin writes enabledPlugins) and to
SessionStart (the backstop, for a disable that happened with no session open).

Claude Code runs nothing on disable and keeps stored config across reinstalls
by design, so a token entered once outlives the plugin unless something clears
it. This is that something. It answers one question — is this plugin disabled
now? — and if so deletes its entry under `pluginConfigs`, so re-enabling asks
for the token again instead of silently reusing it. Disabling IS uninstalling
here; nothing removes the plugin's folder.

It deletes the plugin's whole pluginConfigs entry — the token, the `agent` id
left behind by versions before 1.4.0, and any option added later — rather than
named keys, so nothing can be forgotten selectively by accident. It never reads those values and never touches the
keychain: they live in ~/.claude/settings.json precisely so a JSON key deletion
is enough. (That is why `token` must NOT be declared `sensitive` — a sensitive
value goes into the Keychain item shared with your OAuth session, which nothing
here is allowed to rewrite.)

Exit code is always 0. A cleanup hook that fails a session is worse than a
token that lingers one more start.
"""
import json
import os
import shutil
import sys
import time
from pathlib import Path

HOME = Path.home()
# pluginConfigs is read from user settings only, so that is the one file that
# can be holding the token.
USER_SETTINGS = HOME / ".claude" / "settings.json"
BACKUPS = HOME / ".claude" / "backups"

# enabledPlugins, unlike pluginConfigs, is honoured at several scopes. Highest
# precedence first: a project-scoped `true` must not be overridden by a stale
# user-scoped `false`, or we would delete a token that is still in use.
ENABLED_SOURCES = [
    Path("/Library/Application Support/ClaudeCode/managed-settings.json"),
    Path.cwd() / ".claude" / "settings.local.json",
    Path.cwd() / ".claude" / "settings.json",
    USER_SETTINGS,
]


def _load(path: Path) -> dict:
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def plugin_name() -> str:
    """This plugin's name, from its own manifest; dir name as the fallback."""
    root = Path(os.environ.get("CLAUDE_PLUGIN_ROOT") or Path(__file__).parent.parent)
    name = _load(root / ".claude-plugin" / "plugin.json").get("name")
    return name or root.name


def config_key(options: dict, name: str) -> str | None:
    """The pluginConfigs key for this plugin.

    Keys are "<name>@<marketplace>" and the marketplace is not knowable from
    inside the plugin, so match on the name half. An exact match is honoured
    too, in case a future version drops the suffix.
    """
    for key in options:
        if key == name or key.startswith(f"{name}@"):
            return key
    return None


def is_disabled(name: str) -> bool:
    """Effective enabled state. Absent everywhere counts as NOT disabled.

    Being unlisted is the state of a plugin that is loading normally under
    defaultEnabled, so it must never trigger deletion — only an explicit
    `false` does.
    """
    for path in ENABLED_SOURCES:
        enabled = _load(path).get("enabledPlugins")
        if not isinstance(enabled, dict):
            continue
        key = config_key(enabled, name)
        if key is not None:
            return enabled[key] is False
    return False


def main() -> int:
    name = plugin_name()
    if not is_disabled(name):
        return 0                      # enabled, or simply not mentioned

    settings = _load(USER_SETTINGS)
    configs = settings.get("pluginConfigs")
    if not isinstance(configs, dict):
        return 0
    key = config_key(configs, name)
    if key is None:
        return 0                      # already forgotten; this is a no-op

    try:
        BACKUPS.mkdir(parents=True, exist_ok=True)
        stamp = time.strftime("%Y%m%d-%H%M%S")
        backup = BACKUPS / f"settings.json.{stamp}"
        shutil.copy2(USER_SETTINGS, backup)
        os.chmod(backup, 0o600)

        configs.pop(key)
        if not configs:
            settings.pop("pluginConfigs")

        # Same directory, then replace: a crash mid-write cannot leave a
        # truncated settings.json behind.
        tmp = USER_SETTINGS.with_suffix(".json.a2a-tmp")
        tmp.write_text(json.dumps(settings, indent=2) + "\n")
        os.chmod(tmp, 0o600)
        os.replace(tmp, USER_SETTINGS)
    except Exception as e:
        print(f"[a2a] could not clear stored config: {e}", file=sys.stderr)
        return 0

    print(f"[a2a] plugin disabled — cleared stored config for {key} "
          f"(backup: {backup})", file=sys.stderr)
    return 0


if __name__ == "__main__":
    sys.exit(main())
