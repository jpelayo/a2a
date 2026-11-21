#!/usr/bin/env python3
"""a2a — Antigravity Stop hook: at a turn's end, send the agent back when an
a2a message is waiting or nothing would wake it for the next one.

agy runs this (hooks.json) every time a conversation's loop ends, with the
event as JSON on stdin and cwd = the plugin folder. Answering
{"decision": "continue", "reason": …} re-enters the loop with `reason` as a
system message; anything else lets it stop. Measured on agy 1.5.14: it fires
after woken turns too, and `fullyIdle` is false while a background command
runs.

It reads ONLY local files — the mailbox the a2a MCP server writes in
$TMPDIR/a2a-agy/<project hash>/ — and never the network, the token or the
inbox. It is silent (allows the stop) unless every one of these holds:
  - this is the CLI (the app only sends);
  - a live a2a receiver owns that project's mailbox (`live`, its pid alive);
  - the ending conversation is the route (the last root conversation that
    called an a2a tool), or no route is known yet — or no waiter runs and agy
    is fully idle, so the conversation ending is the one in use (a /clear or
    a new conversation in the same agy would otherwise never be nudged);
  - mail is waiting, or no waiter has beaten in BEAT_FRESH_S.
And it gives up after MAX_NUDGES nudges in a row that changed nothing, so a
model that ignores it cannot be held in a loop. Progress is what the nudge
asked for: for mail, an a2a_inbox call; for the waiter, a waiter STARTED
(it writes `waiter` as it starts) — an a2a_inbox call alone is not, or a
waiter that cannot start would be nudged for ever.

The reason names no command: it says to call a2a_inbox and do what that
says. An agy that does not receive (another one holds the project) is told
there not to start a waiter, which it would otherwise take over.

Every failure allows the stop: a hook that errs must never trap a session.
"""
import hashlib
import json
import os
import sys
import tempfile
import time

BEAT_FRESH_S = 10.0
MAX_NUDGES = 2


def project_hash(path):
    """MUST stay identical to a2a-antigravity.py's project_hash."""
    p = os.path.normpath(os.path.realpath(path))
    if sys.platform in ("darwin", "win32"):
        p = p.lower()
    return hashlib.sha256(p.encode("utf8", "replace")).hexdigest()[:16]


def _age(path):
    try:
        return time.time() - os.stat(path).st_mtime
    except OSError:
        return None


def _mtime(path):
    try:
        return os.stat(path).st_mtime
    except OSError:
        return 0.0


def _alive(pid):
    try:
        os.kill(int(pid), 0)
    except PermissionError:
        return True
    except (OSError, ValueError, TypeError):
        return False
    return True


def _read_json(path):
    try:
        with open(path) as f:
            out = json.load(f)
        return out if isinstance(out, dict) else {}
    except (OSError, ValueError):
        return {}


def _write_json(path, data):
    try:
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "w") as f:
            json.dump(data, f)
        os.replace(tmp, path)
    except OSError:
        pass


def decide(event, env=None, now=None):
    """The answer for one Stop event: {} (stop) or a continue."""
    env = os.environ if env is None else env
    now = time.time() if now is None else now
    data_dir = env.get("PLUGIN_DATA") or ""
    if data_dir and "antigravity-cli" not in data_dir.split(os.sep):
        return {}                                   # the app: send-only
    base = os.path.join(env.get("TMPDIR") or tempfile.gettempdir(), "a2a-agy")
    box = live = None
    for ws in event.get("workspacePaths") or []:
        if not isinstance(ws, str) or not ws:
            continue
        d = os.path.join(base, project_hash(ws))
        info = _read_json(os.path.join(d, "live"))
        if info and _alive(info.get("pid")):
            box, live = d, info
            break
    if not box:
        return {}                                   # no receiver: not ours
    conv = event.get("conversationId") or ""
    route = live.get("route") or ""
    beat = _age(os.path.join(box, "beat"))
    # fullyIdle: agy says no background command runs at all, so no waiter
    # can, whatever `beat` says (measured: false while one runs).
    no_waiter = beat is None or beat > BEAT_FRESH_S \
        or event.get("fullyIdle") is True
    if route and conv and conv != route \
            and not (no_waiter and event.get("fullyIdle") is True):
        return {}                                   # another conversation

    try:
        with open(os.path.join(box, "mail")) as f:
            mail = f.read().strip()
    except OSError:
        mail = ""
    if mail:
        why, reason = "mail", (
            f"a2a: {mail}. Call the a2a_inbox tool now, answer with the a2a "
            "tools, then do exactly what the last line of a2a_inbox says, and "
            "end your turn.")
    elif no_waiter:
        why, reason = "waiter", (
            "a2a: nothing will wake you when the next a2a message arrives, "
            "because the a2a waiter is not running. Call the a2a_inbox tool "
            "now (it hands over anything waiting), do exactly what its last "
            "line says — it names the waiter to start in the BACKGROUND, or "
            "says not to — and end your turn.")
    else:
        _write_json(os.path.join(box, "guard.json"),
                    {"conv": conv, "n": 0, "last": "stop", "at": now})
        return {}

    guard = _read_json(os.path.join(box, "guard.json"))
    started = _mtime(os.path.join(box, "waiter"))
    progress = (max(_mtime(os.path.join(box, "inbox")), started)
                if why == "mail" else started)
    n = guard.get("n") or 0
    if guard.get("conv") != conv or guard.get("last") != why \
            or progress > (guard.get("at") or 0):
        n = 0                       # something happened since the last nudge
    if n >= MAX_NUDGES:
        _write_json(os.path.join(box, "guard.json"),
                    dict(guard, last=why, gave_up=True, at=now))
        return {}
    _write_json(os.path.join(box, "guard.json"),
                {"conv": conv, "n": n + 1, "last": why, "gave_up": False,
                 "at": now})
    return {"decision": "continue", "reason": reason}


def main():
    try:
        raw = sys.stdin.read()
        event = json.loads(raw) if raw.strip() else {}
        out = decide(event if isinstance(event, dict) else {})
    except Exception:
        out = {}
    sys.stdout.write(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
