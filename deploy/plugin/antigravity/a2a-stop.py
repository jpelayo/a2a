#!/usr/bin/env python3
"""a2a — Antigravity hooks: at a turn's end (Stop), send the agent back when
an a2a message is waiting or nothing would wake it for the next one; at a new
conversation's first prompt (SessionStart, see decide_start), ask it to start
the waiter in that same turn, only where .a2a.json turns a2a on.

agy runs this (hooks.json) every time a conversation's loop ends, with the
event as JSON on stdin and cwd = the plugin folder. Answering
{"decision": "continue", "reason": …} re-enters the loop with `reason` as a
system message; anything else lets it stop. Measured on agy 1.5.14: it fires
after woken turns too. It does not trust `fullyIdle` to say whether the
waiter runs: a real run said true while it did.

It reads ONLY local files — the mailbox the a2a MCP server writes in
<PLUGIN_DATA>/boxes/<project hash>/ — and never the network, the token or the
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


def _receiver(event, env):
    """(box, live, workspace) of the live a2a receiver for this event's
    workspace, or (None, None, None): the app, or no receiver here."""
    data_dir = env.get("PLUGIN_DATA") or ""
    if data_dir and "antigravity-cli" not in data_dir.split(os.sep):
        return None, None, None                     # the app: send-only
    # MUST match a2a-antigravity.py's MAILBOX and wait.sh's search.
    base = os.path.join(
        data_dir or os.path.join(os.path.expanduser("~"), ".gemini",
                                 "antigravity-cli", "plugin_data", "a2a"),
        "boxes")
    for ws in event.get("workspacePaths") or []:
        if not isinstance(ws, str) or not ws:
            continue
        d = os.path.join(base, project_hash(ws))
        info = _read_json(os.path.join(d, "live"))
        if info and _alive(info.get("pid")):
            return d, info, ws
    return None, None, None


def _waiter(box):
    """(beat age or None, a waiter pid alive)."""
    def pid_in(name):
        try:
            with open(os.path.join(box, name)) as f:
                p = f.read().strip()
        except OSError:
            return False
        return bool(p) and _alive(p)
    # A waiter runs while its beat is fresh AND its pid lives: a2a_inbox's
    # waiter_running() rule. The pid covers one killed outright (fresh beat,
    # dead pid). `starting` holds the pid of one still in its agentapi check,
    # which ends the turn that started it right away. NOT agy's fullyIdle: it
    # said true with the waiter running outside the sandbox (a real run, agy
    # 1.5.14), and the hook sent the agent back twice for a waiter that was
    # there.
    return (_age(os.path.join(box, "beat")),
            pid_in("waiter") or pid_in("starting"))


def decide_start(event, env=None):
    """SessionStart: agy fires it once, as the first prompt of a NEW
    conversation is sent (measured: not at launch, not on `agy -c`). When
    this project is on and no waiter runs, it asks the agent, in that same
    turn, to call a2a_inbox and do what it says — which starts the waiter —
    so the Stop hook need not send it back for one afterwards. A system
    message, not a user message: a user message took the place of the
    person's prompt as `lastUserInput` (the probe)."""
    env = os.environ if env is None else env
    box, _live, ws = _receiver(event, env)
    if not box:
        return {}
    try:
        with open(os.path.join(ws, ".a2a.json")) as f:
            on = json.load(f).get("enabled_antigravity") is True
    except (OSError, ValueError, AttributeError):
        on = False
    if not on:
        return {}
    beat, alive = _waiter(box)
    if beat is not None and beat <= BEAT_FRESH_S and alive:
        return {}                                   # already armed
    return {"injectSteps": [{"systemMessage": {"systemMessage": (
        "a2a is on in this project, and nothing will wake you when an a2a "
        "message arrives until its waiter runs. Before you answer, call the "
        "a2a_inbox tool once and do exactly what its last line says (it may "
        "name the a2a waiter to start in the BACKGROUND, or say not to). "
        "Then answer the person's message as usual.")}}]}


def decide(event, env=None, now=None):
    """The answer for one Stop event: {} (stop) or a continue."""
    env = os.environ if env is None else env
    now = time.time() if now is None else now
    box, live, _ws = _receiver(event, env)
    if not box:
        return {}                                   # no receiver: not ours
    conv = event.get("conversationId") or ""
    route = live.get("route") or ""
    beat, alive = _waiter(box)
    no_waiter = beat is None or beat > BEAT_FRESH_S or not alive
    # What it saw, kept in guard.json for the doctor's push.hook: what agy
    # reports and what the files say, side by side.
    saw = {"fully_idle": event.get("fullyIdle"),
           "beat_s": None if beat is None else round(beat, 1),
           "waiter_alive": alive}
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
                    {"conv": conv, "n": 0, "last": "stop", "at": now,
                     "saw": saw})
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
                    dict(guard, last=why, gave_up=True, at=now, saw=saw))
        return {}
    _write_json(os.path.join(box, "guard.json"),
                {"conv": conv, "n": n + 1, "last": why, "gave_up": False,
                 "at": now, "saw": saw})
    return {"decision": "continue", "reason": reason}


def main():
    try:
        raw = sys.stdin.read()
        event = json.loads(raw) if raw.strip() else {}
        event = event if isinstance(event, dict) else {}
        out = (decide_start(event)
               if event.get("hookEventName") == "SessionStart"
               else decide(event))
    except Exception:
        out = {}
    sys.stdout.write(json.dumps(out))
    return 0


if __name__ == "__main__":
    sys.exit(main())
