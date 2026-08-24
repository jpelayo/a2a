#!/usr/bin/env python3
"""What the channel is allowed to inject into a session.

    python3 tests/test_channel_notify.py

The session transcript belongs to the agent, not to the plugin. The channel
may inject exactly two things:

  1. messages that arrived from the broker
  2. one "channel online" line at startup, suppressible with A2A_HELLO=0

Everything else — an unknown agent id, a dead broker, a refused stream — is a
log line. In particular the channel must NOT tell the agent to register, or
which name to register under: whether to register is the agent's decision, it
has register_me and my_realm for that, and a retry loop that never ends turns
any injected advice into an endless nag.

Both twins are checked, because the Claude and OpenCode implementations drifted
apart on exactly this before.

No deps.
"""
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
CHANNEL = PLUGIN / "a2a" / "server" / "a2a-channel.py"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"

py = CHANNEL.read_text()
js = OPENCODE.read_text()

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


# --- the 403 / unregistered path stays silent --------------------------------
pump = py[py.index("def _pump"):py.index("def main")]
start = pump.index("if e.code == 403")
# Search for the terminator AFTER the branch: an earlier `except Exception`
# guards the body read, and slicing to it produced an empty branch.
branch = pump[start:pump.index("except Exception", start)]
check("claude: 403 path injects nothing", "_notify(" not in branch,
      "the unregistered branch still pushes into the session")
check("claude: 403 path still backs off", "min(300" in branch,
      "no capped backoff")

jbranch = js[js.index('if (res.status === 403'):js.index("throw new Error(`HTTP")]
check("opencode: 403 path injects nothing", "push(" not in jbranch,
      "the unregistered branch still pushes into the session")

# --- no naming advice anywhere ----------------------------------------------
ADVICE = ["A2A_AGENT_TAIL=", "export A2A_AGENT", "which the broker does not know",
          "To use ", "and restart"]
for needle in ADVICE:
    check(f"claude: no naming advice ({needle!r})", needle not in py,
          "found stale rename instructions")

# The removed helpers must stay removed; they existed only to build that advice.
for gone in ("_unregistered_message", "_known_agents", "_notify_once"):
    check(f"claude: {gone} is gone", gone not in py, "still present")

# --- what IS allowed ---------------------------------------------------------
notifies = re.findall(r"^\s*_notify\(", py, re.M)
# Four, and the count is the assertion: every one of these costs the user
# context in the session they are working in, so a fifth has to be argued for
# rather than added.
#
#   1 a delivered message      2 the hello
#   3 the startup channel check — off by default here, and behind
#     read_on_init when it is not
#   4 "this install is behind the broker", said once per session and only
#     when it is true. Argued for by the alternative: a tool silently absent
#     with nothing anywhere explaining it, which is what happened to
#     propose_me and cost an afternoon.
check("claude: exactly four injection sites remain (message + hello + "
      "channel check + stale-install notice)",
      len(notifies) == 4, f"{len(notifies)} found")
check("claude: the stale-install notice is one of them, and fires only when "
      "the versions actually differ",
      "_check_version()" in py and "theirs != mine" in py, "not gated")
check("claude: the channel check is one of them, and gated",
      "_check_channels()" in py and "if CATCHUP <= 0:" in py, "no gate")
check("claude: it ASKS the agent to read rather than reading for it — a "
      "client-side read is invisible and acks with nothing on screen",
      "read_channel (limit" in py and "?limit=" not in py,
      "the client still fetches transcripts itself")
check("claude: the hello is suppressible",
      'A2A_HELLO' in py and 'if HELLO:' in py, "no A2A_HELLO gate")
check("claude: real messages are still delivered, and the body opens with "
      "the sender — the host renders only the content on the transcript "
      "line, so without this a human cannot tell one peer from another",
      '_notify(f"\u2039{who}\u203a {text}" if who else text, meta)' in py,
      "the delivery path is gone")

# --- the advice must not come back through the instructions ------------------
instr = py[py.index("def _instructions("):py.index("_ready = threading.Event()")]
check("claude: instructions do not order the agent to register",
      "register_me" not in instr or "must register" not in instr,
      "instructions push registration on the agent")

print()
for f in fails:
    print("FAIL", f)
print("FAILED" if fails else "PASS")
sys.exit(1 if fails else 0)
