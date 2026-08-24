#!/usr/bin/env python3
"""Agent-identity conformance test.

    python3 tests/test_agent_id.py

One session must claim exactly one id, and two installs must never claim the
same one. Two pieces of code announce it and they must never disagree:

  1. the headersHelper in a2a/.mcp.json, which Claude Code runs to build
     X-A2A-Agent for the `a2a` HTTP server
  2. the channel in a2a/server/a2a-channel.py, which streams under it

They agree by reading one file — the identity store — rather than by computing
the same thing twice. If they drift, one session talks to the broker as two
agents: the tools post as one id while the channel streams as another, and
messages addressed to the channel land in a receipt nothing ever acks. That
failure is silent, which is why it is pinned here.

The id is stored, not derived. A derived id is the same string for every client
launched from one directory, and delivery is a destructive read, so two of them
split a single inbox at random — which is exactly what happened when a Claude
session and an OpenCode session shared a project.

Requires python3 only. No pip, no test framework.
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
MCP_JSON = PLUGIN / "a2a" / ".mcp.json"
CHANNEL = PLUGIN / "a2a" / "server" / "a2a-channel.py"



# --- 1. the header, as Claude Code expands it --------------------------------

def expand(value: str, env: dict) -> str:
    """${VAR} and ${VAR:-default}, per the documented .mcp.json syntax.

    Innermost-first so ${A2A_AGENT:-${CLAUDE_PROJECT_DIR}} resolves, and
    repeated until stable so concatenated expansions all land.
    """
    pat = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)(?::-([^{}]*))?\}")
    for _ in range(10):
        new = pat.sub(lambda m: env.get(m.group(1)) or (m.group(2) or ""), value)
        if new == value:
            return new
        value = new
    raise AssertionError(f"expansion did not settle: {value!r}")


def header_id(project_dir: str, store: str) -> str:
    """Run the real headersHelper, exactly as Claude Code would."""
    cmd = json.loads(MCP_JSON.read_text())["mcpServers"]["a2a"]["headersHelper"]
    cmd = expand(cmd, {
        "CLAUDE_PLUGIN_ROOT": str(PLUGIN / "a2a"),
        "CLAUDE_PROJECT_DIR": project_dir,
        "CLAUDE_PLUGIN_DATA": store,
    })
    out = subprocess.run(cmd, shell=True, capture_output=True, text=True,
                         check=True)
    return json.loads(out.stdout)["X-A2A-Agent"]


# --- 2. the Claude channel ---------------------------------------------------

def channel_id(project_dir: str, store: str) -> str:
    """What the channel announces, from the same store the helper reads."""
    out = subprocess.run(
        [sys.executable, str(CHANNEL.parent / "identity.py"), "get",
         "--project", project_dir, "--store", store],
        capture_output=True, text=True, check=True,
        env={k: v for k, v in os.environ.items() if not k.startswith("A2A_")},
    )
    return out.stdout.strip()


# --- the contract ------------------------------------------------------------
# One session, one id: the headersHelper and the channel must announce the same
# string, because they read the same store. And two installs must NOT, however
# alike their projects look — that is the collision that made two harnesses one
# agent, and it is the reason identity is stored rather than derived.


def main() -> int:
    fails = []
    tmp = Path(tempfile.mkdtemp(prefix="a2a-ids-"))
    a_store, b_store = str(tmp / "install-a"), str(tmp / "install-b")
    project = "/Users/x/acme-api"

    # The regression this file exists for: an id that comes from anywhere but
    # the client's own store is an id two clients can compute identically.
    server = json.loads(MCP_JSON.read_text())["mcpServers"]["a2a"]
    if "X-A2A-Agent" in (server.get("headers") or {}):
        fails.append(
            "X-A2A-Agent is a static header again: it must come from the "
            "headersHelper, or the client cannot change what it announces"
        )
    if "user_config" in (server.get("headersHelper") or ""):
        fails.append(
            "headersHelper references user_config, which Claude Code refuses "
            "to substitute in a shell-parsed command"
        )

    h, c = header_id(project, a_store), channel_id(project, a_store)
    print(f"{'same install':<24} helper={h:<20} channel={c}")
    if h != c:
        fails.append(f"helper says {h!r}, channel says {c!r} — one session, "
                     f"two identities on the broker")

    h2 = header_id(project, b_store)
    print(f"{'second install':<24} helper={h2}")
    # Before either has renamed itself they share the compatibility default;
    # once one records a name, they must part company for good.
    subprocess.run(
        [sys.executable, str(CHANNEL.parent / "identity.py"), "set",
         "bozo-the-clown", "--project", project, "--store", a_store],
        capture_output=True, check=True)
    h, h2 = header_id(project, a_store), header_id(project, b_store)
    print(f"{'after one renames':<24} a={h:<20} b={h2}")
    if h == h2:
        fails.append(f"both installs announce {h!r}: delivery is a destructive "
                     f"read, so they would split one inbox at random")
    if h != "bozo-the-clown":
        fails.append(f"the recorded name is not what gets announced: {h!r}")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — one session one id, two installs two ids")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
