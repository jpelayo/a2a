#!/usr/bin/env python3
"""The clients must call themselves what the broker calls them.

    python3 tests/test_identity_brief.py

A client sends a per-project KEY and the broker resolves it to a NAME. Two
things must follow the name and not the key:

  the brief   — "You are a2a agent X" is what the model believes it is, and
                what it will answer to when another agent @mentions it.
  the sender  — post_to_channel trusts its `sender` argument (the broker does
                NOT derive it from the header), so signing with a stale key
                attributes messages to an id nobody can address.

Both were wrong until the key/name split: the brief was built from the derived
key at startup and the broker was never asked. This pins it against a live
broker, so it covers the REST rename route and the resolution path too.

Needs python3 (with mcp/uvicorn/starlette), node, and a free port.
"""
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import dbharness

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"
CHANNEL = PLUGIN / "a2a" / "server" / "a2a-channel.py"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"

KEY = "Atlas"            # the project directory, sent forever
NAME = "atlas-desk"      # what the agent renames itself to

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-brief-"))
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, **dbharness.db_env(), A2A_AUTH_DISABLED="1",
               A2A_HOST="127.0.0.1", A2A_PORT=str(port))

    subprocess.run(
        [sys.executable, str(BROKER), "agent", "add", KEY,
         "--station", "default"],
        env=env, capture_output=True, text=True, check=True)

    proc = subprocess.Popen(
        [sys.executable, str(BROKER), "serve"], env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    ch = None
    try:
        def api(path, body=None, method="GET"):
            req = urllib.request.Request(
                f"{base}{path}",
                data=json.dumps(body).encode() if body is not None else None,
                headers={"Content-Type": "application/json",
                         "X-A2A-Agent": KEY},
                method=method)
            with urllib.request.urlopen(req, timeout=10) as r:
                return json.load(r)

        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("broker did not come up")

        # --- rename over REST, which is the only route OpenCode can use -----
        out = api(f"/me/agents/{KEY}", {"rename": NAME}, method="PATCH")
        check("PATCH /me/agents/<key> renames", out.get("agent_id") == NAME,
              json.dumps(out))

        me = api("/me")
        check("the renamed-away id stops working at once — nothing redirects "
              "it, which is the whole reason aliases are gone",
              me.get("agent") == KEY and not me.get("registered"),
              json.dumps(me))

        # --- the Claude channel's brief -------------------------------------
        # The client announces what it stores; the brief must name that.
        cenv = dict(os.environ, A2A_URL=base, A2A_TOKEN="dev",
                    A2A_AGENT=NAME, A2A_HELLO="0")
        ch = subprocess.Popen(
            [sys.executable, str(CHANNEL)], env=cenv, text=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, bufsize=1)
        ch.stdin.write(json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05"}}) + "\n")
        ch.stdin.flush()
        brief = json.loads(ch.stdout.readline())["result"]["instructions"]

        check("the brief names the id this client announces", NAME in brief,
              brief[:160])
        check("and not the one it was renamed away from",
              f'You are a2a agent "{KEY}"' not in brief, brief[:160])

        # --- the OpenCode twin ----------------------------------------------
        src = OPENCODE.read_text()
        body = src[src.index("const instructions ="):src.index("const esc =")]
        script = (
            body + ";process.stdout.write(instructions("
            + json.dumps(NAME) + "))"
        )
        js = subprocess.run(["node", "-e", script], capture_output=True,
                            text=True, check=True).stdout
        check("opencode brief names whatever it is given", NAME in js, js[:160])
        check("opencode brief no longer tells the model to pass a sender",
              'sender="' not in js.split("Inbound messages")[0], js[:200])

        # The three places a message gets signed. A key there is the bug this
        # file exists for, and it is invisible until another agent replies.
        for tool in ("post_to_channel", "send_dm", "submit_bid"):
            seg = src[src.index(f"{tool}: {{"):]
            seg = seg[:seg.index("},\n\n") + 1] if "},\n\n" in seg else seg[:600]
            signed_with_key = "sender: key" in seg or "agent_id: key" in seg
            check(f"{tool} signs with the resolved name, not the key",
                  not signed_with_key
                  and ("sender: name" in seg or "agent_id: name" in seg),
                  seg[:200])
    finally:
        if ch:
            ch.terminate()
            ch.wait(timeout=10)
        proc.terminate()
        proc.wait(timeout=10)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — both clients answer to the broker's name")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
