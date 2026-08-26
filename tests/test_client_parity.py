#!/usr/bin/env python3
"""The clients must offer the same vocabulary.

    python3 tests/test_client_parity.py

Four clients now speak to the same broker, and three of them (OpenCode, Pi and
Codex) carry their own tool surface because those harnesses either lack MCP,
refuse it by policy, or — in Codex's case — cannot persist the header the
broker's own MCP endpoint needs without editing a config file. That means every
capability exists several times, and the failure mode is quiet: a tool is added
to one client, the agents on that harness can do something the others cannot,
and nobody notices until an agent asks a peer to do a thing its client has no
word for.

This compares the three hand-written surfaces against each other, and all of
them against the broker's own MCP tools, so a name added in one place has to be
added in the others or the build fails.

Pure python3 — it reads sources rather than running any harness, because
neither OpenCode nor Pi is installed here.
"""
import asyncio
import importlib.util
import os
import re
import sys
import tempfile
from pathlib import Path

import dbharness

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
PI = PLUGIN / "pi" / "index.ts"
CODEX = PLUGIN / "codex" / "a2a-codex.py"

# Tools that legitimately exist on only one surface, with the reason.
ONLY_BROKER = {
    # Pull-side and admin-ish reads the hand-written clients have not needed.
    "my_realm", "list_my_agents", "bind_me", "unbind_me", "move_me",
    "get_channel", "add_channel_member", "remove_channel_member",
    "evict_off_project", "ping_me", "list_broadcasts", "get_broadcast",
    "close_broadcast", "broadcast",
    # share_md/fetch_md were here, listed as "not needed". They were: an agent
    # was handed an md:// URI, had no tool that could open it, went looking for
    # a resource server, and asked its peer to paste the file into the channel.
    # Both clients carry them now, so this list is what keeps them there.
    # The clients own renaming: it writes the local identity store as well as
    # the broker, which a broker-side tool cannot do.
    "rename_me",
}
ONLY_CLIENTS = {
    # Membership convenience the broker expresses as add/remove_channel_member.
    "join_channel", "leave_channel",
    # Client-side, for the reason above.
    "rename_me",
    # Asking an operator to register this id. It CANNOT be an MCP tool: an
    # unregistered agent is denied every path except /me/*, which is exactly
    # when this is needed. So it lives on the clients whose tools are local
    # REST wrappers and keep working unregistered.
    "propose_me",
    # Writes <project>/.a2a.json, which is a fact about THIS MACHINE's install
    # scope. The broker has no opinion about which of your directories use it.
    "enable_a2a_here",
}

# Tools that configure THIS install rather than name a capability. The parity
# rule exists so an agent asking a peer for something need not know which
# harness answered — and "use a2a in my project" is not something you ask a
# peer. TEMPORARY: it empties again when Codex and Claude Code get the same
# project switch. Read the note above DIAGNOSTIC before adding a second name.
INSTALL_SCOPE = {"enable_a2a_here"}

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def opencode_tools() -> set[str]:
    """Names in the `tool: { … }` object of the OpenCode plugin."""
    src = OPENCODE.read_text()
    body = src[src.index("    tool: {"):]
    return set(re.findall(r"^      ([a-z_][a-z0-9_]*): \{", body, re.M))


def pi_tools() -> set[str]:
    return set(re.findall(r'pi\.registerTool\(\{\s*\n\s*name: "([^"]+)"',
                          PI.read_text()))


def codex_tools() -> set[str]:
    """Names in the Codex client's TOOLS table."""
    src = CODEX.read_text()
    body = src[src.index("TOOLS = ["):]
    return set(re.findall(r'^    \("([a-z_][a-z0-9_]*)",', body, re.M))


def status_shapes() -> dict:
    """Every client's REAL status builder, run with the network stubbed.

    The python clients are imported; the JS/TS ones have `statusReport` lifted
    out verbatim and run under node, exactly as the envelope contract does —
    a copy of the function would drift from the client and pass while the
    client was broken.
    """
    import json as _json
    import re as _re
    import subprocess as _sub
    import sys as _sys

    out = {}
    os.environ.update(A2A_URL="http://127.0.0.1:1", A2A_TOKEN="t",
                      A2A_CODEX_SOCK="")

    spec = importlib.util.spec_from_file_location("parity_codex", CODEX)
    cx = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cx)
    cx.api = lambda m, p, body=None, timeout=30: (
        {"agent": "x", "stations": ["s"], "registered": True} if p == "/me"
        else {"channels": []})
    out["codex"] = _json.loads(cx._status({}))

    claude = PLUGIN / "a2a" / "server" / "a2a-channel.py"
    _sys.path.insert(0, str(claude.parent))
    spec = importlib.util.spec_from_file_location("parity_claude", claude)
    ch = importlib.util.module_from_spec(spec)
    _sys.modules["parity_claude"] = ch
    spec.loader.exec_module(ch)
    ch._me_view = lambda: {"agent": "x", "stations": ["s"], "registered": True}
    ch._my_channels = lambda: []
    sent = []
    ch._send = lambda o: sent.append(o)
    ch._handle({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                "params": {"name": "a2a_channel_status", "arguments": {}}})
    out["claude"] = _json.loads(sent[-1]["result"]["content"][0]["text"])

    for label, path in (("opencode", OPENCODE), ("pi", PI)):
        src = path.read_text()
        i = src.index("async function statusReport")
        body = _re.sub(r"^  ", "", src[i:src.index("\n  }", i) + 4], flags=_re.M)
        pre = ('const key="x", name="x", toAck=new Set(), LOG="/tmp/x.log";\n'
               'const BAKED={version:"0"};\n'
               'const pumpState={connected:true,lastLine:Date.now(),'
               'delivered:0,lastError:null,last:null};\n'
               'const api=async(m,p)=>p==="/me"?JSON.stringify({agent:"x",'
               'stations:["s"],registered:true}):'
               'JSON.stringify({channels:[]});\n')
        tmp = Path(tempfile.mkdtemp(prefix="a2a-status-")) / f"d{path.suffix}"
        tmp.write_text(pre + body + "\nstatusReport()"
                       ".then(s=>console.log('@@'+s))\n")
        res = _sub.run(["node", str(tmp)], capture_output=True, text=True,
                       timeout=60)
        # console.log prints MULTI-LINE json: split on the marker, do not take
        # the first line, which is just "{".
        if "@@" in res.stdout:
            out[label] = _json.loads(res.stdout.split("@@", 1)[1])
        else:
            print(f"  (could not run {label} statusReport: "
                  f"{res.stderr.strip()[:160]})")
    return out


def broker_tools() -> set[str]:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-parity-"))
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("a2a_broker", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return {t.name for t in asyncio.run(mod.mcp.list_tools())}


# `a2a_channel_status` used to be exempt here, as a per-client diagnostic. It
# is not one any more: an OpenCode agent that could not ask "am I registered,
# is push alive, which rooms am I in" answered into a channel it was not a
# member of, tried to DM a label, and reported the probe as broken. All three
# hand-written clients carry it now, so this list keeps them carrying it.
DIAGNOSTIC: set[str] = set()


def main() -> int:
    oc, pi, broker = opencode_tools(), pi_tools(), broker_tools()
    cx = codex_tools() - DIAGNOSTIC
    check("the OpenCode plugin registers tools at all", bool(oc), str(oc))
    check("the Pi extension registers tools at all", bool(pi), str(pi))
    check("the Codex client registers tools at all", bool(cx), str(cx))

    # --- the three hand-written clients must match exactly -----------------
    check("OpenCode and Pi expose the same tools — a capability on one "
          "harness and not the other is invisible until an agent asks a peer "
          "to do something its client cannot name",
          oc == pi, f"only in opencode: {sorted(oc - pi)}; only in pi: {sorted(pi - oc)}")
    check("and Codex exposes exactly the same vocabulary as the other two, "
          "for the same reason: an agent asking a peer for something must not "
          "have to know which harness answered",
          cx == oc - INSTALL_SCOPE,
          f"only in codex: {sorted(cx - oc)}; "
          f"missing: {sorted(oc - INSTALL_SCOPE - cx)}")

    # --- and neither may drift from the broker without a stated reason -----
    missing = (broker - oc) - ONLY_BROKER
    check("no broker tool is missing from the clients without being listed "
          "as broker-only", not missing, f"unaccounted: {sorted(missing)}")
    invented = (oc - broker) - ONLY_CLIENTS
    check("no client tool exists that the broker cannot serve",
          not invented, f"unaccounted: {sorted(invented)}")

    # --- the lockdown holds on every surface -------------------------------
    for forbidden in ("create_agent", "delete_agent", "register_me",
                      "delete_channel"):
        check(f"no client offers {forbidden}",
              forbidden not in oc and forbidden not in pi
              and forbidden not in cx)

    # --- and they must hit the same REST paths for the same capability -----
    def paths(src: str) -> set[str]:
        return set(re.findall(r'api\(\s*"(?:GET|POST|PATCH|DELETE)",\s*[`"]([^`"$]+)', src))
    shared = paths(OPENCODE.read_text()) & paths(PI.read_text())
    check("the two clients call the same broker routes", len(shared) >= 5,
          f"shared: {sorted(shared)}")
    # Codex calls the same routes through its own helper, so the pattern
    # differs while the paths must not.
    cx_paths = set(re.findall(r'api_text\(\s*"(?:GET|POST|PATCH|DELETE)",\s*[f]?"([^"{]+)',
                              CODEX.read_text()))
    check("and Codex calls those same routes",
          len({p for p in cx_paths if p in shared}) >= 3,
          f"codex: {sorted(cx_paths)}")

    # --- the orient call must answer the same questions everywhere ----------
    # It is only useful if an agent can rely on the answer being there. The
    # shape is checked by RUNNING each client's real builder, not by grepping
    # for key names, because a key that exists in the source and is never
    # emitted helps nobody.
    shapes = status_shapes()
    for label, shape in shapes.items():
        check(f"{label}: a2a_channel_status answers the four questions — who "
              f"am I, am I registered, which rooms am I in, is push alive",
              {"agent", "registered", "channels", "push"} <= set(shape),
              str(sorted(shape)))
        check(f"{label}: and carries next_step, which names the ONE thing to "
              f"do when something is wrong (null when nothing is)",
              "next_step" in shape, str(sorted(shape)))
    if shapes:
        keysets = {tuple(sorted(v)) for v in shapes.values()}
        check("all clients emit the same top-level status keys, so an agent "
              "reading the answer does not have to know its own harness",
              len(keysets) == 1,
              str({k: sorted(v) for k, v in shapes.items()}))
        shared = set.intersection(*(set(v["push"]) for v in shapes.values()))
        check("and the push block shares the fields that decide whether "
              "delivery is alive",
              {"enabled", "stream_connected", "stale", "last_error",
               "delivered_this_session"} <= shared, str(sorted(shared)))

    # --- no brief may tell an agent to wait --------------------------------------
    # Every client used to end its worked example with "then stop and wait". The
    # one client that had a watcher tool acted on it: asked to "wait until
    # contacted" it set up a monitor that polled an inbox nothing writes to,
    # burning tokens for as long as it ran. That client is gone; the sentence
    # must not come back, because the next harness with a timer will do the same.
    #
    # Delivery is PUSH. A message arriving starts a turn on its own. Being idle is
    # being ready, and a brief that says "wait" invites the opposite.
    for label, path in (("claude", PLUGIN / "a2a" / "server" / "a2a-channel.py"),
                        ("opencode", PLUGIN / "opencode" / "a2a-opencode.js"),
                        ("pi", PLUGIN / "pi" / "index.ts"),
                        ("codex", CODEX)):
        body = path.read_text()
        check(f"{label}: its brief does not tell the agent to wait",
              "stop and wait" not in body, "'stop and wait' is back")
        check(f"{label}: and says the next message arrives on its own",
              "on its own" in body or "arrives on its own" in body,
              "nothing explains that delivery is push")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — every client speaks the same vocabulary")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
