#!/usr/bin/env python3
"""Agents cannot create or destroy the structure they live in.

    python3 tests/test_agent_surface.py

Stations, tokens and agents are provisioned by an operator — the CLI, the TUI,
/admin. An agent holding a station token may take part in a station: post, DM,
bid, read, ack, keep its own card, and open a channel for a conversation that
does not exist yet. It may not conjure a new agent, and it may not DELETE a
channel: that holds other agents' transcript, so it is not a participant's to
destroy.

This is not a style preference. A confused client that could register itself
did: it re-registered under a directory-derived id while its identity was
broken, and the station gained a blank agent nobody asked for. The broker
already says a station is closed by default and users can never grant
themselves in; self-registration quietly undid that.

Both surfaces are checked, because MCP and REST are two doors to the same
registries and removing one is worthless.

Needs python3 with the broker's deps and a free port.
"""
import asyncio
import importlib.util
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import dbharness

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"

# Tools an agent must NOT have. Each one creates or destroys structure.
FORBIDDEN_TOOLS = ("register_me", "create_agent", "delete_agent",
                   "delete_channel")
# Tools it must keep — taking part is the whole point of the service.
REQUIRED_TOOLS = ("whoami", "my_realm", "list_agents", "get_agent",
                  "update_agent", "list_channels", "create_channel",
                  "add_channel_member", "remove_channel_member",
                  "post_to_channel", "read_channel", "send_dm", "read_dms",
                  "my_pending", "ack_messages", "broadcast", "submit_bid")

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
    tmp = Path(tempfile.mkdtemp(prefix="a2a-surface-"))
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("a2a_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b._startup()

    # --- the MCP door --------------------------------------------------------
    names = {t.name for t in asyncio.run(b.mcp.list_tools())}
    for t in FORBIDDEN_TOOLS:
        check(f"MCP does not offer {t}", t not in names)
    missing = [t for t in REQUIRED_TOOLS if t not in names]
    check("the tools that let an agent take part are all still there",
          not missing, f"missing: {missing}")

    # --- the REST door -------------------------------------------------------
    port = free_port()
    env = dict(os.environ, **dbharness.db_env(),
               A2A_AUTH_DISABLED="1", A2A_HOST="127.0.0.1", A2A_PORT=str(port))
    base = f"http://127.0.0.1:{port}"
    subprocess.run([sys.executable, str(BROKER), "agent", "add", "member",
                    "--station", "default"], env=env, check=True,
                   capture_output=True)
    proc = subprocess.Popen([sys.executable, str(BROKER), "serve"], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("broker did not come up")

        def status(path: str, method: str, body=None) -> int:
            req = urllib.request.Request(
                f"{base}{path}",
                data=json.dumps(body).encode() if body is not None else None,
                headers={"Content-Type": "application/json",
                         "X-A2A-Agent": "member"},
                method=method)
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    return r.status
            except urllib.error.HTTPError as e:
                return e.code

        for path, method, body in (
            ("/me/agents", "POST", {"agent_id": "sneaky", "station": "default"}),
            ("/me/agents/member", "DELETE", None),
            ("/agents", "POST", {"agent_id": "sneaky"}),
            ("/agents/member", "DELETE", None),
            ("/channels/ops", "DELETE", None),
        ):
            code = status(path, method, body)
            check(f"{method} {path} is refused", code in (404, 405),
                  f"got HTTP {code}")

        # A channel may only contain agents that exist. Membership decides a
        # message's audience, so a phantom member silently swallows a receipt
        # for every post while the agent whose name was misspelt receives
        # nothing and reports no error — a whole channel going deaf with no
        # failure anywhere.
        check("a non-existent agent cannot be added to a channel",
              status("/channels/ops/members", "POST",
                     {"agent_id": "no-such-agent"}) in (400, 404),
              str(status("/channels/ops/members", "POST",
                         {"agent_id": "no-such-agent"})))

        # Taking part still works, or the lockdown went too far. Channels are
        # the deliberate exception: an agent may open the conversation it
        # needs, it just may not destroy one holding other agents' transcript.
        check("POST /channels is allowed — agents open their own channels",
              status("/channels", "POST",
                     {"name": "advisory", "members": []}) in (200, 201))
        check("GET /agents still lists them", status("/agents", "GET") == 200)
        check("PATCH /agents/<id> still writes the card",
              status("/agents/member", "PATCH", {"description": "a card"}) == 200)
        check("POST /dms still sends",
              status("/dms", "POST",
                     {"to": "member", "text": "hello"}) in (200, 201))

        # --- proposing is asking, and must never become creating ----------
        # An agent may put a NAME in front of an operator. What it may not do
        # is answer. If a station token can ever approve, this stopped being
        # a request queue and became create_agent with extra steps.
        check("an agent may propose a name",
              status("/me/proposals", "POST",
                     {"agent_id": "asked-for"}) in (200, 201))
        # 503 belongs here with the rest: /admin/* answers it when
        # A2A_ADMIN_TOKEN is unset, which is a refusal wearing a different
        # number. What matters is that no code path returns success.
        for path in ("/me/proposals/asked-for/approve",
                     "/proposals/asked-for/approve",
                     "/admin/proposals/asked-for"):
            code = status(path, "POST", {})
            check(f"a station token cannot approve via {path}",
                  code in (401, 403, 404, 405, 503), str(code))
        pending = subprocess.run(
            [sys.executable, str(BROKER), "agent", "proposals"],
            env=env, capture_output=True, text=True).stdout
        check("the proposal is still only a request after all of that",
              "asked-for" in pending, pending)
        listed = subprocess.run(
            [sys.executable, str(BROKER), "agent", "list"],
            env=env, capture_output=True, text=True).stdout
        check("and no agent exists for it — approval is the operator's, and "
              "only on the CLI or TUI, straight on the database",
              "asked-for" not in listed, listed)

        # And nothing crept in behind our back.
        after = subprocess.run(
            [sys.executable, str(BROKER), "agent", "list"],
            env=env, capture_output=True, text=True).stdout
        check("no agent was created by any of the refused calls",
              "sneaky" not in after, after)
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    # --- the credential tier, against a broker where admin is ENABLED --------
    # Minting a token or opening a station lives on three operator surfaces
    # (/admin/*, the CLI, the TUI) and on none of an agent's. Nothing asserted
    # that until now: it rests on AuthMiddleware's tier check alone, one
    # refactor away from resting on nothing.
    #
    # It needs its own broker because the one above has A2A_ADMIN_TOKEN unset,
    # so every /admin/* path answers 503 whatever the caller holds — probing
    # it there would pass without the tier check ever running, which is a test
    # that reports success for a property it never examined.
    aport = free_port()
    aenv = dict(os.environ, **dbharness.db_env(),
                A2A_AUTH_DISABLED="1", A2A_ADMIN_TOKEN="the-operator-secret",
                A2A_HOST="127.0.0.1", A2A_PORT=str(aport))
    abase = f"http://127.0.0.1:{aport}"
    aproc = subprocess.Popen([sys.executable, str(BROKER), "serve"], env=aenv,
                             stdout=subprocess.DEVNULL,
                             stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{abase}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("admin broker did not come up")

        def astatus(path: str, method: str, body=None, admin=False) -> int:
            headers = {"Content-Type": "application/json",
                       "X-A2A-Agent": "member"}
            if admin:
                headers["Authorization"] = "Bearer the-operator-secret"
            req = urllib.request.Request(
                f"{abase}{path}",
                data=json.dumps(body).encode() if body is not None else None,
                headers=headers, method=method)
            try:
                with urllib.request.urlopen(req, timeout=10) as r:
                    return r.status
            except urllib.error.HTTPError as e:
                return e.code

        # First prove the endpoint is live and reachable WITH the operator's
        # bearer. Without this the refusals below could be a 404 on a route
        # that simply is not there, and the test would be pinning nothing.
        check("with the operator's bearer, /admin/tokens really does mint — "
              "so the refusals below are the tier check, not a missing route",
              astatus("/admin/tokens", "POST", {"user": "operator-made"},
                      admin=True) in (200, 201),
              str(astatus("/admin/tokens", "GET", admin=True)))

        for label, path, body in (
            ("mint a token", "/admin/tokens", {"user": "sneaky"}),
            ("open a station", "/admin/stations", {"name": "sneaky-station"}),
            ("grant itself in", "/admin/stations/default/allow",
             {"token": "whatever"}),
            # Screening acks on behalf of OTHER agents — it can retire mail
            # its peers never read. Clearing its own inbox is ack_all, which
            # is scoped to the caller; this one is the operator's.
            ("screen a station", "/admin/stations/default/screen", {}),
            ("screen another agent",
             "/admin/stations/default/screen", {"agent": "member"}),
            # Marking a segment acks on behalf of everyone in it, or shortens
            # a shelf life the sender chose. Same power as screening, same
            # tier, and it must be refused the same way.
            ("mark a whole segment of messages",
             "/admin/stations/default/messages", {"segment": "unread"}),
        ):
            code = astatus(path, "POST", body)
            check(f"an agent cannot {label} ({path})",
                  code in (401, 403), str(code))
        for label, path in (("list tokens", "/admin/tokens"),
                            ("list stations", "/admin/stations"),
                            ("read what is in another station",
                             "/admin/stations/default/messages")):
            code = astatus(path, "GET")
            check(f"nor {label} ({path})", code in (401, 403), str(code))

        stations_after = subprocess.run(
            [sys.executable, str(BROKER), "station", "list"],
            env=aenv, capture_output=True, text=True).stdout
        check("no station was opened by any of that",
              "sneaky-station" not in stations_after, stations_after)
        tokens_after = subprocess.run(
            [sys.executable, str(BROKER), "token", "list"],
            env=aenv, capture_output=True, text=True).stdout
        check("and no token was minted — the credential that grants access "
              "cannot be issued by something that already has one",
              "sneaky" not in tokens_after, tokens_after)
        check("while the operator's own token IS there, which is what makes "
              "the line above a real absence rather than an empty table",
              "operator-made" in tokens_after, tokens_after)
    finally:
        aproc.terminate()
        aproc.wait(timeout=10)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — agents can take part, not restructure")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
