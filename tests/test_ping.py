#!/usr/bin/env python3
"""`ping` must prove delivery without teaching an agent the wrong thing.

    python3 tests/test_ping.py

An operator ran it and the agent that received the witness spent a turn trying
to answer: it replied in the channel (where it was not a member, so the reply
reached nobody), then tried to DM the sender (a label, not an agent, so 404),
then reported the probe as broken. Every one of those complaints was correct.

Three properties, and the first is the regression that made the command throw:

  it runs        `_channel_audience` lost its fourth parameter in the
                 audience/addressed rename, and this caller kept passing one.
  it stays in    the witness goes through a channel the agent IS a member of.
                 A channel post never reaches outside the channel — a
                 diagnostic must not be the exception that says otherwise.
  it says so     the text states that arrival is the result and that the
                 sender cannot be answered, so no turn is spent replying.

Needs a MariaDB/MySQL server; see dbharness.
"""
import argparse
import importlib.util
import inspect
import os
import sys
from pathlib import Path

import dbharness

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load(startup: bool = True):
    # db_env() PROVISIONS a database, so the source-only path must not call it:
    # importing the module touches nothing.
    if startup:
        os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("a2a_ping", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    if startup:
        mod._startup()
    return mod


def source_checks(b) -> None:
    """No database needed, and these are the two that regressed."""
    src = inspect.getsource(b._cli_ping)
    sig = inspect.signature(b._channel_audience)
    check("the ping calls _channel_audience with the arity it actually has — "
          "the rename dropped its fourth parameter and this caller kept "
          "passing one, which threw at runtime with nothing to catch it",
          f"_channel_audience(sid, name, args.sender, [agent])" not in src
          and len(sig.parameters) == 3, str(sig))
    check("the witness tells the agent not to answer: a probe read as a peer "
          "message costs a turn and produces a reply nobody can receive",
          "DO NOT REPLY" in src and "arrival is the whole result" in src)
    check("and it says the sender is a label, so nobody tries to DM it",
          "is a label, not an agent" in src)
    check("it only pings through channels the agent belongs to",
          "if agent in (c.get(\"members\") or [])" in src, src[:200])


def registry(b) -> None:
    sid = b.STATIONS.create("pingtest")["station_id"]
    for who in ("target", "bystander"):
        b.AGENTS.add(sid, who)
    import asyncio
    asyncio.run(b.CHANNELS.create(sid, "ops", "", ["target", "bystander"]))
    asyncio.run(b.CHANNELS.create(sid, "elsewhere", "", ["bystander"]))

    args = argparse.Namespace(agent_id="target", channel=None, text=None,
                              sender="doctor")
    rc = b._cli_ping(args)
    check("the ping runs at all and reports the agent as reachable",
          rc == 0, f"exit {rc}")

    rows = list(b.CONN.execute(
        "SELECT channel, text FROM transcripts WHERE station_id = %s", (sid,)))
    check("it posted into a channel the agent is a MEMBER of, so a reply "
          "would reach the room rather than nobody",
          len(rows) == 1 and rows[0]["channel"] == "ops",
          str([r["channel"] for r in rows]))

    audience = sorted(r["agent_id"] for r in b.CONN.execute(
        "SELECT agent_id FROM message_receipts WHERE station_id = %s", (sid,)))
    check("only the agent under test owes an ack — a diagnostic must not wake "
          "a room or land in a bystander's unacked pile",
          audience == ["target"], str(audience))

    args = argparse.Namespace(agent_id="target", channel="elsewhere",
                              text=None, sender="doctor")
    check("pinging through a channel the agent is NOT in is refused, with the "
          "fix named, rather than delivering a message it cannot answer",
          b._cli_ping(args) == 1)

    b.AGENTS.add(sid, "roomless")
    args = argparse.Namespace(agent_id="roomless", channel=None, text=None,
                              sender="doctor")
    check("an agent in no channel is refused and pointed at ping_me, which is "
          "a self-DM and needs no room",
          b._cli_ping(args) == 1)


def main() -> int:
    try:
        dbharness.require_db()
    except SystemExit:
        source_checks(load(startup=False))
        print("\n(registry checks skipped: no database)")
        return 1 if fails else 0
    b = load()
    source_checks(b)
    registry(b)
    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — the probe proves delivery and asks for nothing back")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
