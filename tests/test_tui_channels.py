#!/usr/bin/env python3
"""The TUI's channels tab — and the async trap that would make it a no-op.

    python3 tests/test_tui_channels.py

Channels were invisible to the operator: five tabs, none of them showing who
is in which room. Membership decides who receives a post, so that was a
routing fact with no operator surface at all.

The tab is a handful of lines, and exactly one thing about it is dangerous:
every ChannelRegistry method is a COROUTINE while everything else the TUI
calls is a plain function. `CHANNELS.create(...)` without asyncio.run returns
a coroutine object, does nothing, raises nothing, and leaves a handler that
looks like it worked. That mistake was made for real in
tests/test_transfer_requests.py during the transfer work.

So every assertion here reads the DATABASE back through CHANNELS.list, never
the handler's return value: a missing asyncio.run has to fail this file.

Drives _Tui directly with a stub screen and scripted ask/confirm/pick — no
curses, no TTY. Needs a MariaDB/MySQL via dbharness.
"""
import asyncio
import importlib.util
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


def load():
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("tui_broker", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return mod


def make_tui(b):
    """A _Tui with no curses behind it, and scripted prompts.

    Only the input helpers are replaced. load(), line() and act_channel() are
    the real ones — they are what is being tested.
    """
    t = b._Tui.__new__(b._Tui)
    t.scr = None
    t.view = "channels"
    t.sel = {"stations": 0, "tokens": 0, "agents": 0, "logs": 0,
             "messages": 0, "channels": 0}
    t.msg_station = None
    t.ch_station = None
    t.msg = ""
    t.rows = []
    t.answers: list[str] = []
    t.picks: list[str] = []
    t.say_yes = True
    t.ask = lambda prompt: (t.answers.pop(0) if t.answers else "")
    t.confirm = lambda prompt: t.say_yes
    t.pick = lambda title, items: (t.picks.pop(0) if t.picks else None)
    return t


def main() -> int:
    b = load()
    sid = b.STATIONS.create("tui")["station_id"]
    for who in ("alice", "bob", "carol"):
        b.AGENTS.add(sid, who)

    def channels() -> list[dict]:
        """The truth, read back from the database."""
        return asyncio.run(b.CHANNELS.list(sid))

    def by_name(name: str) -> dict | None:
        return next((c for c in channels() if c["name"] == name), None)

    t = make_tui(b)

    # --- empty ---------------------------------------------------------------
    t.load()
    check("an empty station lists no channels rather than erroring — the tab "
          "is reached before anything exists in it",
          t.rows == [], str(t.rows))

    # --- create --------------------------------------------------------------
    t.answers = ["ops", "operations chatter"]
    t.act_channel("n", None)
    check("n CREATES the channel in the database — the whole point of this "
          "file: without asyncio.run this returns a coroutine, changes "
          "nothing, and raises nothing",
          by_name("ops") is not None, str(channels()))
    check("and it says the channel reaches nobody yet, because an empty "
          "channel has an empty audience and posting to it is a no-op",
          "no members" in t.msg, t.msg)

    t.load()
    check("the new channel shows up on the next load", len(t.rows) == 1)
    check("the row names the members — 'who is in this room' is the question "
          "the tab exists to answer",
          "nobody" in t.line(t.rows[0]), t.line(t.rows[0]))

    # --- membership ----------------------------------------------------------
    t.picks = ["alice"]
    t.act_channel("a", t.rows[0])
    check("a adds a member, in the database",
          (by_name("ops") or {}).get("members") == ["alice"],
          str(by_name("ops")))

    t.load()
    t.picks = ["bob"]
    t.act_channel("a", t.rows[0])
    t.load()
    check("and a second, so the row shows both",
          sorted((by_name("ops") or {}).get("members") or []) == ["alice", "bob"],
          str(by_name("ops")))
    line = t.line(t.rows[0])
    check("the line renders the member ids themselves, not just a count",
          "alice" in line and "bob" in line, line)
    check("with the count and the message total beside them",
          "2m" in line and "0 msg" in line, line)

    # The offer must exclude who is already there: re-adding is a choice that
    # can only be a mistake.
    offered: list = []
    t.pick = lambda title, items: (offered.extend(items) or None)
    t.act_channel("a", t.rows[0])
    check("the add picker offers only agents NOT already in the channel",
          [i[0] for i in offered] == ["carol"], str(offered))
    t.pick = lambda title, items: (t.picks.pop(0) if t.picks else None)

    t.picks = ["alice"]
    t.act_channel("r", t.rows[0])
    check("r removes a member, in the database",
          (by_name("ops") or {}).get("members") == ["bob"],
          str(by_name("ops")))

    # --- the ACL marker ------------------------------------------------------
    asyncio.run(b.CHANNELS.create(
        sid, "gated", "restricted", [],
        policy={"blocked_agents": ["carol"]}))
    t.load()
    gated = next(r for r in t.rows if r["name"] == "gated")
    check("a channel with a policy is flagged ACL — an ACL that silently "
          "refuses add_member is exactly what an operator should see before "
          "pressing a and wondering why nothing happened",
          "ACL" in t.line(gated), t.line(gated))

    # --- delete --------------------------------------------------------------
    t.sel["channels"] = 0
    t.load()
    ops = next(r for r in t.rows if r["name"] == "ops")
    t.say_yes = False
    t.act_channel("x", ops)
    check("x asks first, and answering no leaves the channel alone",
          by_name("ops") is not None)
    t.say_yes = True
    t.act_channel("x", ops)
    check("answering yes deletes it, in the database",
          by_name("ops") is None, str(channels()))
    check("and the other channel is untouched", by_name("gated") is not None)

    # --- every station by default -------------------------------------------
    # The tab opened scoped to one station, and the first station having no
    # channels showed "(empty)" — indistinguishable from "there are none",
    # while another station was full of them. All stations is the default now.
    other = b.STATIONS.create("elsewhere")["station_id"]
    asyncio.run(b.CHANNELS.create(other, "far-away", "another station", []))
    t.ch_station = None
    t.load()
    names = sorted(r["name"] for r in t.rows)
    check("with no filter the tab lists channels from EVERY station — an "
          "empty screen must mean there are none, not that the default "
          "station happens to have none",
          names == ["far-away", "gated"], str(names))
    check("and each row names its station, since the list spans them",
          {r["station_name"] for r in t.rows} == {"tui", "elsewhere"},
          str([r.get("station_name") for r in t.rows]))
    far = next(r for r in t.rows if r["name"] == "far-away")
    check("which the line renders as a column",
          "elsewhere" in t.line(far), t.line(far))

    # --- filtering, and coming back ------------------------------------------
    t.picks = [other]
    t.act_channel("s", None)
    t.load()
    check("s filters to one station",
          [r["name"] for r in t.rows] == ["far-away"], str(t.rows))
    t.picks = [""]          # the "(all stations)" entry
    t.act_channel("s", None)
    t.load()
    check("and picking (all stations) comes back to everything — a filter "
          "you cannot leave is the trap this replaced",
          sorted(r["name"] for r in t.rows) == ["far-away", "gated"],
          str(t.rows))

    # --- row actions follow the ROW's station --------------------------------
    b.AGENTS.add(other, "dave")
    far = next(r for r in t.rows if r["name"] == "far-away")
    offered: list = []
    t.pick = lambda title, items: (offered.extend(items) or "dave")
    t.act_channel("a", far)
    check("a row action uses the ROW's station, not the tab's: the picker "
          "offered that station's agents",
          [i[0] for i in offered] == ["dave"], str(offered))
    check("and the member landed in the right station's channel",
          [c["members"] for c in asyncio.run(b.CHANNELS.list(other))
           if c["name"] == "far-away"] == [["dave"]],
          str(asyncio.run(b.CHANNELS.list(other))))
    check("while the other station's channel is untouched",
          (by_name("gated") or {}).get("members") == [],
          str(by_name("gated")))

    print()
    if fails:
        print(f"{len(fails)} failure(s):")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("PASS — channels are visible, and every action reaches the database")
    return 0


if __name__ == "__main__":
    dbharness.require_db()
    sys.exit(main())
