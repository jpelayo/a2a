#!/usr/bin/env python3
"""What a reconnect replays, and what it must not.

    python3 tests/test_replay.py

A stream re-sends unacked messages on the first fetch of each connection, so a
push lost between the socket write and the client reading it is not lost. That
recovery window is seconds — but the stream reconnects every few minutes, so
applying it to everything unacked forever means an agent is handed its old mail
again on every reconnect and every boot, and answers it again. That is the
"stale DMs on boot" bug.

The rule this pins: replay covers what was never delivered, plus what was
delivered RECENTLY (STREAM_REPLAY_WINDOW). Anything delivered long ago and
still unacked is stale — the agent saw it — so it stops being pushed. It is not
deleted: it stays unacked, stays in my_pending, and is still never collected.

Needs python3 with the broker's deps. No pip.
"""
import asyncio
import importlib.util
import json
import os
import sys
import tempfile
from pathlib import Path

import dbharness

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"
ST, ME, PEER = "default", "receiver", "sender"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-replay-"))
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("a2a_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b._startup()

    b.AGENTS.add(ST, ME)
    b.AGENTS.add(ST, PEER)

    def dm(text: str) -> str:
        return asyncio.run(b.DIRECT.send(ST, PEER, ME, text))["dm"]["id"]

    def texts(replay: bool) -> list[str]:
        msgs, _ = b._fetch_for_agent(ST, ME, 50, replay=replay)
        return [m.get("text") for m in msgs]

    def set_delivered(msg_id: str, seconds_ago: float) -> None:
        b.CONN.execute(
            "UPDATE message_receipts SET delivered_at = %s "
            "WHERE station_id = %s AND agent_id = %s AND msg_id = %s",
            (b.time.time() - seconds_ago, ST, ME, msg_id))

    stale = dm("stale — read long ago, never acked")
    recent = dm("recent — pushed seconds ago, may have been lost")
    fresh = dm("fresh — never delivered at all")
    acked = dm("acked — handled and retired")

    set_delivered(stale, b.STREAM_REPLAY_WINDOW + 60)
    set_delivered(recent, 5)
    set_delivered(acked, 5)
    b.CONN.execute(
        "UPDATE message_receipts SET acked_at = %s WHERE station_id = %s "
        "AND agent_id = %s AND msg_id = %s", (b.time.time(), ST, ME, acked))

    # --- a reconnect ---------------------------------------------------------
    got = texts(replay=True)
    check("a never-delivered message is replayed",
          any("fresh" in t for t in got), json.dumps(got))
    check("a message pushed seconds ago is replayed — that is the crash "
          "recovery the window exists for",
          any("recent" in t for t in got), json.dumps(got))
    check("a message delivered long ago is NOT replayed — this is the stale "
          "DMs on boot bug", not any("stale" in t for t in got),
          json.dumps(got))
    check("an acked message is never replayed",
          not any("acked" in t for t in got), json.dumps(got))

    # --- but nothing was destroyed -------------------------------------------
    pending = [r["msg_id"] for r in b._pending_rows(ST, ME, 50)]
    check("the stale message is still pending — not replayed is not deleted",
          stale in pending, json.dumps(pending))
    check("the acked one is gone from pending", acked not in pending)

    # --- steady-state polling is unchanged -----------------------------------
    got2 = texts(replay=False)
    check("a steady-state poll only sees what was never delivered",
          not any("stale" in t or "recent" in t for t in got2),
          json.dumps(got2))

    # --- and the consequence: an acked inbox actually empties ---------------
    # With clients acking on receipt this is the normal path, not a rare one:
    # the collector's rule is "every receipt for this message is acked".
    before = b.CONN.execute("SELECT COUNT(*) c FROM dms").fetchone()["c"]
    b.collect(ST)
    after = b.CONN.execute("SELECT COUNT(*) c FROM dms").fetchone()["c"]
    left = [r["id"] for r in b.CONN.execute("SELECT id FROM dms").fetchall()]
    check("collect() removes the acked DM", acked not in left,
          f"{before} -> {after}: {left}")
    check("and keeps every message still unacked — including the stale one, "
          "which is quiet but not gone",
          all(i in left for i in (stale, recent, fresh)), json.dumps(left))

    # --- reading is acking, and a fully-read message is collected -----------
    # The guarantee is "kept until everyone it was addressed to has acked".
    # That only empties a station if acking actually happens, which is why
    # every path that hands a message to its recipient now acks it.
    b.AGENTS.add(ST, "reader-two")
    asyncio.run(b.CHANNELS.create(ST, "room", "", [PEER, ME, "reader-two"]))
    asyncio.run(b.CHANNELS.post(ST, "room", PEER, "for the whole room"))

    def read_as(who: str) -> None:
        b._current_station.set(ST)
        b._current_agent.set(who)
        asyncio.run(b.my_pending())

    def room_texts() -> list[str]:
        return [r["text"] for r in b.CONN.execute(
            "SELECT text FROM transcripts WHERE channel = 'room'")]

    read_as(ME)
    b.collect(ST)
    check("a post one recipient has read is NOT collected — the other has "
          "not seen it yet", room_texts() == ["for the whole room"],
          str(room_texts()))

    read_as("reader-two")
    b.collect(ST)
    check("once every recipient has read it, the post is collected",
          room_texts() == [], str(room_texts()))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — replay recovers, it does not nag")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
