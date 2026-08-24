#!/usr/bin/env python3
"""Deleting an agent must not leave anything of it behind.

    python3 tests/test_remove_agent.py

An orphan receipt is the dangerous one: no agent is left to ack it, so its
message is never collected and sits until the 365-day retention ceiling. A
stale channel membership is worse — the dead id keeps being written into new
messages' audiences, minting a fresh orphan on every post. Both were real:
`remove` used to clear only the agent row and its stream cursor.

What must SURVIVE is just as much the point. Transcripts and DMs record what
was said; deleting the speaker does not unsay it.

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
STATION = "default"
GONE = "doomed-agent"
PEER = "surviving-peer"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-rm-"))
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("a2a_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b._startup()

    def count(table: str, column: str = "agent_id") -> int:
        return b.CONN.execute(
            f"SELECT COUNT(*) c FROM {table} WHERE {column} = %s", (GONE,)
        ).fetchone()["c"]

    # --- an agent with something of everything attached ---------------------
    b.AGENTS.add(STATION, GONE)
    b.AGENTS.add(STATION, PEER)
    asyncio.run(b.CHANNELS.create(STATION, "ops", "", [GONE, PEER]))
    asyncio.run(b.CHANNELS.post(STATION, "ops", GONE, "something it said"))
    asyncio.run(b.DIRECT.send(STATION, PEER, GONE, "unread when it died"))
    bc = asyncio.run(b.BROADCASTS.create(STATION, "who can help?", PEER,
                                        None, None))
    asyncio.run(b.BROADCASTS.submit_bid(STATION, bc["id"], GONE, "claim", "me"))
    b._fetch_for_agent(STATION, GONE, 50, replay=True)     # give it a cursor

    check("it starts with a receipt", count("message_receipts") > 0)
    check("it starts with a bid", count("bids") > 0)
    check("it starts as a channel member",
          GONE in asyncio.run(b.CHANNELS.get(STATION, "ops"))["members"])

    # --- delete it -----------------------------------------------------------
    b.AGENTS.remove(GONE, STATION)

    check("the agent row is gone", count("agents") == 0)
    check("no orphan receipt — nothing could ever ack it",
          count("message_receipts") == 0, f"{count('message_receipts')} left")
    check("no orphan bid", count("bids") == 0)
    check("no stream cursor", count("stream_cursors") == 0)

    members = asyncio.run(b.CHANNELS.get(STATION, "ops"))["members"]
    check("removed from channel members — else every later post mints a new "
          "orphan receipt for it", GONE not in members, json.dumps(members))
    check("the surviving member is untouched", PEER in members,
          json.dumps(members))

    cand = json.loads(b.CONN.execute(
        "SELECT candidates FROM broadcasts WHERE id = %s", (bc["id"],)
    ).fetchone()["candidates"] or "[]")
    check("removed from broadcast candidates", GONE not in cand,
          json.dumps(cand))

    # --- and what must NOT be erased -----------------------------------------
    said = b.CONN.execute(
        "SELECT COUNT(*) c FROM transcripts WHERE sender = %s", (GONE,)
    ).fetchone()["c"]
    check("what it said in channels is still on the record", said == 1,
          f"{said} transcript rows")
    dm = b.CONN.execute(
        "SELECT COUNT(*) c FROM dms WHERE recipient = %s", (GONE,)
    ).fetchone()["c"]
    check("the DM it never read is still on the record", dm == 1, f"{dm} dms")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — deletion takes its own rows and no more")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
