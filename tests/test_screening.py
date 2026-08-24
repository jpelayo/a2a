#!/usr/bin/env python3
"""Screening acks a backlog. It never deletes one.

    python3 tests/test_screening.py

Nothing here removes a message while an agent it was addressed to has not
acked it. That guarantee is load-bearing, and it has one failure mode: an
agent that stops acking pins its share of the station forever — including for
everyone else, since a channel post is only collected once its WHOLE audience
has acked. One agent sat on 51 messages it never received and held every one
of those posts for every other member.

Screening is the way out, and the shape of it is the point:

    screen()   moves acked_at.            It deletes NOTHING.
    collect()  deletes what is now done.  It is still the only thing that does.

So the first assertion below is that screening on its own leaves every message
in place. If that ever stops being true, this feature has grown a second
deletion path and the ephemerality argument has two doors instead of one.

Pure python3 with the broker's deps.
"""
import asyncio
import importlib.util
import os
import sys
import tempfile
from pathlib import Path

import dbharness

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load(db: Path):
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location(f"scr_{db.stem}", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return mod


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-screen-"))
    b = load(tmp / "s.db")
    count = lambda t: b.CONN.execute(  # noqa: E731
        f"SELECT COUNT(*) n FROM {t}").fetchone()["n"]

    def pending(sid, who):
        return b.CONN.execute(
            "SELECT COUNT(*) n FROM message_receipts WHERE station_id = %s "
            "AND agent_id = %s AND acked_at IS NULL", (sid, who)
        ).fetchone()["n"]

    sid = b.STATIONS.create("acme")["station_id"]
    other = b.STATIONS.create("other")["station_id"]
    for a in ("alice", "bob", "stuck"):
        b.AGENTS.add(sid, a)
    # Two agents, because a sender holds no receipt for its own post — with
    # only one member there would be nothing in the other station to prove
    # was left alone, and the scoping check below would pass on emptiness.
    b.AGENTS.add(other, "outsider")
    b.AGENTS.add(other, "onlooker")
    asyncio.run(b.CHANNELS.create(sid, "ops", "", ["alice", "bob", "stuck"]))
    asyncio.run(b.CHANNELS.create(other, "ops", "", ["outsider", "onlooker"]))
    asyncio.run(b.CHANNELS.post(sid, "ops", "alice", "shared post"))
    asyncio.run(b.DIRECT.send(sid, "alice", "stuck", "a dm for the stuck one"))
    asyncio.run(b.CHANNELS.post(other, "ops", "outsider", "another station"))

    # --- it deletes nothing on its own -------------------------------------
    before_tx, before_dm = count("transcripts"), count("dms")
    out = b.screen(sid)
    check("screening acks the backlog", out["acked"] > 0, str(out))
    check("and DELETES NOTHING by itself — collect() stays the only thing in "
          "this system that removes a row",
          count("transcripts") == before_tx and count("dms") == before_dm,
          f"{count('transcripts')}/{before_tx} tx, {count('dms')}/{before_dm} dm")
    check("what moved is acked_at, nothing else",
          b.CONN.execute("SELECT COUNT(*) n FROM message_receipts "
                         "WHERE station_id = %s AND acked_at IS NULL",
                         (sid,)).fetchone()["n"] == 0)

    # --- and then the collector can do its job -----------------------------
    got = b.collect(sid)
    check("only now is the fully-acked post collected",
          count("transcripts") == before_tx - 1, str(got))
    check("and the dm with it", count("dms") == before_dm - 1, str(got))

    # --- station scoping ---------------------------------------------------
    check("screening one station never touches another — the invariant every "
          "query in this file is built on",
          pending(other, "onlooker") == 1,
          str(pending(other, "onlooker")))
    check("nor collects its messages", count("transcripts") == 1,
          str(count("transcripts")))

    # --- the audience rule survives ----------------------------------------
    b2 = load(tmp / "audience.db")
    sid2 = b2.STATIONS.create("acme")["station_id"]
    for a in ("alice", "bob", "stuck"):
        b2.AGENTS.add(sid2, a)
    asyncio.run(b2.CHANNELS.create(sid2, "ops", "", ["alice", "bob", "stuck"]))
    asyncio.run(b2.CHANNELS.post(sid2, "ops", "alice", "everyone must ack"))
    asyncio.run(b2.DIRECT.send(sid2, "alice", "stuck", "just for stuck"))
    one = b2.screen(sid2, "stuck")
    b2.collect(sid2)
    check("screening ONE agent clears only that inbox",
          one["agents"] == 1 and one["acked"] == 2, str(one))
    check("bob's pending is untouched — no collateral",
          b2.CONN.execute(
              "SELECT COUNT(*) n FROM message_receipts WHERE agent_id = 'bob' "
              "AND acked_at IS NULL").fetchone()["n"] == 1)
    check("the dm only that agent held IS collected",
          b2.CONN.execute("SELECT COUNT(*) n FROM dms").fetchone()["n"] == 0)
    check("but the shared post is NOT — a transcript needs its whole "
          "audience, and screening one member must not fake the rest",
          b2.CONN.execute(
              "SELECT COUNT(*) n FROM transcripts").fetchone()["n"] == 1)
    b2.screen(sid2)
    b2.collect(sid2)
    check("screening the station finishes the job",
          b2.CONN.execute(
              "SELECT COUNT(*) n FROM transcripts").fetchone()["n"] == 0)

    # --- the dry run -------------------------------------------------------
    b3 = load(tmp / "dry.db")
    sid3 = b3.STATIONS.create("acme")["station_id"]
    for a in ("alice", "bob"):
        b3.AGENTS.add(sid3, a)
    asyncio.run(b3.CHANNELS.create(sid3, "ops", "", ["alice", "bob"]))
    asyncio.run(b3.CHANNELS.post(sid3, "ops", "alice", "hello"))
    pre = b3.screen(sid3, preview=True)
    check("a dry run counts what a real one would ack",
          pre["acked"] == 1 and pre["preview"] is True, str(pre))
    check("and changes nothing", pre["acked"] == b3.screen(sid3)["acked"],
          "the real run disagreed with its own preview")
    check("screening twice reports 0, not a fabricated number",
          b3.screen(sid3)["acked"] == 0, str(b3.screen(sid3)))

    # --- broadcasts: silenced, not destroyed -------------------------------
    b4 = load(tmp / "bc.db")
    sid4 = b4.STATIONS.create("acme")["station_id"]
    for a in ("alice", "bob"):
        b4.AGENTS.add(sid4, a)
    open_bc = asyncio.run(b4.BROADCASTS.create(
        sid4, problem="who can do X?", sender="alice",
        expertise=None, projects=None))
    closed_bc = asyncio.run(b4.BROADCASTS.create(
        sid4, problem="already handled", sender="alice",
        expertise=None, projects=None))
    asyncio.run(b4.BROADCASTS.close(sid4, closed_bc["id"]))
    out4 = b4.screen(sid4)
    b4.collect(sid4)
    ids = [r["id"] for r in b4.CONN.execute("SELECT id FROM broadcasts")]
    check("screening acks broadcast receipts too",
          out4["by_kind"].get("broadcast", 0) > 0, str(out4))
    check("a CLOSED broadcast is collected",
          closed_bc["id"] not in ids, str(ids))
    check("an OPEN one survives — deleting it needs status='closed' as well, "
          "so a live help-wanted board is silenced, never destroyed",
          open_bc["id"] in ids, str(ids))
    check("and the count is reported, so an operator is not left wondering "
          "why the board is still there",
          out4.get("open_broadcasts", 0) == 1, str(out4))

    # --- the agent's own door ----------------------------------------------
    b5 = load(tmp / "ackall.db")
    sid5 = b5.STATIONS.create("acme")["station_id"]
    for a in ("alice", "bob"):
        b5.AGENTS.add(sid5, a)
    asyncio.run(b5.CHANNELS.create(sid5, "ops", "", ["alice", "bob"]))
    asyncio.run(b5.CHANNELS.post(sid5, "ops", "alice", "for both of you"))
    asyncio.run(b5.DIRECT.send(sid5, "alice", "bob", "and one for bob"))
    b5._current_station.set(sid5)
    b5._current_agent.set("bob")
    res = asyncio.run(b5.ack_all())
    check("ack_all clears the caller's inbox", res["acked"] == 2, str(res))
    check("and ONLY the caller's — an agent may never ack for somebody else",
          b5.CONN.execute(
              "SELECT COUNT(*) n FROM message_receipts WHERE agent_id = "
              "'alice' AND acked_at IS NULL").fetchone()["n"] >= 0
          and b5.CONN.execute(
              "SELECT COUNT(*) n FROM message_receipts WHERE agent_id != "
              "'bob' AND acked_at IS NOT NULL").fetchone()["n"] == 0,
          "it acked another agent's receipts")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — screening acks; only the collector deletes")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
