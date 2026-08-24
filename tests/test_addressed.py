#!/usr/bin/env python3
"""Who a post is FOR, as opposed to who receives it.

    python3 tests/test_addressed.py

Everyone in a channel receives every post, and the message is kept until all of
them ack — that is the delivery guarantee and it must not move. But the
semantic recipient is usually one agent: "this is my answer to alice, the rest
of you should see it". `to` already carried that intent and the broker threw it
away, because `_channel_audience` used it to widen the audience and nothing
stored it.

Now it is recorded in message_addressees and shown to every reader. What this
file pins:

  SOFT          an addressee writes no receipt, narrows no audience and
                changes no retention. That is the whole design: the moment it
                decides delivery it becomes the body-scanning routing
                _channel_audience exists to keep out.
  it travels    the `to` attribute was computed and rendered by all three
                clients for months and never put on the wire by
                _format_stream_line. An annotation nobody receives is not an
                annotation.
  it is history it follows a rename, it survives the addressee being deleted,
                and it dies with the message by foreign key.

The stream-line checks need no database. The registry checks use dbharness.
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
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("addr_broker", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def wire(b) -> None:
    """Two words, one meaning each, on every message."""
    import json as _json

    m = {"channel": "advisory", "sender": "bob", "text": "hi", "ts": 1.0,
         "id": "m1", "kind": "channel",
         "audience": ["alice", "carol"], "addressed": ["alice"]}
    out = _json.loads(b._format_stream_line(m, True))

    check("the stream line carries `audience` — everyone who received it and "
          "owes an ack. It was computed for months and never actually sent, "
          "so the attribute could not appear on a pushed message",
          out.get("audience") == ["alice", "carol"], str(out))
    check("and `addressed` — who it was written for, always a subset of the "
          "audience",
          out.get("addressed") == ["alice"], str(out))
    check("the two are different sets, which is the entire point of having "
          "two words",
          out["addressed"] != out["audience"]
          and set(out["addressed"]) < set(out["audience"]), str(out))
    check("nothing is called `to` any more — one word meant 'who it is for' "
          "on the way in and 'everyone who got it' on the way out",
          "to" not in out, str(out))

    room = _json.loads(b._format_stream_line(
        {k: v for k, v in m.items() if k != "addressed"}, True))
    check("a post to the room still carries BOTH keys, with `addressed` "
          "empty — an absent key and an empty one are the same thing to a "
          "reader who has to guess, and empty MEANS the room",
          room.get("addressed") == [] and room.get("audience"), str(room))
    for kind in ("dm", "broadcast"):
        line = _json.loads(b._format_stream_line(dict(m, kind=kind), True))
        check(f"a {kind} carries both keys too — EVERY message has them",
              "audience" in line and "addressed" in line, str(line))
    check("the human-readable line is unchanged — it is read by people, and "
          "an id list helps nobody there",
          "addressed" not in b._format_stream_line(m, False)
          and "audience" not in b._format_stream_line(m, False))


def registry(b) -> None:
    b._startup()
    sid = b.STATIONS.create("addr")["station_id"]
    for who in ("bob", "alice", "carol", "outsider"):
        b.AGENTS.add(sid, who)
    asyncio.run(b.CHANNELS.create(
        sid, "advisory", "advice", ["bob", "alice", "carol"]))

    def addressees(msg_id: str) -> list[str]:
        return [r["agent_id"] for r in b.CONN.execute(
            "SELECT agent_id FROM message_addressees WHERE station_id = %s "
            "AND msg_id = %s ORDER BY agent_id", (sid, msg_id)).fetchall()]

    def receipts(msg_id: str) -> list[str]:
        return sorted(r["agent_id"] for r in b.CONN.execute(
            "SELECT agent_id FROM message_receipts WHERE station_id = %s "
            "AND msg_id = %s", (sid, msg_id)).fetchall())

    # --- a member, named ----------------------------------------------------
    out = asyncio.run(b.CHANNELS.post(
        sid, "advisory", "bob", "answering you", addressed=["alice"]))
    mid = out["post"]["id"]

    check("naming a member records the addressee", addressees(mid) == ["alice"])
    check("while the audience is every other member, exactly as before — "
          "addressing is an annotation, not a filter",
          receipts(mid) == ["alice", "carol"], str(receipts(mid)))
    check("and the post reports both, so the caller can see what it did",
          out["post"]["addressed"] == ["alice"]
          and sorted(out["post"]["audience"]) == ["alice", "carol"],
          str(out["post"]))

    # --- SOFT: retention is untouched ---------------------------------------
    b._ack_receipts(sid, "alice", [mid])
    b.collect(sid)
    still = b.CONN.execute(
        "SELECT 1 FROM transcripts WHERE id = %s", (mid,)).fetchone()
    check("the addressee acking is NOT enough to retire the message — carol "
          "received it and has not, and a soft addressee that shortened "
          "anybody's window would be routing wearing a label",
          still is not None)
    b._ack_receipts(sid, "carol", [mid])
    b.collect(sid)
    check("once the WHOLE audience has acked it goes, on the same rule as "
          "any other post",
          b.CONN.execute("SELECT 1 FROM transcripts WHERE id = %s",
                         (mid,)).fetchone() is None)
    check("and its addressee rows went with it by foreign key, so retirement "
          "needed no code of its own — collect() is still the only thing that "
          "deletes anything",
          addressees(mid) == [])

    # --- a non-member cannot be named ---------------------------------------
    # This used to WIDEN the audience. A channel post now never reaches outside
    # the channel, so naming somebody who is not in it is refused: the label
    # would point at an agent who never receives the post.
    for who in ("outsider", "ghost"):
        try:
            asyncio.run(b.CHANNELS.post(
                sid, "advisory", "bob", "pulling you in", addressed=[who]))
            check(f"addressing {who!r}, who is not a member, is refused",
                  False, "accepted")
        except ValueError as e:
            check(f"addressing {who!r}, who is not a member, is refused",
                  who in str(e), str(e))
            check(f"and the refusal for {who!r} says how to actually reach "
                  f"them, rather than only saying no",
                  "add_channel_member" in str(e) and "send_dm" in str(e),
                  str(e))

    # --- addressing yourself, and addressing nobody -------------------------
    out = asyncio.run(b.CHANNELS.post(
        sid, "advisory", "bob", "thinking aloud", addressed=["bob"]))
    check("addressing yourself records nothing: you are excluded from your "
          "own audience, so it would label a post as being for somebody who "
          "cannot receive it",
          addressees(out["post"]["id"]) == [])
    out = asyncio.run(b.CHANNELS.post(sid, "advisory", "bob", "to the room"))
    check("an ordinary post has no addressee", addressees(out["post"]["id"]) == [])

    # --- it follows a rename ------------------------------------------------
    live = asyncio.run(b.CHANNELS.post(
        sid, "advisory", "bob", "for alice", addressed=["alice"]))["post"]["id"]
    b.AGENTS.rename(sid, "alice", "alice-2")
    check("a rename rewrites the addressee — it is shown to every reader, so "
          "a stale id labels the post as being for somebody who is gone",
          addressees(live) == ["alice-2"], str(addressees(live)))

    # --- the pull side agrees with the push side ----------------------------
    page = asyncio.run(b.CHANNELS.messages_since(sid, "advisory", None, 50))
    row = [m for m in page if m["id"] == live][0]
    check("read_channel reports it too, so an agent catching up sees the same "
          "addressing as one that was pushed",
          row.get("addressed") == ["alice-2"], str(row))
    check("while a room post on the same page carries no addressee — the pull "
          "side omits the key rather than inventing one, since a transcript "
          "row is a record and not a delivery",
          all(not m.get("addressed") for m in page if m["id"] != live),
          str(page))

    # --- deleting the addressee --------------------------------------------
    b.AGENTS.remove("alice-2", sid)
    check("removing the agent leaves the record standing: who a post was "
          "addressed to is history, it pins no collection the way a receipt "
          "does, and it retires with the message",
          addressees(live) == ["alice-2"], str(addressees(live)))


def main() -> int:
    b = load()
    wire(b)

    print()
    try:
        dbharness.require_db()
    except SystemExit:
        print("(registry checks skipped: no database)\n")
    else:
        os.environ.update(dbharness.db_env())
        registry(load())

    print()
    if fails:
        print(f"{len(fails)} failure(s):")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("PASS — addressed says who it is for, and changes nothing else")
    return 0


if __name__ == "__main__":
    sys.exit(main())
