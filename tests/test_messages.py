#!/usr/bin/env python3
"""What is in a station, and what is holding it.

    python3 tests/test_messages.py

`doctor` names agents that pin things and `compact` reports what it removed;
neither answers "why will this station not shrink". The messages view does,
because the collector's reasons are precise: a message survives because its
audience has not all acked, because it has NO audience at all, because it is a
broadcast that is not closed, or because the collector has not run.

The property this suite exists to protect is the one the whole ephemerality
argument rests on: **marking a segment deletes nothing.** It acks, or it sets
an expiry, and `collect()` remains the only thing in the broker that removes a
row. That is asserted with the collector stubbed out, because with it running
the difference is invisible.

Needs a MariaDB/MySQL server; see dbharness.
"""
import importlib.util
import os
import sys
import time
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
    spec = importlib.util.spec_from_file_location("a2a_msg", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return mod


def seg(stats: dict, name: str) -> int:
    return next(r["count"] for r in stats["rows"] if r["segment"] == name)


def kinds(stats: dict, name: str) -> dict:
    return next(r["by_kind"] for r in stats["rows"] if r["segment"] == name)


def windows() -> None:
    """The age buckets partition time. No database — so this half always runs.

    `age_window` exists to be checkable here: the boundaries are declared once
    and the three tables read them, so a bucket that overlaps its neighbour or
    leaves a gap would put a message in two rows or in none, and the operator
    would be reading a total that is not the station.
    """
    spec = importlib.util.spec_from_file_location("a2a_msg_pure", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)                 # no _startup: this touches no DB
    now = 1_700_000_000.0

    def bucket(ts: float) -> list[str]:
        return [s for s in b.AGE_SEGMENTS
                for lo, hi in [b.age_window(s, now)]
                if (lo is None or ts > lo) and (hi is None or ts <= hi)]

    for days, label, want in ((0, "just now", "age_day"),
                              (2 / 24, "two hours ago", "age_day"),
                              (1, "exactly a day ago", "age_week"),
                              (3, "three days ago", "age_week"),
                              (7, "exactly a week ago", "age_month"),
                              (14, "a fortnight ago", "age_month"),
                              (30, "exactly a month ago", "age_older"),
                              (3650, "ten years ago", "age_older")):
        got = bucket(now - days * 86400)
        check(f"a message posted {label} is in exactly one bucket, {want}",
              got == [want], str(got))

    check("a message dated in the FUTURE — a client with a wrong clock — has "
          "no bucket above it and lands in the newest, which is the harmless "
          "end: it is reported as new rather than vanishing from every row",
          bucket(now + 3600) == ["age_day"], str(bucket(now + 3600)))
    check("the newest bucket is open above and the oldest open below, so "
          "every timestamp that can exist has a row",
          b.age_window("age_day", now)[1] is None
          and b.age_window("age_older", now)[0] is None)
    check("each bucket starts exactly where the previous one ends — no gap, "
          "no overlap, which is what makes the totals add up",
          all(b.age_window(a, now)[0] == b.age_window(c, now)[1]
              for a, c in zip(b.AGE_SEGMENTS, b.AGE_SEGMENTS[1:])))
    check("and a segment from another view is refused rather than silently "
          "windowed",
          _raises(lambda: b.age_window("far", now), ValueError))


def _raises(fn, exc) -> bool:
    try:
        fn()
    except exc:
        return True
    return False


def main() -> int:
    windows()
    print()
    dbharness.require_db()
    b = load()
    now = time.time()
    far, soon, overdue = now + 400 * 86400, now + 3 * 86400, now - 10

    st = b.STATIONS.create("acme")
    sid = st["station_id"]
    other = b.STATIONS.create("beta")["station_id"]
    for a in ("alice", "bob"):
        b.AGENTS.add(sid, a)
    for ch, members in (("room", '["alice","bob"]'), ("empty", "[]")):
        b.CONN.execute("INSERT INTO channels (station_id, name, members, "
                       "created_at) VALUES (%s, %s, %s, %s)",
                       (sid, ch, members, now))
    b.CONN.execute("INSERT INTO channels (station_id, name, created_at) "
                   "VALUES (%s, 'room', %s)", (other, now))

    def post(mid, channel, expires, receipts, station=None):
        s = station or sid
        b.CONN.execute(
            "INSERT INTO transcripts (id, station_id, channel, ts, sender, "
            "text, expires_at) VALUES (%s, %s, %s, %s, 'alice', 'hi', %s)",
            (mid, s, channel, now, expires))
        for who, acked in receipts:
            b.CONN.execute(
                "INSERT INTO message_receipts (station_id, msg_id, kind, "
                "agent_id, ts, expires_at, acked_at) "
                "VALUES (%s, %s, 'channel', %s, %s, %s, %s)",
                (s, mid, who, now, expires, now if acked else None))

    post("m-unread",  "room",  far,     [("alice", False), ("bob", False)])
    post("m-partial", "room",  far,     [("alice", True),  ("bob", False)])
    post("m-acked",   "room",  far,     [("alice", True),  ("bob", True)])
    post("m-orphan",  "empty", far,     [])
    post("m-overdue", "room",  overdue, [("alice", False), ("bob", False)])
    post("m-soon",    "room",  soon,    [("alice", False), ("bob", False)])
    post("m-noexp",   "room",  0,       [("alice", False), ("bob", False)])
    post("b-unread",  "room",  far,     [("alice", False)], station=other)

    stats = b.message_stats(sid)

    # --- the two views are independent partitions --------------------------
    # Every message appears in exactly one row of each group, so each group
    # sums to the station total and the eight numbers together are twice it.
    # Getting this wrong would double-count or lose messages silently.
    ack = sum(r["count"] for r in stats["rows"] if r["group"] == "ack")
    exp = sum(r["count"] for r in stats["rows"] if r["group"] == "expiry")
    check("the ack view covers every message exactly once",
          ack == stats["total"] == 7, f"{ack} vs {stats['total']}")
    check("and the shelf-life view covers every message that HAS one — "
          "broadcasts age by created_at, so they are excluded and counted",
          exp == stats["expiry_total"], f"{exp} vs {stats['expiry_total']}")

    check("a message half its audience has read is `partially read` — not in "
          "both neighbours, and not in neither",
          seg(stats, "unread") == 4 and seg(stats, "partial") == 1
          and seg(stats, "acked") == 1,
          f"unread {seg(stats,'unread')} partial {seg(stats,'partial')} "
          f"acked {seg(stats,'acked')}")
    check("a post to a channel with no members is `no audience`",
          seg(stats, "orphan") == 1, str(seg(stats, "orphan")))
    check("the shelf-life rows split by deadline",
          (seg(stats, "no_expiry"), seg(stats, "overdue"),
           seg(stats, "near")) == (1, 1, 1),
          str([seg(stats, s) for s in ("no_expiry", "overdue", "near", "far")]))
    check("and it names who is holding the unacked ones, as doctor does",
          dict(stats["holders"]).get("bob") == 5, str(stats["holders"]))

    # --- no audience is exactly the case acks cannot fix --------------------
    b._collect_station(sid, now)
    left = {r["id"] for r in b.CONN.execute(
        "SELECT id FROM transcripts WHERE station_id = %s", (sid,))}
    check("collecting does NOT remove a message with no audience — rules 1 "
          "and 2 only look at messages that have receipts, which is why this "
          "row is worth having at all",
          "m-orphan" in left, str(sorted(left)))

    # --- marking deletes nothing by itself ---------------------------------
    real = b._collect_station
    b._collect_station = lambda s, n=None: {"stubbed": 0}
    try:
        before = b.CONN.execute(
            "SELECT COUNT(*) n FROM transcripts WHERE station_id = %s",
            (sid,)).fetchone()["n"]
        out = b.mark_segment(sid, "unread")
        after = b.CONN.execute(
            "SELECT COUNT(*) n FROM transcripts WHERE station_id = %s",
            (sid,)).fetchone()["n"]
        check("marking a segment DELETES NOTHING by itself — collect() stays "
              "the only thing in the broker that removes a row",
              before == after and out["acked"] > 0,
              f"{before} -> {after}, acked {out['acked']}")
        unacked = b.CONN.execute(
            "SELECT COUNT(*) n FROM message_receipts WHERE station_id = %s "
            "AND acked_at IS NULL", (sid,)).fetchone()["n"]
        check("what moved is acked_at, nothing else",
              unacked == 1, f"{unacked} left unacked (bob on m-partial)")
    finally:
        b._collect_station = real

    check("and only then does the collector take them",
          b._collect_station(sid, now)["transcripts"] > 0, "nothing collected")

    # --- an expiry segment removes messages nobody ever acked --------------
    # Rule 4 is the only rule that does, and that is the point of the row.
    post("x-1", "room", far, [("alice", False), ("bob", False)])
    post("x-2", "room", far, [("alice", False), ("bob", False)])
    out = b.mark_segment(sid, "far")
    gone = b.CONN.execute(
        "SELECT COUNT(*) n FROM transcripts WHERE station_id = %s AND id IN "
        "('x-1','x-2')", (sid,)).fetchone()["n"]
    check("marking a shelf-life segment retires messages NO agent acked — "
          "expiry is the one rule that can, and the reason an abandoned agent "
          "cannot pin a queue forever",
          gone == 0 and out["expired"] >= 2,
          f"{gone} left, expired {out['expired']}")

    # --- a preview changes nothing -----------------------------------------
    post("p-1", "room", far, [("alice", False)])
    pre = b.mark_segment(sid, "unread", preview=True)
    still = b.CONN.execute(
        "SELECT COUNT(*) n FROM message_receipts WHERE station_id = %s AND "
        "msg_id = 'p-1' AND acked_at IS NULL", (sid,)).fetchone()["n"]
    check("a dry run counts what a real one would touch",
          pre["found"] >= 1, str(pre))
    check("and changes nothing", still == 1, f"{still} unacked left")

    # --- an open broadcast is silenced, not destroyed ----------------------
    b.CONN.execute(
        "INSERT INTO broadcasts (id, station_id, sender, problem, created_at, "
        "updated_at, status) VALUES ('bc1', %s, 'alice', 'help', %s, %s, "
        "'open')", (sid, now, now))
    b.CONN.execute(
        "INSERT INTO message_receipts (station_id, msg_id, kind, agent_id, ts, "
        "expires_at) VALUES (%s, 'bc1', 'broadcast', 'bob', %s, %s)",
        (sid, now, far))
    out = b.mark_segment(sid, "unread")
    alive = b.CONN.execute(
        "SELECT status FROM broadcasts WHERE id = 'bc1'").fetchone()
    check("a fully-acked but OPEN broadcast is silenced, never destroyed — "
          "deleting one needs status='closed' as well",
          alive is not None and out.get("open_broadcasts", 0) >= 1,
          f"{alive}, open {out.get('open_broadcasts')}")
    b.CONN.execute("UPDATE broadcasts SET status = 'closed' WHERE id = 'bc1'")
    b._collect_station(sid, now)
    check("and closing it is what lets the collector take it",
          b.CONN.execute("SELECT COUNT(*) n FROM broadcasts WHERE id = 'bc1'"
                         ).fetchone()["n"] == 0, "still there")

    # --- station scoping ----------------------------------------------------
    other_before = b.message_stats(other)["total"]
    b.mark_segment(sid, "unread")
    check("marking one station never reaches into another — the invariant "
          "every query in this file is built on",
          b.message_stats(other)["total"] == other_before == 1,
          str(b.message_stats(other)["total"]))

    # --- honest counts on a second run --------------------------------------
    again = b.mark_segment(sid, "unread")
    check("marking twice reports 0 the second time, not a fabricated number",
          again["found"] == 0 and again["acked"] == 0, str(again))

    try:
        b.mark_segment(sid, "not-a-segment")
        check("an unknown segment is refused", False, "no error raised")
    except ValueError:
        check("an unknown segment is refused", True)

    # --- the age view: how long has this been sitting here ------------------
    # Its own station, because the two views above are asserted against `sid`
    # down to the message. The point of this view is the one thing the other
    # two cannot say: a station reads healthy on both while a year of
    # transcript quietly accumulates.
    aged = b.STATIONS.create("aged")["station_id"]
    b.CONN.execute("INSERT INTO channels (station_id, name, created_at) "
                   "VALUES (%s, 'room', %s)", (aged, now))
    for mid, days in (("a-2h", 2 / 24), ("a-3d", 3), ("a-14d", 14),
                      ("a-90d", 90)):
        b.CONN.execute(
            "INSERT INTO transcripts (id, station_id, channel, ts, sender, "
            "text, expires_at) VALUES (%s, %s, 'room', %s, 'alice', 'hi', %s)",
            (mid, aged, now - days * 86400, far))
    b.CONN.execute(
        "INSERT INTO dms (id, station_id, sender, recipient, `text`, ts, "
        "expires_at) VALUES ('d-3d', %s, 'alice', 'bob', 'hi', %s, %s)",
        (aged, now - 3 * 86400, far))
    # A broadcast, which has no expires_at at all — the case that makes this
    # view different from the one above it.
    b.CONN.execute(
        "INSERT INTO broadcasts (id, station_id, sender, problem, created_at, "
        "updated_at, status) VALUES ('bc-90d', %s, 'alice', 'help', %s, %s, "
        "'open')", (aged, now - 90 * 86400, now))

    st = b.message_stats(aged, now)
    check("the age rows split by when it was posted",
          (seg(st, "age_day"), seg(st, "age_week"), seg(st, "age_month"),
           seg(st, "age_older")) == (1, 2, 1, 2),
          str([seg(st, s) for s in ("age_day", "age_week", "age_month",
                                    "age_older")]))
    check("and each row names the kinds in it, so 'over a month' says WHAT is "
          "over a month",
          kinds(st, "age_week") == {"channel": 1, "dm": 1}
          and kinds(st, "age_older") == {"channel": 1, "broadcast": 1},
          f"{kinds(st, 'age_week')} / {kinds(st, 'age_older')}")

    age = sum(r["count"] for r in st["rows"] if r["group"] == "age")
    check("the age view covers EVERY message, broadcasts included — a "
          "broadcast has no deadline, so the shelf-life view skips it, but it "
          "does have a birthday. That is the whole difference between the two",
          age == st["total"] == st["age_total"] == 6,
          f"{age} vs total {st['total']}")
    check("while the shelf-life view is still short by exactly the broadcasts",
          st["expiry_total"] == 5
          and sum(r["count"] for r in st["rows"]
                  if r["group"] == "expiry") == 5,
          str(st["expiry_total"]))

    # --- and it is a LEVER, not just a report -------------------------------
    # It shipped read-only, on the reasoning that expiry cannot touch a
    # broadcast so the action would cover less than the row. The live station
    # that followed had 162 channel posts, every one of them `no audience`:
    # no ack could ever free them, and the only two rows that COULD act took
    # all 162 regardless of age. "Retire what is older than a month" was the
    # one operation the view existed to make visible and the only one that
    # could not be performed.
    pre = b.mark_segment(aged, "age_older", preview=True)
    check("a preview of an age row counts it and changes nothing",
          pre["found"] == 2 and pre["expired"] == 0
          and b.message_stats(aged, now)["total"] == 6, str(pre))
    check("and it says up front which kinds it will not touch, rather than "
          "reporting a number afterwards that covered less than the row",
          pre["untouched"] == {"broadcast": 1}, str(pre.get("untouched")))

    out = b.mark_segment(aged, "age_older")
    check("marking an age row EXPIRES what has an expiry — the only mechanism "
          "that retires a message no agent ever acked, which is exactly what "
          "an orphaned transcript is",
          out["expired"] == 1, str(out))
    check("the broadcast is reported untouched instead of raising: it has no "
          "expires_at, it ages by created_at, and backdating that would be "
          "forging when it was created",
          out["untouched"] == {"broadcast": 1}, str(out.get("untouched")))
    left = {r["id"] for r in b.CONN.execute(
        "SELECT id FROM transcripts WHERE station_id = %s", (aged,))}
    check("and the collector then takes the expired post",
          "a-90d" not in left and "a-14d" in left, str(sorted(left)))
    check("while the broadcast is still there — untouched means untouched",
          b.CONN.execute("SELECT COUNT(*) n FROM broadcasts WHERE id = "
                         "'bc-90d'").fetchone()["n"] == 1)

    after = b.message_stats(aged, now)
    check("the row is down to what could not be marked, so pressing it twice "
          "reports honestly rather than counting the same thing again",
          seg(after, "age_older") == 1
          and kinds(after, "age_older") == {"broadcast": 1},
          str(kinds(after, "age_older")))
    check("and the younger rows are untouched — expiring by age takes the "
          "bucket it was pressed on and nothing above it",
          (seg(after, "age_day"), seg(after, "age_week"),
           seg(after, "age_month")) == (1, 2, 1),
          str([seg(after, s) for s in ("age_day", "age_week", "age_month")]))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — the view explains the station; only the collector deletes")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
