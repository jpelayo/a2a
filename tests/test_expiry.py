#!/usr/bin/env python3
"""Messages have a shelf life.

    python3 tests/test_expiry.py

A message that is only useful for ten minutes is worse than useless an hour
later: an agent that reads it then acts on a decision already taken. So a
sender can say how long what it wrote is worth reading, and the broker stops
delivering it after that — and collects it, whether or not anyone acked.

Two things must be true at once, and the second is the one that would bite:

  the feature  — an expired message is not delivered, not pending, and is
                 collected even unacked
  the default  — a message sent without an expiry behaves exactly as it did
                 before this existed. The default IS the old 365-day ceiling,
                 so if that drifted, every station would quietly start losing
                 mail early.

Pure python3 with the broker's deps.
"""
import asyncio
import importlib.util
import os
import sqlite3
import subprocess
import sys
import tempfile
import time
from pathlib import Path

import dbharness
import pymysql
import pymysql.cursors

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"
ST = "default"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load(db: Path, **env):
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    for k in ("A2A_MAX_RETENTION_TIME", "A2A_MAX_RETENTION_DAYS"):
        os.environ.pop(k, None)
    os.environ.update(env)
    spec = importlib.util.spec_from_file_location(
        f"broker_{db.stem}_{len(env)}", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return mod


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-exp-"))
    b = load(tmp / "e.db")

    # --- the parser, in every unit -----------------------------------------
    p = b.parse_duration
    check("a bare number is seconds", p(90) == 90)
    check("units agree with each other",
          p("1h") == p("60m") == p("3600s") == 3600, f"{p('1h')} {p('60m')}")
    check("days and weeks", p("7d") == 604800 and p("2w") == 1209600)
    check("unset is not zero — it means 'use the default'",
          p(None) is None and p("") is None)
    for bad in ("soon", 0, -5, "5x"):
        try:
            p(bad)
            check(f"{bad!r} is refused", False, "accepted")
        except ValueError:
            check(f"{bad!r} is refused", True)

    # --- the default is the old ceiling, exactly ---------------------------
    for a in ("poster", "alice", "absent"):
        b.AGENTS.add(ST, a)
    asyncio.run(b.CHANNELS.create(ST, "ops", "", ["poster", "alice"]))

    plain = asyncio.run(b.CHANNELS.post(ST, "ops", "poster", "no expiry given"))
    life = plain["post"]["expires_at"] - plain["post"]["ts"] \
        if "post" in plain else 0
    check("a message with no expires_in lasts MAX_RETENTION — the default is "
          "the old ceiling, not a new policy",
          abs(life - b.MAX_RETENTION) < 2, f"{life} vs {b.MAX_RETENTION}")

    # --- shortening works, lengthening does not ----------------------------
    soon = asyncio.run(
        b.CHANNELS.post(ST, "ops", "poster", "urgent", expires_in="10m"))
    check("a sender can shorten it",
          abs((soon["post"]["expires_at"] - soon["post"]["ts"]) - 600) < 2)

    far = asyncio.run(
        b.CHANNELS.post(ST, "ops", "poster", "forever", expires_in="9999d"))
    check("a sender cannot outlive the station ceiling — clamped, not "
          "honoured",
          abs((far["post"]["expires_at"] - far["post"]["ts"])
              - b.MAX_RETENTION) < 2)

    asyncio.run(b.CHANNELS.create(ST, "brief-room", "", ["poster", "alice"],
                                  {"retention_days": 1}))
    capped = asyncio.run(b.CHANNELS.post(
        ST, "brief-room", "poster", "in a short-lived room", expires_in="30d"))
    check("a channel's own retention wins when it is shorter",
          abs((capped["post"]["expires_at"] - capped["post"]["ts"])
              - 86400) < 2,
          str(capped["post"]["expires_at"] - capped["post"]["ts"]))

    try:
        asyncio.run(b.CHANNELS.post(ST, "ops", "poster", "bad", expires_in="soon"))
        check("a nonsensical expires_in is refused, not treated as 'never'",
              False, "accepted")
    except ValueError:
        check("a nonsensical expires_in is refused, not treated as 'never'", True)

    # --- expiry stops delivery and collects, unacked -----------------------
    asyncio.run(b.DIRECT.send(ST, "poster", "absent", "gone in a moment",
                              expires_in="1s"))
    asyncio.run(b.DIRECT.send(ST, "poster", "absent", "still good"))

    def pending(who):
        return [m.get("text")
                for m in b._resolve_receipts(ST, b._pending_rows(ST, who, 50))]

    check("before it expires, it is pending like anything else",
          "gone in a moment" in pending("absent"), str(pending("absent")))
    time.sleep(1.2)
    after = pending("absent")
    check("once expired it is not delivered — not late, wrong",
          "gone in a moment" not in after, str(after))
    check("and the unexpired one beside it is untouched",
          "still good" in after, str(after))

    msgs, _ = b._fetch_for_agent(ST, "absent", 50, replay=True)
    check("nor replayed on reconnect",
          not any("gone in a moment" in (m.get("text") or "") for m in msgs))

    st = b.collect(ST)
    left = [r["text"] for r in b.CONN.execute("SELECT text FROM dms")]
    check("collected even though nobody acked it — the one exception to the "
          "keep-until-acked guarantee", "gone in a moment" not in left,
          str(left))
    check("collect reports it as expired", st.get("expired", 0) >= 1, str(st))
    check("the unexpired DM survives the sweep", "still good" in left, str(left))
    orphans = b.CONN.execute(
        "SELECT COUNT(*) c FROM message_receipts r WHERE r.kind = 'dm' "
        "AND r.msg_id NOT IN (SELECT id FROM dms)").fetchone()["c"]
    check("its receipts went with it", orphans == 0, f"{orphans} orphaned")

    # --- acking still collects first, deadline or not ----------------------
    dm = asyncio.run(b.DIRECT.send(ST, "poster", "alice", "read me quickly",
                                   expires_in="30m"))
    b._current_station.set(ST)
    b._current_agent.set("alice")
    asyncio.run(b.my_pending())
    b.collect(ST)
    left = [r["text"] for r in b.CONN.execute("SELECT text FROM dms")]
    check("a message its audience read is collected on the ack, not held "
          "until its deadline — the two triggers are independent",
          "read me quickly" not in left, str(left))

    # --- diagnostics do not outlive their answer ---------------------------
    # A ping proves delivery works and is spent on arrival. Stored like
    # ordinary traffic it is kept until acked, so one aimed at an agent that
    # never comes back pins a receipt for a year — and pollutes what it
    # measures, since `doctor` counts pending and its own pings were in the
    # count.
    b._current_station.set(ST)
    b._current_agent.set("alice")
    ping = asyncio.run(b.ping_me())
    ordinary = asyncio.run(b.DIRECT.send(ST, "poster", "alice", "ordinary"))
    pd, od = ping["dm"], ordinary["dm"]
    ping_ttl = pd["expires_at"] - pd["ts"]
    check("a ping expires in PING_TTL, not the station default",
          abs(ping_ttl - b.PING_TTL) < 2, f"{ping_ttl} vs {b.PING_TTL}")
    check("and that is measurably SHORTER than an ordinary DM sent beside it "
          "— asserted as a difference, so changing either default cannot make "
          "this pass by coincidence",
          ping_ttl < (od["expires_at"] - od["ts"]) / 100,
          f"ping {ping_ttl} vs dm {od['expires_at'] - od['ts']}")

    # It must go even though the agent it was testing never acked — which is
    # the whole point, because that agent is usually the one not answering.
    b.CONN.execute("UPDATE dms SET expires_at = %s WHERE id = %s",
                   (time.time() - 1, pd["id"]))
    st_ping = b.collect(ST)
    left = [r["id"] for r in b.CONN.execute("SELECT id FROM dms")]
    check("an expired witness is collected unacked", pd["id"] not in left,
          str(st_ping))

    bp = load(tmp / "ping.db", A2A_PING_TTL="30s")
    check("A2A_PING_TTL is honoured", bp.PING_TTL == 30, str(bp.PING_TTL))
    check("and it is a duration like every other one — '30s' == 30",
          bp.PING_TTL == bp.parse_duration("30s"))

    # --- the deprecated env var still works --------------------------------
    b7 = load(tmp / "legacy.db", A2A_MAX_RETENTION_DAYS="7")
    check("A2A_MAX_RETENTION_DAYS is still honoured rather than ignored",
          abs(b7.MAX_RETENTION - 7 * 86400) < 1, str(b7.MAX_RETENTION))
    bt = load(tmp / "new.db", A2A_MAX_RETENTION_TIME="7d")
    check("and agrees with A2A_MAX_RETENTION_TIME=7d",
          bt.MAX_RETENTION == b7.MAX_RETENTION,
          f"{bt.MAX_RETENTION} vs {b7.MAX_RETENTION}")

    # --- bringing across a database written before expiry existed ----------
    # This used to be an in-place upgrade on open. Storage is MariaDB now, so
    # the same rule lives in `migrate` — and it matters MORE there, not less:
    # the collector deletes `WHERE expires_at <= now`, so importing a
    # pre-expiry database without dating its messages would destroy every one
    # of them on the first collection.
    old = tmp / "old.db"
    con = sqlite3.connect(old)
    con.executescript("""
      CREATE TABLE stations (station_id TEXT PRIMARY KEY, name TEXT NOT NULL
        UNIQUE, description TEXT DEFAULT '', created_at REAL NOT NULL, open
        INTEGER NOT NULL DEFAULT 0);
      INSERT INTO stations VALUES ('default','default','',0,0);
      CREATE TABLE agents (station_id TEXT NOT NULL, agent_id TEXT NOT NULL,
        name TEXT NOT NULL, created_at REAL NOT NULL,
        PRIMARY KEY (station_id, agent_id));
      CREATE TABLE channels (station_id TEXT NOT NULL, name TEXT NOT NULL,
        created_at REAL NOT NULL, PRIMARY KEY (station_id, name));
      CREATE TABLE dms (id TEXT PRIMARY KEY, station_id TEXT NOT NULL,
        sender TEXT NOT NULL, recipient TEXT NOT NULL, text TEXT NOT NULL,
        ts REAL NOT NULL);
      INSERT INTO dms VALUES ('old1','default','a','b','written last year',
        1000000);
    """)
    con.commit()
    con.close()

    # Pinned explicitly. An earlier block in this file puts
    # A2A_MAX_RETENTION_TIME in the environment, and inheriting it here made
    # the assertion compare the subprocess's ceiling against a different
    # module's — a test that fails for a reason unrelated to what it checks.
    env = dict(dbharness.db_env(), A2A_MAX_RETENTION_TIME="30d")
    expected = 30 * 86400
    proc = subprocess.run(
        [sys.executable, str(BROKER), "migrate", str(old)],
        env=env, input="y\n", capture_output=True, text=True)
    check("migrate imports a pre-expiry database", proc.returncode == 0,
          proc.stdout[-400:] + proc.stderr[-400:])

    conn = pymysql.connect(host=env["A2A_DB_HOST"], port=int(env["A2A_DB_PORT"]),
                           user=env["A2A_DB_USER"], password=env["A2A_DB_PASSWORD"],
                           database=env["A2A_DB_NAME"],
                           cursorclass=pymysql.cursors.DictCursor)
    with conn.cursor() as cur:
        cur.execute("SELECT ts, expires_at FROM dms WHERE id = 'old1'")
        row = cur.fetchone()
    conn.close()
    check("an imported message is dated to the ceiling it already had — "
          "without this the collector would delete every one of them at once",
          row is not None
          and abs((row["expires_at"] - row["ts"]) - expected) < 2,
          str(row))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — messages expire, and the default is unchanged")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
