#!/usr/bin/env python3
"""The properties that only exist because storage is MariaDB.

    python3 tests/test_mariadb.py

The other suites test what the broker does. This one tests what the move to
MariaDB could silently break — every assertion here corresponds to something
that was wrong at some point during the port, or that the database's defaults
would get wrong if a table were ever recreated by hand.

Needs a MariaDB/MySQL server; see dbharness.
"""
import os
import shutil
import subprocess
import sqlite3
import sys
import tempfile
import time
from pathlib import Path

import dbharness
import pymysql
import pymysql.cursors

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def run(env: dict, *args, stdin: str = "") -> subprocess.CompletedProcess:
    return subprocess.run([sys.executable, str(BROKER), *args],
                          env=dict(os.environ, **env), input=stdin,
                          capture_output=True, text=True)


def connect(env: dict):
    return pymysql.connect(
        host=env["A2A_DB_HOST"], port=int(env["A2A_DB_PORT"]),
        user=env["A2A_DB_USER"], password=env["A2A_DB_PASSWORD"],
        database=env["A2A_DB_NAME"], cursorclass=pymysql.cursors.DictCursor,
        autocommit=True)


def q(env: dict, sql: str, params=()):
    conn = connect(env)
    try:
        with conn.cursor() as cur:
            cur.execute(sql, params)
            return cur.fetchall()
    finally:
        conn.close()


def legacy_sqlite(path: Path, n_msgs: int, base_ts: float,
                  with_expiry: bool = True) -> Path:
    """A database in the shape the sqlite broker left behind."""
    con = sqlite3.connect(path)
    exp = ", expires_at REAL NOT NULL DEFAULT 0" if with_expiry else ""
    con.executescript(f"""
      CREATE TABLE stations (station_id TEXT PRIMARY KEY, name TEXT NOT NULL
        UNIQUE, description TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL,
        open INTEGER NOT NULL DEFAULT 0);
      CREATE TABLE agents (station_id TEXT NOT NULL, agent_id TEXT NOT NULL,
        name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
        expertise TEXT NOT NULL DEFAULT '[]', projects TEXT NOT NULL
        DEFAULT '[]', system_prompt TEXT NOT NULL DEFAULT '', metadata TEXT
        NOT NULL DEFAULT '{{}}', created_at REAL NOT NULL,
        owner_token_hash TEXT, PRIMARY KEY (station_id, agent_id));
      CREATE TABLE channels (station_id TEXT NOT NULL, name TEXT NOT NULL,
        theme TEXT NOT NULL DEFAULT '', members TEXT NOT NULL DEFAULT '[]',
        policy TEXT NOT NULL DEFAULT '{{}}', created_at REAL NOT NULL,
        PRIMARY KEY (station_id, name));
      CREATE TABLE transcripts (id TEXT PRIMARY KEY, station_id TEXT NOT NULL,
        channel TEXT NOT NULL, ts REAL NOT NULL, sender TEXT NOT NULL,
        text TEXT NOT NULL{exp});
      INSERT INTO stations VALUES ('s1','acme','Acme Corp',{base_ts},0);
      INSERT INTO channels (station_id,name,created_at)
        VALUES ('s1','advisory',{base_ts});
    """)
    for i in range(n_msgs):
        cols = "(?,?,?,?,?,?,?)" if with_expiry else "(?,?,?,?,?,?)"
        vals = ["m%d" % i, "s1", "advisory", base_ts + i, "worker",
                "message %d — ñ 日本 <&>" % i]
        if with_expiry:
            vals.append(base_ts + 999999)
        con.execute(f"INSERT INTO transcripts VALUES {cols}", vals)
    con.commit()
    con.close()
    return path



WAL_FIXTURE = r'''
import sqlite3, os, sys, time
db, n, now = sys.argv[1], int(sys.argv[2]), time.time()
c = sqlite3.connect(db)
c.execute("PRAGMA journal_mode=WAL")
c.executescript("""
CREATE TABLE stations (station_id TEXT PRIMARY KEY, name TEXT NOT NULL UNIQUE,
  description TEXT NOT NULL DEFAULT '', created_at REAL NOT NULL,
  open INTEGER NOT NULL DEFAULT 0);
CREATE TABLE agents (station_id TEXT NOT NULL, agent_id TEXT NOT NULL,
  name TEXT NOT NULL, description TEXT NOT NULL DEFAULT '',
  expertise TEXT NOT NULL DEFAULT '[]', projects TEXT NOT NULL DEFAULT '[]',
  system_prompt TEXT NOT NULL DEFAULT '', metadata TEXT NOT NULL DEFAULT '{}',
  created_at REAL NOT NULL, owner_token_hash TEXT,
  PRIMARY KEY (station_id, agent_id));
CREATE TABLE channels (station_id TEXT NOT NULL, name TEXT NOT NULL,
  theme TEXT NOT NULL DEFAULT '', members TEXT NOT NULL DEFAULT '[]',
  policy TEXT NOT NULL DEFAULT '{}', created_at REAL NOT NULL,
  PRIMARY KEY (station_id, name));
CREATE TABLE transcripts (id TEXT PRIMARY KEY, station_id TEXT NOT NULL,
  channel TEXT NOT NULL, ts REAL NOT NULL, sender TEXT NOT NULL,
  text TEXT NOT NULL, expires_at REAL NOT NULL DEFAULT 0);
""")
c.execute("INSERT INTO stations VALUES ('s1','acme','Acme',?,0)", (now,))
c.execute("INSERT INTO channels (station_id,name,created_at) VALUES ('s1','advisory',?)", (now,))
c.execute("INSERT INTO agents (station_id,agent_id,name,created_at) VALUES ('s1','worker','worker',?)", (now,))
c.commit()
c.execute("PRAGMA wal_checkpoint(TRUNCATE)")   # the .db is genuinely non-empty
# A reader pins the WAL open, so nothing below ever reaches the .db file.
hold = sqlite3.connect(db); hold.execute("BEGIN")
hold.execute("SELECT * FROM stations").fetchall()
for i in range(n):
    c.execute("INSERT INTO transcripts VALUES (?,?,?,?,?,?,?)",
              ("m%d" % i, "s1", "advisory", now + i, "worker",
               "only in the WAL %d — ñ 日本" % i, now + 999999))
c.commit()
# Exit with both connections OPEN: killed, the way a container stop kills it.
os._exit(0)
'''


def wal_sqlite(path: Path, n_msgs: int) -> Path:
    """A source whose newest writes exist ONLY in its write-ahead log.

    Built in a subprocess that is killed with its connections open, because
    that is the only way the WAL survives: a writer that exits cleanly
    checkpoints on close, and a fixture built that way passes whatever the
    code does.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    subprocess.run([sys.executable, "-c", WAL_FIXTURE, str(path),
                    str(n_msgs)], check=False, capture_output=True)
    return path


def rows_in_db_alone(path: Path, tmp: Path) -> int:
    """What importing the .db WITHOUT its WAL would have seen."""
    alone = tmp / f"alone-{path.stem}.db"
    shutil.copyfile(path, alone)
    conn = sqlite3.connect(f"file:{alone}?mode=ro", uri=True)
    try:
        return conn.execute("SELECT COUNT(*) FROM transcripts").fetchone()[0]
    finally:
        conn.close()


def main() -> int:
    dbharness.require_db()
    tmp = Path(tempfile.mkdtemp(prefix="a2a-mariadb-"))

    # --- collation: the trap the database's own default would spring -------
    # Agent ids are matched literally and delivery is a destructive read, so
    # `Foo` and `foo` are two agents. MariaDB's default collation is case
    # INSENSITIVE: under it the second one collides with the first and the
    # unique constraint rejects it. This is the assertion that fails the day
    # somebody recreates a table without utf8mb4_bin.
    env = dbharness.db_env()
    run(env, "station", "create", "acme")
    a = run(env, "agent", "add", "Foo", "--station", "acme")
    b = run(env, "agent", "add", "foo", "--station", "acme")
    check("`Foo` and `foo` are two different agents",
          a.returncode == 0 and b.returncode == 0,
          (a.stderr + b.stderr)[:200])
    rows = q(env, "SELECT agent_id FROM agents WHERE agent_id IN ('Foo','foo')")
    check("and both are stored, distinctly", len(rows) == 2, str(rows))

    # A station name is unique — case-sensitively, for the same reason.
    run(env, "station", "create", "ACME")
    names = {r["name"] for r in q(env, "SELECT name FROM stations")}
    check("station names are case-sensitive too",
          {"acme", "ACME"} <= names, str(names))

    # --- text that a bad charset conversion would quietly damage -----------
    weird = "agente-ñandú-日本-🙂"
    run(env, "agent", "add", weird, "--station", "acme")
    rows = q(env, "SELECT agent_id FROM agents WHERE agent_id = %s", (weird,))
    check("non-ASCII and astral characters survive a round trip byte for byte",
          len(rows) == 1 and rows[0]["agent_id"] == weird, str(rows))

    # --- rowcount is CHANGED rows, not matched -----------------------------
    # screen() and _ack_receipts pair `WHERE acked_at IS NULL` with rowcount to
    # report what they actually did. With CLIENT.FOUND_ROWS on, the second run
    # would claim to have acked everything all over again.
    env2 = dbharness.db_env()
    run(env2, "station", "create", "acme")
    run(env2, "agent", "add", "alice", "--station", "acme")
    run(env2, "agent", "add", "bob", "--station", "acme")
    run(env2, "channel", "create", "room", "--station", "acme",
        "--members", "alice,bob")
    conn = connect(env2)
    with conn.cursor() as cur:
        cur.execute("SELECT station_id FROM stations WHERE name = 'acme'")
        sid = cur.fetchone()["station_id"]
        now = time.time()
        cur.execute(
            "INSERT INTO transcripts (id, station_id, channel, ts, sender, "
            "text, expires_at) VALUES ('t1', %s, 'room', %s, 'alice', 'hi', %s)",
            (sid, now, now + 99999))
        cur.execute(
            "INSERT INTO message_receipts (station_id, msg_id, kind, agent_id, "
            "ts, expires_at) VALUES (%s, 't1', 'channel', 'bob', %s, %s)",
            (sid, now, now + 99999))
    conn.close()
    first = run(env2, "station", "screen", "acme")
    second = run(env2, "station", "screen", "acme")
    check("screening reports what it acked", "1" in first.stdout, first.stdout[:200])
    check("and screening again reports 0, not a fabricated number — rowcount "
          "must be changed rows, never matched rows",
          " 0" in second.stdout or "0 " in second.stdout, second.stdout[:200])

    # --- logging must survive the database being gone ----------------------
    # The whole argument for putting logs in the database rests on this: the
    # lines explaining a database fault must not need the database.
    dead = dict(dbharness.db_env(), A2A_DB_PORT="1")   # nothing listens there
    p = run(dead, "station", "list")
    check("an unreachable database fails with a sentence, not a traceback",
          p.returncode != 0 and "cannot reach MariaDB" in p.stderr
          and "Traceback" not in p.stderr, (p.stderr or p.stdout)[:240])

    # --- migrate ------------------------------------------------------------
    src = legacy_sqlite(tmp / "live.db", 25, time.time() - 3600)
    menv = dbharness.db_env()
    p = run(menv, "migrate", str(src), stdin="y\n")
    check("migrate imports a legacy database", p.returncode == 0,
          (p.stdout + p.stderr)[-300:])
    check("and says the counts and checksums match",
          "counts and checksums match" in p.stdout, p.stdout[-300:])
    got = q(menv, "SELECT COUNT(*) n FROM transcripts")[0]["n"]
    check("every message came across", got == 25, str(got))
    body = q(menv, "SELECT text FROM transcripts WHERE id = 'm0'")[0]["text"]
    check("with its text intact", "ñ 日本 <&>" in body, body)

    # The source must be untouched — it is the rollback.
    before = src.read_bytes()
    run(menv, "migrate", str(src), stdin="n\n")
    check("the sqlite source is never written to",
          src.read_bytes() == before, "source changed")

    # A second import into a populated database is refused, not merged.
    p = run(menv, "migrate", str(src), stdin="y\n")
    check("migrating into a non-empty database is refused",
          p.returncode != 0 and "already holds data" in p.stderr,
          (p.stderr + p.stdout)[:240])

    # --- content, not mtime -------------------------------------------------
    # The incident that prompted the move, reconstructed: the live database had
    # the OLDER file mtime and the newer rows; the detached one looked fresh.
    # Ranking on mtime picks the wrong one.
    live = legacy_sqlite(tmp / "a2a.db", 40, time.time() - 600)
    stale = legacy_sqlite(tmp / "other.db", 5, time.time() - 6 * 86400)
    now = time.time()
    os.utime(live, (now - 86400, now - 86400))   # older file...
    os.utime(stale, (now, now))                  # ...newer file, older content
    renv = dict(dbharness.db_env(), A2A_DB_FILE=str(live))
    p = run(renv, "migrate")                     # no path, no tty -> refuses
    out = p.stdout
    check("with several candidates it lists them all",
          str(live) in out and str(stale) in out, out[:300])
    check("and offers the one with the NEWER CONTENT first, though its file "
          "is older — ranking on mtime would pick the detached copy",
          out.index(str(live)) < out.index(str(stale)), out[:300])
    check("it reports that they have diverged rather than sorting it away",
          "DIVERGED" in out, out[:300])
    check("and refuses to choose with no terminal and no path",
          p.returncode != 0 and "refusing to choose" in p.stderr,
          (p.stderr or out)[:240])

    # --- a database written before messages had a shelf life ---------------
    # expires_at defaults to 0 and the collector deletes `expires_at <= now`,
    # so importing one of these without dating the rows destroys every message
    # it holds at the first collection.
    old = legacy_sqlite(tmp / "preexpiry.db", 6, 1_000_000, with_expiry=False)
    oenv = dict(dbharness.db_env(), A2A_MAX_RETENTION_TIME="30d")
    p = run(oenv, "migrate", str(old), stdin="y\n")
    check("migrate imports a pre-expiry database", p.returncode == 0,
          (p.stdout + p.stderr)[-300:])
    rows = q(oenv, "SELECT ts, expires_at FROM transcripts")
    check("and dates every message to the retention ceiling instead of "
          "leaving it at 0, which the collector would read as long expired",
          bool(rows) and all(
              abs((r["expires_at"] - r["ts"]) - 30 * 86400) < 2 for r in rows),
          str(rows[:2]))

    # --- backup / restore round trip ---------------------------------------
    benv = dbharness.db_env()
    run(benv, "station", "create", "acme")
    run(benv, "agent", "add", "Reviewer", "--station", "acme")
    run(benv, "agent", "add", "reviewer", "--station", "acme")
    run(benv, "agent", "add", "ñandú-日本", "--station", "acme")
    tgz = tmp / "backup.tgz"
    p = run(benv, "backup", str(tgz))
    check("backup writes a .tgz", p.returncode == 0 and tgz.is_file(),
          (p.stdout + p.stderr)[-200:])
    p = run(benv, "backup", str(tgz))
    check("and refuses to overwrite one without --force",
          p.returncode != 0 and "use --force" in p.stderr, p.stderr[:160])

    renv = dbharness.db_env()
    p = run(renv, "restore", str(tgz), "--yes")
    check("restore loads it into an empty database", p.returncode == 0,
          (p.stdout + p.stderr)[-300:])
    check("and verifies counts and checksums rather than hoping",
          "counts and checksums match" in p.stdout, p.stdout[-200:])
    names = {r["agent_id"] for r in q(renv, "SELECT agent_id FROM agents")}
    check("every agent came back, case and unicode intact",
          {"Reviewer", "reviewer", "ñandú-日本"} <= names, str(names))
    p = run(renv, "restore", str(tgz), "--yes")
    check("restoring over existing data is refused, not merged",
          p.returncode != 0 and "already holds data" in p.stderr,
          p.stderr[:160])

    # --- doctor is the one command that says the deploy is actually right --
    # The container stack cannot be rehearsed here, so after `up` there has to
    # be something better than "no errors appeared". This is it.
    denv = dbharness.db_env()
    run(denv, "station", "create", "acme")
    p = run(denv, "doctor")
    check("doctor reports which database it is really connected to — the "
          "question that was unanswerable from outside for an afternoon",
          denv["A2A_DB_NAME"] in p.stdout and "storage" in p.stdout,
          p.stdout[-400:])
    check("and that every table is present and case-sensitive",
          "utf8mb4_bin on every table" in p.stdout, p.stdout[-400:])

    # Mis-collate a table the way a hand-rebuilt one would be. `logs` has no
    # foreign key, so nothing else stops it — which is exactly why it is worth
    # a check rather than trusting the schema to stay as written.
    conn = connect(denv)
    with conn.cursor() as cur:
        cur.execute("ALTER TABLE logs CONVERT TO CHARACTER SET utf8mb4 "
                    "COLLATE utf8mb4_general_ci")
    conn.close()
    p = run(denv, "doctor")
    check("doctor FLAGS a table that lost the binary collation — under a "
          "case-insensitive one `Foo` and `foo` collide and one inbox is "
          "split between two agents",
          p.returncode != 0 and "not case-sensitive: logs" in p.stdout,
          p.stdout[-400:])
    check("and says what to do about it rather than only naming it",
          "COLLATE utf8mb4_bin" in p.stdout, p.stdout[-300:])

    # --- a write-ahead log is handled, not refused --------------------------
    # The cutover stopped here: /legacy held a 3 MB database beside a 4.5 MB
    # WAL. A WAL holds committed transactions that are NOT in the database
    # file, so importing the file alone loses them — and with a writer still
    # attached, "loses them" can mean the tables are not there at all.
    #
    # /legacy is mounted read-only, so the fixture is read-only too: that is
    # what caught the first version of this, where the staged copy inherited
    # the source's permissions and could not be checkpointed.
    walroot = tmp / "wal"
    src = wal_sqlite(walroot / "legacy" / "a2a.db", 4000)
    alone = rows_in_db_alone(src, tmp)
    wal_size = src.with_name(src.name + "-wal").stat().st_size
    check("the fixture is honest: the .db alone is missing what the WAL holds",
          alone == 0 and wal_size > 0,
          f"{alone} rows in the .db alone, wal {wal_size} bytes")
    # Files first, then the directory: chmod'ing the directory first strips
    # its execute bit and nothing inside can be reached any more.
    for f in walroot.joinpath("legacy").iterdir():
        f.chmod(0o444)
    walroot.joinpath("legacy").chmod(0o555)

    before = {f.name: f.read_bytes()
              for f in walroot.joinpath("legacy").iterdir()}
    wenv = dbharness.db_env()
    p = run(wenv, "migrate", str(src), stdin="y\n")
    check("migrate imports a source with an uncheckpointed WAL, with no "
          "manual checkpoint step", p.returncode == 0,
          (p.stdout + p.stderr)[-400:])
    check("and says it staged a copy rather than doing it silently",
          "write-ahead log" in p.stdout and "staged" in p.stdout,
          p.stdout[-300:])
    got = q(wenv, "SELECT COUNT(*) n FROM transcripts")[0]["n"]
    check("every message in the WAL came across — strictly more than the .db "
          "alone holds, which is what proves the log was merged rather than "
          "the two happening to agree",
          got == 4000 and got > alone, f"{got} imported, {alone} in .db alone")
    body = q(wenv, "SELECT text FROM transcripts WHERE id = 'm0'")[0]["text"]
    check("with its text intact", "ñ 日本" in body, body)

    after = {f.name: f.read_bytes()
             for f in walroot.joinpath("legacy").iterdir()}
    check("and the source is byte-identical afterwards — database, WAL and "
          "shm — because it is the rollback",
          before == after,
          f"changed: {[k for k in before if before.get(k) != after.get(k)]}")

    # --- the case where staging is not optional ----------------------------
    # With the -shm gone, SQLite must RECOVER the WAL, which needs write
    # access a read-only mount does not give: it cannot open the file at all,
    # and says only "unable to open database file". Before staging, such a
    # source dropped out of the scan entirely — the operator would be told
    # there is no database while looking straight at one.
    hard = tmp / "noshm"
    src2 = wal_sqlite(hard / "legacy" / "a2a.db", 500)
    src2.with_name(src2.name + "-shm").unlink(missing_ok=True)
    for f in hard.joinpath("legacy").iterdir():
        f.chmod(0o444)
    hard.joinpath("legacy").chmod(0o555)

    unreadable = False
    try:
        sqlite3.connect(f"file:{src2}?mode=ro", uri=True).execute(
            "SELECT COUNT(*) FROM transcripts")
    except sqlite3.Error:
        unreadable = True
    check("the fixture is honest: sqlite cannot even open this one read-only",
          unreadable, "sqlite could read it, so the case is not reproduced")

    henv = dbharness.db_env()
    p = run(henv, "migrate", str(src2), stdin="y\n")
    check("migrate still imports it, by staging a copy it CAN checkpoint",
          p.returncode == 0, (p.stdout + p.stderr)[-400:])
    got = q(henv, "SELECT COUNT(*) n FROM transcripts")[0]["n"]
    check("recovering every row from a log nothing else could read",
          got == 500, str(got))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — the database's defaults cannot quietly change meaning")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
