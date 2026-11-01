#!/usr/bin/env python3
"""fakemaria behaves like the MariaDB the broker relies on.

    python3 tests/test_fakemaria.py

Every DB suite runs on tests/fakemaria (a test-only `pymysql` backed by
sqlite3) unless A2A_TEST_DB_REAL=1, so a suite passing proves something only
as far as the fake is faithful. This pins each MariaDB behaviour the broker
depends on, one check per behaviour, and that a statement the fake cannot
translate fails LOUDLY rather than answering differently.

Stdlib only. The broker's real SCHEMA is read from its source (ast), not
imported, so this needs none of the broker's packages.
"""
import ast
import atexit
import os
import shutil
import sys
import tempfile
import threading
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE / "fakemaria"))
DATA = tempfile.mkdtemp(prefix="a2a-fakemaria-")
os.environ["A2A_FAKEMARIA_DIR"] = DATA
atexit.register(shutil.rmtree, DATA, ignore_errors=True)

import pymysql                      # noqa: E402 — the fake, by the path above
import pymysql.cursors              # noqa: E402
from pymysql.constants import CLIENT  # noqa: E402

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def raises(fn, exc, code=None, text=None):
    try:
        fn()
    except exc as e:
        if code is not None and (not e.args or e.args[0] != code):
            return f"code {e.args[:1]}, wanted {code}: {e}"
        if text is not None and text not in str(e):
            return f"{text!r} not in {e}"
        return None
    except Exception as e:                      # the wrong exception
        return f"{type(e).__name__}: {e}"
    return "nothing raised"


def schema() -> tuple:
    src = (HERE.parent / "deploy" / "a2a_mcp" / "a2a-mcp.py").read_text()
    for node in ast.parse(src).body:
        if isinstance(node, ast.Assign) and any(
                getattr(t, "id", None) == "SCHEMA" for t in node.targets):
            return ast.literal_eval(node.value)
    raise SystemExit("SCHEMA not found in the broker")


def server(**kw):
    return pymysql.connect(host="127.0.0.1", port=3306, user="root",
                           password="", autocommit=True, **kw)


def fresh() -> str:
    name = f"fm_{os.getpid()}_{int(time.time() * 1e6) % 10**9}"
    with server() as s, s.cursor() as cur:
        cur.execute(f"CREATE DATABASE `{name}` CHARACTER SET utf8mb4 "
                    "COLLATE utf8mb4_bin")
    return name


def conn(db, **kw):
    kw.setdefault("cursorclass", pymysql.cursors.DictCursor)
    kw.setdefault("autocommit", True)
    return pymysql.connect(host="127.0.0.1", port=3306, user="root",
                           password="", database=db, **kw)


def q(c, sql, params=()):
    with c.cursor() as cur:
        cur.execute(sql, params)
        return cur.fetchall() if cur.description else cur.rowcount


def main() -> int:
    # --- the server: hosts, ports, databases --------------------------------
    check("a host nothing listens on is 2003, as pymysql says it",
          raises(lambda: pymysql.connect(host="db.invalid", port=3306,
                                         user="u", password=""),
                 pymysql.err.OperationalError, 2003) is None)
    check("so is a port nothing listens on (test_mariadb's A2A_DB_PORT=1)",
          raises(lambda: pymysql.connect(host="127.0.0.1", port=1, user="u",
                                         password=""),
                 pymysql.err.OperationalError, 2003) is None)
    check("an unknown database is 1049",
          raises(lambda: conn("no_such_db"),
                 pymysql.err.OperationalError, 1049) is None)
    db = fresh()
    check("CREATE DATABASE twice is 1007",
          raises(lambda: q(server(), f"CREATE DATABASE `{db}`"),
                 pymysql.err.ProgrammingError, 1007) is None)

    c = conn(db)
    # --- the broker's own schema -------------------------------------------
    SCHEMA = schema()
    with c.cursor() as cur:
        for stmt in SCHEMA:
            cur.execute(stmt)
        for stmt in SCHEMA:                 # every start runs it again
            cur.execute(stmt)
    check("the broker's whole SCHEMA runs, and runs again (idempotent)", True)
    rows = q(c, "SELECT table_name AS tname, table_collation AS tcoll FROM "
                "information_schema.tables WHERE table_schema = %s", (db,))
    names = {r["tname"] for r in rows}
    check("information_schema.tables lists every table and nothing of "
          "fakemaria's own",
          {"stations", "agents", "logs", "agent_client_state"} <= names
          and not any(n.startswith(("sqlite_", "__fakemaria")) for n in names),
          str(sorted(names)))
    check("…each utf8mb4_bin, as the DDL says",
          {r["tcoll"] for r in rows} == {"utf8mb4_bin"}, str(rows))
    has_col = q(c, "SELECT 1 FROM information_schema.columns WHERE "
                   "table_schema = %s AND table_name = %s AND column_name = %s",
                (db, "agents", "owner_client_hash"))
    check("the additive ALTER (ADD COLUMN IF NOT EXISTS) leaves the column "
          "there once", len(has_col) == 1, str(has_col))
    q(c, "ALTER TABLE logs CONVERT TO CHARACTER SET utf8mb4 COLLATE "
         "utf8mb4_general_ci")
    coll = {r["tname"]: r["tcoll"] for r in q(
        c, "SELECT table_name AS tname, table_collation AS tcoll FROM "
           "information_schema.tables WHERE table_schema = %s", (db,))}
    check("CONVERT TO … COLLATE is recorded, so the doctor can flag it",
          coll["logs"] == "utf8mb4_general_ci"
          and coll["agents"] == "utf8mb4_bin", str(coll))
    size = q(c, "SELECT table_name AS tname, (data_length + index_length) AS "
                "nbytes FROM information_schema.tables WHERE table_schema = %s",
             (db,))
    check("data_length + index_length answers (vacuum's before/after)",
          all(isinstance(r["nbytes"], int) for r in size), str(size[:2]))

    now = time.time()
    q(c, "INSERT IGNORE INTO stations (station_id, name, description, "
         "created_at) VALUES (%s, %s, %s, %s)", ("s1", "acme", "", now))

    # --- utf8mb4_bin --------------------------------------------------------
    add = ("INSERT INTO agents (station_id, agent_id, name, created_at) "
           "VALUES (%s, %s, %s, %s)")
    q(c, add, ("s1", "Foo", "Foo", now))
    check("`Foo` and `foo` are two agents under the primary key",
          raises(lambda: q(c, add, ("s1", "foo", "foo", now)), Exception)
          == "nothing raised")
    check("…and `=` tells them apart",
          len(q(c, "SELECT * FROM agents WHERE agent_id = 'foo'")) == 1)
    check("LIKE is case-sensitive too (utf8mb4_bin)",
          len(q(c, "SELECT * FROM agents WHERE agent_id LIKE %s", ("fo%",)))
          == 1)
    check("LIKE's escape is the backslash, as MariaDB's",
          q(c, "SELECT 'a_b' LIKE %s AS y, 'axb' LIKE %s AS n",
            ("a\\_b", "a\\_b")) == [{"y": 1, "n": 0}])
    check("a duplicate key is IntegrityError 1062",
          raises(lambda: q(c, add, ("s1", "Foo", "x", now)),
                 pymysql.err.IntegrityError, 1062) is None)
    weird = "agente-ñandú-日本-🙂"
    q(c, add, ("s1", weird, weird, now))
    got = q(c, "SELECT agent_id FROM agents WHERE agent_id = %s", (weird,))
    check("non-ASCII and astral characters round-trip",
          got == [{"agent_id": weird}], str(got))

    # --- strict mode ---------------------------------------------------------
    check("a value longer than VARCHAR(n) is refused (DataError 1406), not "
          "truncated",
          raises(lambda: q(c, add, ("s1", "x" * 129, "x", now)),
                 pymysql.err.DataError, 1406) is None)
    check("a value of the wrong type is refused (DataError 1366)",
          raises(lambda: q(c, add, ("s1", "typed", "t", "not-a-number")),
                 pymysql.err.DataError, 1366) is None)
    check("a missing NOT NULL value is IntegrityError 1048",
          raises(lambda: q(c, "INSERT INTO agents (station_id, agent_id, "
                              "name) VALUES ('s1', 'n', NULL)"),
                 pymysql.err.IntegrityError, 1048) is None)
    check("TEXT is capped at 65535 BYTES, as MariaDB's",
          raises(lambda: q(c, "UPDATE agents SET description = %s WHERE "
                              "agent_id = 'Foo'", ("ñ" * 40000,)),
                 pymysql.err.DataError, 1406) is None)

    # --- rowcount is CHANGED rows ---------------------------------------------
    q(c, "INSERT INTO agents (station_id, agent_id, name, created_at) VALUES "
         "('s1', 'bob', 'bob', %s), ('s1', 'amy', 'amy', %s)", (now, now))
    n1 = q(c, "UPDATE agents SET description = 'x' WHERE station_id = 's1'")
    n2 = q(c, "UPDATE agents SET description = 'x' WHERE station_id = 's1'")
    check("UPDATE counts the rows it changed…", n1 == 5, str(n1))
    check("…and 0 when it changes nothing (CLIENT.FOUND_ROWS off)",
          n2 == 0, str(n2))
    fr = conn(db, client_flag=CLIENT.FOUND_ROWS)
    n3 = q(fr, "UPDATE agents SET description = 'x' WHERE station_id = 's1'")
    fr.close()
    check("with FOUND_ROWS it counts matched rows, as MariaDB would",
          n3 == 5, str(n3))
    check("an UPDATE whose expression reads a column assigned before it is "
          "refused: MariaDB evaluates SET left to right, sqlite does not",
          raises(lambda: q(c, "UPDATE agents SET name = 'z', description = "
                              "name WHERE agent_id = 'bob'"),
                 pymysql.err.ProgrammingError, None,
                 "fakemaria cannot run") is None)

    # The broker's _bind_hash statement, verbatim: IF(), <=> and the order.
    bind = ("UPDATE agents SET owner_client_hash = "
            "IF(owner_token_hash <=> %s, owner_client_hash, NULL), "
            "owner_token_hash = %s WHERE agent_id = %s")
    q(c, "UPDATE agents SET owner_token_hash = 'T1', owner_client_hash = 'C1' "
         "WHERE agent_id = 'bob'")
    same = q(c, bind, ("T1", "T1", "bob"))
    kept = q(c, "SELECT owner_client_hash AS h FROM agents WHERE "
                "agent_id = 'bob'")
    other = q(c, bind, ("T2", "T2", "bob"))
    gone = q(c, "SELECT owner_client_hash AS h, owner_token_hash AS t FROM "
                "agents WHERE agent_id = 'bob'")
    nul = q(c, bind, (None, None, "bob"))
    check("_bind_hash: the same token changes nothing and keeps the client",
          same == 0 and kept == [{"h": "C1"}], f"{same} {kept}")
    check("_bind_hash: another token releases the client pin",
          other == 1 and gone == [{"h": None, "t": "T2"}], f"{other} {gone}")
    check("_bind_hash: <=> treats NULL as a value (unbind)",
          nul == 1, str(nul))

    # --- upserts --------------------------------------------------------------
    up = ("INSERT INTO agent_client_state (station_id, agent_id, harness, "
          "client_version, state, reported_at) VALUES (%s, %s, %s, %s, %s, %s) "
          "ON DUPLICATE KEY UPDATE harness = VALUES(harness), "
          "client_version = VALUES(client_version), state = VALUES(state), "
          "reported_at = VALUES(reported_at)")
    a = q(c, up, ("s1", "bob", "pi", "0.6.0", "{}", 1.0))
    b = q(c, up, ("s1", "bob", "hermes", "0.6.0", "{}", 2.0))
    z = q(c, up, ("s1", "bob", "hermes", "0.6.0", "{}", 2.0))
    row = q(c, "SELECT harness, reported_at FROM agent_client_state")
    check("ON DUPLICATE KEY UPDATE inserts, then updates in place (one row)",
          row == [{"harness": "hermes", "reported_at": 2.0}], str(row))
    check("…and an upsert that changes nothing counts 0, as MariaDB's",
          (a, b, z) == (1, 1, 0), str((a, b, z)))
    ig = q(c, "INSERT IGNORE INTO stations (station_id, name, description, "
              "created_at) VALUES ('s1', 'acme', 'other', 0)")
    check("INSERT IGNORE skips a duplicate and counts 0", ig == 0, str(ig))
    bids = ("INSERT INTO bids (station_id, broadcast_id, agent_id, bid, pitch, "
            "created_at) VALUES (%s, %s, %s, %s, %s, %s) ON DUPLICATE KEY "
            "UPDATE bid = VALUES(bid), pitch = VALUES(pitch), "
            "created_at = VALUES(created_at)")
    q(c, "INSERT INTO broadcasts (id, station_id, sender, problem, created_at, "
         "updated_at) VALUES ('b1', 's1', 'amy', 'help', 1, 1)")
    q(c, bids, ("s1", "b1", "bob", "claim", "", 1.0))
    q(c, bids, ("s1", "b1", "bob", "pass", "", 2.0))
    got = q(c, "SELECT id, bid FROM bids")
    check("an upsert on a table whose conflict is a UNIQUE KEY, not the "
          "(AUTO_INCREMENT) primary key", got == [{"id": 1, "bid": "pass"}],
          str(got))

    # --- foreign keys -----------------------------------------------------------
    check("a child row with no parent is IntegrityError 1452",
          raises(lambda: q(c, add, ("nope", "x", "x", now)),
                 pymysql.err.IntegrityError, 1452) is None)
    q(c, "INSERT INTO stations (station_id, name, created_at) VALUES "
         "('s2', 'ops', 0)")
    q(c, add, ("s2", "carol", "carol", now))
    q(c, "INSERT INTO channels (station_id, name, created_at) VALUES "
         "('s2', 'room', 0)")
    q(c, "INSERT INTO transcripts (id, station_id, channel, ts, sender, text) "
         "VALUES ('t1', 's2', 'room', 1, 'carol', 'hi')")
    q(c, "INSERT INTO message_addressees (station_id, msg_id, agent_id) "
         "VALUES ('s2', 't1', 'carol')")
    q(c, "DELETE FROM transcripts WHERE id = 't1'")
    check("ON DELETE CASCADE reaches message_addressees (retirement needs no "
          "code of its own)",
          q(c, "SELECT * FROM message_addressees") == [])
    q(c, "DELETE FROM stations WHERE station_id = 's2'")
    check("deleting a station cascades to its agents and channels",
          q(c, "SELECT * FROM agents WHERE station_id = 's2'") == []
          and q(c, "SELECT * FROM channels WHERE station_id = 's2'") == [])

    # The channel move: foreign_key_checks = 0 around re-keying a parent.
    q(c, "INSERT INTO stations (station_id, name, created_at) VALUES "
         "('s3', 'dst', 0)")
    q(c, "INSERT INTO channels (station_id, name, created_at) VALUES "
         "('s1', 'mv', 0)")
    q(c, "INSERT INTO transcripts (id, station_id, channel, ts, sender, text) "
         "VALUES ('t2', 's1', 'mv', 1, 'amy', 'hi')")
    c.begin()
    q(c, "SET SESSION foreign_key_checks = 0")
    q(c, "UPDATE channels SET station_id = 's3' WHERE station_id = 's1' AND "
         "name = 'mv'")
    q(c, "UPDATE transcripts SET station_id = 's3' WHERE station_id = 's1' "
         "AND channel = 'mv'")
    q(c, "SET SESSION foreign_key_checks = 1")
    c.commit()
    check("foreign_key_checks = 0 in a transaction lets a parent be re-keyed "
          "under its children",
          q(c, "SELECT station_id AS s FROM transcripts WHERE id = 't2'")
          == [{"s": "s3"}])
    c.begin()
    q(c, "SET SESSION foreign_key_checks = 0")
    q(c, "INSERT INTO transcripts (id, station_id, channel, ts, sender, text) "
         "VALUES ('t3', 's1', 'ghost', 1, 'amy', 'hi')")
    q(c, "SET SESSION foreign_key_checks = 1")
    bad = raises(c.commit, pymysql.err.IntegrityError, 1452)
    check("…but what is still dangling at COMMIT is refused and rolled back "
          "(stricter than MariaDB, never laxer)",
          bad is None and q(c, "SELECT * FROM transcripts WHERE id = 't3'")
          == [], str(bad))

    # --- transactions ---------------------------------------------------------
    c.begin()
    q(c, add, ("s1", "ghost", "ghost", now))
    c.rollback()
    check("rollback undoes the transaction",
          q(c, "SELECT * FROM agents WHERE agent_id = 'ghost'") == [])
    c.begin()
    rows = q(c, "SELECT sender FROM transcripts WHERE id = %s FOR UPDATE",
             ("t2",))
    c.commit()
    check("SELECT … FOR UPDATE runs (the transaction holds the lock)",
          rows == [{"sender": "amy"}], str(rows))

    other = conn(db)
    done = threading.Event()

    def writer():
        q(other, add, ("s1", "waited", "w", now))
        done.set()
    c.begin()
    q(c, add, ("s1", "first", "f", now))
    t = threading.Thread(target=writer)
    t.start()
    time.sleep(0.6)
    blocked = not done.is_set()
    c.commit()
    t.join(10)
    check("a second writer WAITS for an open transaction (begin() locks, as "
          "InnoDB's row locks would for the same rows)…",
          blocked, "the writer did not wait")
    check("…and goes through once it commits",
          done.is_set() and len(q(c, "SELECT * FROM agents WHERE agent_id IN "
                                     "('first', 'waited')")) == 2)

    # md_store's shape: read FOR UPDATE, decide, then write. Two first shares
    # of one name must not both read "nobody has it" — so the lock must be
    # held from begin(), before anything has been written.
    done.clear()
    c.begin()
    q(c, "SELECT sender FROM md_files WHERE station_id = 's1' AND uri = 'x' "
         "FOR UPDATE")

    def writer2():
        q(other, add, ("s1", "waited2", "w", now))
        done.set()
    t = threading.Thread(target=writer2)
    t.start()
    time.sleep(0.6)
    blocked = not done.is_set()
    c.commit()
    t.join(10)
    check("a transaction that has only READ (… FOR UPDATE) already holds the "
          "lock: a writer waits for it", blocked and done.is_set(),
          "the writer did not wait")
    other.close()

    nc = conn(db, autocommit=False)
    q(nc, add, ("s1", "uncommitted", "u", now))
    seen = q(c, "SELECT * FROM agents WHERE agent_id = 'uncommitted'")
    nc.close()
    after = q(c, "SELECT * FROM agents WHERE agent_id = 'uncommitted'")
    check("autocommit=False (pymysql's default): nothing is visible before "
          "commit, and close() without it rolls back",
          seen == [] and after == [], f"{seen} {after}")

    # --- order and result shapes -----------------------------------------------
    ids = [r["agent_id"] for r in q(c, "SELECT agent_id FROM agents WHERE "
                                       "station_id = 's1'")]
    check("an unordered scan comes back in primary-key order, as InnoDB's "
          "clustered index returns it", ids == sorted(ids), str(ids))
    tup = conn(db, cursorclass=pymysql.cursors.Cursor)
    r = q(tup, "SELECT agent_id, COUNT(*) n FROM agents GROUP BY agent_id "
               "ORDER BY agent_id LIMIT 1")
    tup.close()
    check("the default cursor returns tuples", r == [("Foo", 1)], str(r))
    r = q(c, "SELECT a.name, s.name FROM agents a JOIN stations s ON "
             "s.station_id = a.station_id WHERE a.agent_id = 'Foo'")
    check("DictCursor: a duplicate column name keeps the FIRST under the bare "
          "name, as pymysql's does", r[0]["name"] == "Foo", str(r))
    with c.cursor() as cur:
        cur.executemany("INSERT INTO logs (ts, message) VALUES (%s, %s)",
                        [(1.0, "a"), (2.0, "b"), (3.0, "c")])
        many = cur.rowcount
    check("executemany inserts every row and counts them", many == 3, str(many))
    with c.cursor() as cur:
        cur.execute("INSERT INTO logs (ts, message) VALUES (4, 'd')")
        rid = cur.lastrowid
    check("AUTO_INCREMENT numbers rows, and lastrowid says which", rid == 4,
          str(rid))

    # --- functions and operators ------------------------------------------------
    r = q(c, "SELECT VERSION() AS v, DATABASE() AS d, CURRENT_USER() AS u")[0]
    check("VERSION() says fakemaria, DATABASE() names this one",
          "fakemaria" in r["v"] and r["d"] == db and r["u"].startswith("root"),
          str(r))
    r = q(c, "SELECT LENGTH('ñ') AS b, CHAR_LENGTH('ñ') AS ch")[0]
    check("LENGTH is bytes and CHAR_LENGTH characters, as MariaDB's",
          r == {"b": 2, "ch": 1}, str(r))
    r = q(c, "SELECT 5 / 2 AS d")[0]["d"]
    check("`/` is exact division (sqlite would truncate 5/2 to 2)", r == 2.5,
          str(r))
    r = q(c, "SELECT SUM(acked_at IS NULL) AS n FROM message_receipts")
    check("SUM(cond IS NULL) — the collector's HAVING — runs",
          r == [{"n": None}], str(r))
    r = q(c, "SELECT '100%%' AS p", ())
    r2 = q(c, "SELECT '100%%' AS p", None)
    check("`%%` is `%` exactly when pymysql formats (args given, even ())",
          r == [{"p": "100%"}] and r2 == [{"p": "100%%"}], f"{r} {r2}")
    check("too few arguments is pymysql's TypeError",
          raises(lambda: q(c, "SELECT %s, %s", (1,)), TypeError) is None)
    r = q(c, "SELECT agent_id FROM agents WHERE agent_id IN %s ORDER BY "
             "agent_id", (("Foo", "bob"),))
    check("a sequence argument expands to a list, as pymysql escapes it",
          [x["agent_id"] for x in r] == ["Foo", "bob"], str(r))
    r = q(c, "OPTIMIZE TABLE `logs`")
    check("OPTIMIZE TABLE answers a status row",
          r and r[0]["Msg_type"] == "status", str(r))

    # --- loud, never different ---------------------------------------------------
    for label, sql in (
            ("`||` (OR in MariaDB)", "SELECT 1 || 0"),
            ("JSON_* functions", "SELECT JSON_EXTRACT('{}', '$.a')"),
            ("SHOW", "SHOW TABLES"),
            ("multi-table UPDATE", "UPDATE agents a JOIN stations s ON "
             "s.station_id = a.station_id SET a.name = 'x'"),
            ("DELETE … LIMIT", "DELETE FROM logs LIMIT 1"),
            ("a function sqlite lacks", "SELECT RAND()"),
            ("SET of another variable", "SET SESSION sql_mode = ''")):
        check(f"untranslatable is loud: {label}",
              raises(lambda: q(c, sql), pymysql.err.ProgrammingError, None,
                     "fakemaria cannot run") is None, sql)
    check("an unknown table is ProgrammingError 1146",
          raises(lambda: q(c, "SELECT * FROM nope"),
                 pymysql.err.ProgrammingError, 1146) is None)

    c.ping(reconnect=False)
    c.close()
    check("a closed connection refuses ping and a second close",
          raises(lambda: c.ping(reconnect=False), pymysql.err.Error) is None
          and raises(c.close, pymysql.err.Error) is None)
    with server() as s, s.cursor() as cur:
        cur.execute(f"DROP DATABASE IF EXISTS `{db}`")
    check("DROP DATABASE removes it",
          raises(lambda: conn(db), pymysql.err.OperationalError, 1049) is None)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — fakemaria behaves as the MariaDB the broker relies on")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
