"""fakemaria — a TEST-ONLY stand-in for PyMySQL, backed by stdlib sqlite3.

    tests/dbharness.py puts tests/fakemaria first on sys.path and PYTHONPATH,
    so the broker's own `import pymysql` — in the test process and in every
    `serve` / CLI subprocess a suite starts — lands here. The broker is not
    changed and does not know. A2A_TEST_DB_REAL=1 turns it off.

The broker reaches its database through one seam (`pymysql.connect` in
`_new_connection`) and uses only `cursors.DictCursor`, `err.IntegrityError`,
`cursor.execute/executemany/fetch*/description/rowcount` and
`begin/commit/rollback/ping/close`. That is the surface here.

Storage: one sqlite file per database, `$A2A_FAKEMARIA_DIR/<name>.sqlite3`,
in WAL mode with a busy timeout, so the test process, `serve` and the CLI
share it as clients share a server.

WHAT IT REPRODUCES — each pinned by tests/test_fakemaria.py:
  - the dialect the broker writes (see _translate.py), and a loud
    ProgrammingError("fakemaria cannot run: …") for anything else;
  - utf8mb4_bin: comparisons, unique keys and LIKE are case-sensitive;
  - strict mode: a value too long or of the wrong type is refused
    (DataError 1406 / 1366), not truncated;
  - rowcount = CHANGED rows (CLIENT.FOUND_ROWS off), matched with it on;
  - an upsert that changes nothing counts 0;
  - `%s` / `%%` formatting exactly when pymysql formats;
  - foreign keys with ON DELETE CASCADE, and foreign_key_checks = 0 inside
    a transaction (checked at COMMIT instead: stricter, never laxer);
  - a transaction is BEGIN IMMEDIATE: one writer at a time, the others wait
    up to 30 s, then 1205 "Lock wait timeout exceeded";
  - an unscanned table comes back in primary-key order, as InnoDB's
    clustered index returns it;
  - information_schema.tables / .columns for this database, with each
    table's collation (ALTER … CONVERT TO … COLLATE x records x);
  - CREATE / DROP DATABASE, an unknown database (1049), a host or port
    nothing listens on (2003).

WHAT IT DOES NOT — the price of no real server:
  - InnoDB's row locks (here the whole database has one writer at a time);
  - a case-INSENSITIVE table: a recorded _ci collation is reported, but
    comparisons stay binary;
  - PAD SPACE ('a' = 'a ' is false here);
  - an upsert that UPDATES counts 1 here, 2 in MariaDB — the broker reads
    no upsert's rowcount.
"""
from __future__ import annotations

import datetime
import decimal
import os
import re
import sqlite3
import threading
import time
from pathlib import Path

from . import cursors, err
from ._translate import Untranslatable, translate
from .constants import CLIENT
from .err import (DatabaseError, DataError, Error, IntegrityError,  # noqa
                  InterfaceError, InternalError, MySQLError,
                  NotSupportedError, OperationalError, ProgrammingError,
                  Warning)

__version__ = "1.1.1-fakemaria"
version_info = (1, 1, 1, "final", 0)
apilevel = "2.0"
threadsafety = 1
paramstyle = "pyformat"
FAKEMARIA = True
SERVER_VERSION = f"11.4.0-MariaDB-fakemaria (sqlite {sqlite3.sqlite_version})"

LOCAL_HOSTS = {"127.0.0.1", "localhost", "::1", "fakemaria"}
BUSY_S = 30.0
META = ("CREATE TABLE IF NOT EXISTS main.__fakemaria_tables "
        "(name TEXT PRIMARY KEY, collation TEXT NOT NULL)")
_SEQ = (list, tuple, set, frozenset)


def _data_dir() -> Path:
    d = os.environ.get("A2A_FAKEMARIA_DIR")
    if not d:
        raise OperationalError(
            2003, "Can't connect to MySQL server: fakemaria has no "
                  "A2A_FAKEMARIA_DIR (tests/dbharness.py and tests/run.py "
                  "set it)")
    p = Path(d)
    p.mkdir(parents=True, exist_ok=True)
    return p


def _db_path(name: str) -> Path:
    if not re.fullmatch(r"[A-Za-z0-9_$]{1,64}", name or ""):
        raise ProgrammingError(1102, f"Incorrect database name '{name}'")
    return _data_dir() / f"{name}.sqlite3"


def _short(s: str) -> str:
    s = " ".join(str(s).split())
    return s if len(s) <= 240 else s[:237] + "..."


def _conv(v):
    if isinstance(v, bool):
        return int(v)
    if v is None or isinstance(v, (str, int, float, bytes)):
        return v
    if isinstance(v, decimal.Decimal):
        return float(v)
    if isinstance(v, datetime.datetime):
        return v.isoformat(sep=" ")
    if isinstance(v, (datetime.date, datetime.time)):
        return v.isoformat()
    if isinstance(v, (bytearray, memoryview)):
        return bytes(v)
    return str(v)


_CACHE: dict = {}
_CACHE_LOCK = threading.Lock()


def _plan(query: str, args, found_rows: bool):
    """translate(), cached by statement text and argument COUNT — the plan
    does not depend on the values unless one of them is a sequence (pymysql
    expands those into a list), and then it is not cached."""
    try:
        if args is None:
            key = (query, None, found_rows)
        elif isinstance(args, dict) or any(isinstance(v, _SEQ) for v in args):
            return translate(query, args, found_rows)
        else:
            key = (query, len(args), found_rows)
        hit = _CACHE.get(key)
        if hit is None:
            hit = translate(query, args, found_rows)[0]
            with _CACHE_LOCK:
                if len(_CACHE) > 4096:
                    _CACHE.clear()
                _CACHE[key] = hit
        return hit, ([] if args is None else list(args))
    except Untranslatable as e:
        raise ProgrammingError(
            1064, f"fakemaria cannot run: {e} — in: {_short(query)}") from None


def _concat(*a):
    return None if any(x is None for x in a) else "".join(
        str(int(x)) if isinstance(x, float) and x.is_integer() else str(x)
        for x in a)


def _greatest(*a):
    return None if any(x is None for x in a) else max(a)


def _least(*a):
    return None if any(x is None for x in a) else min(a)


class Connection:
    def __init__(self, *, host=None, user=None, password="", database=None,
                 port=0, unix_socket=None, charset="", collation=None,
                 client_flag=0, cursorclass=cursors.Cursor, init_command=None,
                 connect_timeout=10, autocommit=False, db=None, passwd=None,
                 **_ignored):
        if db is not None and database is None:
            database = db
        host = host or "localhost"
        want = int(os.environ.get("A2A_FAKEMARIA_PORT") or 3306)
        try:
            port = int(port or 3306)
        except (TypeError, ValueError):
            port = -1
        if unix_socket or host not in LOCAL_HOSTS or port != want:
            raise OperationalError(
                2003, f"Can't connect to MySQL server on {host!r} "
                      "([Errno 61] Connection refused)")
        self.host, self.port = host, port
        self.user = user or ""
        self.db = database
        self.charset = charset or "utf8mb4"
        self.cursorclass = cursorclass
        self.found_rows = bool(int(client_flag or 0) & CLIENT.FOUND_ROWS)
        self.autocommit_mode = bool(autocommit)
        self._c = None
        self.open = False
        self._connect(database)
        if init_command:
            with self.cursor() as cur:
                cur.execute(init_command)

    # -- the sqlite side -----------------------------------------------------
    def _connect(self, database):
        if database:
            path = _db_path(database)
            if not path.exists():
                raise OperationalError(1049, f"Unknown database '{database}'")
            target = str(path)
        else:
            target = ":memory:"
        c = sqlite3.connect(target, timeout=BUSY_S, isolation_level=None,
                            check_same_thread=False)
        c.execute("PRAGMA foreign_keys = ON")
        c.execute("PRAGMA case_sensitive_like = ON")     # utf8mb4_bin
        c.execute("PRAGMA synchronous = OFF")
        c.execute("ATTACH DATABASE ':memory:' AS information_schema")
        c.execute("CREATE TABLE information_schema.tables (table_catalog, "
                  "table_schema, table_name, table_type, engine, table_rows, "
                  "data_length, index_length, table_collation)")
        c.execute("CREATE TABLE information_schema.columns (table_catalog, "
                  "table_schema, table_name, column_name, ordinal_position, "
                  "column_default, is_nullable, data_type, column_key)")
        c.create_function("fm_version", 0, lambda: SERVER_VERSION)
        c.create_function("fm_database", 0, lambda: self.db)
        c.create_function("fm_schema", 0, lambda: self.db)
        c.create_function("fm_current_user", 0, lambda: f"{self.user}@%")
        c.create_function("fm_now", 0, lambda: time.strftime(
            "%Y-%m-%d %H:%M:%S", time.gmtime()))
        c.create_function("fm_unix_timestamp", 0, lambda: int(time.time()))
        c.create_function("fm_concat", -1, _concat)
        c.create_function("fm_greatest", -1, _greatest)
        c.create_function("fm_least", -1, _least)
        self._c = c
        self.open = True

    def _alive(self):
        if not self.open or self._c is None:
            raise InterfaceError(0, "")

    def _map(self, e: Exception, text: str, query: str) -> Exception:
        msg = str(e)
        verb = (query.split(None, 1) or [""])[0].upper()
        if isinstance(e, sqlite3.IntegrityError):
            if msg.startswith("UNIQUE constraint failed"):
                return IntegrityError(1062, "Duplicate entry for key "
                                            f"({msg.split(': ', 1)[-1]})")
            if msg.startswith("NOT NULL constraint failed"):
                return IntegrityError(
                    1048, f"Column '{msg.rsplit('.', 1)[-1]}' cannot be null")
            if "FOREIGN KEY constraint failed" in msg:
                if verb == "DELETE":
                    return IntegrityError(1451, "Cannot delete or update a "
                                          "parent row: a foreign key "
                                          "constraint fails")
                return IntegrityError(1452, "Cannot add or update a child "
                                      "row: a foreign key constraint fails")
            m = re.search(r"CHECK constraint failed: fm_len:(.+):\d+", msg)
            if m:
                return DataError(1406, f"Data too long for column "
                                       f"'{m.group(1)}' at row 1")
            m = re.search(r"CHECK constraint failed: fm_range:(\S+)", msg)
            if m:
                return DataError(1264, f"Out of range value for column "
                                       f"'{m.group(1)}' at row 1")
            m = re.search(r"cannot store (\w+) value in (\w+) column "
                          r"\S*?\.?(\w+)$", msg)
            if m:
                return DataError(1366, f"Incorrect {m.group(2).lower()} value "
                                       f"for column '{m.group(3)}' at row 1")
            return IntegrityError(1105, msg)
        if isinstance(e, sqlite3.OperationalError):
            if "locked" in msg or "busy" in msg:
                return OperationalError(
                    1205, "Lock wait timeout exceeded; try restarting "
                          "transaction")
            m = re.match(r"no such table: (?:\w+\.)?(\S+)", msg)
            if m:
                if not self.db:
                    return ProgrammingError(1046, "No database selected")
                return ProgrammingError(
                    1146, f"Table '{self.db}.{m.group(1)}' doesn't exist")
            m = re.match(r"(?:no such column: |table \S+ has no column named )"
                         r"(\S+)", msg)
            if m:
                return OperationalError(
                    1054, f"Unknown column '{m.group(1)}' in 'field list'")
        return ProgrammingError(
            1064, f"fakemaria cannot run: {msg} — in: {_short(text)}")

    # -- running statements --------------------------------------------------
    def _run(self, cur, query, args):
        self._alive()
        if isinstance(query, bytes):
            query = query.decode("utf-8")
        plan, flat = _plan(query, args, self.found_rows)
        names, rows, count, lastrowid = None, [], 0, None
        for kind, payload in plan.steps:
            if kind == "sql":
                text, order = payload
                if not self.autocommit_mode and not self._c.in_transaction:
                    self._c.execute("BEGIN")     # pymysql: implicit tx
                try:
                    sc = self._c.execute(text, [_conv(flat[i]) for i in order])
                    if sc.description is not None:
                        names = [d[0] for d in sc.description]
                        rows = sc.fetchall()
                        count = len(rows)
                    else:
                        names, rows = None, []
                        count = max(sc.rowcount, 0)
                    lastrowid = sc.lastrowid
                except sqlite3.Error as e:
                    raise self._map(e, text, query) from None
            else:
                got = getattr(self, "_op_" + kind)(*payload)
                if got is not None:
                    names, rows, count = got
        cur._set(names, rows, count, lastrowid)

    def _run_many(self, cur, query, rows):
        self._alive()
        if isinstance(query, bytes):
            query = query.decode("utf-8")
        loop = isinstance(rows[0], dict) or any(
            isinstance(v, _SEQ) for r in rows for v in r)
        plan = None if loop else _plan(query, rows[0], self.found_rows)[0]
        if loop or len(plan.steps) != 1 or plan.steps[0][0] != "sql" \
                or re.match(r"\s*(SELECT|WITH)\b", query, re.I):
            total = 0
            for r in rows:
                self._run(cur, query, r)
                total += max(cur.rowcount, 0)
            cur._set(None, [], total, cur.lastrowid)
            return
        text, order = plan.steps[0][1]
        n = len(rows[0])
        for r in rows:
            if len(r) != n:
                raise TypeError("not all arguments converted during string "
                                "formatting" if len(r) > n else
                                "not enough arguments for format string")
        if not self.autocommit_mode and not self._c.in_transaction:
            self._c.execute("BEGIN")
        try:
            sc = self._c.executemany(
                text, [[_conv(r[i]) for i in order] for r in rows])
        except sqlite3.Error as e:
            raise self._map(e, text, query) from None
        cur._set(None, [], max(sc.rowcount, 0), sc.lastrowid)

    # -- the statements that are not sqlite statements -----------------------
    def _need_db(self):
        if not self.db:
            raise ProgrammingError(1046, "No database selected")

    def _has_table(self, name: str) -> bool:
        return self._c.execute(
            "SELECT 1 FROM main.sqlite_master WHERE type = 'table' AND "
            "name = ?", (name,)).fetchone() is not None

    def _op_noop(self):
        return None

    def _op_create_db(self, name, if_not_exists):
        path = _db_path(name)
        if path.exists():
            if if_not_exists:
                return None, [], 0
            raise ProgrammingError(
                1007, f"Can't create database '{name}'; database exists")
        c = sqlite3.connect(str(path), timeout=BUSY_S, isolation_level=None)
        try:
            c.execute("PRAGMA journal_mode = WAL")
            c.execute(META)
        finally:
            c.close()
        return None, [], 1

    def _op_drop_db(self, name, if_exists):
        path = _db_path(name)
        if not path.exists():
            if if_exists:
                return None, [], 0
            raise OperationalError(
                1008, f"Can't drop database '{name}'; database doesn't exist")
        for suffix in ("", "-wal", "-shm"):
            try:
                os.unlink(str(path) + suffix)
            except FileNotFoundError:
                pass
        return None, [], 0

    def _op_meta(self):
        self._need_db()
        self._c.execute(META)

    def _op_collation(self, table, collation, keep):
        self._need_db()
        if not self._has_table(table):
            raise ProgrammingError(
                1146, f"Table '{self.db}.{table}' doesn't exist")
        self._c.execute(META)
        self._c.execute(
            f"INSERT OR {'IGNORE' if keep else 'REPLACE'} INTO "
            "main.__fakemaria_tables (name, collation) VALUES (?, ?)",
            (table, collation))

    def _op_forget_table(self, table):
        if self.db and self._has_table("__fakemaria_tables"):
            self._c.execute("DELETE FROM main.__fakemaria_tables WHERE "
                            "name = ?", (table,))

    def _op_reset_sequence(self, table):
        if self._has_table("sqlite_sequence"):
            self._c.execute("DELETE FROM main.sqlite_sequence WHERE name = ?",
                            (table,))

    def _op_add_column(self, table, column, if_not_exists, stmt):
        self._need_db()
        if not self._has_table(table):
            raise ProgrammingError(
                1146, f"Table '{self.db}.{table}' doesn't exist")
        have = {r[1] for r in self._c.execute(
            f'PRAGMA main.table_info("{table}")')}
        if column in have:
            if if_not_exists:
                return None, [], 0
            raise OperationalError(1060, f"Duplicate column name '{column}'")
        text, _order = stmt
        try:
            self._c.execute(text)
        except sqlite3.Error as e:
            raise self._map(e, text, text) from None
        return None, [], 0

    def _op_fk_checks(self, on):
        if self._c.in_transaction:
            # A session variable in MariaDB. Inside a transaction (where the
            # broker sets it) the check moves to COMMIT; turning it back on
            # changes nothing, as MariaDB does not re-check either.
            if not on:
                self._c.execute("PRAGMA defer_foreign_keys = ON")
        else:
            self._c.execute(f"PRAGMA foreign_keys = {'ON' if on else 'OFF'}")

    def _op_optimize(self, names):
        out = []
        for t in names:
            if not self._has_table(t):
                out.append((f"{self.db}.{t}", "optimize", "Error",
                            f"Table '{self.db}.{t}' doesn't exist"))
                continue
            out.append((f"{self.db}.{t}", "optimize", "status", "OK"))
        return ["Table", "Op", "Msg_type", "Msg_text"], out, len(out)

    def _op_info_refresh(self):
        c = self._c
        c.execute("DELETE FROM information_schema.tables")
        c.execute("DELETE FROM information_schema.columns")
        if not self.db:
            return
        tables = [r[0] for r in c.execute(
            "SELECT name FROM main.sqlite_master WHERE type = 'table' AND "
            "name NOT LIKE 'sqlite!_%' ESCAPE '!' AND "
            "name NOT LIKE '!_!_fakemaria%' ESCAPE '!' ORDER BY name")]
        meta = {}
        if self._has_table("__fakemaria_tables"):
            meta = dict(c.execute(
                "SELECT name, collation FROM main.__fakemaria_tables"))
        sizes: dict = {}
        try:
            sizes = dict(c.execute(
                "SELECT name, SUM(pgsize) FROM dbstat('main') GROUP BY name"))
        except sqlite3.Error:
            pass
        indexes = list(c.execute(
            "SELECT name, tbl_name FROM main.sqlite_master WHERE "
            "type = 'index'"))
        for t in tables:
            n = c.execute(f'SELECT COUNT(*) FROM main."{t}"').fetchone()[0]
            c.execute(
                "INSERT INTO information_schema.tables VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                ("def", self.db, t, "BASE TABLE", "InnoDB", n,
                 sizes.get(t, 0),
                 sum(sizes.get(i, 0) for i, tt in indexes if tt == t),
                 meta.get(t, "utf8mb4_bin")))
            for cid, name, typ, notnull, dflt, pk in c.execute(
                    f'PRAGMA main.table_info("{t}")').fetchall():
                c.execute(
                    "INSERT INTO information_schema.columns VALUES "
                    "(?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    ("def", self.db, t, name, cid + 1, dflt,
                     "NO" if notnull or pk else "YES", (typ or "").lower(),
                     "PRI" if pk else ""))

    def _op_begin(self):
        self.begin()

    def _op_commit(self):
        self.commit()

    def _op_rollback(self):
        self.rollback()

    # -- pymysql's Connection API --------------------------------------------
    def cursor(self, cursor=None):
        self._alive()
        return (cursor or self.cursorclass)(self)

    def begin(self):
        self._alive()
        if self._c.in_transaction:
            self.commit()               # MariaDB: BEGIN commits the open one
        try:
            self._c.execute("BEGIN IMMEDIATE")
        except sqlite3.Error as e:
            raise self._map(e, "BEGIN", "BEGIN") from None

    def commit(self):
        self._alive()
        if not self._c.in_transaction:
            return
        try:
            self._c.execute("COMMIT")
        except sqlite3.IntegrityError:
            self._c.execute("ROLLBACK")
            raise IntegrityError(
                1452, "Cannot add or update a child row: a foreign key "
                      "constraint fails (checked at COMMIT: "
                      "foreign_key_checks was 0 in this transaction)") from None
        except sqlite3.Error as e:
            raise self._map(e, "COMMIT", "COMMIT") from None

    def rollback(self):
        self._alive()
        if self._c.in_transaction:
            self._c.execute("ROLLBACK")

    def autocommit(self, value):
        self._alive()
        value = bool(value)
        if value and not self.autocommit_mode:
            self.commit()
        self.autocommit_mode = value

    def get_autocommit(self):
        return self.autocommit_mode

    def select_db(self, db):
        self._alive()
        self.close()
        self.db = db
        self._connect(db)

    def ping(self, reconnect=False):
        if not self.open:
            if not reconnect:
                raise Error("Already closed")
            self._connect(self.db)
        try:
            self._c.execute("SELECT 1").fetchall()
        except sqlite3.Error as e:
            raise OperationalError(2013, f"Lost connection ({e})") from None

    def get_server_info(self):
        return SERVER_VERSION

    def insert_id(self):
        return 0

    def character_set_name(self):
        return self.charset

    def close(self):
        if not self.open:
            raise Error("Already closed")
        self.open = False
        c, self._c = self._c, None
        try:
            if c.in_transaction:
                c.execute("ROLLBACK")
        finally:
            c.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def __del__(self):
        try:
            if self.open:
                self.close()
        except Exception:
            pass


def connect(*args, **kwargs) -> Connection:
    if args:
        raise TypeError("connect() takes keyword arguments only")
    return Connection(**kwargs)


Connect = connect
