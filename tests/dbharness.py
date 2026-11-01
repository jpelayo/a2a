"""One throwaway database per suite — on fakemaria unless told otherwise.

    from dbharness import db_env, require_db

A suite gets a private database by CREATE DATABASE — a unique name, dropped
at exit. Per suite and not per run, so suites stay independent and can run in
parallel: two of them minting tokens or adding agents called "worker" must
not collide.

WHERE THAT DATABASE LIVES

  by default    tests/fakemaria — a test-only `pymysql` backed by sqlite3,
                one file per database under $A2A_FAKEMARIA_DIR. Importing
                this module puts it first on sys.path AND on PYTHONPATH, so
                every `serve` and CLI subprocess a suite starts imports it
                too. No server, nothing installed. What it reproduces and
                what it cannot: tests/fakemaria/pymysql/__init__.py, pinned
                by tests/test_fakemaria.py.

  A2A_TEST_DB_REAL=1
                a real MariaDB/MySQL server, which this does not start:
                    A2A_TEST_DB_HOST   default 127.0.0.1
                    A2A_TEST_DB_PORT   default 3306
                    A2A_TEST_DB_USER   default root
                    A2A_TEST_DB_PASSWORD

Import this BEFORE anything imports pymysql.
"""
import atexit
import os
import shutil
import sys
import tempfile
import uuid
from pathlib import Path

REAL = os.environ.get("A2A_TEST_DB_REAL", "") not in ("", "0")
FAKE = str(Path(__file__).resolve().parent / "fakemaria")
_own_dir = None

if not REAL:
    if FAKE not in sys.path:
        sys.path.insert(0, FAKE)
    paths = [p for p in os.environ.get("PYTHONPATH", "").split(os.pathsep) if p]
    if FAKE not in paths:
        os.environ["PYTHONPATH"] = os.pathsep.join([FAKE, *paths])
    if not os.environ.get("A2A_FAKEMARIA_DIR"):
        _own_dir = tempfile.mkdtemp(prefix="a2a-fakemaria-")
        os.environ["A2A_FAKEMARIA_DIR"] = _own_dir

        @atexit.register              # registered first, so it runs last
        def _remove_dir() -> None:
            shutil.rmtree(_own_dir, ignore_errors=True)

try:
    import pymysql
except ImportError:                                   # pragma: no cover
    pymysql = None

if not REAL and pymysql is not None and not getattr(pymysql, "FAKEMARIA",
                                                     False):
    raise SystemExit(
        "dbharness: a real pymysql was imported before dbharness, so the "
        "suite would not run on fakemaria. Import dbharness first.")

HOST = os.environ.get("A2A_TEST_DB_HOST", "127.0.0.1") if REAL else "127.0.0.1"
PORT = int(os.environ.get("A2A_TEST_DB_PORT", "3306")) if REAL else 3306
USER = os.environ.get("A2A_TEST_DB_USER", "root") if REAL else "root"
PASSWORD = os.environ.get("A2A_TEST_DB_PASSWORD", "") if REAL else ""

_made: list[str] = []


def _admin():
    return pymysql.connect(host=HOST, port=PORT, user=USER,
                           password=PASSWORD, autocommit=True)


def require_db() -> None:
    """Exit with a usable message rather than a driver traceback.

    Only A2A_TEST_DB_REAL=1 can get here without a database: the fake is
    always there. A suite that cannot reach one has not passed and has not
    failed — it has not run, and saying so is the only honest outcome.
    """
    if pymysql is None:
        print("SKIP: A2A_TEST_DB_REAL=1 needs the pymysql package, which "
              "this Python does not have (python3 tests/run.py --setup puts "
              "it in the repo's .venv)", file=sys.stderr)
        raise SystemExit(2)
    try:
        _admin().close()
    except Exception as e:
        print(f"SKIP: no MariaDB at {USER}@{HOST}:{PORT} ({e}).\n"
              "      Point A2A_TEST_DB_HOST/_PORT/_USER/_PASSWORD at one, or "
              "unset A2A_TEST_DB_REAL to run on fakemaria.", file=sys.stderr)
        raise SystemExit(2)


def make_db(tag: str = "") -> str:
    """Create a throwaway database and return its name."""
    name = f"a2a_t_{os.getpid()}_{uuid.uuid4().hex[:8]}"
    conn = _admin()
    try:
        with conn.cursor() as cur:
            # utf8mb4_bin at the database level too: a table created without an
            # explicit collation must not silently become case-insensitive,
            # because `Foo` and `foo` are two different agents.
            cur.execute(f"CREATE DATABASE `{name}` "
                        "CHARACTER SET utf8mb4 COLLATE utf8mb4_bin")
    finally:
        conn.close()
    _made.append(name)
    return name


def drop_db(name: str) -> None:
    try:
        conn = _admin()
        with conn.cursor() as cur:
            cur.execute(f"DROP DATABASE IF EXISTS `{name}`")
        conn.close()
    except Exception:
        pass
    if name in _made:
        _made.remove(name)


@atexit.register
def _cleanup() -> None:
    for name in list(_made):
        drop_db(name)


def db_env(tag: str = "") -> dict:
    """The database keys, pointing at a fresh throwaway database.

    Never a copy of the whole environment, so it composes both ways without a
    duplicate-keyword collision:

        os.environ.update(dbharness.db_env())
        env = dict(os.environ, **dbharness.db_env(), A2A_AUTH_DISABLED="1")

    On fakemaria it also carries PYTHONPATH and A2A_FAKEMARIA_DIR: a suite
    that builds a subprocess's environment from this alone must still reach
    the same fake, and the same files.
    """
    env = {
        "A2A_DB_HOST": HOST,
        "A2A_DB_PORT": str(PORT),
        "A2A_DB_USER": USER,
        "A2A_DB_PASSWORD": PASSWORD,
        "A2A_DB_NAME": make_db(tag),
    }
    if not REAL:
        env["PYTHONPATH"] = os.environ["PYTHONPATH"]
        env["A2A_FAKEMARIA_DIR"] = os.environ["A2A_FAKEMARIA_DIR"]
    return env
