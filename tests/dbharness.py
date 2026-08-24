"""One throwaway MariaDB database per suite.

    from dbharness import db_env, require_db

Storage moved from a sqlite file to a MariaDB server, so a suite can no longer
get a private database by naming a path in a temp directory. It gets one by
CREATE DATABASE instead — a real database with a unique name, dropped at exit.

Per suite and not per run, so suites stay independent and can run in parallel:
two of them minting tokens or adding agents called "worker" must not collide.
Twenty-one files should not each reinvent this, which is why it lives here.

The server itself is not started here. Point these at one:

    A2A_TEST_DB_HOST   default 127.0.0.1
    A2A_TEST_DB_PORT   default 3306
    A2A_TEST_DB_USER   default root
    A2A_TEST_DB_PASSWORD
"""
import atexit
import os
import sys
import uuid

try:
    import pymysql
except ImportError:                                   # pragma: no cover
    pymysql = None

HOST = os.environ.get("A2A_TEST_DB_HOST", "127.0.0.1")
PORT = int(os.environ.get("A2A_TEST_DB_PORT", "3306"))
USER = os.environ.get("A2A_TEST_DB_USER", "root")
PASSWORD = os.environ.get("A2A_TEST_DB_PASSWORD", "")

_made: list[str] = []


def _admin():
    return pymysql.connect(host=HOST, port=PORT, user=USER,
                           password=PASSWORD, autocommit=True)


def require_db() -> None:
    """Exit with a usable message rather than a driver traceback.

    A suite that cannot reach a database has not passed and has not failed —
    it has not run, and saying so is the only honest outcome.
    """
    if pymysql is None:
        print("SKIP: pymysql is not installed (pip3 install pymysql)",
              file=sys.stderr)
        raise SystemExit(2)
    try:
        _admin().close()
    except Exception as e:
        print(f"SKIP: no MariaDB at {USER}@{HOST}:{PORT} ({e}).\n"
              "      Start one, or set A2A_TEST_DB_HOST/_PORT/_USER/"
              "_PASSWORD.", file=sys.stderr)
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
    """Just the database keys, pointing at a fresh throwaway database.

    Only the five keys, never a copy of the whole environment, so it composes
    both ways without a duplicate-keyword collision:

        os.environ.update(dbharness.db_env())
        env = dict(os.environ, **dbharness.db_env(), A2A_AUTH_DISABLED="1")
    """
    return {
        "A2A_DB_HOST": HOST,
        "A2A_DB_PORT": str(PORT),
        "A2A_DB_USER": USER,
        "A2A_DB_PASSWORD": PASSWORD,
        "A2A_DB_NAME": make_db(tag),
    }
