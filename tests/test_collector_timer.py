#!/usr/bin/env python3
"""The collector runs on its own clock, over every station.

    python3 tests/test_collector_timer.py

About seventy messages sat in a station hours past their expires_at until an
operator ran `compact` by hand. Nothing had failed — `compact` runs the same
code and succeeded, and the log held no traceback. The collector had simply
never run there: it had no clock of its own and rode on traffic, a stream's
quiet tick and a handful of read/ack handlers, behind ONE global debounce, and
each pass collected only the station whose request had triggered it. A station
that kept losing that race, or had nothing connected at all, was never
collected, and nothing said so.

Two halves:

  no database   the loop itself — it runs at once and then on its interval,
                survives a pass that raises, and stops when cancelled — and
                the source holds no traffic-driven trigger any more.
  database      the exact case that went wrong: expired messages in two
                stations, NO clients and NO requests, and a real `serve`
                removes them anyway, at startup and on every tick after. And
                one station that fails cannot stop the others being collected.

The database half SKIPs, like every other DB suite, without a MariaDB to point
at (see dbharness.py); the exit code is then 2, because it has not run.
"""
import ast
import asyncio
import importlib.util
import os
import socket
import subprocess
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


def import_broker(tag: str):
    """The broker as a module. Importing opens no database connection —
    `_startup()` does that — so this works with or without MariaDB."""
    spec = importlib.util.spec_from_file_location(f"broker_ct_{tag}", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# Every function that may call the collector. All of them are an operator
# acting on purpose, or the timer. A read, an ack, or a stream tick must never
# appear here again: that is the trigger that starved stations.
ON_DEMAND = {
    "_collector_loop",                       # the clock
    "collect",                               # calls _collect_station
    "_cli_compact", "_cli_screen",           # CLI
    "admin_screen_station",                  # admin HTTP
    "act", "screen_confirm", "mark_segment", "_fallback_screen",   # TUI
}
FORMER_TRIGGERS = {"read_channel", "ack_messages", "ack_all", "read_dms",
                   "list_messages_route", "ack_all_route", "list_dms_route",
                   "stream_route", "_gen"}


def collector_callers(src: str) -> set[str]:
    """Names of the functions whose bodies call collect / _collect_station."""
    found: set[str] = set()

    def walk(node, stack):
        for ch in ast.iter_child_nodes(node):
            if isinstance(ch, (ast.FunctionDef, ast.AsyncFunctionDef)):
                walk(ch, stack + [ch.name])
                continue
            if isinstance(ch, ast.Call):
                f = ch.func
                direct = isinstance(f, ast.Name) and f.id in (
                    "collect", "_collect_station")
                via_db = (isinstance(f, ast.Name) and f.id == "_db"
                          and ch.args and isinstance(ch.args[0], ast.Name)
                          and ch.args[0].id == "collect")
                if direct or via_db:
                    found.add(stack[-1] if stack else "<module>")
            walk(ch, stack)

    walk(ast.parse(src), [])
    return found


def no_database_half() -> None:
    b = import_broker("nodb")

    calls: list[float] = []
    logged: list[tuple] = []

    def fake_collect():
        calls.append(time.monotonic())
        if len(calls) == 2:
            raise RuntimeError("a pass that fails")
        return {}

    b.collect = fake_collect
    b.log = lambda msg, **kw: logged.append(
        (kw.get("level"), kw.get("event"), msg))

    async def drive():
        start = time.monotonic()
        task = asyncio.create_task(b._collector_loop(0.05))
        await asyncio.sleep(0.6)
        task.cancel()
        try:
            await task
            return start, "returned"
        except asyncio.CancelledError:
            return start, "cancelled"

    start, outcome = asyncio.run(drive())
    check("the first pass runs at once — what expired while the broker was "
          "down goes at startup, not one interval later",
          bool(calls) and calls[0] - start < 0.05,
          f"first pass after {calls[0] - start if calls else None}s")
    check("and it keeps running on its interval with no traffic at all",
          len(calls) >= 4, f"{len(calls)} passes in 0.6 s at 0.05 s")
    check("a pass that raises is logged as an ERROR collect.error …",
          any(lvl == "ERROR" and ev == "collect.error"
              for lvl, ev, _ in logged), str(logged))
    check("… and does NOT stop the clock",
          len(calls) > 2, f"{len(calls)} passes")
    check("cancelling it ends it — that is how serve's lifespan stops it",
          outcome == "cancelled", outcome)

    src = BROKER.read_text()
    check("the traffic-driven trigger is gone: no _maybe_collect, no shared "
          "_last_collect debounce",
          "_maybe_collect" not in src and "_last_collect" not in src, "")
    callers = collector_callers(src)
    check("only the timer and operator commands call the collector — no "
          "read, ack or stream path",
          callers <= ON_DEMAND and not (callers & FORMER_TRIGGERS),
          f"unexpected callers: {sorted(callers - ON_DEMAND)}")
    check("serve starts the timer in its lifespan and floors the interval",
          "_collector_loop(max(COLLECT_INTERVAL, 1.0))" in src, "")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def database_half() -> None:
    env = dbharness.db_env()
    os.environ.update(env)
    os.environ["A2A_AUTH_DISABLED"] = "1"
    b = import_broker("db")
    b._startup()

    A = b.STATIONS.create("alpha")["station_id"]
    B = b.STATIONS.create("beta")["station_id"]
    for sid in (A, B):
        for who in ("poster", "absent"):
            b.AGENTS.add(sid, who)

    def stale(sid, label):
        """A DM that expires in a second and that nobody will ever ack."""
        asyncio.run(b.DIRECT.send(sid, "poster", "absent", label,
                                  expires_in="1s"))

    def texts(sid):
        return [r["text"] for r in b.CONN.execute(
            "SELECT text FROM dms WHERE station_id = %s", (sid,)).fetchall()]

    # --- one station failing cannot hold the others ------------------------
    stale(A, "alpha-1")
    stale(B, "beta-1")
    time.sleep(1.2)
    real = b._collect_station

    def flaky(sid, now=None):
        if sid == A:
            raise RuntimeError("alpha is broken")
        return real(sid, now)

    b._collect_station = flaky
    try:
        st = b.collect()
    finally:
        b._collect_station = real
    check("a station that raises is skipped and the next one is still "
          "collected — beta's expired, unacked DM is gone",
          "beta-1" not in texts(B), str(texts(B)))
    check("the failing station is named in the result",
          st.get("failed_stations") == [A], str(st))
    err = b.CONN.execute(
        "SELECT level, station FROM logs WHERE event = 'collect.error'"
    ).fetchall()
    check("and recorded in the logs table as an ERROR collect.error for that "
          "station, so a stuck station is visible rather than silent",
          any(r["level"] == "ERROR" and r["station"] == A for r in err),
          str(err))
    check("its own expired DM is still there for the next pass",
          "alpha-1" in texts(A), str(texts(A)))

    # --- a real serve, with no clients and no requests ---------------------
    stale(B, "beta-2")
    time.sleep(1.2)
    port = free_port()
    proc = subprocess.Popen(
        [sys.executable, str(BROKER), "serve"],
        env=dict(os.environ, **env, A2A_AUTH_DISABLED="1",
                 A2A_HOST="127.0.0.1", A2A_PORT=str(port),
                 A2A_COLLECT_INTERVAL="1"),
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    try:
        def gone(*pairs, secs):
            end = time.time() + secs
            while time.time() < end:
                if all(label not in texts(sid) for sid, label in pairs):
                    return True
                time.sleep(0.2)
            return False

        # No stream, no read, no ack, not even /healthz: nothing but the
        # timer can be what removes these.
        check("serve collects EVERY station with nothing connected and no "
              "request made — the case that left 70 expired messages for hours",
              gone((A, "alpha-1"), (B, "beta-2"), secs=20),
              f"alpha={texts(A)} beta={texts(B)}")

        stale(A, "alpha-3")
        check("and again on the next ticks, not just once at startup",
              gone((A, "alpha-3"), secs=6), str(texts(A)))
        rows = b.CONN.execute(
            "SELECT level FROM logs WHERE event = 'collect'").fetchall()
        check("a pass that removed something is on record at INFO",
              any(r["level"] == "INFO" for r in rows), str(rows))
    finally:
        proc.terminate()
        try:
            _, err_out = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            _, err_out = proc.communicate()
    check("serve shut down cleanly with the timer running",
          "Traceback" not in (err_out or ""), (err_out or "")[-400:])


def main() -> int:
    no_database_half()
    try:
        dbharness.require_db()
    except SystemExit:
        print("\nSKIP: the database half — no MariaDB (see dbharness.py)")
        for f in fails:
            print("FAIL", f)
        return 1 if fails else 2
    database_half()
    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — every station is collected on a clock, whatever its traffic")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
