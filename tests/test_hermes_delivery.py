#!/usr/bin/env python3
"""The Hermes plugin's recovery, against the REAL broker.

    python3 tests/test_hermes_delivery.py

test_hermes_client.py proves the lifecycle against a fake broker that simply
replays everything unacked on every connection. The real one does not: a live
stream fetches only receipts never handed out (`delivered_at IS NULL`), and a
delivered-but-unacked message comes back only when its connection ends (the
stream's close returns what it took) or on a reconnect's replay pass, which is
bounded to STREAM_REPLAY_WINDOW. So the two recovery paths are proven here,
against `serve`, with the receipt read from the database:

  unseen     the gateway accepted the injection and no turn ever showed it.
             Waiting would do nothing — the healthy stream will never send it
             again — so the plugin reopens the stream after T_UNSEEN, and the
             broker sends it again; seen, its receipt is acked.
  refused    the gateway refused it. The plugin reopens the stream, and the
             broker offers it again.
  closed     a desktop-app conversation closed for longer than the replay
             window: the stream will never offer its message again, so the
             plugin reads it with GET /pending/peek — which marks nothing —
             and delivers it once the conversation is open.

And one constant: T_UNSEEN must sit inside STREAM_REPLAY_WINDOW, because the
replay pass is what covers a broker killed too hard to run the stream's close.

The database half SKIPs, like every DB suite, without a MariaDB (see
dbharness.py); the exit code is then 2, because it has not run.
"""
import importlib.util
import os
import socket
import subprocess
import sys
import time
import urllib.request
from pathlib import Path

sys.dont_write_bytecode = True

import dbharness  # noqa: E402

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"
sys.path.insert(0, str(HERE))
# The stubbed Hermes (session ContextVars, home) and the fake PluginContext.
import test_hermes_client as th  # noqa: E402

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def import_broker(tag: str):
    """The broker as a module; importing opens no database connection."""
    spec = importlib.util.spec_from_file_location(f"broker_hd_{tag}", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def no_database_half() -> None:
    b = import_broker("nodb")
    os.environ.pop("A2A_HERMES_UNSEEN", None)
    plugin = th.load("http://127.0.0.1:1")
    check("T_UNSEEN sits inside the broker's STREAM_REPLAY_WINDOW, with "
          "margin — the replay pass is what recovers a message when the "
          "broker died too hard to return it on the stream's close",
          plugin.T_UNSEEN < b.STREAM_REPLAY_WINDOW * 0.9,
          f"{plugin.T_UNSEEN} vs {b.STREAM_REPLAY_WINDOW}")


def database_half() -> None:
    env = dbharness.db_env()
    os.environ.update(env)
    os.environ["A2A_AUTH_DISABLED"] = "1"
    b = import_broker("db")
    b._startup()
    sid = b.DEFAULT_STATION_ID
    for who in ("bot", "peer"):
        b.AGENTS.add(sid, who)

    def receipt(mid):
        return b.CONN.execute(
            "SELECT delivered_at, acked_at FROM message_receipts "
            "WHERE station_id = %s AND agent_id = %s AND msg_id = %s",
            (sid, "bot", mid)).fetchone() or {}

    def dm(text):
        import asyncio
        out = asyncio.run(b.DIRECT.send(sid, "peer", "bot", text))
        return (out.get("dm") or out).get("id")

    port = free_port()
    base = f"http://127.0.0.1:{port}"
    proc = subprocess.Popen(
        [sys.executable, str(BROKER), "serve"],
        env=dict(os.environ, **env, A2A_AUTH_DISABLED="1",
                 A2A_HOST="127.0.0.1", A2A_PORT=str(port),
                 A2A_STREAM_KEEPALIVE="1"),
        stdout=subprocess.DEVNULL, stderr=subprocess.PIPE, text=True)
    ctx = None
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("broker did not come up")

        os.environ["A2A_HERMES_UNSEEN"] = "2"
        plugin = th.load(base, token="dev")
        ctx = th.Ctx("bot", config=th.ON)
        inst = th.start(plugin, ctx)
        check("the plugin streams from the real broker as the bot",
              th.until(lambda: inst.state["connected"], 10), str(inst.state))

        # --- accepted, never seen ------------------------------------------
        d1 = dm("accepted but lost")
        check("a DM is injected into the route chat",
              th.until(lambda: d1 in ctx.ids_injected(), 10),
              str(ctx.ids_injected()))
        check("and the broker has it as delivered, not acked",
              receipt(d1).get("delivered_at") and not receipt(d1).get(
                  "acked_at"), str(receipt(d1)))
        check("no turn shows it: after T_UNSEEN the plugin reopens the "
              "stream and the REAL broker sends it again",
              th.until(lambda: ctx.ids_injected().count(d1) >= 2, 20),
              f"injected {ctx.ids_injected().count(d1)}x")
        ctx.turn(th.GROUP, "s1",
                 [c for c, _ in ctx.injected if f'id="{d1}"' in c][-1])
        check("seen at last, its receipt is ACKED in the database",
              th.until(lambda: bool(receipt(d1).get("acked_at")), 10),
              str(receipt(d1)))

        # --- refused --------------------------------------------------------
        ctx.inject_result = False
        d2 = dm("refused once")
        check("a refused injection acks nothing",
              th.until(lambda: d2 in ctx.ids_injected(), 10)
              and not receipt(d2).get("acked_at"), str(receipt(d2)))
        ctx.inject_result = True
        check("the next connection offers it again, and it is accepted",
              th.until(lambda: inst.deliveries.get(d2, {}).get("state")
                       == "scheduled", 20)
              and ctx.ids_injected().count(d2) >= 2,
              f"{inst.deliveries.get(d2)} "
              f"injected {ctx.ids_injected().count(d2)}x")
        ctx.turn(th.GROUP, "s1",
                 [c for c, _ in ctx.injected if f'id="{d2}"' in c][-1])
        check("and, seen, acked in the database",
              th.until(lambda: bool(receipt(d2).get("acked_at")), 10),
              str(receipt(d2)))
        th.stop(ctx)
        ctx = None

        # --- a desktop-app conversation, closed past the replay window -------
        os.environ["HERMES_DESKTOP"] = "1"
        plugin.PEEK_S = 0.5
        ctx = th.Ctx("bot", gateway=False, tui=True, config=th.DESK_ON)
        inst = th.start(plugin, ctx)
        check("the app's backend streams the app route from the real broker",
              th.until(lambda: inst.state["connected"], 10), str(inst.state))
        d3 = dm("waits while the conversation is closed")
        check("closed in the app: refused, and nothing is acked",
              th.until(lambda: inst._closed_since is not None, 10)
              and not receipt(d3).get("acked_at"), str(receipt(d3)))
        before = receipt(d3)
        peek = plugin.api("GET", "/pending/peek", agent="bot")
        check("GET /pending/peek returns it, in the stream's own shape",
              isinstance(peek, dict) and any(
                  m.get("id") == d3 and "audience" in m
                  for m in peek.get("messages") or []), str(peek)[:300])
        check("and writes nothing — not delivered, not acked (/pending would "
              "have acked it)", receipt(d3) == before, f"{before} → "
              f"{receipt(d3)}")
        # Past the replay window: the stream will never offer it again.
        b.CONN.execute(
            "UPDATE message_receipts SET delivered_at = %s "
            "WHERE station_id = %s AND agent_id = %s AND msg_id = %s",
            (time.time() - 2 * b.STREAM_REPLAY_WINDOW, sid, "bot", d3))
        ctx.live_keys = {th.DESK["session_key"]}
        check("opened again: the peek hands over what the stream never would",
              th.until(lambda: d3 in ctx.ids_injected(), 15),
              str(ctx.ids_injected()))
        ctx.turn(th.DESK, "s1",
                 [c for c, _ in ctx.injected if f'id="{d3}"' in c][-1])
        check("and, seen in that conversation, acked in the database",
              th.until(lambda: bool(receipt(d3).get("acked_at")), 10),
              str(receipt(d3)))
    finally:
        os.environ.pop("HERMES_DESKTOP", None)
        if ctx is not None:
            th.stop(ctx)
        proc.terminate()
        try:
            proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()


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
          "PASS — what the gateway lost, the real broker sends again")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
