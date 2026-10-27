#!/usr/bin/env python3
"""The admin TUI answers name requests with one key, and sees new ones arrive.

    python3 tests/test_tui_requests.py

Two things an operator waiting on agents needs:

  live      a request that arrives while the TUI is open shows up by itself —
            run() used to block on getch(), so it appeared only after some
            key was pressed. Now the wait gives up every REFRESH_MS, looks at
            the requests, and redraws only when they changed (the other tabs
            are not re-queried for nothing). A tick never clears the status
            line, and the wait goes back to BLOCKING straight after, so ask()
            and pick() wait for the operator as they always did.
  one key   `a` approves and `x` rejects a request — claim or transfer — with
            no y/N; the status line says what was done. Removing an AGENT
            (`x` on an agent row) still asks.

Because a request lands at the TOP of the agents list, a refresh used to be
able to slide the cursor onto another row under a one-key approve. load()
keeps the cursor on the row it was on; that is checked too.

Drives the real _Tui with a fake screen and stubbed registries: importing the
broker opens no database, and nothing here needs one.
"""
import curses
import importlib.util
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load_broker():
    spec = importlib.util.spec_from_file_location("tui_req_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)               # importing opens no database
    return b


class Scr:
    """A curses window that draws nothing and answers getch() from a script.
    A callable in the script runs at that moment (to look, or to change the
    world) and returns the key."""

    def __init__(self, keys):
        self.keys = list(keys)
        self.timeouts: list[int] = []
        self.waited: list = []            # the timeout in force at each getch

    def timeout(self, ms):
        self.timeouts.append(ms)

    def getch(self):
        self.waited.append(self.timeouts[-1] if self.timeouts else None)
        k = self.keys.pop(0) if self.keys else ord("q")
        return k() if callable(k) else k

    def erase(self):
        pass

    def getmaxyx(self):
        return (40, 200)

    def addnstr(self, *a):
        pass

    def refresh(self):
        pass


def proposal(agent, kind="claim", **kw):
    return dict({"station_id": "s1", "station_name": "bots", "agent_id": agent,
                 "kind": kind, "owner_prefix": "newtok12", "note": "",
                 "expires_at": time.time() + 3600}, **kw)


def main() -> int:
    curses.curs_set = lambda n: None      # run() hides the cursor; no terminal
    b = load_broker()
    PROPS: list[dict] = []
    AGENTS_ROWS = [{"agent_id": "zeta", "station_id": "s1",
                    "station_name": "bots", "owner_token_hash": "h",
                    "owner_prefix": "abcd1234"}]
    done: list[tuple] = []

    def approve(sid, aid):
        done.append(("approve", sid, aid))
        PROPS[:] = [p for p in PROPS if p["agent_id"] != aid]
        return {"agent_id": aid}

    def reject(sid, aid):
        done.append(("reject", sid, aid))
        row = next(p for p in PROPS if p["agent_id"] == aid)
        PROPS[:] = [p for p in PROPS if p["agent_id"] != aid]
        return ({"locked_until": time.time() + 60}
                if row["kind"] == "transfer" else {})

    b.PROPOSALS.list = lambda *a, **k: [dict(p) for p in PROPS]
    b.PROPOSALS.approve = approve
    b.PROPOSALS.reject = reject
    b.AGENTS.list_all = lambda *a, **k: [dict(r) for r in AGENTS_ROWS]
    b.AGENTS.remove = lambda aid, sid: done.append(("remove", sid, aid))
    b.TOKENS.list = lambda *a, **k: []
    b._pending_rows = lambda sid, aid, n: [{}, {}]

    def make(keys=()):
        t = b._Tui(Scr(keys))
        t.view = "agents"
        t.confirms: list[str] = []
        t.confirm = lambda prompt: (t.confirms.append(prompt), True)[1]
        t.loads = 0
        real = t._load_rows

        def counted():
            t.loads += 1
            real()
        t._load_rows = counted
        return t

    # --- one key ------------------------------------------------------------
    PROPS[:] = [proposal("alpha")]
    t = make()
    t.load()
    t.act(ord("a"))
    check("one key: `a` on a request approves it — no y/N",
          done == [("approve", "s1", "alpha")] and not t.confirms,
          f"{done} confirms={t.confirms}")
    check("…and the status line says what was done",
          "alpha approved and bound to newtok12" in t.msg, t.msg)

    done.clear()
    PROPS[:] = [proposal("beta", "transfer", current_owner_prefix="old12345")]
    t = make()
    t.load()
    t.act(ord("a"))
    check("one key: `a` on a TRANSFER approves it too — no y/N",
          done == [("approve", "s1", "beta")] and not t.confirms,
          f"{done} confirms={t.confirms}")
    check("…and says who it moved from and to, and what it keeps",
          "transferred from old12345 to newtok12" in t.msg
          and "2 unacked" in t.msg, t.msg)

    done.clear()
    PROPS[:] = [proposal("gamma", "transfer", current_owner_prefix="old1")]
    t = make()
    t.load()
    t.act(ord("x"))
    check("one key: `x` on a request rejects it — no y/N",
          done == [("reject", "s1", "gamma")] and not t.confirms,
          f"{done} confirms={t.confirms}")
    check("…and a rejected transfer says the token is locked out",
          "gamma rejected" in t.msg and "may not ask again" in t.msg, t.msg)

    done.clear()
    PROPS[:] = []
    t = make()
    t.load()
    t.act(ord("x"))
    check("`x` on an AGENT row (remove) still asks first",
          t.confirms and done == [("remove", "s1", "zeta")],
          f"{done} confirms={t.confirms}")

    # --- live ---------------------------------------------------------------
    done.clear()
    PROPS[:] = []
    seen: dict = {}

    def first():
        t.msg = "kept"                    # what an action left on screen
        return -1

    def second():
        seen["msg_after_quiet_tick"] = t.msg
        seen["loads_after_quiet_tick"] = t.loads
        PROPS.append(proposal("delta", station_name="bots"))
        return -1

    def third():
        seen["msg"] = t.msg
        seen["loads"] = t.loads
        seen["cursor"] = (t.current() or {}).get("agent_id")
        seen["rows"] = [r["agent_id"] for r in t.rows]
        return ord("q")

    t = make([first, second, third])
    t.run()
    check("live: a tick with nothing new neither reloads nor clears the "
          "status line",
          seen.get("loads_after_quiet_tick") == 1
          and seen.get("msg_after_quiet_tick") == "kept", str(seen))
    check("live: a request arriving while the TUI waits shows up with no "
          "key pressed", seen.get("rows") == ["delta", "zeta"]
          and seen.get("loads") == 2, str(seen))
    check("live: and the status line names it and the keys that answer it",
          "new name request: delta (bots)" in (seen.get("msg") or "")
          and "a approve" in (seen.get("msg") or ""), str(seen.get("msg")))
    check("live: the cursor stays on the row it was on — the request landed "
          "above it, and a cursor kept by index would now sit on the request",
          seen.get("cursor") == "zeta", str(seen))
    check("live: every wait gives up after REFRESH_MS…",
          t.scr.waited == [b._Tui.REFRESH_MS] * 3, str(t.scr.waited))
    check("…and goes back to BLOCKING straight after, so ask() and pick() "
          "wait for the operator as before",
          t.scr.timeouts == [b._Tui.REFRESH_MS, -1] * 3, str(t.scr.timeouts))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — requests arrive by themselves and one key answers them")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
