#!/usr/bin/env python3
"""The TUI's key-help line fits the screen, and `t` shows its state in it.

    python3 tests/test_tui_help.py

The second line of the admin TUI lists the tab's keys. It is as wide as the
terminal, measured on every redraw:

  fits        it stands still, and the TUI waits the plain REFRESH_MS
  too long    it slides one column every TICK_MS, in a loop (the keys, a gap,
              the keys again), so every key passes through it; a new tab
              starts at its first key; name requests are still looked for
              every REFRESH_MS, not on every slide
  resized     wider and it stops, narrower and it starts

On the agents tab, the `t` of "t auto-approve" is drawn REVERSED (dark on
light) while the token under the cursor auto-approves, plain otherwise; it
follows the cursor and the slide, and nothing is drawn while it is off
screen. `t` itself is one key with no question, and the message it leaves on
the bottom line goes by itself after BANNER_S, with no key pressed.

Drives the real _Tui with a fake screen, a fake clock and stubbed
registries, so it needs no database.
"""
import curses
import importlib.util
import sys
import types
from pathlib import Path

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "deploy" / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []
REV = curses.A_REVERSE


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load_broker():
    spec = importlib.util.spec_from_file_location("tui_help_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)               # importing opens no database
    return b


class Scr:
    """Records each line drawn and every single cell drawn REVERSED."""

    def __init__(self, keys=(), width=400):
        self.keys = list(keys)
        self.width = width
        self.timeouts: list[int] = []
        self.lines: dict[int, str] = {}
        self.rev: list[tuple] = []           # (y, x, text) drawn reversed

    def timeout(self, ms):
        self.timeouts.append(ms)

    def getch(self):
        k = self.keys.pop(0) if self.keys else ord("q")
        return k() if callable(k) else k

    def erase(self):
        self.lines, self.rev = {}, []

    def getmaxyx(self):
        return (40, self.width)

    def addnstr(self, y, x, s, n, *a):
        if a and a[0] & REV and len(s) == 1:
            self.rev.append((y, x, s))
            return
        self.lines[y] = s[:n]

    def refresh(self):
        pass


def main() -> int:
    curses.curs_set = lambda n: None
    b = load_broker()
    PROPS: list[dict] = []
    AGENTS = [{"agent_id": "on-agent", "station_id": "s1",
               "station_name": "bots", "owner_prefix": "ontok123",
               "owner_token_hash": "h1"},
              {"agent_id": "off-agent", "station_id": "s1",
               "station_name": "bots", "owner_prefix": "offtok12",
               "owner_token_hash": "h2"}]
    AUTO = {"ontok123"}
    looks: list[float] = []
    b.PROPOSALS.list = lambda *a, **k: [dict(p) for p in PROPS]
    b.AGENTS.list_all = lambda *a, **k: [dict(r) for r in AGENTS]
    b.TOKENS.list = lambda *a, **k: [
        {"prefix": p, "auto_approve": 1} for p in AUTO]
    b.TOKENS.auto_eligible = lambda p: True
    b.TOKENS.set_auto_approve = lambda p, on: (
        AUTO.add(p) if on else AUTO.discard(p))
    b.CLIENT_STATE.all = lambda *a, **k: []
    b.STATIONS.list = lambda: []
    b.LINKS.of = lambda sid: []
    # A clock the test moves: slides every TICK_MS, requests every
    # REFRESH_MS and the `t` message's BANNER_S, all on time.monotonic().
    clock = {"now": 1000.0}
    real_time = b.time
    b.time = types.SimpleNamespace(
        **{k: getattr(real_time, k) for k in dir(real_time)
           if not k.startswith("_")})
    b.time.monotonic = lambda: clock["now"]

    def counted(t):
        """Count the request looks run() makes (load() reads the list too,
        to draw the rows, and is not one)."""
        real = t.requests

        def requests():
            looks.append(clock["now"])
            return real()
        t.requests = requests
        return t

    def tick(ms=None):
        def k():
            clock["now"] += (ms or b._Tui.TICK_MS) / 1000
            return -1
        return k

    T = b._Tui
    help_ = T.HELP["agents"]
    loop = help_ + T.HELP_GAP
    narrow = 60
    room = narrow - 2
    check("the agents tab's keys do not fit a 60-column terminal (the case "
          "the slide is for)", len(help_) > room, str(len(help_)))

    # --- help_line: fits / slides --------------------------------------------
    t = T(Scr())
    t.view = "agents"
    check("fits: on a wide terminal the keys stand still, whole",
          t.help_line(help_, 398) == help_ and not t._scrolling)
    first = t.help_line(help_, room)
    check("too long: a window exactly the terminal's width, from the first "
          "key", first == help_[:room] and t._scrolling, repr(first))
    t.help_off = 1
    check("one tick slides it one column",
          t.help_line(help_, room) == help_[1:room + 1])
    seen = ""
    for off in range(len(loop)):
        t.help_off = off
        seen += t.help_line(help_, room)[0]
    check("over one loop every column of every key passes through it",
          seen == loop, repr(seen[:60]))
    t.help_off = len(loop)
    check("and then it starts again from the first key",
          t.help_line(help_, room) == help_[:room])
    t.help_off = 9
    t.view = "stations"
    t.help_line(T.HELP["stations"], room)
    check("a new tab starts at its first key", t.help_off == 0,
          str(t.help_off))

    # --- run(): still when it fits, sliding when not, resize switches -----
    drawn: list[str] = []

    def snap(ms=None):
        k = tick(ms)

        def f():
            drawn.append(t.scr.lines.get(1, ""))
            return k()
        return f

    looks.clear()
    t = counted(T(Scr([snap(T.REFRESH_MS), snap(T.REFRESH_MS)], width=400)))
    t.view = "agents"
    t.run()
    check("run, wide: the line is drawn whole and still",
          drawn and all(d == " " + help_ for d in drawn), str(drawn[:1]))
    check("run, wide: the wait is the plain REFRESH_MS, blocking after",
          t.scr.timeouts[:4] == [T.REFRESH_MS, -1, T.REFRESH_MS, -1],
          str(t.scr.timeouts[:4]))

    drawn.clear()
    t = counted(T(Scr([snap(), snap(), snap()], width=narrow)))
    t.view = "agents"
    t.run()
    check("run, narrow: the line fits the terminal and slides one column a "
          "tick", [d[1:] for d in drawn]
          == [help_[:room], help_[1:room + 1], help_[2:room + 2]],
          str(drawn[:2]))
    check("run, narrow: the wait is TICK_MS while it slides",
          t.scr.timeouts[:4] == [T.TICK_MS, -1, T.TICK_MS, -1],
          str(t.scr.timeouts[:4]))

    drawn.clear()

    def widen():
        t.scr.width = 400
        return tick()()
    t = counted(T(Scr([snap(), widen, snap(T.REFRESH_MS)], width=narrow)))
    t.view = "agents"
    t.run()
    check("resized wider: it stops sliding and the wait goes back to "
          "REFRESH_MS", drawn[-1] == " " + help_
          and t.scr.timeouts[-2:] == [T.REFRESH_MS, -1],
          f"{drawn[-1][:40]!r} {t.scr.timeouts[-4:]}")

    # --- requests still every REFRESH_MS while sliding ---------------------
    looks.clear()
    per = int(T.REFRESH_MS / T.TICK_MS) + 1
    got = {}

    def late():
        if not PROPS:
            PROPS.append({"station_id": "s1", "station_name": "bots",
                          "agent_id": "delta", "kind": "claim",
                          "owner_prefix": "tok", "expires_at": 1e12})
        return tick()()

    def look():
        got["msg"] = t.msg
        return ord("q")
    t = counted(T(Scr([late] * per + [look], width=narrow)))
    t.view = "agents"
    t.run()
    check("sliding: name requests are still looked for every REFRESH_MS, "
          "not on every slide", len(looks) == 2,
          f"{len(looks)} looks over {per} ticks")
    check("and one that arrives meanwhile is announced",
          "new name request: delta" in (got.get("msg") or ""), str(got))
    PROPS.clear()

    # --- the reversed `t` ----------------------------------------------------
    t = T(Scr(width=400))
    t.view = "agents"
    t.load()
    t.sel["agents"] = 0                       # on-agent: ontok123, auto on
    t.draw()
    at = 1 + help_.index(T.AUTO_KEY)
    check("t: on the row of a token that auto-approves, the `t` is drawn "
          "reversed, exactly where it is in the key line",
          t.scr.rev == [(1, at, "t")], f"{t.scr.rev} want x={at}")
    t.sel["agents"] = 1                       # off-agent: plain
    t.draw()
    check("t: on a token that does not, it is a plain `t`",
          t.scr.rev == [], str(t.scr.rev))
    t = T(Scr(width=narrow))
    t.view = "agents"
    t.load()
    t.sel["agents"] = 0
    t.draw()
    t.help_off = help_.index(T.AUTO_KEY) - 5
    t.draw()
    check("t: while sliding, the reversed `t` moves with the line",
          t.scr.rev == [(1, 1 + 5, "t")], str(t.scr.rev))
    t.help_off = help_.index(T.AUTO_KEY) + 3
    t.draw()
    check("t: and once it has slid off screen nothing is reversed",
          t.scr.rev == [], str(t.scr.rev))

    # --- `t`: one key, and its message goes by itself after BANNER_S -------
    AUTO.discard("offtok12")
    state = {}

    def press_t():
        t.sel["agents"] = 1                   # off-agent
        return ord("t")

    def after_press():
        state["msg"] = t.msg
        return tick(1000)()

    def later():
        state["msg_3s"] = t.msg
        state["rev"] = list(t.scr.rev)
        return ord("q")
    t = T(Scr([press_t, after_press, tick(1000), tick(1000), tick(1000),
               later], width=400))
    t.view = "agents"
    t.confirm = lambda p: state.setdefault("asked", p) and True
    t.run()
    check("t: one key switches it on, with no question",
          "offtok12" in AUTO and "asked" not in state, str(state))
    check("t: the message is on the bottom line right after",
          state.get("msg") == "auto-approve ON · offtok12",
          str(state.get("msg")))
    check("t: and gone BANNER_S later, with no key pressed",
          state.get("msg_3s") == "", repr(state.get("msg_3s")))
    check("t: while that row's `t` is now drawn reversed",
          (1, 1 + help_.index(T.AUTO_KEY), "t") in state.get("rev", []),
          str(state.get("rev")))
    check("t: the TUI woke for it — a wait no longer than what was left",
          any(0 < w < T.REFRESH_MS for w in t.scr.timeouts if w != -1),
          str(t.scr.timeouts))

    b.time = real_time
    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — the key line fits the screen, and `t` shows its state")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
