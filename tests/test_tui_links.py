#!/usr/bin/env python3
"""The admin TUI links two stations with a station picker and an agent picker.

    python3 tests/test_tui_links.py

`l` on a station row, and nothing is typed:

  1. a station picker — every other station, the linked ones marked;
  2. for an unlinked one: `everyone` or `choose agents…`;
     for a linked one: `change agents…`, `everyone` or `unlink`;
  3. the agent picker (pick_many): the agents of BOTH stations, space ticks,
     ⏎ saves, esc cancels and changes nothing. Saving with nothing ticked is
     refused — on a link an empty list means NOBODY, which is what agents
     leaving produce, never a choice made by hand.

Unlink asks first, like every other destructive key. The station row shows
its links (`↔ B`).

Drives the real _Tui with a fake screen and stubbed registries, so it needs no
database. What a link DOES to a DM is tests/test_station_links.py (MariaDB).
"""
import curses
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "deploy" / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []
ENTER, ESC, SPACE, DOWN = 10, 27, ord(" "), curses.KEY_DOWN


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load_broker():
    spec = importlib.util.spec_from_file_location("tui_links_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)               # importing opens no database
    return b


class Scr:
    """A curses window that records what is drawn and answers getch() from a
    script of keys."""

    def __init__(self, keys):
        self.keys = list(keys)
        self.drawn: list[str] = []

    def timeout(self, ms):
        pass

    def getch(self):
        return self.keys.pop(0) if self.keys else ESC

    def erase(self):
        pass

    def getmaxyx(self):
        return (40, 200)

    def addnstr(self, y, x, s, *a):
        self.drawn.append(s)

    def refresh(self):
        pass


A = {"station_id": "id-a", "name": "A", "open": 0}
B = {"station_id": "id-b", "name": "B", "open": 0}
C = {"station_id": "id-c", "name": "C", "open": 0}
AGENTS_OF = {"id-a": ["alice", "carol"], "id-b": ["bob"], "id-c": []}


def main() -> int:
    curses.curs_set = lambda n: None
    b = load_broker()
    state: dict = {}                       # frozenset({sid, sid}) -> only
    calls: list[tuple] = []

    def of(sid):
        out = []
        for pair, only in state.items():
            if sid in pair:
                other = next(s for s in (A, B, C)
                             if s["station_id"] in pair and s["station_id"] != sid)
                out.append({"station_id": other["station_id"],
                            "name": other["name"], "only": only,
                            "only_names": None})
        return out

    def link(x, y, only=None):
        pairs = None if only is None else [list(p) for p in only]
        calls.append(("link", x, y, pairs))
        state[frozenset((x, y))] = pairs
        names = {s["station_id"]: s["name"] for s in (A, B, C)}
        return {"stations": [names[x], names[y]],
                "only": None if pairs is None
                else [f"{aid}@{names[sid]}" for sid, aid in pairs]}

    def unlink(x, y):
        calls.append(("unlink", x, y))
        return 1 if state.pop(frozenset((x, y)), "gone") != "gone" else 0

    b.STATIONS.list = lambda: [dict(s) for s in (A, B, C)]
    b.STATIONS.allowed = lambda sid: {"tokens": []}
    b.AGENTS.list_all = lambda sid=None: [
        {"agent_id": a, "station_id": sid} for a in AGENTS_OF.get(sid, [])]
    b.LINKS.of = of
    b.LINKS.link = link
    b.LINKS.unlink = unlink

    def run(keys, confirm=True):
        calls.clear()
        t = b._Tui(Scr(keys))
        t.view = "stations"
        t.confirms = []
        t.confirm = lambda p: (t.confirms.append(p), confirm)[1]
        t.load()                           # cursor on A, the first station
        t.act(ord("l"))
        return t

    # --- unlinked: everyone -------------------------------------------------
    t = run([ENTER, ENTER])                # B, then "everyone"
    check("l → B → everyone links A and B for everyone",
          calls == [("link", "id-a", "id-b", None)], str(calls))
    check("…and says so", t.msg == "A ↔ B linked for everyone", t.msg)
    check("the station picker offered the other stations, not A itself",
          any(s.startswith("B ") for s in t.scr.drawn)
          and any(s.startswith("C ") for s in t.scr.drawn)
          and not any(s.startswith("A ") for s in t.scr.drawn),
          str(t.scr.drawn[:6]))

    # --- unlinked: choose agents ---------------------------------------------
    state.clear()
    t = run([ENTER, DOWN, ENTER,           # B, "choose agents…"
             SPACE, DOWN, DOWN, SPACE, ENTER])   # tick alice and bob, save
    check("choose agents… → space on two rows → ⏎ links with exactly those two",
          calls == [("link", "id-a", "id-b",
                     [["id-a", "alice"], ["id-b", "bob"]])], str(calls))
    check("the agent picker lists the agents of BOTH stations",
          any("alice" in s and s.rstrip().endswith("A") for s in t.scr.drawn)
          and any("bob" in s and s.rstrip().endswith("B") for s in t.scr.drawn),
          str([s for s in t.scr.drawn if "☐" in s or "☑" in s][:6]))
    check("…and the status line names them",
          t.msg == "A ↔ B: only alice@A, bob@B", t.msg)

    # --- esc changes nothing -------------------------------------------------
    state.clear()
    t = run([ENTER, DOWN, ENTER, SPACE, ESC])
    check("esc in the agent picker changes nothing", calls == [], str(calls))

    # --- nothing ticked ------------------------------------------------------
    state.clear()
    t = run([ENTER, DOWN, ENTER, ENTER, ESC])
    check("⏎ with nothing ticked is refused and links nothing",
          calls == [], str(calls))
    check("…and says what to do instead",
          any("tick at least one, or choose everyone" in s
              for s in t.scr.drawn), str(t.scr.drawn[-4:]))
    check("…and the picker stayed open for the next key (esc), rather than "
          "returning on the refused ⏎", t.scr.keys == [], str(t.scr.keys))

    # --- a linked station: current list pre-ticked ---------------------------
    state.clear()
    state[frozenset(("id-a", "id-b"))] = [["id-a", "alice"]]
    t = run([ENTER, ENTER, ENTER])         # B, "change agents…", save as is
    check("a linked station is marked in the station picker",
          any(s.startswith("B ") and "✓ linked · 1 agent(s)" in s
              for s in t.scr.drawn), str(t.scr.drawn[:4]))
    check("change agents… opens with the current list ticked — ⏎ alone keeps it",
          calls == [("link", "id-a", "id-b", [["id-a", "alice"]])], str(calls))
    check("…and draws it ticked",
          any(s.startswith("☑ alice") for s in t.scr.drawn)
          and any(s.startswith("☐ bob") for s in t.scr.drawn),
          str([s for s in t.scr.drawn if "☐" in s or "☑" in s][:6]))

    # --- a linked station: everyone lifts the limit --------------------------
    t = run([ENTER, DOWN, ENTER])          # B, "everyone"
    check("everyone on a linked station lifts the limit",
          calls == [("link", "id-a", "id-b", None)], str(calls))

    # --- unlink asks first ---------------------------------------------------
    t = run([ENTER, DOWN, DOWN, ENTER], confirm=False)
    check("unlink asks first, and no keeps the link",
          t.confirms and calls == [] and frozenset(("id-a", "id-b")) in state,
          f"{t.confirms} {calls}")
    t = run([ENTER, DOWN, DOWN, ENTER], confirm=True)
    check("unlink + yes unlinks",
          calls == [("unlink", "id-a", "id-b")]
          and t.msg == "A and B unlinked", f"{calls} {t.msg}")

    # --- the station row -----------------------------------------------------
    state.clear()
    state[frozenset(("id-a", "id-b"))] = None
    t = b._Tui(Scr([]))
    t.view = "stations"
    t.load()
    rows = {r["name"]: t.line(r) for r in t.rows}
    check("the station row shows its links",
          rows["A"].endswith("↔ B") and rows["B"].endswith("↔ A")
          and "↔" not in rows["C"], str(rows))
    check("HELP names the key", "l link stations" in b._Tui.HELP["stations"],
          b._Tui.HELP["stations"])

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — stations are linked by picking, never by typing")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
