#!/usr/bin/env python3
"""The admin TUI manages a station's tokens from the stations tab.

    python3 tests/test_tui_tokens.py

`t` on a station row opens that station's tokens: prefix, user, label,
status (fresh/used/revoked) and how many of the station's agents use each.

  n   mints a token and grants it into THIS station; shown once
  g   adds an existing token: only valid ones not already here are offered
  r   removes the token from THIS station only — asks, naming the agents it
      leaves out; never a revoke or a delete (those are the tokens tab's)
  esc back to the stations list, reloaded

`g` and `v` on the stations tab are gone: they live behind `t` now.

Drives the real _Tui with a fake screen and stubbed registries, so it needs
no database. What a grant DOES is tests/test_mariadb.py's.
"""
import curses
import importlib.util
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "deploy" / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []
ENTER, ESC, DOWN = 10, 27, curses.KEY_DOWN


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load_broker():
    spec = importlib.util.spec_from_file_location("tui_tokens_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)               # importing opens no database
    return b


class Scr:
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


def main() -> int:
    curses.curs_set = lambda n: None
    b = load_broker()

    stations = {"id-a": {"station_id": "id-a", "name": "A", "open": 0},
                "id-b": {"station_id": "id-b", "name": "B", "open": 0}}
    tokens = {
        "aaaa1111": {"prefix": "aaaa1111", "user": "pelayo", "label": "laptop",
                     "last_used_at": 1.0, "revoked_at": None},
        "bbbb2222": {"prefix": "bbbb2222", "user": "bots", "label": "server",
                     "last_used_at": None, "revoked_at": None},
        "cccc3333": {"prefix": "cccc3333", "user": "old", "label": "",
                     "last_used_at": 1.0, "revoked_at": 2.0},
        "dddd4444": {"prefix": "dddd4444", "user": "spare", "label": "",
                     "last_used_at": None, "revoked_at": None},
    }
    grants: set = set()
    agents = {"id-a": [("alice", "aaaa1111"), ("carol", "aaaa1111"),
                       ("dave", "cccc3333")], "id-b": [("bob", "dddd4444")]}
    calls: list[tuple] = []

    def reset():
        grants.clear()
        grants.update({("id-a", "aaaa1111"), ("id-a", "cccc3333"),
                       ("id-b", "dddd4444")})
        for p in list(tokens):
            if p.startswith("new"):
                del tokens[p]
        stations["id-a"]["open"] = 0
        calls.clear()

    def tlist(station_id=None, include_revoked=False):
        return [dict(t) for p, t in tokens.items()
                if (station_id is None or (station_id, p) in grants)
                and (include_revoked or not t["revoked_at"])]

    def create(label="", user=""):
        p = f"new{len(tokens):05d}"
        tokens[p] = {"prefix": p, "user": user, "label": label,
                     "last_used_at": None, "revoked_at": None}
        calls.append(("create", user, label))
        return dict(tokens[p], token="a2a_st_SECRET_" + p, stations=[])

    def allow(sid, p):
        calls.append(("allow", sid, p))
        grants.add((sid, p))
        return True

    def disallow(sid, p):
        calls.append(("disallow", sid, p))
        grants.discard((sid, p))
        return 1

    def never(name):
        return lambda *a, **k: calls.append((name,) + a)

    b.STATIONS.list = lambda: [dict(s) for s in stations.values()]
    b.STATIONS.get = lambda sid: dict(stations[sid]) if sid in stations else None
    b.STATIONS.allowed = lambda sid: {"tokens": [
        dict(tokens[p]) for s, p in grants if s == sid]}
    b.STATIONS.allow = allow
    b.STATIONS.disallow = disallow
    b.TOKENS.list = tlist
    b.TOKENS.create = create
    b.TOKENS.revoke = never("revoke")
    b.TOKENS.delete = never("delete")
    b.AGENTS.list_all = lambda sid=None: [
        {"agent_id": a, "station_id": sid, "owner_prefix": p}
        for a, p in agents.get(sid, [])]
    b.LINKS.of = lambda sid: []

    def run(keys, confirm=True, answers=()):
        t = b._Tui(Scr(keys))
        t.view = "stations"
        t.confirms = []
        t.confirm = lambda p: (t.confirms.append(p), confirm)[1]
        queue = list(answers)
        t.ask = lambda p: queue.pop(0) if queue else ""
        t.shown = []
        real_show = t.show_token
        t.show_token = lambda row, reach=None: (
            t.shown.append((row, reach)), real_show(row, reach))[0]
        t.load()                             # cursor on A
        t.act(ord("t"))
        return t

    def rows(t):
        return [s for s in t.scr.drawn if s[:8] in tokens]

    # --- the list ------------------------------------------------------------
    reset()
    t = run([ESC])
    r = rows(t)
    check("t lists exactly this station's tokens, revoked included",
          {s[:8] for s in r} == {"aaaa1111", "cccc3333"}, str(r))
    check("each row: user, label, status and the agents here that use it",
          any(s.startswith("aaaa1111") and "pelayo" in s and "laptop" in s
              and "used" in s and "2 agents here" in s for s in r)
          and any(s.startswith("cccc3333") and "revoked" in s
                  and "1 agent here" in s for s in r), str(r))
    check("the header names the station and the keys",
          any(s.startswith(" tokens of A") and "n new" in s
              and "r remove from station" in s for s in t.scr.drawn),
          str(t.scr.drawn[:3]))
    check("a closed station says only these tokens act there",
          any("closed: only these tokens act here" in s for s in t.scr.drawn))
    check("esc goes back to the stations tab, reloaded",
          t.view == "stations" and t.rows and t.msg == "tokens of A: 2",
          f"{t.view} {t.msg}")

    stations["id-a"]["open"] = 1
    t = run([ESC])
    check("an open station says every token acts there",
          any("open (*): every token acts here" in s for s in t.scr.drawn))

    # --- n -------------------------------------------------------------------
    reset()
    t = run([ord("n"), ESC], answers=["eve", "phone"])
    made = [c for c in calls if c[0] == "create"]
    newp = next((p for p in tokens if p.startswith("new")), None)
    check("n mints a token with the user and label typed",
          made == [("create", "eve", "phone")], str(calls))
    check("…and grants it into THIS station",
          ("allow", "id-a", newp) in calls, str(calls))
    check("…shows it once, saying where it was granted",
          len(t.shown) == 1 and t.shown[0][1] == "granted into A"
          and any("a2a_st_SECRET_" in s for s in t.scr.drawn),
          str(t.shown))
    check("…and the new row is listed",
          any(s.startswith(newp) for s in t.scr.drawn), str(rows(t)))

    # --- g -------------------------------------------------------------------
    reset()
    t = run([ord("g"), ENTER, ESC])
    picked = [s for s in t.scr.drawn if s.startswith(("aaaa", "bbbb", "cccc",
                                                      "dddd"))
              and "  " in s and "agent" not in s]
    check("g offers only valid tokens that are not already here",
          {s[:8] for s in picked} == {"bbbb2222", "dddd4444"}, str(picked))
    check("…and grants the one picked into this station",
          [c for c in calls if c[0] == "allow"] == [("allow", "id-a",
                                                      "bbbb2222")], str(calls))
    reset()
    t = run([ord("g"), ESC, ESC])
    check("esc in the picker grants nothing",
          not [c for c in calls if c[0] == "allow"], str(calls))

    # --- r -------------------------------------------------------------------
    reset()
    t = run([ord("r"), ESC], confirm=False)
    check("r asks, naming the agents it leaves out",
          t.confirms and "remove aaaa1111 from A?" in t.confirms[0]
          and "2 agent(s) here use it and will be refused" in t.confirms[0],
          str(t.confirms))
    check("…and no keeps the grant", not [c for c in calls
                                         if c[0] == "disallow"]
          and ("id-a", "aaaa1111") in grants, str(calls))
    t = run([ord("r"), ESC], confirm=True)
    check("yes removes it from THIS station only — no revoke, no delete",
          calls == [("disallow", "id-a", "aaaa1111")]
          and ("id-a", "aaaa1111") not in grants, str(calls))
    check("…and says it is still valid elsewhere",
          any("removed from A (still valid elsewhere)" in s
              for s in t.scr.drawn), str(t.scr.drawn[-3:]))
    reset()
    stations["id-a"]["open"] = 1
    t = run([ord("r"), ESC], confirm=False)
    check("on an open station r says the agents keep working while it is open",
          t.confirms and "keep working while the station is open"
          in t.confirms[0], str(t.confirms))

    # --- the stations tab ----------------------------------------------------
    reset()
    h = b._Tui.HELP["stations"]
    check("HELP names t tokens and no longer g/v",
          "t tokens" in h and "grant token" not in h
          and "revoke grant" not in h, h)
    t = b._Tui(Scr([]))
    t.view = "stations"
    t.confirm = lambda p: True
    t.load()
    t.act(ord("g"))
    t.act(ord("v"))
    check("g and v on the stations tab do nothing", calls == [], str(calls))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — a station's tokens are managed from its row")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
