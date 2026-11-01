#!/usr/bin/env python3
"""The admin TUI shows what each client's own doctor last found.

    python3 tests/test_tui_client_state.py

A client's a2a_channel_status ends by reporting its finding to the broker
(agent_client_state, one row per agent). On the agents tab:

  - a row whose finding has a flag (failing checks, a client older than the
    broker) carries the first one: `working · ⚠ doctor fails: stream`;
  - a row with no finding, or a clean one, carries nothing extra;
  - ⏎ on an agent row shows the whole report in the message line;
  - HELP names the key.

Drives the real _Tui with a fake screen and stubbed registries, so it needs
no database. What the broker stores is tests/test_client_state.py (MariaDB).
"""
import curses
import importlib.util
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "deploy" / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


class Scr:
    def __init__(self):
        self.drawn: list[str] = []

    def timeout(self, ms):
        pass

    def getch(self):
        return 27

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
    spec = importlib.util.spec_from_file_location("tui_state_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)               # importing opens no database
    now = time.time()
    findings = {
        ("id-a", "sick"): {"station_id": "id-a", "agent_id": "sick",
                           "harness": "hermes", "version": b.VERSION,
                           "reported_at": now - 3600, "switch": True,
                           "route": "tui", "receives": True,
                           "settings": "own", "failing": ["stream"],
                           "fixed": ["stream"]},
        ("id-a", "fine"): {"station_id": "id-a", "agent_id": "fine",
                           "harness": "codex", "version": b.VERSION,
                           "reported_at": now - 60, "switch": True,
                           "route": "project", "receives": True,
                           "settings": None, "failing": [], "fixed": []},
        ("id-a", "behind"): {"station_id": "id-a", "agent_id": "behind",
                             "harness": "pi", "version": "0.5.0",
                             "reported_at": now - 60, "switch": True,
                             "route": "project", "receives": True,
                             "settings": None, "failing": [], "fixed": []},
    }
    b.CLIENT_STATE.all = lambda sid=None: list(findings.values())
    b.CLIENT_STATE.get = lambda sid, aid: findings.get((sid, aid))
    b.AGENTS.list_all = lambda sid=None: [
        {"agent_id": a, "station_id": "id-a", "station_name": "A",
         "owner_token_hash": "h", "owner_prefix": "p1"}
        for a in ("behind", "fine", "never", "sick")]
    b.PROPOSALS.list = lambda **k: []
    b.TOKENS.list = lambda include_revoked=False: []

    t = b._Tui(Scr())
    t.view = "agents"
    t.load()
    status = {r["agent_id"]: r["status"] for r in t.rows}
    check("a failing finding rides on its row as the first flag",
          status["sick"] == "working · ⚠ doctor fails: stream",
          status["sick"])
    check("a client older than the broker is flagged too",
          status["behind"] == f"working · ⚠ client v0.5.0, broker "
                              f"v{b.VERSION}", status["behind"])
    check("a clean finding, or none at all, adds nothing",
          status["fine"] == "working" and status["never"] == "working",
          f"{status['fine']} / {status['never']}")

    t.sel["agents"] = [r["agent_id"] for r in t.rows].index("sick")
    t.act(10)                                       # ⏎
    check("⏎ shows the whole report: who, how long ago, what failed, what "
          "it fixed",
          t.msg.startswith("sick: last doctor run 1h ago")
          and "hermes" in t.msg and "doctor fails: stream" in t.msg
          and "fixed: stream" in t.msg, t.msg)
    t.sel["agents"] = [r["agent_id"] for r in t.rows].index("never")
    t.act(10)
    check("…and says so when no doctor has run there",
          t.msg == "never: no doctor run recorded", t.msg)
    check("HELP names the key", "⏎ client state" in b._Tui.HELP["agents"],
          b._Tui.HELP["agents"])

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — each client's last doctor finding is one keystroke away")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
