#!/usr/bin/env python3
"""Proposing a name is asking. Only an operator answers.

    python3 tests/test_proposals.py

Registering an agent is the one step in this system that needs a human, and
it had no path between the two people: the client logged "ask an operator to
run agent add ..." and the id travelled by Slack or luck, while the
operator's screen showed nothing at all.

So a client proposes, an operator approves in the TUI, and a proposal nobody
answers expires and is deleted. The whole security argument is an asymmetry,
and it is what this file mostly tests:

  a station token MAY   propose a name, list its own proposals, withdraw one
  a station token MAY NOT approve anything, ever, by any route

If that second line ever stops being true, this is `create_agent` wearing a
hat — the exact tool removed from agents' reach on purpose.

Pure python3 with the broker's deps; no HTTP server, the registries are
called directly, plus a route audit over the built app.
"""
import asyncio
import importlib.util
import os
import sys
import tempfile
import time
from pathlib import Path

import dbharness

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load(db: Path, **env):
    for k in ("A2A_PROPOSAL_TTL",):
        os.environ.pop(k, None)
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    os.environ.update(env)
    spec = importlib.util.spec_from_file_location(f"prop_{db.stem}", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return mod


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-prop-"))
    b = load(tmp / "p.db")

    sid = b.STATIONS.create("acme")["station_id"]
    t1 = b.TOKENS.create(user="alice", label="laptop")
    t2 = b.TOKENS.create(user="bob", label="desktop")
    h1, h2 = b.TOKENS._hash_of(t1["prefix"]), b.TOKENS._hash_of(t2["prefix"])

    # --- the default -------------------------------------------------------
    check("a proposal lasts 48h by default — long enough to survive a "
          "weekend, short enough that a dead name does not sit there forever",
          abs(b.PROPOSAL_TTL - 48 * 3600) < 1, str(b.PROPOSAL_TTL))

    # --- proposing ---------------------------------------------------------
    out = b.PROPOSALS.propose(sid, "acme-api-pi-1", h1, note="pi peer")
    check("a client can ask for a name",
          out["agent_id"] == "acme-api-pi-1"
          and out["status"] == "pending approval", str(out))
    check("and it creates NO agent — asking is not creating",
          not b.AGENTS.list_all(sid), str(b.AGENTS.list_all(sid)))
    check("the operator sees it",
          [p["agent_id"] for p in b.PROPOSALS.list(sid)] == ["acme-api-pi-1"],
          str(b.PROPOSALS.list(sid)))
    check("with the token that asked, so approving is not a blind act",
          b.PROPOSALS.list(sid)[0]["owner_prefix"] == t1["prefix"],
          str(b.PROPOSALS.list(sid)[0]))

    # --- collisions --------------------------------------------------------
    try:
        b.PROPOSALS.propose(sid, "acme-api-pi-1", h2)
        check("another client cannot hijack a live proposal", False,
              "accepted")
    except ValueError:
        check("another client cannot hijack a live proposal", True)
    again = b.PROPOSALS.propose(sid, "acme-api-pi-1", h1)
    check("but re-proposing your own name refreshes it rather than erroring "
          "— a client that restarts should not be punished for it",
          again["expires_at"] >= out["expires_at"], str(again))
    check("and does not duplicate the row", len(b.PROPOSALS.list(sid)) == 1,
          str(b.PROPOSALS.list(sid)))

    b.AGENTS.add(sid, "already-here")
    try:
        b.PROPOSALS.propose(sid, "already-here", h2)
        check("a name that already exists is refused", False, "accepted")
    except ValueError:
        check("a name that already exists is refused", True)

    # --- the flood cap -----------------------------------------------------
    for i in range(b.MAX_PENDING_PROPOSALS):
        try:
            b.PROPOSALS.propose(sid, f"flood-{i}", h2)
        except ValueError:
            break
    try:
        b.PROPOSALS.propose(sid, "flood-last", h2)
        check("a client in a restart loop cannot fill the operator's screen",
              False, f"{len(b.PROPOSALS.list(sid))} pending")
    except ValueError:
        check("a client in a restart loop cannot fill the operator's screen",
              True)

    # --- approval ----------------------------------------------------------
    ok = b.PROPOSALS.approve(sid, "acme-api-pi-1")
    ids = [a["agent_id"] for a in b.AGENTS.list_all(sid)]
    check("approving mints the agent", "acme-api-pi-1" in ids, str(ids))
    row = [a for a in b.AGENTS.list_all(sid)
           if a["agent_id"] == "acme-api-pi-1"][0]
    check("bound to the token that asked for it, so the name belongs to "
          "whoever wanted it",
          row["owner_token_hash"] == h1, str(row.get("owner_prefix")))
    check("and the request is gone — a name is never both an agent and a "
          "pending ask",
          "acme-api-pi-1" not in
          [p["agent_id"] for p in b.PROPOSALS.list(sid)],
          str(b.PROPOSALS.list(sid)))
    check("approving twice is refused, not a second agent",
          _raises(lambda: b.PROPOSALS.approve(sid, "acme-api-pi-1")),
          "accepted")

    # --- rejection and withdrawal -----------------------------------------
    # A fresh token: the flood test above deliberately left h2 at its cap.
    t3 = b.TOKENS.create(user="dave")
    h3a = b.TOKENS._hash_of(t3["prefix"])
    b.PROPOSALS.propose(sid, "doomed", h3a)
    b.PROPOSALS.reject(sid, "doomed")
    check("rejecting deletes and creates nothing",
          "doomed" not in [p["agent_id"] for p in b.PROPOSALS.list(sid)]
          and "doomed" not in [a["agent_id"] for a in b.AGENTS.list_all(sid)],
          "still present")

    b.PROPOSALS.propose(sid, "mine", h1)
    check("a client cannot withdraw somebody else's request",
          _raises(lambda: b.PROPOSALS.withdraw(sid, "mine", h3a),
                  PermissionError), "allowed")
    b.PROPOSALS.withdraw(sid, "mine", h1)
    check("but can withdraw its own",
          "mine" not in [p["agent_id"] for p in b.PROPOSALS.list(sid)])

    # --- expiry, swept by the one thing that deletes -----------------------
    b2 = load(tmp / "fast.db", A2A_PROPOSAL_TTL="1s")
    sid2 = b2.STATIONS.create("acme")["station_id"]
    tok = b2.TOKENS.create(user="carol")
    h3 = b2.TOKENS._hash_of(tok["prefix"])
    check("A2A_PROPOSAL_TTL overrides the default through the same duration "
          "parser the message expiry uses",
          abs(b2.PROPOSAL_TTL - 1) < 0.01, str(b2.PROPOSAL_TTL))
    b2.PROPOSALS.propose(sid2, "ephemeral", h3)
    time.sleep(1.2)
    check("an expired proposal stops being pending the moment it expires, "
          "before anything sweeps it",
          not b2.PROPOSALS.list(sid2), str(b2.PROPOSALS.list(sid2)))
    check("and can no longer be approved — the deadline is real",
          _raises(lambda: b2.PROPOSALS.approve(sid2, "ephemeral"), KeyError),
          "approved after expiry")
    st = b2.collect(sid2)
    check("collect() sweeps it and counts it — the one place that deletes",
          st.get("proposals_expired", 0) == 1, str(st))
    left = b2.CONN.execute("SELECT COUNT(*) c FROM agent_proposals").fetchone()
    check("the row is really gone", left["c"] == 0, str(left["c"]))
    check("and no agent was ever created for it",
          not b2.AGENTS.list_all(sid2), str(b2.AGENTS.list_all(sid2)))

    # --- the asymmetry, asserted over the real route table -----------------
    app = b.build_app()
    paths = {
        getattr(r, "path", ""): set(getattr(r, "methods", None) or [])
        for r in app.routes
    }
    check("a station token can propose over /me/proposals — reachable while "
          "unregistered, which is the whole point",
          "POST" in paths.get("/me/proposals", set()), str(paths.keys()))
    approve_routes = [
        p for p in paths
        if any(w in p.lower() for w in ("approve", "reject"))
    ]
    check("NO route approves or rejects anything — approval is an operator "
          "act, on the TUI or CLI, straight on the database. A route here "
          "would make this create_agent with extra steps",
          not approve_routes, str(approve_routes))

    # /me/* must stay exempt from the unknown-agent denial or an unregistered
    # client could never ask for anything.
    src = BROKER.read_text()
    check("/me/* stays exempt from the unknown-agent denial, which is what "
          "lets an unregistered client propose at all",
          'path == "/me" or path.startswith("/me/")' in src,
          "the realm exemption moved")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — clients ask, operators answer, unanswered names expire")
    return 1 if fails else 0


def _raises(fn, exc=Exception) -> bool:
    try:
        fn()
    except exc:
        return True
    except Exception:
        return False
    return False


if __name__ == "__main__":
    sys.exit(main())
