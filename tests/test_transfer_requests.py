#!/usr/bin/env python3
"""Claiming a name somebody already holds is a request, not a refusal.

    python3 tests/test_transfer_requests.py

Before this, an agent that claimed a taken name hit a wall with no door in it:
resolve_request_station raised "bound to another token" so every request 403'd
forever, and propose() refused outright with "ask an operator to bind it to
you" — advice the client could not act on, about a person who could not see
that anybody had asked.

Now it becomes a TRANSFER request in the same queue as a name claim, and an
operator answers it. What this file pins:

  kind is DERIVED   a request is a transfer exactly when the name already
                    exists. Nothing stores it, so nothing can disagree.
  approve moves it  ownership changes; channels and unacked messages do not,
                    because they are keyed by agent_id. The old token is then
                    refused, which is the point.
  deny LOCKS        a refused transfer bars that token for A2A_TRANSFER_LOCKTIME.
                    "No" has to outlast the next reconnect or it means nothing.
  a claim does not  a rejected claim is usually a typo; it stays free to retry.
  unclaimed is free an existing agent nobody owns needs no operator at all.

Registries called directly, like test_proposals.py. Needs a MariaDB/MySQL via
dbharness.
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


def refused(fn, *a, **kw) -> str:
    """Run it, expecting a ValueError, and hand back the message."""
    try:
        fn(*a, **kw)
        return ""
    except ValueError as e:
        return str(e)


def load(**env):
    for k in ("A2A_PROPOSAL_TTL", "A2A_TRANSFER_LOCKTIME"):
        os.environ.pop(k, None)
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    os.environ.update(env)
    spec = importlib.util.spec_from_file_location("xfer_broker", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return mod


def main() -> int:
    tempfile.mkdtemp(prefix="a2a-xfer-")
    b = load()

    sid = b.STATIONS.create("acme")["station_id"]
    t1 = b.TOKENS.create(user="alice", label="old-laptop")
    t2 = b.TOKENS.create(user="bob", label="new-laptop")
    h1 = b.TOKENS._hash_of(t1["prefix"])
    h2 = b.TOKENS._hash_of(t2["prefix"])

    check("the lock is 24h by default — long enough that a refusal outlasts "
          "a reconnect loop, short enough to expire without an operator",
          abs(b.TRANSFER_LOCKTIME - 24 * 3600) < 1, str(b.TRANSFER_LOCKTIME))

    # --- an existing but UNCLAIMED name needs nobody ------------------------
    b.AGENTS.add(sid, "free-name")
    msg = refused(b.PROPOSALS.propose, sid, "free-name", h2)
    check("an unowned agent is not a transfer: the broker binds it to the "
          "first token that uses it, so asking would only add a step",
          "unclaimed" in msg, msg or "accepted")

    # --- a name this token already owns -------------------------------------
    b.AGENTS.add(sid, "mine", owner_token_hash=h2)
    msg = refused(b.PROPOSALS.propose, sid, "mine", h2)
    check("and asking for a name you already hold is refused as a no-op",
          "already yours" in msg, msg or "accepted")

    # --- the real thing -----------------------------------------------------
    b.AGENTS.add(sid, "acme-api", owner_token_hash=h1)
    out = b.PROPOSALS.propose(sid, "acme-api", h2, note="laptop replaced")
    check("claiming a name another token holds opens a TRANSFER request "
          "rather than dying with a 403 nobody can act on",
          out["kind"] == "transfer", str(out))

    row = [p for p in b.PROPOSALS.list(sid) if p["agent_id"] == "acme-api"][0]
    check("the operator's row says it is a transfer, and nothing stored it — "
          "it is derived from the name existing, so it cannot go stale",
          row["kind"] == "transfer", str(row))
    check("and names the token that holds it now, which is what approving "
          "would take the name away from",
          row["current_owner_prefix"] == t1["prefix"], str(row))
    check("while still naming the token that asked",
          row["owner_prefix"] == t2["prefix"], str(row))

    # --- denial locks -------------------------------------------------------
    rej = b.PROPOSALS.reject(sid, "acme-api")
    check("denying it reports the kind and a lock deadline",
          rej["kind"] == "transfer" and rej["locked_until"], str(rej))
    left = b.PROPOSALS.lock_left(sid, "acme-api", h2)
    check("the asker is barred for the locktime — a refusal that let the "
          "next reconnect ask again would not be a refusal",
          23 * 3600 < left <= 24 * 3600, str(left))
    msg = refused(b.PROPOSALS.propose, sid, "acme-api", h2)
    check("and asking again says so, with the time left",
          "denied" in msg and "unlock" in msg, msg or "accepted")

    check("a DIFFERENT token is not locked out by someone else's refusal",
          b.PROPOSALS.lock_left(sid, "acme-api",
                                b.TOKENS._hash_of(
                                    b.TOKENS.create(user="carol")["prefix"])
                                ) == 0)

    # --- the operator's undo ------------------------------------------------
    locks = b.PROPOSALS.locks(sid, "acme-api")
    check("the operator can see who is locked out before lifting it",
          len(locks) == 1 and locks[0]["token_prefix"] == t2["prefix"],
          str(locks))
    check("and lifting it reports what it cleared",
          b.PROPOSALS.unlock(sid, "acme-api") == 1)
    check("after which the same client may ask again — an accidental keypress "
          "must not wedge a legitimate transfer for a day",
          b.PROPOSALS.propose(sid, "acme-api", h2)["kind"] == "transfer")

    # --- approving moves ownership and nothing else -------------------------
    # ChannelRegistry.create/get are async (they do ACL work before touching
    # the database), so they are driven rather than called.
    asyncio.run(b.CHANNELS.create(sid, "advisory", "advice", ["acme-api"]))
    n_before = len(b.AGENTS.list_all(sid))
    before = b.AGENTS.get(sid, "acme-api")
    ok = b.PROPOSALS.approve(sid, "acme-api")
    after = b.AGENTS.get(sid, "acme-api")
    check("approving a transfer rebinds rather than creating a second agent",
          ok["kind"] == "transfer"
          and len(b.AGENTS.list_all(sid)) == n_before, str(ok))
    check("the name now answers to the token that asked",
          after["owner_token_hash"] == h2, str(after))
    check("and it is taken from the one that held it",
          ok["taken_from"] == h1, str(ok))
    check("the agent itself is the same row — a transfer is an ownership "
          "change, not a new agent",
          before["created_at"] == after["created_at"], str(after))
    ch = asyncio.run(b.CHANNELS.get(sid, "advisory"))
    check("so it keeps its channel memberships: receipts are keyed by "
          "agent_id and would be delivered to nobody if the name moved",
          "acme-api" in (ch or {}).get("members", []), str(ch))
    check("the request is gone from the operator's queue",
          not [p for p in b.PROPOSALS.list(sid) if p["agent_id"] == "acme-api"])
    check("and winning the argument clears any lock left from losing it "
          "earlier, or the new owner could never ask about its own name",
          b.PROPOSALS.lock_left(sid, "acme-api", h2) == 0)

    # --- a denied CLAIM is not locked ---------------------------------------
    b.PROPOSALS.propose(sid, "brand-new", h1)
    rej = b.PROPOSALS.reject(sid, "brand-new")
    check("a rejected CLAIM reports itself as one and sets no lock — it is "
          "usually a typo, and the agent should be free to fix it at once",
          rej["kind"] == "claim" and not rej["locked_until"], str(rej))
    check("so the same client may propose again immediately",
          b.PROPOSALS.propose(sid, "brand-new", h1)["kind"] == "claim")

    # --- withdrawing is not a denial ----------------------------------------
    b.PROPOSALS.propose(sid, "mine-later", h1)
    b.AGENTS.add(sid, "mine-later-taken", owner_token_hash=h2)
    b.PROPOSALS.propose(sid, "mine-later-taken", h1)
    b.PROPOSALS.withdraw(sid, "mine-later-taken", h1)
    check("withdrawing your OWN transfer request does not lock you out — "
          "changing your mind is not being refused",
          b.PROPOSALS.lock_left(sid, "mine-later-taken", h1) == 0)

    # --- housekeeping -------------------------------------------------------
    b.PROPOSALS.propose(sid, "mine-later-taken", h1)
    b.PROPOSALS.reject(sid, "mine-later-taken")
    n = b.PROPOSALS.sweep_denials(sid, now=time.time() + 25 * 3600)
    check("collect() drops locks whose time is up, so the table does not "
          "accumulate rows for names nobody asks about any more", n == 1,
          str(n))

    print()
    if fails:
        print(f"{len(fails)} failure(s):")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("PASS — a taken name is a question an operator can answer")
    return 0


if __name__ == "__main__":
    dbharness.require_db()
    sys.exit(main())
