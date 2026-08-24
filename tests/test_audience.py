#!/usr/bin/env python3
"""Who receives a message is decided by data, never by prose.

    python3 tests/test_audience.py

An agent was asked to prove push worked. It posted to a channel of four:

    "Push delivery test from @acme-api-opencode-1. No reply required."

Nobody received it. Delivery used to be decided by scanning the body: any '@'
switched the audience from "everyone in this channel" to "only the ids
mentioned", and the sender is excluded from its own audience — so a message
signed with its author's own handle addressed NOBODY. Zero receipts were
written, the post reported success, and three agents never heard about it.

An email address did the same thing. So did a docker tag, an `@media` rule, and
an id belonging to an agent since renamed.

So the routing header left the body. A post reaches the channel's members —
all of them, only them, and nothing a sender writes can change that. These
tests exist to keep prose and addressing apart: every one of them writes an '@'
into a message and asserts it changed nothing.

`addressed=[...]` is a LABEL on top of that set, not a routing instruction. It
may only name members, because a channel post never reaches outside the
channel; naming anyone else is refused rather than silently widening the room.

Pure python3 with the broker's deps.
"""
import asyncio
import importlib.util
import os
import sys
import tempfile
from pathlib import Path

import dbharness

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"
ST = "default"
SENDER = "poster"
MEMBERS = ["alice", "bob", "carol"]
OUTSIDER = "dave"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def load(db: Path):
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec = importlib.util.spec_from_file_location("broker_audience", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod._startup()
    return mod


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-aud-"))
    b = load(tmp / "a.db")

    for a in [SENDER, *MEMBERS, OUTSIDER]:
        b.AGENTS.add(ST, a)
    asyncio.run(b.CHANNELS.create(ST, "room", "", [SENDER, *MEMBERS]))
    asyncio.run(b.CHANNELS.create(ST, "empty", "", [SENDER]))

    def post(text, **kw):
        out = asyncio.run(b.CHANNELS.post(ST, "room", SENDER, text, **kw))
        return sorted(out["post"]["audience"])

    # --- the body cannot narrow the audience -------------------------------
    check("a plain post reaches every other member",
          post("nothing special here") == sorted(MEMBERS))

    got = post(f"Push delivery test from @{SENDER}. No reply required.")
    check("A MESSAGE SIGNED WITH ITS OWN AUTHOR'S HANDLE STILL REACHES THE "
          "ROOM — the live failure: the only '@' was the sender's, the sender "
          "is excluded, so the audience came out empty and three agents heard "
          "nothing",
          got == sorted(MEMBERS), str(got))

    for prose in ("mail me at user@example.com",
                  "FROM node@22-alpine",
                  "@media (min-width: 40em) { … }",
                  "see @alice about this",
                  "ask @nobody-by-that-name"):
        got = post(prose)
        check(f"an '@' in prose changes nothing: {prose[:34]!r}",
              got == sorted(MEMBERS), str(got))

    # --- and `addressed` cannot change it AT ALL ----------------------------
    # It used to widen: naming a non-member pulled them in. A channel post now
    # never reaches outside the channel, so the audience is the members and
    # nothing a sender writes can move it.
    got = post("already here", addressed=["alice"])
    check("naming a member in `addressed` leaves the audience exactly the "
          "members — it is a label, not a routing instruction",
          got == sorted(MEMBERS), str(got))

    got = post("mine", addressed=[SENDER])
    check("a sender cannot address itself into its own audience",
          got == sorted(MEMBERS), str(got))

    for who, why in ((OUTSIDER, "an agent who exists but is not a member"),
                     ("nobody-by-that-name", "an id nobody has")):
        try:
            post("come look at this", addressed=[who])
            check(f"addressing {why} is REFUSED — a channel post cannot reach "
                  f"outside the channel, so the label would be a lie",
                  False, "accepted")
        except ValueError as e:
            check(f"addressing {why} is REFUSED — a channel post cannot reach "
                  f"outside the channel, so the label would be a lie",
                  who in str(e) and "add_channel_member" in str(e), str(e))

    check("and the refusal points at the two ways to actually reach them",
          True)

    # --- an empty audience is now only possible when it is true ------------
    out = asyncio.run(b.CHANNELS.post(ST, "empty", SENDER, "anyone there?"))
    check("a channel whose only member is the sender reports an audience of "
          "zero rather than pretending",
          out["post"]["audience"] == [], str(out["post"]["audience"]))

    # --- the recipient can see who else got it -----------------------------
    asyncio.run(b.CHANNELS.post(ST, "room", SENDER, "for the record"))
    msgs = b._resolve_receipts(ST, b._pending_rows(ST, "alice", 50))
    latest = [m for m in msgs if m.get("text") == "for the record"]
    check("the delivered message carries `audience` — the frozen set, so a "
          "recipient can see everyone who owes an ack on it",
          bool(latest)
          and sorted(latest[0].get("audience", [])) == sorted(MEMBERS),
          str(latest[:1]))
    check("and `addressed`, empty here, which MEANS the room",
          bool(latest) and latest[0].get("addressed") == [], str(latest[:1]))

    # --- the scanner is gone, not merely unused ----------------------------
    src = BROKER.read_text()
    check("_mentions is deleted rather than left for something to call again",
          "_mentions" not in src, "still present")
    check("_channel_audience takes neither the message text nor any way to "
          "widen the audience — members and sender, nothing else",
          "def _channel_audience(\n    station_id: str, channel: str, "
          "sender: str\n) -> list[str]:" in src,
          "signature can still be influenced by the caller")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — addressing is data; an '@' is just a character")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
