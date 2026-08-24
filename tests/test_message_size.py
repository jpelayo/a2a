#!/usr/bin/env python3
"""A message has a ceiling, and the sender is told what it is.

    python3 tests/test_message_size.py

There was no limit anywhere, which does not mean messages could be any size —
it means the limit was whichever hop failed first, and every one of them fails
in a way the sender cannot act on:

  * a reverse proxy it cannot see answers 413 (nginx defaults to 1 MiB),
  * MariaDB rejects a packet larger than max_allowed_packet,
  * or — the common case — the write succeeds and the message cannot fit in the
    context of the agent it was delivered to. Storage would take 16 MiB per
    row; nothing that reads one could.

So the ceiling is set by the reader, not the database, and the refusal has to
name the size, the limit and the way round it, because the sender is a model
that will otherwise retry the identical body.

The size checks run with no database. The registry checks need one, via
dbharness — they are what proves the limit is enforced where BOTH surfaces go
through it, rather than in one tool.
"""
import asyncio
import importlib.util
import os
import sys
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
    for k in ("A2A_MAX_MESSAGE_SIZE", "A2A_MAX_MD_SIZE"):
        os.environ.pop(k, None)
    os.environ["A2A_AUTH_DISABLED"] = "1"
    os.environ.update(env)
    spec = importlib.util.spec_from_file_location("size_broker", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def pure(b) -> None:
    """Everything that needs no database — the parser and the check itself."""
    check("64 KiB by default: ~16k tokens, so one message still fits in the "
          "context of whatever receives it, and ten of them fit in a "
          "read_channel",
          b.MAX_MESSAGE_SIZE == 65536, str(b.MAX_MESSAGE_SIZE))
    check("and md files get 512 KiB — bulk is meant to go this way, and the "
          "binding constraint there is the upload, not the reader",
          b.MAX_MD_SIZE == 524288, str(b.MAX_MD_SIZE))
    check("both stay under nginx's 1 MiB default body size, so the broker's "
          "answer is the one the sender sees rather than a proxy's 413",
          b.MAX_MESSAGE_SIZE < 1048576 and b.MAX_MD_SIZE < 1048576)
    check("and far under MEDIUMTEXT, which is the point: the database is not "
          "the tight hop and sizing to it would help nobody",
          b.MAX_MESSAGE_SIZE < 16777215)

    for spelling, want in (("65536", 65536), ("64k", 65536), ("512K", 524288),
                           ("4M", 4194304), (131072, 131072)):
        check(f"parse_size accepts {spelling!r}",
              b.parse_size(spelling) == want, str(b.parse_size(spelling)))
    check("powers of two, not ten — every limit it is compared against is",
          b.parse_size("1k") == 1024)
    check("'' means unset, so a caller can fall back to its default",
          b.parse_size("") is None)
    check("nonsense raises rather than reading as 0, which would refuse every "
          "message in the station",
          "not a size" in refused(b.parse_size, "soon"))
    check("and so does zero, for the same reason",
          "must be positive" in refused(b.parse_size, "0"))

    # --- the check itself ---------------------------------------------------
    check("a body exactly at the limit is accepted — an off-by-one here is a "
          "message somebody cannot send for no reason",
          b.check_size("x" * 65536, b.MAX_MESSAGE_SIZE) is None)
    msg = refused(b.check_size, "x" * 65537, b.MAX_MESSAGE_SIZE, "this post")
    check("one byte over is refused", bool(msg), "accepted")
    check("and the refusal names the actual size and the ceiling, so the "
          "sender knows how much to cut",
          "65537" in msg and "65536" in msg, msg)
    check("and points at share_md, because a model told only 'too large' "
          "retries the same body",
          "share_md" in msg and "Do not retry" in msg, msg)

    # BYTES, not characters. 32769 two-byte characters is 65538 bytes: over the
    # limit while being well under it by any character count.
    check("the limit counts UTF-8 bytes, which is what MEDIUMTEXT and "
          "max_allowed_packet count — a cap in characters could not be "
          "checked against either",
          bool(refused(b.check_size, "é" * 32769, b.MAX_MESSAGE_SIZE)))
    check("so the same body one character shorter is fine",
          b.check_size("é" * 32768, b.MAX_MESSAGE_SIZE) is None)

    over = refused(b.check_size, "x" * (b.MAX_MD_SIZE + 1), b.MAX_MD_SIZE,
                   "notes.md")
    check("an oversized md file is refused too, named after the file",
          "notes.md" in over, over)


def with_db(b) -> None:
    """The registry path — one enforcement point, both surfaces."""
    b._startup()
    sid = b.STATIONS.create("sizes")["station_id"]
    b.AGENTS.add(sid, "sender")
    b.AGENTS.add(sid, "peer")
    asyncio.run(b.CHANNELS.create(sid, "room", "sizing", ["sender", "peer"]))

    big = "x" * (b.MAX_MESSAGE_SIZE + 1)
    fits = "y" * b.MAX_MESSAGE_SIZE

    msg = refused(lambda: asyncio.run(
        b.CHANNELS.post(sid, "room", "sender", big)))
    check("a channel post over the ceiling is refused", bool(msg), "accepted")
    check("and nothing was written — the check runs before the transcript, so "
          "an oversized post cannot leave a row nobody can read back",
          not [m for m in asyncio.run(
              b.CHANNELS.messages_since(sid, "room", limit=50))
              if len(m["text"]) > b.MAX_MESSAGE_SIZE])
    out = asyncio.run(b.CHANNELS.post(sid, "room", "sender", fits))
    check("a post exactly at the ceiling goes through and gets receipts, so "
          "the limit is a limit and not a margin",
          bool(out.get("id")), str(out)[:200])

    check("a DM is capped the same way — it is the same read on the other end",
          bool(refused(lambda: asyncio.run(
              b.DIRECT.send(sid, "sender", "peer", big)))))
    check("so is a broadcast problem statement, which fans out to every "
          "candidate at once",
          bool(refused(lambda: asyncio.run(
              b.BROADCASTS.create(sid, big, "sender", None, None)))))

    check("and md takes what a message may not, which is the whole reason it "
          "is the route for bulk",
          asyncio.run(b.CHANNELS.post(
              sid, "room", "sender",
              "x" * b.MAX_MESSAGE_SIZE)) is not None
          and b.MAX_MD_SIZE > b.MAX_MESSAGE_SIZE)


def main() -> int:
    b = load()
    pure(b)

    print()
    try:
        dbharness.require_db()
    except SystemExit:
        print("(registry checks skipped: no database)\n")
    else:
        os.environ.update(dbharness.db_env())
        with_db(load())

    print()
    if fails:
        print(f"{len(fails)} failure(s):")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("PASS — the ceiling is the reader's, and the refusal says so")
    return 0


if __name__ == "__main__":
    sys.exit(main())
