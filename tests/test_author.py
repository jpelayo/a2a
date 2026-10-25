#!/usr/bin/env python3
"""Who sent a message is the broker's call — never the caller's word.

    python3 tests/test_author.py

A model in one harness-directory sent messages as another named agent. It
could, because five broker MCP tools took the author as an argument the model
filled in (post_to_channel/broadcast/share_md `sender`, submit_bid and
update_agent `agent_id`), and the REST routes trusted the body the same way —
nothing compared any of it with the agent the request named.

Since 0.5.0 the author of every write is the agent the request names
(X-A2A-Agent). This runs a REAL broker and asserts, on every surface:

  refused     a body naming another author is a 403, and nothing is stored
  derived     a body naming no author is signed as the request's agent
  own only    a card is edited, and a broadcast closed, only by its owner
  targets     fields that name a TARGET — a member to add, a name to
              propose — are not authors and still work
  no args     the MCP tools carry no sender/agent_id argument at all, and a
              post over /mcp/ is signed as the request's agent

Needs a MariaDB (see dbharness.py) and a free port. Auth is disabled, so every
request lands in the `default` station of a throwaway database.
"""
import asyncio
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import dbharness

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"

A, B, C, D = "author-a", "author-b", "author-c", "author-d"
CHAN = "ops"
AUTHOR_TOOLS = ("post_to_channel", "broadcast", "share_md", "submit_bid",
                "update_agent")

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(base: str, method: str, path: str, agent: str,
         body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"{base}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", "X-A2A-Agent": agent})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode()
            return r.status, (json.loads(raw) if raw.strip() else {})
    except urllib.error.HTTPError as e:
        raw = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(raw)
        except ValueError:
            return e.code, {"raw": raw[:200]}


def mcp(base: str, agent: str, name: str, args: dict) -> dict:
    """One tools/call over /mcp/ (stateless: no initialize needed)."""
    req = urllib.request.Request(
        f"{base}/mcp/", method="POST",
        data=json.dumps({"jsonrpc": "2.0", "id": 1, "method": "tools/call",
                         "params": {"name": name, "arguments": args}}).encode(),
        headers={"Content-Type": "application/json",
                 "Accept": "application/json, text/event-stream",
                 "X-A2A-Agent": agent})
    with urllib.request.urlopen(req, timeout=15) as r:
        text = r.read().decode()
    data = [ln[5:].strip() for ln in text.splitlines() if ln.startswith("data:")]
    return json.loads(data[-1] if data else text or "{}")


def transcript(base: str) -> list[tuple[str, str]]:
    """(sender, text) of every post in the channel, read as C."""
    _, out = call(base, "GET", f"/channels/{CHAN}/messages?limit=200", C)
    return [(m.get("sender"), m.get("text") or "")
            for m in out.get("transcript") or []]


def senders(base: str) -> list[str]:
    return [s for s, _ in transcript(base)]


def said(base: str, sender: str, needle: str) -> bool:
    """Did `sender` post something containing `needle`?"""
    return any(s == sender and needle in t for s, t in transcript(base))


def main() -> int:
    dbharness.require_db()
    tmp = Path(tempfile.mkdtemp(prefix="a2a-author-"))
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, **dbharness.db_env(), A2A_AUTH_DISABLED="1",
               A2A_HOST="127.0.0.1", A2A_PORT=str(port))
    for agent in (A, B, C, D):
        subprocess.run([sys.executable, str(BROKER), "agent", "add", agent,
                        "--station", "default"],
                       env=env, capture_output=True, text=True, check=True)
    subprocess.run([sys.executable, str(BROKER), "channel", "create", CHAN,
                    "--station", "default", "--members", f"{A},{B},{C}"],
                   env=env, capture_output=True, text=True, check=True)
    log = tmp / "broker.log"
    proc = subprocess.Popen([sys.executable, str(BROKER), "serve"], env=env,
                            stdout=log.open("a"), stderr=subprocess.STDOUT)
    try:
        for _ in range(150):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError(f"broker did not come up; see {log}")

        # --- channel posts ----------------------------------------------------
        code, out = call(base, "POST", f"/channels/{CHAN}/messages", A,
                         {"sender": B, "text": "impersonated"})
        check("a post that names ANOTHER author is refused (403)…",
              code == 403 and "never as" in json.dumps(out), f"{code} {out}")
        check("…and nothing was stored, under any name",
              not any("impersonated" in t for _, t in transcript(base)),
              str(transcript(base)))
        code, _ = call(base, "POST", f"/channels/{CHAN}/messages", A,
                       {"text": "no sender given"})
        check("a post naming no author is accepted and signed as the request's "
              "agent", code == 200 and said(base, A, "no sender given"),
              f"{code} {transcript(base)}")
        code, _ = call(base, "POST", f"/channels/{CHAN}/messages", A,
                       {"sender": A, "text": "my own name"})
        check("a post naming ITSELF — what older clients send — is accepted",
              code == 200, str(code))

        # --- DMs -------------------------------------------------------------
        code, out = call(base, "POST", "/dms", A,
                         {"sender": B, "to": C, "text": "dm as b"})
        check("a DM naming another author is refused (403)",
              code == 403, f"{code} {out}")
        code, _ = call(base, "POST", "/dms", A, {"to": C, "text": "dm as me"})
        _, inbox = call(base, "GET", "/dms", C)
        got = [(m.get("sender"), m.get("text")) for m in inbox.get("dms") or []]
        check("a DM naming no author arrives signed as the request's agent, and "
              "the refused one never arrived",
              code == 201 and (A, "dm as me") in got
              and not any(t == "dm as b" for _, t in got), f"{code} {got}")

        # --- shared files ----------------------------------------------------
        code, out = call(base, "POST", "/md", A,
                         {"channel": CHAN, "sender": B, "filename": "x.md",
                          "content": "# x"})
        uri = f"md://channel/{CHAN}/x.md"
        missing, _ = call(base, "GET", f"/md?uri={uri}", A)
        check("a shared file naming another author is refused (403), and no "
              "file was stored", code == 403 and missing == 404,
              f"{code} {out} / fetch {missing}")
        code, _ = call(base, "POST", "/md", A,
                       {"channel": CHAN, "filename": "x.md", "content": "# x"})
        _, got = call(base, "GET", f"/md?uri={uri}", A)
        check("a shared file naming no author is stored and announced as the "
              "request's agent",
              code == 201 and got.get("sender") == A and said(base, A, uri),
              f"{code} {got.get('sender')} {transcript(base)[-1:]}")
        call(base, "POST", "/md", B,
             {"channel": CHAN, "filename": "b.md", "content": "# by b"})
        code, out = call(base, "POST", "/md", A,
                         {"channel": CHAN, "filename": "b.md",
                          "content": "# replaced by a"})
        _, got = call(base, "GET", f"/md?uri=md://channel/{CHAN}/b.md", A)
        check("another agent's shared file cannot be replaced — that would put "
              "new words behind a message signed by its author (403)",
              code == 403 and got.get("content") == "# by b"
              and got.get("sender") == B, f"{code} {out} {got.get('content')}")
        code, out = call(base, "POST", "/md", A,
                         {"channel": "no-such-room", "filename": "y.md",
                          "content": "# y"})
        missing, _ = call(base, "GET", "/md?uri=md://channel/no-such-room/y.md",
                          A)
        check("a share to a channel that does not exist stores nothing (404)",
              code == 404 and missing == 404, f"{code} {out} / {missing}")

        # --- broadcasts, bids, closing ---------------------------------------
        code, out = call(base, "POST", "/broadcasts", A,
                         {"problem": "help as b", "sender": B})
        _, listed = call(base, "GET", "/broadcasts", A)
        check("a broadcast naming another author is refused (403), and none "
              "was posted",
              code == 403 and not any(x.get("problem") == "help as b"
                                      for x in listed.get("broadcasts") or []),
              f"{code} {out}")
        code, bc = call(base, "POST", "/broadcasts", A, {"problem": "help"})
        check("a broadcast naming no author is posted as the request's agent",
              code == 201 and bc.get("sender") == A, f"{code} {bc}")
        bid_path = f"/broadcasts/{bc.get('id')}/bids"
        code, out = call(base, "POST", bid_path, B,
                         {"agent_id": C, "bid": "claim"})
        check("a bid naming another bidder is refused (403)",
              code == 403, f"{code} {out}")
        code, _ = call(base, "POST", bid_path, B, {"bid": "claim"})
        _, full = call(base, "GET", f"/broadcasts/{bc.get('id')}", A)
        bidders = [x.get("agent_id") for x in full.get("bids") or []]
        check("a bid naming no bidder is the request's agent's",
              code == 201 and bidders == [B], f"{code} {bidders}")
        code, out = call(base, "POST", f"/broadcasts/{bc.get('id')}/close", B, {})
        check("a broadcast is closed only by whoever posted it (403 for B)",
              code == 403, f"{code} {out}")
        code, out = call(base, "POST", f"/broadcasts/{bc.get('id')}/close", A, {})
        check("and its poster closes it", code == 200
              and out.get("status") == "closed", f"{code} {out}")
        # A broadcast whose poster is gone would otherwise stay open for the
        # whole retention period: nobody left could close it.
        _, orphan = call(base, "POST", "/broadcasts", D, {"problem": "orphan"})
        subprocess.run([sys.executable, str(BROKER), "agent", "rm", D,
                        "--station", "default"],
                       env=env, capture_output=True, text=True, check=True)
        code, out = call(base, "POST",
                         f"/broadcasts/{orphan.get('id')}/close", A, {})
        check("a broadcast whose poster was removed can be closed by anyone in "
              "the station", code == 200 and out.get("status") == "closed",
              f"{code} {out}")
        code, out = call(base, "POST", "/broadcasts", A, {"sender": A})
        check("a broadcast with no problem is a 400, not a 500",
              code == 400, f"{code} {out}")

        # --- cards -----------------------------------------------------------
        code, out = call(base, "PATCH", f"/agents/{B}", A,
                         {"description": "written by a"})
        _, card = call(base, "GET", f"/agents/{B}", A)
        check("one agent cannot edit another's card (403), and the card is "
              "untouched", code == 403
              and card.get("description") != "written by a", f"{code} {out}")
        code, out = call(base, "PATCH", f"/agents/{A}", A,
                         {"description": "my own card"})
        check("but edits its own", code == 200, f"{code} {out}")
        code, out = call(base, "PATCH", f"/agents/{A}", A,
                         {"agent_id": B, "description": "sneaky"})
        check("a body naming another agent is refused even on your own path",
              code == 403, f"{code} {out}")
        code, out = call(base, "POST", f"/channels/{CHAN}/messages", A,
                         {"sender": 5, "text": "x"})
        check("an author that is not a name is a 403, not a 500",
              code == 403, f"{code} {out}")

        # --- targets are not authors -----------------------------------------
        code, out = call(base, "POST", "/channels", A,
                         {"name": "side", "members": []})
        code, out = call(base, "POST", "/channels/side/members", A,
                         {"agent_id": C})
        check("adding ANOTHER agent to a channel still works — agent_id there "
              "is a target, not an author", code in (200, 201), f"{code} {out}")
        code, out = call(base, "POST", "/me/proposals", A,
                         {"agent_id": "asked-for", "note": "x"})
        check("proposing ANOTHER name still works — also a target",
              code in (200, 201), f"{code} {out}")

        # --- the MCP surface -------------------------------------------------
        os.environ.update(dbharness.db_env(), A2A_AUTH_DISABLED="1")
        spec = importlib.util.spec_from_file_location("author_broker", BROKER)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        tools = {t.name: t for t in asyncio.run(mod.mcp.list_tools())}
        leaked = {n: sorted(set((tools[n].inputSchema or {}).get("properties")
                                or {}) & {"sender", "agent_id"})
                  for n in AUTHOR_TOOLS if n in tools}
        check("no MCP write tool takes a sender or agent_id — there is no "
              "argument for a model to fill in with someone else's name",
              len(leaked) == len(AUTHOR_TOOLS)
              and not any(leaked.values()), str(leaked))
        reply = mcp(base, A, "post_to_channel",
                    {"name": CHAN, "message": "over mcp"})
        check("a post over /mcp/ is signed as the request's agent",
              not (reply.get("result") or {}).get("isError")
              and "error" not in reply and said(base, A, "over mcp"),
              f"{reply} {transcript(base)[-1:]}")
        try:
            mcp(base, A, "post_to_channel",
                {"name": CHAN, "message": "mcp as b", "sender": B})
        except urllib.error.HTTPError:
            pass                    # refusing the call outright is fine too
        check("and passing a sender anyway cannot make it anyone else's — "
              "whatever the tool does with it, nothing is stored as B",
              not said(base, B, "mcp as b"), str(transcript(base)[-3:]))
    finally:
        proc.terminate()
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — the broker decides who sent it, on every surface")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
