#!/usr/bin/env python3
"""A DM crosses stations only over a link an operator made — both ways.

    python3 tests/test_station_links.py

Needs a MariaDB (see dbharness.py) and a free port.

A link joins two stations. While it exists an agent on either side may
send_dm(to="bob@B"): the DM is written into B as an ordinary B DM, signed
"alice@A", and Bob answers "alice@A". This runs the REAL broker — the registry
in-process, then `serve` over HTTP with a real token — and asserts:

  closed      no link → refused, the same words whether or not the station
              exists, and nothing written anywhere
  across      linked → the DM lands in B signed alice@A, with a receipt for
              bob, and nothing is kept in A; the reply comes back to A
  only        a link limited to named agents refuses the others, either side;
              a list emptied by agents leaving means NOBODY, never everyone
  checked     `only` names real agents of the two stations, or is refused
  follows     rename rewrites `only` and the alice@A the DMs were signed with;
              remove and move drop the agent; deleting a station drops its
              links
  local       an agent whose name has an "@" in it is still local first
  realm       /me and my_realm show the links — never the other roster
  rest        POST /dms takes the same path, under a real token
"""
import asyncio
import importlib.util
import json
import os
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

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def call(base: str, method: str, path: str, token: str, agent: str,
         body: dict | None = None) -> tuple[int, dict]:
    req = urllib.request.Request(
        f"{base}{path}", method=method,
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json",
                 "Authorization": f"Bearer {token}", "X-A2A-Agent": agent})
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


def main() -> int:
    dbharness.require_db()
    env = dbharness.db_env()
    os.environ.update(env)
    os.environ.pop("A2A_AUTH_DISABLED", None)
    spec = importlib.util.spec_from_file_location("links_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b._startup()

    A = b.STATIONS.create("lnk-a")["station_id"]
    B = b.STATIONS.create("lnk-b")["station_id"]
    C = b.STATIONS.create("lnk-c")["station_id"]
    for sid, aid in ((A, "alice"), (A, "carol"), (A, "frank"),
                     (B, "bob"), (B, "dave"), (C, "eve")):
        b.AGENTS.add(sid, aid)

    def send(sid, sender, to, text="hi"):
        return asyncio.run(b.DIRECT.send(sid, sender, to, text))

    def refused(fn) -> str:
        try:
            fn()
        except (KeyError, ValueError) as e:
            return str(e)
        return ""

    def dms(sid) -> list[tuple]:
        return [(r["sender"], r["recipient"], r["text"]) for r in b.CONN.execute(
            "SELECT sender, recipient, text FROM dms WHERE station_id = %s "
            "ORDER BY ts", (sid,)).fetchall()]

    # --- closed ---------------------------------------------------------------
    why = refused(lambda: send(A, "alice", "bob@lnk-b"))
    check("no link: a DM to bob@lnk-b is refused", "no link from lnk-a to lnk-b"
          in why, why)
    nowhere = refused(lambda: send(A, "alice", "bob@nowhere"))
    check("…in the same words for a station that does not exist — an unlinked "
          "station is none of the sender's business",
          "no link from lnk-a to nowhere" in nowhere
          and nowhere.replace("nowhere", "lnk-b") == why, f"{nowhere!r} {why!r}")
    check("…and nothing was written in either station",
          dms(A) == [] and dms(B) == [], f"{dms(A)} {dms(B)}")
    check("a plain unknown name is refused as before",
          "is not in this station" in refused(lambda: send(A, "alice", "zed")))

    # --- across ---------------------------------------------------------------
    out = b.LINKS.link("lnk-a", "lnk-b")
    check("link A B: everyone", out == {"stations": ["lnk-a", "lnk-b"],
                                        "only": None}, str(out))
    res = send(A, "alice", "bob@lnk-b", "hello bob")
    check("linked: the DM lands in B, signed alice@lnk-a, for bob",
          dms(B) == [("alice@lnk-a", "bob", "hello bob")], str(dms(B)))
    check("…nothing is kept in A", dms(A) == [], str(dms(A)))
    check("…the sender is told where it went",
          res.get("delivered_to") == "bob@lnk-b"
          and res["dm"].get("station") == "lnk-b", str(res))
    pending = b._pending_rows(B, "bob", 50)
    check("…bob has a receipt for it in B", len(pending) == 1
          and pending[0]["kind"] == "dm", str(pending))
    got, _ = b._fetch_for_agent(B, "bob", 50)
    check("…and his stream delivers it with sender alice@lnk-a, so the reply "
          "address is in the envelope",
          [m.get("sender") for m in got] == ["alice@lnk-a"], str(got))
    send(B, "bob", "alice@lnk-a", "hi alice")
    check("the reply bob → alice@lnk-a lands in A — a link works both ways",
          dms(A) == [("bob@lnk-b", "alice", "hi alice")], str(dms(A)))
    why = refused(lambda: send(A, "alice", "nobody@lnk-b"))
    check("a name that is not in B is not reachable there",
          "'nobody' is not reachable in lnk-b" in why, why)
    why = refused(lambda: send(A, "alice", "eve@lnk-c"))
    check("a link to B opens nothing towards C", "no link from lnk-a to lnk-c"
          in why, why)

    # --- only -----------------------------------------------------------------
    out = b.LINKS.link("lnk-a", "lnk-b", "alice@lnk-a,bob@lnk-b")
    check("linking again replaces only", out["only"] == ["alice@lnk-a",
                                                         "bob@lnk-b"], str(out))
    why = refused(lambda: send(A, "carol", "bob@lnk-b"))
    check("only: an unlisted sender is refused", "you are not on the link to "
          "lnk-b" in why, why)
    why = refused(lambda: send(A, "alice", "dave@lnk-b"))
    check("only: an unlisted recipient is not reachable",
          "'dave' is not reachable in lnk-b" in why, why)
    why = refused(lambda: send(B, "dave", "alice@lnk-a"))
    check("only applies the other way too", "you are not on the link to lnk-a"
          in why, why)
    n = len(dms(B))
    send(A, "alice", "bob@lnk-b", "listed")
    check("only: listed sender to listed recipient goes through",
          len(dms(B)) == n + 1, str(dms(B)))

    # --- checked --------------------------------------------------------------
    for label, only, kind in (
            ("an unknown agent", "ghost@lnk-a", "not found"),
            ("an agent of a third station", "eve@lnk-c", "must be"),
            ("a name with no station", "alice", "name@station"),
            ("an empty list", " , ", "names nobody")):
        why = refused(lambda: b.LINKS.link("lnk-a", "lnk-b", only))
        check(f"only refuses {label}", kind in why, why)
    check("a station cannot be linked to itself",
          "itself" in refused(lambda: b.LINKS.link("lnk-a", "lnk-a")))
    check("…and a refused change left the link as it was",
          b.LINKS.between(A, B)["only"] == [[A, "alice"], [B, "bob"]],
          str(b.LINKS.between(A, B)))

    # --- follows: rename --------------------------------------------------------
    b.AGENTS.rename(A, "alice", "alicia")
    check("rename: only follows the agent",
          b.LINKS.between(A, B)["only"] == [[A, "alicia"], [B, "bob"]],
          str(b.LINKS.between(A, B)))
    check("rename: the DMs it signed in B now say alicia@lnk-a — a reply to "
          "the old string would reach nobody",
          all(s != "alice@lnk-a" for s, _, _ in dms(B))
          and ("alicia@lnk-a", "bob", "hello bob") in dms(B), str(dms(B)))
    n = len(dms(A))
    send(B, "bob", "alicia@lnk-a", "after the rename")
    check("…and bob's reply to the new name lands", len(dms(A)) == n + 1)

    # --- follows: remove → nobody -------------------------------------------------
    b.AGENTS.remove("bob", B)
    check("remove: its entry is dropped",
          b.LINKS.between(A, B)["only"] == [[A, "alicia"]],
          str(b.LINKS.between(A, B)))
    b.AGENTS.remove("alicia", A)
    check("a list emptied by agents leaving is [] — nobody, NOT everyone",
          b.LINKS.between(A, B)["only"] == [], str(b.LINKS.between(A, B)))
    why = refused(lambda: send(A, "carol", "dave@lnk-b"))
    check("…and it refuses everyone", "you are not on the link" in why, why)
    check("…and the station shows it",
          [ln["only_names"] for ln in b.LINKS.of(A)
           if ln["station_id"] == B] == [[]], str(b.LINKS.of(A)))

    # --- follows: move ------------------------------------------------------------
    b.LINKS.link("lnk-a", "lnk-c", "carol@lnk-a,eve@lnk-c")
    b.AGENTS.move("carol", "lnk-b", "lnk-a")
    check("move: the agent is dropped from the links of the station it left",
          b.LINKS.between(A, C)["only"] == [[C, "eve"]],
          str(b.LINKS.between(A, C)))

    # --- local first --------------------------------------------------------------
    b.LINKS.link("lnk-a", "lnk-b")
    b.AGENTS.add(A, "x@lnk-b")
    send(A, "frank", "x@lnk-b", "to the local one")
    check("an agent literally named x@lnk-b is local — a literal match always "
          "wins over a link", ("frank", "x@lnk-b", "to the local one") in dms(A)
          and all(t != "to the local one" for _, _, t in dms(B)), str(dms(A)))
    send(A, "frank", "frank@lnk-a", "own station named")
    check("name@<this station> is the local name",
          ("frank", "frank", "own station named") in dms(A), str(dms(A)))

    # --- delete -------------------------------------------------------------------
    b.STATIONS.delete("lnk-c")
    check("deleting a station drops its links",
          [ln["name"] for ln in b.LINKS.of(A)] == ["lnk-b"], str(b.LINKS.of(A)))

    # --- realm ----------------------------------------------------------------------
    tok = b.TOKENS.create(user="links-test")
    b.STATIONS.allow("lnk-a", tok["token"])
    b.STATIONS.allow("lnk-b", tok["token"])
    auth = {"stations": [A, B], "token_hash": b._hash_token(tok["token"]),
            "user": "links-test"}
    view = b.AGENTS.realm_view(auth, "frank")
    check("realm: the agent sees its station's links",
          view.get("links") == [{"station": "lnk-b", "send_as": "name@lnk-b",
                                 "may_use": True, "reach": "everyone"}],
          str(view.get("links")))
    b.LINKS.link("lnk-a", "lnk-b", "frank@lnk-a,dave@lnk-b")
    view = b.AGENTS.realm_view(auth, "frank")
    check("realm: with only, it sees whom it reaches there — the listed names",
          view["links"][0]["reach"] == ["dave@lnk-b"]
          and view["links"][0]["may_use"] is True, str(view["links"]))
    b.AGENTS.add(A, "gina")
    view = b.AGENTS.realm_view(auth, "gina")
    check("realm: an agent not on the link is told so",
          view["links"][0]["may_use"] is False, str(view["links"]))

    # --- rest -------------------------------------------------------------------------
    b.LINKS.link("lnk-a", "lnk-b")
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    log = Path(tempfile.mkdtemp(prefix="a2a-links-")) / "broker.log"
    proc = subprocess.Popen(
        [sys.executable, str(BROKER), "serve"],
        env=dict(os.environ, **env, A2A_HOST="127.0.0.1", A2A_PORT=str(port)),
        stdout=log.open("a"), stderr=subprocess.STDOUT)
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=1)
                break
            except Exception:
                time.sleep(0.2)
        code, out = call(base, "POST", "/dms", tok["token"], "frank",
                         {"to": "dave@lnk-b", "text": "over rest"})
        check("rest: POST /dms to dave@lnk-b is accepted", code == 201,
              f"{code} {out}")
        check("…and lands in B signed frank@lnk-a",
              ("frank@lnk-a", "dave", "over rest") in dms(B), str(dms(B)))
        code, out = call(base, "POST", "/dms", tok["token"], "frank",
                         {"to": "eve@lnk-c", "text": "x"})
        check("rest: no link is a 404 that says so",
              code == 404 and "no link" in (out.get("error") or ""),
              f"{code} {out}")
        code, out = call(base, "GET", "/me", tok["token"], "frank")
        check("rest: /me lists the links",
              [ln.get("station") for ln in out.get("links") or []] == ["lnk-b"],
              f"{code} {out.get('links')}")
        check("…and no agent of the other station",
              "dave" not in json.dumps(out.get("links")), str(out.get("links")))
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — a DM crosses stations only over a link, and both ways")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
