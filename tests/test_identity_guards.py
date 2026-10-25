#!/usr/bin/env python3
"""The broker's identity guards, run for real — with no database.

    python3 tests/test_identity_guards.py

test_author.py and test_client_pin.py drive these through a served broker,
which needs MariaDB. The guards themselves decide without one, so this
imports the real broker module and runs them directly — the functions as
shipped, not copies — with only the station lookup stubbed:

  one agent   the middleware and /stream read ?agent= the SAME way. They
              used to disagree on `?%61gent=victim` and on a repeated key,
              so a stream checked as one agent served another's inbox.
  author      the author is the request's agent; another name, or a value
              that is not a name, is NotYou (a 403).
  client      the client gate refuses another key and a missing key on a
              held agent, with messages that name the fix, and lets the
              holder through.
"""
import importlib.util
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def main() -> int:
    # Refusals log at WARN; with the level above it nothing reaches for the
    # (absent) database at all.
    os.environ["A2A_LOG_LEVEL"] = "ERROR"
    spec = importlib.util.spec_from_file_location("guards_broker", BROKER)
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)
    b.STATIONS.get = lambda s: {"station_id": s, "name": "acme"}

    from starlette.datastructures import QueryParams

    # --- one agent per request, however the query is spelled -----------------
    def middleware_agent(qs: str, header: str = "") -> str:
        scope = {"query_string": qs.encode(),
                 "headers": [(b"x-a2a-agent", header.encode())] if header else []}
        return b._extract_agent(scope)

    for qs in ("agent=me", "%61gent=victim", "agent=me&agent=victim",
               "agent=me&%61gent=victim", "format=json&agent=a%2Fb",
               "agent=x+y"):
        route = b.normalize_agent_id(QueryParams(qs).get("agent") or "")
        check(f"middleware and route agree on ?{qs}",
              middleware_agent(qs, header="mine") == route,
              f"middleware={middleware_agent(qs, header='mine')!r} "
              f"route={route!r}")
    check("with no ?agent= at all, the header names the agent",
          middleware_agent("format=json", header="mine") == "mine")

    # --- the author is the request's agent ----------------------------------
    tok = b._current_agent.set("me")
    try:
        check("no claimed author: it is the request's agent",
              b._author() == "me" and b._author(None) == "me")
        check("claiming yourself is accepted", b._author("me") == "me")
        for claimed in ("other", "OTHER", "  other  ", 5, ["me"]):
            try:
                b._author(claimed)
                check(f"claiming {claimed!r} is refused", False, "accepted")
            except b.NotYou as e:
                check(f"claiming {claimed!r} is refused (NotYou -> 403)",
                      "you are 'me'" in str(e), str(e))
    finally:
        b._current_agent.reset(tok)
    try:
        b._author()
        check("a request naming no agent cannot author anything", False,
              "accepted")
    except b.AuthRequired as e:
        check("a request naming no agent cannot author anything",
              "X-A2A-Agent" in str(e), str(e))

    # --- the client gate ----------------------------------------------------
    held = b.client_hash("a2a_ck_holder")
    check("the broker keeps a hash, never the key",
          held and len(held) == 64 and "holder" not in held, str(held))
    check("and no key hashes to nothing", b.client_hash("") is None)
    b._client_gate("s1", "agent-x", held, held, claim=False)
    check("the holder's own key passes", True)
    for label, key, needle in (
            ("another client's key", b.client_hash("a2a_ck_other"),
             "agent free agent-x --station acme"),
            ("no key at all", None, "run the installer")):
        try:
            b._client_gate("s1", "agent-x", held, key, claim=False)
            check(f"{label} is refused", False, "accepted")
        except b.AgentDenied as e:
            check(f"{label} is refused, and told how to fix it",
                  needle in str(e), str(e))
    b._client_gate("s1", "agent-y", None, None, claim=True)
    check("an agent nobody holds is not refused to a keyless client — and "
          "claiming needs a key, so nothing was attempted", True)
    check("a client-pin refusal carries its own fix, not the register hint",
          b._with_hint("agent 'x' belongs to another client") ==
          "agent 'x' belongs to another client"
          and b.REGISTER_HINT in b._with_hint("unknown agent 'x'"))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — one agent per request, signed by the broker, held by one "
          "client")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
