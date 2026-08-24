#!/usr/bin/env python3
"""What each client actually renders. The test that did not exist.

    python3 tests/test_envelope_contract.py

A wire change broke every channel push in Claude Code for hours, and nothing
caught it: the broker started sending `audience`/`addressed` as JSON ARRAYS,
the Claude client forwarded one verbatim into the notification meta, and Claude
Code silently discards a channel notification whose meta holds a non-string.
DMs carried no array, so pings kept working; the client's own counter counted
emissions, not renders, so every diagnostic said healthy.

No test asserted what a client RENDERS. This one does, for all three, against
their real envelope builders — and it needs no database, so there is no excuse
for it not to run.

Two properties, and they are the whole file:

  flat        every meta value / attribute is a plain string. Never a list,
              never None. This is the exact bug.
  literal     the rendered envelope equals an expected string, so a rename
              that half-lands fails here rather than in production.
"""
import importlib.util
import json
import os
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
CLAUDE = PLUGIN / "a2a" / "server" / "a2a-channel.py"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"
PI = PLUGIN / "pi" / "index.ts"
CODEX = PLUGIN / "codex" / "a2a-codex.py"
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


# One message, as the broker now puts it on the wire: both keys, always, and
# addressed a strict subset of audience.
MSG = {
    "channel": "ops", "sender": "bob", "text": "your turn",
    "ts": 1.0, "id": "m1", "kind": "channel",
    "audience": ["alice", "carol"], "addressed": ["alice"],
    # A deadline, because a client that rendered this as a raw epoch shipped:
    # an agent cannot tell "a day from now" from "the moment it was sent"
    # without doing arithmetic, and one of them said so out loud.
    "expires_at": 1787342315.1517916,
}
# The same post, to the room: addressed empty, and empty MEANS the room.
ROOM = dict(MSG, addressed=[])


def claude_notify(m: dict) -> tuple[str, dict]:
    """Exactly what the Claude client hands to notifications/claude/channel.

    BOTH halves. The meta is where the flat-string bug lived; the content is
    what a human actually reads on the transcript line, because the host puts
    nothing from meta on it.
    """
    spec = importlib.util.spec_from_file_location("a2a_channel", CLAUDE)
    mod = importlib.util.module_from_spec(spec)
    sys.modules["a2a_channel"] = mod
    sys.path.insert(0, str(CLAUDE.parent))
    spec.loader.exec_module(mod)
    captured: list = []
    mod._notify = lambda content, meta: captured.append((content, meta))
    mod._seen.clear()
    mod._emit(json.dumps(m))
    return captured[0] if captured else ("", {})


def claude_meta(m: dict) -> dict:
    """The meta dict alone, for the checks that only care about it."""
    return claude_notify(m)[1]


def codex_envelopes() -> list[str]:
    """Both envelopes from the Codex client's REAL envelope()."""
    spec = importlib.util.spec_from_file_location("a2a_codex", CODEX)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return [mod.envelope(MSG), mod.envelope(ROOM)]


def node_envelopes(path: Path) -> list[str]:
    """Both envelopes from a JS/TS client's REAL envelope().

    The builders are module-private, so the function's source is lifted out
    verbatim and run in a file of the same extension — node strips the types
    for the .ts one. Verbatim matters: a copy of the function would drift from
    the client and pass while the client was broken, which is exactly the
    failure mode this file exists to end.
    """
    src = path.read_text()
    start = src.index("function envelope")
    end = src.index("\n}", start) + 2
    body = src[start:end]
    # `esc` and the Msg type live elsewhere in the client; the harness supplies
    # them so the extracted function compiles on its own.
    prelude = (
        "type Msg = any;\n" if path.suffix == ".ts" else ""
    ) + (
        'const esc = (s) => String(s).replace(/&/g, "&amp;")'
        '.replace(/</g, "&lt;").replace(/>/g, "&gt;")'
        '.replace(/"/g, "&quot;");\n'
        if path.suffix != ".ts" else
        'const esc = (s: any) => String(s).replace(/&/g, "&amp;")'
        '.replace(/</g, "&lt;").replace(/>/g, "&gt;")'
        '.replace(/"/g, "&quot;");\n'
    )
    driver = (
        f'{prelude}{body}\n'
        f'console.log("@@" + JSON.stringify(['
        f'envelope({json.dumps(MSG)}), envelope({json.dumps(ROOM)})]));\n'
    )
    tmp = Path(tempfile.mkdtemp(prefix="a2a-env-")) / f"drive{path.suffix}"
    tmp.write_text(driver)
    res = subprocess.run(["node", str(tmp)], capture_output=True, text=True,
                         timeout=60)
    for line in res.stdout.splitlines():
        if line.startswith("@@"):
            return json.loads(line[2:])
    raise SystemExit(f"{path.name}: no result\n{res.stdout}\n{res.stderr}")


def main() -> int:
    # --- Claude Code: meta values, which is where the bug lived -------------
    meta = claude_meta(MSG)
    check("claude: meta carries both keys",
          "audience" in meta and "addressed" in meta, str(meta))
    check("claude: EVERY meta value is a flat string — a list here is what "
          "made Claude Code discard the whole notification, silently, while "
          "the client counted it as delivered",
          all(isinstance(v, str) for v in meta.values()),
          str({k: type(v).__name__ for k, v in meta.items()}))
    check("claude: audience is the comma-joined audience",
          meta.get("audience") == "alice,carol", str(meta))
    check("claude: addressed is who it is for",
          meta.get("addressed") == "alice", str(meta))
    check("claude: the deadline reaches the session as a readable instant",
          meta.get("expires", "").startswith("2026-08-21T19:58:35"),
          str(meta.get("expires")))
    content, _ = claude_notify(MSG)
    check("claude: the body opens with the sender, because the host renders "
          "only the content on the transcript line and a human reading it "
          "otherwise cannot tell one peer from another",
          content == "‹bob› your turn", repr(content))
    nameless, _ = claude_notify(dict(MSG, sender="", id="m2"))
    check("claude: no sender means no mark — never an empty ‹›",
          nameless == "your turn", repr(nameless))
    room_meta = claude_meta(ROOM)
    check("claude: a post to the room still carries addressed, empty — an "
          "absent key and an empty one are the same to a reader who guesses",
          room_meta.get("addressed") == "" and room_meta.get("audience"),
          str(room_meta))

    # --- OpenCode and Pi: the rendered attribute string ---------------------
    for label, path in (("opencode", OPENCODE), ("pi", PI)):
        named, room = node_envelopes(path)
        check(f"{label}: renders audience and addressed as attributes",
              'audience="alice,carol"' in named
              and 'addressed="alice"' in named, named)
        check(f"{label}: a room post carries an EMPTY addressed, not a "
              f"missing one",
              'addressed=""' in room and 'audience="alice,carol"' in room,
              room)
        check(f"{label}: no stray 'to=' attribute survives the rename",
              ' to="' not in named and ' to="' not in room, named)
        check(f"{label}: the body is still the message text",
              named.endswith("your turn</channel>"), named)
        check(f"{label}: renders the deadline as an ISO instant in UTC",
              'expires="2026-08-21T19:58:35.151Z"' in named, named)

    # --- Codex: the same string, from a third language -----------------------
    named, room = codex_envelopes()
    check("codex: renders audience and addressed as attributes",
          'audience="alice,carol"' in named
          and 'addressed="alice"' in named, named)
    check("codex: a room post carries an EMPTY addressed, not a missing one",
          'addressed=""' in room and 'audience="alice,carol"' in room, room)
    check("codex: no stray 'to=' attribute", ' to="' not in named, named)
    check("codex: the body is still the message text",
          named.endswith("your turn</channel>"), named)
    check("codex: a deadline is a READABLE instant, not a raw epoch",
          'expires="2026-08-21T19:58:35.151Z"' in named, named)
    check("codex renders BYTE-IDENTICALLY to the JS clients — agents quote "
          "each other's messages, so a fourth dialect would be a fourth thing "
          "to learn",
          named == node_envelopes(PI)[0], f"{named!r} vs pi")

    # --- posting returns a receipt, not the message back ---------------------
    # The broker echoes the whole post. Handed to a model that is a copy of the
    # body it just wrote — twice the context, and in a client that renders tool
    # results verbatim it reads like a second message. All four trim it, and a
    # test that only checked one of them would have missed the other three.
    RAW = {"channel": "advisory", "post": {
        "id": "p-1", "channel": "advisory", "sender": "me", "ts": 1.0,
        "text": "x" * 4000, "audience": ["alice", "bob"],
        "addressed": ["alice"], "expires_at": 1787300095.73}}
    raw_json = json.dumps(RAW)

    spec = importlib.util.spec_from_file_location("a2a_codex_r", CODEX)
    cx = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cx)
    trimmed = {"codex": cx._confirm(raw_json)}

    spec = importlib.util.spec_from_file_location("a2a_broker_r", BROKER)
    br = importlib.util.module_from_spec(spec)
    os.environ["A2A_AUTH_DISABLED"] = "1"
    spec.loader.exec_module(br)
    trimmed["broker"] = json.dumps(br._receipt(RAW))

    for label, path in (("opencode", OPENCODE), ("pi", PI)):
        src = path.read_text()
        i = src.index("function receipt")
        body = src[i:src.index("\n}", i) + 2]
        tmp = Path(tempfile.mkdtemp(prefix="a2a-rcpt-")) / f"d{path.suffix}"
        tmp.write_text(body + f"\nconsole.log('@@' + receipt({raw_json}))\n")
        res = subprocess.run(["node", str(tmp)], capture_output=True,
                             text=True, timeout=60)
        got = [l for l in res.stdout.splitlines() if l.startswith("@@")]
        trimmed[label] = got[0][2:] if got else f"ERROR {res.stderr[:200]}"

    for label, got in trimmed.items():
        check(f"{label}: a post returns a RECEIPT — id, room, audience, "
              f"deadline — and never echoes the body back",
              "x" * 100 not in got and '"id"' in got and "p-1" in got,
              got[:120])
        check(f"{label}: the deadline in that receipt is a readable instant",
              "2026-08-21T08:14:55" in got, got[:160])
    shapes = {label: json.loads(got) for label, got in trimmed.items()}
    check("all four agree on the receipt, so an agent sees the same answer "
          "whichever harness it is running in",
          len({json.dumps(v, sort_keys=True) for v in shapes.values()}) == 1,
          str(shapes))

    # --- the vocabulary, everywhere -----------------------------------------
    # Only two words are allowed to name these two concepts. A third would put
    # us straight back where this started.
    for label, path in (("claude", CLAUDE), ("opencode", OPENCODE),
                        ("pi", PI), ("codex", CODEX)):
        src = path.read_text()
        check(f"{label}: no delivered field is called 'to' any more",
              'm.to' not in src and 'm["to"]' not in src
              and 'm.get("to")' not in src, "a 'to' field survives")

    print()
    if fails:
        print(f"{len(fails)} failure(s):")
        for f in fails:
            print(f"  - {f}")
        return 1
    print("PASS — every client renders both fields, flat, and nothing is 'to'")
    return 0


if __name__ == "__main__":
    sys.exit(main())
