#!/usr/bin/env python3
"""The OpenCode plugin's contract with OPENCODE — checked against its source.

    OPENCODE_SRC=/path/to/opencode python3 tests/test_opencode_contract.py
    OPENCODE_V1_SRC=/path/to/opencode-at-v1.19.4 ...    (optional: the V1 half)

test_project_gate.py runs the plugin against a fake V2 ctx and a fake V1
client, and fakes can only ever agree with themselves: if OpenCode renames
session.prompt, or stops honouring codemode:false, the fakes keep the old
names and the suite stays green while every session loses a2a. This file
reads a checkout instead — as text, so nothing needs installing — and pins
each thing the plugin relies on from both ends: that OpenCode still provides
it, and that the plugin still uses it.

Written against anomalyco/opencode `dev` on 2026-10-22 (V2, @opencode/plugin
2.1.3) for the V2 half, and tags v1.19.4 / v1.18.28 for the V1 half.

SKIPs (exit 2) when OPENCODE_SRC does not point at a V2 checkout: nothing has
been checked, and saying so is the only honest outcome.
"""
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLIENT = HERE.parent / "plugin" / "opencode" / "a2a-opencode.js"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def read(src: Path, rel: str) -> str:
    p = src / rel
    return p.read_text() if p.is_file() else ""


def between(text: str, start: str, end: str) -> str:
    """The text from `start` to the next `end` after it, or ''."""
    i = text.find(start)
    if i < 0:
        return ""
    j = text.find(end, i + len(start))
    return text[i:j if j >= 0 else len(text)]


def v2(src: Path, plugin: str) -> None:
    module = read(src, "packages/core/src/plugin/module.ts")
    adapter = read(src, "packages/plugin/src/promise/adapter.ts")
    protocol = read(src, "packages/protocol/src/groups/session.ts")
    session = read(src, "packages/schema/src/session.ts")
    tool = read(src, "packages/core/src/tool.ts")
    prompt = read(src, "packages/core/src/session/prompt.ts")
    status = read(src, "packages/schema/src/session-status-event.ts")
    events = read(src, "packages/schema/src/session-event.ts")
    envelope = read(src, "packages/schema/src/event.ts")
    setup = between(plugin, "async function setup(ctx)", "\nexport default")

    # --- loading ---------------------------------------------------------------
    check("V2 loads only a module whose default is {id, setup | effect}",
          "setup: Schema.declare" in module
          and "Plugin must export a default definition" in module,
          "packages/core/src/plugin/module.ts changed")
    check("…and the plugin's one export is that default, with both entry "
          "points",
          'export default { id: "a2a", server, setup }' in plugin
          and len(re.findall(r"^export ", plugin, re.M)) == 1,
          "the export line changed, or a second export appeared")

    # --- the context it uses ---------------------------------------------------
    for name, provided, used in (
        ("ctx.location", "location: host.location", "ctx.location?.directory"),
        ("ctx.session.list", 'SessionEndpoints["session.list"]',
         "ctx.session.list("),
        ("ctx.session.prompt", 'SessionEndpoints["session.prompt"]',
         "ctx.session.prompt("),
        ("ctx.event.subscribe", "subscribe: (options)", "ctx.event.subscribe("),
        ("ctx.tool.transform → editor.add", "add: (tool",
         "editor.add("),
    ):
        check(f"V2's promise context provides {name}, and the plugin uses it",
              provided in adapter and used in setup,
              f"provided={provided in adapter} used={used in setup}")

    # --- prompting -------------------------------------------------------------
    block = between(protocol, '"session.prompt", "/api/session/:sessionID/prompt"',
                    'identifier: "session.prompt"')
    check("session.prompt takes `resume` (false = admit without a turn, V1's "
          "noReply) and `delivery`, and the plugin passes both",
          "resume: Schema.Boolean" in block
          and "delivery: SessionInbox.Delivery" in block
          and "resume: !noReply" in setup and 'delivery: "steer"' in setup,
          "packages/protocol/src/groups/session.ts changed, or the plugin did")
    check("a prompt's delivery defaults to steer — the plugin passes the same "
          "value explicitly, so the brief and the catch-up share a turn",
          'delivery ?? "steer"' in prompt, "packages/core/src/session/prompt.ts")
    check("a prompt waits for plugin activation — so setup() must not await "
          "one: the plugin starts its pump detached and setup awaits no prompt",
          "Plugin.awaitActivation" in prompt
          and "if (ENABLED) void pump()" in plugin
          and "await ctx.session.prompt" not in setup,
          "an awaited prompt in setup would deadlock activation")

    # --- choosing the session --------------------------------------------------
    check("sessions carry location.directory (not directory), and the list "
          "filters by directory and parentID — the plugin asks for roots in "
          "its own directory and renames the field",
          "location: Location.Ref" in session
          and "directory: AbsolutePath.pipe(Schema.optional)" in session
          and "Use null to return only root sessions" in session
          and "parentID: null" in setup and "s?.location?.directory" in setup,
          "packages/schema/src/session.ts changed, or the plugin did")

    # --- tools -----------------------------------------------------------------
    check("only tools with codemode === false reach the model directly; every "
          "other one is behind Code Mode's `execute` — the plugin opts out",
          "tool.options?.codemode === false" in tool
          and "options: { codemode: false }" in setup,
          "packages/core/src/tool.ts changed, or the plugin did")

    # --- events ----------------------------------------------------------------
    check("events are {type, data, location}, and the plugin reads data and "
          "location",
          "location: optional(Location.Ref)" in envelope
          and re.search(r"^\s+data,\s*$", envelope, re.M) is not None
          and "ev?.data" in setup and "ev?.location?.directory" in setup,
          "packages/schema/src/event.ts changed, or the plugin did")
    check("the wake events still exist: session.status (idle), session.idle, "
          "session.created with sessionID/parentID/location",
          'type: "session.status"' in status and 'type: "session.idle"' in status
          and 'type: "session.created"' in events
          and "sessionID: SessionID" in events
          and "parentID: SessionID.pipe(optional)" in events,
          "packages/schema/src/session-*.ts changed")

    # --- the instructions, in the system prompt ---------------------------------
    # Added 2026-10-26 against `dev`: the plugin no longer posts its
    # instructions into the conversation; the session "context" hook puts them
    # in each model request's system prompt as a SystemPart {type: "text"}.
    hooks = read(src, "packages/plugin/src/promise/session.ts")
    messages = read(src, "packages/ai/src/schema/messages.ts")
    check("sessions expose hook(\"context\", …) whose input carries a mutable "
          "system: Array<SystemPart> — and the plugin registers it and pushes "
          "into it",
          re.search(r"\bcontext\??\s*:\s*SessionContext", hooks) is not None
          and re.search(r"system\s*:\s*Array<SystemPart>", hooks) is not None
          and 'ctx.session.hook("context"' in setup
          and "input.system.push(" in setup,
          "packages/plugin/src/promise/session.ts changed, or the plugin did")
    check("a SystemPart is {type: \"text\", text} — what the plugin pushes",
          'Schema.Literal("text")' in between(messages, "systemPartSchema", "})")
          and '{ type: "text", text }' in setup,
          "packages/ai/src/schema/messages.ts changed, or the plugin did")


def v1(src: Path, plugin: str) -> None:
    index = read(src, "packages/opencode/src/plugin/index.ts")
    shared = read(src, "packages/opencode/src/plugin/shared.ts")
    check("V1 reads a default object first, and calls its server()",
          'readV1Plugin(load.mod, load.spec, "server", "detect")' in index
          and ".server(input" in index,
          "packages/opencode/src/plugin/index.ts changed")
    check("…a path plugin's default must carry an id, and this one does",
          "must export id" in shared and 'id: "a2a"' in plugin,
          "packages/opencode/src/plugin/shared.ts changed")
    check("…server and tui together are refused; the plugin has no tui",
          "not both" in shared and "tui" not in plugin.split(
              "export default", 1)[1],
          "")
    hooks = read(src, "packages/plugin/src/index.ts")
    server = between(plugin, "async function server(", "\nasync function setup(")
    check("V1's experimental.chat.system.transform hands output.system: "
          "string[] — and the plugin returns it and pushes the instructions",
          '"experimental.chat.system.transform"' in hooks
          and re.search(r"system\s*:\s*string\[\]", hooks) is not None
          and '"experimental.chat.system.transform"' in server
          and "output.system.push(" in server,
          "packages/plugin/src/index.ts changed, or the plugin did")


def main() -> int:
    plugin = CLIENT.read_text()
    src = Path(os.environ.get("OPENCODE_SRC") or "")
    if not str(src) or not (src / "packages/core/src/plugin/module.ts").is_file():
        print("SKIP: set OPENCODE_SRC to an OpenCode V2 checkout "
              "(packages/core/src/plugin/module.ts not found)")
        return 2
    v2(src, plugin)

    old = Path(os.environ.get("OPENCODE_V1_SRC") or "")
    if str(old) and (old / "packages/opencode/src/plugin/index.ts").is_file():
        v1(old, plugin)
    else:
        print("(V1 half skipped: set OPENCODE_V1_SRC to a v1.x checkout)")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — every OpenCode name the plugin relies on is still there")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
