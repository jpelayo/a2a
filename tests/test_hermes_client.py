#!/usr/bin/env python3
"""The Hermes bot plugin, without Hermes and without a database.

    python3 tests/test_hermes_client.py

The real plugin file is imported and register()ed against a fake PluginContext
— the surface it uses: register_tool, register_hook, on_unload,
get_config/set_config, profile_name, inject_message, and the manager's
has_gateway_message_injector — with Hermes's session variables stubbed as a
ContextVar, as gateway.session_context keeps them. The broker is the counting
FakeBroker from test_project_gate.py, which replays what is unacked on every
new connection, as the real one does.

What is pinned, and why each matters:

  silence     no route, no gateway, or no id → ZERO broker requests. The
              plugin loads into the CLI, TUI and cron processes of the bot
              too, and delivery is a destructive read.
  one owner   a2a is pinned to the chat it is asked for in, by the user who
              asked; nobody else in that group can use the tools or move it.
  lifecycle   a message is acked only once a turn has SHOWN it — never on
              inject_message's True, which means scheduled, not run.
  recovery    refused → re-offered; accepted but never seen → the stream is
              reopened and the broker sends it again; a failed ack retries on
              its own clock.
  isolation   two profiles in one process share nothing.

Contract with Hermes itself — that these names and signatures exist — is
tests/test_hermes_contract.py, against a Hermes checkout.
"""
import contextvars
import importlib.util
import json
import os
import re
import sys
import tempfile
import threading
import time
import types
from pathlib import Path

sys.dont_write_bytecode = True        # nothing lands in plugin/hermes/

HERE = Path(__file__).resolve().parent
PLUGIN = HERE.parent / "plugin"
CLIENT = PLUGIN / "hermes" / "__init__.py"
MANIFEST = PLUGIN / "hermes" / "plugin.yaml"
CODEX = PLUGIN / "codex" / "a2a-codex.py"

sys.path.insert(0, str(HERE))
from test_project_gate import FakeBroker  # noqa: E402

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def settle():
    """Let a stream handler orphaned by the last reconnect die on its next
    keepalive write. Once the broker goes quiet it writes nothing, and a
    survivor would take the next live line meant for the live connection."""
    time.sleep(0.6)


def until(cond, secs=5.0):
    end = time.time() + secs
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.03)
    return bool(cond())


# --- Hermes, stubbed -----------------------------------------------------------
# A gateway turn binds its session as ContextVars; so does this stub, so a
# turn simulated in one thread is invisible to every other — as in Hermes.
_TURN: contextvars.ContextVar = contextvars.ContextVar("turn", default={})
_HOME: contextvars.ContextVar = contextvars.ContextVar("home", default="")
VAR = {"HERMES_SESSION_PLATFORM": "platform",
       "HERMES_SESSION_CHAT_ID": "chat_id",
       "HERMES_SESSION_CHAT_TYPE": "chat_type",
       "HERMES_SESSION_THREAD_ID": "thread_id",
       "HERMES_SESSION_USER_ID": "user_id",
       "HERMES_SESSION_SCOPE_ID": "scope_id"}

gateway = types.ModuleType("gateway")
session_context = types.ModuleType("gateway.session_context")
session_context.get_session_env = (
    lambda name, default="": _TURN.get().get(VAR.get(name, name)) or default)
session_context.session_is_messaging_surface = (
    lambda: _TURN.get().get("platform") not in (None, "", "cli", "tui"))
gateway.session_context = session_context
hermes_constants = types.ModuleType("hermes_constants")
hermes_constants.get_hermes_home = lambda: _HOME.get()
sys.modules.update({"gateway": gateway,
                    "gateway.session_context": session_context,
                    "hermes_constants": hermes_constants})


def in_turn(chat: dict, fn, *a, **kw):
    """Run fn as part of a gateway turn in `chat`."""
    def inner():
        _TURN.set(dict(chat))
        return fn(*a, **kw)
    return contextvars.copy_context().run(inner)


class Manager:
    def __init__(self, gateway=True):
        self.has_gateway_message_injector = gateway


class Ctx:
    """The PluginContext surface the plugin uses, and nothing else."""

    def __init__(self, profile, gateway=True, config=None, allowed=True):
        self.profile_name = profile
        self._manager = Manager(gateway)
        self.allowed = allowed
        self.tools: dict = {}
        self.hooks: dict = {}
        self.unload: list = []
        self.config: dict = dict(config or {})
        self.injected: list[tuple[str, dict]] = []
        self.inject_result = True
        self.on_inject = None

    def register_tool(self, name, toolset, schema, handler, check_fn=None,
                      requires_env=None, is_async=False, description="",
                      emoji="", override=False):
        self.tools[name] = {"schema": schema, "handler": handler,
                            "check_fn": check_fn, "toolset": toolset}

    def register_hook(self, name, cb):
        self.hooks.setdefault(name, []).append(cb)

    def on_unload(self, cb):
        self.unload.append(cb)

    def get_config(self, key, default=None):
        return json.loads(json.dumps(self.config.get(key, default)))

    def set_config(self, key, value):
        self.config[key] = json.loads(json.dumps(value))

    def inject_message(self, content, role="user", *, session_key=None,
                       origin=None):
        self.injected.append((content, dict(origin or {})))
        if self.on_inject:
            self.on_inject(content, dict(origin or {}))
        return self.inject_result

    def _gateway_injection_allowed(self):
        return self.allowed

    # -- driving it, as Hermes would --
    def call(self, tool, chat, args=None):
        out = in_turn(chat, self.tools[tool]["handler"], args or {})
        try:
            return json.loads(out)
        except (TypeError, ValueError):
            return out

    def turn(self, chat, session_id, user_message="", first=False):
        """A turn in `chat`: what every pre_llm_call hook returns."""
        return [in_turn(chat, cb, session_id=session_id, task_id="t",
                        turn_id="u", user_message=user_message,
                        conversation_history=[], is_first_turn=first,
                        model="m", platform=chat.get("platform"),
                        parent_session_id=None,
                        sender_id=chat.get("user_id"))
                for cb in self.hooks.get("pre_llm_call", [])]

    def ids_injected(self):
        return [m for c, _ in self.injected
                for m in re.findall(r'\sid="([^"]+)"', c)]


_n = 0


def load(url, token="t"):
    """A fresh copy of the plugin module, as Hermes imports one per profile."""
    global _n
    _n += 1
    os.environ["A2A_URL"], os.environ["A2A_TOKEN"] = url, token
    spec = importlib.util.spec_from_file_location(f"hermes_a2a_{_n}", CLIENT)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    mod.GATE_POLL_S = 0.05
    mod.RECONNECT_S = 0.1
    mod.UNREGISTERED_S = 0.2
    mod.ACK_RETRY_MIN_S = 0.05
    mod.ACK_RETRY_MAX_S = 0.2
    return mod


def start(mod, ctx):
    """register(ctx) the way Hermes calls it: under the profile's home."""
    home = tempfile.mkdtemp(prefix="a2a-hermes-home-")

    def inner():
        _HOME.set(home)
        return mod.register(ctx)
    return contextvars.copy_context().run(inner)


def stop(ctx):
    for cb in ctx.unload:
        cb()


GROUP = {"platform": "x", "chat_id": "-100", "chat_type": "group",
         "thread_id": "", "user_id": "alice", "scope_id": ""}
ALICE = GROUP
BOB = dict(GROUP, user_id="bob")
TOPIC = dict(GROUP, thread_id="7")
ELSEWHERE = dict(GROUP, chat_id="-200")
CLI = {"platform": "cli", "chat_id": "", "chat_type": "", "thread_id": "",
       "user_id": "alice", "scope_id": ""}
ON = {"enabled": True, "route": GROUP,
      "owner": {"platform": "x", "user_id": "alice"}}


def msg(i, text="hello"):
    return {"id": i, "channel": "ops", "sender": "peer", "text": text,
            "audience": ["bot"], "addressed": []}


def main() -> int:
    codex_src = CODEX.read_text()
    codex_tools = re.findall(r'^    \("([a-z_][a-z0-9_]*)",',
                             codex_src[codex_src.index("TOOLS = ["):], re.M)
    manifest_tools = re.findall(
        r"^  - ([a-z0-9_]+)$",
        MANIFEST.read_text().split("provides_tools:", 1)[1]
        .split("provides_hooks:", 1)[0], re.M)

    # --- registration -------------------------------------------------------
    fb = FakeBroker()
    mod = load(fb.url)
    ctx = Ctx("bot")
    start(mod, ctx)
    check("registers the shared vocabulary — the same tools as Codex, so an "
          "agent need not know which harness answered",
          sorted(ctx.tools) == sorted(codex_tools),
          f"only hermes: {sorted(set(ctx.tools) - set(codex_tools))}; "
          f"missing: {sorted(set(codex_tools) - set(ctx.tools))}")
    check("and plugin.yaml declares exactly what is registered",
          sorted(manifest_tools) == sorted(ctx.tools), str(manifest_tools))
    check("hooks pre_llm_call (acks and the brief) and registers an "
          "on_unload", "pre_llm_call" in ctx.hooks and ctx.unload,
          f"{ctx.hooks} {ctx.unload}")
    check("no tool carries a check_fn — Hermes caches availability per "
          "profile, so hiding per chat could hide the route chat's tools; "
          "the handler is the gate",
          all(t["check_fn"] is None for t in ctx.tools.values()), "")
    time.sleep(0.5)
    check("SILENT with no route: zero broker requests — the plugin loads in "
          "every chat and process of the bot, and nobody has asked",
          fb.calls == [], str(fb.calls))
    st = ctx.call("a2a_channel_status", ALICE)
    check("asked what is wrong, it says a2a is off first, and leaves the "
          "decision to the user",
          "a2a is off" in (st.get("next_step") or "")
          and "enable_a2a_here" in st["next_step"]
          and "user" in st["next_step"], str(st.get("next_step")))
    refused = ctx.call("list_channels", ALICE)
    check("while off, the tools refuse — no route, no owner",
          "a2a is off" in str(refused.get("error")), str(refused))
    os.environ["A2A_CODEX_SOCK"] = ""
    # Codex's identity store and log, kept out of the real ~/.codex.
    os.environ["CODEX_HOME"] = tempfile.mkdtemp(prefix="a2a-hermes-cx-")
    spec = importlib.util.spec_from_file_location("hermes_vs_codex", CODEX)
    cx = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cx)
    cx.api = lambda m, p, body=None, timeout=30: (
        {"agent": "x", "stations": ["s"], "registered": True} if p == "/me"
        else {"channels": []})
    cx_status = json.loads(cx._status({}))
    check("a2a_channel_status answers with the same top-level keys as "
          "Codex's, so an agent reading it need not know its harness",
          sorted(st) == sorted(cx_status),
          f"hermes {sorted(st)} vs codex {sorted(cx_status)}")
    check("and the push block carries the fields that say whether delivery "
          "is alive",
          {"enabled", "stream_connected", "stale", "last_error",
           "delivered_this_session"} <= set(st["push"]), str(st["push"]))
    stop(ctx)
    fb.close()

    # --- no credentials -----------------------------------------------------
    mod0 = load("", token="")
    c0 = Ctx("bot")
    start(mod0, c0)
    check("no url or token: ONLY a2a_channel_status is registered, no hook, "
          "no thread", list(c0.tools) == ["a2a_channel_status"]
          and not c0.hooks and not c0.unload, str(list(c0.tools)))
    st = c0.call("a2a_channel_status", ALICE)
    check("and it says what is missing and how to get it",
          "no broker url or token" in (st.get("next_step") or "")
          and "reinstall" in st["next_step"], str(st.get("next_step")))

    # --- no gateway, generic profile, injection not allowed -----------------
    for label, ctxkw in (
            ("a route but NO gateway in this process (CLI, TUI, cron)",
             dict(profile="bot", gateway=False)),
            ("a route but the gateway may not inject for this plugin",
             dict(profile="bot", allowed=False)),
            ("the DEFAULT profile, whose name is no agent id",
             dict(profile="default"))):
        fb = FakeBroker()
        m = load(fb.url)
        c = Ctx(config=ON, **ctxkw)
        start(m, c)
        time.sleep(0.5)
        check(f"{label} → zero broker requests, the stream never claimed",
              fb.calls == [], str(fb.calls))
        st = c.call("a2a_channel_status", ALICE)
        step = st.get("next_step") or ""
        # The default profile's fix is the installer, which asks for an id.
        want = {"profile": "allow_gateway_injection",
                "default": "/install | python3"}
        key = ("default" if ctxkw.get("profile") == "default"
               else "profile" if ctxkw.get("allowed") is False else None)
        if key:
            check(f"  and status names the fix ({want[key]})",
                  want[key] in step, step)
        else:
            check("  and status says this process is not the gateway",
                  "not the bot's gateway" in step, step)
        stop(c)
        fb.close()

    # --- pinning and the owner ------------------------------------------------
    fb = FakeBroker()
    mod = load(fb.url)
    ctx = Ctx("bot")
    start(mod, ctx)
    out = ctx.call("enable_a2a_here", CLI, {"enabled": True})
    check("enabling from the CLI is refused — a2a runs in a messaging chat",
          "error" in out and not ctx.config.get("enabled"), str(out))
    out = ctx.call("enable_a2a_here", dict(GROUP, user_id=""),
                   {"enabled": True})
    check("enabling from a turn with no user id is refused — nobody could "
          "own it", "error" in out and not ctx.config.get("enabled"), str(out))
    out = ctx.call("enable_a2a_here", ALICE, {"enabled": True})
    check("alice says 'enable a2a here' in the group: on, and the route is "
          "EXACTLY that turn's session", out.get("enabled") is True
          and ctx.config.get("route") == GROUP
          and ctx.config.get("enabled") is True, str(ctx.config))
    check("and alice is its owner",
          ctx.config.get("owner") == {"platform": "x", "user_id": "alice"},
          str(ctx.config.get("owner")))
    check("it connects at once, no restart",
          until(lambda: any(c == "/stream" for c in fb.calls), 3),
          str(fb.calls))

    check("two chats are one route only if chat AND thread match",
          mod.same_chat(GROUP, dict(GROUP, user_id="bob", chat_type="x"))
          and not mod.same_chat(GROUP, TOPIC)
          and not mod.same_chat(GROUP, ELSEWHERE), "")

    before = len(fb.calls)
    gated = sorted(set(ctx.tools) - mod.UNGATED)
    bob_out = {t: ctx.call(t, BOB, {"name": "ops", "text": "x", "to": "y",
                                    "ids": ["z"], "uri": "md://x",
                                    "new_id": "evil", "agent_id": "y",
                                    "broadcast_id": "b", "bid": "claim",
                                    "channel": "ops", "filename": "a.md",
                                    "content": "c"})
               for t in gated}
    check("bob, IN THE SAME GROUP, is refused by every a2a tool — he cannot "
          "drive this bot's a2a identity",
          all("only the user who turned a2a on" in str(v.get("error"))
              for v in bob_out.values()),
          str({k: v for k, v in bob_out.items()
               if "only the user" not in str(v.get("error"))}))
    check("and none of his calls reached the broker",
          fb.calls[before:] == [], str(fb.calls[before:]))
    for label, chat in (("another thread of the same group", TOPIC),
                        ("another chat", ELSEWHERE)):
        out = ctx.call("list_channels", chat)
        check(f"alice herself is refused in {label}",
              "error" in out, str(out))
    out = ctx.call("list_channels", ALICE)
    check("alice, in the route chat, gets through",
          isinstance(out, dict) and "error" not in out, str(out))
    out = ctx.call("enable_a2a_here", BOB, {"enabled": False})
    check("bob cannot turn it off", "error" in out
          and ctx.config.get("enabled") is True, str(out))
    out = ctx.call("enable_a2a_here", dict(ELSEWHERE, user_id="bob"),
                   {"enabled": True})
    check("nor move it to his own chat", "error" in out
          and ctx.config.get("route") == GROUP, str(out))

    # --- lifecycle: reserved → scheduled → observed → acked ------------------
    until(lambda: mod_state(ctx_inst(ctx))["connected"], 3)
    fb.live.put(msg("m1", "first"))
    check("a message is injected into the route chat, as that chat's turn",
          until(lambda: "m1" in ctx.ids_injected())
          and ctx.injected[-1][1].get("chat_id") == "-100"
          and ctx.injected[-1][1].get("user_id") == "alice",
          str(ctx.injected[-1:]))
    time.sleep(0.4)
    check("NOT acked on inject_message's True — scheduled is not run",
          "m1" not in fb.acked, str(fb.acked))
    out = ctx.call("enable_a2a_here", dict(ELSEWHERE), {"enabled": True})
    check("moving the route is refused while a message is still settling",
          "settling" in str(out.get("error")) and ctx.config["route"] == GROUP,
          str(out))
    out = ctx.call("enable_a2a_here", ALICE, {"enabled": False})
    check("and so is turning it off", "settling" in str(out.get("error"))
          and ctx.config["enabled"] is True, str(out))
    content = ctx.injected[-1][0]
    ctx.turn(dict(BOB, user_id="alice", chat_id="-999"), "s-other", content)
    time.sleep(0.3)
    check("a turn in ANOTHER chat quoting the envelope does not count",
          "m1" not in fb.acked, str(fb.acked))
    ctx.turn(GROUP, "s1", content)
    check("the turn that carries it is seen, and the message acked",
          until(lambda: "m1" in fb.acked), str(fb.acked))
    check("  as the id it arrived under",
          ("/ack", "bot") in fb.agents, str(fb.agents[-4:]))

    # --- the race: the turn starts before inject_message returns -----------
    inst = ctx_inst(ctx)
    ctx.on_inject = lambda content, origin: ctx.turn(origin, "s-race",
                                                    content)
    fb.live.put(msg("m2", "racing"))
    check("a turn that fires pre_llm_call BEFORE inject_message returns is "
          "still seen and acked — the id was reserved first",
          until(lambda: "m2" in fb.acked), str(fb.acked))
    ctx.on_inject = None

    # --- refused injection ------------------------------------------------------
    ctx.inject_result = False
    streams = fb.streams
    fb.live.put(msg("m3", "refused"))
    check("a refused injection releases the reservation and acks nothing",
          until(lambda: "m3" in ctx.ids_injected())
          and until(lambda: "m3" not in inst.deliveries)
          and "m3" not in fb.acked, str(inst.deliveries))
    check("the stream is reopened, so the broker offers it again",
          until(lambda: fb.streams > streams)
          and until(lambda: ctx.ids_injected().count("m3") >= 2),
          f"streams {streams}→{fb.streams}")
    ctx.inject_result = True
    check("and once the gateway accepts, it is injected again",
          until(lambda: inst.deliveries.get("m3", {}).get("state")
                == "scheduled"), str(inst.deliveries.get("m3")))
    ctx.turn(GROUP, "s1", ctx.injected[-1][0])
    check("  and acked when seen", until(lambda: "m3" in fb.acked),
          str(fb.acked))

    # --- an ack that fails retries on its own clock ---------------------------
    settle()
    fb.quiet.set()                             # no keepalives, no new lines
    fb.fail_acks = 2
    fb.live.put(msg("m4", "flaky ack"))
    until(lambda: "m4" in ctx.ids_injected())
    ctx.turn(GROUP, "s1", ctx.injected[-1][0])
    check("a failed ack is retried with NO further message arriving",
          until(lambda: "m4" in fb.acked) and fb.fail_acks == 0,
          f"acked={fb.acked} fail_acks={fb.fail_acks}")
    fb.quiet.clear()

    # --- the brief: once per session, route chat only ---------------------------
    first = ctx.turn(GROUP, "s-new", "hi")
    again = ctx.turn(GROUP, "s-new", "and again")
    other = ctx.turn(dict(ELSEWHERE), "s-elsewhere", "hi", first=True)
    second = ctx.turn(GROUP, "s-newer", "hi")
    check("a new session in the route chat is briefed",
          "ADDRESSING IS AN ARGUMENT" in str(first), str(first)[:80])
    check("once — not on its next turn", again == [None], str(again)[:80])
    check("never in another chat of the bot", other == [None],
          str(other)[:80])
    check("and the next session is briefed again",
          "ADDRESSING IS AN ARGUMENT" in str(second), str(second)[:80])
    check("the brief says a chat reply does not reach the other agent",
          "NOT the agent that wrote" in str(first), "")

    # --- rename: in flight is acked as the id that owns it --------------------
    fb.me_agents = ["other"]
    fb.live.put(msg("m5", "before adopting"))
    until(lambda: "m5" in ctx.ids_injected())
    until(lambda: inst.deliveries.get("m5", {}).get("state") == "scheduled")
    out = ctx.call("rename_me", ALICE, {"new_id": "other"})
    check("rename_me to an id this token already owns ADOPTS it",
          out.get("adopted") is True and ctx.config.get("agent") == "other",
          str(out))
    check("the stream reconnects as the new id",
          until(lambda: ("/stream", "other") in fb.agents), str(fb.agents[-5:]))
    ctx.turn(GROUP, "s1", [c for c, _ in ctx.injected if 'id="m5"' in c][0])
    check("a message that arrived before adopting is acked as the OLD id — "
          "the old agent still exists and the receipt is its",
          until(lambda: "m5" in fb.acked)
          and ("/ack", "bot") in fb.agents[-8:], str(fb.agents[-8:]))
    fb.me_agents = []
    settle()
    fb.quiet.set()
    fb.live.put(msg("m6", "before renaming"))
    until(lambda: "m6" in ctx.ids_injected())
    until(lambda: inst.deliveries.get("m6", {}).get("state") == "scheduled")
    out = ctx.call("rename_me", ALICE, {"new_id": "renamed"})
    ctx.turn(GROUP, "s1", [c for c, _ in ctx.injected if 'id="m6"' in c][0])
    check("after a real RENAME (the receipts move with it) it is acked as "
          "the NEW id",
          until(lambda: "m6" in fb.acked)
          and ("/ack", "renamed") in fb.agents, str(fb.agents[-8:]))
    fb.quiet.clear()

    # --- off stops what is running ----------------------------------------------
    check("after the rename the stream reconnects as the new id",
          until(lambda: ("/stream", "renamed") in fb.agents),
          str(fb.agents[-5:]))
    until(lambda: inst.state["connected"])
    settle()
    fb.quiet.set()                    # the next line on the wire is m7
    out = ctx.call("enable_a2a_here", ALICE, {"enabled": False})
    check("alice turns it off (nothing settling)",
          out.get("enabled") is False and ctx.config["enabled"] is False,
          str(out))
    fb.live.put(msg("m7", "after off"))
    check("the stream closes", until(lambda: not inst.state["connected"]),
          str(inst.state))
    time.sleep(0.4)
    check("and a message arriving after that is neither injected nor acked",
          "m7" not in ctx.ids_injected() and "m7" not in fb.acked,
          str(ctx.ids_injected()))
    fb.quiet.clear()
    streams = fb.streams
    ctx.call("enable_a2a_here", ALICE, {"enabled": True})
    check("on again: the broker hands that same message over",
          until(lambda: "m7" in ctx.ids_injected())
          and fb.streams > streams, str(ctx.ids_injected()))
    ctx.turn(GROUP, "s1", ctx.injected[-1][0])
    until(lambda: "m7" in fb.acked)

    # --- accepted but never seen: reopen, the broker sends it again ----------
    mod.T_UNSEEN = 0.5
    streams = fb.streams
    fb.live.put(msg("m8", "lost in a queue"))
    check("a message the gateway accepted and no turn ever showed is taken "
          "back: the stream is reopened and the broker re-sends it, and it is "
          "injected again",
          until(lambda: ctx.ids_injected().count("m8") >= 2, 6)
          and fb.streams > streams,
          f"injected {ctx.ids_injected().count('m8')}x, streams "
          f"{streams}→{fb.streams}")
    for c, _ in [x for x in ctx.injected if 'id="m8"' in x[0]]:
        ctx.turn(GROUP, "s1", c)            # both copies end up seen
    check("  and, seen twice, it is acked ONCE",
          until(lambda: "m8" in fb.acked) and fb.acked.count("m8") == 1,
          str(fb.acked))
    mod.T_UNSEEN = 480.0

    # --- unload ------------------------------------------------------------------
    threads = list(inst._threads)
    stop(ctx)
    check("unload stops both threads",
          until(lambda: not any(t.is_alive() for t in threads), 5),
          str([t.name for t in threads if t.is_alive()]))
    fb.close()

    # --- two bots in one process share nothing -----------------------------------
    fa, fbb = FakeBroker(), FakeBroker()
    ma, mb = load(fa.url), load(fbb.url)
    ca = Ctx("alpha", config=ON)
    cb = Ctx("beta", config=dict(ON, route=ELSEWHERE))
    ia, ib = start(ma, ca), start(mb, cb)
    check("two profiles stream as two ids",
          until(lambda: ("/stream", "alpha") in fa.agents
                and ("/stream", "beta") in fbb.agents),
          f"{fa.agents} / {fbb.agents}")
    fa.live.put(msg("a1", "for alpha"))
    check("a message for one is injected into its own route only",
          until(lambda: "a1" in ca.ids_injected())
          and ca.injected[-1][1]["chat_id"] == "-100"
          and not cb.injected and "a1" not in ib.deliveries,
          f"{ca.injected} / {cb.injected}")
    check("and their switches, routes and delivery tables are separate",
          ia.route != ib.route and ia.deliveries is not ib.deliveries, "")
    stop(ca)
    stop(cb)
    fa.close()
    fbb.close()

    served_artifact()

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — one chat, one owner, nothing acked until a turn has seen it")
    return 1 if fails else 0


def served_artifact() -> None:
    """What the installer serves, unpacked and LOADED — not the source file.

    The broker's real /install/hermes handler, called with the token check
    and the database stubbed (test_installers.py covers the route over HTTP,
    with a database). The baked __init__.py is then imported and register()ed:
    a seam replaced into invalid Python, or credentials that do not reach the
    module, would pass every test above, which all load the unbaked source.
    """
    import asyncio
    import io
    import tarfile

    from starlette.requests import Request

    spec = importlib.util.spec_from_file_location(
        "broker_hermes_install", HERE.parent / "a2a_mcp" / "a2a-mcp.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)        # importing opens no database

    async def run_inline(fn):
        return fn()
    b._db = run_inline
    b.TOKENS.resolve = lambda t: {"stations": ["s"]} if t == "good" else None
    route = next(r for r in b.build_app().routes
                 if getattr(r, "path", "") == "/install/hermes/{token}")

    def get(token, query=b""):
        req = Request({"type": "http", "method": "GET", "scheme": "http",
                       "path": f"/install/hermes/{token}",
                       "query_string": query,
                       "server": ("broker.example.com", 80),
                       "headers": [(b"host", b"broker.example.com")],
                       "path_params": {"token": token}})
        return asyncio.run(route.endpoint(req))

    resp = get("good", b"agent=bot1")
    members = {}
    with tarfile.open(fileobj=io.BytesIO(resp.body), mode="r:gz") as t:
        for m in t.getmembers():
            members[m.name] = t.extractfile(m).read()
    check("installer: serves exactly plugin.yaml and __init__.py",
          sorted(members) == ["__init__.py", "plugin.yaml"],
          str(sorted(members)))
    check("installer: plugin.yaml's version is the broker's",
          f'version: "{b.VERSION}"' in members["plugin.yaml"].decode(),
          members["plugin.yaml"][:300])
    bad = get("wrong")
    check("installer: a bad token is the same 404 as the Codex route's",
          bad.status_code == 404
          and bad.body == b'{"error":"no installer available"}', bad.body)

    d = Path(tempfile.mkdtemp(prefix="a2a-hermes-served-"))
    (d / "__init__.py").write_bytes(members["__init__.py"])
    for k in ("A2A_URL", "A2A_TOKEN"):
        os.environ.pop(k, None)       # only the baked values may count
    spec = importlib.util.spec_from_file_location("served_hermes",
                                                  d / "__init__.py")
    baked = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(baked)
    check("the BAKED plugin imports, and its credentials are the ones served",
          baked.URL == "http://broker.example.com" and baked.TOKEN == "good"
          and baked.A2A_BAKED.get("agent") == "bot1"
          and baked.CLIENT_VERSION == b.VERSION,
          f"{baked.URL} {baked.TOKEN} {baked.A2A_BAKED}")
    c = Ctx("default")
    inst = start(baked, c)
    check("and register()s with the full tool set, under the baked id — the "
          "default profile gets its name from ?agent=",
          len(c.tools) == 22 and inst.key == "bot1" and inst.key_valid,
          f"{len(c.tools)} tools, key {inst.key!r}")
    stop(c)


# The tests reach the object register() built through the hook it registered.
def ctx_inst(ctx):
    return ctx.hooks["pre_llm_call"][0].__self__


def mod_state(inst):
    return inst.state


if __name__ == "__main__":
    sys.exit(main())
