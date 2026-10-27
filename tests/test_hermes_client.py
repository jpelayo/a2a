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
  desktop     a conversation in the Hermes desktop app is a route too: queued
              by its session key in the app's backend only; closed in the app
              → nothing goes in, nothing is lost, and it all arrives when the
              conversation is open again (a turn there, or the peek); a key
              Hermes rotates by compressing is followed by the app's id.

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
       "HERMES_SESSION_SCOPE_ID": "scope_id",
       # What a turn in the desktop app binds instead: no platform, no user.
       "HERMES_SESSION_SOURCE": "source",
       "HERMES_SESSION_KEY": "session_key",
       "HERMES_UI_SESSION_ID": "ui"}

gateway = types.ModuleType("gateway")
session_context = types.ModuleType("gateway.session_context")
session_context.get_session_env = (
    lambda name, default="": _TURN.get().get(VAR.get(name, name)) or default)
session_context.session_is_messaging_surface = (
    lambda: _TURN.get().get("platform") not in (None, "", "cli", "tui"))
gateway.session_context = session_context
hermes_constants = types.ModuleType("hermes_constants")
hermes_constants.get_hermes_home = lambda: _HOME.get()
# Cron: the running job (PluginContext.current_cron_execution(), a
# CronExecution with job_id) and the job store (cron.jobs.get_job), whose
# `origin` is the chat a job was created in — None when created at the
# machine (tools/cronjob_job_args.py _origin_from_env).
_CRON: contextvars.ContextVar = contextvars.ContextVar("cron", default=None)
JOBS: dict = {}
cron_pkg = types.ModuleType("cron")
cron_jobs = types.ModuleType("cron.jobs")
cron_jobs.get_job = lambda job_id: JOBS.get(job_id)
cron_pkg.jobs = cron_jobs
sys.modules.update({"gateway": gateway,
                    "gateway.session_context": session_context,
                    "hermes_constants": hermes_constants,
                    "cron": cron_pkg, "cron.jobs": cron_jobs})


def in_turn(chat: dict, fn, *a, **kw):
    """Run fn as part of a gateway turn in `chat`."""
    def inner():
        _TURN.set(dict(chat))
        return fn(*a, **kw)
    return contextvars.copy_context().run(inner)


# What Hermes's cron scope binds: platform, chat and user deliberately EMPTY
# (cron/scheduler.py _CronRunScope), and the cron marker.
CRON_TURN = {"platform": "", "chat_id": "", "chat_type": "", "thread_id": "",
             "user_id": "", "scope_id": "", "HERMES_CRON_SESSION": "1"}


def in_cron(job_id: str, fn, *a, **kw):
    """Run fn as part of a cron run of job `job_id`."""
    def inner():
        _TURN.set(dict(CRON_TURN))
        _CRON.set(types.SimpleNamespace(job_id=job_id, job_name=job_id))
        return fn(*a, **kw)
    return contextvars.copy_context().run(inner)


class Manager:
    def __init__(self, gateway=True, tui=False):
        self.has_gateway_message_injector = gateway
        # The desktop app's backend publishes this one instead.
        self.has_tui_message_injector = tui


class Ctx:
    """The PluginContext surface the plugin uses, and nothing else."""

    def __init__(self, profile, gateway=True, config=None, allowed=True,
                 tui=False):
        self.profile_name = profile
        self._manager = Manager(gateway, tui)
        self.allowed = allowed
        self.tools: dict = {}
        self.hooks: dict = {}
        self.unload: list = []
        self.config: dict = dict(config or {})
        # (content, origin) for a chat; (content, {"session_key": K}) for an
        # app conversation.
        self.injected: list[tuple[str, dict]] = []
        self.inject_result = True
        self.on_inject = None
        # The app conversations open in the app: Hermes's TUI host accepts a
        # session_key only when a live session has it.
        self.live_keys: set = set()

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
        if session_key is not None:
            if session_key not in self.live_keys:
                return False                 # no live session has that key
            where = {"session_key": session_key}
        else:
            where = dict(origin or {})
        self.injected.append((content, where))
        if self.on_inject:
            self.on_inject(content, where)
        return self.inject_result

    def _gateway_injection_allowed(self):
        return self.allowed

    def current_cron_execution(self):
        return _CRON.get()

    # -- driving it, as Hermes would --
    def call(self, tool, chat, args=None):
        out = in_turn(chat, self.tools[tool]["handler"], args or {})
        try:
            return json.loads(out)
        except (TypeError, ValueError):
            return out

    def cron_call(self, tool, job_id, args=None):
        """A tool call made by the agent of a cron run of `job_id`."""
        out = in_cron(job_id, self.tools[tool]["handler"], args or {})
        try:
            return json.loads(out)
        except (TypeError, ValueError):
            return out

    def turn(self, chat, session_id, user_message="", first=False):
        """A turn in `chat`: what every pre_llm_call hook returns."""
        return [in_turn(chat, cb, session_id=session_id, task_id="t",
                        turn_id="u", user_message=user_message,
                        conversation_history=[], is_first_turn=first,
                        model="m",
                        platform=chat.get("platform") or chat.get("source"),
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
# Conversations in the Hermes desktop app: a key, and the app's own id.
DESK = {"source": "desktop", "session_key": "20261013_120000_aaaaaa",
        "ui": "a1b2c3d4"}
DESK2 = {"source": "desktop", "session_key": "20261013_120500_bbbbbb",
         "ui": "e5f6a7b8"}
TUI = {"source": "tui", "session_key": "20261013_121000_eeeeee",
       "ui": "0a0b0c0d"}
DESK_ROUTE = {"surface": "desktop", "session_key": DESK["session_key"],
              "ui_session_id": DESK["ui"]}
DESK_ON = {"enabled": True, "route": DESK_ROUTE}


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
    desktop()
    cron()
    propose()

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — one chat or one app conversation, nothing acked until a "
          "turn has seen it")
    return 1 if fails else 0


def _no_tui_gateway():
    for name in ("tui_gateway.server", "tui_gateway"):
        sys.modules.pop(name, None)


def desktop() -> None:
    """A conversation in the Hermes DESKTOP APP as the route.

    The app's backend (`hermes serve`, HERMES_DESKTOP set) publishes Hermes's
    TUI/desktop injector, which queues text on a live session by its key. The
    fake Ctx accepts a key only while it is in `live_keys` — open in the app.
    """
    saved = os.environ.get("HERMES_DESKTOP")
    os.environ["HERMES_DESKTOP"] = "1"
    try:
        _desktop_route()
        _desktop_hosts()
        _desktop_owner()
    finally:
        _no_tui_gateway()
        if saved is None:
            os.environ.pop("HERMES_DESKTOP", None)
        else:
            os.environ["HERMES_DESKTOP"] = saved


def _desktop_route() -> None:
    fb = FakeBroker()
    mod = load(fb.url)
    mod.PEEK_S = 0.3
    key = DESK["session_key"]
    ctx = Ctx("bot", gateway=False, tui=True)
    ctx.live_keys = {key}
    start(mod, ctx)
    inst = ctx_inst(ctx)
    time.sleep(0.3)
    check("desktop: off until asked — no broker request", fb.calls == [],
          str(fb.calls))

    out = ctx.call("enable_a2a_here", DESK, {"enabled": True})
    check("desktop: 'enable a2a here' in an app conversation is accepted, "
          "with no user id — the app is the bot's operator",
          out.get("enabled") is True, str(out))
    check("desktop: it pins that conversation by its key and by the app's id "
          "for it", ctx.config.get("route") == DESK_ROUTE,
          str(ctx.config.get("route")))
    check("desktop: and the app's backend opens the stream at once",
          until(lambda: "/stream" in fb.calls), str(fb.calls))

    # --- delivery, by key -------------------------------------------------
    fb.live.put(msg("d1"))
    check("desktop: a message is queued on the conversation by its KEY — "
          "never by origin, which is the gateway's",
          until(lambda: "d1" in ctx.ids_injected())
          and ctx.injected[-1][1] == {"session_key": key},
          str(ctx.injected[-1:]))
    content = ctx.injected[-1][0]
    ctx.turn(DESK2, "s9", content)
    time.sleep(0.3)
    check("desktop: a turn in ANOTHER app conversation does not count",
          "d1" not in fb.acked, str(fb.acked))
    ctx.turn(DESK, "s1", content)
    check("desktop: a turn in the route conversation shows it: acked",
          until(lambda: "d1" in fb.acked), str(fb.acked))

    # --- the tools ----------------------------------------------------------
    check("desktop: the a2a tools work in the route conversation",
          "error" not in json.dumps(ctx.call("whoami", DESK))[:40],
          str(ctx.call("whoami", DESK))[:200])
    other = ctx.call("whoami", DESK2)
    chat = ctx.call("whoami", GROUP)
    check("desktop: and refuse in another conversation and in a messaging "
          "chat", isinstance(other, dict) and "error" in other
          and isinstance(chat, dict) and "error" in chat,
          f"{other} / {chat}")

    # --- the brief: once per conversation session ----------------------------
    first = ctx.turn(DESK, "s5", "hello")
    again = ctx.turn(DESK, "s5", "hello")
    check("desktop: the brief comes once per session, here too",
          first and first[0] and "context" in first[0]
          and again == [None], f"{first} / {again}")

    # --- closed in the app: nothing goes in, nothing is lost -------------------
    settle()
    fb.quiet.set()
    ctx.live_keys = set()
    fb.live.put(msg("d2", "while you were away"))
    check("closed: the app refuses it, so it is neither delivered nor acked",
          until(lambda: inst._closed_since is not None)
          and "d2" not in ctx.ids_injected() and "d2" not in fb.acked,
          f"{ctx.ids_injected()} {fb.acked}")
    st = ctx.call("a2a_channel_status", DESK)
    check("closed: status says to open the conversation, and that messages "
          "wait", "not open in the Hermes app" in (st.get("next_step") or ""),
          str(st.get("next_step")))
    streams = fb.streams
    time.sleep(0.8)
    check("closed: the stream stays shut, so the broker keeps new messages "
          "undelivered", fb.streams == streams and not inst.state["connected"],
          f"streams {streams}→{fb.streams}")
    # One past the broker's replay window: only the peek still has it.
    fb.aged.append(msg("d3", "from long ago"))
    ctx.live_keys = {key}
    ctx.turn(DESK, "s1", "back again")
    check("reopened: a turn there shows it open, and what waited arrives — "
          "including one the stream would never offer again",
          until(lambda: {"d2", "d3"} <= set(ctx.ids_injected())),
          str(ctx.ids_injected()))
    for c, _ in [x for x in ctx.injected if 'id="d2"' in x[0]
                 or 'id="d3"' in x[0]]:
        ctx.turn(DESK, "s1", c)
    check("reopened: each is acked once",
          until(lambda: {"d2", "d3"} <= set(fb.acked))
          and fb.acked.count("d2") == 1 and fb.acked.count("d3") == 1,
          str(fb.acked))

    # Closed again, then opened with no turn at all: the peek notices.
    # (Keepalives on for a moment, so the handler of the stream closed above
    # dies on a write instead of taking the next line.)
    fb.quiet.clear()
    settle()
    fb.quiet.set()
    ctx.live_keys = set()
    fb.live.put(msg("d4"))
    check("closed again: a message arriving meanwhile is refused",
          until(lambda: inst._closed_since is not None, 3)
          and "d4" not in ctx.ids_injected(), str(ctx.ids_injected()))
    ctx.live_keys = {key}
    check("reopened: with no turn, the periodic peek finds it open",
          until(lambda: "d4" in ctx.ids_injected(), 4),
          str(ctx.ids_injected()))
    ctx.turn(DESK, "s1", [c for c, _ in ctx.injected if 'id="d4"' in c][0])
    until(lambda: "d4" in fb.acked)

    # --- Hermes compresses the conversation: its key rotates ----------------
    new_key = "20261013_130000_cccccc"
    srv = types.ModuleType("tui_gateway.server")
    srv._sessions_lock = threading.Lock()
    srv._sessions = {DESK["ui"]: {"session_key": new_key}}
    pkg = types.ModuleType("tui_gateway")
    pkg.server = srv
    sys.modules.update({"tui_gateway": pkg, "tui_gateway.server": srv})
    ctx.live_keys = {new_key}
    fb.quiet.clear()
    settle()
    fb.quiet.set()
    fb.live.put(msg("d5"))
    check("compressed: the conversation's new key is found by the app's id, "
          "and the message goes in under it",
          until(lambda: "d5" in ctx.ids_injected())
          and ctx.injected[-1][1] == {"session_key": new_key}
          and (ctx.config.get("route") or {}).get("session_key") == new_key,
          f"{ctx.injected[-1:]} {ctx.config.get('route')}")
    ctx.turn(dict(DESK, session_key=new_key), "s1", ctx.injected[-1][0])
    check("compressed: a turn under the new key acks it",
          until(lambda: "d5" in fb.acked), str(fb.acked))
    newer = dict(DESK, session_key="20261013_140000_dddddd")
    ctx.turn(newer, "s1", "x")
    check("compressed: a turn under yet another key, same app id, is followed",
          (ctx.config.get("route") or {}).get("session_key")
          == newer["session_key"], str(ctx.config.get("route")))
    _no_tui_gateway()
    stop(ctx)
    fb.close()


def _desktop_hosts() -> None:
    """Exactly one process may hold a bot's stream: the gateway for a chat,
    the app's backend for an app conversation."""
    fb = FakeBroker()
    mod = load(fb.url)
    for label, kw, env, cfg in (
            ("an app route in the bot's GATEWAY",
             dict(gateway=True, tui=False), "1", DESK_ON),
            ("an app route in a TUI process (no HERMES_DESKTOP)",
             dict(gateway=False, tui=True), "", DESK_ON),
            ("a chat route in the APP's backend",
             dict(gateway=False, tui=True), "1", ON)):
        os.environ["HERMES_DESKTOP"] = env
        fb.calls.clear()
        c = Ctx("bot", config=cfg, **kw)
        start(mod, c)
        time.sleep(0.4)
        check(f"host: {label} → zero broker requests", fb.calls == [],
              str(fb.calls))
        stop(c)

    # The backend is the process that SERVES the conversation, whatever its
    # environment says: HERMES_DESKTOP alone left a backend started any other
    # way holding the stream for nobody, and saying nothing about it.
    os.environ["HERMES_DESKTOP"] = ""
    fb.calls.clear()
    c = Ctx("bot", gateway=False, tui=True, config=DESK_ON)
    start(mod, c)
    time.sleep(0.4)
    check("host: the desktop injector with NO HERMES_DESKTOP and no turn of "
          "the conversation yet → still nothing", fb.calls == [],
          str(fb.calls))
    c.turn(TUI, "s-tui")
    time.sleep(0.4)
    check("host: a terminal UI turn there does not make it the app's backend",
          fb.calls == [], str(fb.calls))
    c.turn(DESK, "s-desk")
    check("host: a turn of the route conversation there DOES — the stream "
          "opens at once", until(lambda: "/stream" in fb.calls, 3),
          str(fb.calls))
    held = ctx_inst(c).log_file.read_text().count(
        "not streaming here: this process does not serve")
    check("host: while it waited, it said why in the log — once, not on "
          "every poll", held == 1, f"{held} lines")
    stop(c)

    # Hermes's session table in this process holding the conversation is
    # the same fact, known before any turn: a backend restarted under an
    # open conversation streams without waiting for someone to type.
    fb.calls.clear()
    srv = types.ModuleType("tui_gateway.server")
    srv._sessions_lock = threading.Lock()
    srv._sessions = {DESK["ui"]: {"session_key": DESK["session_key"]}}
    pkg = types.ModuleType("tui_gateway")
    pkg.server = srv
    sys.modules.update({"tui_gateway": pkg, "tui_gateway.server": srv})
    c = Ctx("bot", gateway=False, tui=True, config=DESK_ON)
    start(mod, c)
    check("host: the conversation in THIS process's session table, no turn "
          "and no HERMES_DESKTOP → the stream opens",
          until(lambda: "/stream" in fb.calls, 3), str(fb.calls))
    stop(c)
    _no_tui_gateway()

    fb.calls.clear()
    c = Ctx("bot", gateway=True, tui=False, config=DESK_ON)
    start(mod, c)
    c.turn(DESK, "s-gw")
    time.sleep(0.4)
    check("host: the GATEWAY, even with a route turn, never holds an app "
          "route — it has no desktop injector", fb.calls == [], str(fb.calls))
    stop(c)
    os.environ["HERMES_DESKTOP"] = "1"
    fb.close()


def _desktop_owner() -> None:
    fb = FakeBroker()
    mod = load(fb.url)
    c = Ctx("bot", gateway=True, tui=True, config=ON)
    c.live_keys = {DESK["session_key"]}
    start(mod, c)
    out = c.call("enable_a2a_here", DESK, {"enabled": True})
    check("owner: the app takes a2a from a chat",
          out.get("enabled") is True
          and (c.config.get("route") or {}).get("surface") == "desktop",
          str(out))
    check("owner: and the chat's owner is kept, to move it back later",
          c.config.get("owner") == {"platform": "x", "user_id": "alice"},
          str(c.config.get("owner")))
    out = c.call("enable_a2a_here", BOB, {"enabled": True})
    check("owner: someone else in the chat cannot take it",
          "error" in out, str(out))
    out = c.call("enable_a2a_here", ALICE, {"enabled": True})
    check("owner: the chat's owner can move it back to their chat",
          out.get("enabled") is True and c.config.get("route") == GROUP,
          str(out))
    stop(c)

    c = Ctx("bot", gateway=True, tui=True)
    c.live_keys = {DESK["session_key"]}
    start(mod, c)
    c.call("enable_a2a_here", DESK, {"enabled": True})
    out = c.call("enable_a2a_here", BOB, {"enabled": True})
    check("owner: on in the app with no chat owner, nobody in a chat can "
          "take it", "error" in out and "Hermes app" in out["error"],
          str(out))
    c.call("enable_a2a_here", DESK, {"enabled": False})
    out = c.call("enable_a2a_here", BOB, {"enabled": True})
    check("owner: once the app turns it off, a chat user can turn it on and "
          "owns it", out.get("enabled") is True
          and c.config.get("owner") == {"platform": "x", "user_id": "bob"},
          str(out))
    out = c.call("enable_a2a_here", TUI, {"enabled": True})
    check("owner: a terminal UI session is refused, and told where a2a can "
          "run", "error" in out and "Hermes app" in out["error"], str(out))
    stop(c)
    fb.close()


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


SENDS = {"send_dm": "/dms", "post_to_channel": "/channels/ops/messages",
         "share_md": "/md"}
SEND_ARGS = {"name": "ops", "text": "from cron", "to": "peer",
             "channel": "ops", "filename": "a.md", "content": "c"}
INBOUND = ("my_pending", "read_dms", "read_channel", "ack_messages",
           "ack_all", "join_channel")


def origin(chat: dict) -> dict:
    """A job's origin as Hermes records it from the creator's turn."""
    return {"platform": chat["platform"], "chat_id": chat["chat_id"],
            "chat_name": None, "thread_id": chat.get("thread_id") or None,
            "user_id": chat.get("user_id") or None, "scope_id": None}


def cron() -> None:
    """A CRON JOB of the bot may SEND as the bot if whoever created it could.

    Hermes runs a job with the session's chat and user deliberately empty,
    so the route gate refuses it; the creator is the job's `origin` — the
    chat it was created in, or None when created at the machine. Without the
    creator check, anyone who can chat with the bot could schedule it to
    speak on a2a as itself.
    """
    JOBS.clear()
    JOBS.update({
        "j-alice": {"id": "j-alice", "origin": origin(ALICE)},
        "j-bob": {"id": "j-bob", "origin": origin(BOB)},
        "j-alice-elsewhere": {"id": "j-alice-elsewhere",
                              "origin": origin(ELSEWHERE)},
        "j-machine": {"id": "j-machine", "origin": None},
    })

    def sends(ctx, fb, job):
        """{tool: (out, [(path, agent)] it caused)}"""
        r = {}
        for t in SENDS:
            before = len(fb.agents)
            out = ctx.cron_call(t, job, SEND_ARGS)
            r[t] = (out, fb.agents[before:])
        return r

    def went(r, agent="bot"):
        return all("error" not in json.dumps(out)[:60]
                   and calls == [(SENDS[t], agent)]
                   for t, (out, calls) in r.items())

    def refused(r):
        return all(isinstance(out, dict) and "error" in out and not calls
                   for out, calls in r.values())

    # --- a messaging route: owner alice, in GROUP --------------------------
    fb = FakeBroker()
    mod = load(fb.url)
    ctx = Ctx("bot", config=ON)
    start(mod, ctx)
    until(lambda: "/stream" in fb.calls, 3)
    streams, injected = fb.streams, len(ctx.injected)

    r = sends(ctx, fb, "j-alice")
    check("cron: a job alice (the owner) created in the route chat sends as "
          "the bot — send_dm, post_to_channel and share_md reach the broker "
          "under the bot's id", went(r), str(r))
    r = sends(ctx, fb, "j-machine")
    check("cron: so does a job created at the machine (no origin: the app, "
          "the CLI)", went(r), str(r))
    r = sends(ctx, fb, "j-bob")
    check("cron: a job BOB created in the same group is refused, and nothing "
          "reaches the broker — he cannot have the bot speak through a "
          "schedule either", refused(r), str(r))
    r = sends(ctx, fb, "j-alice-elsewhere")
    check("cron: a job alice created in ANOTHER chat is refused",
          refused(r), str(r))
    r = sends(ctx, fb, "j-unknown")
    check("cron: a job the store does not know is refused", refused(r), str(r))
    ctx.current_cron_execution = None          # a Hermes without the API
    r = sends(ctx, fb, "j-alice")
    del ctx.current_cron_execution
    check("cron: a Hermes with no current_cron_execution refuses, as before",
          refused(r), str(r))

    before = len(fb.agents)
    inbound = {t: ctx.cron_call(t, "j-alice", {"ids": ["z"], "name": "ops"})
               for t in INBOUND}
    check("cron: even the owner's job cannot READ — my_pending, read_dms, "
          "read_channel, the acks and join_channel refuse, and the broker "
          "hears nothing: receiving stays with the route",
          all(isinstance(v, dict) and "error" in v for v in inbound.values())
          and fb.agents[before:] == [],
          f"{inbound} {fb.agents[before:]}")
    hooks = in_cron("j-alice", lambda: [
        cb(session_id="c1", task_id="t", turn_id="u", user_message="hi",
           conversation_history=[], is_first_turn=True, model="m",
           platform="", parent_session_id=None, sender_id=None)
        for cb in ctx.hooks.get("pre_llm_call", [])])
    check("cron: nothing streams for it, nothing is injected, and its turn "
          "gets no brief",
          fb.streams == streams and len(ctx.injected) == injected
          and all(h is None for h in hooks),
          f"streams {streams}->{fb.streams} injected "
          f"{injected}->{len(ctx.injected)} hooks={hooks}")
    r = ctx.call("send_dm", BOB, SEND_ARGS)
    check("cron: and outside cron nothing changed — bob in the group is still "
          "refused", isinstance(r, dict) and "only the user" in str(r.get("error")),
          str(r))
    stop(ctx)
    fb.close()

    # --- an app route, from a process that delivers nothing (cron's) -------
    fb = FakeBroker()
    mod = load(fb.url)
    ctx = Ctx("bot", gateway=False, config=DESK_ON)
    start(mod, ctx)
    r = sends(ctx, fb, "j-machine")
    check("cron, app route: a job created at the machine sends as the bot",
          went(r), str(r))
    r = sends(ctx, fb, "j-alice")
    check("cron, app route: a job created in a messaging chat is refused — "
          "an app route has no chat owner to vouch for it", refused(r), str(r))
    check("cron, app route: and that process never opened a stream",
          "/stream" not in fb.calls, str(fb.calls))
    stop(ctx)
    fb.close()
    JOBS.clear()


def propose() -> None:
    """propose_me can name the station. A token that reaches two stations
    cannot propose without it (the broker's 409 "name one with station"),
    and the tool used to send {agent_id, note} and nothing else. Without a
    station the body must stay exactly that. The other four clients are in
    test_propose_station.py."""
    fb = FakeBroker()
    mod = load(fb.url)
    ctx = Ctx("bot", config=ON)
    start(mod, ctx)
    props = (ctx.tools["propose_me"]["schema"].get("parameters") or {}).get(
        "properties") or {}
    check("propose: the tool offers a station argument", "station" in props,
          str(sorted(props)))
    for args in ({"note": "n1", "station": "bots"}, {"note": "n2"},
                 {"note": "n3", "station": None}):
        ctx.call("propose_me", ALICE, args)
    sent = [b for p, b in fb.bodies if p == "/me/proposals"]
    check("propose: with a station, the body carries it",
          sent[:1] == [{"agent_id": "bot", "note": "n1", "station": "bots"}],
          str(sent))
    check("propose: without one — absent or null — the body is exactly "
          "{agent_id, note}, as before",
          sent[1:] == [{"agent_id": "bot", "note": "n2"},
                       {"agent_id": "bot", "note": "n3"}], str(sent))
    stop(ctx)
    fb.close()

    # Signing up turns push on where it is asked, through enable_a2a_here
    # and every rule it has — so the bot receives once it is approved.
    def asked(fb):
        return [b for p, b in fb.bodies if p == "/me/proposals"]

    fb = FakeBroker()
    mod = load(fb.url)
    ctx = Ctx("bot")                            # a2a off
    start(mod, ctx)
    out = ctx.call("propose_me", CLI, {"note": "from the cli"})
    check("propose: off, from the CLI → refused as enabling there is: "
          "nothing asked, still off",
          isinstance(out, dict) and "error" in out and not asked(fb)
          and ctx.config.get("enabled") is not True, f"{out} {asked(fb)}")
    out = ctx.call("propose_me", ALICE, {"note": "hi"})
    route = ctx.config.get("route") or {}
    check("propose: off, alice in the group → a2a turned on THERE, alice "
          "owns it, and the request is posted",
          ctx.config.get("enabled") is True
          and same_chat_keys(route, GROUP)
          and ctx.config.get("owner") == {"platform": "x", "user_id": "alice"}
          and asked(fb) == [{"agent_id": "bot", "note": "hi"}]
          and "a2a" in out, f"{out} {route} {asked(fb)}")
    check("propose: and the stream opens at once — it delivers the moment "
          "the name is approved", until(lambda: "/stream" in fb.calls, 3),
          str(fb.calls))
    stop(ctx)
    fb.close()

    saved = os.environ.pop("HERMES_DESKTOP", None)
    try:
        fb = FakeBroker()
        mod = load(fb.url)
        ctx = Ctx("bot", gateway=False, tui=True)   # the app's backend
        ctx.live_keys = {DESK["session_key"]}
        start(mod, ctx)
        out = ctx.call("propose_me", DESK, {"note": "app"})
        check("propose: off, in an app conversation → that conversation is "
              "the route, the request posted, the stream open — with no "
              "HERMES_DESKTOP: the turn ran here",
              ctx.config.get("route") == DESK_ROUTE
              and asked(fb) == [{"agent_id": "bot", "note": "app"}]
              and until(lambda: "/stream" in fb.calls, 3),
              f"{out} {ctx.config.get('route')} {fb.calls}")
        stop(ctx)
        fb.close()
    finally:
        if saved is not None:
            os.environ["HERMES_DESKTOP"] = saved


def same_chat_keys(route: dict, chat: dict) -> bool:
    return all(str(route.get(k) or "") == str(chat.get(k) or "")
               for k in ("platform", "chat_id", "thread_id", "user_id"))


# The tests reach the object register() built through the hook it registered.
def ctx_inst(ctx):
    return ctx.hooks["pre_llm_call"][0].__self__


def mod_state(inst):
    return inst.state


if __name__ == "__main__":
    sys.exit(main())
