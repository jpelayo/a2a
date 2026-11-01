#!/usr/bin/env python3
"""The Hermes plugin's contract with HERMES — checked against Hermes's source.

    HERMES_SRC=/path/to/hermes-agent python3 tests/test_hermes_contract.py

test_hermes_client.py runs the plugin against a fake PluginContext, which can
only ever agree with itself: if Hermes renames a method, the fake keeps the
old name and the suite stays green while every bot breaks. This file reads a
Hermes checkout instead — by AST, so no Hermes dependency needs installing —
and pins every name and signature the plugin relies on. Run it against the
release named in plugin.yaml's `requires_hermes` (git tag v0.21.7) and
against main; if the release fails where main passes, `requires_hermes` must
rise.

The plugin was written against main ee36aed (2026-10-12), latest release
v0.21.7.

SKIPs (exit 2) when HERMES_SRC does not point at a checkout: nothing has been
checked, and saying so is the only honest outcome.
"""
import ast
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLIENT = HERE.parent / "deploy" / "plugin" / "hermes" / "__init__.py"
MANIFEST = HERE.parent / "deploy" / "plugin" / "hermes" / "plugin.yaml"
API = HERE.parent / "deploy" / "plugin" / "hermes" / "dashboard" / "api.py"

fails: list[str] = []


def real_scope_check(src: Path) -> None:
    """Run Hermes's REAL tui_gateway/method_ctx.py and model_switch.py — not
    a reading of them — to show what the waker must call. Only what their
    function bodies import is stubbed (agent.secret_scope,
    tui_gateway.launch_profile_policy, tools.terminal_scope), and the fake
    server supplies the server globals those bodies name. The waker once
    called the module's copy, and crashed on every profile in a real deploy.
    """
    import contextlib
    import importlib.util
    import types
    saved = dict(sys.modules)
    calls = []
    try:
        pkg = types.ModuleType("tui_gateway")
        pkg.__path__ = [str(src / "tui_gateway")]
        stub = {
            "tui_gateway": pkg,
            "agent": types.ModuleType("agent"),
            "agent.secret_scope": types.ModuleType("agent.secret_scope"),
            "tui_gateway.launch_profile_policy":
                types.ModuleType("tui_gateway.launch_profile_policy"),
            "tools": types.ModuleType("tools"),
            "tools.terminal_scope": types.ModuleType("tools.terminal_scope"),
        }
        stub["agent.secret_scope"].is_multiplex_active = lambda: False
        lpp = stub["tui_gateway.launch_profile_policy"]
        lpp.launch_secret_scope = lambda home: ("secrets-of", str(home))
        lpp.launch_terminal_env = lambda: {}
        stub["tools.terminal_scope"].reset_terminal_scope = (
            lambda token: calls.append(("reset_terminal", token)))
        sys.modules.update(stub)

        def load(name):
            spec = importlib.util.spec_from_file_location(
                f"tui_gateway.{name}", src / "tui_gateway" / f"{name}.py")
            mod = importlib.util.module_from_spec(spec)
            sys.modules[spec.name] = mod
            spec.loader.exec_module(mod)
            return mod
        load("method_ctx")
        ms = load("model_switch")

        err = None
        try:
            with ms._session_profile_runtime_scope({"profile_home": None}):
                pass
        except NameError as e:
            err = str(e)
        except Exception as e:      # noqa: BLE001 — any other is a FAIL below
            err = f"{type(e).__name__}: {e}"
        check("REAL model_switch: its own _session_profile_runtime_scope "
              "raises NameError on _TurnScopes — what crashed the waker",
              err is not None and "_TurnScopes" in err, str(err))

        class _TurnScopes:
            def __init__(self):
                self.home = self.secret = self.terminal = None

        server = types.ModuleType("tui_gateway.server")
        server.__dict__.update(
            _TurnScopes=_TurnScopes, Path=Path,
            _launch_home=lambda: Path("/launch"),
            set_secret_scope=lambda s, **kw: calls.append(("secret", s)) or "T",
            reset_secret_scope=lambda t: calls.append(("reset_secret", t)),
            set_hermes_home_override=lambda h: calls.append(("home", h)) or "H",
            reset_hermes_home_override=lambda t: calls.append(("reset_home", t)),
            register_method=lambda name, fn: None,
            _profile_scoped=lambda fn: fn,
            logger=__import__("logging").getLogger("fake-server"))
        ms.register(server)
        scope = getattr(server, "_session_profile_runtime_scope", None)
        ran = False
        try:
            with scope({"profile_home": None}):
                ran = True
        except Exception as e:      # noqa: BLE001
            calls.append(("raised", repr(e)))
        check("REAL bind_module: the SERVER's copy runs, on the server's "
              "globals — the one the waker enters",
              ran and ("secret", ("secrets-of", "/launch")) in calls
              and ("reset_secret", "T") in calls, str(calls))
        check("REAL bind_module: the server's copy is a context manager "
              "whose function was rebuilt on the server's namespace",
              getattr(getattr(scope, "__wrapped__", None), "__globals__",
                      None) is vars(server), repr(scope))
    except Exception as e:          # noqa: BLE001
        check("REAL model_switch and method_ctx load from the checkout", False,
              f"{type(e).__name__}: {e}")
    finally:
        for name in set(sys.modules) - set(saved):
            sys.modules.pop(name, None)
        sys.modules.update(saved)


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def tree(src: Path, rel: str):
    p = src / rel
    return ast.parse(p.read_text()) if p.is_file() else None


def find(node, kind, name):
    for n in ast.walk(node) if node else ():
        if isinstance(n, kind) and getattr(n, "name", None) == name:
            return n
    return None


def params(fn) -> list[str]:
    a = fn.args
    return ([x.arg for x in a.posonlyargs + a.args + a.kwonlyargs]
            + ([a.vararg.arg] if a.vararg else [])
            + ([a.kwarg.arg] if a.kwarg else []))


def is_property(fn) -> bool:
    return any(getattr(d, "id", getattr(d, "attr", "")) in
               ("property", "cached_property") for d in fn.decorator_list)


def main() -> int:
    src = Path(os.environ.get("HERMES_SRC") or "")
    if not str(src) or not (src / "hermes_cli" / "plugins.py").is_file():
        print("SKIP: set HERMES_SRC to a hermes-agent checkout "
              "(hermes_cli/plugins.py not found)")
        return 2

    plugins = tree(src, "hermes_cli/plugins.py")
    ctx = find(plugins, ast.ClassDef, "PluginContext")
    mgr = find(plugins, ast.ClassDef, "PluginManager")
    check("PluginContext and PluginManager exist", ctx and mgr, "")

    def method(cls, name):
        return find(cls, (ast.FunctionDef, ast.AsyncFunctionDef), name)

    # What the plugin calls, with the arguments it passes.
    wants = {
        "inject_message": {"content", "role", "origin"},
        "register_tool": {"name", "toolset", "schema", "handler",
                          "description"},
        "register_hook": {"hook_name", "callback"},
        "on_unload": {"callback"},
        "get_config": {"key", "default"},
        "set_config": {"key", "value"},
        "_gateway_injection_allowed": set(),
    }
    for name, args in wants.items():
        fn = method(ctx, name)
        got = set(params(fn)) if fn else set()
        check(f"PluginContext.{name} takes {sorted(args) or 'no arguments'}",
              fn is not None and args <= got, f"has {sorted(got)}")
    inj = method(ctx, "inject_message")
    check("inject_message takes origin as KEYWORD-only, as the plugin passes "
          "it", inj is not None
          and "origin" in [a.arg for a in inj.args.kwonlyargs], "")
    prof = method(ctx, "profile_name")
    check("PluginContext.profile_name is a property",
          prof is not None and is_property(prof), "")
    gw = method(mgr, "has_gateway_message_injector")
    check("PluginManager.has_gateway_message_injector is a property — the "
          "plugin streams only in a process where it is true",
          gw is not None and is_property(gw), "")
    text = (src / "hermes_cli" / "plugins.py").read_text()
    check("the context reaches its manager as ctx._manager",
          "self._manager = manager" in text, "")
    check("pre_llm_call is a valid hook", re.search(
        r'VALID_HOOKS[^=]*=\s*\{[^}]*"pre_llm_call"', text, re.S) is not None,
        "")

    # The hook payload the plugin reads.
    calls = []
    for py in src.rglob("*.py"):
        if "test" in py.parts[-1] or "tests" in py.parts:
            continue
        try:
            t = py.read_text()
        except Exception:
            continue
        if "pre_llm_call" in t:
            calls += re.findall(r'invoke_hook\(\s*"pre_llm_call"(.*?)\)\s*$',
                                t, re.S | re.M)
    payload = " ".join(calls)
    for kw in ("session_id", "user_message", "conversation_history",
               "is_first_turn"):
        check(f"pre_llm_call is invoked with {kw}=", f"{kw}=" in payload,
              payload[:200])

    sc = tree(src, "gateway/session_context.py")
    sc_text = ((src / "gateway" / "session_context.py").read_text()
               if sc else "")
    for name in ("get_session_env", "session_is_messaging_surface"):
        check(f"gateway.session_context.{name} exists",
              find(sc, ast.FunctionDef, name) is not None, "")
    client = CLIENT.read_text()
    for var in sorted(set(re.findall(r'"(HERMES_(?:UI_)?SESSION_[A-Z_]+)"',
                                     client))):
        check(f"Hermes binds {var}", f'"{var}"' in sc_text, "")
    hc = tree(src, "hermes_constants.py")
    check("hermes_constants.get_hermes_home exists",
          find(hc, ast.FunctionDef, "get_hermes_home") is not None, "")

    # The desktop app's conversations: a second host, by session key.
    check("inject_message takes session_key as KEYWORD-only — how a text is "
          "queued on a desktop-app conversation",
          inj is not None
          and "session_key" in [a.arg for a in inj.args.kwonlyargs], "")
    tui = method(mgr, "has_tui_message_injector")
    check("PluginManager.has_tui_message_injector is a property — the plugin "
          "streams an app route only where it is true",
          tui is not None and is_property(tui), "")
    pi = tree(src, "tui_gateway/plugin_inject.py")
    check("tui_gateway/plugin_inject.py defines inject_tui_session_message",
          find(pi, ast.FunctionDef, "inject_tui_session_message") is not None,
          "")
    # web_server.py until Hermes split it; web_server_app.py since.
    ws = [p for p in (src / "hermes_cli" / "web_server.py",
                      src / "hermes_cli" / "web_server_app.py") if p.is_file()]
    check("the desktop app's backend (hermes serve) installs the TUI injector",
          any("install_tui_message_injector" in p.read_text() for p in ws),
          str([p.name for p in ws]))
    check("a desktop turn binds HERMES_UI_SESSION_ID — the app's id the plugin "
          "follows a rotated key by", '"HERMES_UI_SESSION_ID"' in sc_text, "")
    check("'desktop' is the session source a desktop-app turn carries",
          '"desktop"' in sc_text, "")
    srv = src / "tui_gateway" / "server.py"
    srv_text = srv.read_text() if srv.is_file() else ""
    check("tui_gateway.server keeps _sessions and _sessions_lock (read, best "
          "effort, to find a compressed conversation's new key)",
          re.search(r"^_sessions\s*[:=]", srv_text, re.M) is not None
          and re.search(r"^_sessions_lock\s*[:=]", srv_text, re.M) is not None,
          "")
    # The process that serves a conversation is the one whose table holds it:
    # the injector queues only on a live session in THIS process's _sessions
    # whose session_key matches — the match _table_holds makes.
    pi_text = ((src / "tui_gateway" / "plugin_inject.py").read_text()
               if (src / "tui_gateway" / "plugin_inject.py").is_file() else "")
    check("the injector matches a session in _sessions by its session_key",
          re.search(r'_sessions\.items\(\).*?session\.get\("session_key"\)\s*'
                    r'==\s*session_key', pi_text, re.S) is not None, "")
    # A conversation's turns carry "desktop" or "tui", and both are a route:
    # Hermes picks "desktop" only from the app's own say-so or HERMES_DESKTOP,
    # so a bot's own chat on a gateway reached by URL is "tui".
    af = src / "tui_gateway" / "agent_factory.py"
    af_text = af.read_text() if af.is_file() else ""
    plat = (af_text.split("def _resolve_session_platform", 1) + [""])[1][:800]
    check("_resolve_session_platform answers 'desktop' or else 'tui'",
          '"desktop"' in plat and '"tui"' in plat
          and "HERMES_DESKTOP" in plat, plat[:300])
    nm = re.search(r"NON_MESSAGING_SESSION_SURFACES\s*=\s*frozenset\(\{(.*?)\}",
                   sc_text, re.S)
    check("'tui' and 'desktop' are not messaging surfaces — enable_a2a_here "
          "takes them as conversations, not chats",
          nm is not None and '"tui"' in nm.group(1)
          and '"desktop"' in nm.group(1), "")
    # Settings older than `home` are judged against the bot's own sessions.
    common = src / "hermes_state_common.py"
    common_text = common.read_text() if common.is_file() else ""
    check("state.db keeps sessions(id …): the session key is that id",
          re.search(r"CREATE TABLE IF NOT EXISTS sessions \(\s*id TEXT "
                    r"PRIMARY KEY", common_text) is not None, "")
    hs = src / "hermes_state.py"
    check("state.db lives in the profile home",
          hs.is_file() and 'get_hermes_home() / "state.db"' in hs.read_text(),
          "")
    reg = src / "tui_gateway" / "session_registry.py"
    reg_text = reg.read_text() if reg.is_file() else ""
    check("a conversation's session key is a new session id",
          re.search(r"def _new_session_key\(\)[^:]*:\s*return new_session_id"
                    r"\(\)", reg_text) is not None, "")
    # The plugin's own state: ctx.state, Hermes's per-plugin store, instead
    # of config.yaml (which a clone copies whole).
    st_prop = method(ctx, "state")
    check("PluginContext.state is a property — the plugin's own store",
          st_prop is not None and is_property(st_prop), "")
    ps_tree = tree(src, "hermes_cli/plugins_state.py")
    ps = find(ps_tree, ast.ClassDef, "PluginState")
    for name in ("get", "set", "data_dir"):
        check(f"PluginState.{name} exists", ps is not None
              and method(ps, name) is not None, "")
    ps_text = ((src / "hermes_cli" / "plugins_state.py").read_text()
               if (src / "hermes_cli" / "plugins_state.py").is_file() else "")
    check("PluginState keeps state.json under <profile>/plugin-data",
          '"plugin-data"' in ps_text and '"state.json"' in ps_text, "")
    # Why a bot can arrive holding another's settings at all.
    prof = src / "hermes_cli" / "profiles.py"
    prof_text = prof.read_text() if prof.is_file() else ""
    boot = (prof_text.split("def _bootstrap_profile_dir", 1) + [""])[1][:4000]
    boot = boot.split("\ndef ", 1)[0]
    check("the app's clone (_bootstrap_profile_dir) copies no plugin-data — "
          "so the plugin's own state does not travel into a new bot",
          bool(boot) and "plugin-data" not in boot, "")
    files = re.search(r"_CLONE_CONFIG_FILES\s*=\s*\[([^\]]*)\]", prof_text)
    check("a cloned profile copies config.yaml whole (this plugin's settings "
          "in it) — and not state.db",
          files is not None and '"config.yaml"' in files.group(1)
          and "state.db" not in files.group(1),
          files.group(0) if files else "")

    # Cron: a job may send as the bot if whoever created it could. The plugin
    # reads the running job through the plugin API and its creator from the
    # job store's `origin`. Added 2026-10-27 against main.
    cce = method(ctx, "current_cron_execution")
    check("PluginContext.current_cron_execution exists — how a tool call "
          "learns it is part of a cron run", cce is not None, "")
    ei = tree(src, "cron/execution_identity.py")
    ce = find(ei, ast.ClassDef, "CronExecution")
    check("CronExecution carries job_id",
          ce is not None and any(
              isinstance(n, ast.AnnAssign) and getattr(n.target, "id", "")
              == "job_id" for n in ce.body), "")
    jobs = tree(src, "cron/jobs.py")
    gj = find(jobs, ast.FunctionDef, "get_job")
    check("cron.jobs.get_job(job_id) exists", gj is not None
          and params(gj)[:1] == ["job_id"],
          f"params {params(gj) if gj else None}")
    ja = src / "tools" / "cronjob_job_args.py"
    ja_text = ja.read_text() if ja.is_file() else ""
    origin_fn = (ja_text.split("def _origin_from_env", 1) + [""])[1][:3000]
    for key in ("platform", "chat_id", "thread_id", "user_id"):
        check(f"a job's origin records {key!r} from the creator's turn — what "
              "the plugin compares with the route and the owner",
              f'"{key}"' in origin_fn, "tools/cronjob_job_args.py")
    tc = src / "hermes_cli" / "tools_config.py"
    tc_text = tc.read_text() if tc.is_file() else ""
    check("'a2a' is a default-off toolset — a cron job gets the tools only by "
          "listing it in enabled_toolsets, as CLAUDE.md says",
          re.search(r"_DEFAULT_OFF_TOOLSETS[^=]*=\s*\{[^}]*\"a2a\"", tc_text,
                    re.S) is not None, "")

    # The hold: in the dashboard the plugin loads its route conversation
    # itself — the installed session.resume handler, called in-process as
    # Hermes's hosted Group Chat does — with its own transport bound, so no
    # reaper takes it. Added 2026-11-01 against main.
    def text_of(rel):
        p = src / rel
        return p.read_text() if p.is_file() else ""
    check("server.py keeps the handler table (_methods), the launch home "
          "(_hermes_home) and the stdio fallback (_stdio_transport)",
          all(re.search(rf"^{n}\b", srv_text, re.M) for n in
              ("_methods", "_hermes_home", "_stdio_transport")), "")
    tr = tree(src, "tui_gateway/transport.py")
    for fn in ("bind_transport", "reset_transport", "current_transport"):
        check(f"tui_gateway.transport.{fn} exists — how a caller attaches "
              f"its own transport", find(tr, ast.FunctionDef, fn) is not None,
              "")
    check("a record made with nothing bound falls back to stdout — why the "
          "plugin always binds its own",
          "current_transport() or _stdio_transport"
          in text_of("tui_gateway/session_registry.py"), "")
    ms = text_of("tui_gateway/methods_session.py")
    reuse = (ms.split("def _resume_reuse_live_locked", 1) + [""])[1][:1500]
    check("session.resume on a loaded conversation attaches the CALLER's "
          "transport beside the clients already on it",
          "transport=current_transport() or _stdio_transport" in reuse, "")
    check("session.resume is an installed handler",
          '@method("session.resume")' in ms, "")
    cs = text_of("tui_gateway/contracts/sessions.py")
    resume_params = (cs.split("class SessionResumeParams", 1) + [""])[1][:900]
    common_text = text_of("tui_gateway/contracts/common.py")
    sp = (common_text.split("class SessionParams", 1) + [""])[1][:300]
    for key in ("source", "omit_messages"):
        check(f"session.resume accepts {key}", f"{key}:" in resume_params, "")
    for key in ("session_id", "profile"):
        check(f"session.resume accepts {key} (SessionParams)",
              "SessionResumeParams(SessionParams)" in cs and f"{key}:" in sp,
              "")
    hr = text_of("tui_gateway/hosted_room_server_rpc.py")
    check("Hermes's own hosted rooms call the installed handlers in-process "
          "the same way (server._methods[method](rid, params))",
          "self.server._methods[method]" in hr and '"session.resume"' in hr,
          "")
    stx = text_of("tui_gateway/session_transports.py")
    live = (stx.split("def _transport_is_live_peer", 1) + [""])[1][:600]
    check("a live peer is anything but stdio, the drop sink, the parked "
          "sentinel or a dead transport — so the plugin's _Hold counts",
          all(s in live for s in ("_detached_ws_transport", "_stdio_transport",
                                  "_DropTransport", "StdioTransport",
                                  "_transport_is_dead")), live[:300])
    rp = text_of("tui_gateway/session_reaper.py")
    dead = (rp.split("def _transport_is_dead", 1) + [""])[1][:1500]
    check("a transport is dead when its _closed is True — what _Hold.close "
          "sets", 'getattr(transport, "_closed", None) is True' in dead, "")
    evict = (rp.split("def _session_is_lru_evictable", 1) + [""])[1][:900]
    check("no reaper takes a session on a live transport",
          'return _transport_is_dead(session.get("transport"))' in evict, "")
    check("a fan-out of several clients lists them (transports())",
          find(tree(src, "tui_gateway/transport.py"), ast.FunctionDef,
               "transports") is not None, "")
    check("_close_sessions_for_transport is the disconnect path the plugin "
          "releases through",
          "def _close_sessions_for_transport(transport"
          in text_of("tui_gateway/session_lifecycle.py"), "")
    check("server._profile_home resolves a profile name (None: the launch "
          "home)", "def _profile_home(profile" in srv_text, "")
    check("the dashboard's modules are hermes_cli.web_server_app",
          (src / "hermes_cli" / "web_server_app.py").is_file(), "")

    # When a dashboard is one: the order `hermes serve` starts in. Added
    # 2026-11-01 after a real deploy: a plugin that decided at load never
    # held the default bot's conversation, and a waker that ran at import
    # would have frozen the launch profile's environment before Hermes did.
    def body(text, name, n=6000):
        part = (text.split(f"def {name}(", 1) + [""])[1]
        return part.split("\ndef ", 1)[0][:n]
    main_text = text_of("hermes_cli/main.py")
    prep = body(main_text, "_dashboard_prepare_runtime")
    cmd = body(main_text, "cmd_dashboard")
    check("hermes serve loads the launch profile's plugins in "
          "_dashboard_prepare_runtime (discover_plugins)…",
          "discover_plugins()" in prep, prep[:200])
    check("…BEFORE it imports its web server — so a plugin cannot tell a "
          "dashboard at load, and its hold thread must decide later",
          0 <= cmd.find("_dashboard_prepare_runtime(")
          < cmd.find("from hermes_cli.web_server import start_server"),
          cmd[:400])
    ws = text_of("hermes_cli/web_server.py")
    start = body(ws, "start_server", 20000)
    check("start_server makes the multi-profile decision BEFORE the server "
          "starts (and with it the lifespan that installs the injector)",
          0 <= start.find("activate_multi_profile_hosting_eagerly()")
          < start.find("server.startup()"), "")
    check("the dashboard plugins' api files are imported while web_server is "
          "imported — long before that decision",
          re.search(r"^_mount_plugin_api_routes\(\)\s*$", ws, re.M)
          is not None, "")
    wsa = text_of("hermes_cli/web_server_app.py")
    check("the dashboard's own lifespan installs the desktop injector — the "
          "end of startup the waker waits for",
          "install_tui_message_injector()"
          in body(wsa, "standalone_lifespan", 9000), "")
    check("a gateway that mounts the dashboard runs http_lifespan instead, "
          "which installs no desktop injector",
          "http_lifespan if getattr(app.state, \"gateway_runner\", None) is "
          "not None else standalone_lifespan" in wsa
          and "install_tui_message_injector" not in body(wsa, "http_lifespan"),
          "")
    for prop in ("has_tui_message_injector", "has_gateway_message_injector"):
        fn = find(mgr, ast.FunctionDef, prop)
        check(f"PluginManager.{prop} is a property", fn is not None
              and is_property(fn), "")
    check("server._profile_home of a second profile turns multi-profile on "
          "(why the waker must not run before Hermes decides)",
          "activate_multi_profile_hosting()" in body(srv_text, "_profile_home"),
          "")
    lp = text_of("tui_gateway/launch_profile_policy.py")
    check("and turning it on freezes the launch environment",
          "capture_launch_env()" in body(lp, "activate_multi_profile_hosting"),
          "")
    check("a dashboard plugin's api is imported as "
          "hermes_dashboard_plugin_<name> — where the doctor reads the "
          "waker's status", "f\"hermes_dashboard_plugin_{plugin['name']}\""
          in text_of("hermes_cli/web_server_dashboard.py"), "")

    # The waking routine: the plugin's dashboard part, imported by the
    # dashboard at start, wakes every a2a bot. Added 2026-11-01 against main.
    wd = text_of("hermes_cli/web_server_dashboard.py")
    check("the dashboard scans <plugin>/dashboard/manifest.json",
          '"dashboard" / "manifest.json"' in wd, "")
    check("it imports the manifest's `api` file and mounts its `router`",
          'data.get("api")' in wd and 'getattr(mod, "router", None)' in wd
          and "spec.loader.exec_module(mod)" in wd, "")
    check("only for a plugin in plugins.enabled",
          '"not in plugins.enabled"' in wd, "")
    check("in the dashboard process by default (plugins.isolation: "
          "in_process)", '"isolation": "in_process"'
          in text_of("hermes_cli/config_defaults.py"), "")
    pr = text_of("hermes_cli/profiles.py")
    for fn in ("list_profile_names", "get_profile_dir"):
        check(f"hermes_cli.profiles.{fn} exists", f"def {fn}(" in pr, "")
    check("discover_plugins(force=False): idempotent, never a reload unless "
          "forced", "def discover_plugins(force: bool = False)" in text, "")
    msw = text_of("tui_gateway/model_switch.py")
    check("_session_profile_runtime_scope is defined in "
          "tui_gateway/model_switch.py…",
          "def _session_profile_runtime_scope(session" in msw, "")
    check("…a split module, copied onto the session server "
          "(bind_module), which server.py registers",
          "bind_module(globals(), server" in body(msw, "register")
          and "_model_switch" in srv_text, "")
    check("…whose body names server globals it does not define itself "
          "(_TurnScopes) — so only the SERVER's copy can run",
          "_TurnScopes()" in msw
          and re.search(r"^class _TurnScopes\b", msw, re.M) is None, "")
    real_scope_check(src)
    api_code = API.read_text().split('"""', 2)[-1]
    check("the waker never imports tui_gateway.model_switch",
          re.search(r"from\s+tui_gateway\.model_switch\s+import|"
                    r"from\s+tui_gateway\s+import\s+model_switch|"
                    r"import\s+tui_gateway\.model_switch", api_code) is None,
          "")
    check("the waker enters the server's _session_profile_runtime_scope",
          '"_session_profile_runtime_scope"' in api_code, "")
    check("plugins_cmd._get_enabled_set reads plugins.enabled",
          re.search(r"_get_enabled_set\s*=.*\"plugins\",\s*\"enabled\"",
                    text_of("hermes_cli/plugins_cmd.py")) is not None, "")

    # plugin.yaml: every key the plugin's manifest uses is one Hermes reads.
    manifest_src = ""
    for rel in ("hermes_cli/plugins.py", "hermes_cli/plugins_manifest.py",
                "hermes_cli/plugins_loader.py",
                "hermes_cli/plugins_discovery.py"):
        p = src / rel
        if p.is_file():
            manifest_src += p.read_text()
    keys = re.findall(r"^([a-z_]+):", MANIFEST.read_text(), re.M)
    for key in keys:
        check(f"plugin.yaml key {key!r} is read by Hermes",
              re.search(rf"['\"]{key}['\"]|\b{key}\b\s*:", manifest_src)
              is not None, "")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — every Hermes name the plugin uses exists in this checkout")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
