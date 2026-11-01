"""a2a's dashboard part: wakes every a2a bot when Hermes's dashboard starts.

WHY. a2a delivers into a bot's conversation only while that conversation is
loaded in the dashboard, so each bot's a2a plugin keeps its own conversation
loaded there (the hold, in ../__init__.py). But a dashboard starts a profile's
plugins lazily — the first time something runs as that profile (a turn builds
its agent and that runs plugin discovery: tui_gateway/agent_factory.py) — and
Hermes has no startup hook for plugins. After a restart, a bot nobody opens
would wait.

HOW. Hermes imports this file when the dashboard starts: a dashboard plugin's
`api` module, for a plugin in the launch profile's plugins.enabled, in the
dashboard process itself under the default plugins.isolation: in_process
(hermes_cli/web_server_dashboard.py _mount_plugin_api_routes, run while
hermes_cli.web_server is imported). Importing it starts one thread, which:

  1. waits for the END of the dashboard's startup: the desktop injector on the
     launch profile's plugin manager (has_tui_message_injector), which the
     dashboard's lifespan installs (hermes_cli/web_server_app.py
     standalone_lifespan). Not earlier: this file is imported long before
     start_server makes Hermes's own multi-profile decision, "LAST boot step,
     deliberately" (web_server.py, activate_multi_profile_hosting_eagerly) —
     that freezes the default profile's environment, and resolving a second
     profile (server._profile_home) would freeze it early. A messaging
     gateway that mounts the dashboard runs another lifespan and never gets
     that injector; seeing the gateway's own injector instead, it stops —
     no app conversation lives there;
  2. for every profile on this host (hermes_cli.profiles.list_profile_names),
     enters that profile's scope exactly as Hermes's own code does —
     server._profile_home(name), then the SERVER's
     _session_profile_runtime_scope. Never tui_gateway.model_switch's: that
     module's functions use the server's names bare and run only in the copy
     Hermes publishes onto the server (method_ctx.bind_module); called from
     the module they raise NameError: _TurnScopes. Inside, if the profile has
     a2a in plugins.enabled, it runs Hermes's own plugin discovery
     (hermes_cli.plugins.discover_plugins). That is idempotent: plugins
     already running are left alone, never reloaded. The bot's a2a plugin
     then starts and takes its own hold, by its own rules (off, copied
     settings, route). The first pass wakes them STAGGER_S apart: each hold
     builds that bot's agent, and all at once at boot would stall the
     dashboard;
  3. does it again every RESCAN_S, for bots enabled since.

Nothing here reads or writes a bot's a2a state, and nothing streams: waking a
bot is all it does. Waking a profile loads all of its enabled plugins in the
dashboard, as its first use would. Under plugins.isolation: host this file runs
in a plugin host, where there is no session server: it says so and does
nothing.

`router` is what Hermes mounts at /api/plugins/a2a/: GET status reports the
last pass. Each bot's a2a_channel_status shows the same (push.hold.waker).
"""
from __future__ import annotations

import logging
import sys
import threading
import time

PLUGIN = "a2a"
WAIT_S = 900.0          # for the end of the dashboard's startup
POLL_S = 2.0
STAGGER_S = 2.0         # between bots, on the first pass
RESCAN_S = 300.0

log = logging.getLogger("hermes_plugins.a2a.dashboard")

STATUS = {"started": None, "server": False, "waited": None, "ready_at": None,
          "first_pass_at": None, "passes": 0, "last_pass": None,
          "woken": [], "skipped": [], "errors": {}, "note": None}


def _server():
    """The dashboard's session server, once it is up; None until then."""
    srv = sys.modules.get("tui_gateway.server")
    return srv if callable(getattr(srv, "_profile_home", None)) else None


def _ready():
    """"ready" once this dashboard has finished starting (its session server
    is up and its lifespan installed the desktop injector), "gateway" in a
    messaging gateway (its own injector, never the desktop one), else None."""
    try:
        from hermes_cli.plugins import get_plugin_manager
        mgr = get_plugin_manager()
        if _server() is not None and getattr(
                mgr, "has_tui_message_injector", False):
            return "ready"
        if getattr(mgr, "has_gateway_message_injector", False):
            return "gateway"
    except Exception:
        pass
    return None


def wake(srv, name):
    """Start profile `name`'s plugins here if a2a is enabled in it: "woken",
    or "skipped" when a2a is not enabled there. Raises what Hermes raises."""
    from hermes_cli.plugins import discover_plugins
    from hermes_cli.plugins_cmd import _get_enabled_set
    scope = getattr(srv, "_session_profile_runtime_scope", None)
    if not callable(scope):
        raise RuntimeError("this Hermes has no profile scope on its session "
                           "server (_session_profile_runtime_scope)")
    home = srv._profile_home(name)            # None: the launch profile
    with scope({"profile_home": str(home) if home else None}):
        if PLUGIN not in _get_enabled_set():
            return "skipped"
        discover_plugins()
    return "woken"


def one_pass(srv, stop=None, stagger_s=0.0):
    from hermes_cli.profiles import list_profile_names
    woken, skipped, errors = [], [], {}
    for name in list_profile_names():
        if stop is not None and stop.is_set():
            break
        try:
            result = wake(srv, name)
        except Exception as e:
            errors[name] = f"{type(e).__name__}: {e}"
            continue
        if result == "woken":
            woken.append(name)
            # Only after a bot that was woken: its hold builds its agent.
            if stagger_s and stop is not None:
                stop.wait(stagger_s)
        else:
            skipped.append(name)
    # Said when it changes, not on every pass: a profile that keeps failing
    # must not write the same warning every RESCAN_S.
    if errors and errors != STATUS["errors"]:
        log.warning("a2a: could not wake %s", errors)
    STATUS.update(passes=STATUS["passes"] + 1, last_pass=time.time(),
                  woken=woken, skipped=skipped, errors=errors)
    return woken


def run(stop=None, wait_s=WAIT_S, rescan_s=RESCAN_S, poll_s=POLL_S,
        stagger_s=STAGGER_S):
    stop = stop or threading.Event()
    STATUS["started"] = time.time()
    deadline = time.monotonic() + wait_s
    state = _ready()
    while state != "ready":
        if state == "gateway":
            STATUS.update(waited="gateway", note=(
                "a messaging gateway: no conversation of the Hermes app "
                "lives here, so there is nothing to wake"))
            log.info("a2a: %s", STATUS["note"])
            return
        if time.monotonic() >= deadline or stop.wait(poll_s):
            STATUS.update(waited="timeout", note=(
                "this process never finished starting a dashboard (no "
                "session server with the desktop injector: a plugin host, "
                "or not a dashboard): nothing to wake here"))
            log.info("a2a: %s", STATUS["note"])
            return
        state = _ready()
    srv = _server()
    STATUS.update(server=True, waited="ready", ready_at=time.time())
    while True:
        try:
            first = STATUS["passes"] == 0
            if first:
                STATUS["first_pass_at"] = time.time()
            woken = one_pass(srv, stop, stagger_s if first else 0.0)
            if first:
                log.info("a2a: woke the a2a bots of this dashboard: %s",
                         ", ".join(woken) or "none")
        except Exception as e:
            STATUS["note"] = f"{type(e).__name__}: {e}"
            log.warning("a2a: waking pass failed: %r", e)
        if stop.wait(rescan_s):
            return


def _status():
    return {k: (list(v) if isinstance(v, list) else
                dict(v) if isinstance(v, dict) else v)
            for k, v in STATUS.items()}


def _router():
    try:
        from fastapi import APIRouter
    except Exception:
        return None
    r = APIRouter()

    @r.get("/status")
    def status():
        return _status()
    return r


router = _router()

# One thread per process, however often this file is imported (a hot mount
# imports it again under the same module name).
if not getattr(sys, "_a2a_waker_started", False):
    sys._a2a_waker_started = True
    threading.Thread(target=run, name="a2a-waker", daemon=True).start()
