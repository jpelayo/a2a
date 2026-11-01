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
(hermes_cli/web_server_dashboard.py _mount_plugin_api_routes). Importing it
starts one thread, which:

  1. waits for the dashboard's session server (tui_gateway.server), which the
     dashboard's lifespan brings up after the app is built;
  2. for every profile on this host (hermes_cli.profiles.list_profile_names),
     enters that profile's scope exactly as a profile-scoped dashboard call
     does — server._profile_home(name), then
     tui_gateway.model_switch._session_profile_runtime_scope — and, if the
     profile has a2a in plugins.enabled, runs Hermes's own plugin discovery
     there (hermes_cli.plugins.discover_plugins). That is idempotent: plugins
     already running are left alone, never reloaded. The bot's a2a plugin then
     starts and takes its own hold, by its own rules (off, copied settings,
     route);
  3. does it again every RESCAN_S, for bots enabled since.

Nothing here reads or writes a bot's a2a state, and nothing streams: waking a
bot is all it does. Waking a profile loads all of its enabled plugins in the
dashboard, as its first use would. Under plugins.isolation: host this file runs
in a plugin host, where there is no session server: it says so and does
nothing.

`router` is what Hermes mounts at /api/plugins/a2a/: GET status reports the
last pass.
"""
from __future__ import annotations

import logging
import sys
import threading
import time

PLUGIN = "a2a"
WAIT_S = 120.0          # for the session server, after the dashboard starts
POLL_S = 1.0
RESCAN_S = 300.0

log = logging.getLogger("hermes_plugins.a2a.dashboard")

STATUS = {"started": None, "server": False, "passes": 0, "last_pass": None,
          "woken": [], "skipped": [], "errors": {}, "note": None}


def _server():
    """The dashboard's session server, once it is up; None until then."""
    srv = sys.modules.get("tui_gateway.server")
    return srv if callable(getattr(srv, "_profile_home", None)) else None


def wake(srv, name):
    """Start profile `name`'s plugins here if a2a is enabled in it: "woken",
    or "skipped" when a2a is not enabled there. Raises what Hermes raises."""
    from hermes_cli.plugins import discover_plugins
    from hermes_cli.plugins_cmd import _get_enabled_set
    from tui_gateway.model_switch import _session_profile_runtime_scope
    home = srv._profile_home(name)            # None: the launch profile
    with _session_profile_runtime_scope(
            {"profile_home": str(home) if home else None}):
        if PLUGIN not in _get_enabled_set():
            return "skipped"
        discover_plugins()
    return "woken"


def one_pass(srv):
    from hermes_cli.profiles import list_profile_names
    woken, skipped, errors = [], [], {}
    for name in list_profile_names():
        try:
            (woken if wake(srv, name) == "woken" else skipped).append(name)
        except Exception as e:
            errors[name] = f"{type(e).__name__}: {e}"
    # Said when it changes, not on every pass: a profile that keeps failing
    # must not write the same warning every RESCAN_S.
    if errors and errors != STATUS["errors"]:
        log.warning("a2a: could not wake %s", errors)
    STATUS.update(passes=STATUS["passes"] + 1, last_pass=time.time(),
                  woken=woken, skipped=skipped, errors=errors)
    return woken


def run(stop=None, wait_s=WAIT_S, rescan_s=RESCAN_S, poll_s=POLL_S):
    stop = stop or threading.Event()
    STATUS["started"] = time.time()
    deadline = time.monotonic() + wait_s
    srv = _server()
    while srv is None:
        if time.monotonic() >= deadline or stop.wait(poll_s):
            STATUS["note"] = ("no Hermes session server in this process (a "
                              "plugin host, or not a dashboard): nothing to "
                              "wake here")
            log.info("a2a: %s", STATUS["note"])
            return
        srv = _server()
    STATUS["server"] = True
    while True:
        try:
            woken = one_pass(srv)
            if STATUS["passes"] == 1:
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
