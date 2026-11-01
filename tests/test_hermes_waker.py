#!/usr/bin/env python3
"""a2a's dashboard part wakes every a2a bot when Hermes's dashboard starts.

    python3 tests/test_hermes_waker.py

A bot's a2a plugin keeps its conversation loaded in the dashboard (the hold,
tests/test_hermes_client.py), but a dashboard starts a profile's plugins only
when something first runs as that profile. deploy/plugin/hermes/dashboard/
api.py is the plugin's own entry point at dashboard start: Hermes imports a
dashboard plugin's `api` file there (web_server_dashboard.py
_mount_plugin_api_routes) and mounts its `router`.

The real file is loaded the way the dashboard loads it (spec_from_file_location
+ exec_module), against fakes of the Hermes names it uses, each recording what
it was asked in which profile scope. The fakes fail the way Hermes does — a
fake that agrees with the plugin's mistakes proves nothing (the first version
of this file passed while the real waker crashed on every profile):

  manifest   dashboard/manifest.json names api.py, is hidden, and its entry
             exists; the module exposes `router` with GET /status.
  ready      nothing is woken before the dashboard has FINISHED starting —
             its session server up AND the desktop injector installed by its
             lifespan, which runs after Hermes's own multi-profile decision
             (web_server.py start_server). The session server alone is not
             enough: in Hermes it is importable long before that.
  gateway    a process with the messaging gateway's injector and not the
             desktop one stops at once, saying so, and wakes nothing.
  no server  a process that never finishes starting a dashboard (a plugin
             host) waits, says so, and wakes nothing.
  scope      each profile is woken INSIDE its own scope — server._profile_home
             then the SERVER's _session_profile_runtime_scope. The fake
             tui_gateway.model_switch copy raises NameError: _TurnScopes,
             exactly as Hermes's does (its functions run only in the copy
             method_ctx.bind_module publishes onto the server). Only a profile
             with a2a in its plugins.enabled is woken.
  stagger    the first pass waits STAGGER_S after each bot it woke; rescans
             do not wait.
  no reload  discover_plugins is never called with force=True, and a rescan
             loads nothing twice.
  isolation  a profile that fails does not stop the others.
  once       importing the file again starts no second thread.

Contract with Hermes itself — that these names exist, and behave so — is
tests/test_hermes_contract.py, and the real thing is tests/test_hermes_real.py.
"""
import contextlib
import contextvars
import importlib.util
import json
import sys
import threading
import time
import types
from pathlib import Path

sys.dont_write_bytecode = True

HERE = Path(__file__).resolve().parent
DASH = HERE.parent / "deploy" / "plugin" / "hermes" / "dashboard"
API = DASH / "api.py"
MANIFEST = DASH / "manifest.json"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


# --- Hermes, faked -----------------------------------------------------------
_SCOPE: contextvars.ContextVar = contextvars.ContextVar("scope", default=None)
PROFILES = ["default", "alpha", "beta", "gamma", "delta", "ghost"]
ENABLED = {"LAUNCH": {"a2a"}, "/homes/alpha": {"a2a", "other"},
           "/homes/beta": {"other"}, "/homes/gamma": {"a2a"},
           "/homes/delta": {"a2a"}}
CALLS: list = []          # (scope, force) of every discover_plugins call
LOADED: dict = {}         # scope -> times a profile's plugins were loaded


def _discover(force=False):
    scope = _SCOPE.get()
    CALLS.append((scope, force))
    if scope == "/homes/delta":
        raise RuntimeError("delta's plugins failed to load")
    if force or scope not in LOADED:      # Hermes's own idempotence
        LOADED[scope] = LOADED.get(scope, 0) + 1


@contextlib.contextmanager
def _server_scope(session):
    """The server's copy: bound to the server's globals, it works."""
    token = _SCOPE.set(session.get("profile_home") or "LAUNCH")
    try:
        yield
    finally:
        _SCOPE.reset(token)


@contextlib.contextmanager
def _module_scope(session):
    """tui_gateway.model_switch's own copy: its body names server globals
    bare, so — as in Hermes — it fails before it binds anything."""
    raise NameError("name '_TurnScopes' is not defined")
    yield


def _profile_home(name):
    if name == "ghost":
        raise LookupError("Profile 'ghost' does not exist.")
    return None if name == "default" else f"/homes/{name}"


class Manager:
    """The launch profile's plugin manager: which injectors it carries."""
    has_tui_message_injector = False
    has_gateway_message_injector = False


MANAGER = Manager()


class APIRouter:
    def __init__(self):
        self.routes = {}

    def get(self, path):
        def deco(fn):
            self.routes[("GET", path)] = fn
            return fn
        return deco


class Stop(threading.Event):
    """A stop event that remembers every wait it was asked for."""

    def __init__(self):
        super().__init__()
        self.waits = []

    def wait(self, timeout=None):
        self.waits.append(timeout)
        return super().wait(timeout)


def install_fakes():
    hermes_cli = types.ModuleType("hermes_cli")
    profiles = types.ModuleType("hermes_cli.profiles")
    profiles.list_profile_names = lambda: list(PROFILES)
    plugins = types.ModuleType("hermes_cli.plugins")
    plugins.discover_plugins = _discover
    plugins.get_plugin_manager = lambda: MANAGER
    plugins_cmd = types.ModuleType("hermes_cli.plugins_cmd")
    plugins_cmd._get_enabled_set = (
        lambda: set(ENABLED.get(_SCOPE.get() or "", set())))
    model_switch = types.ModuleType("tui_gateway.model_switch")
    model_switch._session_profile_runtime_scope = _module_scope
    pkg = types.ModuleType("tui_gateway")
    pkg.model_switch = model_switch
    fastapi = types.ModuleType("fastapi")
    fastapi.APIRouter = APIRouter
    sys.modules.update({
        "hermes_cli": hermes_cli, "hermes_cli.profiles": profiles,
        "hermes_cli.plugins": plugins, "hermes_cli.plugins_cmd": plugins_cmd,
        "tui_gateway": pkg, "tui_gateway.model_switch": model_switch,
        "fastapi": fastapi})


def install_server(scoped=True):
    srv = types.ModuleType("tui_gateway.server")
    srv._profile_home = _profile_home
    if scoped:
        srv._session_profile_runtime_scope = _server_scope
    sys.modules["tui_gateway"].server = srv
    sys.modules["tui_gateway.server"] = srv
    return srv


def load(name="hermes_dashboard_plugin_a2a"):
    """As the dashboard imports it: by file, under its module name."""
    spec = importlib.util.spec_from_file_location(name, API)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


def reset(mod):
    CALLS.clear()
    LOADED.clear()
    mod.STATUS.update(started=None, server=False, waited=None, ready_at=None,
                      first_pass_at=None, passes=0, last_pass=None, woken=[],
                      skipped=[], errors={}, note=None)


def until(cond, timeout=5.0):
    end = time.time() + timeout
    while time.time() < end:
        if cond():
            return True
        time.sleep(0.01)
    return cond()


def main() -> int:
    install_fakes()

    # --- manifest -------------------------------------------------------------
    m = json.loads(MANIFEST.read_text())
    check("manifest: named a2a, hidden, its api is api.py and exists, its "
          "entry exists",
          m.get("name") == "a2a" and (m.get("tab") or {}).get("hidden") is True
          and m.get("api") == "api.py" and API.is_file()
          and (DASH / m.get("entry", "")).is_file(), str(m))

    # Loaded with the per-process thread already marked started: the test
    # drives run() itself.
    sys._a2a_waker_started = True
    mod = load()
    check("module: exposes `router` with GET /status — what Hermes mounts",
          isinstance(mod.router, APIRouter)
          and ("GET", "/status") in mod.router.routes,
          str(getattr(mod.router, "routes", None)))
    check("module: does not import tui_gateway.model_switch (its functions "
          "run only on the server)",
          "model_switch" not in API.read_text().split('"""', 2)[-1]
          .replace("tui_gateway.model_switch's", ""), "api.py names it")

    # --- no dashboard ever: nothing to wake ---------------------------------------
    mod.run(Stop(), wait_s=0.3, poll_s=0.05)
    check("no server: it waits, says why, and wakes nothing",
          CALLS == [] and mod.STATUS["waited"] == "timeout"
          and "never finished starting a dashboard" in (mod.STATUS["note"]
                                                       or ""),
          f"{CALLS} {mod.STATUS}")

    # --- a messaging gateway: stop at once ------------------------------------
    reset(mod)
    MANAGER.has_gateway_message_injector = True
    t0 = time.time()
    mod.run(Stop(), wait_s=30, poll_s=0.05)
    check("gateway: with only the gateway's injector it stops at once, says "
          "so, and wakes nothing",
          CALLS == [] and mod.STATUS["waited"] == "gateway"
          and "messaging gateway" in (mod.STATUS["note"] or "")
          and time.time() - t0 < 5, f"{CALLS} {mod.STATUS}")
    MANAGER.has_gateway_message_injector = False

    # --- the session server alone is not the end of startup ---------------------
    reset(mod)
    srv = install_server()
    stop = Stop()
    t = threading.Thread(target=mod.run, args=(stop,), kwargs={
        "wait_s": 30, "poll_s": 0.02, "rescan_s": 30, "stagger_s": 0.011})
    t.start()
    time.sleep(0.4)
    check("ready: with the session server up but no desktop injector yet, "
          "nothing is woken", CALLS == [] and mod.STATUS["passes"] == 0,
          f"{CALLS} {mod.STATUS}")
    injected_at = time.time()
    MANAGER.has_tui_message_injector = True
    check("ready: once the lifespan installed the desktop injector, it wakes",
          until(lambda: mod.STATUS["passes"] >= 1), str(mod.STATUS))
    stop.set()
    t.join(5)
    check("ready: its first pass came AFTER the injector was installed",
          (mod.STATUS["first_pass_at"] or 0) >= injected_at
          and mod.STATUS["waited"] == "ready", str(mod.STATUS))

    # --- scope ----------------------------------------------------------------
    check("scope: wakes exactly the profiles with a2a enabled",
          mod.STATUS["woken"] == ["default", "alpha", "gamma"],
          str(mod.STATUS["woken"]))
    check("scope: each inside its OWN profile scope (the launch profile in "
          "its own), never in another's — through the server's scope, not "
          "model_switch's",
          sorted(s for s, _ in CALLS) == sorted(
              ["LAUNCH", "/homes/alpha", "/homes/gamma", "/homes/delta"])
          and not any("_TurnScopes" in e
                      for e in mod.STATUS["errors"].values()),
          f"{CALLS} {mod.STATUS['errors']}")
    check("scope: a profile without a2a is not woken at all",
          "/homes/beta" not in [s for s, _ in CALLS]
          and mod.STATUS["skipped"] == ["beta"], str(mod.STATUS))
    check("isolation: a profile that fails, or does not exist, is recorded "
          "and the rest still woken",
          set(mod.STATUS["errors"]) == {"delta", "ghost"}
          and "delta's plugins failed" in mod.STATUS["errors"]["delta"],
          str(mod.STATUS["errors"]))
    check("no reload: never force=True",
          all(force is False for _, force in CALLS), str(CALLS))
    check("stagger: the first pass waited STAGGER_S after each bot it woke",
          stop.waits.count(0.011) == 3, str(stop.waits))
    status = mod.router.routes[("GET", "/status")]()
    check("status: GET /status reports the pass and when it was ready",
          status["woken"] == ["default", "alpha", "gamma"]
          and status["passes"] == 1 and status["ready_at"]
          and status["waited"] == "ready", str(status))

    # --- a Hermes whose server has no profile scope -----------------------------
    reset(mod)
    bare = install_server(scoped=False)
    mod.one_pass(bare)
    check("scope: a server without _session_profile_runtime_scope wakes "
          "nothing, and says why for each profile",
          CALLS == [] and all("no profile scope" in e for e in
                              mod.STATUS["errors"].values())
          and set(mod.STATUS["errors"]) == set(PROFILES),
          f"{CALLS} {mod.STATUS['errors']}")

    # --- the timer: a rescan loads nothing twice, and does not stagger -------------
    reset(mod)
    srv = install_server()
    stop = Stop()
    t = threading.Thread(target=mod.run, args=(stop,), kwargs={
        "wait_s": 1, "rescan_s": 0.1, "poll_s": 0.05, "stagger_s": 0.011})
    t.start()
    until(lambda: mod.STATUS["passes"] >= 4)
    stop.set()
    t.join(5)
    check("timer: it passes again on its own",
          mod.STATUS["passes"] >= 4, str(mod.STATUS["passes"]))
    check("stagger: only the first pass waits between bots",
          stop.waits.count(0.011) == 3, str(stop.waits))
    check("no reload: across every pass each profile's plugins were loaded "
          "once, and nothing was forced",
          all(n == 1 for n in LOADED.values())
          and all(force is False for _, force in CALLS), f"{LOADED}")

    # --- one thread per process ------------------------------------------------------
    MANAGER.has_tui_message_injector = False      # the new thread just waits
    sys._a2a_waker_started = False
    before = {th.ident for th in threading.enumerate()}
    load("hermes_dashboard_plugin_a2a")
    load("hermes_dashboard_plugin_a2a")       # a hot mount imports it again
    time.sleep(0.1)
    new = [th for th in threading.enumerate()
           if th.ident not in before and th.name == "a2a-waker"]
    check("once: importing it twice starts ONE waker thread", len(new) == 1,
          str(new))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — the dashboard wakes every a2a bot, each in its own scope, "
          "once it has finished starting")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
