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
it was asked in which profile scope. What is pinned:

  manifest   dashboard/manifest.json names api.py, is hidden, and its entry
             exists; the module exposes `router` with GET /status.
  no server  in a process with no session server (a plugin host, or not a
             dashboard) it waits, says so, and wakes nothing.
  scope      each profile is woken INSIDE its own scope — server._profile_home
             then _session_profile_runtime_scope, as a profile-scoped
             dashboard call does — and only if a2a is in that profile's
             plugins.enabled.
  no reload  discover_plugins is never called with force=True, and a rescan
             loads nothing twice.
  isolation  a profile that fails does not stop the others.
  once       importing the file again starts no second thread.

Contract with Hermes itself — that these names exist — is
tests/test_hermes_contract.py.
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
def _profile_scope(session):
    token = _SCOPE.set(session.get("profile_home") or "LAUNCH")
    try:
        yield
    finally:
        _SCOPE.reset(token)


def _profile_home(name):
    if name == "ghost":
        raise LookupError("Profile 'ghost' does not exist.")
    return None if name == "default" else f"/homes/{name}"


class APIRouter:
    def __init__(self):
        self.routes = {}

    def get(self, path):
        def deco(fn):
            self.routes[("GET", path)] = fn
            return fn
        return deco


def install_fakes():
    hermes_cli = types.ModuleType("hermes_cli")
    profiles = types.ModuleType("hermes_cli.profiles")
    profiles.list_profile_names = lambda: list(PROFILES)
    plugins = types.ModuleType("hermes_cli.plugins")
    plugins.discover_plugins = _discover
    plugins_cmd = types.ModuleType("hermes_cli.plugins_cmd")
    plugins_cmd._get_enabled_set = (
        lambda: set(ENABLED.get(_SCOPE.get() or "", set())))
    model_switch = types.ModuleType("tui_gateway.model_switch")
    model_switch._session_profile_runtime_scope = _profile_scope
    fastapi = types.ModuleType("fastapi")
    fastapi.APIRouter = APIRouter
    sys.modules.update({
        "hermes_cli": hermes_cli, "hermes_cli.profiles": profiles,
        "hermes_cli.plugins": plugins, "hermes_cli.plugins_cmd": plugins_cmd,
        "tui_gateway.model_switch": model_switch, "fastapi": fastapi})


def install_server():
    srv = types.ModuleType("tui_gateway.server")
    srv._profile_home = _profile_home
    pkg = types.ModuleType("tui_gateway")
    pkg.server = srv
    sys.modules.update({"tui_gateway": pkg, "tui_gateway.server": srv})
    return srv


def load(name="hermes_dashboard_plugin_a2a"):
    """As the dashboard imports it: by file, under its module name."""
    spec = importlib.util.spec_from_file_location(name, API)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[name] = mod
    spec.loader.exec_module(mod)
    return mod


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

    # --- no session server: nothing to wake --------------------------------------
    stop = threading.Event()
    mod.run(stop, wait_s=0.3, poll_s=0.05)
    check("no server: it waits, says why, and wakes nothing",
          CALLS == [] and "no Hermes session server" in (mod.STATUS["note"]
                                                          or ""),
          f"{CALLS} {mod.STATUS['note']}")

    # --- one pass ---------------------------------------------------------------
    srv = install_server()
    woken = mod.one_pass(srv)
    check("scope: wakes exactly the profiles with a2a enabled",
          woken == ["default", "alpha", "gamma"], str(woken))
    check("scope: each inside its OWN profile scope (the launch profile in "
          "its own), never in another's",
          sorted(s for s, _ in CALLS) == sorted(
              ["LAUNCH", "/homes/alpha", "/homes/gamma", "/homes/delta"]),
          str(CALLS))
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
    status = mod.router.routes[("GET", "/status")]()
    check("status: GET /status reports the pass",
          status["woken"] == ["default", "alpha", "gamma"]
          and status["passes"] == 1, str(status))

    # --- the timer: a rescan loads nothing twice -----------------------------------
    CALLS.clear()
    stop = threading.Event()
    t = threading.Thread(target=mod.run, args=(stop,),
                         kwargs={"wait_s": 1, "rescan_s": 0.1, "poll_s": 0.05})
    t.start()
    end = time.time() + 5
    while time.time() < end and mod.STATUS["passes"] < 4:
        time.sleep(0.02)
    stop.set()
    t.join(5)
    check("timer: it passes again on its own",
          mod.STATUS["passes"] >= 4, str(mod.STATUS["passes"]))
    check("no reload: across every pass each profile's plugins were loaded "
          "once, and nothing was forced",
          all(n == 1 for n in LOADED.values())
          and all(force is False for _, force in CALLS), f"{LOADED}")

    # --- one thread per process ------------------------------------------------------
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
          "PASS — the dashboard wakes every a2a bot, each in its own scope")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
