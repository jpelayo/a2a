#!/usr/bin/env python3
"""Runs INSIDE the real Hermes, for tests/test_hermes_real.py — with the
Python of a venv where the Hermes checkout is installed (HERMES_PY). Never by
hand, and never against a real home: the test runs it with HOME and
HERMES_HOME inside a temp dir.

    setup <spec.json>                 builds the Hermes home the spec
                                      describes, with Hermes's OWN APIs only
                                      (profiles, config, state.db).
    serve <spec.json> <report.json> <stop-file>
                                      runs `hermes serve` the way the desktop
                                      app starts it — Hermes's fast serve
                                      path, its own boot order — beside an
                                      inspector thread that writes what the
                                      test asserts on, then stops Hermes the
                                      way an operator does (SIGTERM).

The inspector only READS Hermes's state (sys.modules, the session table): it
never calls into the plugin or changes anything Hermes holds.
"""
import json
import os
import shutil
import signal
import sys
import threading
import time
from pathlib import Path

sys.dont_write_bytecode = True


def setup(spec: dict) -> None:
    root = Path(os.environ["HERMES_HOME"])
    root.mkdir(parents=True, exist_ok=True)
    from hermes_cli.config import load_config, save_config
    from hermes_cli.profiles import create_profile
    from hermes_constants import (reset_hermes_home_override,
                                  set_hermes_home_override)
    from hermes_state import SessionDB

    plugin = Path(spec["plugin"])
    for bot in spec["bots"]:
        name = bot["profile"]
        home = root if name == "default" else Path(
            create_profile(name, no_skills=True))
        # What the installer writes: the whole plugin folder, dashboard
        # part included, in every profile.
        shutil.copytree(plugin, home / "plugins" / "a2a",
                        ignore=shutil.ignore_patterns("__pycache__",
                                                      ".DS_Store"))
        token = set_hermes_home_override(str(home))
        try:
            cfg = load_config()
            plugins = cfg.setdefault("plugins", {})
            enabled = [p for p in (plugins.get("enabled") or []) if p != "a2a"]
            if bot["a2a"]:
                enabled.append("a2a")
            plugins["enabled"] = enabled
            entry = plugins.setdefault("entries", {}).setdefault("a2a", {})
            entry["allow_gateway_injection"] = True
            # The plugin's legacy settings place, read until its first write:
            # a2a on, in this bot's own app conversation.
            settings = {"enabled": True,
                        "route": {"surface": "desktop",
                                  "session_key": bot["key"],
                                  "ui_session_id": "", "source": "desktop"}}
            if bot.get("agent"):
                settings["agent"] = bot["agent"]
            entry["settings"] = settings
            cfg["model"] = dict(spec["model"])
            save_config(cfg)
            db = SessionDB(home / "state.db")
            try:
                db.create_session(bot["key"], source="desktop")
            finally:
                db.close()
        finally:
            reset_hermes_home_override(token)
        print(f"setup: {name} at {home}", flush=True)


def _describe(record):
    if not isinstance(record, dict):
        return None
    t = record.get("transport")
    try:
        peers = t.transports() if callable(getattr(t, "transports", None)) \
            else [t]
    except Exception:
        peers = [t]
    names = [type(p).__name__ for p in peers]
    return {"profile_home": record.get("profile_home"), "transports": names,
            "held": "_Hold" in names}


def _write(path: Path, data: dict) -> None:
    tmp = path.with_suffix(".tmp")
    tmp.write_text(json.dumps(data, default=str, indent=1))
    os.replace(tmp, path)


def serve(spec: dict, report: Path, stop: Path) -> None:
    bots = spec["bots"]
    state = {"injector_at": None, "multiplex_at_injector": None,
             "waker_passes_at_injector": None, "since": {}}

    def waker_status():
        status = getattr(sys.modules.get("hermes_dashboard_plugin_a2a"),
                         "STATUS", None)
        return dict(status) if isinstance(status, dict) else None

    def inspect():
        while True:
            plugins = sys.modules.get("hermes_cli.plugins")
            if (state["injector_at"] is None and plugins is not None and
                    getattr(plugins, "_published_tui_message_injector",
                            None) is not None):
                state["injector_at"] = time.time()
                w = waker_status()
                state["waker_passes_at_injector"] = (w or {}).get("passes", 0)
                try:
                    from agent.secret_scope import is_multiplex_active
                    state["multiplex_at_injector"] = bool(is_multiplex_active())
                except Exception as e:
                    state["multiplex_at_injector"] = repr(e)
            held = {}
            srv = sys.modules.get("tui_gateway.server")
            if srv is not None:
                try:
                    with srv._sessions_lock:
                        rows = list(srv._sessions.values())
                except Exception:
                    rows = []
                for b in bots:
                    rec = next((r for r in rows if isinstance(r, dict)
                                and r.get("session_key") == b["key"]), None)
                    held[b["profile"]] = _describe(rec)
                    if (held[b["profile"]] or {}).get("held"):
                        state["since"].setdefault(b["profile"], time.time())
                    else:
                        state["since"].pop(b["profile"], None)
            _write(report, {**state, "now": time.time(),
                            "waker": waker_status(), "records": held})
            if stop.exists():
                break
            # Fast until the injector is there: the order matters to the test.
            time.sleep(0.01 if state["injector_at"] is None else 0.5)
        os.kill(os.getpid(), signal.SIGTERM)

    threading.Thread(target=inspect, name="e2e-inspector", daemon=True).start()
    from hermes_cli.main import _try_fast_serve_launch
    sys.argv = ["hermes", "serve", "--host", "127.0.0.1", "--port", "0"]
    if not _try_fast_serve_launch():
        raise SystemExit("hermes serve's fast path refused this argv")


def main() -> int:
    mode = sys.argv[1]
    spec = json.loads(Path(sys.argv[2]).read_text())
    if mode == "setup":
        setup(spec)
        return 0
    if mode == "serve":
        serve(spec, Path(sys.argv[3]), Path(sys.argv[4]))
        return 0
    raise SystemExit(f"unknown mode {mode!r}")


if __name__ == "__main__":
    sys.exit(main())
