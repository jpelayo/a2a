#!/usr/bin/env python3
"""A stale install must say so, once.

    python3 tests/test_client_version.py

`propose_me` was in the source and absent from the running channel's tool
list, and nothing anywhere reported it. A client is a copy on somebody's disk:
rebuilding the broker cannot update it, and `/reload-plugins` deliberately
keeps connections whose config has not changed — so an install can run for
weeks against a broker that has moved on, and the only symptom is a tool that
quietly is not there.

The time that cost went into working out why, rather than into the one command
that fixes it. This is the line that would have skipped the detour.

Two things it must not do: spend a model turn on it, and say it more than once.

Needs python3. No broker and no database — the broker is faked here.
"""
import json
import os
import subprocess
import sys
import tempfile
import threading
import time
import http.server
from pathlib import Path

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
CHANNEL = PLUGIN / "a2a" / "server" / "a2a-channel.py"
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def fake_broker(serves: str):
    """A broker that reports `serves` as its client-tree version."""
    class H(http.server.BaseHTTPRequestHandler):
        def log_message(self, *a):
            pass

        def do_GET(self):
            if self.path.startswith("/healthz"):
                body = json.dumps({"ok": True, "version": serves,
                                   "clients": serves}).encode()
            elif self.path.startswith("/stream"):
                self.send_response(200)
                self.send_header("Content-Type", "text/plain")
                self.end_headers()
                return                       # held open, sends nothing
            else:
                body = json.dumps({"agent": "probe", "registered": True,
                                   "stations": ["s"], "channels": []}).encode()
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

    srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
    threading.Thread(target=srv.serve_forever, daemon=True).start()
    return srv


def run_channel(port: int, tmp: Path, client_version: str | None) -> str:
    env = dict(os.environ, A2A_URL=f"http://127.0.0.1:{port}",
               A2A_TOKEN="tok", A2A_AGENT="probe", A2A_HELLO="0",
               A2A_READ_ON_INIT="0", A2A_IDENTITY_STORE=str(tmp),
               A2A_AGENT_DIR=str(tmp), A2A_LOG_FILE=str(tmp / "c.log"),
               A2A_STREAM_TIMEOUT="3")
    env.pop("A2A_CLIENT_VERSION", None)
    if client_version is not None:
        env["A2A_CLIENT_VERSION"] = client_version
    proc = subprocess.Popen([sys.executable, str(CHANNEL)],
                            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE, env=env, text=True)
    proc.stdin.write(json.dumps(
        {"jsonrpc": "2.0", "id": 1, "method": "initialize",
         "params": {"protocolVersion": "2024-11-05"}}) + "\n")
    proc.stdin.write(json.dumps(
        {"jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
    proc.stdin.flush()
    # Do NOT close stdin here. The channel's main loop reads it, so closing it
    # ends the process — before the pump thread that does the version check
    # has run. Give it a moment, then stop it.
    time.sleep(3.0)
    proc.terminate()
    try:
        out, _ = proc.communicate(timeout=10)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
    return out


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-ver-"))

    # --- behind: says so, once, and never spends a turn --------------------
    srv = fake_broker("9.9.9")
    port = srv.server_address[1]
    try:
        out = run_channel(port, tmp / "old", "0.1.0")
        notes = [json.loads(l) for l in out.splitlines()
                 if l.strip().startswith("{")
                 and '"method"' in l and "channel" in l]
        behind = [n for n in notes
                  if "9.9.9" in json.dumps(n.get("params", {}))]
        check("a client behind the broker says so", len(behind) >= 1,
              out[-400:])
        check("exactly once, not on every reconnect", len(behind) == 1,
              f"{len(behind)} notifications")
        text = json.dumps(behind[0]) if behind else ""
        check("and names the command that fixes it, since knowing you are "
              "stale without knowing what to do is not much better",
              "curl" in text and "tar" in text, text[:300])
        check("it is a notification, never a prompt — a stale install must "
              "not cost a model turn",
              all(n.get("method", "").startswith("notifications/")
                  for n in behind), text[:200])
    finally:
        srv.shutdown()

    # --- current: silent ----------------------------------------------------
    srv = fake_broker("0.1.0")
    port = srv.server_address[1]
    try:
        out = run_channel(port, tmp / "cur", "0.1.0")
        check("a current client says nothing at all",
              "reinstall" not in out and "now serves" not in out,
              out[-300:])
    finally:
        srv.shutdown()

    # --- installed before versions existed: also silent ---------------------
    srv = fake_broker("0.1.0")
    port = srv.server_address[1]
    try:
        out = run_channel(port, tmp / "none", None)
        check("an install predating the stamp is not nagged about a version "
              "it could not have had",
              "now serves" not in out, out[-300:])
    finally:
        srv.shutdown()

    # --- the other three clients carry the same check ----------------------
    # The point of the check is that a stale copy on a disk says so. That is
    # true of every client, not only the one whose missing tool started this,
    # so an implementation in one of four is the gap rather than the fix.
    #
    # Driving Pi and OpenCode end-to-end needs their runtimes; what is
    # asserted here is that each reads the version the broker bakes, compares
    # it against /healthz's `clients`, says it ONCE, and does so through its
    # free path rather than by spending a model turn.
    # `called` is a separate assertion from `once` on purpose: a check that is
    # defined but never invoked passes every grep for its own internals while
    # doing nothing at all. Removing the call site is the likelier regression,
    # since it is one line and far from the function.
    clients = {
        "pi": (PLUGIN / "pi" / "index.ts", "notify(", "versionChecked",
               "await checkVersion()"),
        "opencode": (PLUGIN / "opencode" / "a2a-opencode.js", 'log("warn"',
                     "versionChecked", "await checkVersion()"),
        "codex": (PLUGIN / "codex" / "a2a-codex.py", "log(", "_version_checked",
                  "\n    check_version()"),
    }
    for name, (path, free_path, once, called) in clients.items():
        src = path.read_text()
        check(f"{name}: reads the version the broker baked into it",
              "version" in src and ("BAKED.version" in src
                                    or 'A2A_BAKED.get("version")' in src),
              "does not read a baked version")
        check(f"{name}: compares it against the broker's `clients`",
              "healthz" in src and "clients" in src, "no comparison")
        check(f"{name}: says it once, not on every reconnect",
              once in src, f"no {once} one-shot")
        check(f"{name}: through its free path, never a model turn",
              free_path in src, f"does not use {free_path}")
        check(f"{name}: and something actually CALLS it — a check nobody "
              f"invokes passes every test of its own internals",
              called in src, f"no call site ({called.strip()})")

    # --- the broker actually serves the field the client reads -------------
    src = BROKER.read_text()
    check("the broker reports a client-tree version on /healthz",
          '"clients": VERSION' in src, "healthz does not carry it")
    check("and stamps it into every client it serves, so no constant has to "
          "be kept in step by hand",
          src.count('"version": VERSION,') >= 1
          and 'env["A2A_CLIENT_VERSION"] = VERSION' in src,
          "a client is served without its version")
    check("the broker's version and the version it stamps are ONE constant — "
          "two would drift, and a client would report staleness that is not "
          "real or miss staleness that is",
          '"clients": VERSION' in src, "the two can diverge")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — a stale install announces itself, once, for free")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
