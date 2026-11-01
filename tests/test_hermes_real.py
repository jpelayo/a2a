#!/usr/bin/env python3
"""The Hermes plugin's hold and waking routine, inside the REAL Hermes.

    HERMES_PY=/path/to/venv/bin/python python3 tests/test_hermes_real.py

HERMES_PY is the Python of a venv where a hermes-agent checkout is installed
with its web extra (`pip install -e "<checkout>[web]"`; Hermes runs on
3.11-3.14). Without it this file has not run, and says so (exit 2; run.py
lists it as needing HERMES_PY).

Every other Hermes test runs the plugin against fakes, and a fake can agree
with the plugin's mistakes: the waker's first version passed its test and
crashed on every profile of a real deploy (it called a Hermes function only
the session server's copy of can run), and the default bot never held its
conversation (`hermes serve` loads that profile's plugins before its web
server exists). This one runs Hermes itself, with its own boot order:

  * a temp HOME and HERMES_HOME — no real ~/.hermes is read or written;
  * the default profile and three bots, set up with Hermes's own APIs by
    tests/hermes_real_driver.py: bota and botb with a2a on in their own app
    conversation, botc with a2a NOT enabled. Each gets the plugin folder as
    the installer writes it (deploy/plugin/hermes, dashboard part included);
  * the real broker (`serve`, on the test database) and a stub
    OpenAI-compatible model on 127.0.0.1, so a turn runs offline;
  * `hermes serve` started through Hermes's own fast serve path, as the
    desktop app starts it, with nothing ever attaching to it.

What must hold:
  wake      the waker woke bota and botb, skipped botc, with no error, and
            only once the dashboard had finished starting (no pass before the
            desktop injector was installed, which comes after Hermes's own
            multi-profile decision);
  held      default, bota and botb have their conversation loaded with the
            plugin's own transport attached, no app needed; botc does not;
  kept      they are still held well past the 20 s orphan grace;
  delivered a DM to each held bot is ACKED in the broker's database: it went
            through the stream, the injector, a real Hermes turn, the
            plugin's pre_llm_call and the ack — the whole path;
  logged    each held bot's a2a.log says it keeps its conversation loaded,
            and never that it could not.
"""
import asyncio
import importlib.util
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

sys.dont_write_bytecode = True

import dbharness  # noqa: E402

HERE = Path(__file__).resolve().parent
REPO = HERE.parent
BROKER = REPO / "deploy" / "a2a_mcp" / "a2a-mcp.py"
PLUGIN = REPO / "deploy" / "plugin" / "hermes"
DRIVER = HERE / "hermes_real_driver.py"

BOTS = [
    {"profile": "default", "agent": "e2e-default", "a2a": True,
     "key": "20261101_100000_aaaaaa"},
    {"profile": "bota", "a2a": True, "key": "20261101_100001_bbbbbb"},
    {"profile": "botb", "a2a": True, "key": "20261101_100002_cccccc"},
    {"profile": "botc", "a2a": False, "key": "20261101_100003_dddddd"},
]
HELD = [b for b in BOTS if b["a2a"]]
AGENT = {b["profile"]: b.get("agent") or b["profile"] for b in BOTS}
BOOT_S = 240.0            # Hermes's cold start, then the waker and the holds
GRACE_S = 25.0            # past the dashboard's 20 s orphan reap
ACK_S = 180.0             # a first turn builds the agent

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}", flush=True)
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def until(cond, timeout):
    end = time.time() + timeout
    while time.time() < end:
        try:
            if cond():
                return True
        except Exception:
            pass
        time.sleep(0.5)
    try:
        return bool(cond())
    except Exception:
        return False


# --- a model that answers, offline -------------------------------------------
class StubModel(BaseHTTPRequestHandler):
    calls: list = []

    def log_message(self, *a):
        pass

    def _json(self, code, obj):
        body = json.dumps(obj).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self):
        if self.path.rstrip("/").endswith("/models"):
            return self._json(200, {"object": "list", "data": [
                {"id": "stub", "object": "model", "owned_by": "e2e"}]})
        return self._json(404, {"error": "not found"})

    def do_POST(self):
        n = int(self.headers.get("Content-Length") or 0)
        try:
            req = json.loads(self.rfile.read(n) or b"{}")
        except ValueError:
            req = {}
        StubModel.calls.append(self.path)
        if not self.path.rstrip("/").endswith("/chat/completions"):
            return self._json(404, {"error": "not found"})
        base = {"id": "chatcmpl-e2e", "created": int(time.time()),
                "model": req.get("model") or "stub"}
        if not req.get("stream"):
            return self._json(200, {**base, "object": "chat.completion",
                                    "choices": [{"index": 0,
                                                 "finish_reason": "stop",
                                                 "message": {
                                                     "role": "assistant",
                                                     "content": "ok"}}],
                                    "usage": {"prompt_tokens": 1,
                                              "completion_tokens": 1,
                                              "total_tokens": 2}})
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.end_headers()
        for delta, finish in (({"role": "assistant", "content": "ok"}, None),
                              ({}, "stop")):
            chunk = {**base, "object": "chat.completion.chunk",
                     "choices": [{"index": 0, "delta": delta,
                                  "finish_reason": finish}]}
            self.wfile.write(f"data: {json.dumps(chunk)}\n\n".encode())
        self.wfile.write(b"data: [DONE]\n\n")
        self.wfile.flush()


def import_broker():
    spec = importlib.util.spec_from_file_location("broker_hreal", BROKER)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def hermes_env(tmp: Path, base: str) -> dict:
    """Hermes's environment: a temp HOME, nothing of this test's own Python
    path (the test database's fake driver is the broker's, not Hermes's)."""
    env = {k: v for k, v in os.environ.items()
           if not k.startswith(("A2A_", "HERMES_", "PYTHON", "VIRTUAL_ENV"))}
    home = tmp / "home"
    env.update(HOME=str(home), HERMES_HOME=str(home / ".hermes"),
               A2A_URL=base, A2A_TOKEN="dev", PYTHONDONTWRITEBYTECODE="1",
               PYTHONUNBUFFERED="1")
    return env


def tail(path: Path, n=40) -> str:
    try:
        return "\n".join(path.read_text(errors="replace").splitlines()[-n:])
    except OSError:
        return f"({path} unreadable)"


def run(hermes_py: str) -> None:
    dbharness.require_db()
    env = dbharness.db_env()
    os.environ.update(env)
    os.environ["A2A_AUTH_DISABLED"] = "1"
    b = import_broker()
    b._startup()
    sid = b.DEFAULT_STATION_ID
    for agent in list(AGENT.values()) + ["peer"]:
        b.AGENTS.add(sid, agent)

    def receipt(agent, mid):
        return b.CONN.execute(
            "SELECT delivered_at, acked_at FROM message_receipts "
            "WHERE station_id = %s AND agent_id = %s AND msg_id = %s",
            (sid, agent, mid)).fetchone() or {}

    def dm(agent, text):
        out = asyncio.run(b.DIRECT.send(sid, "peer", agent, text))
        return (out.get("dm") or out).get("id")

    tmp = Path(tempfile.mkdtemp(prefix="a2a-hermes-real-"))
    model = ThreadingHTTPServer(("127.0.0.1", 0), StubModel)
    threading.Thread(target=model.serve_forever, daemon=True).start()
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    broker = subprocess.Popen(
        [sys.executable, "-B", str(BROKER), "serve"],
        env=dict(os.environ, **env, A2A_AUTH_DISABLED="1",
                 A2A_HOST="127.0.0.1", A2A_PORT=str(port),
                 PYTHONDONTWRITEBYTECODE="1"),
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    hermes = None
    stop = tmp / "stop"
    report_path = tmp / "report.json"
    out = (tmp / "hermes.out").open("w")
    try:
        check("the broker comes up", until(lambda: urllib.request.urlopen(
            f"{base}/healthz", timeout=2).status == 200, 20), base)
        spec = {"plugin": str(PLUGIN), "bots": BOTS,
                "model": {"provider": "custom", "default": "stub",
                          "base_url": f"http://127.0.0.1:"
                                      f"{model.server_address[1]}/v1",
                          "api_key": "e2e-stub"}}
        spec_path = tmp / "spec.json"
        spec_path.write_text(json.dumps(spec))
        henv = hermes_env(tmp, base)
        setup = subprocess.run([hermes_py, "-B", str(DRIVER), "setup",
                                str(spec_path)], env=henv, cwd=tmp,
                               capture_output=True, text=True, timeout=300)
        check("setup: Hermes's own APIs build the home and its bots",
              setup.returncode == 0, setup.stdout[-2000:] + setup.stderr[-4000:])
        if setup.returncode != 0:
            return
        hermes = subprocess.Popen(
            [hermes_py, "-B", str(DRIVER), "serve", str(spec_path),
             str(report_path), str(stop)],
            env=henv, cwd=tmp, stdout=out, stderr=subprocess.STDOUT)

        def report():
            return json.loads(report_path.read_text())

        def all_held():
            r = report()
            return all((r["records"].get(x["profile"]) or {}).get("held")
                       for x in HELD)
        held = until(all_held, BOOT_S)
        r = report() if report_path.exists() else {}
        hermes_home = tmp / "home" / ".hermes"
        if not held:
            print("--- hermes output ---\n" + tail(tmp / "hermes.out", 60))
            print("--- agent.log ---\n" + tail(hermes_home / "logs" /
                                               "agent.log", 60))

        waker = r.get("waker") or {}
        check("wake: the waker woke the a2a bots, skipped the one without "
              "a2a, and recorded no error",
              sorted(waker.get("woken") or []) == ["bota", "botb", "default"]
              and "botc" in (waker.get("skipped") or [])
              and not waker.get("errors"), json.dumps(waker))
        check("wake: not before the dashboard finished starting — no pass "
              "before the desktop injector, and its first pass after it",
              r.get("injector_at") is not None
              and r.get("waker_passes_at_injector") == 0
              and (waker.get("first_pass_at") or 0)
              >= (r.get("injector_at") or 1e18) - 0.05,
              json.dumps({k: r.get(k) for k in (
                  "injector_at", "waker_passes_at_injector",
                  "multiplex_at_injector")} | {
                  "first_pass_at": waker.get("first_pass_at")}))
        check("wake: Hermes had made its own multi-profile decision by then",
              r.get("multiplex_at_injector") is True,
              str(r.get("multiplex_at_injector")))
        for x in HELD:
            rec = (r.get("records") or {}).get(x["profile"]) or {}
            check(f"held: {x['profile']}'s conversation is loaded with the "
                  f"plugin's own transport, and nothing else attached",
                  rec.get("held") is True and rec.get("transports") == ["_Hold"],
                  json.dumps(rec))
        recs = r.get("records") or {}
        check("held: the bot without a2a has nothing loaded",
              recs.get("botc") is None, json.dumps(recs.get("botc")))
        homes = {x["profile"]: (hermes_home if x["profile"] == "default"
                                else hermes_home / "profiles" / x["profile"])
                 for x in BOTS}
        for x in HELD:
            if x["profile"] == "default":
                continue
            rec = recs.get(x["profile"]) or {}
            check(f"held: {x['profile']}'s record belongs to its own profile "
                  f"home", Path(str(rec.get("profile_home") or "")).resolve()
                  == homes[x["profile"]].resolve(), json.dumps(rec))

        if held:
            time.sleep(GRACE_S)
            r2 = report()
            since = r2.get("since") or {}
            check("kept: every held conversation is still held past the "
                  "orphan grace, held the whole time",
                  all((r2["records"].get(x["profile"]) or {}).get("held")
                      and r2["now"] - since.get(x["profile"], r2["now"])
                      >= GRACE_S for x in HELD), json.dumps(r2["records"]))

            ids = {x["profile"]: dm(AGENT[x["profile"]],
                                    f"e2e hello {x['profile']}")
                   for x in HELD}
            acked = until(lambda: all(receipt(AGENT[p], m).get("acked_at")
                                      for p, m in ids.items()), ACK_S)
            check("delivered: a DM to each held bot is ACKED in the "
                  "broker's database — stream, injector, a real Hermes "
                  "turn, pre_llm_call, ack",
                  acked, json.dumps({p: {k: bool(v) for k, v in
                                         receipt(AGENT[p], m).items()}
                                     for p, m in ids.items()}))
            if not acked:
                print("--- agent.log ---\n" + tail(hermes_home / "logs" /
                                                   "agent.log", 80))
            check("delivered: the turns reached the model stub",
                  any(c.rstrip("/").endswith("/chat/completions")
                      for c in StubModel.calls), str(StubModel.calls[-5:]))

        for x in HELD:
            logs = list(homes[x["profile"]].glob(
                "plugin-data/agent-plugin-a2a-*/a2a.log"))
            text = "".join(p.read_text(errors="replace") for p in logs)
            check(f"logged: {x['profile']}'s a2a.log says it keeps its "
                  f"conversation loaded, and never that it could not",
                  "loaded in this dashboard" in text
                  and "could not keep" not in text,
                  tail(logs[0], 15) if logs else "no a2a.log")
    finally:
        stop.touch()
        if hermes is not None:
            try:
                hermes.wait(30)
            except subprocess.TimeoutExpired:
                hermes.kill()
                hermes.wait(10)
        out.close()
        broker.terminate()
        try:
            broker.wait(15)
        except subprocess.TimeoutExpired:
            broker.kill()
        model.shutdown()
        if not fails:
            shutil.rmtree(tmp, ignore_errors=True)
        else:
            print(f"(kept for a look: {tmp})")


def main() -> int:
    hermes_py = os.environ.get("HERMES_PY") or ""
    if not hermes_py or not Path(hermes_py).is_file():
        print("SKIP: set HERMES_PY to the Python of a venv with the Hermes "
              "checkout installed (pip install -e '<checkout>[web]')")
        return 2
    run(hermes_py)
    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — in the real Hermes, every a2a bot is held without being "
          "opened, and its DMs arrive")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
