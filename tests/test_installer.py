#!/usr/bin/env python3
"""The installer, run the way a user runs it, against the clients it serves.

    python3 tests/test_installer.py

The installer is the one command every user types, and the only one that
updates a client, so what it writes IS every install. It is run here as the
artifact the broker serves (the real /install handler, seam baked), as a
separate process, against a fake broker whose client downloads come from the
REAL install handlers — a client served wrong fails here just as it would on
somebody's machine. Harness commands are fake binaries on PATH that only log
their argv; HOME is a temp directory, and nothing may be written outside it.

The answers go in on stdin, which selects the installer's plain prompts — the
curses screens share every decision with them, and their keys are checked
directly below.

Needs python3 with the broker's deps (starlette). No database, no harness.
"""
import asyncio
import http.server
import importlib.util
import io
import json
import os
import subprocess
import sys
import tarfile
import tempfile
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SOURCE = ROOT / "plugin" / "installer" / "a2a-install.py"
TOKEN = "a2a_st_TESTsecretTOKENvalue123"
# A second user's token, in another station: what a harness given its own
# token (Hermes bots on `bots`, say) is installed with.
TOKEN2 = "a2a_st_TESTsecondTOKENforBOTS456"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


# ---------------------------------------------------------------------------
# The broker, in-process: its real handlers, the token check stubbed
# ---------------------------------------------------------------------------

def load_broker():
    os.environ.pop("A2A_PUBLIC_URL", None)   # the url must come from the host
    spec = importlib.util.spec_from_file_location(
        "broker_installer", ROOT / "a2a_mcp" / "a2a-mcp.py")
    b = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(b)               # importing opens no database

    async def run_inline(fn):
        return fn()
    b._db = run_inline
    b.TOKENS.resolve = lambda t: ({"stations": ["s"]} if t == TOKEN else
                                  {"stations": ["bots"]} if t == TOKEN2
                                  else None)
    return b


def call(b, routes, host: str, path: str, query: str = ""):
    """(status, body bytes, content-type) from the broker's own route."""
    from starlette.requests import Request
    from starlette.routing import Match
    scope = {"type": "http", "method": "GET", "scheme": "http", "path": path,
             "query_string": query.encode(), "root_path": "",
             "server": tuple(host.split(":")[:1]) + (80,),
             "headers": [(b"host", host.encode())]}
    for r in routes:
        match, child = r.matches(scope)
        if match == Match.FULL:
            scope.update(child)
            resp = asyncio.run(r.endpoint(Request(scope)))
            return (resp.status_code, resp.body,
                    resp.headers.get("content-type", ""))
    return 404, b'{"error":"not found"}', "application/json"


class FakeBroker:
    """/me answers the token; everything else is the broker's real handler."""

    def __init__(self, b):
        self.b, self.routes, self.seen = b, b.build_app().routes, []
        outer = self

        class H(http.server.BaseHTTPRequestHandler):
            def log_message(self, *a):
                pass

            def do_GET(self):
                path, _, query = self.path.partition("?")
                outer.seen.append((path, dict(self.headers)))
                if path == "/me":
                    auth = self.headers.get("Authorization", "")
                    if auth == f"Bearer {TOKEN}":
                        code, body = 200, json.dumps(
                            {"user": "alice", "stations": ["s"],
                             "agent": None, "registered": False}).encode()
                    elif auth == f"Bearer {TOKEN2}":
                        code, body = 200, json.dumps(
                            {"user": "bob", "stations": ["bots"],
                             "agent": None, "registered": False}).encode()
                    else:
                        code, body = 403, b'{"error":"invalid or revoked token"}'
                    ctype = "application/json"
                else:
                    code, body, ctype = call(outer.b, outer.routes,
                                             self.headers.get("Host", ""),
                                             path, query)
                self.send_response(code)
                self.send_header("Content-Type", ctype)
                self.send_header("Content-Length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.srv = http.server.ThreadingHTTPServer(("127.0.0.1", 0), H)
        self.base = f"http://127.0.0.1:{self.srv.server_address[1]}"
        threading.Thread(target=self.srv.serve_forever, daemon=True).start()

    def close(self):
        self.srv.shutdown()


# ---------------------------------------------------------------------------
# A machine: HOME with harness folders, fake harness binaries on PATH
# ---------------------------------------------------------------------------

def machine(root: Path):
    home, bin_, cwd = root / "home", root / "bin", root / "cwd"
    for d in (home / ".claude", home / ".config" / "opencode",
              home / ".pi" / "agent", home / ".codex",
              home / ".hermes" / "profiles" / "bot1",
              home / ".hermes" / "profiles" / "empty", bin_, cwd):
        d.mkdir(parents=True, exist_ok=True)
    (home / ".hermes" / "config.yaml").write_text("model: x\n")
    (home / ".hermes" / "profiles" / "bot1" / "config.yaml").write_text("x\n")
    log = root / "commands.log"
    for name in ("claude", "opencode", "pi", "codex", "hermes"):
        p = bin_ / name
        # Every fake talks on both streams — the installer must show none of
        # it — and one command fails, the way Hermes refused a per-bot
        # gateway restart on a real machine with a screenful of text.
        p.write_text(f'#!/bin/sh\necho "{name} $*" >> "{log}"\n'
                     'echo "JUNK-OUT $*"; echo "JUNK-ERR $*" >&2\n'
                     'case "$*" in "-p bot1 plugins enable a2a") exit 1;; '
                     'esac\n')
        p.chmod(0o755)
    return home, bin_, cwd, log


def snapshot(root: Path) -> dict:
    return {str(p.relative_to(root)): p.stat().st_size
            for p in root.rglob("*") if p.is_file()}


def run_installer(installer: Path, home, bin_, cwd, answers):
    env = {"HOME": str(home), "PATH": f"{bin_}:/usr/bin:/bin",
           "TERM": "dumb", "LANG": "C.UTF-8"}
    r = subprocess.run([sys.executable, str(installer)],
                       input="\n".join(answers) + "\n", cwd=cwd, env=env,
                       capture_output=True, text=True, timeout=120)
    if os.environ.get("A2A_TEST_SHOW"):     # read what a person would see
        print("-" * 72, r.stdout + r.stderr, "-" * 72, sep="\n")
    return r.returncode, r.stdout + r.stderr


# ---------------------------------------------------------------------------

def pure():
    """The checklist keys and the safety checks, with no terminal."""
    spec = importlib.util.spec_from_file_location("installer_src", SOURCE)
    m = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(m)

    rows = [{"label": "a", "enabled": True, "checked": False},
            {"label": "b", "enabled": False, "checked": False},
            {"label": "c", "enabled": True, "checked": False}]
    r, c, done = m.checklist_key(rows, 0, "toggle")
    check("keys: SPACE ticks the row under the cursor",
          r[0]["checked"] and done is None, str(r))
    r2, c, _ = m.checklist_key(r, 1, "toggle")
    check("keys: a row that was not found cannot be ticked",
          not r2[1]["checked"], str(r2))
    check("keys: up stops at the top, down at the bottom",
          m.checklist_key(rows, 0, "up")[1] == 0
          and m.checklist_key(rows, 2, "down")[1] == 2)
    check("keys: j/k and the arrows name the same moves",
          m.key_name("j") == "down" and m.key_name("k") == "up"
          and m.key_name(" ") == "toggle" and m.key_name("\n") == "enter")
    check("keys: in an input field j, k, q and space are text, ESC still "
          "cancels",
          m.key_name("j", True) == "j" and m.key_name("q", True) == "q"
          and m.key_name(" ", True) == " "
          and m.key_name("\x1b", True) == "esc")
    check("keys: ENTER with nothing ticked does not finish",
          m.checklist_key(rows, 0, "enter")[2] is None)
    check("keys: ENTER with a row ticked finishes",
          m.checklist_key(r, 0, "enter")[2] == "ok")
    check("keys: ESC cancels", m.checklist_key(rows, 0, "esc")[2] == "cancel")

    home = Path(tempfile.mkdtemp(prefix="a2a-inst-rows-"))
    (home / ".claude" / "skills" / "a2a").mkdir(parents=True)
    (home / ".claude" / "skills" / "a2a" / ".mcp.json").write_text(json.dumps(
        {"mcpServers": {"a2a-channel": {"env": {"A2A_CLIENT_VERSION": "0.1"}}}}))
    (home / ".config" / "opencode").mkdir(parents=True)
    m.HOME = home
    path = os.environ.get("PATH", "")
    os.environ["PATH"] = str(home / "nobin")   # no harness binary is found
    try:
        rows = {r["key"]: r for r in m.harness_rows()}
    finally:
        os.environ["PATH"] = path
    check("rows: an installed client starts ticked, so an update is ENTER",
          rows["claude"]["checked"] and "0.1" in rows["claude"]["label"],
          str(rows["claude"]))
    check("rows: a harness found without a2a starts unticked",
          rows["opencode"]["enabled"] and not rows["opencode"]["checked"])
    check("rows: a harness not found cannot be picked",
          not rows["pi"]["enabled"])

    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as t:
        info = tarfile.TarInfo("../escape")
        info.size = 1
        t.addfile(info, io.BytesIO(b"x"))
    try:
        m.unpack(buf.getvalue())
        refused = False
    except ValueError:
        refused = True
    check("unpack refuses a member that climbs out of its folder", refused)

    m.SECRET["tokens"] = [TOKEN, TOKEN2]
    check("redact hides the token in anything printed",
          TOKEN not in m.redact(f"GET /install/pi/{TOKEN} failed"))
    shown = m.redact(f"GET /install/pi/{TOKEN} and /install/hermes/{TOKEN2}")
    check("redact hides EVERY token typed — one per harness is allowed",
          TOKEN not in shown and TOKEN2 not in shown, shown)


def main() -> int:
    pure()
    b = load_broker()
    broker = FakeBroker(b)
    host = broker.base.split("//", 1)[1]
    try:
        # --- the route ------------------------------------------------------
        code, src, ctype = call(b, b.build_app().routes, host, "/install")
        check("route: /install serves the installer", code == 200, str(code))
        check("route: it is public — the auth middleware lets it through "
              "with no token", "/install" in b.PUBLIC_PATHS, "")
        text = src.decode()
        check("route: the url and version are baked into the seam",
              f'A2A_BAKED = {{"url": "{broker.base}", "version": '
              f'"{b.VERSION}"}}' in text, text[:300])
        check("route: and nothing else — no token rides in it",
              TOKEN not in text and '"token"' not in text.split(
                  "A2A_BAKED = ", 1)[1].split("\n", 1)[0], "")

        root = Path(tempfile.mkdtemp(prefix="a2a-inst-"))
        installer = root / "a2a-install.py"
        installer.write_bytes(src)

        # --- a fresh machine, everything, commands run -----------------------
        m1 = root / "m1"
        home, bin_, cwd, log = machine(m1)
        before = snapshot(root)
        code, out = run_installer(installer, home, bin_, cwd, [
            "all",          # harnesses
            TOKEN,          # token
            "",             # ENTER: every harness uses it
            "all",          # hermes bots: default, bot1 (not "empty")
            "botd",         # agent id for the default profile
            "y",            # install
            "y",            # run the commands
        ])
        check("fresh: the installer finishes", code == 0, out[-1500:])
        check("fresh: the token never appears in what it prints",
              TOKEN not in out, "token printed")
        cc = home / ".claude" / "skills" / "a2a" / ".mcp.json"
        env = (json.loads(cc.read_text())["mcpServers"]["a2a-channel"]["env"]
               if cc.is_file() else {})
        check("fresh: Claude Code lands in ~/.claude/skills/a2a with this "
              "broker's url and version",
              env.get("A2A_URL") == broker.base
              and env.get("A2A_CLIENT_VERSION") == b.VERSION, str(env))
        oc = home / ".config" / "opencode" / "plugins" / "a2a-opencode.js"
        check("fresh: OpenCode lands as one file, credentials baked",
              oc.is_file() and oc.read_text().startswith("const A2A_BAKED = {")
              and f'"token": "{TOKEN}"' in oc.read_text().split("\n", 1)[0])
        pi = home / ".pi" / "agent" / "extensions" / "a2a"
        check("fresh: Pi lands as a directory with its package.json",
              (pi / "index.ts").is_file() and (pi / "package.json").is_file())
        cx = home / ".codex" / "a2a" / "a2a-codex.py"
        check("fresh: Codex lands in ~/.codex/a2a, credentials baked",
              cx.is_file() and f'"token": "{TOKEN}"' in cx.read_text())
        hd = home / ".hermes" / "plugins" / "a2a"
        hb = home / ".hermes" / "profiles" / "bot1" / "plugins" / "a2a"
        check("fresh: the default bot lands in ~/.hermes/plugins/a2a with the "
              "id it was given",
              (hd / "plugin.yaml").is_file()
              and '"agent": "botd"' in (hd / "__init__.py").read_text())
        check("fresh: a named bot lands in its profile, named by its profile",
              (hb / "__init__.py").is_file()
              and '"agent": ""' in (hb / "__init__.py").read_text())
        check("fresh: a profile folder with no Hermes identity file is no bot",
              not (home / ".hermes" / "profiles" / "empty" / "plugins").exists())
        ran = log.read_text().splitlines() if log.is_file() else []
        want = [f"codex mcp add a2a -- python3 {cx}",
                "hermes -p default plugins enable a2a",
                "hermes -p default config set "
                "plugins.entries.a2a.allow_gateway_injection true",
                "hermes -p bot1 plugins enable a2a",
                "hermes -p bot1 config set "
                "plugins.entries.a2a.allow_gateway_injection true"]
        check("fresh: with yes, exactly the needed commands ran, in order — "
              "and no gateway restart, which a host gateway refuses",
              ran == want, "\n".join(ran))
        check("fresh: a command's own output is never shown",
              "JUNK" not in out, out[-1500:])
        check("fresh: a command that failed says so in one line…",
              "turn the a2a plugin on" in out and "did not work" in out,
              out[-1500:])
        steps = out.split("Next steps", 1)[-1]
        check("fresh: …and is in the next steps, to run by hand",
              "hermes -p bot1 plugins enable a2a" in steps, steps[:1200])
        check("fresh: the Hermes step is to quit the app and open it again",
              "quit the Hermes app and open it again" in steps
              and "gateway restart" not in out, steps[:1200])
        check("fresh: the next steps name what no installer can do",
              "plugin:a2a@skills-dir" in steps and "enable a2a here" in steps
              and "app-server --listen unix://" in steps, steps[:1500])
        after = snapshot(root)
        outside = [p for p in after if p not in before
                   and not p.startswith("m1/home/") and p != "m1/commands.log"]
        check("fresh: nothing was written outside HOME", not outside,
              str(outside[:8]))
        check("fresh: /me was asked with the bare token, naming no agent "
              "(naming one would pin it)",
              any(p == "/me" for p, _ in broker.seen)
              and not any(p == "/me" and h.get("X-A2A-Agent")
                          for p, h in broker.seen), str(broker.seen[:4]))

        # --- running it again: an update -----------------------------------
        (pi / "node_modules").mkdir(exist_ok=True)
        (pi / "node_modules" / "keep.txt").write_text("installed by pi")
        (home / ".codex" / "config.toml").write_text(
            '[mcp_servers.a2a]\ncommand = "python3"\n')
        cx.write_text(cx.read_text().replace(
            f'"version": "{b.VERSION}"', '"version": "0.0.9"', 1))
        # Same version, different bytes — what a client changed without a
        # version bump looks like on disk.
        (pi / "package.json").write_text(
            (pi / "package.json").read_text() + "\n")
        # The default bot is set up the way the two Hermes commands and
        # "enable a2a here" leave a config.yaml; bot1 is not (its enable
        # failed above).
        (home / ".hermes" / "config.yaml").write_text(
            "model: x\nplugins:\n  enabled:\n  - a2a\n  entries:\n"
            "    a2a:\n      allow_gateway_injection: true\n"
            "      settings:\n        enabled: true\n")
        log.unlink()
        code, out = run_installer(installer, home, bin_, cwd, [
            "",             # ENTER: the installed ones, already ticked
            TOKEN,
            "",             # ENTER: every harness uses it
            "",             # ENTER: both bots, already installed
            "",             # ENTER: keep the default bot's id
            "y",
            "y",
        ])
        check("update: it finishes", code == 0, out[-1500:])
        check("update: it shows the installed version and the new one",
              "installed 0.0.9" in out and f"installed {b.VERSION}" in out,
              out[:1500])
        check("update: Codex is brought to the broker's version",
              f'"version": "{b.VERSION}"' in cx.read_text())
        check("update: a client that was behind is told to restart",
              "Codex: restart running sessions" in out, out[-1500:])
        check("update: a client whose files are byte-identical is 'already "
              "current' and not told to restart — nothing changed",
              "Claude Code: restart" not in out
              and "OpenCode: restart" not in out
              and "already current" in out, out[-1500:])
        check("update: a client changed WITHOUT a version bump is 'updated' "
              "and told to restart — the version alone would have called it "
              "current", f"{b.VERSION}, updated" in out
              and "Pi: restart running sessions" in out, out[-1500:])
        steps = out.split("Next steps", 1)[-1]
        check("update: 'enable a2a here' is asked only for the bot whose a2a "
              "is not on yet, and points at the Hermes app",
              "Hermes bot 'bot1': in the Hermes app" in steps
              and "Hermes bot 'default': in the Hermes app" not in steps,
              steps[:1500])
        ran = log.read_text().splitlines() if log.is_file() else []
        check("update: only the setup not yet done is offered — the default "
              "bot's config already has a2a enabled and allowed",
              ran == ["hermes -p bot1 plugins enable a2a",
                      "hermes -p bot1 config set "
                      "plugins.entries.a2a.allow_gateway_injection true"],
              "\n".join(ran))

        # --- running it once more, everything current ----------------------
        code, out = run_installer(installer, home, bin_, cwd, [
            "1", "y"])          # Claude Code only: no token, no commands
        check("current: picking an up-to-date Claude Code asks for no token",
              code == 0 and "Paste or type the token" not in out, out[-1200:])
        check("current: and ends with nothing to do — no restart line",
              "Nothing else to do" in out and "restart" not in out,
              out[-1200:])
        check("update: files it did not ship are left alone (Pi's "
              "node_modules)", (pi / "node_modules" / "keep.txt").is_file())
        check("update: the default bot keeps the id it already had",
              '"agent": "botd"' in (hd / "__init__.py").read_text())
        ran = log.read_text().splitlines() if log.is_file() else []
        check("update: codex mcp add is not offered again once Codex has "
              "the server registered",
              ran and not any(r.startswith("codex") for r in ran),
              "\n".join(ran))

        # --- commands declined ---------------------------------------------
        m2 = root / "m2"
        home, bin_, cwd, log = machine(m2)
        code, out = run_installer(installer, home, bin_, cwd, [
            "4", TOKEN, "y", "n"])
        check("declined: the install still finishes", code == 0, out[-800:])
        check("declined: no command ran", not log.exists(),
              log.read_text() if log.exists() else "")
        check("declined: and they are in the next steps, to run by hand",
              "they are in your next steps" in out
              and "codex mcp add a2a" in out.split("Next steps", 1)[-1],
              out[-800:])

        # --- one token for most, another for Hermes ---------------------------
        m4 = root / "m4"
        home, bin_, cwd, log = machine(m4)
        broker.seen.clear()
        code, out = run_installer(installer, home, bin_, cwd, [
            "all",          # harnesses
            TOKEN,          # the token for all of them…
            "1 2 3",        # …but Hermes (row 4) gets its own
            TOKEN2,         # Hermes's token
            "all",          # bots
            "botd",
            "y",
            "n",            # no commands: not what this case is about
        ])
        check("per harness: the installer finishes", code == 0, out[-1500:])
        check("per harness: neither token appears in what it prints",
              TOKEN not in out and TOKEN2 not in out, "token printed")
        baked = {
            "OpenCode": home / ".config" / "opencode" / "plugins"
            / "a2a-opencode.js",
            "Pi": home / ".pi" / "agent" / "extensions" / "a2a" / "index.ts",
            "Codex": home / ".codex" / "a2a" / "a2a-codex.py",
        }
        check("per harness: OpenCode, Pi and Codex carry the first token",
              all(p.is_file() and f'"token": "{TOKEN}"' in p.read_text()
                  for p in baked.values()),
              str({k: p.is_file() for k, p in baked.items()}))
        bots = [home / ".hermes" / "plugins" / "a2a" / "__init__.py",
                home / ".hermes" / "profiles" / "bot1" / "plugins" / "a2a"
                / "__init__.py"]
        check("per harness: both Hermes bots carry Hermes's own token, and "
              "not the other",
              all(p.is_file() and f'"token": "{TOKEN2}"' in p.read_text()
                  and TOKEN not in p.read_text() for p in bots),
              str([p.is_file() for p in bots]))
        got = [p for p, _ in broker.seen if p.startswith("/install/hermes/")]
        check("per harness: the Hermes clients were downloaded with that token",
              got and all(p == f"/install/hermes/{TOKEN2}" for p in got),
              str([p.replace(TOKEN, "<1>").replace(TOKEN2, "<2>")
                   for p in got]))
        check("per harness: /me checked each token naming no agent",
              sum(1 for p, h in broker.seen if p == "/me"
                  and not h.get("X-A2A-Agent")) == 2,
              str([p for p, _ in broker.seen]))
        check("per harness: the summary says whose token goes where, and its "
              "stations",
              "Token of alice for OpenCode, Pi, Codex" in out
              and "Token of bob for Hermes" in out
              and "stations: bots" in out, out[:2500])

        # --- a refused SECOND token -------------------------------------------
        m5 = root / "m5"
        home, bin_, cwd, log = machine(m5)
        before = snapshot(m5)
        code, out = run_installer(installer, home, bin_, cwd, [
            "all", TOKEN, "1 2 3",
            "a2a_st_wrong1", "a2a_st_wrong2", "a2a_st_wrong3"])
        check("per harness: three refusals of a harness's own token end it",
              code != 0 and "refused" in out, out[-600:])
        check("per harness: and nothing was written", snapshot(m5) == before,
              str(set(snapshot(m5)) - set(before)))

        # --- a refused token ---------------------------------------------------
        m3 = root / "m3"
        home, bin_, cwd, log = machine(m3)
        before = snapshot(m3)
        code, out = run_installer(installer, home, bin_, cwd, [
            "2", "a2a_st_wrong1", "a2a_st_wrong2", "a2a_st_wrong3"])
        check("refused: three refusals end it", code != 0, out[-600:])
        check("refused: it says why", "refused" in out, out[-600:])
        check("refused: and nothing was written", snapshot(m3) == before,
              str(set(snapshot(m3)) - set(before)))

        # --- cancelled ---------------------------------------------------------
        code, out = run_installer(installer, home, bin_, cwd, ["q"])
        check("cancelled: q at the first question writes nothing",
              code != 0 and snapshot(m3) == before, out[-400:])
    finally:
        broker.close()

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — one command installs and updates every client, and asks "
          "before it runs anything")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
