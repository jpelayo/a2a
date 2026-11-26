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
import shutil
import subprocess
import sys
import tarfile
import tempfile
import threading
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
SOURCE = ROOT / "deploy" / "plugin" / "installer" / "a2a-install.py"
TOKEN = "a2a_st_TESTsecretTOKENvalue123"
# A second user's token, in another station: what a harness given its own
# token (Hermes bots on `bots`, say) is installed with.
TOKEN2 = "a2a_st_TESTsecondTOKENforBOTS456"
# Planted in Claude Code's settings, where /plugin keeps its token: it must
# never appear in anything the installer prints.
CC_TOKEN = "a2a_st_CLAUDEsentinelNEVERprinted789"

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
        "broker_installer", ROOT / "deploy" / "a2a_mcp" / "a2a-mcp.py")
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

def machine(root: Path, gateway_chats=False, codex_lazy=False):
    """`gateway_chats`: a Hermes that runs its chats in a gateway, which is
    how the installer tells: `hermes gateway --help` lists `ensure`.
    `codex_lazy`: a Codex 0.172+, whose `codex features list` shows
    mcp_lazy_stdio_startup on (an older one has no such row)."""
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
                     + ('[ "$*" = "gateway --help" ] && echo "  ensure  '
                        'Ensure a local runtime"\n'
                        if gateway_chats and name == "hermes" else "")
                     + ('[ "$*" = "features list" ] && printf "%s\\n" '
                        '"mcp_connection_sharing  under development  false" '
                        '"mcp_lazy_stdio_startup  stable             true"\n'
                        if codex_lazy and name == "codex" else "")
                     + 'case "$*" in "-p bot1 plugins enable a2a") exit 1;; '
                     'esac\n')
        p.chmod(0o755)
    return home, bin_, cwd, log


READ_ONLY = ("hermes gateway --help", "codex features list")


def commands(log: Path) -> list:
    """What was run, without the read-only looks (`hermes gateway --help`,
    `codex features list`)."""
    ran = log.read_text().splitlines() if log.is_file() else []
    return [r for r in ran if r not in READ_ONLY]


def snapshot(root: Path) -> dict:
    return {str(p.relative_to(root)): p.stat().st_size
            for p in root.rglob("*") if p.is_file()}


def run_installer(installer: Path, home, bin_, cwd, answers, extra_env=None):
    env = {"HOME": str(home), "PATH": f"{bin_}:/usr/bin:/bin",
           "TERM": "dumb", "LANG": "C.UTF-8", **(extra_env or {})}
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

    # --- OpenCode's one setting: where, and the JSONC edit ---------------------
    # OpenCode 2 unloads an idle project, and a2a with it, after 60 minutes;
    # experimental.location_idle_timeout = false is its own "never unload".
    keep = {k: os.environ.pop(k, None)
            for k in ("OPENCODE_CONFIG_DIR", "XDG_CONFIG_HOME")}
    try:
        plain_dir = m.opencode_config_dir()
        os.environ["XDG_CONFIG_HOME"] = "/x/cfg"
        xdg_dir = m.opencode_config_dir()
        os.environ["OPENCODE_CONFIG_DIR"] = "/x/oc"
        own_dir = m.opencode_config_dir()
    finally:
        for k, v in keep.items():
            os.environ.pop(k, None)
            if v is not None:
                os.environ[k] = v
    check("setting: OpenCode's global config folder, by OpenCode's rule — "
          "OPENCODE_CONFIG_DIR, else $XDG_CONFIG_HOME/opencode, else "
          "~/.config/opencode",
          plain_dir == home / ".config" / "opencode"
          and xdg_dir == Path("/x/cfg/opencode") and own_dir == Path("/x/oc"),
          f"{plain_dir} {xdg_dir} {own_dir}")
    w, load = m._with_idle_setting, m._jsonc_load
    on = {"location_idle_timeout": False}
    check("setting: an empty file becomes a config holding only the setting",
          json.loads(w("")) == {"experimental": on} and w("\n  \n") == w(""))
    check("setting: {} gets it", load(w("{}")) == {"experimental": on})
    check("setting: a one-line file gets it as its first member, the rest "
          "kept as it was",
          w('{"model": "x/y"}') == '{"experimental": { '
          '"location_idle_timeout": false }, "model": "x/y"}',
          w('{"model": "x/y"}'))
    src = ('{\n  // a comment holding { and "quotes"\n'
           '  "url": "http://h/{x}", /* and this one */\n'
           '  "experimental": {\n    "subagent_depth": 2,\n  },\n}\n')
    got = w(src)
    check("setting: into an existing experimental object, as its first "
          "member — comments, trailing commas and every other byte kept",
          got == src.replace('"experimental": {', '"experimental": {\n'
                             '    "location_idle_timeout": false,', 1), got)
    check("setting: …and OpenCode would read the same config plus the setting",
          load(got) == {"url": "http://h/{x}",
                        "experimental": {**on, "subagent_depth": 2}})
    crlf = w('{\r\n  "model": "x"\r\n}\r\n')
    check("setting: a CRLF file stays CRLF",
          crlf == '{\r\n  "experimental": { "location_idle_timeout": false },'
                  '\r\n  "model": "x"\r\n}\r\n', repr(crlf))
    check("setting: a byte-order mark stays where it was",
          w('﻿{"a": 1}').startswith('﻿{"experimental"'))
    check("setting: a repeated experimental key — the last one counts, so "
          "it goes there",
          load(w('{"experimental": {"a": true}, "experimental": {"b": true}}'))
          == {"experimental": {**on, "b": True}})
    check("setting: one already set, to any value, is left alone",
          w('{"experimental": {"location_idle_timeout": "4 hours"}}') is None
          and w('{"experimental": {"location_idle_timeout": false}}') is None)
    refused = []
    for bad in ('{"model": ', '[1]', '{"experimental": true}',
                '{"a": 1} {"b": 2}', '{"a": "unterminated}', '{"a": 1 /* }'):
        try:
            w(bad)
        except ValueError:
            refused.append(bad)
    check("setting: anything OpenCode could not read is refused, not edited",
          len(refused) == 6, str(refused))


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
        check("fresh: each bot's line names the profile it was baked for — "
              "Hermes copies the plugins folder when it clones a bot",
              '"profile": "default"' in (hd / "__init__.py").read_text()
              and '"profile": "bot1"' in (hb / "__init__.py").read_text())
        check("fresh: a profile folder with no Hermes identity file is no bot",
              not (home / ".hermes" / "profiles" / "empty" / "plugins").exists())
        ran = commands(log)
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
        check("fresh: a Hermes with no gateway chats gets no warning and no "
              "setting changed", "orphan_end_grace_s" not in out, out[-1500:])
        check("fresh: the next steps name what no installer can do",
              "plugin:a2a@skills-dir" in steps and "enable a2a here" in steps
              and "Codex: open it as always, with `codex`." in steps
              and "app-server --listen" not in steps, steps[:1500])
        occ = home / ".config" / "opencode" / "opencode.jsonc"
        check("fresh: OpenCode's global config gets its one setting, in a new "
              "opencode.jsonc — the file OpenCode itself writes when there is "
              "none",
              occ.is_file() and json.loads(occ.read_text())
              == {"experimental": {"location_idle_timeout": False}},
              occ.read_text() if occ.is_file() else "absent")
        check("fresh: the confirmation screen showed it, and why, before "
              "anything was written",
              out.find("Setting it will change") >= 0
              and out.find("Setting it will change") < out.find("Installed")
              and "experimental.location_idle_timeout = false" in out
              and "60 idle minutes" in out, out[:2500])
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

        check("fresh: Hermes bots still off share ONE 'enable a2a here' step",
              "Hermes bots 'default' and 'bot1': in the Hermes app" in steps
              and steps.count("enable a2a here") == 2, steps[:1500])
        check("fresh: the last line is the verdict, counting what needs you",
              out.rstrip().splitlines()[-1].startswith("⚠ 2 things need you"),
              out[-300:])

        # Claude Code set up the way /plugin leaves it: on, with its token.
        # The installer reads that the key EXISTS; the value is never read
        # into anything, printed or written.
        cc_settings = home / ".claude" / "settings.json"
        cc_settings.write_text(json.dumps({
            "enabledPlugins": {"a2a@skills-dir": True},
            "pluginConfigs": {"a2a@skills-dir": {"options": {
                "token": CC_TOKEN}}}}, indent=2))
        cc_seeded = cc_settings.read_bytes()

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
        check("update: the OpenCode setting, already there, is left alone and "
              "not shown again",
              "Setting it will change" not in out and json.loads(
                  occ.read_text()) == {"experimental":
                                       {"location_idle_timeout": False}},
              out[:2500])
        check("update: it shows the installed version and the new one",
              "installed 0.0.9" in out and f"installed {b.VERSION}" in out,
              out[:1500])
        check("update: Codex is brought to the broker's version",
              f'"version": "{b.VERSION}"' in cx.read_text())
        check("update: a client that was behind is told to restart",
              "Codex: close the Codex sessions you have open and open them "
              "again" in out, out[-1500:])
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
        ran = commands(log)
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
              out.rstrip().splitlines()[-1] == "✓ Done — nothing to do."
              and "restart" not in out and "Needs you" not in out,
              out[-1200:])
        check("claude: on, with its token — no warning, Claude Code's settings "
              "byte-identical, and the token never printed",
              cc_settings.read_bytes() == cc_seeded and CC_TOKEN not in out,
              out[-600:])
        check("update: files it did not ship are left alone (Pi's "
              "node_modules)", (pi / "node_modules" / "keep.txt").is_file())
        check("update: the default bot keeps the id it already had",
              '"agent": "botd"' in (hd / "__init__.py").read_text())
        ran = commands(log)
        check("update: codex mcp add is not offered again once Codex has "
              "the server registered",
              ran and not any(r.startswith("codex") for r in ran),
              "\n".join(ran))

        # --- a bot Hermes cloned from the default one ------------------------
        # The app's "new bot" clones `default`: config.yaml and the plugins
        # folder are copied (hermes_cli/profiles.py), so the clone holds the
        # default bot's plugin with ITS id baked in.
        hc = home / ".hermes" / "profiles" / "clone"
        hc.mkdir(parents=True)
        (hc / "config.yaml").write_text("model: x\n")
        shutil.copytree(hd, hc / "plugins" / "a2a")
        check("clone: the copy carries the default bot's id (the setup)",
              '"agent": "botd"' in (hc / "plugins" / "a2a" / "__init__.py")
              .read_text())
        # bot1 was switched on since, by the plugin, which keeps its switch
        # in its own state (plugin-data/), not in config.yaml any more.
        b1 = home / ".hermes" / "profiles" / "bot1"
        st = b1 / "plugin-data" / "agent-plugin-a2a-0123abcd" / "state.json"
        st.parent.mkdir(parents=True)
        st.write_text(json.dumps({"enabled": True,
                                  "home": str(b1.resolve())}))
        code, out = run_installer(installer, home, bin_, cwd, [
            "",             # ENTER: the installed ones, already ticked
            TOKEN,
            "",             # ENTER: every harness uses it
            "",             # ENTER: default, bot1 and the clone, all installed
            "",             # ENTER: keep the default bot's id
            "y",
            "n",
        ])
        check("clone: the installer finishes", code == 0, out[-1500:])
        cl = next((ln for ln in (hc / "plugins" / "a2a" / "__init__.py")
                   .read_text().splitlines()
                   if ln.startswith("A2A_BAKED = ")), "")
        check("clone: the copied id is NOT kept — the clone is named by its "
              "own profile, and its line says so",
              '"agent": ""' in cl and '"profile": "clone"' in cl,
              cl.replace(TOKEN, "<token>"))
        check("clone: the default bot keeps its id",
              '"agent": "botd"' in (hd / "__init__.py").read_text()
              and '"profile": "default"' in (hd / "__init__.py").read_text())
        steps = out.split("Next steps", 1)[-1]
        check("clone: a bot switched on in the plugin's own state is not "
              "asked to 'enable a2a here' again; the clone is",
              "Hermes bot 'bot1': in the Hermes app" not in steps
              and "Hermes bot 'clone': in the Hermes app" in steps,
              steps[:1500])

        # --- commands declined ---------------------------------------------
        m2 = root / "m2"
        home, bin_, cwd, log = machine(m2)
        code, out = run_installer(installer, home, bin_, cwd, [
            "4", TOKEN, "y", "n"])
        check("declined: the install still finishes", code == 0, out[-800:])
        check("declined: no command ran — only the read-only look at "
              "`codex features list`", not commands(log),
              log.read_text() if log.exists() else "")
        check("declined: and they are under Needs you, to run by hand",
              "they are under Needs you" in out
              and "codex mcp add a2a" in out.split("Needs you", 1)[-1],
              out[-800:])
        check("declined: with OpenCode not picked, its config is not touched",
              not list((home / ".config" / "opencode").iterdir())
              and "Setting it will change" not in out)
        check("declined: an older Codex (no mcp_lazy_stdio_startup row in "
              "`codex features list`) is not asked to disable it — `codex "
              "features disable` refuses a feature it does not know",
              "features disable" not in out, out[-800:])

        # --- Codex 0.172: MCP servers start lazily ----------------------------
        m3 = root / "m3"
        home, bin_, cwd, log = machine(m3, codex_lazy=True)
        code, out = run_installer(installer, home, bin_, cwd, [
            "4", TOKEN, "y", "y"])
        ran = commands(log)
        check("lazy: on a Codex that starts MCP servers lazily, the installer "
              "offers Codex's own command to turn that off, after the "
              "registration, and runs it on yes",
              code == 0 and ran == [
                  f"codex mcp add a2a -- python3 "
                  f"{home / '.codex' / 'a2a' / 'a2a-codex.py'}",
                  "codex features disable mcp_lazy_stdio_startup"],
              "\n".join(ran) + out[-600:])
        check("lazy: before it is run, it says what it changes, for every "
              "MCP server",
              0 <= out.find("as before 0.172") < out.find("Run these"),
              out[-2500:])
        check("the next steps say to open `codex` as always, and show no "
              "launch line",
              "Codex: open it as always, with `codex`." in out
              and "app-server --listen" not in out
              and "--remote" not in out, out[-1500:])
        (home / ".codex" / "config.toml").write_text(
            '[mcp_servers.a2a]\ncommand = "python3"\n\n'
            "[features]\nmcp_lazy_stdio_startup = true\n")
        log.unlink()
        code, out = run_installer(installer, home, bin_, cwd, [
            "4", TOKEN, "y", "y"])
        ran = log.read_text().splitlines() if log.is_file() else []
        check("lazy: a value config.toml already sets — even `true` — is left "
              "alone, and Codex is not even asked",
              code == 0 and not any("features" in r for r in ran),
              "\n".join(ran))

        # --- Claude Code's a2a switched off ----------------------------------
        # What a real machine had for days: /plugin switched a2a off, its
        # hook cleared the token, and reinstalling said nothing about it.
        m11 = root / "m11"
        home, bin_, cwd, log = machine(m11)
        code, out = run_installer(installer, home, bin_, cwd, ["1", "y"])
        cc_settings = home / ".claude" / "settings.json"
        cc_settings.write_text(json.dumps(
            {"enabledPlugins": {"a2a@skills-dir": False}}))
        cc_seeded = cc_settings.read_bytes()
        code, out = run_installer(installer, home, bin_, cwd,
                                  ["2", TOKEN, "y"])     # OpenCode only
        confirm = out.split("Install? [Y/n]", 1)[0]
        after = out.split("Install? [Y/n]", 1)[-1]
        check("claude off: said on the confirmation screen, before anything "
              "is written — with Claude Code not even picked",
              "switched OFF" in confirm and "Needs you" in confirm,
              confirm[-900:])
        check("claude off: and first under Needs you, with the way back: "
              "/plugin → a2a → Enable → paste your token, then the launch line",
              after.find("Needs you") < after.find("switched OFF")
              and "its token is gone" in after
              and "/plugin → a2a → Enable → paste your token" in after
              and "plugin:a2a@skills-dir" in after, after[-900:])
        check("claude off: the verdict counts it",
              out.rstrip().splitlines()[-1] == "⚠ 1 thing needs you (above).",
              out[-300:])
        check("claude off: Claude Code's settings are left exactly as they "
              "were", cc_settings.read_bytes() == cc_seeded)
        cc_settings.write_text(json.dumps({
            "enabledPlugins": {"a2a@skills-dir": False},
            "pluginConfigs": {"a2a@skills-dir": {"token": CC_TOKEN}}}))
        code, out = run_installer(installer, home, bin_, cwd,
                                  ["2", TOKEN, "y"])
        after = out.split("Install? [Y/n]", 1)[-1]
        check("claude off, token kept: just Enable — no token to paste, and "
              "the token is never printed",
              "switched OFF." in after and "token is gone" not in after
              and "Enable. Then start" in after and CC_TOKEN not in out,
              after[-700:])
        cc_settings.write_text(json.dumps(
            {"enabledPlugins": {"a2a@skills-dir": True}}))
        code, out = run_installer(installer, home, bin_, cwd,
                                  ["2", TOKEN, "y"])
        check("claude on but no token: told how to give it one",
              "a2a has no token" in out and "paste your token" in out,
              out[-700:])
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

        # --- Kilo Code and dsh: each its own client ---------------------------
        m6 = root / "m6"
        home, bin_, cwd = m6 / "home", m6 / "bin", m6 / "cwd"
        for d in (home / ".config" / "kilo", home / ".dsh" / "profiles" / "desktop",
                  bin_, cwd):
            d.mkdir(parents=True, exist_ok=True)
        log = m6 / "commands.log"
        for name in ("kilo", "dsh"):
            p = bin_ / name
            p.write_text(f'#!/bin/sh\necho "{name} $*" >> "{log}"\n')
            p.chmod(0o755)
        code, out = run_installer(installer, home, bin_, cwd, [
            "all",          # the two harnesses found: Kilo Code, dsh
            TOKEN,
            "",             # ENTER: both use it
            "y",            # install
            "y",            # run the commands
        ])
        check("kilo+dsh: the installer finishes", code == 0, out[-1500:])
        kf = home / ".config" / "kilo" / "plugins" / "a2a-kilocode.js"
        ksrc = (ROOT / "deploy" / "plugin" / "kilocode" / "a2a-kilocode.js").read_text()
        khead, _, krest = (kf.read_text() if kf.is_file() else "").partition("\n")
        check("kilo+dsh: Kilo Code lands as ONE file in ~/.config/kilo/plugins, "
              "its own client with credentials baked",
              khead.startswith("const A2A_BAKED = {") and f'"token": "{TOKEN}"' in khead
              and krest == ksrc, khead[:80])
        check("kilo+dsh: and only in plugins/ — Kilo also scans plugin/, and "
              "a second copy would load twice",
              not (home / ".config" / "kilo" / "plugin").exists())
        pkg = home / ".dsh" / "a2a" / "plugin"
        check("kilo+dsh: dsh's package lands in ~/.dsh/a2a/plugin",
              sorted(p.name for p in pkg.iterdir()) == [
                  "a2a-dsh.js", "cordis.patch.yml", "index.js", "package.json"]
              if pkg.is_dir() else False,
              str(sorted(p.name for p in pkg.iterdir())) if pkg.is_dir() else "absent")
        dsrc = (ROOT / "deploy" / "plugin" / "dsh" / "a2a-dsh.js").read_text()
        dhead, _, drest = ((pkg / "a2a-dsh.js").read_text()
                           if (pkg / "a2a-dsh.js").is_file() else "").partition("\n")
        check("kilo+dsh: with dsh's own client, credentials baked",
              dhead.startswith("const A2A_BAKED = {") and drest == dsrc, dhead[:80])
        ran = log.read_text().splitlines() if log.is_file() else []
        check("kilo+dsh: it ran dsh's own command for dsh web's profile and, "
              "since the Desktop app made one, for the desktop profile",
              ran == [f"dsh plugin --profile web add link:{pkg}",
                      f"dsh plugin --profile desktop add link:{pkg}"], str(ran))
        check("kilo+dsh: and nothing for Kilo, whose folder is enough",
              not any(r.startswith("kilo ") for r in ran), str(ran))
        check("kilo+dsh: nothing is created for OpenCode",
              not (home / ".config" / "opencode").exists())
        check("kilo+dsh: the next steps say to restart dsh, where Kilo "
              "receives, and where the privacy settings are",
              "restart dsh web" in out and "plain `kilo`" in out
              and "telemetry" in out and "README" in out, out[-1800:])

        # --- Antigravity: the plugin folder, written where agy keeps it --------
        m8 = root / "m8"
        home, bin_, cwd = m8 / "home", m8 / "bin", m8 / "cwd"
        for d in (home / ".gemini" / "antigravity-cli", bin_, cwd):
            d.mkdir(parents=True, exist_ok=True)
        log = m8 / "commands.log"
        p = bin_ / "agy"
        p.write_text(f'#!/bin/sh\necho "agy $*" >> "{log}"\n')
        p.chmod(0o755)
        code, out = run_installer(installer, home, bin_, cwd, [
            "all",          # the one harness found: Antigravity
            TOKEN,
            "y",            # install
        ])
        check("antigravity: the installer finishes", code == 0, out[-1500:])
        pdir = home / ".gemini" / "config" / "plugins" / "a2a"
        names = sorted(str(q.relative_to(pdir)) for q in pdir.rglob("*")
                       if q.is_file()) if pdir.is_dir() else []
        check("antigravity: the plugin lands in ~/.gemini/config/plugins/a2a, "
              "where agy keeps installed plugins",
              names == ["a2a-antigravity.py", "a2a-stop.py", "hooks.json",
                        "mcp_config.json", "plugin.json", "rules/AGENTS.md",
                        "wait.sh"],
              str(names))
        srv = (pdir / "a2a-antigravity.py").read_text() if names else ""
        check("antigravity: with the token baked into the server's seam",
              f'"token": "{TOKEN}"' in srv and "A2A_BAKED = {}" not in srv,
              srv[:120])
        check("antigravity: and no agy command was run — the folder is the "
              "install", not log.is_file(), log.read_text() if log.is_file()
              else "")
        check("antigravity: the next steps say to restart agy, the one allow "
              "rule to add, and that the app only sends",
              "quit agy" in out and "mcp(a2a_a2a/*)" in out
              and "/permissions" in out and "only sends" in out
              and "plugins/a2a/wait.sh` outside the sandbox" in out
              and "enable a2a here" in out, out[-1800:])
        code, out = run_installer(installer, home, bin_, cwd, [
            "all", TOKEN, "y"])
        check("antigravity: run again, it is already current and asks for no "
              "restart", code == 0 and "already current" in out
              and "quit agy" not in out, out[-1200:])

        # --- a Hermes that runs its chats in a gateway ------------------------
        m7 = root / "m7"
        home, bin_, cwd, log = machine(m7, gateway_chats=True)
        code, out = run_installer(installer, home, bin_, cwd, [
            "all", TOKEN, "", "all", "botd",
            "y",            # install
            "y",            # run the commands
        ])
        check("gateway: the installer finishes", code == 0, out[-1500:])
        ran = commands(log)
        check("gateway: it changes no Hermes setting and warns of nothing: the "
              "plugin opens a chat itself when a message needs it",
              "orphan_end_grace_s" not in out + "\n".join(ran)
              and "Warning" not in out, out[-1500:])
        check("gateway: Hermes is asked once whether it runs chats in a "
              "gateway", log.read_text().splitlines().count(
                  "hermes gateway --help") == 1, log.read_text())
        steps = out.split("Next steps", 1)[-1]
        check("gateway: the next step restarts its gateway, since it outlives "
              "the app — and the installer runs no restart itself",
              "hermes gateway restart" in steps
              and "quit the Hermes app" not in steps
              and not any("gateway restart" in r for r in ran), steps[:1500])
        check("gateway: and it says a2a keeps receiving after the app quits",
              "keeps receiving" in steps, steps[:1500])

        # --- OpenCode's setting in a config you already have -----------------
        # In $XDG_CONFIG_HOME, as OpenCode looks; with comments, a trailing
        # comma, another experimental key, a provider key and 0600.
        m8 = root / "m8"
        home, bin_, cwd, log = machine(m8)
        xdg = {"XDG_CONFIG_HOME": str(m8 / "xdg")}
        cfg = m8 / "xdg" / "opencode" / "opencode.json"
        cfg.parent.mkdir(parents=True)
        secret = "sk-SECRET-PROVIDER-KEY-123"
        orig = ('{\n  // my provider\n  "provider": {"x": {"options": '
                f'{{"apiKey": "{secret}"}}}}}},\n'
                '  "experimental": {\n    "subagent_depth": 2,\n  },\n}\n')
        cfg.write_text(orig)
        cfg.chmod(0o600)
        code, out = run_installer(installer, home, bin_, cwd,
                                  ["2", TOKEN, "y"], xdg)
        new = cfg.read_text()
        check("setting: an existing config gets it where OpenCode looks "
              "($XDG_CONFIG_HOME/opencode), with no second file",
              code == 0 and new != orig
              and not (home / ".config" / "opencode" / "opencode.jsonc")
              .exists(), out[-1200:])
        check("setting: added inside its experimental object, every other "
              "byte kept — comments and trailing commas included",
              new == orig.replace('"experimental": {', '"experimental": {\n'
                                  '    "location_idle_timeout": false,', 1),
              new.replace(secret, "<key>"))
        check("setting: the file keeps its own permissions (0600 here)",
              cfg.stat().st_mode & 0o777 == 0o600,
              oct(cfg.stat().st_mode & 0o777))
        check("setting: nothing of the file's content is ever printed — it "
              "may hold provider keys",
              secret not in out and "my provider" not in out
              and "subagent_depth" not in out, "content printed")
        code, out = run_installer(installer, home, bin_, cwd,
                                  ["2", TOKEN, "y"], xdg)
        check("setting: run again, it is there: untouched and not shown",
              code == 0 and cfg.read_text() == new
              and "Setting it will change" not in out, out[-1200:])

        # A value you set yourself wins.
        m9 = root / "m9"
        home, bin_, cwd, log = machine(m9)
        own = home / ".config" / "opencode" / "opencode.json"
        mine = '{"experimental": {"location_idle_timeout": "4 hours"}}\n'
        own.write_text(mine)
        code, out = run_installer(installer, home, bin_, cwd,
                                  ["2", TOKEN, "y"])
        check("setting: one you set yourself, to any value, is left alone — "
              "not edited, not shown, no second file",
              code == 0 and own.read_text() == mine
              and "Setting it will change" not in out
              and not (home / ".config" / "opencode" / "opencode.jsonc")
              .exists(), out[-1200:])

        # A config OpenCode cannot read is not edited.
        m10 = root / "m10"
        home, bin_, cwd, log = machine(m10)
        bad = home / ".config" / "opencode" / "opencode.jsonc"
        bad.write_text('{"model": ')
        code, out = run_installer(installer, home, bin_, cwd,
                                  ["2", TOKEN, "y"])
        steps = out.split("Next steps", 1)[-1]
        check("setting: a config OpenCode cannot read is left as it was, the "
              "install still finishes, and a next step says why",
              code == 0 and bad.read_text() == '{"model": '
              and "could not set experimental.location_idle_timeout = false "
                  "in ~/.config/opencode/opencode.jsonc" in steps
              and "Setting it will change" not in out, steps[:1500])
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
