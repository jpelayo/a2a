#!/usr/bin/env python3
"""a2a installer: installs or updates the a2a client of every harness here.

    curl -fsSL https://<broker>/install | python3

Served by the broker with its url and the client version it serves baked in,
and NOTHING else: the token is typed (or pasted) into a masked field, checked
against /me, and only ever sent to the broker's own install routes — the same
routes the manual lines use. It is never printed, logged, or read back out of
an installed copy.

What it does, in order:
  1. finds the harnesses on this machine and shows which carry an a2a client,
     at which version;
  2. asks which to install or update (installed ones start ticked, so an
     update is ENTER);
  3. asks for the token when a selected client needs one (all but Claude
     Code, whose token lives in its own /plugin screen);
  4. for Hermes, asks which bots (profiles), and an agent id for a profile
     whose name is no id (`default`);
  5. downloads everything, shows every file it will write, and waits for
     ENTER;
  6. writes the files — overwriting in place, deleting nothing;
  7. lists the harness commands still needed (`codex mcp add`, `hermes …
     plugins enable`, …) and asks before running any of them;
  8. prints what is left for you: launch lines, "enable a2a here", approval.

The screens are curses, like `hermes plugins`. With no terminal for them it
asks the same questions as plain line prompts. Standard library only, Python
3.8+, POSIX only.
"""
import io
import json
import locale
import os
import re
import shlex
import shutil
import subprocess
import sys
import tarfile
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

A2A_BAKED = {}

URL = (A2A_BAKED.get("url") or "").rstrip("/")
VERSION = A2A_BAKED.get("version") or ""

HOME = Path.home()

# Hermes counts a directory as a profile only when it holds one of these.
# A bare directory is ignored by `profile list`, by gateways and by -p, so it
# is not offered here either.
HERMES_IDENTITY = ("config.yaml", ".env", "SOUL.md", "profile.yaml",
                   "auth.json", "state.db")
# Profile names that name no bot (the Hermes client refuses them as ids).
GENERIC_PROFILES = {"default", "custom", ""}
AGENT_ID = re.compile(r"^[^\s/]{1,128}$")

CODEX_LAUNCH = ("codex app-server --listen unix://$TMPDIR/a2a-$$.sock "
                "& sleep 1; codex --remote unix://$TMPDIR/a2a-$$.sock")
CLAUDE_LAUNCH = ("claude --dangerously-load-development-channels "
                 "plugin:a2a@skills-dir")


# ---------------------------------------------------------------------------
# Where each client lives, and what version an installed copy is
# ---------------------------------------------------------------------------

HARNESSES = (
    # key, name, needs token, harness home, binary
    ("claude", "Claude Code", False, ".claude", "claude"),
    ("opencode", "OpenCode", True, ".config/opencode", "opencode"),
    ("pi", "Pi", True, ".pi/agent", "pi"),
    ("codex", "Codex", True, ".codex", "codex"),
    ("hermes", "Hermes (bots)", True, ".hermes", "hermes"),
)
NAMES = {h[0]: h[1] for h in HARNESSES}


def install_dir(key, profile_home=None):
    """Where a client is written. Hermes: per bot, so per profile home."""
    return {
        "claude": HOME / ".claude" / "skills" / "a2a",
        "opencode": HOME / ".config" / "opencode" / "plugins",
        "pi": HOME / ".pi" / "agent" / "extensions" / "a2a",
        "codex": HOME / ".codex" / "a2a",
    }.get(key) or (profile_home / "plugins" / "a2a")


BAKED_LINE = re.compile(r"^(const |globalThis\.)?A2A_BAKED = \{")


def _baked_field(path, field):
    """One field of a client's baked line, WITHOUT returning the line.

    OpenCode, Pi, Codex and Hermes carry url, token, agent and version on one
    line. Only the asked-for field is pulled out of it; the line — and the
    token on it — is never returned, printed or kept. None: no such file.
    """
    try:
        with open(path, encoding="utf-8", errors="replace") as f:
            for line in f:
                if BAKED_LINE.match(line):
                    m = re.search(r'"%s":\s*"([^"]*)"' % re.escape(field),
                                  line)
                    return m.group(1) if m else ""
    except OSError:
        return None
    return ""


def client_file(key, profile_home=None):
    """The installed file that carries a client's baked line."""
    d = install_dir(key, profile_home)
    return {"opencode": d / "a2a-opencode.js", "pi": d / "index.ts",
            "codex": d / "a2a-codex.py", "hermes": d / "__init__.py"}.get(key)


def installed_version(key, profile_home=None):
    """The installed client's version; "" if installed but unstamped; None
    if not installed."""
    d = install_dir(key, profile_home)
    if key == "claude":
        p = d / ".mcp.json"
        if not p.is_file():
            return None
        try:
            env = json.loads(p.read_text())["mcpServers"]["a2a-channel"]["env"]
            return env.get("A2A_CLIENT_VERSION") or ""
        except Exception:
            return ""
    if key == "hermes":
        p = d / "plugin.yaml"
        if not p.is_file():
            return None
        m = re.search(r'(?m)^version:\s*"?([^"\s]*)"?', p.read_text())
        v = m.group(1) if m else ""
        return "" if v == "0.0.0" else v
    p = client_file(key)
    if not p.is_file():
        return None
    return _baked_field(p, "version") or ""


def harness_found(key):
    home, binary = next((h[3], h[4]) for h in HARNESSES if h[0] == key)
    return (HOME / home).is_dir() or shutil.which(binary) is not None


def hermes_profiles():
    """(name, home) of every Hermes bot here: `default` is ~/.hermes itself."""
    root = HOME / ".hermes"
    found = []
    if any((root / f).exists() for f in HERMES_IDENTITY):
        found.append(("default", root))
    pdir = root / "profiles"
    if pdir.is_dir():
        for d in sorted(pdir.iterdir()):
            if d.is_dir() and any((d / f).exists() for f in HERMES_IDENTITY):
                found.append((d.name, d))
    return found


def codex_registered():
    """Whether ~/.codex/config.toml already registers an `a2a` MCP server."""
    p = HOME / ".codex" / "config.toml"
    if not p.is_file():
        return False
    try:
        import tomllib
        with open(p, "rb") as f:
            return "a2a" in (tomllib.load(f).get("mcp_servers") or {})
    except Exception:
        text = p.read_text(errors="replace")
        return re.search(r'(?m)^\s*\[mcp_servers\.("a2a"|a2a)\]', text) \
            is not None


# ---------------------------------------------------------------------------
# Rows and the checklist keys — pure, so they are testable with no terminal
# ---------------------------------------------------------------------------

def _state(v, found):
    # Not "current": the version stays the same across many changes to a
    # client, so whether the files are current is only known after the
    # download, by comparing them (Target.same).
    if v is not None:
        return (f"installed {v or '(unstamped)'} {G.arrow} {VERSION}"
                if v != VERSION else f"installed {VERSION}")
    return "found, not installed" if found else "not found"


def harness_rows():
    """One row per harness. Installed ones start ticked: an update is ENTER."""
    rows = []
    for key, name, _, _, _ in HARNESSES:
        found = harness_found(key)
        if key == "hermes":
            vs = [installed_version("hermes", h) for _, h in hermes_profiles()]
            have = [v for v in vs if v is not None]
            v = (have[0] if len(set(have)) == 1 else "mixed") if have else None
        else:
            v = installed_version(key)
        rows.append({"key": key, "label": f"{name} {G.dash} {_state(v, found)}",
                     "enabled": found or v is not None,
                     "checked": v is not None})
    return rows


def key_name(k, typing=False):
    """A typed character as a key name. `typing` (an input field): letters
    stay letters, so j, k, q and space are text there, not navigation."""
    special = {"\n": "enter", "\r": "enter", "\x1b": "esc",
               "\x7f": "backspace", "\b": "backspace", "\x15": "clear"}
    if k in special:
        return special[k]
    if typing:
        return k
    return {"k": "up", "j": "down", " ": "toggle", "q": "esc"}.get(k, k)


def checklist_key(rows, cursor, key):
    """Apply one key to a checklist: (rows, cursor, done).

    `key` is a name — up, down, top, bottom, pgup, pgdn, toggle, enter, esc —
    so the curses screen and the tests share this exactly. `done` is None
    while the list is open, "ok" on ENTER with at least one row ticked,
    "cancel" on ESC. A row that is not enabled (harness not found) can be
    neither ticked nor confirmed.
    """
    rows = [dict(r) for r in rows]
    last = len(rows) - 1
    if key == "up":
        cursor = max(0, cursor - 1)
    elif key == "down":
        cursor = min(last, cursor + 1)
    elif key == "top":
        cursor = 0
    elif key == "bottom":
        cursor = last
    elif key == "pgup":
        cursor = max(0, cursor - 10)
    elif key == "pgdn":
        cursor = min(last, cursor + 10)
    elif key == "toggle" and rows and rows[cursor]["enabled"]:
        rows[cursor]["checked"] = not rows[cursor]["checked"]
    elif key == "enter":
        if any(r["checked"] and r["enabled"] for r in rows):
            return rows, cursor, "ok"
    elif key == "esc":
        return rows, cursor, "cancel"
    return rows, cursor, None


# ---------------------------------------------------------------------------
# Talking to the broker
# ---------------------------------------------------------------------------

SECRET = {"token": ""}


def redact(text):
    """Every line that leaves this program goes through here."""
    t = SECRET["token"]
    text = str(text)
    if t:
        text = text.replace(t, "<token>")
        text = text.replace(urllib.parse.quote(t, safe=""), "<token>")
    return text


def _get(path, token=None, timeout=30):
    headers = {"User-Agent": f"a2a-install/{VERSION}"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    req = urllib.request.Request(URL + path, headers=headers)
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read()


def check_token(token):
    """/me for this token, or None when the broker refuses it.

    No agent is named: /me answers a bare token, and naming an agent would
    pin an unowned one to it.
    """
    try:
        return json.loads(_get("/me", token=token, timeout=20))
    except urllib.error.HTTPError as e:
        if e.code in (401, 403):
            return None
        raise


def fetch(key, token, agent=""):
    q = urllib.parse.quote(token, safe="")
    path = {"claude": "/a2a-claudecode.tar.gz", "opencode": f"/install/{q}",
            "pi": f"/install/pi/{q}", "codex": f"/install/codex/{q}",
            "hermes": f"/install/hermes/{q}"}[key]
    if key != "claude" and agent:
        path += "?agent=" + urllib.parse.quote(agent, safe="")
    return _get(path)


def unpack(data):
    """[(relative path, bytes, mode)] from a .tar.gz, refusing anything that
    is not a plain file at a plain relative path."""
    out = []
    with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as t:
        for m in t.getmembers():
            if m.isdir():
                continue
            parts = Path(m.name).parts
            if (not m.isfile() or m.name.startswith("/") or not parts
                    or ".." in parts):
                raise ValueError(f"refusing archive member {m.name!r}")
            out.append((Path(*parts), t.extractfile(m).read(),
                        (m.mode & 0o777) or 0o644))
    return out


def write_file(path, data, mode):
    """Write beside, then rename over: a crash never leaves half a file."""
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_name(f".{path.name}.a2a-tmp")
    with open(tmp, "wb") as f:
        f.write(data)
    os.chmod(tmp, mode)
    os.replace(tmp, path)


def tilde(p):
    s = str(p)
    h = str(HOME)
    return "~" + s[len(h):] if s == h or s.startswith(h + os.sep) else s


# ---------------------------------------------------------------------------
# Glyphs: unicode where the terminal takes it
# ---------------------------------------------------------------------------

class G:
    check, cross, dot, arrow, dash, bullet, updown = (
        "x", "!", "*", "->", "-", "-", "up/down")

    @classmethod
    def setup(cls):
        try:
            locale.setlocale(locale.LC_ALL, "")
        except locale.Error:
            pass
        if "utf" in (locale.getpreferredencoding(False) or "").lower():
            (cls.check, cls.cross, cls.dot, cls.arrow, cls.dash, cls.bullet,
             cls.updown) = ("✓", "✗", "•", "→", "—",
                            "·", "↑↓")


# ---------------------------------------------------------------------------
# Two faces for the same questions
# ---------------------------------------------------------------------------

class Cancelled(Exception):
    pass


class PlainUI:
    """Line prompts. Used with no curses terminal, and by the tests."""

    def __init__(self, inp):
        self.inp = inp

    def _line(self, prompt):
        sys.stdout.write(prompt)
        sys.stdout.flush()
        line = self.inp.readline()
        if not line:
            raise Cancelled()
        return line.rstrip("\r\n")

    def say(self, *lines):
        for line in lines:
            print(redact(line))
        sys.stdout.flush()

    def status(self, text):
        self.say(text)

    def notice(self, title, lines):
        self.say("", title, *lines)

    def checklist(self, title, rows):
        while True:
            self.say("", f"{title}")
            for i, r in enumerate(rows, 1):
                box = (G.check if r["checked"] else " ") if r["enabled"] else "-"
                self.say(f"  {i}) [{box}] {r['label']}")
            ans = self._line("Numbers to install, separated by spaces "
                             "(ENTER keeps the ticked ones, q cancels): ")
            ans = ans.strip().lower()
            if ans in ("q", "quit", "esc"):
                raise Cancelled()
            if not ans:
                picked = [i for i, r in enumerate(rows) if r["checked"]]
            elif ans == "all":
                picked = [i for i, r in enumerate(rows) if r["enabled"]]
            else:
                try:
                    picked = sorted({int(x) - 1
                                     for x in re.split(r"[\s,]+", ans) if x})
                except ValueError:
                    self.say("  type numbers from the list")
                    continue
                if any(i < 0 or i >= len(rows) or not rows[i]["enabled"]
                       for i in picked):
                    self.say("  only rows that were found can be picked")
                    continue
            if picked:
                return picked
            self.say("  pick at least one")

    def ask(self, title, prompt, default="", secret=False, error=""):
        if error:
            self.say(f"  {error}")
        if secret and hasattr(self.inp, "isatty") and self.inp.isatty():
            import getpass
            value = getpass.getpass(f"{prompt}: ", stream=sys.stdout)
        else:
            shown = f" [{default}]" if default else ""
            value = self._line(f"{prompt}{shown}: ")
        return value.strip() or default

    def confirm(self, title, lines, prompt="Install?"):
        self.say("", title, *lines)
        return self.yes_no(prompt)

    def yes_no(self, prompt):
        ans = self._line(f"{prompt} [Y/n] ").strip().lower()
        return ans in ("", "y", "yes")

    def close(self):
        pass


class CursesUI:
    """The same questions as screens, in the manner of `hermes plugins`."""

    def __init__(self):
        import curses
        self.c = curses
        os.environ.setdefault("ESCDELAY", "25")
        self.scr = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.scr.keypad(True)
        try:
            curses.curs_set(0)
        except curses.error:
            pass
        self.head = self.ok = curses.A_BOLD
        self.dim = curses.A_DIM
        if curses.has_colors():
            try:
                curses.start_color()
                curses.use_default_colors()
                curses.init_pair(1, curses.COLOR_YELLOW, -1)
                curses.init_pair(2, curses.COLOR_GREEN, -1)
                self.head = curses.color_pair(1) | curses.A_BOLD
                self.ok = curses.color_pair(2)
            except curses.error:
                pass

    def close(self):
        try:
            self.c.endwin()
        except Exception:
            pass

    # -- drawing --
    def _put(self, y, x, text, attr=0):
        h, w = self.scr.getmaxyx()
        if 0 <= y < h and x < w - 1:
            try:
                self.scr.addnstr(y, x, redact(text), w - 1 - x, attr)
            except self.c.error:
                pass

    def _frame(self, title, keys):
        self.scr.erase()
        self._put(0, 0, title, self.head)
        self._put(1, 2, keys, self.dim)
        self._put(2, 2, f"a2a {VERSION} from {URL}", self.dim)

    def _key(self, typing=False):
        """The next key as a name. `typing`: letters stay letters (an input
        field), so j, k, q and space are not navigation there."""
        try:
            if hasattr(self.scr, "get_wch"):
                k = self.scr.get_wch()
            else:                   # a curses built without wide chars
                k = self.scr.getch()
                if 0 <= k < 256:
                    k = chr(k)
        except self.c.error:
            return None
        except KeyboardInterrupt:
            raise Cancelled()
        names = {self.c.KEY_UP: "up", self.c.KEY_DOWN: "down",
                 self.c.KEY_HOME: "top", self.c.KEY_END: "bottom",
                 self.c.KEY_PPAGE: "pgup", self.c.KEY_NPAGE: "pgdn",
                 self.c.KEY_ENTER: "enter", self.c.KEY_BACKSPACE: "backspace"}
        if isinstance(k, int):
            return names.get(k)
        return key_name(k, typing)

    # -- screens --
    def say(self, *lines):
        pass                       # screens redraw; nothing scrolls here

    def status(self, text):
        self._frame("a2a installer", "")
        self._put(4, 2, text)
        self.scr.refresh()

    def notice(self, title, lines):
        self._frame(title, "any key to continue")
        for i, line in enumerate(lines):
            self._put(4 + i, 2, line)
        self.scr.refresh()
        self._key()

    def checklist(self, title, rows):
        keys = (f"{G.updown}/j/k navigate  SPACE toggle  ENTER confirm  "
                "ESC cancel")
        cursor = next((i for i, r in enumerate(rows) if r["enabled"]), 0)
        top = 0
        while True:
            h, _ = self.scr.getmaxyx()
            room = max(1, h - 5)
            top = min(max(top, cursor - room + 1), cursor)
            self._frame(title, keys)
            for i, r in enumerate(rows[top:top + room]):
                n = top + i
                box = G.check if r["checked"] else " "
                mark = G.arrow if n == cursor else " "
                attr = (self.dim if not r["enabled"]
                        else self.ok if r["checked"] else 0)
                if n == cursor:
                    attr |= self.c.A_BOLD
                self._put(4 + i, 1, f"{mark} [{box}] {r['label']}", attr)
            self.scr.refresh()
            rows, cursor, done = checklist_key(rows, cursor, self._key())
            if done == "cancel":
                raise Cancelled()
            if done == "ok":
                return [i for i, r in enumerate(rows)
                        if r["checked"] and r["enabled"]]

    def ask(self, title, prompt, default="", secret=False, error=""):
        value = default
        try:
            self.c.curs_set(1)
        except self.c.error:
            pass
        try:
            while True:
                self._frame(title, "ENTER confirm  ESC cancel")
                self._put(4, 2, prompt)
                shown = G.dot * len(value) if secret else value
                self._put(6, 2, f"> {shown}")
                if error:
                    self._put(8, 2, error, self.head)
                try:
                    self.scr.move(6, min(4 + len(shown),
                                         self.scr.getmaxyx()[1] - 2))
                except self.c.error:
                    pass
                self.scr.refresh()
                k = self._key(typing=True)
                if k == "enter":
                    return value.strip()
                if k == "esc":
                    raise Cancelled()
                if k == "backspace":
                    value = value[:-1]
                elif k == "clear":
                    value = ""
                elif isinstance(k, str) and len(k) == 1 and k.isprintable():
                    value += k
        finally:
            try:
                self.c.curs_set(0)
            except self.c.error:
                pass

    def confirm(self, title, lines, prompt="Install?"):
        top = 0
        while True:
            h, _ = self.scr.getmaxyx()
            room = max(1, h - 5)
            self._frame(title, f"ENTER {prompt.rstrip('?').lower()}  "
                               f"ESC cancel  {G.updown} scroll")
            for i, line in enumerate(lines[top:top + room]):
                self._put(4 + i, 2, line)
            self.scr.refresh()
            k = self._key()
            if k == "enter":
                return True
            if k == "esc":
                return False
            if k in ("down", "pgdn"):
                top = min(max(0, len(lines) - room),
                          top + (1 if k == "down" else room))
            elif k in ("up", "pgup"):
                top = max(0, top - (1 if k == "up" else room))


# ---------------------------------------------------------------------------
# The flow
# ---------------------------------------------------------------------------

class Target:
    def __init__(self, key, label, dest, before, profile="", agent=""):
        self.key, self.label, self.dest = key, label, dest
        self.before, self.profile, self.agent = before, profile, agent
        self.files = []          # [(absolute path, bytes, mode)]
        self.same = False        # every file already on disk, byte for byte

    def compare(self):
        """Is what was downloaded exactly what is installed? This, not the
        version, decides "already current": the version stays the same
        across changes to a client."""
        try:
            self.same = bool(self.files) and all(
                p.is_file() and p.read_bytes() == data
                for p, data, _ in self.files)
        except OSError:
            self.same = False


def ask_token(ui):
    error = ""
    for _ in range(3):
        token = ui.ask("Your a2a token",
                       "Paste or type the token your operator gave you "
                       "(a2a_st_...)", secret=True, error=error)
        if not token:
            error = "the token is empty"
            continue
        SECRET["token"] = token
        ui.status("checking the token with the broker...")
        try:
            me = check_token(token)
        except Exception as e:
            raise SystemExit(redact(f"a2a: could not reach {URL}: {e}"))
        if me is not None:
            return token, me
        error = "the broker refused that token (unknown or revoked)"
    raise SystemExit("a2a: the token was refused three times; nothing was "
                     "written. Ask your operator for a valid token.")


def ask_hermes(ui):
    """[(profile name, profile home, agent id)] for the bots picked."""
    profiles = hermes_profiles()
    if not profiles:
        ui.notice("Hermes", [
            "No Hermes bot was found: a bot is ~/.hermes (named default) or",
            "~/.hermes/profiles/<name>, holding config.yaml or similar.",
            "Hermes is skipped."])
        return []
    rows = []
    for name, home in profiles:
        v = installed_version("hermes", home)
        rows.append({"label": f"{name} {G.dash} {_state(v, True)}",
                     "enabled": True, "checked": v is not None})
    picked = ui.checklist("Which Hermes bots?", rows)
    out = []
    for i in picked:
        name, home = profiles[i]
        # An id baked by an earlier install is kept: an update must not
        # quietly turn the bot into somebody else.
        prior = _baked_field(client_file("hermes", home), "agent") or ""
        agent = prior
        if name in GENERIC_PROFILES:
            error = ""
            while True:
                agent = ui.ask(
                    f"Agent id for the Hermes bot '{name}'",
                    f"'{name}' is no agent id. Which id should this bot use?",
                    default=prior, error=error)
                if AGENT_ID.match(agent or "") and agent not in GENERIC_PROFILES:
                    break
                error = "an id is one word: no spaces, no '/', not 'default'"
        out.append((name, home, agent))
    return out


def plan(selected, bots):
    targets = []
    for key in selected:
        if key == "hermes":
            for name, home, agent in bots:
                targets.append(Target(
                    "hermes", f"Hermes bot '{name}'",
                    install_dir("hermes", home),
                    installed_version("hermes", home), name, agent))
        else:
            # A starting id baked by an earlier install (?agent=) is kept.
            prior = (_baked_field(client_file(key), "agent") or ""
                     if key != "claude" else "")
            targets.append(Target(key, NAMES[key], install_dir(key),
                                  installed_version(key), agent=prior))
    return targets


def download(ui, targets, token):
    for t in targets:
        ui.status(f"downloading the {t.label} client...")
        try:
            data = fetch(t.key, token, t.agent)
        except urllib.error.HTTPError as e:
            raise SystemExit(redact(
                f"a2a: the broker would not serve the {t.label} client "
                f"(HTTP {e.code}); nothing was written."))
        except Exception as e:
            raise SystemExit(redact(f"a2a: could not download the {t.label} "
                                    f"client: {e}; nothing was written."))
        if t.key == "opencode":
            t.files = [(t.dest / "a2a-opencode.js", data, 0o644)]
        else:
            try:
                t.files = [(t.dest / rel, body, mode)
                           for rel, body, mode in unpack(data)]
            except Exception as e:
                raise SystemExit(redact(
                    f"a2a: the {t.label} download is not a usable archive "
                    f"({e}); nothing was written."))
        t.compare()


def _hermes_config(home):
    """(a2a plugin enabled, a2a may start turns, a2a switched on by "enable
    a2a here") from a bot's config.yaml.

    Read conservatively, without a YAML library: only the plain block layout
    `hermes config set` and `hermes plugins enable` write is recognised, and
    anything else counts as NOT done — the cost of a wrong "not done" is one
    harmless command offered again, the cost of a wrong "done" is a bot that
    silently never loads a2a.
    """
    try:
        text = (home / "config.yaml").read_text(errors="replace")
    except OSError:
        return False, False, False
    path, enabled, disabled, allowed, on = [], False, False, False, False
    for raw in text.splitlines():
        if not raw.strip() or raw.lstrip().startswith("#"):
            continue
        line = raw.split(" #", 1)[0].rstrip()
        indent = len(line) - len(line.lstrip())
        s = line.strip()
        if s.startswith("- "):
            while path and path[-1][0] > indent:
                path.pop()
            item = s[2:].strip().strip("'\"")
            keys = [k for _, k in path]
            if item == "a2a" and keys == ["plugins", "enabled"]:
                enabled = True
            if item == "a2a" and keys == ["plugins", "disabled"]:
                disabled = True
            continue
        key, sep, value = s.partition(":")
        if not sep:
            continue
        while path and path[-1][0] >= indent:
            path.pop()
        keys = [k for _, k in path] + [key.strip().strip("'\"")]
        value = value.strip()
        if value.startswith("[") and value.endswith("]"):
            items = [v.strip().strip("'\"") for v in value[1:-1].split(",")]
            if keys == ["plugins", "enabled"] and "a2a" in items:
                enabled = True
            if keys == ["plugins", "disabled"] and "a2a" in items:
                disabled = True
        elif keys == ["plugins", "entries", "a2a", "allow_gateway_injection"]:
            allowed = value.lower() in ("true", "yes", "on")
        elif keys == ["plugins", "entries", "a2a", "settings", "enabled"]:
            # The plugin's own switch ("enable a2a here" writes it).
            on = value.lower() in ("true", "yes", "on")
        if not value:
            path.append((indent, keys[-1]))
    return enabled and not disabled, allowed, on


def setup_needed(targets):
    """The harness commands still needed: [(whose, what it does, argv)].

    Only what is not done yet. An update of a Codex that is registered, or of
    a bot whose a2a is already enabled and allowed, asks for nothing. No
    gateway restart is run: on a Hermes that serves every bot from one host
    gateway, `hermes -p <bot> gateway restart` is refused outright, and the
    desktop app restarts its own. Quitting and reopening the app is a step
    for the person, in the next steps.
    """
    cmds = []
    for t in targets:
        if t.key == "codex" and not codex_registered():
            cmds.append((t.label, "register a2a with Codex",
                         ["codex", "mcp", "add", "a2a", "--", "python3",
                          str(t.dest / "a2a-codex.py")]))
        if t.key == "hermes":
            enabled, allowed, _ = _hermes_config(t.dest.parent.parent)
            p = ["hermes", "-p", t.profile]
            if not enabled:
                cmds.append((t.label, "turn the a2a plugin on",
                             p + ["plugins", "enable", "a2a"]))
            if not allowed:
                cmds.append((t.label, "let a2a start conversations in the bot",
                             p + ["config", "set",
                                  "plugins.entries.a2a.allow_gateway_injection",
                                  "true"]))
    return cmds


def command_text(argv):
    """A command as you would type it: quoted where needed, with your home
    folder shown as ~ (left unquoted, so the shell still expands it)."""
    out = []
    h = str(HOME) + os.sep
    for a in argv:
        rest = a[len(h):] if a.startswith(h) else None
        if rest is not None and shlex.quote(rest) == rest:
            out.append("~/" + rest)
        else:
            out.append(shlex.quote(a))
    return " ".join(out)


def _change(t):
    if t.before is None:
        return f"new {G.dash} {VERSION}"
    if t.same:
        return f"{VERSION}, already current"
    if t.before == VERSION:
        return f"{VERSION}, updated"
    return f"{t.before or 'unstamped'} {G.arrow} {VERSION}"


def _where(t):
    if t.key == "opencode":
        return tilde(t.files[0][0]) if t.files else tilde(t.dest)
    n = len(t.files)
    return f"{tilde(t.dest)}/ ({n} file{'s' if n != 1 else ''})"


def _table(rows, indent="  "):
    """Rows of cells, columns padded to line up."""
    widths = [max(len(r[i]) for r in rows) for i in range(len(rows[0]) - 1)]
    return [indent + "  ".join(c.ljust(w) for c, w in zip(r, widths))
            + "  " + r[-1] for r in rows]


def summary(targets, me, cmds):
    """The confirmation screen: what will be written, and what will be asked."""
    lines = []
    if me is not None:
        stations = me.get("stations") or []
        lines.append(f"Token of {me.get('user') or '?'} {G.dash} stations: "
                     f"{', '.join(stations) if stations else 'none'}")
        if not stations:
            lines.append("  This token is in no station yet: your operator "
                         "has to grant it before any client can work.")
        lines.append("")
    lines.append("Will be installed")
    lines += _table([[t.label, _change(t), _where(t)] for t in targets])
    if cmds:
        lines += ["", "Setup it will ask to run afterwards"]
        lines += _table([[who, what] for who, what, _ in cmds])
    lines += ["", "Nothing outside these folders is touched, and nothing is "
                  "deleted."]
    return lines


def run_setup(say, cmds, use_tty):
    """Run each command quietly: its output is captured, never dumped. What
    the person sees is one line per command — done, or not done and why it
    is now in their next steps. [(whose, what, argv)] that did not work."""
    failed = []
    for who, what, argv in cmds:
        try:
            r = subprocess.run(argv, stdin=None if use_tty
                               else subprocess.DEVNULL,
                               stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                               timeout=300)
            ok, why = r.returncode == 0, ""
        except FileNotFoundError:
            ok, why = False, f"{argv[0]} is not on PATH"
        except subprocess.TimeoutExpired:
            ok, why = False, "it did not finish"
        if ok:
            say(f"  {G.check} {who}: {what}")
        else:
            say(f"  {G.cross} {who}: {what} {G.dash} did not work"
                + (f" ({why})" if why else "") + "; it is in your next steps")
            failed.append((who, what, argv))
    return failed


def next_steps(targets, pending, hermes_touched):
    """What is left for the person, in the order to do it: [[line, ...]]."""
    first = {t.key for t in targets if t.before is None}
    # Byte-identical to what was installed: there is nothing to restart, and
    # saying so would only cost a restart.
    changed = {t.key for t in targets if not t.same}
    keys = {t.key for t in targets}
    steps = []
    for who, what, argv in pending:
        steps.append([f"{who}: {what}. Run:", f"   {command_text(argv)}"])
    if "claude" in first:
        steps.append([f"Claude Code: start it with",
                      f"   {CLAUDE_LAUNCH}",
                      f"then type /plugin {G.arrow} enable a2a {G.arrow} "
                      "paste your token."])
    elif "claude" in changed:
        steps.append(["Claude Code: restart your sessions "
                      "(/reload-plugins does not pick up a new copy)."])
    off = [NAMES[k] for k in ("opencode", "pi", "codex") if k in first]
    if off:
        names = (", ".join(off[:-1]) + " and " + off[-1] if len(off) > 1
                 else off[0])
        steps.append([f"{names}: a2a stays off in each project until you turn "
                      "it on there: ask the agent in that project to "
                      "\"enable a2a here\"."])
    for key in ("opencode", "pi", "codex"):
        if key not in keys:
            continue
        if key == "codex" and "codex" in changed:
            steps.append([("Codex: start each session like this to receive "
                           "messages:" if "codex" in first else
                           "Codex: restart running sessions, starting each "
                           "like this to receive messages:"),
                          f"   {CODEX_LAUNCH}"])
        elif key in changed and key not in first:
            # OpenCode 2 watches its plugin files and reloads a changed one by
            # itself; 1.x loads them once per process, so it needs a restart.
            steps.append([f"{NAMES[key]}: restart running sessions"
                          + (" and the desktop app (OpenCode 2 reloads the "
                             "plugin by itself)" if key == "opencode"
                             else "") + "."])
    if hermes_touched:
        steps.append(["Hermes: quit the Hermes app and open it again, so it "
                      "loads a2a.",
                      "(If you run the Hermes gateway yourself instead, "
                      "restart it.)"])
    for t in targets:
        if t.key == "hermes" and not _hermes_config(t.dest.parent.parent)[2]:
            steps.append([f"{t.label}: in the Hermes app, open the "
                          "conversation with this bot that should carry a2a "
                          "and say \"enable a2a here\" (a messaging chat with "
                          "the bot works too).",
                          "Keep that conversation open: a2a messages go into "
                          "it while it is open, and wait on the broker while "
                          "it is closed."])
    if first:
        steps.append(["Your operator approves each new agent when it asks to "
                      "be registered."])
    return steps


def choose_ui():
    """(ui, uses a terminal). Curses when there is a terminal to draw on."""
    piped = sys.argv[0] in ("", "-")
    if piped and not sys.stdin.isatty():
        # The program came in on stdin, so the answers cannot: use the
        # terminal itself.
        try:
            fd = os.open("/dev/tty", os.O_RDWR)
        except OSError:
            raise SystemExit("a2a: the installer needs a terminal to ask its "
                             "questions. Run it in one: "
                             f"curl -fsSL {URL}/install | python3")
        os.dup2(fd, 0)
        if not sys.stdout.isatty():
            os.dup2(fd, 1)
        os.close(fd)
        sys.stdin = open(0, "r", closefd=False)
    if sys.stdin.isatty() and os.environ.get("TERM", "dumb") != "dumb":
        try:
            return CursesUI(), True
        except Exception:
            pass
    return PlainUI(sys.stdin), sys.stdin.isatty()


def main():
    if not URL or not VERSION:
        print("a2a: this installer was not served by a broker. Run: "
              "curl -fsSL https://<your broker>/install | python3")
        return 1
    if os.name != "posix":
        print("a2a: the installer supports macOS and Linux only.")
        return 1
    G.setup()
    ui, tty = choose_ui()
    plain = PlainUI(sys.stdin)
    try:
        try:
            rows = harness_rows()
            if not any(r["enabled"] for r in rows):
                ui.close()
                print("a2a: no supported harness was found here (Claude Code, "
                      "OpenCode, Pi, Codex, Hermes).")
                return 1
            picked = ui.checklist("Install or update a2a for which "
                                  "harnesses?", rows)
            selected = [rows[i]["key"] for i in picked]
            token, me = "", None
            if any(k != "claude" for k in selected):
                token, me = ask_token(ui)
            bots = ask_hermes(ui) if "hermes" in selected else []
            if "hermes" in selected and not bots:
                selected.remove("hermes")
            if not selected:
                ui.close()
                print("a2a: nothing left to install.")
                return 1
            targets = plan(selected, bots)
            download(ui, targets, token)
            cmds = setup_needed(targets)
            if not ui.confirm("Ready to install", summary(targets, me, cmds)):
                raise Cancelled()
        finally:
            ui.close()
    except Cancelled:
        print("a2a: cancelled; nothing was written.")
        return 1

    say = plain.say
    rows, broken = [], []
    for t in targets:
        try:
            for path, data, mode in t.files:
                write_file(path, data, mode)
            rows.append([f"{G.check} {t.label}", _change(t), _where(t)])
        except OSError as e:
            rows.append([f"{G.cross} {t.label}", "could not write",
                         str(e.strerror or e)])
            broken.append(t)
    say("", f"a2a {VERSION} {G.dash} {URL}", "", "Installed", *_table(rows))
    targets = [t for t in targets if t not in broken]
    cmds = [c for c in cmds if c[0] in {t.label for t in targets}]

    pending = []
    if cmds:
        say("", "Setup still needed")
        width = max(len(who) for who, _, _ in cmds)
        for who, what, argv in cmds:
            say(f"  {who.ljust(width)}  {what}",
                f"  {' ' * width}  {G.dash} {command_text(argv)}")
        ask = ("Run this command now?" if len(cmds) == 1
               else f"Run these {len(cmds)} commands now?")
        try:
            run = plain.yes_no(ask)
        except Cancelled:
            run = False
        if run:
            pending = run_setup(say, cmds, tty)
        else:
            say("  Not run; they are in your next steps.")
            pending = list(cmds)
    hermes_touched = any(t.key == "hermes" and not t.same
                         for t in targets) or any(
        argv[0] == "hermes" for _, _, argv in cmds)

    steps = next_steps(targets, pending, hermes_touched)
    if broken:
        steps.insert(0, ["Run the installer again for what could not be "
                         "written: " + ", ".join(t.label for t in broken)
                         + "."])
    if steps:
        say("", "Next steps")
        for i, lines in enumerate(steps, 1):
            say(f"  {i}. {lines[0]}", *[f"     {l}" for l in lines[1:]])
    else:
        say("", "Nothing else to do: everything you picked was already "
                "current.")
    say("", "To update later, run the same command again.")
    return 1 if broken else 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\na2a: interrupted.")
        sys.exit(130)
