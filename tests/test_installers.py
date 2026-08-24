#!/usr/bin/env python3
"""The artifacts users actually download.

    python3 tests/test_installers.py

Nothing else checks these. A client is installed by one curl line, so the bytes
that line receives are the product — and the failure mode is silent: the wrong
container format extracts fine on the developer's Mac and fails on everyone
else's Linux box, because BSD tar is libarchive and GNU tar is not.

That is a bug that shipped: `curl … /a2a-claudecode.zip | tar -xf -` is a zip
piped into tar. This file exists so the format claimed by the install command
is the format served.

Needs python3 with the broker's deps and a free port.
"""
import io
import json
import os
import socket
import subprocess
import sys
import tarfile
import tempfile
import time
import urllib.error
import urllib.request
import zipfile
from pathlib import Path

import dbharness

ROOT = Path(__file__).resolve().parent.parent
BROKER = ROOT / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def get(url: str) -> tuple[int, bytes, str]:
    try:
        with urllib.request.urlopen(url, timeout=15) as r:
            return r.status, r.read(), r.headers.get("Content-Type", "")
    except urllib.error.HTTPError as e:
        return e.code, e.read(), e.headers.get("Content-Type", "")


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-inst-"))
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, **dbharness.db_env(),
               A2A_HOST="127.0.0.1", A2A_PORT=str(port))
    env.pop("A2A_AUTH_DISABLED", None)

    minted = subprocess.run(
        [sys.executable, str(BROKER), "token", "create", "--user", "installer"],
        env=env, capture_output=True, text=True, check=True).stdout
    token = next((w for w in minted.split() if w.startswith("a2a_st_")), "")
    check("a token was minted for the installer routes", bool(token), minted)

    proc = subprocess.Popen([sys.executable, str(BROKER), "serve"], env=env,
                            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    try:
        for _ in range(100):
            try:
                urllib.request.urlopen(f"{base}/healthz", timeout=2)
                break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("broker did not come up")

        # --- Claude Code: a real gzipped tar, extractable by tar ------------
        code, body, ctype = get(f"{base}/a2a-claudecode.tar.gz")
        check("claude: served", code == 200, f"HTTP {code}")
        check("claude: is application/gzip", "gzip" in ctype, ctype)
        names: list[str] = []
        try:
            with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as t:
                names = t.getnames()
        except Exception as e:
            check("claude: extracts as a gzipped tar", False, repr(e))
        for want in (".mcp.json", ".claude-plugin/plugin.json",
                     "server/a2a-channel.py", "server/identity.py"):
            check(f"claude: contains {want}", want in names, str(names[:12]))
        check("claude: members are relative, never absolute or ..",
              all(not n.startswith(("/", "..")) for n in names), str(names[:6]))

        # Byte-identical rebuilds: a cached artifact must not churn.
        check("claude: an unchanged tree rebuilds byte-identically",
              get(f"{base}/a2a-claudecode.tar.gz")[1] == body)

        # The old path still works, and is still a zip — an install line from
        # an older README must not 404.
        zcode, zbody, zctype = get(f"{base}/a2a-claudecode.zip")
        check("claude: the deprecated .zip path still serves", zcode == 200)
        check("claude: and it really is a zip",
              zipfile.is_zipfile(io.BytesIO(zbody)), zctype)

        # --- Pi: a directory, with credentials baked ------------------------
        code, body, ctype = get(f"{base}/install/pi/{token}")
        check("pi: served", code == 200, f"HTTP {code}")
        members: dict[str, bytes] = {}
        try:
            with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as t:
                for m in t.getmembers():
                    f = t.extractfile(m)
                    if f:
                        members[m.name] = f.read()
        except Exception as e:
            check("pi: extracts as a gzipped tar", False, repr(e))
        check("pi: contains index.ts", "index.ts" in members, str(list(members)))
        check("pi: contains package.json — typebox must be declared, or the "
              "extension cannot register a tool",
              "package.json" in members, str(list(members)))
        head = members.get("index.ts", b"")[:200].decode("utf8", "replace")
        check("pi: credentials are baked as a globalThis assignment, not a "
              "const that would collide with a declaration",
              head.startswith("globalThis.A2A_BAKED = {"), head[:80])
        check("pi: the baked payload carries the token used", token in head,
              head[:120])
        if "package.json" in members:
            try:
                deps = json.loads(members["package.json"]).get("dependencies", {})
            except ValueError:
                deps = {}
            check("pi: package.json declares typebox", "typebox" in deps,
                  str(deps))

        # --- what is actually running ---------------------------------------
        # The clients are served from the container's own tree, so "the
        # feature is missing" and "the container was never rebuilt" look
        # identical from outside. /healthz is the only public path, which
        # makes it the only place that question can be answered.
        hcode, hbody, _ = get(f"{base}/healthz")
        health = json.loads(hbody)
        check("healthz reports the version, so a deploy can be told apart "
              "from a stale image without guessing",
              hcode == 200 and health.get("ok") is True
              and health.get("version"), hbody.decode()[:120])
        cli_version = subprocess.run(
            [sys.executable, str(BROKER), "--version"],
            capture_output=True, text=True, timeout=30).stdout.strip()
        check("and --version agrees with it — one constant, not two",
              health.get("version", "") in cli_version, cli_version)

        # --- every client carries the version of the tree it came from ------
        # A client is a copy on a disk; the broker cannot update it. The only
        # way a stale install can say so is if it knows which tree it came
        # from, so the stamp is what the whole staleness check rests on.
        check("pi: the baked payload carries the version of the serving tree",
              '"version"' in head, head[:160])
        try:
            with tarfile.open(fileobj=io.BytesIO(
                    get(f"{base}/a2a-claudecode.tar.gz")[1]), mode="r:gz") as t:
                mcp = json.loads(t.extractfile(".mcp.json").read())
        except Exception as e:
            mcp = {}
            check("claude: .mcp.json is readable", False, repr(e))
        chan_env = ((mcp.get("mcpServers") or {}).get("a2a-channel")
                    or {}).get("env") or {}
        check("claude: .mcp.json carries A2A_CLIENT_VERSION, written at pack "
              "time like the url beside it",
              bool(chan_env.get("A2A_CLIENT_VERSION")), str(chan_env))
        check("claude: and it agrees with what healthz reports, because both "
              "are the same constant",
              chan_env.get("A2A_CLIENT_VERSION") == health.get("version"),
              f"{chan_env.get('A2A_CLIENT_VERSION')} vs "
              f"{health.get('version')}")
        check("healthz names the client-tree version a client compares "
              "against", bool(health.get("clients")), hbody.decode()[:120])

        # --- Codex: a directory whose launcher must stay executable ---------
        code, body, ctype = get(f"{base}/install/codex/{token}")
        check("codex: served", code == 200, f"HTTP {code}")
        cmembers: dict[str, bytes] = {}
        cmodes: dict[str, int] = {}
        try:
            with tarfile.open(fileobj=io.BytesIO(body), mode="r:gz") as t:
                for m in t.getmembers():
                    cmodes[m.name] = m.mode
                    f = t.extractfile(m)
                    if f:
                        cmembers[m.name] = f.read()
        except Exception as e:
            check("codex: extracts as a gzipped tar", False, repr(e))
        for needed in ("a2a-codex.py",):
            check(f"codex: contains {needed}", needed in cmembers,
                  str(list(cmembers)))
        check("codex: ships NO launcher and no script — the run procedure is "
              "two plain codex commands on one line, and the only "
              "registration is codex's own `codex mcp add`",
              not [n for n in cmembers if n.endswith(("codex-a2a", ".sh"))],
              str(list(cmembers)))
        check("codex: no bytecode rides along — a stray py_compile in the "
              "source tree must not end up in anybody's install",
              not [n for n in cmembers if "__pycache__" in n],
              str([n for n in cmembers if "__pycache__" in n]))
        cx_src = cmembers.get("a2a-codex.py", b"").decode("utf8", "replace")
        check("codex: credentials are baked by replacing the A2A_BAKED seam",
              "A2A_BAKED = {}" not in cx_src
              and f'"token": "{token}"' in cx_src, cx_src[:200])
        check("codex: the docstring survives the baking (a prepend would have "
              "demoted it to a no-op expression)",
              cx_src.lstrip().startswith(("#!", '"""')), cx_src[:60])
        # The clients ship no README of their own — there is one, at the
        # repo root — so this asserts against that, which is now the only
        # place the launch line is written down for a user.
        readme = (ROOT / "README.md").read_text()
        check("codex: the root README teaches the two bare commands that make "
              "a session reachable — `--listen unix://` and `--remote "
              "unix://` — because a plain codex has no socket to deliver into",
              "app-server --listen unix://" in readme
              and "--remote unix://" in readme, readme[:200])

        # --- OpenCode: still one file ---------------------------------------
        code, body, _ = get(f"{base}/install/{token}")
        check("opencode: served", code == 200, f"HTTP {code}")
        check("opencode: is the plugin source with credentials prepended",
              body[:24].decode("utf8", "replace").startswith("const A2A_BAKED = {"),
              body[:60].decode("utf8", "replace"))

        # --- a bad token must be indistinguishable from no installer --------
        bad_pi = get(f"{base}/install/pi/a2a_st_wrong")
        bad_cx = get(f"{base}/install/codex/a2a_st_wrong")
        bad_oc = get(f"{base}/install/a2a_st_wrong")
        check("a bad token 404s on the pi route", bad_pi[0] == 404, str(bad_pi[0]))
        # Grok Build was removed as a client. Its route must be gone too, or
        # the image keeps serving a plugin that is no longer in the tree.
        gone = get(f"{base}/install/grok/{token}")
        check("the grok route is gone, with a GOOD token — a client removed "
              "from the repo but still served is the stale-artifact failure "
              "this design exists to prevent",
              gone[0] == 404, str(gone[0]))
        check("a bad token 404s on the codex route", bad_cx[0] == 404,
              str(bad_cx[0]))
        check("a bad token 404s on the opencode route", bad_oc[0] == 404,
              str(bad_oc[0]))
        check("all refusals are byte-identical, so none can be used to "
              "probe which tokens exist",
              bad_pi[1] == bad_oc[1] == bad_gk[1] == bad_cx[1],
              f"{bad_pi[1]!r} vs {bad_oc[1]!r} vs {bad_gk[1]!r} "
              f"vs {bad_cx[1]!r}")
    finally:
        proc.terminate()
        proc.wait(timeout=10)

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — every install line gets the format it claims")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
