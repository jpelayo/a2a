#!/usr/bin/env python3
"""No real hostname may be committed to this repo.

    python3 tests/test_no_hardcoded_host.py

The broker's url is a property of a DEPLOYMENT, not of the software. Every
client the broker serves gets it written in as it is served, so nothing here
needs to know it — and a url that does get committed is wrong twice over:

  privacy    — the repo names somebody's personal server, and every clone
               carries it
  function   — a client that falls back to a committed url does not fail, it
               succeeds against the WRONG BROKER. That is worse than an error:
               someone else's deployment quietly points at your host, carrying
               their own token.

The second one was real. `plugin/a2a/.mcp.json` was packed verbatim into the
Claude Code archive, so the url in source WAS the url every install used.

So this asserts the absence, which is the only way to keep it absent: the
placeholder hosts (example.com, localhost) are allowed, anything that looks
like a real host is not.

Pure python3, no broker.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SKIP_DIRS = {"old", ".git", "node_modules", "__pycache__", ".venv"}
SKIP_SUFFIX = {".db", ".zip", ".legacy", ".png", ".jpg", ".gz", ".pyc"}
# Boilerplate we did not write and do not deploy from.
SKIP_NAMES = {"LICENSE", ".gitignore"}

# Hosts that carry no information about anyone's infrastructure.
ALLOWED = re.compile(
    r"^(localhost|127\.0\.0\.1|0\.0\.0\.0|\[?::1\]?|broker\.invalid|"
    r"[a-z0-9-]*\.?example\.(com|org|net)|<[^>]+>|\$\{[^}]+\}|%s|%\([a-z_]+\)s|"
    # Private ranges: a docker gateway is not a public host.
    r"10(\.\d{1,3}){3}|192\.168(\.\d{1,3}){2}|172\.(1[6-9]|2\d|3[01])(\.\d{1,3}){2}"
    r")$",
    re.I,
)
# Real-world docs the READMEs legitimately link to.
DOC_HOSTS = {
    "pi.dev", "opencode.ai", "claude.ai", "github.com", "anthropic.com",
    "docs.anthropic.com", "modelcontextprotocol.io", "www.gnu.org",
    "opensource.org", "hub.docker.com", "www.apache.org", "apache.org",
}
# Any host in a url. Ports and paths trimmed by the group itself.
URL_HOST = re.compile(r"https?://([^/\s\"'`)\]}>,;]+)", re.I)

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def files():
    for p in ROOT.rglob("*"):
        if not p.is_file() or p.suffix in SKIP_SUFFIX:
            continue
        if any(part in SKIP_DIRS for part in p.relative_to(ROOT).parts):
            continue
        if p.name in SKIP_NAMES:
            continue
        yield p


def main() -> int:
    offenders: list[str] = []
    scanned = 0
    for p in files():
        try:
            text = p.read_text(errors="ignore")
        except Exception:
            continue
        scanned += 1
        for lineno, line in enumerate(text.splitlines(), 1):
            for host in URL_HOST.findall(line):
                host = host.split(":")[0].rstrip(".,")
                if ALLOWED.match(host) or host.lower() in DOC_HOSTS:
                    continue
                # A name with no dot is not reachable from the internet — it is
                # an nginx upstream or a docker service, which is exactly what
                # DEPLOY.md should be using in its examples.
                if "." not in host:
                    continue
                offenders.append(
                    f"{p.relative_to(ROOT)}:{lineno}  {host}  |  {line.strip()[:80]}")

    check(f"no committed hostname anywhere in the repo ({scanned} files "
          f"scanned) — the url belongs to the deployment, and a client that "
          f"falls back to a committed one succeeds against the wrong broker",
          not offenders, "\n      " + "\n      ".join(offenders[:12]))

    # --- the clients must refuse rather than guess -------------------------
    oc = (ROOT / "plugin" / "opencode" / "a2a-opencode.js").read_text()
    pi = (ROOT / "plugin" / "pi" / "index.ts").read_text()
    cc = (ROOT / "plugin" / "a2a" / "server" / "a2a-channel.py").read_text()

    check("OpenCode has no fallback url",
          'process.env.A2A_URL || ""' in oc, "a default crept back in")
    check("Pi has no fallback url",
          'process.env.A2A_URL || ""' in pi, "a default crept back in")
    check("the Claude channel has no fallback url",
          'os.environ.get("A2A_URL", "")' in cc, "a default crept back in")
    check("OpenCode refuses to start without one, rather than failing every "
          "request against an empty string",
          "if (!URL_BASE || !TOKEN)" in oc, "no guard")
    check("Pi refuses too", "if (!URL_BASE || !TOKEN)" in pi, "no guard")

    # --- and the broker fills it in ----------------------------------------
    broker = (ROOT / "a2a_mcp" / "a2a-mcp.py").read_text()
    check("the broker rewrites .mcp.json as it packs the Claude Code archive "
          "— otherwise the committed placeholder IS what every install uses",
          'rewrite={".mcp.json"' in broker, "archive still packed verbatim")
    check("and it derives the url from the deployment, not from source",
          broker.count('PUBLIC_URL or f"{request.url.scheme}') >= 3,
          "an installer route is missing the fallback")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — the url belongs to the deployment, not to the repo")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
