#!/usr/bin/env python3
"""Kilo Code's client and its contract with KILO — checked against its source.

    KILO_SRC=/path/to/kilocode python3 tests/test_kilocode_contract.py

test_kilocode_client.py runs the plugin against a fake Kilo client, and fakes
can only ever agree with themselves: if Kilo stops marking its TUI server
with KILO_PROCESS_ROLE=worker, or renames session.status, the fake keeps the
old names and the suite stays green while the TUI stops receiving. This
reads a checkout instead — as text, nothing installed — and pins each thing
the client relies on from both ends: Kilo still provides it, and the client
still uses it.

The names come from a read of Kilo-Org/kilocode `main` (~7.8.25) on
2026-11-03. A FAIL means Kilo moved or renamed something: find where it went
in the checkout before touching the client.

SKIPs (exit 2) when KILO_SRC does not point at a checkout: nothing has been
checked, and saying so is the only honest outcome.
"""
import os
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLIENT = HERE.parent / "deploy" / "plugin" / "kilocode" / "a2a-kilocode.js"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def found(src: Path, needles: tuple[str, ...], under: str = "packages") -> str:
    """The first .ts file under `under` holding every needle, or ''."""
    for p in sorted((src / under).rglob("*.ts")):
        if "node_modules" in p.parts:
            continue
        try:
            text = p.read_text(errors="replace")
        except OSError:
            continue
        if all(n in text for n in needles):
            return str(p.relative_to(src))
    return ""


def main() -> int:
    raw = os.environ.get("KILO_SRC", "")
    src = Path(raw) if raw else None
    if not src or not (src / "packages").is_dir():
        print("SKIP: set KILO_SRC to a Kilo-Org/kilocode checkout", file=sys.stderr)
        return 2
    plugin = CLIENT.read_text()

    for label, needles, used in (
        ("plugins load from {plugin,plugins}/ folders, as V1 server plugins",
         ("{plugin,plugins}",), "~/.config/kilo/plugins"),
        ("the V1 reader takes default.server and requires an id",
         ("readV1Plugin",), 'export default { id: "a2a", server }'),
        ("the TUI's server is a Worker marked KILO_PROCESS_ROLE=worker",
         ("KILO_PROCESS_ROLE", "new Worker("), 'process.env.KILO_PROCESS_ROLE === "worker"'),
        ("…and the role is set with ??=, so a child can inherit it (hence the "
         "thread check)", ("KILO_PROCESS_ROLE", "??="), "!isMainThread"),
        ("the daemon is spawned with KILOCODE_FEATURE: \"daemon\"",
         ('KILOCODE_FEATURE: "daemon"',), 'env.KILOCODE_FEATURE === "daemon"'),
        ("a plugin hook's dispose is called when the instance closes",
         ("dispose", "plugin dispose hook failed"), "dispose: () => a2a.stop()"),
        ("session status: idle sessions are deleted from the record",
         ('status.type === "idle"', "delete(sessionID)"), "client.session.status()"),
        ("a prompt into a busy session supersedes its turn (why we hold)",
         ('"superseded"',), "async function held(id)"),
        ("the system-prompt hook still exists",
         ('"experimental.chat.system.transform"',),
         '"experimental.chat.system.transform": async'),
        ("Kilo's own folders are named kilo", ('app = "kilo"',), "/.config/kilo"),
    ):
        where = found(src, needles)
        check(f"Kilo: {label}", bool(where), f"none of packages/**/*.ts holds {needles}")
        check(f"  …and the client relies on it ({used})", used in plugin, used)

    # Paths are template literals here, so a path in CODE ends in a backtick
    # or a slash; the comment explaining why nothing creates the folder does
    # not.
    check("the client never builds an OpenCode folder path",
          not any(p in plugin for p in ("/.config/opencode`", "/.config/opencode/",
                                        "}/opencode`", "}/opencode/")),
          "an OpenCode path is back in the Kilo client")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — Kilo still provides what its client relies on")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
