#!/usr/bin/env python3
"""DeepSeek Harness's client and its contract with DSH — checked against its source.

    DSH_SRC=/path/to/deepseek-harness python3 tests/test_dsh_contract.py

test_dsh_client.py runs the package against a fake Cordis context, and fakes
can only ever agree with themselves: if dsh renames followup, or its loader
stops unwrapping `default ?? module`, the fake keeps the old names and the
suite stays green while dsh loads nothing. This reads a checkout instead —
as text, nothing installed — and pins each thing the client relies on from
both ends: dsh still provides it, and the client still uses it.

The names come from a read of deepseek-ai/deepseek-harness `master`
(~0.2.2-alpha.3) on 2026-11-03/04. A FAIL means dsh moved or renamed
something: find where it went in the checkout before touching the client.

SKIPs (exit 2) when DSH_SRC does not point at a checkout: nothing has been
checked, and saying so is the only honest outcome.
"""
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
DSH = HERE.parent / "deploy" / "plugin" / "dsh"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def found(src: Path, needles: tuple[str, ...]) -> str:
    """The first .ts file in the checkout holding every needle, or ''."""
    for top in ("packages", "vendor", "apps"):
        for p in sorted((src / top).rglob("*.ts")):
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
    raw = os.environ.get("DSH_SRC", "")
    src = Path(raw) if raw else None
    if not src or not (src / "packages").is_dir():
        print("SKIP: set DSH_SRC to a deepseek-ai/deepseek-harness checkout",
              file=sys.stderr)
        return 2
    client = (DSH / "a2a-dsh.js").read_text()
    entry = (DSH / "index.js").read_text()

    check("the loader unwraps `default ?? module` — so the entry has no default",
          bool(found(src, ("exports.default ?? exports",))),
          "vendor/loader no longer unwraps that way")
    check("  …and index.js exports name, inject and apply, and no default",
          not re.search(r"^export default", entry, re.M)
          and "export const name" in entry and "export const inject" in entry
          and "export async function apply" in entry, "the entry changed")

    for label, needles, used in (
        ("an agent's followup starts or queues a turn",
         ("followup(message",), ".followup(message)"),
        ("an agent's inject adds context without waking it",
         ("inject(message",), ".inject(message)"),
        ("agent/created and agent/status are agent events",
         ("'agent/created'", "'agent/status'"), 'ctx.on("agent/created"'),
        ("the session controller resolves an agent, live or cold",
         ("resolveAgent",), "controller.resolveAgent(id)"),
        ("the session controller lists sessions with their cwd",
         ("SessionListRequest",), "controller.list({}"),
        ("a system-prompt section may have a function as its text",
         ("section(section: PromptSection)",), "systemPrompt?.section?.({"),
        ("a raw tool declares output {schema, render}",
         ("must declare output",), "render: (_args, value)"),
        ("a tool call carries the calling agent",
         ("readonly agent?: Agent",), "exec?.agent"),
        ("dsh's home is $DSH_HOME, else ~/.dsh",
         ("DSH_HOME",), "process.env.DSH_HOME"),
        ("a bundle with no peer ranges passes the compatibility check",
         ("Object.hasOwn(fields, 'peerDependencies')",), ""),
    ):
        where = found(src, needles)
        check(f"dsh: {label}", bool(where), f"no .ts file holds {needles}")
        if used:
            check(f"  …and the client relies on it ({used})", used in client, used)

    pkg = (DSH / "package.json").read_text()
    check("the package declares no engines.dsh and no peers, so that check "
          "passes", '"engines"' not in pkg and '"peerDependencies"' not in pkg, pkg)
    check("the client builds no other harness's folder path",
          not any(p in client for p in ("/.config/opencode", "/.config/kilo",
                                        "}/opencode", "}/kilo")),
          "another harness's path is in the dsh client")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — dsh still provides what its client relies on")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
