#!/usr/bin/env python3
"""Importing the broker emits no warnings — because the TUI wears them.

    python3 tests/test_clean_import.py

The broker is one module, so `tui`, `serve` and every CLI subcommand pay the
full import cost — including anything the import prints. A warning at import
time lands on the operator's terminal ahead of the TUI looking exactly like a
crash, which is how this was reported.

The instance that prompted this: mcp's FastMCP `Settings` declares `lifespan`
with a forward reference to FastMCP itself and never rebuilds the model, so
the annotation stays unresolved forever; pydantic-settings >= 2.15 started
warning about it (IncompleteFieldDefinitionWarning) on every FastMCP()
construction. The broker now rebuilds the model itself, which resolves the
annotation for real. Two assertions, because they fail on different versions:

  the warning count  — bites only where pydantic-settings >= 2.15 is
                       installed (the container), older ones never warn
  the _complete flag — bites everywhere pydantic exposes it, so the local
                       run still guards the fix even on an older stack

Pure python3 with the broker's deps.
"""
import importlib.util
import os
import sys
import tempfile
import warnings
from pathlib import Path

import dbharness

BROKER = Path(__file__).resolve().parent.parent / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def main() -> int:
    os.environ.update(dbharness.db_env())
    os.environ["A2A_AUTH_DISABLED"] = "1"

    with warnings.catch_warnings(record=True) as caught:
        warnings.simplefilter("always")
        spec = importlib.util.spec_from_file_location("broker_clean", BROKER)
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)
        mod._startup()

    # Deprecations from our own deps are somebody else's release notes; what
    # must never appear is a warning an OPERATOR is expected to act on.
    loud = [w for w in caught
            if w.category.__name__ in ("IncompleteFieldDefinitionWarning",
                                       "UserWarning", "RuntimeWarning")]
    check("importing the broker prints nothing an operator must act on — "
          "the TUI runs on a terminal and wears every import-time warning",
          not loud,
          "; ".join(f"{w.category.__name__}: {str(w.message)[:80]}"
                    for w in loud[:3]))

    try:
        import mcp.server.fastmcp.server as fastmcp_server
        incomplete = [n for n, f in fastmcp_server.Settings.model_fields.items()
                      if not getattr(f, "_complete", True)]
        check("no FastMCP Settings field is left with an unresolved forward "
              "reference — resolved for real, not suppressed",
              not incomplete, str(incomplete))
    except ImportError:
        check("mcp internals importable for the completeness check", False,
              "module path changed; revisit the model_rebuild in a2a-mcp.py")

    check("the FastMCP instance is alive after the rebuild",
          getattr(mod, "mcp", None) is not None
          and mod.mcp.name == "a2a-mcp", str(getattr(mod, "mcp", None)))

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — the broker imports silently")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
