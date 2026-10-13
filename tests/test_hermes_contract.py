#!/usr/bin/env python3
"""The Hermes plugin's contract with HERMES — checked against Hermes's source.

    HERMES_SRC=/path/to/hermes-agent python3 tests/test_hermes_contract.py

test_hermes_client.py runs the plugin against a fake PluginContext, which can
only ever agree with itself: if Hermes renames a method, the fake keeps the
old name and the suite stays green while every bot breaks. This file reads a
Hermes checkout instead — by AST, so no Hermes dependency needs installing —
and pins every name and signature the plugin relies on. Run it against the
release named in plugin.yaml's `requires_hermes` (git tag v0.21.7) and
against main; if the release fails where main passes, `requires_hermes` must
rise.

The plugin was written against main ee36aed (2026-10-12), latest release
v0.21.7.

SKIPs (exit 2) when HERMES_SRC does not point at a checkout: nothing has been
checked, and saying so is the only honest outcome.
"""
import ast
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLIENT = HERE.parent / "plugin" / "hermes" / "__init__.py"
MANIFEST = HERE.parent / "plugin" / "hermes" / "plugin.yaml"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def tree(src: Path, rel: str):
    p = src / rel
    return ast.parse(p.read_text()) if p.is_file() else None


def find(node, kind, name):
    for n in ast.walk(node) if node else ():
        if isinstance(n, kind) and getattr(n, "name", None) == name:
            return n
    return None


def params(fn) -> list[str]:
    a = fn.args
    return ([x.arg for x in a.posonlyargs + a.args + a.kwonlyargs]
            + ([a.vararg.arg] if a.vararg else [])
            + ([a.kwarg.arg] if a.kwarg else []))


def is_property(fn) -> bool:
    return any(getattr(d, "id", getattr(d, "attr", "")) in
               ("property", "cached_property") for d in fn.decorator_list)


def main() -> int:
    src = Path(os.environ.get("HERMES_SRC") or "")
    if not str(src) or not (src / "hermes_cli" / "plugins.py").is_file():
        print("SKIP: set HERMES_SRC to a hermes-agent checkout "
              "(hermes_cli/plugins.py not found)")
        return 2

    plugins = tree(src, "hermes_cli/plugins.py")
    ctx = find(plugins, ast.ClassDef, "PluginContext")
    mgr = find(plugins, ast.ClassDef, "PluginManager")
    check("PluginContext and PluginManager exist", ctx and mgr, "")

    def method(cls, name):
        return find(cls, (ast.FunctionDef, ast.AsyncFunctionDef), name)

    # What the plugin calls, with the arguments it passes.
    wants = {
        "inject_message": {"content", "role", "origin"},
        "register_tool": {"name", "toolset", "schema", "handler",
                          "description"},
        "register_hook": {"hook_name", "callback"},
        "on_unload": {"callback"},
        "get_config": {"key", "default"},
        "set_config": {"key", "value"},
        "_gateway_injection_allowed": set(),
    }
    for name, args in wants.items():
        fn = method(ctx, name)
        got = set(params(fn)) if fn else set()
        check(f"PluginContext.{name} takes {sorted(args) or 'no arguments'}",
              fn is not None and args <= got, f"has {sorted(got)}")
    inj = method(ctx, "inject_message")
    check("inject_message takes origin as KEYWORD-only, as the plugin passes "
          "it", inj is not None
          and "origin" in [a.arg for a in inj.args.kwonlyargs], "")
    prof = method(ctx, "profile_name")
    check("PluginContext.profile_name is a property",
          prof is not None and is_property(prof), "")
    gw = method(mgr, "has_gateway_message_injector")
    check("PluginManager.has_gateway_message_injector is a property — the "
          "plugin streams only in a process where it is true",
          gw is not None and is_property(gw), "")
    text = (src / "hermes_cli" / "plugins.py").read_text()
    check("the context reaches its manager as ctx._manager",
          "self._manager = manager" in text, "")
    check("pre_llm_call is a valid hook", re.search(
        r'VALID_HOOKS[^=]*=\s*\{[^}]*"pre_llm_call"', text, re.S) is not None,
        "")

    # The hook payload the plugin reads.
    calls = []
    for py in src.rglob("*.py"):
        if "test" in py.parts[-1] or "tests" in py.parts:
            continue
        try:
            t = py.read_text()
        except Exception:
            continue
        if "pre_llm_call" in t:
            calls += re.findall(r'invoke_hook\(\s*"pre_llm_call"(.*?)\)\s*$',
                                t, re.S | re.M)
    payload = " ".join(calls)
    for kw in ("session_id", "user_message", "conversation_history",
               "is_first_turn"):
        check(f"pre_llm_call is invoked with {kw}=", f"{kw}=" in payload,
              payload[:200])

    sc = tree(src, "gateway/session_context.py")
    sc_text = ((src / "gateway" / "session_context.py").read_text()
               if sc else "")
    for name in ("get_session_env", "session_is_messaging_surface"):
        check(f"gateway.session_context.{name} exists",
              find(sc, ast.FunctionDef, name) is not None, "")
    client = CLIENT.read_text()
    for var in sorted(set(re.findall(r'"(HERMES_SESSION_[A-Z_]+)"', client))):
        check(f"the gateway binds {var}", f'"{var}"' in sc_text, "")
    hc = tree(src, "hermes_constants.py")
    check("hermes_constants.get_hermes_home exists",
          find(hc, ast.FunctionDef, "get_hermes_home") is not None, "")

    # plugin.yaml: every key the plugin's manifest uses is one Hermes reads.
    manifest_src = ""
    for rel in ("hermes_cli/plugins.py", "hermes_cli/plugins_manifest.py",
                "hermes_cli/plugins_loader.py",
                "hermes_cli/plugins_discovery.py"):
        p = src / rel
        if p.is_file():
            manifest_src += p.read_text()
    keys = re.findall(r"^([a-z_]+):", MANIFEST.read_text(), re.M)
    for key in keys:
        check(f"plugin.yaml key {key!r} is read by Hermes",
              re.search(rf"['\"]{key}['\"]|\b{key}\b\s*:", manifest_src)
              is not None, "")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — every Hermes name the plugin uses exists in this checkout")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
