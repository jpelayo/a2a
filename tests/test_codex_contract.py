#!/usr/bin/env python3
"""Codex's client and its contract with Codex — checked against its source.

    CODEX_SRC=/path/to/openai/codex python3 tests/test_codex_contract.py

test_codex_client.py runs the client against a mock app-server, and a mock
can only ever agree with itself: if Codex renamed `--managed-daemon`, moved
its control socket, made `turn/start` subscribe its caller, or stopped
naming the calling thread in a tool call's _meta, the mock would keep the old
behaviour and the suite would stay green while a plain `codex` stopped
receiving — or started receiving with nobody there. This reads a checkout
instead — as text, nothing built — and pins each thing the client and the
installer rely on, from both ends: Codex still does it, and our side still
uses it.

The facts come from a read of openai/codex `rust-v0.172.0` on 2026-11-25. A
FAIL means Codex moved or changed something: find where it went in the
checkout before touching the client.

SKIPs (exit 2) when CODEX_SRC does not point at a checkout: nothing has been
checked, and saying so is the only honest outcome.
"""
import os
import re
import sys
from pathlib import Path

HERE = Path(__file__).resolve().parent
CLIENT = HERE.parent / "deploy" / "plugin" / "codex" / "a2a-codex.py"
INSTALLER = HERE.parent / "deploy" / "plugin" / "installer" / "a2a-install.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def main() -> int:
    raw = os.environ.get("CODEX_SRC", "")
    src = Path(raw) if raw else None
    rs = src / "codex-rs" if src else None
    if not rs or not (rs / "app-server").is_dir():
        print("SKIP: set CODEX_SRC to an openai/codex checkout",
              file=sys.stderr)
        return 2
    client = CLIENT.read_text()
    installer = INSTALLER.read_text()

    def read(rel: str) -> str:
        try:
            return (rs / rel).read_text(errors="replace")
        except OSError:
            return ""

    def feature(key: str) -> str:
        """The FeatureSpec block for `key` in features/src/lib.rs."""
        m = re.search(r'key: "' + re.escape(key) + r'",(.*?)\}',
                      read("features/src/lib.rs"), re.S)
        return m.group(1) if m else ""

    def body(text: str, start: str) -> str:
        """A Rust fn's text, from its signature to the next fn at its
        indentation (enough to ask what it calls)."""
        i = text.find(start)
        if i < 0:
            return ""
        indent = text.rfind("\n", 0, i)
        pad = text[indent + 1:i]
        j = text.find("\n" + pad + "pub", i + len(start))
        k = text.find("\n" + pad + "async fn", i + len(start))
        ends = [x for x in (j, k) if x > 0]
        return text[i:min(ends)] if ends else text[i:]

    # --- the background server ------------------------------------------------
    pid = read("app-server-daemon/src/backend/pid.rs")
    check("daemon: Codex starts it as `app-server --listen unix://`",
          '"--listen".into(), "unix://".into()' in pid
          and "--managed-daemon" in read(
              "app-server-daemon/src/backend/pid_start.rs"),
          "app-server-daemon/src/backend/pid*.rs")
    check("daemon: and the client recognises exactly that argv",
          '"--managed-daemon" in words' in client and "def daemon_parent"
          in client)
    tr = read("app-server-transport/src/transport/mod.rs")
    check("daemon: it listens on CODEX_HOME/app-server-control/"
          "app-server-control.sock, which is where the client looks",
          '"app-server-control"' in tr and '"app-server-control.sock"' in tr
          and 'CODEX_HOME / "app-server-control" / "app-server-control.sock"'
          in client)
    check("daemon: a plain `codex` attaches to it by default",
          "default_enabled: true" in feature("daemon_auto_start"),
          feature("daemon_auto_start"))
    ds = read("tui/src/daemon_startup.rs")
    check("daemon: these launches stay in-process — the list the doctor "
          "and the installer name",
          all(n in ds for n in ('Some("--no-daemon")', 'Some("--profile")',
                                "command-line configuration overrides "
                                "(-c, --enable, --disable, or --search)"))
          and "--no-daemon, -c, --enable, --disable or --profile" in client,
          "tui/src/daemon_startup.rs exclusion")
    init = read("app-server/src/request_processors/initialize_processor.rs")
    check("daemon: `codex_app_server_daemon` is a client name that does NOT "
          "rewrite the server's originator and user agent — the name the "
          "client connects with",
          re.search(r"NON_ORIGINATING_CLIENT_NAMES: &\[&str\] = &\[[^\]]*"
                    r'"codex_app_server_daemon"', init) is not None
          and 'APP_SERVER_CLIENT_NAME = "codex_app_server_daemon"' in client,
          "initialize_processor.rs")

    # --- MCP processes --------------------------------------------------------
    launcher = read("rmcp-client/src/stdio_server_launcher.rs")
    check("mcp: one process per thread, started in the thread's cwd, in its "
          "own process group",
          "command.current_dir(&cwd)" in launcher
          and "ProcessMode::NewGroup" in launcher, "stdio_server_launcher.rs")
    utils = read("rmcp-client/src/utils.rs")
    check("mcp: with a cleared environment plus a short whitelist (so a "
          "shell's A2A_AGENT or CODEX_HOME never reaches the client)",
          ".env_clear()" in read("utils/pty/src/child_command.rs")
          and "DEFAULT_ENV_VARS" in utils and '"TMPDIR"' in utils
          and '"CODEX_HOME"' not in utils, "rmcp-client/src/utils.rs")
    check("mcp: Codex initializes with 2025-06-18 …",
          ".with_protocol_version(ProtocolVersion::V_2025_06_18)"
          in read("codex-mcp/src/rmcp_client.rs"))
    check("mcp: … and shares one process between chats only under "
          "2026-07-28, which the client never claims",
          "ProtocolVersion::V_2026_07_28" in read(
              "codex-mcp/src/connection_pool.rs")
          and '"2026-07-28"' not in client.split("MCP_VERSIONS = ", 1)[1]
          .split("\n", 1)[0], "connection_pool.rs finish()")
    check("mcp: sharing is still off by default",
          "default_enabled: false" in feature("mcp_connection_sharing"),
          feature("mcp_connection_sharing"))

    # --- lazy start (0.172) ---------------------------------------------------
    check("lazy: `mcp_lazy_stdio_startup` exists, and the installer and the "
          "doctor name it",
          feature("mcp_lazy_stdio_startup") != ""
          and 'CODEX_LAZY = "mcp_lazy_stdio_startup"' in installer
          and "mcp_lazy_stdio_startup" in client)
    types = read("config/src/mcp_types.rs")
    check("lazy: only `required` exempts a stdio server from it",
          re.search(r"fn can_defer_stdio_startup.*?!self\.required", types,
                    re.S) is not None, "config/src/mcp_types.rs")
    sess = read("core/src/session/session.rs")
    check("lazy: and a required server that fails stops the session — why "
          "the client is never made `required`",
          re.search(r"install_initial_mcp_runtime\([^;]*?\)\s*\)?\s*"
                    r"\.await\?", sess, re.S) is not None
          and "required MCP servers failed to initialize" in read(
              "codex-mcp/src/connection_manager/required.rs"),
          "core/src/session/session.rs")
    main_rs = read("cli/src/main.rs")
    check("lazy: `codex features disable` and `codex features list` exist, "
          "and disable refuses an unknown feature (why the installer asks "
          "list first)",
          "Disable(FeatureSetArgs)" in main_rs and "List," in main_rs
          and "FeatureToggles::validate_feature(feature)?" in main_rs,
          "cli/src/main.rs")

    # --- which thread, and how it is delivered into ---------------------------
    meta = read("core/src/mcp_tool_call.rs")
    check("thread: every tool call carries the turn metadata …",
          "crate::X_CODEX_TURN_METADATA_HEADER.to_string()" in meta
          and re.search(r'X_CODEX_TURN_METADATA_HEADER: &str = '
                        r'"x-codex-turn-metadata"',
                        "".join(p.read_text(errors="replace") for p in
                                (rs / "core" / "src").glob("*.rs")))
          is not None, "core/src/mcp_tool_call.rs")
    check("thread: … with the calling thread as `thread_id`, which the "
          "client pins",
          'THREAD_ID_KEY: &str = "thread_id"'
          in read("core/src/responses_metadata.rs")
          and '"x-codex-turn-metadata"' in client
          and 'turn.get("thread_id")' in client)
    data = read("app-server-protocol/src/protocol/v2/thread_data.rs")
    check("thread: a thread/read record says whether it is a subagent's "
          "(parentThreadId, threadSource, source.subAgent)",
          all(n in data for n in ("pub parent_thread_id",
                                  "pub thread_source", "pub cwd",
                                  "SubAgent(CoreSubAgentSource)",
                                  'rename_all = "camelCase"'))
          and all(n in client for n in ('"parentThreadId"', '"threadSource"',
                                        '"subAgent"')), "thread_data.rs")
    common = read("app-server-protocol/src/protocol/common.rs")
    for method, variant in (("thread/loaded/list", "ThreadLoadedList"),
                            ("thread/read", "ThreadRead"),
                            ("turn/start", "TurnStart")):
        m = re.search(r"(#\[experimental[^\n]*\n\s*)?" + variant
                      + r' => "' + re.escape(method) + '"', common)
        check(f"api: {method} exists and is stable",
              m is not None and not m.group(1), "protocol/common.rs")
    tp = read("app-server/src/request_processors/turn_processor.rs")
    inner = body(tp, "async fn turn_start_inner")
    check("api: turn/start starts a turn or joins the running one, as the "
          "client has always delivered",
          ".start_or_steer_turn(" in inner, "turn_processor.rs")
    check("api: and does not subscribe the caller, so delivering never "
          "keeps a closed session's thread loaded",
          inner and "ensure_conversation_listener" not in inner,
          "turn_processor.rs turn_start_inner")

    # --- how long a thread lives --------------------------------------------
    check("life: a thread with no clients unloads after "
          "thread_unload_delay_secs, 60 by default — which, with turns "
          "extending it, is why the client checks for a TUI itself",
          "thread_unload_delay_secs.unwrap_or(/*default*/ 60)"
          in read("core/src/config/mod.rs")
          and "has_subscribers" in read(
              "app-server/src/request_processors/thread_lifecycle.rs"))
    check("life: and no API reports whether a client is attached",
          "subscriber" not in read(
              "app-server-protocol/src/protocol/v2/thread.rs")
          .split("pub enum ThreadStatus", 1)[-1].split("\n}\n", 1)[0])

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — Codex still does what the client relies on")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
