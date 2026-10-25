#!/usr/bin/env python3
"""Every client derives ONE client key per harness-directory, race-free.

    python3 tests/test_client_key.py

The broker (0.5.0) pins each agent to the first X-A2A-Client key that acts as
it, so a key that changed between two processes of one client would lock that
client out of its own agent. Claude Code is the sharp case: its headersHelper
and its channel are two processes that start together, read nothing in
common but one secret file, and must present the same key. So this starts
the real artifacts concurrently against an EMPTY store and asserts:

  one key      every concurrent first use derives the same key, and exactly
               one secret file exists afterwards — mode 0600, no temp left
  per scope    another directory is another key; an explicit A2A_AGENT is a
               scope of its own
  one formula  all five clients turn the same secret and scope into the same
               key, so the derivation cannot drift between them unnoticed

No broker, no database. Every secret lives in this run's temp dirs, never in
a real home.
"""
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
from pathlib import Path

HERE = Path(__file__).resolve().parent
PLUGIN = HERE.parent / "plugin"
IDENTITY = PLUGIN / "a2a" / "server" / "identity.py"
CODEX = PLUGIN / "codex" / "a2a-codex.py"
HERMES = PLUGIN / "hermes" / "__init__.py"
PI = PLUGIN / "pi" / "index.ts"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def clean_env(**extra: str) -> dict:
    env = {k: v for k, v in os.environ.items() if not k.startswith("A2A_")}
    env.update(extra)
    return env


def concurrently(cmds: list[list[str]], **kw) -> list[str]:
    """Start every command at once, then collect stdout — a real race."""
    procs = [subprocess.Popen(c, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                              text=True, **kw) for c in cmds]
    return [p.communicate(timeout=60)[0].strip() for p in procs]


def secret_state(root: Path, name: str) -> tuple[bool, str]:
    files = sorted(p.name for p in root.iterdir())
    secret = root / name
    ok = (secret.exists() and (secret.stat().st_mode & 0o777) == 0o600
          and [f for f in files if "secret" in f] == [name])
    return ok, str(files)


def load(path: Path, label: str):
    spec = importlib.util.spec_from_file_location(label, path)
    mod = importlib.util.module_from_spec(spec)
    sys.modules[label] = mod
    sys.path.insert(0, str(path.parent))
    spec.loader.exec_module(mod)
    return mod


def main() -> int:
    if not shutil.which("node"):
        print("SKIP: node is not installed", file=sys.stderr)
        return 2
    tmp = Path(tempfile.mkdtemp(prefix="a2a-key-"))
    project = "/Users/x/acme-api"

    # --- Claude Code: headersHelper processes racing on an empty store -------
    store = tmp / "claude-store"
    out = concurrently([[sys.executable, str(IDENTITY), "header", "--project",
                         project, "--store", str(store)]] * 8, env=clean_env())
    keys = [json.loads(o or "{}").get("X-A2A-Client") for o in out]
    check("claude: 8 handshakes racing on an empty store present ONE key",
          len(set(keys)) == 1 and str(keys[0]).startswith("a2a_ck_"), str(keys))
    ok, files = secret_state(store, "client.secret")
    check("claude: exactly one secret, mode 0600, no temp file left behind",
          ok, files)
    check("claude: and the handshake wrote no identity.json",
          not (store / "identity.json").exists(), files)
    claude_key = keys[0]

    other = json.loads(subprocess.run(
        [sys.executable, str(IDENTITY), "header", "--project", "/Users/x/ledger",
         "--store", str(store)], capture_output=True, text=True,
        env=clean_env()).stdout)["X-A2A-Client"]
    check("claude: another directory is another client — another key",
          other != claude_key, f"{other} == {claude_key}")
    explicit = [json.loads(subprocess.run(
        [sys.executable, str(IDENTITY), "header", "--project", p,
         "--store", str(store)], capture_output=True, text=True,
        env=clean_env(A2A_AGENT="worker-2")).stdout)["X-A2A-Client"]
        for p in (project, "/Users/x/ledger")]
    check("claude: an explicit A2A_AGENT is a scope of its own — a second "
          "agent in the same directory is a second client…",
          explicit[0] not in (claude_key, other), str(explicit))
    check("…and that agent keeps its key in any directory, since it was "
          "chosen for the process, not derived from the path",
          explicit[0] == explicit[1], str(explicit))

    # --- one formula: the other four, from the SAME secret and scope ---------
    os.environ.setdefault("A2A_URL", "http://127.0.0.1:1")
    os.environ.setdefault("A2A_TOKEN", "a2a_st_test")
    identity = load(IDENTITY, "key_identity")
    secret_hex = (store / "client.secret").read_text()

    # Hermes: its secret lives in the profile home, under its own name.
    hermes_home = tmp / "hermes-home"
    hermes_home.mkdir()
    (hermes_home / "a2a-client.secret").write_text(secret_hex)
    hermes = load(HERMES, "key_hermes")
    check("hermes: the same secret and scope give the same key as Claude's",
          hermes.derive_client_key(hermes_home, f"dir:{project}")
          == claude_key, "the formulas drifted")
    race_home = tmp / "hermes-race"
    out = concurrently([[sys.executable, "-c",
                         "import importlib.util,sys;"
                         f"s=importlib.util.spec_from_file_location('h',{str(HERMES)!r});"
                         "m=importlib.util.module_from_spec(s);s.loader.exec_module(m);"
                         f"print(m.derive_client_key({str(race_home)!r},'profile:bot'))"]]
                       * 8, env=clean_env(A2A_URL="http://127.0.0.1:1",
                                          A2A_TOKEN="t"))
    check("hermes: 8 processes of one bot racing on an empty home derive ONE "
          "key", len(set(out)) == 1 and out[0].startswith("a2a_ck_"), str(out))
    ok, files = secret_state(race_home, "a2a-client.secret")
    check("hermes: exactly one secret, mode 0600, no temp file left behind",
          ok, files)

    # Codex: secret under CODEX_HOME, scope from the process's cwd.
    codex_home = tmp / "codex-home"
    codex_home.mkdir()
    (codex_home / "a2a-client.secret").write_text(secret_hex)
    os.environ["CODEX_HOME"] = str(codex_home)
    os.environ["A2A_CODEX_SOCK"] = ""
    codex = load(CODEX, "key_codex")
    check("codex: the same secret and scope give the same key as Claude's",
          codex.client_key() == identity.client_key(os.getcwd(), str(store)),
          "the formulas drifted")

    # Pi: its derivation, lifted verbatim — Pi itself cannot be imported here.
    src = PI.read_text()
    i = src.index("async function deriveClientKey")
    body = src[i:src.index("\n}\n", i) + 3]
    pi_secret = tmp / "pi" / "a2a-client.secret"
    pi_secret.parent.mkdir()
    pi_secret.write_text(secret_hex)
    harness = tmp / "pi-key.mts"
    harness.write_text(
        'import { link, mkdir, readFile, unlink, writeFile } from "node:fs/promises";\n'
        'import { dirname } from "node:path";\n'
        + body +
        "\nconst [path, scope] = process.argv.slice(2);\n"
        "console.log(await deriveClientKey(path, scope));\n")
    node = ["node", "--experimental-strip-types", "--no-warnings", str(harness)]
    got = subprocess.run(node + [str(pi_secret), f"dir:{project}"],
                         capture_output=True, text=True, timeout=60)
    check("pi: the same secret and scope give the same key as Claude's",
          got.stdout.strip() == claude_key, got.stdout + got.stderr[:300])
    pi_race = tmp / "pi-race" / "a2a-client.secret"
    out = concurrently([node + [str(pi_race), "dir:/p"]] * 6)
    check("pi: 6 processes racing on an empty dir derive ONE key",
          len(set(out)) == 1 and out[0].startswith("a2a_ck_"), str(out))
    ok, files = secret_state(pi_race.parent, "a2a-client.secret")
    check("pi: exactly one secret, mode 0600, no temp file left behind",
          ok, files)

    # OpenCode: the real plugin, its key as it sends it on the first call.
    def opencode_key(data: Path, directory: str) -> list[list[str]]:
        script = tmp / f"oc-{data.name}.mjs"
        script.write_text(f"""
process.env.A2A_URL = "http://broker.invalid"
process.env.A2A_TOKEN = "t"
process.env.A2A_HELLO = "0"
process.env.HOME = {json.dumps(str(tmp / "oc-home"))}
process.env.XDG_DATA_HOME = {json.dumps(str(data))}
delete process.env.A2A_AGENT
let seen = ""
globalThis.fetch = async (url, init = {{}}) => {{
  seen = seen || init.headers?.["X-A2A-Client"] || ""
  return {{ ok: true, status: 200, text: async () => "{{}}", json: async () => ({{}}) }}
}}
const mod = await import({json.dumps(str(OPENCODE))})
const s = await mod.default.server({{ client: {{ app: {{ log: async () => {{}} }} }},
                                      directory: {json.dumps(directory)} }})
await s.tool.whoami.execute({{}})
console.log(seen)
process.exit(0)
""")
        return [["node", str(script)]]

    oc_data = tmp / "oc-data"
    (oc_data / "opencode").mkdir(parents=True)
    (oc_data / "opencode" / "a2a-client.secret").write_text(secret_hex)
    got = concurrently(opencode_key(oc_data, project))
    check("opencode: the same secret and scope give the same key as Claude's",
          got[0] == claude_key, str(got))
    race = tmp / "oc-race"
    out = concurrently(opencode_key(race, "/p") * 6)
    check("opencode: 6 processes racing on an empty data dir send ONE key",
          len(set(out)) == 1 and out[0].startswith("a2a_ck_"), str(out))
    ok, files = secret_state(race / "opencode", "a2a-client.secret")
    check("opencode: exactly one secret, mode 0600, no temp file left behind",
          ok, files)

    shutil.rmtree(tmp, ignore_errors=True)
    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else
          "PASS — one harness-directory, one key, whoever gets there first")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
