#!/usr/bin/env python3
"""Live end-to-end delivery test — both harnesses, against a real broker.

    python3 tests/test_end_to_end.py

Starts an actual broker on a temp DB and loopback port, registers two agents,
and drives BOTH client plugins against it:

  Claude Code — server/a2a-channel.py over stdio JSON-RPC. Asserts it emits a
                notifications/claude/channel carrying the posted text.
  OpenCode    — a2a-opencode.js imported with a stub `client`. Asserts it calls
                client.session.prompt with the <channel ...> envelope.

The unit tests prove the two agree on an agent id. This proves each one
actually receives a message a third agent posted, which is the only claim that
matters. Nothing here touches the production broker or a real token: auth is
disabled, so every request lands in the `default` station of a throwaway DB.

Needs python3 (with mcp/uvicorn/starlette), node 18+, and a free port.
"""
import json
import os
import shutil
import socket
import subprocess
import sys
import tempfile
import time
import urllib.error
import urllib.request
from pathlib import Path

import dbharness
import pymysql

HERE = Path(__file__).resolve().parent
# The clients live in plugin/; the suite lives here, beside it.
PLUGIN = HERE.parent / "plugin"
BROKER = HERE.parent / "a2a_mcp" / "a2a-mcp.py"
CHANNEL = PLUGIN / "a2a" / "server" / "a2a-channel.py"
OPENCODE = PLUGIN / "opencode" / "a2a-opencode.js"

CLAUDE_AGENT = "acme-api-claudecode-1"
OPENCODE_AGENT = "acme-api-oc"
POSTER = "someone-else"
CHAN = "ops"

fails: list[str] = []
notes: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def api(base: str, path: str, body: dict | None = None, method: str = "GET"):
    req = urllib.request.Request(
        f"{base}{path}",
        data=json.dumps(body).encode() if body is not None else None,
        headers={"Content-Type": "application/json", "X-A2A-Agent": POSTER},
        method=method,
    )
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


def main() -> int:
    tmp = Path(tempfile.mkdtemp(prefix="a2a-e2e-"))
    db = tmp / "e2e.db"
    port = free_port()
    base = f"http://127.0.0.1:{port}"
    env = dict(os.environ, **dbharness.db_env(), A2A_AUTH_DISABLED="1",
               A2A_HOST="127.0.0.1", A2A_PORT=str(port))
    proc = None
    try:
        # --- register the cast before the server holds the DB --------------
        for agent in (CLAUDE_AGENT, OPENCODE_AGENT, POSTER):
            subprocess.run(
                [sys.executable, str(BROKER), "agent", "add", agent,
                 "--station", "default"],
                env=env, capture_output=True, text=True, check=True,
            )

        proc = subprocess.Popen(
            [sys.executable, str(BROKER), "serve"], env=env,
            stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True,
        )
        for _ in range(100):                      # ~10s for uvicorn to bind
            try:
                if api(base, "/healthz").get("ok"):
                    break
            except Exception:
                time.sleep(0.1)
        else:
            raise RuntimeError("broker did not come up")
        check("broker up on a temp DB", True)

        # Channels are provisioned by an operator now, not over the agent API.
        subprocess.run(
            [sys.executable, str(BROKER), "channel", "create", CHAN,
             "--station", "default",
             "--members", ",".join([CLAUDE_AGENT, OPENCODE_AGENT, POSTER])],
            env=env, capture_output=True, text=True, check=True,
        )
        check("channel created with both agents as members", True)

        # ================= Claude Code =====================================
        # Only the project directory, exactly as .mcp.json passes it. Nothing
        # names the agent: if the derivation broke, the handshake below claims
        # the wrong id and the delivery checks fail.
        cenv = dict(os.environ, A2A_URL=base, A2A_TOKEN="dev",
                    A2A_AGENT_DIR=str(tmp / CLAUDE_AGENT), A2A_HELLO="1")
        ch = subprocess.Popen(
            [sys.executable, str(CHANNEL)], env=cenv, text=True,
            stdin=subprocess.PIPE, stdout=subprocess.PIPE,
            stderr=subprocess.DEVNULL, bufsize=1,
        )
        ch.stdin.write(json.dumps({
            "jsonrpc": "2.0", "id": 1, "method": "initialize",
            "params": {"protocolVersion": "2024-11-05"}}) + "\n")
        ch.stdin.write(json.dumps({
            "jsonrpc": "2.0", "method": "notifications/initialized"}) + "\n")
        ch.stdin.flush()

        init = json.loads(ch.stdout.readline())
        check("claude: initialize handshake",
              init.get("result", {}).get("serverInfo", {}).get("name") == "a2a",
              json.dumps(init)[:200])
        check("claude: claims the id derived from the project directory",
              CLAUDE_AGENT in init["result"].get("instructions", ""),
              "instructions do not name the agent")

        hello = json.loads(ch.stdout.readline())
        check("claude: pushes the online event",
              hello.get("method") == "notifications/claude/channel"
              and "channel online" in hello["params"]["content"],
              json.dumps(hello)[:200])

        api(base, f"/channels/{CHAN}/messages",
            # Signed with the SENDER's own handle — the exact shape of the
            # live message that reached nobody. Delivery must come from
            # membership; the '@' must be inert.
            {"sender": POSTER, "text": f"ping from the test, from @{POSTER}"},
            method="POST")
        line = ch.stdout.readline()
        got = json.loads(line) if line.strip() else {}
        p = got.get("params", {})
        check("claude: DELIVERS a message posted by another agent",
              got.get("method") == "notifications/claude/channel"
              and "ping from the test" in p.get("content", ""),
              json.dumps(got)[:250])
        check("claude: carries the ack id",
              bool(p.get("meta", {}).get("id")), json.dumps(p)[:200])
        check("claude: names the sender",
              p.get("meta", {}).get("sender") == POSTER, json.dumps(p)[:200])
        ch.stdin.close()
        ch.wait(timeout=10)

        # Checked HERE, not at the end: the plain channel post in the OpenCode
        # section below is addressed to this agent too, and by then its client
        # has exited. An agent cannot ack what it was not running to receive,
        # and asserting otherwise would just be asserting the test's ordering.
        pc = api(base, f"/pending?agent={CLAUDE_AGENT}")
        check("claude acked what it received, with no action by the agent",
              pc.get("pending_total", 0) == 0, json.dumps(pc)[:200])

        # ================= OpenCode ========================================
        harness = tmp / "drive.mjs"
        harness.write_text(f"""
// The module reads A2A_* at import time, so the env must be set FIRST.
process.env.A2A_URL = {json.dumps(base)}
process.env.A2A_TOKEN = "dev"
process.env.A2A_AGENT = {json.dumps(OPENCODE_AGENT)}
const mod = await import({json.dumps(str(OPENCODE))})
const seen = []
const errors = []
// The stub models the REAL API, and that distinction is the whole point of
// this harness. `prompt` does not resolve until the model has finished
// replying; the old stub returned instantly, so it described a world in which
// the delivery deadlock could not happen — and the suite stayed green through
// 23 live sessions that delivered 40 messages and showed 1.
//
// `promptAsync` is documented as "start if needed and return immediately", so
// only it resolves here. Anything still calling `prompt` hangs, exactly as it
// does in production.
const client = {{
  app: {{ log: async (a) => {{ if (a?.body?.level === "error") errors.push(a.body.message) }} }},
  session: {{
    prompt: async () => new Promise(() => {{}}),     // never settles, like life
    promptAsync: async (a) => {{ seen.push(a); return {{}} }},
    list: async () => [{{ id: "ses_test", directory: "/tmp/acme-api",
                         time: {{ updated: 1 }} }}],
  }},
}}
const hooks = await mod.A2A({{ client, directory: "/tmp/acme-api" }})
// NO events are fed. session.idle fires only after a turn completes and
// session.created never fires on a resume, so a client that needs either one
// is a client that never speaks at boot — which is the bug this asserts is
// gone. Everything below must happen off the plugin's own initiative.
const hasPing = () => seen.some(c =>
  (c?.body?.parts || []).some(pt => (pt.text || "").includes("plain channel post")))
const deadline = Date.now() + 25000
while (Date.now() < deadline && !hasPing()) {{
  await new Promise(r => setTimeout(r, 200))
}}
console.log(JSON.stringify({{ seen, errors }}))
process.exit(0)
""")
        oc = subprocess.Popen(
            ["node", str(harness)], text=True,
            stdout=subprocess.PIPE, stderr=subprocess.PIPE,
        )
        time.sleep(3)   # let it attach to /stream before we post
        api(base, f"/channels/{CHAN}/messages",
            # Membership is the ONLY reason this arrives. It also carries an
            # email address, because a stray '@' used to switch delivery to
            # "only the ids mentioned" and quietly address nobody.
            {"sender": POSTER,
             "text": "ping from the test — plain channel post (mail "
                     "nobody@example.com)"},
            method="POST")
        out, err = oc.communicate(timeout=40)
        try:
            payload = json.loads(out.strip().splitlines()[-1])
            seen, oc_errors = payload["seen"], payload["errors"]
        except Exception:
            seen, oc_errors = [], []
            notes.append(f"opencode stderr: {err.strip()[:400]}")
        for e in oc_errors:
            notes.append(f"opencode logged: {e[:200]}")
        texts = [
            part.get("text", "")
            for call in seen for part in call.get("body", {}).get("parts", [])
        ]
        check("opencode: injects into the session at all", bool(texts),
              f"no prompts; stderr={err.strip()[:300]}")
        # Against the REAL broker, not a fake: the agent is a member of a real
        # channel, so the client must work that out from /channels and ask it
        # to look — the visible boot action.
        check("opencode: asks the agent to check its channels at boot, naming "
              "the tools, so the check is a tool call you can watch rather "
              "than a summary that appears from nowhere",
              any("Check your channels" in t and "my_pending" in t
                  for t in texts), json.dumps(texts)[:300])
        check("opencode: DELIVERS a plain channel post — membership fan-out, "
              "not just @mentions",
              any("plain channel post" in t for t in texts),
              json.dumps(texts)[:300])
        check("opencode: wraps it in the <channel> envelope",
              any(t.startswith("<channel ") and 'source="a2a"' in t
                  for t in texts), json.dumps(texts)[:300])
        check("opencode: carries the ack id",
              any("id=" in t for t in texts if t.startswith("<channel ")),
              json.dumps(texts)[:300])

        # ================= separate receipts ================================
        # Both are members, so both legitimately receive every channel post.
        # What must NOT happen is one agent's delivery consuming the other's:
        # delivery is a destructive read per (message, recipient) receipt.
        conn = pymysql.connect(
            host=env["A2A_DB_HOST"], port=int(env["A2A_DB_PORT"]),
            user=env["A2A_DB_USER"], password=env["A2A_DB_PASSWORD"],
            database=env["A2A_DB_NAME"])
        with conn.cursor() as cur:
            cur.execute(
                "SELECT agent_id, COUNT(*) n, SUM(acked_at IS NOT NULL) acked "
                "FROM message_receipts GROUP BY agent_id")
            rows = cur.fetchall()
        conn.close()
        got = {r[0]: (r[1], r[2]) for r in rows}
        check("each agent got its OWN receipt — delivery is destructive per "
              "(message, recipient), so a shared inbox would show one",
              got.get(CLAUDE_AGENT, (0, 0))[0] > 0
              and got.get(OPENCODE_AGENT, (0, 0))[0] > 0, str(got))

        # Acking is the client's job now, not the model's: a message that
        # reached the session is acked without the agent doing anything.
        po = api(base, f"/pending?agent={OPENCODE_AGENT}")
        check("opencode acked what it received, with no action by the agent",
              po.get("pending_total", 0) == 0, json.dumps(po)[:200])

    finally:
        if proc:
            proc.terminate()
            try:
                proc.wait(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
        shutil.rmtree(tmp, ignore_errors=True)

    print()
    for n in notes:
        print("note:", n)
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails else "PASS — both harnesses deliver live messages")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
