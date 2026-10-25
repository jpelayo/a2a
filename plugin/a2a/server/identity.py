#!/usr/bin/env python3
"""This client's a2a identity — chosen by the agent, stored on this machine.

    identity.py header --project <dir> --store <dir>   # for headersHelper
    identity.py get    --project <dir> --store <dir>
    identity.py set <id> --project <dir> --store <dir>

The id lives in `<store>/identity.json`, keyed by project directory. Both
surfaces of the plugin read that one file — the `a2a` HTTP server through
`headersHelper`, the channel directly — so the tools and the push channel
cannot claim two different identities on the broker.

**With no entry, the id is the project directory's name.** That is the
compatibility anchor and it is not negotiable: every client shipped before this
store existed sent exactly that, and dozens of agents are registered and
pushing under those ids. An upgrade must be invisible to them — no reinstall,
no reclaim call, no gap in delivery.

Uniqueness is therefore earned, not assumed. `<store>` is the plugin's own
persistent directory, so once a client owns an entry it is that client's alone;
a second client in the same project takes a distinct id the moment it discovers
the directory name is already claimed (the channel does that check, because it
holds the token and this module does not). Until then two fresh clients in one
directory do share an id, and delivery is a destructive read — each message
reaches whichever asked first — so that window is what the channel closes.

Pure standard library, and it never raises: this runs inside a connection
handshake, where refusing to answer costs the session its tools.

THE CLIENT KEY. The agent id says who this client claims to be; the client
key says which harness-directory is saying it. The broker pins each agent to
the first key that acts as it, so no other harness or directory — and no
model in one — can speak as an agent it does not own. The key is DERIVED, not
stored per project: HMAC-SHA256 of a scope (`dir:<project>`, or `agent:<id>`
when A2A_AGENT chose the id) under one secret per install, `client.secret`
beside identity.json. The secret is created once, by whichever process gets
there first — written whole to a private temp file, then link()ed into place,
which fails if another process already won — so the headersHelper and the
channel, starting together, can never end up with two different keys.
"""
import argparse
import base64
import hashlib
import hmac
import json
import os
import secrets
import sys
from pathlib import Path

FILE = "identity.json"
SECRET = "client.secret"


def _store_path(store: str, create: bool = False) -> Path | None:
    if not store:
        return None
    try:
        p = Path(store).expanduser()
        if create:
            p.mkdir(parents=True, exist_ok=True)
        return p / FILE
    except Exception:
        return None


def _load(path: Path | None) -> dict:
    if not path:
        return {}
    try:
        data = json.loads(path.read_text())
        return data if isinstance(data, dict) else {}
    except Exception:
        return {}


def _save(path: Path | None, data: dict) -> None:
    """Atomic, and silent on failure — see the module docstring."""
    if not path:
        return
    try:
        tmp = path.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(data, indent=2, sort_keys=True) + "\n")
        os.replace(tmp, path)
    except Exception:
        pass


def legacy_id(project: str) -> str:
    """What every client sent before this store existed: the directory name.

    This is the compatibility anchor. An install that predates the store has no
    entry, and must keep claiming exactly the id the broker already knows it
    by — anything else would strand a registered, pushing agent behind an
    identity nobody has heard of.
    """
    base = Path(project).name if project else ""
    return base or "agent"


def generate(project: str) -> str:
    """A fresh id nothing else can be using, for a client that owns no name.

    Only ever chosen by the channel, and only once it has confirmed with the
    broker that the legacy id belongs to nobody — see a2a-channel.py. Never
    chosen here, because this module also runs inside a connection handshake
    where a surprise identity would silently fork a running agent in two.
    """
    return f"{legacy_id(project)}-{secrets.token_hex(2)}"


def resolve(project: str, store: str) -> str:
    """This client's id: what it stored, else the id it has always sent.

    Read-only on purpose. The headersHelper calls this on every connection, and
    a handshake must never be the thing that decides an identity.
    """
    key = str(project or os.getcwd())
    current = _load(_store_path(store, create=False)).get(key)
    if isinstance(current, str) and current:
        return current
    return legacy_id(key)


def _secret(store: str) -> bytes | None:
    """This install's secret, created on first use. None if it cannot be had.

    Race-free across processes without a lock: the secret is written whole to
    a temp file only this process knows, then link()ed to its real name. link
    refuses to replace an existing file, so exactly one process's secret
    becomes THE secret and every other one reads it back.
    """
    if not store:
        return None
    try:
        root = Path(store).expanduser()
        root.mkdir(parents=True, exist_ok=True)
        path = root / SECRET
        if not path.exists():
            tmp = root / f".{SECRET}.{os.getpid()}.{secrets.token_hex(4)}"
            try:
                fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o600)
                with os.fdopen(fd, "w") as fh:
                    fh.write(secrets.token_hex(32) + "\n")
                try:
                    os.link(tmp, path)
                except FileExistsError:
                    pass                  # someone else won; theirs is THE one
            finally:
                try:
                    os.unlink(tmp)
                except OSError:
                    pass
        secret = bytes.fromhex(path.read_text().strip())
        # A damaged secret is no secret: a short key would still derive
        # SOMETHING, and that something would not be this install's key.
        return secret if len(secret) == 32 else None
    except Exception:
        return None


def client_key(project: str, store: str, explicit: str = "") -> str:
    """This client's key for the broker: one per harness-directory. '' if none.

    `explicit` is an id chosen for the PROCESS (A2A_AGENT). It becomes the
    scope instead of the directory, so two instances in one directory — the
    supported way to run two agents there — are two clients."""
    secret = _secret(store)
    if not secret:
        return ""
    scope = (f"agent:{explicit}" if explicit
             else f"dir:{os.path.abspath(project or os.getcwd())}")
    mac = hmac.new(secret, scope.encode(), hashlib.sha256).digest()
    return "a2a_ck_" + base64.urlsafe_b64encode(mac).decode().rstrip("=")[:32]


def assign(project: str, store: str, agent_id: str) -> str:
    """Record the id this client owns. The only writer."""
    agent_id = (agent_id or "").strip()
    if not agent_id:
        return resolve(project, store)
    key = str(project or os.getcwd())
    path = _store_path(store, create=True)
    data = _load(path)
    if data.get(key) != agent_id:
        data[key] = agent_id
        _save(path, data)
    return agent_id


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("cmd", choices=("header", "get", "set"))
    ap.add_argument("value", nargs="?", default="")
    ap.add_argument("--project", default=os.environ.get("A2A_AGENT_DIR", ""))
    ap.add_argument("--store", default=os.environ.get("A2A_IDENTITY_STORE", ""))
    args = ap.parse_args()

    if args.cmd == "set":
        print(assign(args.project, args.store, args.value))
        return 0
    explicit = os.environ.get("A2A_AGENT") or ""
    agent = explicit or resolve(args.project, args.store)
    if args.cmd == "header":
        # headersHelper contract: one JSON object of string pairs on stdout.
        # Authorization is NOT set here — a plugin's helper cannot read
        # ${user_config.*}, so the token stays in the static headers block.
        headers = {"X-A2A-Agent": agent}
        key = client_key(args.project, args.store, explicit)
        if key:
            headers["X-A2A-Client"] = key
        print(json.dumps(headers))
    else:
        print(agent)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception:
        # Never fail a connection: emit a usable header and let the broker
        # decide, rather than leaving the session with no tools at all.
        print(json.dumps({"X-A2A-Agent": legacy_id(os.getcwd())}))
        sys.exit(0)
