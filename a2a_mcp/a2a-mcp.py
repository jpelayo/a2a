"""A2A↔MCP communications hub — broker only, no agent execution.

Agents (running anywhere) connect to this server, register themselves, and
talk to each other through:
  - **channels**: themed transcripts, fan-out is read-side (members poll).
  - **broadcasts**: a help-wanted board; agents submit CLAIM / PASS bids.
  - **md_files**: blob-sized markdown attachments referenced from channels.

All data is partitioned per **station**; a token only sees its own station.
The server never spawns an agent process and has no Claude/LLM dependency.

Install:
    pip3 install --break-system-packages mcp uvicorn starlette

Run:
    python3 a2a-mcp.py serve

CLI (operates on A2A_DB_FILE directly — no HTTP, no auth):
    python3 a2a-mcp.py station create <name> [--description ...]
    python3 a2a-mcp.py station list
    python3 a2a-mcp.py station delete <name-or-id>
    python3 a2a-mcp.py token create --station <name-or-id> [--label ...]
    python3 a2a-mcp.py token list [--station ...] [--include-revoked]
    python3 a2a-mcp.py token revoke <token-or-prefix>

Env:
    A2A_HOST              (default 0.0.0.0)
    A2A_PORT              (default 9999)
    A2A_DB_FILE           (default a2a.db next to this script)
    A2A_AGENTS_FILE       (legacy JSON — imported once → 'default' station)
    A2A_CHANNELS_FILE     (legacy JSON — imported once → 'default' station)
    A2A_ADMIN_TOKEN       (enables /admin/* HTTP endpoints)
    A2A_AUTH_DISABLED     (default 0; dev only — all requests → 'default')

Endpoints:
    GET  /healthz                            public liveness
    *    /mcp/                                MCP streamable-http (token)
    *    /agents, /channels, /broadcasts      REST (token, station-scoped)
    *    /admin/stations, /admin/tokens       superuser REST (admin token)
"""
from __future__ import annotations

import argparse
import asyncio
import contextlib
import contextvars
import hashlib
from datetime import datetime, timezone
import gzip
import io
import json
import os
import secrets
import shutil
# Only the `migrate` subcommand opens sqlite, read-only, to read a pre-MariaDB
# database across. Nothing in the serving path touches it.
import sqlite3
import sys
import tempfile
import threading
import time
import urllib.parse
import uuid
import tarfile
import zipfile
from contextlib import asynccontextmanager
from pathlib import Path

import pymysql
import pymysql.cursors
import uvicorn
from mcp.server.fastmcp import FastMCP

# mcp's FastMCP Settings declares `lifespan` with a forward reference to
# FastMCP itself (defined later in that module) and never rebuilds the model,
# so the annotation stays unresolved forever. pydantic-settings >= 2.15 warns
# about exactly this on every FastMCP() construction — in the TUI that lands
# on the operator's terminal looking like something is broken. Rebuilding
# here resolves the annotation for real (verified: FastMCP still constructs,
# no field left incomplete), instead of suppressing the messenger. Wrapped
# because the module path is mcp's internals, not its API.
try:
    import mcp.server.fastmcp.server as _fastmcp_server
    _fastmcp_server.Settings.model_rebuild()
except Exception:  # pragma: no cover - only an mcp-internal rename lands here
    pass
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.middleware import Middleware
from starlette.requests import Request
from starlette.responses import (
    FileResponse,
    JSONResponse,
    PlainTextResponse,
    Response,
    StreamingResponse,
)
from starlette.routing import Mount, Route


# ---------------------------------------------------------------------------
# Configuration.
# ---------------------------------------------------------------------------

# One version for the whole thing — broker, clients and admin surfaces ship
# together from this file's tree, so there is nothing to keep in sync.
# 0.1.0: the schema has migrated in place since before it had a number, and
# it keeps doing so; a version is not a licence to break an existing volume.
# 0.2.0: still true — this bump adds tables (agent_transfer_denials,
# message_addressees) and reads old ones unchanged. It is here because the
# CLIENTS changed in ways an installed copy cannot discover for itself: the
# OpenCode brief no longer repeats until the context is full, messages are
# capped, and posts carry `to`/`addressed`. A client compares its baked
# version against /healthz's `clients` and says so once when it is behind, so
# leaving this at 0.1.0 would let every stale install stay quiet.
VERSION = "0.2.0"

# Storage is MariaDB (A2A_DB_HOST and friends, defined with the pool below).
# A2A_DB_FILE survives as one thing only: where `migrate` starts looking for a
# pre-MariaDB database to read across. It is never opened by the serving path.
LEGACY_DB_FILE = Path(os.environ.get(
    "A2A_DB_FILE", str(Path(__file__).parent / "a2a.db")
))
LEGACY_AGENTS = Path(os.environ.get(
    "A2A_AGENTS_FILE", str(Path(__file__).parent / "agents.json")
))
LEGACY_CHANNELS = Path(os.environ.get(
    "A2A_CHANNELS_FILE", str(Path(__file__).parent / "channels.json")
))

HOST = os.environ.get("A2A_HOST", "0.0.0.0")
PORT = int(os.environ.get("A2A_PORT", "9999"))
ADMIN_TOKEN = os.environ.get("A2A_ADMIN_TOKEN") or ""
AUTH_DISABLED = os.environ.get("A2A_AUTH_DISABLED", "0") == "1"

# /stream tick: how often a parked stream re-polls the DB, re-checks its token
# and (in json mode) emits a keepalive newline. Short on purpose:
#  - writes from another process (CLI, a second worker) can't fire the in-
#    process wake event, so the tick bounds their delivery latency;
#  - the keepalive byte defeats reverse-proxy buffering and read timeouts
#    (nginx defaults: proxy_buffering on, proxy_read_timeout 60s) without any
#    proxy configuration. json consumers skip blank lines by contract.
#
# THIS NUMBER IS COUPLED TO THE CLIENTS. Every client treats a stream that is
# silent for A2A_STREAM_TIMEOUT (default 30s) as dead and reconnects — that is
# what makes a link killed by a lid close or a NAT eviction recover instead of
# hanging forever. Raise this above that and every client starts flapping:
# they will give up on a healthy stream just before the next keepalive.
# The two live in different files, so changing one means checking the other —
# see STREAM_READ_TIMEOUT in plugin/a2a/server/a2a-channel.py.
STREAM_KEEPALIVE = float(os.environ.get("A2A_STREAM_KEEPALIVE", "5"))

# Client plugin sources, shipped in the image. Both artifacts are produced
# FROM THIS TREE at request time, never from a pre-built file, so a client can
# never be handed a stale plugin: rebuild the image (or bind-mount the tree)
# and the very next download is the new one. There is no artifact to forget.
def _default_plugin_src() -> Path:
    """`/app/plugin` in the container, `plugin/` at the repo root in a checkout."""
    here = Path(__file__).resolve().parent
    for cand in (here / "plugin", here.parent / "plugin"):
        if cand.is_dir():
            return cand
    return here / "plugin"


PLUGIN_SRC = Path(os.environ.get("A2A_PLUGIN_SRC") or _default_plugin_src())
# Claude Code: GET /a2a-claudecode.tar.gz, packed on the fly from <src>/a2a.
PLUGIN_DIR = PLUGIN_SRC / "a2a"
# OpenCode: GET /install/<token>, served with the caller's token prepended.
OPENCODE_JS = PLUGIN_SRC / "opencode" / "a2a-opencode.js"
# Pi: GET /install/pi/<token>. A directory, not a file — the extension needs a
# package.json beside it because Pi does not re-export typebox.
PI_DIR = PLUGIN_SRC / "pi"
# The Codex client: one MCP server plus the launcher that gives it a private
# app-server to inject into. A directory, like Pi.
CODEX_DIR = PLUGIN_SRC / "codex"



def _archive_dir(root: Path, fmt: str = "tar.gz",
                 rewrite: dict[str, bytes] | None = None) -> bytes:
    """Pack `root` in memory, cached until any file in it changes.

    Keyed on the tree's newest mtime plus its file count, so an edit, an added
    file and a deleted file all invalidate. `rewrite` replaces the bytes of a
    named member (the installers prepend credentials to one file) and is NOT
    cached, since it differs per caller.

    The output is deterministic — fixed member metadata, no gzip timestamp —
    so an unchanged tree always yields identical bytes and a proxy or client
    comparing them sees no spurious change.
    """
    files = sorted(p for p in root.rglob("*") if p.is_file()
                   and "__pycache__" not in p.parts)
    stamp = (max((p.stat().st_mtime_ns for p in files), default=0), len(files))
    key = (str(root), fmt)
    if not rewrite and _archive_dir._cache.get(key, (None,))[0] == stamp:
        return _archive_dir._cache[key][1]

    buf = io.BytesIO()
    if fmt == "zip":
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
            for p in files:
                z.write(p, p.relative_to(root).as_posix())
    else:
        raw = io.BytesIO()
        with tarfile.open(fileobj=raw, mode="w") as t:
            for p in files:
                rel = p.relative_to(root).as_posix()
                data = (rewrite or {}).get(rel) or p.read_bytes()
                info = tarfile.TarInfo(rel)
                info.size = len(data)
                info.mode = 0o755 if p.stat().st_mode & 0o100 else 0o644
                # Fixed metadata: the archive must not change just because a
                # file was touched or the builder runs as a different user.
                info.mtime = 0
                info.uid = info.gid = 0
                info.uname = info.gname = "root"
                t.addfile(info, io.BytesIO(data))
        buf = io.BytesIO()
        with gzip.GzipFile(fileobj=buf, mode="wb", mtime=0) as g:
            g.write(raw.getvalue())

    body = buf.getvalue()
    if not rewrite:
        _archive_dir._cache[key] = (stamp, body)
    return body


_archive_dir._cache: dict = {}

# Public base url advertised to installed clients. Falls back to the Host
# header the installer was reached on, so the common case needs no config.
PUBLIC_URL = os.environ.get("A2A_PUBLIC_URL", "").rstrip("/")

# Cap how many backlog messages a freshly-attached stream replays, so an agent
# connecting for the first time (or after a long absence) gets a bounded
# catch-up instead of the entire channel history injected as keystrokes.
STREAM_BACKLOG_LIMIT = int(os.environ.get("A2A_STREAM_BACKLOG_LIMIT", "50"))
# How far back a reconnect re-sends messages it already delivered but that were
# never acked. This is crash recovery — a push lost between the socket write
# and the client reading it — so it is measured in minutes, not forever. An
# unbounded window makes every reconnect replay every stale unacked message,
# and the agent answers its old mail again on each boot.
STREAM_REPLAY_WINDOW = float(os.environ.get("A2A_STREAM_REPLAY_WINDOW", "600"))

def _short_duration(seconds: float) -> str:
    """`47h`, `3d`, `12m` — the inverse of parse_duration, for humans.

    Coarse on purpose: an operator deciding whether to approve a name needs to
    know "tomorrow" or "in a minute", never the seconds.
    """
    seconds = max(0, int(seconds))
    if seconds >= 172800:
        return f"{seconds // 86400}d"
    if seconds >= 3600:
        return f"{seconds // 3600}h"
    if seconds >= 60:
        return f"{seconds // 60}m"
    return f"{seconds}s"


def parse_duration(value: str | float | int | None) -> float | None:
    """Seconds, from `90`, `"90s"`, `"30m"`, `"12h"`, `"7d"` or `"2w"`.

    A bare number is seconds. None and "" mean "unset", which is not the same
    as zero: callers use that to fall back to a default, while zero is a
    refusal.

    Durations are the whole point of the field it serves — a message may be
    worth reading for ten minutes or for a year — so it must not be measured
    in any one unit.
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        seconds = float(value)
    else:
        text = str(value).strip().lower()
        unit = {"s": 1, "m": 60, "h": 3600, "d": 86400, "w": 604800}
        mult = unit.get(text[-1:], None)
        try:
            seconds = float(text[:-1]) * mult if mult else float(text)
        except ValueError:
            raise ValueError(
                f"not a duration: {value!r} — use 90, '90s', '30m', '12h', "
                f"'7d' or '2w'"
            ) from None
    if seconds <= 0:
        raise ValueError(f"duration must be positive, got {value!r}")
    return seconds


def parse_size(value: str | int | float | None) -> int | None:
    """Bytes, from `65536`, `"64k"`, `"512K"`, `"4M"` or `"1G"`.

    Same contract as parse_duration: a bare number is bytes, None and "" mean
    "unset" so a caller can fall back to a default, and nonsense raises rather
    than silently becoming zero — a size limit that quietly reads as 0 would
    reject every message in the station.

    Powers of two, not of ten, because every limit it is compared against is:
    MEDIUMTEXT is 2**24-1 and max_allowed_packet is set in MiB.
    """
    if value is None or value == "":
        return None
    if isinstance(value, (int, float)):
        size = float(value)
    else:
        text = str(value).strip()
        unit = {"k": 1024, "m": 1024 ** 2, "g": 1024 ** 3}
        mult = unit.get(text[-1:].lower(), None)
        try:
            size = float(text[:-1]) * mult if mult else float(text)
        except ValueError:
            raise ValueError(
                f"not a size: {value!r} — use 65536, '64k', '512K' or '4M'"
            ) from None
    if size <= 0:
        raise ValueError(f"size must be positive, got {value!r}")
    return int(size)


def _short_size(nbytes: int) -> str:
    """`64k`, `1.5M` — the inverse of parse_size, for error messages."""
    if nbytes >= 1024 ** 2:
        mb = nbytes / 1024 ** 2
        return f"{mb:.1f}M".replace(".0M", "M")
    if nbytes >= 1024:
        kb = nbytes / 1024
        return f"{kb:.1f}k".replace(".0k", "k")
    return f"{nbytes}B"


# The ceiling on one message. There was none, which meant the real limit was
# whichever hop failed first, and every one of them fails in a way the sender
# cannot read: a 413 from a reverse proxy it cannot see, a packet error from
# MariaDB, or — the common case — a body that arrives fine and then does not
# fit in the recipient's context.
#
# 64 KiB is set by the tightest hop, and that hop is not the database. Storage
# would take 256 times more (MEDIUMTEXT, 16 MiB), but every message here exists
# to be injected into a model session: 64 KiB is roughly 16k tokens, large
# enough for any real message and small enough that a read_channel(limit=10)
# and a fan-out to twenty members are both unremarkable. It also sits well
# inside nginx's 1 MiB default body size, so no edge configuration has to
# change for the broker's answer to be the one the sender sees.
MAX_MESSAGE_SIZE = parse_size(os.environ.get("A2A_MAX_MESSAGE_SIZE") or "64k")
# Markdown blobs are the deliberate exception, and they earn it: md_files.content
# is LONGTEXT, fetch_md returns ONE of them when an agent asks, and nothing
# about them is fanned out into receipts. So this is the route for bulk, and the
# limit is the transport rather than the reader — 512 KiB leaves room for JSON
# escaping to inflate the upload without crossing that same 1 MiB proxy default.
MAX_MD_SIZE = parse_size(os.environ.get("A2A_MAX_MD_SIZE") or "512k")


def check_size(text: str, limit: int, what: str = "message") -> None:
    """Refuse an oversized body, in bytes, before it reaches the database.

    Bytes and not characters: bytes are what MEDIUMTEXT and max_allowed_packet
    count, and a limit expressed in characters cannot be checked against either
    — one emoji is four of these.

    The error names the size, the ceiling and the way round it, because the
    sender is a model that will otherwise retry the same body unchanged.
    """
    size = len(text.encode("utf-8"))
    if size > limit:
        raise ValueError(
            f"{what} is {_short_size(size)} ({size} bytes) — the limit is "
            f"{_short_size(limit)} ({limit} bytes). Do not retry it unchanged: "
            f"share_md uploads up to {_short_size(MAX_MD_SIZE)} and returns an "
            f"md:// URI to post instead, or split it and send the parts."
        )


# Ephemerality. A message is kept while any member of its audience has not
# acked it (see message_receipts) — that guarantee is absolute, with exactly
# one escape hatch: a message that has expired. Every message carries an
# expiry; MAX_RETENTION is the default and the ceiling, so an agent that never
# comes back cannot pin its messages forever.
#
# Expressed as a duration, not a day count: the same setting has to say "a
# year" for reference traffic and "ten minutes" for a message that is only
# worth acting on now.
MAX_RETENTION = parse_duration(
    os.environ.get("A2A_MAX_RETENTION_TIME") or "365d"
)
_legacy_days = os.environ.get("A2A_MAX_RETENTION_DAYS")
if _legacy_days:
    # Still honoured rather than ignored: it is in DEPLOY.md and may be set in
    # a live compose file, and silently dropping it would quietly change how
    # long every station keeps everything.
    MAX_RETENTION = float(_legacy_days) * 86400.0
    print("[config] A2A_MAX_RETENTION_DAYS is deprecated — use "
          f"A2A_MAX_RETENTION_TIME={_legacy_days}d", file=sys.stderr)
# Retained for the messages that still speak in days (channel policy, doctor).
MAX_RETENTION_DAYS = MAX_RETENTION / 86400.0

# How long a proposed agent name waits for an operator. Long enough to survive
# a weekend, short enough that a name nobody wanted does not sit on the screen
# forever. Same duration syntax as everything else here.
PROPOSAL_TTL = parse_duration(os.environ.get("A2A_PROPOSAL_TTL") or "48h")
# A client in a restart loop must not be able to fill an operator's screen.
MAX_PENDING_PROPOSALS = 5
# How long a DENIED transfer request bars the same token from asking again.
# A transfer hands one agent's identity — and with it that agent's unacked
# inbox — to a different token, so "no" has to mean no for a while rather than
# inviting the next attempt. Transfers only: a rejected CLAIM is usually a typo
# the agent should be free to correct at once. An operator can always clear a
# lock early with `agent unlock`.
TRANSFER_LOCKTIME = parse_duration(
    os.environ.get("A2A_TRANSFER_LOCKTIME") or "24h"
)
# How long a diagnostic is worth keeping. A ping proves delivery works, and its
# value is spent the moment it arrives — but stored like ordinary traffic it is
# kept until its audience acks, so one aimed at an agent that never comes back
# pins a receipt for a year. It also pollutes what it measures: `doctor` counts
# pending messages, and its own past pings were in that count.
#
# Long enough to outlive a client on a 5s reconnect or one backing off 30s
# against a 403; short enough that yesterday's diagnostics are never in today's
# numbers.
PING_TTL = parse_duration(os.environ.get("A2A_PING_TTL") or "10m")
# How often a serving process may run the collector off the stream tick.
COLLECT_INTERVAL = float(os.environ.get("A2A_COLLECT_INTERVAL", "300"))

DEFAULT_STATION_ID = "default"
DEFAULT_STATION_NAME = "default"


# ---------------------------------------------------------------------------
# MariaDB storage.
#
# Everything here is one server's business: a2a used to open the database file
# directly from every process — server, CLI, TUI — which is why a file swapped
# under a running broker forked it in silence. One process owns the storage
# now, and everyone else speaks a protocol to it.
#
# Three conventions hold across every table, and each is load-bearing:
#
#   utf8mb4_bin   MariaDB's default collation is case-INSENSITIVE. Agent ids
#                 are matched literally by contract and delivery is a
#                 destructive read, so `Foo` and `foo` must stay two agents.
#                 Under the default they would collide and a unique constraint
#                 would reject the second as a duplicate.
#   VARCHAR keys  TEXT cannot be indexed without a prefix length, so every key
#                 column is a sized VARCHAR. The widest composite is
#                 md_files(station_id, uri) at 319 chars = 1276 bytes, well
#                 inside InnoDB's 3072-byte limit.
#   DOUBLE ts     Timestamps stay epoch seconds, not DATETIME. Converting them
#                 would touch every comparison in the file for no gain.
#
# Lengths on both sides of a foreign key match exactly — InnoDB requires
# compatible types, and matching them removes the question.
#
# DEFAULTs on TEXT columns are a MariaDB affordance (10.2+); MySQL rejects
# them. This will not port sideways without revisiting that.
# ---------------------------------------------------------------------------

# A tuple, not one string: PyMySQL executes one statement per call, and
# splitting a blob of DDL on semicolons is a parser waiting to be wrong.
SCHEMA = (
    """
CREATE TABLE IF NOT EXISTS stations (
  station_id   VARCHAR(64)  NOT NULL,
  `name`       VARCHAR(64)  NOT NULL,
  description  TEXT         NOT NULL DEFAULT (''),
  created_at   DOUBLE       NOT NULL,
  -- 1 = open station: every valid token is on its allow list (shown as '*').
  -- Closed by default; only a server-side command can change it.
  `open`       TINYINT      NOT NULL DEFAULT 0,
  PRIMARY KEY (station_id),
  UNIQUE KEY uq_stations_name (`name`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
-- A token is a USER credential, not a tenant key: it authenticates, and the
-- stations it may act in are exactly its token_grants rows. Nothing is
-- inferred — which station a request acts in comes from the agent it names
-- (see resolve_request_station), so a request with no agent has no station.
CREATE TABLE IF NOT EXISTS tokens (
  token_hash      VARCHAR(64)  NOT NULL,
  `user`          VARCHAR(128) NOT NULL DEFAULT '',
  label           VARCHAR(128) NOT NULL DEFAULT '',
  prefix          VARCHAR(16)  NOT NULL,
  created_at      DOUBLE       NOT NULL,
  revoked_at      DOUBLE       NULL,
  last_used_at    DOUBLE       NULL,
  PRIMARY KEY (token_hash),
  KEY idx_tokens_prefix (prefix)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
-- A station's ALLOW LIST: which tokens may act in it. Entries are added by a
-- server-side command only (station allow) — never by the token itself.
CREATE TABLE IF NOT EXISTS token_grants (
  token_hash  VARCHAR(64) NOT NULL,
  station_id  VARCHAR(64) NOT NULL,
  PRIMARY KEY (token_hash, station_id),
  KEY idx_token_grants_station (station_id),
  FOREIGN KEY (token_hash) REFERENCES tokens(token_hash) ON DELETE CASCADE,
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
CREATE TABLE IF NOT EXISTS agents (
  station_id        VARCHAR(64)  NOT NULL,
  agent_id          VARCHAR(128) NOT NULL,
  `name`            VARCHAR(128) NOT NULL,
  description       TEXT         NOT NULL DEFAULT (''),
  expertise         LONGTEXT     NOT NULL DEFAULT ('[]'),
  projects          LONGTEXT     NOT NULL DEFAULT ('[]'),
  system_prompt     TEXT         NOT NULL DEFAULT (''),
  metadata          LONGTEXT     NOT NULL DEFAULT ('{}'),
  created_at        DOUBLE       NOT NULL,
  -- NULL = unclaimed; the first token to act as this agent claims it, and
  -- afterwards only that token may (agent pinning).
  owner_token_hash  VARCHAR(64)  NULL,
  PRIMARY KEY (station_id, agent_id),
  KEY idx_agents_agent_id (agent_id),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """

-- There is deliberately no alias/redirect table here. An agent has ONE name,
-- and X-A2A-Agent is matched against it literally. Clients keep their own name
-- (see the plugins' identity store), so a rename is theirs to record; a
-- server-side redirect would capture every other client sending the old id and
-- silently merge two sessions into one identity.

CREATE TABLE IF NOT EXISTS channels (
  station_id  VARCHAR(64)  NOT NULL,
  `name`      VARCHAR(128) NOT NULL,
  theme       TEXT         NOT NULL DEFAULT (''),
  members     LONGTEXT     NOT NULL DEFAULT ('[]'),
  policy      LONGTEXT     NOT NULL DEFAULT ('{}'),
  created_at  DOUBLE       NOT NULL,
  PRIMARY KEY (station_id, `name`),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
CREATE TABLE IF NOT EXISTS transcripts (
  id          VARCHAR(64)  NOT NULL,
  station_id  VARCHAR(64)  NOT NULL,
  channel     VARCHAR(128) NOT NULL,
  ts          DOUBLE       NOT NULL,
  sender      VARCHAR(128) NOT NULL,
  `text`      MEDIUMTEXT   NOT NULL,
  -- When this stops being worth reading. Always set: the sender may shorten
  -- it, and MAX_RETENTION is both the default and the ceiling.
  expires_at  DOUBLE       NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  KEY idx_transcripts_channel_ts (station_id, channel, ts),
  FOREIGN KEY (station_id, channel)
    REFERENCES channels(station_id, `name`) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
CREATE TABLE IF NOT EXISTS broadcasts (
  id          VARCHAR(64)  NOT NULL,
  station_id  VARCHAR(64)  NOT NULL,
  sender      VARCHAR(128) NOT NULL,
  problem     MEDIUMTEXT   NOT NULL,
  expertise   LONGTEXT     NOT NULL DEFAULT ('[]'),
  projects    LONGTEXT     NOT NULL DEFAULT ('[]'),
  `status`    VARCHAR(16)  NOT NULL DEFAULT 'open',
  created_at  DOUBLE       NOT NULL,
  updated_at  DOUBLE       NOT NULL,
  -- the agents this was addressed to, persisted so /stream can push it to
  -- them; without this a broadcast is write-only (nothing polls any more).
  candidates  LONGTEXT     NOT NULL DEFAULT ('[]'),
  PRIMARY KEY (id),
  -- Declared ascending: MariaDB before 10.8 parses DESC in an index and
  -- ignores it, and the optimizer scans this one backwards regardless.
  KEY idx_broadcasts_station_status (station_id, `status`, created_at),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
CREATE TABLE IF NOT EXISTS bids (
  id            BIGINT       NOT NULL AUTO_INCREMENT,
  station_id    VARCHAR(64)  NOT NULL,
  broadcast_id  VARCHAR(64)  NOT NULL,
  agent_id      VARCHAR(128) NOT NULL,
  bid           VARCHAR(16)  NOT NULL,
  pitch         TEXT         NOT NULL DEFAULT (''),
  created_at    DOUBLE       NOT NULL,
  PRIMARY KEY (id),
  UNIQUE KEY uq_bids_broadcast_agent (broadcast_id, agent_id),
  KEY idx_bids_broadcast (station_id, broadcast_id),
  FOREIGN KEY (broadcast_id) REFERENCES broadcasts(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """

-- Direct messages: agent-to-agent inside one station. Kept out of channel
-- transcripts so they are private to the two parties, and delivered by the
-- same /stream push path. A DM to yourself is allowed on purpose: it is the
-- end-to-end test for Claude Code channel delivery.
CREATE TABLE IF NOT EXISTS dms (
  id          VARCHAR(64)  NOT NULL,
  station_id  VARCHAR(64)  NOT NULL,
  sender      VARCHAR(128) NOT NULL,
  recipient   VARCHAR(128) NOT NULL,
  `text`      MEDIUMTEXT   NOT NULL,
  ts          DOUBLE       NOT NULL,
  expires_at  DOUBLE       NOT NULL DEFAULT 0,
  PRIMARY KEY (id),
  KEY idx_dms_recipient (station_id, recipient, ts),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
CREATE TABLE IF NOT EXISTS md_files (
  uri         VARCHAR(255) NOT NULL,
  station_id  VARCHAR(64)  NOT NULL,
  channel     VARCHAR(128) NULL,
  sender      VARCHAR(128) NOT NULL,
  filename    VARCHAR(255) NOT NULL,
  content     LONGTEXT     NOT NULL,
  sha256      VARCHAR(64)  NOT NULL,
  `size`      BIGINT       NOT NULL,
  created_at  DOUBLE       NOT NULL,
  -- The widest composite key here: 319 chars = 1276 bytes, against InnoDB's
  -- 3072-byte limit.
  PRIMARY KEY (station_id, uri),
  KEY idx_md_files_channel (station_id, channel),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """

-- THE AUDIENCE, MATERIALIZED. One row per (message, agent it is meant for),
-- written when the message is posted and never extended afterwards — which is
-- what keeps a newcomer from drowning: an agent that joins a channel today is
-- in no receipt written yesterday, so it has no backlog by construction.
--   delivered_at  pushed into a session (or returned by a read_* tool)
--   acked_at      the agent said it had processed it
-- Delivery reads delivered_at; the collector reads acked_at only, so a message
-- delivered but never acked is redelivered and is never garbage-collected.
CREATE TABLE IF NOT EXISTS message_receipts (
  station_id   VARCHAR(64)  NOT NULL,
  msg_id       VARCHAR(64)  NOT NULL,  -- transcripts.id | dms.id | broadcasts.id
  kind         VARCHAR(16)  NOT NULL,  -- 'channel' | 'dm' | 'broadcast'
  agent_id     VARCHAR(128) NOT NULL,
  ts           DOUBLE       NOT NULL,  -- copy of the message ts: order, no join
  -- Copy of the message's expiry, for the same reason as ts: the delivery
  -- query runs on every stream tick, and it must not join three possible
  -- parent tables for a value that never changes after insert.
  expires_at   DOUBLE       NOT NULL DEFAULT 0,
  delivered_at DOUBLE       NULL,
  acked_at     DOUBLE       NULL,
  PRIMARY KEY (station_id, msg_id, agent_id),
  -- The delivery query: this agent's undelivered receipts, oldest first.
  KEY idx_receipts_pending (station_id, agent_id, delivered_at, ts),
  -- The collector's query: is anything still unacked for this message?
  KEY idx_receipts_msg (station_id, msg_id, acked_at),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """

-- WHO A POST IS FOR, as opposed to who receives it. Everyone in a channel
-- receives every post and the message is kept until all of them ack; this
-- records the far smaller thing the sender meant by `to`, so a reader can tell
-- "answering me" from "said it to the room".
--
-- SOFT, and that word is the whole design: no receipt is written from here,
-- the audience is not narrowed, and retention is untouched. The moment an
-- addressee changes who gets a message, this stops being an annotation and
-- becomes routing — which _channel_audience exists to keep out of the body.
--
-- Channel posts only, which is what lets msg_id reference transcripts
-- directly (message_receipts cannot: its msg_id spans three tables). That FK
-- is load-bearing — retirement needs no code of its own, because collect()'s
-- DELETE FROM transcripts cascades here.
CREATE TABLE IF NOT EXISTS message_addressees (
  station_id  VARCHAR(64)  NOT NULL,
  msg_id      VARCHAR(64)  NOT NULL,  -- transcripts.id
  agent_id    VARCHAR(128) NOT NULL,
  PRIMARY KEY (station_id, msg_id, agent_id),
  KEY idx_addressees_msg (station_id, msg_id),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE,
  FOREIGN KEY (msg_id) REFERENCES transcripts(id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
-- Only the station-wide firehose (/stream with no ?agent=) uses a cursor.
-- Per-agent delivery is tracked per message in message_receipts, so the
-- agent_id='' row is the one that matters here.
CREATE TABLE IF NOT EXISTS stream_cursors (
  station_id  VARCHAR(64)  NOT NULL,
  agent_id    VARCHAR(128) NOT NULL DEFAULT '',
  last_ts     DOUBLE       NOT NULL DEFAULT 0,
  PRIMARY KEY (station_id, agent_id),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
-- A name a client has ASKED for. Not an agent: nothing here can send, receive
-- or be addressed, and only an operator (TUI/CLI) can turn one into an agent.
-- That asymmetry is the point — a station token may propose and nothing else,
-- so this is not create_agent with extra steps.
--
-- Same composite PK as `agents`, so two clients proposing one name in one
-- station collide instead of queueing.
CREATE TABLE IF NOT EXISTS agent_proposals (
  station_id  VARCHAR(64)  NOT NULL,
  agent_id    VARCHAR(128) NOT NULL,
  token_hash  VARCHAR(64)  NOT NULL,
  note        TEXT         NOT NULL DEFAULT (''),
  created_at  DOUBLE       NOT NULL,
  -- Unapproved proposals are deleted, not kept: an operator's screen is not
  -- a graveyard of names some client tried once.
  expires_at  DOUBLE       NOT NULL,
  PRIMARY KEY (station_id, agent_id),
  KEY idx_proposals_token (token_hash),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
-- A denied TRANSFER request, and how long the refusal stands. Its own table
-- rather than a column on agent_proposals for two reasons: that table's PK is
-- (station_id, agent_id), so a denial parked there would block every OTHER
-- token from proposing the same name; and this file has no add-column
-- migration path, while a new table lands on an old volume by itself.
--
-- Keyed by the REQUESTING token. Telling one client no says nothing about the
-- next one, which may have a perfectly good reason to ask.
CREATE TABLE IF NOT EXISTS agent_transfer_denials (
  station_id    VARCHAR(64)  NOT NULL,
  agent_id      VARCHAR(128) NOT NULL,
  token_hash    VARCHAR(64)  NOT NULL,
  denied_at     DOUBLE       NOT NULL,
  denied_until  DOUBLE       NOT NULL,
  PRIMARY KEY (station_id, agent_id, token_hash),
  KEY idx_denials_until (denied_until),
  FOREIGN KEY (station_id) REFERENCES stations(station_id) ON DELETE CASCADE
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
    """
-- Operational log and audit trail in one table, because the question that
-- matters ("what happened, and who did it") does not respect the boundary.
-- Rotation is retention: collect() deletes past A2A_LOG_RETENTION, and stays
-- the only thing in this file that deletes anything.
--
-- DELIBERATELY NO FOREIGN KEY on `station`. Most lines carry one and some
-- carry none, and a log write that can be rejected for referential integrity
-- is a log write that vanishes exactly when it is worth having. For the same
-- reason _log() falls back to stderr rather than raising: the logs explaining
-- a database fault must not need the database.
CREATE TABLE IF NOT EXISTS logs (
  id       BIGINT       NOT NULL AUTO_INCREMENT,
  ts       DOUBLE       NOT NULL,
  level    VARCHAR(8)   NOT NULL DEFAULT 'INFO',
  station  VARCHAR(64)  NOT NULL DEFAULT '',
  actor    VARCHAR(128) NOT NULL DEFAULT '',
  event    VARCHAR(64)  NOT NULL DEFAULT '',
  message  MEDIUMTEXT   NOT NULL,
  PRIMARY KEY (id),
  KEY idx_logs_ts (ts),
  KEY idx_logs_station_ts (station, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin;""",
)


# ---------------------------------------------------------------------------
# A small MariaDB pool behind the same `CONN.execute(sql, params)` surface the
# rest of this file has always used.
#
# The old shape was one sqlite connection behind a re-entrant lock, because the
# sqlite3 module will not take concurrent statements on one connection and a
# dying stream generator looks exactly like silent message loss. MariaDB has no
# such constraint, so the lock goes and real concurrency arrives — but the
# surface stays, so every call site keeps working.
#
# Two things worth knowing before changing anything here:
#
#   rowcount is CHANGED rows, not matched, because CLIENT.FOUND_ROWS is left
#   off. _ack_receipts and screen() count what they actually changed by pairing
#   `WHERE acked_at IS NULL` with rowcount, and matched-rows would make them
#   report work they did not do. Turning that flag on quietly breaks both.
#
#   `with CONN:` is now a REAL transaction. Under sqlite with
#   isolation_level=None it only ever held a mutex — the statements inside
#   autocommitted one by one and a failure half way through left half the group
#   applied. Here the block gets one connection, BEGIN on entry, COMMIT on
#   exit, ROLLBACK on exception.
# ---------------------------------------------------------------------------

DB_HOST = os.environ.get("A2A_DB_HOST", "mariadb")
DB_PORT = int(os.environ.get("A2A_DB_PORT", "3306"))
DB_NAME = os.environ.get("A2A_DB_NAME", "a2a")
DB_USER = os.environ.get("A2A_DB_USER", "a2a")
def _db_password() -> str:
    """The database password, from a file if one is named.

    `A2A_DB_PASSWORD_FILE` wins over `A2A_DB_PASSWORD`, matching the `_FILE`
    convention the mariadb image uses. It is how the deployment gets a
    per-host credential with nothing to configure and no secret in this
    repository: a one-shot init service writes a random password into a volume
    both containers mount, and neither this file nor docker-compose.yml ever
    contains one.
    """
    path = os.environ.get("A2A_DB_PASSWORD_FILE", "")
    if path:
        try:
            return Path(path).read_text(encoding="utf-8").strip()
        except OSError as e:
            raise SystemExit(
                f"a2a-mcp: A2A_DB_PASSWORD_FILE={path!r} is unreadable ({e}). "
                "It is written once by the a2a-secret init service; check that "
                "the volume is mounted and that service ran."
            )
    return os.environ.get("A2A_DB_PASSWORD", "")


DB_PASSWORD = _db_password()
DB_POOL_SIZE = int(os.environ.get("A2A_DB_POOL", "8"))
# The broker may come up before MariaDB is accepting connections. compose's
# `depends_on: service_healthy` covers the cold start; this covers everything
# else — a restart of the database under a running broker, a network blip.
DB_CONNECT_TIMEOUT = float(os.environ.get("A2A_DB_CONNECT_TIMEOUT", "60"))


class _Rows:
    """A fully-fetched result: the cursor never escapes the pool."""

    def __init__(self, rows: list, rowcount: int):
        self._rows = rows
        self._i = 0
        self.rowcount = rowcount

    def fetchone(self):
        if self._i < len(self._rows):
            row = self._rows[self._i]
            self._i += 1
            return row
        return None

    def fetchall(self) -> list:
        out = self._rows[self._i:]
        self._i = len(self._rows)
        return out

    def __iter__(self):
        return iter(self.fetchall())


def _new_connection():
    return pymysql.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER,
        password=DB_PASSWORD, database=DB_NAME,
        charset="utf8mb4", autocommit=True,
        cursorclass=pymysql.cursors.DictCursor,
        # NOT client_flag=CLIENT.FOUND_ROWS — see the note above.
    )


class _Pool:
    """Idle connections, handed out one statement or one transaction at a time.

    Nothing here is clever on purpose. The workload is a handful of concurrent
    streams and the occasional operator command; the only real requirement is
    that a connection parked for hours on a quiet stream still works, which is
    what the ping is for.
    """

    def __init__(self, size: int):
        self._size = size
        self._idle: list = []
        self._lock = threading.Lock()
        # Set only inside `with CONN:` — the transaction's connection, pinned
        # to this thread so every execute() in the block joins it. _db() runs
        # each callable in one worker thread, and no `with CONN:` block spans
        # an await, so a thread-local is exactly the right scope.
        self._tx = threading.local()

    def _checkout(self):
        with self._lock:
            conn = self._idle.pop() if self._idle else None
        if conn is None:
            return _new_connection()
        try:
            # Liveness only. `reconnect=True` is deprecated in pymysql 2.x and
            # would hand back a connection with a silently reset session, so a
            # dead one is replaced rather than revived — which is what happens
            # every time MariaDB drops an idle connection past wait_timeout
            # (8h default) under a quiet /stream.
            conn.ping(reconnect=False)
        except Exception:
            try:
                conn.close()
            except Exception:
                pass
            return _new_connection()
        return conn

    def _release(self, conn) -> None:
        with self._lock:
            if len(self._idle) < self._size:
                self._idle.append(conn)
                return
        try:
            conn.close()
        except Exception:
            pass

    @contextlib.contextmanager
    def _lease(self):
        pinned = getattr(self._tx, "conn", None)
        if pinned is not None:          # inside `with CONN:` — join it
            yield pinned
            return
        conn = self._checkout()
        try:
            yield conn
        finally:
            self._release(conn)

    def execute(self, sql: str, params=()) -> _Rows:
        with self._lease() as conn:
            with conn.cursor() as cur:
                cur.execute(sql, params)
                rows = cur.fetchall() if cur.description else []
                return _Rows(list(rows), cur.rowcount)

    def script(self, statements) -> None:
        """Run a sequence of DDL statements. Replaces sqlite's executescript."""
        with self._lease() as conn:
            with conn.cursor() as cur:
                for stmt in statements:
                    cur.execute(stmt)

    def __enter__(self):
        if getattr(self._tx, "conn", None) is not None:
            # Nested `with CONN:`. The outermost owns the transaction; inner
            # blocks are a no-op rather than a second BEGIN that would commit
            # the outer one early.
            self._tx.depth += 1
            return self
        conn = self._checkout()
        conn.begin()
        self._tx.conn = conn
        self._tx.depth = 1
        return self

    def __exit__(self, exc_type, exc, tb):
        self._tx.depth -= 1
        if self._tx.depth > 0:
            return False
        conn = self._tx.conn
        self._tx.conn = None
        try:
            if exc_type is None:
                conn.commit()
            else:
                conn.rollback()
        finally:
            self._release(conn)
        return False


# Answered before the database is touched. Connecting runs migrations and needs
# a reachable server, and the usual moment to ask "what version is this?" is
# when something is already wrong — an unreachable database must not be what
# stops you finding out. argparse cannot do this for us: it only runs after the
# whole module, connection included, has been imported.
if __name__ == "__main__" and "--version" in sys.argv[1:]:
    print(f"a2a-mcp {VERSION}")
    raise SystemExit(0)


def _wait_for_db(timeout: float = DB_CONNECT_TIMEOUT) -> None:
    """Block until MariaDB answers, or give up with something readable.

    A traceback from deep inside the driver is a poor way to learn that the
    database container has not finished starting.
    """
    deadline = time.monotonic() + timeout
    delay, last = 0.25, None
    while True:
        try:
            _new_connection().close()
            return
        except Exception as e:
            last = e
            if time.monotonic() >= deadline:
                pwfile = os.environ.get("A2A_DB_PASSWORD_FILE", "")
                raise SystemExit(
                    f"a2a-mcp: cannot reach MariaDB at {DB_USER}@{DB_HOST}:"
                    f"{DB_PORT}/{DB_NAME} after {timeout:g}s: {last}\n"
                    "\nThe usual causes, most likely first:\n"
                    "  - the mariadb service is not up yet, or refused to "
                    "start. `docker compose logs mariadb` — if it names an "
                    "unknown variable, that is a2a_mcp/my.cnf; comment out "
                    "its mount in docker-compose.yml to run on defaults.\n"
                    + (f"  - the password file {pwfile} is mounted but holds "
                       "something MariaDB does not accept. This happens when "
                       "the a2a-mariadb volume SURVIVES while a2a-secret is "
                       "recreated: the database keeps the password it was "
                       "initialised with, and the new file no longer matches. "
                       "Restore that volume, or drop both together.\n"
                       if pwfile else
                       "  - A2A_DB_PASSWORD is wrong for this database.\n")
                    + "  - A2A_DB_HOST/_PORT/_NAME/_USER name something else "
                    "than the running service."
                )
            time.sleep(delay)
            delay = min(delay * 2, 2.0)


# Constructing the pool opens nothing: connections are made on first use. That
# keeps `--help` instant, lets this module be imported by a test that supplies
# its own database, and means an unreachable server is reported by _startup()
# with a sentence rather than by a 60s stall during import.
CONN = _Pool(DB_POOL_SIZE)


def _has_table(name: str) -> bool:
    return CONN.execute(
        "SELECT 1 FROM information_schema.tables "
        "WHERE table_schema = %s AND table_name = %s",
        (DB_NAME, name),
    ).fetchone() is not None


def _has_col(table: str, col: str) -> bool:
    return CONN.execute(
        "SELECT 1 FROM information_schema.columns "
        "WHERE table_schema = %s AND table_name = %s AND column_name = %s",
        (DB_NAME, table, col),
    ).fetchone() is not None



def _init_schema() -> None:
    """Create the schema if it is not there. Idempotent, runs at every start.

    There is no in-place migration path here any more, and that is the point of
    the move. The sqlite build carried a stack of them — v1 station scoping,
    tokens-as-users, expiry columns, the receipts backfill — because every
    deployment upgraded its own file, in place, on boot. A MariaDB database is
    created by this DDL and filled either by use or, once, by `migrate`.

    Nothing on this path reads a sqlite file. Bringing an old one across is an
    operator action taken deliberately, not something a container start does to
    a database while nobody is looking.
    """
    CONN.script(SCHEMA)
    CONN.execute(
        "INSERT IGNORE INTO stations "
        "(station_id, name, description, created_at) "
        "VALUES (%s, %s, %s, %s)",
        (DEFAULT_STATION_ID, DEFAULT_STATION_NAME,
         "Default station", time.time()),
    )


# ---------------------------------------------------------------------------
# Logging: operational lines and the audit trail, in one table.
#
# The two were split in an earlier draft and it was the wrong shape — the
# question that actually gets asked ("what happened here, and who did it")
# crosses the boundary. `station allow` landing next to the stream error it
# explains is the whole value.
#
# Two rules hold this together, and both were learned the hard way:
#
#   NEVER RAISE. A log call is not allowed to fail its caller. If the insert
#   throws, or the pool does not exist yet, the line goes to stderr instead —
#   which docker captures on a size-capped driver. The logs explaining a
#   database fault must not need the database.
#
#   Rotation is retention. collect() deletes past A2A_LOG_RETENTION and stays
#   the only thing in this file that removes rows.
# ---------------------------------------------------------------------------

LOG_LEVELS = {"DEBUG": 10, "INFO": 20, "WARN": 30, "ERROR": 40}
LOG_LEVEL = LOG_LEVELS.get(
    (os.environ.get("A2A_LOG_LEVEL") or "INFO").upper(), 20
)
LOG_RETENTION = parse_duration(os.environ.get("A2A_LOG_RETENTION") or "30d")


def log(
    message: str,
    *,
    level: str = "INFO",
    station: str = "",
    actor: str = "",
    event: str = "",
) -> None:
    """Record one line. Never raises, whatever the database is doing."""
    if LOG_LEVELS.get(level, 20) < LOG_LEVEL:
        return
    row = (time.time(), level, (station or "")[:64], (actor or "")[:128],
           (event or "")[:64], message)
    try:
        CONN.execute(
            "INSERT INTO logs (ts, level, station, actor, event, message) "
            "VALUES (%s, %s, %s, %s, %s, %s)",
            row,
        )
    except Exception as e:
        # Deliberately broad. Anything at all — no pool yet, table missing
        # during first-boot DDL, database down — must still leave a trace.
        try:
            where = f" [{station}]" if station else ""
            who = f" {actor}" if actor else ""
            print(f"[a2a {level}]{where}{who} {event}: {message}"
                  f"  (not logged to db: {e})", file=sys.stderr, flush=True)
        except Exception:
            pass


def _sweep_logs(now: float) -> int:
    """Delete logs past the retention horizon. Called only from collect()."""
    if LOG_RETENTION <= 0:
        return 0
    try:
        return CONN.execute(
            "DELETE FROM logs WHERE ts < %s", (now - LOG_RETENTION,)
        ).rowcount
    except Exception as e:
        log(f"log retention sweep failed: {e}", level="WARN", event="collect")
        return 0




# ---------------------------------------------------------------------------
# Per-request station context.
# ---------------------------------------------------------------------------

_current_station: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "a2a_current_station", default=None
)
_current_agent: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "a2a_current_agent", default=None
)
# Why this request has no station, when it named an agent but could not be
# resolved to one (unregistered, not granted, bound elsewhere). Surfaced
# verbatim so the caller is told how to fix it instead of just being refused.
_current_denial: contextvars.ContextVar[str | None] = contextvars.ContextVar(
    "a2a_current_denial", default=None
)
_current_auth: contextvars.ContextVar[dict | None] = contextvars.ContextVar(
    "a2a_current_auth", default=None
)

REGISTER_HINT = (
    "ask an operator to create it: "
    "a2a-mcp.py agent add <id> --station <station>. Agents cannot create "
    "or delete agents, channels or stations — that is deliberate"
)


class AuthRequired(Exception):
    """Raised when a station-scoped operation has no station bound."""


def require_station() -> str:
    """The station this request acts in, resolving late if it has to.

    An MCP session binds its context once, at initialize, and every later tool
    call runs inside that same context. So an agent that registers itself
    mid-session would stay unresolved forever if we only trusted what the
    middleware bound. When no station is bound we therefore re-resolve from the
    DB using the token and agent name (both stable), and cache the result.
    """
    sid = _current_station.get()
    if sid:
        return sid
    auth, agent = _current_auth.get(), _current_agent.get()
    if auth and agent:
        try:
            sid = resolve_request_station(auth, agent)
        except AgentDenied as e:
            raise AuthRequired(f"{e}; {REGISTER_HINT}") from e
        _current_station.set(sid)
        _current_denial.set(None)
        return sid
    reason = _current_denial.get() or "this request named no agent"
    raise AuthRequired(f"{reason}; {REGISTER_HINT}")


def current_agent() -> str | None:
    """The agent this request is acting as, if it named one."""
    return _current_agent.get()


def require_auth() -> dict:
    """The authenticated token's identity: token_hash, user, granted stations."""
    auth = _current_auth.get()
    if not auth:
        raise AuthRequired("no authenticated token bound to this request")
    return auth


def _rewrite_member_lists(station_id: str, agent_id: str,
                          new_id: str | None) -> None:
    """Rewrite every JSON-in-TEXT list of agent ids that names `agent_id`.

    `new_id=None` removes it instead of replacing it — which is what deleting
    an agent needs: left in a channel's members it keeps being written into new
    messages' audiences, minting receipts that no agent exists to ack.

    LIKE only narrows the scan; the decode is what decides, so an id that
    merely contains another as a substring cannot be corrupted.
    """
    # Addressed by each table's real primary key. This used to use `rowid`,
    # which is a sqlite pseudo-column that does not exist here — and which was
    # never right anyway, since these tables have perfectly good keys.
    for table, column, keys in (("channels", "members", ("station_id", "name")),
                                ("broadcasts", "candidates", ("id",))):
        cols = ", ".join(f"`{k}`" for k in keys)
        rows = CONN.execute(
            f"SELECT {cols}, {column} FROM {table} "
            f"WHERE station_id = %s AND {column} LIKE %s",
            (station_id, f'%"{agent_id}"%'),
        ).fetchall()
        for r in rows:
            try:
                items = json.loads(r[column])
            except (TypeError, ValueError):
                continue
            if not isinstance(items, list) or agent_id not in items:
                continue
            if new_id is None:
                out = [x for x in items if x != agent_id]
            else:
                out = [new_id if x == agent_id else x for x in items]
            where = " AND ".join(f"`{k}` = %s" for k in keys)
            CONN.execute(
                f"UPDATE {table} SET {column} = %s WHERE {where}",
                (json.dumps(out), *(r[k] for k in keys)),
            )


def normalize_agent_id(raw: str) -> str:
    """`${CLAUDE_PROJECT_DIR}` arrives as a full path; use its last segment.

    A plain name (no separator) is taken verbatim, so an explicit agent id
    still works.
    """
    s = (raw or "").strip().strip('"').rstrip("/")
    if not s:
        return ""
    if "/" in s or "\\" in s:
        s = s.replace("\\", "/").rsplit("/", 1)[-1]
    return s


class AgentDenied(Exception):
    """Agent unknown, ambiguous, not granted, or bound to another token."""


# ---------------------------------------------------------------------------
# Token realm: what a user token may administer on its own — its granted
# stations and the agents living in them. Every realm operation goes through
# these two guards, so REST and the MCP tools cannot drift apart.
# ---------------------------------------------------------------------------

def realm_station(auth: dict, station: str) -> dict:
    """Resolve a station name/id, refusing anything this token isn't granted."""
    st = STATIONS.get(station)
    if not st or st["station_id"] not in (auth.get("stations") or []):
        # Same message either way: never disclose stations outside the grants.
        raise AgentDenied(f"station {station!r} is not one of your stations")
    return st


def realm_agent(auth: dict, agent_id: str) -> dict | None:
    """Find an agent inside this token's granted stations.

    Returns None when it doesn't exist there (which is also what a caller sees
    for an agent that exists only in a station they aren't granted). Raises if
    it exists but is pinned to a different token.
    """
    granted = auth.get("stations") or []
    if not granted:
        return None
    ph = ",".join(["%s"] * len(granted))
    rows = CONN.execute(
        f"SELECT station_id, owner_token_hash FROM agents "
        f"WHERE agent_id = %s AND station_id IN ({ph})",
        [agent_id, *granted],
    ).fetchall()
    if not rows:
        return None
    if len(rows) > 1:
        raise AgentDenied(
            f"agent {agent_id!r} exists in several of your stations"
        )
    row = dict(rows[0])
    owner = row["owner_token_hash"]
    if owner and owner != auth["token_hash"]:
        raise AgentDenied(f"agent {agent_id!r} is bound to another token")
    return row


def resolve_request_station(auth: dict, agent_id: str) -> str:
    """Map the agent a request names to the station it may act in.

    The token authenticates; the agent selects the tenant. An agent must exist
    (no auto-registration), must live in a station this token is granted, and
    is pinned to the first token that claims it.
    """
    granted = auth.get("stations") or []
    if not granted:
        raise AgentDenied("token has no station grants")
    ph = ",".join(["%s"] * len(granted))
    rows = CONN.execute(
        f"SELECT station_id, owner_token_hash FROM agents "
        f"WHERE agent_id = %s AND station_id IN ({ph})",
        [agent_id, *granted],
    ).fetchall()
    if not rows:
        raise AgentDenied(f"unknown agent {agent_id!r}")
    if len(rows) > 1:
        raise AgentDenied(
            f"agent {agent_id!r} exists in several granted stations"
        )
    row = rows[0]
    owner = row["owner_token_hash"]
    if owner and owner != auth["token_hash"]:
        raise AgentDenied(f"agent {agent_id!r} is bound to another token")
    if not owner:
        try:
            CONN.execute(
                "UPDATE agents SET owner_token_hash = %s "
                "WHERE station_id = %s AND agent_id = %s",
                (auth["token_hash"], row["station_id"], agent_id),
            )
        except Exception:
            pass
    return row["station_id"]


# /stream wakeups, keyed by station. post_to_channel signals every attached
# stream for that station; each stream then re-reads its OWN per-agent,
# membership-filtered view from the DB. The DB is the source of truth, so a
# missed signal never loses a message — the next read still finds it. Each
# agent keeps a private cursor, so agents sharing one token (one station) don't
# steal or starve each other's backlog.
_stream_wakers: dict[str, set[asyncio.Event]] = {}

# ONE live stream per agent — newest wins. Keyed (station_id, agent_id) to a
# claim token; the generator holding the current token serves, any other exits
# at its next tick.
#
# This exists because delivery is a destructive read with no lease: fetching
# stamps delivered_at, a live stream only fetches delivered_at IS NULL, and
# replay is one pass per connection bounded to STREAM_REPLAY_WINDOW. A client
# that dies SILENTLY (sleep, NAT drop — no FIN, so is_disconnected() stays
# false while the kernel retransmits for 15-30 minutes) therefore leaves a
# zombie generator racing the reconnected one for the same receipts, and every
# message the zombie won was written into a dead socket and never pushed
# again. The visible symptom was push "dying with time" while the client
# reported a healthy connection.
_STREAM_OWNERS: dict[tuple[str, str], str] = {}


def _wake_station(station_id: str) -> None:
    for ev in _stream_wakers.get(station_id) or ():
        ev.set()


# ---------------------------------------------------------------------------
# Helpers.
# ---------------------------------------------------------------------------

def _db(call):
    return asyncio.to_thread(call)


def _row_to_agent(row) -> dict:
    return {
        "agent_id": row["agent_id"],
        "name": row["name"],
        "description": row["description"],
        "expertise": json.loads(row["expertise"] or "[]"),
        "projects": json.loads(row["projects"] or "[]"),
        "system_prompt": row["system_prompt"],
        "metadata": json.loads(row["metadata"] or "{}"),
        "created_at": row["created_at"],
        "station_id": row["station_id"],
    }


def _row_to_channel_summary(row, message_count: int) -> dict:
    return {
        "name": row["name"],
        "theme": row["theme"],
        "members": json.loads(row["members"] or "[]"),
        "policy": json.loads(row["policy"] or "{}"),
        "messages": message_count,
        "created_at": row["created_at"],
        "station_id": row["station_id"],
    }


def _row_to_transcript(row) -> dict:
    out = {
        "id": row["id"],
        "channel": row["channel"],
        "ts": row["ts"],
        "sender": row["sender"],
        "text": row["text"],
    }
    # Carried so a client can show the reader how long it has. Absent on rows
    # from a database written before expiry existed.
    if "expires_at" in row.keys() and row["expires_at"]:
        out["expires_at"] = row["expires_at"]
    return out


def _row_to_broadcast(row, bids: list[dict] | None = None) -> dict:
    out = {
        "id": row["id"],
        "station_id": row["station_id"],
        "sender": row["sender"],
        "problem": row["problem"],
        "expertise": json.loads(row["expertise"] or "[]"),
        "projects": json.loads(row["projects"] or "[]"),
        "status": row["status"],
        "created_at": row["created_at"],
        "updated_at": row["updated_at"],
    }
    if bids is not None:
        out["bids"] = bids
    return out


def _row_to_bid(row) -> dict:
    return {
        "id": row["id"],
        "broadcast_id": row["broadcast_id"],
        "agent_id": row["agent_id"],
        "bid": row["bid"],
        "pitch": row["pitch"],
        "created_at": row["created_at"],
    }


# ---------------------------------------------------------------------------
# Message receipts — the audience, resolved once at post time.
#
# Every message gets one receipt row per agent it is meant for. That set is
# the message's audience and it is never extended: an agent joining a channel
# afterwards is in no earlier receipt, so it starts with an empty inbox
# instead of the channel's history. Delivery reads these rows (no scanning),
# and the collector keeps a message alive until every one of them is acked.
#
# The audience rules mirror what delivery used to match by scanning, so
# nothing about who-sees-what changes:
#   channel post with @mentions -> exactly the agents mentioned (even if they
#                                  are not members — a mention reaches anyone)
#   channel post without any @  -> the channel's members at post time
#   dm                          -> the recipient (self-DM included: ping_me)
#   broadcast                   -> its candidates
# minus the sender in every case; an agent is never its own audience.
# ---------------------------------------------------------------------------

RECEIPT_KINDS = ("channel", "dm", "broadcast")


def _station_agent_ids(station_id: str) -> list[str]:
    return [
        r["agent_id"] for r in CONN.execute(
            "SELECT agent_id FROM agents WHERE station_id = %s", (station_id,)
        ).fetchall()
    ]


def _channel_members(station_id: str, channel: str) -> list[str]:
    """The channel's member list. Sync — call inside _db()."""
    row = CONN.execute(
        "SELECT members FROM channels WHERE station_id = %s AND name = %s",
        (station_id, channel),
    ).fetchone()
    return json.loads(row["members"] or "[]") if row else []


def _channel_audience(
    station_id: str, channel: str, sender: str
) -> list[str]:
    """Who receives this channel message: the members, minus the sender.

    Sync — call inside _db(). Nothing else goes in. A channel post NEVER
    reaches anyone outside the channel, so there is no argument here that
    could widen it: to reach someone else, add them to the channel or send a
    DM. `addressed` is a label on top of this set and can only be a subset of
    it — it never adds anyone.

    It does NOT look at the message text, and that is the whole point. This
    used to decide delivery by scanning the body: any '@' switched the audience
    from "everyone in the channel" to "only the ids mentioned". A message
    signed `from @myself` therefore addressed nobody — the one handle in it was
    the sender's, which is excluded — so zero receipts were written and the
    post reached no one while reporting success. An email address, a docker tag
    or an `@media` rule did the same thing. Addressing is structured data now;
    prose is prose.
    """
    members = _channel_members(station_id, channel)
    return list(dict.fromkeys(a for a in members if a != sender))


def _expiry_from(expires_in, ts: float, ceiling: float | None = None) -> float:
    """When a message written at `ts` stops being worth reading.

    Unset means the station default. The sender may shorten it and may not
    lengthen it past MAX_RETENTION, nor past a channel's own retention when
    that is shorter: an operator's ceiling is not something a sender can
    raise.
    """
    seconds = parse_duration(expires_in)          # raises on nonsense
    limit = min(MAX_RETENTION, ceiling or MAX_RETENTION)
    return ts + min(seconds or limit, limit)


def _attach_addressees(station_id: str, msgs: list[dict]) -> None:
    """Set `addressed` on a page of transcript rows. Sync — call inside _db().

    One query for the whole page, not one per row: read_channel hands back up
    to 200 messages, and a per-row lookup there is 200 round trips for a field
    that is empty on most posts.
    """
    ids = [m["id"] for m in msgs if m.get("id")]
    if not ids:
        return
    ph = ",".join(["%s"] * len(ids))
    by_msg: dict[str, list[str]] = {}
    for r in CONN.execute(
        f"SELECT msg_id, agent_id FROM message_addressees "
        f"WHERE station_id = %s AND msg_id IN ({ph}) ORDER BY agent_id",
        (station_id, *ids),
    ).fetchall():
        by_msg.setdefault(r["msg_id"], []).append(r["agent_id"])
    for m in msgs:
        who = by_msg.get(m.get("id", ""))
        if who:
            m["addressed"] = who


def _write_receipts(
    station_id: str, msg_id: str, kind: str, ts: float, audience: list[str],
    expires_at: float = 0.0,
) -> int:
    """Record the audience of one message. Sync — call inside _db().

    The expiry is copied onto every receipt so the delivery query never joins
    to find it; both are written here, in one transaction, so they cannot
    disagree.
    """
    n = 0
    for agent_id in audience:
        CONN.execute(
            """INSERT IGNORE INTO message_receipts
                   (station_id, msg_id, kind, agent_id, ts, expires_at)
               VALUES (%s, %s, %s, %s, %s, %s)""",
            (station_id, msg_id, kind, agent_id, ts,
             expires_at or (ts + MAX_RETENTION)),
        )
        n += 1
    return n


def _mark_delivered(station_id: str, agent_id: str, ids: list[str]) -> None:
    """Stamp delivered_at on receipts we just handed to this agent. Delivery
    is not consumption: acked_at is untouched, so an undelivered-again message
    still cannot be collected. Sync — call inside _db()."""
    if not ids:
        return
    ph = ",".join(["%s"] * len(ids))
    CONN.execute(
        f"UPDATE message_receipts SET delivered_at = %s "
        f"WHERE station_id = %s AND agent_id = %s AND msg_id IN ({ph}) "
        f"AND delivered_at IS NULL",
        [time.time(), station_id, agent_id, *ids],
    )


def _ack_receipts(station_id: str, agent_id: str, ids: list[str]) -> int:
    """Mark messages processed by this agent. Returns how many receipts moved
    from unacked to acked (so a repeat ack reports 0, not a lie)."""
    if not ids:
        return 0
    now = time.time()
    ph = ",".join(["%s"] * len(ids))
    res = CONN.execute(
        f"UPDATE message_receipts SET acked_at = %s, "
        f"    delivered_at = COALESCE(delivered_at, %s) "
        f"WHERE station_id = %s AND agent_id = %s AND msg_id IN ({ph}) "
        f"AND acked_at IS NULL",
        [now, now, station_id, agent_id, *ids],
    )
    return max(0, res.rowcount)


SCREENABLE_KINDS = ("channel", "dm", "broadcast")

# kind -> the table its messages live in. The collector already knows this
# implicitly; naming it once lets the stats below walk all three the same way.
KIND_TABLE = {"channel": "transcripts", "dm": "dms", "broadcast": "broadcasts"}
# ...and the column holding WHEN IT WAS POSTED is not spelled the same in all
# three. A broadcast is a lifecycle rather than a message, so its clock is
# created_at. Every kind has one, which is what lets the age view below cover
# all three where the shelf-life view cannot.
KIND_TS = {"channel": "ts", "dm": "ts", "broadcast": "created_at"}
# ...but only these two carry a shelf life. A broadcast has no expires_at: the
# collector ages it by created_at instead, because it carries a lifecycle
# rather than a deadline. The shelf-life view therefore covers channel posts
# and DMs, and says so, rather than inventing a deadline for broadcasts or
# marking one by rewriting when it was created.
EXPIRING_KINDS = ("channel", "dm")

# "soon" on the shelf-life view. Everything carries a deadline (MAX_RETENTION
# unless the sender shortened it), so without a near/far split the whole
# station would sit in one bucket a year out and say nothing.
EXPIRY_NEAR = parse_duration(os.environ.get("A2A_EXPIRY_NEAR") or "7d")

# How long ago it was posted. The other two views say who is holding a message
# and when its deadline falls; neither answers "how long has this been sitting
# here", so a station can read healthy on both while a year of transcript
# quietly accumulates. Boundaries are calendar words, not a tuning parameter
# like A2A_EXPIRY_NEAR, so they are fixed: a day, a week, a month.
AGE_BOUNDS = (("age_day", 86400.0), ("age_week", 7 * 86400.0),
              ("age_month", 30 * 86400.0))

# The rows of the three views, in display order. Ack, expiry and age are
# INDEPENDENT partitions of the same messages: each covers every message
# exactly once — expiry only every message that HAS a deadline — so the groups
# each sum to a station total and never to each other. The screen says so,
# because a reader will try to add them.
ACK_SEGMENTS = ("unread", "partial", "acked", "orphan")
EXPIRY_SEGMENTS = ("no_expiry", "overdue", "near", "far")
AGE_SEGMENTS = (*(name for name, _ in AGE_BOUNDS), "age_older")
SEGMENT_LABEL = {
    "unread":    "nobody has read",
    "partial":   "partially read",
    "acked":     "fully read",
    "orphan":    "no audience",
    "no_expiry": "no expiry date",
    "overdue":   "overdue",
    "near":      f"expires within {_short_duration(EXPIRY_NEAR)}",
    "far":       "expires later",
    "age_day":   "posted in the last day",
    "age_week":  "posted 1–7 days ago",
    "age_month": "posted 7–30 days ago",
    "age_older": "posted over 30 days ago",
}


def age_window(segment: str, now: float) -> tuple[float | None, float | None]:
    """The timestamp window of one age bucket: a message is in it when
    `lo < ts <= hi`, with None meaning unbounded on that side.

    Defined once, here, rather than inline in the query that reads it, so the
    boundaries can be asserted without a database and so anything that later
    ACTS on an age row cannot drift from the numbers the operator was shown.

    Contiguous and disjoint by construction: each bucket starts where the
    previous one ends. A message dated in the future (clock skew) has no upper
    bound above it and lands in `age_day`, which is the harmless end.
    """
    edges = [now - secs for _, secs in AGE_BOUNDS]      # newest → oldest
    names = [name for name, _ in AGE_BOUNDS]
    if segment not in AGE_SEGMENTS:
        raise ValueError(f"not an age segment: {segment!r}")
    if segment == "age_older":
        return None, edges[-1]
    i = names.index(segment)
    return edges[i], (edges[i - 1] if i else None)


def _segment_ids(station_id: str, segment: str,
                 now: float | None = None) -> dict[str, list[str]]:
    """The message ids in one segment, grouped by kind.

    Every view is computed the same way — from `message_receipts` for the ack
    view, from the message tables for the shelf-life and age views — so that
    the counts the operator reads and the rows an action touches can never
    disagree. That is also why this returns ids and not counts.
    """
    now = now if now is not None else time.time()
    out: dict[str, list[str]] = {}

    for kind, table in KIND_TABLE.items():
        if segment in ACK_SEGMENTS:
            if segment == "orphan":
                # No receipt at all. `_collect_station`'s `_fully_acked` only
                # looks at messages that HAVE receipts, so these are invisible
                # to rules 1 and 2 no matter who acks what — they wait out the
                # retention window or their expiry. Nothing else reports them.
                sql = (f"SELECT m.id AS id FROM {table} m "
                       "LEFT JOIN message_receipts r "
                       "  ON r.station_id = m.station_id AND r.msg_id = m.id "
                       "WHERE m.station_id = %s AND r.msg_id IS NULL")
                params: list = [station_id]
            else:
                having = {
                    "unread":  "SUM(acked_at IS NOT NULL) = 0",
                    "partial": "SUM(acked_at IS NULL) > 0 "
                               "AND SUM(acked_at IS NOT NULL) > 0",
                    "acked":   "SUM(acked_at IS NULL) = 0",
                }[segment]
                sql = ("SELECT msg_id AS id FROM message_receipts "
                       "WHERE station_id = %s AND kind = %s "
                       f"GROUP BY msg_id HAVING {having}")
                params = [station_id, kind]
        elif segment in AGE_SEGMENTS:
            # Every kind, with no EXPIRING_KINDS exclusion: a broadcast has no
            # deadline, but it does have a birthday, so this is the one view
            # that covers the whole station.
            lo, hi = age_window(segment, now)
            col = KIND_TS[kind]
            clauses, args = [], []
            if lo is not None:
                clauses.append(f"{col} > %s")
                args.append(lo)
            if hi is not None:
                clauses.append(f"{col} <= %s")
                args.append(hi)
            sql = (f"SELECT id FROM {table} WHERE station_id = %s"
                   + "".join(f" AND {c}" for c in clauses))
            params = [station_id, *args]
        else:
            if kind not in EXPIRING_KINDS:
                out[kind] = []          # no shelf life — see EXPIRING_KINDS
                continue
            where = {
                "no_expiry": "expires_at = 0",
                "overdue":   "expires_at > 0 AND expires_at <= %s",
                "near":      "expires_at > %s AND expires_at <= %s",
                "far":       "expires_at > %s",
            }[segment]
            args = {
                "no_expiry": [],
                "overdue":   [now],
                "near":      [now, now + EXPIRY_NEAR],
                "far":       [now + EXPIRY_NEAR],
            }[segment]
            sql = f"SELECT id FROM {table} WHERE station_id = %s AND {where}"
            params = [station_id, *args]
        out[kind] = [r["id"] for r in CONN.execute(sql, params)]
    return out


def message_stats(station_id: str, now: float | None = None) -> dict:
    """What is in this station, and what is holding it.

    Sync — call inside _db().

    `doctor` names agents that are pinning things and `compact` reports what it
    removed; neither answers "why will this station not shrink". The collector
    has precise reasons (`_collect_station`), so the answer can be precise too:
    a message survives because its audience has not all acked, because it has
    no audience at all, because it is a broadcast that is not closed, or
    because the collector has not run since it became eligible.
    """
    now = now if now is not None else time.time()
    totals = {t: CONN.execute(
        f"SELECT COUNT(*) AS n FROM {t} WHERE station_id = %s", (station_id,)
    ).fetchone()["n"] for t in KIND_TABLE.values()}
    total = sum(totals.values())

    rows = []
    for group, segments in (("ack", ACK_SEGMENTS), ("expiry", EXPIRY_SEGMENTS),
                            ("age", AGE_SEGMENTS)):
        for seg in segments:
            by_kind = _segment_ids(station_id, seg, now)
            rows.append({
                "group": group,
                "segment": seg,
                "label": SEGMENT_LABEL[seg],
                "count": sum(len(v) for v in by_kind.values()),
                "by_kind": {k: len(v) for k, v in by_kind.items() if v},
            })

    # Who is holding the two rows that an agent could free by reading. Same
    # shape doctor uses, so the two surfaces agree rather than each having
    # their own idea of who is at fault.
    holders = [
        (r["agent_id"], r["n"]) for r in CONN.execute(
            "SELECT agent_id, COUNT(*) AS n FROM message_receipts "
            "WHERE station_id = %s AND acked_at IS NULL "
            "GROUP BY agent_id ORDER BY n DESC", (station_id,)
        )
    ]
    # A fully-acked broadcast still needs status='closed' before rule 2 will
    # take it, so "fully read" does not mean "about to go" for that kind.
    open_broadcasts = CONN.execute(
        "SELECT COUNT(*) AS n FROM broadcasts "
        "WHERE station_id = %s AND status != 'closed'", (station_id,)
    ).fetchone()["n"]
    return {
        "station_id": station_id, "total": total, "by_table": totals,
        "rows": rows, "holders": holders,
        "open_broadcasts": open_broadcasts, "near_seconds": EXPIRY_NEAR,
        # The ack and age views cover every message; the shelf-life view covers
        # every message that HAS a shelf life. Reported so the sums can be
        # checked rather than assumed equal.
        "ack_total": total,
        "expiry_total": total - totals["broadcasts"],
        "age_total": total,
        "broadcasts_no_shelf_life": totals["broadcasts"],
    }


def mark_segment(station_id: str, segment: str, preview: bool = False,
                 now: float | None = None) -> dict:
    """Make a segment eligible for collection. Deletes nothing itself.

    Sync — call inside _db().

    `collect()` stays the only thing in this file that removes a row, so
    "mark for deletion" means reaching for one of the two mechanisms that
    already make a message collectable:

      ack     the outstanding receipts, exactly as screen() does — for the
              rows whose messages are held by an audience that has not read
      expire  set expires_at = now, which rule 4 then removes even unacked —
              for messages no ack can ever free (no audience), for the
              shelf-life rows where expiry IS the subject, and for the age
              rows, which is the only way to say "retire what is older than a
              month" in a station whose transcript nobody can ack

    A kind with no expires_at (a broadcast ages by created_at) is reported in
    `untouched` rather than marked: rewriting when it was created would be a
    forgery, and a count that covered less than the row it came from would be
    a lie.

    Acking says HANDLED. Marking "nobody has read" destroys messages no agent
    ever saw; that is sometimes exactly right, and never something to do by
    reflex, which is why every caller states the count first.
    """
    if segment not in (*ACK_SEGMENTS, *EXPIRY_SEGMENTS, *AGE_SEGMENTS):
        raise ValueError(f"unknown segment {segment!r}")
    now = now if now is not None else time.time()
    st = STATIONS.get(station_id)
    if not st:
        raise KeyError(f"station {station_id!r} not found")
    sid = st["station_id"]

    by_kind = _segment_ids(sid, segment, now)
    n = sum(len(v) for v in by_kind.values())
    out = {"station": st["name"], "segment": segment,
           "label": SEGMENT_LABEL[segment], "found": n,
           "by_kind": {k: len(v) for k, v in by_kind.items() if v},
           "preview": preview, "acked": 0, "expired": 0, "untouched": {}}

    # "fully read" has nothing to mark: it is already collectable, and the
    # only thing standing between it and deletion is the collector running.
    use_ack = segment in ("unread", "partial")

    # A broadcast has no expires_at — it ages by created_at, and backdating
    # that would be forging when it was created. So the expire path skips it
    # and SAYS SO, in the preview as well as afterwards, rather than reporting
    # a number that quietly covered less than the row the operator pressed on.
    # This also matters for `no audience`, which has always used expire and
    # would have thrown "unknown column" on a receiptless broadcast.
    if not use_ack and segment != "acked":
        for kind in list(by_kind):
            if kind not in EXPIRING_KINDS and by_kind[kind]:
                out["untouched"][kind] = len(by_kind.pop(kind))

    if preview or not n:
        return out

    for kind, ids in by_kind.items():
        for i in range(0, len(ids), 400):
            chunk = ids[i:i + 400]
            ph = ",".join(["%s"] * len(chunk))
            if use_ack:
                out["acked"] += max(0, CONN.execute(
                    f"UPDATE message_receipts SET acked_at = %s, "
                    f"    delivered_at = COALESCE(delivered_at, %s) "
                    f"WHERE station_id = %s AND msg_id IN ({ph}) "
                    f"  AND acked_at IS NULL",
                    [now, now, sid, *chunk],
                ).rowcount)
            elif segment != "acked":
                out["expired"] += max(0, CONN.execute(
                    f"UPDATE {KIND_TABLE[kind]} SET expires_at = %s "
                    f"WHERE station_id = %s AND id IN ({ph})",
                    [now, sid, *chunk],
                ).rowcount)

    out["collected"] = _collect_station(sid, now)
    out["open_broadcasts"] = CONN.execute(
        "SELECT COUNT(*) AS n FROM broadcasts "
        "WHERE station_id = %s AND status != 'closed'", (sid,)
    ).fetchone()["n"]
    log(f"marked {segment!r} ({n} message(s)) for collection: {out['collected']}",
        event="messages.mark", station=st["name"], level="WARN")
    return out


def screen(station_id: str, agent_id: str | None = None,
           kinds: tuple[str, ...] = SCREENABLE_KINDS,
           preview: bool = False) -> dict:
    """Declare a backlog handled: ack every unacked receipt, delete nothing.

    Sync — call inside _db().

    Nothing here deletes a message while an agent it was addressed to has not
    acked it. That guarantee is load-bearing and has one failure mode: an
    agent that stops acking pins its share of the station forever, including
    for everyone else, since a channel transcript is only collected once its
    WHOLE audience has acked. This is the way out — and deliberately not a
    delete. It moves `acked_at`, and `collect()` then removes whatever has
    become fully handled, through the same rules as always. `collect()` stays
    the only thing in this system that removes rows.

    `agent_id` narrows it to one wedged inbox instead of the whole station.
    `preview` counts without changing anything, so an irreversible action can
    state its size before it happens.

    Broadcasts are included but behave differently, and callers should say so:
    deleting one needs `status = 'closed'` as well as fully-acked, so
    screening silences an open help-wanted request in every candidate's inbox
    without destroying the board.
    """
    kinds = tuple(k for k in kinds if k in SCREENABLE_KINDS)
    if not kinds:
        return {"acked": 0, "by_kind": {}, "agents": 0}
    st = STATIONS.get(station_id)
    if not st:
        raise KeyError(f"station {station_id!r} not found")
    sid = st["station_id"]
    ph = ",".join(["%s"] * len(kinds))
    where = (f"station_id = %s AND acked_at IS NULL AND kind IN ({ph})")
    params: list = [sid, *kinds]
    if agent_id:
        where += " AND agent_id = %s"
        params.append(agent_id)

    by_kind = {
        r["kind"]: r["n"] for r in CONN.execute(
            f"SELECT kind, COUNT(*) AS n FROM message_receipts "
            f"WHERE {where} GROUP BY kind", params
        ).fetchall()
    }
    agents = CONN.execute(
        f"SELECT COUNT(DISTINCT agent_id) AS n FROM message_receipts "
        f"WHERE {where}", params
    ).fetchone()["n"]
    out = {"station": st["name"], "agent": agent_id,
           "acked": sum(by_kind.values()), "by_kind": by_kind,
           "agents": agents, "preview": preview}
    if preview or not out["acked"]:
        return out

    now = time.time()
    # Same UPDATE as _ack_receipts, minus the id list: acked, and delivered
    # if it never was, so the two timestamps cannot disagree afterwards.
    res = CONN.execute(
        f"UPDATE message_receipts SET acked_at = %s, "
        f"    delivered_at = COALESCE(delivered_at, %s) WHERE {where}",
        [now, now, *params],
    )
    out["acked"] = max(0, res.rowcount)
    # An open broadcast survives collection by design; naming the count lets
    # the caller say that rather than leaving an operator wondering why the
    # board is still there.
    out["open_broadcasts"] = CONN.execute(
        "SELECT COUNT(*) AS n FROM broadcasts "
        "WHERE station_id = %s AND status != 'closed'", (sid,)
    ).fetchone()["n"]
    # Worth an audit line rather than a debug one: screening acks on behalf of
    # agents that never read the messages, so "who declared this handled" is a
    # question somebody will eventually ask.
    log(f"screened {out['acked']} receipt(s) across {out['agents']} agent(s): "
        f"{by_kind}", event="screen", station=st["name"],
        actor=agent_id or "", level="WARN")
    return out


def _mark_read(station_id: str, agent_id: str, ids: list[str]) -> int:
    """Handing a message to its recipient counts as handling it: delivered AND
    acked, in one step. Sync — call inside _db().

    Acking is the only thing that lets a message be collected, and leaving it
    to the agent to remember meant nothing was ever collected: an inbox that
    only grows, and a channel transcript nobody can retire because one member
    read a message months ago and never said so. Reading it IS saying so.

    The push path acks from the client, which knows the message reached the
    session. This is the pull half of the same rule.
    """
    if not agent_id or not ids:
        return 0
    _mark_delivered(station_id, agent_id, ids)
    return _ack_receipts(station_id, agent_id, ids)


def _pending_rows(station_id: str, agent_id: str, limit: int) -> list:
    """This agent's unacked receipts, oldest first."""
    return CONN.execute(
        "SELECT msg_id, kind, ts, delivered_at FROM message_receipts "
        "WHERE station_id = %s AND agent_id = %s AND acked_at IS NULL "
        # Expired: acting on it now would be acting on a decision already
        # taken. It is not late, it is wrong.
        "AND expires_at > %s "
        "ORDER BY ts ASC LIMIT %s",
        (station_id, agent_id, time.time(), limit),
    ).fetchall()


# ---------------------------------------------------------------------------
# The collector.
#
# DMs and broadcasts are queues: they exist to be handled, so they die once
# everyone they were addressed to has acked them. A channel transcript is a
# record rather than a queue — several agents read it, agents come and go, and
# "everyone acked" would rarely be true — so channels are cleaned by AGE
# instead, per channel via policy.retention_days.
#
# One escape hatch, and only one: nothing outlives MAX_RETENTION_DAYS. An
# agent that never comes back would otherwise pin its messages forever. That
# ceiling is the single place where a message can be removed unacked, and
# `doctor` names the agents heading towards it.
# ---------------------------------------------------------------------------

def _collect_station(station_id: str, now: float | None = None) -> dict:
    """Delete what is finished. Sync — call inside _db()."""
    now = now or time.time()
    stats = {"dms": 0, "broadcasts": 0, "transcripts": 0,
             "expired": 0, "expired_broadcasts": 0, "receipts": 0}

    def _fully_acked(kind: str) -> list[str]:
        # Messages of this kind with at least one receipt and none unacked.
        return [
            r["msg_id"] for r in CONN.execute(
                "SELECT msg_id FROM message_receipts "
                "WHERE station_id = %s AND kind = %s "
                "GROUP BY msg_id HAVING SUM(acked_at IS NULL) = 0",
                (station_id, kind),
            ).fetchall()
        ]

    def _delete(table: str, ids: list[str]) -> int:
        n = 0
        for i in range(0, len(ids), 400):
            chunk = ids[i:i + 400]
            ph = ",".join(["%s"] * len(chunk))
            n += max(0, CONN.execute(
                f"DELETE FROM {table} WHERE station_id = %s AND id IN ({ph})",
                [station_id, *chunk],
            ).rowcount)
            CONN.execute(
                f"DELETE FROM message_receipts "
                f"WHERE station_id = %s AND msg_id IN ({ph})",
                [station_id, *chunk],
            )
        return n

    # 1 + 2. Handled queues: everyone they were addressed to has acked.
    stats["dms"] = _delete("dms", _fully_acked("dm"))
    # Channel posts too. "Kept until everyone it was addressed to has acked"
    # is the whole ephemerality guarantee, and applying it to DMs but not to
    # channel messages left every transcript pinned for the full retention
    # window even when the entire audience had read it. A post with no
    # audience has no receipts, so it is not in _fully_acked at all and still
    # ages out by rule 3 — nothing is deleted before it reaches anyone.
    stats["transcripts"] += _delete("transcripts", _fully_acked("channel"))
    done = set(_fully_acked("broadcast"))
    stats["broadcasts"] = _delete("broadcasts", [
        r["id"] for r in CONN.execute(
            "SELECT id FROM broadcasts "
            "WHERE station_id = %s AND status = 'closed'", (station_id,)
        ).fetchall()
        if r["id"] in done
    ])

    # 3. Channel transcripts, by age. A channel with no policy uses the server
    #    ceiling, so every channel is bounded even if nobody configures one.
    #    Receipts of the deleted rows are swept by rule 5 below.
    for ch in CONN.execute(
        "SELECT name, policy FROM channels WHERE station_id = %s",
        (station_id,),
    ).fetchall():
        try:
            days = float(
                (json.loads(ch["policy"] or "{}") or {}).get("retention_days")
                or MAX_RETENTION_DAYS
            )
        except (TypeError, ValueError):
            days = MAX_RETENTION_DAYS
        stats["transcripts"] += max(0, CONN.execute(
            "DELETE FROM transcripts "
            "WHERE station_id = %s AND channel = %s AND ts < %s",
            (station_id, ch["name"], now - days * 86400.0),
        ).rowcount)

    # 4. Expiry: the ONLY rule that removes something unacked, and the only
    #    reason an abandoned agent cannot pin a queue forever. Every message
    #    carries a deadline — MAX_RETENTION unless its sender shortened it —
    #    so this replaces the old global "nothing outlives 365 days" sweep
    #    with the same thing expressed per message.
    stats["expired"] = 0
    for table in ("dms", "transcripts"):
        stats["expired"] += max(0, CONN.execute(
            f"DELETE FROM {table} WHERE station_id = %s AND expires_at <= %s",
            (station_id, now),
        ).rowcount)
    # Broadcasts still age out: they carry a lifecycle, not a shelf life.
    stats["expired_broadcasts"] = max(0, CONN.execute(
        "DELETE FROM broadcasts WHERE station_id = %s AND created_at < %s",
        (station_id, now - MAX_RETENTION),
    ).rowcount)

    # 5. Orphans: a receipt whose message is gone. No FK can express this
    #    (three possible parents), so the collector owns the invariant.
    stats["receipts"] = max(0, CONN.execute(
        """DELETE FROM message_receipts
            WHERE station_id = %s
              AND (   (kind = 'channel'   AND msg_id NOT IN
                          (SELECT id FROM transcripts WHERE station_id = %s))
                   OR (kind = 'dm'        AND msg_id NOT IN
                          (SELECT id FROM dms WHERE station_id = %s))
                   OR (kind = 'broadcast' AND msg_id NOT IN
                          (SELECT id FROM broadcasts WHERE station_id = %s)))""",
        (station_id, station_id, station_id, station_id),
    ).rowcount)
    return stats


def collect(station_id: str | None = None) -> dict:
    """Run the collector over one station or all of them. Sync."""
    ids = (
        [station_id] if station_id
        else [r["station_id"] for r in CONN.execute(
            "SELECT station_id FROM stations"
        ).fetchall()]
    )
    total: dict = {}
    now = time.time()
    for sid in ids:
        for k, v in _collect_station(sid, now).items():
            total[k] = total.get(k, 0) + v
        # Names nobody approved. Swept here rather than anywhere closer to
        # the proposal code so that this function stays the only thing that
        # deletes — the property the whole ephemerality argument rests on —
        # and per station, so collecting one never reaches into another.
        total["proposals_expired"] = (
            total.get("proposals_expired", 0) + PROPOSALS.sweep(sid, now)
        )
        # Denial locks whose time is up, for the same reason and in the same
        # place: a lapsed lock stops one client asking for nothing.
        total["denials_expired"] = (
            total.get("denials_expired", 0)
            + PROPOSALS.sweep_denials(sid, now)
        )
    # Log retention rides here for the same reason: collect() is the only
    # thing that deletes, and that stays true now that logs are rows too.
    # Once per run, not per station — the logs table is not station-scoped.
    total["logs_expired"] = _sweep_logs(now)
    if any(total.values()):
        log(f"collected {total}", event="collect",
            station=station_id or "", level="DEBUG")
    return total


_last_collect = 0.0


def _maybe_collect(station_id: str) -> dict | None:
    """Debounced collection off the stream tick. Cheap when there is nothing
    to do, and never more often than COLLECT_INTERVAL per process."""
    global _last_collect
    now = time.time()
    if now - _last_collect < COLLECT_INTERVAL:
        return None
    _last_collect = now
    return _collect_station(station_id, now)


# ---------------------------------------------------------------------------
# Station + token registries.
# ---------------------------------------------------------------------------

TOKEN_PREFIX_LITERAL = "a2a_st_"


def _new_token() -> str:
    return TOKEN_PREFIX_LITERAL + secrets.token_urlsafe(32)


def _hash_token(token: str) -> str:
    return hashlib.sha256(token.encode("utf-8")).hexdigest()


def _display_prefix(token: str) -> str:
    return token[-8:]


class StationRegistry:
    def create(self, name: str, description: str = "") -> dict:
        name = (name or "").strip()
        if not name:
            raise ValueError("station name required")
        station_id = str(uuid.uuid4())
        now = time.time()
        try:
            with CONN:
                CONN.execute(
                    """INSERT INTO stations (station_id, name, description,
                           created_at)
                       VALUES (%s, %s, %s, %s)""",
                    (station_id, name, description or "", now),
                )
        except pymysql.err.IntegrityError as e:
            # uq_stations_name. The driver's exception type changed with the
            # database; catching sqlite3's here would have let a duplicate name
            # escape as a raw driver error instead of this message.
            raise ValueError(f"station name {name!r} already exists") from e
        log(f"station {name!r} created", event="station.create", station=name)
        return {
            "station_id": station_id,
            "name": name,
            "description": description or "",
            "created_at": now,
        }

    def get(self, id_or_name: str) -> dict | None:
        row = CONN.execute(
            "SELECT * FROM stations WHERE station_id = %s OR name = %s",
            (id_or_name, id_or_name),
        ).fetchone()
        return dict(row) if row else None

    def list(self) -> list[dict]:
        rows = CONN.execute(
            "SELECT * FROM stations ORDER BY created_at"
        ).fetchall()
        return [dict(r) for r in rows]

    # --- allow list: who may act in this station (server-side only) --------

    def allow(self, id_or_name: str, token_or_prefix: str) -> bool:
        """Put a token on this station's allow list."""
        st = self.get(id_or_name)
        if not st:
            raise KeyError(f"station {id_or_name!r} not found")
        h = TOKENS._hash_of(token_or_prefix)
        if not h:
            raise KeyError(f"token {token_or_prefix!r} not found")
        with CONN:
            CONN.execute(
                "INSERT IGNORE INTO token_grants (token_hash, station_id) "
                "VALUES (%s, %s)",
                (h, st["station_id"]),
            )
        log(f"token {token_or_prefix} allowed into {st['name']!r}",
            event="station.allow", station=st["name"], actor=token_or_prefix)
        return True

    def disallow(self, id_or_name: str, token_or_prefix: str) -> int:
        st = self.get(id_or_name)
        if not st:
            raise KeyError(f"station {id_or_name!r} not found")
        h = TOKENS._hash_of(token_or_prefix)
        if not h:
            raise KeyError(f"token {token_or_prefix!r} not found")
        with CONN:
            cur = CONN.execute(
                "DELETE FROM token_grants WHERE token_hash = %s "
                "AND station_id = %s",
                (h, st["station_id"]),
            )
        log(f"token {token_or_prefix} removed from {st['name']!r}",
            event="station.disallow", station=st["name"],
            actor=token_or_prefix)
        return cur.rowcount

    def set_open(self, id_or_name: str, is_open: bool) -> dict:
        """Open ('*': every token allowed) or close a station."""
        st = self.get(id_or_name)
        if not st:
            raise KeyError(f"station {id_or_name!r} not found")
        with CONN:
            CONN.execute(
                "UPDATE stations SET open = %s WHERE station_id = %s",
                (1 if is_open else 0, st["station_id"]),
            )
        log(f"station {st['name']!r} "
            f"{'opened to every token' if is_open else 'closed'}",
            event="station.open" if is_open else "station.close",
            station=st["name"], level="WARN" if is_open else "INFO")
        return {"station": st["name"], "open": bool(is_open)}

    def allowed(self, id_or_name: str) -> dict:
        """This station's allow list: '*' when open, else the listed tokens."""
        st = self.get(id_or_name)
        if not st:
            raise KeyError(f"station {id_or_name!r} not found")
        rows = CONN.execute(
            "SELECT t.prefix, t.user, t.label, t.revoked_at FROM token_grants g "
            "JOIN tokens t ON t.token_hash = g.token_hash "
            "WHERE g.station_id = %s ORDER BY t.created_at",
            (st["station_id"],),
        ).fetchall()
        return {
            "station": st["name"],
            "station_id": st["station_id"],
            "open": bool(st.get("open")),
            "tokens": [dict(r) for r in rows],
        }

    def delete(self, id_or_name: str) -> bool:
        st = self.get(id_or_name)
        if not st:
            return False
        if st["station_id"] == DEFAULT_STATION_ID:
            raise ValueError("the 'default' station cannot be deleted")
        sid = st["station_id"]
        # Explicit cleanup so this works even when ON DELETE CASCADE isn't
        # present (mid-migration databases). Tokens are USER credentials, so a
        # station deletion drops its grants and clears it as a default — it
        # never deletes the token itself.
        with CONN:
            for tbl in (
                "md_files", "bids", "broadcasts", "message_addressees",
                "transcripts", "dms",
                "channels", "agents", "stream_cursors", "token_grants",
            ):
                CONN.execute(
                    f"DELETE FROM {tbl} WHERE station_id = %s", (sid,)
                )
            cur = CONN.execute(
                "DELETE FROM stations WHERE station_id = %s", (sid,)
            )
        log(f"station {st['name']!r} DELETED with all its data",
            event="station.delete", station=st["name"], level="WARN")
        return cur.rowcount > 0


STATIONS = StationRegistry()


class TokenRegistry:
    def create(self, label: str = "", user: str = "") -> dict:
        """Mint a bare user token.

        It can reach nothing until an admin puts it on a station's allow list
        (`station allow <station> --token <prefix>`) or a station is opened.
        """
        token = _new_token()
        h = _hash_token(token)
        prefix = _display_prefix(token)
        now = time.time()
        with CONN:
            CONN.execute(
                """INSERT INTO tokens (token_hash, user, label, prefix,
                       created_at)
                   VALUES (%s, %s, %s, %s, %s)""",
                (h, user or "", label or "", prefix, now),
            )
        log(f"token {prefix} minted for user {user or '-'!r}",
            event="token.create", actor=prefix)
        return {
            "token": token,
            "prefix": prefix,
            "user": user or "",
            "label": label or "",
            "stations": [],
            "created_at": now,
        }

    @staticmethod
    def _hash_of(token_or_prefix: str) -> str | None:
        """Accept a full token or an 8-char prefix; return its hash."""
        s = (token_or_prefix or "").strip()
        if not s:
            return None
        if s.startswith(TOKEN_PREFIX_LITERAL):
            return _hash_token(s)
        row = CONN.execute(
            "SELECT token_hash FROM tokens WHERE prefix = %s", (s,)
        ).fetchone()
        return row["token_hash"] if row else None

    def grants(self, token_hash: str) -> list[str]:
        """Stations this token may act in: its allow-list entries plus every
        open station. Single chokepoint — everything downstream (auth checks,
        realm view, stream revalidation) reads what this returns, so open
        stations behave exactly like an explicit entry everywhere."""
        return [
            r["station_id"] for r in CONN.execute(
                "SELECT station_id FROM token_grants WHERE token_hash = %s "
                "UNION SELECT station_id FROM stations WHERE open = 1",
                (token_hash,),
            ).fetchall()
        ]

    def list(
        self,
        station_id: str | None = None,
        include_revoked: bool = False,
    ) -> list[dict]:
        sql = (
            "SELECT t.token_hash, t.user, t.label, t.prefix, "
            "t.created_at, t.revoked_at, t.last_used_at FROM tokens t"
        )
        where: list[str] = []
        params: list = []
        if station_id:
            st = STATIONS.get(station_id)
            if not st:
                raise KeyError(f"station {station_id!r} not found")
            where.append(
                "t.token_hash IN (SELECT token_hash FROM token_grants "
                "WHERE station_id = %s)"
            )
            params.append(st["station_id"])
        if not include_revoked:
            where.append("t.revoked_at IS NULL")
        if where:
            sql += " WHERE " + " AND ".join(where)
        sql += " ORDER BY t.created_at DESC"
        rows = [dict(r) for r in CONN.execute(sql, params).fetchall()]
        names = {
            s["station_id"]: s["name"] for s in STATIONS.list()
        }
        for r in rows:
            r["stations"] = [
                names.get(g, g) for g in self.grants(r["token_hash"])
            ]
        return rows

    def delete(self, token_or_prefix: str) -> int:
        """Hard-delete a token and its grants (revoke only marks it)."""
        h = self._hash_of(token_or_prefix)
        if not h:
            return 0
        with CONN:
            CONN.execute("DELETE FROM token_grants WHERE token_hash = %s", (h,))
            CONN.execute(
                "UPDATE agents SET owner_token_hash = NULL "
                "WHERE owner_token_hash = %s", (h,)
            )
            cur = CONN.execute(
                "DELETE FROM tokens WHERE token_hash = %s", (h,)
            )
        return cur.rowcount

    def purge(
        self, revoked_only: bool = False, station_id: str | None = None
    ) -> int:
        """Bulk-delete tokens. Returns how many rows went."""
        sql = "SELECT token_hash FROM tokens"
        where: list[str] = []
        params: list = []
        if revoked_only:
            where.append("revoked_at IS NOT NULL")
        if station_id:
            st = STATIONS.get(station_id)
            if not st:
                raise KeyError(f"station {station_id!r} not found")
            where.append(
                "token_hash IN (SELECT token_hash FROM token_grants "
                "WHERE station_id = %s)"
            )
            params.append(st["station_id"])
        if where:
            sql += " WHERE " + " AND ".join(where)
        hashes = [r["token_hash"] for r in CONN.execute(sql, params).fetchall()]
        n = 0
        for h in hashes:
            with CONN:
                CONN.execute(
                    "DELETE FROM token_grants WHERE token_hash = %s", (h,)
                )
                CONN.execute(
                    "UPDATE agents SET owner_token_hash = NULL "
                    "WHERE owner_token_hash = %s", (h,)
                )
                n += CONN.execute(
                    "DELETE FROM tokens WHERE token_hash = %s", (h,)
                ).rowcount
        return n

    def revoke(self, token_or_prefix: str) -> int:
        s = (token_or_prefix or "").strip()
        if not s:
            return 0
        now = time.time()
        if s.startswith(TOKEN_PREFIX_LITERAL):
            h = _hash_token(s)
            with CONN:
                cur = CONN.execute(
                    "UPDATE tokens SET revoked_at = %s "
                    "WHERE token_hash = %s AND revoked_at IS NULL",
                    (now, h),
                )
            if cur.rowcount:
                log("token revoked", event="token.revoke", level="WARN",
                    actor=_display_prefix(s))
            return cur.rowcount
        with CONN:
            cur = CONN.execute(
                "UPDATE tokens SET revoked_at = %s "
                "WHERE prefix = %s AND revoked_at IS NULL",
                (now, s),
            )
        if cur.rowcount:
            log("token revoked", event="token.revoke", level="WARN", actor=s)
        return cur.rowcount

    def resolve(self, token: str) -> dict | None:
        """Authenticate a token -> {token_hash, user, stations}.

        Returns None for unknown or revoked tokens. Station selection happens
        later, from the agent the request names (see resolve_request_station);
        a token by itself never implies a station.
        """
        if not token:
            return None
        h = _hash_token(token)
        row = CONN.execute(
            "SELECT user, revoked_at FROM tokens WHERE token_hash = %s",
            (h,),
        ).fetchone()
        if not row or row["revoked_at"] is not None:
            return None
        try:
            CONN.execute(
                "UPDATE tokens SET last_used_at = %s WHERE token_hash = %s",
                (time.time(), h),
            )
        except Exception:
            pass
        return {
            "token_hash": h,
            "user": row["user"],
            "stations": self.grants(h),
        }


TOKENS = TokenRegistry()


# ---------------------------------------------------------------------------
# Agent registry: just profile CRUD. `metadata` is opaque JSON the server
# never inspects.
# ---------------------------------------------------------------------------

class AgentRegistry:
    PROFILE_FIELDS = (
        "name",
        "description",
        "expertise",
        "projects",
        "system_prompt",
        "metadata",
    )

    async def create(
        self,
        station_id: str,
        agent_id: str,
        name: str,
        description: str = "",
        expertise: list[str] | None = None,
        projects: list[str] | None = None,
        system_prompt: str = "",
        metadata: dict | None = None,
    ) -> dict:
        created_at = time.time()

        def _do() -> dict:
            existing = CONN.execute(
                "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
                (station_id, agent_id),
            ).fetchone()
            if existing:
                raise ValueError(f"agent {agent_id!r} already exists")
            CONN.execute(
                """INSERT INTO agents (station_id, agent_id, name,
                       description, expertise, projects, system_prompt,
                       metadata, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (
                    station_id,
                    agent_id,
                    name or agent_id,
                    description or "",
                    json.dumps(list(expertise or [])),
                    json.dumps(list(projects or [])),
                    system_prompt or "",
                    json.dumps(dict(metadata or {})),
                    created_at,
                ),
            )
            row = CONN.execute(
                "SELECT * FROM agents WHERE station_id = %s AND agent_id = %s",
                (station_id, agent_id),
            ).fetchone()
            return _row_to_agent(row)

        return await _db(_do)

    async def update(
        self, station_id: str, agent_id: str, **fields
    ) -> dict:
        def _do() -> dict:
            row = CONN.execute(
                "SELECT * FROM agents WHERE station_id = %s AND agent_id = %s",
                (station_id, agent_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"agent {agent_id!r} not found")
            sets, params = [], []
            for k, v in fields.items():
                if k not in self.PROFILE_FIELDS or v is None:
                    continue
                if k in ("expertise", "projects"):
                    sets.append(f"{k} = %s")
                    params.append(json.dumps(list(v)))
                elif k == "metadata":
                    sets.append(f"{k} = %s")
                    params.append(json.dumps(dict(v)))
                else:
                    sets.append(f"{k} = %s")
                    params.append(v)
            if sets:
                params.extend([station_id, agent_id])
                CONN.execute(
                    f"UPDATE agents SET {', '.join(sets)} "
                    f"WHERE station_id = %s AND agent_id = %s",
                    params,
                )
            row = CONN.execute(
                "SELECT * FROM agents WHERE station_id = %s AND agent_id = %s",
                (station_id, agent_id),
            ).fetchone()
            return _row_to_agent(row)

        return await _db(_do)

    def get(self, station_id: str, agent_id: str) -> dict | None:
        row = CONN.execute(
            "SELECT * FROM agents WHERE station_id = %s AND agent_id = %s",
            (station_id, agent_id),
        ).fetchone()
        return _row_to_agent(row) if row else None

    # --- admin-side helpers (station-crossing; used by the CLI/admin API) ---

    def add(self, station_id: str, agent_id: str, name: str = "",
            owner_token_hash: str | None = None) -> dict:
        """Create an agent in a station, optionally pre-bound to a token."""
        st = STATIONS.get(station_id)
        if not st:
            raise KeyError(f"station {station_id!r} not found")
        sid = st["station_id"]
        existing = CONN.execute(
            "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
            (sid, agent_id),
        ).fetchone()
        if existing:
            raise ValueError(f"agent {agent_id!r} already exists in {st['name']}")
        with CONN:
            CONN.execute(
                """INSERT INTO agents (station_id, agent_id, name, created_at,
                       owner_token_hash)
                   VALUES (%s, %s, %s, %s, %s)""",
                (sid, agent_id, name or agent_id, time.time(), owner_token_hash),
            )
        log(f"agent {agent_id!r} added", event="agent.add",
            station=st["name"], actor=agent_id)
        return {"agent_id": agent_id, "station_id": sid,
                "station_name": st["name"]}

    def list_all(self, station_id: str | None = None) -> list[dict]:
        sql = (
            "SELECT a.agent_id, a.station_id, s.name AS station_name, "
            "a.owner_token_hash, t.prefix AS owner_prefix, t.user AS owner_user, "
            "a.created_at FROM agents a "
            "LEFT JOIN stations s ON s.station_id = a.station_id "
            "LEFT JOIN tokens t ON t.token_hash = a.owner_token_hash"
        )
        params: list = []
        if station_id:
            st = STATIONS.get(station_id)
            if not st:
                raise KeyError(f"station {station_id!r} not found")
            sql += " WHERE a.station_id = %s"
            params.append(st["station_id"])
        sql += " ORDER BY s.name, a.agent_id"
        return [dict(r) for r in CONN.execute(sql, params).fetchall()]

    def move(
        self, agent_id: str, station_id: str, from_station: str | None = None
    ) -> bool:
        """Move an agent (and its stream cursor) to another station.

        `from_station` disambiguates when the same agent id exists in several
        stations — callers that already know which row they mean (the TUI, which
        lists one row per station) pass it and never hit the ambiguity error.
        """
        st = STATIONS.get(station_id)
        if not st:
            raise KeyError(f"station {station_id!r} not found")
        if from_station:
            src = STATIONS.get(from_station)
            if not src:
                raise KeyError(f"station {from_station!r} not found")
            rows = CONN.execute(
                "SELECT station_id FROM agents "
                "WHERE agent_id = %s AND station_id = %s",
                (agent_id, src["station_id"]),
            ).fetchall()
        else:
            rows = CONN.execute(
                "SELECT station_id FROM agents WHERE agent_id = %s", (agent_id,)
            ).fetchall()
        if not rows:
            return False
        if len(rows) > 1:
            raise ValueError(
                f"agent {agent_id!r} exists in several stations; "
                f"say which one to move"
            )
        old = rows[0]["station_id"]
        if old == st["station_id"]:
            return True
        clash = CONN.execute(
            "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
            (st["station_id"], agent_id),
        ).fetchone()
        if clash:
            raise ValueError(
                f"{st['name']} already has an agent {agent_id!r}; "
                f"remove one of the two first"
            )
        with CONN:
            CONN.execute(
                "UPDATE agents SET station_id = %s WHERE station_id = %s "
                "AND agent_id = %s", (st["station_id"], old, agent_id)
            )
            CONN.execute(
                "DELETE FROM stream_cursors WHERE station_id = %s AND agent_id = %s",
                (old, agent_id),
            )
        return True

    def bind(
        self, agent_id: str, token_or_prefix: str | None,
        station_id: str | None = None,
    ) -> int:
        """Pin an agent to a token, or unpin it when passed None.

        Without `station_id` this touches every copy of the id; pass it to bind
        just the one in that station (what the TUI does, since its rows are
        per-station).
        """
        h = TOKENS._hash_of(token_or_prefix) if token_or_prefix else None
        if token_or_prefix and not h:
            raise KeyError(f"token {token_or_prefix!r} not found")
        return self._bind_hash(agent_id, h, station_id)

    def _bind_hash(
        self, agent_id: str, token_hash: str | None,
        station_id: str | None = None,
    ) -> int:
        """The UPDATE behind `bind`, for callers that already hold the hash.

        Approving a transfer request is one: the proposal row carries the
        requesting token's hash and never its prefix, so routing it back
        through `bind` would mean un-hashing something that cannot be
        un-hashed. Same SQL and the same CHANGED-rows return either way, so
        the two paths cannot drift.
        """
        sql = "UPDATE agents SET owner_token_hash = %s WHERE agent_id = %s"
        params: list = [token_hash, agent_id]
        if station_id:
            st = STATIONS.get(station_id)
            if not st:
                raise KeyError(f"station {station_id!r} not found")
            sql += " AND station_id = %s"
            params.append(st["station_id"])
        with CONN:
            cur = CONN.execute(sql, params)
        return cur.rowcount

    def bind_all(
        self, token_or_prefix: str | None, station_id: str | None = None
    ) -> int:
        """Pin (or with None, unpin) every agent, optionally in one station."""
        h = TOKENS._hash_of(token_or_prefix) if token_or_prefix else None
        if token_or_prefix and not h:
            raise KeyError(f"token {token_or_prefix!r} not found")
        sql = "UPDATE agents SET owner_token_hash = %s"
        params: list = [h]
        if station_id:
            st = STATIONS.get(station_id)
            if not st:
                raise KeyError(f"station {station_id!r} not found")
            sql += " WHERE station_id = %s"
            params.append(st["station_id"])
        with CONN:
            cur = CONN.execute(sql, params)
        return cur.rowcount

    # --- token-realm operations (shared by /me REST and the MCP tools) ------

    def realm_view(self, auth: dict, agent_id: str | None) -> dict:
        """Everything this token may see about itself and its agents."""
        names = {s["station_id"]: s["name"] for s in STATIONS.list()}
        granted = [
            names.get(g, g) for g in (auth.get("stations") or [])
        ]
        mine = []
        for g in auth.get("stations") or []:
            for r in self.list_all(g):
                mine.append({
                    "agent_id": r["agent_id"],
                    "station": r.get("station_name") or r["station_id"],
                    "bound_to_me": r.get("owner_token_hash") == auth["token_hash"],
                    "bound": bool(r.get("owner_token_hash")),
                })
        here = None
        if agent_id:
            match = [a for a in mine if a["agent_id"] == agent_id]
            here = match[0] if match else None
        return {
            "user": auth.get("user") or "",
            "stations": granted,
            "agent": agent_id,
            "registered": here is not None,
            "this_agent": here,
            "agents": mine,
        }

    def realm_register(
        self, auth: dict, agent_id: str, station: str, bind: bool = True
    ) -> dict:
        """Create an agent in one of this token's stations and claim it."""
        if not agent_id:
            raise ValueError("no agent id: this request named no agent")
        st = realm_station(auth, station)
        existing = realm_agent(auth, agent_id)      # raises if owned elsewhere
        if existing:
            raise ValueError(
                f"agent {agent_id!r} already exists in "
                f"{existing['station_id']}"
            )
        row = self.add(
            st["station_id"], agent_id,
            owner_token_hash=auth["token_hash"] if bind else None,
        )
        return row

    def realm_bind(self, auth: dict, agent_id: str, bind: bool) -> dict:
        row = realm_agent(auth, agent_id)
        if not row:
            raise KeyError(f"agent {agent_id!r} is not one of your agents")
        self.bind(agent_id, None)
        if bind:
            with CONN:
                CONN.execute(
                    "UPDATE agents SET owner_token_hash = %s "
                    "WHERE station_id = %s AND agent_id = %s",
                    (auth["token_hash"], row["station_id"], agent_id),
                )
        return {"agent_id": agent_id, "bound": bind}

    def realm_move(self, auth: dict, agent_id: str, station: str) -> dict:
        row = realm_agent(auth, agent_id)
        if not row:
            raise KeyError(f"agent {agent_id!r} is not one of your agents")
        st = realm_station(auth, station)           # target must be granted too
        self.move(agent_id, st["station_id"])
        return {"agent_id": agent_id, "station": st["name"]}

    def free(self, agent_id: str, station_id: str) -> dict:
        """Make a NAME claimable again, without disturbing the agent.

        The row keeps its id, its receipts, its channel memberships and its
        bids. What is released is everything that ties the name to whoever was
        using it:

          owner_token_hash  cleared, so no token is pinned to it
          stream_cursors    cleared, so the next holder does not inherit a
                            delivery position and start mid-stream

        Use this when a name is taken and a client needs to become it. Renaming
        or removing the agent would free the name too, but both throw away the
        history the name exists to carry.

        One limit, and it is not fixable from here: a client that is *currently*
        announcing this id keeps doing so until it is pointed elsewhere. Freeing
        decides who may claim the name, not who is already saying it.
        """
        st = STATIONS.get(station_id)
        if not st:
            raise KeyError(f"station {station_id!r} not found")
        sid = st["station_id"]
        row = CONN.execute(
            "SELECT owner_token_hash FROM agents "
            "WHERE station_id = %s AND agent_id = %s", (sid, agent_id),
        ).fetchone()
        if not row:
            raise KeyError(f"agent {agent_id!r} not found in {st['name']}")
        with CONN:
            CONN.execute(
                "UPDATE agents SET owner_token_hash = NULL "
                "WHERE station_id = %s AND agent_id = %s", (sid, agent_id),
            )
            CONN.execute(
                "DELETE FROM stream_cursors "
                "WHERE station_id = %s AND agent_id = %s", (sid, agent_id),
            )
        return {"agent_id": agent_id, "station_id": sid,
                "was_held": bool(row["owner_token_hash"])}

    def rename(self, station_id: str, agent_id: str, new_id: str) -> dict:
        """Rename an agent inside its station, moving everything with it.

        The mechanics only — no ownership check — so the TUI and the CLI can
        use it directly against the database, the way every other admin
        operation here works. `realm_rename` is this plus a token check.

        Receipts matter most: one left under the old id would be delivered to
        nobody and collected by nobody, pinning its message until the retention
        ceiling. Freeing a name this way keeps the agent's whole history, which
        deleting it does not.
        """
        st = STATIONS.get(station_id)
        if not st:
            raise KeyError(f"station {station_id!r} not found")
        sid = st["station_id"]
        new_id = normalize_agent_id(new_id)
        if not new_id:
            raise ValueError("the new id is empty")
        if not CONN.execute(
            "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
            (sid, agent_id),
        ).fetchone():
            raise KeyError(f"agent {agent_id!r} not found in {st['name']}")
        renaming = new_id != agent_id
        if renaming and CONN.execute(
            "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
            (sid, new_id),
        ).fetchone():
            raise ValueError(f"agent {new_id!r} already exists in {st['name']}")

        with CONN:
            if renaming:
                CONN.execute(
                    "UPDATE agents SET agent_id = %s, name = %s "
                    "WHERE station_id = %s AND agent_id = %s",
                    (new_id, new_id, sid, agent_id),
                )
                # Delivery state first — these are the rows that lose messages.
                CONN.execute(
                    "UPDATE message_receipts SET agent_id = %s "
                    "WHERE station_id = %s AND agent_id = %s",
                    (new_id, sid, agent_id),
                )
                CONN.execute(
                    "UPDATE stream_cursors SET agent_id = %s "
                    "WHERE station_id = %s AND agent_id = %s",
                    (new_id, sid, agent_id),
                )
                CONN.execute(
                    "UPDATE bids SET agent_id = %s "
                    "WHERE station_id = %s AND agent_id = %s",
                    (new_id, sid, agent_id),
                )
                # A rename is an identity change, not a pseudonym: the id is
                # how agents address each other, so leaving the old string in
                # transcripts would leave @mentions pointing at nobody.
                for table, column in (
                    ("transcripts", "sender"),
                    ("dms", "sender"),
                    ("dms", "recipient"),
                    ("broadcasts", "sender"),
                    # Same reason: an addressee is shown to every reader, so a
                    # stale id here labels a post as being for somebody who no
                    # longer exists.
                    ("message_addressees", "agent_id"),
                ):
                    CONN.execute(
                        f"UPDATE {table} SET {column} = %s "
                        f"WHERE station_id = %s AND {column} = %s",
                        (new_id, sid, agent_id),
                    )
                _rewrite_member_lists(sid, agent_id, new_id)
        return {"agent_id": new_id, "was": agent_id,
                "station_id": sid, "renamed": renaming}


    def realm_rename(self, auth: dict, agent_id: str, new_id: str) -> dict:
        """Rename one of this token's agents. One agent, one name.

        The broker matches ids literally — there is no alias layer and nothing
        is resolved — so a rename is only half the job: the client that sends
        this id has to store the new one, or it keeps announcing the old. That
        is why the tool driving this lives in the clients, which can write both
        sides, and the broker exposes only the endpoint.
        """
        row = realm_agent(auth, agent_id)
        if not row:
            raise KeyError(f"agent {agent_id!r} is not one of your agents")
        return self.rename(row["station_id"], agent_id, new_id)

    def realm_remove(self, auth: dict, agent_id: str) -> int:
        row = realm_agent(auth, agent_id)
        if not row:
            raise KeyError(f"agent {agent_id!r} is not one of your agents")
        return self.remove(agent_id, row["station_id"])

    def remove(self, agent_id: str, station_id: str | None = None) -> int:
        """Delete an agent from one station (and its stream cursor).

        Without `station_id` the agent must be unambiguous — an id present in
        several stations raises rather than guessing which copy to drop.
        """
        if station_id:
            st = STATIONS.get(station_id)
            if not st:
                raise KeyError(f"station {station_id!r} not found")
            sids = [st["station_id"]]
        else:
            sids = [
                r["station_id"] for r in CONN.execute(
                    "SELECT station_id FROM agents WHERE agent_id = %s",
                    (agent_id,),
                ).fetchall()
            ]
            if len(sids) > 1:
                raise ValueError(
                    f"agent {agent_id!r} exists in several stations; "
                    f"pass --station to say which"
                )
        n = 0
        with CONN:
            for sid in sids:
                n += CONN.execute(
                    "DELETE FROM agents WHERE station_id = %s AND agent_id = %s",
                    (sid, agent_id),
                ).rowcount
                CONN.execute(
                    "DELETE FROM stream_cursors "
                    "WHERE station_id = %s AND agent_id = %s",
                    (sid, agent_id),
                )
                # Everything else that named this agent has to go with it.
                # A receipt is the one that bites: no agent is left to ack it,
                # so its message can never be collected and sits until the
                # retention ceiling. A stale channel membership is worse still
                # — it keeps putting the dead id into new audiences, minting a
                # fresh orphan on every post.
                CONN.execute(
                    "DELETE FROM message_receipts "
                    "WHERE station_id = %s AND agent_id = %s",
                    (sid, agent_id),
                )
                CONN.execute(
                    "DELETE FROM bids WHERE station_id = %s AND agent_id = %s",
                    (sid, agent_id),
                )
                _rewrite_member_lists(sid, agent_id, None)
                # transcripts.sender and dms.sender are deliberately untouched:
                # they record what was said, and deleting an agent does not
                # unsay it. message_addressees is left for the same reason —
                # who a post was addressed to is history, it pins no collection
                # (unlike a receipt), and the FK retires it with the message.
        return n

    def list(
        self,
        station_id: str,
        expertise: list[str] | None = None,
        projects: list[str] | None = None,
    ) -> list[dict]:
        rows = CONN.execute(
            "SELECT * FROM agents WHERE station_id = %s ORDER BY created_at",
            (station_id,),
        ).fetchall()
        out = [_row_to_agent(r) for r in rows]
        ex_set = {e.lower() for e in (expertise or [])}
        pj_set = {p.lower() for p in (projects or [])}
        if not (ex_set or pj_set):
            return out
        filtered = []
        for p in out:
            if ex_set:
                tags = {t.lower() for t in p.get("expertise") or []}
                if not (ex_set & tags):
                    continue
            if pj_set:
                pjs = {t.lower() for t in p.get("projects") or []}
                if not (pj_set & pjs):
                    continue
            filtered.append(p)
        return filtered


AGENTS = AgentRegistry()


class ProposalRegistry:
    """Names clients have asked for, and operators have not answered yet.

    Registering an agent is the one step here that needs a human, and it was
    the step with no path between the two people involved: a client logged
    "ask an operator to run agent add ..." and the id then had to travel by
    Slack or memory, while the operator's screen showed nothing at all.

    A proposal is a request, not an object: it cannot send, receive, or be
    addressed, and only the operator surfaces (TUI/CLI, straight on the
    database) can turn one into an agent. A station token may propose and
    nothing else — that asymmetry is the whole security argument, and
    `test_agent_surface.py` asserts the absence of any approval route.
    """

    def propose(self, station_id: str, agent_id: str, token_hash: str,
                note: str = "") -> dict:
        st = STATIONS.get(station_id)
        if not st:
            raise KeyError(f"station {station_id!r} not found")
        sid = st["station_id"]
        agent_id = (agent_id or "").strip()
        if not agent_id:
            raise ValueError("agent_id is required")
        now = time.time()
        # Does the name already exist? That single fact decides everything
        # below, and it is never stored on the proposal: a claim is a request
        # for a name nobody has, a TRANSFER is a request for one somebody does.
        # Deriving it means the two can never disagree about which this is.
        existing = CONN.execute(
            "SELECT owner_token_hash FROM agents "
            "WHERE station_id = %s AND agent_id = %s",
            (sid, agent_id),
        ).fetchone()
        kind = "claim"
        if existing:
            kind = "transfer"
            owner = existing["owner_token_hash"]
            if not owner:
                # Nobody holds it. resolve_request_station binds an unowned
                # agent to the first token that uses it, so there is nothing
                # for an operator to decide — asking would only add a step.
                raise ValueError(
                    f"agent {agent_id!r} exists in {st['name']} and is "
                    f"unclaimed — just connect as it and it becomes yours"
                )
            if owner == token_hash:
                raise ValueError(
                    f"agent {agent_id!r} in {st['name']} is already yours"
                )
            locked = self.lock_left(sid, agent_id, token_hash, now)
            if locked > 0:
                raise ValueError(
                    f"a transfer of {agent_id!r} was denied; this client may "
                    f"ask again in {_short_duration(locked)} (an operator can "
                    f"lift it sooner with `agent unlock`)"
                )
        held = CONN.execute(
            "SELECT token_hash, expires_at FROM agent_proposals "
            "WHERE station_id = %s AND agent_id = %s",
            (sid, agent_id),
        ).fetchone()
        if held and held["expires_at"] > now and held["token_hash"] != token_hash:
            # Somebody else asked first. Refusing beats overwriting: two
            # clients would otherwise race for a name and the operator would
            # approve whichever wrote last.
            raise ValueError(
                f"{agent_id!r} is already proposed by another client in "
                f"{st['name']}"
            )
        if not held:
            pending = CONN.execute(
                "SELECT COUNT(*) AS n FROM agent_proposals "
                "WHERE token_hash = %s AND expires_at > %s",
                (token_hash, now),
            ).fetchone()["n"]
            if pending >= MAX_PENDING_PROPOSALS:
                raise ValueError(
                    f"this token already has {pending} pending proposals "
                    f"(max {MAX_PENDING_PROPOSALS}); withdraw one or wait for "
                    f"an operator"
                )
        expires_at = now + PROPOSAL_TTL
        with CONN:
            # Re-proposing your own name refreshes its deadline rather than
            # erroring: a client that restarts should not be punished for it.
            CONN.execute(
                """INSERT INTO agent_proposals (station_id, agent_id,
                       token_hash, note, created_at, expires_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                     note = VALUES(note), expires_at = VALUES(expires_at)""",
                (sid, agent_id, token_hash, note or "", now, expires_at),
            )
        return {"agent_id": agent_id, "station_id": sid,
                "station_name": st["name"], "created_at": now,
                "expires_at": expires_at, "kind": kind,
                "status": "pending approval"}

    # --- denial locks ------------------------------------------------------
    def lock_left(self, station_id: str, agent_id: str, token_hash: str,
                  now: float | None = None) -> float:
        """Seconds this token must still wait before re-asking, else 0.

        Takes a raw station_id: every caller already resolved the station, and
        resolving it twice would turn a lock check into two queries.
        """
        ts = now if now is not None else time.time()
        row = CONN.execute(
            "SELECT denied_until FROM agent_transfer_denials "
            "WHERE station_id = %s AND agent_id = %s AND token_hash = %s",
            (station_id, agent_id, token_hash),
        ).fetchone()
        return max(0.0, row["denied_until"] - ts) if row else 0.0

    def locks(self, station_id: str, agent_id: str) -> list[dict]:
        """Who is currently barred from asking for this name, and until when.
        What the TUI shows before offering to lift a lock."""
        return [dict(r) for r in CONN.execute(
            "SELECT d.token_hash, d.denied_at, d.denied_until, "
            "t.prefix AS token_prefix, t.user AS token_user "
            "FROM agent_transfer_denials d "
            "LEFT JOIN tokens t ON t.token_hash = d.token_hash "
            "WHERE d.station_id = %s AND d.agent_id = %s "
            "AND d.denied_until > %s ORDER BY d.denied_until",
            (station_id, agent_id, time.time()),
        ).fetchall()]

    def unlock(self, station_id: str, agent_id: str) -> int:
        """Lift every denial lock on a name. The operator's undo for a
        mis-aimed `x`, which would otherwise wedge a legitimate transfer for
        the whole locktime."""
        st = STATIONS.get(station_id)
        if not st:
            raise KeyError(f"station {station_id!r} not found")
        with CONN:
            cur = CONN.execute(
                "DELETE FROM agent_transfer_denials "
                "WHERE station_id = %s AND agent_id = %s",
                (st["station_id"], agent_id),
            )
        n = cur.rowcount or 0
        if n:
            log(f"transfer lock on {agent_id!r} lifted ({n})",
                event="transfer.unlock", station=st["station_id"],
                actor=agent_id)
        return n

    def list(self, station_id: str | None = None,
             token_hash: str | None = None) -> list[dict]:
        """Live proposals, newest deadline last. Expired ones are never
        returned even before `collect()` gets to them — a deadline that has
        passed is not a pending request, whatever the row says."""
        sql = (
            "SELECT p.station_id, p.agent_id, p.token_hash, p.note, "
            "p.created_at, p.expires_at, s.name AS station_name, "
            "t.prefix AS owner_prefix, t.user AS owner_user, "
            # Whether the name already exists is what makes this a transfer
            # rather than a claim, so it is read here instead of stored.
            "a.agent_id AS existing_agent, "
            "ao.prefix AS current_owner_prefix, "
            "ao.user AS current_owner_user, "
            "ao.revoked_at AS current_owner_revoked "
            "FROM agent_proposals p "
            "LEFT JOIN stations s ON s.station_id = p.station_id "
            "LEFT JOIN tokens t ON t.token_hash = p.token_hash "
            "LEFT JOIN agents a ON a.station_id = p.station_id "
            "  AND a.agent_id = p.agent_id "
            "LEFT JOIN tokens ao ON ao.token_hash = a.owner_token_hash "
            "WHERE p.expires_at > %s"
        )
        params: list = [time.time()]
        if station_id:
            st = STATIONS.get(station_id)
            if not st:
                raise KeyError(f"station {station_id!r} not found")
            sql += " AND p.station_id = %s"
            params.append(st["station_id"])
        if token_hash:
            sql += " AND p.token_hash = %s"
            params.append(token_hash)
        sql += " ORDER BY s.name, p.agent_id"
        out = []
        for r in CONN.execute(sql, params).fetchall():
            r = dict(r)
            r["kind"] = "transfer" if r.pop("existing_agent", None) else "claim"
            out.append(r)
        return out

    def _live(self, station_id: str, agent_id: str) -> dict:
        st = STATIONS.get(station_id)
        if not st:
            raise KeyError(f"station {station_id!r} not found")
        row = CONN.execute(
            "SELECT * FROM agent_proposals "
            "WHERE station_id = %s AND agent_id = %s AND expires_at > %s",
            (st["station_id"], agent_id, time.time()),
        ).fetchone()
        if not row:
            raise KeyError(
                f"no live proposal for {agent_id!r} in {st['name']} "
                f"(it may have expired)"
            )
        return dict(row)

    def approve(self, station_id: str, agent_id: str) -> dict:
        """Mint the agent, or hand an existing one to the token that asked.

        One transaction: an approved name can never exist as both an agent and
        a pending request, whichever way this is interrupted.

        A TRANSFER changes ownership and nothing else. Channel memberships and
        receipts are keyed by agent_id, so the name keeps its inbox and the new
        token inherits whatever the old client never acked. There is no
        "start clean" variant on purpose: it would mean deleting receipts, and
        collect() is the only thing in this system that deletes. The operator
        is the one who can tell a replaced laptop from a name grab, which is
        why the TUI states what moves before asking.
        """
        row = self._live(station_id, agent_id)
        sid, holder = row["station_id"], row["token_hash"]
        st = STATIONS.get(sid) or {}
        existing = CONN.execute(
            "SELECT owner_token_hash FROM agents "
            "WHERE station_id = %s AND agent_id = %s",
            (sid, agent_id),
        ).fetchone()
        with CONN:
            if existing:
                # Rebind rather than add: the row is already there, and add()
                # would collide on the composite key.
                AGENTS._bind_hash(agent_id, holder, sid)
                out = {"agent_id": agent_id, "station_id": sid,
                       "kind": "transfer",
                       "taken_from": existing["owner_token_hash"]}
            else:
                out = dict(AGENTS.add(sid, agent_id,
                                      owner_token_hash=holder),
                           kind="claim")
            CONN.execute(
                "DELETE FROM agent_proposals "
                "WHERE station_id = %s AND agent_id = %s",
                (sid, agent_id),
            )
            # An approved transfer settles the argument, so any lock from an
            # earlier refusal is spent. Leaving it would bar the token that
            # just WON the name from ever asking again.
            CONN.execute(
                "DELETE FROM agent_transfer_denials "
                "WHERE station_id = %s AND agent_id = %s",
                (sid, agent_id),
            )
        out["bound_to"] = holder
        # Callers print the station by name; the claim branch gets it from
        # AGENTS.add and the transfer branch would otherwise not have it.
        out.setdefault("station_name", st.get("name") or sid)
        log(f"{out['kind']} of {agent_id!r} approved",
            event="proposal.approve", station=sid, actor=agent_id)
        return out

    def reject(self, station_id: str, agent_id: str,
               lock: bool = True) -> dict:
        """Refuse a request. A denied TRANSFER also locks the asker out.

        `lock=False` is for a client withdrawing its own request: that is not a
        refusal, and locking a client out of a name because it changed its mind
        would be nonsense.

        A denied CLAIM never locks. It is usually a typo, the agent should be
        able to correct it at once, and MAX_PENDING_PROPOSALS already stops a
        client from flooding the screen.
        """
        row = self._live(station_id, agent_id)
        sid = row["station_id"]
        transfer = bool(CONN.execute(
            "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
            (sid, agent_id),
        ).fetchone())
        now = time.time()
        locked_until = None
        with CONN:
            CONN.execute(
                "DELETE FROM agent_proposals "
                "WHERE station_id = %s AND agent_id = %s",
                (sid, agent_id),
            )
            if transfer and lock:
                locked_until = now + TRANSFER_LOCKTIME
                CONN.execute(
                    """INSERT INTO agent_transfer_denials (station_id,
                           agent_id, token_hash, denied_at, denied_until)
                       VALUES (%s, %s, %s, %s, %s)
                       ON DUPLICATE KEY UPDATE
                         denied_at = VALUES(denied_at),
                         denied_until = VALUES(denied_until)""",
                    (sid, agent_id, row["token_hash"], now, locked_until),
                )
        kind = "transfer" if transfer else "claim"
        log(f"{kind} request for {agent_id!r} resolved"
            + (f"; locked {_short_duration(TRANSFER_LOCKTIME)}"
               if locked_until else ""),
            event="proposal.resolve", station=sid, actor=agent_id)
        return {"agent_id": agent_id, "station_id": sid, "kind": kind,
                "locked_until": locked_until}

    def withdraw(self, station_id: str, agent_id: str,
                 token_hash: str) -> dict:
        """The proposer taking its own request back."""
        row = self._live(station_id, agent_id)
        if row["token_hash"] != token_hash:
            raise PermissionError(
                f"{agent_id!r} was proposed by another client"
            )
        return self.reject(station_id, agent_id, lock=False)

    def sweep(self, station_id: str | None = None,
              now: float | None = None) -> int:
        """Delete proposals nobody answered. Called only from collect(),
        which is the single place in this system that removes rows.

        Station-scoped like everything else: collecting one station must not
        reach into another's rows, even to throw away garbage.
        """
        ts = now if now is not None else time.time()
        if station_id:
            cur = CONN.execute(
                "DELETE FROM agent_proposals "
                "WHERE station_id = %s AND expires_at <= %s",
                (station_id, ts),
            )
        else:
            cur = CONN.execute(
                "DELETE FROM agent_proposals WHERE expires_at <= %s", (ts,)
            )
        return cur.rowcount or 0

    def sweep_denials(self, station_id: str | None = None,
                      now: float | None = None) -> int:
        """Drop denial locks that have run out. A lapsed lock is not a lock,
        and lock_left() already treats it as none — this is what stops the
        rows themselves accumulating for names nobody asks about any more."""
        ts = now if now is not None else time.time()
        if station_id:
            cur = CONN.execute(
                "DELETE FROM agent_transfer_denials "
                "WHERE station_id = %s AND denied_until <= %s",
                (station_id, ts),
            )
        else:
            cur = CONN.execute(
                "DELETE FROM agent_transfer_denials WHERE denied_until <= %s",
                (ts,),
            )
        return cur.rowcount or 0


PROPOSALS = ProposalRegistry()


# ---------------------------------------------------------------------------
# Channel registry: pure message bus. post_to_channel persists; members
# read via get_channel / read_channel.
# ---------------------------------------------------------------------------

class ACLViolation(Exception):
    """Raised when add_member fails channel policy."""


class ChannelRegistry:
    # List-valued ACL fields, plus retention_days which is a number — see
    # _normalize_policy.
    POLICY_FIELDS = (
        "required_expertise",
        "allowed_projects",
        "blocked_agents",
    )
    POLICY_SCALARS = ("retention_days",)

    def _row(self, station_id: str, name: str):
        row = CONN.execute(
            "SELECT * FROM channels WHERE station_id = %s AND name = %s",
            (station_id, name),
        ).fetchone()
        if row is None:
            raise KeyError(f"channel {name!r} not found")
        return row

    def _message_count(self, station_id: str, name: str) -> int:
        row = CONN.execute(
            "SELECT COUNT(*) AS c FROM transcripts "
            "WHERE station_id = %s AND channel = %s",
            (station_id, name),
        ).fetchone()
        return int(row["c"])

    def _channel_dict(self, row, transcript: list[dict]) -> dict:
        return {
            "name": row["name"],
            "theme": row["theme"],
            "members": json.loads(row["members"] or "[]"),
            "policy": json.loads(row["policy"] or "{}"),
            "created_at": row["created_at"],
            "station_id": row["station_id"],
            "transcript": transcript,
        }

    @staticmethod
    def _normalize_policy(policy: dict | None) -> dict:
        p = dict(policy or {})
        allowed = (
            ChannelRegistry.POLICY_FIELDS + ChannelRegistry.POLICY_SCALARS
        )
        bad = sorted(set(p) - set(allowed))
        if bad:
            raise ValueError(
                f"unknown policy field(s): {bad}. Allowed: {list(allowed)}"
            )
        out: dict = {}
        for k in ChannelRegistry.POLICY_FIELDS:
            v = p.get(k) or []
            if not isinstance(v, list):
                raise ValueError(f"policy.{k} must be a list")
            out[k] = list(dict.fromkeys(v))
        # retention_days is optional: absent means "use the server default"
        # (MAX_RETENTION_DAYS), so it is left out of the stored policy rather
        # than pinned to today's default value.
        if p.get("retention_days") is not None:
            try:
                days = float(p["retention_days"])
            except (TypeError, ValueError):
                raise ValueError("policy.retention_days must be a number")
            if days < 1:
                raise ValueError("policy.retention_days must be >= 1")
            if days > MAX_RETENTION_DAYS:
                raise ValueError(
                    f"policy.retention_days must be <= {MAX_RETENTION_DAYS:g} "
                    f"(A2A_MAX_RETENTION_DAYS)"
                )
            out["retention_days"] = days
        return out

    @staticmethod
    def _check_acl(policy: dict, agent_profile: dict | None,
                   agent_id: str) -> None:
        if agent_id in (policy.get("blocked_agents") or []):
            raise ACLViolation(
                f"agent {agent_id!r} is blocked from this channel"
            )
        if not (
            policy.get("required_expertise")
            or policy.get("allowed_projects")
        ):
            return
        if agent_profile is None:
            raise ACLViolation(
                f"agent {agent_id!r} has no profile; cannot evaluate ACL"
            )
        req = {x.lower() for x in policy.get("required_expertise") or []}
        if req:
            tags = {x.lower() for x in agent_profile.get("expertise") or []}
            if not (req & tags):
                raise ACLViolation(
                    f"agent {agent_id!r} expertise does not satisfy "
                    f"channel policy"
                )
        allowed_p = {x.lower() for x in policy.get("allowed_projects") or []}
        if allowed_p:
            pjs = {x.lower() for x in agent_profile.get("projects") or []}
            if not (allowed_p & pjs):
                raise ACLViolation(
                    f"agent {agent_id!r} projects do not satisfy "
                    f"channel policy"
                )

    async def create(
        self,
        station_id: str,
        name: str,
        theme: str,
        members: list[str],
        policy: dict | None = None,
    ) -> dict:
        normalized_policy = self._normalize_policy(policy)
        members = list(dict.fromkeys(members or []))
        for m in members:
            self._check_acl(
                normalized_policy, AGENTS.get(station_id, m), m
            )
        created_at = time.time()

        def _do() -> dict:
            existing = CONN.execute(
                "SELECT 1 FROM channels WHERE station_id = %s AND name = %s",
                (station_id, name),
            ).fetchone()
            if existing:
                raise ValueError(f"channel {name!r} already exists")
            CONN.execute(
                """INSERT INTO channels (station_id, name, theme, members,
                       policy, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s)""",
                (
                    station_id,
                    name,
                    theme or "",
                    json.dumps(members),
                    json.dumps(normalized_policy),
                    created_at,
                ),
            )
            row = CONN.execute(
                "SELECT * FROM channels WHERE station_id = %s AND name = %s",
                (station_id, name),
            ).fetchone()
            return self._channel_dict(row, [])

        return await _db(_do)

    async def delete(self, station_id: str, name: str) -> bool:
        """Delete a channel, its transcript and the receipts pointing at it.

        The transcript goes by foreign-key cascade, but receipts reference a
        message id, not the channel, so they would survive as rows nothing can
        ever ack — the message they belong to no longer exists, so no agent can
        retire them and the collector cannot either.
        """
        def _do() -> bool:
            ids = [r["id"] for r in CONN.execute(
                "SELECT id FROM transcripts WHERE station_id = %s "
                "AND channel = %s", (station_id, name),
            ).fetchall()]
            with CONN:
                if ids:
                    ph = ",".join(["%s"] * len(ids))
                    CONN.execute(
                        f"DELETE FROM message_receipts WHERE station_id = %s "
                        f"AND msg_id IN ({ph})", [station_id, *ids],
                    )
                cur = CONN.execute(
                    "DELETE FROM channels WHERE station_id = %s AND name = %s",
                    (station_id, name),
                )
            return cur.rowcount > 0

        return await _db(_do)

    async def list(self, station_id: str) -> list[dict]:
        def _do() -> list[dict]:
            rows = CONN.execute(
                "SELECT * FROM channels WHERE station_id = %s "
                "ORDER BY created_at",
                (station_id,),
            ).fetchall()
            return [
                _row_to_channel_summary(
                    r, self._message_count(station_id, r["name"])
                )
                for r in rows
            ]

        return await _db(_do)

    async def get(
        self, station_id: str, name: str, limit: int | None = None
    ) -> dict:
        def _do() -> dict:
            row = self._row(station_id, name)
            sql = (
                "SELECT * FROM transcripts "
                "WHERE station_id = %s AND channel = %s ORDER BY ts"
            )
            params: tuple = (station_id, name)
            if limit:
                sql = (
                    "SELECT * FROM (SELECT * FROM transcripts "
                    "WHERE station_id = %s AND channel = %s "
                    "ORDER BY ts DESC LIMIT %s) AS recent "
                    "ORDER BY ts"
                )
                params = (station_id, name, limit)
            tx = [
                _row_to_transcript(t)
                for t in CONN.execute(sql, params).fetchall()
            ]
            return self._channel_dict(row, tx)

        return await _db(_do)

    async def messages_since(
        self,
        station_id: str,
        name: str,
        since: float | None,
        limit: int = 200,
    ) -> list[dict]:
        """Messages of a channel, oldest first.

        With `since` this is a catch-up from a cursor. Without one it is the
        NEWEST `limit` messages — not the oldest, which used to hand a
        newcomer the least relevant end of a long channel.
        """
        def _do() -> list[dict]:
            self._row(station_id, name)  # 404 if missing
            if since is None:
                sql = (
                    "SELECT * FROM (SELECT * FROM transcripts "
                    "WHERE station_id = %s AND channel = %s "
                    "ORDER BY ts DESC LIMIT %s) AS recent ORDER BY ts ASC"
                )
                params: list = [station_id, name, limit]
            else:
                sql = (
                    "SELECT * FROM transcripts "
                    "WHERE station_id = %s AND channel = %s AND ts > %s "
                    "ORDER BY ts ASC LIMIT %s"
                )
                params = [station_id, name, since, limit]
            out = [
                _row_to_transcript(t)
                for t in CONN.execute(sql, params).fetchall()
            ]
            _attach_addressees(station_id, out)
            return out

        return await _db(_do)

    async def add_member(
        self, station_id: str, name: str, agent_id: str
    ) -> dict:
        """Add an agent to a channel. The agent must exist in this station.

        Refusing an unknown id is not pedantry — it is the difference between
        a typo and a black hole. Membership decides a message's audience, so a
        phantom member receives a receipt for every post that nothing can ever
        collect or ack, while the agent whose name was misspelt sits there
        receiving nothing and reporting no error. That is exactly how a client
        that joined under a stale id went silently deaf to a whole channel.
        """
        agent_id = normalize_agent_id(agent_id)
        profile = AGENTS.get(station_id, agent_id)
        if not profile:
            raise KeyError(
                f"agent {agent_id!r} does not exist in this station, so it "
                f"cannot join {name!r}"
            )

        def _do() -> dict:
            row = self._row(station_id, name)
            policy = json.loads(row["policy"] or "{}")
            self._check_acl(policy, profile, agent_id)
            members = json.loads(row["members"] or "[]")
            if agent_id not in members:
                members.append(agent_id)
                CONN.execute(
                    "UPDATE channels SET members = %s "
                    "WHERE station_id = %s AND name = %s",
                    (json.dumps(members), station_id, name),
                )
            row = self._row(station_id, name)
            return _row_to_channel_summary(
                row, self._message_count(station_id, name)
            )

        return await _db(_do)

    async def remove_member(
        self, station_id: str, name: str, agent_id: str
    ) -> dict:
        def _do() -> dict:
            row = self._row(station_id, name)
            members = [
                m for m in json.loads(row["members"] or "[]")
                if m != agent_id
            ]
            CONN.execute(
                "UPDATE channels SET members = %s "
                "WHERE station_id = %s AND name = %s",
                (json.dumps(members), station_id, name),
            )
            row = self._row(station_id, name)
            return _row_to_channel_summary(
                row, self._message_count(station_id, name)
            )

        return await _db(_do)

    async def evict_off_project(
        self, station_id: str, name: str, project: str
    ) -> dict:
        proj_l = (project or "").lower()

        def _list_members() -> list[str]:
            row = self._row(station_id, name)
            return json.loads(row["members"] or "[]")

        members = await _db(_list_members)

        evicted: list[str] = []
        kept: list[str] = []
        for m in members:
            profile = AGENTS.get(station_id, m)
            agent_projects = {
                p.lower() for p in (profile or {}).get("projects") or []
            }
            if proj_l in agent_projects:
                kept.append(m)
            else:
                evicted.append(m)

        def _persist() -> dict:
            CONN.execute(
                "UPDATE channels SET members = %s "
                "WHERE station_id = %s AND name = %s",
                (json.dumps(kept), station_id, name),
            )
            row = self._row(station_id, name)
            summary = _row_to_channel_summary(
                row, self._message_count(station_id, name)
            )
            return {"channel": summary, "evicted": evicted, "kept": kept}

        return await _db(_persist)

    # --- admin: move a channel between stations ---------------------------

    def list_all(self, station_id: str | None = None) -> list[dict]:
        sql = (
            "SELECT c.station_id, s.name AS station_name, c.name, c.members, "
            "(SELECT COUNT(*) FROM transcripts t WHERE t.station_id = c.station_id "
            " AND t.channel = c.name) AS messages "
            "FROM channels c LEFT JOIN stations s ON s.station_id = c.station_id"
        )
        params: list = []
        if station_id:
            st = STATIONS.get(station_id)
            if not st:
                raise KeyError(f"station {station_id!r} not found")
            sql += " WHERE c.station_id = %s"
            params.append(st["station_id"])
        sql += " ORDER BY s.name, c.name"
        return [dict(r) for r in CONN.execute(sql, params).fetchall()]

    def move_station(self, name: str, to_station: str,
                     from_station: str | None = None) -> dict:
        """Move a channel — with its transcript and md files — to another
        station. Agents live in stations, so a channel stranded in a station
        with no members is invisible to everyone; this is the repair."""
        dst = STATIONS.get(to_station)
        if not dst:
            raise KeyError(f"station {to_station!r} not found")
        if from_station:
            src = STATIONS.get(from_station)
            if not src:
                raise KeyError(f"station {from_station!r} not found")
            rows = CONN.execute(
                "SELECT station_id FROM channels WHERE name = %s "
                "AND station_id = %s", (name, src["station_id"])
            ).fetchall()
        else:
            rows = CONN.execute(
                "SELECT station_id FROM channels WHERE name = %s", (name,)
            ).fetchall()
        if not rows:
            raise KeyError(f"channel {name!r} not found")
        if len(rows) > 1:
            raise ValueError(
                f"channel {name!r} exists in several stations; pass --from"
            )
        old = rows[0]["station_id"]
        if old == dst["station_id"]:
            return {"channel": name, "station": dst["name"], "moved": 0}
        if CONN.execute(
            "SELECT 1 FROM channels WHERE station_id = %s AND name = %s",
            (dst["station_id"], name),
        ).fetchone():
            raise ValueError(
                f"{dst['name']} already has a channel {name!r}"
            )
        # transcripts reference channels(station_id, name), so neither row
        # satisfies the FK for the instant between the two updates. The check
        # is suspended for this connection only, and restored in the finally
        # even if the transaction rolls back — it is a session variable, not a
        # database-wide switch like sqlite's PRAGMA was.
        with CONN:
            CONN.execute("SET SESSION foreign_key_checks = 0")
            try:
                CONN.execute(
                    "UPDATE channels SET station_id = %s WHERE station_id = %s "
                    "AND name = %s", (dst["station_id"], old, name)
                )
                n = CONN.execute(
                    "UPDATE transcripts SET station_id = %s WHERE station_id = %s "
                    "AND channel = %s", (dst["station_id"], old, name)
                ).rowcount
                CONN.execute(
                    "UPDATE md_files SET station_id = %s WHERE station_id = %s "
                    "AND channel = %s", (dst["station_id"], old, name)
                )
            finally:
                CONN.execute("SET SESSION foreign_key_checks = 1")
        return {"channel": name, "from": old, "station": dst["name"],
                "messages": n}

    async def post(
        self,
        station_id: str,
        name: str,
        sender: str,
        text: str,
        expires_in: str | float | None = None,
        addressed: list[str] | None = None,
    ) -> dict:
        """Persist one message in the channel transcript. No fan-out.

        Two sets, and only one of them decides delivery:

          audience   the channel's members, minus the sender. Everyone in it
                     receives the post, and every one of them must ack before
                     it can be retired.
          addressed  who the post is FOR — a label the whole room can see, so
                     a reader can tell "answering them" from "telling
                     everyone". It changes nothing about delivery or retention
                     and is always a subset of the audience.

        A channel post never reaches anyone outside the channel, so `addressed`
        may only name members: use add_member or send_dm to reach anyone else.
        """
        # Before anything else, and before any database work: an oversized body
        # must cost the sender an error, not a row it cannot read back.
        check_size(text, MAX_MESSAGE_SIZE, "this post")
        row = await _db(lambda: self._row(station_id, name))  # 404 if missing
        if addressed:
            # Naming a non-member is refused, never silently widened: a channel
            # post cannot reach outside the channel, so a label pointing at
            # somebody who will not receive it is a lie the sender should hear
            # about now rather than wonder about later.
            members = set(await _db(lambda: _channel_members(station_id, name)))
            outside = [a for a in addressed if a not in members and a != sender]
            if outside:
                raise ValueError(
                    f"{', '.join(outside)} not in #{name} — a channel post "
                    f"never reaches outside the channel. Add them with "
                    f"add_channel_member, or send_dm instead."
                )
        try:
            policy = json.loads(row["policy"] or "{}") or {}
        except (TypeError, ValueError):
            policy = {}
        ceiling = parse_duration(policy.get("retention")) or (
            float(policy["retention_days"]) * 86400.0
            if policy.get("retention_days") else None
        )
        now = time.time()
        entry = {
            "id": str(uuid.uuid4()),
            "ts": now,
            "channel": name,
            "sender": sender,
            "text": text,
            "expires_at": _expiry_from(expires_in, now, ceiling),
        }
        # Addressing yourself is a no-op: you are not in your own audience,
        # so the label would point at somebody who never receives the post.
        addressed = list(dict.fromkeys(
            a for a in (addressed or []) if a != sender))

        def _persist() -> list[str]:
            CONN.execute(
                """INSERT INTO transcripts (id, station_id, channel, ts,
                       sender, text, expires_at)
                   VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                (
                    entry["id"], station_id, entry["channel"], entry["ts"],
                    entry["sender"], entry["text"], entry["expires_at"],
                ),
            )
            # Freeze the audience now, in the same call: whoever is a member at
            # this instant, and nobody who joins later.
            audience = _channel_audience(station_id, name, sender)
            _write_receipts(
                station_id, entry["id"], "channel", entry["ts"], audience,
                entry["expires_at"]
            )
            # And who it was FOR, which is a different and much smaller fact:
            # the sender's own `addressed`, verbatim, minus itself. Written
            # here so a post can never exist with half its metadata — same call
            # as the row and the receipts. It adds nobody to the audience and
            # takes nobody out; _channel_audience above has already decided
            # that, and the members check above guarantees this is a subset.
            for who in addressed:
                CONN.execute(
                    "INSERT IGNORE INTO message_addressees "
                    "(station_id, msg_id, agent_id) VALUES (%s, %s, %s)",
                    (station_id, entry["id"], who),
                )
            return audience

        entry["addressed"] = addressed
        entry["audience"] = await _db(_persist)
        _wake_station(station_id)
        return {"channel": name, "post": entry}


CHANNELS = ChannelRegistry()


# ---------------------------------------------------------------------------
# Broadcast registry: a help-wanted board.
#
# `broadcast` is fire-and-forget — the server persists it and returns the
# candidate agent_ids (matching the expertise/project filter). Candidates
# poll list_broadcasts / get_broadcast to find work and call submit_bid with
# CLAIM (with a one-line pitch) or PASS to respond.
# ---------------------------------------------------------------------------

class BroadcastRegistry:
    async def create(
        self,
        station_id: str,
        problem: str,
        sender: str,
        expertise: list[str] | None,
        projects: list[str] | None,
    ) -> dict:
        check_size(problem, MAX_MESSAGE_SIZE, "this broadcast")
        candidates = [
            c["agent_id"] for c in AGENTS.list(
                station_id, expertise=expertise, projects=projects
            )
            if c["agent_id"] != sender
        ]
        bid = str(uuid.uuid4())
        now = time.time()

        def _do() -> dict:
            CONN.execute(
                """INSERT INTO broadcasts (id, station_id, sender, problem,
                       expertise, projects, status, created_at, updated_at,
                       candidates)
                   VALUES (%s, %s, %s, %s, %s, %s, 'open', %s, %s, %s)""",
                (
                    bid, station_id, sender, problem,
                    json.dumps(list(expertise or [])),
                    json.dumps(list(projects or [])),
                    now, now, json.dumps(candidates),
                ),
            )
            # The candidate list IS the audience of a help-wanted request.
            _write_receipts(station_id, bid, "broadcast", now, candidates)
            row = CONN.execute(
                "SELECT * FROM broadcasts WHERE id = %s", (bid,)
            ).fetchone()
            return _row_to_broadcast(row)

        out = await _db(_do)
        out["candidates"] = candidates
        # Push it: candidates no longer have to poll for it (nothing does).
        _wake_station(station_id)
        return out

    async def list(
        self,
        station_id: str,
        status: str | None = None,
        since: float | None = None,
        limit: int = 50,
    ) -> list[dict]:
        def _do() -> list[dict]:
            sql = "SELECT * FROM broadcasts WHERE station_id = %s"
            params: list = [station_id]
            if status:
                sql += " AND status = %s"
                params.append(status)
            if since is not None:
                sql += " AND created_at >= %s"
                params.append(since)
            sql += " ORDER BY created_at DESC LIMIT %s"
            params.append(limit)
            return [
                _row_to_broadcast(r)
                for r in CONN.execute(sql, params).fetchall()
            ]

        return await _db(_do)

    async def get(self, station_id: str, broadcast_id: str) -> dict:
        def _do() -> dict:
            row = CONN.execute(
                "SELECT * FROM broadcasts "
                "WHERE id = %s AND station_id = %s",
                (broadcast_id, station_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"broadcast {broadcast_id!r} not found")
            bid_rows = CONN.execute(
                """SELECT * FROM bids
                    WHERE broadcast_id = %s AND station_id = %s
                    ORDER BY created_at""",
                (broadcast_id, station_id),
            ).fetchall()
            return _row_to_broadcast(
                row, [_row_to_bid(b) for b in bid_rows]
            )

        return await _db(_do)

    async def submit_bid(
        self,
        station_id: str,
        broadcast_id: str,
        agent_id: str,
        bid: str,
        pitch: str = "",
    ) -> dict:
        if bid not in ("claim", "pass"):
            raise ValueError("bid must be 'claim' or 'pass'")
        now = time.time()

        def _do() -> dict:
            row = CONN.execute(
                "SELECT status FROM broadcasts "
                "WHERE id = %s AND station_id = %s",
                (broadcast_id, station_id),
            ).fetchone()
            if row is None:
                raise KeyError(f"broadcast {broadcast_id!r} not found")
            if row["status"] != "open":
                raise ValueError(
                    f"broadcast {broadcast_id!r} is {row['status']}"
                )
            CONN.execute(
                """INSERT INTO bids (station_id, broadcast_id, agent_id,
                       bid, pitch, created_at)
                   VALUES (%s, %s, %s, %s, %s, %s)
                   ON DUPLICATE KEY UPDATE
                       bid = VALUES(bid),
                       pitch = VALUES(pitch),
                       created_at = VALUES(created_at)""",
                (station_id, broadcast_id, agent_id, bid, pitch or "", now),
            )
            CONN.execute(
                "UPDATE broadcasts SET updated_at = %s WHERE id = %s",
                (now, broadcast_id),
            )
            # Bidding IS processing the request, so it acks it — a candidate
            # that answered never has to ack the broadcast separately.
            _ack_receipts(station_id, agent_id, [broadcast_id])
            row = CONN.execute(
                "SELECT * FROM bids "
                "WHERE broadcast_id = %s AND agent_id = %s",
                (broadcast_id, agent_id),
            ).fetchone()
            return _row_to_bid(row)

        return await _db(_do)

    async def close(self, station_id: str, broadcast_id: str) -> dict:
        def _do() -> dict:
            cur = CONN.execute(
                """UPDATE broadcasts
                      SET status = 'closed', updated_at = %s
                    WHERE id = %s AND station_id = %s""",
                (time.time(), broadcast_id, station_id),
            )
            if cur.rowcount == 0:
                raise KeyError(f"broadcast {broadcast_id!r} not found")
            # A closed request is no longer actionable, so it stops being
            # pending for the candidates who never answered — otherwise it
            # would pin itself until the retention ceiling.
            CONN.execute(
                "UPDATE message_receipts SET acked_at = %s "
                "WHERE station_id = %s AND msg_id = %s AND acked_at IS NULL",
                (time.time(), station_id, broadcast_id),
            )
            row = CONN.execute(
                "SELECT * FROM broadcasts WHERE id = %s", (broadcast_id,)
            ).fetchone()
            return _row_to_broadcast(row)

        return await _db(_do)


BROADCASTS = BroadcastRegistry()


# ---------------------------------------------------------------------------
# Direct messages: one agent to one agent, inside a station.
# ---------------------------------------------------------------------------

class DirectRegistry:
    async def send(
        self, station_id: str, sender: str, recipient: str, text: str,
        expires_in: str | float | None = None,
    ) -> dict:
        """Deliver a DM. Sender and recipient must both live in this station;
        sender == recipient is allowed (self-test)."""
        sender = normalize_agent_id(sender)
        recipient = normalize_agent_id(recipient)
        if not sender:
            raise ValueError("this request names no agent, so it has no sender")
        if not recipient:
            raise ValueError("no recipient given")
        if not text:
            raise ValueError("empty message")
        check_size(text, MAX_MESSAGE_SIZE, "this DM")

        def _check() -> None:
            for who in {sender, recipient}:
                row = CONN.execute(
                    "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
                    (station_id, who),
                ).fetchone()
                if not row:
                    raise KeyError(
                        f"agent {who!r} is not in this station"
                    )

        await _db(_check)
        now = time.time()
        entry = {
            "id": str(uuid.uuid4()), "ts": now,
            "sender": sender, "recipient": recipient, "text": text,
            "expires_at": _expiry_from(expires_in, now),
        }

        def _persist() -> None:
            CONN.execute(
                "INSERT INTO dms (id, station_id, sender, recipient, text,"
                " ts, expires_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
                (entry["id"], station_id, sender, recipient, text,
                 entry["ts"], entry["expires_at"]),
            )
            # Audience of a DM is exactly one agent — including when that is
            # the sender itself, which is what makes ping_me a real end-to-end
            # test rather than a no-op.
            _write_receipts(
                station_id, entry["id"], "dm", entry["ts"], [recipient],
                entry["expires_at"],
            )

        await _db(_persist)
        _wake_station(station_id)
        return {"dm": entry, "delivered_to": recipient}

    async def inbox(
        self, station_id: str, agent_id: str, since: float | None = None,
        limit: int = 50,
    ) -> list[dict]:
        """Pull fallback: DMs addressed to this agent, oldest first."""
        def _do() -> list[dict]:
            rows = CONN.execute(
                "SELECT * FROM dms WHERE station_id = %s AND recipient = %s "
                "AND ts > %s ORDER BY ts ASC LIMIT %s",
                (station_id, agent_id, since or 0.0, limit),
            ).fetchall()
            return [
                {"id": r["id"], "ts": r["ts"], "sender": r["sender"],
                 "recipient": r["recipient"], "text": r["text"]}
                for r in rows
            ]

        return await _db(_do)


DIRECT = DirectRegistry()


# ---------------------------------------------------------------------------
# MCP surface.
# ---------------------------------------------------------------------------

# DNS-rebinding protection auto-enables whenever FastMCP's `host` is
# localhost (its default), with an allowlist of only 127.0.0.1/localhost.
# That rejects every public Host header behind a reverse proxy with HTTP
# 421 "Invalid Host header". Access control here is Bearer-token + TLS
# at the edge, so this gate adds no security and breaks every real deploy.
mcp = FastMCP(
    "a2a-mcp",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=False,
    ),
)
mcp.settings.streamable_http_path = "/"


@mcp.tool()
async def whoami() -> dict:
    """Who am I, and what does my card say?

    Returns this agent's own profile as the other agents see it — the same
    fields list_agents shows them. A blank description/expertise is worth
    fixing with update_agent: it is how anyone decides whether to @mention you
    or send you a broadcast, so an empty card makes an agent invisible in
    practice even though it is registered.
    """
    sid = require_station()
    agent = current_agent()
    st = STATIONS.get(sid) or {"station_id": sid, "name": "(unknown)"}
    out = {
        "agent": agent,
        "station_id": st["station_id"],
        "station_name": st.get("name"),
    }
    row = await _db(lambda: AGENTS.get(sid, agent or ""))
    if row:
        for f in ("description", "expertise", "projects", "system_prompt",
                  "metadata"):
            out[f] = row.get(f)
        out["card_is_blank"] = not any(
            out.get(f) for f in ("description", "expertise", "projects")
        )
    return out


# --- realm: self-service, works even before this agent is registered ---------

@mcp.tool()
async def my_realm() -> dict:
    """Who am I, which stations may I use, and am I registered yet?

    Works before registration — use it when another tool says this agent is
    not set up. Registering is an operator action, so if this reports
    registered=false the fix is on their side, not yours.
    """
    auth = require_auth()
    view = await _db(lambda: AGENTS.realm_view(auth, current_agent()))
    # Only report a denial that is still true: the middleware's reason can be
    # stale inside a long-lived MCP session that has since registered.
    denial = _current_denial.get()
    if denial and not view["registered"]:
        view["denied"] = denial
    # The limits a caller can hit, said before it hits them. A sender that knows
    # the ceiling can split or upload instead of composing a message that will
    # be refused — and it is per-broker, so it cannot be baked into a client.
    view["max_message_size"] = MAX_MESSAGE_SIZE
    view["max_md_size"] = MAX_MD_SIZE
    return view


@mcp.tool()
async def list_my_agents() -> list[dict]:
    """List the agents in my granted stations, with their binding status."""
    auth = require_auth()
    view = await _db(lambda: AGENTS.realm_view(auth, current_agent()))
    return view["agents"]


@mcp.tool()
async def bind_me(agent_id: str = "") -> dict:
    """Pin an agent (default: this one) to the calling token."""
    auth = require_auth()
    return await _db(
        lambda: AGENTS.realm_bind(auth, agent_id or current_agent() or "", True)
    )


@mcp.tool()
async def unbind_me(agent_id: str = "") -> dict:
    """Release an agent (default: this one) so another token may claim it."""
    auth = require_auth()
    return await _db(
        lambda: AGENTS.realm_bind(auth, agent_id or current_agent() or "", False)
    )


@mcp.tool()
async def move_me(station: str, agent_id: str = "") -> dict:
    """Move an agent (default: this one) to another of my granted stations."""
    auth = require_auth()
    return await _db(
        lambda: AGENTS.realm_move(
            auth, agent_id or current_agent() or "", station
        )
    )


# --- agents -----------------------------------------------------------------

@mcp.tool()
async def list_agents(
    expertise: list[str] | None = None,
    project: list[str] | None = None,
) -> list[dict]:
    """List agents in the caller's station, optionally filtered."""
    return AGENTS.list(require_station(), expertise=expertise, projects=project)


@mcp.tool()
async def get_agent(agent_id: str) -> dict:
    """Fetch one agent profile."""
    p = AGENTS.get(require_station(), agent_id)
    if p is None:
        raise KeyError(f"agent {agent_id!r} not found")
    return p


@mcp.tool()
async def update_agent(
    agent_id: str,
    name: str | None = None,
    description: str | None = None,
    expertise: list[str] | None = None,
    projects: list[str] | None = None,
    system_prompt: str | None = None,
    metadata: dict | None = None,
) -> dict:
    """Update profile fields. None = unchanged."""
    fields: dict = {}
    if name is not None:
        fields["name"] = name
    if description is not None:
        fields["description"] = description
    if expertise is not None:
        fields["expertise"] = expertise
    if projects is not None:
        fields["projects"] = projects
    if system_prompt is not None:
        fields["system_prompt"] = system_prompt
    if metadata is not None:
        fields["metadata"] = metadata
    return await AGENTS.update(require_station(), agent_id, **fields)


@mcp.tool()
async def create_channel(
    name: str,
    theme: str = "",
    members: list[str] | None = None,
    policy: dict | None = None,
) -> dict:
    """Open a channel in your station, with yourself in it.

    Channels are yours to make — if the conversation you need does not exist,
    create it rather than asking anyone. `members` defaults to just you; add
    the agents you want with join-side calls or by listing them here. Nobody
    is retro-added to history: an agent only receives what was posted after it
    joined, so creating a channel and inviting people costs them nothing.

    You cannot delete one. A channel holds other agents' transcript, so
    removing it is an operator's call, not a participant's.
    """
    me = current_agent()
    who = list(members) if members else []
    if me and me not in who:
        who.append(me)
    return await CHANNELS.create(require_station(), name, theme, who, policy)


@mcp.tool()
async def list_channels() -> list[dict]:
    """List channels in the caller's station."""
    return await CHANNELS.list(require_station())


@mcp.tool()
async def get_channel(name: str, transcript_limit: int = 50) -> dict:
    """Fetch channel state. transcript_limit=0 returns the full transcript."""
    return await CHANNELS.get(
        require_station(), name,
        limit=transcript_limit if transcript_limit else None,
    )


@mcp.tool()
async def add_channel_member(name: str, agent_id: str) -> dict:
    """Add an agent to a channel; rejected if channel ACL forbids it."""
    return await CHANNELS.add_member(require_station(), name, agent_id)


@mcp.tool()
async def remove_channel_member(name: str, agent_id: str) -> dict:
    """Remove an agent from a channel."""
    return await CHANNELS.remove_member(require_station(), name, agent_id)


@mcp.tool()
async def evict_off_project(channel: str, project: str) -> dict:
    """Bulk-remove members whose project tags don't include `project`."""
    return await CHANNELS.evict_off_project(
        require_station(), channel, project
    )


def _receipt(out: dict) -> dict:
    """What a MODEL needs back after writing: that it landed, its id, who owes
    an ack, and the deadline. Not the message.

    REST keeps returning the whole row — an external script legitimately wants
    what it created — but a tool result is injected into the session that just
    composed the text, so echoing it spends the body's own length a second time
    and, in a client that renders results verbatim, reads like a second
    message. A Codex agent's screen showed exactly that.
    """
    post = (out or {}).get("post") or (out or {}).get("dm") or out or {}
    if not isinstance(post, dict):
        return out
    kept = {k: post[k] for k in ("id", "channel", "recipient", "uri",
                                 "audience", "addressed") if post.get(k)}
    if post.get("expires_at"):
        # Milliseconds, matching what JavaScript's toISOString() gives the
        # other clients: one instant must have one spelling everywhere.
        kept["expires"] = (
            datetime.fromtimestamp(post["expires_at"], tz=timezone.utc)
            .isoformat(timespec="milliseconds").replace("+00:00", "Z"))
    return kept


@mcp.tool()
async def post_to_channel(
    name: str, message: str, sender: str, expires_in: str | None = None,
    addressed: list[str] | None = None,
) -> dict:
    """Persist `message` in the channel transcript. No server-side fan-out —
    members read via get_channel / read_channel.

    EVERY member of the channel receives this, reads it and must ack it. That
    set is the `audience` and you do not choose it — a channel post never
    reaches anyone outside the channel.

    `addressed` is who the post is FOR: the agent you are answering. Name them
    even though they would have received it anyway — it is how the room tells
    "answering them" from "telling everyone", and every reader sees it. Leave
    it out for general traffic. It changes nothing about who receives the post
    or how long it is kept, and it may only name MEMBERS of this channel: to
    reach anyone else, add_channel_member first, or send_dm instead.

    Writing "@name" in the message text addresses nobody — it is decoration,
    and delivery never reads the body.

    `expires_in` is how long this is worth reading — "10m", "2h", "7d", or
    seconds. Leave it alone unless the message has a real shelf life: the
    default is a year, and an expired message is never delivered, because
    acting on it late is worse than not acting.

    A post is capped (64 KiB by default; my_realm reports the exact figure).
    For anything longer use share_md and post the md:// URI — a message has to
    fit in the context of every agent that receives it.
    """
    return _receipt(await CHANNELS.post(
        require_station(), name, sender, message, expires_in, addressed))


@mcp.tool()
async def read_channel(
    name: str, since: float | None = None, limit: int = 50
) -> list[dict]:
    """Return channel messages, oldest first.

    With `since` (Unix ts) it catches up from that point; without one it
    returns the most recent `limit` messages — the useful end of a long
    channel. Reading here also counts as delivery for any of your pending
    messages that show up, but not as an ack: use ack_messages for that.
    """
    sid = require_station()
    msgs = await CHANNELS.messages_since(
        sid, name, since=since, limit=limit
    )
    me = current_agent()
    if me and msgs:
        ids = [m["id"] for m in msgs]
        await _db(lambda: _mark_read(sid, me, ids))
        await _db(lambda: _maybe_collect(sid))
    return msgs


# --- pending / ack ----------------------------------------------------------

@mcp.tool()
async def my_pending(limit: int = 50) -> dict:
    """Everything addressed to you that you have not acked yet, oldest first.

    This is the whole of your inbox — not the channel's history. Messages
    posted before you joined a channel were never addressed to you, so they
    never appear here: arriving somewhere busy costs you nothing.

    Use it after a restart or when you suspect push missed something. Ack what
    you have handled with ack_messages so it stops being pending (and can be
    cleaned up).
    """
    sid = require_station()
    me = current_agent()
    if not me:
        raise ValueError(
            "this request names no agent, so it has no inbox"
        )

    def _do() -> dict:
        rows = _pending_rows(sid, me, limit + 1)
        more = len(rows) > limit
        rows = rows[:limit]
        msgs = _resolve_receipts(sid, rows)
        _mark_read(sid, me, [r["msg_id"] for r in rows])
        total = CONN.execute(
            "SELECT COUNT(*) AS n FROM message_receipts "
            "WHERE station_id = %s AND agent_id = %s AND acked_at IS NULL",
            (sid, me),
        ).fetchone()["n"]
        return {
            "agent_id": me, "pending_total": total,
            "returned": len(msgs), "has_more": more, "messages": msgs,
        }

    return await _db(_do)


@mcp.tool()
async def ack_messages(ids: list[str]) -> dict:
    """Confirm you have processed these messages, by id.

    Every message you receive carries an id (`meta.id` on a pushed event, the
    `id` field from my_pending / read_channel). Acking is what lets the broker
    delete a message: nothing is ever removed while someone it was addressed
    to has not acked it. Ids that are not yours, or already acked, are ignored.
    """
    sid = require_station()
    me = current_agent()
    if not me:
        raise ValueError("this request names no agent, so it can ack nothing")
    clean = [str(i) for i in (ids or []) if str(i)]
    acked = await _db(lambda: _ack_receipts(sid, me, clean))
    if acked:
        # An ack is the only event that can make something collectible, so it
        # is the natural moment to try.
        await _db(lambda: _maybe_collect(sid))
    remaining = await _db(lambda: CONN.execute(
        "SELECT COUNT(*) AS n FROM message_receipts "
        "WHERE station_id = %s AND agent_id = %s AND acked_at IS NULL",
        (sid, me),
    ).fetchone()["n"])
    return {"acked": acked, "pending_total": remaining}


@mcp.tool()
async def ack_all() -> dict:
    """Ack everything waiting for you, without reading it. Inbox zero.

    For a backlog you have decided not to work through — you were away, the
    conversation moved on, and none of it needs an answer now. Nothing is
    deleted by this: acking is what lets the broker retire a message, and a
    channel post is only removed once its whole audience has acked, so this
    also unblocks messages your peers are still holding on your behalf.

    Be honest with it. Acking says "handled", and an agent that clears its
    inbox to look responsive is telling its peers their questions were dealt
    with when they were not. If you might answer, read it with my_pending
    instead — that acks as it goes, one message at a time.

    Only ever your own receipts, in your own station.
    """
    sid = require_station()
    me = current_agent()
    if not me:
        raise ValueError("this request names no agent, so it can ack nothing")
    out = await _db(lambda: screen(sid, me))
    if out["acked"]:
        await _db(lambda: _maybe_collect(sid))
    return {"acked": out["acked"], "by_kind": out.get("by_kind", {}),
            "pending_total": 0,
            "note": "everything addressed to you is marked handled"}


# --- direct messages --------------------------------------------------------

@mcp.tool()
async def send_dm(to: str, message: str, expires_in: str | None = None) -> dict:
    """Send a private message to one agent in your station.

    Unlike post_to_channel this does not go into any channel transcript — only
    the recipient receives it, pushed to their session. `to` is the other
    agent's id (see list_agents).

    `expires_in` is how long this is worth reading — "10m", "2h", "7d", or
    seconds. Leave it alone unless the message has a real shelf life: the
    default is a year, and an expired message is never delivered, because
    acting on it late is worse than not acting.

    Same size cap as a channel post (64 KiB by default, see my_realm); use
    share_md and send the md:// URI for anything longer.
    """
    return _receipt(await DIRECT.send(
        require_station(), current_agent() or "", to, message, expires_in
    ))


@mcp.tool()
async def ping_me(text: str = "") -> dict:
    """Send yourself a DM to prove Claude Code channel delivery end to end.

    Call it, then wait a moment: if push is working the message comes back to
    you as a <channel source="plugin:a2a:a2a-channel" channel="dm"> event
    carrying the same witness id, with no polling.

    If it never arrives the channel was not registered for this session, which
    Claude Code does silently. Relaunch naming the PLUGIN, not the server —
    `server:<name>` only matches a bare .mcp.json server, so it silently
    matches nothing here and no listener is attached:

        claude --dangerously-load-development-channels plugin:a2a@skills-dir

    (--resume is fine.) The startup dialog prints `Channels: <entry>`; if that
    line does not read plugin:a2a@skills-dir, the entry did not match, and the
    "I am using this for local development" prompt must be confirmed or the
    flag does not apply. a2a_channel_status tells you whether the client is
    connected and how many messages it has pushed.
    """
    me = current_agent() or ""
    witness = f"PING-{uuid.uuid4().hex[:8].upper()}"
    body = f"{witness}{(' ' + text) if text else ''}"
    # Short-lived: a witness that proves delivery has done its job on arrival,
    # and one that proves the opposite should not sit in a queue for a year
    # being counted as backlog by the tool that reports backlog.
    out = await DIRECT.send(require_station(), me, me, body, PING_TTL)
    out["witness"] = witness
    out["expect"] = (
        f'a <channel source="a2a" channel="dm" sender="{me}"> event '
        f"containing {witness} within a few seconds"
    )
    return out


@mcp.tool()
async def read_dms(since: float | None = None, limit: int = 50) -> list[dict]:
    """Pull fallback: direct messages addressed to you, oldest first.

    Counts as delivery, not as an ack — ack_messages still decides what may be
    cleaned up.
    """
    sid = require_station()
    me = current_agent() or ""
    msgs = await DIRECT.inbox(sid, me, since=since, limit=limit)
    if me and msgs:
        ids = [m["id"] for m in msgs]
        await _db(lambda: _mark_read(sid, me, ids))
        await _db(lambda: _maybe_collect(sid))
    return msgs


# --- broadcasts -------------------------------------------------------------

@mcp.tool()
async def broadcast(
    problem: str,
    sender: str,
    expertise: list[str] | None = None,
    projects: list[str] | None = None,
) -> dict:
    """Post a help-wanted broadcast.

    Returns the broadcast id, status="open", and the list of candidate
    agent_ids matching the expertise/project filter. Candidates respond
    asynchronously via submit_bid.
    """
    return await BROADCASTS.create(
        require_station(), problem, sender, expertise, projects
    )


@mcp.tool()
async def list_broadcasts(
    status: str | None = None,
    since: float | None = None,
    limit: int = 50,
) -> list[dict]:
    """List broadcasts. Filter by status ('open' | 'closed') and/or since."""
    return await BROADCASTS.list(
        require_station(), status=status, since=since, limit=limit
    )


@mcp.tool()
async def get_broadcast(broadcast_id: str) -> dict:
    """Fetch one broadcast with its bid history."""
    return await BROADCASTS.get(require_station(), broadcast_id)


@mcp.tool()
async def submit_bid(
    broadcast_id: str, agent_id: str, bid: str, pitch: str = ""
) -> dict:
    """Submit a bid on an open broadcast. `bid` is 'claim' or 'pass'."""
    return await BROADCASTS.submit_bid(
        require_station(), broadcast_id, agent_id, bid, pitch
    )


@mcp.tool()
async def close_broadcast(broadcast_id: str) -> dict:
    """Mark a broadcast as closed (no more bids accepted)."""
    return await BROADCASTS.close(require_station(), broadcast_id)


# --- md_files ---------------------------------------------------------------

def _md_uri(channel: str | None, filename: str) -> str:
    return (
        f"md://channel/{channel}/{filename}" if channel
        else f"md://global/{filename}"
    )


# The work lives here and not in the tool, because it has TWO surfaces like
# everything else in this file: the MCP tools below and the /md routes in
# build_app(). md was the one capability that skipped that rule, and the cost
# was exactly what the rule exists to prevent — for months it existed only for
# clients that speak MCP, while the OpenCode and Pi briefs told their agents to
# use it. Keep both surfaces on these two functions.

async def md_store(station_id: str, channel: str, sender: str, filename: str,
                   content: str, note: str = "") -> dict:
    """Store a markdown blob and reference it from `channel`'s transcript.

    The caller supplies the bytes — the server never reads from anyone's
    filesystem.
    """
    if not filename.endswith(".md"):
        raise ValueError(".md files only")
    # A much higher ceiling than a message gets, and deliberately so: this is
    # the route bulk is supposed to take. fetch_md hands back one blob when an
    # agent asks for it, and none of this is copied into receipts. Checked
    # here so both surfaces inherit it, as the four message entry points do.
    check_size(content, MAX_MD_SIZE, f"{filename}")
    uri = _md_uri(channel, filename)
    size = len(content.encode("utf-8"))
    sha = hashlib.sha256(content.encode("utf-8")).hexdigest()

    def _persist() -> None:
        CONN.execute(
            """INSERT INTO md_files (uri, station_id, channel, sender,
                   filename, content, sha256, size, created_at)
               VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
               ON DUPLICATE KEY UPDATE
                   sender = VALUES(sender),
                   content = VALUES(content),
                   sha256 = VALUES(sha256),
                   size = VALUES(size),
                   created_at = VALUES(created_at)""",
            (uri, station_id, channel, sender, filename, content,
             sha, size, time.time()),
        )

    await _db(_persist)

    # The URI twice on purpose: once as a field, once inside the call the
    # reader should make. This line used to read "Fetch via MCP resources/read
    # or the fetch_md tool", and an agent handed a plan went looking for a
    # resource server, could not find one, and asked its peer to paste the
    # body into the channel instead. resources/read is a templated resource
    # most clients never surface; the tool is the route that works, so the
    # tool is what this says, spelled as a call that can be copied.
    body = (
        f"[md file shared]\n"
        f"uri: {uri}\n"
        f"size: {size} bytes  sha256: {sha[:12]}\n"
        + (f"note: {note}\n" if note else "")
        + f'Read it with fetch_md(uri="{uri}") — the whole file comes back '
          "in one call."
    )
    return await CHANNELS.post(station_id, channel, sender, body)


async def md_get(station_id: str, uri: str) -> dict:
    """One stored blob and its metadata, or KeyError. Station-scoped."""

    def _do() -> dict | None:
        row = CONN.execute(
            "SELECT * FROM md_files WHERE station_id = %s AND uri = %s",
            (station_id, uri),
        ).fetchone()
        if row is None:
            return None
        return {
            "uri": row["uri"],
            "channel": row["channel"],
            "sender": row["sender"],
            "filename": row["filename"],
            "size": row["size"],
            "sha256": row["sha256"],
            "content": row["content"],
            "created_at": row["created_at"],
        }

    out = await _db(_do)
    if out is None:
        raise KeyError(f"no md resource at {uri}")
    return out


@mcp.tool()
async def share_md(
    channel: str,
    sender: str,
    filename: str,
    content: str,
    note: str = "",
) -> dict:
    """Upload a markdown blob and reference it from `channel`'s transcript.

    For anything too long to post: the channel gets a short message carrying
    the md:// URI, and whoever needs the text calls fetch_md on it. The caller
    supplies the bytes — the server never reads from anyone's filesystem.
    """
    return _receipt(await md_store(require_station(), channel, sender,
                                   filename, content, note))


@mcp.tool()
async def fetch_md(uri: str) -> dict:
    """Read a markdown file somebody shared, by its md:// URI.

    The URI is not a path on anyone's disk and not a resource server you have
    to connect to — it is the argument to this tool. The whole file comes back
    in one call; its size is in the channel message that announced it.
    """
    return await md_get(require_station(), uri)


# The two resource templates are kept for the clients that do surface them —
# they are why the scheme is md:// at all — but they are the route that fails
# silently everywhere else, so nothing points at them any more. ASYNC, because
# a synchronous CONN.execute here blocked the event loop, and a 512 KiB blob
# read that way stalls every live /stream for its duration.
@mcp.resource("md://channel/{channel}/{filename}")
async def _md_channel_resource(channel: str, filename: str) -> str:
    """Channel-scoped markdown resource. Station-filtered."""
    uri = _md_uri(channel, filename)
    try:
        return (await md_get(require_station(), uri))["content"]
    except KeyError:
        raise ValueError(f"{uri} not found")


@mcp.resource("md://global/{filename}")
async def _md_global_resource(filename: str) -> str:
    """Operator-scoped markdown resource (no channel). Station-filtered.

    Nothing writes one: share_md requires a channel. Kept because the scheme
    has two halves and removing one silently would be worse than an empty one.
    """
    uri = _md_uri(None, filename)
    try:
        return (await md_get(require_station(), uri))["content"]
    except KeyError:
        raise ValueError(f"{uri} not found")


# ---------------------------------------------------------------------------
# Channel stream: per-agent, bounded backlog + live.
#
# Several agents of one human share a single station token, so /stream segments
# by the ?agent=<id> query param. An agent receives:
#   - messages that @mention it (targeted), from any channel, plus
#   - untargeted channel broadcasts (no @ at all) in channels it belongs to,
# never its own posts. A message aimed at a different agent is not delivered.
# It keeps a private per-(station, agent) cursor so agents don't starve each
# other's backlog. Without ?agent it falls back to the whole-station firehose.
# Backlog is bounded; the consumer (an iTerm2 coprocess running `curl -N`)
# injects each line as keystrokes, so every message is one line + one newline.
# ---------------------------------------------------------------------------

def _get_stream_cursor(station_id: str, agent_id: str = "") -> float:
    row = CONN.execute(
        "SELECT last_ts FROM stream_cursors "
        "WHERE station_id = %s AND agent_id = %s",
        (station_id, agent_id),
    ).fetchone()
    return float(row["last_ts"]) if row else 0.0


def _set_stream_cursor(station_id: str, ts: float, agent_id: str = "") -> None:
    CONN.execute(
        "INSERT INTO stream_cursors (station_id, agent_id, last_ts) "
        "VALUES (%s, %s, %s) "
        "ON DUPLICATE KEY UPDATE "
        "last_ts = VALUES(last_ts)",
        (station_id, agent_id, ts),
    )


def _fetch_backlog(
    station_id: str, since: float, limit: int
) -> tuple[list[dict], bool]:
    """Whole-station firehose: the most recent `limit` messages after `since`,
    oldest-first, plus a flag for whether older ones were skipped."""
    rows = CONN.execute(
        "SELECT * FROM transcripts WHERE station_id = %s AND ts > %s "
        "ORDER BY ts DESC LIMIT %s",
        (station_id, since, limit + 1),
    ).fetchall()
    truncated = len(rows) > limit
    rows = list(reversed(rows[:limit]))
    return [_row_to_transcript(r) for r in rows], truncated


def _member_channels(station_id: str, agent_id: str) -> list[str]:
    """Channels in this station that `agent_id` is a member of."""
    rows = CONN.execute(
        "SELECT name, members FROM channels WHERE station_id = %s",
        (station_id,),
    ).fetchall()
    return [
        r["name"] for r in rows
        if agent_id in json.loads(r["members"] or "[]")
    ]


def _resolve_receipts(station_id: str, rows: list) -> list[dict]:
    """Turn receipt rows into deliverable messages by looking up each parent.

    A receipt whose message is gone is skipped (the collector sweeps it), and a
    broadcast that is no longer open is skipped too — it is not actionable any
    more, and `close()` acks it so it stops being pending.
    """
    msgs: list[dict] = []
    for r in rows:
        mid, kind = r["msg_id"], r["kind"]
        if kind == "channel":
            t = CONN.execute(
                "SELECT * FROM transcripts WHERE id = %s AND station_id = %s",
                (mid, station_id),
            ).fetchone()
            if t is None:
                continue
            m = _row_to_transcript(t)
            m["kind"] = "channel"
            # AUDIENCE: everyone who received it and owes an ack. Frozen at
            # post time, and the receipts are right here, so it costs one
            # indexed read.
            m["audience"] = [
                x["agent_id"] for x in CONN.execute(
                    "SELECT agent_id FROM message_receipts WHERE station_id = %s"
                    " AND kind = 'channel' AND msg_id = %s ORDER BY agent_id",
                    (station_id, mid),
                ).fetchall()
            ]
            # ADDRESSED: who it was FOR. Everyone above received it; these are
            # the ones the sender was answering. Empty on a post to the room,
            # which is most of them — and empty MEANS the room, which is why
            # the key is always present.
            m["addressed"] = [
                x["agent_id"] for x in CONN.execute(
                    "SELECT agent_id FROM message_addressees "
                    "WHERE station_id = %s AND msg_id = %s ORDER BY agent_id",
                    (station_id, mid),
                ).fetchall()
            ]
            msgs.append(m)
        elif kind == "dm":
            d = CONN.execute(
                "SELECT * FROM dms WHERE id = %s AND station_id = %s",
                (mid, station_id),
            ).fetchone()
            if d is None:
                continue
            msgs.append({
                "kind": "dm", "id": d["id"], "channel": "dm",
                "sender": d["sender"], "text": d["text"], "ts": d["ts"],
                # Both sets on every message, DMs included: a DM goes to one
                # agent and is unambiguously FOR them, so the two are equal
                # here. Stating it beats making a reader infer it from a
                # missing field.
                "audience": [d["recipient"]],
                "addressed": [d["recipient"]],
                **({"expires_at": d["expires_at"]}
                   if "expires_at" in d.keys() and d["expires_at"] else {}),
            })
        elif kind == "broadcast":
            b = CONN.execute(
                "SELECT * FROM broadcasts WHERE id = %s AND station_id = %s",
                (mid, station_id),
            ).fetchone()
            if b is None or b["status"] != "open":
                continue
            msgs.append({
                "kind": "broadcast", "id": b["id"], "channel": "broadcast",
                "sender": b["sender"], "text": b["problem"],
                "ts": b["created_at"],
                # The candidates it went out to, and nobody in particular:
                # a broadcast is a call to whoever can take it, so `addressed`
                # is empty by construction rather than by omission.
                "audience": [
                    x["agent_id"] for x in CONN.execute(
                        "SELECT agent_id FROM message_receipts WHERE "
                        "station_id = %s AND kind = 'broadcast' AND "
                        "msg_id = %s ORDER BY agent_id", (station_id, mid),
                    ).fetchall()
                ],
                "addressed": [],
            })
    msgs.sort(key=lambda m: m["ts"])
    return msgs


def _fetch_for_agent(
    station_id: str, agent_id: str, limit: int, replay: bool = False
) -> tuple[list[dict], bool]:
    """What `agent_id` should receive now, oldest first, and whether more was
    left behind.

    There is no scanning and no cursor: the audience was decided when each
    message was posted (see message_receipts), so this is simply "my receipts
    that have not been handed to me yet". A newcomer therefore starts empty —
    it is in no receipt written before it arrived.

    `replay=True` also re-sends what was delivered but never acked, so a push
    that died between the socket write and the client reading it is not lost.
    That is a recovery window of seconds, so it is BOUNDED: only redeliveries
    newer than STREAM_REPLAY_WINDOW come back. Without the bound, every
    reconnect re-pushes everything the agent ever read and never acked — the
    agent sees the same stale DMs on every boot and answers them again. The
    messages are not lost by this: they stay unacked, stay in my_pending and
    are still never collected; they simply stop being shoved into a session
    that has already seen them.

    Marking delivered_at is deliberately not marking acked_at: if the client
    dies between this call and the write to the socket, the message stays
    unacked, stays in my_pending, and is never collected.
    """
    sql = (
        "SELECT msg_id, kind, ts FROM message_receipts "
        "WHERE station_id = %s AND agent_id = %s AND acked_at IS NULL"
    )
    params: list = [station_id, agent_id, time.time()]
    sql += " AND expires_at > %s"
    if replay:
        sql += " AND (delivered_at IS NULL OR delivered_at > %s)"
        params.append(time.time() - STREAM_REPLAY_WINDOW)
    else:
        sql += " AND delivered_at IS NULL"
    sql += " ORDER BY ts ASC LIMIT %s"
    params.append(limit + 1)
    rows = CONN.execute(sql, params).fetchall()
    truncated = len(rows) > limit
    rows = rows[:limit]

    msgs = _resolve_receipts(station_id, rows)
    # Stamp every receipt we consumed, including ones whose parent vanished —
    # otherwise a dangling receipt would be re-read on every single poll.
    _mark_delivered(station_id, agent_id, [r["msg_id"] for r in rows])
    return msgs, truncated


def _format_stream_line(m: dict, json_mode: bool = False) -> str:
    # One newline-delimited line per message. json_mode emits a structured
    # object for programmatic consumers (the channel server); the default is a
    # human-readable line. Either way the line carries no raw newline.
    if json_mode:
        # id + kind are what the client echoes back to ack_messages, so they
        # ride on every message, not just broadcasts.
        out = {"channel": m["channel"], "sender": m["sender"],
               "text": m["text"], "ts": m["ts"],
               "id": m.get("id", ""), "kind": m.get("kind") or "channel"}
        # Routing travels BESIDE the message, never inside it — and it has to
        # actually travel. Both keys, on EVERY message, always present:
        #   audience   everyone who got it and owes an ack
        #   addressed  who it was written for; empty MEANS the room
        # Unconditional because an absent key and an empty one are the same
        # thing to a reader who has to guess, and this pair is exactly what
        # decides whether a message wants an answer.
        out["audience"] = list(m.get("audience") or [])
        out["addressed"] = list(m.get("addressed") or [])
        if m.get("expires_at"):
            out["expires_at"] = m["expires_at"]
        if m.get("kind") == "broadcast":
            out["broadcast_id"] = m["id"]
        return json.dumps(out, ensure_ascii=False) + "\n"
    text = " ".join(str(m["text"]).split())
    if m.get("kind") == "broadcast":
        return (f"[a2a broadcast from {m['sender']}] {text} "
                f"(respond with submit_bid broadcast_id={m['id']})\n")
    if m.get("kind") == "dm":
        return f"[a2a DM from {m['sender']}] {text}\n"
    return f"[a2a #{m['channel']} from {m['sender']}] {text}\n"


# ---------------------------------------------------------------------------
# Auth middleware: Bearer token authenticates the user; the agent it names
# (X-A2A-Agent header, or ?agent= on /stream) selects the station.
# ---------------------------------------------------------------------------

PUBLIC_PATHS: tuple[str, ...] = (
    "/healthz", "/a2a-claudecode.tar.gz", "/a2a-claudecode.zip",
)
# /install/<token> carries its credential in the path, so it cannot present a
# bearer header; the handler validates the token itself and 404s otherwise.
PUBLIC_PREFIXES: tuple[str, ...] = ("/install/",)
AGENT_HEADER = b"x-a2a-agent"


def _extract_bearer(scope) -> str | None:
    for k, v in scope.get("headers") or []:
        if k == b"authorization":
            val = v.decode("latin-1", errors="ignore")
            if val.lower().startswith("bearer "):
                return val[7:].strip() or None
            return None
    return None


def _extract_agent(scope) -> str:
    """Agent id from ?agent= (wins, used by /stream) else X-A2A-Agent."""
    qs = (scope.get("query_string") or b"").decode("latin-1", errors="ignore")
    for part in qs.split("&"):
        if part.startswith("agent="):
            return normalize_agent_id(
                urllib.parse.unquote_plus(part[len("agent="):])
            )
    for k, v in scope.get("headers") or []:
        if k == AGENT_HEADER:
            return normalize_agent_id(v.decode("latin-1", errors="ignore"))
    return ""


async def _send_json(send, status: int, body: dict) -> None:
    payload = json.dumps(body).encode("utf-8")
    await send({
        "type": "http.response.start",
        "status": status,
        "headers": [
            (b"content-type", b"application/json"),
            (b"content-length", str(len(payload)).encode()),
        ],
    })
    await send({"type": "http.response.body", "body": payload})


class AuthMiddleware:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        path = scope["path"]
        if path in PUBLIC_PATHS or path.startswith(PUBLIC_PREFIXES):
            await self.app(scope, receive, send)
            return
        if path.startswith("/admin/") or path == "/admin":
            if not ADMIN_TOKEN:
                await _send_json(
                    send, 503,
                    {"error": "admin endpoints disabled "
                              "(A2A_ADMIN_TOKEN not set)"},
                )
                return
            tok = _extract_bearer(scope)
            if not tok or not secrets.compare_digest(tok, ADMIN_TOKEN):
                await _send_json(
                    send, 401, {"error": "invalid admin token"}
                )
                return
            await self.app(scope, receive, send)
            return
        agent = _extract_agent(scope)
        is_realm = path == "/me" or path.startswith("/me/")
        if AUTH_DISABLED:
            dev_auth = {
                "token_hash": "", "user": "(auth disabled)",
                "stations": [s["station_id"] for s in STATIONS.list()],
            }
            await self._run(
                scope, receive, send, DEFAULT_STATION_ID, agent, None, dev_auth,
            )
            return
        token = _extract_bearer(scope)
        if not token:
            await _send_json(send, 401, {"error": "missing bearer token"})
            return
        auth = TOKENS.resolve(token)
        if not auth:
            await _send_json(
                send, 403, {"error": "invalid or revoked token"}
            )
            return

        sid: str | None = None
        denial: str | None = None
        # The id arrives as-is and is matched as-is. Nothing is resolved or
        # redirected here: an agent has one name, and the client that owns it
        # is the thing that decides what to send.
        if agent:
            # The agent selects the tenant: it must exist, sit in a granted
            # station, and not be pinned to a different token. A failure here
            # is NOT fatal — the request proceeds with no station bound, so the
            # caller can still reach /me and the realm tools to provision
            # itself. Every station-scoped operation then fails with `denial`.
            try:
                sid = resolve_request_station(auth, agent)
            except AgentDenied as e:
                denial = str(e)
        elif not is_realm:
            # Nothing is inferred from a token alone: no agent, no station.
            await _send_json(
                send, 403,
                {"error": "agent name required: send X-A2A-Agent (or ?agent=)"},
            )
            return
        await self._run(scope, receive, send, sid, agent, denial, auth)

    async def _run(self, scope, receive, send, sid, agent, denial, auth):
        ctx = _current_station.set(sid)
        actx = _current_agent.set(agent or None)
        dctx = _current_denial.set(denial)
        authctx = _current_auth.set(auth)
        try:
            await self.app(scope, receive, send)
        finally:
            _current_station.reset(ctx)
            _current_agent.reset(actx)
            _current_denial.reset(dctx)
            _current_auth.reset(authctx)


# ---------------------------------------------------------------------------
# Starlette app: /healthz, station-scoped REST, /admin/*, MCP at /mcp.
# ---------------------------------------------------------------------------

def build_app() -> Starlette:
    async def healthz(_: Request) -> JSONResponse:
        # The version is here because "is my container running the new code?"
        # is otherwise unanswerable from outside, and guessing at it has
        # already cost real debugging time — a feature looked missing when it
        # was only unbuilt. This is the one public path, so it is the one
        # place that answer can live.
        # `clients` is the version of the client tree this broker SERVES.
        # It is the same number today, because everything ships from one tree
        # — but a client compares against this rather than against `version`,
        # so the two can diverge later without every client lying about it.
        return JSONResponse({"ok": True, "version": VERSION,
                             "clients": VERSION})

    async def _serve_plugin(request: Request, fmt: str):
        """The Claude Code plugin archive, packed from source per request.

        Public and token-free: it carries no secret. The station token is
        entered per machine through the plugin's own setup screen.
        """
        if not PLUGIN_DIR.is_dir():
            return JSONResponse(
                {"error": "plugin source not installed on this broker"},
                status_code=404,
            )
        ext = "zip" if fmt == "zip" else "tar.gz"
        # The manifest names the broker, and until now it named ONE broker:
        # this archive was packed verbatim, so the url committed to source was
        # the url every Claude Code install used. Anyone else running this
        # repo shipped a plugin pointing at somebody else's server, with their
        # own token. The url is a property of the deployment, so the
        # deployment fills it in — the same trick the Pi and OpenCode
        # installers already use to bake in credentials.
        base = PUBLIC_URL or f"{request.url.scheme}://{request.url.netloc}"
        manifest = json.loads((PLUGIN_DIR / ".mcp.json").read_text())
        servers = manifest.get("mcpServers", {})
        if "a2a" in servers:
            servers["a2a"]["url"] = f"{base}/mcp/"
        if "a2a-channel" in servers:
            env = servers["a2a-channel"].setdefault("env", {})
            env["A2A_URL"] = base
            # Same purpose as the baked `version` in the other clients: this
            # install can then tell that the broker has moved on. A missing
            # tool with no explanation is what this costs to avoid.
            env["A2A_CLIENT_VERSION"] = VERSION
        body = await _db(lambda: _archive_dir(
            PLUGIN_DIR, fmt,
            rewrite={".mcp.json":
                     json.dumps(manifest, indent=2).encode() + b"\n"},
        ))
        return Response(
            body,
            media_type="application/zip" if fmt == "zip" else "application/gzip",
            headers={
                "Content-Disposition":
                    f'attachment; filename="a2a-claudecode.{ext}"',
                # Nothing may hold a copy: a reinstall must always fetch what
                # the broker has now, not what a proxy cached this morning.
                "Cache-Control": "no-store, must-revalidate",
            },
        )

    async def plugin_targz(request: Request):
        return await _serve_plugin(request, "tar.gz")

    async def plugin_zip(request: Request):
        """DEPRECATED — kept so an install line from an older README still
        works. `curl … | tar -xf -` on a zip only succeeds on macOS, where tar
        is libarchive; GNU tar refuses it. The tar.gz route is the portable
        one and is what the docs now say."""
        return await _serve_plugin(request, "zip")

    async def pi_install(request: Request):
        """One-command installer for the Pi extension:

            mkdir -p ~/.pi/agent/extensions/a2a && \
            curl -fsSL https://<host>/install/pi/<token> \
                 | tar -xzf - -C ~/.pi/agent/extensions/a2a

        A directory rather than one file, because the extension needs a
        package.json beside it: Pi does not re-export typebox, and it runs
        npm install for a declared dependency by itself.

        Credentials are baked as a `globalThis` assignment rather than a
        `const`, so the source stays valid TypeScript on its own — a prepended
        `const` would collide with any declaration of the same name.

        Unknown, revoked and not-installed all answer 404 with the same body,
        so this cannot be used to probe which tokens exist.
        """
        missing = JSONResponse(
            {"error": "no installer available"}, status_code=404
        )
        token = request.path_params["token"]
        if not (PI_DIR / "index.ts").is_file() or not await _db(
            lambda: TOKENS.resolve(token)
        ):
            return missing

        base = PUBLIC_URL or f"{request.url.scheme}://{request.url.netloc}"
        baked = json.dumps({
            "url": base,
            "token": token,
            "station": request.query_params.get("station") or "",
            "agent": request.query_params.get("agent") or "",
            # Stamped at pack time, so a client always knows which tree it
            # came from without a constant anybody has to remember to bump.
            # It compares this against /healthz's `clients` at startup and
            # says so, once, when it is behind.
            "version": VERSION,
        })
        head = f"globalThis.A2A_BAKED = {baked};\n".encode()
        body = await _db(lambda: _archive_dir(
            PI_DIR, "tar.gz",
            rewrite={"index.ts": head + (PI_DIR / "index.ts").read_bytes()},
        ))
        return Response(
            body, media_type="application/gzip",
            headers={
                "Content-Disposition": 'attachment; filename="a2a-pi.tar.gz"',
                "Cache-Control": "no-store, must-revalidate",
            },
        )

    async def codex_install(request: Request):
        """One-command installer for the Codex client:

            mkdir -p ~/.codex/a2a && \
            curl -fsSL https://<host>/install/codex/<token> \
                 | tar -xzf - -C ~/.codex/a2a

        A directory (the client alone). No launcher ships: push comes from one
        line of plain codex — `codex app-server --listen unix://$TMPDIR/a2a-$$
        .sock & sleep 1; codex --remote unix://$TMPDIR/a2a-$$.sock` — and the
        client finds that socket from its own parent app-server, then stops it
        when the session ends. The third piece of the install line is codex's
        own registration:
        `codex mcp add a2a -- python3 ~/.codex/a2a/a2a-codex.py`.

        Credentials are baked by REPLACING the client's inert
        `A2A_BAKED = {}` line rather than prepending — the file opens with a
        module docstring, and an assignment shoved above it would silently
        demote that docstring to a no-op expression.

        Unknown, revoked and not-installed all answer 404 with the same body,
        so this cannot be used to probe which tokens exist.
        """
        missing = JSONResponse(
            {"error": "no installer available"}, status_code=404
        )
        token = request.path_params["token"]
        client = CODEX_DIR / "a2a-codex.py"
        if not client.is_file() or not await _db(
            lambda: TOKENS.resolve(token)
        ):
            return missing

        base = PUBLIC_URL or f"{request.url.scheme}://{request.url.netloc}"
        baked = json.dumps({
            "url": base,
            "token": token,
            "station": request.query_params.get("station") or "",
            "agent": request.query_params.get("agent") or "",
            # Stamped at pack time, so a client always knows which tree it
            # came from without a constant anybody has to remember to bump.
            "version": VERSION,
        })
        src = client.read_bytes()
        marker = b"A2A_BAKED = {}"
        if marker not in src:
            # The client was edited and the seam is gone. Refusing beats
            # serving a client that would start with no credentials.
            return JSONResponse(
                {"error": "installer source is missing its A2A_BAKED seam"},
                status_code=500,
            )
        src = src.replace(marker, f"A2A_BAKED = {baked}".encode(), 1)
        body = await _db(lambda: _archive_dir(
            CODEX_DIR, "tar.gz", rewrite={"a2a-codex.py": src},
        ))
        return Response(
            body, media_type="application/gzip",
            headers={
                "Content-Disposition":
                    'attachment; filename="a2a-codex.tar.gz"',
                "Cache-Control": "no-store, must-revalidate",
            },
        )

    async def opencode_install(request: Request):
        """Serve a one-command installer for the OpenCode client plugin:

            mkdir -p ~/.config/opencode/plugins && \
            curl -fsSL https://<host>/install/<token> \
                 -o ~/.config/opencode/plugins/a2a-opencode.js

        The token in the path is the credential — this route is public because
        a `curl | bash` cannot carry a bearer header. It is validated here and
        baked into the served plugin, so the user never handles it again. No
        station is resolved: the agent that selects one does not exist yet, and
        the plugin registers itself via /me/agents on first run.

        Unknown, revoked and not-installed all answer 404 with the same body,
        so this cannot be used to probe which tokens exist.
        """
        missing = JSONResponse(
            {"error": "no installer available"}, status_code=404
        )
        token = request.path_params["token"]
        if not OPENCODE_JS.is_file() or not await _db(
            lambda: TOKENS.resolve(token)
        ):
            return missing

        base = PUBLIC_URL or f"{request.url.scheme}://{request.url.netloc}"
        # Baked so a machine needs no environment of its own. `agent` is an
        # optional starting id; without it the plugin uses the project
        # directory and the agent renames itself from there. The old
        # suffix/tail/n knobs are gone — they existed to hand-avoid id
        # collisions, which the plugin's identity store now prevents outright.
        baked = json.dumps({
            "url": base,
            "token": token,
            "station": request.query_params.get("station") or "",
            "agent": request.query_params.get("agent") or "",
            # Stamped at pack time, so a client always knows which tree it
            # came from without a constant anybody has to remember to bump.
            # It compares this against /healthz's `clients` at startup and
            # says so, once, when it is behind.
            "version": VERSION,
        })
        # The plugin itself, credentials prepended — one file, downloaded
        # straight into the plugins dir. No archive: there is nothing to pack.
        return PlainTextResponse(
            f"const A2A_BAKED = {baked}\n{OPENCODE_JS.read_text()}",
            media_type="application/javascript",
            headers={"Cache-Control": "no-store, must-revalidate"},
        )

    async def stream_route(request: Request) -> StreamingResponse:
        """Stream the caller's messages: pending first, then live.

        With ?agent= this is receipt-driven — it delivers exactly the messages
        whose audience includes that agent and which it has not acked, so a
        newly-registered agent starts silent rather than swallowing history.
        The first pass of each connection replays unacked messages (resume);
        after that, only new ones.

        Query params:
          agent           segment to this agent. Required when several agents
                          share one token; omit only for a whole-station feed.
          exclude_sender  also skip messages from this sender.
          since           start ts for the station-wide feed (no ?agent=).
                          Ignored for an agent stream, which uses receipts.
        """
        # Raises AuthRequired -> 403 before any stream is opened, so an
        # unregistered agent gets a clear refusal instead of a silent stream.
        station_id = require_station()
        # Keep the raw token so a long-lived stream can be re-checked: a token
        # revoked after connect must stop receiving, not run until disconnect.
        token = _extract_bearer(request.scope)
        revalidate = not AUTH_DISABLED and token is not None
        agent_id = normalize_agent_id(request.query_params.get("agent") or "")
        json_mode = request.query_params.get("format") == "json"
        since_param = request.query_params.get("since")
        use_server_cursor = since_param is None
        # Never echo an agent its own posts; honour an extra exclusion too.
        exclude = {agent_id} if agent_id else set()
        extra = request.query_params.get("exclude_sender")
        if extra:
            exclude.add(extra)

        async def gen():
            ev = asyncio.Event()
            wakers = _stream_wakers.setdefault(station_id, set())
            wakers.add(ev)
            # Claim this agent's stream BEFORE the first fetch, and wake the
            # station so a predecessor parked on ev.wait() re-checks now
            # rather than at its next keepalive. Agent streams only: several
            # operators may legitimately watch the firehose at once.
            my_claim = uuid.uuid4().hex
            if agent_id:
                stale = (station_id, agent_id) in _STREAM_OWNERS
                _STREAM_OWNERS[(station_id, agent_id)] = my_claim
                _wake_station(station_id)
                if stale:
                    log(f"stream for {agent_id!r} superseded by a newer "
                        f"connection — evicting the previous one",
                        event="stream.supersede")
            # Every id this connection yields and the client has not yet
            # acked is OURS to give back: stamping delivered_at happens at
            # fetch time, before the bytes reach anyone, so a connection that
            # ends owes the difference between what it took and what was
            # confirmed. Settled in the finally below.
            delivered_here: set[str] = set()

            async def _advance(ts: float) -> None:
                # Only the station-wide feed still has a cursor; an agent
                # stream tracks state per message in message_receipts.
                if use_server_cursor and not agent_id:
                    await _db(
                        lambda t=ts: _set_stream_cursor(station_id, t, agent_id)
                    )

            try:
                if agent_id:
                    cursor = 0.0
                elif use_server_cursor:
                    cursor = await _db(
                        lambda: _get_stream_cursor(station_id, agent_id)
                    )
                else:
                    try:
                        cursor = float(since_param)
                    except (TypeError, ValueError):
                        cursor = 0.0

                next_auth_check = time.time() + STREAM_KEEPALIVE
                noted = False
                # TWO replay passes, not one. The first covers the client's
                # own restart, as before. The second exists for the handover
                # race: a predecessor evicted below may have been mid-fetch
                # while our first pass ran, stamping a batch delivered into
                # its dead socket after we had already looked. Those stamps
                # are seconds old, so a second replay pass re-fetches them;
                # clients dedupe by id, so the rare double push is harmless.
                replays_left = 2
                while True:
                    if await request.is_disconnected():
                        return
                    # Superseded? A newer connection for this agent claimed
                    # the stream (its claim also woke us). Exit before
                    # fetching, so a zombie cannot steal one more batch.
                    if agent_id and _STREAM_OWNERS.get(
                        (station_id, agent_id)
                    ) != my_claim:
                        return
                    # Re-check the token periodically; cut the stream the moment
                    # it is revoked, loses the grant, or the agent is rebound.
                    if revalidate and time.time() >= next_auth_check:
                        def _still_valid() -> bool:
                            a = TOKENS.resolve(token)
                            if not a:
                                return False
                            if not agent_id:
                                return station_id in (a.get("stations") or [])
                            try:
                                return resolve_request_station(
                                    a, agent_id
                                ) == station_id
                            except AgentDenied:
                                return False

                        if not await _db(_still_valid):
                            return
                        next_auth_check = time.time() + STREAM_KEEPALIVE

                    ev.clear()
                    if agent_id:
                        # First pass of this connection replays anything the
                        # agent never acked (resume / restart); after that only
                        # genuinely undelivered messages.
                        batch, truncated = await _db(
                            lambda r=replays_left > 0: _fetch_for_agent(
                                station_id, agent_id,
                                STREAM_BACKLOG_LIMIT, replay=r,
                            )
                        )
                        replays_left = max(0, replays_left - 1)
                        new_cursor = cursor
                    else:
                        batch, truncated = await _db(
                            lambda c=cursor: _fetch_backlog(
                                station_id, c, STREAM_BACKLOG_LIMIT
                            )
                        )
                        new_cursor = batch[-1]["ts"] if batch else cursor

                    if truncated and not noted and not json_mode:
                        yield (
                            f"[a2a] more than {STREAM_BACKLOG_LIMIT} messages "
                            "pending; delivering oldest first, in batches\n"
                            if agent_id else
                            "[a2a] earlier messages skipped; showing the "
                            f"latest {STREAM_BACKLOG_LIMIT}\n"
                        )
                        noted = True
                    for m in batch:
                        # DMs are already addressed to this agent, so the
                        # own-posts filter must not apply — a DM to yourself is
                        # the channel self-test and has to come back.
                        if m.get("kind") == "dm" or m["sender"] not in exclude:
                            yield _format_stream_line(m, json_mode)
                            if agent_id and m.get("id"):
                                delivered_here.add(m["id"])

                    if new_cursor > cursor:
                        cursor = new_cursor
                        await _advance(cursor)

                    if batch:
                        continue  # re-check for more before blocking

                    try:
                        await asyncio.wait_for(
                            ev.wait(), timeout=STREAM_KEEPALIVE
                        )
                    except asyncio.TimeoutError:
                        # Keepalive: a blank line the json consumer ignores.
                        # It forces reverse proxies to flush their buffer and
                        # resets their read timeout — without it, a default
                        # nginx holds or kills the quiet stream and delivery
                        # looks dead even though the server is fine. The loop
                        # then also re-polls the DB, which bounds delivery of
                        # writes made by OTHER processes (CLI) to one tick.
                        if json_mode:
                            yield "\n"
                        # Quiet tick: cheap moment to retire finished
                        # messages. Debounced to COLLECT_INTERVAL, so many
                        # parked streams still collect at most once each.
                        await _db(lambda: _maybe_collect(station_id))
            finally:
                wakers.discard(ev)
                # Release only OUR claim: if a successor has already taken
                # over, deleting here would orphan its live stream.
                if agent_id and _STREAM_OWNERS.get(
                    (station_id, agent_id)
                ) == my_claim:
                    del _STREAM_OWNERS[(station_id, agent_id)]
                # Give back what this connection took and never got confirmed.
                # Un-acked here means the client never saw it — these clients
                # ack on delivery — so clearing delivered_at returns the
                # message to "undelivered" and the NEXT connection fetches it
                # plainly, however old it is. This is what makes an abandoned
                # connection cost nothing: without it, anything stamped into a
                # dead socket more than STREAM_REPLAY_WINDOW before the next
                # reconnect was stranded unacked until expiry — delivered to
                # nobody, redelivered to nobody. The replay pass still exists
                # for the one exit this cannot cover: a broker killed too hard
                # to run this block.
                if agent_id and delivered_here:
                    ids = list(delivered_here)

                    def _unstamp() -> int:
                        ph = ",".join(["%s"] * len(ids))
                        return CONN.execute(
                            f"UPDATE message_receipts SET delivered_at = NULL "
                            f"WHERE station_id = %s AND agent_id = %s "
                            f"AND acked_at IS NULL AND msg_id IN ({ph})",
                            (station_id, agent_id, *ids),
                        ).rowcount
                    try:
                        n = await _db(_unstamp)
                        if n:
                            log(f"stream for {agent_id!r} ended with {n} "
                                f"delivered-but-unacked message(s); returned "
                                f"them to the queue", event="stream.unstamp")
                    except Exception:
                        # Shutting-down event loop or dead pool: the replay
                        # window remains the fallback, as it always was.
                        pass

        return StreamingResponse(
            gen(),
            media_type="text/plain; charset=utf-8",
            headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
        )

    # ----- /me: token realm (no agent registration required) ----------------

    async def me_route(_: Request) -> JSONResponse:
        auth = require_auth()
        view = await _db(lambda: AGENTS.realm_view(auth, current_agent()))
        denial = _current_denial.get()
        if denial and not view["registered"]:
            view["denied"] = denial
        return JSONResponse(view)

    async def me_agents_route(_: Request) -> JSONResponse:
        auth = require_auth()
        view = await _db(lambda: AGENTS.realm_view(auth, current_agent()))
        return JSONResponse({"agents": view["agents"]})

    def _proposal_station(auth: dict, asked: str | None) -> str:
        """Which station a proposal lands in.

        The token's own grant, and only that. Named explicitly when the token
        reaches more than one, because guessing would file the request where
        nobody is looking for it.
        """
        granted = auth.get("stations") or []
        if not granted:
            raise PermissionError("this token is not allowed in any station")
        if asked:
            st = STATIONS.get(asked)
            if not st or st["station_id"] not in granted:
                raise PermissionError(
                    f"this token has no access to station {asked!r}"
                )
            return st["station_id"]
        if len(granted) > 1:
            names = ", ".join(
                (STATIONS.get(g) or {}).get("name", g) for g in granted
            )
            raise ValueError(
                f"this token reaches several stations ({names}); name one "
                f"with \"station\""
            )
        return granted[0]

    async def me_propose_route(request: Request) -> JSONResponse:
        """Ask for an agent name. Reachable while UNREGISTERED — that is the
        entire point, and it works because AuthMiddleware exempts /me/* from
        the unknown-agent denial so a client can provision itself.

        This creates nothing an agent can use. Only an operator, on the TUI or
        CLI, can turn a proposal into an agent.
        """
        auth = require_auth()
        try:
            body = await request.json()
        except Exception:
            body = {}
        try:
            sid = _proposal_station(auth, body.get("station"))
            out = await _db(lambda: PROPOSALS.propose(
                sid,
                normalize_agent_id(body.get("agent_id") or ""),
                auth["token_hash"],
                (body.get("note") or "")[:200],
            ))
        except PermissionError as e:
            return JSONResponse({"error": str(e)}, status_code=403)
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        out["note"] = (
            "waiting for an operator to approve it. Nothing to do but keep "
            "streaming — this client connects with no restart once approved."
        )
        if out.get("kind") == "transfer":
            out["note"] = (
                "that name belongs to another client, so this is a TRANSFER "
                "request and an operator has to agree to move it. Nothing to "
                "do but keep streaming — this client connects with no restart "
                "if it is granted. If it is refused, asking again is barred "
                "for a while, so do not retry in a loop."
            )
        return JSONResponse(out, status_code=201)

    async def me_proposals_route(_: Request) -> JSONResponse:
        auth = require_auth()
        rows = await _db(lambda: PROPOSALS.list(token_hash=auth["token_hash"]))
        now = time.time()
        return JSONResponse({"proposals": [
            {"agent_id": r["agent_id"], "station": r.get("station_name"),
             "note": r["note"], "expires_at": r["expires_at"],
             "expires_in": max(0, int(r["expires_at"] - now))}
            for r in rows
        ]})

    async def me_withdraw_proposal_route(request: Request) -> JSONResponse:
        auth = require_auth()
        agent_id = normalize_agent_id(request.path_params["agent_id"])
        try:
            sid = _proposal_station(
                auth, request.query_params.get("station")
            )
            out = await _db(lambda: PROPOSALS.withdraw(
                sid, agent_id, auth["token_hash"]
            ))
        except PermissionError as e:
            return JSONResponse({"error": str(e)}, status_code=403)
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        return JSONResponse({"withdrawn": out["agent_id"]})

    async def me_update_agent_route(request: Request) -> JSONResponse:
        auth = require_auth()
        body = await request.json()
        agent_id = normalize_agent_id(request.path_params["agent_id"])
        out: dict = {"agent_id": agent_id}
        try:
            if body.get("station"):
                out.update(await _db(
                    lambda: AGENTS.realm_move(auth, agent_id, body["station"])
                ))
            if "bind" in body:
                out.update(await _db(
                    lambda: AGENTS.realm_bind(auth, agent_id, bool(body["bind"]))
                ))
            if body.get("rename"):
                # The endpoint every client's rename tool calls. The broker
                # renames the row; recording the new id locally is the client's
                # half, and without it the client keeps announcing the old one.
                out.update(await _db(lambda: AGENTS.realm_rename(
                    auth, agent_id, body["rename"],
                )))
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        return JSONResponse(out)

    async def list_agents_route(request: Request) -> JSONResponse:
        ex = request.query_params.getlist("expertise") or None
        pj = request.query_params.getlist("project") or None
        return JSONResponse(
            {"agents": AGENTS.list(require_station(), ex, pj)}
        )

    async def get_agent_route(request: Request) -> JSONResponse:
        p = AGENTS.get(require_station(), request.path_params["agent_id"])
        if p is None:
            return JSONResponse({"error": "not found"}, status_code=404)
        return JSONResponse(p)

    async def update_agent_route(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            p = await AGENTS.update(
                require_station(),
                request.path_params["agent_id"], **body
            )
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(p)

    async def create_channel_route(request: Request) -> JSONResponse:
        body = await request.json()
        me = current_agent()
        members = list(body.get("members") or [])
        if me and me not in members:
            members.append(me)
        try:
            ch = await CHANNELS.create(
                require_station(), body["name"], body.get("theme", ""),
                members, body.get("policy"),
            )
        except ACLViolation as e:
            return JSONResponse({"error": str(e)}, status_code=403)
        except KeyError as e:
            return JSONResponse({"error": f"missing {e}"}, status_code=400)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        return JSONResponse(ch, status_code=201)

    async def list_channels_route(_: Request) -> JSONResponse:
        return JSONResponse({"channels": await CHANNELS.list(require_station())})

    async def get_channel_route(request: Request) -> JSONResponse:
        limit = request.query_params.get("limit")
        try:
            ch = await CHANNELS.get(
                require_station(),
                request.path_params["name"],
                limit=int(limit) if limit else None,
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(ch)

    async def add_member_route(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            s = await CHANNELS.add_member(
                require_station(),
                request.path_params["name"], body["agent_id"]
            )
        except ACLViolation as e:
            return JSONResponse({"error": str(e)}, status_code=403)
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(s)

    async def remove_member_route(request: Request) -> JSONResponse:
        try:
            s = await CHANNELS.remove_member(
                require_station(),
                request.path_params["name"],
                request.path_params["agent_id"],
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(s)

    async def evict_route(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            result = await CHANNELS.evict_off_project(
                require_station(),
                request.path_params["name"], body.get("project") or ""
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(result)

    async def post_message_route(request: Request) -> JSONResponse:
        body = await request.json()
        name = request.path_params["name"]
        try:
            result = await CHANNELS.post(
                require_station(), name, body["sender"], body["text"],
                body.get("expires_in"), body.get("addressed"),
            )
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(result)

    async def list_messages_route(request: Request) -> JSONResponse:
        since = request.query_params.get("since")
        limit = request.query_params.get("limit") or "50"
        sid = require_station()
        try:
            tx = await CHANNELS.messages_since(
                sid,
                request.path_params["name"],
                since=float(since) if since else None,
                limit=int(limit),
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        me = current_agent()
        if me and tx:
            ids = [m["id"] for m in tx]
            await _db(lambda: _mark_read(sid, me, ids))
            await _db(lambda: _maybe_collect(sid))
        await _db(lambda: _maybe_collect(sid))
        return JSONResponse({"transcript": tx})

    # ----- pending / ack ----------------------------------------------------

    async def pending_route(request: Request) -> JSONResponse:
        sid = require_station()
        me = current_agent()
        if not me:
            return JSONResponse(
                {"error": "this request names no agent, so it has no inbox"},
                status_code=400,
            )
        limit = int(request.query_params.get("limit") or 50)

        def _do() -> dict:
            rows = _pending_rows(sid, me, limit + 1)
            more = len(rows) > limit
            rows = rows[:limit]
            msgs = _resolve_receipts(sid, rows)
            _mark_read(sid, me, [r["msg_id"] for r in rows])
            total = CONN.execute(
                "SELECT COUNT(*) AS n FROM message_receipts "
                "WHERE station_id = %s AND agent_id = %s AND acked_at IS NULL",
                (sid, me),
            ).fetchone()["n"]
            return {"agent_id": me, "pending_total": total,
                    "returned": len(msgs), "has_more": more,
                    "messages": msgs}

        return JSONResponse(await _db(_do))

    async def ack_route(request: Request) -> JSONResponse:
        sid = require_station()
        me = current_agent()
        if not me:
            return JSONResponse(
                {"error": "this request names no agent, so it can ack nothing"},
                status_code=400,
            )
        body = await request.json()
        ids = [str(i) for i in (body.get("ids") or []) if str(i)]
        acked = await _db(lambda: _ack_receipts(sid, me, ids))
        total = await _db(lambda: CONN.execute(
            "SELECT COUNT(*) AS n FROM message_receipts "
            "WHERE station_id = %s AND agent_id = %s AND acked_at IS NULL",
            (sid, me),
        ).fetchone()["n"])
        return JSONResponse({"acked": acked, "pending_total": total})

    async def ack_all_route(_: Request) -> JSONResponse:
        """The REST twin of the ack_all tool: this agent's whole inbox."""
        sid = require_station()
        me = current_agent()
        if not me:
            return JSONResponse(
                {"error": "this request names no agent, so it can ack nothing"},
                status_code=400,
            )
        out = await _db(lambda: screen(sid, me))
        if out["acked"]:
            await _db(lambda: _maybe_collect(sid))
        return JSONResponse({"acked": out["acked"],
                             "by_kind": out.get("by_kind", {}),
                             "pending_total": 0})

    # ----- direct messages --------------------------------------------------

    async def send_dm_route(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            out = await DIRECT.send(
                require_station(),
                body.get("sender") or current_agent() or "",
                body.get("to") or body.get("recipient") or "",
                body.get("text") or body.get("message") or "",
                body.get("expires_in"),
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        return JSONResponse(out, status_code=201)

    async def list_dms_route(request: Request) -> JSONResponse:
        since = request.query_params.get("since")
        sid, me = require_station(), current_agent() or ""
        msgs = await DIRECT.inbox(
            sid, me,
            since=float(since) if since else None,
            limit=int(request.query_params.get("limit") or 50),
        )
        # Same rule as the MCP twin: reading is receiving.
        if me and msgs:
            await _db(lambda: _mark_read(sid, me, [m["id"] for m in msgs]))
            await _db(lambda: _maybe_collect(sid))
        return JSONResponse({"dms": msgs})

    # ----- md files ---------------------------------------------------------
    # The REST half of share_md/fetch_md. It did not exist, so the two clients
    # that speak REST instead of MCP could not share or read a file at all —
    # while their own tool descriptions told the agent to do exactly that.

    async def share_md_route(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            out = await md_store(
                require_station(),
                body.get("channel") or "",
                body.get("sender") or current_agent() or "",
                body.get("filename") or "",
                body.get("content") or "",
                body.get("note") or "",
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        return JSONResponse(out, status_code=201)

    async def fetch_md_route(request: Request) -> JSONResponse:
        # ?uri= and not a path shaped like the URI: the client passes back the
        # exact string it read in the message, with nothing to take apart.
        uri = request.query_params.get("uri") or ""
        try:
            return JSONResponse(await md_get(require_station(), uri))
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)

    # ----- broadcasts -------------------------------------------------------

    async def create_broadcast_route(request: Request) -> JSONResponse:
        body = await request.json()
        b = await BROADCASTS.create(
            require_station(),
            problem=body["problem"],
            sender=body["sender"],
            expertise=body.get("expertise") or None,
            projects=body.get("projects") or None,
        )
        return JSONResponse(b, status_code=201)

    async def list_broadcasts_route(request: Request) -> JSONResponse:
        status = request.query_params.get("status") or None
        since = request.query_params.get("since")
        limit = request.query_params.get("limit") or "50"
        rows = await BROADCASTS.list(
            require_station(),
            status=status,
            since=float(since) if since else None,
            limit=int(limit),
        )
        return JSONResponse({"broadcasts": rows})

    async def get_broadcast_route(request: Request) -> JSONResponse:
        try:
            b = await BROADCASTS.get(
                require_station(),
                request.path_params["broadcast_id"],
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(b)

    async def submit_bid_route(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            r = await BROADCASTS.submit_bid(
                require_station(),
                request.path_params["broadcast_id"],
                body["agent_id"], body["bid"], body.get("pitch") or "",
            )
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(r, status_code=201)

    async def close_broadcast_route(request: Request) -> JSONResponse:
        try:
            b = await BROADCASTS.close(
                require_station(),
                request.path_params["broadcast_id"],
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(b)

    # ----- admin (superuser) -----------------------------------------------

    async def admin_list_stations(_: Request) -> JSONResponse:
        return JSONResponse({"stations": STATIONS.list()})

    async def admin_create_station(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            st = STATIONS.create(
                body["name"], body.get("description") or ""
            )
        except ValueError as e:
            msg = str(e)
            code = 409 if "already exists" in msg else 400
            return JSONResponse({"error": msg}, status_code=code)
        except KeyError:
            return JSONResponse({"error": "missing 'name'"}, status_code=400)
        return JSONResponse(st, status_code=201)

    async def admin_delete_station(request: Request) -> JSONResponse:
        id_or_name = request.path_params["id_or_name"]
        try:
            removed = STATIONS.delete(id_or_name)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        return JSONResponse({"deleted": removed, "id_or_name": id_or_name})

    async def admin_list_tokens(request: Request) -> JSONResponse:
        st = request.query_params.get("station") or None
        include_revoked = (
            request.query_params.get("include_revoked", "0") in ("1", "true")
        )
        try:
            rows = TOKENS.list(
                station_id=st, include_revoked=include_revoked
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse({"tokens": rows})

    async def admin_create_token(request: Request) -> JSONResponse:
        """Mint a bare token; add it to stations via /admin/stations/{s}/allow."""
        try:
            body = await request.json()
        except Exception:
            body = {}
        row = TOKENS.create(
            label=body.get("label") or "", user=body.get("user") or ""
        )
        return JSONResponse(row, status_code=201)

    async def admin_revoke_token(request: Request) -> JSONResponse:
        prefix = request.path_params["token_or_prefix"]
        hard = request.query_params.get("delete") in ("1", "true")
        if hard:
            n = TOKENS.delete(prefix)
            return JSONResponse({"deleted": n, "token_or_prefix": prefix})
        n = TOKENS.revoke(prefix)
        return JSONResponse({"revoked": n, "token_or_prefix": prefix})

    async def admin_show_station(request: Request) -> JSONResponse:
        try:
            return JSONResponse(
                STATIONS.allowed(request.path_params["id_or_name"])
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)

    async def admin_messages(request: Request) -> JSONResponse:
        """What is in a station, and what is holding it.

        ADMIN tier like the screen route beside it, and for the same reason:
        the POST twin acks on behalf of other agents, which is exactly the
        power a station token must never have.
        """
        try:
            st = STATIONS.get(request.path_params["id_or_name"])
            if not st:
                raise KeyError(
                    f"station {request.path_params['id_or_name']!r} not found"
                )
            out = await _db(lambda: message_stats(st["station_id"]))
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(out)

    async def admin_mark_messages(request: Request) -> JSONResponse:
        """Make one segment collectable. Deletes nothing here — collect does."""
        try:
            body = await request.json()
        except Exception:
            body = {}
        try:
            st = STATIONS.get(request.path_params["id_or_name"])
            if not st:
                raise KeyError(
                    f"station {request.path_params['id_or_name']!r} not found"
                )
            segment = body.get("segment") or ""
            dry = bool(body.get("dry_run"))
            out = await _db(lambda: mark_segment(
                st["station_id"], segment, preview=dry))
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=400)
        return JSONResponse(out)

    async def admin_screen_station(request: Request) -> JSONResponse:
        """Ack a backlog so the collector can retire it. Deletes nothing here.

        ADMIN tier, and that is the whole point: this acks on behalf of other
        agents, which is exactly the power a station token must never have.
        An agent clearing its OWN inbox is the ack_all tool instead.
        """
        try:
            body = await request.json()
        except Exception:
            body = {}
        try:
            st = STATIONS.get(request.path_params["id_or_name"])
            if not st:
                raise KeyError(
                    f"station {request.path_params['id_or_name']!r} not found"
                )
            sid = st["station_id"]
            agent_id = body.get("agent") or None
            dry = bool(body.get("dry_run"))
            out = await _db(lambda: screen(sid, agent_id, preview=dry))
            if not dry:
                out["collected"] = await _db(lambda: collect(sid))
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        return JSONResponse(out)

    async def admin_station_allow(request: Request) -> JSONResponse:
        """Station allow list: add/remove a token, or open/close it ('*')."""
        station = request.path_params["id_or_name"]
        adding = request.method == "POST"
        try:
            body = await request.json()
        except Exception:
            body = {}
        try:
            if body.get("any"):
                return JSONResponse(STATIONS.set_open(station, adding))
            token = body.get("token")
            if not token:
                return JSONResponse(
                    {"error": "give 'token' or 'any': true"}, status_code=400
                )
            if adding:
                STATIONS.allow(station, token)
                return JSONResponse({"allowed": True, "station": station},
                                    status_code=201)
            n = STATIONS.disallow(station, token)
            return JSONResponse({"removed": n, "station": station})
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)

    async def admin_list_agents(request: Request) -> JSONResponse:
        try:
            rows = AGENTS.list_all(request.query_params.get("station"))
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        for r in rows:      # never expose the owning token's hash
            r.pop("owner_token_hash", None)
        return JSONResponse({"agents": rows})

    async def admin_add_agent(request: Request) -> JSONResponse:
        body = await request.json()
        try:
            owner = (
                TOKENS._hash_of(body["token"]) if body.get("token") else None
            )
            row = AGENTS.add(
                body["station"], body["agent_id"], owner_token_hash=owner
            )
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        return JSONResponse(row, status_code=201)

    async def admin_remove_agent(request: Request) -> JSONResponse:
        agent_id = request.path_params["agent_id"]
        try:
            n = AGENTS.remove(agent_id, request.query_params.get("station"))
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        return JSONResponse({"removed": n, "agent_id": agent_id})

    async def admin_update_agent(request: Request) -> JSONResponse:
        """Move an agent between stations and/or (un)bind it to a token."""
        body = await request.json()
        agent_id = request.path_params["agent_id"]
        try:
            if body.get("station"):
                if not AGENTS.move(agent_id, body["station"]):
                    return JSONResponse({"error": "agent not found"},
                                        status_code=404)
            if "token" in body:
                AGENTS.bind(agent_id, body["token"])
        except KeyError as e:
            return JSONResponse({"error": str(e)}, status_code=404)
        except ValueError as e:
            return JSONResponse({"error": str(e)}, status_code=409)
        return JSONResponse({"agent_id": agent_id, "updated": True})

    routes: list = []
    routes.append(Route("/healthz", healthz, methods=["GET"]))
    routes.append(Route("/a2a-claudecode.tar.gz", plugin_targz, methods=["GET"]))
    routes.append(Route("/a2a-claudecode.zip", plugin_zip, methods=["GET"]))
    # Before the general token route: Starlette matches in order, so
    # /install/{token} would otherwise swallow "pi" as a token.
    # Specific before general: /install/{token} would swallow these.
    routes.append(Route("/install/pi/{token}", pi_install, methods=["GET"]))
    routes.append(Route("/install/codex/{token}", codex_install, methods=["GET"]))
    routes.append(Route("/install/{token}", opencode_install, methods=["GET"]))
    routes.append(Route("/stream", stream_route, methods=["GET"]))

    routes.append(Route("/me", me_route, methods=["GET"]))
    routes.append(Route("/me/agents", me_agents_route, methods=["GET"]))
    # Specific before general, and both before /me/agents/{agent_id}.
    routes.append(Route("/me/proposals", me_proposals_route, methods=["GET"]))
    routes.append(Route("/me/proposals", me_propose_route, methods=["POST"]))
    routes.append(Route("/me/proposals/{agent_id}",
                        me_withdraw_proposal_route, methods=["DELETE"]))
    routes.append(Route("/me/agents/{agent_id}", me_update_agent_route, methods=["PATCH"]))

    routes.append(Route("/agents", list_agents_route, methods=["GET"]))
    routes.append(Route("/agents/{agent_id}", get_agent_route, methods=["GET"]))
    routes.append(Route("/agents/{agent_id}", update_agent_route, methods=["PATCH"]))

    routes.append(Route("/channels", list_channels_route, methods=["GET"]))
    routes.append(Route("/channels", create_channel_route, methods=["POST"]))
    routes.append(Route("/channels/{name}", get_channel_route, methods=["GET"]))
    routes.append(Route("/channels/{name}/members", add_member_route, methods=["POST"]))
    routes.append(Route("/channels/{name}/members/{agent_id}", remove_member_route, methods=["DELETE"]))
    routes.append(Route("/channels/{name}/messages", post_message_route, methods=["POST"]))
    routes.append(Route("/channels/{name}/messages", list_messages_route, methods=["GET"]))
    routes.append(Route("/channels/{name}/evict", evict_route, methods=["POST"]))

    routes.append(Route("/dms", send_dm_route, methods=["POST"]))
    routes.append(Route("/dms", list_dms_route, methods=["GET"]))

    routes.append(Route("/md", share_md_route, methods=["POST"]))
    routes.append(Route("/md", fetch_md_route, methods=["GET"]))

    routes.append(Route("/pending", pending_route, methods=["GET"]))
    routes.append(Route("/ack", ack_route, methods=["POST"]))
    routes.append(Route("/ack/all", ack_all_route, methods=["POST"]))

    routes.append(Route("/broadcasts", list_broadcasts_route, methods=["GET"]))
    routes.append(Route("/broadcasts", create_broadcast_route, methods=["POST"]))
    routes.append(Route("/broadcasts/{broadcast_id}", get_broadcast_route, methods=["GET"]))
    routes.append(Route("/broadcasts/{broadcast_id}/bids", submit_bid_route, methods=["POST"]))
    routes.append(Route("/broadcasts/{broadcast_id}/close", close_broadcast_route, methods=["POST"]))

    routes.append(Route("/admin/stations", admin_list_stations, methods=["GET"]))
    routes.append(Route("/admin/stations", admin_create_station, methods=["POST"]))
    routes.append(Route("/admin/stations/{id_or_name}", admin_delete_station, methods=["DELETE"]))
    routes.append(Route("/admin/stations/{id_or_name}", admin_show_station, methods=["GET"]))
    routes.append(Route("/admin/stations/{id_or_name}/allow", admin_station_allow, methods=["POST", "DELETE"]))
    routes.append(Route("/admin/stations/{id_or_name}/screen", admin_screen_station, methods=["POST"]))
    routes.append(Route("/admin/stations/{id_or_name}/messages", admin_messages, methods=["GET"]))
    routes.append(Route("/admin/stations/{id_or_name}/messages", admin_mark_messages, methods=["POST"]))
    routes.append(Route("/admin/tokens", admin_list_tokens, methods=["GET"]))
    routes.append(Route("/admin/tokens", admin_create_token, methods=["POST"]))
    routes.append(Route("/admin/tokens/{token_or_prefix}", admin_revoke_token, methods=["DELETE"]))
    routes.append(Route("/admin/agents", admin_list_agents, methods=["GET"]))
    routes.append(Route("/admin/agents", admin_add_agent, methods=["POST"]))
    routes.append(Route("/admin/agents/{agent_id}", admin_update_agent, methods=["PATCH"]))
    routes.append(Route("/admin/agents/{agent_id}", admin_remove_agent, methods=["DELETE"]))

    mcp_app = mcp.streamable_http_app()
    routes.append(Mount("/mcp", app=mcp_app))

    @asynccontextmanager
    async def lifespan(app):
        async with mcp_app.router.lifespan_context(app):
            if AUTH_DISABLED:
                print(
                    "[startup] WARNING: A2A_AUTH_DISABLED=1 — auth is OFF, "
                    "all requests route to the 'default' station"
                )
            elif not ADMIN_TOKEN:
                print(
                    "[startup] note: A2A_ADMIN_TOKEN not set — /admin/* "
                    "endpoints disabled (use the CLI for tokens/stations)"
                )
            yield

    async def _denied(_: Request, exc: Exception) -> JSONResponse:
        """A station-scoped route ran without a station bound."""
        return JSONResponse({"error": str(exc)}, status_code=403)

    return Starlette(
        routes=routes,
        middleware=[Middleware(AuthMiddleware)],
        exception_handlers={AuthRequired: _denied, AgentDenied: _denied},
        lifespan=lifespan,
    )


# ---------------------------------------------------------------------------
# CLI.
# ---------------------------------------------------------------------------

def _print_token_table(rows: list[dict]) -> None:
    if not rows:
        print("(no tokens)")
        return
    print(f"{'prefix':<10} {'user':<14} {'stations':<28} "
          f"{'label':<16} {'created':<12} status")
    for r in rows:
        status = (
            "revoked" if r.get("revoked_at") else
            ("used" if r.get("last_used_at") else "fresh")
        )
        created = time.strftime(
            "%Y-%m-%d", time.gmtime(r["created_at"])
        )
        stations = ",".join(r.get("stations") or []) or "-"
        print(
            f"{r['prefix']:<10} {(r.get('user') or '-')[:14]:<14} "
            f"{stations[:28]:<28} {(r.get('label') or '')[:16]:<16} "
            f"{created:<12} {status}"
        )


def _print_proposal_table(rows: list[dict]) -> None:
    if not rows:
        print("(no pending proposals)")
        return
    now = time.time()
    print(f"{'agent_id':<24} {'station':<14} {'token':<10} {'kind':<24} "
          f"{'expires':<8} note")
    for r in rows:
        # A transfer names who holds the id now: approving it takes the name
        # off that token, which is the fact the operator is deciding about.
        if r.get("kind") == "transfer":
            held = r.get("current_owner_prefix") or "?"
            if r.get("current_owner_revoked"):
                held += " REVOKED"
            kind = f"transfer from {held}"
        else:
            kind = "claim (new name)"
        print(f"{r['agent_id'][:24]:<24} "
              f"{(r.get('station_name') or '')[:14]:<14} "
              f"{(r.get('owner_prefix') or '-'):<10} "
              f"{kind[:24]:<24} "
              f"{_short_duration(r['expires_at'] - now):<8} "
              f"{(r.get('note') or '')[:30]}")


def _print_agent_table(rows: list[dict]) -> None:
    if not rows:
        print("(no agents)")
        return
    print(f"{'agent_id':<28} {'station':<20} bound to")
    for r in rows:
        owner = r.get("owner_prefix")
        who = (
            f"{owner} ({r['owner_user']})" if owner and r.get("owner_user")
            else (owner or "-")
        )
        print(
            f"{r['agent_id'][:28]:<28} "
            f"{(r.get('station_name') or r['station_id'])[:20]:<20} {who}"
        )


def _print_station_table(rows: list[dict]) -> None:
    if not rows:
        print("(no stations)")
        return
    print(f"{'station_id':<38} {'name':<24} {'access':<8} created")
    for r in rows:
        created = time.strftime(
            "%Y-%m-%d", time.gmtime(r["created_at"])
        )
        access = "* open" if r.get("open") else "closed"
        print(f"{r['station_id']:<38} {r['name'][:24]:<24} "
              f"{access:<8} {created}")


def _cli_station(args: argparse.Namespace) -> int:
    if args.station_cmd == "screen":
        return _cli_screen(args, None)
    if args.station_cmd == "create":
        try:
            st = STATIONS.create(args.name, args.description or "")
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
        print("station created:")
        print(f"  station_id:  {st['station_id']}")
        print(f"  name:        {st['name']}")
        return 0
    if args.station_cmd == "list":
        _print_station_table(STATIONS.list())
        return 0
    if args.station_cmd == "delete":
        try:
            ok = STATIONS.delete(args.id_or_name)
        except ValueError as e:
            print(f"error: {e}", file=sys.stderr)
            return 2
        if not ok:
            print(f"station {args.id_or_name!r} not found", file=sys.stderr)
            return 1
        print(f"station {args.id_or_name!r} deleted")
        return 0
    if args.station_cmd in ("allow", "disallow"):
        adding = args.station_cmd == "allow"
        try:
            if args.any:
                out = STATIONS.set_open(args.id_or_name, adding)
                print(f"station {out['station']!r} is now "
                      f"{'OPEN (*: every token allowed)' if adding else 'closed'}")
                return 0
            if not args.token:
                print("error: give --token <prefix> or --any", file=sys.stderr)
                return 2
            if adding:
                STATIONS.allow(args.id_or_name, args.token)
                print(f"token {args.token} allowed in {args.id_or_name!r}")
                return 0
            n = STATIONS.disallow(args.id_or_name, args.token)
            print(f"{n} entr(y/ies) removed from {args.id_or_name!r}")
            return 0 if n else 1
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
    if args.station_cmd == "show":
        try:
            info = STATIONS.allowed(args.id_or_name)
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"station:  {info['station']}  ({info['station_id']})")
        print(f"access:   {'* open — every valid token' if info['open'] else 'closed — allow list only'}")
        print("allow list:")
        if info["open"]:
            print("  *")
        if not info["tokens"]:
            print("  (no tokens)" if not info["open"] else "  (no explicit entries)")
        for t in info["tokens"]:
            status = " [revoked]" if t["revoked_at"] else ""
            print(f"  {t['prefix']:<10} {(t['user'] or '-')[:16]:<16} "
                  f"{(t['label'] or '')[:20]}{status}")
        agents = AGENTS.list_all(info["station_id"])
        print(f"agents:   {len(agents)}")
        for a in agents:
            print(f"  {a['agent_id']}")
        return 0
    if args.station_cmd == "purge":
        empty = [
            s for s in STATIONS.list()
            if s["station_id"] != DEFAULT_STATION_ID and not any(
                CONN.execute(
                    f"SELECT 1 FROM {t} WHERE station_id = %s LIMIT 1",
                    (s["station_id"],),
                ).fetchone()
                for t in ("agents", "channels", "transcripts", "broadcasts")
            )
        ]
        if not empty:
            print("(no empty stations)")
            return 0
        print("would delete:", ", ".join(s["name"] for s in empty))
        if not args.yes:
            print("re-run with --yes to confirm", file=sys.stderr)
            return 1
        for s in empty:
            STATIONS.delete(s["station_id"])
        print(f"{len(empty)} station(s) deleted")
        return 0
    return 2


def _cli_agent(args: argparse.Namespace) -> int:
    if args.agent_cmd == "add":
        try:
            owner = TOKENS._hash_of(args.token) if args.token else None
            if args.token and not owner:
                print(f"error: token {args.token!r} not found", file=sys.stderr)
                return 1
            row = AGENTS.add(args.station, args.agent_id, owner_token_hash=owner)
        except (KeyError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"agent {row['agent_id']!r} added to station "
              f"{row['station_name']!r}")
        return 0
    if args.agent_cmd == "list":
        try:
            _print_agent_table(AGENTS.list_all(args.station))
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        return 0
    if args.agent_cmd == "move":
        try:
            ok = AGENTS.move(args.agent_id, args.station)
        except (KeyError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not ok:
            print(f"agent {args.agent_id!r} not found", file=sys.stderr)
            return 1
        print(f"agent {args.agent_id!r} moved to {args.station!r}")
        return 0
    if args.agent_cmd == "rm":
        try:
            n = AGENTS.remove(args.agent_id, args.station)
        except (KeyError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not n:
            print(f"agent {args.agent_id!r} not found", file=sys.stderr)
            return 1
        print(f"agent {args.agent_id!r} removed")
        return 0
    if args.agent_cmd == "screen":
        return _cli_screen(args, args.agent_id)
    if args.agent_cmd == "proposals":
        try:
            rows = PROPOSALS.list(args.station)
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if getattr(args, "kind", None):
            rows = [r for r in rows if r.get("kind") == args.kind]
        if not rows:
            print("no pending proposals")
            return 0
        _print_proposal_table(rows)
        return 0
    if args.agent_cmd == "unlock":
        try:
            n = PROPOSALS.unlock(args.station, args.agent_id)
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"{n} transfer lock(s) on {args.agent_id!r} lifted"
              if n else f"no transfer lock on {args.agent_id!r}")
        return 0
    if args.agent_cmd in ("approve", "reject"):
        try:
            if args.agent_cmd == "approve":
                out = PROPOSALS.approve(args.station, args.agent_id)
                if out.get("kind") == "transfer":
                    print(f"agent {out['agent_id']!r} in "
                          f"{out['station_name']!r} TRANSFERRED to the token "
                          f"that asked for it")
                    print("  it keeps its channels and every message it had "
                          "not acked yet.")
                    print("  the previous client is refused from now on.")
                else:
                    print(f"agent {out['agent_id']!r} approved in "
                          f"{out['station_name']!r}, bound to the token that "
                          f"asked for it")
                print("  its client connects with no restart.")
            else:
                out = PROPOSALS.reject(args.station, args.agent_id)
                print(f"{out['kind']} request for {out['agent_id']!r} "
                      f"rejected")
                if out.get("locked_until"):
                    print(f"  that token may not ask again for "
                          f"{_short_duration(TRANSFER_LOCKTIME)} "
                          f"(`agent unlock` lifts it).")
        except (KeyError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        return 0
    if args.agent_cmd == "free":
        try:
            out = AGENTS.free(args.agent_id, args.station)
        except (KeyError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"agent {out['agent_id']!r} freed"
              f"{' (was held)' if out['was_held'] else ' (was already unheld)'}")
        print("  the name, its unacked messages and its channel memberships "
              "are untouched;")
        print("  any client announcing this id now claims it.")
        return 0
    if args.agent_cmd in ("bind", "unbind"):
        tok = args.token if args.agent_cmd == "bind" else None
        try:
            if args.all:
                n = AGENTS.bind_all(tok, args.station)
                print(f"{n} agent(s) "
                      f"{'bound to ' + tok if tok else 'unbound'}")
                return 0 if n else 1
            if not args.agent_id:
                print("error: give an agent_id or --all", file=sys.stderr)
                return 2
            n = AGENTS.bind(args.agent_id, tok)
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not n:
            print(f"agent {args.agent_id!r} not found", file=sys.stderr)
            return 1
        print(f"agent {args.agent_id!r} "
              f"{'bound to ' + tok if tok else 'unbound'}")
        return 0
    return 2


def _cli_token(args: argparse.Namespace) -> int:
    if args.token_cmd == "create":
        row = TOKENS.create(label=args.label or "", user=args.user or "")
        print("token created — copy it now, it will not be shown again:")
        print()
        print(f"  {row['token']}")
        print()
        if row["user"]:
            print(f"  user:     {row['user']}")
        print(f"  prefix:   {row['prefix']}")
        if row["label"]:
            print(f"  label:    {row['label']}")
        print()
        print("  it can reach nothing yet — add it to a station:")
        print(f"    station allow <station> --token {row['prefix']}")
        return 0
    if args.token_cmd == "list":
        try:
            rows = TOKENS.list(
                station_id=args.station,
                include_revoked=args.include_revoked,
            )
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        _print_token_table(rows)
        return 0
    if args.token_cmd == "revoke":
        n = TOKENS.revoke(args.token_or_prefix)
        print(f"{n} token(s) revoked")
        return 0 if n > 0 else 1
    if args.token_cmd == "delete":
        n = TOKENS.delete(args.token_or_prefix)
        print(f"{n} token(s) deleted")
        return 0 if n > 0 else 1
    if args.token_cmd == "purge":
        try:
            victims = TOKENS.list(
                station_id=args.station, include_revoked=True
            )
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if args.revoked:
            victims = [v for v in victims if v.get("revoked_at")]
        if not victims:
            print("(nothing to purge)")
            return 0
        print("would delete:", ", ".join(v["prefix"] for v in victims))
        if not args.yes:
            print("re-run with --yes to confirm", file=sys.stderr)
            return 1
        n = TOKENS.purge(revoked_only=args.revoked, station_id=args.station)
        print(f"{n} token(s) deleted")
        return 0
    return 2


# ---------------------------------------------------------------------------
# TUI: one screen for everything the server commands do. Calls the registries
# directly (no HTTP), so it can never drift from the CLI. Standard library only
# — curses, with a prompt fallback when there is no TTY.
# ---------------------------------------------------------------------------

class _Tui:
    # The tabs, in order, in ONE place. This list used to be written twice —
    # once in draw() for the header and once in act() for switching — and a
    # sixth tab added to act() alone was reachable by key while being absent
    # from the header, which reads as "the feature is not there".
    VIEWS = ("stations", "tokens", "agents", "logs", "messages", "channels")

    # Keys are lowercase-only and each letter means one thing: nothing here
    # depends on shift, so a missed modifier can never reach a different (and
    # more destructive) action than the one intended. Tabs moved off letters
    # to digits/tab so 'a' is free to mean "allow" where that reads naturally.
    HELP = {
        # "ack all", not "screen": the CLI verb is `screen`, but on a row an
        # operator is looking at, the useful label is what it DOES. A key
        # nobody recognises is a key nobody presses.
        "stations": "n new · g grant token · v revoke grant · o open/close · "
                    "x delete · e purge empty · s ack all · c collect · ⏎ agents",
        "tokens":   "n new · g grant into station · v revoke · x delete · "
                    "z purge revoked · c collect",
        "agents":   "a approve/transfer · n add · f free (claimable again) · "
                    "m move · b bind · s ack all · x remove/reject · "
                    "u unlock transfer · c collect",
        # Read-only on purpose. Everything here is history; the way to
        # change it is to do something that gets logged.
        "logs":     "newest last (UTC) · ↑/↓ scroll · c collect",
        # x, not d: every other destructive key in this TUI is x, and a key
        # that means something else here is a key pressed by mistake.
        "messages": "s station · x mark segment for collection "
                    "(ack, expiry or age) · c collect",
        # Membership is routing: who is in the room decides who receives a
        # post, so a/r are the keys that matter here, not n/x.
        "channels": "all stations · s filter · n new · a add member · "
                    "r remove member · x delete · ⏎ members + last posts",
    }

    def __init__(self, scr):
        self.scr = scr
        self.view = "stations"
        self.sel = {"stations": 0, "tokens": 0, "agents": 0, "logs": 0,
                    "messages": 0, "channels": 0}
        # Which station the messages tab is looking at. Segments only
        # mean anything inside one, and a destructive key should reach
        # one station rather than the whole broker.
        self.msg_station: str | None = None
        # Same for channels: a channel name is unique within a station and
        # says nothing across them, so the tab always looks at exactly one.
        self.ch_station: str | None = None
        self.msg = ""
        self.rows: list[dict] = []

    # --- data ------------------------------------------------------------
    def load(self) -> None:
        if self.view == "stations":
            self.rows = STATIONS.list()
            for r in self.rows:
                r["agents"] = len(AGENTS.list_all(r["station_id"]))
                r["allowed"] = len(STATIONS.allowed(r["station_id"])["tokens"])
        elif self.view == "tokens":
            self.rows = TOKENS.list(include_revoked=True)
        elif self.view == "messages":
            st = self.scoped_station("msg_station")
            if not st:
                self.rows = []
                return
            stats = message_stats(st["station_id"])
            self.msg_stats = stats
            self.rows = []
            for group, total_key in (("ack", "ack_total"),
                                     ("expiry", "expiry_total"),
                                     ("age", "age_total")):
                for r in stats["rows"]:
                    if r["group"] == group:
                        self.rows.append(dict(r, station_id=st["station_id"]))
                # A separator carrying the sum, because the two groups are
                # independent partitions and the eight numbers do not add up
                # to one thing. Saying so beats letting someone try.
                self.rows.append({
                    "sep": True,
                    "label": f"{group} view covers {stats[total_key]} message(s)"
                             + (" — broadcasts have no shelf life"
                                if group == "expiry"
                                and stats["broadcasts_no_shelf_life"] else ""),
                })
        elif self.view == "channels":
            # EVERY station by default, with the station named on each row.
            # Scoping to one by default showed "(empty)" whenever the first
            # station happened to have no channels, which reads as "there are
            # no channels" while another station is full of them. `s` filters
            # to one; ch_station None means all.
            stations = [
                st for st in STATIONS.list()
                if self.ch_station in (None, st["name"])
            ]
            # asyncio.run, and this is the whole trap in this tab: every
            # ChannelRegistry method is a coroutine while everything else the
            # TUI calls is a plain function. Calling one without awaiting
            # returns a coroutine object and does nothing — no error, no rows,
            # no channel created. The CLI's `channel rm` already does it this
            # way; there is no event loop under curses, so run() is correct.
            self.rows = [
                dict(c, station_id=st["station_id"], station_name=st["name"])
                for st in stations
                for c in asyncio.run(CHANNELS.list(st["station_id"]))
            ]
        elif self.view == "logs":
            # Newest 500 by the ts index, then flipped so the screen reads
            # top-to-bottom in time order like every other log.
            self.rows = list(reversed(list(CONN.execute(
                "SELECT * FROM logs ORDER BY ts DESC LIMIT 500"
            ))))
        else:
            # Proposals sit in the same list as agents, and first: a name
            # waiting on the operator is the row they are here to act on, and
            # in a long station it would otherwise sort out of sight.
            revoked = {
                t["prefix"] for t in TOKENS.list(include_revoked=True)
                if t.get("revoked_at")
            }
            # `kind` here means "which sort of ROW is this" and is already
            # spoken for, so the request's own kind (claim/transfer) rides
            # along as `request` rather than being overwritten by the merge.
            pending = [
                dict(p, kind="proposal", request=p.get("kind") or "claim")
                for p in PROPOSALS.list()
            ]
            agents = []
            for r in AGENTS.list_all():
                r = dict(r, kind="agent")
                if not r.get("owner_token_hash"):
                    r["status"] = "pending token"
                elif r.get("owner_prefix") in revoked:
                    r["status"] = "token revoked"
                else:
                    r["status"] = "working"
                agents.append(r)
            self.rows = pending + agents
        self.sel[self.view] = max(
            0, min(self.sel[self.view], len(self.rows) - 1)
        )

    def current(self) -> dict | None:
        return self.rows[self.sel[self.view]] if self.rows else None

    def scoped_station(self, attr: str) -> dict | None:
        """The station a station-scoped tab is looking at, defaulting to the
        first one. Shared by messages and channels so the two tabs cannot
        drift apart on what "no station chosen yet" means."""
        stations = STATIONS.list()
        if not stations:
            return None
        name = getattr(self, attr)
        if name not in [s["name"] for s in stations]:
            name = stations[0]["name"]
            setattr(self, attr, name)
        return next(s for s in stations if s["name"] == name)

    # --- drawing ---------------------------------------------------------
    def line(self, r: dict) -> str:
        if self.view == "stations":
            access = "*open " if r.get("open") else "closed"
            return (f"{r['name'][:26]:<26} {access:<7} "
                    f"{r['allowed']:>3} tokens  {r['agents']:>3} agents")
        if self.view == "tokens":
            status = ("revoked" if r.get("revoked_at")
                      else ("used" if r.get("last_used_at") else "fresh"))
            return (f"{r['prefix']:<10} {(r.get('user') or '-')[:14]:<14} "
                    f"{(r.get('label') or '')[:16]:<16} "
                    f"{','.join(r.get('stations') or [])[:24]:<24} {status}")
        if self.view == "messages":
            if r.get("sep"):
                return f"  ── {r['label']}"
            kinds = " ".join(f"{k}:{n}" for k, n in (r.get("by_kind") or {}).items())
            note = ""
            if r["segment"] in ("unread", "partial") and r["count"]:
                note = "held by its audience"
            elif r["segment"] == "acked" and r["count"]:
                note = "goes on the next collect"
            elif r["segment"] == "orphan" and r["count"]:
                note = "no ack can free these — only age or expiry"
            elif r["segment"] == "overdue" and r["count"]:
                note = "goes on the next collect, even unacked"
            elif r["segment"] == "no_expiry" and r["count"]:
                note = "immortal until something gives them a deadline"
            return (f"  {r['label']:<24} {r['count']:>6}   "
                    f"{kinds:<22} {note}")
        if self.view == "channels":
            members = r.get("members") or []
            # The members themselves, not just a count: membership IS the
            # routing rule, and "who is in this room" is the question this
            # tab exists to answer.
            who = ", ".join(members) or "(nobody — posts reach no one)"
            pol = r.get("policy") or {}
            # A policy that silently refuses add_member is worth seeing before
            # you press a and wonder why nothing happened.
            gate = " ACL" if any(pol.get(k) for k in
                                 ("required_expertise", "allowed_projects",
                                  "blocked_agents")) else ""
            return (f"{r['name'][:18]:<18} "
                    f"{(r.get('station_name') or '')[:14]:<14} "
                    f"{len(members):>2}m {r.get('messages', 0):>5} msg"
                    f"{gate:<4} {who[:40]}")
        if self.view == "logs":
            when = time.strftime("%m-%d %H:%M:%S", time.gmtime(r["ts"]))
            where = (r.get("station") or "")[:12]
            return (f"{when}  {r['level']:<5} {where:<12} "
                    f"{(r.get('event') or ''):<16} {r['message']}")
        who = r.get("owner_prefix") or "-"
        if r.get("kind") == "proposal":
            left = max(0, r["expires_at"] - time.time())
            if r.get("request") == "transfer":
                # Name the holder: approving takes the id off that token, and
                # a revoked holder is the case where the answer is usually yes.
                held = r.get("current_owner_prefix") or "?"
                if r.get("current_owner_revoked"):
                    held += " REVOKED"
                status = (f"wants transfer from {held} · "
                          f"{_short_duration(left)} left")
            else:
                status = f"pending approval · {_short_duration(left)} left"
        else:
            status = r.get("status", "")
        return (f"{r['agent_id'][:28]:<28} "
                f"{(r.get('station_name') or '')[:16]:<16} "
                f"{who:<10} {status}")

    def draw(self) -> None:
        import curses
        self.scr.erase()
        h, w = self.scr.getmaxyx()
        tabs = "  ".join(
            f"[{'*' if v == self.view else ' '}] {v}" for v in self.VIEWS
        )
        self.scr.addnstr(0, 0, f" a2a admin — {tabs}", w - 1, curses.A_BOLD)
        # Version, hard right. Drawn second and only when the header leaves
        # room for it, so a narrow terminal loses the version rather than
        # having curses refuse the whole line.
        stamp = f"v{VERSION} "
        if w > len(f" a2a admin — {tabs}") + len(stamp) + 1:
            self.scr.addnstr(0, w - len(stamp) - 1, stamp, len(stamp),
                             curses.A_DIM)
        hint = self.HELP[self.view]
        # Which slice you are looking at, where a filter can hide rows. An
        # empty screen must never be ambiguous between "none exist" and "none
        # in the station I am filtered to".
        if self.view == "channels" and self.ch_station:
            hint = f"[{self.ch_station} only] " + hint
        elif self.view == "messages" and self.msg_station:
            hint = f"[{self.msg_station}] " + hint
        self.scr.addnstr(1, 0, " " + hint, w - 1)
        self.scr.addnstr(2, 0, " " + "─" * (w - 2), w - 1)
        top = 3
        # Scroll with the selection: drawing a fixed slice from 0 let the
        # cursor run past the last drawn row, so on a short terminal you acted
        # on a row you could not see.
        page = max(1, h - top - 2)
        off = self.offset(self.sel[self.view], len(self.rows), page)
        for i, r in enumerate(self.rows[off:off + page]):
            attr = curses.A_REVERSE if off + i == self.sel[self.view] else 0
            self.scr.addnstr(top + i, 1, self.line(r).ljust(w - 2), w - 2, attr)
        if not self.rows:
            self.scr.addnstr(top, 2, "(empty)", w - 3)
        more = f"  ({off + 1}-{min(off + page, len(self.rows))}"
        more += f" of {len(self.rows)})" if len(self.rows) > page else ")"
        self.scr.addnstr(h - 1, 0,
                         f" {self.msg}"[: w - 1] if self.msg
                         else f" ←/→ or tab/1..{len(self.VIEWS)} switch · "
                              f"↑/↓ rows · q quit"
                              f"{more if len(self.rows) > page else ''}", w - 1)
        self.scr.refresh()

    # --- input helpers ---------------------------------------------------
    @staticmethod
    def offset(sel: int, total: int, page: int) -> int:
        """First visible index so that `sel` is always on screen."""
        if total <= page:
            return 0
        return max(0, min(sel - page + 1 if sel >= page else 0, total - page))

    def ask(self, prompt: str) -> str:
        import curses
        h, w = self.scr.getmaxyx()
        curses.echo()
        curses.curs_set(1)   # run() hides it; typing blind is not typing
        # THE CURSOR MUST LAND INSIDE THE WINDOW. getstr(y, x, n) moves there
        # first, and a move past the last column fails the whole call — which
        # the except below then swallowed, so confirm() got "" and read it as
        # "no". A prompt wider than the terminal therefore made the key look
        # dead: nothing happened, and nothing said why. The drawing was always
        # clamped (addnstr); only the cursor was not. Prompts state a count and
        # a consequence, so they are long — truncate the TEXT, never the
        # interaction.
        room = 12                          # columns kept clear for the answer
        line = f" {prompt} "
        if len(line) > w - room:
            line = line[:max(0, w - room - 2)] + "… "
        x = max(0, min(len(line), w - 2))
        self.scr.addnstr(h - 1, 0, line.ljust(w - 1), w - 1, curses.A_REVERSE)
        self.scr.refresh()
        try:
            out = self.scr.getstr(h - 1, x, max(1, min(60, w - x - 1))).decode()
        except Exception:
            out = ""
        curses.noecho()
        curses.curs_set(0)
        return out.strip()

    def confirm(self, prompt: str) -> bool:
        return self.ask(f"{prompt} [y/N]").lower().startswith("y")

    def pick(self, title: str, items: list[tuple[str, str]]) -> str | None:
        """Modal list picker -> the chosen key (no ids to type by hand)."""
        import curses
        if not items:
            self.msg = f"nothing to pick for {title}"
            return None
        i = 0
        while True:
            h, w = self.scr.getmaxyx()
            self.scr.erase()
            self.scr.addnstr(0, 0, f" {title} — ⏎ choose · esc cancel",
                             w - 1, curses.A_BOLD)
            page = max(1, h - 3)
            off = self.offset(i, len(items), page)
            for n, (_, label) in enumerate(items[off:off + page]):
                self.scr.addnstr(2 + n, 2, label.ljust(w - 4), w - 4,
                                 curses.A_REVERSE if off + n == i else 0)
            if len(items) > page:
                self.scr.addnstr(h - 1, 0,
                                 f" {i + 1} of {len(items)}", w - 1)
            self.scr.refresh()
            c = self.scr.getch()
            if c in (27, ord("q")):
                return None
            if c in (curses.KEY_UP, ord("k")):
                i = max(0, i - 1)
            elif c in (curses.KEY_DOWN, ord("j")):
                i = min(len(items) - 1, i + 1)
            elif c in (curses.KEY_ENTER, 10, 13):
                return items[i][0]

    def show_token(self, row: dict) -> None:
        """A new token is shown once, exactly like `token create`."""
        import curses
        self.scr.erase()
        w = self.scr.getmaxyx()[1]
        for n, s in enumerate([
            " token created — copy it now, it will NOT be shown again",
            "",
            f"   {row['token']}",
            "",
            f"   user: {row['user'] or '-'}   prefix: {row['prefix']}",
            "",
            "   it can reach nothing yet — grant it into a station (g)",
            "",
            " press any key",
        ]):
            self.scr.addnstr(1 + n, 1, s, w - 2,
                             curses.A_BOLD if n == 2 else 0)
        self.scr.refresh()
        self.scr.getch()

    def station_items(self) -> list[tuple[str, str]]:
        return [
            (s["station_id"],
             f"{s['name']}{'  (*open)' if s.get('open') else ''}")
            for s in STATIONS.list()
        ]

    def token_items(self) -> list[tuple[str, str]]:
        return [
            (t["prefix"], f"{t['prefix']}  {t.get('user') or '-'}  "
                          f"{t.get('label') or ''}")
            for t in TOKENS.list(include_revoked=False)
        ]

    # --- actions ---------------------------------------------------------
    def act(self, c: int) -> bool:
        import curses
        # Lowercased: shift never selects a different action, so a slipped
        # modifier cannot turn "bind this agent" into "bind every agent".
        ch = chr(c).lower() if 0 <= c < 256 else ""
        views = self.VIEWS
        row = self.current()
        try:
            if ch == "q":
                return False
            if ch == "\t" or c == 9 or c == curses.KEY_RIGHT:
                self.view = views[(views.index(self.view) + 1) % len(views)]
                self.msg = ""
            elif c == curses.KEY_LEFT:
                # Left and right walk the tabs; up and down walk the rows.
                # Arrows are what a hand reaches for before it learns tab,
                # and the two axes are what the screen already looks like.
                self.view = views[(views.index(self.view) - 1) % len(views)]
                self.msg = ""
            elif ch.isdigit() and 1 <= int(ch) <= len(views):
                self.view = views[int(ch) - 1]
                self.msg = ""
            elif ch == "c":
                # Whole-broker collection, or just this station on the
                # stations tab. Never destructive to unacked messages.
                sid = (row or {}).get("station_id") \
                    if self.view == "stations" else None
                st = collect(sid)
                self.msg = (
                    f"collected: {st.get('dms', 0)} dms, "
                    f"{st.get('broadcasts', 0)} broadcasts, "
                    f"{st.get('transcripts', 0)} channel msgs past TTL, "
                    f"{st.get('expired', 0) + st.get('expired_broadcasts', 0)}"
                    f" expired unread"
                )
            elif c in (curses.KEY_UP, ord("k")):
                self.sel[self.view] = max(0, self.sel[self.view] - 1)
            elif c in (curses.KEY_DOWN, ord("j")):
                self.sel[self.view] = min(
                    len(self.rows) - 1, self.sel[self.view] + 1
                )
            elif self.view == "stations":
                self.act_station(ch, row)
            elif self.view == "tokens":
                self.act_token(ch, row)
            elif self.view == "logs":
                # Read-only by design: a log is what happened, not a thing to
                # act on. `c` (collect) is handled above and applies here too,
                # since retention is what trims this table.
                pass
            elif self.view == "messages":
                self.act_messages(ch, row)
            elif self.view == "channels":
                self.act_channel(ch, row, c)
            else:
                self.act_agent(ch, row)
        except (KeyError, ValueError) as e:
            self.msg = f"error: {e}"
        return True

    def screen_confirm(self, sid: str, agent_id: str | None,
                       label: str) -> None:
        """Preview first, then ask. An irreversible action states its size
        before it happens — the counts ARE the prompt."""
        pre = screen(sid, agent_id, preview=True)
        who = agent_id or f"everyone in {label}"
        if not pre["acked"]:
            self.msg = f"nothing unacked for {who}"
            return
        kinds = ", ".join(f"{k} {n}" for k, n in sorted(pre["by_kind"].items()))
        if not self.confirm(
            f"mark {pre['acked']} unacked message(s) ({kinds}) as HANDLED "
            f"for {who}? messages nobody read will then be collected"
        ):
            return
        out = screen(sid, agent_id)
        got = collect(sid)
        self.msg = (
            f"screened {who}: {out['acked']} acked → collected "
            f"{got.get('transcripts', 0)} posts, {got.get('dms', 0)} dms, "
            f"{got.get('broadcasts', 0)} broadcasts"
            + (f" ({out['open_broadcasts']} open broadcast(s) silenced, "
               f"not deleted)" if out.get("open_broadcasts") else "")
        )

    def act_station(self, ch: str, row: dict | None) -> None:
        if ch == "n":
            name = self.ask("new station name:")
            if name:
                STATIONS.create(name)
                self.msg = f"station {name!r} created (closed)"
        elif ch == "x" and row:
            if self.confirm(f"delete station {row['name']!r} and ALL its data?"):
                STATIONS.delete(row["station_id"])
                self.msg = f"station {row['name']!r} deleted"
        elif ch == "o" and row:
            out = STATIONS.set_open(row["station_id"], not row.get("open"))
            self.msg = (f"{out['station']} is now "
                        f"{'OPEN (*)' if out['open'] else 'closed'}")
        elif ch == "g" and row:
            p = self.pick(f"grant which token access to {row['name']}?",
                          self.token_items())
            if p:
                STATIONS.allow(row["station_id"], p)
                self.msg = f"{p} granted access to {row['name']}"
        elif ch == "v" and row:
            allowed = STATIONS.allowed(row["station_id"])["tokens"]
            p = self.pick(
                f"revoke which token's access to {row['name']}?",
                [(t["prefix"], f"{t['prefix']}  {t['user'] or '-'}")
                 for t in allowed],
            )
            if p and self.confirm(f"revoke {p}'s access to {row['name']}?"):
                self.msg = f"{STATIONS.disallow(row['station_id'], p)} revoked"
        elif ch == "s" and row:
            self.screen_confirm(row["station_id"], None, row["name"])
        elif ch == "e":
            empty = [
                s for s in STATIONS.list()
                if s["station_id"] != DEFAULT_STATION_ID
                and not AGENTS.list_all(s["station_id"])
            ]
            if not empty:
                self.msg = "no empty stations"
            elif self.confirm(f"purge {len(empty)} empty station(s)?"):
                for s in empty:
                    STATIONS.delete(s["station_id"])
                self.msg = f"{len(empty)} purged"
        elif ch in ("\n", "\r") and row:
            self.view, self.msg = "agents", f"station {row['name']}"

    def act_messages(self, ch: str, row: dict | None) -> None:
        if ch == "s":
            pick = self.pick("show which station?", self.station_items())
            if pick:
                st = STATIONS.get(pick)
                self.msg_station = st["name"] if st else self.msg_station
                self.sel["messages"] = 0
            return
        if ch != "x" or not row or row.get("sep"):
            return
        if not row["count"]:
            self.msg = f"{row['label']}: nothing to mark"
            return
        sid = row["station_id"]
        if row["segment"] == "acked":
            self.msg = ("already collectable — press c; nothing to mark")
            return

        # State the size and the word before doing it. Acking says HANDLED:
        # marking "nobody has read" retires messages no agent ever saw, which
        # is sometimes exactly right and never a reflex. Expiring by age does
        # the same to anything old enough, read or not, so it says that too.
        pre = mark_segment(sid, row["segment"], preview=True)
        verb = ("ack as handled" if row["segment"] in ("unread", "partial")
                else "expire now")
        # `found` counts every kind in the row; the untouched ones are not
        # going anywhere, so the prompt names both rather than one number
        # standing for two different fates.
        # Short on purpose: this line shares one terminal row with the answer.
        left = sum(pre.get("untouched", {}).values())
        prompt = (f"{verb} {pre['found'] - left} msg in {row['label']!r} "
                  f"({self.msg_station})")
        if row["segment"] in AGE_SEGMENTS:
            prompt += " — read or not"
        prompt += "; collector then deletes"
        if left:
            kinds = ", ".join(f"{n} {k}(s)"
                              for k, n in pre["untouched"].items())
            prompt += f" · {kinds} left alone"
        if not self.confirm(prompt):
            return
        out = mark_segment(sid, row["segment"])
        got = out.get("collected") or {}
        gone = sum(v for k, v in got.items() if k != "receipts")
        bits = [f"marked {out['found'] - sum(out.get('untouched', {}).values())}",
                f"collected {gone}"]
        if out.get("untouched"):
            bits.append(", ".join(f"{n} {k}(s) untouched"
                                  for k, n in out["untouched"].items()))
        if out.get("open_broadcasts"):
            bits.append(f"{out['open_broadcasts']} open broadcast(s) silenced "
                        "but not deleted")
        self.msg = " · ".join(bits)

    def act_channel(self, ch: str, row: dict | None, code: int = 0) -> None:
        # Every CHANNELS call here goes through asyncio.run for the reason
        # given in load(): these are coroutines, and forgetting one fails
        # silently rather than loudly.
        import curses
        if ch == "s":
            # "(all)" first and always offered: filtering to one station is
            # useful, being stuck in one is what hid every other station's
            # channels behind an empty screen.
            pick = self.pick(
                "show which station?",
                [("", "(all stations)"), *self.station_items()],
            )
            if pick is not None:
                st = STATIONS.get(pick) if pick else None
                self.ch_station = st["name"] if st else None
                self.sel["channels"] = 0
            return

        if ch == "n":
            # Which station to create in: the filter when there is one, the
            # selected row's station otherwise, and only ask when neither
            # answers — a new channel in the wrong station is invisible to
            # the agents meant to use it.
            sid = None
            if self.ch_station:
                st = next((s for s in STATIONS.list()
                           if s["name"] == self.ch_station), None)
                sid = st["station_id"] if st else None
            elif row:
                sid = row.get("station_id")
            if not sid:
                sid = self.pick("create the channel in which station?",
                                self.station_items())
            if not sid:
                return
            name = self.ask("new channel name:")
            if not name:
                return
            asyncio.run(CHANNELS.create(sid, name, self.ask("theme:"), []))
            # Said out loud because an empty channel is a channel that reaches
            # nobody: the audience is its members, and it has none yet.
            self.msg = (f"channel {name!r} created with no members — "
                        f"press a to add some, or posts reach nobody")
            return
        if not row:
            return
        # Row actions follow the ROW's station, never a global scope: the list
        # can span stations, so the selected row is the only correct answer to
        # "which station is this channel in".
        sid = row["station_id"]

        if ch == "a":
            # Only agents not already in it: offering a member again is a
            # choice that can only be a mistake.
            here = set(row.get("members") or [])
            items = [(a["agent_id"], a["agent_id"])
                     for a in AGENTS.list_all(sid)
                     if a["agent_id"] not in here]
            pick = self.pick(f"add whom to #{row['name']}?", items)
            if pick:
                asyncio.run(CHANNELS.add_member(sid, row["name"], pick))
                self.msg = (f"{pick} added to #{row['name']} — they receive "
                            f"posts from now on, not the ones already there")
        elif ch == "r":
            items = [(m, m) for m in (row.get("members") or [])]
            pick = self.pick(f"remove whom from #{row['name']}?", items)
            if pick:
                asyncio.run(CHANNELS.remove_member(sid, row["name"], pick))
                self.msg = (f"{pick} removed from #{row['name']} — messages "
                            f"already addressed to them still await their ack")
        elif ch == "x":
            n = row.get("messages", 0)
            if self.confirm(
                f"DELETE #{row['name']} and its {n} message(s)? "
                f"the transcript and every receipt pointing at it go too"
            ):
                asyncio.run(CHANNELS.delete(sid, row["name"]))
                self.msg = f"#{row['name']} deleted with {n} message(s)"
        elif code in (curses.KEY_ENTER, 10, 13):
            last = asyncio.run(
                CHANNELS.messages_since(sid, row["name"], None, 3)
            )
            tail = " | ".join(
                f"{m['sender']}: {' '.join(str(m['text']).split())[:40]}"
                for m in last
            ) or "no posts yet"
            self.msg = (f"#{row['name']} members: "
                        f"{', '.join(row.get('members') or []) or 'none'} — "
                        f"{tail}")

    def act_token(self, ch: str, row: dict | None) -> None:
        if ch == "n":
            user = self.ask("user name:")
            self.show_token(TOKENS.create(user=user, label=self.ask("label:")))
            self.msg = "token created — grant it into a station with g"
        elif ch == "g" and row:
            s = self.pick(f"grant {row['prefix']} access to which station?",
                          self.station_items())
            if s:
                STATIONS.allow(s, row["prefix"])
                self.msg = f"{row['prefix']} granted access"
        elif ch == "v" and row:
            if self.confirm(f"revoke {row['prefix']}?"):
                self.msg = f"{TOKENS.revoke(row['prefix'])} revoked"
        elif ch == "x" and row:
            if self.confirm(f"DELETE {row['prefix']} permanently?"):
                self.msg = f"{TOKENS.delete(row['prefix'])} deleted"
        elif ch == "z":
            if self.confirm("purge ALL revoked tokens?"):
                self.msg = f"{TOKENS.purge(revoked_only=True)} purged"

    def act_agent(self, ch: str, row: dict | None) -> None:
        # A proposal is not an agent: it cannot be moved, bound or freed,
        # because none of those things exist yet. Approve it or reject it.
        if row and row.get("kind") == "proposal":
            transfer = row.get("request") == "transfer"
            if ch == "a":
                who = row.get("owner_prefix") or "?"
                if transfer:
                    # Say what changes hands. Ownership is the only column
                    # that moves, but channels and receipts are keyed by
                    # agent_id, so the name keeps its memberships and its
                    # unacked inbox and the new token inherits both. Right for
                    # a replaced laptop, wrong for a name grab — and only the
                    # person reading this line can tell those apart.
                    held = row.get("current_owner_prefix") or "?"
                    n = len(_pending_rows(row["station_id"], row["agent_id"],
                                          500))
                    ok = self.confirm(
                        f"TRANSFER {row['agent_id']!r} from {held} to {who}? "
                        f"it keeps its channels and {n} unacked message(s); "
                        f"{held} is refused from now on"
                    )
                else:
                    ok = self.confirm(
                        f"approve {row['agent_id']!r} in "
                        f"{row.get('station_name')} and bind it to {who}?"
                    )
                if ok:
                    out = PROPOSALS.approve(row["station_id"], row["agent_id"])
                    did = "transferred" if transfer else "approved and bound"
                    self.msg = (
                        f"{out['agent_id']} {did} to {who} — its "
                        f"client connects with no restart"
                    )
            elif ch == "x":
                extra = (
                    f" that token may not ask again for "
                    f"{_short_duration(TRANSFER_LOCKTIME)}"
                    if transfer else " (it expires on its own otherwise)"
                )
                if self.confirm(
                    f"reject the request for {row['agent_id']!r}?{extra}"
                ):
                    out = PROPOSALS.reject(row["station_id"], row["agent_id"])
                    self.msg = f"{row['agent_id']} rejected"
                    if out.get("locked_until"):
                        self.msg += (
                            f" — locked "
                            f"{_short_duration(TRANSFER_LOCKTIME)}; u lifts it"
                        )
            elif ch == "u":
                n = PROPOSALS.unlock(row["station_id"], row["agent_id"])
                self.msg = (f"{n} transfer lock(s) lifted" if n
                            else "no transfer lock on this name")
            elif ch in ("m", "b", "f", "s"):
                self.msg = (
                    f"{row['agent_id']} is only a request — approve it with a "
                    f"first"
                )
            return
        if ch == "a":
            self.msg = "nothing to approve on this row"
        elif ch == "u" and row:
            # An accidental x would otherwise wedge a legitimate transfer for
            # the whole locktime, so the undo lives on the agent row too —
            # once the request is rejected there is no proposal row to press.
            locks = PROPOSALS.locks(row["station_id"], row["agent_id"])
            if not locks:
                self.msg = f"no transfer lock on {row['agent_id']}"
            else:
                who = ", ".join(
                    f"{lk.get('token_prefix') or '?'} "
                    f"({_short_duration(lk['denied_until'] - time.time())})"
                    for lk in locks
                )
                if self.confirm(f"lift the transfer lock on "
                                f"{row['agent_id']!r}? locked: {who}"):
                    n = PROPOSALS.unlock(row["station_id"], row["agent_id"])
                    self.msg = f"{n} transfer lock(s) lifted"
        elif ch == "n":
            s = self.pick("add agent in which station?", self.station_items())
            if s:
                name = self.ask("agent id (project folder name):")
                if name:
                    AGENTS.add(s, name)
                    self.msg = f"agent {name!r} added"
        elif ch == "m" and row:
            s = self.pick(
                f"move {row['agent_id']} (now in {row.get('station_name')}) to:",
                self.station_items(),
            )
            if s:
                # Pass the row's own station: the list is per-station, so a
                # duplicated agent id still moves exactly the row you selected.
                AGENTS.move(row["agent_id"], s, from_station=row["station_id"])
                self.msg = f"{row['agent_id']} moved"
        elif ch == "b" and row:
            p = self.pick("bind to which token?", self.token_items())
            if not p:
                return
            # Scope is a second, explicit choice. It used to be a separate
            # shifted key one slip away from the single-agent bind.
            scope = self.pick(
                f"bind {p} to:",
                [("one", f"just {row['agent_id']}"),
                 ("all", "ALL agents in every station")],
            )
            if scope == "one":
                AGENTS.bind(row["agent_id"], p, station_id=row["station_id"])
                self.msg = f"{row['agent_id']} bound to {p}"
            elif scope == "all" and self.confirm(f"bind every agent to {p}?"):
                self.msg = f"{AGENTS.bind_all(p)} bound to {p}"
        elif ch == "s" and row:
            self.screen_confirm(row["station_id"], row["agent_id"],
                                row.get("station_name") or "")
        elif ch == "f" and row:
            out = AGENTS.free(row["agent_id"], row["station_id"])
            self.msg = (
                f"{row['agent_id']} freed — name, messages and memberships "
                f"kept; any client may now claim it"
                + (" (it was held)" if out["was_held"] else " (was already free)")
            )
        elif ch == "x" and row:
            if self.confirm(f"remove agent {row['agent_id']!r}?"):
                AGENTS.remove(row["agent_id"], row["station_id"])
                self.msg = "agent removed"

    def run(self) -> None:
        import curses
        curses.curs_set(0)
        while True:
            self.load()
            self.draw()
            c = self.scr.getch()
            self.msg = ""
            if not self.act(c):
                return


def _fallback_screen() -> None:
    """Screening from the numbered menu. Same preview-then-confirm as the
    TUI: this acks on behalf of agents that never read the messages, so it
    states its size and asks before doing it."""
    st = STATIONS.get(input("station: ").strip())
    if not st:
        print("no such station")
        return
    agent = input("agent id (blank = the whole station): ").strip() or None
    pre = screen(st["station_id"], agent, preview=True)
    _report_screen(pre, None)
    if not pre["acked"]:
        return
    if not input("proceed? [y/N] ").strip().lower().startswith("y"):
        print("cancelled")
        return
    out = screen(st["station_id"], agent)
    _report_screen(out, collect(st["station_id"]))


def _fallback_station() -> dict | None:
    st = STATIONS.get(input("station: ").strip())
    if not st:
        print("no such station")
    return st


def _fallback_channels() -> None:
    st = _fallback_station()
    if not st:
        return
    rows = asyncio.run(CHANNELS.list(st["station_id"]))
    if not rows:
        print("no channels in this station")
        return
    for c in rows:
        members = ", ".join(c.get("members") or []) or "(nobody)"
        print(f"  {c['name']:<20} {c.get('messages', 0):>5} msg  {members}")


def _fallback_channel_new() -> None:
    st = _fallback_station()
    if not st:
        return
    name = input("channel name: ").strip()
    if not name:
        return
    asyncio.run(CHANNELS.create(
        st["station_id"], name, input("theme: ").strip(), []))
    print(f"channel {name!r} created with no members — add some with "
          f"`agent`/`channel` on the CLI, or posts reach nobody")


def _tui_fallback() -> int:
    """No curses / no TTY: same operations from a numbered menu."""
    print("a2a admin (plain mode — run with a TTY for the full screen:")
    print("  docker compose exec -it a2a-mcp python3 /app/a2a-mcp.py tui)\n")
    actions = {
        "1": ("list stations", lambda: _print_station_table(STATIONS.list())),
        "2": ("list tokens", lambda: _print_token_table(
            TOKENS.list(include_revoked=True))),
        "3": ("list agents", lambda: _print_agent_table(AGENTS.list_all())),
        "4": ("show station", lambda: print(json.dumps(
            STATIONS.allowed(input("station: ").strip()), indent=2))),
        "5": ("create station", lambda: STATIONS.create(
            input("name: ").strip())),
        "6": ("create token", lambda: print(json.dumps(
            TOKENS.create(user=input("user: ").strip()), indent=2))),
        "7": ("allow token in station", lambda: STATIONS.allow(
            input("station: ").strip(), input("token prefix: ").strip())),
        "8": ("disallow token", lambda: STATIONS.disallow(
            input("station: ").strip(), input("token prefix: ").strip())),
        "9": ("open/close station", lambda: STATIONS.set_open(
            input("station: ").strip(),
            input("open? [y/N] ").strip().lower().startswith("y"))),
        "10": ("add agent", lambda: AGENTS.add(
            input("station: ").strip(), input("agent id: ").strip())),
        # Approving is the one operator action a client is actively waiting
        # on, so it cannot be TTY-only — `docker compose exec` without -it is
        # exactly where operators land.
        "11": ("list proposals", lambda: _print_proposal_table(
            PROPOSALS.list())),
        "12": ("approve proposal", lambda: print(json.dumps(
            PROPOSALS.approve(input("station: ").strip(),
                              input("agent id: ").strip()), indent=2))),
        "13": ("reject proposal (a denied TRANSFER locks that token)",
               lambda: print(json.dumps(
                   PROPOSALS.reject(input("station: ").strip(),
                                    input("agent id: ").strip()), indent=2))),
        "14": ("ack all for a station or agent (so the collector can "
               "retire it)", _fallback_screen),
        "15": ("lift a transfer lock", lambda: print(
            f"{PROPOSALS.unlock(input('station: ').strip(), input('agent id: ').strip())}"
            f" lock(s) lifted")),
        # Read-first, like the tab: seeing who is in which room is the thing
        # that was missing. The `channel` CLI covers the rest.
        "16": ("list channels (members decide who receives a post)",
               _fallback_channels),
        "17": ("create channel", _fallback_channel_new),
    }
    while True:
        for k, (label, _) in actions.items():
            print(f"  {k:>2}. {label}")
        choice = input("\nchoice (q to quit): ").strip()
        if choice.lower() in ("q", ""):
            return 0
        act = actions.get(choice)
        if not act:
            print("?")
            continue
        try:
            act[1]()
        except Exception as e:
            print(f"error: {e}")
        print()


def _cli_ping(args: argparse.Namespace) -> int:
    """Post a witness message that @mentions an agent, then report whether it
    is now deliverable to it. Turns 'it doesn't work' into a yes/no."""
    agent = normalize_agent_id(args.agent_id)
    rows = CONN.execute(
        "SELECT station_id FROM agents WHERE agent_id = %s", (agent,)
    ).fetchall()
    if not rows:
        print(f"agent {agent!r} is not registered", file=sys.stderr)
        return 1
    if len(rows) > 1:
        print(f"agent {agent!r} exists in several stations", file=sys.stderr)
        return 1
    sid = rows[0]["station_id"]
    # Ping THROUGH A CHANNEL THE AGENT IS IN. It used to take the station's
    # first channel and widen the audience to include the agent, which put a
    # witness in a room the agent was not a member of: it arrived, the agent
    # answered in that channel, and its reply reached nobody. A channel post
    # never reaches outside the channel, and a diagnostic must not be the one
    # exception that teaches an agent otherwise.
    mine = [c for c in CHANNELS.list_all(sid)
            if agent in (c.get("members") or [])]
    if args.channel:
        mine = [c for c in mine if c["name"] == args.channel]
        if not mine:
            print(f"{agent} is not a member of #{args.channel}, so a ping "
                  f"there would prove nothing. Add them with "
                  f"`add_channel_member`, or ping through a channel they are "
                  f"in.", file=sys.stderr)
            return 1
    if not mine:
        print(f"{agent} is in no channel, so there is nothing to ping "
              f"through. Add them to one, or have the agent run `ping_me` "
              f"inside its session — that is a self-DM and needs no room.",
              file=sys.stderr)
        return 1
    name = mine[0]["name"]
    witness = args.text or f"PING-{uuid.uuid4().hex[:8].upper()}"
    # Say what it is. Without this an agent treats a probe as a peer message,
    # spends a turn composing PONG, and then reports that it could not deliver
    # it — the sender is a label, not an agent, so there is nobody to answer.
    text = (f"{witness} (delivery check for {agent} from the operator's "
            f"console — DO NOT REPLY: arrival is the whole result, and "
            f"{args.sender!r} is a label, not an agent)")
    entry_id, ts = str(uuid.uuid4()), time.time()
    # Addressed explicitly, not by writing "@agent" and hoping the body is
    # parsed — which is what this used to do, and what made a message signed
    # with its own author's handle reach nobody.
    expires_at = ts + PING_TTL
    with CONN:
        CONN.execute(
            "INSERT INTO transcripts (id, station_id, channel, ts, sender,"
            " text, expires_at) VALUES (%s, %s, %s, %s, %s, %s, %s)",
            (entry_id, sid, name, ts, args.sender, text, expires_at),
        )
        # Same two writes post_to_channel does: the message, then its
        # audience — except the audience is the ONE agent being checked. The
        # others can see the row in the transcript, but a diagnostic must not
        # wake a room or land in anybody else's unacked pile.
        _write_receipts(sid, entry_id, "channel", ts, [agent], expires_at)
    _wake_station(sid)
    # Look, don't consume: this must not mark the ping delivered.
    pending = _resolve_receipts(
        sid, _pending_rows(sid, agent, STREAM_BACKLOG_LIMIT)
    )
    hit = [m for m in pending if m.get("text") == text]
    print(f"posted to #{name} (station {sid[:8]}): {text}")
    print(f"deliverable to {agent} now: "
          f"{'YES — the server will push it' if hit else 'NO'}"
          f"  ({len(pending)} message(s) unacked in total)")
    if not hit:
        print(f"  the message exists but {agent} is not in its audience; "
              f"run `doctor {agent}`")
        return 1
    print("\nNOTE: this CLI writes from outside the server process, so the "
          "running stream picks it up on its next tick (up to "
          f"{int(STREAM_KEEPALIVE)}s), not instantly.")
    print("if it does not appear in that agent's session at all, the server "
          "side is fine and the client is not receiving:")
    print("  - did the session show the '[a2a] channel online' event at "
          "startup? if not, the channel flag did not register")
    print("  - /mcp shows a2a-channel connected?")
    print("  - more than one session open for this agent? they share one "
          "inbox, so whichever polls first receives it; the others pick it "
          "up on their next reconnect if it is still unacked")
    print("  watch it from here with:")
    print(f"    curl -sN -H 'Authorization: Bearer <token>' "
          f"'<url>/stream?agent={agent}'")
    return 0


def _cli_channel(args: argparse.Namespace) -> int:
    # Channels are provisioned here, not by agents: creating and deleting the
    # rooms is an operator act, the same as creating the agents in them.
    #
    # --station takes a NAME, and the registries take a station_id. Those are
    # the same string only for `default`, which is why this went unnoticed:
    # under sqlite the mismatch produced a channel in a station that did not
    # exist, and the foreign key that would have said so was the one thing
    # never exercised. Resolve it here, once, for every subcommand.
    def sid_of(name: str | None) -> str | None:
        if not name:
            return None
        st = STATIONS.get(name)
        if not st:
            raise KeyError(f"station {name!r} not found")
        return st["station_id"]

    if args.channel_cmd == "create":
        members = [m.strip() for m in (args.members or "").split(",") if m.strip()]
        try:
            out = asyncio.run(CHANNELS.create(
                sid_of(args.station), args.name, args.theme, members))
        except (KeyError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"channel {out['name']!r} created in {args.station} "
              f"with {len(out.get('members') or [])} member(s)")
        if not members:
            print("  no members yet: only @mentions reach anyone until you "
                  "add some (POST /channels/<name>/members)")
        return 0
    if args.channel_cmd == "rm":
        try:
            ok = asyncio.run(CHANNELS.delete(sid_of(args.station), args.name))
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not ok:
            print(f"channel {args.name!r} not found in {args.station}",
                  file=sys.stderr)
            return 1
        print(f"channel {args.name!r} deleted, with its transcript")
        return 0
    if args.channel_cmd == "list":
        try:
            rows = CHANNELS.list_all(sid_of(args.station))
        except KeyError as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        if not rows:
            print("(no channels)")
            return 0
        print(f"{'channel':<24} {'station':<20} {'msgs':>6}  members")
        for r in rows:
            members = ", ".join(json.loads(r["members"] or "[]")) or "-"
            print(f"{r['name'][:24]:<24} "
                  f"{(r.get('station_name') or r['station_id'])[:20]:<20} "
                  f"{r['messages']:>6}  {members[:60]}")
        return 0
    if args.channel_cmd == "move":
        try:
            out = CHANNELS.move_station(args.name, args.to, args.from_station)
        except (KeyError, ValueError) as e:
            print(f"error: {e}", file=sys.stderr)
            return 1
        print(f"channel {out['channel']!r} -> station {out['station']!r} "
              f"({out.get('messages', 0)} messages moved)")
        return 0
    return 2


EXPECTED_TABLES = (
    "stations", "tokens", "token_grants", "agents", "channels", "transcripts",
    "broadcasts", "bids", "dms", "md_files", "message_receipts",
    "message_addressees",
    "stream_cursors", "agent_proposals", "agent_transfer_denials", "logs",
)


def _doctor_storage(problems: list[str]) -> None:
    """Is this process talking to the database you think it is, and is that
    database shaped the way the code assumes?

    Both halves have already cost real time. "Which database is the running
    broker actually reading" was unanswerable from outside for an afternoon,
    and the collation is a trap the server's own defaults spring: MariaDB's
    default is case-INSENSITIVE, so a table recreated by hand without
    utf8mb4_bin silently merges `Foo` and `foo` into one agent — and delivery
    is a destructive read, so they would split one inbox between them.

    This is the command to run straight after a deploy, instead of inferring
    that it worked from the absence of errors.
    """
    print("\n--- storage ---")
    try:
        ver = CONN.execute("SELECT VERSION() AS v").fetchone()["v"]
        who = CONN.execute("SELECT DATABASE() AS d, CURRENT_USER() AS u"
                           ).fetchone()
    except Exception as e:
        problems.append(f"cannot query the database: {e}")
        print(f"  UNREACHABLE: {e}")
        return
    print(f"  server     {ver}")
    print(f"  database   {who['d']}   as {who['u']}")
    print(f"  configured {DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
    if who["d"] != DB_NAME:
        problems.append(
            f"connected to database {who['d']!r} but configured for "
            f"{DB_NAME!r} — the process is not reading what you think"
        )

    rows = CONN.execute(
        "SELECT table_name AS tname, table_collation AS tcoll "
        "FROM information_schema.tables WHERE table_schema = %s", (DB_NAME,)
    ).fetchall()
    present = {r["tname"] for r in rows}
    missing = [t for t in EXPECTED_TABLES if t not in present]
    if missing:
        problems.append(f"tables missing: {', '.join(missing)} — the schema "
                        "did not finish creating, or this is the wrong "
                        "database")
    wrong = sorted(r["tname"] for r in rows
                   if r["tcoll"] and r["tcoll"] != "utf8mb4_bin")
    print(f"  tables     {len(present)} present"
          f"{', MISSING ' + ', '.join(missing) if missing else ''}")
    if wrong:
        problems.append(
            f"not case-sensitive: {', '.join(wrong)} — agent ids are matched "
            "literally, so under a case-insensitive collation `Foo` and `foo` "
            "collide and one inbox is split between two agents. Rebuild those "
            "tables with COLLATE utf8mb4_bin."
        )
        print(f"  collation  WRONG on {', '.join(wrong)}")
    else:
        print("  collation  utf8mb4_bin on every table")

    try:
        n_logs = CONN.execute("SELECT COUNT(*) AS n FROM logs").fetchone()["n"]
        oldest = CONN.execute("SELECT MIN(ts) AS t FROM logs").fetchone()["t"]
        age = (time.time() - float(oldest)) if oldest else 0
        print(f"  logs       {n_logs} rows, oldest {_short_duration(age)} old "
              f"(retention {_short_duration(LOG_RETENTION)})")
        if oldest and age > LOG_RETENTION * 1.5:
            problems.append(
                f"log rows are older than the retention window — collect() is "
                f"not running, which also means nothing else is being retired"
            )
    except Exception as e:
        problems.append(f"cannot read the logs table: {e}")


def _doctor_all() -> int:
    """Whole-broker health: find the misconfigurations that cause silence."""
    problems: list[str] = []
    stations = STATIONS.list()
    agents = AGENTS.list_all()
    channels = CHANNELS.list_all()
    tokens = TOKENS.list(include_revoked=False)

    by_station_agents: dict[str, list[dict]] = {}
    for a in agents:
        by_station_agents.setdefault(a["station_id"], []).append(a)
    by_station_chans: dict[str, list[dict]] = {}
    for c in channels:
        by_station_chans.setdefault(c["station_id"], []).append(c)

    print("stations")
    for s in stations:
        sid = s["station_id"]
        na = len(by_station_agents.get(sid, []))
        nc = len(by_station_chans.get(sid, []))
        allowed = len(STATIONS.allowed(sid)["tokens"])
        access = "*open" if s.get("open") else f"{allowed} token(s)"
        print(f"  {s['name']:<22} {na:>3} agents  {nc:>3} channels  {access}")
        if nc and not na:
            problems.append(
                f"station {s['name']!r} holds {nc} channel(s) but has NO "
                f"agents — nobody can see them.\n"
                f"      fix: channel move <name> --to <station-with-agents>  "
                f"(or agent move <id> --station {s['name']})"
            )
        if na and not nc:
            problems.append(
                f"station {s['name']!r} has {na} agent(s) but NO channels — "
                f"they can only exchange direct @mentions once a channel "
                f"exists here."
            )
        if not s.get("open") and not allowed and (na or nc):
            problems.append(
                f"station {s['name']!r} has no token on its allow list — "
                f"no client can reach it.\n"
                f"      fix: station allow {s['name']} --token <prefix>"
            )

    print("\nchannels")
    for c in channels:
        here = {a["agent_id"] for a in by_station_agents.get(c["station_id"], [])}
        members = json.loads(c["members"] or "[]")
        missing = [mem for mem in members if mem not in here]
        print(f"  #{c['name']:<20} {(c.get('station_name') or '')[:16]:<16} "
              f"msgs={c['messages']:<6} members={len(members)}"
              f"{'  MEMBERS NOT IN THIS STATION: ' + ', '.join(missing[:4]) if missing else ''}")
        if missing:
            # The classic post-`agent move` stranding: the channel stayed put
            # while its members were re-homed, so nobody can see it any more.
            elsewhere = {
                a["station_name"] for a in agents if a["agent_id"] in missing
            }
            problems.append(
                f"channel #{c['name']} is in station "
                f"{c.get('station_name')!r} but {len(missing)} of its members "
                f"({', '.join(missing[:4])}) live in "
                f"{', '.join(sorted(x for x in elsewhere if x)) or 'no station'}"
                f" — they cannot see it.\n"
                f"      fix: channel move {c['name']} --to "
                f"{sorted(x for x in elsewhere if x)[0] if elsewhere else '<station>'}"
            )

    print("\nagents")
    seen: dict[str, int] = {}
    for a in agents:
        seen[a["agent_id"]] = seen.get(a["agent_id"], 0) + 1
    now = time.time()
    stale_agents: list[str] = []
    for a in agents:
        sid, aid = a["station_id"], a["agent_id"]
        st = CONN.execute(
            "SELECT COUNT(*) AS n, MIN(ts) AS oldest, MAX(delivered_at) AS seen "
            "FROM message_receipts WHERE station_id = %s AND agent_id = %s "
            "AND acked_at IS NULL", (sid, aid)
        ).fetchone()
        n_pending = st["n"] or 0
        oldest_days = (now - st["oldest"]) / 86400.0 if st["oldest"] else 0.0
        ever = CONN.execute(
            "SELECT MAX(delivered_at) AS seen FROM message_receipts "
            "WHERE station_id = %s AND agent_id = %s", (sid, aid)
        ).fetchone()["seen"]
        member = [
            c["name"] for c in by_station_chans.get(sid, [])
            if aid in json.loads(c["members"] or "[]")
        ]
        flags = []
        if seen[aid] > 1:
            flags.append("DUPLICATE")
        if not a.get("owner_prefix"):
            flags.append("unbound")
        if not ever:
            flags.append("never-received")
        if not member:
            flags.append("no-channel-membership")
        # Pinning: unacked messages old enough to be nearing the ceiling, i.e.
        # this agent is why something cannot be collected.
        if oldest_days > MAX_RETENTION_DAYS * 0.75:
            flags.append(f"PINNING({oldest_days:.0f}d)")
            stale_agents.append(aid)
        age = f" oldest={oldest_days:.0f}d" if n_pending else ""
        print(f"  {aid:<26} {(a.get('station_name') or '')[:16]:<16} "
              f"pending={n_pending:<4}{age} {' '.join(flags)}")
    dups = sorted({a for a, n in seen.items() if n > 1})
    if dups:
        problems.append(
            f"agent id(s) {', '.join(dups)} exist in more than one station — "
            f"every request naming them is refused as ambiguous.\n"
            f"      fix: agent rm <id> --station <the-wrong-one>"
        )
    if stale_agents:
        problems.append(
            f"agent(s) {', '.join(stale_agents[:6])} have unacked messages "
            f"approaching the {MAX_RETENTION_DAYS:g}-day ceiling — nothing "
            f"addressed to them can be collected until they ack, and at the "
            f"ceiling it is deleted unacked.\n"
            f"      fix: bring the agent back so it acks, or "
            f"agent rm <id> --station <s> to drop it from future audiences"
        )
    nomem = [
        a["agent_id"] for a in agents
        if not [c for c in by_station_chans.get(a["station_id"], [])
                if a["agent_id"] in json.loads(c["members"] or "[]")]
    ]
    if nomem:
        problems.append(
            f"{len(nomem)} agent(s) belong to no channel "
            f"({', '.join(nomem[:6])}{'…' if len(nomem) > 6 else ''}) — they "
            f"receive @mentions only, never channel broadcasts.\n"
            f"      fix: POST /channels/<name>/members  (or the "
            f"add_channel_member tool)"
        )

    print("\ntokens")
    for t in tokens:
        sts = t.get("stations") or []
        print(f"  {t['prefix']:<10} {(t.get('user') or '-')[:14]:<14} "
              f"{', '.join(sts) or 'NO STATIONS'}")
        if not sts:
            problems.append(
                f"token {t['prefix']} can reach no station.\n"
                f"      fix: station allow <station> --token {t['prefix']}"
            )

    # Last, because it is the section you want after a deploy and the one
    # worth having in view when the summary prints.
    _doctor_storage(problems)

    print("\n" + ("problems found:" if problems else "no problems found."))
    for i, p in enumerate(problems, 1):
        print(f"  {i}. {p}")
    return 1 if problems else 0


def _cli_doctor(args: argparse.Namespace) -> int:
    """Explain why an agent does or doesn't receive messages."""
    if not args.agent_id or args.agent_id == "all":
        return _doctor_all()
    agent = normalize_agent_id(args.agent_id)
    rows = CONN.execute(
        "SELECT a.station_id, s.name AS station_name, a.owner_token_hash "
        "FROM agents a LEFT JOIN stations s ON s.station_id = a.station_id "
        "WHERE a.agent_id = %s", (agent,)
    ).fetchall()
    print(f"agent: {agent}")
    if not rows:
        print("  NOT REGISTERED — no station, every request 403s.")
        print("  fix: agent add <id> --station <station>")
        return 1
    if len(rows) > 1:
        print(f"  WARNING: exists in {len(rows)} stations — requests are "
              f"ambiguous and 403.")
    for r in rows:
        sid, sname = r["station_id"], r["station_name"]
        print(f"  station: {sname} ({sid[:8]})")
        chans = CHANNELS.list_all(sid)
        print(f"  channels in this station: {len(chans)}")
        member_of = []
        for c in chans:
            mem = agent in json.loads(c["members"] or "[]")
            member_of.append(c["name"]) if mem else None
            print(f"    #{c['name']:<20} msgs={c['messages']:<6} "
                  f"member={'yes' if mem else 'NO'}")
        st = CONN.execute(
            "SELECT COUNT(*) AS n, MIN(ts) AS oldest, "
            "       SUM(delivered_at IS NOT NULL) AS seen "
            "FROM message_receipts WHERE station_id = %s AND agent_id = %s "
            "AND acked_at IS NULL", (sid, agent)
        ).fetchone()
        ever = CONN.execute(
            "SELECT MAX(delivered_at) AS t FROM message_receipts "
            "WHERE station_id = %s AND agent_id = %s", (sid, agent)
        ).fetchone()["t"]
        print("  last delivery: " + (
            time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(ever))
            if ever else "never — this agent has never received anything"
        ))
        print(f"  UNACKED (pending): {st['n'] or 0} message(s), "
              f"{st['seen'] or 0} already pushed at least once")
        if st["oldest"]:
            print(f"  oldest unacked: "
                  f"{(time.time() - st['oldest']) / 86400.0:.1f} days "
                  f"(ceiling {MAX_RETENTION_DAYS:g}d)")
        # Show them without consuming: this is a diagnostic, so it must not
        # stamp delivered_at.
        preview = _resolve_receipts(sid, _pending_rows(sid, agent, 5))
        for m in preview:
            print(f"    [{m['channel']}] {m['sender']}: {m['text'][:60]}")
        total = CONN.execute(
            "SELECT COUNT(*) c FROM transcripts WHERE station_id = %s", (sid,)
        ).fetchone()["c"]
        if total and not (st["n"] or 0):
            print(f"  note: the station holds {total} channel message(s), none "
                  f"of them addressed to this agent.")
            print(f"        an agent's audience is set when a message is "
                  f"posted: the members of the channel at that instant, plus "
                  f"anyone named in `to`. This agent is in "
                  f"({', '.join(member_of) or 'none'}). Messages posted before "
                  f"it joined were never for it.")
        if not chans:
            print("  note: this station has NO channels — check `channel list`;"
                  " a channel may be stranded in another station "
                  "(fix: channel move <name> --to <station>).")
        # A message stored and delivered to nobody. This is the shape of a
        # routing bug, and it reports success at post time, so nothing else
        # ever surfaces it — a whole class of "push is broken" turned out to be
        # posts that were never addressed to anyone.
        orphans = CONN.execute(
            "SELECT t.channel, t.sender, t.text, t.ts FROM transcripts t "
            "WHERE t.station_id = %s AND t.ts > %s AND NOT EXISTS ("
            "  SELECT 1 FROM message_receipts r WHERE r.station_id = "
            "t.station_id AND r.kind = 'channel' AND r.msg_id = t.id) "
            "ORDER BY t.ts DESC LIMIT 5",
            (sid, time.time() - 86400.0),
        ).fetchall()
        if orphans:
            print(f"  WARNING: {len(orphans)} message(s) in the last 24h "
                  f"reached NOBODY — stored with an empty audience:")
            for o in orphans:
                print(f"    [{o['channel']}] {o['sender']}: {o['text'][:50]}")
            print("    a post reaches the channel's members; an empty audience "
                  "means the channel had none besides the sender.")
    return 0


def _report_screen(out: dict, collected: dict | None) -> None:
    """One shape for every surface that screens, so the numbers mean the same
    thing in the CLI, the TUI and the admin route."""
    who = out.get("agent") or "everyone"
    head = "would screen" if out.get("preview") else "screened"
    print(f"{head} {out.get('station')} ({who})")
    kinds = out.get("by_kind") or {}
    detail = ", ".join(f"{k} {n}" for k, n in sorted(kinds.items())) or "none"
    print(f"  receipts       : {out.get('acked', 0)}  ({detail})")
    print(f"  agents touched : {out.get('agents', 0)}")
    if out.get("preview"):
        print("  nothing changed — this was a dry run")
        return
    if collected is not None:
        print(f"  collected      : "
              f"{collected.get('transcripts', 0)} channel posts, "
              f"{collected.get('dms', 0)} dms, "
              f"{collected.get('broadcasts', 0)} broadcasts")
    if out.get("open_broadcasts"):
        # Deleting a broadcast needs status='closed' as well as fully-acked,
        # so an open board survives on purpose. Said out loud, because an
        # operator who screened a station and still sees it would otherwise
        # reasonably conclude that screening had not worked.
        print(f"  note           : {out['open_broadcasts']} open broadcast(s) "
              f"silenced in every inbox, but not deleted — they are a live "
              f"board, not backlog")


def _cli_screen(args: argparse.Namespace, agent_id: str | None) -> int:
    """Shared by `station screen` and `agent screen`."""
    st = STATIONS.get(args.station)
    if not st:
        print(f"no such station: {args.station}", file=sys.stderr)
        return 1
    sid = st["station_id"]
    if agent_id and not CONN.execute(
        "SELECT 1 FROM agents WHERE station_id = %s AND agent_id = %s",
        (sid, agent_id),
    ).fetchone():
        print(f"no agent {agent_id!r} in {st['name']}", file=sys.stderr)
        return 1
    dry = bool(getattr(args, "dry_run", False))
    out = screen(sid, agent_id, preview=dry)
    _report_screen(out, None if dry else collect(sid))
    return 0


# ---------------------------------------------------------------------------
# migrate: bring a pre-MariaDB sqlite database across, once.
#
# One command, no flags. Everything safe is unconditional rather than optional,
# because the one mistake this cannot undo is importing the WRONG file — and
# that is not hypothetical here: a database detached by a file swap ran beside
# the real one for an unknown period, with nothing anywhere saying so.
#
# The source is opened read-only and never written, which is what keeps
# rollback total: point A2A_DB_* back at sqlite and the old broker runs.
# ---------------------------------------------------------------------------

# Copy order matters: every child follows its parent so a broken reference
# stops the import and names the row, rather than being laundered by
# foreign_key_checks=0.
MIGRATION_TABLES = (
    "stations", "tokens", "token_grants", "agents", "channels",
    "transcripts", "broadcasts", "bids", "dms", "md_files",
    "message_receipts", "stream_cursors", "agent_proposals",
    # Follows stations (its only parent). Absent from any pre-MariaDB source,
    # which both the copy and the verify tolerate by skipping unknown tables —
    # it is here so backup/restore carries locks rather than dropping them.
    "agent_transfer_denials",
    # AFTER transcripts, which it references: restore walks this tuple in
    # order, so a row here inserted before its parent would fail the FK.
    "message_addressees",
)
# Where a database plausibly lives. A fixed list, not a filesystem sweep: the
# answer is always one of these, and walking a container's disk to find a file
# is how a migration tool becomes something nobody trusts to run.
MIGRATION_SEARCH = ("/legacy", "/data", "/app", "/var/lib/a2a")


def _sqlite_ro(path: Path) -> sqlite3.Connection:
    """Open read-only. Two independent guarantees, with the :ro mount."""
    conn = sqlite3.connect(f"file:{path}?mode=ro", uri=True)
    conn.row_factory = sqlite3.Row
    return conn


def _wal_bytes(path: Path) -> int:
    """Size of the source's write-ahead log, from the filesystem alone.

    Deliberately does not open the database: this has to work for a source
    SQLite cannot read yet, which is exactly the case a WAL creates.
    """
    wal = path.with_name(path.name + "-wal")
    try:
        return wal.stat().st_size if wal.exists() else 0
    except OSError:
        return 0


def _stage_source(path: Path) -> tuple[Path, tempfile.TemporaryDirectory | None]:
    """Give back a path that can be read completely, and a temp dir to clean.

    A source with a write-ahead log cannot simply be copied: the WAL holds
    committed transactions that are not in the database file, so importing the
    file alone silently loses them. Measured on a real one — with the writer
    still attached, a copy of the .db by itself did not even contain the
    tables.

    So copy the three files TOGETHER into somewhere writable and checkpoint the
    copy. The source is never opened for writing and never touched, which is
    what keeps it usable as the rollback.

    Returns (path, None) unchanged when there is no WAL, so the ordinary case
    gains nothing to go wrong.
    """
    n = _wal_bytes(path)
    if not n:
        return path, None

    print(f"  {path} has a {n / 1048576:.1f} MB write-ahead log — its newest "
          f"writes are not in the database file yet.")
    tmp = tempfile.TemporaryDirectory(prefix="a2a-stage-")
    try:
        need = path.stat().st_size + n
        free = shutil.disk_usage(tmp.name).free
        if free < need * 1.2:
            raise OSError(
                f"needs about {need / 1048576:.0f} MB to stage a checkpointed "
                f"copy, and {tempfile.gettempdir()} has {free / 1048576:.0f} MB"
            )
        staged = Path(tmp.name) / path.name
        for suffix in ("", "-wal", "-shm"):
            src = path.with_name(path.name + suffix)
            if not src.exists():
                continue
            dst = staged.with_name(staged.name + suffix)
            # copyfile, not copy2: the source is on a read-only mount, so its
            # files are read-only, and copy2 would preserve that — leaving a
            # copy the checkpoint below cannot write to. Set the mode
            # explicitly rather than relying on whatever the umask gives.
            shutil.copyfile(src, dst)
            dst.chmod(0o600)
        # On the COPY, which is writable. TRUNCATE folds the log into the
        # database file and empties it, so everything downstream sees one
        # complete file.
        conn = sqlite3.connect(staged)
        try:
            conn.execute("PRAGMA wal_checkpoint(TRUNCATE)")
        finally:
            conn.close()
        left = _wal_bytes(staged)
        if left:
            raise OSError(f"checkpoint left {left} bytes in the log")
        print(f"  staged a checkpointed copy: "
              f"{staged.stat().st_size / 1048576:.1f} MB "
              f"(the source is untouched)")
        return staged, tmp
    except Exception:
        tmp.cleanup()
        raise


def _inspect_sqlite(path: Path, original: Path | None = None) -> dict | None:
    """Describe a candidate, or None if it is not an a2a database.

    `newest` is the newest CONTENT, never the file's mtime. A copy resets
    mtime; the detached database in the incident that prompted all this had a
    recent mtime and content frozen at the moment of the swap, while the live
    file looked older. Row timestamps tell them apart and `ls -l` does not.

    `original` is the path to REPORT when reading a staged copy: the operator
    thinks about the file they can see, not the temp file we made.
    """
    shown = original or path

    def unreadable() -> dict | None:
        """A source SQLite refused. Keep it if it has a WAL.

        A WAL that needs recovery cannot be read without write access, which a
        read-only mount does not give — sqlite says only "unable to open
        database file". Returning None would drop the candidate from the scan
        entirely, telling the operator there is no database when it is right in
        front of them, which is worse than any refusal. Keep it with what the
        filesystem alone can say; staging then gives real counts.
        """
        n = _wal_bytes(path)
        if not n:
            return None
        st = path.stat()
        return {
            "path": shown, "counts": {}, "newest": 0.0, "size": st.st_size,
            "mtime": st.st_mtime, "wal_bytes": n, "v1": False,
            "unread": True,      # counts unknown until it is staged
        }

    try:
        conn = _sqlite_ro(path)
    except Exception:
        return unreadable()
    try:
        tables = {
            r["name"] for r in conn.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        if not {"stations", "agents", "channels"} <= tables:
            return None
        counts, newest = {}, 0.0
        for t in MIGRATION_TABLES:
            if t not in tables:
                counts[t] = 0
                continue
            counts[t] = conn.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
        for t, col in (("transcripts", "ts"), ("dms", "ts"),
                       ("message_receipts", "ts"), ("stations", "created_at"),
                       ("tokens", "created_at"), ("agents", "created_at")):
            if t not in tables:
                continue
            try:
                v = conn.execute(f"SELECT MAX({col}) AS m FROM {t}").fetchone()["m"]
            except sqlite3.Error:
                continue
            newest = max(newest, float(v or 0))
        return {
            # The path the OPERATOR sees. When reading a staged copy this is
            # the original, because the temp file is our business, not theirs.
            "path": shown,
            "counts": counts,
            "newest": newest,
            "size": path.stat().st_size,
            "mtime": path.stat().st_mtime,
            # A WAL with content means the file ALONE is missing the newest
            # writes. `migrate` stages a checkpointed copy rather than
            # refusing, so this is reported, not a dead end.
            "wal_bytes": _wal_bytes(path),
            "v1": not _sqlite_has_col(conn, "agents", "station_id"),
            "unread": False,
        }
    except Exception:
        # sqlite3.connect is lazy: a file it cannot actually open gets past
        # the connect above and fails HERE, on the first statement. Without
        # this the unreadable-WAL case fell through to None and the candidate
        # vanished — which is what happened the first time this was tried.
        return unreadable()
    finally:
        conn.close()


def _sqlite_has_col(conn: sqlite3.Connection, table: str, col: str) -> bool:
    return any(r["name"] == col for r in conn.execute(f"PRAGMA table_info({table})"))


def _find_sqlite() -> list[dict]:
    """Every a2a database in the known locations, best candidate first."""
    seen, found = set(), []
    roots = [LEGACY_DB_FILE, *(Path(d) / "a2a.db" for d in MIGRATION_SEARCH)]
    # Every *.db in each search directory AND in the one A2A_DB_FILE points at:
    # the likeliest place a second, diverged copy lives is right beside the
    # first — a backup, a restore that was never cleaned up, a hand copy.
    # Looking only for the exact name is how the other one stays invisible.
    for d in (*MIGRATION_SEARCH, LEGACY_DB_FILE.parent):
        p = Path(d)
        roots.extend(sorted(p.glob("*.db")) if p.is_dir() else [])
    for p in roots:
        try:
            rp = p.resolve()
        except Exception:
            continue
        if rp in seen or not rp.is_file():
            continue
        seen.add(rp)
        info = _inspect_sqlite(rp)
        if info:
            found.append(info)
    # Newest content first. Not mtime — see _inspect_sqlite.
    found.sort(key=lambda i: i["newest"], reverse=True)
    return found


def _describe(info: dict) -> str:
    c = info["counts"]
    when = (time.strftime("%Y-%m-%d %H:%M UTC", time.gmtime(info["newest"]))
            if info["newest"] else "empty")
    msgs = c.get("transcripts", 0) + c.get("dms", 0)
    return (f"{str(info['path']):<34} {c.get('stations', 0)} stations · "
            f"{c.get('agents', 0)} agents · {msgs} messages   newest {when}")


def _cli_migrate(args: argparse.Namespace) -> int:
    """Copy a sqlite database into MariaDB. Interactive, and refuses to guess."""
    if getattr(args, "path", None):
        chosen = _inspect_sqlite(Path(args.path))
        if not chosen:
            print(f"{args.path} is not an a2a sqlite database", file=sys.stderr)
            return 1
        found = [chosen]
    else:
        found = _find_sqlite()
        if not found:
            print("found no a2a sqlite database in "
                  f"{', '.join(MIGRATION_SEARCH)} or {LEGACY_DB_FILE}.\n"
                  "Pass the path explicitly if it lives somewhere else.",
                  file=sys.stderr)
            return 1
        print(f"found {len(found)} a2a database"
              f"{'s' if len(found) > 1 else ''}:\n")
        for i, info in enumerate(found, 1):
            print(f"  {i}) {_describe(info)}")
        print()
        if len(found) > 1:
            # Two live databases is exactly the state that caused this move.
            # Say so; do not sort one quietly to the top and import it.
            a, b = found[0], found[1]
            if a["newest"] and b["newest"] and a["counts"] != b["counts"]:
                print("  1 has rows 2 does not; these two have DIVERGED.\n"
                  "  That is a decision for you, not a sort order.\n")
            if not sys.stdin.isatty():
                sys.stdout.flush()
                print("several candidates and no path given — refusing to "
                      "choose. Re-run with the path, or on a terminal.",
                      file=sys.stderr)
                return 1
        chosen = found[0]

    # A write-ahead log is handled, not refused. Stage a checkpointed copy —
    # the source is never written — and re-inspect it, because a candidate
    # whose WAL could not be read has no counts yet.
    original = Path(chosen["path"])
    staged, tmp = original, None
    if chosen["wal_bytes"]:
        sys.stdout.flush()
        try:
            staged, tmp = _stage_source(original)
        except Exception as e:
            print(f"refusing: {original} has a write-ahead log holding "
                  f"{chosen['wal_bytes']} bytes that are not in the database "
                  f"file, and it could not be staged: {e}\n"
                  "Importing the file alone would silently leave those writes "
                  "behind.", file=sys.stderr)
            return 1
        restaged = _inspect_sqlite(staged, original=original)
        if not restaged:
            tmp.cleanup()
            print(f"refusing: {original} could not be read even after its "
                  "write-ahead log was folded in.", file=sys.stderr)
            return 1
        chosen = restaged

    try:
        return _migrate_chosen(chosen, staged, original)
    finally:
        if tmp is not None:
            tmp.cleanup()


def _migrate_chosen(chosen: dict, staged: Path, original: Path) -> int:
    """The import itself, against a source that is complete on disk."""
    sys.stdout.flush()
    if chosen["v1"]:
        print(f"refusing: {chosen['path']} predates station scoping. "
              "Open it once with a pre-MariaDB build to upgrade it in place, "
              "then migrate.", file=sys.stderr)
        return 1

    existing = {
        t: CONN.execute(f"SELECT COUNT(*) AS n FROM {t}").fetchone()["n"]
        for t in MIGRATION_TABLES
    }
    # `stations` always holds the default row that _init_schema creates, so a
    # fresh target is "nothing but that", not "nothing".
    sys.stdout.flush()
    if any(v for t, v in existing.items() if t != "stations") or \
            existing["stations"] > 1:
        print("refusing: the MariaDB database already holds data "
              f"({', '.join(f'{t} {n}' for t, n in existing.items() if n)}).\n"
              "Import into an empty database — merging two is not something "
              "this command can do safely.", file=sys.stderr)
        return 1

    if sys.stdin.isatty():
        print(f"migrating {original} -> "
              f"{DB_USER}@{DB_HOST}:{DB_PORT}/{DB_NAME}")
        if not input("proceed? [y/N] ").strip().lower().startswith("y"):
            print("nothing was changed.")
            return 0

    return _do_migrate(dict(chosen, path=staged), shown=original)


def _do_migrate(info: dict, shown: Path | None = None) -> int:
    """Copy the source in. `shown` is the path to REPORT when `info["path"]`
    is a staged copy — the operator thinks about the file they can see."""
    label = shown or info["path"]
    src = _sqlite_ro(info["path"])
    moved: dict[str, int] = {}
    try:
        src_tables = {
            r["name"] for r in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        with CONN:
            # _startup() creates the bootstrap `default` station, and the
            # source almost certainly has its own — with a different
            # description and created_at. INSERT IGNORE keeps the row already
            # there, so the source's would be silently dropped and the import
            # would be quietly lossy. Clear the bootstrap rows first: the
            # source is the truth here, and the target was already checked
            # to hold nothing else.
            CONN.execute("SET SESSION foreign_key_checks = 0")
            for table in reversed(MIGRATION_TABLES):
                CONN.execute(f"DELETE FROM `{table}`")
            CONN.execute("SET SESSION foreign_key_checks = 1")
            for table in MIGRATION_TABLES:
                if table not in src_tables:
                    continue
                rows = list(src.execute(f"SELECT * FROM {table}"))
                if not rows:
                    moved[table] = 0
                    continue
                cols = list(rows[0].keys())
                sql = (f"INSERT INTO `{table}` "
                       f"({', '.join('`' + c + '`' for c in cols)}) "
                       f"VALUES ({', '.join(['%s'] * len(cols))})")
                n = 0
                for i in range(0, len(rows), 500):
                    chunk = [tuple(r[c] for c in cols) for r in rows[i:i + 500]]
                    with CONN._lease() as conn:
                        with conn.cursor() as cur:
                            cur.executemany(sql, chunk)
                    n += len(chunk)
                moved[table] = n
                print(f"  {table:18} {n}")

            # A database written before messages had a shelf life has no
            # expires_at, so the copy leaves it at the column default of 0 —
            # and the collector deletes `WHERE expires_at <= now`. Migrating
            # such a database without this would silently destroy every
            # message it holds on the first collection.
            #
            # The rule is the one the retired in-place migration applied:
            # date them to the ceiling they already had, so nothing is
            # collected earlier than it would have been.
            for table in ("transcripts", "dms", "message_receipts"):
                fixed = CONN.execute(
                    f"UPDATE `{table}` SET expires_at = ts + %s "
                    f"WHERE expires_at = 0",
                    (MAX_RETENTION,),
                ).rowcount
                if fixed:
                    print(f"  {table:18} {fixed} dated to the retention "
                          f"ceiling (written before expiry existed)")
    finally:
        src.close()

    # Verify by CONTENT, not just counts: equal row counts would still pass
    # with every value mangled by a bad utf8mb4 conversion, and a non-ASCII
    # agent name is exactly what that damages quietly.
    print("\nverifying...")
    sys.stdout.flush()
    bad = _verify_migration(info["path"])
    if bad:
        for line in bad:
            print(f"  MISMATCH {line}", file=sys.stderr)
        print("\nthe copy does not match the source. Nothing was removed from "
              "the sqlite file; fix the cause and import into an empty "
              "database again.", file=sys.stderr)
        return 1
    print("  counts and checksums match")
    log(f"migrated {label} ({sum(moved.values())} rows)",
        event="migrate", level="WARN")
    print(f"\ndone. {label} was not modified.")
    return 0


def _verify_migration(path: Path) -> list[str]:
    """Compare each table both ways: row count, and a checksum of the rows."""
    src = _sqlite_ro(path)
    problems = []
    try:
        src_tables = {
            r["name"] for r in src.execute(
                "SELECT name FROM sqlite_master WHERE type='table'"
            )
        }
        for table in MIGRATION_TABLES:
            if table not in src_tables:
                continue
            s_rows = list(src.execute(f"SELECT * FROM {table}"))
            d_rows = list(CONN.execute(f"SELECT * FROM `{table}`"))
            if len(s_rows) != len(d_rows):
                problems.append(
                    f"{table}: {len(s_rows)} in sqlite, {len(d_rows)} in mariadb"
                )
                continue
            if not s_rows:
                continue
            cols = sorted(s_rows[0].keys())
            if _digest(s_rows, cols) != _digest(d_rows, cols):
                problems.append(f"{table}: same row count, different content")
    finally:
        src.close()
    return problems


def _digest(rows, cols) -> str:
    """Order-independent digest of a result set, over the named columns.

    Floats are formatted rather than repr'd: sqlite REAL and MariaDB DOUBLE
    both hold IEEE doubles, but the two drivers spell them differently and a
    textual difference there is not a data difference.
    """
    def cell(v):
        if v is None:
            return "\x00"
        if isinstance(v, float):
            return f"{v:.6f}"
        if isinstance(v, (bytes, bytearray)):
            return v.decode("utf-8", "replace")
        return str(v)

    h = hashlib.sha256()
    for line in sorted("\x1f".join(cell(r[c]) for c in cols) for r in rows):
        h.update(line.encode("utf-8"))
        h.update(b"\x1e")
    return h.hexdigest()


# ---------------------------------------------------------------------------
# backup / restore: one .tgz, written and read by this script alone.
#
# Deliberately not a shell-out to mariadb-dump. The broker image is
# python:slim and has no mariadb client in it, so a dump-based backup would
# mean either a bigger image or a recipe that only works from the database
# container — and a backup you can only take from somewhere else is a backup
# nobody takes. This uses the connection the broker already has.
#
# The archive holds one JSON-lines file per table plus a meta.json carrying
# per-table counts and checksums, so a restore can prove it landed intact
# instead of hoping.
# ---------------------------------------------------------------------------


def _cli_backup(args: argparse.Namespace) -> int:
    """Write every table to a .tgz, from one consistent snapshot."""
    out = Path(args.path or
               f"a2a-{time.strftime('%Y%m%d-%H%M%S')}.tgz").expanduser()
    if out.is_dir():
        out = out / f"a2a-{time.strftime('%Y%m%d-%H%M%S')}.tgz"
    if out.exists() and not args.force:
        print(f"refusing: {out} exists (use --force to overwrite)",
              file=sys.stderr)
        return 1

    meta: dict = {"version": VERSION, "taken_at": time.time(),
                  "database": DB_NAME, "tables": {}}
    tmp = out.with_suffix(out.suffix + ".part")
    try:
        with tarfile.open(tmp, "w:gz") as tar:
            # One connection, one snapshot. `with CONN:` pins a connection and
            # opens a transaction, and InnoDB's default REPEATABLE READ fixes
            # the read view at the first SELECT — so every table below is read
            # as of the same instant. Without that a message could land in
            # message_receipts while its transcript row was missed, and the
            # backup would restore a state the broker never actually had.
            #
            # (Setting the isolation level explicitly here is not possible and
            # not needed: it has to precede BEGIN, which has already run.)
            with CONN:
                for table in (*MIGRATION_TABLES, "logs"):
                    rows = list(CONN.execute(f"SELECT * FROM `{table}`"))
                    payload = "".join(
                        json.dumps(r, default=str, ensure_ascii=False) + "\n"
                        for r in rows
                    ).encode("utf-8")
                    info = tarfile.TarInfo(f"{table}.jsonl")
                    info.size = len(payload)
                    info.mtime = int(meta["taken_at"])
                    tar.addfile(info, io.BytesIO(payload))
                    cols = sorted(rows[0].keys()) if rows else []
                    meta["tables"][table] = {
                        "rows": len(rows),
                        "digest": _digest(rows, cols) if rows else "",
                        "columns": cols,
                    }
                    print(f"  {table:18} {len(rows)}")
            blob = json.dumps(meta, indent=2).encode("utf-8")
            info = tarfile.TarInfo("meta.json")
            info.size = len(blob)
            info.mtime = int(meta["taken_at"])
            tar.addfile(info, io.BytesIO(blob))
        tmp.replace(out)     # atomic: no half-written file is ever named .tgz
    except Exception:
        tmp.unlink(missing_ok=True)
        raise

    total = sum(t["rows"] for t in meta["tables"].values())
    size = out.stat().st_size
    print(f"\n{out}  ({total} rows, {size / 1048576:.1f} MB)")
    log(f"backup written to {out} ({total} rows)", event="backup")
    return 0


def _cli_restore(args: argparse.Namespace) -> int:
    """Load a .tgz written by `backup` into an empty database."""
    src = Path(args.path).expanduser()
    if not src.is_file():
        print(f"no such file: {src}", file=sys.stderr)
        return 1

    existing = {
        t: CONN.execute(f"SELECT COUNT(*) AS n FROM `{t}`").fetchone()["n"]
        for t in MIGRATION_TABLES
    }
    if any(v for t, v in existing.items() if t != "stations") or \
            existing["stations"] > 1:
        sys.stdout.flush()
        print("refusing: this database already holds data "
              f"({', '.join(f'{t} {n}' for t, n in existing.items() if n)}).\n"
              "Restore into an empty database — merging two is not something "
              "this command can do safely.", file=sys.stderr)
        return 1

    with tarfile.open(src, "r:gz") as tar:
        try:
            meta = json.loads(tar.extractfile("meta.json").read())
        except Exception:
            print(f"{src} is not an a2a backup (no meta.json)", file=sys.stderr)
            return 1
        print(f"restoring {src} — taken "
              f"{time.strftime('%Y-%m-%d %H:%M UTC', time.gmtime(meta['taken_at']))}"
              f" by a2a-mcp {meta.get('version', '?')}")
        if sys.stdin.isatty() and not args.yes:
            if not input("proceed? [y/N] ").strip().lower().startswith("y"):
                print("nothing was changed.")
                return 0
        with CONN:
            CONN.execute("SET SESSION foreign_key_checks = 0")
            for table in reversed((*MIGRATION_TABLES, "logs")):
                CONN.execute(f"DELETE FROM `{table}`")
            for table in (*MIGRATION_TABLES, "logs"):
                try:
                    raw = tar.extractfile(f"{table}.jsonl").read()
                except Exception:
                    continue          # a table added since the backup
                rows = [json.loads(l) for l in raw.splitlines() if l.strip()]
                if not rows:
                    continue
                cols = list(rows[0].keys())
                sql = (f"INSERT INTO `{table}` "
                       f"({', '.join('`' + c + '`' for c in cols)}) "
                       f"VALUES ({', '.join(['%s'] * len(cols))})")
                for i in range(0, len(rows), 500):
                    chunk = [tuple(r[c] for c in cols) for r in rows[i:i + 500]]
                    with CONN._lease() as conn:
                        with conn.cursor() as cur:
                            cur.executemany(sql, chunk)
                print(f"  {table:18} {len(rows)}")
            CONN.execute("SET SESSION foreign_key_checks = 1")

    print("\nverifying...")
    bad = []
    for table, want in meta["tables"].items():
        rows = list(CONN.execute(f"SELECT * FROM `{table}`"))
        if len(rows) != want["rows"]:
            bad.append(f"{table}: {want['rows']} in backup, {len(rows)} restored")
        elif want["digest"] and _digest(rows, want["columns"]) != want["digest"]:
            bad.append(f"{table}: same row count, different content")
    if bad:
        for line in bad:
            print(f"  MISMATCH {line}", file=sys.stderr)
        return 1
    print("  counts and checksums match")
    log(f"restored from {src}", event="restore", level="WARN")
    return 0


def _cli_messages(args: argparse.Namespace) -> int:
    """What is in a station, and what is holding it."""
    stations = ([STATIONS.get(args.station)] if args.station
                else STATIONS.list())
    if args.station and not stations[0]:
        print(f"no such station: {args.station}", file=sys.stderr)
        return 1
    for st in stations:
        sid = st["station_id"]
        if args.mark:
            try:
                out = mark_segment(sid, args.mark, preview=args.dry_run)
            except (KeyError, ValueError) as e:
                print(f"error: {e}", file=sys.stderr)
                return 1
            verb = "would mark" if args.dry_run else "marked"
            left = sum(out.get("untouched", {}).values())
            print(f"{st['name']}: {verb} {out['found'] - left} message(s) in "
                  f"{out['label']!r}")
            if left:
                print("  untouched: " + ", ".join(
                    f"{k} {n}" for k, n in out["untouched"].items())
                    + " — no expires_at to set; a broadcast ages by "
                      "created_at and backdating that would forge it")
            if not args.dry_run:
                got = out.get("collected") or {}
                print("  collected: " + ", ".join(
                    f"{k} {v}" for k, v in got.items() if v) or "  collected: 0")
                if out.get("open_broadcasts"):
                    print(f"  note: {out['open_broadcasts']} open broadcast(s) "
                          "silenced but not deleted — closing one is what lets "
                          "the collector take it")
            continue

        stats = message_stats(sid)
        print(f"\n{st['name']}  —  {stats['total']} message(s)")
        for group, key in (("ack", "ack_total"), ("expiry", "expiry_total"),
                           ("age", "age_total")):
            for r in stats["rows"]:
                if r["group"] != group:
                    continue
                kinds = " ".join(f"{k}:{n}"
                                 for k, n in (r["by_kind"] or {}).items())
                print(f"  {r['label']:<24} {r['count']:>6}   {kinds}")
            # The two views are independent partitions of the same messages:
            # each covers every message once, so they do not add together.
            note = ""
            if group == "expiry" and stats["broadcasts_no_shelf_life"]:
                note = (f" ({stats['broadcasts_no_shelf_life']} broadcast(s) "
                        "have no shelf life; they age out by created_at)")
            print(f"  {'':<24} {'──':>6}   {group} view covers "
                  f"{stats[key]}{note}\n")
        if stats["holders"]:
            held = ", ".join(f"{a} {n}" for a, n in stats["holders"][:6])
            print(f"  unacked receipts held by: {held}")
    return 0


def _cli_logs(args: argparse.Namespace) -> int:
    """Read the log table. Newest LAST, so a terminal reads like `tail -f`."""
    where, params = ["1 = 1"], []
    if args.station:
        st = STATIONS.get(args.station)
        if not st:
            print(f"no such station: {args.station}", file=sys.stderr)
            return 1
        where.append("station = %s")
        params.append(st["name"])
    if args.level:
        floor = LOG_LEVELS.get(args.level.upper())
        if floor is None:
            print(f"unknown level: {args.level} "
                  f"(one of {', '.join(LOG_LEVELS)})", file=sys.stderr)
            return 1
        keep = [k for k, v in LOG_LEVELS.items() if v >= floor]
        where.append(f"level IN ({','.join(['%s'] * len(keep))})")
        params.extend(keep)
    if args.event:
        where.append("event = %s")
        params.append(args.event)
    if args.since:
        try:
            back = parse_duration(args.since)
        except ValueError as e:
            print(f"bad --since: {e}", file=sys.stderr)
            return 1
        where.append("ts >= %s")
        params.append(time.time() - back)

    # Newest N by the index, then flipped for reading.
    rows = list(CONN.execute(
        f"SELECT * FROM logs WHERE {' AND '.join(where)} "
        f"ORDER BY ts DESC LIMIT %s",
        (*params, max(1, args.tail)),
    ))
    if not rows:
        print("no matching log lines")
        return 0
    for r in reversed(rows):
        when = time.strftime("%Y-%m-%d %H:%M:%SZ", time.gmtime(r["ts"]))
        bits = [when, f"{r['level']:<5}"]
        if r["station"]:
            bits.append(f"[{r['station']}]")
        if r["actor"]:
            bits.append(str(r["actor"]))
        if r["event"]:
            bits.append(f"{r['event']}:")
        bits.append(r["message"])
        print(" ".join(bits))
    return 0


def _cli_compact(args: argparse.Namespace) -> int:
    """Retire finished messages. Safe to run against the live broker."""
    sid = None
    if getattr(args, "station", None):
        st = STATIONS.get(args.station)
        if not st:
            print(f"no such station: {args.station}", file=sys.stderr)
            return 1
        sid = st["station_id"]
    stats = collect(sid)
    print("collected:")
    print(f"  dms acked by everyone      {stats.get('dms', 0)}")
    print(f"  broadcasts closed + acked  {stats.get('broadcasts', 0)}")
    print(f"  channel messages past TTL  {stats.get('transcripts', 0)}")
    print(f"  expired unread             {stats.get('expired', 0)}")
    print(f"  broadcasts past ceiling    {stats.get('expired_broadcasts', 0)}")
    print(f"  orphaned receipts          {stats.get('receipts', 0)}")
    left = CONN.execute(
        "SELECT COUNT(*) AS n FROM message_receipts WHERE acked_at IS NULL"
    ).fetchone()["n"]
    print(f"\n{left} message(s) still unacked and therefore kept.")
    print("Space is reused by the database but not returned to the disk; "
          "run `vacuum` for that.")
    return 0


def _table_bytes() -> dict[str, int]:
    """On-disk size per table, from information_schema.

    Approximate by nature — InnoDB reports whole extents — but it is the same
    number before and after, so the difference is honest.
    """
    # Aliased on purpose: information_schema reports its own column names in
    # different case depending on the server, so indexing a row by the bare
    # name is a KeyError waiting for the next database.
    return {
        r["tname"]: int(r["nbytes"] or 0)
        for r in CONN.execute(
            "SELECT table_name AS tname, "
            "(data_length + index_length) AS nbytes "
            "FROM information_schema.tables WHERE table_schema = %s",
            (DB_NAME,),
        )
    }


def _cli_vacuum(_args: argparse.Namespace) -> int:
    """Return freed space to the filesystem.

    Operator action: OPTIMIZE TABLE rebuilds each InnoDB table, which takes a
    write lock on it for the duration and needs roughly its size in scratch
    space. Collection reuses space inside the tablespace on its own, so this is
    only worth running after something large was retired.
    """
    before = _table_bytes()
    for table in sorted(before):
        # Not parameterizable — an identifier, not a value. The names come from
        # information_schema for this schema, so there is nothing user-supplied
        # in the string.
        CONN.execute(f"OPTIMIZE TABLE `{table}`")
    after = _table_bytes()
    b, a = sum(before.values()), sum(after.values())
    for table in sorted(before):
        delta = before[table] - after.get(table, 0)
        if delta:
            print(f"  {table:18} {delta / 1048576:+.1f} MB")
    print(f"{b / 1048576:.1f} MB -> {a / 1048576:.1f} MB")
    return 0


def _cli_tui(_args: argparse.Namespace) -> int:
    try:
        import curses
    except ImportError:
        return _tui_fallback()
    if not sys.stdin.isatty() or not sys.stdout.isatty():
        return _tui_fallback()
    curses.wrapper(lambda scr: _Tui(scr).run())
    return 0


def _cli_serve(_args: argparse.Namespace) -> int:
    # Trust X-Forwarded-Proto. Without it request.url.scheme reads "http"
    # behind a TLS-terminating proxy, and the installers we serve get an
    # http:// base baked in — whereupon the edge 301s to https and BOTH fetch
    # and curl strip the Authorization header, because a scheme change is a
    # cross-origin redirect. The symptom is a client that 401s with a token it
    # is demonstrably sending.
    #
    # forwarded_allow_ips="*" is safe only because this process is never
    # exposed directly: it is reachable solely through the proxy network (see
    # docker-compose.yml), so nothing else can forge those headers.
    uvicorn.run(build_app(), host=HOST, port=PORT,
                proxy_headers=True, forwarded_allow_ips="*")
    return 0


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="a2a-mcp",
        description="A2A↔MCP communications hub with per-station token auth.",
    )
    # Declared so it appears in --help and in the usage line. The flag is
    # actually served much earlier, before the database is opened (see the
    # guard above CONN); by the time argparse runs, the answer is already out.
    p.add_argument("--version", action="version",
                   version=f"a2a-mcp {VERSION}")
    sub = p.add_subparsers(dest="cmd")

    sp = sub.add_parser("serve", help="run the HTTP server (default)")
    sp.set_defaults(func=_cli_serve)

    tp = sub.add_parser(
        "tui", help="interactive admin for stations, tokens and agents"
    )
    tp.set_defaults(func=_cli_tui)

    st = sub.add_parser("station", help="manage stations")
    st_sub = st.add_subparsers(dest="station_cmd", required=True)
    st_create = st_sub.add_parser("create", help="create a station")
    st_create.add_argument("name")
    st_create.add_argument("--description", default="")
    st_create.set_defaults(func=_cli_station)
    st_list = st_sub.add_parser("list", help="list stations")
    st_list.set_defaults(func=_cli_station)
    st_delete = st_sub.add_parser("delete", help="delete a station")
    st_delete.add_argument("id_or_name")
    st_delete.set_defaults(func=_cli_station)
    st_allow = st_sub.add_parser(
        "allow", help="put a token on this station's allow list (or open it)"
    )
    st_allow.add_argument("id_or_name")
    st_allow.add_argument("--token", default=None,
                          help="token or 8-char prefix to allow")
    st_allow.add_argument("--any", action="store_true", default=False,
                          help="open the station: '*', every valid token")
    st_allow.set_defaults(func=_cli_station)
    st_disallow = st_sub.add_parser(
        "disallow", help="remove a token from the allow list (or close it)"
    )
    st_disallow.add_argument("id_or_name")
    st_disallow.add_argument("--token", default=None)
    st_disallow.add_argument("--any", action="store_true", default=False,
                             help="close the station again")
    st_disallow.set_defaults(func=_cli_station)
    st_screen = st_sub.add_parser(
        "screen",
        help="ack ALL unacked messages in a station, so the collector can "
             "retire them (acks; deletes nothing itself)",
    )
    st_screen.add_argument("station")
    st_screen.add_argument("--dry-run", action="store_true", default=False,
                           help="count what would be acked, change nothing")
    st_screen.set_defaults(func=_cli_station, station_cmd="screen")
    st_show = st_sub.add_parser(
        "show", help="show a station's allow list and agents"
    )
    st_show.add_argument("id_or_name")
    st_show.set_defaults(func=_cli_station)
    st_purge = st_sub.add_parser(
        "purge", help="delete stations with no agents/channels/messages"
    )
    st_purge.add_argument("--empty", action="store_true", default=True,
                          help="(only mode supported; kept explicit)")
    st_purge.add_argument("--yes", action="store_true", default=False)
    st_purge.set_defaults(func=_cli_station)

    tk = sub.add_parser("token", help="manage user tokens")
    tk_sub = tk.add_subparsers(dest="token_cmd", required=True)
    tk_create = tk_sub.add_parser(
        "create",
        help="mint a bare user token (add it to a station with 'station allow')",
    )
    tk_create.add_argument("--user", default="", help="owner name")
    tk_create.add_argument("--label", default="")
    tk_create.set_defaults(func=_cli_token)
    tk_list = tk_sub.add_parser("list", help="list tokens")
    tk_list.add_argument("--station", default=None)
    tk_list.add_argument(
        "--include-revoked", action="store_true", default=False
    )
    tk_list.set_defaults(func=_cli_token)
    tk_revoke = tk_sub.add_parser(
        "revoke", help="revoke a token by full value or 8-char prefix"
    )
    tk_revoke.add_argument("token_or_prefix")
    tk_revoke.set_defaults(func=_cli_token)
    tk_delete = tk_sub.add_parser(
        "delete", help="hard-delete a token and its grants"
    )
    tk_delete.add_argument("token_or_prefix")
    tk_delete.set_defaults(func=_cli_token)
    tk_purge = tk_sub.add_parser("purge", help="bulk-delete tokens")
    tk_purge.add_argument("--revoked", action="store_true", default=False,
                          help="only revoked tokens")
    tk_purge.add_argument("--station", default=None)
    tk_purge.add_argument("--yes", action="store_true", default=False)
    tk_purge.set_defaults(func=_cli_token)

    dr = sub.add_parser(
        "doctor",
        help="diagnose delivery: no args = whole broker, or name one agent",
    )
    dr.add_argument("agent_id", nargs="?", default=None,
                    help="agent id (or 'all' / omit for a full check-up)")
    dr.set_defaults(func=_cli_doctor)

    pg = sub.add_parser(
        "ping", help="post a witness message to an agent and prove delivery"
    )
    pg.add_argument("agent_id")
    pg.add_argument("--channel", default=None, help="which channel to post in")
    pg.add_argument("--sender", default="doctor")
    pg.add_argument("--text", default=None)
    pg.set_defaults(func=_cli_ping)

    cp = sub.add_parser(
        "compact",
        help="delete messages everyone acked, and anything past its TTL",
    )
    cp.add_argument("--station", default=None, help="limit to one station")
    cp.set_defaults(func=_cli_compact)

    vc = sub.add_parser(
        "vacuum", help="return space freed by compact to the filesystem"
    )
    vc.set_defaults(func=_cli_vacuum)

    mg = sub.add_parser(
        "migrate",
        help="copy a pre-MariaDB sqlite database in (asks before writing)",
    )
    mg.add_argument("path", nargs="?", default=None,
                    help="the sqlite file; omit to be shown what was found")
    mg.set_defaults(func=_cli_migrate)

    bk = sub.add_parser(
        "backup", help="write the whole database to a .tgz")
    bk.add_argument("path", nargs="?", default=None,
                    help="output file or directory "
                         "(default a2a-<date>.tgz here)")
    bk.add_argument("--force", action="store_true", default=False,
                    help="overwrite an existing file")
    bk.set_defaults(func=_cli_backup)

    rs = sub.add_parser(
        "restore", help="load a backup .tgz into an EMPTY database")
    rs.add_argument("path")
    rs.add_argument("--yes", action="store_true", default=False,
                    help="do not ask for confirmation")
    rs.set_defaults(func=_cli_restore)

    ms = sub.add_parser(
        "messages",
        help="what is in a station, and what is holding it",
    )
    ms.add_argument("--station", default=None, help="limit to one station")
    ms.add_argument("--mark", default=None,
                    help="make a segment collectable: "
                         + ", ".join((*ACK_SEGMENTS, *EXPIRY_SEGMENTS,
                                      *AGE_SEGMENTS)))
    ms.add_argument("--dry-run", action="store_true", default=False,
                    help="count what --mark would touch, change nothing")
    ms.set_defaults(func=_cli_messages)

    lg = sub.add_parser(
        "logs",
        help="read the broker's own log, newest last",
    )
    lg.add_argument("--station", default=None, help="limit to one station")
    lg.add_argument("--level", default=None,
                    help="minimum level: DEBUG, INFO, WARN, ERROR")
    lg.add_argument("--event", default=None,
                    help="exact event, e.g. station.allow or token.revoke")
    lg.add_argument("--since", default=None,
                    help="duration back from now, e.g. 2h or 7d")
    lg.add_argument("--tail", type=int, default=50,
                    help="how many lines (default 50)")
    lg.set_defaults(func=_cli_logs)

    ch = sub.add_parser("channel", help="inspect and re-home channels")
    ch_sub = ch.add_subparsers(dest="channel_cmd", required=True)
    ch_create = ch_sub.add_parser("create", help="create a channel")
    ch_create.add_argument("name")
    ch_create.add_argument("--station", required=True)
    ch_create.add_argument("--members", default="",
                           help="comma-separated agent ids")
    ch_create.add_argument("--theme", default="")
    ch_create.set_defaults(func=_cli_channel)
    ch_rm = ch_sub.add_parser("rm", help="delete a channel and its transcript")
    ch_rm.add_argument("name")
    ch_rm.add_argument("--station", required=True)
    ch_rm.set_defaults(func=_cli_channel)
    ch_list = ch_sub.add_parser("list", help="channels, station and members")
    ch_list.add_argument("--station", default=None)
    ch_list.set_defaults(func=_cli_channel)
    ch_move = ch_sub.add_parser(
        "move", help="move a channel (with its messages) to another station"
    )
    ch_move.add_argument("name")
    ch_move.add_argument("--to", required=True, help="target station")
    ch_move.add_argument("--from", dest="from_station", default=None,
                         help="source station, if the name is ambiguous")
    ch_move.set_defaults(func=_cli_channel)

    ag = sub.add_parser("agent", help="manage agents (station membership)")
    ag_sub = ag.add_subparsers(dest="agent_cmd", required=True)
    ag_add = ag_sub.add_parser("add", help="create an agent in a station")
    ag_add.add_argument("agent_id")
    ag_add.add_argument("--station", required=True)
    ag_add.add_argument("--token", default=None,
                        help="pre-bind to this token prefix")
    ag_add.set_defaults(func=_cli_agent)
    ag_list = ag_sub.add_parser("list", help="list agents")
    ag_list.add_argument("--station", default=None)
    ag_list.set_defaults(func=_cli_agent)
    ag_move = ag_sub.add_parser("move", help="move an agent to another station")
    ag_move.add_argument("agent_id")
    ag_move.add_argument("--station", required=True)
    ag_move.set_defaults(func=_cli_agent)
    ag_rm = ag_sub.add_parser("rm", help="remove an agent from a station")
    ag_rm.add_argument("agent_id")
    ag_rm.add_argument("--station", default=None,
                       help="required when the id exists in several stations")
    ag_rm.set_defaults(func=_cli_agent)
    ag_screen = ag_sub.add_parser(
        "screen",
        help="ack ALL messages directed at one agent, clearing a wedged "
             "inbox without touching anybody else's",
    )
    ag_screen.add_argument("agent_id")
    ag_screen.add_argument("--station", required=True)
    ag_screen.add_argument("--dry-run", action="store_true", default=False,
                           help="count what would be acked, change nothing")
    ag_screen.set_defaults(func=_cli_agent)
    ag_props = ag_sub.add_parser(
        "proposals", help="names clients have asked for, awaiting approval"
    )
    ag_props.add_argument("--station", default=None)
    ag_props.add_argument(
        "--kind", choices=("claim", "transfer"), default=None,
        help="claim: a name nobody holds. transfer: a name another token does",
    )
    ag_props.set_defaults(func=_cli_agent)
    ag_ok = ag_sub.add_parser(
        "approve",
        help="grant a request: mint a proposed name, or hand an existing "
             "agent (with its channels and unacked inbox) to the asking token",
    )
    ag_ok.add_argument("agent_id")
    ag_ok.add_argument("--station", required=True)
    ag_ok.set_defaults(func=_cli_agent)
    ag_no = ag_sub.add_parser(
        "reject",
        help="refuse a request now instead of letting it expire; a refused "
             "TRANSFER also bars that token for A2A_TRANSFER_LOCKTIME",
    )
    ag_no.add_argument("agent_id")
    ag_no.add_argument("--station", required=True)
    ag_no.set_defaults(func=_cli_agent)
    ag_unlock = ag_sub.add_parser(
        "unlock",
        help="lift the transfer lock a rejection left, so a client may ask "
             "again before it expires",
    )
    ag_unlock.add_argument("agent_id")
    ag_unlock.add_argument("--station", required=True)
    ag_unlock.set_defaults(func=_cli_agent)
    ag_free = ag_sub.add_parser(
        "free",
        help="release a NAME so any client can claim it, keeping the agent",
    )
    ag_free.add_argument("agent_id")
    ag_free.add_argument("--station", required=True)
    ag_free.set_defaults(func=_cli_agent)
    ag_bind = ag_sub.add_parser("bind", help="pin agent(s) to a token")
    ag_bind.add_argument("agent_id", nargs="?", default=None)
    ag_bind.add_argument("--token", required=True)
    ag_bind.add_argument("--all", action="store_true", default=False,
                         help="bind every agent (optionally --station-scoped)")
    ag_bind.add_argument("--station", default=None)
    ag_bind.set_defaults(func=_cli_agent)
    ag_unbind = ag_sub.add_parser(
        "unbind", help="unpin agent(s) so another token can claim them"
    )
    ag_unbind.add_argument("agent_id", nargs="?", default=None)
    ag_unbind.add_argument("--all", action="store_true", default=False)
    ag_unbind.add_argument("--station", default=None)
    ag_unbind.set_defaults(func=_cli_agent)

    return p


def _startup() -> None:
    """Reach the database and make sure the schema is there.

    Called once, after argparse, so `--help` and `--version` never touch the
    database — and so an unreachable server produces one readable line instead
    of a driver traceback out of an import.
    """
    _wait_for_db()
    _init_schema()


if __name__ == "__main__":
    parser = _build_parser()
    args = parser.parse_args()
    _startup()
    if not args.cmd:
        sys.exit(_cli_serve(args))
    sys.exit(args.func(args))
