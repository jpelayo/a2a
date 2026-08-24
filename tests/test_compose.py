#!/usr/bin/env python3
"""The compose file must be internally consistent.

    python3 tests/test_compose.py

The container stack was written on a machine with no Docker, so `my.cnf`, the
generated-secret handoff, the healthcheck and the boot ordering have never been
executed. A full rehearsal was declined; this is what can be checked without
one — and it is the class of mistake a rehearsal would have caught first: a
service that reads a path nothing mounts, a volume referenced but not declared,
an ordering that lets the broker start before the thing it depends on.

None of this needs Docker. It is all in the file.

Needs python3 and pyyaml.
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
COMPOSE = ROOT / "docker-compose.yml"
MYCNF = ROOT / "a2a_mcp" / "my.cnf"
BROKER = ROOT / "a2a_mcp" / "a2a-mcp.py"

fails: list[str] = []


def check(name: str, ok: bool, detail: str = "") -> None:
    print(f"{'ok  ' if ok else 'FAIL'} {name}")
    if not ok:
        fails.append(f"{name}: {detail}")


def mounts_of(svc: dict) -> dict:
    """{container path: volume or host source} for one service."""
    out = {}
    for v in svc.get("volumes") or []:
        if isinstance(v, str):
            bits = v.split(":")
            if len(bits) >= 2:
                out[bits[1]] = bits[0]
        elif isinstance(v, dict):
            out[v.get("target", "")] = v.get("source", "")
    return out


def main() -> int:
    try:
        import yaml
    except ImportError:
        print("SKIP: pyyaml is not installed (pip3 install pyyaml)",
              file=sys.stderr)
        return 2

    raw = COMPOSE.read_text()
    doc = yaml.safe_load(raw)
    services = doc.get("services") or {}
    volumes = doc.get("volumes") or {}

    # --- nothing may be required before the first run ----------------------
    # Setup is one command. A `${VAR:?...}` makes compose refuse to start until
    # somebody edits a file, which is the thing this deployment must not need.
    required = re.findall(r"\$\{([A-Z_]+):\?", raw)
    check("no environment variable is required before the first run — setup "
          "stays one command", not required, str(required))

    # --- no credential in the repository -----------------------------------
    # A default password in compose is a secret published in source control
    # that every deployment shares. The a2a-secret service exists so there is
    # neither that nor a file to edit.
    check("no database password is defaulted in the compose file",
          "A2A_DB_PASSWORD:-" not in raw and "MARIADB_PASSWORD:" not in raw,
          "a password default is present")
    check("the secret is generated per host instead",
          "a2a-secret" in services
          and "/dev/urandom" in yaml.dump(services["a2a-secret"]),
          "a2a-secret does not generate anything")

    # --- every path read is a path mounted ---------------------------------
    # The mistake a rehearsal catches on the first `up`: a service told to read
    # /run/secret/... that nothing mounts there. It surfaces as an unreadable
    # file at boot, which is a confusing way to learn about a missing line.
    for name, svc in services.items():
        env = svc.get("environment") or {}
        paths = [v for v in env.values()
                 if isinstance(v, str) and v.startswith("/run/secret/")]
        if not paths:
            continue
        mounted = mounts_of(svc)
        ok = any(p.startswith(m.rstrip("/") + "/")
                 for p in paths for m in mounted if m)
        check(f"{name} mounts the secret it is told to read",
              ok, f"reads {paths}, mounts {list(mounted)}")

    # --- every volume used is declared -------------------------------------
    for name, svc in services.items():
        for target, source in mounts_of(svc).items():
            if source.startswith(".") or source.startswith("/"):
                continue                      # a bind mount from the repo
            check(f"{name}: volume {source!r} is declared",
                  source in volumes, f"{source} used but not under volumes:")

    # --- bind mounts point at files that exist -----------------------------
    for name, svc in services.items():
        for target, source in mounts_of(svc).items():
            if source.startswith("./"):
                check(f"{name}: {source} exists in the repo",
                      (ROOT / source[2:]).exists(), f"{source} is missing")

    # --- the volumes that must never be auto-created -----------------------
    # Left to create them, compose makes an EMPTY volume whenever a name fails
    # to resolve, and the broker comes up healthy having lost everything. The
    # secret is worse: an empty one means a new password against a database
    # that no longer accepts it.
    for must in ("a2a-mariadb", "a2a-secret"):
        check(f"{must} is external, so a missing volume is an error rather "
              f"than an empty one", (volumes.get(must) or {}).get("external"),
              f"{must} would be auto-created")
    # ...and the one that must be, because a fresh host has no sqlite history.
    check("a2a-data is NOT external — it is only ever read, so an empty one "
          "on a host that never ran sqlite is correct",
          not (volumes.get("a2a-data") or {}).get("external"),
          "a2a-data would stop a fresh install")

    # --- ordering ----------------------------------------------------------
    broker = services.get("a2a-mcp") or {}
    dep = broker.get("depends_on") or {}
    check("the broker waits for the database to be HEALTHY, not merely "
          "started — a started MariaDB is not yet accepting connections",
          (dep.get("mariadb") or {}).get("condition") == "service_healthy",
          str(dep))
    check("and for the secret to have been written",
          (dep.get("a2a-secret") or {}).get("condition")
          == "service_completed_successfully", str(dep))
    check("the database has a healthcheck for that to mean anything",
          bool((services.get("mariadb") or {}).get("healthcheck")),
          "no healthcheck")
    check("the database service also waits for the secret",
          ((services.get("mariadb") or {}).get("depends_on") or {})
          .get("a2a-secret", {}).get("condition")
          == "service_completed_successfully", "mariadb does not wait")

    # --- the database is not on the host -----------------------------------
    check("the database publishes no host port",
          not (services.get("mariadb") or {}).get("ports"),
          "mariadb is exposed on the host")

    # --- the footprint the host was promised -------------------------------
    check("the database is capped, so a spike degrades instead of eating a "
          "host that already runs ~50 containers",
          (services.get("mariadb") or {}).get("mem_limit"), "no mem_limit")
    for name in ("mariadb", "a2a-mcp"):
        opts = ((services.get(name) or {}).get("logging") or {}).get("options")
        check(f"{name}'s json-file log is capped — the stderr fallback writes "
              f"there, and an uncapped driver is how a disk fills",
              bool(opts and opts.get("max-size")), str(opts))

    # --- my.cnf: what can be checked without a database --------------------
    cnf = MYCNF.read_text()
    check("my.cnf keeps durability: this is the system of record, and the "
          "move happened because data went missing silently",
          {"innodb_flush_log_at_trx_commit": "1",
           "innodb_doublewrite": "ON"}.items() <= {
              l.split("=")[0].strip(): l.split("=")[1].strip()
              for l in cnf.splitlines() if "=" in l}.items(),
          "durability tuned away")
    check("my.cnf pins the binary collation server-wide, so a table created "
          "by hand later cannot get the case-insensitive default",
          any(l.split("=")[0].strip() == "collation-server"
              and l.split("=")[1].strip() == "utf8mb4_bin"
              for l in cnf.splitlines() if "=" in l),
          "collation not pinned")
    # Every key is `name = value` under one section — a typo'd key is what
    # stops MariaDB starting, and the preflight is what catches it. This only
    # checks the shape.
    bad = [l for l in cnf.splitlines()
           if l.strip() and not l.strip().startswith(("#", "["))
           and "=" not in l and not l.strip().startswith("skip-")]
    check("my.cnf has no malformed lines", not bad, str(bad))

    # --- the coupling that is in two files ---------------------------------
    src = BROKER.read_text()
    check("the broker's keepalive names the client timeout it is coupled to, "
          "since raising it above that makes every client flap",
          "A2A_STREAM_TIMEOUT" in src, "no coupling comment")

    print()
    for f in fails:
        print("FAIL", f)
    print("FAILED" if fails
          else "PASS — the stack is internally consistent")
    return 1 if fails else 0


if __name__ == "__main__":
    sys.exit(main())
