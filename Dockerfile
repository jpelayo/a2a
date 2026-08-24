# A2A↔MCP server image — just the Python broker.
#
# Stateless. Data lives in MariaDB, reached over the network (A2A_DB_HOST and
# friends); this image holds no database and mounts no data volume at all.

FROM python:3.12-slim

ENV PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

RUN apt-get update \
 && apt-get install -y --no-install-recommends tini \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /app

COPY a2a_mcp/requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY a2a_mcp/a2a-mcp.py /app/a2a-mcp.py

# Client plugin SOURCE, not a built artifact. The broker zips a2a/ on the fly
# per request and serves opencode/a2a-opencode.js the same way, so whatever is
# in this tree is what clients get — a rebuild can never ship a stale plugin,
# and there is no zip to remember to regenerate.
COPY plugin /app/plugin

ENV A2A_HOST=0.0.0.0 \
    A2A_PORT=9999 \
    A2A_DB_HOST=mariadb \
    A2A_DB_PORT=3306 \
    A2A_DB_NAME=a2a \
    A2A_DB_USER=a2a \
    A2A_PLUGIN_SRC=/app/plugin

EXPOSE 9999

HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD python3 -c "import urllib.request as u; u.urlopen('http://127.0.0.1:9999/healthz', timeout=3)" \
        || exit 1

ENTRYPOINT ["/usr/bin/tini", "--", "python3", "/app/a2a-mcp.py"]
CMD ["serve"]
