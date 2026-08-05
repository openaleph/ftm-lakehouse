FROM python:3.14-slim AS deps
# Multi-stage build:
#   1. ``deps``  – system + python dependencies from a committed
#                  ``requirements.txt``. Cached unless the requirements
#                  file changes.
#   2. ``app``   – install the application on top of ``deps``. Source
#                  edits only invalidate this stage.

RUN apt-get update && \
    apt-get install -y git pkg-config libicu-dev build-essential && \
    apt-get autoremove -y && apt-get clean && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir psycopg2-binary

# DuckDB auto-installs the `delta` extension into `$HOME/.duckdb` on first
# `delta_scan`, which fails for a container running without a writable HOME.
# Pre-install it into a world-readable directory instead – no runtime download,
# no HOME needed.
ENV LAKEHOUSE_DUCKDB_EXTENSION_DIRECTORY=/opt/duckdb/extensions
RUN python -c "import duckdb, os; \
    d = os.environ['LAKEHOUSE_DUCKDB_EXTENSION_DIRECTORY']; \
    duckdb.connect(config={'extension_directory': d}).execute('INSTALL delta')" && \
    chmod -R a+rX /opt/duckdb


FROM deps AS app

COPY ftm_lakehouse /src/ftm_lakehouse
COPY setup.py pyproject.toml README.md VERSION LICENSE NOTICE /src/

RUN pip install --no-cache-dir --no-deps -q ".[api]"
