FROM python:3.14-slim as deps
# Multi-stage build:
#   1. ``deps``  – system + python dependencies from a committed
#                  ``requirements.txt``. Cached unless the requirements
#                  file changes.
#   2. ``app``   – install the application on top of ``deps``. Source
#                  edits only invalidate this stage.

RUN apt-get update && apt-get install -y git pkg-config libicu-dev build-essential && rm -rf /var/lib/apt/lists/*

WORKDIR /src

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt
RUN pip install --no-cache-dir psycopg2-binary


FROM deps AS app

COPY ftm_lakehouse /src/ftm_lakehouse
COPY setup.py pyproject.toml README.md VERSION LICENSE NOTICE /src/

RUN pip install --no-cache-dir --no-deps -q ".[api]"

ENTRYPOINT [""]
