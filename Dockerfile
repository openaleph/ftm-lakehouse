FROM python:3.13-slim

RUN apt-get update && apt-get install -y git pkg-config libicu-dev build-essential && rm -rf /var/lib/apt/lists/*

COPY ftm_lakehouse /src/ftm_lakehouse
COPY setup.py pyproject.toml README.md VERSION LICENSE NOTICE /src/

WORKDIR /src
RUN pip install --no-cache-dir psycopg2-binary
RUN pip install --no-cache-dir -q ".[api]"
RUN pip install --no-cache-dir --force-reinstall --no-deps \
    "ftmq[lake] @ git+https://github.com/dataresearchcenter/ftmq.git" \
    "anystore @ git+https://github.com/dataresearchcenter/anystore.git"

ENTRYPOINT [""]
