FROM ghcr.io/dataresearchcenter/ftmq:latest


COPY ftm_lakehouse /src/ftm_lakehouse
COPY setup.py /src/setup.py
COPY README.md /src/README.md
COPY pyproject.toml /src/pyproject.toml
COPY VERSION /src/VERSION
COPY LICENSE /src/LICENSE
COPY NOTICE /src/NOTICE

WORKDIR /src
RUN pip install --no-cache-dir -q "."

ENTRYPOINT ["ftm-lakehouse"]
