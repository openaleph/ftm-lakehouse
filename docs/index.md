[![Docs](https://img.shields.io/badge/docs-live-brightgreen)](https://openaleph.org/docs/lib/ftm-lakehouse)
[![ftm-lakehouse on pypi](https://img.shields.io/pypi/v/ftm-lakehouse)](https://pypi.org/project/ftm-lakehouse/)
[![PyPI Downloads](https://static.pepy.tech/badge/ftm-lakehouse/month)](https://pepy.tech/projects/ftm-lakehouse)
[![PyPI - Python Version](https://img.shields.io/pypi/pyversions/ftm-lakehouse)](https://pypi.org/project/ftm-lakehouse/)
[![Python test and package](https://github.com/openaleph/ftm-lakehouse/actions/workflows/python.yml/badge.svg)](https://github.com/openaleph/ftm-lakehouse/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/openaleph/ftm-lakehouse/badge.svg?branch=main)](https://coveralls.io/github/openaleph/ftm-lakehouse?branch=main)
[![AGPLv3+ License](https://img.shields.io/pypi/l/ftm-lakehouse)](./LICENSE)
[![Pydantic v2](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/pydantic/pydantic/main/docs/badge/v2.json)](https://pydantic.dev)

# ftm-lakehouse

`ftm-lakehouse` provides a _data standard_ and _archive storage_ for leaked data, private and public document collections and structured [FollowTheMoney](https://followthemoney.tech) data.

It is a multi-tenant storage and retrieval layer for entity data, documents and their metadata. _Tenants_ produce and consume it – for example [investigraph](https://docs.investigraph.dev), [memorious](https://docs.investigraph.dev/lib/memorious/), and platforms such as [_OpenAleph_](https://openaleph.org), [_ICIJ Datashare_](https://datashare.icij.org/) or [_Liquid Investigations_](https://github.com/liquidinvestigations/).

[What is a lakehouse?](https://www.databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html)

## Open formats

The file layout follows documented [conventions](conventions.md) and the statements are [parquet](https://parquet.apache.org/), so third-party tools can populate and consume a lakehouse directly. Data, change history and versions all live in the storage backend; reading needs no running service. Writers use a SQL write-ahead journal (sqlite, or postgres in production).

## Core Components

### Entities

The **entities** interface is the primary way to work with [FollowTheMoney](https://followthemoney.tech) data:

- **Write** entities through a buffered journal
- **Query** them from a [Delta Lake](https://delta-io.github.io/delta-rs/) statement store
- **Export** them as JSON, CSV and statistics

Entities are stored as _[statements](https://followthemoney.tech/docs/statements/)_: one property value of one entity from one source (`entity_id`, `schema`, `prop`, `value`, `dataset`, plus provenance such as `origin`). That keeps the provenance of every value, and lets sources merge and update incrementally. A lakehouse dataset does no entity resolution, so `canonical_id` is not stored – it always equals `entity_id`.

```python
from ftmq.query import C, Query

from ftm_lakehouse import ensure_dataset, get_entities

ensure_dataset("my_dataset")
entities = get_entities("my_dataset")

# Write entities through the journal (buffered, then flushed to parquet)
with entities.writer(origin="import") as writer:
    for entity in source:
        writer.add_entity(entity)
entities.flush()

# Read back
entity = entities.get("entity-id-123")

# Query the parquet store
for entity in entities.query(Query(C(origin="crawl"))):
    process(entity)
```

The statement store is partitioned by `(shard, bucket, origin)` and written append-only. Reads reconcile duplicates and tombstones; `optimize` compacts them – see [Maintenance](usage/entities.md#maintenance).

### Archive

The **archive** interface manages source files, content-addressed by SHA256 checksum, so identical files are stored once. Files become FollowTheMoney entities as well.

```python
from ftm_lakehouse import get_archive

archive = get_archive("my_dataset")

# Archive a file
file = archive.store("/path/to/document.pdf")

# Retrieve file content
with archive.open(file.checksum) as fh:
    content = fh.read()
```

## Installation

Requires Python 3.12 or later.

```bash
pip install ftm-lakehouse
```

Optional extras:

```bash
pip install "ftm-lakehouse[postgres]"  # postgres journal (ADBC + psycopg)
pip install "ftm-lakehouse[api]"       # the lakehouse API server
pip install "ftm-lakehouse[s3]"        # S3-compatible object storage (s3fs)
pip install "ftm-lakehouse[gcs]"       # Google Cloud Storage (gcsfs)
pip install "ftm-lakehouse[azure]"     # Azure Blob Storage (adlfs)
pip install "ftm-lakehouse[http]"      # HTTP(S)-backed api stores (aiohttp)
```

Extras combine, e.g. `pip install "ftm-lakehouse[s3,postgres]"`.

## Quickstart

[>> Get started here](quickstart.md)

## Background

The design grew out of the [FollowTheMoney data lake RFC discussion](https://discuss.openaleph.org/t/rfc-followthemoney-data-lake/37) and prior art in [mmmeta](https://github.com/simonwoerpel/mmmeta), [Aleph's servicelayer archive](https://github.com/alephdata/servicelayer), [OpenSanctions](https://opensanctions.org) dataset metadata and [nomenklatura statements](https://followthemoney.tech/docs/statements/).

For contributing, development setup and testing see the [repository README](https://github.com/openaleph/ftm-lakehouse#development).

## License and Copyright

`leakrfc` (_predecessor_), (c) 2024 [investigativedata.io](https://investigativedata.io)

`ftm-lakehouse`, (c) 2024 [investigativedata.io](https://investigativedata.io)

`ftm-lakehouse`, (c) 2025-2026 [Data and Research Center - DARC](https://dataresearchcenter.org)

`ftm-lakehouse` is licensed under the AGPLv3 or later license.
