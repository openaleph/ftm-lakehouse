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

`ftm-lakehouse` provides a _data standard_ and _archive storage_ for leaked data, private and public document collections and structured [FollowTheMoney](https://followthemoney.tech) data. The concepts and implementations are originally inspired by [mmmeta](https://github.com/simonwoerpel/mmmeta), [Aleph's servicelayer archive](https://github.com/alephdata/servicelayer) and [OpenSanctions](https://opensanctions.org) work on dataset catalog metadata.

`ftm-lakehouse` acts as a multi-tenant storage and retrieval mechanism for structured entity data, documents and their metadata. It can be used by _tenants_ to produce and/or consume data such as [investigraph](https://docs.investigraph.dev), [memorious](https://docs.investigraph.dev/lib/memorious/) and the full suites of various search and analysis platforms, such as [_OpenALeph_](https://openaleph.org), [_ICIJ Datashare_](https://datashare.icij.org/) or [_Liquid Investigations_](https://github.com/liquidinvestigations/)

[What is a lakehouse?](https://www.databricks.com/blog/2020/01/30/what-is-a-data-lakehouse.html)

## Open formats

Given the convention-based file structure and the use of [parquet](https://parquet.apache.org/) files, the storage layer can be populated and consumed by other 3rd-party tools which makes it free and easy to integrate `ftm-lakehouse` into other analytics systems or data platforms.

As well the complete data lakehouse is stored in the file-like storage backend, including change history and versions. It doesn't rely on any other running services (like a database) and therefore maintenance, scalability and data consistency is ensured. (For runtime, a sql database is needed for task management and a write ahead journal).

## Core Components

`ftm-lakehouse` organizes data around two main components:

### Entities

The **entities** interface is the primary way to work with [FollowTheMoney](https://followthemoney.tech) data. It provides:

- **Writing entities** to a buffered journal for efficient batch processing
- **Querying entities** from a [Delta Lake-based](https://delta-io.github.io/delta-rs/) statement store
- **Exporting** to various formats (JSON, CSV, statistics)

!!! info
    See below for the **archive** layer that stores source files. As per the FollowTheMoney spec and logic, files are converted into _entities_ as well and therefore part of the Entity store as well.

Entities are stored as _[statements](https://followthemoney.tech/docs/statements/)_ - granular property-level records that enable versioning, provenance tracking, and incremental updates.

A statement represents a single fact: one property value for one entity from one source. Each statement contains an `entity_id`, `schema` (entity type), `prop` (property name), `value`, and `dataset` identifier. This decomposition allows tracking where each piece of information originated - which source file, processing step, or import batch contributed a specific value. This is a single-dataset store with no in-store entity resolution, so entities are keyed on `entity_id` and `canonical_id` is not persisted (it always equals `entity_id`).

This statement-based storage model makes it possible to merge data from multiple sources while preserving full provenance, perform incremental updates without reprocessing entire datasets, and use standard file-based tools (sorting, filtering) rather than requiring database infrastructure.

```python
from ftmq.query import M, Query

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

# Live query of the parquet store
for entity in entities.query(Query(M(origin="crawl"))):
    process(entity)
```

The parquet statement store is partitioned by `(shard, bucket, origin)` and written append-only on the hot path. Three async maintenance ops collapse the redundancy – `compact` (file bin-pack), `merge` (per-partition dedup + tombstone reaping), `vacuum` (drop obsolete files) – all coordinated by a dataset-wide write fence.

### Archive

The **archive** interface manages source documents and files:

- **Store files** with content-addressable storage (SHA256 checksums)
- **Retrieve files** by checksum or iterate through all files
- **Track metadata** including MIME types, sizes, and custom properties

Files are automatically deduplicated across the archive.

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

Requires Python 3.11 or later.

```bash
pip install ftm-lakehouse
```

Remote storage backends are optional extras – install the one matching your archive/lake URI:

```bash
pip install "ftm-lakehouse[s3]"     # S3-compatible object storage (s3fs)
pip install "ftm-lakehouse[gcs]"    # Google Cloud Storage (gcsfs)
pip install "ftm-lakehouse[azure]"  # Azure Blob Storage (adlfs)
pip install "ftm-lakehouse[http]"   # HTTP(S)-backed api stores (aiohttp)
```

Extras combine, e.g. `pip install "ftm-lakehouse[s3,gcs]"`.

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
