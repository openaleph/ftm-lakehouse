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

`ftm-lakehouse` acts as a multi-tenant storage and retrieval mechanism for structured entity data, documents and their metadata. It provides a high-level interface for generating and sharing document collections and importing them into various search and analysis platforms, such as [_OpenAleph_](https://openaleph.org), [_ICIJ Datashare_](https://datashare.icij.org/) or [_Liquid Investigations_](https://github.com/liquidinvestigations/).

[Read the specification](./rfc.md)

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

A statement represents a single fact: one property value for one entity from one source. Each statement contains an `entity_id`, `schema` (entity type), `prop` (property name), `value`, and `dataset` identifier. This decomposition allows tracking where each piece of information originated - which source file, processing step, or import batch contributed a specific value. The `canonical_id` field enables entity deduplication by linking multiple source entities that represent the same real-world thing.

This statement-based storage model makes it possible to merge data from multiple sources while preserving full provenance, perform incremental updates without reprocessing entire datasets, and use standard file-based tools (sorting, filtering) rather than requiring database infrastructure.

```python
from ftm_lakehouse import lake

# Write entities
lake.write_entities("my_dataset", entities, origin="import")

# Read an entity
entity = lake.get_entity("my_dataset", "entity-id-123")

# Query entities
for entity in lake.iterate_entities("my_dataset", origin="crawl"):
    process(entity)
```

### Archive

The **archive** interface manages source documents and files:

- **Store files** with content-addressable storage (SHA1 checksums)
- **Retrieve files** by checksum or iterate through all files
- **Track metadata** including MIME types, sizes, and custom properties

Files are automatically deduplicated across the archive.

```python
from ftm_lakehouse import lake

# Archive a file
file = lake.archive_file("my_dataset", "/path/to/document.pdf")

# Retrieve file content
with lake.open_file("my_dataset", file.checksum) as fh:
    content = fh.read()
```

## Installation

Requires Python 3.11 or later.

```bash
pip install ftm-lakehouse
```

## Quickstart

[>> Get started here](quickstart.md)

## Development

This package uses [poetry](https://python-poetry.org/) for packaging and dependencies management, so first [install it](https://python-poetry.org/docs/#installation).

Clone [this repository](https://github.com/openaleph/ftm-lakehouse) to a local destination.

Within the repo directory, run:

```bash
poetry install --with dev
```

This installs development dependencies, including [pre-commit](https://pre-commit.com/) which needs to be registered:

```bash
poetry run pre-commit install
```

Before creating a commit, this checks for correct code formatting (isort, black) and other useful checks (see: `.pre-commit-config.yaml`).

### Testing

`ftm-lakehouse` uses [pytest](https://docs.pytest.org/en/stable/) as the testing framework.

```bash
make test
```

## License and Copyright

`ftm-lakehouse`, (c) 2024 [investigativedata.io](https://investigativedata.io)

`ftm-lakehouse`, (c) 2025 [Data and Research Center - DARC](https://dataresearchcenter.org)

`ftm-lakehouse` is licensed under the AGPLv3 or later license.
