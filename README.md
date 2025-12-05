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

`ftm-lakehouse` provides a _data standard_ and _archive storage_ for leaked data, private and public document collections. The concepts and implementations are originally inspired by [mmmeta](https://github.com/simonwoerpel/mmmeta) and [Aleph's servicelayer archive](https://github.com/alephdata/servicelayer).

`ftm-lakehouse` acts as a multi-tenant storage and retrieval mechanism for structured entity data, documents and their metadata. It provides a high-level interface for generating and sharing document collections and importing them into various search and analysis platforms, such as [_OpenALeph_](https://openaleph.org), [_ICIJ Datashare_](https://datashare.icij.org/) or [_Liquid Investigations_](https://github.com/liquidinvestigations/)

## Installation

Requires python 3.11 or later.

```bash
pip install ftm-lakehouse
```

## Documentation

[openaleph.org/docs/lib/ftm-lakehouse](https://openaleph.org/docs/lib/ftm-lakehouse)

## Development

This package is using [poetry](https://python-poetry.org/) for packaging and dependencies management, so first [install it](https://python-poetry.org/docs/#installation).

Clone [this repository](https://github.com/openaleph/ftm-lakehouse) to a local destination.

Within the repo directory, run

    poetry install --with dev

This installs a few development dependencies, including [pre-commit](https://pre-commit.com/) which needs to be registered:

    poetry run pre-commit install

Before creating a commit, this checks for correct code formatting (isort, black) and some other useful stuff (see: `.pre-commit-config.yaml`)

### Testing

`ftm-lakehouse` uses [pytest](https://docs.pytest.org/en/stable/) as the testing framework.

    make test

## License and Copyright

`ftm-lakehouse`, (c) 2024 [investigativedata.io](https://investigativedata.io)

`ftm-lakehouse`, (c) 2025 [Data and Research Center – DARC](https://dataresearchcenter.org)

`ftm-lakehouse` is licensed under the AGPLv3 or later license.
