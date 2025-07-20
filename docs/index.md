[![ftm-lakehouse on pypi](https://img.shields.io/pypi/v/ftm-lakehouse)](https://pypi.org/project/ftm-lakehouse/)
[![Python test and package](https://github.com/dataresearchcenter/ftm-lakehouse/actions/workflows/python.yml/badge.svg)](https://github.com/dataresearchcenter/ftm-lakehouse/actions/workflows/python.yml)
[![pre-commit](https://img.shields.io/badge/pre--commit-enabled-brightgreen?logo=pre-commit)](https://github.com/pre-commit/pre-commit)
[![Coverage Status](https://coveralls.io/repos/github/dataresearchcenter/ftm-lakehouse/badge.svg?branch=main)](https://coveralls.io/github/dataresearchcenter/ftm-lakehouse?branch=main)
[![AGPLv3+ License](https://img.shields.io/pypi/l/ftm-lakehouse)](./LICENSE)

# ftm-lakehouse

`ftm-lakehouse` provides a _data standard_ and _archive storage_ for leaked data, private and public document collections. The concepts and implementations are originally inspired by [mmmeta](https://github.com/simonwoerpel/mmmeta) and [Aleph's servicelayer archive](https://github.com/alephdata/servicelayer).

`ftm-lakehouse` acts as a multi-tenant storage and retrieval mechanism for structured entity data, documents and their metadata. It provides a high-level interface for generating and sharing document collections and importing them into various search and analysis platforms, such as [_ICIJ Datashare_](https://datashare.icij.org/), [_Liquid Investigations_](https://github.com/liquidinvestigations/), and [_OpenAleph_](https://openaleph.org/).

[Read the specification](./rfc.md)

## Installation

Requires python 3.11 or later.

```bash
pip install ftm-lakehouse
```

## Quickstart

[>> get started here](quickstart.md)

## Development

This package is using [poetry](https://python-poetry.org/) for packaging and dependencies management, so first [install it](https://python-poetry.org/docs/#installation).

Clone [this repository](https://github.com/dataresearchcenter/ftm-lakehouse) to a local destination.

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
