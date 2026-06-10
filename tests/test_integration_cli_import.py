"""Round-trip tests for the CLI bulk-import commands."""

import pytest
from ftmq.io import smart_write_proxies
from ftmq.util import make_entity
from typer.testing import CliRunner

from ftm_lakehouse.cli import cli as cli_app
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.repository.entities.main import EntityRepository
from tests.shared import JANE, JOHN


@pytest.fixture()
def cli_runner(tmp_path, monkeypatch) -> CliRunner:
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))
    # The CLI memoises the catalog / dataset on first invocation, so wipe
    # the module-level state between tests to keep the new URI honoured.
    from ftm_lakehouse import cli as cli_module
    from ftm_lakehouse.lake import get_lakehouse

    cli_module.STATE["catalog"] = None
    cli_module.STATE["dataset"] = None
    get_lakehouse.cache_clear()
    return CliRunner()


def _seed_source(tmp_path) -> EntityRepository:
    repo = EntityRepository("src", tmp_path / "src")
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))
    repo.flush()
    return repo


def test_cli_statements_import_roundtrip(tmp_path, cli_runner):
    """An exported ``statements.csv`` imports back with coerced field types.

    Regression: the import used to feed raw CSV string dicts into
    ``Statement.from_dict`` – ``external`` arrived as the string ``"False"``
    and crashed the parquet append (and would have been truthy otherwise).
    """
    src = _seed_source(tmp_path)
    src._store.ensure_parent(path.EXPORTS_STATEMENTS)
    src._statements.export_csv(path.EXPORTS_STATEMENTS)
    csv_uri = str(tmp_path / "src" / path.EXPORTS_STATEMENTS)

    result = cli_runner.invoke(
        cli_app, ["-d", "dst", "statements", "import", "-i", csv_uri]
    )
    assert result.exit_code == 0, result.output

    dst = EntityRepository("dst", tmp_path / "dst")
    stmts = list(dst._statements.query_statements())
    assert len(stmts) == 4  # 2 entities x (id + name statements)
    for stmt in stmts:
        assert stmt.external is False
        assert stmt.lang is None
        assert stmt.origin == "test"  # carried from the CSV, not the default

    entities = {e.id: e for e in dst.query()}
    assert set(entities) == {"jane", "john"}
    assert "Jane Doe" in entities["jane"].get("name")


def test_cli_entities_import_roundtrip(tmp_path, cli_runner):
    """FtM JSON entities bulk-import straight into the parquet store."""
    in_uri = str(tmp_path / "entities.ftm.json")
    smart_write_proxies(in_uri, [make_entity(JANE), make_entity(JOHN)])

    result = cli_runner.invoke(
        cli_app, ["-d", "dst", "entities", "import", "-i", in_uri]
    )
    assert result.exit_code == 0, result.output

    dst = EntityRepository("dst", tmp_path / "dst")
    entities = {e.id: e for e in dst.query()}
    assert set(entities) == {"jane", "john"}
    assert "John Doe" in entities["john"].get("name")
