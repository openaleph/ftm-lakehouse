"""Round-trip tests for the CLI bulk-import commands."""

import orjson
import pytest
from followthemoney import Statement
from ftmq.io import smart_write_proxies
from ftmq.util import make_entity
from typer.testing import CliRunner

from ftm_lakehouse.cli import cli as cli_app
from ftm_lakehouse.cli.io import import_entities_unsafe
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.repository.entities.main import EntityRepository
from ftm_lakehouse.repository.factories import get_entities
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


UNSAFE_ENTITIES = [
    {
        # signed entity id and entity-type prop value – both get stripped
        "id": "jane.f00dbabe",
        "schema": "Person",
        "properties": {"name": ["Jane Doe"], "proof": ["doc-1.beef"]},
        "last_change": "2024-01-01T00:00:00",
        "origin": ["crawl"],
    },
    {
        "id": "john",
        "schema": "Person",
        "properties": {"name": ["John Doe"]},
        "last_change": "2024-01-01T00:00:00",
    },
]


def _write_jsonl(fp, datas) -> str:
    with open(fp, "wb") as fh:
        for data in datas:
            fh.write(orjson.dumps(data, option=orjson.OPT_APPEND_NEWLINE))
    return str(fp)


def test_cli_entities_import_unsafe_roundtrip(tmp_path, cli_runner):
    """--unsafe explodes entity JSON straight to parquet rows, stripping
    namespace signatures from ids and entity-type values."""
    in_uri = _write_jsonl(tmp_path / "in.ftm.json", UNSAFE_ENTITIES)

    result = cli_runner.invoke(
        cli_app, ["-d", "dst", "entities", "import", "--unsafe", "-i", in_uri]
    )
    assert result.exit_code == 0, result.output

    dst = EntityRepository("dst", tmp_path / "dst")
    entities = {e.id: e for e in dst.query()}
    assert set(entities) == {"jane", "john"}
    assert entities["jane"].get("proof") == ["doc-1"]
    assert "Jane Doe" in entities["jane"].get("name")

    stmts = list(dst._statements._query_statement_data())
    origins = {r["origin"] for r in stmts if r["entity_id"] == "jane"}
    assert origins == {"crawl"}  # payload origin wins over the bulk default


def _sorted_rows(repo: EntityRepository) -> list[dict]:
    rows = list(repo._statements._query_statement_data())
    return sorted(rows, key=lambda r: (r["id"], r["fragment"]))


def test_cli_entities_import_unsafe_parity(tmp_path, cli_runner):
    """The same input file lands as identical physical rows via both paths.

    Both imports target a dataset of the same name (in separate lakehouse
    roots, addressed via ``--uri``) because statement ids content-hash
    under the target dataset.
    """
    in_uri = _write_jsonl(tmp_path / "in.ftm.json", UNSAFE_ENTITIES)

    for root, flags in (("root_safe", []), ("root_unsafe", ["--unsafe"])):
        result = cli_runner.invoke(
            cli_app,
            ["--uri", str(tmp_path / root), "-d", "ds"]
            + ["entities", "import", *flags, "-i", in_uri],
        )
        assert result.exit_code == 0, result.output

    safe = _sorted_rows(EntityRepository("ds", tmp_path / "root_safe" / "ds"))
    unsafe = _sorted_rows(EntityRepository("ds", tmp_path / "root_unsafe" / "ds"))
    assert len(safe) == 5  # 2x name + proof + 2x BASE checksum
    assert safe == unsafe
    # ids hash under the target dataset, not the payload's dataset context
    name_row = next(r for r in safe if r["prop"] == "name" and r["value"] == "Jane Doe")
    assert name_row["id"] == Statement.make_key("ds", "jane", "name", "Jane Doe", False)


def test_import_entities_unsafe_bounded_buffer(tmp_path):
    """A payload larger than bulk_size flushes mid-entity instead of growing
    the row buffer unbounded."""
    lake = get_lakehouse(str(tmp_path))
    repo = get_entities("ds", lake.dataset_uri("ds"))
    payload = {
        "id": "big",
        "schema": "Person",
        "properties": {"name": [f"Name {i}" for i in range(10)]},
        "last_change": "2024-01-01T00:00:00",
    }
    import_entities_unsafe(repo, [payload], bulk_size=3)

    rows = list(repo._statements._query_statement_data())
    assert len(rows) == 11  # 10 names + BASE stub
    assert repo.version is not None and repo.version >= 3  # several flushes


def test_cli_statements_import_unsafe_roundtrip(tmp_path, cli_runner):
    """--unsafe maps exported CSV rows straight to parquet rows, identical
    to the safe statement import (same target dataset name in separate
    roots via ``--uri`` – statement ids content-hash under the target
    dataset)."""
    src = _seed_source(tmp_path)
    src._store.ensure_parent(path.EXPORTS_STATEMENTS)
    src._statements.export_csv(path.EXPORTS_STATEMENTS)
    csv_uri = str(tmp_path / "src" / path.EXPORTS_STATEMENTS)

    for root, flags in (("root_safe", []), ("root_unsafe", ["--unsafe"])):
        result = cli_runner.invoke(
            cli_app,
            ["--uri", str(tmp_path / root), "-d", "ds"]
            + ["statements", "import", *flags, "-i", csv_uri],
        )
        assert result.exit_code == 0, result.output

    unsafe = EntityRepository("ds", tmp_path / "root_unsafe" / "ds")
    stmts = list(unsafe._statements.query_statements())
    assert len(stmts) == 4  # 2 entities x (id + name statements)
    for stmt in stmts:
        assert stmt.external is False
        assert stmt.lang is None
        assert stmt.origin == "test"  # carried from the CSV, not the default
    entities = {e.id: e for e in unsafe.query()}
    assert set(entities) == {"jane", "john"}

    safe_rows = _sorted_rows(EntityRepository("ds", tmp_path / "root_safe" / "ds"))
    unsafe_rows = _sorted_rows(unsafe)
    assert safe_rows == unsafe_rows


def test_cli_entities_import_per_item_origin(tmp_path, cli_runner):
    """Payload origin applies per item: an origin-less entity gets the CLI
    default, and a multi-origin payload (ambiguous) falls back to the default
    too."""
    in_uri = str(tmp_path / "entities.ftm.json")
    rows = [
        {
            "id": "a",
            "schema": "Person",
            "properties": {"name": ["A"]},
            "origin": ["crawl"],
        },
        {"id": "b", "schema": "Person", "properties": {"name": ["B"]}},
        {
            "id": "c",
            "schema": "Person",
            "properties": {"name": ["C"]},
            "origin": ["x", "y"],
        },
    ]
    with open(in_uri, "wb") as fh:
        for row in rows:
            fh.write(orjson.dumps(row, option=orjson.OPT_APPEND_NEWLINE))

    result = cli_runner.invoke(
        cli_app, ["-d", "dst", "entities", "import", "-i", in_uri]
    )
    assert result.exit_code == 0, result.output

    dst = EntityRepository("dst", tmp_path / "dst")
    origins: dict[str, set[str]] = {}
    for stmt in dst._statements.query_statements():
        origins.setdefault(stmt.entity_id, set()).add(stmt.origin)
    assert origins == {"a": {"crawl"}, "b": {"bulk"}, "c": {"bulk"}}


def test_cli_stream_commands(tmp_path, cli_runner):
    """``entities stream`` / ``statements stream`` pipe the exported files
    byte-for-byte to the output."""
    # seed + export under the CLI's LAKEHOUSE_URI so DatasetContext finds it
    repo = EntityRepository("dst", tmp_path / "dst")
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    repo._store.ensure_parent(path.EXPORTS_STATEMENTS)
    repo._statements.export_csv(path.EXPORTS_STATEMENTS)
    entities_json = tmp_path / "dst" / path.ENTITIES_JSON
    smart_write_proxies(str(entities_json), repo.query())

    out = tmp_path / "streamed.ftm.json"
    result = cli_runner.invoke(
        cli_app, ["-d", "dst", "entities", "stream", "-o", str(out)]
    )
    assert result.exit_code == 0, result.output
    assert out.read_bytes() == entities_json.read_bytes()

    out_csv = tmp_path / "streamed.csv"
    result = cli_runner.invoke(
        cli_app, ["-d", "dst", "statements", "stream", "-o", str(out_csv)]
    )
    assert result.exit_code == 0, result.output
    exported = tmp_path / "dst" / path.EXPORTS_STATEMENTS
    assert out_csv.read_bytes() == exported.read_bytes()
