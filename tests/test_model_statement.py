"""Statement packing: a statement stream into a ``JOURNAL_SCHEMA`` table."""

from datetime import datetime, timezone

from followthemoney import Statement
from rigour.time import utc_now
from sqlalchemy import MetaData

from ftm_lakehouse.model.statement import (
    JOURNAL_SCHEMA,
    LakehouseStatement,
    journal_table,
    statements_to_arrow,
)

NOW = utc_now()


def make_stmt(**kwargs) -> LakehouseStatement:
    fields = {
        "entity_id": "jane",
        "prop": "name",
        "schema": "Person",
        "value": "Jane Doe",
        "dataset": "test",
    }
    fields.update(kwargs)
    return LakehouseStatement(**fields)


def test_model_statement_pack_fields():
    """Every statement field lands in its own typed column."""
    stmt = make_stmt(lang="en", origin="import", external=True, fragment="row1")
    table = statements_to_arrow([stmt], NOW)

    assert table.schema.equals(JOURNAL_SCHEMA)
    # `shard` is not packed - `ParquetStore.append` derives it
    assert "shard" not in table.schema.names
    (packed,) = table.to_pylist()
    assert packed["id"] == stmt.id
    assert packed["entity_id"] == "jane"
    assert packed["dataset"] == "test"
    assert packed["bucket"] == "thing"
    assert packed["origin"] == "import"
    assert packed["source"] is None
    assert packed["schema"] == "Person"
    assert packed["prop"] == "name"
    assert packed["prop_type"] == "name"
    assert packed["value"] == "Jane Doe"
    assert packed["lang"] == "en"
    assert packed["external"] is True
    assert packed["fragment"] == "row1"
    assert packed["deleted_at"] is None


def test_model_statement_pack_defaults():
    """Missing seen-timestamps are stamped, origin falls back."""
    (packed,) = statements_to_arrow([make_stmt()], NOW).to_pylist()
    assert packed["origin"] == "default"
    assert packed["first_seen"] == NOW
    assert packed["last_seen"] == NOW
    assert packed["fragment"] == ""
    assert packed["external"] is False


def test_model_statement_pack_tombstone_bumps_last_seen():
    """A tombstone's last_seen becomes deleted_at, so merge lets it win."""
    deleted_at = datetime(2026, 8, 19, 12, 30, 0, 500000, tzinfo=timezone.utc)
    stmt = make_stmt(deleted_at=deleted_at, last_seen="2020-01-01T00:00:00")
    (packed,) = statements_to_arrow([stmt], NOW).to_pylist()
    assert packed["deleted_at"] == deleted_at
    assert packed["last_seen"] == deleted_at  # microseconds intact
    assert packed["first_seen"] == NOW


def test_model_statement_pack_timestamp_forms():
    """Seen-timestamps arrive in every ISO shape – and as datetimes."""
    rows = [
        make_stmt(first_seen="2020-01-01T10:00:00"),  # naive
        make_stmt(first_seen="2020-01-01T12:00:00+02:00"),  # offset
        make_stmt(first_seen="2020-01-01"),  # date only
        make_stmt(
            first_seen=datetime(2020, 1, 1, 10, tzinfo=timezone.utc)
        ),  # read-back
    ]
    packed = statements_to_arrow(rows, NOW).column("first_seen").to_pylist()
    assert packed[0] == datetime(2020, 1, 1, 10, tzinfo=timezone.utc)
    assert packed[1] == datetime(2020, 1, 1, 10, tzinfo=timezone.utc)
    assert packed[2] == datetime(2020, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert packed[3] == datetime(2020, 1, 1, 10, tzinfo=timezone.utc)


def test_model_statement_pack_defaults_fragment_and_deleted_at():
    """Constructed without storage facts: no fragment, no tombstone, no shard."""
    stmt = LakehouseStatement(
        entity_id="jane", prop="name", schema="Person", value="x", dataset="test"
    )
    assert not hasattr(stmt, "shard")
    (packed,) = statements_to_arrow([stmt], NOW).to_pylist()
    assert "shard" not in packed
    assert packed["fragment"] == ""
    assert packed["deleted_at"] is None


def test_model_statement_upgrades_a_lake_statement():
    """``from_statement`` lifts a plain statement, defaults the lake columns."""
    plain = Statement(
        entity_id="jane", prop="name", schema="Person", value="x", dataset="test"
    )
    stmt = LakehouseStatement.from_statement(plain, fragment="row1")
    assert isinstance(stmt, LakehouseStatement)
    assert (stmt.fragment, stmt.deleted_at) == ("row1", None)


def test_model_statement_pack_empty():
    """An empty stream still yields the schema, so writers can append it."""
    table = statements_to_arrow([], NOW)
    assert table.num_rows == 0
    assert table.schema.equals(JOURNAL_SCHEMA)


def test_model_journal_table_mirrors_schema():
    """The journal DDL is the statement schema – keyless, index-free."""
    table = journal_table(MetaData(), "journal_test")
    assert [c.name for c in table.columns] == list(JOURNAL_SCHEMA.names)
    assert "shard" not in table.columns
    assert list(table.primary_key) == []
    assert table.indexes == set()
    assert not any(c.unique for c in table.columns)
