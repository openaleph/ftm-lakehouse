"""The two statement folds must answer the same thing.

`aggregate_unsafe` keeps every statement dict, `aggregate_batches` reads the
columns straight out of the Arrow batch – so the export sweep's rows and an
ad-hoc query's rows fold into the same entity either way.
"""

import pyarrow as pa
import pytest

from ftm_lakehouse.logic.entities.aggregate import (
    aggregate_batches,
    aggregate_unsafe,
)
from ftm_lakehouse.model.statement import STATEMENT_CSV_COLUMNS

FIRST = "2026-10-01T10:00:00+00:00"
LATER = "2026-10-02T10:00:00+00:00"
LATEST = "2026-10-03T10:00:00+00:00"


def row(entity_id: str, prop: str, value: str, **kwargs) -> dict:
    """One statement row, as the sweep's projection returns it."""
    data = {
        "id": f"{entity_id}-{prop}-{value}",
        "entity_id": entity_id,
        "canonical_id": entity_id,
        "prop": prop,
        "prop_type": "string",
        "schema": "Pages",
        "value": value,
        "original_value": None,
        "dataset": "test",
        "origin": "crawl",
        "lang": None,
        "external": False,
        "first_seen": FIRST,
        "last_seen": LATER,
        "fragment": "",
        "role": None,
    }
    return {**data, **kwargs}


ROWS = [
    # a bare id statement, later than the properties -> last_change
    row("one", "id", "one", first_seen=LATEST),
    row("one", "fileName", "doc.pdf"),
    row("one", "contentHash", "a" * 64, origin="archive", role="user:42"),
    # the same value twice, from two origins -> one property value, two origins
    row("one", "mimeType", "application/pdf"),
    row("one", "mimeType", "application/pdf", origin="other"),
    row("one", "parent", "folder-a", first_seen=LATER, last_seen=LATEST),
    # a second entity, so a fold has to close the first one
    row("two", "id", "two"),
    row("two", "fileName", "sheet.xlsx", schema="Table"),
    row("two", "title", "Sheet", schema="Document"),
]


def make_table(rows: list[dict]) -> pa.Table:
    schema = pa.schema(
        [
            pa.field(c, pa.bool_() if c == "external" else pa.string())
            for c in STATEMENT_CSV_COLUMNS
        ]
    )
    return pa.Table.from_pylist(rows, schema=schema)


def normalize(data: dict) -> dict:
    """Both folds build their lists out of sets, so order is arbitrary."""
    out = dict(data)
    out["properties"] = {k: sorted(v) for k, v in data["properties"].items()}
    for key in ("datasets", "referents", "origin", "role"):
        if key in out:
            out[key] = sorted(out[key])
    return out


@pytest.mark.parametrize("chunksize", [1, 2, 4, 100])
def test_logic_aggregate_batches_parity(chunksize):
    """Identical entities, whatever the batch an entity is split across."""
    table = make_table(ROWS)
    expected = list(aggregate_unsafe(iter(table.to_pylist()), "test"))
    folded = list(aggregate_batches(table.to_batches(chunksize), "test"))

    assert [p.id for p in folded] == [p.id for p in expected] == ["one", "two"]
    assert [p.count for p in folded] == [p.count for p in expected] == [6, 3]
    for got, want in zip(folded, expected):
        assert normalize(got.to_dict()) == normalize(want.to_dict())
        assert got.min_first_seen == want.min_first_seen
        assert got.max_first_seen == want.max_first_seen
        assert got.origins == want.origins

    # the bits the fold is actually about
    one = folded[0].to_dict()
    assert one["schema"] == "Pages"
    assert one["caption"] == "doc.pdf"
    assert one["properties"]["mimeType"] == ["application/pdf"]
    assert sorted(one["origin"]) == ["archive", "crawl", "other"]
    assert one["role"] == ["user:42"]
    assert one["referents"] == []
    assert one["first_seen"] == FIRST  # non-id statements only
    assert one["last_seen"] == LATEST
    assert one["last_change"] == LATEST  # the id statement
    assert folded[0].min_first_seen == FIRST  # id statements included
    assert folded[0].max_first_seen == LATEST
    # schemata that cannot merge fall back to their common ancestor, the
    # same lenient `merge_schema` either path uses
    assert folded[1].to_dict()["schema"] == "Document"


def test_logic_aggregate_batches_empty():
    assert list(aggregate_batches([], "test")) == []
    assert list(aggregate_batches(make_table([]).to_batches(), "test")) == []


def test_logic_aggregate_batches_needs_iso_columns():
    """The fold reads the seen columns as the projection's ISO strings."""
    table = make_table(ROWS)
    stamped = table.set_column(
        table.schema.get_field_index("first_seen"),
        "first_seen",
        pa.array([None] * table.num_rows, pa.timestamp("us", tz="UTC")),
    )
    with pytest.raises(ValueError, match="first_seen"):
        list(aggregate_batches(stamped.to_batches(), "test"))


def test_logic_aggregate_folded_keeps_no_statements():
    """A folded payload answers `to_dict`, not `to_entity`."""
    table = make_table(ROWS)
    folded, _ = list(aggregate_batches(table.to_batches(), "test"))
    assert folded.statements == []
    with pytest.raises(ValueError, match="no statements"):
        folded.to_entity()

    kept, _ = list(aggregate_unsafe(iter(table.to_pylist()), "test"))
    entity = kept.to_entity()
    assert entity.id == "one"
    assert entity.get("fileName") == ["doc.pdf"]
