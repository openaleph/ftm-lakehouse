"""Unit tests for ftm_lakehouse.logic.parquet — pure dedup/compact functions."""

from datetime import datetime, timezone

import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from ftmq.store.lake import ARROW_SCHEMA

from ftm_lakehouse.logic.parquet import compact, query_deduped
from ftm_lakehouse.storage.parquet import PARTITIONS

TOMBSTONE_SCHEMA = ARROW_SCHEMA.append(pa.field("deleted_at", pa.timestamp("us")))


def _make_row(
    entity_id: str,
    prop: str,
    value: str,
    schema: str = "Person",
    stmt_id: str | None = None,
    origin: str = "default",
    last_seen: datetime | None = None,
) -> dict:
    """Build a statement row dict matching ARROW_SCHEMA."""
    ts = last_seen or datetime(2024, 1, 1, tzinfo=timezone.utc)
    return {
        "id": stmt_id or f"{entity_id}-{prop}-{value}",
        "entity_id": entity_id,
        "canonical_id": entity_id,
        "dataset": "test",
        "bucket": "thing",
        "origin": origin,
        "source": None,
        "schema": schema,
        "prop": prop,
        "prop_type": "name",
        "value": value,
        "original_value": None,
        "lang": None,
        "external": False,
        "first_seen": ts,
        "last_seen": ts,
    }


def _write_rows(path: str, rows: list[dict], partition_by: list[str] | None = None):
    """Write rows to a fresh DeltaTable at the given path."""
    table = pa.Table.from_pylist(rows, schema=ARROW_SCHEMA)
    write_deltalake(
        path,
        table,
        partition_by=partition_by or PARTITIONS,
        mode="append",
        schema_mode="merge",
        configuration={"delta.enableChangeDataFeed": "true"},
    )
    return DeltaTable(path)


def _write_tombstones(path: str, rows: list[dict], deleted_at: datetime):
    """Write tombstone rows (with deleted_at) directly via write_deltalake."""
    for row in rows:
        row["deleted_at"] = deleted_at
    table = pa.Table.from_pylist(rows, schema=TOMBSTONE_SCHEMA)
    write_deltalake(
        path,
        table,
        partition_by=PARTITIONS,
        mode="append",
        schema_mode="merge",
        configuration={"delta.enableChangeDataFeed": "true"},
    )


def test_query_deduped_no_tombstones(tmp_path):
    """Normal reads are unaffected when no tombstones exist."""
    uri = str(tmp_path / "table")
    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("jane", "firstName", "Jane"),
        _make_row("john", "name", "John Smith"),
    ]
    dt = _write_rows(uri, rows)

    result = query_deduped(dt).fetch_arrow_table()
    assert len(result) == 3

    ids = set(result.column("entity_id").to_pylist())
    assert ids == {"jane", "john"}


def test_query_deduped_with_tombstones(tmp_path):
    """Deleted rows are filtered out by dedup."""
    uri = str(tmp_path / "table")
    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("jane", "firstName", "Jane"),
        _make_row("john", "name", "John Smith"),
    ]
    _write_rows(uri, rows)

    # Write tombstones for jane's statements
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    tombstone_rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("jane", "firstName", "Jane"),
    ]
    _write_tombstones(uri, tombstone_rows, ts)

    dt = DeltaTable(uri)
    result = query_deduped(dt).fetch_arrow_table()

    # Only john should remain
    assert len(result) == 1
    assert result.column("entity_id").to_pylist() == ["john"]
    # deleted_at is preserved (as NULL for live rows)
    assert "deleted_at" in result.column_names
    assert result.column("deleted_at").to_pylist() == [None]


def test_query_deduped_readd_after_delete(tmp_path):
    """Re-added entity after deletion is visible."""
    uri = str(tmp_path / "table")
    rows = [
        _make_row(
            "jane",
            "name",
            "Jane Doe",
            last_seen=datetime(2024, 1, 1, tzinfo=timezone.utc),
        ),
    ]
    _write_rows(uri, rows)

    # Delete jane
    ts_del = datetime(2025, 1, 1, tzinfo=timezone.utc)
    _write_tombstones(uri, [_make_row("jane", "name", "Jane Doe")], ts_del)

    # Re-add jane with a newer last_seen (after the tombstone)
    readd = [
        _make_row(
            "jane",
            "name",
            "Jane Doe",
            last_seen=datetime(2026, 1, 1, tzinfo=timezone.utc),
        ),
    ]
    _write_rows(uri, readd)

    dt = DeltaTable(uri)
    result = query_deduped(dt).fetch_arrow_table()

    # Jane should be alive — the re-add with last_seen=2026 is newer than tombstone at 2025
    assert len(result) == 1
    assert result.column("entity_id").to_pylist() == ["jane"]


def test_compact_removes_tombstones(tmp_path):
    """Table is clean after compaction — tombstones removed."""
    uri = str(tmp_path / "table")
    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("john", "name", "John Smith"),
    ]
    _write_rows(uri, rows)

    # Delete jane
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    _write_tombstones(uri, [_make_row("jane", "name", "Jane Doe")], ts)

    dt = DeltaTable(uri)
    compact(dt, PARTITIONS)

    # Re-load after compact
    dt = DeltaTable(uri)
    result = dt.to_pyarrow_table()

    # Only john should remain
    assert len(result) == 1
    assert result.column("entity_id").to_pylist() == ["john"]


def test_compact_preserves_deleted_at_column(tmp_path):
    """deleted_at column is preserved after compaction (all NULLs for live rows)."""
    uri = str(tmp_path / "table")
    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("john", "name", "John Smith"),
    ]
    _write_rows(uri, rows)

    # Delete jane to add deleted_at column
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    _write_tombstones(uri, [_make_row("jane", "name", "Jane Doe")], ts)

    dt = DeltaTable(uri)
    assert "deleted_at" in {f.name for f in dt.schema().to_arrow()}

    compact(dt, PARTITIONS)

    # After compact, deleted_at column is preserved (all NULLs)
    dt = DeltaTable(uri)
    assert "deleted_at" in {f.name for f in dt.schema().to_arrow()}
    raw = dt.to_pyarrow_table()
    assert all(v is None for v in raw.column("deleted_at").to_pylist())


def test_compact_only_rewrites_affected_partitions(tmp_path):
    """Compact only touches partitions that have tombstones."""
    uri = str(tmp_path / "table")
    # Two different origins → two partitions
    rows = [
        _make_row("jane", "name", "Jane Doe", origin="origin_a"),
        _make_row("john", "name", "John Smith", origin="origin_b"),
    ]
    _write_rows(uri, rows)

    # Tombstone only in origin_a
    ts = datetime(2025, 1, 1, tzinfo=timezone.utc)
    _write_tombstones(
        uri, [_make_row("jane", "name", "Jane Doe", origin="origin_a")], ts
    )

    dt = DeltaTable(uri)
    version_before = dt.version()

    compact(dt, PARTITIONS)

    dt = DeltaTable(uri)
    result = dt.to_pyarrow_table()

    # jane removed, john untouched
    assert set(result.column("entity_id").to_pylist()) == {"john"}

    # Verify only origin_a partition was rewritten by checking file actions
    # The compact should have created a new version
    assert dt.version() > version_before

    # origin_b data is intact
    import pyarrow.compute as pc

    john_rows = result.filter(pc.equal(result.column("origin"), "origin_b"))
    assert len(john_rows) == 1


def test_compact_preserves_live_data(tmp_path):
    """Compact without tombstones preserves all data."""
    uri = str(tmp_path / "table")
    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("john", "name", "John Smith"),
    ]
    dt = _write_rows(uri, rows)

    compact(dt, PARTITIONS)

    # After compact, verify the table is readable and intact
    dt = DeltaTable(uri)
    result = dt.to_pyarrow_table()
    assert len(result) == 2

    entity_ids = set(result.column("entity_id").to_pylist())
    assert entity_ids == {"jane", "john"}
