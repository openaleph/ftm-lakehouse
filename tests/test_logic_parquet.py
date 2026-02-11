"""Unit tests for ftm_lakehouse.logic.parquet — sidecar-aware query functions."""

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from ftmq.store.lake import ARROW_SCHEMA

from ftm_lakehouse.logic.parquet import sidecar_aware_sql
from ftm_lakehouse.storage.parquet import PARTITIONS, SIDECAR_SCHEMA


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


def _write_sidecar(path: str, rows: list[dict]):
    """Write sidecar metadata rows."""
    table = pa.Table.from_pylist(rows, schema=SIDECAR_SCHEMA)
    write_deltalake(path, table, mode="append", schema_mode="merge")
    return DeltaTable(path)


def _make_sidecar_row(
    stmt_id: str,
    first_seen: datetime | None = None,
    last_seen: datetime | None = None,
    deleted_at: datetime | None = None,
) -> dict:
    """Build a sidecar metadata row."""
    ts = first_seen or datetime(2024, 1, 1, tzinfo=timezone.utc)
    return {
        "id": stmt_id,
        "first_seen": ts,
        "last_seen": last_seen or ts,
        "deleted_at": deleted_at,
    }


def test_sidecar_aware_sql_filters_deleted(tmp_path):
    """Deleted rows (deleted_at set in sidecar) are excluded from queries."""
    main_uri = str(tmp_path / "main")
    sidecar_uri = str(tmp_path / "sidecar")

    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("john", "name", "John Smith"),
    ]
    dt = _write_rows(main_uri, rows)

    # Sidecar: jane is deleted, john is live
    sidecar_rows = [
        _make_sidecar_row(
            "jane-name-Jane Doe", deleted_at=datetime(2025, 1, 1, tzinfo=timezone.utc)
        ),
        _make_sidecar_row("john-name-John Smith"),
    ]
    sidecar_dt = _write_sidecar(sidecar_uri, sidecar_rows)

    from ftmq.query import Query
    from ftmq.store.lake import compile_query

    compiled = compile_query(Query().sql.statements)

    sql = sidecar_aware_sql(compiled, dt, sidecar_dt)

    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("sidecar", sidecar_dt.to_pyarrow_dataset())
    result = con.execute(sql).fetchall()

    entity_ids = {r[1] for r in result}  # entity_id is second column
    assert "jane" not in entity_ids
    assert "john" in entity_ids


def test_sidecar_aware_sql_uses_sidecar_timestamps(tmp_path):
    """Sidecar first_seen/last_seen overrides main table values."""
    main_uri = str(tmp_path / "main")
    sidecar_uri = str(tmp_path / "sidecar")

    main_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = [_make_row("jane", "name", "Jane Doe", last_seen=main_ts)]
    dt = _write_rows(main_uri, rows)

    sidecar_ts = datetime(2025, 6, 15, tzinfo=timezone.utc)
    sidecar_rows = [
        _make_sidecar_row(
            "jane-name-Jane Doe", first_seen=main_ts, last_seen=sidecar_ts
        ),
    ]
    sidecar_dt = _write_sidecar(sidecar_uri, sidecar_rows)

    from ftmq.query import Query
    from ftmq.store.lake import compile_query

    compiled = compile_query(Query().sql.statements)

    sql = sidecar_aware_sql(compiled, dt, sidecar_dt)

    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("sidecar", sidecar_dt.to_pyarrow_dataset())
    result = con.execute(sql)
    rows_out = result.fetchall()
    cols = [desc[0] for desc in result.description]

    assert len(rows_out) == 1
    row_dict = dict(zip(cols, rows_out[0]))
    # last_seen should come from sidecar (DuckDB may strip tzinfo)
    result_ts = row_dict["last_seen"]
    expected_ts = sidecar_ts.replace(tzinfo=None)
    assert result_ts == expected_ts


def test_sidecar_aware_sql_no_deletes(tmp_path):
    """All rows visible when sidecar has no deletions."""
    main_uri = str(tmp_path / "main")
    sidecar_uri = str(tmp_path / "sidecar")

    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("jane", "firstName", "Jane"),
        _make_row("john", "name", "John Smith"),
    ]
    dt = _write_rows(main_uri, rows)

    sidecar_rows = [
        _make_sidecar_row("jane-name-Jane Doe"),
        _make_sidecar_row("jane-firstName-Jane"),
        _make_sidecar_row("john-name-John Smith"),
    ]
    sidecar_dt = _write_sidecar(sidecar_uri, sidecar_rows)

    from ftmq.query import Query
    from ftmq.store.lake import compile_query

    compiled = compile_query(Query().sql.statements)

    sql = sidecar_aware_sql(compiled, dt, sidecar_dt)

    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("sidecar", sidecar_dt.to_pyarrow_dataset())
    result = con.execute(sql).fetchall()

    assert len(result) == 3
    entity_ids = {r[1] for r in result}
    assert entity_ids == {"jane", "john"}
