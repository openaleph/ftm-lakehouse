"""Unit tests for ftm_lakehouse.logic.parquet — translog-aware query functions."""

from datetime import datetime, timezone

import duckdb
import pyarrow as pa
from deltalake import DeltaTable, write_deltalake
from ftmq.query import Query
from ftmq.store.lake import ARROW_SCHEMA, compile_query

from ftm_lakehouse.logic.parquet import translog_aware_sql
from ftm_lakehouse.storage.parquet import PARTITIONS, TRANSLOG_SCHEMA


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


def _write_translog(path: str, rows: list[dict]):
    """Write translog metadata rows."""
    table = pa.Table.from_pylist(rows, schema=TRANSLOG_SCHEMA)
    write_deltalake(path, table, mode="append", schema_mode="merge")
    return DeltaTable(path)


def _make_translog_row(
    stmt_id: str,
    first_seen: datetime | None = None,
    last_seen: datetime | None = None,
    deleted_at: datetime | None = None,
) -> dict:
    """Build a translog metadata row."""
    ts = first_seen or datetime(2024, 1, 1, tzinfo=timezone.utc)
    return {
        "id": stmt_id,
        "first_seen": ts,
        "last_seen": last_seen or ts,
        "deleted_at": deleted_at,
    }


def test_translog_aware_sql_filters_deleted(tmp_path):
    """Deleted rows (deleted_at set in translog) are excluded from queries."""
    main_uri = str(tmp_path / "main")
    translog_uri = str(tmp_path / "translog")

    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("john", "name", "John Smith"),
    ]
    dt = _write_rows(main_uri, rows)

    # Translog: jane is deleted, john is live
    translog_rows = [
        _make_translog_row(
            "jane-name-Jane Doe", deleted_at=datetime(2025, 1, 1, tzinfo=timezone.utc)
        ),
        _make_translog_row("john-name-John Smith"),
    ]
    translog_dt = _write_translog(translog_uri, translog_rows)

    compiled = compile_query(Query().sql.statements)

    sql = translog_aware_sql(compiled, dt)

    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("translog", translog_dt.to_pyarrow_dataset())
    result = con.execute(sql).fetchall()

    entity_ids = {r[1] for r in result}  # entity_id is second column
    assert "jane" not in entity_ids
    assert "john" in entity_ids


def test_translog_aware_sql_uses_translog_timestamps(tmp_path):
    """Translog first_seen/last_seen overrides main table values."""
    main_uri = str(tmp_path / "main")
    translog_uri = str(tmp_path / "translog")

    main_ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    rows = [_make_row("jane", "name", "Jane Doe", last_seen=main_ts)]
    dt = _write_rows(main_uri, rows)

    translog_ts = datetime(2025, 6, 15, tzinfo=timezone.utc)
    translog_rows = [
        _make_translog_row(
            "jane-name-Jane Doe", first_seen=main_ts, last_seen=translog_ts
        ),
    ]
    translog_dt = _write_translog(translog_uri, translog_rows)

    compiled = compile_query(Query().sql.statements)

    sql = translog_aware_sql(compiled, dt)

    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("translog", translog_dt.to_pyarrow_dataset())
    result = con.execute(sql)
    rows_out = result.fetchall()
    cols = [desc[0] for desc in result.description]

    assert len(rows_out) == 1
    row_dict = dict(zip(cols, rows_out[0]))
    # last_seen should come from translog (DuckDB may strip tzinfo)
    result_ts = row_dict["last_seen"]
    expected_ts = translog_ts.replace(tzinfo=None)
    assert result_ts == expected_ts


def test_translog_aware_sql_no_deletes(tmp_path):
    """All rows visible when translog has no deletions."""
    main_uri = str(tmp_path / "main")
    translog_uri = str(tmp_path / "translog")

    rows = [
        _make_row("jane", "name", "Jane Doe"),
        _make_row("jane", "firstName", "Jane"),
        _make_row("john", "name", "John Smith"),
    ]
    dt = _write_rows(main_uri, rows)

    translog_rows = [
        _make_translog_row("jane-name-Jane Doe"),
        _make_translog_row("jane-firstName-Jane"),
        _make_translog_row("john-name-John Smith"),
    ]
    translog_dt = _write_translog(translog_uri, translog_rows)

    compiled = compile_query(Query().sql.statements)

    sql = translog_aware_sql(compiled, dt)

    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("translog", translog_dt.to_pyarrow_dataset())
    result = con.execute(sql).fetchall()

    assert len(result) == 3
    entity_ids = {r[1] for r in result}
    assert entity_ids == {"jane", "john"}
