"""Pure functions for Delta Lake parquet operations with translog-based metadata.

Provides stateless operations on DeltaTable/DuckDB/PyArrow for:
- Translog-aware querying (join main table + translog for accurate timestamps and soft deletes)
- Compaction (apply translog to main table, remove deleted translog entries)
- Deleted entity ID retrieval
- Changed entity ID detection via translog timestamps
"""

from datetime import datetime
from typing import Generator

import duckdb
import pyarrow as pa
from deltalake import DeltaTable
from ftmq.store.lake import Row, compile_query

QUERY_IN_BATCH_SIZE = 5_000
TRANSLOG = "translog"
STATEMENTS = "statements"


def translog_aware_sql(compiled_query: str, dt: DeltaTable) -> str:
    """Wrap a compiled SQL query with a CTE that joins the translog.

    The CTE joins the main statements table with the translog to:
    - Use translog's first_seen/last_seen instead of main table's
    - Filter out rows where translog.deleted_at IS NOT NULL

    Note: compile_query() from ftmq hardcodes ``FROM arrow as statement``.
    The CTE reads from {STATEMENTS}/{TRANSLOG} and the rewrite swaps
    ``arrow`` for ``__live`` so the compiled query hits the CTE result.
    """
    all_cols = [f.name for f in dt.schema().to_arrow()]
    main_cols = [
        f"{STATEMENTS}.{c}" for c in all_cols if c not in ("first_seen", "last_seen")
    ]
    select_cols = ", ".join(main_cols + ["sc.first_seen", "sc.last_seen"])

    cte = f"""WITH __live AS (
    SELECT {select_cols}
    FROM {STATEMENTS}
    JOIN {TRANSLOG} sc ON {STATEMENTS}.id = sc.id
    WHERE sc.deleted_at IS NULL
)
"""
    # compile_query() hardcodes "FROM arrow as statement"
    rewritten = compiled_query.replace(
        "FROM arrow as statement", "FROM __live as statement"
    )
    return cte + rewritten


def stream_duckdb_translog(
    q, dt: DeltaTable, translog_dt: DeltaTable
) -> Generator[Row, None, None]:
    """Like stream_duckdb but joins with translog for accurate timestamps and soft deletes."""
    con = duckdb.connect()
    con.register(STATEMENTS, dt.to_pyarrow_dataset())
    con.register(TRANSLOG, translog_dt.to_pyarrow_dataset())
    compiled = compile_query(q)
    sql = translog_aware_sql(compiled, dt)
    rel = con.sql(sql)
    columns = rel.columns
    while rows := rel.fetchmany(100_000):
        for row in rows:
            yield Row(dict(zip(columns, row)))


def query_duckdb_translog(
    q, dt: DeltaTable, translog_dt: DeltaTable
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyConnection]:
    """Like query_duckdb but joins with translog for accurate timestamps and soft deletes.

    Returns (relation, connection) tuple. Caller must hold the connection reference
    to prevent GC from closing it while the relation is still in use.
    """
    con = duckdb.connect()
    con.register(STATEMENTS, dt.to_pyarrow_dataset())
    con.register(TRANSLOG, translog_dt.to_pyarrow_dataset())
    compiled = compile_query(q)
    sql = translog_aware_sql(compiled, dt)
    return con.sql(sql), con


def compact_with_translog(
    dt: DeltaTable, translog_dt: DeltaTable
) -> pa.RecordBatchReader:
    """Join main table with translog, returning only live rows with accurate timestamps.

    Args:
        dt: Main statement DeltaTable
        translog_dt: Translog metadata DeltaTable

    Returns:
        RecordBatchReader of live rows with translog timestamps applied
    """
    all_cols = [f.name for f in dt.schema().to_arrow()]
    main_cols = [
        f"{STATEMENTS}.{c}" for c in all_cols if c not in ("first_seen", "last_seen")
    ]
    select_cols = ", ".join(main_cols + ["sc.first_seen", "sc.last_seen"])

    con = duckdb.connect()
    con.register(STATEMENTS, dt.to_pyarrow_dataset())
    con.register(TRANSLOG, translog_dt.to_pyarrow_dataset())
    return con.sql(
        f"SELECT {select_cols} FROM {STATEMENTS} "
        f"JOIN {TRANSLOG} sc ON {STATEMENTS}.id = sc.id "
        "WHERE sc.deleted_at IS NULL"
    ).fetch_arrow_reader()


def get_deleted_entity_ids(dt: DeltaTable, translog_dt: DeltaTable) -> set[str]:
    """Get entity IDs that have been soft-deleted via translog.

    Args:
        dt: Main statement DeltaTable
        translog_dt: Translog metadata DeltaTable

    Returns:
        Set of entity_id strings with at least one deleted statement
    """
    con = duckdb.connect()
    con.register(STATEMENTS, dt.to_pyarrow_dataset())
    con.register(TRANSLOG, translog_dt.to_pyarrow_dataset())
    result = con.execute(
        f"SELECT DISTINCT {STATEMENTS}.entity_id FROM {STATEMENTS} "
        f"JOIN {TRANSLOG} sc ON {STATEMENTS}.id = sc.id "
        "WHERE sc.deleted_at IS NOT NULL"
    )
    return {r[0] for r in result.fetchall()}


def filter_live_translog(translog_dt: DeltaTable) -> pa.RecordBatchReader:
    """Return only live (non-deleted) translog rows.

    Args:
        translog_dt: Translog metadata DeltaTable

    Returns:
        RecordBatchReader of translog rows where deleted_at IS NULL
    """
    rel = duckdb.arrow(translog_dt.to_pyarrow_dataset())
    return rel.query(
        TRANSLOG, f"SELECT * FROM {TRANSLOG} WHERE deleted_at IS NULL"
    ).fetch_arrow_reader()


def get_changed_entity_ids(
    dt: DeltaTable,
    translog_dt: DeltaTable,
    since: datetime,
    schema_in: list[str] | None = None,
    prop: str | None = None,
) -> set[str]:
    """Get entity IDs with statements added since a timestamp.

    Joins the main table with the translog to find entities whose statements
    have a first_seen at or after the given timestamp (truncated to seconds).

    FtM serialization truncates timestamps to second precision, so ``since``
    is floored to whole seconds and the comparison uses ``>=``.  This may
    re-export entities from the same second as the previous diff — a harmless
    false positive since diffs are idempotent.

    Args:
        dt: Main statement DeltaTable
        translog_dt: Translog metadata DeltaTable
        since: Only include entities with statements added at or after this time
        schema_in: Optional list of schema names to filter by
        prop: Optional property name to filter by

    Returns:
        Set of entity_id strings with changed statements
    """
    # Truncate to seconds — FtM timestamps have second precision only
    since_truncated = since.replace(microsecond=0)

    con = duckdb.connect()
    con.register(STATEMENTS, dt.to_pyarrow_dataset())
    con.register(TRANSLOG, translog_dt.to_pyarrow_dataset())

    sql = (
        f"SELECT DISTINCT {STATEMENTS}.entity_id FROM {STATEMENTS} "
        f"JOIN {TRANSLOG} sc ON {STATEMENTS}.id = sc.id "
        "WHERE sc.first_seen >= $since"
    )
    params: dict = {"since": since_truncated}
    if schema_in:
        placeholders = ",".join(f"'{s}'" for s in schema_in)
        sql += f" AND {STATEMENTS}.schema IN ({placeholders})"
    if prop:
        sql += f" AND {STATEMENTS}.prop = '{prop}'"

    result = con.execute(sql, params)
    return {r[0] for r in result.fetchall()}


def make_dedup_connection(dt: DeltaTable) -> duckdb.DuckDBPyConnection | None:
    """Create a DuckDB connection with a temp table of existing statement IDs.

    Returns None if the main parquet table doesn't exist yet (first flush).
    """
    con = duckdb.connect()
    con.register(STATEMENTS, dt.to_pyarrow_dataset())
    con.execute(
        f"CREATE TEMP TABLE existing_ids AS SELECT DISTINCT id FROM {STATEMENTS}"
    )
    return con
