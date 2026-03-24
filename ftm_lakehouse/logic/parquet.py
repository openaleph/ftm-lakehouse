"""Pure functions for Delta Lake parquet operations with translog-based metadata.

Provides stateless operations on DeltaTable/DuckDB/PyArrow for:
- Translog-aware querying (join main table + translog for accurate timestamps and soft deletes)
- Compaction (apply translog to main table, remove deleted translog entries)
- Deleted entity ID retrieval
- Changed entity ID detection via translog timestamps
"""

import os
from datetime import datetime
from typing import Generator

import duckdb
import pyarrow as pa
from deltalake import DeltaTable
from ftmq.store.lake import Row, compile_query

from ftm_lakehouse.core.settings import Settings

QUERY_IN_BATCH_SIZE = 5_000
TRANSLOG = "translog"
STATEMENTS = "statements"
settings = Settings()


def _default_memory_limit() -> str:
    """60% of system RAM as an absolute GiB value."""
    mem_bytes = os.sysconf("SC_PAGE_SIZE") * os.sysconf("SC_PHYS_PAGES")
    return f"{int(mem_bytes * 0.6) // (1024 ** 3)}GiB"


def _connect() -> duckdb.DuckDBPyConnection:
    """Create a DuckDB connection with temp_directory for disk-spill on large sorts."""
    return duckdb.connect(
        config={
            "temp_directory": settings.tmp_dir,
            "memory_limit": settings.duckdb_memory_limit or _default_memory_limit(),
        }
    )


def _register_delta(con: duckdb.DuckDBPyConnection, name: str, dt: DeltaTable) -> None:
    """Register a DeltaTable via delta_scan() for predicate pushdown.

    Uses Delta column statistics for file skipping on filtered queries.
    delta-kernel-rs metadata lives outside DuckDB's buffer pool, so this is
    not suitable for full-scan operations that need reliable disk spilling —
    use _register_parquet() for those.
    """
    con.execute("INSTALL delta")
    con.execute("LOAD delta")
    con.sql(
        f"CREATE OR REPLACE VIEW {name} AS "
        f"SELECT * FROM delta_scan('{dt.table_uri}')"
    )


def _register_parquet(
    con: duckdb.DuckDBPyConnection, name: str, dt: DeltaTable
) -> None:
    """Register a DeltaTable via read_parquet() for full memory tracking.

    Uses DeltaTable.file_uris() to resolve active files, then DuckDB's native
    parquet reader so all memory is inside DuckDB's buffer pool and disk
    spilling via temp_directory works reliably.  No Delta column statistics
    for file skipping — use for full-scan operations (export, compact, dedup).
    """
    files = dt.file_uris()
    if not files:
        con.register(name, dt.to_pyarrow_dataset())
        return
    quoted = ", ".join(f"'{f}'" for f in files)
    con.sql(
        f"CREATE OR REPLACE VIEW {name} AS "
        f"SELECT * FROM read_parquet([{quoted}], hive_partitioning=true)"
    )


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
    con = _connect()
    _register_delta(con, STATEMENTS, dt)
    _register_delta(con, TRANSLOG, translog_dt)
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

    Uses read_parquet() for full memory tracking — callers (export_csv, query_raw)
    do full-table scans with ORDER BY that need reliable disk spilling.
    """
    con = _connect()
    _register_parquet(con, STATEMENTS, dt)
    _register_parquet(con, TRANSLOG, translog_dt)
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

    con = _connect()
    _register_parquet(con, STATEMENTS, dt)
    _register_parquet(con, TRANSLOG, translog_dt)
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
    con = _connect()
    _register_delta(con, STATEMENTS, dt)
    _register_delta(con, TRANSLOG, translog_dt)
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
    con = _connect()
    _register_delta(con, TRANSLOG, translog_dt)
    return con.sql(
        f"SELECT * FROM {TRANSLOG} WHERE deleted_at IS NULL"
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

    con = _connect()
    _register_delta(con, STATEMENTS, dt)
    _register_delta(con, TRANSLOG, translog_dt)

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
    con = _connect()
    _register_parquet(con, STATEMENTS, dt)
    con.execute(
        f"CREATE TEMP TABLE existing_ids AS SELECT DISTINCT id FROM {STATEMENTS}"
    )
    return con
