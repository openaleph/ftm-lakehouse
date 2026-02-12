"""Pure functions for Delta Lake parquet operations with sidecar-based metadata.

Provides stateless operations on DeltaTable/DuckDB/PyArrow for:
- Sidecar-aware querying (join main table + sidecar for accurate timestamps and soft deletes)
- Compaction (apply sidecar to main table, remove deleted sidecar entries)
- Deleted entity ID retrieval
"""

from typing import Generator

import duckdb
import pyarrow as pa
from deltalake import DeltaTable
from ftmq.store.lake import Row, compile_query


def sidecar_aware_sql(compiled_query: str, dt: DeltaTable) -> str:
    """Wrap a compiled SQL query with a CTE that joins the sidecar.

    The CTE joins the main arrow table with the sidecar to:
    - Use sidecar's first_seen/last_seen instead of main table's
    - Filter out rows where sidecar.deleted_at IS NOT NULL
    """
    all_cols = [f.name for f in dt.schema().to_arrow()]
    # Use sidecar's first_seen/last_seen instead of main table's
    main_cols = [f"arrow.{c}" for c in all_cols if c not in ("first_seen", "last_seen")]
    select_cols = ", ".join(main_cols + ["sc.first_seen", "sc.last_seen"])

    cte = f"""WITH __live AS (
    SELECT {select_cols}
    FROM arrow
    JOIN sidecar sc ON arrow.id = sc.id
    WHERE sc.deleted_at IS NULL
)
"""
    rewritten = compiled_query.replace(
        "FROM arrow as statement", "FROM __live as statement"
    )
    return cte + rewritten


def stream_duckdb_sidecar(
    q, dt: DeltaTable, sidecar_dt: DeltaTable
) -> Generator[Row, None, None]:
    """Like stream_duckdb but joins with sidecar for accurate timestamps and soft deletes."""
    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("sidecar", sidecar_dt.to_pyarrow_dataset())
    compiled = compile_query(q)
    sql = sidecar_aware_sql(compiled, dt)
    rel = con.sql(sql)
    columns = rel.columns
    while rows := rel.fetchmany(100_000):
        for row in rows:
            yield Row(dict(zip(columns, row)))


def query_duckdb_sidecar(
    q, dt: DeltaTable, sidecar_dt: DeltaTable
) -> tuple[duckdb.DuckDBPyRelation, duckdb.DuckDBPyConnection]:
    """Like query_duckdb but joins with sidecar for accurate timestamps and soft deletes.

    Returns (relation, connection) tuple. Caller must hold the connection reference
    to prevent GC from closing it while the relation is still in use.
    """
    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("sidecar", sidecar_dt.to_pyarrow_dataset())
    compiled = compile_query(q)
    sql = sidecar_aware_sql(compiled, dt)
    return con.sql(sql), con


def compact_with_sidecar(
    dt: DeltaTable, sidecar_dt: DeltaTable
) -> pa.RecordBatchReader:
    """Join main table with sidecar, returning only live rows with accurate timestamps.

    Args:
        dt: Main statement DeltaTable
        sidecar_dt: Sidecar metadata DeltaTable

    Returns:
        RecordBatchReader of live rows with sidecar timestamps applied
    """
    all_cols = [f.name for f in dt.schema().to_arrow()]
    main_cols = [f"arrow.{c}" for c in all_cols if c not in ("first_seen", "last_seen")]
    select_cols = ", ".join(main_cols + ["sc.first_seen", "sc.last_seen"])

    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("sidecar", sidecar_dt.to_pyarrow_dataset())
    return con.sql(
        f"SELECT {select_cols} FROM arrow "
        "JOIN sidecar sc ON arrow.id = sc.id "
        "WHERE sc.deleted_at IS NULL"
    ).fetch_arrow_reader()


def get_deleted_entity_ids(dt: DeltaTable, sidecar_dt: DeltaTable) -> set[str]:
    """Get entity IDs that have been soft-deleted via sidecar.

    Args:
        dt: Main statement DeltaTable
        sidecar_dt: Sidecar metadata DeltaTable

    Returns:
        Set of entity_id strings with at least one deleted statement
    """
    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.register("sidecar", sidecar_dt.to_pyarrow_dataset())
    result = con.execute(
        "SELECT DISTINCT arrow.entity_id FROM arrow "
        "JOIN sidecar sc ON arrow.id = sc.id "
        "WHERE sc.deleted_at IS NOT NULL"
    )
    return {r[0] for r in result.fetchall()}


def filter_live_sidecar(sidecar_dt: DeltaTable) -> pa.RecordBatchReader:
    """Return only live (non-deleted) sidecar rows.

    Args:
        sidecar_dt: Sidecar metadata DeltaTable

    Returns:
        RecordBatchReader of sidecar rows where deleted_at IS NULL
    """
    rel = duckdb.arrow(sidecar_dt.to_pyarrow_dataset())
    return rel.query(
        "sidecar", "SELECT * FROM sidecar WHERE deleted_at IS NULL"
    ).fetch_arrow_reader()


def make_dedup_connection(dt: DeltaTable) -> duckdb.DuckDBPyConnection | None:
    """Create a DuckDB connection with a temp table of existing statement IDs.

    Returns None if the main parquet table doesn't exist yet (first flush).
    """
    con = duckdb.connect()
    con.register("arrow", dt.to_pyarrow_dataset())
    con.execute("CREATE TEMP TABLE existing_ids AS SELECT DISTINCT id FROM arrow")
    return con
