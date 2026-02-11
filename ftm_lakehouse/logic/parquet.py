"""Pure functions for Delta Lake parquet operations with tombstone-based soft deletes.

Provides stateless operations on DeltaTable/DuckDB/PyArrow for:
- Deduplication with tombstone filtering (used during compaction)
- Full compaction (dedup + rewrite + optimize + vacuum)
"""

from typing import Generator

import duckdb
from deltalake import DeltaTable, write_deltalake
from ftmq.store.lake import Row, compile_query, storage_options
from sqlalchemy import Select


def query_deduped(dt: DeltaTable) -> duckdb.DuckDBPyRelation:
    """Return a DuckDB relation with tombstones filtered and rows deduped.

    Uses ROW_NUMBER() OVER (PARTITION BY id ORDER BY COALESCE(deleted_at, last_seen) DESC)
    to keep only the most recent action per statement. Rows where deleted_at IS NOT NULL
    are filtered out.

    The deleted_at column is kept in output (as all NULLs for live rows) so that
    compact preserves it in the rewritten table.

    NOTE: This scans and sorts the entire table. Only use during compaction.
    """
    rel = duckdb.arrow(dt.to_pyarrow_dataset())

    all_cols = [f.name for f in dt.schema().to_arrow()]

    # Legacy tables without deleted_at — just return as-is
    if "deleted_at" not in all_cols:
        return rel.query("arrow", "SELECT * FROM arrow")

    cols_sql = ", ".join(all_cols)

    sql = f"""
        WITH ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY COALESCE(deleted_at, last_seen) DESC
                ) AS rn
            FROM arrow
        )
        SELECT {cols_sql}
        FROM ranked
        WHERE rn = 1 AND deleted_at IS NULL
    """
    return rel.query("arrow", sql)


def compact_deletes(dt: DeltaTable, partition_by: list[str]) -> None:
    """Dedup + drop tombstones, rewriting only affected partitions.

    Identifies partitions containing tombstone rows, deduplicates and filters
    only those partitions, then overwrites them using predicate-based replace.
    Unaffected partitions are never read or rewritten.

    After this call affected partitions are clean (no tombstone rows, deleted_at
    column preserved as all NULLs). Caller is responsible for optimize + vacuum
    afterwards.
    """
    all_cols = [f.name for f in dt.schema().to_arrow()]
    if "deleted_at" not in all_cols:
        return

    rel = duckdb.arrow(dt.to_pyarrow_dataset())

    # Find which partitions have tombstones
    parts_cols = ", ".join(partition_by)
    affected = rel.query(
        "arrow",
        f"SELECT DISTINCT {parts_cols} FROM arrow WHERE deleted_at IS NOT NULL",
    ).fetchall()

    if not affected:
        return

    # Build predicate covering all affected partitions
    partition_predicates = []
    for values in affected:
        clause = " AND ".join(
            f"{col} = '{val}'" for col, val in zip(partition_by, values)
        )
        partition_predicates.append(f"({clause})")
    predicate = " OR ".join(partition_predicates)

    # Dedup only affected partitions
    cols_sql = ", ".join(all_cols)
    sql = f"""
        WITH scoped AS (
            SELECT * FROM arrow WHERE {predicate}
        ),
        ranked AS (
            SELECT *,
                ROW_NUMBER() OVER (
                    PARTITION BY id
                    ORDER BY COALESCE(deleted_at, last_seen) DESC
                ) AS rn
            FROM scoped
        )
        SELECT {cols_sql}
        FROM ranked
        WHERE rn = 1 AND deleted_at IS NULL
    """
    clean = rel.query("arrow", sql).fetch_arrow_reader()

    write_deltalake(
        str(dt.table_uri),
        clean,
        partition_by=partition_by,
        mode="overwrite",
        predicate=predicate,
        schema_mode="overwrite",
        storage_options=storage_options(),
        configuration={"delta.enableChangeDataFeed": "true"},
    )


def tombstone_aware_sql(compiled_query: str, dt: DeltaTable) -> str:
    """Wrap a compiled SQL query with CTEs that filter tombstoned rows.

    The CTE strategy:
    - __tombstoned: collects max(deleted_at) per statement id from tombstone rows
    - __live: keeps rows where deleted_at IS NULL and either no tombstone exists
      or last_seen > max(deleted_at) (handles re-add after delete)
    - original query runs against __live instead of arrow/statement
    """
    all_cols = [f.name for f in dt.schema().to_arrow()]
    # Qualify with arrow. to avoid ambiguous refs after LEFT JOIN
    live_cols = ", ".join(f"arrow.{c}" for c in all_cols if c != "deleted_at")

    cte = f"""WITH __tombstoned AS (
    SELECT id, MAX(deleted_at) AS __max_del
    FROM arrow WHERE deleted_at IS NOT NULL GROUP BY id
),
__live AS (
    SELECT {live_cols}
    FROM arrow
    LEFT JOIN __tombstoned ON arrow.id = __tombstoned.id
    WHERE arrow.deleted_at IS NULL
    AND (__tombstoned.id IS NULL OR arrow.last_seen > __tombstoned.__max_del)
)
"""
    # Replace FROM clause: "FROM arrow as statement" or "FROM statement"
    rewritten = compiled_query.replace(
        "FROM arrow as statement", "FROM __live as statement"
    )
    return cte + rewritten


def stream_duckdb_tombstone_aware(
    q: Select, dt: DeltaTable
) -> Generator[Row, None, None]:
    """Like stream_duckdb but filters tombstoned rows via CTE."""
    rel = duckdb.arrow(dt.to_pyarrow_dataset())
    compiled = compile_query(q)
    sql = tombstone_aware_sql(compiled, dt)
    res = rel.query("arrow", sql)
    while rows := res.fetchmany(100_000):
        for row in rows:
            yield Row(dict(zip(res.columns, row)))


def query_duckdb_tombstone_aware(q: Select, dt: DeltaTable) -> duckdb.DuckDBPyRelation:
    """Like query_duckdb but filters tombstoned rows via CTE."""
    rel = duckdb.arrow(dt.to_pyarrow_dataset())
    compiled = compile_query(q)
    sql = tombstone_aware_sql(compiled, dt)
    return rel.query("arrow", sql)
