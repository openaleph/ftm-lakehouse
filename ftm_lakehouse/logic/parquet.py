"""Pure functions for Delta Lake parquet operations with tombstone-based soft deletes.

Provides stateless operations on DeltaTable/DuckDB/PyArrow for:
- Deduplication with tombstone filtering (used during compaction)
- Full compaction (dedup + rewrite + optimize + vacuum)
"""

import duckdb
from deltalake import DeltaTable, write_deltalake
from ftmq.store.lake import storage_options


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


def compact(dt: DeltaTable, partition_by: list[str]) -> None:
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
