"""DuckDB test helpers.

Bare DuckDB connections against a specific Delta table outside the
``LakeStore`` lifecycle – production code lets :class:`ftmq.store.lake.LakeStore`
manage connections and register the configured ``view_sqls`` instead.
"""

import duckdb
from deltalake import DeltaTable

from ftm_lakehouse.logic.parquet import duckdb_config, raw_view_sql
from ftm_lakehouse.model.statement import TABLE


def make_duckdb() -> duckdb.DuckDBPyConnection:
    """Stand-alone DuckDB connection with the lakehouse memory / spill config."""
    config: dict[str, str] = {
        "autoinstall_known_extensions": "true",
        "autoload_known_extensions": "true",
        **duckdb_config(),
    }
    return duckdb.connect(":memory:", config=config)


def register_view(
    con: duckdb.DuckDBPyConnection,
    dt: DeltaTable,
    name: str = TABLE.name,
) -> None:
    """Register a raw ``delta_scan`` view named ``name`` on ``con``."""
    con.sql(f"CREATE OR REPLACE VIEW {name} AS {raw_view_sql(dt)}")
