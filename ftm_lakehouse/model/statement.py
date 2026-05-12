"""Schema definitions for the sharded statement store.

Single source of truth for the parquet ``SHARDED_SCHEMA`` (pyarrow) and the
matching SQLAlchemy ``sharded_table()`` factory used to compose queries that
execute against DuckDB views over the parquet data.

The schema is: ``shard`` (entity-id hash bucket, hex-padded) prepended to
ftmq's ``ARROW_SCHEMA`` (all statement columns), with ``deleted_at`` appended
as a tombstone marker.
"""

import pyarrow as pa
from ftmq.store.lake import ARROW_SCHEMA
from nomenklatura import settings as nks
from sqlalchemy import Boolean, DateTime, TableClause, column, table

PA_TS = pa.timestamp("us", tz="UTC")
"""Timezone-aware microsecond timestamp type for metadata columns."""

# ftmq derives first_seen / last_seen from SQLAlchemy ``DateTime`` which yields
# tz-naive ``pa.timestamp("us")``. Override to tz-aware UTC so the entire
# timestamp surface (first_seen, last_seen, deleted_at) is consistent.
_TZ_AWARE_FIELDS = {"first_seen", "last_seen"}

SHARDED_SCHEMA = pa.schema(
    [
        pa.field("shard", pa.string()),
        *(
            pa.field(f.name, PA_TS) if f.name in _TZ_AWARE_FIELDS else f
            for f in ARROW_SCHEMA
        ),
        pa.field("deleted_at", PA_TS),
    ]
)
"""Parquet schema: ``shard`` + ftmq ``ARROW_SCHEMA`` (with tz-aware
timestamps) + ``deleted_at``."""

_PA_TO_SA = {
    pa.bool_(): Boolean,
    PA_TS: DateTime,
    pa.timestamp("us"): DateTime,
}


def _sharded_table() -> TableClause:
    """SQLAlchemy ``table()`` mirroring ``SHARDED_SCHEMA``.

    Use to compose queries (e.g. ``build_merge_query``) that compile to DuckDB
    SQL via ``literal_binds`` and execute against a registered view named
    ``name``. Column types are derived from the pyarrow schema so the two stay
    in lockstep.
    """
    cols = []
    for field in SHARDED_SCHEMA:
        sa_type = _PA_TO_SA.get(field.type)
        cols.append(column(field.name, sa_type) if sa_type else column(field.name))
    return table(nks.STATEMENT_TABLE, *cols)


# singleton
TABLE = _sharded_table()
