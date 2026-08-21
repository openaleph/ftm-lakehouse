"""Schema definitions for the sharded statement store.

Single source of truth for the parquet ``SHARDED_SCHEMA`` (pyarrow), the
matching SQLAlchemy ``sharded_table()`` factory used to compose queries that
execute against DuckDB views over the parquet data, the physical
``journal_table()`` DDL (the journal buffers exactly these rows), and
``statements_to_arrow()``, the one packer both statement write paths use, and
``LakehouseStatement``, the statement those paths pass around.

The schema is: ``shard`` (entity-id hash bucket, hex-padded) prepended to
ftmq's ``ARROW_SCHEMA`` (all statement columns, including ``fragment`` – the
supersession group key, empty-string sentinel for non-fragment rows) minus
``canonical_id``, with ``deleted_at`` (tombstone marker) appended.

``canonical_id`` is dropped from physical storage: this is a single-dataset
store with no entity resolution, so ``canonical_id`` always equals
``entity_id``. It is re-derived as ``entity_id AS canonical_id`` in the live
``statement`` view (:func:`ftm_lakehouse.logic.parquet.live_view_sql`) so
ftmq's query layer (which keys entity identity on ``canonical_id``) keeps
working unchanged.
"""

from datetime import datetime
from typing import Any, Generator, Iterable, TypeAlias

import pyarrow as pa
import pyarrow.compute as pc
from ftmq.store.lake import ARROW_SCHEMA, LakeStatement, statements_to_table
from nomenklatura import settings as nks
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    MetaData,
    Select,
    Table,
    TableClause,
    Text,
    column,
    select,
    table,
)

PA_TS = pa.timestamp("us", tz="UTC")
"""Timezone-aware microsecond timestamp type for metadata columns."""

REQUIRED_COLUMNS = frozenset(
    {
        "shard",
        "id",
        "entity_id",
        "dataset",
        "bucket",
        "origin",
        "schema",
        "prop",
        "value",
        "external",
        "first_seen",
        "last_seen",
        "fragment",
    }
)
"""Columns a statement row must carry, used by every ingress:
:func:`statements_to_arrow` casts into it, the api bulk route's
:meth:`BaseJournalWriter.add_batch` casts the client's batch into it, and
:func:`journal_table` derives ``NOT NULL`` from it.
"""

SHARDED_SCHEMA = pa.schema(
    [
        pa.field("shard", pa.string(), nullable=False),
        *(
            f.with_nullable(False) if f.name in REQUIRED_COLUMNS else f
            for f in ARROW_SCHEMA
            if f.name != "canonical_id"
        ),
        pa.field("deleted_at", PA_TS),
    ]
)
"""Parquet schema: ``shard`` + ftmq ``ARROW_SCHEMA`` (minus ``canonical_id``)
+ ``deleted_at``.

``fragment`` (part of ``ARROW_SCHEMA``) uses the empty string – never
NULL – as the "no fragment" sentinel; :class:`ftmq.store.lake.LakeStatement`
guarantees it on every write path."""

_PA_TO_SA: dict[Any, Any] = {
    pa.bool_(): Boolean(),
    PA_TS: DateTime(timezone=True),
}
"""Column type per pyarrow type – strings fall through to ``Text``.

Serves both SQLAlchemy shapes built from :data:`SHARDED_SCHEMA`: the
``table()`` clause queries compile against, and the physical journal DDL.
"""


def _sa_type(field: pa.Field) -> Any:
    return _PA_TO_SA.get(field.type, Text())


def _sharded_table(name: str) -> TableClause:
    """SQLAlchemy ``table()`` named ``name``, mirroring ``SHARDED_SCHEMA``.

    Use to compose queries that compile to DuckDB SQL via ``literal_binds``
    and execute against a registered view of the same name. Column types
    are derived from the pyarrow schema so the two stay in lockstep.
    """
    return table(name, *(column(f.name, _sa_type(f)) for f in SHARDED_SCHEMA))


# Default view name (``"statement"``) – this is the one the LakeStore
# connection registers as a *deduped* view in ftm_lakehouse, so read
# code targeting ``TABLE`` automatically sees one row per statement id
# with tombstones filtered.
TABLE = _sharded_table(nks.STATEMENT_TABLE)

# Raw view name (``"statement_raw"``) – registered alongside ``TABLE``
# on the same LakeStore connection and surfaces the underlying Delta
# rows unchanged. Targeted by paths that need tombstones and per-row
# physical layout visible: :func:`build_merge_sql` (grace-period
# tombstone retention) and raw-source ``get_entity_ids`` queries (diff
# consumers emit DEL ops).
TABLE_RAW = _sharded_table(f"{nks.STATEMENT_TABLE}_raw")


def journal_table(metadata: MetaData, name: str) -> Table:
    """Physical journal table named ``name``, mirroring :data:`SHARDED_SCHEMA`.

    The journal buffers exactly the rows the parquet store persists, so its
    DDL is derived from the same pyarrow schema – a journal row needs no
    packing to become a statement row, and a segment can be streamed
    straight into :meth:`ParquetStore.append` as Arrow.

    No primary key, no unique constraint, no index: the journal is an
    append-only heap – but the schema's own ``NOT NULL`` columns
    (:data:`REQUIRED_COLUMNS`) still hold, so a row that could not be read
    back never lands. Re-emissions accumulate as extra rows and
    :meth:`ParquetStore.merge` collapses them, which is where dedup lives
    anyway – and without a key, row identity ``(origin, id, fragment)``
    survives the journal instead of collapsing to ``(id, fragment)``.

    Args:
        metadata: The ``MetaData`` to attach the table to.
        name: Table name – the live journal or one of its segments.

    Returns:
        The SQLAlchemy ``Table``.
    """
    cols = (Column(f.name, _sa_type(f), nullable=f.nullable) for f in SHARDED_SCHEMA)
    return Table(name, metadata, *cols)


STATEMENT_CSV_COLUMNS = [
    "id",
    "entity_id",
    "canonical_id",
    "prop",
    "prop_type",
    "schema",
    "value",
    "original_value",
    "dataset",
    "origin",
    "lang",
    "external",
    "first_seen",
    "last_seen",
    "fragment",
]
"""Columns of the exported ``statements.csv``: the followthemoney standard set
plus the lakehouse ``fragment`` supersession key, so ``statements import``
round-trips it (followthemoney's ``read_csv_statements`` has no notion of
``fragment`` – :func:`ftm_lakehouse.helpers.statements.read_csv_statements`
reads it back). ``canonical_id`` is the ``entity_id`` alias, kept for FtM
interop."""

_STATEMENT_CSV_TABLE = table(
    nks.STATEMENT_TABLE, *(column(c) for c in STATEMENT_CSV_COLUMNS)
)


def statement_csv_select() -> Select[Any]:
    """SELECT of :data:`STATEMENT_CSV_COLUMNS` from the live ``statement`` view,
    ordered by ``entity_id`` (so an entity's rows stay contiguous for
    per-partition streaming exports)."""
    return select(_STATEMENT_CSV_TABLE).order_by(_STATEMENT_CSV_TABLE.c.entity_id)


class LakehouseStatement(LakeStatement):
    """A statement carrying the two columns the lakehouse adds to the schema.

    ``shard`` is the entity-id hash bucket the row is partitioned into and
    ``deleted_at`` is the tombstone marker – both are storage facts about a
    statement rather than statement content, and both are lakehouse-only
    (ftmq's lake store partitions by dataset and deletes physically). They
    live here for the same reason ``fragment`` lives on
    :class:`ftmq.store.lake.LakeStatement`: so the write path can pass
    statements around instead of ``(shard, stmt, deleted_at)`` tuples.

    ``shard`` defaults to the single-shard sentinel – :class:`EntityBuffer`
    is the one producer that knows the dataset's shard count, and it always
    sets it.
    """

    __slots__ = ["shard", "deleted_at"]

    def __init__(
        self,
        *args: Any,
        shard: str = "0",
        deleted_at: datetime | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.shard = shard
        self.deleted_at = deleted_at


LakehouseStatements: TypeAlias = Generator[LakehouseStatement, None, None]


def statements_to_arrow(
    statements: Iterable[LakehouseStatement], now: datetime
) -> pa.Table:
    """Pack a stream of statements into a :data:`SHARDED_SCHEMA` table.

    ftmq's :func:`~ftmq.store.lake.statements_to_table` packs the statement
    columns columnwise; this adds the two columns the lakehouse stores on top
    (``shard``, ``deleted_at``), drops ``canonical_id`` (this store never
    resolves entities), and applies the two rules both write paths share:

    - ``first_seen`` / ``last_seen`` fall back to ``now`` when the statement
      carries none,
    - tombstones (``deleted_at`` set) bump ``last_seen`` to the delete
      timestamp so they win the ``ROW_NUMBER() OVER (... ORDER BY last_seen
      DESC)`` tiebreak in :meth:`ParquetStore.merge`.

    Both rules are vectorized fills over the packed columns rather than
    per-row branches, and every column swap below is zero-copy. The closing
    cast is what makes the result *be* :data:`SHARDED_SCHEMA` – including
    its ``NOT NULL`` columns, so a statement missing one is rejected here
    rather than by a reader later.

    Args:
        statements: Statements, typically shard-sorted from
            :meth:`EntityBuffer.flush_buffer`.
        now: Default timestamp for missing ``first_seen`` / ``last_seen``.

    Returns:
        A table with exactly :data:`SHARDED_SCHEMA`.
    """
    statements = list(statements)  # needs materialization before
    table = statements_to_table(statements)
    stamp = pa.scalar(now, PA_TS)
    deleted_at = pa.array([s.deleted_at for s in statements], PA_TS)
    first_seen = pc.fill_null(table.column("first_seen"), stamp)
    last_seen = pc.coalesce(deleted_at, pc.fill_null(table.column("last_seen"), stamp))
    return (
        table.set_column(
            table.schema.get_field_index("first_seen"), "first_seen", first_seen
        )
        .set_column(table.schema.get_field_index("last_seen"), "last_seen", last_seen)
        .append_column("shard", pa.array([s.shard for s in statements], pa.string()))
        .append_column("deleted_at", deleted_at)
        .select(SHARDED_SCHEMA.names)
        .cast(SHARDED_SCHEMA)
    )
