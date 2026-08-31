"""Schema definitions for the sharded statement store.

Single source of truth for the parquet ``SHARDED_SCHEMA`` (pyarrow), the
matching SQLAlchemy ``sharded_table()`` factory used to compose queries that
execute against DuckDB views over the parquet data, the physical
``journal_table()`` DDL (the journal buffers exactly these rows), and
``statements_to_arrow()``, the one packer both statement write paths use, and
``LakehouseStatement``, the statement those paths pass around.

``JOURNAL_SCHEMA`` is ftmq's ``ARROW_SCHEMA`` (all statement columns, including
``fragment`` – the supersession group key, empty-string sentinel for
non-fragment rows) minus ``canonical_id``, with the two lakehouse-only columns
appended – ``role`` (who asserted the statement) and ``deleted_at`` (tombstone
marker): what producers pack and what the journal stores.
``SHARDED_SCHEMA`` prepends ``shard`` (entity-id hash bucket, hex-padded) and is
what parquet holds – [`append`][ftm_lakehouse.storage.parquet.ParquetStore.append]
derives that column from ``entity_id``, so no producer carries a shard key of
its own.

``canonical_id`` is dropped from physical storage: this is a single-dataset
store with no entity resolution, so ``canonical_id`` always equals
``entity_id``. It is re-derived as ``entity_id AS canonical_id`` in the live
``statement`` view ([`live_view_sql`][ftm_lakehouse.logic.parquet.live_view_sql]) so
ftmq's query layer (which keys entity identity on ``canonical_id``) keeps
working unchanged.
"""

from datetime import datetime
from typing import Any, Generator, Iterable, TypeAlias, cast

import pyarrow as pa
import pyarrow.compute as pc
from anystore.io.read import smart_stream_csv
from anystore.types import Uri
from followthemoney.statement.statement import StatementDict
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
[`statements_to_arrow`][statements_to_arrow] casts into it, the api bulk route's
`BaseJournalWriter.add_batch` casts the client's batch into it, and
[`journal_table`][journal_table] derives ``NOT NULL`` from it.

``shard`` is deliberately absent: producers do not carry it (see
`JOURNAL_SCHEMA`)."""

JOURNAL_SCHEMA = pa.schema(
    [
        *(
            f.with_nullable(False) if f.name in REQUIRED_COLUMNS else f
            for f in ARROW_SCHEMA
            if f.name != "canonical_id"
        ),
        pa.field("role", pa.string()),
        pa.field("deleted_at", PA_TS),
    ]
)
"""Producer schema: ftmq ``ARROW_SCHEMA`` (minus ``canonical_id``) + ``role`` +
``deleted_at``, and **no** ``shard``.

What every write path packs and what the journal physically stores. ``shard``
is not producer state – it is derived from ``entity_id`` during parquet write.

``fragment`` (part of ``ARROW_SCHEMA``) uses the empty string – never
NULL – as the "no fragment" sentinel; `ftmq.store.lake.LakeStatement`
guarantees it on every write path.

``role`` is nullable and deliberately *not* in `REQUIRED_COLUMNS`: NULL is
the "no role" case, the way it is for ``deleted_at``. It is still part of row
identity (`LakehouseStatement.dedupe_key`), which is sound because DuckDB
groups NULLs together in a window ``PARTITION BY`` – role-less rows dedupe
against each other in
[`merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge] like any other
role."""

SHARDED_SCHEMA = pa.schema(
    [pa.field("shard", pa.string(), nullable=False), *JOURNAL_SCHEMA]
)
"""Parquet schema: `JOURNAL_SCHEMA` with the derived ``shard``
partition key prepended."""

_PA_TO_SA: dict[Any, Any] = {
    pa.bool_(): Boolean(),
    PA_TS: DateTime(timezone=True),
}
"""Column type per pyarrow type – strings fall through to ``Text``.

Serves both SQLAlchemy shapes built from `SHARDED_SCHEMA`: the
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
# physical layout visible: `build_merge_sql` (grace-period
# tombstone retention) and raw-source ``get_entity_ids`` queries (diff
# consumers emit DEL ops).
TABLE_RAW = _sharded_table(f"{nks.STATEMENT_TABLE}_raw")


def journal_table(metadata: MetaData, name: str) -> Table:
    """Physical journal table named ``name``, mirroring `JOURNAL_SCHEMA`.

    The journal buffers exactly the rows producers pack, so its DDL is
    derived from the same pyarrow schema – a journal row needs no packing to
    become a statement row, and a segment can be streamed straight into
    [`ParquetStore.append`][ftm_lakehouse.storage.parquet.ParquetStore.append]
    as Arrow, which appends the derived ``shard`` partition key.

    No primary key, no unique constraint, no index: the journal is an
    append-only heap – but the schema's own ``NOT NULL`` columns
    (`REQUIRED_COLUMNS`) still hold, so a row that could not be read
    back never lands. Re-emissions accumulate as extra rows and
    [`ParquetStore.merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge]
    collapses them, which is where dedup lives anyway – and without a key, row
    identity ``(origin, id, fragment, role)`` survives the journal instead of
    collapsing to ``(id, fragment)``.

    Args:
        metadata: The ``MetaData`` to attach the table to.
        name: Table name – the live journal or one of its segments.

    Returns:
        The SQLAlchemy ``Table``.
    """
    cols = (Column(f.name, _sa_type(f), nullable=f.nullable) for f in JOURNAL_SCHEMA)
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
    "role",
]
"""Columns of the exported ``statements.csv``: the followthemoney standard set
plus the two lakehouse row-identity columns ``fragment`` (supersession key) and
``role`` (who asserted the statement), so ``statements import`` round-trips
them (followthemoney's ``read_csv_statements`` knows neither –
`read_csv_statements` reads them back). ``canonical_id`` is the ``entity_id``
alias, kept for FtM interop."""

_STATEMENT_CSV_TABLE = table(
    nks.STATEMENT_TABLE, *(column(c) for c in STATEMENT_CSV_COLUMNS)
)


def statement_csv_select() -> Select[Any]:
    """SELECT of `STATEMENT_CSV_COLUMNS` from the live ``statement`` view,
    ordered by ``entity_id`` (so an entity's rows stay contiguous for
    per-partition streaming exports)."""
    return select(_STATEMENT_CSV_TABLE).order_by(_STATEMENT_CSV_TABLE.c.entity_id)


class LakehouseStatement(LakeStatement):
    """A statement carrying the two columns the lakehouse adds to the schema.

    ``deleted_at`` is the tombstone marker – a storage fact about a statement
    rather than statement content, and lakehouse-only (ftmq's lake store
    deletes physically). ``role`` records *who* asserted the statement: an
    identifier a submitting application supplies, alongside ``origin``'s
    *where*. Both live here for the same reason ``fragment`` lives on
    `ftmq.store.lake.LakeStatement`: so the write path can pass statements
    around instead of ``(stmt, deleted_at, role)`` tuples.

    ``role`` joins ``origin`` and ``fragment`` in `dedupe_key`, so two roles
    asserting identical content stay two rows through
    [`merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge] – full
    provenance, rather than one row whose role is whoever wrote last. The
    empty string collapses to ``None`` so "no role" has one representation.

    There is deliberately no ``shard`` attribute – a statement is content plus
    provenance, and which partition it lands in is
    [`append`][ftm_lakehouse.storage.parquet.ParquetStore.append]'s call.
    """

    __slots__ = ["deleted_at", "role"]

    def __init__(
        self,
        *args: Any,
        deleted_at: datetime | None = None,
        role: str | None = None,
        **kwargs: Any,
    ) -> None:
        super().__init__(*args, **kwargs)
        self.deleted_at = deleted_at
        self.role = role or None

    @property
    def dedupe_key(self) -> str:
        """Stable row identity: ``id``, ``origin``, ``fragment``, ``role``.

        Extends `ftmq.store.lake.LakeStatement.dedupe_key` with the
        lakehouse's fourth identity dimension, so the write buffers collapse
        re-emissions exactly where `merge` does.
        `ftm_lakehouse.helpers.statements.dedupe_key` keeps the same key shape
        for the packed-row paths that never build a statement object.
        """
        return f"{super().dedupe_key}\t{self.role or ''}"

    @classmethod
    def from_dict(cls, data: StatementDict) -> "LakehouseStatement":
        """Read a statement back from a row dict, keeping ``role``.

        ``deleted_at`` is deliberately not read back: every consumer of this
        (statement queries, the api NDJSON wire) reads the *live* view, where
        a surfaced row is by definition not a tombstone.
        """
        stmt = cast("LakehouseStatement", super().from_dict(data))
        stmt.role = cast(dict[str, Any], data).get("role") or None
        return stmt

    @classmethod
    def from_db_row(cls, row: Any) -> "LakehouseStatement":
        """Read a statement back from a SQL row, keeping ``role``."""
        stmt = cast("LakehouseStatement", super().from_db_row(row))
        stmt.role = getattr(row, "role", None) or None
        return stmt


LakehouseStatements: TypeAlias = Generator[LakehouseStatement, None, None]


def clamp_first_seen(table: pa.Table) -> pa.Table:
    """Enforce ``last_seen >= first_seen`` on a packed `JOURNAL_SCHEMA` table.

    followthemoney defaults *forward* – ``Statement.__init__`` does
    ``last_seen or first_seen`` – but never backward, so a statement carrying
    only a past ``last_seen`` gets ``first_seen`` filled with ``now`` and lands
    inverted. Garbled input (both supplied, out of order) does the same.

    ``first_seen`` is what moves. Raising ``last_seen`` instead would let a row
    with a bogus ``first_seen`` win
    [`merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge]'s
    ``ROW_NUMBER`` window against legitimate newer rows – it is the ranking
    column – and would overwrite the source's own ``last_seen`` with the wall
    clock in the common case. Lowering ``first_seen`` touches no ranking and
    matches the ``MIN(first_seen)`` fold `merge` already applies across a row's
    re-emissions.

    Applied by both packers – [`statements_to_arrow`][statements_to_arrow] and
    the unsafe `RowBuffer.flush` – which between them are every producer of a
    `JOURNAL_SCHEMA` table; the journal drain and the api bulk route re-cast
    rows that were clamped on the way in.

    Args:
        table: A packed table. Every producer fills both timestamps before
            calling this, so no null guard is needed – ``pa.Table.from_pylist``
            does *not* enforce the schema's ``NOT NULL``, and a hand-built
            table carrying nulls propagates them rather than having a value
            invented for it.

    Returns:
        The table with ``first_seen`` clamped to at most ``last_seen``.
    """
    first_seen = table.column("first_seen")
    last_seen = table.column("last_seen")
    return table.set_column(
        JOURNAL_SCHEMA.get_field_index("first_seen"),
        # the field, not the name: `set_column` with a string would build a
        # nullable field and the result would no longer equal JOURNAL_SCHEMA
        JOURNAL_SCHEMA.field("first_seen"),
        pc.if_else(pc.less(first_seen, last_seen), first_seen, last_seen),
    )


def statements_to_arrow(
    statements: Iterable[LakehouseStatement], now: datetime
) -> pa.Table:
    """Pack a stream of statements into a `JOURNAL_SCHEMA` table.

    ftmq's `statements_to_table` packs the statement
    columns columnwise; this adds the two columns the lakehouse stores on top
    (``role`` and ``deleted_at``), drops ``canonical_id`` (this store never
    resolves entities), and applies the two rules both write paths share:

    - ``first_seen`` / ``last_seen`` fall back to ``now`` when the statement
      carries none,
    - tombstones (``deleted_at`` set) bump ``last_seen`` to the *later* of the
      delete timestamp and the row they shadow, so they win the ``ROW_NUMBER()
      OVER (... ORDER BY last_seen DESC, deleted_at DESC NULLS LAST)`` tiebreak
      in [`ParquetStore.merge`][ftm_lakehouse.storage.parquet.ParquetStore.merge].
      Taking the delete timestamp alone would lose to a row dated in the future
      – input carries ``last_seen``, so nothing bounds it by the wall clock –
      and ``merge`` would drop the tombstone rather than the row, leaving the
      entity undeletable on every retry. On the tie this leaves, the
      ``deleted_at`` tiebreak decides, which is what it is there for.
    - ``first_seen`` is clamped to `clamp_first_seen` – *after* the tombstone
      rule, so it settles against the final ``last_seen``.

    All three rules are vectorized fills over the packed columns rather than
    per-row branches, and every column swap below is zero-copy. The closing cast is what
    makes the result align with `JOURNAL_SCHEMA` – including its ``NOT
    NULL`` columns, so a statement missing one is rejected here rather than by a
    reader later.

    Args:
        statements: Statements, typically a whole drained
            `EntityBuffer.flush_buffer`.
        now: Default timestamp for missing ``first_seen`` / ``last_seen``.

    Returns:
        A table with exactly `JOURNAL_SCHEMA`.
    """
    statements = list(statements)  # needs materialization before
    table = statements_to_table(statements)
    stamp = pa.scalar(now, PA_TS)
    deleted_at = pa.array([s.deleted_at for s in statements], PA_TS)
    role = pa.array([s.role for s in statements], pa.string())
    first_seen = pc.fill_null(table.column("first_seen"), stamp)
    seen = pc.fill_null(table.column("last_seen"), stamp)
    # element-wise MAX(deleted_at, last_seen): `max_element_wise` has no
    # timestamp kernel, and a bare `greater` would go null on the non-tombstone
    # rows, so the delete stamp is coalesced onto `seen` before the comparison
    deleted_or_seen = pc.coalesce(deleted_at, seen)
    last_seen = pc.if_else(pc.greater(deleted_or_seen, seen), deleted_or_seen, seen)
    return clamp_first_seen(
        table.set_column(
            table.schema.get_field_index("first_seen"), "first_seen", first_seen
        )
        .set_column(table.schema.get_field_index("last_seen"), "last_seen", last_seen)
        .append_column("role", role)
        .append_column("deleted_at", deleted_at)
        .select(JOURNAL_SCHEMA.names)
        .cast(JOURNAL_SCHEMA)
    )


def read_csv_statements(uri: Uri) -> LakehouseStatements:
    """Stream a lakehouse ``statements.csv`` as `LakehouseStatement` objects.

    followthemoney's ``read_csv_statements`` yields plain ``Statement`` objects
    and has no notion of the ``fragment`` supersession key or of ``role``, so
    the lakehouse needs its own reader. Rows are streamed as dicts via
    `anystore.io.read.smart_stream_csv` and mapped straight to
    `LakehouseStatement`; ``fragment`` is read from its column when present
    and falls back to the empty-string (non-fragment) sentinel otherwise,
    ``role`` to ``None``.

    It lives here rather than in ``helpers/`` because it is the read side of
    `STATEMENT_CSV_COLUMNS` and has to build the lakehouse's own statement
    class – which ``helpers/`` may not import.

    Args:
        uri: Location of the statements CSV.

    Yields:
        `LakehouseStatement` – ``canonical_id`` is left unset (FtM defaults it
        to ``entity_id``; this store does no resolution).
    """
    for row in smart_stream_csv(uri):
        yield LakehouseStatement(
            id=row.get("id") or None,
            entity_id=row["entity_id"],
            prop=row["prop"],
            schema=row["schema"],
            value=row.get("value") or "",
            dataset=row["dataset"],
            lang=row.get("lang") or None,
            original_value=row.get("original_value") or None,
            external=str(row.get("external", "")).strip().lower() in ("true", "1"),
            first_seen=row.get("first_seen") or None,
            last_seen=row.get("last_seen") or None,
            origin=row.get("origin") or None,
            fragment=row.get("fragment") or "",
            role=row.get("role") or None,
        )
