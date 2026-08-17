"""Statement serialization logic."""

from hashlib import sha1
from typing import Generator, Iterable

from anystore.io.read import smart_stream_csv
from anystore.types import Uri
from followthemoney import Statement
from followthemoney.statement.util import BASE_ID
from followthemoney.util import HASH_ENCODING
from ftmq.store.base import DEFAULT_ORIGIN
from ftmq.store.lake import LakeStatement
from ftmq.util import datetime_iso

from ftm_lakehouse.exceptions import MalformedStatementError

UNIT_SEP = "\x1f"
"""Field separator used to pack a Statement into the journal ``data`` column."""

UNPACK_MIN_FIELDS = 12
"""Minimum field count :func:`unpack_journal_row` requires.

:func:`pack_journal_row` currently emits 13 fields (trailing ``prop_type``);
``unpack_journal_row`` only reads the first 12, so extra trailing fields
are tolerated for forward compatibility – but anything shorter is a
malformed row and rejected.
"""


def pack_journal_row(stmt: Statement) -> str:
    """
    Pack a Statement into the journal's unit-separator delimited ``data`` string.

    Not to be confused with :func:`ftmq.store.lake.pack_statement`, which
    packs a statement into a parquet row dict.

    Format: id, entity_id, prop, schema, value, dataset, lang,
            original_value, external, first_seen, last_seen, origin, prop_type

    ``canonical_id`` is not serialised – this store never resolves entities,
    so :func:`unpack_journal_row` lets FtM default it to ``entity_id``.
    """
    parts = [
        stmt.id or "",
        stmt.entity_id,
        stmt.prop,
        stmt.schema,
        stmt.value,
        stmt.dataset,
        stmt.lang or "",
        stmt.original_value or "",
        "1" if stmt.external else "0",
        # Missing timestamps are stamped at pack time – journal rows always
        # carry concrete seen-timestamps.
        datetime_iso(stmt.first_seen, default_now=True) or "",
        datetime_iso(stmt.last_seen, default_now=True) or "",
        stmt.origin or DEFAULT_ORIGIN,
        stmt.prop_type or "",
    ]
    return UNIT_SEP.join(parts)


def unpack_journal_row(data: str) -> Statement:
    """Unpack a journal ``data`` string back into a Statement.

    Raises:
        MalformedStatementError: If ``data`` has fewer than
            :data:`UNPACK_MIN_FIELDS` separator-delimited fields. The
            journal flush loop catches this and logs+skips the row so
            one bad row can't abort an entire flush.
    """
    parts = data.split(UNIT_SEP)
    if len(parts) < UNPACK_MIN_FIELDS:
        raise MalformedStatementError(
            f"Packed statement has {len(parts)} fields; "
            f"expected at least {UNPACK_MIN_FIELDS}"
        )
    return Statement(
        id=parts[0] or None,
        entity_id=parts[1],  # required
        prop=parts[2],  # required
        schema=parts[3],  # required
        value=parts[4],  # required
        dataset=parts[5],  # required
        lang=parts[6] or None,
        original_value=parts[7] or None,
        external=parts[8] == "1",
        first_seen=parts[9] or None,
        last_seen=parts[10] or None,
        origin=parts[11] or None,
    )


def make_base_id_statement(
    dataset: str,
    entity_id: str,
    schema: str,
    statement_ids: Iterable[str],
    first_seen: str | None = None,
    last_seen: str | None = None,
) -> Statement:
    """Synthesize the ``BASE_ID`` checksum stub statement for an entity.

    Mirrors FtM's ``StatementEntity.statements`` checksum formula –
    ``sha1(schema)`` folded over the sorted statement ids – but over ids the
    caller supplies: the lakehouse feeds the *re-keyed* ids (content-hashed
    under the target dataset), so the checksum – and with it the stub's own
    content-addressed statement id – is stable across payload dataset
    contexts and round-trips. Both import paths
    (:meth:`EntityBuffer.add_entity` and the unsafe explode) build their
    stub through this one helper so the formula cannot drift between them.

    Args:
        dataset: Target dataset – hashed into the stub's statement id.
        entity_id: The entity the stub belongs to.
        schema: FtM schema name, the digest seed.
        statement_ids: The entity's (re-keyed) property statement ids.
        first_seen: Optional ``first_seen`` for the stub.
        last_seen: Optional ``last_seen`` for the stub.

    Returns:
        The synthesized ``Statement`` with ``prop=BASE_ID`` and the
        checksum as value.
    """
    digest = sha1(schema.encode(HASH_ENCODING))
    for stmt_id in sorted(set(statement_ids)):
        digest.update(stmt_id.encode(HASH_ENCODING))
    return Statement(
        entity_id=entity_id,
        prop=BASE_ID,
        schema=schema,
        value=digest.hexdigest(),
        dataset=dataset,
        first_seen=first_seen,
        last_seen=last_seen,
    )


def read_csv_statements(uri: Uri) -> Generator[LakeStatement, None, None]:
    """Stream a lakehouse ``statements.csv`` as :class:`LakeStatement` objects.

    followthemoney's ``read_csv_statements`` yields plain ``Statement`` objects
    and has no notion of the ``fragment`` supersession key, so the lakehouse
    needs its own reader. Rows are streamed as dicts via
    :func:`anystore.io.read.smart_stream_csv` and mapped straight to
    ``LakeStatement``; ``fragment`` is read from its column when present and
    falls back to the empty-string (non-fragment) sentinel otherwise.

    Args:
        uri: Location of the statements CSV.

    Yields:
        ``LakeStatement`` – ``canonical_id`` is left unset (FtM defaults it to
        ``entity_id``; this store does no resolution).
    """
    for row in smart_stream_csv(uri):
        yield LakeStatement(
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
        )
