"""Statement serialization logic."""

from hashlib import sha1
from typing import Generator, Iterable

from anystore.io.read import smart_stream_csv
from anystore.types import Uri
from followthemoney import Statement
from followthemoney.statement.util import BASE_ID
from followthemoney.util import HASH_ENCODING
from ftmq.store.lake import LakeStatement


def dedupe_key(id: str, origin: str, fragment: str) -> str:
    """Row identity of a stored statement: ``id``, ``origin``, ``fragment``.

    The same tab-joined key
    `ftmq.store.lake.LakeStatement.dedupe_key` builds from a statement
    object, for the packed-row paths that never construct one
    (`RowBuffer`). Both write
    buffers collapse re-emissions on this key, matching the store's
    per-origin row identity – the same id under distinct fragments *or*
    origins stays a distinct row.
    """
    return f"{id}\t{origin}\t{fragment}"


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
    (`EntityBuffer.add_entity` and the unsafe explode) build their
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
    """Stream a lakehouse ``statements.csv`` as `LakeStatement` objects.

    followthemoney's ``read_csv_statements`` yields plain ``Statement`` objects
    and has no notion of the ``fragment`` supersession key, so the lakehouse
    needs its own reader. Rows are streamed as dicts via
    `anystore.io.read.smart_stream_csv` and mapped straight to
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
