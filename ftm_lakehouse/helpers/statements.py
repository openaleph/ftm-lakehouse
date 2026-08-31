"""Statement serialization logic."""

from hashlib import sha1
from typing import Iterable

from followthemoney import Statement
from followthemoney.statement.util import BASE_ID
from followthemoney.util import HASH_ENCODING


def dedupe_key(id: str, origin: str, fragment: str, role: str | None) -> str:
    """Row identity of a stored statement: ``id``, ``origin``, ``fragment``,
    ``role``.

    The same tab-joined key
    `ftm_lakehouse.model.statement.LakehouseStatement.dedupe_key` builds from
    a statement object, for the packed-row paths that never construct one
    (`RowBuffer`). Both write
    buffers collapse re-emissions on this key, matching the store's
    per-origin row identity – the same id under distinct fragments,
    origins *or* roles stays a distinct row.
    """
    return f"{id}\t{origin}\t{fragment}\t{role or ''}"


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
