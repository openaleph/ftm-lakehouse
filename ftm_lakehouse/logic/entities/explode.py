"""Unsafe bulk-import explode logic – the write-side inverse of "unsafe
aggregation".

Turns aggregated FtM entity payload dicts (and raw ``statements.csv`` row
dicts) directly into packed parquet row dicts matching
:data:`~ftm_lakehouse.model.statement.SHARDED_SCHEMA`, bypassing the
EntityProxy → Namespace → StatementEntity → Statement → LakeStatement
object chain entirely. Like :func:`aggregate_unsafe
<ftm_lakehouse.logic.entities.aggregate.aggregate_unsafe>` on the read side,
this trades validation for speed – input is trusted.

Parity with the safe path is the contract: identical statement ids
(:meth:`Statement.make_key`), identical ``BASE_ID`` checksum rows, identical
namespace stripping and timestamp pinning – so the same payload imported
through either path collapses to the same physical rows on :meth:`merge`.
"""

from datetime import datetime
from operator import itemgetter
from typing import Iterator

from anystore.types import SDict
from banal import ensure_list
from followthemoney import Statement, model
from followthemoney.exc import InvalidData
from followthemoney.namespace import Namespace
from followthemoney.statement.util import NON_LANG_TYPE_NAMES, get_prop_type
from followthemoney.types import registry
from ftmq.store.lake import get_schema_bucket
from rigour.time import iso_datetime

from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.helpers.statements import make_base_id_statement
from ftm_lakehouse.util import single_string, validate_origin


def strip_namespace(value: str) -> str | None:
    """Plain entity id without a namespace signature, or ``None``"""
    return Namespace.strip(value)


def explode_unsafe(
    data: SDict,
    dataset: str,
    shards: int,
    *,
    now: datetime,
    origin: str,
    override_origin: bool = False,
    last_seen: datetime | None = None,
) -> Iterator[SDict]:
    """Explode one aggregated FtM entity payload into packed parquet rows.

    Emits one row per ``(prop, value)`` plus the trailing ``BASE_ID``
    checksum row, replicating what the safe path produces for the same
    payload: the entity id and every entity-type property value are
    namespace-stripped, statement ids come from :meth:`Statement.make_key`,
    unknown / stub properties are skipped silently and every row shares one
    pinned ``last_seen`` (the payload's ``last_change``, else ``last_seen``,
    else ``now`` at second granularity – multi-valued props must tie to survive
    fragment supersession together).

    Args:
        data: Aggregated FtM entity dict (``id`` / ``schema`` /
            ``properties`` plus context keys). Payloads with embedded
            ``statements`` are not supported – use the statement import.
        dataset: Target dataset name, stamped on (and hashed into) every row.
        shards: The dataset's shard count.
        now: Run-level timestamp for missing ``first_seen`` / ``last_seen``.
        origin: Default origin tag if the payload carries none.
        override_origin: Force ``origin`` over the payload's own.
        last_seen: Default pinned ``last_seen`` if the payload has no
            ``last_change``.

    Yields:
        Packed row dicts carrying every ``SHARDED_SCHEMA`` column.

    Raises:
        InvalidData: If the payload's schema is unknown.
        ValueError: If the resolved origin is not a safe origin name.
    """
    entity_id = data.get("id")
    schema_name = data.get("schema")
    if not entity_id or not schema_name:
        return
    schema = model.get(schema_name)
    if schema is None:
        raise InvalidData(f"No schema for entity: `{schema_name}`")
    entity_id = strip_namespace(entity_id)
    if not entity_id:
        return

    ctx_origin = None if override_origin else single_string(data.get("origin"))
    last_change = data.get("last_change")
    changed = iso_datetime(last_change) if isinstance(last_change, str) else None
    base = {
        "shard": entity_shard(entity_id, shards),
        "entity_id": entity_id,
        "dataset": dataset,
        "bucket": get_schema_bucket(schema.name),
        "origin": validate_origin(ctx_origin or origin),
        "source": None,
        "schema": schema.name,
        "original_value": None,
        "lang": None,
        "external": False,
        "first_seen": changed or now,
        "last_seen": changed or last_seen or now.replace(microsecond=0),
        "fragment": single_string(data.get("fragment")) or "",
        "deleted_at": None,
    }

    ids: set[str] = set()
    properties = data.get("properties") or {}
    for prop_name, values in properties.items():
        prop = schema.get(prop_name)
        if prop is None or prop.stub:
            continue
        prop_type = prop.type.name
        is_ref = prop.type == registry.entity
        for value in ensure_list(values):
            if not value or not isinstance(value, str):
                continue
            if is_ref:
                value = strip_namespace(value)
                if not value:
                    # the safe path's namespace.apply drops unclean refs too
                    continue
            stmt_id = Statement.make_key(dataset, entity_id, prop_name, value, False)
            if stmt_id is None or stmt_id in ids:
                continue
            ids.add(stmt_id)
            yield {
                **base,
                "id": stmt_id,
                "prop": prop_name,
                "prop_type": prop_type,
                "value": value,
            }

    stub = make_base_id_statement(dataset, entity_id, schema.name, ids)
    yield {
        **base,
        "id": stub.id,
        "prop": stub.prop,
        "prop_type": stub.prop_type,
        "value": stub.value,
    }


def statement_row_unsafe(
    row: SDict,
    dataset: str,
    shards: int,
    *,
    now: datetime,
    origin: str,
) -> SDict | None:
    """Map one ``statements.csv`` row dict to a packed parquet row.

    Mirrors the safe statement import field by field: ``prop_type`` is
    recomputed from the model (the CSV column is ignored, as
    ``Statement.__init__`` does), ``lang`` is nulled for non-linguistic
    types, and the ``id`` is always re-derived – content-hashed under the
    *target* dataset via :meth:`Statement.make_key`, a carried-over id is
    ignored – so identical content collapses on merge across imports and
    round-trips. Entity ids are **not** namespace-stripped – parity with
    the safe statement path.

    Args:
        row: CSV row dict (string values).
        dataset: Target dataset name.
        shards: The dataset's shard count.
        now: Run-level timestamp for missing ``first_seen`` / ``last_seen``.
        origin: Default origin tag if the row carries none.

    Returns:
        A packed row dict, or ``None`` for rows without entity id, schema
        or prop.

    Raises:
        TypeError: If the row's schema / prop combination is unknown.
        ValueError: If the resolved origin is not a safe origin name.
    """
    entity_id = row.get("entity_id")
    schema = row.get("schema")
    prop = row.get("prop")
    if not entity_id or not schema or not prop:
        return None
    value = row.get("value") or ""
    prop_type = get_prop_type(schema, prop)
    lang = row.get("lang") or None
    if prop_type in NON_LANG_TYPE_NAMES:
        lang = None
    external = str(row.get("external", "")).strip().lower() in ("true", "1")
    stmt_id = Statement.make_key(dataset, entity_id, prop, value, external, lang=lang)
    if stmt_id is None:
        return None
    first_seen = iso_datetime(row.get("first_seen") or None) or now
    return {
        "shard": entity_shard(entity_id, shards),
        "id": stmt_id,
        "entity_id": entity_id,
        "dataset": dataset,
        "bucket": get_schema_bucket(schema),
        "origin": validate_origin(row.get("origin") or origin),
        "source": None,
        "schema": schema,
        "prop": prop,
        "prop_type": prop_type,
        "value": value,
        "original_value": row.get("original_value") or None,
        "lang": lang,
        "external": external,
        "first_seen": first_seen,
        "last_seen": iso_datetime(row.get("last_seen") or None) or first_seen,
        "fragment": row.get("fragment") or "",
        "deleted_at": None,
    }


class RowBuffer:
    """``(id, fragment)``-keyed packed-row buffer, flushed sorted by shard.

    The unsafe twin of :class:`~ftm_lakehouse.logic.entities.buffer.
    EntityBuffer`: deduplicates re-emissions within one batch by
    ``(id, fragment)`` – the same id under distinct fragments stays
    distinct – and yields rows shard-sorted so
    :meth:`EntityRepository.write_rows` can accumulate per-shard parquet
    batches with bounded memory.
    """

    def __init__(self) -> None:
        self._rows: dict[tuple[str, str], SDict] = {}

    def add(self, row: SDict | None) -> None:
        """Buffer a packed row; ``None`` (a skipped input row) is ignored."""
        if row is None:
            return
        self._rows[(row["id"], row["fragment"])] = row

    def flush(self) -> Iterator[SDict]:
        """Yield buffered rows sorted by shard, then clear the buffer."""
        for row in sorted(self._rows.values(), key=itemgetter("shard")):
            yield row
        self._rows = {}

    def __len__(self) -> int:
        return len(self._rows)

    def __bool__(self) -> bool:
        return bool(self._rows)
