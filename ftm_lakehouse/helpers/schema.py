"""FollowTheMoney schema introspection helpers."""

from followthemoney import Schema, model
from followthemoney.exc import InvalidData
from ftmq.aggregate import common_ancestor
from ftmq.query.refs import PropRef

CAPTION_PROPS: frozenset[PropRef] = frozenset(
    PropRef(prop) for schema in model.schemata.values() for prop in schema.caption
)
"""Every property any schema uses as its caption, as `select`-able refs.

Spread this into a projection to keep captions intact while still projecting:

    Query(*Q_DOCUMENTS).select(P("contentHash"), *CAPTION_PROPS)
"""


def merge_schema(s1: str | Schema, s2: str | Schema) -> Schema:
    """Lenient merge: Find common ancestors if schemata can't merge"""
    _s1 = model.get(s1)
    _s2 = model.get(s2)
    if _s1 is None or _s2 is None:
        raise RuntimeError("Invalid schema, can't merge")
    try:
        return model.common_schema(s1, s2)
    except InvalidData:
        return common_ancestor(_s1, _s2)
