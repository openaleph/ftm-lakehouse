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

    Query(M(schemata="Document")).select(P("contentHash"), *CAPTION_PROPS)
"""

FOLDER_SCHEMATA: frozenset[str] = frozenset(
    s.name for s in model.schemata.values() if s.is_a("Folder")
)
"""Every schema a document's ``parent`` can point at – ``Folder`` and the
schemata extending it (``Package``, ``Workbook``, ``Email``, ``Message``) – as
plain names, so a stream of entities can be classified with one set lookup.
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
