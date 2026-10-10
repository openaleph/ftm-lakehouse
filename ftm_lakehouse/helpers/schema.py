"""FollowTheMoney schema introspection helpers."""

from followthemoney import model
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
