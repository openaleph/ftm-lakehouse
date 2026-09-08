"""FollowTheMoney schema introspection helpers."""

from followthemoney import model
from ftmq.query.refs import PropRef

CAPTION_PROPS: frozenset[PropRef] = frozenset(
    PropRef(prop) for schema in model.schemata.values() for prop in schema.caption
)
"""Every property any schema uses as its caption, as `select`-able refs.

Spread this into a projection to keep captions intact while still projecting:

    Query(*Q_DOCUMENTS).select(P("contentHash"), *CAPTION_PROPS)
"""
