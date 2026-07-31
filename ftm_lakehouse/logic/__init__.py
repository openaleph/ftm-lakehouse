"""Pure business logic layer.

This module contains stateless transformation functions with no infrastructure
dependencies. Functions here take inputs and produce outputs without side effects.

Modules:
    entities: Statement aggregation and entity assembly
    statements: Statement serialization (pack/unpack)
"""

from ftm_lakehouse.helpers.statements import pack_statement, unpack_statement
from ftm_lakehouse.logic.entities import aggregate_unsafe

__all__ = [
    "aggregate_unsafe",
    "pack_statement",
    "unpack_statement",
]
