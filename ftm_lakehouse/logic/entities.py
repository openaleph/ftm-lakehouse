"""Entity aggregation and assembly logic.

This module provides functions for processing and assembling FollowTheMoney
entities from statement streams.
"""

from followthemoney import Statement, StatementEntity
from ftmq.types import StatementEntities, Statements
from ftmq.util import make_dataset


def aggregate_statements(stmts: Statements, dataset: str) -> StatementEntities:
    """
    Aggregate sorted statements into entities.

    Takes a stream of statements sorted by canonical_id and yields
    StatementEntity objects by grouping consecutive statements with
    the same canonical_id.

    This function is the core entity assembly logic used when exporting
    entities from the statement store. It expects statements to be pre-sorted
    by canonical_id for correct grouping.

    Args:
        stmts: Iterable of statements, must be sorted by canonical_id
        dataset: Dataset name for the resulting entities

    Yields:
        StatementEntity for each unique canonical_id

    Example:
        ```python
        from ftm_lakehouse.logic import aggregate_statements
        from followthemoney.statement.serialize import read_csv_statements

        # Read sorted statements from CSV
        with open("statements.csv") as f:
            statements = read_csv_statements(f)
            for entity in aggregate_statements(statements, "my_dataset"):
                print(f"{entity.id}: {entity.caption}")
        ```
    """
    ds = make_dataset(dataset)
    statements: list[Statement] = []
    for s in stmts:
        if len(statements) and statements[0].canonical_id != s.canonical_id:
            yield StatementEntity.from_statements(ds, statements)
            statements = []
        statements.append(s)
    if len(statements):
        yield StatementEntity.from_statements(ds, statements)
