"""Entity aggregation and assembly logic.

This module provides functions for processing and assembling FollowTheMoney
entities from statement streams.
"""

from collections import defaultdict
from datetime import datetime
from typing import Any, Generator, Iterator

from followthemoney import Model, Statement, StatementEntity
from followthemoney.statement import StatementDict
from followthemoney.statement.util import BASE_ID
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


def _ts_str(value: str | datetime | None) -> str | None:
    """Normalize a timestamp to ISO string, handling both str and datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class EntityPayload:
    """Lightweight entity accumulator that works on raw statement dicts.

    Mirrors StatementEntity.from_statements() + to_dict() behavior but
    bypasses all FtM object construction for speed.
    """

    __slots__ = (
        "id",
        "_schemata",
        "_origins",
        "_datasets",
        "_referents",
        "_first_seens",
        "_last_seens",
        "_last_change_candidates",
        "_properties",
    )

    def __init__(self, id: str | None = None) -> None:
        self.id = id
        self._schemata: set[str] = set()
        self._origins: set[str] = set()
        self._datasets: set[str] = set()
        self._referents: set[str] = set()
        self._first_seens: list[str] = []
        self._last_seens: list[str] = []
        self._last_change_candidates: list[str] = []
        self._properties: dict[str, set[str]] = defaultdict(set)

    def add(self, s: StatementDict) -> None:
        self._schemata.add(s["schema"])
        self._datasets.add(s["dataset"])

        origin = s.get("origin")
        if origin:
            self._origins.add(origin)

        entity_id = s.get("entity_id")
        if entity_id and entity_id != self.id:
            self._referents.add(entity_id)

        first_seen = _ts_str(s.get("first_seen"))
        last_seen = _ts_str(s.get("last_seen"))

        if s["prop"] == BASE_ID:
            # last_change = max of BASE_ID statement first_seen values
            if first_seen is not None:
                self._last_change_candidates.append(first_seen)
        else:
            self._properties[s["prop"]].add(s["value"])
            # first_seen/last_seen only from non-id statements
            # (matches StatementEntity.to_context_dict which iterates _statements,
            # which excludes BASE_ID)
            if first_seen is not None:
                self._first_seens.append(first_seen)
            if last_seen is not None:
                self._last_seens.append(last_seen)

    def to_dict(self) -> dict[str, Any]:
        model = Model.instance()

        # Schema merging — pick the most specific schema
        schema = None
        for name in self._schemata:
            if schema is None:
                schema = model.get(name)
            elif schema.name != name:
                schema = model.common_schema(schema, name)

        if schema is None:
            return {}

        # Caption — first caption property with values, or schema label
        # (simplified: no pick_lang_name language detection)
        caption = schema.label
        for prop_name in schema.caption:
            values = self._properties.get(prop_name)
            if values:
                caption = next(iter(sorted(values)))
                break

        data: dict[str, Any] = {
            "id": self.id,
            "caption": caption,
            "schema": schema.name,
            "properties": {k: list(v) for k, v in self._properties.items()},
            "referents": list(self._referents),
            "datasets": list(self._datasets),
        }

        if self._origins:
            data["origin"] = list(self._origins)
        if self._first_seens:
            data["first_seen"] = min(self._first_seens)
        if self._last_seens:
            data["last_seen"] = max(self._last_seens)
        if self._last_change_candidates:
            data["last_change"] = max(self._last_change_candidates)

        return data


def aggregate_unsafe(
    data: Iterator[StatementDict],
) -> Generator[dict[str, Any], None, None]:
    """
    Aggregate statement dicts (e.g. from DuckDB rows) to entity payloads.

    Completely circumvents the dict -> Statement -> StatementEntity -> dict
    Python path, but therefore has no validation checks. Input must be sorted
    by canonical_id.
    """
    current: EntityPayload | None = None
    for statement in data:
        if current is None or statement["canonical_id"] != current.id:
            if current is not None:
                yield current.to_dict()
            current = EntityPayload(id=statement["canonical_id"])
        current.add(statement)
    if current is not None:
        yield current.to_dict()
