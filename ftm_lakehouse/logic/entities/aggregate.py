"""Entity aggregation and assembly logic.

This module provides functions for processing and assembling FollowTheMoney
entities from statement streams.
"""

from collections import defaultdict
from datetime import datetime
from typing import Any, Iterator, TypedDict

from followthemoney import Schema, Statement, StatementEntity, model
from followthemoney.exc import InvalidData
from followthemoney.statement import StatementDict
from followthemoney.statement.util import BASE_ID
from ftmq.aggregate import common_ancestor
from ftmq.util import DEFAULT_DATASET, make_dataset


def _merge_schema(s1: str | Schema, s2: str | Schema) -> Schema:
    """Lenient merge: Find common ancestors if schemata can't merge"""
    _s1 = model.get(s1)
    _s2 = model.get(s2)
    if _s1 is None or _s2 is None:
        raise RuntimeError("Invalid schema, can't merge")
    try:
        return model.common_schema(s1, s2)
    except InvalidData:
        return common_ancestor(_s1, _s2)


def _ts_str(value: str | datetime | None) -> str | None:
    """Normalize a timestamp to ISO string, handling both str and datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return value


class EntityData(TypedDict):
    """All data needed to compile a proper EntityDict"""

    schemata: set[str]
    datasets: set[str]
    referents: set[str]
    origins: set[str]
    first_seens: set[str]
    last_seens: set[str]
    last_changes: set[str]
    properties: defaultdict


class EntityPayload:
    """Lightweight entity accumulator that works on raw statement dicts.

    Mirrors StatementEntity.from_statements() + to_dict() behavior but
    bypasses all FtM object construction for speed.
    """

    __slots__ = (
        "id",
        "dataset",
        "statements",
    )

    def __init__(self, id: str | None = None, dataset: str | None = None) -> None:
        self.id = id
        self.statements: list[StatementDict] = []
        self.dataset = make_dataset(dataset or DEFAULT_DATASET)

    def add(self, s: StatementDict) -> None:
        self.statements.append(s)

    def _build(self) -> EntityData:
        data = EntityData(
            schemata=set(),
            datasets=set(),
            referents=set(),
            origins=set(),
            first_seens=set(),
            last_seens=set(),
            last_changes=set(),
            properties=defaultdict(set),
        )

        # collect statements
        for s in self.statements:
            data["schemata"].add(s["schema"])
            data["datasets"].add(s["dataset"])

            origin = s.get("origin")
            if origin:
                data["origins"].add(origin)

            entity_id = s.get("entity_id")
            if entity_id and entity_id != self.id:
                data["referents"].add(entity_id)

            first_seen = _ts_str(s.get("first_seen"))
            last_seen = _ts_str(s.get("last_seen"))

            if s["prop"] == BASE_ID:
                # last_change = max of BASE_ID statement first_seen values
                if first_seen is not None:
                    data["last_changes"].add(first_seen)
            else:
                data["properties"][s["prop"]].add(s["value"])
                # first_seen/last_seen only from non-id statements
                # (matches StatementEntity.to_context_dict which iterates _statements,
                # which excludes BASE_ID)
                if first_seen is not None:
                    data["first_seens"].add(first_seen)
                if last_seen is not None:
                    data["last_seens"].add(last_seen)

        return data

    def to_dict(self) -> dict[str, Any]:
        compiled = self._build()
        # Schema merging – pick the most specific schema
        schema = None
        for name in compiled["schemata"]:
            if schema is None:
                schema = model.get(name)
            elif schema.name != name:
                schema = _merge_schema(schema, name)

        if schema is None:
            return {}

        # Caption – first caption property with values, or schema label
        # (simplified: no pick_lang_name language detection)
        caption = schema.label
        for prop_name in schema.caption:
            values = compiled["properties"].get(prop_name)
            if values:
                caption = next(iter(sorted(values)))
                break

        data: dict[str, Any] = {
            "id": self.id,
            "caption": caption,
            "schema": schema.name,
            "properties": {k: list(v) for k, v in compiled["properties"].items()},
            "referents": list(compiled["referents"]),
            "datasets": list(compiled["datasets"]),
        }

        if compiled["origins"]:
            data["origin"] = list(compiled["origins"])
        if compiled["first_seens"]:
            data["first_seen"] = min(compiled["first_seens"])
        if compiled["last_seens"]:
            data["last_seen"] = max(compiled["last_seens"])
        if compiled["last_changes"]:
            data["last_change"] = max(compiled["last_changes"])

        return data

    def to_entity(self) -> StatementEntity:
        statements = [Statement.from_dict(s) for s in self.statements]
        return StatementEntity.from_statements(self.dataset, statements)


def aggregate_unsafe(
    data: Iterator[StatementDict], dataset: str | None = None
) -> Iterator[EntityPayload]:
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
                yield current
            current = EntityPayload(id=statement["canonical_id"], dataset=dataset)
        current.add(statement)
    if current is not None:
        yield current
