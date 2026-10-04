"""Entity aggregation and assembly logic.

This module provides functions for processing and assembling FollowTheMoney
entities from statement streams.
"""

from collections import defaultdict
from typing import Any, Iterator, TypedDict

from followthemoney import Statement, StatementEntity, model
from followthemoney.statement import StatementDict
from followthemoney.statement.util import BASE_ID
from ftmq.util import DEFAULT_DATASET, datetime_iso, make_dataset

from ftm_lakehouse.helpers.schema import merge_schema


class EntityData(TypedDict):
    """All data needed to compile a proper EntityDict"""

    schemata: set[str]
    datasets: set[str]
    referents: set[str]
    origins: set[str]
    roles: set[str]
    first_seens: set[str]
    last_seens: set[str]
    last_changes: set[str]
    properties: defaultdict
    min_first_seen: str | None
    max_first_seen: str | None


class EntityPayload:
    """Lightweight entity accumulator that works on raw statement dicts.

    Mirrors StatementEntity.from_statements() + to_dict() behavior but
    bypasses all FtM object construction for speed.
    """

    __slots__ = (
        "id",
        "dataset",
        "statements",
        "_data",
        "_dict",
    )

    def __init__(self, id: str | None = None, dataset: str | None = None) -> None:
        self.id = id
        self.statements: list[StatementDict] = []
        self.dataset = make_dataset(dataset or DEFAULT_DATASET)
        self._data: EntityData | None = None
        self._dict: dict[str, Any] | None = None

    def add(self, s: StatementDict) -> None:
        self.statements.append(s)

    @property
    def compiled(self) -> EntityData:
        """The folded statement data, built once per payload."""
        if self._data is None:
            self._data = self._build()
        return self._data

    @property
    def origins(self) -> set[str]:
        """Every source tag that asserts something about this entity."""
        return self.compiled["origins"]

    @property
    def min_first_seen(self) -> str | None:
        """Earliest ``first_seen`` across **all** statements, ``id`` rows included.

        The diff's ADD/MOD discriminator: when this is at or after a diff's
        ``since``, every statement the entity has is new, so the entity itself
        is new. Deliberately not the ``first_seen`` of
        [`to_dict`][EntityPayload.to_dict], which folds non-``id`` statements
        only – an entity that existed as a bare id and just gained its first
        properties would read as brand new there.
        """
        return self.compiled["min_first_seen"]

    @property
    def max_first_seen(self) -> str | None:
        """Latest ``first_seen`` across **all** statements, ``id`` rows included.

        The diff's change predicate: when this is at or after a diff's
        ``since``, the entity gained at least one statement in the window.
        """
        return self.compiled["max_first_seen"]

    def _build(self) -> EntityData:
        data = EntityData(
            schemata=set(),
            datasets=set(),
            referents=set(),
            origins=set(),
            roles=set(),
            first_seens=set(),
            last_seens=set(),
            last_changes=set(),
            properties=defaultdict(set),
            min_first_seen=None,
            max_first_seen=None,
        )

        # speed up when using locals here
        schemata = data["schemata"]
        datasets = data["datasets"]
        origins = data["origins"]
        roles = data["roles"]
        referents = data["referents"]
        properties = data["properties"]
        first_seens = data["first_seens"]
        last_seens = data["last_seens"]
        last_changes = data["last_changes"]
        min_first_seen: str | None = None
        max_first_seen: str | None = None
        entity = self.id

        # collect statements
        for s in self.statements:
            schemata.add(s["schema"])
            datasets.add(s["dataset"])

            origin = s.get("origin")
            if origin:
                origins.add(origin)

            role = s.get("role")
            if role:
                roles.add(role)

            entity_id = s.get("entity_id")
            if entity_id and entity_id != entity:
                referents.add(entity_id)

            first_seen = datetime_iso(s.get("first_seen"))
            last_seen = datetime_iso(s.get("last_seen"))

            # the diff bounds span every statement, `id` rows included – a
            # bare-id entity that just gained properties predates the window
            if first_seen is not None:
                if min_first_seen is None or first_seen < min_first_seen:
                    min_first_seen = first_seen
                if max_first_seen is None or first_seen > max_first_seen:
                    max_first_seen = first_seen

            if s["prop"] == BASE_ID:
                # last_change = max of BASE_ID statement first_seen values
                if first_seen is not None:
                    last_changes.add(first_seen)
            else:
                properties[s["prop"]].add(s["value"])
                # first_seen/last_seen only from non-id statements
                # (matches StatementEntity.to_context_dict which excludes BASE_ID)
                if first_seen is not None:
                    first_seens.add(first_seen)
                if last_seen is not None:
                    last_seens.add(last_seen)

        data["min_first_seen"] = min_first_seen
        data["max_first_seen"] = max_first_seen
        return data

    def to_dict(self) -> dict[str, Any]:
        """The entity as an FtM-shaped dict, built once per payload."""
        if self._dict is None:
            self._dict = self._to_dict()
        return self._dict

    def _to_dict(self) -> dict[str, Any]:
        compiled = self.compiled

        # Schema merging – pick the most specific schema
        schema = None
        for name in compiled["schemata"]:
            if schema is None:
                schema = model.get(name)
            elif schema.name != name:
                schema = merge_schema(schema, name)

        if schema is None:
            return {}

        # Caption – first caption property with values, or schema label
        properties = compiled["properties"]
        caption = None
        for prop_name in schema.caption:
            for value in properties.get(prop_name, []):
                caption = value
                break
        if caption is None:
            caption = schema.label

        data: dict[str, Any] = {
            "id": self.id,
            "caption": caption,
            "schema": schema.name,
            "properties": {k: list(v) for k, v in properties.items()},
            "referents": list(compiled["referents"]),
            "datasets": list(compiled["datasets"]),
        }

        if compiled["origins"]:
            data["origin"] = list(compiled["origins"])
        if compiled["roles"]:
            data["role"] = list(compiled["roles"])
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
    by entity_id.
    """
    current: EntityPayload | None = None
    for statement in data:
        if current is None or statement["entity_id"] != current.id:
            if current is not None:
                yield current
            current = EntityPayload(id=statement["entity_id"], dataset=dataset)
        current.add(statement)
    if current is not None:
        yield current
