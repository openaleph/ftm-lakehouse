"""Entity aggregation and assembly logic.

This module provides functions for processing and assembling FollowTheMoney
entities from statement streams.
"""

from collections import defaultdict
from typing import Any, Iterable, Iterator, Self, TypedDict, cast

import pyarrow as pa  # type: ignore[import-untyped]  # no py.typed marker
from followthemoney import Statement, StatementEntity, model
from followthemoney.statement import StatementDict
from followthemoney.statement.util import BASE_ID
from ftmq.util import DEFAULT_DATASET, datetime_iso, make_dataset

from ftm_lakehouse.helpers.schema import merge_schema
from ftm_lakehouse.model.statement import SEEN_COLUMNS

FOLD_COLUMNS = (
    "entity_id",
    "prop",
    "value",
    "schema",
    "dataset",
    "origin",
    "role",
    "first_seen",
    "last_seen",
)
"""The columns [`aggregate_batches`][aggregate_batches] reads out of a sweep
batch – the nine of ``STATEMENT_CSV_COLUMNS`` an entity is folded from. The
rest (``id``, ``canonical_id``, ``prop_type``, ``original_value``, ``lang``,
``external``, ``fragment``) belong to ``statements.csv``, and a columnar fold
never touches them."""


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
        "count",
        "_data",
        "_dict",
    )

    def __init__(self, id: str | None = None, dataset: str | None = None) -> None:
        self.id = id
        self.statements: list[StatementDict] = []
        self.count = 0
        """How many statements folded into this entity.

        ``len(statements)`` where they are kept (`aggregate_unsafe`), and the
        only record of them where they are not (`aggregate_batches`) – so a
        consumer counting statements asks for this, not for the list."""
        self.dataset = make_dataset(dataset or DEFAULT_DATASET)
        self._data: EntityData | None = None
        self._dict: dict[str, Any] | None = None

    @classmethod
    def folded(
        cls, id: str, data: "EntityData", count: int, dataset: str | None = None
    ) -> Self:
        """A payload whose fold is already done – see `aggregate_batches`.

        Carries no statements: the columnar fold reads the columns it needs
        straight out of the Arrow batch and never materialises a row.

        Args:
            id: The entity id.
            data: The folded statement data.
            count: How many statements went into it.
            dataset: Dataset the entity belongs to.

        Returns:
            The payload, ready to be asked for its
            [`to_dict`][EntityPayload.to_dict].
        """
        payload = cls(id=id, dataset=dataset)
        payload._data = data
        payload.count = count
        return payload

    def add(self, s: StatementDict) -> None:
        self.statements.append(s)
        self.count += 1

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
        """The FtM entity, built from the statements this payload kept.

        Only a payload from `aggregate_unsafe` has them – a folded one
        (`folded`) answers `to_dict` and nothing that needs the rows back.

        Raises:
            ValueError: on a folded payload, which keeps no statements.
        """
        if not self.statements:
            raise ValueError(f"Payload `{self.id}` carries no statements")
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

    Keeps every statement, so the payloads answer
    [`to_entity`][EntityPayload.to_entity]. The export sweep folds the same
    rows out of Arrow instead (`aggregate_batches`), which it can because it
    only ever asks for [`to_dict`][EntityPayload.to_dict].
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


def aggregate_batches(
    batches: Iterable[pa.RecordBatch], dataset: str | None = None
) -> Iterator[EntityPayload]:
    """Fold Arrow batches of statements into entity payloads, column-wise.

    What `aggregate_unsafe` does, without the per-statement dict: each batch
    hands over `FOLD_COLUMNS` as nine python lists – one bulk conversion in C
    per column, and the columns ``statements.csv`` needs but an entity does
    not are never materialised at all – and the fold walks them by index.
    Three things the dict path pays per statement are gone with it: the dict
    itself, the two ``datetime_iso`` calls (the sweep's projection formats
    the seen columns in SQL, so they arrive as the strings the fold wants),
    and the sets of timestamps, folded here as running bounds and handed on
    as the single values [`to_dict`][EntityPayload.to_dict] reduces them to.

    ``referents`` stays empty, as it is on the dict path: rows are grouped by
    ``entity_id`` and the payload is keyed on it, so no row can carry a
    different one. This store has no ``canonical_id`` and no resolution to
    make one.

    Batches must arrive entity-contiguous – ftmq's statement selects order by
    ``entity_id``, and the model places an entity in exactly one
    ``(shard, bucket)`` partition – and an entity may span two of them, so a
    payload is only emitted once a row of the next one shows up.

    Args:
        batches: Arrow batches of the sweep's projection
            (`ftm_lakehouse.model.statement.statement_csv_select`), whose
            seen columns are ISO strings.
        dataset: Dataset the entities belong to.

    Yields:
        One `EntityPayload` per entity, folded (`EntityPayload.folded`).

    Raises:
        ValueError: when the seen columns are not the projection's strings.
    """
    current: str | None = None
    count = 0
    schemata: set[str] = set()
    datasets: set[str] = set()
    origins: set[str] = set()
    roles: set[str] = set()
    properties: defaultdict[str, set[str]] = defaultdict(set)
    min_first: str | None = None
    max_first: str | None = None
    first: str | None = None
    last: str | None = None
    change: str | None = None

    def payload() -> EntityPayload:
        """The entity the fold has just finished."""
        data = EntityData(
            schemata=schemata,
            datasets=datasets,
            referents=set(),
            origins=origins,
            roles=roles,
            # `to_dict` folds these to one value each, which is what the
            # running bounds already are
            first_seens={first} if first is not None else set(),
            last_seens={last} if last is not None else set(),
            last_changes={change} if change is not None else set(),
            properties=properties,
            min_first_seen=min_first,
            max_first_seen=max_first,
        )
        return EntityPayload.folded(cast(str, current), data, count, dataset)

    for batch in batches:
        if not batch.num_rows:
            continue
        columns = {name: batch.column(name) for name in FOLD_COLUMNS}
        for name in SEEN_COLUMNS:
            if not pa.types.is_string(columns[name].type):
                raise ValueError(
                    f"Column `{name}` is `{columns[name].type}`, expected the "
                    "sweep projection's ISO strings"
                )
        entity_ids = columns["entity_id"].to_pylist()
        props = columns["prop"].to_pylist()
        values = columns["value"].to_pylist()
        row_schemata = columns["schema"].to_pylist()
        row_datasets = columns["dataset"].to_pylist()
        row_origins = columns["origin"].to_pylist()
        row_roles = columns["role"].to_pylist()
        row_first = columns["first_seen"].to_pylist()
        row_last = columns["last_seen"].to_pylist()

        for i in range(batch.num_rows):
            entity = entity_ids[i]
            if entity != current:
                if current is not None:
                    yield payload()
                current = entity
                count = 0
                schemata = set()
                datasets = set()
                origins = set()
                roles = set()
                properties = defaultdict(set)
                min_first = max_first = first = last = change = None

            count += 1
            schemata.add(row_schemata[i])
            datasets.add(row_datasets[i])
            origin = row_origins[i]
            if origin:
                origins.add(origin)
            role = row_roles[i]
            if role:
                roles.add(role)

            # the diff bounds span every statement, `id` rows included
            row = row_first[i]
            if row is not None:
                if min_first is None or row < min_first:
                    min_first = row
                if max_first is None or row > max_first:
                    max_first = row

            if props[i] == BASE_ID:
                # last_change = the latest `id` statement's first_seen
                if row is not None and (change is None or row > change):
                    change = row
                continue

            properties[props[i]].add(values[i])
            # first_seen / last_seen only from non-id statements, matching
            # `StatementEntity.to_context_dict`
            if row is not None and (first is None or row < first):
                first = row
            row = row_last[i]
            if row is not None and (last is None or row > last):
                last = row

    if current is not None:
        yield payload()
