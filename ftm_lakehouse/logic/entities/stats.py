"""Dataset statistics folded from the entity payload stream."""

from collections import Counter
from functools import cache

from anystore.types import SDict
from followthemoney import model
from followthemoney.types import PropertyType, registry
from ftmq.model.stats import DatasetStats, compile_stats
from ftmq.store.sql import INTERVALS, THINGS


@cache
def _typed_props(schema: str, prop_type: PropertyType) -> tuple[str, ...]:
    """The properties of ``schema`` whose values are of ``prop_type``."""
    schema_ = model.get(schema)
    if schema_ is None:
        return ()
    return tuple(p.name for p in schema_.properties.values() if p.type == prop_type)


@cache
def _date_props(schema: str) -> tuple[str, ...]:
    """The properties of ``schema`` whose values bound the date coverage.
    Exclude hidden props (currently only `processedAt`)"""
    schema_ = model.get(schema)
    if schema_ is None:
        return ()
    return tuple(
        p.name
        for p in schema_.properties.values()
        if p.type == registry.date and not p.hidden
    )


@cache
def _groups(schema: str) -> tuple[str, ...]:
    """Which `DatasetStats` groups ``schema`` counts in. Some schemata (e.g.
    Event) are both Thing and Interval, thus are counted twice."""
    return tuple(
        group
        for group, bucket in (("things", THINGS), ("intervals", INTERVALS))
        if schema in bucket
    )


class StatsCollector:
    """Folds `DatasetStats` out of a stream of entity dicts. Avoids
    `EntityProxy` for performance

    Example:
        ```python
        collector = StatsCollector()
        for payload in entities:
            collector.collect(payload.to_dict())
        collector.export()
        ```
    """

    def __init__(self) -> None:
        self.entities = 0
        self.schemata: dict[str, Counter[str]] = {
            "things": Counter(),
            "intervals": Counter(),
        }
        self.countries: dict[str, Counter[str]] = {
            "things": Counter(),
            "intervals": Counter(),
        }
        self.start: str | None = None
        self.end: str | None = None

    def collect(self, data: SDict) -> None:
        """Take one entity dict, as `EntityPayload.to_dict` returns it.

        Args:
            data: The entity dict. An entity with no resolvable schema – what
                the sweep skips anyway – is not counted.
        """
        schema = data.get("schema")
        if not schema:
            return
        self.entities += 1
        properties = data.get("properties") or {}

        for prop in _date_props(schema):
            for value in properties.get(prop, ()):
                if self.start is None or value < self.start:
                    self.start = value
                if self.end is None or value > self.end:
                    self.end = value

        groups = _groups(schema)
        if not groups:
            return
        # once per entity, however many of its properties name the country
        countries = {
            value
            for prop in _typed_props(schema, registry.country)
            for value in properties.get(prop, ())
        }
        for group in groups:
            self.schemata[group][schema] += 1
            self.countries[group].update(countries)

    def merge(self, other: "StatsCollector") -> None:
        """Fold another collector's counts into this one.

        Every field is a commutative accumulator, which is what lets a
        partitioned sweep collect per partition and merge: ``entities`` and
        the four ``Counter`` fields sum, ``start`` / ``end`` take the min / max of
        the stored ISO strings. The result is what one collector that had seen
        every entity would hold – ftmq's ``compile_stats`` was factored out
        for exactly this ("so partitioned backends can compile per-partition
        stats and merge them").

        Sound only because an entity reaches exactly one collector: an entity
        id is placed in one ``(shard, bucket)`` pair, which is the unit a
        parallel sweep hands to a worker, so ``entities`` cannot double-count.

        Args:
            other: A collector over a disjoint set of entities.
        """
        self.entities += other.entities
        for group in self.schemata:
            self.schemata[group].update(other.schemata[group])
            self.countries[group].update(other.countries[group])
        if other.start is not None and (self.start is None or other.start < self.start):
            self.start = other.start
        if other.end is not None and (self.end is None or other.end > self.end):
            self.end = other.end

    def export(self) -> DatasetStats:
        """The statistics this collector folded.

        Returns:
            `DatasetStats`, with ``entity_count`` the entities seen – not the
            sum of the group totals, which leaves out the schemata that are
            in neither group.
        """
        return compile_stats(
            things=self.schemata["things"].items(),
            intervals=self.schemata["intervals"].items(),
            things_countries=self.countries["things"].items(),
            intervals_countries=self.countries["intervals"].items(),
            date_range=(self.start, self.end),
            entity_count=self.entities,
        )
