"""`statistics.json` folded from the stream must say what the store says.

The sweep folds the counts out of the entity stream (`StatsCollector`) where
ftmq's `stats()` asks the store with six aggregate queries. The two have to
agree – that is the whole point of moving it into the sweep.
"""

from ftmq.model.stats import DatasetStats
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.logic.entities.stats import StatsCollector, _date_props
from ftm_lakehouse.operation.export import ExportJob, ExportOperation
from ftm_lakehouse.repository import EntityRepository

DATASET = "stats_test"

ENTITIES = [
    # Things, one with the same country under two properties
    {
        "id": "jane",
        "schema": "Person",
        "properties": {"name": ["Jane"], "country": ["de"]},
    },
    {
        "id": "acme",
        "schema": "Company",
        "properties": {
            "name": ["Acme"],
            "country": ["de"],
            "jurisdiction": ["de"],
            "incorporationDate": ["2001-06-01"],
        },
    },
    # a document, also a Thing. `processedAt` is an ingestion timestamp and
    # the latest date in the dataset - it must not bound the coverage
    {
        "id": "doc",
        "schema": "Pages",
        "properties": {
            "fileName": ["doc.pdf"],
            "contentHash": ["a" * 64],
            "date": ["1999-05-05"],
            "processedAt": ["2026-10-04"],
        },
    },
    # an Interval, with a country of its own
    {
        "id": "payment",
        "schema": "Payment",
        "properties": {"amount": ["23"], "date": ["2010-01-01"], "payer": ["acme"]},
    },
    {
        "id": "event",
        "schema": "Event",
        "properties": {
            "name": ["Gala"],
            "country": ["fr"],
            "startDate": ["2020-01-01"],
        },
    },
    # neither a Thing nor an Interval: counted in entity_count and nowhere else
    {"id": "page", "schema": "Page", "properties": {"index": ["1"], "bodyText": ["x"]}},
]


def _setup(uri) -> EntityRepository:
    repo = EntityRepository(dataset=DATASET, uri=uri)
    with repo.writer() as writer:
        for data in ENTITIES:
            writer.add_entity(make_entity(data))
    repo.flush()
    return repo


def _normalize(stats: DatasetStats) -> dict:
    """Group order is arbitrary on both sides."""
    data = stats.model_dump(mode="json")
    for group in ("things", "intervals"):
        data[group]["schemata"] = sorted(
            data[group]["schemata"], key=lambda s: s["name"]
        )
        data[group]["countries"] = sorted(
            data[group]["countries"], key=lambda c: c["code"]
        )
    data["countries"] = sorted(data["countries"])
    return data


def test_logic_stats_collector():
    """What the collector folds out of plain entity dicts."""
    collector = StatsCollector()
    for data in ENTITIES:
        collector.collect(
            {
                "id": data["id"],
                "schema": data["schema"],
                "properties": data["properties"],
            }
        )
    stats = collector.export()

    assert stats.entity_count == 6
    # Event is a Thing *and* an Interval, so it counts in both groups - the
    # totals are not a partition of the entities
    assert stats.things.total == 4  # Person, Company, Pages, Event
    assert stats.intervals.total == 2  # Payment, Event
    # Page is in neither group, but counted in entity_count
    assert {s.name for s in stats.things.schemata} == {
        "Person",
        "Company",
        "Pages",
        "Event",
    }
    assert {s.name for s in stats.intervals.schemata} == {"Payment", "Event"}
    # a country counts once per entity, whatever property names it
    assert sorted((c.code, c.count) for c in stats.things.countries) == [
        ("de", 2),
        ("fr", 1),
    ]
    assert [(c.code, c.count) for c in stats.intervals.countries] == [("fr", 1)]
    assert stats.countries == {"de", "fr"}
    # every date-typed property the model shows, compared as the ISO strings
    # they are stored as - hidden `processedAt` (2026) is not coverage
    assert str(stats.start) == "1999-05-05"
    assert str(stats.end) == "2020-01-01"

    # an entity without a resolvable schema is not counted
    collector.collect({"id": "x"})
    assert collector.export().entity_count == 6


def test_logic_stats_matches_the_store(tmp_path):
    """The exported artifact equals what ftmq's aggregate queries answer.

    Bar the coverage bounds: the hidden date properties stay out of them
    (`_date_props`), and the SQL path's ``min`` / ``max`` over every
    date-typed value has no notion of that – it reports the day of the last
    ingest as the end of the dataset's coverage.
    """
    repo = _setup(tmp_path)

    job = ExportJob.make(dataset=DATASET)
    ExportOperation(job=job, uri=tmp_path).run(force=True)

    written = repo._store.get(path.EXPORTS_STATISTICS, model=DatasetStats)
    swept, queried = _normalize(written), _normalize(repo.stats())
    assert {k: v for k, v in swept.items() if k not in ("start", "end")} == {
        k: v for k, v in queried.items() if k not in ("start", "end")
    }

    assert (swept["start"], swept["end"]) == ("1999-05-05", "2020-01-01")
    # the divergence, asserted rather than left to be discovered
    assert queried["end"] == "2026-10-04"
    assert "processedAt" not in _date_props("Pages")
