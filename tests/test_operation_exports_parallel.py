"""Worker processes export what one process exports.

The export folds each ``(shard, bucket)`` pair into *parts* of every artifact,
in `LAKEHOUSE_WORKERS` processes or in-process, and concatenates them. The
contract suite (`tests/test_operation_exports.py`) runs at both settings; this
pins that they produce the same thing, plus a ``DEL``, which no pair can see.
"""

import hashlib
import json
from importlib import import_module

import orjson
import pytest
from anystore.logic.compress import CompressKind
from ftmq.util import make_entity

from ftm_lakehouse.catalog import ensure_dataset
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.helpers.shards import entity_shard
from ftm_lakehouse.model.statement import statement_csv_header
from ftm_lakehouse.operation.export import settings as export_settings
from ftm_lakehouse.operation.factories import export
from ftm_lakehouse.repository.factories import (
    clear_caches,
    get_artifacts,
    get_entities,
)

MAGIC = {
    CompressKind.gz: b"\x1f\x8b",
    CompressKind.zst: b"\x28\xb5\x2f\xfd",
}

# the module, which `ftm_lakehouse.operation.export` (the function) shadows
export_module = import_module("ftm_lakehouse.operation.export")

DATASET = "export_parallel"
SHARDS = 4
ENTITIES = [
    {
        "id": f"e{i}",
        "schema": "Person" if i % 2 else "Company",
        "properties": {"name": [f"Name {i}"], "country": ["de" if i % 3 else "fr"]},
    }
    for i in range(60)
]


def _setup(tmp_path, workers: int, entities=ENTITIES, **config) -> str:
    """A sharded dataset with `entities` in it, at a worker setting."""
    uri = str(tmp_path / f"w{workers}")
    clear_caches()
    export_settings.workers = workers
    ensure_dataset(DATASET, uri=uri, shards=SHARDS, **config)
    repo = get_entities(DATASET, uri)
    with repo.writer() as writer:
        for data in entities:
            writer.add_entity(make_entity(data))
    repo.flush()
    return uri


def _rows(uri: str, key) -> list[str]:
    repo = get_entities(DATASET, uri)
    with repo._store.open(key, "rb") as fh:
        return sorted(fh.read().decode().splitlines())


def _entities_digest(uri: str) -> str:
    """Entities as a set, with multi-valued properties normalised.

    Row order and the order of values *within* a property both follow the
    statement order, which `statement_csv_select` leaves undefined within one
    entity – two runs of the same code differ there, so neither is a property
    of the fan-out.
    """
    repo = get_entities(DATASET, uri)
    with repo._store.open(path.ENTITIES_JSON, "rb") as fh:
        rows = [orjson.loads(line) for line in fh if line.strip()]
    key = sorted(
        (
            row["id"],
            row["schema"],
            json.dumps(
                {k: sorted(v) for k, v in row["properties"].items()}, sort_keys=True
            ),
        )
        for row in rows
    )
    return hashlib.sha256(repr(key).encode()).hexdigest()


def _stats(uri: str) -> dict:
    with get_entities(DATASET, uri)._store.open(path.EXPORTS_STATISTICS) as fh:
        return json.loads(fh.read())


def test_export_parallel_matches_serial(tmp_path):
    """Three workers write what one process writes.

    One store, exported twice, so the statements are the same rows – anything
    that differs is the fan-out's doing and not a second run's timestamps.
    """
    uri = _setup(tmp_path, 1)
    assert (
        len(get_entities(DATASET, uri)._statements.sweep_sources()) > 1
    ), "the dataset must span several pairs or the fan-out proves nothing"

    one = export(DATASET, uri, make_diff=False)
    serial_csv = _rows(uri, path.EXPORTS_STATEMENTS)
    serial_entities = _entities_digest(uri)
    serial_stats = _stats(uri)
    # this dataset has no documents at all, so `documents.csv` is the case
    # where an empty artifact still has to be a header rather than a blank
    serial_documents = _rows(uri, path.EXPORTS_DOCUMENTS)
    assert len(serial_documents) == 1

    export_settings.workers = 3
    clear_caches()
    many = export(DATASET, uri, make_diff=False, force=True)

    assert one.result == many.result
    assert one.result is not None and one.result["entities"] == 60
    # the csv as a row set, header included – the parts are headerless and the
    # assembled file takes its header from `statement_csv_header`
    parallel_csv = _rows(uri, path.EXPORTS_STATEMENTS)
    assert parallel_csv == serial_csv
    assert sum(1 for row in parallel_csv if row.startswith('"id","entity_id"')) == 1
    assert _entities_digest(uri) == serial_entities
    # the statistics, folded per worker and merged in the parent
    assert _stats(uri) == serial_stats
    assert serial_stats["entity_count"] == 60
    assert _rows(uri, path.EXPORTS_DOCUMENTS) == serial_documents


def test_export_parallel_merged_store(tmp_path):
    """On a merged store every pair is swept by merging its origins' streams
    instead of sorting it – and writes what the sorted sweep wrote."""
    uri = _setup(tmp_path, 3)
    repo = get_entities(DATASET, uri)
    with repo.writer(origin="extra") as writer:  # a second origin, same entities
        for data in ENTITIES[::3]:
            writer.add_entity(make_entity({**data, "properties": {"alias": ["A"]}}))
    repo.flush()
    sorted_ = export(DATASET, uri, make_diff=False)
    csv, entities, stats = (
        _rows(uri, path.EXPORTS_STATEMENTS),
        _entities_digest(uri),
        _stats(uri),
    )

    repo.merge()
    sources = repo._statements.sweep_sources()
    assert all(source.presorted for source in sources)
    assert any(len(source.relations) > 1 for source in sources)
    merged = export(DATASET, uri, make_diff=False, force=True)

    assert merged.result == sorted_.result
    assert _rows(uri, path.EXPORTS_STATEMENTS) == csv
    assert _entities_digest(uri) == entities
    assert _stats(uri) == stats


def test_export_parallel_spill_directory_per_pair(tmp_path, monkeypatch):
    """Each pair's DuckDB instance spills into its own directory – workers
    sharing one crash on each other's spill files."""
    uri = _setup(tmp_path, 1)
    spill = []
    export_partition = export_module.export_partition

    def record(task):
        spill.append(task.duckdb_config["temp_directory"])
        return export_partition(task)

    monkeypatch.setattr(export_module, "export_partition", record)
    export(DATASET, uri, make_diff=False)
    assert len(spill) > 1
    assert len(set(spill)) == len(spill)


def test_export_parallel_diff_del(tmp_path):
    """A tombstoned entity becomes a ``DEL``, and nothing else does.

    The piece the fan-out could get wrong: a delete never comes past a
    live-view sweep, so the parent loads the candidates, hands each worker its
    *shard's* ids, and whatever no worker met alive is gone. An id names its
    shard but not its bucket, so narrowing any further would emit a ``DEL``
    for an entity that is still there.
    """
    uri = _setup(tmp_path, 3)
    export(DATASET, uri, make_diff=True)  # records the state, writes no diff
    repo = get_entities(DATASET, uri)

    gone, kept = "e7", "e8"
    assert entity_shard(gone, SHARDS) != entity_shard(
        kept, SHARDS
    ), "the deleted entity must live in a different shard from a kept one"
    repo.delete_entity(gone)
    with repo.writer() as writer:
        writer.add_entity(
            make_entity(
                {"id": "new", "schema": "Person", "properties": {"name": ["New"]}}
            )
        )
    repo.flush()

    export(DATASET, uri, make_diff=True, force=True)

    (diff_key,) = list(repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES))
    with repo._store.open(diff_key, "rb") as fh:
        envelopes = [orjson.loads(line) for line in fh if line.strip()]
    ops = {e["entity"]["id"]: e["op"] for e in envelopes}
    assert ops == {"new": "ADD", gone: "DEL"}
    assert kept not in ops
    assert {e.id for e in repo.stream()} >= {kept, "new"}
    assert gone not in {e.id for e in repo.stream()}


@pytest.mark.parametrize("algorithm", (CompressKind.zst, CompressKind.gz))
def test_export_parallel_multi_frame(tmp_path, algorithm):
    """A compressed artifact assembled from parts is several codec frames back
    to back, and still reads back as one file through anystore's stdlib file
    classes – a single-shot ``decompress()`` on the blob would not."""
    uri = _setup(tmp_path, 1, compression=algorithm)
    artifacts = get_artifacts(DATASET, uri)
    assert artifacts.statements.compression == algorithm
    assert len(get_entities(DATASET, uri)._statements.sweep_sources()) > 1

    export(DATASET, uri, make_diff=False)

    with artifacts.statements.dataset._store.open(artifacts.statements.key, "rb") as fh:
        raw = fh.read()
    assert raw.startswith(MAGIC[algorithm])
    assert raw.count(MAGIC[algorithm]) > 1
    with artifacts.statements.reader() as fh:
        rows = fh.read().decode().splitlines()
    assert rows[0].encode() + b"\n" == statement_csv_header()
    statements = list(get_entities(DATASET, uri).query_statements())
    assert len(rows) == 1 + len(statements)
    # and the entities artifact, read through the repository's own stream
    assert {e.id for e in get_entities(DATASET, uri).stream()} == {
        data["id"] for data in ENTITIES
    }
