"""The parallel export sweep says what the serial one says.

`LAKEHOUSE_WORKERS` fans the store's ``(shard, bucket)`` pairs out to worker
processes, each folding its own entities into *parts* of every artifact, which
the parent then concatenates. The contract suite
(`tests/test_operation_exports.py`) already runs on both paths; what is left to
pin is that the two produce the same thing, and the one piece of the diff
machinery the fan-out could get wrong – a ``DEL``, which no worker can see.
"""

import hashlib
import json

import orjson
import pytest
from anystore.logic.compress import CompressKind
from ftmq.util import make_entity

from ftm_lakehouse.catalog import ensure_dataset
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.helpers.shards import entity_shard
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
    """A compressed artifact assembled from parts is a multi-frame file.

    The assembly copies each part's bytes verbatim rather than re-encoding, so
    the result is several codec frames back to back – a multi-member gzip or
    multi-frame zstd stream. anystore layers the stdlib file classes
    (``GzipFile`` / ``ZstdFile`` / ...), which are streaming multi-member
    readers, so it decodes as one file; what would break is a consumer calling
    a single-shot ``decompress()`` on the whole blob.

    Pinned by comparing against the serial run on the *same* store: different
    bytes (the framing), identical content (the rows).
    """
    uri = _setup(tmp_path, 1, compression=algorithm)
    artifacts = get_artifacts(DATASET, uri)
    assert artifacts.statements.compression == algorithm
    assert len(get_entities(DATASET, uri)._statements.sweep_sources()) > 1

    export(DATASET, uri, make_diff=False)
    key = artifacts.statements.key
    with artifacts.statements.dataset._store.open(key, "rb") as fh:
        serial_raw = fh.read()
    with artifacts.statements.reader() as fh:
        serial_rows = sorted(fh.read().decode().splitlines())

    export_settings.workers = 3
    clear_caches()
    export(DATASET, uri, make_diff=False, force=True)

    artifacts = get_artifacts(DATASET, uri)
    with artifacts.statements.dataset._store.open(key, "rb") as fh:
        parallel_raw = fh.read()
    assert parallel_raw.startswith(MAGIC[algorithm])
    # several frames, not one: same content, different framing
    assert parallel_raw != serial_raw
    with artifacts.statements.reader() as fh:
        assert sorted(fh.read().decode().splitlines()) == serial_rows
    # and the entities artifact, read through the repository's own stream
    assert {e.id for e in get_entities(DATASET, uri).stream()} == {
        data["id"] for data in ENTITIES
    }
