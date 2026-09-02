"""Export compression is derived from the dataset config, never passed in.

``compression`` is recorded in ``config.yml`` like ``shards``, so every writer
of a dataset produces the same artifact layout and no caller can compress one
export differently from the next.
"""

import csv
import io

import orjson
import pytest
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.logic.compress import CompressKind, decompress_stream
from ftm_lakehouse.operation import ExportKind, export
from ftm_lakehouse.repository.base import DatasetRef
from ftm_lakehouse.repository.factories import get_entities
from tests.shared import BOB, JANE, JOHN

MAGIC = {CompressKind.gz: b"\x1f\x8b", CompressKind.zst: b"\x28\xb5\x2f\xfd"}


def _seed(tmp_path, name: str, **config):
    lake = get_lakehouse(str(tmp_path))
    lake.ensure_dataset(name, **config)
    dataset = DatasetRef(name, lake.dataset_uri(name))
    entities = get_entities(*dataset)
    with entities.writer() as bulk:
        bulk.add_entity(make_entity(JANE))
        bulk.add_entity(make_entity(JOHN))
    entities.flush()
    export(dataset.name, ExportKind.statements, dataset.uri)
    export(dataset.name, ExportKind.entities, dataset.uri)
    return dataset


def _read(dataset, key: str) -> bytes:
    with get_entities(*dataset)._store.open(key, "rb") as fh:
        return fh.read()


def test_export_uncompressed_by_default(tmp_path):
    dataset = _seed(tmp_path, "plain")
    assert get_entities(*dataset).compression is None

    statements = _read(dataset, path.EXPORTS_STATEMENTS)
    entities = _read(dataset, path.ENTITIES_JSON)
    assert not any(statements.startswith(m) for m in MAGIC.values())

    rows = list(csv.DictReader(io.StringIO(statements.decode())))
    assert {r["entity_id"] for r in rows} == {"jane", "john"}
    assert {orjson.loads(line)["id"] for line in entities.splitlines()} == {
        "jane",
        "john",
    }


@pytest.mark.parametrize("algorithm", list(CompressKind))
def test_export_compression_from_dataset_config(tmp_path, algorithm):
    """The codec recorded in config.yml drives both exports, with no runtime
    argument anywhere in the export path."""
    dataset = _seed(tmp_path, f"packed_{algorithm.value}", compression=algorithm)
    assert get_entities(*dataset).compression == algorithm
    # ... and reaches the parquet store that writes statements.csv
    assert get_entities(*dataset)._statements.compression == algorithm

    statements = _read(dataset, path.exports_statements(algorithm))
    entities = _read(dataset, path.entities_json(algorithm))
    assert statements.startswith(MAGIC[algorithm])
    assert entities.startswith(MAGIC[algorithm])

    with decompress_stream(io.BytesIO(statements), algorithm) as fh:
        rows = list(csv.DictReader(io.TextIOWrapper(fh)))
    assert {r["entity_id"] for r in rows} == {"jane", "john"}

    with decompress_stream(io.BytesIO(entities), algorithm) as fh:
        ids = {orjson.loads(line)["id"] for line in fh}
    assert ids == {"jane", "john"}

    # a diff carries the same codec (it re-encodes per line, since each
    # entity gets an envelope). The first export only records the state, so
    # write one more entity to get an actual diff file.
    entities_repo = get_entities(*dataset)
    with entities_repo.writer() as bulk:
        bulk.add_entity(make_entity(BOB))
    entities_repo.flush()
    export(dataset.name, ExportKind.entities, dataset.uri, force=True)

    (diff_key,) = list(entities_repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES))
    diff = _read(dataset, diff_key)
    assert diff.startswith(MAGIC[algorithm])
    with decompress_stream(io.BytesIO(diff), algorithm) as fh:
        envelopes = [orjson.loads(line) for line in fh]
    assert {e["entity"]["id"] for e in envelopes} == {"bob"}
    assert {e["op"] for e in envelopes} == {"ADD"}

    # and the read-back path decodes it again (the re-export carries bob too)
    assert {e.id for e in get_entities(*dataset).stream()} == {"jane", "john", "bob"}
