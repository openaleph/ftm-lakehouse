"""Tests for the ShardOperation (rewrite the store onto a new shard count)."""

import pytest
from ftmq.query import M, Query
from ftmq.util import make_entity

from ftm_lakehouse.catalog import get_dataset_model
from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.operation.maintenance import (
    OptimizeJob,
    OptimizeOperation,
    ShardJob,
    ShardOperation,
)
from ftm_lakehouse.repository import EntityRepository

DATASET = "shard_test"
ENTITIES = 25


def _fill(repo: EntityRepository, origins: tuple[str, ...] = ("a", "b")) -> None:
    for origin in origins:
        with repo.writer(origin=origin) as writer:
            for i in range(ENTITIES):
                writer.add_entity(
                    make_entity(
                        {
                            "id": f"entity-{i}",
                            "schema": "Person",
                            "properties": {"name": [f"Person {i}"]},
                        }
                    )
                )
    repo.flush()


def _shards_on_disk(repo: EntityRepository) -> set[str]:
    return {s for s, _, _ in repo._statements._list_partitions()}


def test_operation_shard(tmp_path):
    """A re-shard moves every row to the shard its entity id hashes to,
    keeps the data readable, and records the new count in ``config.yml``."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    _fill(repo)
    assert repo.shards == 0
    assert _shards_on_disk(repo) == {"0"}
    before = {e.id: e.caption for e in repo.query()}
    assert len(before) == ENTITIES

    job = ShardJob.make(dataset=DATASET, shards=8)
    op = ShardOperation(job=job, uri=tmp_path)
    result = op.run()
    assert result.done == 2  # rewrite + config write
    # the repository that did the rewrite adopted the new count, so it keeps
    # pruning id lookups to the shard the row actually moved to
    assert op.entities.shards == 8
    assert op.entities.get("entity-0") is not None
    assert (tmp_path / f"tags/lakehouse/{tag.OP_SHARD}").exists()

    # the config now declares the new layout ...
    assert get_dataset_model(DATASET, tmp_path).shards == 8
    # ... and the physical layout agrees with it
    assert _shards_on_disk(repo) == {
        path.entity_shard(f"entity-{i}", 8) for i in range(ENTITIES)
    }
    for _, bucket, origin in repo._statements._list_partitions():
        assert bucket == "thing"
        assert origin in ("a", "b")

    # data is unchanged, and still reachable through the shard prune
    reread = EntityRepository(dataset=DATASET, uri=tmp_path)
    assert reread.shards == 8
    assert {e.id: e.caption for e in reread.query()} == before
    for entity_id in before:
        assert reread.get(entity_id) is not None
        assert len(list(reread.query(Query(M(entity_id=entity_id))))) == 1


def test_operation_shard_is_fresh(tmp_path):
    """Freshness is the configured count – and ``force`` overrides it."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    _fill(repo, origins=("a",))

    op = ShardOperation(job=ShardJob.make(dataset=DATASET, shards=4), uri=tmp_path)
    assert not op.is_fresh()
    op.run()

    op = ShardOperation(job=ShardJob.make(dataset=DATASET, shards=4), uri=tmp_path)
    assert op.is_fresh()
    assert op.run().done == 0  # skipped
    assert op.run(force=True).done == 2  # rewritten anyway


def test_operation_shard_roundtrip(tmp_path):
    """Growing and collapsing again returns the store to a single shard
    with the same statements."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    _fill(repo, origins=("a",))
    before = sorted(s.id for s in repo.query_statements())

    ShardOperation(job=ShardJob.make(dataset=DATASET, shards=16), uri=tmp_path).run()
    grown = EntityRepository(dataset=DATASET, uri=tmp_path)
    assert len(_shards_on_disk(grown)) > 1
    assert sorted(s.id for s in grown.query_statements()) == before

    ShardOperation(job=ShardJob.make(dataset=DATASET, shards=0), uri=tmp_path).run()
    collapsed = EntityRepository(dataset=DATASET, uri=tmp_path)
    assert _shards_on_disk(collapsed) == {"0"}
    assert sorted(s.id for s in collapsed.query_statements()) == before


def test_operation_shard_keeps_unmerged_rows(tmp_path):
    """The rewrite moves rows, it does not decide which survive: duplicates
    and tombstones ride along, and the partitions come out dirty so the
    next optimize collapses them."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    for _ in range(2):  # same entity twice -> duplicate rows per statement id
        with repo.writer(origin="ingest") as writer:
            writer.add_entity(
                make_entity(
                    {
                        "id": "entity-0",
                        "schema": "Person",
                        "properties": {"name": ["Jane"]},
                    }
                )
            )
        repo.flush()
    raw = len(list(repo.query_statements()))
    assert raw > len({s.id for s in repo.query_statements()})

    ShardOperation(job=ShardJob.make(dataset=DATASET, shards=8), uri=tmp_path).run()
    resharded = EntityRepository(dataset=DATASET, uri=tmp_path)
    assert len(list(resharded.query_statements())) == raw
    assert resharded.needs_merge

    OptimizeOperation(job=OptimizeJob.make(dataset=DATASET), uri=tmp_path).run(
        force=True
    )
    merged = EntityRepository(dataset=DATASET, uri=tmp_path)
    statements = list(merged.query_statements())
    assert len(statements) == len({s.id for s in statements})


def test_operation_shard_empty_store(tmp_path):
    """A dataset with no statements yet only gets its config written."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    assert not repo.exists

    ShardOperation(job=ShardJob.make(dataset=DATASET, shards=8), uri=tmp_path).run()
    assert get_dataset_model(DATASET, tmp_path).shards == 8

    reread = EntityRepository(dataset=DATASET, uri=tmp_path)
    _fill(reread, origins=("a",))
    assert _shards_on_disk(reread) == {
        path.entity_shard(f"entity-{i}", 8) for i in range(ENTITIES)
    }


def test_operation_shard_rejects_negative():
    with pytest.raises(ValueError):
        ShardJob.make(dataset=DATASET, shards=-1)
