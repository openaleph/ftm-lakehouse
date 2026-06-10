"""Factory cache identity and per-dataset shards resolution.

The factories are the single repository-instantiation path: every way of
addressing the same dataset must resolve to the same cached instance, and
the shard count must come from the dataset's own config – never from the
environment.
"""

from pathlib import Path

from ftmq.util import make_entity

from ftm_lakehouse.lake import ensure_dataset, get_lakehouse
from ftm_lakehouse.model.dataset import DEFAULT_SHARDS
from ftm_lakehouse.repository.base import resolve_shards
from ftm_lakehouse.repository.factories import dataset_uri, get_entities


def test_factories_identity_across_paths(tmp_path, monkeypatch):
    """Same dataset, same instance – name-only, explicit uri, Dataset method."""
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))
    lake = get_lakehouse(tmp_path)
    dataset = lake.get_dataset("ident")

    repo = dataset.get_entities()
    assert repo is get_entities("ident")
    assert repo is get_entities("ident", dataset.uri)
    assert repo is get_entities("ident", Path(str(tmp_path)) / "ident")

    # one ParquetStore (and so one LakeStore / DuckDB connection) per dataset
    assert dataset.get_entities()._statements is repo._statements


def test_factories_canonical_uri(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))
    derived = dataset_uri("x")
    explicit = dataset_uri("x", tmp_path / "x")
    assert derived == explicit
    assert derived.startswith("file://")


def test_factory_resolves_config_shards(tmp_path, monkeypatch):
    """The dataset's recorded shard count wins on every path."""
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))
    dataset = ensure_dataset("sharded", shards=4)
    assert dataset.model.shards == 4

    assert resolve_shards(dataset.uri) == 4
    repo = get_entities("sharded")
    assert repo.shards == 4
    assert repo is dataset.get_entities()


def test_factory_resolves_default_shards_without_config(tmp_path, monkeypatch):
    """A fresh dataset without config falls back to the hardcoded default."""
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))
    repo = get_entities("fresh")
    assert repo.shards == DEFAULT_SHARDS == 0


def test_config_shards_drive_partitioning(tmp_path, monkeypatch):
    """Statements written through any path land in config-driven shard
    partitions, and shard-scoped lookups resolve."""
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))
    ensure_dataset("sharded", shards=4)
    repo = get_entities("sharded")

    with repo.writer(origin="test") as writer:
        for i in range(32):
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

    shard_dirs = {
        p.name for p in (tmp_path / "sharded/entities/statements").glob("shard=*")
    }
    assert len(shard_dirs) > 1  # data actually spread across shards

    # shard-scoped single-entity lookup
    entity = repo.get("entity-1")
    assert entity is not None
    assert "Person 1" in entity.get("name")
