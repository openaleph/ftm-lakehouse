"""Tests for MappingRepository - mapping configuration storage."""

from pathlib import Path

import yaml

from ftm_lakehouse.model.mapping import DatasetMapping
from ftm_lakehouse.repository.mapping import MappingRepository

DATASET = "test"


def load_fixture(fixtures_path: Path) -> DatasetMapping:
    """Load the mapping fixture as a DatasetMapping model."""
    with open(fixtures_path / "mapping.yml") as f:
        data = yaml.safe_load(f)
    return DatasetMapping(**data)


def test_repository_mapping_list(tmp_path, fixtures_path):
    """Test list() extracts content_hash from mapping paths."""
    repo = MappingRepository(dataset=DATASET, uri=tmp_path)

    # Initially empty
    assert list(repo.list()) == []

    # Add fixture mapping
    mapping = load_fixture(fixtures_path)
    repo.put(mapping)

    # list() should return content_hash extracted from path
    hashes = list(repo.list())
    assert hashes == ["abc123def456"]


def test_repository_mapping_list_multiple(tmp_path):
    """Test list() with multiple mappings."""
    repo = MappingRepository(dataset=DATASET, uri=tmp_path)

    # Create mappings with different content_hashes
    for hash_val in ["hash_aaa", "hash_bbb", "hash_ccc"]:
        mapping = DatasetMapping(
            dataset="test",
            content_hash=hash_val,
            queries=[],
        )
        repo.put(mapping)

    hashes = sorted(repo.list())
    assert hashes == ["hash_aaa", "hash_bbb", "hash_ccc"]


def test_repository_mapping_iterate(tmp_path, fixtures_path):
    """Test iterate() yields DatasetMapping objects."""
    repo = MappingRepository(dataset=DATASET, uri=tmp_path)
    mapping = load_fixture(fixtures_path)
    repo.put(mapping)

    mappings = list(repo.iterate())

    assert len(mappings) == 1
    assert isinstance(mappings[0], DatasetMapping)
    assert mappings[0].content_hash == "abc123def456"
    assert mappings[0].dataset == "test_dataset"
    assert len(mappings[0].queries) == 1
