"""Tests for VersionStore - timestamped snapshot storage."""

import re
from datetime import datetime, timezone

from anystore.model import BaseModel

from ftm_lakehouse.storage.versions import VersionStore


class VersionedData(BaseModel):
    """Simple model for version storage tests."""

    name: str


def test_storage_versions_make(tmp_path):
    """Test creating a versioned snapshot."""
    store = VersionStore(tmp_path, model=VersionedData)

    # No tag before make
    assert not (tmp_path / "tags/lakehouse/config.json").exists()

    data = VersionedData(name="test_dataset")
    versioned_path = store.make("config.json", data)

    # Main file should exist
    assert (tmp_path / "config.json").exists()

    # Versioned path should start with versions/ and end with config.json
    assert versioned_path.startswith("versions/")
    assert versioned_path.endswith("/config.json")

    # Verify versioned file exists at expected location
    assert (tmp_path / versioned_path).exists()

    # Verify path structure: versions/YYYY/MM/YYYY-MM-DDTHH:MM:SS.../config.json
    pattern = (
        r"^versions/\d{4}/\d{2}/\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}.*?/config\.json$"
    )
    assert re.match(pattern, versioned_path)

    # Tag should be created at hardcoded path
    assert (tmp_path / "tags/lakehouse/config.json").exists()


def test_storage_versions_make_hardcoded_path(tmp_path):
    """Test versioned path structure with hardcoded timestamp."""
    store = VersionStore(tmp_path, model=VersionedData)

    data = VersionedData(name="test_dataset")

    # We can't easily inject a timestamp, but we can verify the path structure
    # by checking the year/month directories match current time
    now = datetime.now(timezone.utc)
    versioned_path = store.make("index.json", data)

    # Path should contain current year and month
    year = str(now.year)
    month = f"{now.month:02d}"
    assert f"versions/{year}/{month}/" in versioned_path

    # Hardcoded structure verification: versions/YYYY/MM/timestamp/filename
    parts = versioned_path.split("/")
    assert parts[0] == "versions"
    assert len(parts[1]) == 4  # YYYY
    assert len(parts[2]) == 2  # MM
    assert "T" in parts[3]  # timestamp contains T separator
    assert parts[4] == "index.json"


def test_storage_versions_get(tmp_path):
    """Test retrieving current version."""
    store = VersionStore(tmp_path, model=VersionedData)

    data = VersionedData(name="retrieved_dataset")
    store.make("data.json", data)

    # Get should return the current version
    retrieved = store.get("data.json")
    assert retrieved.name == "retrieved_dataset"


def test_storage_versions_exists(tmp_path):
    """Test checking if main key exists."""
    store = VersionStore(tmp_path, model=VersionedData)

    assert not store.exists("missing.json")

    store.make("exists.json", VersionedData(name="test"))
    assert store.exists("exists.json")


def test_storage_versions_list_versions(tmp_path):
    """Test listing all versioned copies."""
    store = VersionStore(tmp_path, model=VersionedData)

    # No versions initially
    assert store.list_versions("stats.json") == []

    # Create multiple versions
    for i in range(3):
        store.make("stats.json", VersionedData(name=f"v{i}"))

    versions = store.list_versions("stats.json")
    assert len(versions) == 3

    # Versions should be sorted
    assert versions == sorted(versions)

    # All should end with the filename
    for v in versions:
        assert v.endswith("stats.json")


def test_storage_versions_multiple_files(tmp_path):
    """Test versioning multiple different files."""
    store = VersionStore(tmp_path, model=VersionedData)

    store.make("config.json", VersionedData(name="config"))
    store.make("index.json", VersionedData(name="index"))
    store.make("config.json", VersionedData(name="config2"))

    # Each file should have its own versions
    config_versions = store.list_versions("config.json")
    index_versions = store.list_versions("index.json")

    assert len(config_versions) == 2
    assert len(index_versions) == 1


def test_storage_versions_nested_path(tmp_path):
    """Test versioning files in nested directories."""
    store = VersionStore(tmp_path, model=VersionedData)

    data = VersionedData(name="nested")
    versioned_path = store.make("exports/statistics.json", data)

    # Main file should exist in nested path
    assert (tmp_path / "exports" / "statistics.json").exists()

    # Versioned path structure: {parent}/versions/YYYY/MM/timestamp/{filename}
    parts = versioned_path.split("/")
    assert parts[0] == "exports"
    assert parts[1] == "versions"
    assert len(parts[2]) == 4  # YYYY
    assert len(parts[3]) == 2  # MM
    assert "T" in parts[4]  # timestamp
    assert parts[5] == "statistics.json"  # filename only, not exports/statistics.json
    assert len(parts) == 6

    # Verify file exists
    assert (tmp_path / versioned_path).exists()

    # Tag should be created for nested path
    assert (tmp_path / "tags/lakehouse/exports/statistics.json").exists()


def test_storage_versions_preserves_data(tmp_path):
    """Test that versioned copies preserve data correctly."""
    store = VersionStore(tmp_path, model=VersionedData)

    # Create initial version
    v1_data = VersionedData(name="version1")
    v1_path = store.make("data.json", v1_data)

    # Create updated version
    v2_data = VersionedData(name="version2")
    v2_path = store.make("data.json", v2_data)

    # Current version should be v2
    current = store.get("data.json")
    assert current.name == "version2"

    # Both versioned files should exist
    assert (tmp_path / v1_path).exists()
    assert (tmp_path / v2_path).exists()

    # Read v1 directly to verify it's preserved
    import json

    with open(tmp_path / v1_path) as f:
        v1_stored = json.load(f)
    assert v1_stored["name"] == "version1"


def test_storage_versions_yaml_serialization(tmp_path):
    """Test that .yml files are serialized as YAML."""
    import yaml

    store = VersionStore(tmp_path, model=VersionedData)

    data = VersionedData(name="yaml_test")
    versioned_path = store.make("config.yml", data)

    # Main file should exist
    assert (tmp_path / "config.yml").exists()

    # Tag should be created for .yml file
    assert (tmp_path / "tags/lakehouse/config.yml").exists()

    # Read raw file and verify it's valid YAML
    with open(tmp_path / "config.yml") as f:
        raw_content = f.read()

    # Should be parseable as YAML
    parsed = yaml.safe_load(raw_content)
    assert parsed["name"] == "yaml_test"

    # Reconstruct model from YAML
    reconstructed = VersionedData(**parsed)
    assert reconstructed.name == "yaml_test"

    # Versioned file should also be YAML
    with open(tmp_path / versioned_path) as f:
        versioned_parsed = yaml.safe_load(f)
    assert versioned_parsed["name"] == "yaml_test"

    # Get via store should return the model
    retrieved = store.get("config.yml")
    assert retrieved.name == "yaml_test"
