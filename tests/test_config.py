import yaml
from anystore.io import smart_read, smart_stream_json
from anystore.util import ensure_uri

from ftm_lakehouse.lake.base import get_lakehouse


def test_config_initialization(fixtures_path, tmp_path):
    # by environment (pytest env in pyproject.toml)
    lake = get_lakehouse()
    assert lake.storage.uri == ensure_uri(fixtures_path / "lake")
    assert lake.storage.serialization_mode == "raw"
    assert lake.cache.uri == ensure_uri(fixtures_path / "lake/.cache/ftm_lakehouse")
    assert lake.cache.raise_on_nonexist is False

    # by config uri
    # not implemented
    # uri = fixtures_path / "s3_lake" / "config.yml"
    # lake = get_lakehouse(uri)
    # assert lake.storage.uri == "s3://lakehouse"

    # by path uri
    lake = get_lakehouse(tmp_path)
    assert lake.storage.uri == ensure_uri(tmp_path)

    # dataset
    lake = get_lakehouse(fixtures_path / "lake")
    dataset = lake.get_dataset("test_dataset")
    assert dataset.name == "test_dataset"
    assert dataset.storage.uri == ensure_uri(fixtures_path / "lake/test_dataset")
    assert dataset.cache.uri == ensure_uri(
        fixtures_path / "lake/test_dataset/.cache/ftm_lakehouse"
    )

    # not implemented
    # dataset = get_dataset("external_dataset")
    # assert dataset.storage.uri == "s3://s3_dataset"
    # assert dataset.cache.uri == ensure_uri(
    #     fixtures_path / "lake/.cache/ftm_lakehouse/external_dataset"
    # )


def test_config_edit(tmp_path):
    lake = get_lakehouse(tmp_path)
    dataset = lake.get_dataset("test_dataset")
    dataset.make_config(title="A nice title")
    assert dataset.load_model().title == "A nice title"
    assert len([k for k in dataset.storage.iterate_keys(prefix="versions")]) == 1
    data = yaml.safe_load(smart_read(tmp_path / "test_dataset/config.yml"))
    assert data["title"] == "A nice title"
    assert "description" not in data

    dataset.make_config(description="The description")
    assert dataset.load_model().title == "A nice title"
    assert dataset.load_model().description == "The description"
    assert len([k for k in dataset.storage.iterate_keys(prefix="versions")]) == 2
    data = yaml.safe_load(smart_read(tmp_path / "test_dataset/config.yml"))
    assert data["title"] == "A nice title"
    assert data["description"] == "The description"

    dataset.make_index()
    assert len([k for k in dataset.storage.iterate_keys(prefix="versions")]) == 3
    data = {}
    for line in smart_stream_json(tmp_path / "test_dataset/index.json"):
        data = line
    assert data["title"] == "A nice title"
    assert data["description"] == "The description"
