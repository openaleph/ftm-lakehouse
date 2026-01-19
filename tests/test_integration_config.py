import yaml
from anystore.io import smart_read, smart_stream_json
from anystore.util import ensure_uri

from ftm_lakehouse import get_catalog, get_dataset


def test_config_initialization(fixtures_path, tmp_path):
    # by environment (pytest env in pyproject.toml)
    catalog = get_catalog()
    assert catalog.uri == ensure_uri(fixtures_path / "lake")

    # by path uri
    catalog = get_catalog(tmp_path)
    assert catalog.uri == ensure_uri(tmp_path)

    # dataset
    catalog = get_catalog(fixtures_path / "lake")
    dataset = catalog.get_dataset("test_dataset")
    assert dataset.name == "test_dataset"
    assert dataset.uri == ensure_uri(fixtures_path / "lake/test_dataset")


def test_config_edit(tmp_path):
    catalog = get_catalog(tmp_path)
    dataset = catalog.get_dataset("test_dataset")
    dataset.update_model(title="A nice title")
    assert dataset.model.title == "A nice title"
    assert len([k for k in dataset._store.iterate_keys(prefix="versions")]) == 1
    data = yaml.safe_load(smart_read(tmp_path / "test_dataset/config.yml"))
    assert data["title"] == "A nice title"
    assert "description" not in data

    dataset.update_model(description="The description")
    assert dataset.model.title == "A nice title"
    assert dataset.model.description == "The description"
    assert len([k for k in dataset._store.iterate_keys(prefix="versions")]) == 2
    data = yaml.safe_load(smart_read(tmp_path / "test_dataset/config.yml"))
    assert data["title"] == "A nice title"
    assert data["description"] == "The description"
