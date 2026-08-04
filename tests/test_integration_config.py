"""Dataset config lifecycle – how tenants create, read and update config.yml."""

import yaml
from anystore.io import smart_read
from anystore.util import ensure_uri

from ftm_lakehouse import get_lakehouse, set_model_class
from ftm_lakehouse.catalog import (
    dataset_exists,
    ensure_dataset,
    get_dataset_model,
    update_dataset,
)
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model import DatasetModel
from ftm_lakehouse.repository.factories import get_entities


def test_config_initialization(fixtures_path, tmp_path):
    # by environment (pytest env in pyproject.toml)
    catalog = get_lakehouse()
    assert catalog.uri == ensure_uri(fixtures_path / "lake")

    # by path uri
    catalog = get_lakehouse(tmp_path)
    assert catalog.uri == ensure_uri(tmp_path)

    # dataset addressing
    catalog = get_lakehouse(fixtures_path / "lake")
    uri = catalog.dataset_uri("test_dataset")
    assert uri == ensure_uri(fixtures_path / "lake/test_dataset")
    assert get_dataset_model("test_dataset", uri).name == "test_dataset"


def test_config_edit(tmp_path):
    catalog = get_lakehouse(tmp_path)
    uri = catalog.dataset_uri("test_dataset")
    update_dataset("test_dataset", uri, title="A nice title")
    assert get_dataset_model("test_dataset", uri).title == "A nice title"
    store = get_entities("test_dataset", uri)._store
    assert len([k for k in store.iterate_keys(prefix="versions")]) == 1
    data = yaml.safe_load(smart_read(tmp_path / "test_dataset/config.yml"))
    assert data["title"] == "A nice title"
    assert "description" not in data

    update_dataset("test_dataset", uri, description="The description")
    model = get_dataset_model("test_dataset", uri)
    assert model.title == "A nice title"
    assert model.description == "The description"
    store = get_entities("test_dataset", uri)._store
    assert len([k for k in store.iterate_keys(prefix="versions")]) == 2
    data = yaml.safe_load(smart_read(tmp_path / "test_dataset/config.yml"))
    assert data["title"] == "A nice title"
    assert data["description"] == "The description"


def _config_versions(name, uri=None) -> int:
    store = get_entities(name, uri)._store
    return len(
        [v for v in store.iterate_keys(prefix="versions") if v.endswith("config.yml")]
    )


def test_dataset_metadata(monkeypatch, tmp_path):
    """The former Dataset-API surface: exists / fresh model reads / merge
    updates, addressed by name via the settings-derived uri."""
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))

    # new dataset – doesn't exist yet
    assert not dataset_exists("new_dataset")
    assert get_dataset_model("new_dataset").name == "new_dataset"

    update_dataset("new_dataset")
    assert dataset_exists("new_dataset")
    store = get_entities("new_dataset")._store
    assert store.exists(path.CONFIG)
    assert _config_versions("new_dataset") == 1

    # patch data – merge semantics, fresh reads
    update_dataset("new_dataset", description="A good description")
    assert get_dataset_model("new_dataset").description == "A good description"
    assert _config_versions("new_dataset") == 2

    update_dataset("new_dataset", category="leak")
    assert get_dataset_model("new_dataset").category == "leak"
    assert _config_versions("new_dataset") == 3

    # ensure is get-or-create: data ignored when the dataset exists
    ensure_dataset("new_dataset", title="ignored")
    assert get_dataset_model("new_dataset").title != "ignored"

    # non existing dataset
    assert not dataset_exists("foo")


def test_dataset_custom_model_class(monkeypatch, tmp_path):
    """set_model_class registers a DatasetModel subclass process-wide."""
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))

    class MyDatasetModel(DatasetModel):
        user_id: int = 0

    set_model_class(MyDatasetModel)
    update_dataset("new_dataset", user_id=17)
    model = get_dataset_model("new_dataset")
    assert isinstance(model, MyDatasetModel)
    assert model.user_id == 17

    # repositories snapshot the registered class too
    repo = get_entities("new_dataset")
    assert isinstance(repo._model, MyDatasetModel)
    assert repo._model.user_id == 17
