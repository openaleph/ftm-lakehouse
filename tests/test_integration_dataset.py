"""Integration tests for Dataset API - how tenants use dataset metadata."""

from ftm_lakehouse import get_dataset
from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model import DatasetModel


def test_dataset_metadata(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))

    # new dataset
    dataset = get_dataset("new_dataset")
    assert dataset.name == dataset.model.name == "new_dataset"
    # doesn't exist yet
    assert not dataset._store.exists(path.CONFIG)
    dataset.update_model()
    # now exists
    assert dataset._store.exists(path.CONFIG)
    # with 1 version
    versions = [
        v
        for v in dataset._store.iterate_keys(prefix="versions")
        if v.endswith("config.yml")
    ]
    assert len(versions) == 1
    # patch data
    dataset.update_model(description="A good description")
    assert dataset.model.description == "A good description"
    versions = [
        v
        for v in dataset._store.iterate_keys(prefix="versions")
        if v.endswith("config.yml")
    ]
    assert len(versions) == 2

    # access model directly
    model = dataset.model
    assert model.name == "new_dataset"
    dataset.update_model(category="leak")
    assert dataset.model.category == "leak"
    versions = [
        v
        for v in dataset._store.iterate_keys(prefix="versions")
        if v.endswith("config.yml")
    ]
    assert len(versions) == 3

    # DatasetModel subclass
    class MyDatasetModel(DatasetModel):
        user_id: int = 0

    dataset = get_dataset("new_dataset", model_class=MyDatasetModel)
    assert isinstance(dataset.model, MyDatasetModel)
    dataset.update_model(user_id=17)
    assert dataset.model.user_id == 17
    versions = [
        v
        for v in dataset._store.iterate_keys(prefix="versions")
        if v.endswith("config.yml")
    ]
    assert len(versions) == 4

    # non existing dataset
    other = get_dataset("foo")
    assert not other.exists()
