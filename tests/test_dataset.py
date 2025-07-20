"""This is how OpenAleph uses dataset metadata"""

from ftm_lakehouse import io
from ftm_lakehouse.conventions import path
from ftm_lakehouse.lake.base import get_dataset
from ftm_lakehouse.model import DatasetModel


def test_dataset_metadata(monkeypatch, tmp_path):
    monkeypatch.setenv("LAKEHOUSE_URI", str(tmp_path))

    # new dataset
    dataset = get_dataset("new_dataset")
    assert dataset.name == dataset.model.name == "new_dataset"
    # doesn't exist yet
    assert not dataset.storage.exists(path.CONFIG)
    dataset.make_config()
    # now exists
    assert dataset.storage.exists(path.CONFIG)
    # with 1 version
    versions = dataset.storage.iterate_keys(prefix="versions/config.yml")
    assert len([v for v in versions]) == 1
    # patch data
    dataset.make_config(description="A good description")
    assert dataset.model.description == "A good description"
    versions = dataset.storage.iterate_keys(prefix="versions/config.yml")
    assert len([v for v in versions]) == 2
    # index.json
    assert not dataset.storage.exists(path.INDEX)
    dataset.make_index()
    assert dataset.storage.exists(path.INDEX)
    versions = dataset.storage.iterate_keys(prefix="versions/index.json")
    assert len([v for v in versions]) == 1

    # higher level
    dataset = io.get_dataset_metadata("new_dataset")
    assert dataset.name == "new_dataset"
    dataset = io.update_dataset_metadata("new_dataset", category="leak")
    assert dataset.category == io.get_dataset_metadata("new_dataset").category == "leak"
    dataset = get_dataset("new_dataset")
    versions = dataset.storage.iterate_keys(prefix="versions/config.yml")
    assert len([v for v in versions]) == 3

    # DatasetModel subclass
    class Dataset(DatasetModel):
        user_id: int = 0

    dataset = get_dataset("new_dataset", dataset_model=Dataset)
    assert isinstance(dataset.model, Dataset)
    dataset.make_config(user_id=17)
    assert dataset.model.user_id == 17
    versions = dataset.storage.iterate_keys(prefix="versions/config.yml")
    assert len([v for v in versions]) == 4
    # extra data is not in index.json:
    index = dataset.make_index()
    assert not hasattr(index, "user_id")
    versions = dataset.storage.iterate_keys(prefix="versions/index.json")
    assert len([v for v in versions]) == 2

    # non existing dataset (won't be created implicitly)
    assert not io.has_dataset("foo")
