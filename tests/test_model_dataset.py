from ftmq.model import Dataset

from ftm_lakehouse.model.dataset import DatasetModel


def test_model_dataset(fixtures_path):
    config = fixtures_path / "lake/test_dataset/config.yml"
    dataset = DatasetModel.from_yaml_uri(config)
    assert isinstance(dataset, Dataset)
    assert Dataset(**dataset.model_dump())
