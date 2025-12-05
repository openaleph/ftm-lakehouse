from anystore import get_store
from followthemoney.dataset import DefaultDataset
from ftmq.model import Dataset
from rigour.mime.types import PLAIN

from ftm_lakehouse.model import DatasetModel, File


def test_model():
    checksum = "2928064cd9a743af30b720634dcffacdd84de23d"
    file_id = "file-f47cdafce4cd4c82eb97334b0a215f4af587173a"
    store = get_store("http://localhost:8000")

    file = File.from_info(store.info("src/utf.txt"), checksum=checksum)
    assert file.key == "src/utf.txt"
    assert file.name == "utf.txt"
    assert file.mimetype == PLAIN
    assert file.dataset == DefaultDataset.name
    assert file.id == file_id
    assert file.size == 19
    file_dict = file.to_dict()
    assert "created_at" in file_dict
    assert "updated_at" in file_dict
    assert file_dict["size"] == 19
    assert file_dict["key"] == "src/utf.txt"
    assert file_dict["dataset"] == "default"
    assert file_dict["checksum"] == "2928064cd9a743af30b720634dcffacdd84de23d"

    entity = file.to_entity()
    assert entity.id == file_id
    assert entity.dataset.name == file.dataset
    assert entity.first("fileName") == file.name
    assert entity.first("contentHash") == checksum
    assert entity.first("fileSize") == "19"
    assert entity.first("mimeType") == PLAIN


def test_model_dataset(fixtures_path):
    config = fixtures_path / "lake/test_dataset/config.yml"
    dataset = DatasetModel.from_yaml_uri(config)
    assert isinstance(dataset, Dataset)
    assert Dataset(**dataset.model_dump())
