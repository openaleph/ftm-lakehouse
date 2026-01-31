from anystore import get_store
from followthemoney import StatementEntity
from ftmq.util import DEFAULT_DATASET
from rigour.mime.types import PLAIN

from ftm_lakehouse.model.file import File


def test_model_file(fixtures_path):
    checksum = "2928064cd9a743af30b720634dcffacdd84de23d"
    file_id = "file-4ab9a436dae7c583dbc437dbc7f014d8d084c081"
    store = get_store(fixtures_path)

    file = File.from_info(store.info("src/utf.txt"), checksum=checksum)
    assert file.checksum == checksum
    assert file.key == "src/utf.txt"
    assert file.name == "utf.txt"
    assert file.mimetype == PLAIN
    assert file.dataset == DEFAULT_DATASET == "default"
    assert file.id == file_id
    assert file.size == 19
    file_dict = file.to_dict()
    assert "created_at" in file_dict
    assert "updated_at" in file_dict
    assert file_dict["id"] == file_id
    assert file_dict["size"] == 19
    assert file_dict["key"] == "src/utf.txt"
    assert file_dict["dataset"] == "default"
    assert file_dict["checksum"] == "2928064cd9a743af30b720634dcffacdd84de23d"


def test_model_file_extra_fields():
    """Test that unknown fields are collected into the extra dict."""
    file = File(
        dataset="test",
        checksum="abc123",
        key="test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
        foo="bar",
        custom_field="value",
    )
    assert file.extra == {"foo": "bar", "custom_field": "value"}

    # Test merging with existing extra dict
    file2 = File(
        dataset="test",
        checksum="abc123",
        key="test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
        extra={"existing": "data"},
        new_field="new_value",
    )
    assert file2.extra == {"existing": "data", "new_field": "new_value"}

    # Test normal creation without extra fields
    file3 = File(
        dataset="test",
        checksum="abc123",
        key="test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
    )
    assert file3.extra == {}


def test_model_file_entity():
    """Test generation of entities from file"""
    file = File(
        dataset="test",
        checksum="abc123",
        key="test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
        title="Document title",
        crawler="memorious",
        foo="bar",
    )
    assert file.extra["foo"] == "bar"
    entity = file.to_entity()
    assert isinstance(entity, StatementEntity)
    assert entity.id == "file-abe3bdf54822f196577946c4c3e2f987d3fba7e9"
    assert entity.dataset.name == "test"
    assert entity.datasets == {"test"}
    assert entity.schema.name == "PlainText"
    assert entity.first("fileName") == "test.txt"
    assert entity.first("contentHash") == "abc123"
    assert entity.first("mimeType") == PLAIN
    assert entity.first("title") == "Document title"
    assert entity.first("fileSize") == "100"
    assert entity.first("crawler") == "memorious"


def test_model_file_parents():
    """Test parent folder graph for file entities"""
    file = File(
        dataset="test",
        checksum="abc123",
        key="foo/bar/test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
    )
    folders = list(file.make_parents())
    assert len(folders) == 2
    assert folders[0].id == "folder-0beec7b5ea3f0fdbc95d0dd47f3c5bc275da8a33"
    assert folders[0].schema.name == "Folder"
    assert folders[0].dataset.name == "test"
    assert folders[0].first("fileName") == "foo"
    assert folders[1].id == "folder-8f8fe7be5773bb7f931647ee8ccf43a386569afa"
    assert folders[1].first("fileName") == "bar"
    assert folders[1].first("parent") == folders[0].id
    assert file.parent == folders[1].id
    entity = file.to_entity()
    assert entity.first("parent") == folders[1].id

    # no parents
    file = File(
        dataset="test",
        checksum="abc123",
        key="test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
    )
    assert len(list(file.make_parents())) == 0

    # test weird (trailing WS) but valid folder path
    file = File(
        dataset="test",
        checksum="abc123",
        key="Foo / è e/test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
    )
    folders = list(file.make_parents())
    assert len(folders) == 2
    assert folders[0].first("fileName") == "Foo "
    assert folders[1].first("fileName") == " è e"
