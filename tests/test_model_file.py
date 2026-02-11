from anystore import get_store
from followthemoney import StatementEntity
from ftmq.util import DEFAULT_DATASET
from rigour.mime.types import PLAIN

from ftm_lakehouse.model.file import File

FAKE_CHECKSUM = "5b93539659eb03f4c5dfa64f342a667db6946913ce4d3243f4846bbe37f391d9"


def test_model_file(fixtures_path):
    checksum = FAKE_CHECKSUM
    file_id = "file-df082aa01243e36fed47a2b1de2bd563ad6dae11449431d9a1ef3795c63e0427"
    store = get_store(fixtures_path)

    file = File.from_info(store.info("src/utf.txt"), checksum=checksum)
    assert file.checksum == checksum
    assert file.key == "src/utf.txt"
    assert file.name == "utf.txt"
    assert file.mimetype == PLAIN
    assert file.dataset == DEFAULT_DATASET == "default"
    assert file.id == file_id
    assert file.size == 19
    assert file.store == "lakehouse://"
    assert file.uri == "lakehouse:///src/utf.txt"
    file_dict = file.to_dict()
    assert "created_at" in file_dict
    assert "updated_at" in file_dict
    assert file_dict["id"] == file_id
    assert file_dict["size"] == 19
    assert file_dict["key"] == "src/utf.txt"
    assert file_dict["dataset"] == "default"
    assert file_dict["checksum"] == checksum


def test_model_file_extra_fields():
    """Test that unknown fields are collected into the extra dict."""
    file = File(
        dataset="test",
        checksum=FAKE_CHECKSUM,
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
        checksum=FAKE_CHECKSUM,
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
        checksum=FAKE_CHECKSUM,
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
        checksum=FAKE_CHECKSUM,
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
    assert (
        entity.id
        == "file-725fa66861ba50448ace5adbb6474165ed07540a08f0d07f2fd723e80b172461"
    )
    assert entity.dataset.name == "test"
    assert entity.datasets == {"test"}
    assert entity.schema.name == "PlainText"
    assert entity.first("fileName") == "test.txt"
    assert entity.first("contentHash") == FAKE_CHECKSUM
    assert entity.first("mimeType") == PLAIN
    assert entity.first("title") == "Document title"
    assert entity.first("fileSize") == "100"
    assert entity.first("crawler") == "memorious"


def test_model_file_parents():
    """Test parent folder graph for file entities"""
    file = File(
        dataset="test",
        checksum=FAKE_CHECKSUM,
        key="foo/bar/test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
    )
    folders = list(file.make_parents())
    assert len(folders) == 2
    assert (
        folders[0].id
        == "folder-2c26b46b68ffc68ff99b453c1d30413413422d706483bfa0f98a5e886266e7ae"
    )
    assert folders[0].schema.name == "Folder"
    assert folders[0].dataset.name == "test"
    assert folders[0].first("fileName") == "foo"
    assert (
        folders[1].id
        == "folder-dbbc59510ed49771423ffcd2ddbaaa7846666628fee87ee5e61656a93273b3f5"
    )
    assert folders[1].first("fileName") == "bar"
    assert folders[1].first("parent") == folders[0].id
    assert file.parent == folders[1].id
    entity = file.to_entity()
    assert entity.first("parent") == folders[1].id

    # no parents
    file = File(
        dataset="test",
        checksum=FAKE_CHECKSUM,
        key="test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
    )
    assert len(list(file.make_parents())) == 0

    # test weird (trailing WS) but valid folder path
    file = File(
        dataset="test",
        checksum=FAKE_CHECKSUM,
        key="Foo / è e/test.txt",
        name="test.txt",
        store="s3://bucket",
        size=100,
    )
    folders = list(file.make_parents())
    assert len(folders) == 2
    assert folders[0].first("fileName") == "Foo "
    assert folders[1].first("fileName") == " è e"
