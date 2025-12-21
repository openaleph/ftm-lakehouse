from anystore import serialize

from ftm_lakehouse.model.crud import Crud, CrudAction, CrudResource


def test_model_crud():
    crud = Crud(
        action=CrudAction.UPSERT,
        resource=CrudResource.ARCHIVE,
    )
    assert crud.action == CrudAction.UPSERT
    assert crud.resource == CrudResource.ARCHIVE
    assert crud.payload is None
    assert crud.created_at is not None

    # arbitrary payload serialization & restore
    payload = {
        "checksum": "abc123",
        "key": "documents/file.pdf",
        "size": 1024,
        "nested": {"foo": "bar"},
        "list": [1, 2, 3],
    }
    original = Crud(
        action=CrudAction.UPSERT,
        resource=CrudResource.ARCHIVE,
        payload=payload,
    )

    data = serialize.to_store(original)
    restored = serialize.from_store(data, model=Crud)

    assert restored.action == original.action
    assert restored.resource == original.resource
    assert restored.payload == original.payload
    assert restored.payload["nested"]["foo"] == "bar"
    assert restored.payload["list"] == [1, 2, 3]
    assert restored.created_at is not None
    assert restored.created_at == original.created_at
