"""`Assembly` – an artifact file put together from parts beside its key and
moved into place whole."""

from anystore.store import get_store

from ftm_lakehouse.repository.artifacts import Assembly


def _part(path, data: bytes) -> str:
    path.write_bytes(data)
    return str(path)


def test_assembly(tmp_path):
    store = get_store(str(tmp_path / "store"), serialization_mode="raw")
    store.put("a.csv", b"old")
    parts = [_part(tmp_path / f"p{i}", f"{i};".encode()) for i in range(3)]

    assembly = Assembly(store, "a.csv")
    for part in parts:
        assembly.append(part)
        # a part is gone once appended, and readers still see the old file
        assert not (tmp_path / part).exists()
        assert store.get("a.csv") == b"old"
    assembly.append(str(tmp_path / "never-written"))
    assembly.commit()
    assert store.get("a.csv") == b"0;1;2;"
    assert not store.exists("a.csv.tmp")


def test_assembly_empty_and_abort(tmp_path):
    store = get_store(str(tmp_path / "store"), serialization_mode="raw")

    # no part: an eager file is what `empty` writes, a lazy one is not written
    Assembly(store, "eager", lambda key: store.put(key, b"header")).commit()
    assert store.get("eager") == b"header"
    Assembly(store, "lazy").commit()
    assert not store.exists("lazy")

    # a failed run drops what it appended and leaves the file as it was
    store.put("b.csv", b"old")
    assembly = Assembly(store, "b.csv")
    assembly.append(_part(tmp_path / "p", b"new"))
    assembly.abort()
    assert store.get("b.csv") == b"old"
    assert not store.exists("b.csv.tmp")
