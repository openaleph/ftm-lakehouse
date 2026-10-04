from rigour.mime.types import DEFAULT, HTML, PDF, WORD

from ftm_lakehouse.helpers import file


def test_helpers_file():
    # mime_to_schema returns a Schema object, compare names
    assert file.mime_to_schema(HTML).name == "HyperText"
    assert file.mime_to_schema(PDF).name == "Pages"
    assert file.mime_to_schema(WORD).name == "Pages"
    assert file.mime_to_schema(DEFAULT).name == "Document"
    assert file.mime_to_schema("foo").name == "Document"


def test_helpers_file_folder_tree():
    """Paths are the chain of ancestor names, whatever order they arrive in."""
    tree = file.FolderTree()
    tree.put("f2", "b", ["f1"])
    tree.put("f1", "a")
    assert len(tree) == 2
    assert tree.paths() == {"f1": "a", "f2": "a/b"}

    # memoised, and dropped again when the tree grows
    assert tree.paths() is tree.paths()
    tree.put("f3", "c", ["f2"])
    assert tree.paths()["f3"] == "a/b/c"

    # several parents are one chain – the first
    tree.put("f4", "d", ["f1", "f3"])
    assert tree.paths()["f4"] == "a/d"

    # an unknown parent is a root, a cycle breaks
    tree = file.FolderTree()
    tree.put("f1", "a", ["nope"])
    tree.put("f2", "b", ["f3"])
    tree.put("f3", "c", ["f2"])
    assert tree.paths() == {"f1": "a", "f2": "c/b", "f3": "b/c"}
