from anystore.util import guess_mimetype
from rigour.mime import normalize_mimetype
from rigour.mime.types import DEFAULT, HTML, PDF, WORD

from ftm_lakehouse.helpers import file


def test_helpers_file():
    # mime_to_schema returns a Schema object, compare names
    assert file.mime_to_schema(HTML).name == "HyperText"
    assert file.mime_to_schema(PDF).name == "Pages"
    assert file.mime_to_schema(WORD).name == "Pages"
    assert file.mime_to_schema(DEFAULT).name == "Document"
    assert file.mime_to_schema("foo").name == "Document"


NAMES = [
    "doc.pdf",
    "doc.PDF",
    "archive.tar.gz",
    "ARCHIVE.TAR.GZ",
    "2024.01.15 report.docx",
    "sheet.xlsx",
    "noextension",
    "x.unknownextension",
    "",
]


def test_helpers_file_guess_mime():
    """The memoised guess answers exactly what the uncached one does.

    Keyed on the last two suffixes – the type suffix plus a possible
    encoding one – which is what ``mimetypes`` looks at. Keying on the whole
    extension chain matters: ``.TAR.GZ`` is an encoding suffix `mimetypes`
    only recognizes lowercased, so the cased spellings resolve differently
    and must not collapse into one entry.
    """
    for name in NAMES:
        assert file.guess_mime(name) == guess_mimetype(name), name
    assert file.guess_mime("archive.tar.gz") != file.guess_mime("ARCHIVE.TAR.GZ")

    # one entry per extension, not per name
    file._guess_suffix_mime.cache_clear()
    assert file.guess_mime("a/one.pdf") == file.guess_mime("b/two.pdf") == PDF
    info = file._guess_suffix_mime.cache_info()
    assert (info.misses, info.hits) == (1, 1)


def test_helpers_file_normalize_mime():
    """The memoised normalizer answers what rigour's does."""
    for mime in ("application/pdf", "APPLICATION/PDF", "nonsense", PDF, HTML):
        assert file.normalize_mime(mime) == normalize_mimetype(mime), mime
    assert file.pick_mime(["nonsense", PDF]) == PDF
    assert file.pick_mime([], "doc.pdf") == DEFAULT  # a name is not a mime type
    assert file.pick_mime([], PDF) == PDF
    assert file.pick_mime([]) == DEFAULT


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
