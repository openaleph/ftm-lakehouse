from functools import cache, lru_cache
from pathlib import Path
from typing import Any, Iterable, Iterator

from anystore.util import guess_mimetype, make_data_checksum
from followthemoney import Schema, StatementEntity, model
from ftmq.types import StatementEntities
from ftmq.util import make_entity
from rigour.mime import normalize_mimetype, types

from ftm_lakehouse.core.settings import CHECKSUM_ALGORITHM

MAX_LRU = 10_000


def make_file_id(path: str, checksum: str) -> str:
    """
    Compute a file id based on (relative) path and its checksum. This is used
    for Document Entity ids.
    """
    return f"file-{make_data_checksum((path, checksum), algorithm=CHECKSUM_ALGORITHM)}"


def make_folder_id(name: str, parent_id: str | None = None) -> str:
    """
    Compute a folder id based on its name and optional parent folder id. This is
    used for Folder Entity ids.
    """
    key = name
    if parent_id:
        key = (parent_id, name)
    return f"folder-{make_data_checksum(key, algorithm=CHECKSUM_ALGORITHM)}"


@lru_cache(MAX_LRU)
def make_folder(
    name: str, parent_id: str | None = None, dataset: str | None = None
) -> StatementEntity:
    """
    Create a Folder Entity
    """
    folder = make_entity(
        {"id": make_folder_id(name, parent_id), "schema": model["Folder"]},
        StatementEntity,
        dataset,
    )
    # FIXME we don't want to clean the name here as leading/trailing WS
    # unfortunately is a valid folder name on some systems. No idea if this will
    # haunt us back later.
    folder.add("fileName", name, cleaned=True)
    folder.add("parent", parent_id)
    return folder


def make_folders(path: Path, dataset: str | None = None) -> StatementEntities:
    parent_id = None
    for parent in reversed(path.parents):
        if parent.name:
            folder = make_folder(parent.name, parent_id, dataset)
            parent_id = folder.id
            yield folder
    yield make_folder(path.name, parent_id, dataset)


MIME_SCHEMAS = {
    (types.PDF, types.DOCX, types.WORD): model["Pages"],
    (types.HTML, types.XML): model["HyperText"],
    (types.CSV, types.EXCEL, types.XLS, types.XLSX): model["Table"],
    (types.PNG, types.GIF, types.JPEG, types.TIFF, types.DJVU, types.PSD): model[
        "Image"
    ],
    (types.OUTLOOK, types.OPF, types.RFC822): model["Email"],
    (types.PLAIN, types.RTF): model["PlainText"],
}


@lru_cache(MAX_LRU)
def normalize_mime(mimetype: str) -> str:
    """`rigour.mime.normalize_mimetype`, memoised.

    Normalizing costs ~30µs, and the export sweep asks per document – for a
    handful of distinct mime types over a whole dataset. Memoising it is the
    difference between a documents export bounded by this call and one that
    is not.

    Args:
        mimetype: Any mime type spelling.

    Returns:
        The normalized mime type, `rigour.mime.types.DEFAULT` if it is not one.
    """
    return normalize_mimetype(mimetype)


@lru_cache(MAX_LRU)
def _guess_suffix_mime(suffix: str) -> str:
    """Guess the mime type of a file name ending in ``suffix``."""
    return guess_mimetype(f"x{suffix}")


def guess_mime(name: str) -> str:
    """Guess a mime type off a file name, memoised on its extension.

    `anystore.util.guess_mimetype` is extension-based but costs per call, and
    file names are near-unique while their extensions are not – so the cache is
    keyed on the extension rather than the name. The last *two* suffixes make
    the key, which is what ``mimetypes`` itself looks at: the type suffix plus
    a possible encoding one (``.tar.gz``).

    Args:
        name: A file name (`get_filename`), not a path.

    Returns:
        The normalized mime type, `rigour.mime.types.DEFAULT` if the name
        does not name one.
    """
    return _guess_suffix_mime("".join(Path(name).suffixes[-2:]))


@cache
def mime_to_schema(mimetype: str) -> Schema:
    """
    Map a mimetype to a
    [FollowTheMoney](https://followthemoney.tech/explorer/schemata/Document/)
    File schema.

    Examples:
        >>> mime_to_schema("application/pdf")
        "Pages"

    Args:
        mimetype: The mimetype (will be normalized using `rigour.mime`)

    Returns:
        The schema name as string
    """
    mimetype = normalize_mime(mimetype)
    for mtypes, schema in MIME_SCHEMAS.items():
        if mimetype in mtypes:
            if schema is not None:
                return schema
    return model["Document"]


def pick_mime(mimetypes: Iterable[str], default: str | None = None) -> str:
    """
    Pick a mime type from given input. Useful to sort out
    application/ocet-stream if there is some other available.

    Normalizes through `normalize_mime`, so a caller in a per-row loop pays
    the real normalization once per distinct spelling.
    """
    for mime in mimetypes:
        mime = normalize_mime(mime)
        if mime != types.DEFAULT:
            return mime
    if default:
        return normalize_mime(default)
    return types.DEFAULT


def get_filename(d: dict[str, Any]) -> str:
    """Pick the file name of an entity dict: its ``fileName``, else its caption.

    Args:
        d: Entity dict, as ``EntityProxy.to_dict`` (and all descendants)
            returns.

    Returns:
        The file name, caption, or the schema string when the entity has neither.
    """
    file_names = d.get("properties", {}).get("fileName", [])
    if file_names:
        return str(file_names[0])
    return str(d.get("caption") or d.get("schema") or "")


class FolderTree:
    """Folder ids to the paths a document's ``parent`` resolves against.

    A plain accumulator: [`put`][FolderTree.put] takes one folder's name and
    parents, [`paths`][FolderTree.paths] walks every chain once the last one
    is in. Resolution cannot happen per folder – a path is the chain of its
    ancestors' names, and nothing orders a stream so that they arrive first –
    so whoever fills it has to finish before it answers.

    Filled by the export sweep out of its staged folder rows
    (``ParentsRun.resolve``), the only thing that needs folder paths –
    they exist to be written into ``documents.csv``.

    Example:
        ```python
        tree = FolderTree()
        tree.put("folder-ab12", "sub", ["folder-cd34"])
        tree.put("folder-cd34", "root")
        tree.paths()  # {"folder-ab12": "root/sub", "folder-cd34": "root"}
        ```
    """

    def __init__(self) -> None:
        self._folders: dict[str, tuple[str, str | None]] = {}
        self._paths: dict[str, str] | None = None

    def __len__(self) -> int:
        return len(self._folders)

    def put(self, folder: str, name: str, parents: Iterable[str] | None = None) -> None:
        """Register one folder under the name it appears in a path as.

        Args:
            folder: The folder's entity id.
            name: Its name (`get_filename`) – one segment of a path.
            parents: Its own parents. A path is one chain, so a folder living
                in several places takes the first, as a document does.
        """
        self._folders[folder] = (name, next(iter(parents or ()), None))
        self._paths = None

    def paths(self) -> dict[str, str]:
        """Every registered folder resolved to its path, memoised.

        Returns:
            Mapping of folder id to complete path (e.g. ``"root/sub/folder"``).
        """
        if self._paths is None:
            paths: dict[str, str] = {}
            for folder in self._folders:
                # climb until a resolved ancestor or a root, then resolve the
                # climbed chain top-down off that – each folder walked once
                chain: list[str] = []
                seen: set[str] = set()
                current: str | None = folder
                while current and current in self._folders and current not in paths:
                    if current in seen:  # a cycle: each of its folders on its own
                        paths.update({node: self._path(node) for node in chain})
                        break
                    seen.add(current)
                    chain.append(current)
                    current = self._folders[current][1]
                else:
                    prefix = paths.get(current) if current else None
                    for node in reversed(chain):
                        name = self._folders[node][0]
                        prefix = name if prefix is None else f"{prefix}/{name}"
                        paths[node] = prefix
            self._paths = paths
        return self._paths

    def folders(self) -> Iterator[tuple[str, str, str]]:
        """Every registered folder as ``(id, name, path)``."""
        paths = self.paths()
        for folder, (name, _) in self._folders.items():
            yield folder, name, paths[folder]

    def _path(self, folder: str) -> str:
        """Walk one folder up its parent chain into a path."""
        parts: list[str] = []
        current: str | None = folder
        seen: set[str] = set()
        while current and current in self._folders:
            if current in seen:
                break  # cycle detection
            seen.add(current)
            name, current = self._folders[current]
            parts.append(name)
        return "/".join(reversed(parts))
