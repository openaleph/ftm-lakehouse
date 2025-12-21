from functools import lru_cache
from pathlib import Path

from anystore.util import make_data_checksum
from followthemoney import StatementEntity, model
from ftmq.types import StatementEntities
from ftmq.util import make_entity

MAX_LRU = 10_000


def make_file_id(path: str, checksum: str) -> str:
    """
    Compute a file id based on (relative) path and its checksum. This is used
    for Document Entity ids.
    """
    return f"file-{make_data_checksum((path, checksum))}"


def make_folder_id(name: str, parent_id: str | None = None) -> str:
    """
    Compute a folder id based on its name and optional parent folder id. This is
    used for Folder Entity ids.
    """
    key = name
    if parent_id:
        key = (parent_id, name)
    return f"folder-{make_data_checksum(key)}"


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
    yield make_folder(path.name, parent_id)
