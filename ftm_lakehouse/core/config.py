"""Configuration loading utilities."""

import yaml
from anystore.store.base import BaseStore
from anystore.types import SDict
from anystore.util import dict_merge

from ftm_lakehouse.core.conventions import path


def load_config(storage: BaseStore, **data) -> SDict:
    """
    Load a catalog or dataset configuration.

    Args:
        storage: Base storage to load config from
        data: Additional data to override

    Returns:
        data
    """
    if storage.exists(path.CONFIG):
        config = storage.get(path.CONFIG, deserialization_func=yaml.safe_load)
    else:
        config = {"name": data.get("name") or "catalog"}
    config = dict_merge(config, data)
    config["uri"] = storage.uri
    return config
