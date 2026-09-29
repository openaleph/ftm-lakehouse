"""ASGI entrypoints: ``granian ftm_lakehouse.api:app``, and
``ftm_lakehouse.api:zfs_app`` for ``ftm-lakehouse zfs serve``."""

from fastapi import FastAPI

from ftm_lakehouse.api.main import get_app, get_zfs_app

__all__ = ["app", "get_app", "get_zfs_app"]


def __getattr__(name: str) -> FastAPI:
    """Build the app on attribute access, not at import time.

    A client process runs in api mode (``LAKEHOUSE_URI=http://…``) and
    imports this package for [`get_app`][ftm_lakehouse.api.main.get_app]
    or the dependencies – it never serves blobs, so constructing the app
    eagerly would raise on a lake uri that isn't a local path.
    """
    if name == "app":
        return get_app()
    if name == "zfs_app":
        return get_zfs_app()
    raise AttributeError(name)
