from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.util import ensure_uri, uri_to_path
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from putfs import api as putfs
from starlette.types import ASGIApp, Receive, Scope, Send

from ftm_lakehouse.api.routes.ensure import router as ensure_router
from ftm_lakehouse.api.routes.entities import router as entities_router
from ftm_lakehouse.api.routes.journal import router as journal_router
from ftm_lakehouse.api.routes.operations import router as operations_router
from ftm_lakehouse.api.routes.zfs import router as zfs_router
from ftm_lakehouse.core.settings import ApiSettings, Settings, __version__
from ftm_lakehouse.core.zfs import ensure_zfs_dataset
from ftm_lakehouse.lake import get_lakehouse

settings = Settings()
api_settings = ApiSettings()
log = get_logger(__name__)

_WRITE_METHODS = {"PUT", "POST", "DELETE", "PATCH"}


class ZfsEnsureMiddleware:
    """Ensure ZFS datasets exist before any write hits storage.

    Plain ASGI rather than ``BaseHTTPMiddleware``, which re-wraps every
    chunk of a streamed body – replication streams pass through here.
    """

    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope["method"] in _WRITE_METHODS:
            path = scope["path"].lstrip("/")
            dataset = path.split("/")[0] if path else None
            # A replication receive creates its datasets itself – ensuring
            # them first would leave the full receive nothing to create.
            if dataset and not path.startswith(f"{dataset}/_api/zfs/"):
                ensure_zfs_dataset(settings.zfs_pool, dataset)
        await self.app(scope, receive, send)


async def _not_found_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


async def _bad_request_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


def _add_error_handlers(app: FastAPI) -> None:
    app.add_exception_handler(DoesNotExist, _not_found_handler)
    app.add_exception_handler(FileNotFoundError, _not_found_handler)
    app.add_exception_handler(ValueError, _bad_request_handler)


def _zfs_pool() -> str:
    if not settings.zfs_pool:
        raise RuntimeError("The ZFS api needs `LAKEHOUSE_ZFS_POOL`")
    return settings.zfs_pool


def get_app(lake_uri: str | None = None) -> FastAPI:
    uri = ensure_uri(lake_uri or settings.uri)
    app = FastAPI(
        debug=settings.debug,
        docs_url=None,
        redoc_url="/",
        version=__version__,
        title=api_settings.title,
        description=api_settings.description,
        contact=api_settings.contact.model_dump(),
    )
    app.state.lake = get_lakehouse(uri)

    # lakehouse api
    app.include_router(ensure_router)
    app.include_router(entities_router)
    app.include_router(journal_router)
    app.include_router(operations_router)
    if settings.zfs_api:
        app.state.zfs_pool = _zfs_pool()
        app.include_router(zfs_router)

    # blob storage api
    if uri.startswith("file://"):
        # Mount the whole Starlette app so putfs keeps its own exception
        # handlers; its catch-all /{key:path} sits behind the /{dataset}/_api/*
        # routes above.
        putfs.ROOT = uri_to_path(uri).resolve()
        app.mount("/", putfs.app)
    else:
        raise RuntimeError(f"Unsupported blob storage for api mode: `{uri}`")

    # middlewares
    if settings.on_zfs and settings.zfs_pool:
        app.add_middleware(ZfsEnsureMiddleware)

    _add_error_handlers(app)

    return app


def get_zfs_app() -> FastAPI:
    """Serve only the ZFS replication routes (``ftm-lakehouse zfs serve``).

    For hosts that don't expose the lakehouse api. No blob storage, no
    authentication – bind it to an interface only trusted peers reach. The
    catalog (``LAKEHOUSE_URI``) is still needed: a snapshot for a pull
    flushes the dataset's journal first.
    """
    app = FastAPI(
        debug=settings.debug,
        docs_url=None,
        redoc_url="/",
        version=__version__,
        title=f"{api_settings.title} – ZFS replication",
    )
    app.state.zfs_pool = _zfs_pool()
    app.state.lake = get_lakehouse()
    app.include_router(zfs_router)
    _add_error_handlers(app)
    return app
