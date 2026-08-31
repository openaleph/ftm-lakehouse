from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.util import ensure_uri, uri_to_path
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from putfs import api as putfs
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

from ftm_lakehouse.api.routes.ensure import router as ensure_router
from ftm_lakehouse.api.routes.entities import router as entities_router
from ftm_lakehouse.api.routes.journal import router as journal_router
from ftm_lakehouse.api.routes.operations import router as operations_router
from ftm_lakehouse.core.settings import ApiSettings, Settings, __version__
from ftm_lakehouse.core.zfs import ensure_zfs_dataset
from ftm_lakehouse.lake import get_lakehouse

settings = Settings()
api_settings = ApiSettings()
log = get_logger(__name__)

_WRITE_METHODS = {"PUT", "POST", "DELETE", "PATCH"}


class ZfsEnsureMiddleware(BaseHTTPMiddleware):
    """Ensure ZFS datasets exist before any write hits storage."""

    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        if request.method in _WRITE_METHODS:
            path = request.url.path.lstrip("/")
            dataset = path.split("/")[0] if path else None
            if dataset:
                ensure_zfs_dataset(settings.zfs_pool, dataset)
        return await call_next(request)


async def _not_found_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


async def _bad_request_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


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

    # error handlers
    app.add_exception_handler(DoesNotExist, _not_found_handler)
    app.add_exception_handler(FileNotFoundError, _not_found_handler)
    app.add_exception_handler(ValueError, _bad_request_handler)

    return app


app = get_app()
