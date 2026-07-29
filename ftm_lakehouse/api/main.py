from anystore.api.routes import router as archive_router
from anystore.exceptions import DoesNotExist
from anystore.logging import get_logger
from anystore.store import get_store
from anystore.util import ensure_uri, uri_to_path
from fastapi import FastAPI, Request, Response
from fastapi.responses import JSONResponse
from followthemoney.dataset.util import dataset_name_check
from putfs import api as putfs
from starlette.middleware.base import BaseHTTPMiddleware, RequestResponseEndpoint

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
                try:
                    dataset_name_check(dataset)
                except ValueError:
                    pass
                else:
                    ensure_zfs_dataset(settings.zfs_pool, dataset)
        return await call_next(request)


class StaticHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(
        self, request: Request, call_next: RequestResponseEndpoint
    ) -> Response:
        response = await call_next(request)
        response.headers["X-Lakehouse-Version"] = __version__
        for key, value in api_settings.static_headers.items():
            response.headers[key] = value
        return response


async def _not_found_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


async def _bad_request_handler(_: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=400, content={"detail": str(exc)})


def get_app(lake_uri: str | None = None) -> FastAPI:
    uri = ensure_uri(lake_uri or settings.uri)
    app = FastAPI(docs_url=None, redoc_url="/")
    app.state.lake = get_lakehouse(uri)

    # lakehouse api
    app.include_router(entities_router)
    app.include_router(journal_router)
    app.include_router(operations_router)

    # blob storage api
    if uri.startswith("file://"):
        # local fs, so we can use putfs. Mount the whole Starlette app so putfs
        # keeps its own exception handlers; its catch-all /{key:path} sits
        # behind the /{dataset}/_api/* routes above.
        putfs.ROOT = uri_to_path(uri).resolve()
        app.mount("/", putfs.app)
    else:
        app.state.store = get_store(uri)
        app.include_router(archive_router)

    # middlewares
    app.add_middleware(StaticHeadersMiddleware)
    if settings.on_zfs and settings.zfs_pool:
        app.add_middleware(ZfsEnsureMiddleware)

    # error handlers
    app.add_exception_handler(DoesNotExist, _not_found_handler)
    app.add_exception_handler(FileNotFoundError, _not_found_handler)
    app.add_exception_handler(ValueError, _bad_request_handler)

    return app


app = get_app()
