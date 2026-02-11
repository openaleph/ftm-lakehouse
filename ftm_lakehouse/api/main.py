from anystore.api.routes import router as storage_router
from anystore.exceptions import DoesNotExist
from anystore.store import get_store
from fastapi import Depends, FastAPI, Request
from fastapi.responses import JSONResponse

from ftm_lakehouse.api.auth import ensure_auth
from ftm_lakehouse.api.routes.journal import router as journal_router
from ftm_lakehouse.api.routes.operations import router as operations_router
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.lake import get_lakehouse

settings = Settings()


async def _not_found_handler(_request: Request, exc: Exception) -> JSONResponse:
    return JSONResponse(status_code=404, content={"detail": str(exc)})


def get_app(lake_uri: str | None = None) -> FastAPI:
    app = FastAPI(docs_url=None, redoc_url="/")
    app.state.store = get_store(lake_uri or settings.uri)
    app.state.lake = get_lakehouse(lake_uri or settings.uri)
    app.state.journal_uri = settings.journal_uri
    app.include_router(storage_router, dependencies=[Depends(ensure_auth)])
    app.include_router(journal_router, dependencies=[Depends(ensure_auth)])
    app.include_router(operations_router, dependencies=[Depends(ensure_auth)])
    app.add_exception_handler(DoesNotExist, _not_found_handler)
    app.add_exception_handler(FileNotFoundError, _not_found_handler)
    return app


app = get_app()
