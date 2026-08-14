"""Ensure API: trigger ZFS creation if needed"""

from fastapi import APIRouter
from fastapi.responses import PlainTextResponse

router = APIRouter()


@router.post("/{dataset}/_api/ensure")
async def ensure() -> PlainTextResponse:
    """Trigger the middleware that ensures the zfs dataset if configured. This
    endpoint should be called at the first ever action made to a dataset."""

    return PlainTextResponse("ok")
