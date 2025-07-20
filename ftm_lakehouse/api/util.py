from anystore.logging import get_logger
from anystore.store.fs import DoesNotExist
from anystore.util import clean_dict
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ftm_lakehouse import __version__
from ftm_lakehouse.lake.base import get_lakehouse
from ftm_lakehouse.model import File
from ftm_lakehouse.settings import Settings

settings = Settings()
log = get_logger(__name__)
lake = get_lakehouse()
DEFAULT_ERROR = HTTPException(404)
BASE_HEADER = {"x-ftm-lakehouse-version": __version__}


def get_file_header(file: File) -> dict[str, str]:
    return clean_dict(
        {
            **BASE_HEADER,
            "x-ftm-lakehouse-dataset": file.dataset,
            "x-ftm-lakehouse-sha1": file.checksum,
            "x-ftm-lakehouse-name": file.name,
            "x-ftm-lakehouse-size": str(file.size),
            "x-mimetype": file.mimetype,
            "content-type": file.mimetype,
            "content-length": str(file.size),
        }
    )


class Context(BaseModel):
    dataset: str
    content_hash: str
    file: File

    @property
    def headers(self) -> dict[str, str]:
        return get_file_header(self.file)


class Errors:
    def __enter__(self):
        pass

    def __exit__(self, exc_cls, exc, _):
        if exc_cls is not None:
            log.error(f"{exc_cls.__name__}: `{exc}`")
            if not settings.debug:
                # always just 404 for information hiding
                raise DEFAULT_ERROR
            else:
                if exc_cls == DoesNotExist:
                    raise DEFAULT_ERROR
                raise exc


def get_file_info(dataset: str, content_hash: str) -> File:
    archive = lake.get_dataset(dataset).archive
    return archive.lookup_file(content_hash)


def ensure_path_context(dataset: str, content_hash: str) -> Context:
    with Errors():
        return Context(
            dataset=dataset,
            content_hash=content_hash,
            file=get_file_info(dataset, content_hash),
        )


def stream_file(ctx: Context) -> StreamingResponse:
    archive = lake.get_dataset(ctx.dataset).archive
    file = archive.lookup_file(ctx.content_hash)
    return StreamingResponse(
        archive.stream_file(file),
        headers=ctx.headers,
        media_type=ctx.file.mimetype,
    )
