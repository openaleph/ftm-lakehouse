from anystore.logging import get_logger
from anystore.store.fs import DoesNotExist
from anystore.util import clean_dict
from fastapi import HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from ftm_lakehouse import __version__, lake
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.model import File

settings = Settings()
log = get_logger(__name__)

DEFAULT_ERROR = HTTPException(404)
BASE_HEADER = {"x-ftm-lakehouse-version": __version__}


def get_file_header(file: File) -> dict[str, str]:
    return clean_dict(
        {
            **BASE_HEADER,
            "x-ftm-lakehouse-dataset": file.dataset,
            "x-ftm-lakehouse-sha1": file.checksum,
            "x-ftm-lakehouse-name": file.name,
            "x-ftm-lakehouse-path": file.key,
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
    try:
        archive = lake.get_archive(dataset)
        return archive.get(content_hash)
    except FileNotFoundError:
        raise DEFAULT_ERROR


def ensure_path_context(dataset: str, content_hash: str) -> Context:
    with Errors():
        return Context(
            dataset=dataset,
            content_hash=content_hash,
            file=get_file_info(dataset, content_hash),
        )


def stream_file(ctx: Context) -> StreamingResponse:
    archive = lake.get_archive(ctx.dataset)
    file = archive.get(ctx.content_hash)
    if file is None:
        raise DEFAULT_ERROR
    stream = archive.stream(file)
    return StreamingResponse(
        stream,
        headers=ctx.headers,
        media_type=ctx.file.mimetype,
    )
