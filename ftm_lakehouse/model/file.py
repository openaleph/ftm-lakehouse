"""Document metadata model – a row of the documents csv."""

from datetime import datetime
from typing import Any, Generator, Self, TypeAlias

from anystore.model.base import BaseModel
from anystore.types import HttpUrlStr
from followthemoney import EntityProxy
from pydantic import field_validator

from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.file import get_filename, guess_mime, pick_mime
from ftm_lakehouse.util import validate_checksum

settings = Settings()


class Document(BaseModel):
    """Condensed file metadata for flat csv streaming"""

    id: str
    checksum: str
    name: str
    mimetype: str
    path: str | None = None
    size: int | None = None
    updated_at: datetime | None = None
    public_url: HttpUrlStr | None = None

    @field_validator("checksum")
    @classmethod
    def check_checksum(cls, v: str) -> str:
        return validate_checksum(v, settings.allow_sha1_documents)

    @classmethod
    def from_entity(cls, e: EntityProxy, public_url: str | None = None) -> Self:
        return cls.from_entity_dict(e.to_dict(), public_url)

    @classmethod
    def from_entity_dict(cls, d: dict[str, Any], public_url: str | None = None) -> Self:
        """Create a Document from an entity dict (as returned by query_raw).

        ``name`` is the entity's file name (`get_filename`), not its
        caption – it is what [`relative_path`][Document.relative_path] builds a
        path out of, and the mime type is guessed off the same value.
        """
        props = d.get("properties", {})
        checksums = props.get("contentHash", [])
        if not checksums or not d.get("id"):
            raise ValueError(f"Missing contentHash for entity id `{d.get('id')}`")
        name = get_filename(d)
        mimetypes = props.get("mimeType", [])
        file_sizes = props.get("fileSize", [])
        return cls(
            id=d["id"],
            checksum=checksums[0],
            name=name,
            mimetype=pick_mime(mimetypes, guess_mime(name)),
            size=int(file_sizes[0]) if file_sizes else 0,
            updated_at=d.get("last_change"),
            public_url=public_url,
        )

    @property
    def relative_path(self) -> str:
        if self.path is None:
            return self.name
        return f"{self.path}/{self.name}"


Documents: TypeAlias = Generator[Document, None, None]
"""Type shorthand for a generator of `Document` objects"""
