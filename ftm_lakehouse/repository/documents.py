"""DocumentRepository - compiled metadata (csv) about files to consume for
clients."""

from functools import cached_property

from anystore.logic.compress import CompressKind
from anystore.types import Uri

from ftm_lakehouse.logic.path import StoreKey
from ftm_lakehouse.model.file import Documents
from ftm_lakehouse.repository.artifacts import DocumentsArtifact
from ftm_lakehouse.repository.base import DatasetHandle


class DocumentRepository(DatasetHandle):
    """
    Repository for documents to consume for clients.

    This gathers File entities created during storing blobs in the archive and
    compiles a streamable csv list of document metadata.

    Format: id,checksum,name,mimetype,path,size,updated_at,public_url

    The csv itself is written by the export sweep
    ([`ExportOperation`][ftm_lakehouse.operation.export.ExportOperation]),
    which already holds every entity. The row shape and reading the result
    back belong to
    [`DocumentsArtifact`][ftm_lakehouse.repository.artifacts.DocumentsArtifact];
    this repository streams the written csv back.

    Example:
        ```python
        documents = DocumentRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Iterate through documents metadata
        for document in documents.stream():
            print(document.public_url)  # use uri to download
        ```
    """

    @cached_property
    def _artifact(self) -> DocumentsArtifact:
        """The documents export artifact for this dataset."""
        return DocumentsArtifact(self)

    @property
    def compression(self) -> CompressKind | None:
        """Compression codec of the exported artifacts (the dataset's config)."""
        return self._model.compression

    def csv_uri(self, origin: str | None = None) -> Uri:
        """Uri of the exported documents csv, optionally scoped to ``origin``."""
        return self._artifact[origin].uri

    def csv_key(self, origin: str | None = None) -> StoreKey:
        """Store key of the exported documents csv, carrying the dataset's codec."""
        return self._artifact[origin].key

    def stream(self, origin: str | None = None) -> Documents:
        """Stream the exported documents csv, optionally scoped to ``origin``."""
        yield from self._artifact[origin].stream()
