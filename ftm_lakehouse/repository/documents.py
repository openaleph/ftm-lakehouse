"""DocumentRepository - compiled metadata (csv) about files to consume for
clients."""

from datetime import datetime
from functools import cached_property
from typing import Iterator

from anystore.logic.compress import CompressKind
from anystore.types import Uri
from ftmq.query import C, M, P, Query

from ftm_lakehouse.logic.path import StoreKey
from ftm_lakehouse.model.file import Documents
from ftm_lakehouse.repository.artifacts import DocumentsArtifact
from ftm_lakehouse.repository.base import DatasetHandle
from ftm_lakehouse.storage.parquet import ParquetStore

Q_DOCUMENTS = [M(schemata="Document"), ~M(schema="Folder"), P(contentHash__null=False)]


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
    this repository owns the read side – streaming the written csv back and
    the tombstoned ids the diff series need.

    Example:
        ```python
        documents = DocumentRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Iterate through documents metadata
        for document in documents.stream():
            print(document.public_url)  # use uri to download
        ```
    """

    @cached_property
    def _statements(self) -> ParquetStore:
        return ParquetStore(
            self.uri, self.dataset, self._model.shards, self._model.compression
        )

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

    def deleted_ids(self, since: datetime, origin: str | None = None) -> Iterator[str]:
        """Document ids with statements tombstoned since the given timestamp.

        Reads `ParquetStore.source_raw`, since the live view hides exactly
        the rows this asks about.
        """
        q = Query(*Q_DOCUMENTS, C(deleted_at__gte=since))
        if origin:
            q = q.where(C(origin=origin))
        return self._statements.get_entity_ids(q, source=self._statements.source_raw)
