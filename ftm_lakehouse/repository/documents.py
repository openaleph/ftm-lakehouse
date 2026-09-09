"""DocumentRepository - compiled metadata (csv) about files to consume for
clients."""

from datetime import datetime
from functools import cached_property
from typing import Iterator

from anystore.logic.compress import CompressKind
from anystore.types import Uri
from ftmq.query import C, M, P, Query

from ftm_lakehouse.helpers.file import get_filename
from ftm_lakehouse.helpers.schema import CAPTION_PROPS
from ftm_lakehouse.logic.path import StoreKey
from ftm_lakehouse.model.file import Documents
from ftm_lakehouse.repository.artifacts import DocumentsArtifact
from ftm_lakehouse.repository.base import DatasetHandle
from ftm_lakehouse.storage.parquet import ParquetStore

Q_DOCUMENTS = [M(schemata="Document"), ~M(schema="Folder"), P(contentHash__null=False)]
SELECT = [P("contentHash"), P("fileSize"), P("parent"), *CAPTION_PROPS]


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
    this repository owns the query side – the folder paths the rows resolve
    against ([`make_paths`][DocumentRepository.make_paths]), the ad-hoc
    lookups and the tombstoned ids.

    Example:
        ```python
        documents = DocumentRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Iterate through documents metadata
        for document in documents.stream():
            print(document.public_url)  # use uri to download
        ```
    """

    _paths: dict[str, str] | None = None
    """Memoised `make_paths` result, keyed by `_paths_version`."""

    _paths_version: int | None = None
    """Delta table version `_paths` was built against."""

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

    def make_paths(self) -> dict[str, str]:
        """Folder id to path map, memoised per delta table version.

        Returns:
            Mapping of folder ID to complete path (e.g. "root/sub/folder")
        """
        version = self._statements.version
        if self._paths is None or self._paths_version != version:
            self._paths_version = version
            self._paths = self._build_paths()
        return self._paths

    def _build_paths(self) -> dict[str, str]:
        """Walk the Folder entities into a folder id to path map."""
        # First pass: collect caption and parent for each folder
        folders: dict[str, tuple[str, str | None]] = {}
        for d in self._statements._query_data(
            Query(M(schemata="Folder")).select(P("parent"), *CAPTION_PROPS)
        ):
            data = d.to_dict()
            parents = data.get("properties", {}).get("parent", [])
            folders[data["id"]] = (
                get_filename(data),
                parents[0] if parents else None,
            )

        # Second pass: resolve full paths by walking up parent chain
        paths: dict[str, str] = {}
        for folder_id in folders:
            parts: list[str] = []
            current_id: str | None = folder_id
            seen: set[str] = set()
            while current_id and current_id in folders:
                if current_id in seen:
                    break  # cycle detection
                seen.add(current_id)
                caption, parent_id = folders[current_id]
                parts.append(caption)
                current_id = parent_id
            paths[folder_id] = "/".join(reversed(parts))

        return paths

    def iterate(self, q: Query | None = None) -> Documents:
        """Query the store for documents and build their csv rows.

        The ad-hoc entry point – the export sweep does not use it, since it
        already holds every entity and calls
        [`make_documents`][ftm_lakehouse.repository.artifacts.DocumentsArtifact.make_documents]
        directly against one `make_paths` result.
        """
        paths = self.make_paths()
        public_prefix = self._model.get_public_prefix()
        q = (q or Query()).where(*Q_DOCUMENTS).select(*SELECT)
        for d in self._statements._query_data(q):
            yield from self._artifact.make_documents(d.to_dict(), paths, public_prefix)

    def deleted_ids(self, since: datetime, origin: str | None = None) -> Iterator[str]:
        """Document ids with statements tombstoned since the given timestamp.

        Reads `ParquetStore.source_raw`, since the live view hides exactly
        the rows this asks about.
        """
        q = Query(*Q_DOCUMENTS, C(deleted_at__gte=since))
        if origin:
            q = q.where(C(origin=origin))
        return self._statements.get_entity_ids(q, source=self._statements.source_raw)
