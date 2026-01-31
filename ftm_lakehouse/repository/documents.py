"""DocumentRepository - compiled metadata (csv) about files to consume for
clients, including diffs"""

from datetime import datetime
from typing import Generator

from anystore.io import smart_stream_csv_models, smart_write_models
from anystore.logic.constants import CHUNK_SIZE_LARGE
from anystore.logic.io import stream
from anystore.types import Uri
from anystore.util import join_uri
from followthemoney import Statement, model
from ftmq.query import Query

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model.file import Document, Documents
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.repository.diff import ParquetDiffMixin
from ftm_lakehouse.storage.base import ByteStorage
from ftm_lakehouse.storage.parquet import ParquetStore


class DocumentRepository(ParquetDiffMixin, BaseRepository):
    """
    Repository for documents to consume for clients.

    This gathers File entities created during storing blobs in the archive and
    compiles a streamable csv list of document metadata.

    Format: id,checksum,name,path,size,mimetype,updated_at

    Example:
        ```python
        documents = DocumentRepository(dataset="my_data", uri="s3://bucket/dataset")

        # Iterate through documents metadata
        for document in documents.iterate():
            print(document.uri)  # use uri to download
    """

    def __init__(self, dataset: str, uri: Uri) -> None:
        super().__init__(dataset, uri)
        self._statements = ParquetStore(uri, dataset)
        self._storage = ByteStorage(uri)

    @property
    def csv_uri(self) -> Uri:
        return self._storage.to_uri(path.EXPORTS_DOCUMENTS)

    def stream(self) -> Documents:
        yield from smart_stream_csv_models(self.csv_uri, model=Document)

    def make_paths(self) -> dict[str, str]:
        """Compute folder structure from Folder (parent) entities.

        Returns:
            Mapping of folder ID to complete path (e.g. "root/sub/folder")
        """
        q = Query().where(schema="Folder")

        # First pass: collect caption and parent for each folder
        folders: dict[str, tuple[str, str | None]] = {}
        for entity in self._statements.query(q):
            assert entity.id  # FIXME (typing)
            folders[entity.id] = (entity.caption, entity.first("parent"))

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

    def collect(self, public_url_prefix: str | None = None, **filters) -> Documents:
        paths = self.make_paths()
        q = (
            Query()
            .where(schema="Document", schema_include_descendants=True, **filters)
            .order_by("contentHash")
        )
        for entity in self._statements.query(q):
            if entity.schema.name == "Folder":
                continue
            assert entity.id  # FIXME (typing)
            document = Document.from_entity(entity)
            if public_url_prefix:
                document.public_url = join_uri(
                    public_url_prefix, path.archive_blob(document.checksum)
                )
            yielded = False
            for parent in entity.get("parent"):
                path_ = paths.get(parent)
                if path_:
                    document.path = path_
                    yield document
                    yielded = True
            if not yielded:
                yield document

    def export_csv(self, public_url_prefix: str | None = None) -> None:
        smart_write_models(
            self.csv_uri, self.collect(public_url_prefix), output_format="csv"
        )

    # DiffMixin implementation

    _diff_base_path = path.DIFFS_DOCUMENTS

    def _filter_changes(
        self,
        changes: Generator[tuple[datetime, str, Statement], None, None],
    ) -> set[str]:
        """Filter for Document entities with contentHash changes."""
        changed_entity_ids: set[str] = set()
        for _, change_type, stmt in changes:
            if change_type in ("insert", "update_postimage"):
                schema = model.get(stmt.schema)
                if schema and schema.is_a("Document") and not schema.name == "Folder":
                    if stmt.prop == "contentHash":
                        changed_entity_ids.add(stmt.entity_id)
        return changed_entity_ids

    def _write_diff(self, entity_ids: set[str], v: int, ts: datetime, **kwargs) -> str:
        """Write documents as CSV."""
        docs = self.collect(kwargs.get("public_url_prefix"), entity_id__in=entity_ids)
        key = path.documents_diff(v, ts)
        with self._storage.open(key, "w") as o:
            smart_write_models(o, docs, output_format="csv")
        return self._storage.to_uri(key)

    def _write_initial_diff(self, version: int, ts: datetime, **kwargs) -> None:
        """Copy over exported documents.csv to initial diff version"""
        if not self._storage.exists(path.EXPORTS_DOCUMENTS):
            self.log.info(
                f"Exporting `{path.EXPORTS_DOCUMENTS}` first to create initial diff."
            )
            self.export_csv()
        with self._storage.open(path.EXPORTS_DOCUMENTS, "rb") as i:
            with self._storage.open(path.documents_diff(version, ts), "wb") as o:
                stream(i, o, CHUNK_SIZE_LARGE)
