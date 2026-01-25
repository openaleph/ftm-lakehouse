"""DocumentRepository - compiled metadata (csv) about files to consume for
clients, including diffs"""

from anystore.io import smart_stream_csv_models, smart_write_models
from anystore.types import Uri
from ftmq.query import Query

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model.file import Document, Documents
from ftm_lakehouse.repository.base import BaseRepository
from ftm_lakehouse.storage.base import ByteStorage
from ftm_lakehouse.storage.parquet import ParquetStore


class DocumentRepository(BaseRepository):
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
        self._store = ByteStorage(uri)

    @property
    def csv_uri(self) -> Uri:
        return self._store._store.get_key(path.EXPORTS_DOCUMENTS)

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

    def collect(self) -> Documents:
        paths = self.make_paths()
        q = (
            Query()
            .where(schema="Document", schema_include_descendants=True)
            .order_by("contentHash")
        )
        for entity in self._statements.query(q):
            if entity.schema.name == "Folder":
                continue
            assert entity.id  # FIXME (typing)
            document = Document.from_entity(entity)
            parents = entity.get("parent")
            yielded = False
            for parent in parents:
                path = paths.get(parent)
                if path:
                    document.path = paths.get(parent)
                    yield document
                    yielded = True
            if not yielded:
                yield document

    def export_csv(self) -> None:
        smart_write_models(self.csv_uri, self.collect(), output_format="csv")
