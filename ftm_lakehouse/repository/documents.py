"""DocumentRepository - compiled metadata (csv) about files to consume for
clients, including diffs"""

from datetime import datetime
from functools import cached_property
from itertools import chain, islice
from typing import Generator, Iterator

from anystore.io import smart_stream_csv_models, smart_write_csv, smart_write_models
from anystore.types import Uri
from anystore.util import join_uri
from ftmq.query import C, M, P, Query

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.logic.parquet import QUERY_IN_BATCH_SIZE
from ftm_lakehouse.model.file import Document, Documents
from ftm_lakehouse.repository.base import DatasetHandle
from ftm_lakehouse.repository.diff import ParquetDiffMixin
from ftm_lakehouse.storage.parquet import ParquetStore

Q_DOCUMENTS = [M(schemata="Document"), ~M(schema="Folder"), P(contentHash__null=False)]
SELECT = [P("contentHash"), P("fileName"), P("fileSize"), P("parent")]


class DocumentRepository(ParquetDiffMixin, DatasetHandle):
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

    @cached_property
    def _statements(self) -> ParquetStore:
        return ParquetStore(
            self.uri, self.dataset, self._model.shards, self._model.compression
        )

    def csv_uri(self, origin: str | None = None) -> Uri:
        """Uri of the exported documents csv, optionally scoped to ``origin``."""
        return self._store.to_uri(path.export_documents(origin))

    def stream(self, origin: str | None = None) -> Documents:
        """Stream the exported documents csv, optionally scoped to ``origin``."""
        yield from smart_stream_csv_models(self.csv_uri(origin), model=Document)

    def make_paths(self) -> dict[str, str]:
        """Compute folder structure from Folder (parent) entities.

        Returns:
            Mapping of folder ID to complete path (e.g. "root/sub/folder")
        """
        # First pass: collect caption and parent for each folder
        folders: dict[str, tuple[str, str | None]] = {}
        for d in self._statements._query_data(Query(M(schema="Folder"))):
            d = d.to_dict()
            props = d.get("properties", {})
            file_names = props.get("fileName", [])
            parents = props.get("parent", [])
            caption = file_names[0] if file_names else d.get("caption", "")
            folders[d["id"]] = (caption, parents[0] if parents else None)

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

    def collect(self, q: Query | None = None) -> Documents:
        paths = self.make_paths()
        public_prefix = self._model.get_public_prefix()
        q = (q or Query()).where(*Q_DOCUMENTS).select(*SELECT)
        for d in self._statements._query_data(q):
            d = d.to_dict()
            document = Document.from_entity_dict(d)
            if public_prefix:
                document.public_url = join_uri(
                    public_prefix, path.archive_blob(document.checksum)
                )
            yielded = False
            for parent in d.get("properties", {}).get("parent", []):
                path_ = paths.get(parent)
                if path_:
                    document.path = path_
                    yield document
                    yielded = True
            if not yielded:
                yield document

    def export_csv(self, origin: str | None = None) -> None:
        """Export the document metadata csv, optionally scoped to ``origin``.

        Args:
            origin: Only export documents asserted by this source tag, into
                the origin-scoped export path. ``None`` exports every origin.
        """
        # Short-circuit before the per-partition iteration when the dataset has
        # no documents – a single count(DISTINCT entity_id) that file-skips
        # on the schema filter (and prunes to the origin partition when
        # scoped), so a document-free dataset costs one fast query instead of
        # scanning every partition.
        q = Query(C(origin=origin)) if origin else Query()
        if self._statements.count(q.where(*Q_DOCUMENTS).select(*SELECT)) == 0:
            return
        docs = self.collect(q)
        first = next(docs, None)
        if first is None:
            return
        smart_write_models(
            self.csv_uri(origin), chain([first], docs), output_format="csv"
        )

    # DiffMixin implementation

    _diff_base_path = path.DIFFS_DOCUMENTS

    def _get_diff_base_path(self, origin: str | None = None) -> str:
        """Each origin scope diffs into its own path, tag and state key."""
        return path.diffs_documents(origin)

    def _get_changed_ids(
        self, since: datetime, origin: str | None = None
    ) -> Iterator[str]:
        """Get Document entity IDs with contentHash changes since the given timestamp."""
        q = Query(*Q_DOCUMENTS, (C(first_seen__gte=since) | C(deleted_at__gte=since)))
        if origin:
            q = q.where(C(origin=origin))
        return self._statements.get_entity_ids(q, source=self._statements.source_raw)

    def _write_diff(
        self, entity_ids: Iterator[str], ts: datetime, origin: str | None = None
    ) -> tuple[str, int]:
        """Write documents as CSV with an op column."""
        key = path.documents_diff(ts, origin)
        changed: set[str] = set(entity_ids)
        with self._store.open(key, "w") as o:
            smart_write_csv(o, self._get_delta_documents(changed, origin))
        return self._store.to_uri(key), len(changed)

    def _get_delta_documents(
        self, entity_ids: set[str], origin: str | None = None
    ) -> Generator[dict, None, None]:
        """ADD the changed documents as they stand, DEL the ones that are gone.

        The lookup carries the same ``origin`` scope as the export it belongs
        to, so a diff describes exactly the csv next to it: a document whose
        statements for this origin are all tombstoned falls out of the ADD set
        and is published as a DEL, even when other origins still assert it.
        """
        seen_ids: set[str] = set()
        it = iter(entity_ids)
        while batch := set(islice(it, QUERY_IN_BATCH_SIZE)):
            q = Query(M(entity_id__in=batch))
            if origin:
                q = q.where(C(origin=origin))
            for doc in self.collect(q):
                seen_ids.add(doc.id)
                yield {"op": "ADD", **doc.model_dump(by_alias=True, mode="json")}
        for entity_id in entity_ids - seen_ids:
            yield {"op": "DEL", "id": entity_id}
