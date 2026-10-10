"""The export artifacts of a dataset – one class per artifact.

`Artifact.tag` is codec-free (freshness tag, diff series name), `Artifact.key`
carries the dataset codec (what is on disk). Artifacts are stateless; per-run
state lives on their `ArtifactRun`.
"""

import csv
import os
from collections import Counter
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from enum import StrEnum
from pathlib import Path
from shutil import copyfileobj
from typing import (
    IO,
    Any,
    Callable,
    ClassVar,
    Generator,
    Iterator,
    Self,
    cast,
)

from anystore.io import SyncProgressBar, Writer, smart_open
from anystore.io.read import smart_stream_json
from anystore.io.write import Formats
from anystore.logging import get_logger
from anystore.logic.compress import CompressKind
from anystore.model.base import BaseModel
from anystore.store import Store
from anystore.types import SDict, Uri
from anystore.util import Took, join_uri
from followthemoney import model
from followthemoney.dataset import DataResource
from ftmq.aggregate import EntityPayload
from ftmq.util import datetime_iso
from rigour.mime.types import CSV, FTM, JSON

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import CHECKSUM_ALGORITHM
from ftm_lakehouse.helpers.file import FolderTree, get_filename
from ftm_lakehouse.helpers.schema import FOLDER_SCHEMATA
from ftm_lakehouse.logic.entities.stats import StatsCollector
from ftm_lakehouse.logic.path import DateTimeKey, StoreKey
from ftm_lakehouse.model.file import Document, Documents
from ftm_lakehouse.model.statement import DeleteCandidate
from ftm_lakehouse.repository.base import DatasetHandle
from ftm_lakehouse.util import validate_origin

DOCUMENT_FIELDNAMES = list(Document.model_fields)
"""Fixed documents csv columns – a ``DEL`` row landing first would otherwise
cap the header at ``op`` and ``id``."""

log = get_logger(__name__)

PROGRESS_STEP = 10_000
"""Documents per progress bar update – one per row would cost a lock each."""

Candidates = Callable[[datetime], Iterator[DeleteCandidate]]
"""The delete-candidate scan an `ExportSession` distributes."""

DOCUMENT_ORIGINS: tuple[str | None, ...] = (None, tag.CRAWL_ORIGIN)
"""Scopes the documents export is written for: every origin, and the crawl."""


class ExportKind(StrEnum):
    """What each export artifact is called (`Artifact.kind`) – also the key an
    export run reports its counts under."""

    statements = "statements"
    entities = "entities"
    documents = "documents"
    parents = "parents"
    statistics = "statistics"
    index = "index"  # type: ignore[assignment]  # shadows str.index, fine for enums


class DiffOp(StrEnum):
    """What a diff entry says happened to an entity.

    Ref. https://www.opensanctions.org/docs/bulk/delta/
    """

    ADD = "ADD"
    """The entity is new – every statement it has arrived in this window."""

    MOD = "MOD"
    """The entity predates this window and changed in it – it gained
    statements, or lost some to a tombstone while staying alive."""

    DEL = "DEL"
    """The entity is gone entirely."""


def make_envelope(data: SDict, op: DiffOp | str = DiffOp.ADD) -> SDict:
    """Wrap an entity payload in a diff envelope (OpenSanctions delta format)."""
    return {"op": str(op), "entity": data}


def _move(store: Store, source: Uri, target: Uri) -> None:
    """Move ``source`` over ``target`` via fsspec (anystore has no move) – an
    atomic rename locally, copy and delete elsewhere."""
    store._fs.mv(store._keys.to_fs_key(source), store._keys.to_fs_key(target))


class Assembly:
    """One artifact file assembled from a run's encoded parts, copied as they
    are – a multi-frame stream the codec reads back as one file.

    Parts are appended to ``{key}.tmp`` and deleted as they arrive; `commit`
    moves the file into place, `abort` drops it, so a failed run leaves the
    previous file intact. ``empty`` writes the file when no part came; with
    ``None`` there is then no file.
    """

    def __init__(
        self, store: Store, key: Uri, empty: Callable[[str], None] | None = None
    ) -> None:
        self.store = store
        self.key = key
        self.tmp = f"{key}.tmp"
        self.empty = empty
        self._stack = ExitStack()
        self._out: IO[bytes] | None = None

    def append(self, part: str) -> None:
        """Copy ``part`` in, if it was written, and delete it."""
        if not Path(part).exists():
            return
        if self._out is None:
            self._out = self._stack.enter_context(self.store.open(self.tmp, "wb"))
        with smart_open(part, "rb") as fh:
            copyfileobj(fh, self._out)
        os.remove(part)

    def commit(self) -> None:
        """Move the file into place – an empty one if no part came, or none
        for a lazy file."""
        self._stack.close()
        if self._out is None:
            if self.empty is None:
                return
            self.empty(self.tmp)
        _move(self.store, self.tmp, self.key)

    def abort(self) -> None:
        """Drop whatever was appended."""
        self._stack.close()
        self.store.delete(self.tmp, ignore_errors=True)


class Artifact:
    """One export artifact, bound to a dataset.

    Args:
        dataset: The dataset handle the artifact belongs to.
        origin: Origin this variant is scoped to, ``None`` for all.
    """

    base: ClassVar[StoreKey]
    kind: ClassVar[ExportKind]
    mime_type: ClassVar[str] = CSV
    compressed: ClassVar[bool] = True
    fieldnames: ClassVar[list[str] | None] = None

    def __init__(self, dataset: DatasetHandle, origin: str | None = None) -> None:
        self.dataset = dataset
        self.origin = validate_origin(origin) if origin else None

    def __repr__(self) -> str:
        return f"<{type(self).__name__}({self.key})>"

    def __getitem__(self, origin: str | None = None) -> Self:
        """The variant scoped to one origin – ``documents["crawl"]``."""
        return type(self)(self.dataset, origin)

    @property
    def name(self) -> str:
        """How this variant names itself in a result – ``crawl_documents``."""
        if self.origin:
            return f"{self.origin}_{self.kind}"
        return str(self.kind)

    @property
    def compression(self) -> CompressKind | None:
        """The dataset's codec, or ``None`` where the artifact takes none."""
        if not self.compressed:
            return None
        return self.dataset._model.compression

    @property
    def tag(self) -> StoreKey:
        """The codec-free key: what the freshness tag is named after."""
        return self.base[self.origin]

    @property
    def key(self) -> StoreKey:
        """The key the artifact actually lives at, codec included."""
        return self.tag + self.compression

    @property
    def uri(self) -> str:
        """Full uri of the artifact in the dataset's store."""
        return self.dataset._store.to_uri(self.key)

    @property
    def format(self) -> Formats:
        return "csv" if self.mime_type == CSV else "json"

    def exists(self) -> bool:
        """Whether the artifact has been written."""
        return self.dataset._store.exists(self.key)

    def touch(self, ts: datetime | None = None) -> None:
        """Stamp the freshness tag."""
        self.dataset._tags.set(self.tag, ts)

    def part(self, parts: str) -> str:
        """Where this artifact's part of a run is written, inside ``parts``."""
        return f"{parts}/{self.name}"

    def writer(self, parts: str | None = None, key: str | None = None) -> Writer:
        """A writer for the artifact (at ``key``, its own by default), or for its
        part of a run – lazy, so an unwritten part is never appended."""
        return Writer(
            (
                self.part(parts)
                if parts
                else self.dataset._store.to_uri(key or self.key)
            ),
            output_format=self.format,
            compression=self.compression,
            fieldnames=self.fieldnames,
            lazy=parts is not None,
        )

    def assembly(self) -> "Assembly":
        """This artifact's file, assembled from a run's parts – eager: a run that
        wrote nothing still replaces a stale file with an empty one."""
        return Assembly(self.dataset._store, self.key, self._write_empty)

    def _write_empty(self, key: str) -> None:
        """Write an empty artifact – just the header when the columns are known."""
        writer = self.writer(key=key)
        writer.open()
        writer.close()

    @contextmanager
    def reader(self, mode: str = "rb") -> Generator[IO[Any], None, None]:
        """Open the artifact for reading, decoded with the dataset's codec."""
        with self.dataset._store.open(
            self.key, mode, compression=self.compression
        ) as fh:
            yield fh

    def make_resource(self, public_prefix: str | None = None) -> DataResource | None:
        """Describe the artifact for ``index.json``, or ``None`` if unwritten."""
        if not self.exists():
            return None
        public_prefix = public_prefix or self.dataset._model.get_public_prefix()
        if not public_prefix:
            return None
        info = self.dataset._store.info(self.key)
        return DataResource(
            name=info.name,
            url=join_uri(public_prefix, self.key),
            checksum=self.dataset._store.checksum(self.key, CHECKSUM_ALGORITHM),
            timestamp=info.created_at,
            mime_type=self.mime_type,
            size=info.size,
        )

    def run(self, now: datetime, parts: str) -> "ArtifactRun":
        """The per-run object that writes this artifact's part into ``parts``."""
        return ArtifactRun(self, now, parts)


class DiffableArtifact(Artifact):
    """An artifact with a diff series: its files and its stored
    ``{timestamp}:{version}`` state. The series directory carries no codec, so
    it does not move when the dataset's compression does."""

    diffs: ClassVar[DateTimeKey]

    @property
    def series(self) -> DateTimeKey:
        """The directory this variant's diff files live in."""
        return self.diffs[self.origin]

    @property
    def state_key(self) -> str:
        """Tag key the ``{timestamp}:{version}`` state is stored under."""
        return f"{self.series}-current"

    def get_state(self) -> tuple[datetime, int] | None:
        """Last diff state as ``(timestamp, delta table version)``."""
        state = self.dataset._tags.get(self.state_key)
        if state is None:
            return None
        ts_str, main_v = state.split(":")
        return (
            datetime.strptime(ts_str, path.TS_FORMAT).replace(tzinfo=timezone.utc),
            int(main_v),
        )

    def set_state(self, ts: datetime, version: int) -> None:
        """Store the diff state the next run is taken against."""
        ts_str = ts.strftime(path.TS_FORMAT)
        self.dataset._tags.put(self.state_key, f"{ts_str}:{version}")

    def diff_part(self, parts: str) -> str:
        """Where this series' part of a run's diff is written."""
        return f"{self.part(parts)}.diff"

    def diff_writer(self, parts: str) -> Writer:
        """A lazy writer for this series' part of a run's diff file."""
        fieldnames = ["op", *self.fieldnames] if self.fieldnames else None
        return Writer(
            self.diff_part(parts),
            output_format=self.format,
            compression=self.compression,
            fieldnames=fieldnames,
            lazy=True,
        )

    def diff_assembly(self, ts: datetime) -> "Assembly":
        """This run's diff file, assembled like `assembly` – but lazy: a window
        without changes leaves no file."""
        return Assembly(self.dataset._store, self.series(ts) + self.compression)


class VersionedArtifact(Artifact):
    """An artifact written whole as a versioned model dump – plain JSON."""

    mime_type = JSON
    compressed = False

    def write(self, obj: BaseModel) -> None:
        """Write the model and stamp the freshness tag."""
        self.dataset._versions.make(self.key, obj)
        self.touch()


class StatementsArtifact(Artifact):
    """Every statement – written by the sweep's Arrow tee, so its run consumes
    nothing."""

    base = path.EXPORTS_STATEMENTS
    kind = ExportKind.statements


class EntitiesArtifact(DiffableArtifact):
    """The aggregated entities, and their delta series."""

    base = path.ENTITIES_JSON
    kind = ExportKind.entities
    mime_type = FTM
    diffs = path.DIFFS_ENTITIES

    def run(self, now: datetime, parts: str) -> "EntitiesRun":
        return EntitiesRun(self, now, parts)


class DocumentsArtifact(DiffableArtifact):
    """Document metadata and its diff series, scoped per origin."""

    base = path.EXPORTS_DOCUMENTS
    kind = ExportKind.documents
    diffs = path.DIFFS_DOCUMENTS
    fieldnames = DOCUMENT_FIELDNAMES

    @staticmethod
    def is_document_schema(schema: str | None) -> bool:
        """Whether the documents export carries ``schema``: a ``Document``
        descendant other than ``Folder`` – shared by the live and delete paths."""
        if not schema:
            return False
        schema_ = model.get(str(schema))
        return (
            schema_ is not None
            and schema_.is_a("Document")
            and schema_.name != "Folder"
        )

    @staticmethod
    def is_parent_schema(schema: str | None) -> bool:
        """Whether an entity of ``schema`` can be a document's parent folder."""
        return schema in FOLDER_SCHEMATA

    @staticmethod
    def is_document(data: SDict) -> bool:
        """Whether an entity dict belongs in the documents export: a document
        schema (`is_document_schema`) with a content hash."""
        if not DocumentsArtifact.is_document_schema(data.get("schema")):
            return False
        return bool(data.get("properties", {}).get("contentHash"))

    def make_document(self, data: SDict, public_prefix: str | None = None) -> Document:
        """The row an entity dict contributes, ``path`` unset until the folder
        tree is known.

        Args:
            data: Entity dict, as `EntityPayload.to_dict` returns.
            public_prefix: Public url prefix to build blob links against.
        """
        document = Document.from_entity_dict(data)
        if public_prefix:
            document.public_url = join_uri(
                public_prefix, path.ArchiveKey(document.checksum).blob
            )
        return document

    def stream(self) -> Documents:
        """Stream this variant's csv back as `Document` models."""
        if not self.exists():
            return
        with self.reader("r") as raw:
            for row in csv.DictReader(raw):
                # csv values arrive as strings; pydantic coerces size / updated_at
                yield Document(**cast(dict[str, Any], row))

    def run(self, now: datetime, parts: str) -> "DocumentsRun":
        return DocumentsRun(self, now, parts)


class ParentsArtifact(Artifact):
    """Every folder a document can sit in, with its path."""

    base = path.EXPORTS_PARENTS
    kind = ExportKind.parents
    fieldnames = ["id", "name", "path"]

    def run(self, now: datetime, parts: str) -> "ParentsRun":
        return ParentsRun(self, now, parts)


class StatisticsArtifact(VersionedArtifact):
    """Entity counts and facets, folded from the export's entity stream."""

    base = path.EXPORTS_STATISTICS
    kind = ExportKind.statistics

    def run(self, now: datetime, parts: str) -> "StatisticsRun":
        return StatisticsRun(self, now, parts)


class IndexArtifact(VersionedArtifact):
    """The dataset index – config, resources and statistics, so written last."""

    base = path.INDEX
    kind = ExportKind.index


class ArtifactRun:
    """One artifact being written by one export run. The hooks do nothing by
    default – right for ``statements.csv``, written by the Arrow tee."""

    def __init__(self, artifact: Artifact, now: datetime, parts: str) -> None:
        self.artifact = artifact
        self.now = now
        self.parts = parts
        self.counts: Counter[str] = Counter()

    def __repr__(self) -> str:
        return f"<{type(self).__name__}({self.artifact.key})>"

    @property
    def name(self) -> str:
        return self.artifact.name

    def prepare(self, version: int | None) -> None:
        """Resolve anything this run needs before the sweep starts."""

    def consume(self, payload: EntityPayload) -> None:
        """Take one entity from the sweep."""

    def finish(self) -> None:
        """Write anything only knowable once the sweep has ended."""

    def commit(self, version: int | None) -> None:
        """Record whatever the next run is taken against."""

    def close(self) -> None:
        """Release whatever the run opened."""


class DiffableRun(ArtifactRun):
    """A run that also writes a diff series.

    Additions show in the folded ``first_seen``; deletions never come past, so
    tombstoned ids are loaded up front (`pending`), claimed as the sweep meets
    them alive, and the rest become ``DEL``.
    """

    artifact: DiffableArtifact

    def __init__(self, artifact: DiffableArtifact, now: datetime, parts: str) -> None:
        super().__init__(artifact, now, parts)
        self.writer = artifact.writer(parts)
        self.since: datetime | None = None
        self.since_iso: str | None = None
        self.pending: set[str] = set()
        self.diff: Writer | None = None

    def prepare(self, version: int | None) -> None:
        """Activate the diff unless there is no table, no prior state (a first
        run only records one) or no new version since."""
        super().prepare(version)
        if version is None:
            return
        state = self.artifact.get_state()
        if state is None:
            return
        last_timestamp, last_version = state
        if last_version >= version:
            return
        self.since = last_timestamp
        # the folded bounds' spelling, so both sides compare lexically
        self.since_iso = datetime_iso(last_timestamp)
        self.diff = self.artifact.diff_writer(self.parts)

    def claims(self, candidate: DeleteCandidate) -> bool:
        """Whether ``candidate`` falls into this series' window."""
        return self.since is not None and candidate.deleted_at >= self.since

    def op_for(self, payload: EntityPayload) -> DiffOp | None:
        """This entity's diff op, ``None`` when it did not change. Claims the id
        off `pending` – a partly tombstoned entity is a ``MOD``, not a ``DEL``.
        """
        if self.since_iso is None or not payload.id:
            return None
        touched = payload.id in self.pending
        if touched:
            self.pending.discard(payload.id)
        changed = (
            payload.max_first_seen is not None
            and payload.max_first_seen >= self.since_iso
        )
        if not (touched or changed):
            return None
        if payload.min_first_seen is None or payload.min_first_seen >= self.since_iso:
            return DiffOp.ADD
        return DiffOp.MOD

    def write_delete(self, entity_id: str) -> None:
        """Write one DEL entry – shaped per artifact."""
        raise NotImplementedError

    def finish(self) -> None:
        """Emit a DEL for every candidate the sweep never met alive."""
        if self.diff is None:
            return
        for entity_id in self.pending:
            self.write_delete(entity_id)
            self.counts[DiffOp.DEL.lower()] += 1

    def commit(self, version: int | None) -> None:
        """Record the state the next diff is taken against, changes or not."""
        if version is not None:
            self.artifact.set_state(self.now, version)

    def close(self) -> None:
        self.writer.close()
        if self.diff is not None:
            self.diff.close()


class EntitiesRun(DiffableRun):
    """Writes ``entities.ftm.json`` and its delta series."""

    def consume(self, payload: EntityPayload) -> None:
        data = cast(SDict, payload.to_dict())
        self.writer.write(data)
        # no `total`: that is the session's `entities`
        op = self.op_for(payload)
        if op is None or self.diff is None:
            return
        self.diff.write(make_envelope(data, op))
        self.counts[op.lower()] += 1

    def write_delete(self, entity_id: str) -> None:
        cast(Writer, self.diff).write(make_envelope({"id": entity_id}, DiffOp.DEL))


class DocumentsRun(DiffableRun):
    """One origin scope of ``documents.csv`` and its diff series – its rows are
    staged and written by the shared `ParentsRun`."""

    artifact: DocumentsArtifact

    def claims(self, candidate: DeleteCandidate) -> bool:
        """Only tombstoned documents of this origin scope."""
        if not super().claims(candidate):
            return False
        if self.artifact.origin and self.artifact.origin not in candidate.origins:
            return False
        return candidate.content_hash and any(
            DocumentsArtifact.is_document_schema(s) for s in candidate.schemata
        )

    def in_scope(self, payload: EntityPayload) -> bool:
        return not self.artifact.origin or self.artifact.origin in payload.origins

    def write(self, rows: list[SDict], op: str | None) -> None:
        """One document's rows, and its diff rows under ``op``."""
        for row in rows:
            self.writer.write(row)
        if op is not None and self.diff is not None:
            for row in rows:
                self.diff.write({"op": op, **row})

    def write_delete(self, entity_id: str) -> None:
        cast(Writer, self.diff).write({"op": str(DiffOp.DEL), "id": entity_id})


class ParentsRun(ArtifactRun):
    """Writes ``parents.csv`` and every documents scope, from one staging.

    A path is the chain of a document's ancestors, which arrive in no order:
    `consume` stages folders and documents (with each scope's diff op),
    `finish` resolves one folder tree and writes every scope – ahead of the
    scopes' own `finish`, their DELs.
    """

    artifact: ParentsArtifact

    def __init__(self, artifact: ParentsArtifact, now: datetime, parts: str) -> None:
        super().__init__(artifact, now, parts)
        self.writer = artifact.writer(parts)
        # the documents scopes, wired up by `ArtifactsRepository.runs`
        self.scopes: dict[str, DocumentsRun] = {}
        self.documents_artifact = DocumentsArtifact(artifact.dataset)
        self.public_prefix = artifact.dataset._model.get_public_prefix()
        self.folders = Writer(self.folders_part(parts))
        self.documents = Writer(self.documents_part(parts))
        # the workers' part directories, adopted before `finish`
        self.staged_parts: list[str] = []
        self.rows = 0  # documents to write across the scopes, for the bar

    @staticmethod
    def folders_part(parts: str) -> str:
        return f"{parts}/folders.staged.json"

    @staticmethod
    def documents_part(parts: str) -> str:
        return f"{parts}/documents.staged.json"

    def _staged(self, part: Callable[[str], str]) -> Iterator[SDict]:
        """One kind of staged row – the adopted parts', then this run's own."""
        for parts in (*self.staged_parts, self.parts):
            if Path(part(parts)).exists():
                yield from smart_stream_json(part(parts))

    def prepare(self, version: int | None) -> None:
        self.folders.open()
        self.documents.open()

    def adopt(self, parts: str, counts: dict[str, int]) -> None:
        """Take a worker's staged rows, and its scopes' document counts."""
        self.staged_parts.append(parts)
        self.rows += sum(counts.get(name, 0) for name in self.scopes)

    def consume(self, payload: EntityPayload) -> None:
        """Stage every potential parent – whatever its scope, so paths resolve
        across origins – and every document in some scope."""
        data = cast(SDict, payload.to_dict())
        parents = data.get("properties", {}).get("parent", [])
        if DocumentsArtifact.is_parent_schema(data.get("schema")):
            self.folders.write(
                {"id": data["id"], "folder": get_filename(data), "parents": parents}
            )
        if not DocumentsArtifact.is_document(data):
            return
        scopes: dict[str, str | None] = {}
        for run in self.scopes.values():
            if run.in_scope(payload):
                op = run.op_for(payload)  # claims the id off `pending`
                run.counts["total"] += 1
                if op is not None:
                    run.counts[op.lower()] += 1
                scopes[run.name] = str(op) if op is not None else None
        if scopes:
            document = self.documents_artifact.make_document(data, self.public_prefix)
            self.documents.write(
                {
                    "doc": document.model_dump(by_alias=True, mode="json"),
                    "parents": parents,
                    "scopes": scopes,
                }
            )

    def resolve(self) -> FolderTree:
        """One folder tree over the staged folder rows, resolved."""
        log.info("Resolving folder paths ...")
        tree = FolderTree()
        with Took() as t:
            for staged in self._staged(self.folders_part):
                tree.put(staged["id"], staged["folder"], staged["parents"])
            tree.paths()
        log.info("Resolved folder paths.", folders=len(tree), took=t.took)
        return tree

    def finish(self) -> None:
        """Write the folders, then every scope's documents – one row per
        resolvable parent, one unpathed row otherwise."""
        self.folders.close()
        self.documents.close()
        tree = self.resolve()
        for folder, name, path_ in tree.folders():
            self.writer.write({"id": folder, "name": name, "path": path_})
        self.counts["total"] = len(tree)
        paths = tree.paths()
        written: Counter[str] = Counter()
        step = 0
        with Took() as t, SyncProgressBar("Writing documents", self.rows) as bar:
            for staged in self._staged(self.documents_part):
                document = staged["doc"]
                rows = [
                    {**document, "path": paths[parent]}
                    for parent in staged["parents"]
                    if parent in paths
                ] or [document]
                for name, op in staged["scopes"].items():
                    self.scopes[name].write(rows, op)
                    written[name] += 1
                step += len(staged["scopes"])
                if step >= PROGRESS_STEP:
                    bar.advance(step)
                    step = 0
            bar.advance(step)
        log.info("Wrote the documents.", took=t.took, **written)

    def close(self) -> None:
        self.writer.close()
        self.folders.close()
        self.documents.close()


class StatisticsRun(ArtifactRun):
    """Folds ``statistics.json`` out of the stream, written in `finish`."""

    artifact: StatisticsArtifact

    def __init__(self, artifact: Artifact, now: datetime, parts: str) -> None:
        super().__init__(artifact, now, parts)
        self.collector = StatsCollector()

    def consume(self, payload: EntityPayload) -> None:
        self.collector.collect(cast(SDict, payload.to_dict()))

    def finish(self) -> None:
        self.artifact.write(self.collector.export())
        self.counts["total"] += self.collector.entities


class ExportSession:
    """Every run of one export, driven as one loop.

    On entry: prepare and load the DEL candidates. On a clean exit: finish and
    commit. The writers close either way, so no codec frame is cut off.
    """

    def __init__(
        self,
        runs: tuple[ArtifactRun, ...],
        version: int | None,
        candidates: Candidates | None = None,
    ) -> None:
        self.runs = runs
        # `None` – no table, or no diff asked for – opens no diff window and
        # records no state
        self.version = version
        self.candidates = candidates
        self.counts: Counter[str] = Counter()

    def prepare(self) -> None:
        """Resolve every diff window and open the staging files – all a worker
        calls; `finish` and `commit` are the parent's."""
        for run in self.runs:
            run.prepare(self.version)

    def close(self) -> None:
        """Close every writer, whatever happened to the rest."""
        with ExitStack() as closing:
            for run in self.runs:
                closing.callback(run.close)

    def adopt(
        self,
        parts: str,
        seen: dict[str, frozenset[str]],
        stats: StatsCollector,
        counts: dict[str, int],
    ) -> None:
        """Fold a worker's part back in: the DEL candidates it met alive, its
        staged documents and its statistics."""
        for run in self.runs:
            if isinstance(run, DiffableRun):
                run.pending -= seen.get(run.name, frozenset())
            if isinstance(run, ParentsRun):
                run.adopt(parts, counts)
            if isinstance(run, StatisticsRun):
                run.collector.merge(stats)

    def __enter__(self) -> Self:
        self.prepare()
        self.load_pending()
        return self

    def load_pending(self) -> None:
        """Fill every active series' `pending` from one scan at the earliest
        window – each series claims its own."""
        active = [r for r in self.diffable if r.since is not None]
        if not active or self.candidates is None:
            return
        since = min(cast(datetime, r.since) for r in active)
        log.info(
            "Loading delete candidates ...",
            series=[str(r.artifact.tag) for r in active],
            since=datetime_iso(since),
        )
        with Took() as t:
            total = 0
            for candidate in self.candidates(since):
                total += 1
                for run in active:
                    if run.claims(candidate):
                        run.pending.add(candidate.id)
        log.info(
            "Loaded delete candidates.",
            candidates=total,
            claimed={str(r.artifact.tag): len(r.pending) for r in active},
            took=t.took,
        )

    def __exit__(self, exc_type: type | None, *args: Any) -> None:
        try:
            if exc_type is None:
                for run in self.runs:
                    run.finish()
        finally:
            self.close()
        if exc_type is None:
            for run in self.runs:
                run.commit(self.version)

    @property
    def diffable(self) -> tuple[DiffableRun, ...]:
        return tuple(r for r in self.runs if isinstance(r, DiffableRun))

    def consume(self, payload: EntityPayload) -> None:
        """Hand one entity to every artifact this run is writing."""
        self.counts["statements"] += len(payload.statements)
        self.counts["entities"] += 1
        for run in self.runs:
            run.consume(payload)

    def result(self) -> dict[str, int]:
        """What this run wrote, flattened per artifact and per diff op."""
        out: Counter[str] = Counter(self.counts)
        for run in self.runs:
            for key, value in run.counts.items():
                out[run.name if key == "total" else f"{run.name}_{key}"] += value
        return dict(out)


class ArtifactsRepository(DatasetHandle):
    """The export artifacts of one dataset.

    Example:
        ```python
        artifacts = ArtifactsRepository("my_dataset", uri)
        artifacts.entities.exists()
        artifacts.documents["crawl"].key
        ```
    """

    @property
    def statements(self) -> StatementsArtifact:
        return StatementsArtifact(self)

    @property
    def entities(self) -> EntitiesArtifact:
        return EntitiesArtifact(self)

    @property
    def documents(self) -> DocumentsArtifact:
        return DocumentsArtifact(self)

    @property
    def parents(self) -> ParentsArtifact:
        return ParentsArtifact(self)

    @property
    def statistics(self) -> StatisticsArtifact:
        return StatisticsArtifact(self)

    @property
    def index(self) -> IndexArtifact:
        return IndexArtifact(self)

    def streamed(self) -> Iterator[Artifact]:
        """Every artifact the sweep writes – all but ``index.json`` – with one
        documents scope per `DOCUMENT_ORIGINS`."""
        yield self.statements
        yield self.entities
        yield self.parents  # ahead of the documents scopes – see `runs`
        for origin in DOCUMENT_ORIGINS:
            yield self.documents[origin]
        yield self.statistics

    def runs(self, now: datetime, parts: str) -> tuple[ArtifactRun, ...]:
        """One run per streamed artifact.

        Args:
            now: When the export started – diff files are named after it.
            parts: Directory the runs write their parts into.
        """
        runs = tuple(a.run(now, parts) for a in self.streamed())
        # the parents run writes the documents scopes' rows – first in
        # `streamed`, so ahead of their DELs
        for run in runs:
            if isinstance(run, ParentsRun):
                run.scopes = {r.name: r for r in runs if isinstance(r, DocumentsRun)}
        return runs

    def resources(self) -> Iterator[DataResource]:
        """Describe every written streamed artifact for ``index.json``."""
        public_prefix = self._model.get_public_prefix()
        if not public_prefix:
            return
        for artifact in self.streamed():
            resource = artifact.make_resource(public_prefix)
            if resource is not None:
                yield resource
