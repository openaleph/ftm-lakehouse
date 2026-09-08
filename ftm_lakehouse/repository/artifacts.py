"""ArtifactsRepository

Store, access and check freshness of the diff-able export artifacts a dataset
produces and keeps track of.

One class per artifact, the class body *is* the declaration. The ways
artifacts differ – whether they carry a diff series, whether they are
compressed, whether they are written as a versioned model dump – are
behaviour, so they are subclasses rather than flags nobody would enjoy
branching on later.

The distinction everything else rests on is `Artifact.tag` versus
`Artifact.key`: the tag is the codec-free key, which is also the freshness tag
and the name a diff series takes; the key carries the dataset's compression
codec and is what actually exists on disk. Conflating them moves a dataset's
tags the moment it configures a codec.

An `Artifact` is a *declaration bound to a dataset* and holds no state of its
own, so constructing one is free and the accessors below hand out fresh
instances. Everything true only while an export is running – the open writers,
the diff window, the counters – lives on an `ArtifactRun` instead.
"""

import csv
from collections import Counter
from contextlib import ExitStack, contextmanager
from datetime import datetime, timezone
from enum import StrEnum
from typing import IO, Any, ClassVar, Generator, Iterable, Iterator, Self, cast

from anystore.io import Writer
from anystore.io.write import Formats
from anystore.logic.compress import CompressKind
from anystore.model.base import BaseModel
from anystore.types import SDict
from anystore.util import join_uri
from followthemoney import model
from followthemoney.dataset import DataResource
from ftmq.util import datetime_iso
from rigour.mime.types import CSV, FTM, JSON

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import CHECKSUM_ALGORITHM
from ftm_lakehouse.logic.entities.aggregate import EntityPayload
from ftm_lakehouse.logic.path import DateTimeKey, StoreKey
from ftm_lakehouse.model.file import Document, Documents
from ftm_lakehouse.repository.base import DatasetHandle
from ftm_lakehouse.util import validate_origin

DOCUMENT_FIELDNAMES = list(Document.model_fields)
"""Column order of the documents csv, fixed up front.

A writer would otherwise take it from whichever row comes first, which is
wrong for a diff – a ``DEL`` row carries only ``op`` and ``id``, and landing
first it would cap the header at two columns.
"""

DOCUMENT_ORIGINS: tuple[str | None, ...] = (None, tag.CRAWL_ORIGIN)
"""Scopes the documents export is written for – every origin, plus a csv /
diff series restricted to crawled files, so a consumer of the crawl can follow
it without the archive's other sources bleeding in."""


class ExportKind(StrEnum):
    """The available dataset exports.

    Lives here rather than with the export operation because each `Artifact`
    declares the kind it answers to, and ``repository/`` cannot import
    ``operation/``. `ftm_lakehouse.operation.export` re-exports it.
    """

    all = "all"
    statements = "statements"
    entities = "entities"
    documents = "documents"
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
    """Create a diff action envelope for an entity payload.

    Ref. https://www.opensanctions.org/docs/bulk/delta/
    """
    return {"op": str(op), "entity": data}


class Artifact:
    """One export artifact, bound to a dataset.

    Stateless – see the module docstring. Per-run state lives on `ArtifactRun`.

    Args:
        dataset: The dataset handle the artifact belongs to.
        origin: Source tag this variant is scoped to, ``None`` for the
            unscoped one.
    """

    base: ClassVar[StoreKey]
    kind: ClassVar[ExportKind]
    mime_type: ClassVar[str] = CSV
    dependencies: ClassVar[tuple[str | StoreKey, ...]] = (tag.STATEMENTS_OPTIMIZED,)
    compressed: ClassVar[bool] = True
    fieldnames: ClassVar[list[str] | None] = None

    def __init__(self, dataset: DatasetHandle, origin: str | None = None) -> None:
        self.dataset = dataset
        # an origin scope names a file *and* reaches SQL as a filter, so it is
        # validated where it enters, like every other origin in the codebase
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

    def is_fresh(self) -> bool:
        """Whether the artifact exists *and* is newer than its dependencies.

        A missing artifact is never fresh – there is nothing to be current.
        """
        if not self.exists():
            return False
        return self.dataset._tags.is_latest(self.tag, self.dependencies)

    def touch(self, ts: datetime | None = None) -> None:
        """Stamp the freshness tag"""
        self.dataset._tags.set(self.tag, ts)

    def writer(self, lazy: bool = False) -> Writer:
        """A writer for this artifact, in its own format and codec.

        Args:
            lazy: Defer creating the file to the first row. An artifact is a
                whole picture of the store, so it opens eagerly – an empty
                sweep must truncate a stale one rather than leave it. A diff
                is the opposite: no changes means no file.
        """
        return Writer(
            self.dataset._store.to_uri(self.key),
            output_format=self.format,
            compression=self.compression,
            fieldnames=self.fieldnames,
            lazy=lazy,
        )

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

    def run(self, now: datetime) -> "ArtifactRun":
        """The per-run object that writes this artifact."""
        return ArtifactRun(self, now)


class DiffableArtifact(Artifact):
    """An artifact that keeps a diff series alongside it.

    Owns the series' paths and its stored ``{timestamp}:{version}`` state.
    Whether *this* run writes a diff, and against which window, belongs to
    `DiffableRun`.

    The series directory carries neither extension nor codec – it doubles as
    the series' freshness tag and diff-state key, so it must not move when a
    dataset changes its compression.
    """

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

    def diff_writer(self, ts: datetime) -> Writer:
        """A writer for one diff file in this series."""
        fieldnames = ["op", *self.fieldnames] if self.fieldnames else None
        return Writer(
            self.dataset._store.to_uri(self.series(ts) + self.compression),
            output_format=self.format,
            compression=self.compression,
            fieldnames=fieldnames,
            # a series with no changes in the window leaves no file at all
            lazy=True,
        )


class VersionedArtifact(Artifact):
    """An artifact written as a versioned model dump, never compressed.

    `VersionStore` keeps a timestamped snapshot beside the current copy, so
    these are plain JSON whatever the dataset configures.
    """

    mime_type = JSON
    compressed = False

    def write(self, obj: BaseModel) -> None:
        """Write the model and stamp the freshness tag."""
        self.dataset._versions.make(self.key, obj)
        self.touch()


class StatementsArtifact(Artifact):
    """Every statement, sorted.

    Written by the vectorised Arrow tee inside the sweep rather than row by
    row, so its run consumes nothing.
    """

    base = path.EXPORTS_STATEMENTS
    kind = ExportKind.statements


class EntitiesArtifact(DiffableArtifact):
    """The aggregated entities, and their delta series."""

    base = path.ENTITIES_JSON
    kind = ExportKind.entities
    mime_type = FTM
    diffs = path.DIFFS_ENTITIES

    def run(self, now: datetime) -> "EntitiesRun":
        return EntitiesRun(self, now)


class DocumentsArtifact(DiffableArtifact):
    """Document metadata, and its delta series – origin-scopable.

    Owns the *write* side of the documents export: which entities belong in
    it, and what rows each contributes. The query side – the folder map, the
    ad-hoc lookups, the tombstoned ids – stays on
    [`DocumentRepository`][ftm_lakehouse.repository.documents.DocumentRepository].
    """

    base = path.EXPORTS_DOCUMENTS
    kind = ExportKind.documents
    diffs = path.DIFFS_DOCUMENTS
    fieldnames = DOCUMENT_FIELDNAMES

    @staticmethod
    def is_document(data: SDict) -> bool:
        """Whether an entity dict belongs in the documents export.

        The in-Python spelling of ``Q_DOCUMENTS``: a ``Document`` descendant
        that is not a bare ``Folder`` (those are the path scaffolding, not
        files) and actually has a content hash to point at.
        """
        schema = data.get("schema")
        if not schema:
            return False
        schema_ = model.get(str(schema))
        if schema_ is None or not schema_.is_a("Document"):
            return False
        if schema_.name == "Folder":
            return False
        return bool(data.get("properties", {}).get("contentHash"))

    def make_documents(
        self,
        data: SDict,
        paths: dict[str, str],
        public_prefix: str | None = None,
    ) -> Documents:
        """The csv rows one entity dict contributes.

        One row per resolvable parent folder, so a file living in two places is
        listed under both; a file with no resolvable parent still gets its one
        unpathed row. Each row is its own object, so a caller may materialise
        them – the diff writes the same rows the csv did.

        Args:
            data: Entity dict, as `EntityPayload.to_dict` returns.
            paths: Folder id to path map from `DocumentRepository.make_paths`.
            public_prefix: Public url prefix to build blob links against.

        Yields:
            One `Document` per resolvable parent, else a single unpathed one.
        """
        document = Document.from_entity_dict(data)
        if public_prefix:
            document.public_url = join_uri(
                public_prefix, path.ArchiveKey(document.checksum).blob
            )
        paths_ = [p for p in data.get("properties", {}).get("parent", []) if p in paths]
        if not paths_:
            yield document
            return
        # a copy per parent: the same file in two folders is two rows, and a
        # caller that materialises them must not get two views of one object
        for parent in paths_:
            yield document.model_copy(update={"path": paths[parent]})

    def stream(self) -> Documents:
        """Stream this variant's csv back as `Document` models."""
        if not self.exists():
            return
        with self.reader("r") as raw:
            for row in csv.DictReader(raw):
                # csv values arrive as strings; pydantic coerces size / updated_at
                yield Document(**cast(dict[str, Any], row))

    def run(self, now: datetime) -> "DocumentsRun":
        return DocumentsRun(self, now)


class StatisticsArtifact(VersionedArtifact):
    """Entity counts and facets, from a global SQL aggregate."""

    base = path.EXPORTS_STATISTICS
    kind = ExportKind.statistics


class IndexArtifact(VersionedArtifact):
    """The dataset index – the config enriched with resources and statistics.

    Depends on what the other exports produced, which is why it runs last.
    """

    base = path.INDEX
    kind = ExportKind.index
    dependencies = (
        path.CONFIG,
        path.EXPORTS_STATISTICS,
        path.ENTITIES_JSON,
        path.EXPORTS_DOCUMENTS,
    )


ARTIFACTS: tuple[type[Artifact], ...] = (
    StatementsArtifact,
    EntitiesArtifact,
    DocumentsArtifact,
    StatisticsArtifact,
    IndexArtifact,
)

ARTIFACTS_BY_KIND: dict[ExportKind, type[Artifact]] = {a.kind: a for a in ARTIFACTS}

SWEEP_KINDS = (ExportKind.statements, ExportKind.entities, ExportKind.documents)
"""The kinds the entity sweep produces – the rest are computed, not streamed."""


class ArtifactRun:
    """One artifact being written by one export run.

    Everything true only for the duration of an export – the open writer, the
    diff window, the counts – lives here rather than on the stateless
    `Artifact`. `consume` is the hook the sweep calls for every entity; the
    base does nothing, which is right for ``statements.csv``, written by the
    Arrow tee rather than row by row.
    """

    def __init__(self, artifact: Artifact, now: datetime) -> None:
        self.artifact = artifact
        self.now = now
        self.counts: Counter[str] = Counter()

    def __repr__(self) -> str:
        return f"<{type(self).__name__}({self.artifact.key})>"

    @property
    def name(self) -> str:
        return self.artifact.name

    def prepare(self, version: int | None) -> None:
        """Resolve anything this run needs before the sweep starts."""

    def consume(self, payload: EntityPayload) -> None:
        """Take one entity from the sweep.

        `EntityPayload.to_dict` is memoised, so every run can ask the
        payload for its own view without the fan-out paying for it twice.
        """

    def finish(self) -> None:
        """Write anything only knowable once the sweep has ended."""

    def commit(self, version: int | None) -> None:
        """Record whatever the next run is taken against."""

    def close(self) -> None:
        """Release whatever the run opened."""


class WritingRun(ArtifactRun):
    """A run that writes its artifact row by row.

    Opens eagerly, because an export is a whole picture of the store: a sweep
    that yields nothing has to leave an empty artifact, not the previous run's
    file freshly stamped as current.
    """

    def __init__(self, artifact: Artifact, now: datetime) -> None:
        super().__init__(artifact, now)
        self.writer = artifact.writer()

    def prepare(self, version: int | None) -> None:
        self.writer.open()

    def close(self) -> None:
        self.writer.close()


class DiffableRun(WritingRun):
    """An artifact run that also maintains a diff series.

    A diff has two halves. Gaining statements the sweep can see for itself,
    from the entity's folded ``first_seen`` bounds. Losing them it cannot – a
    fully tombstoned entity never comes past – so the tombstoned ids are
    loaded up front and claimed as the sweep meets them alive; whatever is
    left over at `finish` is gone, and becomes a ``DEL``.
    """

    artifact: DiffableArtifact

    def __init__(self, artifact: DiffableArtifact, now: datetime) -> None:
        super().__init__(artifact, now)
        self.since: datetime | None = None
        self.since_iso: str | None = None
        self.active = False
        self.pending: set[str] = set()
        self.diff: Writer | None = None

    def prepare(self, version: int | None) -> None:
        """Decide whether this run writes a diff, and against what.

        A series is inactive when there is nothing to diff *or* nothing to
        diff against:

        - no table yet – there is no content at all;
        - no prior state – the first run records where the next diff starts
          and writes no file. The full picture at that point is the export
          sitting next to it;
        - the delta version has not moved since the last run.
        """
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
        # through the same `datetime_iso` the folded bounds use, so both sides
        # are the fixed-width UTC spelling and compare lexically - including
        # the timestamps FtM truncated to whole seconds, whose `isoformat`
        # drops the fractional part and sorts before any same-second value
        # that has one
        self.since_iso = datetime_iso(last_timestamp)
        self.active = True
        self.diff = self.artifact.diff_writer(self.now)
        self.pending = set(self.deleted_ids(last_timestamp))

    def deleted_ids(self, since: datetime) -> Iterator[str]:
        """Ids tombstoned since the given timestamp – the DEL candidates."""
        return iter(())

    def op_for(self, payload: EntityPayload) -> DiffOp | None:
        """This entity's diff op, or ``None`` when it did not change.

        Claims the id off `pending` if it is a candidate: a partly-tombstoned
        entity is still here, so it is a change and not a delete, and claiming
        it now is what keeps it out of the DEL drain.

        ``ADD`` means every statement is new – the bound spans *all* of them,
        ``id`` rows included, so an entity that existed as a bare id and just
        gained its first properties is a ``MOD``. The answer is only ever as
        good as what survived: a fragment re-emission supersedes every row it
        replaces, so an entity rewritten wholesale leaves nothing older behind
        and reads as an ``ADD``. Both carry the entity whole and a consumer
        indexes either the same way – only ``DEL`` means something different.
        """
        if not self.active or self.since_iso is None or not payload.id:
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
        """Record the state the next diff is taken against.

        Whether or not a file was written – a run that found no changes still
        moves the series forward, so the next one does not re-examine the same
        window.
        """
        if version is not None:
            self.artifact.set_state(self.now, version)

    def close(self) -> None:
        super().close()
        if self.diff is not None:
            self.diff.close()


class EntitiesRun(DiffableRun):
    """Writes ``entities.ftm.json`` and its delta series."""

    def __init__(self, artifact: DiffableArtifact, now: datetime) -> None:
        super().__init__(artifact, now)
        # local import: `factories` imports this module for `ArtifactsRepository`
        from ftm_lakehouse.repository.factories import get_entities

        self.entities = get_entities(artifact.dataset.dataset, artifact.dataset.uri)

    def deleted_ids(self, since: datetime) -> Iterator[str]:
        return self.entities.deleted_ids(since)

    def consume(self, payload: EntityPayload) -> None:
        data = payload.to_dict()
        self.writer.write(data)
        # no `total`: every entity the sweep yields is written, so the count is
        # the session's `entities` – adding it here would double it
        op = self.op_for(payload)
        if op is None or self.diff is None:
            return
        self.diff.write(make_envelope(data, op))
        self.counts[op.lower()] += 1

    def write_delete(self, entity_id: str) -> None:
        cast(Writer, self.diff).write(make_envelope({"id": entity_id}, DiffOp.DEL))


class DocumentsRun(DiffableRun):
    """Writes one origin scope of ``documents.csv`` and its delta series."""

    artifact: DocumentsArtifact

    def __init__(self, artifact: DocumentsArtifact, now: datetime) -> None:
        super().__init__(artifact, now)
        # local import: `factories` imports this module for `ArtifactsRepository`
        from ftm_lakehouse.repository.factories import get_documents

        self.documents = get_documents(artifact.dataset.dataset, artifact.dataset.uri)
        self.paths = self.documents.make_paths()
        self.public_prefix = artifact.dataset._model.get_public_prefix()

    def deleted_ids(self, since: datetime) -> Iterator[str]:
        return self.documents.deleted_ids(since, self.artifact.origin)

    def consume(self, payload: EntityPayload) -> None:
        if self.artifact.origin and self.artifact.origin not in payload.origins:
            return
        data = payload.to_dict()
        if not self.artifact.is_document(data):
            return
        rows = list(self.artifact.make_documents(data, self.paths, self.public_prefix))
        for row in rows:
            self.writer.write(row.model_dump(by_alias=True, mode="json"))
        self.counts["total"] += 1

        if self.diff is not None:
            op = self.op_for(payload)
            if op is None:
                return
            for row in rows:
                self.diff.write(
                    {"op": str(op), **row.model_dump(by_alias=True, mode="json")}
                )
            self.counts[op.lower()] += 1

    def write_delete(self, entity_id: str) -> None:
        cast(Writer, self.diff).write({"op": str(DiffOp.DEL), "id": entity_id})


class ExportSession:
    """Every artifact one export run is writing, driven as one loop.

    Opens on entry, drains the DEL candidates and closes on a clean exit; on
    an exception the writers still close (so no codec frame is truncated) but
    nothing is finished or committed.
    """

    def __init__(
        self,
        runs: tuple[ArtifactRun, ...],
        version: int | None,
        make_diff: bool = True,
    ) -> None:
        self.runs = runs
        self.version = version
        self.make_diff = make_diff
        self.counts: Counter[str] = Counter()

    def __enter__(self) -> Self:
        # a run prepared against no version has no window to diff, which is
        # exactly what `--no-diff` asks for
        version = self.version if self.make_diff else None
        for run in self.runs:
            run.prepare(version)
        return self

    def __exit__(self, exc_type: type | None, *args: Any) -> None:
        try:
            if exc_type is None:
                for run in self.runs:
                    run.finish()
        finally:
            # every artifact gets closed even if one fails, or the rest are
            # left with an unterminated codec frame
            with ExitStack() as closing:
                for run in self.runs:
                    closing.callback(run.close)
        # only a run that actually resolved a window may move it: committing
        # after `make_diff=False` would skip every change since the last diff
        if exc_type is None and self.make_diff:
            for run in self.runs:
                run.commit(self.version)

    @property
    def diffable(self) -> tuple[DiffableRun, ...]:
        return tuple(r for r in self.runs if isinstance(r, DiffableRun))

    def consume(self, payload: EntityPayload) -> None:
        """Hand one entity to every artifact this run is writing."""
        self.counts["statements"] += len(payload.statements)
        self.counts["entities"] += 1
        if not payload.to_dict():  # no resolvable schema
            return
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
    """The export artifacts one dataset produces.

    Binds the declarations above to this dataset, so a caller addresses an
    artifact by kind and gets something that knows where it lives, whether it
    is current, how to write it and how to describe itself in ``index.json``.

    Example:
        ```python
        artifacts = ArtifactsRepository("my_dataset", uri)
        artifacts.entities.is_fresh()
        artifacts.documents["crawl"].key
        ```
    """

    def __getitem__(self, kind: ExportKind | str) -> Artifact:
        """The artifact answering to one export kind, bound to this dataset."""
        return ARTIFACTS_BY_KIND[ExportKind(kind)](self)

    def __iter__(self) -> Iterator[Artifact]:
        yield from (a(self) for a in ARTIFACTS)

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
    def statistics(self) -> StatisticsArtifact:
        return StatisticsArtifact(self)

    @property
    def index(self) -> IndexArtifact:
        return IndexArtifact(self)

    def document_scopes(self) -> Iterator[DocumentsArtifact]:
        """The documents variants a full export writes, one per origin."""
        yield from (self.documents[origin] for origin in DOCUMENT_ORIGINS)

    def written_by(self, kinds: Iterable[ExportKind]) -> Iterator[Artifact]:
        """Every artifact those kinds cover, origin scopes expanded."""
        for kind in kinds:
            if kind == ExportKind.documents:
                yield from self.document_scopes()
            else:
                yield self[kind]

    def session(
        self,
        now: datetime,
        kinds: Iterable[ExportKind],
        version: int | None,
        make_diff: bool = True,
    ) -> ExportSession:
        """The artifacts an export run covers, ready to be driven as one loop.

        Args:
            now: Timestamp the run started – diff files are named after it and
                diff states are recorded at it.
            kinds: Which exports this run covers.
            version: Current delta table version, which the diff series
                resolve their window against.
            make_diff: Whether diff series run at all.
        """
        runs = tuple(a.run(now) for a in self.written_by(kinds))
        return ExportSession(runs, version, make_diff)

    def resources(self) -> Iterator[DataResource]:
        """Describe every written artifact for ``index.json``.

        ``index.json`` itself is left out – it is the file being written.
        """
        public_prefix = self._model.get_public_prefix()
        if not public_prefix:
            return
        for artifact in (
            self.statements,
            self.entities,
            *self.document_scopes(),
            self.statistics,
        ):
            resource = artifact.make_resource(public_prefix)
            if resource is not None:
                yield resource
