"""ParquetStore – one Delta Lake table per dataset, partitioned by
``(shard, bucket, origin)``.

Layout:
    statements/shard={s}/bucket={b}/origin={o}/{part,merged}-*.parquet
"""

import heapq
import json
import os
import posixpath
from contextlib import ExitStack, closing, contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from functools import cache
from itertools import chain
from operator import itemgetter
from tempfile import TemporaryDirectory
from threading import RLock
from typing import IO, Any, Callable, Iterable, Iterator, TypeVar, cast
from urllib.parse import unquote

import duckdb
import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.parquet as pq
from anystore.decorators import error_handler
from anystore.interface.lock import Lock
from anystore.io import SyncProgressBar
from anystore.logging import get_logger
from anystore.store import get_store
from anystore.types import Uri
from anystore.util import Took, ensure_uri, ensure_uuid, join_uri, mask_uri
from deltalake import DeltaTable, Schema, write_deltalake
from deltalake.exceptions import DeltaError, TableNotFoundError
from deltalake.transaction import AddAction, RemoveAction
from followthemoney.statement import StatementDict
from ftmq.aggregate import aggregate_statement_payloads
from ftmq.query import Query, Sql, SqlSource
from ftmq.store.lake import (
    PRUNE,
    LakeStore,
    setup_duckdb_storage,
    storage_options,
    writer_for_bucket,
)
from ftmq.types import StatementEntities, Statements
from pyarrow.csv import (  # type: ignore[attr-defined]  # missing from stubs
    CSVWriter,
    WriteOptions,
)
from rigour.time import utc_now
from sqlalchemy import Select

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.helpers.shards import entity_shard
from ftm_lakehouse.logic.merge import entity_chunks
from ftm_lakehouse.logic.parquet import (
    MERGE_BATCH_BYTES,
    MERGE_BATCH_ROWS,
    MERGE_CHUNK_BYTES,
    MERGE_COMMIT_BATCH,
    MERGE_DEDUPE_BYTES,
    MERGE_MAX_RUNS,
    MERGE_RUN_BYTES,
    MERGED_PREFIX,
    STAGING_DIR,
    SWEEP_BATCH_SIZE,
    TABLE_CONFIGURATION,
    WRITE_FLUSH_PARTITIONS,
    build_merge_sql,
    build_shard_sql,
    dedupe_rows_sql,
    delta_scan_sql,
    duckdb_config,
    live_rows_sql,
    make_prune_by_shard,
    partition_source_sql,
    raw_view_sql,
    relation_source_sql,
    shard_target_file_size,
    worker_duckdb_config,
)
from ftm_lakehouse.model.dataset import DEFAULT_SHARDS
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    TABLE,
    TABLE_RAW,
    DeleteCandidate,
    LakehouseStatement,
    deleted_candidates_select,
    statement_csv_select,
)
from ftm_lakehouse.storage.tags import TagStore
from ftm_lakehouse.util import (
    parse_byte_size,
    prefetch,
    process_map,
    validate_dataset_name,
    validate_origin,
)

PARTITIONS = ["shard", "bucket", "origin"]

FILE_SCHEMA = pa.schema([f for f in SHARDED_SCHEMA if f.name not in PARTITIONS])
"""A data file's columns – the partition values live in its path."""

T = TypeVar("T")

Partition = tuple[str, str, str]
"""A ``(shard, bucket, origin)`` partition key."""

Files = list[tuple[str, int]]
"""A partition's data files as ``(path, size)``, the path table-relative and
percent-decoded."""


@dataclass
class MergeTask:
    """One partition's merge as handed to a worker – plain data, so it pickles."""

    partition: Partition
    files: Files
    root: str
    grace_cutoff: datetime
    duckdb_config: dict[str, str]


@dataclass
class MergeResult:
    """A worker's merged ``(path, size, stats)`` files in ``entity_id`` order –
    none when every row was reaped, ``stats`` the ``add`` stats delta-rs wrote –
    and its ``took`` for the parent to log."""

    task: MergeTask
    files: list[tuple[str, int, str]]
    took: timedelta


Partitions = dict[Partition, tuple[Files, bool]]
"""``(files, clean)`` per partition – ``clean`` when every file is `merge`
output (`MERGED_PREFIX`)."""

Pairs = dict[tuple[str, str], list[tuple[Partition, Files, bool]]]
"""`Partitions` grouped per ``(shard, bucket)`` pair."""


def _compile(sql: Select) -> str:
    """``sql`` as DuckDB SQL, parameters inlined."""
    return str(sql.compile(compile_kwargs={"literal_binds": True}))


def _relation(root: str, partition: Partition, files: Files) -> str:
    """One partition's files as a relation (`partition_source_sql`)."""
    return partition_source_sql([f"{root}/{file}" for file, _ in files], *partition)


def merge_partition(task: MergeTask) -> MergeResult:
    """Merge one partition into new data files next to its old ones.

    The partition is read as ``entity_id``-sorted streams (`_runs`) and cut
    into entity-closed chunks
    ([`entity_chunks`][ftm_lakehouse.logic.merge.entity_chunks]), each deduped
    by [`build_merge_sql`][ftm_lakehouse.logic.parquet.build_merge_sql] on its
    own – every window key holds ``entity_id``, so the chunks give the rows the
    whole would – and written as they come, sorted within and ascending across
    chunks. Batches hold about `MERGE_BATCH_BYTES` at the partition's average
    row size, so memory follows the merge constants, not the partition.

    Writes but does not commit – [`ParquetStore.merge`][ParquetStore.merge]
    does; an uncommitted file is an orphan the next ``vacuum`` removes.
    """
    bucket = task.partition[1]
    if threads := task.duckdb_config.get("threads"):
        pa.set_cpu_count(int(threads))  # the Arrow sorts, split like DuckDB's
    with (
        Took() as t,
        _connect(task.duckdb_config) as con,
        TemporaryDirectory(prefix="lakehouse-merge-") as tmp,
    ):
        con.execute("SET preserve_insertion_order = true")  # merged files' order
        # a batch's strings may pass 2GiB when `_batch_rows` underestimates
        con.execute("SET arrow_large_buffer_size = true")
        rows = _batch_rows(con, _relation(task.root, task.partition, task.files[:1]))
        chunks = entity_chunks(_runs(con, task, tmp, rows), MERGE_DEDUPE_BYTES)
        batches = pa.RecordBatchReader.from_batches(
            FILE_SCHEMA, _dedupe(con, task, chunks, rows)
        )
        directory = posixpath.dirname(task.files[0][0])
        prefix = f"{directory}/{MERGED_PREFIX}{ensure_uuid()}"
        files = _write_merged(task.root, batches, bucket, prefix)
    return MergeResult(task, files, t.took)


def _batch_rows(con: duckdb.DuckDBPyConnection, source: str) -> int:
    """Rows a batch of about `MERGE_BATCH_BYTES` holds at ``source``'s row size
    – the Arrow bytes of its first rows averaged – at most `MERGE_BATCH_ROWS`,
    at least 1."""
    sql = f"SELECT * EXCLUDE ({', '.join(PARTITIONS)}) FROM {source} LIMIT 4096"
    with closing(con.cursor()) as cur:
        sample = cur.execute(sql).to_arrow_table()
    row_bytes = max(1, int(sample.nbytes) // max(int(sample.num_rows), 1))
    return max(1, min(MERGE_BATCH_ROWS, MERGE_BATCH_BYTES // row_bytes))


def _fit(
    batches: Iterable[pa.RecordBatch], sliced: bool = False
) -> Iterator[pa.RecordBatch]:
    """``batches`` as `FILE_SCHEMA` batches of at most `MERGE_BATCH_BYTES` (or
    one row), halved until they fit – `_batch_rows` estimates from a sample, and
    a batch it underestimated can hold more than the 2GiB of strings a
    `FILE_SCHEMA` column takes."""
    for batch in batches:
        if batch.num_rows > 1 and batch.nbytes > MERGE_BATCH_BYTES:
            half = batch.num_rows // 2
            yield from _fit([batch.slice(0, half), batch.slice(half)], True)
        else:
            # a slice keeps its parent's offsets, which the cast checks
            yield (pa.concat_batches([batch]) if sliced else batch).cast(FILE_SCHEMA)


def _scan(
    con: duckdb.DuckDBPyConnection,
    root: str,
    partition: Partition,
    files: list[str],
    rows: int,
) -> Iterator[pa.RecordBatch]:
    """``files`` in file order as `FILE_SCHEMA` batches of ``rows`` at most
    (`_fit`), on a cursor of their own."""
    source = partition_source_sql([f"{root}/{f}" for f in files], *partition)
    sql = f"SELECT * EXCLUDE ({', '.join(PARTITIONS)}) FROM {source}"
    with closing(con.cursor()) as cur:
        yield from _fit(cur.execute(sql).to_arrow_reader(rows))


def _read_run(path: str, rows: int) -> Iterator[pa.RecordBatch]:
    """A temporary run's batches of ``rows`` at most (`_fit`) in file order."""
    with pq.ParquetFile(path) as file:
        yield from _fit(file.iter_batches(rows))


def _write_run(path: str, tables: Iterable[pa.Table], rows: int) -> str:
    """``tables`` as one temporary run at ``path`` – zstd level 1, row groups
    of ``rows``."""
    with pq.ParquetWriter(
        path, FILE_SCHEMA, compression="zstd", compression_level=1
    ) as writer:
        for table in tables:
            writer.write_table(table, row_group_size=rows)
    return path


def _merge_runs(paths: list[str], path: str, rows: int) -> str:
    """Sorted runs ``paths`` merged into one sorted run at ``path``, the inputs
    removed – through the chunker, so one `MERGE_DEDUPE_BYTES` chunk is held."""
    chunks = entity_chunks([_read_run(p, rows) for p in paths], MERGE_DEDUPE_BYTES)
    _write_run(path, (chunk.sort_by("entity_id") for chunk in chunks), rows)
    for p in paths:
        os.remove(p)
    return path


def _runs(
    con: duckdb.DuckDBPyConnection, task: MergeTask, tmp: str, rows: int
) -> list[Iterator[pa.RecordBatch]]:
    """The partition as ``entity_id``-sorted `FILE_SCHEMA` streams of ``rows``
    a batch.

    The last merge's files are one stream in file order (their sort). The
    ``part-*`` files are read in file order and sorted in Arrow
    `MERGE_RUN_BYTES` at a time into temporary parquet runs under ``tmp``; more
    than `MERGE_MAX_RUNS` of them are first merged (`_merge_runs`) in rounds of
    that many, so no more streams than that are ever open.
    """
    root, partition = task.root, task.partition
    files = [f for f, _ in task.files]
    merged = [f for f in files if posixpath.basename(f).startswith(MERGED_PREFIX)]
    parts = [f for f in files if f not in merged]
    streams: list[Iterator[pa.RecordBatch]] = []
    if merged:
        streams.append(_scan(con, root, partition, merged, rows))
    if not parts:
        return streams
    runs: list[str] = []
    held: list[pa.RecordBatch] = []
    nbytes = 0
    for batch in chain(_scan(con, root, partition, parts, rows), [None]):
        if batch is not None:
            held.append(batch)
            nbytes += batch.nbytes
        if held and (batch is None or nbytes >= MERGE_RUN_BYTES):
            table = pa.Table.from_batches(held, FILE_SCHEMA).sort_by("entity_id")
            held, nbytes = [], 0
            runs.append(
                _write_run(f"{tmp}/run-0-{len(runs):05d}.parquet", [table], rows)
            )
            del table
    rounds = 0
    while len(runs) > MERGE_MAX_RUNS:
        rounds += 1
        runs = [
            _merge_runs(
                runs[i : i + MERGE_MAX_RUNS],
                f"{tmp}/run-{rounds}-{i:05d}.parquet",
                rows,
            )
            for i in range(0, len(runs), MERGE_MAX_RUNS)
        ]
    return streams + [_read_run(run, rows) for run in runs]


def _dedupe(
    con: duckdb.DuckDBPyConnection,
    task: MergeTask,
    chunks: Iterable[pa.Table],
    rows: int,
) -> Iterator[pa.RecordBatch]:
    """Each chunk's rows through
    [`build_merge_sql`][ftm_lakehouse.logic.parquet.build_merge_sql], sorted –
    the chunks ascend, so the whole does."""
    shard, bucket, origin = task.partition
    sql = build_merge_sql(
        shard,
        bucket,
        origin,
        task.grace_cutoff,
        source=relation_source_sql("chunk", shard, bucket, origin),
        select=f"* EXCLUDE ({', '.join(PARTITIONS)})",
    )
    for chunk in chunks:
        con.register("chunk", chunk)
        yield from _fit(con.execute(sql).to_arrow_reader(rows))
        con.unregister("chunk")


def _write_merged(
    root: str, batches: pa.RecordBatchReader, bucket: str, prefix: str
) -> list[tuple[str, int, str]]:
    """Write ``batches`` as ``{prefix}-{n}.zstd.parquet`` files under ``root``,
    uncommitted.

    delta-rs writes them as it writes an append – the bucket's
    `writer_for_bucket` profile, full ``add`` stats: each `MERGE_CHUNK_BYTES`
    chunk is one ``write_deltalake`` into a staging table under `STAGING_DIR`,
    whose file moves into place with the stats logged for it. The names sort in
    write order, the rows in ``batches`` order – a chunk goes in as one record
    batch (`_chunks`).
    """
    store = get_store(root)
    fs_key = store._keys.to_fs_key
    stage = f"{STAGING_DIR}/{ensure_uuid()}"
    files: list[tuple[str, int, str]] = []
    try:
        for version, chunk in enumerate(_chunks(batches, MERGE_CHUNK_BYTES)):
            write_deltalake(
                f"{root}/{stage}",
                chunk,
                mode="append",
                writer_properties=writer_for_bucket(bucket),
                target_file_size=1 << 62,  # one file per chunk
                storage_options=storage_options(),
            )
            with store.open(f"{stage}/_delta_log/{version:020d}.json", "r") as log:
                actions = [json.loads(line) for line in log]
            for add in (action["add"] for action in actions if "add" in action):
                file = f"{prefix}-{len(files):05d}.zstd.parquet"
                store._fs.mv(fs_key(f"{stage}/{unquote(add['path'])}"), fs_key(file))
                files.append((file, add["size"], add["stats"]))
    finally:
        if store.exists(stage):
            store._fs.rm(fs_key(stage), recursive=True)
    return files


def _chunks(batches: Iterable[pa.RecordBatch], size: int) -> Iterator[pa.RecordBatch]:
    """``batches`` cut into chunks of at least ``size`` Arrow bytes (the last one
    short), empty batches dropped, each concatenated into one record batch:
    delta-rs reorders the batches of a multi-batch write within the file
    (measured, deltalake 1.6.6), and file order is the sweep's sort."""
    chunk: list[pa.RecordBatch] = []
    nbytes = 0
    for batch in batches:
        if batch.num_rows:
            chunk.append(batch)
            nbytes += batch.nbytes
        if nbytes >= size:
            # the list goes before the yield, so a write holds one copy
            whole, chunk, nbytes = pa.concat_batches(chunk), [], 0
            yield whole
            del whole
    if chunk:
        whole, chunk = pa.concat_batches(chunk), []
        yield whole


def pair_source(
    root: str, sources: list[tuple[Partition, Files, bool]]
) -> tuple[str, bool]:
    """One ``(shard, bucket)`` pair's origin partitions as one relation, and
    whether all of them are clean."""
    sql = " UNION ALL ".join(_relation(root, p, files) for p, files, _ in sources)
    return f"({sql})", all(clean for _, _, clean in sources)


@dataclass(frozen=True)
class SweepPartition:
    """One origin partition of a pair, as the sweep reads it."""

    relation: str
    clean: bool
    """Only `merge` output – read as a plain scan, else through the dedupe."""
    presorted: bool
    """Only one `merge`'s files, already in ``entity_id`` order – read unsorted."""
    size: int
    """Bytes of its files."""


@dataclass(frozen=True)
class SweepSource:
    """One ``(shard, bucket)`` pair as the export sweeps it – plain data, so it
    pickles to a worker."""

    key: tuple[str, str]
    partitions: list[SweepPartition]

    @property
    def size(self) -> int:
        """Bytes of the pair's files."""
        return sum(partition.size for partition in self.partitions)


def register_partition(
    cur: duckdb.DuckDBPyConnection,
    source: str,
    clean: bool,
    raw: str = TABLE_RAW.name,
    live: str = TABLE.name,
) -> None:
    """Point the ``raw`` / ``live`` temp views (``statement_raw`` /
    ``statement``) at ``source``.

    ``live`` is a plain scan over a clean source, the dedupe otherwise. The
    footer cache is set here: at connect time it autoloads parquet too early
    (offline).
    """
    cur.execute("SET parquet_metadata_cache = true")
    cur.execute(f"CREATE OR REPLACE TEMP VIEW {raw} AS SELECT * FROM {source}")
    rows = live_rows_sql if clean else dedupe_rows_sql
    cur.execute(f"CREATE OR REPLACE TEMP VIEW {live} AS {rows(raw)}")


@contextmanager
def _connect(config: dict[str, str]) -> Iterator[duckdb.DuckDBPyConnection]:
    """A standalone in-memory DuckDB, loading no ``DeltaTable``. Its cursors
    inherit the session settings."""
    duck: dict[str, Any] = {
        "autoinstall_known_extensions": "true",
        "autoload_known_extensions": "true",
        **config,
    }
    with closing(duckdb.connect(":memory:", config=duck)) as con:
        # a spawned worker's DuckDB thinks it is interactive and draws a bar;
        # refused as a connect-time option
        con.execute("SET enable_progress_bar = false")
        con.execute("LOAD icu; SET GLOBAL TimeZone='UTC'")
        setup_duckdb_storage(con)
        yield con


@contextmanager
def sweep_pair(
    source: SweepSource, config: dict[str, str], out: IO[bytes]
) -> Iterator[Iterator[StatementDict]]:
    """A pair's live statements (`statement_csv_select`) in ``entity_id`` order,
    every Arrow batch also written to ``out`` as headerless csv (``out`` stays
    the caller's to close).

    Each partition is read on its own cursor and the streams ``heapq.merge``d –
    never the pair as one query, whose sort DuckDB failed to spill. The cursors
    share one instance, so they spill against one memory limit.
    """
    select = statement_csv_select()
    with ExitStack() as stack:
        con = stack.enter_context(_connect(config))
        # presorted partitions are read without ORDER BY: turning this off (as
        # DuckDB's OOM advice says) would silently split entities
        con.execute("SET preserve_insertion_order = true")
        # `SWEEP_BATCH_SIZE` full-text rows can hold more than 2GiB of strings
        con.execute("SET arrow_large_buffer_size = true")
        readers = []
        for partition in source.partitions:
            cur = stack.enter_context(closing(con.cursor()))
            register_partition(cur, partition.relation, partition.clean)
            sql = _compile(select.order_by(None) if partition.presorted else select)
            readers.append(cur.execute(sql).to_arrow_reader(SWEEP_BATCH_SIZE))
        options = WriteOptions(include_header=False)
        writer = stack.enter_context(
            CSVWriter(out, readers[0].schema, write_options=options)
        )

        def rows(batches: Iterable[pa.RecordBatch]) -> Iterator[StatementDict]:
            for batch in batches:
                writer.write(batch)
                yield from cast(list[StatementDict], batch.to_pylist())

        streams = [rows(stack.enter_context(prefetch(r))) for r in readers]
        yield heapq.merge(*streams, key=itemgetter("entity_id"))


@cache
def make_source(shards: int) -> SqlSource:
    """The `SqlSource` reads compile against, pruning by ``shards``."""
    prune = {**PRUNE, "shard": make_prune_by_shard(shards)}
    return SqlSource(TABLE, id_column="entity_id", prune=prune)


class _LakeStore(LakeStore):
    """ftmq's store, ``exists`` answered from the owning store's snapshot."""

    def __init__(self, *args: Any, exists: Callable[[], bool], **kwargs: Any) -> None:
        self._exists = exists
        super().__init__(*args, **kwargs)

    @property
    def exists(self) -> bool:
        return self._exists()


class ParquetStore:
    """The statement store of one dataset: one Delta table, partitioned by
    ``(shard, bucket, origin)``, at ``{uri}/statements`` – or at
    ``{LAKEHOUSE_STATEMENTS_URI}/{dataset}/statements`` when that is set. Locks
    and tags stay with the dataset at ``uri``."""

    def __init__(
        self,
        uri: Uri,
        dataset: str,
        shards: int | None = None,
    ) -> None:
        self.settings = Settings()
        root = uri
        if self.settings.statements_uri:
            root = join_uri(
                ensure_uri(self.settings.statements_uri),
                validate_dataset_name(dataset),
            )
        self.uri = join_uri(root, path.STATEMENTS)
        self.dataset = dataset
        self.shards = shards if shards is not None else DEFAULT_SHARDS
        self._store = get_store(uri)
        self._tags = TagStore(uri)
        self._snapshot_lock = RLock()
        self._snapshot: DeltaTable | None = None
        self._files: tuple[int, str, Partitions] | None = None
        self._lake = _LakeStore(
            uri=str(self.uri),
            dataset=self.dataset,
            partition_by=PARTITIONS,
            view_sqls={TABLE_RAW.name: raw_view_sql},
            duckdb_config=duckdb_config(),
            exists=lambda: self.exists,
        )
        self.log = get_logger(
            f"{self.dataset}.{self.__class__.__name__}",
            dataset=self.dataset,
            uri=mask_uri(self.uri),
        )

    @property
    def deltatable(self) -> DeltaTable:
        """A freshly loaded handle on the table, for callers outside the store."""
        return self._lake.deltatable

    @property
    def num_rows(self) -> int:
        """Physical row count from the snapshot's file statistics – no scan."""
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            return snapshot.count() if snapshot is not None else 0

    @property
    def version(self) -> int | None:
        """Current version of the main Delta table."""
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            return snapshot.version() if snapshot is not None else None

    @property
    def exists(self) -> bool:
        """Whether the Delta table exists."""
        with self._snapshot_lock:
            return self._current_snapshot() is not None

    def _current_snapshot(self) -> DeltaTable | None:
        """This process's Delta snapshot, advanced with ``update_incremental``.
        Callers hold `_snapshot_lock`.
        """
        if self._snapshot is not None:
            try:
                self._snapshot.update_incremental()
                return self._snapshot
            except DeltaError:
                self._snapshot = None
        try:
            self._snapshot = DeltaTable(
                str(self.uri), storage_options=storage_options()
            )
        except TableNotFoundError:
            return None
        return self._snapshot

    def _existing_snapshot(self) -> DeltaTable:
        """`_current_snapshot` of a table that must exist. Callers hold
        `_snapshot_lock`."""
        snapshot = self._current_snapshot()
        if snapshot is None:
            raise RuntimeError(f"Statement store vanished: `{self.uri}`")
        return snapshot

    def _partitions(self) -> tuple[str, Partitions]:
        """The table root and the snapshot's files per partition, cached per
        version; empty without a table."""
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                return "", {}
            version = snapshot.version()
            if self._files is None or self._files[0] != version:
                actions = pa.table(snapshot.get_add_actions(flatten=True))
                files: dict[Partition, Files] = {}
                for file, size, *partition in zip(
                    actions["path"].to_pylist(),
                    actions["size_bytes"].to_pylist(),
                    actions["partition.shard"].to_pylist(),
                    actions["partition.bucket"].to_pylist(),
                    actions["partition.origin"].to_pylist(),
                ):
                    key = cast(Partition, tuple(partition))
                    files.setdefault(key, []).append((unquote(file), size))
                partitions = {
                    key: (
                        sorted(fs),  # a merge's files sort in entity_id order
                        all(
                            posixpath.basename(f).startswith(MERGED_PREFIX)
                            for f, _ in fs
                        ),
                    )
                    for key, fs in sorted(files.items())
                }
                root = snapshot.table_uri.rstrip("/")
                self._files = (version, root, partitions)
            return self._files[1], self._files[2]

    def _pairs(self) -> tuple[str, Pairs]:
        """`_partitions`, grouped per ``(shard, bucket)`` pair."""
        root, partitions = self._partitions()
        pairs: Pairs = {}
        for partition, (files, clean) in partitions.items():
            pairs.setdefault(partition[:2], []).append((partition, files, clean))
        return root, pairs

    @property
    def source(self) -> SqlSource:
        return make_source(self.shards)

    def _statement_data(self, q: Query | None = None) -> Iterator[StatementDict]:
        """Statement dicts for ``q``, entity-contiguous – per ``(shard, bucket)``
        pair, or over all pairs at once when sorted or sliced."""
        sql = (q or Query()).compile(self.source)
        prune = self._prune_values(q, self.source)
        whole = q is not None and (q.sort is not None or q.slice is not None)
        sources = self._scoped_sources(prune, whole)
        yield from cast(Iterator[StatementDict], self._rows(sql, sources))

    def query(self, q: Query | None = None) -> StatementEntities:
        """Query entities from the store.

        Args:
            q: Filters, plus ordering / slicing.

        Yields:
            `StatementEntity` objects.
        """
        for data in aggregate_statement_payloads(self._statement_data(q), self.dataset):
            yield data.to_entity()

    def query_statements(self, q: Query | None = None) -> Statements:
        """Query statements from the store.

        Args:
            q: Filters, plus ordering / slicing.

        Yields:
            `LakehouseStatement` objects – with their ``fragment`` and ``role``,
            so one can be handed to
            [`delete_statement`][ftm_lakehouse.repository.EntityRepository.delete_statement].
        """
        for stmt_dict in self._statement_data(q):
            yield LakehouseStatement.from_dict(stmt_dict)

    def _lock(self, key: Uri) -> Lock:
        """A store lock file, waited for at most ``lock_max_retries`` times."""
        return Lock(self._store, key=key, max_retries=self.settings.lock_max_retries)

    def _fence_retry(self, attempt: Callable[[], T]) -> T:
        """Retry ``attempt`` until it stops raising – ``lock_max_retries``
        attempts, ``N²/2`` seconds in total, then the error propagates."""
        return cast(
            T,
            error_handler(
                max_retries=self.settings.lock_max_retries,
                backoff_factor=1,
                do_raise=True,
            )(attempt)(),
        )

    def merge_lock(self) -> Lock:
        """The ``.LOCK-MERGE`` – held by [`merge`][ParquetStore.merge], the export
        sweep and, with ``.LOCK``, the exclusive maintenance. Appends don't wait
        for it; it keeps a vacuum off files a running sweep reads."""
        return self._lock(path.LOCK_MERGE)

    def _await_unlocked(self) -> None:
        """Back off while ``.LOCK`` is held. An append that passed this may still be
        in flight when maintenance takes the lock: `delete_origin` retries its
        commit, `shard` runs with writers stopped."""

        def check() -> None:
            if self._store.exists(path.LOCK):
                raise RuntimeError(
                    f"Write fence busy: maintenance lock `{path.LOCK}` is "
                    "held. If a writer crashed, release the fence via "
                    "`ftm-lakehouse maintenance unlock`."
                )

        self._fence_retry(check)

    def _ensure_table(self) -> None:
        """Create the table as an empty commit, under ``.LOCK`` so two first
        imports cannot both commit version ``0``."""
        if self.exists:
            return
        with self._lock(path.LOCK):
            if self.exists:  # lost the create race - the table is there now
                return
            write_deltalake(
                str(self.uri),
                pa.Table.from_pylist([], schema=SHARDED_SCHEMA),
                partition_by=PARTITIONS,
                mode="overwrite",
                configuration=TABLE_CONFIGURATION,
                storage_options=storage_options(),
            )

    @contextmanager
    def _maintenance_fence(self) -> Iterator[None]:
        """``.LOCK`` plus ``.LOCK-MERGE``, for maintenance that rewrites or drops
        files in place."""
        with self._lock(path.LOCK), self.merge_lock():
            yield

    def unlock(self) -> bool:
        """Forcibly release ``.LOCK`` and ``.LOCK-MERGE`` after a crashed writer.

        Breaking a lock a live writer holds can corrupt its write – confirm no
        process is writing first.

        Returns:
            Whether a lock was released.
        """
        released = False
        for key in (path.LOCK, path.LOCK_MERGE):
            if self._store.exists(key):
                self._store.delete(key)
                released = True
        return released

    def evolve_schema(self) -> list[str]:
        """Add the `SHARDED_SCHEMA` columns the table was created without.

        A metadata-only commit; older files read the new columns as NULL.
        Additive only – a removed column needs a full rewrite.

        Returns:
            The columns added.
        """
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                return []
            known = {f.name for f in snapshot.schema().fields}
            missing = [
                f
                for f in Schema.from_arrow(SHARDED_SCHEMA).fields
                if f.name not in known
            ]
            if not missing:
                return []
            names = [f.name for f in missing]
            with self._maintenance_fence():
                snapshot.alter.add_columns(missing)
        self.log.info("Evolved parquet schema.", columns=names)
        return names

    def configure_table(self) -> dict[str, str]:
        """Apply `TABLE_CONFIGURATION`, then checkpoint and clean up the log under
        it.

        Returns:
            The properties that changed.
        """
        with self._snapshot_lock:
            snapshot = self._current_snapshot()
            if snapshot is None:
                return {}
            current = snapshot.metadata().configuration
            changed = {
                k: v for k, v in TABLE_CONFIGURATION.items() if current.get(k) != v
            }
            if not changed:
                return {}
            with self._maintenance_fence(), Took() as t:
                snapshot.alter.set_table_properties(changed)
                snapshot.update_incremental()
                snapshot.create_checkpoint()
                snapshot.cleanup_metadata()
        self.log.info("Configured delta table.", took=t.took, **changed)
        return changed

    def _with_shard(self, batch: pa.Table) -> pa.Table:
        """Derive ``shard`` from ``entity_id`` – the only place a row's partition is
        decided."""
        ids = pc.dictionary_encode(batch.column("entity_id").combine_chunks())
        shards = pa.array(
            [entity_shard(e, self.shards) for e in ids.dictionary.to_pylist()],
            pa.string(),
        )
        return batch.append_column(
            SHARDED_SCHEMA.field("shard"), pc.take(shards, ids.indices)
        ).select(SHARDED_SCHEMA.names)

    def append(self, batch: pa.Table) -> None:
        """Append `JOURNAL_SCHEMA` rows, deriving their ``shard``.

        One write per bucket, for its writer profile. Unsorted and lock-free –
        concurrent appends commit through Delta's optimistic concurrency; only
        ``.LOCK`` makes them wait. The touched partitions are dirty until the
        next [`merge`][ParquetStore.merge].
        """
        if len(batch) == 0:
            return

        batch = self._with_shard(batch)
        buckets = pc.unique(batch["bucket"]).to_pylist()
        shards = pc.unique(batch["shard"]).to_pylist()
        self.log.info(
            f"Flushing {len(batch)} statements to parquet ...",
            buckets=buckets,
            shards=shards,
        )
        self._ensure_table()
        for bucket in buckets:
            self._write(bucket, batch.filter(pc.equal(batch["bucket"], bucket)))

    def _write(
        self, bucket: str, table: pa.Table, target_file_size: int | None = None
    ) -> None:
        """Write a bucket from pyarrow Table to deltalake, exclusively via .LOCK"""
        with self._tags.touch(tag.STATEMENTS_UPDATED):
            self._await_unlocked()
            with self._snapshot_lock:
                write_deltalake(
                    self._existing_snapshot(),
                    table,
                    partition_by=PARTITIONS,
                    mode="append",
                    writer_properties=writer_for_bucket(bucket),
                    target_file_size=target_file_size,
                )

    def _by_partition(self, batch: pa.Table) -> Iterator[tuple[Partition, pa.Table]]:
        """`JOURNAL_SCHEMA` rows as one table per ``(shard, bucket, origin)``,
        ``shard`` derived."""
        batch = self._with_shard(batch)
        order = pc.sort_indices(batch, sort_keys=[(c, "ascending") for c in PARTITIONS])
        keys = pc.binary_join_element_wise(*(batch[c] for c in PARTITIONS), "/")
        runs = pc.value_counts(pc.take(keys, order))  # first seen first: sorted
        offset = 0
        for key, count in zip(
            runs.field("values").to_pylist(), runs.field("counts").to_pylist()
        ):
            shard, bucket, origin = key.split("/")
            # a copy per part – a slice would keep the whole batch alive
            yield (shard, bucket, origin), batch.take(order.slice(offset, count))
            offset += count

    def write_stream(
        self, tables: Iterable[pa.Table], budget: int | None = None
    ) -> int:
        """Append a stream of `JOURNAL_SCHEMA` tables as a ``part-*`` file per
        ``(shard, bucket, origin)``. This is not the journal drain but used for
        bulk imports via cli or scripts.

        A partition's rows are held until they fill a file (`MERGE_CHUNK_BYTES`)
        or the held rows exceed ``budget``; then the largest partitions are
        written, down to half the budget, in one commit per bucket of up to
        `WRITE_FLUSH_PARTITIONS` partitions.

        Args:
            tables: Stream of packed statement tables.
            budget: Arrow bytes to hold, default ``LAKEHOUSE_WRITE_BUFFER``.

        Returns:
            Number of rows written.
        """
        if budget is None:
            budget = parse_byte_size(self.settings.write_buffer)
        held: dict[Partition, list[pa.Table]] = {}
        sizes: dict[Partition, int] = {}
        rows = files = commits = 0

        def flush(keys: list[Partition]) -> None:
            nonlocal files, commits
            for bucket in sorted({key[1] for key in keys}):
                of_bucket = [key for key in keys if key[1] == bucket]
                for i in range(0, len(of_bucket), WRITE_FLUSH_PARTITIONS):
                    chunk = of_bucket[i : i + WRITE_FLUSH_PARTITIONS]
                    table = pa.concat_tables(
                        chain.from_iterable(held.pop(key) for key in chunk)
                    )
                    for key in chunk:
                        del sizes[key]
                    self.log.debug(
                        "Writing partitions ...",
                        bucket=bucket,
                        partitions=len(chunk),
                        rows=len(table),
                    )
                    self._write(bucket, table, target_file_size=1 << 62)  # a file each
                    files += len(chunk)
                    commits += 1

        self._ensure_table()
        for batch in tables:
            for key, part in self._by_partition(batch):
                held.setdefault(key, []).append(part)
                sizes[key] = sizes.get(key, 0) + part.nbytes
                rows += part.num_rows
                if sizes[key] >= MERGE_CHUNK_BYTES:
                    flush([key])
            if (left := sum(sizes.values())) > budget:
                largest: list[Partition] = []
                for key in sorted(sizes, key=sizes.__getitem__, reverse=True):
                    largest.append(key)
                    if (left := left - sizes[key]) <= budget // 2:
                        break
                flush(largest)
        flush(list(held))
        if commits:
            with self._snapshot_lock:
                self._existing_snapshot().create_checkpoint()
        self.log.info("Wrote statements.", rows=rows, files=files, commits=commits)
        return rows

    @property
    def needs_merge(self) -> bool:
        """Whether any partition holds a file [`merge`][ParquetStore.merge] did not
        write."""
        return not all(clean for _, clean in self._partitions()[1].values())

    def merge(self, force: bool = False) -> None:
        """Rewrite every dirty partition into canonical files, in ``entity_id``
        order – one per `MERGE_CHUNK_BYTES` of rows.

        Duplicates collapse, fragments supersede, ``first_seen`` folds and
        tombstones past ``LAKEHOUSE_GRACE_PERIOD_DAYS`` go with the rows they
        shadow ([`build_merge_sql`][ftm_lakehouse.logic.parquet.build_merge_sql]).
        Reads see the same rows before and after (``dataChange = false``).

        Partitions merge in ``LAKEHOUSE_WORKERS`` processes, committed in
        batches of `MERGE_COMMIT_BATCH`; a failed run still commits what
        finished, including the partitions still running when one failed.
        Holds only [`merge_lock`][ParquetStore.merge_lock], so appends
        keep flowing.

        Args:
            force: Rewrite clean partitions too – with a grace period of ``0``
                that purges tombstones in otherwise idle partitions.
        """
        if not self.exists:
            return
        grace_cutoff = utc_now() - timedelta(days=self.settings.grace_period_days)
        workers = self.settings.workers
        merged = skipped = 0
        with self.merge_lock():
            root, partitions = self._partitions()
            tasks = [
                MergeTask(
                    partition, files, root, grace_cutoff, worker_duckdb_config(workers)
                )
                for partition, (files, clean) in partitions.items()
                if force or not clean
            ]
            skipped = len(partitions) - len(tasks)
            # advanced per partition, throughput in bytes read
            with (
                SyncProgressBar("Merging partitions", len(tasks)) as bar,
                process_map(workers, drain=True) as run,
            ):
                done: list[MergeResult] = []
                try:
                    for result in run(merge_partition, tasks):
                        bar.advance(size=sum(size for _, size in result.task.files))
                        shard, bucket, origin = result.task.partition
                        self.log.info(
                            f"Merged partition `{shard}/{bucket}/{origin}`.",
                            took=result.took,
                            shard=shard,
                            bucket=bucket,
                            origin=origin,
                            grace_period_days=self.settings.grace_period_days,
                        )
                        done.append(result)
                        if len(done) == MERGE_COMMIT_BATCH:
                            batch, done = done, []
                            self._commit_merged(batch)
                            merged += len(batch)
                finally:
                    # also on failure: commit what finished, so the next run resumes
                    if done:
                        self._commit_merged(done)
                        merged += len(done)
            if merged:
                with self._snapshot_lock, Took() as t:
                    self._existing_snapshot().create_checkpoint()
                self.log.info("Wrote checkpoint.", took=t.took)
        self.log.info(
            "Merge complete.",
            merged=merged,
            skipped=skipped,
            workers=workers,
            grace_period_days=self.settings.grace_period_days,
        )

    def _commit_merged(self, batch: list[MergeResult]) -> None:
        """Commit a batch of merged partitions as one ``dataChange = false``
        transaction."""
        now = int(utc_now().timestamp() * 1000)
        actions: list[AddAction | RemoveAction] = []
        for result in batch:
            task = result.task
            values: dict[str, str | None] = dict(zip(PARTITIONS, task.partition))
            for file, size, stats in result.files:
                actions.append(AddAction(file, size, values, now, False, stats))
            for file, size in task.files:
                actions.append(RemoveAction(file, False, now, size, values))
        with self._snapshot_lock, Took() as t:
            snapshot = self._existing_snapshot()
            snapshot.create_write_transaction(
                actions,
                mode="append",
                schema=snapshot.schema(),
                partition_by=PARTITIONS,
            )
            snapshot.update_incremental()
        self.log.info(
            "Committed merged partitions.", partitions=len(batch), took=t.took
        )

    def shard(self, shards: int) -> None:
        """Rewrite the store onto ``shards`` entity-hash shards.

        One streamed, atomic overwrite per ``(bucket, origin)`` group; every
        partition comes out dirty, so run ``optimize`` afterwards. Idempotent.
        Blocks parquet appends but not the journal – run it with writers stopped.

        Args:
            shards: Target shard count; ``<= 1`` means a single shard.
        """
        if self.exists:
            self._rewrite_shards(shards)
        self.shards = shards
        self.log.info("Re-shard complete.", shards=shards)

    def _rewrite_shards(self, shards: int) -> None:
        """Rewrite every ``(bucket, origin)`` group, under the maintenance fence,
        read in batches of `MERGE_BATCH_BYTES` (`_batch_rows`) – DuckDB's default
        of a million rows a batch overflows Arrow's 2GiB string buffer on
        document rows."""
        with self._maintenance_fence():
            root, partitions = self._partitions()
            groups: dict[tuple[str, str], list[tuple[Partition, Files]]] = {}
            for partition, (files, _) in partitions.items():
                _, bucket, origin = partition
                groups.setdefault((bucket, origin), []).append((partition, files))
            config = duckdb_config()
            for (bucket, origin), sources in groups.items():
                with (
                    Took() as t,
                    _connect(config) as con,
                    self._snapshot_lock,
                ):
                    # a batch's strings may pass 2GiB when `_batch_rows`
                    # underestimates
                    con.execute("SET arrow_large_buffer_size = true")
                    snapshot = self._existing_snapshot()
                    sqls = [
                        build_shard_sql(
                            *partition, shards, source=_relation(root, partition, files)
                        )
                        for partition, files in sources
                    ]
                    rows = _batch_rows(con, _relation(root, *sources[0]))
                    # lazily: executing the next query closes the current result
                    readers = (con.execute(sql).to_arrow_reader(rows) for sql in sqls)
                    first = next(readers)
                    batches = chain.from_iterable(chain([first], readers))
                    write_deltalake(
                        snapshot,
                        pa.RecordBatchReader.from_batches(first.schema, batches),
                        mode="overwrite",
                        partition_by=PARTITIONS,
                        predicate=(
                            f"bucket = '{bucket}' AND "
                            f"origin = '{validate_origin(origin)}'"
                        ),
                        writer_properties=writer_for_bucket(bucket),
                        target_file_size=shard_target_file_size(shards),
                    )
                    snapshot.update_incremental()
                self.log.info(
                    f"Re-sharded `{bucket}/{origin}`.",
                    took=t.took,
                    bucket=bucket,
                    origin=origin,
                    sources=len(sources),
                    shards=shards,
                )

    def delete_origin(self, origin: str) -> int:
        """Physically drop every row of one origin – whole partitions, at once,
        with no grace period.

        Args:
            origin: The origin to drop.

        Returns:
            Number of rows removed.

        Raises:
            ValueError: If ``origin`` is not a safe origin name.
            RuntimeError: When the write fence cannot be acquired.
        """
        origin = validate_origin(origin)
        if not self.exists:
            return 0
        with self._maintenance_fence(), Took() as t, self._snapshot_lock:
            # safe to interpolate: `validate_origin` rejects quotes
            metrics: dict[str, Any] = self._fence_retry(
                lambda: self._existing_snapshot().delete(f"origin = '{origin}'")
            )
            deleted = int(metrics.get("num_deleted_rows") or 0)
            if deleted:
                self._tags.set(tag.STATEMENTS_UPDATED)
            self.log.info(
                "Dropped origin.",
                took=t.took,
                origin=origin,
                deleted=deleted,
                **metrics,
            )
        return deleted

    def destroy(self) -> None:
        """Soft-delete the store: drop its ``_delta_log``. The parquet files stay
        on disk until a [`vacuum`][ParquetStore.vacuum] of the re-created table.

        Other processes keep the snapshot they hold – restart them.
        """
        if not self.exists:
            return
        with self._maintenance_fence(), Took() as t, self._snapshot_lock:
            table = get_store(self.uri)
            table._fs.rm(table._keys.to_fs_key("_delta_log"), recursive=True)
            self._snapshot = self._files = None
            self._tags.set(tag.STATEMENTS_UPDATED)
        self.log.warning("Destroyed statement store.", took=t.took)

    def vacuum(self, retention_hours: int = 0) -> None:
        """Delete the files the Delta log no longer references, and the staging
        tables a crashed merge left (`STAGING_DIR`).

        Args:
            retention_hours: Keep such files newer than this.
        """
        if not self.exists:
            return
        with self._maintenance_fence(), Took() as t, self._snapshot_lock:
            deleted = self._existing_snapshot().vacuum(
                retention_hours=retention_hours,
                dry_run=False,
                enforce_retention_duration=False,
                full=True,
            )
            # no merge stages under the fence
            table = get_store(self.uri)
            if table.exists(STAGING_DIR):
                table._fs.rm(table._keys.to_fs_key(STAGING_DIR), recursive=True)
        self.log.info("Vacuumed.", files=len(deleted), took=t.took)

    def sweep_sources(self) -> list[SweepSource]:
        """Every ``(shard, bucket)`` pair of one snapshot as a ``SweepSource``, the
        units an export sweeps (no entity spans two)."""
        root, pairs = self._pairs()
        return [
            SweepSource(
                key,
                [
                    SweepPartition(
                        relation=_relation(root, partition, files),
                        clean=clean,
                        presorted=clean,
                        size=sum(size for _, size in files),
                    )
                    for partition, files, clean in pairs[key]
                ],
            )
            for key in sorted(pairs)
        ]

    def deleted_candidates(self, since: datetime) -> Iterator[DeleteCandidate]:
        """Every entity with a tombstone at or after ``since`` – one raw scan over
        the whole store."""
        sql = deleted_candidates_select(since)
        for row in self._rows(sql, self._scoped_sources()):
            yield DeleteCandidate(
                id=row["entity_id"],
                deleted_at=row["deleted_at"],
                origins=frozenset(row["origins"] or ()),
                schemata=frozenset(row["schemata"] or ()),
                content_hash=bool(row["content_hash"]),
            )

    @staticmethod
    def _prune_values(q: Query | None, source: SqlSource) -> dict[str, set[str]]:
        """The partition values ``q`` confines its matches to, per prune column –
        only for a flat positive conjunction."""
        if q is None or not Sql(q, source)._is_flat_and:
            return {}
        return {
            column: set(values)
            for column, prune in source.prune.items()
            if (values := prune(q))
        }

    @staticmethod
    def _pruned_keys(
        pairs: Pairs, prune: dict[str, set[str]] | None
    ) -> list[tuple[str, str]]:
        """The keys of ``pairs`` that ``prune`` leaves in."""
        prune = prune or {}
        return [
            (s, b)
            for s, b in sorted(pairs)
            if s in prune.get("shard", {s}) and b in prune.get("bucket", {b})
        ]

    def _scoped_sources(
        self, prune: dict[str, set[str]] | None = None, whole: bool = False
    ) -> Iterator[tuple[str, bool]]:
        """One ``(relation, clean)`` per ``(shard, bucket)`` pair ``prune`` leaves in.

        A read pruned to shards puts all its pairs into one relation – an id
        names its shard but not its bucket – and so does a ``whole`` one, which
        sorts or slices across pairs.
        """
        root, pairs = self._pairs()
        keys = self._pruned_keys(pairs, prune)
        if whole or (prune and "shard" in prune):
            if keys:
                yield pair_source(root, [s for key in keys for s in pairs[key]])
            return
        for key in keys:
            yield pair_source(root, pairs[key])

    @contextmanager
    def cursor(
        self, raw: str = TABLE_RAW.name, live: str = TABLE.name
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        """A cursor with the whole table registered as a read's views
        (`register_partition`): the live view is a plain scan when every
        partition is clean, the dedupe query otherwise.

        Args:
            raw: Name of the view over every physical row.
            live: Name of the view over the live rows.

        Yields:
            A DuckDB cursor of the store's connection.
        """
        with self._snapshot_lock:
            source = delta_scan_sql(self._existing_snapshot().table_uri)
        with self._cursor_over(source, not self.needs_merge, raw, live) as cur:
            yield cur

    @contextmanager
    def _cursor_over(
        self,
        source: str,
        clean: bool,
        raw: str = TABLE_RAW.name,
        live: str = TABLE.name,
    ) -> Iterator[duckdb.DuckDBPyConnection]:
        """A cursor of the shared connection, its views reading ``source``
        (`register_partition`)."""
        with self._lake.cursor() as cur:
            register_partition(cur, source, clean, raw, live)
            yield cur

    def _rows(
        self, sql: Select, sources: Iterable[tuple[str, bool]]
    ) -> Iterator[dict[str, Any]]:
        """``sql``'s rows over each ``(relation, clean)`` source in turn
        (`_scoped_sources`), in Arrow batches of `SWEEP_BATCH_SIZE`."""
        compiled = _compile(sql)
        for source, clean in sources:
            with self._cursor_over(source, clean) as cur:
                for batch in cur.execute(compiled).to_arrow_reader(SWEEP_BATCH_SIZE):
                    yield from batch.to_pylist()
