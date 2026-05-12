"""Tests for the three async maintenance jobs: Compact, Merge, Vacuum."""

from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.logic.parquet import make_duckdb, register_view
from ftm_lakehouse.model.statement import TABLE
from ftm_lakehouse.operation.maintenance import (
    CompactJob,
    CompactOperation,
    MergeJob,
    MergeOperation,
    VacuumJob,
    VacuumOperation,
)
from ftm_lakehouse.repository import EntityRepository

DATASET = "optimize_test"


def count_parquet_files(repo: EntityRepository) -> int:
    return len(repo._statements.deltatable.file_uris())


def _add_batches(repo: EntityRepository, n: int = 3) -> None:
    """Create ``n`` parquet files by flushing ``n`` separate origin batches."""
    for i in range(n):
        with repo.writer(origin=f"batch_{i}") as writer:
            entity = make_entity(
                {
                    "id": f"entity-{i}",
                    "schema": "Person",
                    "properties": {"name": [f"Person {i}"]},
                }
            )
            writer.add_entity(entity)
        repo.flush()


def test_operation_compact_bin_packs_files(tmp_path):
    """CompactOperation bin-packs small files within each partition.

    Three separate origin flushes produce three partitions, each with one
    small file. Compact rewrites each partition's files into one larger file
    (effectively a no-op when there's already only one file per partition,
    but the call must succeed and touch the freshness tag).
    """
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    _add_batches(repo, n=3)

    initial = count_parquet_files(repo)
    assert initial == 3

    job = CompactJob.make(dataset=DATASET)
    op = CompactOperation(job=job, uri=tmp_path)
    assert op.get_target() == tag.STATEMENTS_COMPACTED
    assert op.get_dependencies() == [tag.STATEMENTS_UPDATED]

    result = op.run()
    assert result.done == 1

    target_path = f"tags/lakehouse/{tag.STATEMENTS_COMPACTED}"
    assert (tmp_path / target_path).exists()
    assert count_parquet_files(repo) <= initial


def test_operation_merge_collapses_duplicates(tmp_path):
    """MergeOperation collapses duplicate statements per partition."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)

    # Two flushes of the same entity → two rows per statement id
    for _ in range(2):
        with repo.writer(origin="ingest") as writer:
            entity = make_entity(
                {
                    "id": "alice",
                    "schema": "Person",
                    "properties": {"name": ["Alice"]},
                }
            )
            writer.add_entity(entity)
        repo.flush()

    def row_count() -> int:
        con = make_duckdb()
        register_view(con, repo._statements.deltatable)
        return con.execute(f"SELECT COUNT(*) FROM {TABLE.name}").fetchone()[0]

    before = row_count()
    assert before == 4

    job = MergeJob.make(dataset=DATASET)
    op = MergeOperation(job=job, uri=tmp_path)
    assert op.get_target() == tag.STATEMENTS_MERGED

    result = op.run()
    assert result.done == 1

    after = row_count()
    assert after == 2  # duplicates collapsed


def test_operation_vacuum_purges_obsolete_files(tmp_path):
    """VacuumOperation deletes obsolete parquet files no longer in Delta log."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    _add_batches(repo, n=3)

    # Compact creates merged files and tombstones the originals (still on disk)
    CompactOperation(job=CompactJob.make(dataset=DATASET), uri=tmp_path).run()

    on_disk_before = sum(
        1
        for root, _, fs in __import__("os").walk(tmp_path)
        for f in fs
        if f.endswith(".parquet")
    )

    job = VacuumJob.make(dataset=DATASET, retention_hours=0)
    op = VacuumOperation(job=job, uri=tmp_path)
    assert op.get_target() == tag.STATEMENTS_VACUUMED

    result = op.run()
    assert result.done == 1

    on_disk_after = sum(
        1
        for root, _, fs in __import__("os").walk(tmp_path)
        for f in fs
        if f.endswith(".parquet")
    )
    assert on_disk_after <= on_disk_before

    # Data still intact
    entities = list(repo.query())
    assert len(entities) == 3
