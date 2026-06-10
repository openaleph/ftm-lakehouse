"""Tests for the OptimizeOperation (merge + compact + vacuum in one pass)."""

import os

from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.model.statement import TABLE
from ftm_lakehouse.operation.maintenance import OptimizeJob, OptimizeOperation
from ftm_lakehouse.repository import EntityRepository
from tests.duck import make_duckdb, register_view

DATASET = "optimize_test"


def count_parquet_files(repo: EntityRepository) -> int:
    return len(repo._statements.deltatable.file_uris())


def count_parquet_on_disk(tmp_path) -> int:
    return sum(
        1 for _, _, fs in os.walk(tmp_path) for f in fs if f.endswith(".parquet")
    )


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


def test_operation_optimize(tmp_path):
    """OptimizeOperation runs merge, compact and vacuum in one pass.

    Three separate origin flushes produce three partitions, each with one
    small file. Optimize must succeed, bound the file count, keep the data
    intact and touch the freshness tag.
    """
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    _add_batches(repo, n=3)

    initial = count_parquet_files(repo)
    assert initial == 3
    on_disk_before = count_parquet_on_disk(tmp_path)

    job = OptimizeJob.make(dataset=DATASET, retention_hours=0)
    op = OptimizeOperation(job=job, uri=tmp_path)
    assert op.get_target() == tag.STATEMENTS_OPTIMIZED
    assert op.get_dependencies() == [tag.STATEMENTS_UPDATED]

    result = op.run()
    assert result.done == 3  # merge + compact + vacuum

    target_path = f"tags/lakehouse/{tag.STATEMENTS_OPTIMIZED}"
    assert (tmp_path / target_path).exists()
    assert count_parquet_files(repo) <= initial
    # Vacuum removed files tombstoned by merge/compact
    assert count_parquet_on_disk(tmp_path) <= on_disk_before

    # Data still intact
    entities = list(repo.query())
    assert len(entities) == 3


def test_operation_optimize_collapses_duplicates(tmp_path):
    """Optimize collapses duplicate statements per partition (merge step)."""
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

    job = OptimizeJob.make(dataset=DATASET)
    result = OptimizeOperation(job=job, uri=tmp_path).run()
    assert result.done == 3

    after = row_count()
    assert after == 2  # duplicates collapsed
