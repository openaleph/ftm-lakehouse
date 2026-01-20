"""Tests for OptimizeOperation - parquet store compaction and vacuum."""

from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import tag
from ftm_lakehouse.operation.optimize import OptimizeJob, OptimizeOperation
from ftm_lakehouse.repository import EntityRepository

DATASET = "optimize_test"


def count_parquet_files(repo: EntityRepository) -> int:
    """Count parquet files in the delta table."""
    return len(repo._statements._store.deltatable.file_uris())


def test_operation_optimize(tmp_path):
    """Test OptimizeOperation: compaction and vacuum with tag verification."""
    dataset_uri = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=dataset_uri)

    # Add entities in multiple batches to create multiple parquet files
    for i in range(3):
        with repo.bulk(origin=f"batch_{i}") as writer:
            for j in range(2):
                entity = make_entity(
                    {
                        "id": f"entity-{i}-{j}",
                        "schema": "Person",
                        "properties": {"name": [f"Person {i}-{j}"]},
                    }
                )
                writer.add_entity(entity)
        repo.flush()

    # Should have multiple parquet files (one per flush/origin)
    initial_file_count = count_parquet_files(repo)
    assert initial_file_count == 3  # 3 origins = 3 files

    # No target tag before run
    target_path = "tags/lakehouse/statements/store_optimized"
    assert not (dataset_uri / target_path).exists()

    # Create operation and verify target/dependencies
    job = OptimizeJob.make(dataset=DATASET)
    op = OptimizeOperation(job=job, lake_uri=tmp_path)

    assert op.get_target() == tag.STORE_OPTIMIZED
    assert op.get_target() == "statements/store_optimized"
    assert op.get_dependencies() == [tag.STATEMENTS_UPDATED]
    assert op.get_dependencies() == ["statements/last_updated"]

    # Run optimize (without vacuum)
    result = op.run()

    assert result.done == 1
    assert result.running is False
    assert result.stopped is not None

    # Tag should exist at hardcoded path after run
    assert (dataset_uri / target_path).exists()

    # After optimization, files should be compacted (reduced or same)
    optimized_file_count = count_parquet_files(repo)
    assert optimized_file_count <= initial_file_count


def test_operation_optimize_vacuum(tmp_path):
    """Test OptimizeOperation with vacuum=True removes old files."""
    dataset_uri = tmp_path / DATASET
    repo = EntityRepository(dataset=DATASET, uri=dataset_uri)

    # Add entities in multiple batches
    for i in range(3):
        with repo.bulk(origin=f"batch_{i}") as writer:
            entity = make_entity(
                {
                    "id": f"entity-{i}",
                    "schema": "Person",
                    "properties": {"name": [f"Person {i}"]},
                }
            )
            writer.add_entity(entity)
        repo.flush()

    initial_file_count = count_parquet_files(repo)
    assert initial_file_count == 3

    # First optimize without vacuum (creates compacted files but keeps old ones)
    job = OptimizeJob.make(dataset=DATASET, vacuum=False)
    op = OptimizeOperation(job=job, lake_uri=tmp_path)
    op.run()

    after_optimize_count = count_parquet_files(repo)

    # Now run with vacuum=True to purge old files
    job_vacuum = OptimizeJob.make(dataset=DATASET, vacuum=True, vacuum_keep_hours=0)
    op_vacuum = OptimizeOperation(job=job_vacuum, lake_uri=tmp_path)
    op_vacuum.run()

    after_vacuum_count = count_parquet_files(repo)

    # After vacuum, should have fewer or equal files than after optimize
    assert after_vacuum_count <= after_optimize_count

    # Verify data is still intact
    entities = list(repo.query())
    assert len(entities) == 3
