"""Tests for the MigrateOperation (apply outstanding storage migrations)."""

import pyarrow as pa
from deltalake import write_deltalake
from ftmq.util import make_entity
from rigour.time import utc_now

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.model.statement import SHARDED_SCHEMA
from ftm_lakehouse.operation.maintenance import MigrateJob, MigrateOperation
from ftm_lakehouse.operation.migrations import MIGRATIONS
from ftm_lakehouse.repository import EntityRepository
from ftm_lakehouse.storage.parquet import PARTITIONS

DATASET = "migrate_test"

PRE_ROLE_SCHEMA = pa.schema([f for f in SHARDED_SCHEMA if f.name != "role"])


def _write_pre_role_store(tmp_path) -> None:
    """Write a statement store the way it looked before ``role`` existed."""
    now = utc_now()
    row = {f.name: None for f in PRE_ROLE_SCHEMA}
    row.update(
        shard="0",
        id="stmt-1",
        entity_id="entity-1",
        dataset=DATASET,
        bucket="thing",
        origin="test",
        schema="Person",
        prop="name",
        prop_type="name",
        value="Jane",
        external=False,
        first_seen=now,
        last_seen=now,
        fragment="",
    )
    write_deltalake(
        str(tmp_path / path.STATEMENTS),
        pa.Table.from_pylist([row], schema=PRE_ROLE_SCHEMA),
        partition_by=PARTITIONS,
        mode="overwrite",
    )


def _columns(repo: EntityRepository) -> set[str]:
    return {f.name for f in repo._statements.deltatable.schema().to_arrow()}


def test_operation_migrate(tmp_path):
    """The role migration evolves a pre-role store in place: no file is
    rewritten, the old rows read back role-less, and writes work again."""
    _write_pre_role_store(tmp_path)
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    assert "role" not in _columns(repo)
    files_before = set(repo._statements.deltatable.file_uris())

    op = MigrateOperation(job=MigrateJob.make(dataset=DATASET), uri=tmp_path)
    assert op.get_target() == tag.OP_MIGRATE
    assert not op.is_fresh()
    result = op.run()
    assert result.done == len(MIGRATIONS) == 1
    assert result.pending == 0
    assert (tmp_path / f"tags/lakehouse/{tag.OP_MIGRATE}").exists()
    assert (
        tmp_path / f"tags/lakehouse/{tag.migration('migrate_parquet_add_role')}"
    ).exists()

    # metadata-only: the column is there, the parquet files are untouched
    assert "role" in _columns(repo)
    assert set(repo._statements.deltatable.file_uris()) == files_before

    # the pre-role row reads back as role-less ...
    statements = list(repo.query_statements())
    assert len(statements) == 1
    assert statements[0].entity_id == "entity-1"
    assert statements[0].role is None

    # ... and the store takes writes again, role and all
    with repo.writer(origin="test", role="user:42") as writer:
        writer.add_entity(
            make_entity(
                {
                    "id": "entity-2",
                    "schema": "Person",
                    "properties": {"name": ["John"]},
                }
            )
        )
    repo.flush()
    repo.merge()
    roles = {s.entity_id: s.role for s in repo.query_statements() if s.prop == "name"}
    assert roles == {"entity-1": None, "entity-2": "user:42"}


def test_operation_migrate_is_fresh(tmp_path):
    """An applied migration is not re-run – unless forced, which is a no-op."""
    _write_pre_role_store(tmp_path)
    MigrateOperation(job=MigrateJob.make(dataset=DATASET), uri=tmp_path).run()

    op = MigrateOperation(job=MigrateJob.make(dataset=DATASET), uri=tmp_path)
    assert op.outstanding == ()
    assert op.is_fresh()
    assert op.run().done == 0

    forced = MigrateOperation(job=MigrateJob.make(dataset=DATASET), uri=tmp_path)
    assert forced.run(force=True).done == len(MIGRATIONS)
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    assert "role" in _columns(repo)
    assert len(list(repo.query_statements())) == 1


def test_operation_migrate_current_store(tmp_path):
    """On a store written by the current code every migration is a no-op that
    still gets stamped, so it never runs again."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    with repo.writer(origin="test") as writer:
        writer.add_entity(
            make_entity(
                {"id": "entity-1", "schema": "Person", "properties": {"name": ["Jane"]}}
            )
        )
    repo.flush()

    op = MigrateOperation(job=MigrateJob.make(dataset=DATASET), uri=tmp_path)
    assert op.run().done == len(MIGRATIONS)
    assert MigrateOperation(
        job=MigrateJob.make(dataset=DATASET), uri=tmp_path
    ).is_fresh()
    assert len(list(repo.query_statements())) == 2  # id + name
