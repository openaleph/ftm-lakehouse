"""Flush appends; queries dedupe on read. Re-adding identical entities is
idempotent at the query layer (one entity in, one out) and at the
statement layer too, because the deduped ``statement`` view registered
on the :class:`LakeStore` connection collapses duplicates per
``(shard, bucket, id)`` and filters tombstones at read time.
Re-flushing still adds new physical rows; :meth:`merge` exists only to
compact those duplicates and reap tombstones past grace.
"""

import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import EntityProxy

from ftm_lakehouse.api.main import archive_router, entities_router, journal_router
from ftm_lakehouse.repository.entities import EntityRepository
from tests.conftest import make_docker_repo, make_test_api
from tests.duck import make_duckdb
from tests.shared import JANE, JOHN

DATASET = "test"


def _make_local_repo(tmp_path) -> EntityRepository:
    return EntityRepository(DATASET, tmp_path)


def _row_count(path: str) -> int:
    con = make_duckdb()
    path = f"{path}/entities/statements"
    return con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()[0]


@pytest.fixture(params=["local", "api", "docker"])
def repo(
    request, tmp_path
) -> Generator[tuple[EntityRepository, Path | None], None, None]:
    if request.param == "local":
        yield _make_local_repo(tmp_path), tmp_path
    elif request.param == "api":
        routers = [entities_router, journal_router, archive_router]
        with make_test_api(tmp_path, routers) as base_url:
            dataset_url = f"{base_url}/{DATASET}"
            r = EntityRepository(DATASET, uri=dataset_url)
            yield r, tmp_path / DATASET
    else:
        yield make_docker_repo()


@pytest.fixture
def local_repo(tmp_path) -> Generator[EntityRepository, None, None]:
    """Local-only fixture for tests that hit ``@no_api`` internals
    (``view()`` and ``get_changed_entity_ids`` aren't exposed via the API).
    """
    yield _make_local_repo(tmp_path)


def test_flush_appends_duplicates(repo):
    """Re-flushing the same entity APPENDS new physical rows."""
    repo, path = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    rows1 = _row_count(path)

    repo.add(jane)
    repo.flush()
    rows2 = _row_count(path)

    assert rows2 == rows1 * 2  # second flush appended a fresh copy


def test_query_dedup_after_re_add(repo):
    """Re-flushing the same entity surfaces one row per statement id."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    stmts1 = list(repo.query_statements())

    repo.add(jane)
    repo.flush()
    stmts2 = list(repo.query_statements())

    entities = list(repo.query(flush_first=False))
    assert {e.id for e in entities} == {"jane"}
    # Statement stream also dedupes – without the deduped view the
    # second flush would have doubled the row count visible to readers.
    assert len(stmts2) == len(stmts1)
    assert {s.id for s in stmts2} == {s.id for s in stmts1}


def test_query_statements_dedup_without_merge(repo):
    """Statement stream surfaces one row per id with merged first_seen / latest last_seen."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    first = {s.id: (s.first_seen, s.last_seen) for s in repo.query_statements()}

    # Re-add with a distinct timestamp so last_seen differs across the
    # two physical rows; sleep keeps the second-resolution last_seen
    # apart from the first.
    time.sleep(1.1)
    repo.add(jane)
    repo.flush()

    stmts = list(repo.query_statements())
    by_id = {s.id: s for s in stmts}
    # No duplicate statement ids in the dedupe-on-read output.
    assert len(stmts) == len(by_id)
    # Dedupe keeps the earliest first_seen and the latest last_seen.
    for stmt_id, (orig_first, orig_last) in first.items():
        assert by_id[stmt_id].first_seen == orig_first
        assert by_id[stmt_id].last_seen > orig_last


def test_query_skips_tombstone_without_merge(repo):
    """Deleting an entity hides it from queries before merge runs."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    assert {e.id for e in repo.query(flush_first=False)} == {"jane"}

    # Sleep keeps the tombstone last_seen strictly greater than the live
    # row's last_seen so ROW_NUMBER picks the tombstone deterministically.
    time.sleep(1.1)
    repo.delete_entity("jane")
    repo.flush()

    # The deduped view picks the tombstone (latest last_seen) and then
    # filters deleted_at IS NOT NULL, so the entity vanishes from reads.
    # Without dedupe-on-read, VIEW_FILTER alone would drop the tombstone
    # and leave the stale live row visible – the bug this refactor fixes.
    assert list(repo.query(flush_first=False)) == []
    assert list(repo.query_statements()) == []


def test_query_re_add_after_delete(repo):
    """Re-adding a deleted entity makes it visible again without merge."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    time.sleep(1.1)
    repo.delete_entity("jane")
    repo.flush()
    assert list(repo.query(flush_first=False)) == []

    # Re-add: new live row has last_seen > the tombstone's last_seen, so
    # ROW_NUMBER picks the re-add and deleted_at IS NULL keeps it.
    time.sleep(1.1)
    repo.add(jane)
    repo.flush()
    assert {e.id for e in repo.query(flush_first=False)} == {"jane"}


def test_query_dedupes_across_origins(repo):
    """Same entity under two origins: latest last_seen wins, regardless of origin."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane, origin="source-a")
    repo.flush()

    time.sleep(1.1)
    repo.add(jane, origin="source-b")
    repo.flush()

    stmts = list(repo.query_statements())
    by_id = {s.id: s for s in stmts}
    # One row per statement id – cross-origin dedupe.
    assert len(stmts) == len(by_id)
    # The later write under "source-b" wins ROW_NUMBER for every id.
    assert {s.origin for s in stmts} == {"source-b"}


def test_view_query_dedupes_without_merge(local_repo):
    """LakeStore view().query() sees deduped entities (global view, no iteration)."""
    repo = local_repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    time.sleep(1.1)
    repo.add(jane)
    repo.flush()

    # Reach through the parquet store to ftmq's view – this path doesn't
    # iterate (shard, bucket) but still sees the deduped view because the
    # view is registered globally on the LakeStore connection.
    entities = list(repo._statements.view().query())
    assert {e.id for e in entities} == {"jane"}


def test_merge_collapses_appended_duplicates(repo):
    """merge() reduces physical row count and leaves query results unchanged."""
    repo, path = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    rows1 = _row_count(path)
    pre_merge = {s.id: s for s in repo.query_statements()}

    repo.add(jane)
    repo.flush()
    assert _row_count(path) == rows1 * 2
    # Dedupe-on-read already collapses the duplicates before merge.
    dup_flush = {s.id: s for s in repo.query_statements()}
    assert set(dup_flush) == set(pre_merge)

    repo.merge()
    assert _row_count(path) == rows1
    # Physical cleanup must not change the visible result.
    post_merge = {s.id: s for s in repo.query_statements()}
    assert set(post_merge) == set(pre_merge)


def test_get_changed_entity_ids_sees_tombstones(local_repo):
    """The diff path targets statement_raw so deletions remain visible."""
    repo = local_repo
    jane = EntityProxy.from_dict(JANE)

    before = datetime.now(timezone.utc) - timedelta(seconds=2)

    repo.add(jane)
    repo.flush()
    time.sleep(1.1)
    repo.delete_entity("jane")
    repo.flush()

    # Even though the deduped view hides the tombstoned entity from
    # normal reads, the diff path queries statement_raw so it still
    # picks up the deletion timestamp.
    changed = list(repo._statements.get_changed_entity_ids(since=before))
    assert "jane" in changed


def test_flush_mixed_new_and_existing(repo):
    """A flush mixing dupes and new entities lands both, queryable as distinct."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)
    john = EntityProxy.from_dict(JOHN)

    repo.add(jane)
    repo.flush()

    with repo.writer() as writer:
        writer.add_entity(jane)
        writer.add_entity(john)
    repo.flush()

    entity_ids = {e.id for e in repo.query(flush_first=False)}
    assert entity_ids == {"jane", "john"}
