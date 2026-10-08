"""Flush appends physical rows; reads reconcile them; ``merge`` compacts.

Every flush lands as new rows – duplicates, re-emissions and tombstones
included. A read over such a partition collapses duplicates per ``(id, origin,
fragment, role)``, applies fragment supersession, folds ``first_seen`` /
``last_seen`` and hides tombstoned ids, so the same statements come back
before and after ``merge``; ``merge`` only rewrites the partition into one
canonical file, after which reads are a plain scan. The ``settle`` fixture
runs each test both ways.
"""

from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import EntityProxy
from ftmq import M, Query

from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import get_entities
from tests.conftest import make_docker_repo, make_test_api, merge
from tests.duck import make_duckdb
from tests.shared import JANE, JOHN

DATASET = "test"


def _make_local_repo(tmp_path) -> EntityRepository:
    return EntityRepository(DATASET, tmp_path)


def _row_count(path: str) -> int:
    con = make_duckdb()
    path = f"{path}/statements"
    return con.execute(f"SELECT COUNT(*) FROM delta_scan('{path}')").fetchone()[0]


@pytest.fixture(params=["local", "api", "docker"])
def repo(
    request, tmp_path
) -> Generator[tuple[EntityRepository, Path | None], None, None]:
    if request.param == "local":
        yield _make_local_repo(tmp_path), tmp_path
    elif request.param == "api":
        with make_test_api(tmp_path) as base_url:
            dataset_url = f"{base_url}/{DATASET}"
            r = get_entities(DATASET, uri=dataset_url)
            yield r, tmp_path / DATASET
    else:
        yield make_docker_repo()


@pytest.fixture
def local_repo(tmp_path) -> Generator[EntityRepository, None, None]:
    """Local-only fixture for tests that hit ``@no_api`` internals
    (``get_changed_entity_ids`` isn't exposed via the API).
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


def test_query_dedup_after_re_add(repo, settle):
    """Re-flushing the same entity still surfaces one row per statement id."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    settle(repo)
    stmts1 = list(repo.query_statements())

    repo.add(jane)
    repo.flush()
    settle(repo)
    stmts2 = list(repo.query_statements())

    entities = list(repo.query(flush_first=False))
    assert {e.id for e in entities} == {"jane"}
    # the second flush's fresh copies collapse to one row per id
    assert len(stmts2) == len(stmts1)
    assert {s.id for s in stmts2} == {s.id for s in stmts1}


def test_query_statements_dedup(repo, settle):
    """One row per id with folded first_seen / latest last_seen."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    settle(repo)
    first = {s.id: (s.first_seen, s.last_seen) for s in repo.query_statements()}

    # Re-add so last_seen differs across the two physical rows
    repo.add(jane)
    repo.flush()
    settle(repo)

    stmts = list(repo.query_statements())
    by_id = {s.id: s for s in stmts}
    # no duplicate statement ids, merged or not
    assert len(stmts) == len(by_id)
    # Dedupe keeps the earliest first_seen and the latest last_seen.
    for stmt_id, (orig_first, orig_last) in first.items():
        assert by_id[stmt_id].first_seen == orig_first
        assert by_id[stmt_id].last_seen > orig_last


def test_query_skips_tombstone(repo, settle):
    """Deleting an entity hides it from queries."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    settle(repo)
    assert {e.id for e in repo.query(flush_first=False)} == {"jane"}

    repo.delete_entity("jane")
    repo.flush()
    # the id collapses to its tombstone (latest last_seen), which the live
    # rows filter out – whether the pair still coexists physically or not
    settle(repo)

    assert list(repo.query(flush_first=False)) == []
    assert list(repo.query_statements()) == []


def test_query_re_add_after_delete(repo, settle):
    """Re-adding a deleted entity makes it visible again."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    repo.delete_entity("jane")
    repo.flush()
    settle(repo)
    assert list(repo.query(flush_first=False)) == []

    # re-add: the new live row has last_seen > the tombstone's, so it wins
    # the id and deleted_at IS NULL keeps it
    repo.add(jane)
    repo.flush()
    settle(repo)
    assert {e.id for e in repo.query(flush_first=False)} == {"jane"}


def test_query_no_cross_origin_dedupe(repo, settle):
    """The same statement under two origins is kept per origin.

    ``origin`` is a partition key, so ``merge`` – which rewrites each
    ``(shard, bucket, origin)`` partition independently – cannot collapse a
    statement observed under two origins into one; both copies survive, each
    carrying its origin. This is a deliberate consequence of the simplified
    model: reads reflect the physical, per-origin layout rather than an
    id-level dedupe that crossed origins (which the old read-time view did).
    """
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane, origin="source-a")
    repo.flush()
    repo.add(jane, origin="source-b")
    repo.flush()
    settle(repo)

    stmts = list(repo.query_statements())
    # Both origins survive; each statement id appears once per origin.
    assert {s.origin for s in stmts} == {"source-a", "source-b"}
    assert set(Counter(s.id for s in stmts).values()) == {2}


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

    merge(repo)
    assert _row_count(path) == rows1
    # Physical cleanup must not change the visible result.
    post_merge = {s.id: s for s in repo.query_statements()}
    assert set(post_merge) == set(pre_merge)


def test_deleted_candidates_see_tombstones(local_repo):
    """The diff path reads statement_raw, so deletions remain visible."""
    repo = local_repo
    jane = EntityProxy.from_dict(JANE)

    before = datetime.now(timezone.utc) - timedelta(seconds=2)

    repo.add(jane)
    repo.flush()

    repo.delete_entity("jane")
    repo.flush()

    # the live view hides jane; the raw scan behind the DEL candidates does not
    assert not list(repo.query(Query(M(entity_id="jane"))))
    candidates = {c.id: c for c in repo.statements.deleted_candidates(before)}
    assert set(candidates) == {"jane"}
    assert candidates["jane"].schemata == {"Person"}


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
