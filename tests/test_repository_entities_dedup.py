"""Flush appends; merge dedupes. Re-adding identical entities is idempotent at
the query layer (one entity in, one out) but does add new physical rows until
``merge()`` collapses them.
"""

from pathlib import Path
from typing import Generator

import pytest
from followthemoney import EntityProxy

from ftm_lakehouse.api.main import archive_router, entities_router, journal_router
from ftm_lakehouse.logic.parquet import make_duckdb
from ftm_lakehouse.repository.entities import EntityRepository
from tests.conftest import make_docker_repo, make_test_api
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
    """Query aggregates statements per entity — same entity appears once."""
    repo, _ = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    repo.add(jane)
    repo.flush()

    entities = list(repo.query(flush_first=False))
    assert {e.id for e in entities} == {"jane"}


def test_merge_collapses_appended_duplicates(repo):
    """merge() reduces row count back to one copy per statement id."""
    repo, path = repo
    jane = EntityProxy.from_dict(JANE)

    repo.add(jane)
    repo.flush()
    rows1 = _row_count(path)

    repo.add(jane)
    repo.flush()
    assert _row_count(path) == rows1 * 2

    repo.merge()
    assert _row_count(path) == rows1


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
