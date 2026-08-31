"""``role`` end-to-end: journal → parquet → merge.

``role`` records *who* asserted a statement, alongside ``origin``'s *where*.
It is the fourth dimension of the store's row identity (after statement
``id``, ``origin`` and ``fragment``), so two roles asserting identical
content survive as two rows instead of the later one overwriting the
earlier – full provenance, not last-writer-wins.

Row identity is applied by ``merge`` (the live view does no dedupe), so
these tests merge before asserting.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import Statement
from ftmq.query import C, M, Query
from rigour.time import utc_now

from ftm_lakehouse.model.statement import LakehouseStatement
from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import get_entities
from tests.conftest import make_docker_repo, make_test_api

DATASET = "test"

T1 = datetime(2026, 1, 1, 12, 0, 0, tzinfo=timezone.utc).isoformat()
T2 = datetime(2026, 1, 1, 13, 0, 0, tzinfo=timezone.utc).isoformat()


def _make_local_repo(tmp_path) -> EntityRepository:
    return EntityRepository(DATASET, tmp_path)


def _stmt(prop: str, value: str, last_seen: str, entity_id: str = "acme") -> Statement:
    return Statement(
        entity_id=entity_id,
        prop=prop,
        schema="Company",
        value=value,
        dataset=DATASET,
        last_seen=last_seen,
    )


def _roles(repo: EntityRepository, prop: str = "name") -> list[str]:
    return sorted(s.role or "" for s in repo.query_statements() if s.prop == prop)


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
    yield _make_local_repo(tmp_path)


def test_roles_both_survive_merge(repo):
    """Two roles asserting the same content keep one row each."""
    repo, _ = repo
    with repo.writer(role="user:42") as w:
        w.add_statement(_stmt("name", "Acme Inc", T1))
    repo.flush()
    with repo.writer(role="user:7") as w:
        w.add_statement(_stmt("name", "Acme Inc", T2))
    repo.flush()
    repo.merge()

    assert _roles(repo) == ["user:42", "user:7"]


def test_role_reassertion_collapses(repo):
    """One role re-asserting the same content still dedupes to one row –
    only the *cross-role* case multiplies."""
    repo, _ = repo
    for last_seen in (T1, T2):
        with repo.writer(role="user:42") as w:
            w.add_statement(_stmt("name", "Acme Inc", last_seen))
        repo.flush()
    repo.merge()

    assert _roles(repo) == ["user:42"]


def test_roleless_and_role_rows_are_distinct(repo):
    """No role is its own identity, not a wildcard: a role-less assertion
    and a role-bearing one of the same content are two rows."""
    repo, _ = repo
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1))
    with repo.writer(role="user:42") as w:
        w.add_statement(_stmt("name", "Acme Inc", T1))
    repo.flush()
    repo.merge()

    assert _roles(repo) == ["", "user:42"]


def test_role_entity_values_unaffected(repo):
    """Row multiplication is provenance only – the assembled entity still
    has one value per distinct value, whoever asserted it."""
    repo, _ = repo
    for role in ("user:42", "user:7"):
        with repo.writer(role=role) as w:
            w.add_statement(_stmt("name", "Acme Inc", T1))
    repo.flush()
    repo.merge()

    entity = repo.get("acme")
    assert entity is not None
    assert entity.get("name") == ["Acme Inc"]


def test_role_query_filter(repo):
    """``role`` is an ordinary storage column, so the ftmq ``C`` family
    filters it with no query-layer work.

    Like ``C(origin=...)``, the filter selects *entities* that have a
    matching statement (ftmq compiles it to ``entity_id IN (SELECT ... WHERE
    role = ...)``), not individual rows – hence two entities here.
    """
    repo, _ = repo
    with repo.writer(role="user:42") as w:
        w.add_statement(_stmt("name", "Acme Inc", T1, entity_id="acme"))
    with repo.writer(role="user:7") as w:
        w.add_statement(_stmt("name", "Beta Ltd", T1, entity_id="beta"))
    repo.flush()
    repo.merge()

    stmts = list(repo.query_statements(Query(C(role="user:42"))))
    assert {s.entity_id for s in stmts} == {"acme"}
    assert sorted(
        s.entity_id for s in repo.query_statements(Query(C(role__in=["user:7"])))
    ) == ["beta"]


def test_delete_entity_tombstones_every_role(repo):
    """Deleting an entity deletes what *every* role asserted.

    ``delete_entity`` reads the live rows and writes one matching tombstone
    each, so a row identity carrying ``role`` needs no special handling – but
    a tombstone that dropped the role would land in a different merge group
    and shadow nothing.
    """
    repo, _ = repo
    for role in ("user:42", "user:7"):
        with repo.writer(role=role) as w:
            w.add_statement(_stmt("name", "Acme Inc", T1))
    repo.flush()
    repo.merge()
    assert repo.get("acme") is not None

    count = repo.delete_entity("acme")
    assert count == 2  # one tombstone per role
    repo.flush()
    repo.merge()

    assert repo.get("acme") is None


def test_query_statements_role_roundtrip(repo):
    """``query_statements`` exposes ``role`` in local AND api mode – the
    NDJSON wire carries it as an explicit field (``Statement.to_dict`` has no
    notion of it) and the client rebuilds ``LakehouseStatement``s, so
    statement-level tombstones land in the right row identity either way."""
    repo, _ = repo
    with repo.writer(role="user:42") as w:
        w.add_statement(_stmt("name", "Acme Inc", T1))
        w.add_statement(_stmt("country", "de", T1), role="user:7")
    repo.flush()

    stmts = list(repo.query_statements())
    assert all(isinstance(s, LakehouseStatement) for s in stmts)
    assert {s.prop: s.role for s in stmts} == {"name": "user:42", "country": "user:7"}


def test_delete_statement_shadows_only_its_role(local_repo):
    """A statement read back carries its role, so deleting it leaves the
    identical assertion of another role live."""
    for role in ("user:42", "user:7"):
        with local_repo.writer(role=role) as w:
            w.add_statement(_stmt("name", "Acme Inc", T1))
    local_repo.flush()
    local_repo.merge()

    target = next(
        s
        for s in local_repo.query_statements(Query(M(entity_id="acme")))
        if s.role == "user:42"
    )
    local_repo.delete_statement(target)
    local_repo.flush()
    local_repo.merge()

    assert _roles(local_repo) == ["user:7"]


def test_tombstone_ignores_the_writer_default_role(local_repo):
    """A tombstone takes the shadowed row's role, never the writer's default.

    The one way a delete can miss because of ``role``: a role-less row
    retracted through a writer opened with a default role would otherwise get
    that role stamped on its tombstone, landing it in a different merge group
    – the row stays live and a stray tombstone accumulates beside it.
    """
    with local_repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1))
    local_repo.flush()
    local_repo.merge()
    (live,) = list(local_repo.query_statements())
    assert live.role is None

    with local_repo.writer(role="user:42") as w:
        w.add_statement(live, deleted_at=utc_now())
    local_repo.flush()
    local_repo.merge()

    assert list(local_repo.query_statements()) == []


def test_delete_entity_across_prior_role_emissions(local_repo):
    """Deletion is unconditional on role – prior, superseded and unflushed
    emissions of every role die together.

    ``delete_entity`` reads the live rows, so each role's supersession group
    gets its own tombstone at ``now``; that is the group maximum, which drops
    the group's older emissions along with it.
    """
    with local_repo.writer(role="user:42") as w:
        w.add_statement(_stmt("name", "Acme v1", T1), fragment="f")
    local_repo.flush()
    with local_repo.writer(role="user:42") as w:
        w.add_statement(_stmt("name", "Acme v2", T2), fragment="f")
    with local_repo.writer(role="user:7") as w:
        w.add_statement(_stmt("name", "Acme B", T2), fragment="f")
    with local_repo.writer() as w:
        w.add_statement(_stmt("country", "de", T1))
    local_repo.flush()
    local_repo.merge()
    # a role-bearing emission that never reached parquet
    with local_repo.writer(role="user:9") as w:
        w.add_statement(_stmt("country", "de", T2))

    local_repo.delete_entity("acme")
    local_repo.flush()
    local_repo.merge()

    assert local_repo.get("acme") is None
    assert list(local_repo.query_statements()) == []


def test_role_empty_string_lands_as_null(local_repo, tmp_path):
    """``role=""`` from the SDK collapses to NULL, so "no role" has exactly
    one representation in storage and in the dedupe key."""
    with local_repo.writer(role="") as w:
        w.add_statement(_stmt("name", "Acme Inc", T1))
    local_repo.flush()

    (stmt,) = list(local_repo.query_statements())
    assert stmt.role is None
