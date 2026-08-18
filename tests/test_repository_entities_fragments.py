"""Fragment supersession end-to-end: journal → parquet → merge.

A statement written with a ``fragment`` participates in ``(entity_id,
prop, fragment)``-keyed supersession – a later emission of the same
triple replaces the older one even though the content-addressed
statement ids differ. Non-fragment statements (the default) keep the
existing content-addressed dedup; the two modes never interact.

Supersession is applied by ``merge`` (the live view does no dedupe), so
these tests merge before asserting which emission survives – between a
write and the next merge both emissions are physically present.
"""

from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import EntityProxy, Statement
from ftmq.query import M, Query
from ftmq.store.lake import LakeStatement

from ftm_lakehouse.repository.entities import EntityRepository
from ftm_lakehouse.repository.factories import get_entities
from tests.conftest import make_docker_repo, make_test_api
from tests.duck import make_duckdb
from tests.shared import JANE

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


def _values(repo: EntityRepository, prop: str) -> list[str]:
    return sorted(s.value for s in repo.query_statements() if s.prop == prop)


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


def test_fragment_supersession_replaces_older_emission(repo):
    """Re-emitting a fragment replaces its older values per prop."""
    repo, _ = repo
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1), fragment="row42")
    repo.flush()
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Corp", T2), fragment="row42")
    repo.flush()
    repo.merge()

    assert _values(repo, "name") == ["Acme Corp"]


def test_fragment_multi_value_props_survive_together(repo):
    """All rows of the latest emission survive – they share last_seen."""
    repo, _ = repo
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1), fragment="row42")
        w.add_statement(_stmt("name", "Acme GmbH", T1), fragment="row42")
    repo.flush()
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Corp", T2), fragment="row42")
        w.add_statement(_stmt("name", "Acme Ltd", T2), fragment="row42")
    repo.flush()
    repo.merge()

    assert _values(repo, "name") == ["Acme Corp", "Acme Ltd"]


def test_fragment_prop_dropped_between_emissions_survives(repo):
    """Supersession is per prop – a prop absent from the newer emission
    keeps its older row."""
    repo, _ = repo
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1), fragment="row42")
        w.add_statement(_stmt("country", "de", T1), fragment="row42")
    repo.flush()
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Corp", T2), fragment="row42")
    repo.flush()
    repo.merge()

    assert _values(repo, "name") == ["Acme Corp"]
    assert _values(repo, "country") == ["de"]


def test_nonfragment_rows_unaffected_by_fragments(repo):
    """Non-fragment statements keep content-addressed semantics even when
    fragment rows exist for the same entity."""
    repo, _ = repo
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1))
        w.add_statement(_stmt("name", "Acme Corp", T2), fragment="row42")
    repo.flush()
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Intl", T2))
    repo.flush()

    # both non-fragment values coexist (distinct ids), fragment value too
    assert _values(repo, "name") == ["Acme Corp", "Acme Inc", "Acme Intl"]


def test_fragment_and_nonfragment_same_content_coexist(repo):
    """The same (entity_id, prop, value) in both modes is two rows with
    the same statement id – the branches are isolated."""
    repo, _ = repo
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1))
        w.add_statement(_stmt("name", "Acme Inc", T1), fragment="row42")
    repo.flush()

    stmts = [s for s in repo.query_statements() if s.prop == "name"]
    assert len(stmts) == 2
    assert len({s.id for s in stmts}) == 1


def test_add_entity_fragment_emissions(repo):
    """Entity-level emissions with a fragment supersede per prop.

    ``last_seen`` has second granularity in the FtM statement model, so
    distinct emissions need distinct timestamps – set via the entity's
    ``last_change`` here (the fallback chain in ``add_entity``).
    """
    repo, _ = repo
    jane = EntityProxy.from_dict({**JANE, "last_change": T1})
    with repo.writer() as w:
        w.add_entity(jane, fragment="row1")
    repo.flush()
    changed = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane D. Doe"]},
            "last_change": T2,
        }
    )
    with repo.writer() as w:
        w.add_entity(changed, fragment="row1")
    repo.flush()
    repo.merge()

    entity = repo.get("jane")
    assert entity is not None
    assert entity.get("name") == ["Jane D. Doe"]


def test_delete_entity_with_fragments(repo):
    """Tombstones carry the live row's fragment so the delete lands in the
    same supersession group."""
    repo, _ = repo
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1), fragment="row42")
        w.add_statement(_stmt("country", "de", T1))
    repo.flush()
    assert repo.get("acme") is not None

    count = repo.delete_entity("acme")
    assert count == 2
    repo.flush()
    repo.merge()

    assert repo.get("acme") is None


def test_query_statements_fragment_roundtrip(repo):
    """``query_statements`` exposes ``fragment`` in local AND api mode – the
    NDJSON wire carries it as an explicit field (``Statement.to_dict`` has
    no notion of it) and the client rebuilds ``LakeStatement``s, so
    statement-level tombstones land in the correct supersession branch
    either way."""
    repo, _ = repo
    with repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1), fragment="row42")
        w.add_statement(_stmt("country", "de", T1))
    repo.flush()

    stmts = list(repo.query_statements())
    assert all(isinstance(s, LakeStatement) for s in stmts)
    assert {s.prop: s.fragment for s in stmts} == {"name": "row42", "country": ""}


def test_read_statements_expose_fragment(local_repo):
    """Statements read back from parquet are LakeStatements carrying their
    fragment, so tombstone writers can shadow the right supersession group."""
    with local_repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1), fragment="row42")
        w.add_statement(_stmt("country", "de", T1))
    local_repo.flush()

    stmts = list(local_repo.query_statements(Query(M(entity_id="acme"))))
    assert all(isinstance(s, LakeStatement) for s in stmts)
    assert {s.prop: s.fragment for s in stmts} == {"name": "row42", "country": ""}


def test_fragment_none_lands_as_empty_string(local_repo, tmp_path):
    """``fragment=None`` from the SDK is stored as the empty-string
    sentinel, never NULL."""
    with local_repo.writer() as w:
        w.add_statement(_stmt("name", "Acme Inc", T1), fragment=None)
        w.add_statement(_stmt("name", "Acme Corp", T1), fragment="row42")
    local_repo.flush()

    con = make_duckdb()
    fragments = {
        r[0]
        for r in con.execute(
            f"SELECT fragment FROM delta_scan('{tmp_path}/statements')"
        ).fetchall()
    }
    assert fragments == {"", "row42"}
