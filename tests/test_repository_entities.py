import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import pytest
from followthemoney import EntityProxy, model
from ftmq.query import M, P, Query
from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path, tag
from ftm_lakehouse.repository import EntityRepository
from ftm_lakehouse.repository.factories import get_entities
from tests.conftest import make_docker_repo, make_test_api
from tests.shared import BOB, JANE, JANE_FIRSTNAME, JOHN


@pytest.fixture(params=["local", "api", "docker"])
def repo(
    request, tmp_path
) -> Generator[tuple[EntityRepository, Path | None], None, None]:
    if request.param == "local":
        yield EntityRepository("test", tmp_path), tmp_path
    elif request.param == "api":
        with make_test_api(tmp_path) as base_url:
            dataset_url = f"{base_url}/test"
            repo = get_entities("test", uri=dataset_url)
            yield repo, tmp_path / "test"
    else:
        # docker: real nginx + lakehouse UDS; data lives at
        # ``./data/{dataset}`` on the host via the bind mount, so tests
        # that assert on the on-disk layout still work.
        yield make_docker_repo()


def test_repository_entities(repo):
    repo, base_path = repo

    # Initially empty (check tags before query which may trigger flush)
    assert not repo._tags.exists(tag.JOURNAL_UPDATED)
    assert not repo._tags.exists(tag.STATEMENTS_UPDATED)
    assert list(repo.query()) == []

    jane = make_entity(JANE)
    john = make_entity(JOHN)

    # Write entities using bulk writer
    with repo.writer() as writer:
        writer.add_entity(jane)
        writer.add_entity(john)

    # Tag should be set after bulk write
    assert repo._tags.exists(tag.JOURNAL_UPDATED)
    journal_updated = repo._tags.get(tag.JOURNAL_UPDATED)
    # Verify actual tag file path (hardcoded to detect convention changes)
    if base_path:
        assert (base_path / "tags/lakehouse/journal/last_updated").exists()

    # Query returns entities (flushes journal first)
    # before flush:
    assert not repo._tags.exists(tag.STATEMENTS_UPDATED)
    assert repo._journal.count() > 0
    assert repo.stats().entity_count == 0

    # This auto flushes the journal:
    entities = list(repo.query(flush_first=True))
    # after flush:
    assert len(entities) == 2
    assert repo._journal.count() == 0
    assert repo.stats().entity_count == 2
    # Tag should be set after flush (triggered by query)
    assert repo._tags.exists(tag.STATEMENTS_UPDATED)
    # Verify actual tag file path (hardcoded to detect convention changes)
    if base_path:
        assert (base_path / "tags/lakehouse/statements/last_updated").exists()

    # Get entity by ID
    jane_entity = repo.get("jane")
    assert jane_entity is not None
    assert jane_entity.first("name") == "Jane Doe"

    john_entity = repo.get("john")
    assert john_entity is not None
    assert john_entity.first("name") == "John Doe"

    # Non-existent entity returns None
    assert repo.get("nobody") is None

    # Node-based Query filters (round-trips as json through the api variant)
    named = list(repo.query(Query().where(M(schema="Person"), P(name="Jane Doe"))))
    assert {e.id for e in named} == {"jane"}
    assert not list(repo.query(Query().where(P(name="nobody"))))
    stmts = list(repo.query_statements(Query().where(M(entity_id="john"))))
    assert stmts and {s.entity_id for s in stmts} == {"john"}

    # Pagination + ordering hold end to end
    assert len(list(repo.query(Query()[:2]))) == 2
    assert len(list(repo.query(Query()[:1]))) == 1
    ordered = [e.first("name") for e in repo.query(Query().order_by("name"))]
    assert ordered == sorted(ordered)
    sliced = list(repo.query_statements(Query()[:1]))
    assert len({s.entity_id for s in sliced}) == 1

    # Adding more entities updates the journal tag
    with repo.writer() as writer:
        writer.add_entity(
            make_entity(
                {"id": "bob", "schema": "Person", "properties": {"name": ["Bob"]}}
            )
        )
    assert repo._tags.get(tag.JOURNAL_UPDATED) > journal_updated


def test_repository_entities_multi_origin(repo):
    """Test entity assembled from fragments with different origins."""
    repo, _ = repo

    # Add same entity ID from different origins with different properties
    with repo.writer(origin="source_a") as writer:
        entity = model.make_entity("Person")
        entity.id = "multi-origin"
        entity.add("name", "John Smith")
        entity.add("nationality", "us")
        writer.add_entity(entity)

    with repo.writer(origin="source_b") as writer:
        entity = model.make_entity("Person")
        entity.id = "multi-origin"
        entity.add("birthDate", "1980-01-15")
        entity.add("nationality", "gb")
        writer.add_entity(entity)

    # Query merged entity (all origins)
    merged = repo.get("multi-origin", flush_first=True)
    assert merged is not None
    assert "John Smith" in merged.get("name")
    assert "1980-01-15" in merged.get("birthDate")
    nationalities = merged.get("nationality")
    assert "us" in nationalities
    assert "gb" in nationalities
    assert set(merged.to_dict()["origin"]) == {"source_a", "source_b"}


def test_repository_entities_export_diff(tmp_path):
    """Test incremental diff export using change detection.

    The first export writes no file - it only records the state the next diff
    is taken against. Subsequent diffs capture incremental changes via
    translog timestamps.

    Sleeps cross second boundaries because FtM truncates timestamps to seconds
    and diff detection uses first_seen >= floor(since).
    """
    import time

    repo = EntityRepository("test", tmp_path)

    # Create multiple flushes to simulate real usage where table is at v > 0
    # before first diff export
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    # version 0 is the empty create commit (ParquetStore._ensure_table)
    assert repo.version == 1

    with repo.writer() as writer:
        writer.add_entity(make_entity(JOHN))
    repo.flush()
    assert repo.version == 2

    # a diff reads canonical rows, so the store has to be merged first
    repo.merge()

    # First export - only records the diff state, writes no file
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None
    assert diff_name_1.endswith("Z")  # timestamp format
    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 0

    # Add more data: creates Delta table v2
    with repo.writer() as writer:
        writer.add_entity(make_entity(BOB))
    repo.flush()
    repo.merge()

    # Incremental diff - captures changes via translog
    diff_name_2 = repo.export_diff()
    assert diff_name_2 is not None
    assert diff_name_2 != diff_name_1

    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 1

    # Find and verify the incremental diff contains only BOB
    diff_files_sorted = sorted(diff_files)
    with repo._store.open(diff_files_sorted[0]) as f:
        lines = f.readlines()
    assert len(lines) == 1
    delta = json.loads(lines[0])
    assert delta["op"] == "ADD"
    assert delta["entity"]["id"] == "bob"

    # Re-adding jane without changes doesn't create new diff after merge
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    repo.merge()

    assert repo.export_diff() is None
    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 1

    # Updating Jane firstName creates diff
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE_FIRSTNAME))
    repo.flush()
    repo.merge()

    diff_name_3 = repo.export_diff()
    assert diff_name_3 is not None
    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 2

    # Find and verify the incremental diff contains only JANE - and carries
    # her whole current state, not just the statement that changed: consumers
    # index an ADD payload wholesale, so a partial one would drop `name`
    diff_files_sorted = sorted(diff_files)
    with repo._store.open(diff_files_sorted[1]) as f:
        lines = f.readlines()
    assert len(lines) == 1
    delta = json.loads(lines[0])
    assert delta["op"] == "ADD"
    assert delta["entity"]["id"] == "jane"
    assert delta["entity"]["properties"] == {
        "firstName": ["Jane"],
        "name": ["Jane Doe"],
    }


def test_repository_entities_export_diff_delete(tmp_path):
    """Deleting an entity produces a DEL op in the incremental diff.

    The merge is what applies the tombstone – until it runs, the deleted
    entity's rows are still live – which is why a diff requires an optimized
    store (covered by ``..._requires_optimized_store``)."""
    repo = EntityRepository("test", tmp_path)

    # Add two entities and flush
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))
    repo.flush()
    repo.merge()

    # First export - only records the diff state
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None

    # Delete jane, flush the tombstones and collapse the partition (the
    # tombstone rows themselves survive the grace period).
    since = datetime.now(timezone.utc)
    repo.delete_entity("jane")
    repo.flush()
    repo.merge()

    # jane is in changed entity ids
    assert list(repo._get_changed_ids(since)) == ["jane"]

    # Incremental diff should contain a DEL for jane
    diff_name_2 = repo.export_diff()
    assert diff_name_2 is not None
    assert diff_name_2 != diff_name_1

    diff_files = sorted(
        (tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"),
        key=lambda p: p.name,
    )
    assert len(diff_files) == 1

    # Read the incremental diff
    with open(diff_files[0]) as f:
        lines = f.readlines()

    ops = [json.loads(line) for line in lines]
    del_ops = [o for o in ops if o["op"] == "DEL"]
    assert len(del_ops) == 1
    assert del_ops[0]["entity"]["id"] == "jane"


def test_repository_entities_query_slice_multi_shard(tmp_path):
    """LIMIT / ORDER BY hold across shards: sliced or sorted queries execute
    globally instead of once per (shard, bucket) partition (which would
    return up to N entities *per partition*)."""
    repo = EntityRepository("test", tmp_path)
    repo.shards = 4
    with repo.writer() as writer:
        for i in range(8):
            writer.add_entity(
                make_entity(
                    {
                        "id": f"e{i}",
                        "schema": "Person",
                        "properties": {"name": [f"P {i}"]},
                    }
                )
            )
    repo.flush()

    # the fixture entities actually spread over multiple shards
    shards = {path.entity_shard(f"e{i}", 4) for i in range(8)}
    assert len(shards) > 1

    assert len(list(repo.query(Query()[:3]))) == 3
    stmts = list(repo.query_statements(Query()[:3]))
    assert len({s.entity_id for s in stmts}) == 3
    names = [e.first("name") for e in repo.query(Query().order_by("name"))]
    assert names == sorted(names)
    assert len(names) == 8


def test_repository_entities_export_diff_fragment_update(tmp_path):
    """An updated fragment emission diffs with only its latest values.

    Supersession has dropped the shadowed emission, so it is not accumulated
    into the ADD entity - and the superseding row keeps its own ``first_seen``
    (``_dedupe_sql`` folds per statement id), so the update is detected at
    all."""
    repo = EntityRepository("test", tmp_path)

    t1 = datetime.now(timezone.utc).isoformat()
    jane_v1 = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane Doe"]},
            "last_change": t1,
        }
    )
    with repo.writer() as writer:
        writer.add_entity(jane_v1, fragment="row1")
    repo.flush()
    repo.merge()

    assert repo.export_diff() is not None

    # Re-emit the same fragment with a changed name; no merge before diffing.
    t2 = datetime.now(timezone.utc).isoformat()
    jane_v2 = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane D. Doe"]},
            "last_change": t2,
        }
    )
    with repo.writer() as writer:
        writer.add_entity(jane_v2, fragment="row1")
    repo.flush()
    repo.merge()

    assert repo.export_diff() is not None
    diff_files = sorted(
        (tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"), key=lambda p: p.name
    )
    ops = [json.loads(line) for line in open(diff_files[-1])]
    assert len(ops) == 1
    assert ops[0]["op"] == "ADD"
    # only the superseding emission's value – v1 is shadowed, not accumulated
    assert ops[0]["entity"]["properties"]["name"] == ["Jane D. Doe"]


def test_repository_entities_export_diff_no_changes(tmp_path):
    """Test diff export when there are no new changes after initial setup."""
    repo = EntityRepository("test", tmp_path)

    # Create data and flush
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()
    # version 0 is the empty create commit (ParquetStore._ensure_table)
    assert repo.version == 1

    with repo.writer() as writer:
        writer.add_entity(make_entity(JOHN))
    repo.flush()
    assert repo.version == 2

    # a diff reads canonical rows, so the store has to be merged first
    repo.merge()

    # First export - only records the diff state
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None
    assert diff_name_1.endswith("Z")

    # Second diff without any new data - no new diff file
    assert repo.export_diff() is None

    # No diff file at all - the first export writes none
    diff_files = list((tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"))
    assert len(diff_files) == 0


def test_repository_entities_export_diff_partial_delete_keeps_the_entity(tmp_path):
    """Tombstoning one statement diffs as an ADD of what remains, not a DEL.

    ``DEL`` means the entity is gone entirely; the entity here still exists,
    so a whole-entity DEL would tell consumers to drop something live.
    """
    repo = EntityRepository("test", tmp_path)
    jane = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane Doe"], "firstName": ["Jane"]},
        }
    )
    with repo.writer() as writer:
        writer.add_entity(jane)
    repo.flush()
    repo.merge()

    assert repo.export_diff() is not None

    victim = next(
        s
        for s in repo.query_statements(Query(M(entity_id="jane")))
        if s.prop == "firstName"
    )
    repo.delete_statement(victim)
    repo.flush()
    repo.merge()

    assert repo.export_diff() is not None
    diff_files = sorted(
        (tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"), key=lambda p: p.name
    )
    ops = [json.loads(line) for line in open(diff_files[-1])]
    assert len(ops) == 1
    assert ops[0]["op"] == "ADD"
    assert ops[0]["entity"]["properties"] == {"name": ["Jane Doe"]}


def test_repository_entities_export_diff_requires_optimized_store(tmp_path):
    """A diff on an un-merged store refuses instead of publishing wrong data.

    Reads are canonical only after ``merge``: before it, a deleted entity's
    rows are still live and a superseded fragment value still shows. A diff
    publishes each changed entity's current state, so it must not run there.
    """
    repo = EntityRepository("test", tmp_path)
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()

    assert repo._statements.needs_merge
    with pytest.raises(RuntimeError, match="un-merged writes"):
        repo.export_diff()

    repo.merge()
    assert not repo._statements.needs_merge

    assert repo.export_diff() is not None
