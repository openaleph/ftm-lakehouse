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
            repo = EntityRepository("test", uri=dataset_url)
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
    assert repo.get_statistics().entity_count == 0

    # This auto flushes the journal:
    entities = list(repo.query(flush_first=True))
    # after flush:
    assert len(entities) == 2
    assert repo._journal.count() == 0
    assert repo.get_statistics().entity_count == 2
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

    Initial diff copies entities.ftm.json regardless of Delta table version.
    Subsequent diffs capture incremental changes via translog timestamps.

    Sleeps cross second boundaries because FtM truncates timestamps to seconds
    and diff detection uses first_seen >= floor(since).
    """
    import time

    from ftmq.io import smart_write_proxies

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

    # Export entities.ftm.json (required for initial diff)
    smart_write_proxies(repo._store.open(path.ENTITIES_JSON, "wb"), repo.query())

    # Initial diff - copies entities.ftm.json even though table is at v1
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None
    assert diff_name_1.endswith("Z")  # timestamp format
    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 1  # Initial diff file created

    # Verify initial diff contains both JANE and JOHN (full export)
    with repo._store.open(diff_files[0]) as f:
        lines = f.readlines()
    assert len(lines) == 2
    entities = {json.loads(line)["entity"]["id"] for line in lines}
    assert entities == {"jane", "john"}

    # Add more data: creates Delta table v2
    with repo.writer() as writer:
        writer.add_entity(make_entity(BOB))
    repo.flush()

    # Incremental diff - captures changes via translog
    diff_name_2 = repo.export_diff()
    assert diff_name_2 is not None
    assert diff_name_2 != diff_name_1

    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 2

    # Find and verify the incremental diff contains only BOB
    diff_files_sorted = sorted(diff_files)
    with repo._store.open(diff_files_sorted[1]) as f:
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
    assert len(diff_files) == 2

    # Updating Jane firstName creates diff
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE_FIRSTNAME))
    repo.flush()

    diff_name_3 = repo.export_diff()
    assert diff_name_3 is not None
    diff_files = list(
        repo._store.iterate_keys(prefix=path.DIFFS_ENTITIES, glob="*.delta.json")
    )
    assert len(diff_files) == 3

    # Find and verify the incremental diff contains only JANE
    diff_files_sorted = sorted(diff_files)
    with repo._store.open(diff_files_sorted[2]) as f:
        lines = f.readlines()
    assert len(lines) == 1
    delta = json.loads(lines[0])
    assert delta["op"] == "ADD"
    assert delta["entity"]["id"] == "jane"


@pytest.mark.parametrize("merge", [False, True])
def test_repository_entities_export_diff_delete(tmp_path, merge):
    """Deleting an entity produces a DEL op in the incremental diff – with
    or without an intervening merge: the diff reads canonical rows via the
    dedupe query, so flushed tombstones shadow their live rows either way."""
    from ftmq.io import smart_write_proxies

    repo = EntityRepository("test", tmp_path)

    # Add two entities and flush
    with repo.writer() as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))
    repo.flush()

    # Export entities.ftm.json (required for initial diff)
    entities_json_path = tmp_path / path.ENTITIES_JSON
    smart_write_proxies(str(entities_json_path), repo.query())

    # Initial diff
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None

    # Delete jane and flush the tombstones to parquet. The merge=True leg
    # additionally collapses the partition (tombstones survive grace); the
    # diff must emit the DEL in both cases.
    since = datetime.now(timezone.utc)
    repo.delete_entity("jane")
    repo.flush()
    if merge:
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
    assert len(diff_files) == 2

    # Read the incremental diff (second file)
    with open(diff_files[1]) as f:
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


def test_repository_entities_export_diff_fragment_update_without_merge(tmp_path):
    """An updated fragment emission diffs with only its latest values on an
    un-merged store – the diff reads canonical (superseded) rows, not the
    raw live view where both emissions still coexist."""
    from ftmq.io import smart_write_proxies

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

    entities_json_path = tmp_path / path.ENTITIES_JSON
    smart_write_proxies(str(entities_json_path), repo.query())
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
    from ftmq.io import smart_write_proxies

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

    # Export entities.ftm.json for initial diff
    entities_json_path = tmp_path / path.ENTITIES_JSON
    smart_write_proxies(str(entities_json_path), repo.query())

    # Initial diff - copies entities.ftm.json
    diff_name_1 = repo.export_diff()
    assert diff_name_1 is not None
    assert diff_name_1.endswith("Z")

    # Second diff without any new data - no new diff file
    assert repo.export_diff() is None

    # Only one diff file should exist (initial)
    diff_files = list((tmp_path / path.DIFFS_ENTITIES).glob("*.delta.json"))
    assert len(diff_files) == 1
