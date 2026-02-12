"""Tests for JournalStore implementations (SQL and API)."""

from typing import Generator

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from followthemoney.statement import Statement

from ftm_lakehouse.api.routes.journal import router
from ftm_lakehouse.core.api import get_api
from ftm_lakehouse.helpers.statements import unpack_statement
from ftm_lakehouse.storage.journal import ApiJournalStore, JournalRows, SqlJournalStore
from ftm_lakehouse.storage.journal import get_journal as _get_journal_factory
from ftm_lakehouse.storage.journal.base import BaseJournalStore

DATASET = "test"


def make_statement(
    entity_id: str,
    prop: str,
    value: str,
    schema: str = "Person",
    origin: str | None = None,
) -> Statement:
    return Statement(
        entity_id=entity_id,
        prop=prop,
        schema=schema,
        value=value,
        dataset=DATASET,
        origin=origin,
    )


def collect_statements(items: JournalRows) -> list[Statement]:
    """Collect all statements from flush items."""
    return [unpack_statement(stmt) for _, _, _, _, stmt, _ in items]


def _make_sql_journal() -> SqlJournalStore:
    return SqlJournalStore(dataset=DATASET, uri="sqlite:///:memory:")


def _make_api_journal() -> ApiJournalStore:
    app = FastAPI()
    app.state.journal_uri = "sqlite:///:memory:"
    app.include_router(router)

    test_client = TestClient(app)
    transport = httpx.MockTransport(
        lambda request: test_client.send(
            test_client.build_request(
                method=request.method,
                url=str(request.url),
                headers=dict(request.headers),
                content=request.read(),
            )
        )
    )
    client = httpx.Client(transport=transport, base_url="http://testserver")

    store = ApiJournalStore(dataset=DATASET, uri=f"http://testserver/{DATASET}")
    store._api.client = client
    return store


@pytest.fixture(params=["sql", "api"])
def journal(request) -> Generator[BaseJournalStore, None, None]:
    if request.param == "sql":
        yield _make_sql_journal()
    else:
        store = _make_api_journal()
        yield store
        store.close()
    _get_journal_factory.cache_clear()
    get_api.cache_clear()


def test_storage_journal_initialize(journal):
    """Test journal can be initialized and starts empty."""
    assert collect_statements(journal.flush()) == []


def test_storage_journal_put_and_flush(journal):
    """Test basic put and flush operations."""
    with journal.writer() as w:
        w.add_statement(make_statement("jane", "name", "Jane Doe"))
        w.add_statement(make_statement("jane", "firstName", "Jane"))
        w.add_statement(make_statement("jane", "lastName", "Doe"))
        w.add_statement(make_statement("john", "name", "John Smith"))
        w.add_statement(make_statement("john", "firstName", "John"))

    flushed = collect_statements(journal.flush())
    entity_ids = {s.entity_id for s in flushed}
    assert "jane" in entity_ids
    assert "john" in entity_ids
    assert len(flushed) == 5

    # After flush, should be empty
    assert collect_statements(journal.flush()) == []


def test_storage_journal_writer_context_manager(journal):
    """Test bulk writer with context manager."""
    with journal.writer() as w:
        for i in range(100):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    flushed = collect_statements(journal.flush())
    assert len(flushed) == 100


def test_storage_journal_flush_empties(journal):
    """Test that flush empties the journal."""
    with journal.writer() as w:
        for i in range(5):
            entity_id = f"entity_{i:02d}"
            w.add_statement(make_statement(entity_id, "name", f"Name {i}"))

    flushed = collect_statements(journal.flush())
    assert len(flushed) == 5

    # Should be empty after flush
    assert collect_statements(journal.flush()) == []


def test_storage_journal_statement_fields(journal):
    """Test that key statement fields are preserved."""
    stmt = Statement(
        entity_id="jane",
        prop="name",
        schema="Person",
        value="Jane Doe",
        dataset=DATASET,
        lang="en",
        origin="import",
    )
    with journal.writer() as w:
        w.add_statement(stmt)

    flushed = collect_statements(journal.flush())
    name_stmts = [s for s in flushed if s.prop == "name"]
    assert len(name_stmts) == 1
    retrieved = name_stmts[0]
    assert retrieved.entity_id == "jane"
    assert retrieved.prop == "name"
    assert retrieved.schema == "Person"
    assert retrieved.value == "Jane Doe"
    assert retrieved.dataset == DATASET
    assert retrieved.lang == "en"
    assert retrieved.origin == "import"
    assert retrieved.id is not None

    assert collect_statements(journal.flush()) == []


def test_storage_journal_flush_yields_bucket_origin(journal):
    """Test that flush yields (id, bucket, origin, canonical_id, data) tuples."""
    with journal.writer() as w:
        w.add_statement(
            Statement(
                entity_id="e1",
                prop="name",
                schema="Person",
                value="Alice",
                dataset=DATASET,
                origin="source_a",
            )
        )
        w.add_statement(
            Statement(
                entity_id="e2",
                prop="name",
                schema="Person",
                value="Bob",
                dataset=DATASET,
                origin="source_b",
            )
        )
        w.add_statement(
            Statement(
                entity_id="e3",
                prop="name",
                schema="Person",
                value="Charlie",
                dataset=DATASET,
                origin="source_a",
            )
        )

    items = list(journal.flush())
    assert len(items) == 3

    for row_id, bucket, origin, canonical_id, data, deleted_at in items:
        assert bucket == "thing"  # Person is a Thing
        assert origin in ("source_a", "source_b")
        stmt = unpack_statement(data)
        assert stmt.origin == origin


def test_storage_journal_flush_sorted_order(journal):
    """Test that flush yields statements in sorted order (bucket, origin, canonical_id)."""
    with journal.writer() as w:
        for origin in ["z_origin", "a_origin", "m_origin"]:
            for i in range(3):
                stmt = Statement(
                    entity_id=f"{origin}_{i}",
                    prop="name",
                    schema="Person",
                    value=f"Name {i}",
                    dataset=DATASET,
                    origin=origin,
                )
                w.add_statement(stmt)

    items = list(journal.flush())
    origins = [origin for _, _, origin, _, _, _ in items]
    assert origins == sorted(origins)


def test_storage_journal_rollback_on_consumer_error(request, journal):
    """Test that statements are preserved if consumer raises an error."""
    if request.node.callspec.params["journal"] == "api":
        pytest.skip("API transport buffers full response; rollback is server-side only")
    with journal.writer() as w:
        for i in range(5):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    # Try to consume but raise error
    try:
        for _id, _bucket, _origin, _canonical_id, _data, _deleted_at in journal.flush():
            raise ValueError("Simulated error")
    except ValueError:
        pass

    # Statements should still be in journal due to rollback
    flushed = collect_statements(journal.flush())
    assert len(flushed) == 5
    assert collect_statements(journal.flush()) == []


def test_storage_journal_upsert_duplicate_statements(journal):
    """Test that duplicate statements are upserted (updated, not duplicated)."""
    stmt = Statement(
        entity_id="jane",
        prop="name",
        schema="Person",
        value="Jane Doe",
        dataset=DATASET,
        origin="import",
    )

    with journal.writer() as w:
        w.add_statement(stmt)

    # Add the same statement again (same id)
    with journal.writer() as w:
        w.add_statement(stmt)

    # Add same statement with different origin - should update
    stmt.origin = "updated"
    with journal.writer() as w:
        w.add_statement(stmt)

    flushed = collect_statements(journal.flush())
    assert len(flushed) == 1
    assert flushed[0].origin == "updated"


def test_storage_journal_count(journal):
    """Test counting rows in journal."""
    assert journal.count() == 0

    with journal.writer() as w:
        for i in range(10):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    assert journal.count() == 10

    # Flush empties the journal
    list(journal.flush())
    assert journal.count() == 0


def test_storage_journal_clear(journal):
    """Test clearing all rows from journal."""
    with journal.writer() as w:
        for i in range(10):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    assert journal.count() == 10

    deleted = journal.clear()
    assert deleted == 10
    assert journal.count() == 0


def test_storage_journal_flush_concurrent_write(tmp_path):
    """Test that rows written during flush are not deleted.

    Simulates a concurrent writer inserting rows while flush() is yielding.
    The new rows must survive the flush DELETE since they were never yielded.
    Uses file-based SQLite so writer and flush use separate connections.
    """
    uri = f"sqlite:///{tmp_path / 'journal.db'}"
    journal = SqlJournalStore(dataset=DATASET, uri=uri)

    # Write initial rows
    with journal.writer() as w:
        for i in range(5):
            w.add_statement(make_statement(f"initial_{i}", "name", f"Initial {i}"))

    assert journal.count() == 5

    # Start flush, inject new rows mid-iteration, then finish
    flushed_ids = []
    injected = False
    for row_id, bucket, origin, canonical_id, data, deleted_at in journal.flush():
        flushed_ids.append(row_id)

        # After first row, inject new rows via a separate writer
        if not injected:
            with journal.writer() as w:
                for i in range(3):
                    w.add_statement(
                        make_statement(f"concurrent_{i}", "name", f"Concurrent {i}")
                    )
            injected = True

    # All 5 initial rows were yielded
    assert len(flushed_ids) == 5

    # The 3 rows written during flush must still be in the journal
    assert journal.count() == 3

    # Flush the remaining rows — should get exactly the concurrent ones
    remaining = collect_statements(journal.flush())
    assert len(remaining) == 3
    remaining_ids = {s.entity_id for s in remaining}
    assert remaining_ids == {"concurrent_0", "concurrent_1", "concurrent_2"}

    # Journal is now empty
    assert journal.count() == 0
