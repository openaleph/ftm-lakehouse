"""Tests for JournalStore implementations (SQL-SQLite, SQL-PostgreSQL, and API)."""

import os
from typing import Generator

import httpx
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from followthemoney.statement import Statement
from sqlalchemy import insert

from ftm_lakehouse.api.routes.journal import router
from ftm_lakehouse.core.api import get_api
from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.exceptions import MalformedStatementError
from ftm_lakehouse.helpers.statements import (
    UNIT_SEP,
    UNPACK_MIN_FIELDS,
    pack_statement,
    unpack_statement,
)
from ftm_lakehouse.storage.journal import ApiJournalStore, JournalRows, SqlJournalStore
from ftm_lakehouse.storage.journal import get_journal as _get_journal_factory
from ftm_lakehouse.storage.journal.base import BaseJournalStore

DATASET = "test"
PSQL_URI = os.environ.get("PYTEST_POSTGRESQL_URI")


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
    return [unpack_statement(row.data) for row in items]


def _make_sql_journal() -> SqlJournalStore:
    return SqlJournalStore(dataset=DATASET, uri="sqlite:///:memory:")


def _make_psql_journal() -> SqlJournalStore:
    store = SqlJournalStore(dataset=DATASET, uri=PSQL_URI)
    store.clear()
    return store


def _make_api_journal() -> ApiJournalStore:
    app = FastAPI()
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

    store = ApiJournalStore(dataset=DATASET, uri="http://testserver")
    store._api.client = client
    return store


def _journal_params():
    params = ["sql", "api"]
    if PSQL_URI:
        params.append("psql")
    return params


@pytest.fixture(params=_journal_params())
def journal(request) -> Generator[BaseJournalStore, None, None]:
    if request.param == "sql":
        yield _make_sql_journal()
    elif request.param == "psql":
        store = _make_psql_journal()
        yield store
        store.clear()
        store.dispose()
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


def test_storage_journal_flush_yields_shard(journal):
    """Test that flush yields (id, shard, data, deleted_at) tuples."""
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

    for row in items:
        # default shards = 8 (single hex char)
        assert len(row.shard) == 1
        stmt = unpack_statement(row.data)
        assert row.shard == entity_shard(stmt.entity_id, 8)
        assert stmt.origin in ("source_a", "source_b")


def test_storage_journal_flush_sorted_order(journal):
    """Test that flush yields statements sorted by shard."""
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
    shards = [row.shard for row in items]
    assert shards == sorted(shards)


def test_storage_journal_rollback_on_consumer_error(request, journal):
    """Test that statements are preserved if consumer raises an error."""
    param = request.node.callspec.params["journal"]
    if param == "api":
        pytest.skip("API transport buffers full response; rollback is server-side only")
    with journal.writer() as w:
        for i in range(5):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    # Try to consume but raise error
    try:
        for _ in journal.flush():
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


@pytest.fixture(params=["sqlite"] + (["psql"] if PSQL_URI else []))
def concurrent_journal(request, tmp_path):
    """Journal fixture for concurrent write tests (needs file-based or network DB)."""
    if request.param == "sqlite":
        uri = f"sqlite:///{tmp_path / 'journal.db'}"
        store = SqlJournalStore(dataset=DATASET, uri=uri)
        yield store
        store.dispose()
    else:
        store = SqlJournalStore(dataset=DATASET, uri=PSQL_URI)
        store.clear()
        yield store
        store.clear()
        store.dispose()


def test_storage_journal_flush_concurrent_write(concurrent_journal):
    """Test that concurrent writes during flush are never silently lost.

    Per-shard flush may pick up rows inserted into not-yet-processed shards
    during the same call, so the exact split between "this flush" and "next
    flush" is non-deterministic. The contract is weaker: every row inserted
    is eventually yielded exactly once, never deleted without being yielded
    first, and the journal ends up empty after two flushes.
    """
    journal = concurrent_journal

    with journal.writer() as w:
        for i in range(5):
            w.add_statement(make_statement(f"initial_{i}", "name", f"Initial {i}"))

    assert journal.count() == 5
    initial_ids = {f"initial_{i}" for i in range(5)}
    concurrent_ids = {f"concurrent_{i}" for i in range(3)}

    flushed_entity_ids: set[str] = set()
    injected = False
    for row in journal.flush():
        flushed_entity_ids.add(unpack_statement(row.data).entity_id)

        # After first row, inject new rows via a separate writer
        if not injected:
            with journal.writer() as w:
                for i in range(3):
                    w.add_statement(
                        make_statement(f"concurrent_{i}", "name", f"Concurrent {i}")
                    )
            injected = True

    # All initial rows must be in this flush; concurrent rows may or may not be.
    assert initial_ids <= flushed_entity_ids
    assert flushed_entity_ids <= initial_ids | concurrent_ids
    assert journal.count() == 5 + 3 - len(flushed_entity_ids)

    remaining = collect_statements(journal.flush())
    remaining_ids = {s.entity_id for s in remaining}

    # The union of both flushes covers every inserted row exactly once.
    assert flushed_entity_ids | remaining_ids == initial_ids | concurrent_ids
    assert flushed_entity_ids.isdisjoint(remaining_ids)
    assert journal.count() == 0


# ---------------------------------------------------------------------------
# Malformed-statement robustness
#
# ``unpack_statement`` raises :class:`MalformedStatementError` on a too-short
# packed payload, and ``BaseJournalStore.flush_statements`` catches+logs+skips
# so one corrupt row can't abort a whole flush.
# ---------------------------------------------------------------------------


def test_unpack_rejects_short_payload() -> None:
    truncated = UNIT_SEP.join(["a"] * (UNPACK_MIN_FIELDS - 1))
    with pytest.raises(MalformedStatementError):
        unpack_statement(truncated)


def test_unpack_accepts_canonical_pack_output() -> None:
    stmt = make_statement("jane", "name", "Jane Doe")
    out = unpack_statement(pack_statement(stmt))
    assert out.entity_id == "jane"
    assert out.value == "Jane Doe"


def test_unpack_tolerates_extra_trailing_fields() -> None:
    """``pack_statement`` emits 14 fields (trailing ``prop_type``);
    ``unpack_statement`` only reads the first 13. Extra trailing fields
    must not trip the validator."""
    canonical = pack_statement(make_statement("x", "name", "v"))
    parts = canonical.split(UNIT_SEP)
    assert len(parts) >= UNPACK_MIN_FIELDS
    unpack_statement(canonical)


def test_storage_journal_flush_skips_malformed_rows(request, journal):
    """A truncated ``data`` payload in the journal doesn't crash flush.

    The malformed row is logged and skipped; good rows on either side are
    yielded normally. Direct row-injection requires SQL access so this is
    SQL-only – the API journal goes through the wire format and can't
    produce a malformed row from the client side."""
    param = request.node.callspec.params["journal"]
    if param == "api":
        pytest.skip("Malformed-row injection requires direct SQL access")

    with journal.writer() as w:
        w.add_statement(make_statement("good_1", "name", "Good One"))
        w.add_statement(make_statement("good_2", "name", "Good Two"))

    # Inject a bad row directly into the underlying SQL table.
    with journal.engine.begin() as conn:
        conn.execute(
            insert(journal.table).values(
                id="bad-1",
                shard="0",
                data=UNIT_SEP.join("abc"),  # < UNPACK_MIN_FIELDS after split
                deleted_at=None,
            )
        )

    assert journal.count() == 3

    flushed = list(journal.flush_statements())

    # Two good rows survive; the malformed row was skipped.
    assert len(flushed) == 2
    assert {row.stmt.entity_id for row in flushed} == {"good_1", "good_2"}
    # Flush remains destructive: nothing left in the journal.
    assert journal.count() == 0
