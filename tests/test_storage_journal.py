"""Tests for JournalStore implementations (SQL-SQLite, SQL-PostgreSQL, and API)."""

import os
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator

import httpx
import pyarrow as pa
import pytest
from fastapi import FastAPI
from fastapi.testclient import TestClient
from followthemoney import EntityProxy
from followthemoney.statement import Statement

from ftm_lakehouse.api.routes.journal import router
from ftm_lakehouse.core.api import get_api
from ftm_lakehouse.core.conventions.path import entity_shard
from ftm_lakehouse.lake import get_lakehouse
from ftm_lakehouse.model.statement import (
    SHARDED_SCHEMA,
    LakehouseStatement,
    statements_to_arrow,
)
from ftm_lakehouse.storage.journal import (
    ApiJournalStore,
    SqlJournalStore,
    StatementTables,
)
from ftm_lakehouse.storage.journal import get_journal as _get_journal_factory
from ftm_lakehouse.storage.journal import sql_journal
from ftm_lakehouse.storage.journal.base import BaseJournalStore

DATASET = "test"
SHARDS = 8
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


def collect_rows(tables: StatementTables) -> list[dict]:
    """Flatten flushed tables into row dicts."""
    return [row for table in tables for row in table.to_pylist()]


def owner(journal: BaseJournalStore) -> BaseJournalStore:
    """The store that holds the rows – the server's, for a remote journal.

    A local repository against a remote journal is not a supported
    combination: :meth:`BaseJournalStore.flush_batches` is ``@no_api``, so
    the api variant reads back through the store the bulk route wrote into.
    """
    if isinstance(journal, ApiJournalStore):
        return _get_journal_factory(DATASET)
    return journal


def flush(journal: BaseJournalStore) -> list[dict]:
    """Drain the journal, whichever store owns it."""
    return collect_rows(owner(journal).flush_batches())


def _make_sql_journal() -> SqlJournalStore:
    return sql_journal(DATASET, "sqlite:///:memory:")


def _make_psql_journal() -> SqlJournalStore:
    store = sql_journal(DATASET, PSQL_URI)
    store.clear()
    return store


def _make_api_journal() -> ApiJournalStore:
    app = FastAPI()
    app.include_router(router)
    # The bulk route resolves the dataset's shard count from its config via
    # ``app.state.lake`` – mirror the client-side SHARDS so both ends agree.
    lake_path = Path(tempfile.mkdtemp())
    app.state.lake = get_lakehouse(lake_path)
    app.state.lake.ensure_dataset(DATASET, shards=SHARDS)

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
    assert flush(journal) == []


def test_storage_journal_put_and_flush(journal):
    """Test basic put and flush operations."""
    with journal.writer(SHARDS) as w:
        w.add_statement(make_statement("jane", "name", "Jane Doe"))
        w.add_statement(make_statement("jane", "firstName", "Jane"))
        w.add_statement(make_statement("jane", "lastName", "Doe"))
        w.add_statement(make_statement("john", "name", "John Smith"))
        w.add_statement(make_statement("john", "firstName", "John"))

    flushed = flush(journal)
    assert {r["entity_id"] for r in flushed} == {"jane", "john"}
    assert len(flushed) == 5

    # After flush, should be empty
    assert flush(journal) == []


def test_storage_journal_writer_context_manager(journal):
    """Test bulk writer with context manager."""
    with journal.writer(SHARDS) as w:
        for i in range(100):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    assert len(flush(journal)) == 100


def test_storage_journal_flush_empties(journal):
    """Test that flush empties the journal."""
    with journal.writer(SHARDS) as w:
        for i in range(5):
            w.add_statement(make_statement(f"entity_{i:02d}", "name", f"Name {i}"))

    assert len(flush(journal)) == 5
    assert flush(journal) == []


def test_storage_journal_statement_fields(journal):
    """Test that key statement fields are preserved as typed columns."""
    stmt = Statement(
        entity_id="jane",
        prop="name",
        schema="Person",
        value="Jane Doe",
        dataset=DATASET,
        lang="en",
        origin="import",
    )
    with journal.writer(SHARDS) as w:
        w.add_statement(stmt)

    rows = [r for r in flush(journal) if r["prop"] == "name"]
    assert len(rows) == 1
    row = rows[0]
    assert row["entity_id"] == "jane"
    assert row["prop"] == "name"
    assert row["schema"] == "Person"
    assert row["value"] == "Jane Doe"
    assert row["dataset"] == DATASET
    assert row["lang"] == "en"
    assert row["origin"] == "import"
    assert row["bucket"] == "thing"
    assert row["id"] is not None
    assert row["first_seen"] is not None
    assert row["last_seen"] is not None

    assert flush(journal) == []


def test_storage_journal_flush_schema(journal):
    """Flushed batches carry the parquet statement schema, unchanged."""
    with journal.writer(SHARDS) as w:
        w.add_statement(make_statement("jane", "name", "Jane Doe"))

    batches = list(owner(journal).flush_batches())
    assert len(batches) == 1
    assert batches[0].schema.equals(SHARDED_SCHEMA)


def test_storage_journal_flush_yields_shard(journal):
    """Every row carries the shard its entity id hashes to."""
    with journal.writer(SHARDS) as w:
        w.add_statement(make_statement("e1", "name", "Alice", origin="source_a"))
        w.add_statement(make_statement("e2", "name", "Bob", origin="source_b"))
        w.add_statement(make_statement("e3", "name", "Charlie", origin="source_a"))

    rows = flush(journal)
    assert len(rows) == 3
    for row in rows:
        # 8 shards = single hex char
        assert len(row["shard"]) == 1
        assert row["shard"] == entity_shard(row["entity_id"], SHARDS)
        assert row["origin"] in ("source_a", "source_b")


def test_storage_journal_flush_sorted_order(journal):
    """Test that flush yields statements sorted by shard."""
    with journal.writer(SHARDS) as w:
        for origin in ["z_origin", "a_origin", "m_origin"]:
            for i in range(3):
                w.add_statement(
                    make_statement(f"{origin}_{i}", "name", f"Name {i}", origin=origin)
                )

    shards = [r["shard"] for r in flush(journal)]
    assert shards == sorted(shards)


def test_storage_journal_values_survive_round_trip(journal):
    """Control characters, newlines and tabs come back byte-identical.

    The rows are typed columns end to end – on the wire too, where they ride
    an Arrow IPC stream – so no delimiter can be confused with content.
    """
    value = "a\x1fb\nc\td\\e\"f'g"
    with journal.writer(SHARDS) as w:
        w.add_statement(make_statement("jane", "name", value))

    (row,) = flush(journal)
    assert row["value"] == value


def test_storage_journal_rollback_on_consumer_error(request, journal):
    """Test that statements are preserved if consumer raises an error."""
    param = request.node.callspec.params["journal"]
    if param == "api":
        pytest.skip("API transport buffers full response; rollback is server-side only")
    with journal.writer(SHARDS) as w:
        for i in range(5):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    # Try to consume but raise error
    try:
        for _ in journal.flush_batches():
            raise ValueError("Simulated error")
    except ValueError:
        pass

    # The claimed segment was never dropped, so the rows are still there
    assert len(flush(journal)) == 5
    assert flush(journal) == []


def test_storage_journal_rollback_on_consumer_abandon(request, journal):
    """If the flush generator is abandoned mid-stream (HTTP client
    disconnect → ``GeneratorExit``), the claimed segment is left in place
    for the next flush – rows are dropped only after they were taken."""
    param = request.node.callspec.params["journal"]
    if param == "api":
        pytest.skip("API rollback is server-side only; client-side abandon is a no-op")

    with journal.writer(SHARDS) as w:
        for i in range(5):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    gen = journal.flush_batches()
    assert next(gen) is not None
    # Simulate the consumer abandoning the iterator (e.g. ASGI client
    # disconnect). Python sends GeneratorExit into the generator at the
    # current yield, before the segment is dropped.
    gen.close()

    # All five rows still present – in the orphaned segment.
    assert journal._segments()
    assert journal.count() == 5
    assert len(flush(journal)) == 5
    assert journal.count() == 0
    assert journal._segments() == []


def test_storage_journal_duplicate_statements_accumulate(journal):
    """Re-emissions pile up: the journal has no key, ``merge`` collapses them.

    Different origins are *not* collapsed either – the parquet row identity
    is ``(origin, id, fragment)``, and a journal key on ``(id, fragment)``
    used to drop the earlier origin's provenance silently.
    """
    stmt = make_statement("jane", "name", "Jane Doe", origin="import")

    with journal.writer(SHARDS) as w:
        w.add_statement(stmt)

    with journal.writer(SHARDS) as w:
        w.add_statement(stmt)

    stmt.origin = "updated"
    with journal.writer(SHARDS) as w:
        w.add_statement(stmt)

    rows = flush(journal)
    assert len(rows) == 3
    assert len({r["id"] for r in rows}) == 1  # same content, same statement id
    assert sorted(r["origin"] for r in rows) == ["import", "import", "updated"]


def test_storage_journal_same_content_distinct_origins(journal):
    """Two origins of one statement survive a single batch.

    The upsert used to hit its own conflict target twice here: sqlite kept
    the last row silently, postgres raised ``CardinalityViolation``.
    """
    stmt = make_statement("jane", "name", "Jane Doe")
    with journal.writer(SHARDS) as w:
        w.add_statement(stmt, origin="source_a")
        w.add_statement(stmt, origin="source_b")

    rows = flush(journal)
    assert sorted(r["origin"] for r in rows) == ["source_a", "source_b"]
    assert len({r["id"] for r in rows}) == 1


def test_storage_journal_count(journal):
    """Test counting rows in journal."""
    assert journal.count() == 0

    with journal.writer(SHARDS) as w:
        for i in range(10):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    assert journal.count() == 10

    # Flush empties the journal
    flush(journal)
    assert journal.count() == 0


def test_storage_journal_clear(journal):
    """Test clearing all rows from journal."""
    with journal.writer(SHARDS) as w:
        for i in range(10):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    assert journal.count() == 10

    deleted = journal.clear()
    assert deleted == 10
    assert journal.count() == 0


def test_storage_journal_flush_large_single_shard_batch(request, journal):
    """A single shard past the write-batch size drains in one pass."""
    param = request.node.callspec.params["journal"]
    if param == "api":
        pytest.skip("Exercises the SQL drain path; server side is covered by sql")

    with journal.writer(1) as w:
        for i in range(10_001):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    assert journal.count() == 10_001
    rows = flush(journal)
    assert len(rows) == 10_001
    assert {r["shard"] for r in rows} == {"0"}
    assert journal.count() == 0


@pytest.fixture(params=["sqlite"] + (["psql"] if PSQL_URI else []))
def concurrent_journal(request, tmp_path):
    """Journal fixture for concurrent write tests (needs file-based or network DB)."""
    if request.param == "sqlite":
        store = sql_journal(DATASET, f"sqlite:///{tmp_path / 'journal.db'}")
        yield store
        store.dispose()
    else:
        store = sql_journal(DATASET, PSQL_URI)
        store.clear()
        yield store
        store.clear()
        store.dispose()


def test_storage_journal_flush_concurrent_write(concurrent_journal):
    """Rows written during a flush land in the fresh table, never lost.

    The flush claims the whole journal up front by rotating it away, so the
    split between "this flush" and "the next one" is exact: a writer that
    starts mid-flush – including one that outlives the rotation – writes into
    the new table and is picked up next time.
    """
    journal = concurrent_journal

    with journal.writer(SHARDS) as w:
        for i in range(5):
            w.add_statement(make_statement(f"initial_{i}", "name", f"Initial {i}"))

    assert journal.count() == 5
    initial_ids = {f"initial_{i}" for i in range(5)}
    concurrent_ids = {f"concurrent_{i}" for i in range(3)}

    # a writer that is already open when the rotation happens
    writer = journal.writer(SHARDS)
    writer.add_statement(make_statement("concurrent_0", "name", "Concurrent 0"))

    flushed: set[str] = set()
    injected = False
    for batch in journal.flush_batches():
        flushed.update(batch.column("entity_id").to_pylist())
        if not injected:
            writer.flush()  # the pre-rotation writer commits into the new table
            with journal.writer(SHARDS) as w:
                for i in range(1, 3):
                    w.add_statement(
                        make_statement(f"concurrent_{i}", "name", f"Concurrent {i}")
                    )
            injected = True
    writer.close()

    assert flushed == initial_ids
    assert journal.count() == 3

    remaining = {r["entity_id"] for r in flush(journal)}
    assert remaining == concurrent_ids
    assert journal.count() == 0


def test_storage_journal_fragment_round_trip(journal):
    """Fragment rides through the journal as its own column."""
    with journal.writer(SHARDS) as w:
        w.add_statement(make_statement("jane", "name", "Jane Doe"), fragment="row1")
        w.add_statement(make_statement("john", "name", "John Smith"))

    rows = flush(journal)
    assert {r["entity_id"]: r["fragment"] for r in rows} == {"jane": "row1", "john": ""}


def test_storage_journal_same_id_multiple_fragments(journal):
    """The same statement content under distinct fragments (and without
    one) stays distinct rows."""
    stmt = make_statement("jane", "name", "Jane Doe")
    with journal.writer(SHARDS) as w:
        w.add_statement(stmt)
        w.add_statement(stmt, fragment="row1")
        w.add_statement(stmt, fragment="row2")

    assert journal.count() == 3
    rows = flush(journal)
    assert sorted(r["fragment"] for r in rows) == ["", "row1", "row2"]
    assert len({r["id"] for r in rows}) == 1


def test_storage_journal_repeated_id_fragment_accumulates(journal):
    """Re-adding the same (id, fragment) appends – nothing upserts."""
    stmt = make_statement("jane", "name", "Jane Doe")
    with journal.writer(SHARDS) as w:
        w.add_statement(stmt, fragment="row1")
    with journal.writer(SHARDS) as w:
        w.add_statement(stmt, fragment="row1")

    assert journal.count() == 2


def test_storage_journal_writer_keeps_entities_whole(journal, monkeypatch):
    """An auto-insert never splits an entity from its checksum row.

    Inserts commit per batch, so a batch boundary inside one entity would
    land its properties without the ``BASE_ID`` row that closes it.
    """
    from ftm_lakehouse.storage.journal import base

    monkeypatch.setattr(base, "WRITE_BATCH_SIZE", 2)
    jane = EntityProxy.from_dict(
        {
            "id": "jane",
            "schema": "Person",
            "properties": {"name": ["Jane Doe"], "firstName": ["Jane"]},
        }
    )
    with journal.writer(SHARDS) as w:
        w.add_entity(jane)

    rows = flush(journal)
    props = {r["prop"] for r in rows}
    assert "id" in props  # the BASE_ID checksum row
    assert {"name", "firstName"} <= props


def test_storage_journal_api_cannot_flush():
    """A remote journal is write-only for its client.

    Draining one into a local parquet store is not a supported combination –
    the server that holds the rows owns their flush, and a repository in api
    mode delegates the whole thing to it.
    """
    store = _make_api_journal()
    # the guard is on the call, not the first iteration
    with pytest.raises(RuntimeError, match="not available in API mode"):
        store.flush_batches()
    with pytest.raises(RuntimeError, match="not available in API mode"):
        store.iterate_entity("jane")
    store.close()


def test_storage_journal_writer_add_batch(journal):
    """``add_batch`` stores packed rows as-is, but re-derives the shard.

    The wire carries whatever the client packed; a stale shard config on the
    sending side must not mis-route a partition.
    """
    stmt = LakehouseStatement.from_statement(make_statement("jane", "name", "Jane Doe"))
    stmt.shard = "99"  # a stale shard config on the sending side
    batch = statements_to_arrow([stmt, stmt], datetime.now(timezone.utc))
    with journal.writer(SHARDS) as w:
        w.add_batch(batch)

    assert journal.count() == 2
    rows = flush(journal)
    assert {r["id"] for r in rows} == {stmt.id}
    for row in rows:
        assert row["shard"] == entity_shard("jane", SHARDS)
        assert row["value"] == "Jane Doe"


def test_storage_journal_add_batch_rejects_foreign_schema(journal):
    """A batch missing statement columns is refused, not silently stored."""
    batch = pa.table({"nope": ["x"]})
    with journal.writer(SHARDS) as w:
        with pytest.raises(KeyError):
            w.add_batch(batch)


def test_storage_journal_add_batch_rejects_null_required_column(journal):
    """A null in a required column is refused at the boundary.

    The journal has no key, but it still has the schema: a row that could
    not be read back – or that would blow up a partition value on write –
    must not reach storage.
    """
    stmt = LakehouseStatement.from_statement(make_statement("jane", "name", "Jane"))
    batch = statements_to_arrow([stmt], datetime.now(timezone.utc))
    holed = batch.set_column(
        batch.schema.get_field_index("origin"),
        pa.field("origin", pa.string()),
        pa.array([None], pa.string()),
    )
    with journal.writer(SHARDS) as w:
        with pytest.raises(ValueError):
            w.add_batch(holed)
    assert journal.count() == 0


def test_storage_journal_iterate_entity(journal):
    """``iterate_entity`` yields the live statements of one entity, across
    segments, and is not implemented on the api journal."""
    with journal.writer(SHARDS) as w:
        w.add_statement(make_statement("jane", "name", "Jane Doe"))
        w.add_statement(make_statement("john", "name", "John Smith"))
        w.add_statement(
            make_statement("jane", "firstName", "Jane"),
            datetime.now(timezone.utc),  # tombstone
        )

    if isinstance(journal, ApiJournalStore):
        with pytest.raises(RuntimeError, match="not available in API mode"):
            journal.iterate_entity("jane")
        return

    values = {s.value for s in journal.iterate_entity("jane")}
    assert values == {"Jane Doe"}  # live rows of that entity only

    # an orphaned segment stays visible until it is drained
    gen = journal.flush_batches()
    next(gen)
    gen.close()
    assert journal._segments()
    assert {s.value for s in journal.iterate_entity("jane")} == {"Jane Doe"}


def test_storage_journal_flush_survives_a_failed_write(journal, request):
    """A failed downstream write must not take the flush window with it.

    The segment is dropped only once the consumer comes back for the next
    batch, so a consumer that raises while writing leaves every row it has
    not written yet in place.
    """
    param = request.node.callspec.params["journal"]
    if param == "api":
        pytest.skip("Server-side drain; the client cannot fail mid-stream here")

    with journal.writer(SHARDS) as w:
        for i in range(5):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    with pytest.raises(RuntimeError):
        for _ in journal.flush_batches():
            raise RuntimeError("downstream write failed")

    assert journal.count() == 5
    assert len(flush(journal)) == 5
    assert journal.count() == 0


def test_storage_journal_concurrent_flushes_drain_once(concurrent_journal):
    """Two flushes racing on one dataset: each row is drained exactly once.

    Rotation alone does not serialize them – the second flush finds the live
    table already empty and would drain the first one's segment, duplicating
    every row and then failing on the double drop. The flush lock is what
    makes the loser a no-op.
    """
    store = concurrent_journal
    with store.writer(SHARDS) as w:
        for i in range(3):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    first = store.flush_batches()
    rows = next(first).num_rows  # rotates and starts draining

    assert collect_rows(store.flush_batches()) == []  # locked out

    rows += sum(b.num_rows for b in first)
    assert rows == 3
    assert store.count() == 0
    assert store._segments() == []


@pytest.mark.skipif(not PSQL_URI, reason="needs PYTEST_POSTGRESQL_URI")
def test_storage_journal_psql_append_only():
    """The postgres journal is a keyless, index-free heap with no dead tuples."""
    store = _make_psql_journal()
    table = store.table.name

    def scalar(sql: str) -> int:
        with store.engine.connect() as conn:
            return conn.exec_driver_sql(sql).scalar() or 0

    assert scalar(f"SELECT count(*) FROM pg_indexes WHERE tablename = '{table}'") == 0
    assert (
        scalar(
            "SELECT count(*) FROM pg_constraint "
            f"WHERE conrelid = '{table}'::regclass AND contype IN ('p', 'u')"
        )
        == 0
    )

    def dead_tuples() -> int:
        # summed in python: a LIKE pattern would need psycopg2's `%%` escape
        with store.engine.connect() as conn:
            rows = conn.exec_driver_sql(
                "SELECT relname, n_dead_tup FROM pg_stat_user_tables"
            ).fetchall()
        return sum(n for name, n in rows if name.startswith(table))

    before = dead_tuples()

    with store.writer(SHARDS) as w:
        for i in range(100):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))
    assert len(collect_rows(store.flush_batches())) == 100

    # no UPDATE and no DELETE anywhere in the write or claim path
    assert dead_tuples() == before
    store.clear()
    store.dispose()
