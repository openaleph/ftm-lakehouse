"""Tests for JournalStore - SQL statement buffer for write-ahead logging."""

from followthemoney.statement import Statement

from ftm_lakehouse.helpers.statements import unpack_statement
from ftm_lakehouse.storage.journal import JournalRows, JournalStore

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
    return [unpack_statement(stmt) for _, _, _, _, stmt in items]


def test_storage_journal_initialize():
    """Test journal can be initialized and starts empty."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")
    assert collect_statements(journal.flush()) == []


def test_storage_journal_put_and_flush():
    """Test basic put and flush operations."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    # Add statements via writer
    with journal.writer() as w:
        w.add_statement(make_statement("jane", "name", "Jane Doe"))
        w.add_statement(make_statement("jane", "firstName", "Jane"))
        w.add_statement(make_statement("jane", "lastName", "Doe"))
        w.add_statement(make_statement("john", "name", "John Smith"))
        w.add_statement(make_statement("john", "firstName", "John"))

    # Flush and verify entities exist
    flushed = collect_statements(journal.flush())
    entity_ids = {s.entity_id for s in flushed}
    assert "jane" in entity_ids
    assert "john" in entity_ids
    assert len(flushed) == 5

    # After flush, should be empty
    assert collect_statements(journal.flush()) == []


def test_storage_journal_writer_context_manager():
    """Test bulk writer with context manager."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    # Use writer directly
    with journal.writer() as w:
        for i in range(100):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    flushed = collect_statements(journal.flush())
    assert len(flushed) == 100


def test_storage_journal_flush_empties():
    """Test that flush empties the journal."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    # Add statements for multiple entities
    with journal.writer() as w:
        for i in range(5):
            entity_id = f"entity_{i:02d}"
            w.add_statement(make_statement(entity_id, "name", f"Name {i}"))

    # Flush all
    flushed = collect_statements(journal.flush())
    assert len(flushed) == 5

    # Should be empty after flush
    assert collect_statements(journal.flush()) == []


def test_storage_journal_statement_fields():
    """Test that key statement fields are preserved."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    # Create statement with core fields
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

    # Flush and verify core fields
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

    # Should be empty after flush
    assert collect_statements(journal.flush()) == []


def test_storage_journal_flush_yields_bucket_origin():
    """Test that flush yields (id, bucket, origin, canonical_id, data) tuples."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    # Add statements with different origins
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

    # Flush and verify tuples
    items = list(journal.flush())
    assert len(items) == 3

    # Each item is (id, bucket, origin, canonical_id, data)
    for row_id, bucket, origin, canonical_id, data in items:
        assert bucket == "thing"  # Person is a Thing
        assert origin in ("source_a", "source_b")
        stmt = unpack_statement(data)
        assert stmt.origin == origin


def test_storage_journal_flush_sorted_order():
    """Test that flush yields statements in sorted order (bucket, origin, canonical_id)."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    # Add statements with different origins (not in sorted order)
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

    # Flush and verify order
    items = list(journal.flush())
    origins = [origin for _, _, origin, _, _ in items]

    # Should be sorted by origin
    assert origins == sorted(origins)


def test_storage_journal_rollback_on_consumer_error():
    """Test that statements are preserved if consumer raises an error."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    # Add statements
    with journal.writer() as w:
        for i in range(5):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    # Try to consume but raise error
    try:
        for _id, _bucket, _origin, _canonical_id, _data in journal.flush():
            raise ValueError("Simulated error")
    except ValueError:
        pass

    # Statements should still be in journal due to rollback
    # Now flush successfully
    flushed = collect_statements(journal.flush())
    assert len(flushed) == 5
    assert collect_statements(journal.flush()) == []


def test_storage_journal_upsert_duplicate_statements():
    """Test that duplicate statements are upserted (updated, not duplicated)."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    # Add a statement
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

    # Flush and verify - should only have 1 statement due to upsert
    flushed = collect_statements(journal.flush())
    assert len(flushed) == 1
    assert flushed[0].origin == "updated"


def test_storage_journal_count():
    """Test counting rows in journal."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    assert journal.count() == 0

    with journal.writer() as w:
        for i in range(10):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    assert journal.count() == 10

    # Flush empties the journal
    list(journal.flush())
    assert journal.count() == 0


def test_storage_journal_clear():
    """Test clearing all rows from journal."""
    journal = JournalStore(dataset=DATASET, uri="sqlite:///:memory:")

    with journal.writer() as w:
        for i in range(10):
            w.add_statement(make_statement(f"e{i}", "name", f"Name {i}"))

    assert journal.count() == 10

    # Clear returns count of deleted rows
    deleted = journal.clear()
    assert deleted == 10
    assert journal.count() == 0
