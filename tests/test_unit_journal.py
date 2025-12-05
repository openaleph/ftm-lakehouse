import uuid

from followthemoney.statement import Statement

from ftm_lakehouse.service.journal import (
    FlushItems,
    Journal,
    pack_statement,
    unpack_statement,
)


def make_statement(
    entity_id: str,
    prop: str,
    value: str,
    schema: str = "Person",
    dataset: str = "test",
    origin: str | None = None,
) -> Statement:
    return Statement(
        entity_id=entity_id,
        prop=prop,
        schema=schema,
        value=value,
        dataset=dataset,
        origin=origin,
    )


def unique_db_uri(tmp_path) -> str:
    """Generate a unique SQLite database URI for test isolation."""
    return f"sqlite:///{tmp_path}/journal_{uuid.uuid4().hex}.db"


def collect_statements(items: FlushItems) -> list[Statement]:
    """Collect all statements from flush items."""
    return [stmt for _, _, stmt in items]


def test_pack_unpack_statement():
    """Test that pack/unpack roundtrips correctly."""
    stmt = Statement(
        entity_id="jane",
        prop="name",
        schema="Person",
        value="Jane Doe",
        dataset="test",
        lang="en",
        origin="import",
        external=True,
    )

    packed = pack_statement(stmt)
    unpacked = unpack_statement(packed)

    assert unpacked.entity_id == stmt.entity_id
    assert unpacked.prop == stmt.prop
    assert unpacked.schema == stmt.schema
    assert unpacked.value == stmt.value
    assert unpacked.dataset == stmt.dataset
    assert unpacked.lang == stmt.lang
    assert unpacked.origin == stmt.origin
    assert unpacked.external == stmt.external
    assert unpacked.id == stmt.id


def test_pack_unpack_with_null_values():
    """Test pack/unpack with None/empty values applies defaults."""
    stmt = Statement(
        entity_id="e1",
        prop="name",
        schema="Person",
        value="Test",
        dataset="test",
    )

    packed = pack_statement(stmt)
    unpacked = unpack_statement(packed)

    assert unpacked.entity_id == "e1"
    assert unpacked.lang is None
    assert unpacked.origin == "default"  # defaults to DEFAULT_ORIGIN
    assert unpacked.external is False
    # Timestamps default to current time
    assert unpacked.first_seen is not None
    assert unpacked.last_seen is not None


def test_journal_initialize(tmp_path):
    db_uri = unique_db_uri(tmp_path)
    uri = f"file://{tmp_path}"
    journal = Journal(name="test_init", uri=uri, journal_uri=db_uri)
    assert isinstance(journal, Journal)
    assert collect_statements(journal.flush()) == []


def test_journal_put_and_flush(tmp_path):
    db_uri = unique_db_uri(tmp_path)
    dataset = f"test_basic_{uuid.uuid4().hex[:8]}"
    uri = f"file://{tmp_path}/{dataset}"
    journal = Journal(name=dataset, uri=uri, journal_uri=db_uri)

    # Add statements
    stmt1 = make_statement("jane", "name", "Jane Doe", dataset=dataset)
    journal.put(stmt1)

    stmt2 = make_statement("jane", "firstName", "Jane", dataset=dataset)
    stmt3 = make_statement("jane", "lastName", "Doe", dataset=dataset)
    journal.put(stmt2)
    journal.put(stmt3)

    stmt4 = make_statement("john", "name", "John Smith", dataset=dataset)
    stmt5 = make_statement("john", "firstName", "John", dataset=dataset)
    journal.put(stmt4)
    journal.put(stmt5)

    # Flush and verify entities exist
    flushed = collect_statements(journal.flush())
    entity_ids = {s.entity_id for s in flushed}
    assert "jane" in entity_ids
    assert "john" in entity_ids
    assert len(flushed) == 5

    # After flush, should be empty
    assert collect_statements(journal.flush()) == []


def test_journal_writer_context_manager(tmp_path):
    """Test bulk writer with context manager."""
    db_uri = unique_db_uri(tmp_path)
    dataset = f"test_writer_{uuid.uuid4().hex[:8]}"
    uri = f"file://{tmp_path}/{dataset}"
    journal = Journal(name=dataset, uri=uri, journal_uri=db_uri)

    # Use writer directly
    with journal.writer() as w:
        for i in range(100):
            w.add_statement(
                make_statement(f"e{i}", "name", f"Name {i}", dataset=dataset)
            )

    flushed = collect_statements(journal.flush())
    assert len(flushed) == 100


def test_journal_flush_empties(tmp_path):
    db_uri = unique_db_uri(tmp_path)
    dataset = f"test_flush_{uuid.uuid4().hex[:8]}"
    uri = f"file://{tmp_path}/{dataset}"
    journal = Journal(name=dataset, uri=uri, journal_uri=db_uri)

    # Add statements for multiple entities
    for i in range(5):
        entity_id = f"entity_{i:02d}"
        journal.put(make_statement(entity_id, "name", f"Name {i}", dataset=dataset))

    # Flush all
    flushed = collect_statements(journal.flush())
    assert len(flushed) == 5

    # Should be empty after flush
    assert collect_statements(journal.flush()) == []


def test_journal_statement_fields(tmp_path):
    """Test that key statement fields are preserved"""
    db_uri = unique_db_uri(tmp_path)
    dataset = f"test_fields_{uuid.uuid4().hex[:8]}"
    uri = f"file://{tmp_path}/{dataset}"
    journal = Journal(name=dataset, uri=uri, journal_uri=db_uri)

    # Create statement with core fields
    stmt = Statement(
        entity_id="jane",
        prop="name",
        schema="Person",
        value="Jane Doe",
        dataset=dataset,
        lang="en",
        origin="import",
    )
    journal.put(stmt)

    # Flush and verify core fields
    flushed = collect_statements(journal.flush())
    name_stmts = [s for s in flushed if s.prop == "name"]
    assert len(name_stmts) == 1
    retrieved = name_stmts[0]
    assert retrieved.entity_id == "jane"
    assert retrieved.prop == "name"
    assert retrieved.schema == "Person"
    assert retrieved.value == "Jane Doe"
    assert retrieved.dataset == dataset
    assert retrieved.lang == "en"
    assert retrieved.origin == "import"
    assert retrieved.id is not None

    # Should be empty after flush
    assert collect_statements(journal.flush()) == []


def test_journal_flush_yields_bucket_origin_statement(tmp_path):
    """Test that flush yields (bucket, origin, statement) tuples."""
    db_uri = unique_db_uri(tmp_path)
    dataset = f"test_origin_{uuid.uuid4().hex[:8]}"
    uri = f"file://{tmp_path}/{dataset}"
    journal = Journal(name=dataset, uri=uri, journal_uri=db_uri)

    # Add statements with different origins
    stmt1 = Statement(
        entity_id="e1",
        prop="name",
        schema="Person",
        value="Alice",
        dataset=dataset,
        origin="source_a",
    )
    stmt2 = Statement(
        entity_id="e2",
        prop="name",
        schema="Person",
        value="Bob",
        dataset=dataset,
        origin="source_b",
    )
    stmt3 = Statement(
        entity_id="e3",
        prop="name",
        schema="Person",
        value="Charlie",
        dataset=dataset,
        origin="source_a",
    )
    journal.put(stmt1)
    journal.put(stmt2)
    journal.put(stmt3)

    # Flush and verify tuples
    items = list(journal.flush())
    assert len(items) == 3

    # Each item is (bucket, origin, statement)
    for bucket, origin, stmt in items:
        assert bucket == "thing"  # Person is a Thing
        assert origin in ("source_a", "source_b")
        assert stmt.origin == origin


def test_journal_flush_sorted_order(tmp_path):
    """Test that flush yields statements in sorted order (bucket, origin, canonical_id)."""
    db_uri = unique_db_uri(tmp_path)
    dataset = f"test_sorted_{uuid.uuid4().hex[:8]}"
    uri = f"file://{tmp_path}/{dataset}"
    journal = Journal(name=dataset, uri=uri, journal_uri=db_uri)

    # Add statements with different origins (not in sorted order)
    with journal.writer() as w:
        for origin in ["z_origin", "a_origin", "m_origin"]:
            for i in range(3):
                stmt = Statement(
                    entity_id=f"{origin}_{i}",
                    prop="name",
                    schema="Person",
                    value=f"Name {i}",
                    dataset=dataset,
                    origin=origin,
                )
                w.add_statement(stmt)

    # Flush and verify order
    items = list(journal.flush())
    origins = [origin for _, origin, _ in items]

    # Should be sorted by origin
    assert origins == sorted(origins)


def test_journal_rollback_on_consumer_error(tmp_path):
    """Test that statements are preserved if consumer raises an error."""
    db_uri = unique_db_uri(tmp_path)
    dataset = f"test_rollback_{uuid.uuid4().hex[:8]}"
    uri = f"file://{tmp_path}/{dataset}"
    journal = Journal(name=dataset, uri=uri, journal_uri=db_uri)

    # Add statements
    for i in range(5):
        journal.put(make_statement(f"e{i}", "name", f"Name {i}", dataset=dataset))

    # Try to consume but raise error
    try:
        for _bucket, _origin, _stmt in journal.flush():
            raise ValueError("Simulated error")
    except ValueError:
        pass

    # Statements should still be in journal due to rollback
    # Now flush successfully
    flushed = collect_statements(journal.flush())
    assert len(flushed) == 5
    assert collect_statements(journal.flush()) == []


def test_journal_upsert_duplicate_statements(tmp_path):
    """Test that duplicate statements are upserted (updated, not duplicated)."""
    db_uri = unique_db_uri(tmp_path)
    dataset = f"test_upsert_{uuid.uuid4().hex[:8]}"
    uri = f"file://{tmp_path}/{dataset}"
    journal = Journal(name=dataset, uri=uri, journal_uri=db_uri)

    # Add a statement
    stmt = Statement(
        entity_id="jane",
        prop="name",
        schema="Person",
        value="Jane Doe",
        dataset=dataset,
        origin="import",
    )
    journal.put(stmt)

    # Add the same statement again (same id)
    journal.put(stmt)

    # Add same statement with different origin - should update
    stmt.origin = "updated"
    journal.put(stmt)

    # Flush and verify - should only have 1 statement due to upsert
    flushed = collect_statements(journal.flush())
    assert len(flushed) == 1
    assert flushed[0].origin == "updated"
