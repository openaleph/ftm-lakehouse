"""Tests for ParquetStore - Delta Lake statement parquet storage."""

from followthemoney import Statement
from sqlalchemy import select

from ftm_lakehouse.storage.parquet import ParquetStore

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


def test_storage_parquet_query_statements(tmp_path):
    """Test query_statements returns raw Statement objects instead of assembled entities."""
    store = ParquetStore(tmp_path, DATASET)

    # Write statements for two entities
    with store.writer() as w:
        w.add_statement(make_statement("jane", "name", "Jane Doe"))
        w.add_statement(make_statement("jane", "firstName", "Jane"))
        w.add_statement(make_statement("jane", "lastName", "Doe"))
        w.add_statement(make_statement("john", "name", "John Smith"))
        w.add_statement(make_statement("john", "firstName", "John"))

    # query() returns assembled entities
    entities = list(store.query())
    assert len(entities) == 2
    entity_ids = {e.id for e in entities}
    assert entity_ids == {"jane", "john"}

    # query_statements() returns raw statements
    statements = list(store.query_statements())
    assert len(statements) == 5

    # Verify they are Statement objects with expected fields
    for stmt in statements:
        assert isinstance(stmt, Statement)
        assert stmt.entity_id in ("jane", "john")
        assert stmt.dataset == DATASET

    # Check specific statements exist
    name_stmts = [s for s in statements if s.prop == "name"]
    assert len(name_stmts) == 2
    name_values = {s.value for s in name_stmts}
    assert name_values == {"Jane Doe", "John Smith"}


def test_storage_parquet_query_statements_with_filter(tmp_path):
    """Test query_statements with custom SQLAlchemy query using TABLE."""
    store = ParquetStore(tmp_path, DATASET)

    # Write statements for two entities
    with store.writer() as w:
        w.add_statement(make_statement("jane", "name", "Jane Doe"))
        w.add_statement(make_statement("jane", "firstName", "Jane"))
        w.add_statement(make_statement("jane", "lastName", "Doe"))
        w.add_statement(make_statement("john", "name", "John Smith"))
        w.add_statement(make_statement("john", "firstName", "John"))

    # Build custom query filtering by prop using TABLE
    T = ParquetStore.TABLE
    q = select(T).where(T.c.prop == "name")

    statements = list(store.query_statements(q))
    assert len(statements) == 2

    # All should be "name" statements
    for stmt in statements:
        assert stmt.prop == "name"

    values = {s.value for s in statements}
    assert values == {"Jane Doe", "John Smith"}

    # Filter by entity_id
    q = select(T).where(T.c.entity_id == "jane")
    statements = list(store.query_statements(q))
    assert len(statements) == 3
    assert all(s.entity_id == "jane" for s in statements)
