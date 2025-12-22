from followthemoney import Statement

from ftm_lakehouse.helpers.statements import pack_statement, unpack_statement


def test_helpers_statement():
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

    # Test pack/unpack with None/empty values applies defaults.
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
