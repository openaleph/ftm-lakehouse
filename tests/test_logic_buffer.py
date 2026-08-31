"""EntityBuffer provenance and timestamp semantics + CLI extractor helpers.

The entity-level write path (``add_entity``) must keep per-statement origin
of read-back ``StatementEntity`` objects (delegating resolution to
``add_statement``: override > statement > buffer default) and pin one
``last_seen`` per emission so heterogeneous read-back timestamps cannot
split an emission across supersession ties. Its ``_buffer_size`` must track
the buffered rows exactly – re-emissions collapse in the dict, and
everything that asks the buffer how full it is reads that counter.
"""

from datetime import datetime, timezone

from followthemoney import StatementEntity, ValueEntity
from ftmq.store.lake import LakeStatement
from ftmq.util import make_dataset, make_entity

from ftm_lakehouse.cli.io import _extract_fragment, _extract_origin, _extract_role
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.model.statement import LakehouseStatement
from tests.shared import JANE

DATASET = "test"
T1 = "2026-01-01T12:00:00"
T2 = "2026-03-01T12:00:00"


def _lake_stmt(prop: str, value: str, last_seen: str, origin: str) -> LakeStatement:
    return LakeStatement(
        entity_id="acme",
        prop=prop,
        schema="Company",
        value=value,
        dataset=DATASET,
        last_seen=last_seen,
        origin=origin,
    )


def _read_back_entity() -> StatementEntity:
    """A StatementEntity as read back from a store: per-statement origin
    and heterogeneous last_seen."""
    return StatementEntity.from_statements(
        make_dataset(DATASET),
        [
            _lake_stmt("name", "Acme Inc", T1, "orig_a"),
            _lake_stmt("country", "de", T2, "orig_b"),
        ],
    )


def _buffered(buffer: EntityBuffer) -> list[LakeStatement]:
    return list(buffer._buffer.values())


def test_add_entity_keeps_statement_origin():
    """Read-back statements keep their own origin over the buffer default."""
    buffer = EntityBuffer(DATASET, origin="importer")
    buffer.add_entity(_read_back_entity())
    origins = {s.prop: s.origin for s in _buffered(buffer)}
    assert origins["name"] == "orig_a"
    assert origins["country"] == "orig_b"
    # the synthesized id statement carries no origin -> buffer default
    assert origins["id"] == "importer"


def test_add_statement_keeps_distinct_origins():
    """The same statement content under two origins stays two buffered rows –
    matching the store's per-origin row identity ``(origin, id, fragment)``;
    collapsing here would silently drop provenance in one batch."""
    buffer = EntityBuffer(DATASET)
    buffer.add_statement(_lake_stmt("name", "Acme Inc", T1, "orig_a"))
    buffer.add_statement(_lake_stmt("name", "Acme Inc", T1, "orig_b"))
    buffer.add_statement(_lake_stmt("name", "Acme Inc", T1, "orig_b"))  # dedupes
    stmts = _buffered(buffer)
    assert len(stmts) == 2
    assert {s.origin for s in stmts} == {"orig_a", "orig_b"}
    assert len({s.id for s in stmts}) == 1  # same content-hashed id


def test_add_statement_size_counts_rows_not_calls():
    """A re-emission overwrites its row – the size must not count it.

    Counting calls left the buffer claiming rows it no longer held once
    drained, so a writer kept open across flushes flushed again on an empty
    buffer.
    """
    buffer = EntityBuffer(DATASET)
    stmt = _lake_stmt("name", "Acme Inc", T1, "orig_a")
    buffer.add_statement(stmt)
    buffer.add_statement(stmt)
    assert len(buffer) == 1

    list(buffer.flush_buffer())
    assert len(buffer) == 0
    assert bool(buffer) is False


def test_add_entity_reemission_keeps_size_exact():
    """Re-emitting an entity collapses in the dict – and in the size.

    ``_buffer_size == len(_buffer)`` is the invariant every capacity check
    and flush trigger depends on.
    """
    buffer = EntityBuffer(DATASET)
    for _ in range(3):
        buffer.add_entity(_read_back_entity())
        assert len(buffer) == len(_buffered(buffer))
    # name, country and the synthesized id statement – once each
    assert len(buffer) == 3


def test_add_entity_origin_override_wins():
    """An explicit origin argument overrides per-statement provenance."""
    buffer = EntityBuffer(DATASET, origin="importer")
    buffer.add_entity(_read_back_entity(), origin="forced")
    assert {s.origin for s in _buffered(buffer)} == {"forced"}


def test_add_entity_default_origin_for_fresh_entities():
    """Fresh FtM entities (no statement origin) get the buffer default."""
    buffer = EntityBuffer(DATASET, origin="importer")
    buffer.add_entity(make_entity(JANE))
    assert {s.origin for s in _buffered(buffer)} == {"importer"}


def test_add_entity_pins_uniform_last_seen_for_fragments():
    """A fragment emission = one last_seen: heterogeneous read-back
    timestamps are pinned to the entity's own (max) last_seen so the
    emission can never split across supersession ties."""
    buffer = EntityBuffer(DATASET)
    buffer.add_entity(_read_back_entity(), fragment="row1")
    last_seens = {s.last_seen for s in _buffered(buffer)}
    assert last_seens == {T2}


def test_add_entity_keeps_statement_last_seen_without_fragment():
    """Non-fragment emissions keep per-statement last_seen – faithful
    provenance on store round-trips; only unset values fall back."""
    buffer = EntityBuffer(DATASET)
    buffer.add_entity(_read_back_entity())
    last_seens = {s.prop: s.last_seen for s in _buffered(buffer)}
    assert last_seens["name"] == T1
    assert last_seens["country"] == T2
    # the synthesized id statement has no last_seen -> pinned fallback
    assert last_seens["id"] == T2


def test_add_entity_buffer_last_seen_default():
    """An entity without timestamps of its own is pinned to the buffer's
    ``last_seen`` default (the CLI ``--last-seen`` option)."""
    default = datetime(2026, 2, 2, 12, 0, 0, tzinfo=timezone.utc)
    buffer = EntityBuffer(DATASET, last_seen=default)
    buffer.add_entity(make_entity(JANE))
    assert {s.last_seen for s in _buffered(buffer)} == {default.isoformat()}


def test_add_entity_pins_now_without_any_timestamp():
    """No entity timestamp, no buffer default: one shared ``now`` per
    emission - never per-row jitter."""
    buffer = EntityBuffer(DATASET)
    buffer.add_entity(make_entity(JANE))
    last_seens = {s.last_seen for s in _buffered(buffer)}
    assert len(last_seens) == 1
    assert None not in last_seens


def test_extract_origin_statement_entity_no_crash():
    """StatementEntity has no ``context`` slot - the extractor returns None
    (per-statement origins apply) instead of raising AttributeError."""
    e = _read_back_entity()
    assert _extract_origin(e) is None
    assert _extract_fragment(e) is None


def test_extract_origin_value_entity_lists():
    """Aggregated entity JSON carries origin as a list - a single element
    counts, multiple are ambiguous (buffer default applies)."""
    single = ValueEntity.from_dict(
        {
            "id": "x",
            "schema": "Person",
            "properties": {"name": ["X"]},
            "origin": ["crawl"],
            "datasets": [DATASET],
        }
    )
    assert _extract_origin(single) == "crawl"
    multi = ValueEntity.from_dict(
        {
            "id": "x",
            "schema": "Person",
            "properties": {"name": ["X"]},
            "origin": ["a", "b"],
            "datasets": [DATASET],
        }
    )
    assert _extract_origin(multi) is None
    plain = ValueEntity.from_dict(
        {
            "id": "x",
            "schema": "Person",
            "properties": {"name": ["X"]},
            "origin": "crawl",
            "datasets": [DATASET],
        }
    )
    assert _extract_origin(plain) == "crawl"


def test_add_statement_keeps_distinct_roles():
    """The same content asserted by two roles stays two buffered rows.

    ``role`` is the fourth dimension of the store's row identity
    ``(origin, id, fragment, role)``; collapsing here would drop the
    provenance the column exists to record.
    """
    buffer = EntityBuffer(DATASET)
    stmt = _lake_stmt("name", "Acme Inc", T1, "orig_a")
    buffer.add_statement(stmt, role="user:42")
    buffer.add_statement(stmt, role="user:7")
    buffer.add_statement(stmt, role="user:7")  # dedupes
    buffer.add_statement(stmt)  # no role - distinct again
    stmts = _buffered(buffer)
    assert len(stmts) == 3
    assert {s.role for s in stmts} == {"user:42", "user:7", None}
    assert len({s.id for s in stmts}) == 1  # same content-hashed id


def test_add_statement_role_resolution():
    """Explicit role > the statement's own > the buffer default."""
    buffer = EntityBuffer(DATASET, role="default_role")
    plain = _lake_stmt("name", "Acme Inc", T1, "orig_a")
    assert buffer.add_statement(plain) is not None
    assert _buffered(buffer)[-1].role == "default_role"

    carried = LakehouseStatement.from_statement(plain)
    carried.role = "carried"
    buffer.add_statement(carried)
    assert _buffered(buffer)[-1].role == "carried"

    buffer.add_statement(carried, role="forced")
    assert _buffered(buffer)[-1].role == "forced"


def test_add_entity_role_applies_to_whole_emission():
    """``add_entity`` stamps one role across the emission, BASE_ID stub
    included – the stub is a statement like any other and would otherwise
    land in a different merge group than the props it checksums."""
    buffer = EntityBuffer(DATASET, origin="importer")
    buffer.add_entity(make_entity(JANE), role="user:42")
    stmts = _buffered(buffer)
    assert {s.role for s in stmts} == {"user:42"}
    assert "id" in {s.prop for s in stmts}  # the BASE_ID stub is in there


def test_extract_role_from_entity_context():
    """The CLI reads a role back off an exported entity's context, the way
    it reads `origin` – what makes an entities.ftm.json round-trip keep it."""
    data = {
        "id": "x",
        "schema": "Person",
        "properties": {"name": ["X"]},
        "datasets": [DATASET],
    }
    assert _extract_role(ValueEntity.from_dict({**data, "role": ["user:42"]})) == (
        "user:42"
    )
    # multiple roles are ambiguous, like multiple origins - buffer default wins
    assert _extract_role(ValueEntity.from_dict({**data, "role": ["a", "b"]})) is None
    assert _extract_role(ValueEntity.from_dict(data)) is None
    # StatementEntity has no context slot; its statements carry their own
    assert _extract_role(_read_back_entity()) is None
