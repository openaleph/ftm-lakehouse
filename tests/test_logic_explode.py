"""Unit tests for the unsafe bulk-import explode logic.

The load-bearing property is *parity*: for the same payload, the unsafe path
must produce byte-identical rows to the safe path (EntityProxy →
namespace.apply → StatementEntity → EntityBuffer → pack_statement), so both
import paths collapse to the same physical rows on merge.
"""

from datetime import datetime, timezone

import orjson
import pytest
from followthemoney import Statement
from followthemoney.exc import InvalidData
from ftmq.io import smart_read_proxies
from ftmq.store.lake import LakeStatement, pack_statement
from rigour.time import iso_datetime

from ftm_lakehouse.cli.io import _extract_fragment, _extract_origin
from ftm_lakehouse.logic.entities.buffer import EntityBuffer
from ftm_lakehouse.logic.entities.explode import (
    RowBuffer,
    explode_unsafe,
    statement_row_unsafe,
    strip_namespace,
)
from ftm_lakehouse.model.statement import JOURNAL_SCHEMA

NOW = datetime(2026, 7, 31, 12, 0, 0, 123456, tzinfo=timezone.utc)
PIN = datetime(2026, 1, 1, 0, 0, 0, tzinfo=timezone.utc)

PAYLOADS = [
    {
        # signed ids, per-entity origin, deterministic timestamps
        "id": "jane.f00dbabe",
        "schema": "Person",
        "properties": {"name": ["Jane Doe", "Jane Doe"], "proof": ["doc-1.beef"]},
        "last_change": "2024-01-01T00:00:00",
        "origin": ["crawl"],
    },
    {
        # payload with its own dataset (statement ids hash under it) + fragment
        "id": "acme",
        "schema": "Company",
        "properties": {"name": ["ACME Inc"], "jurisdiction": ["de"]},
        "datasets": ["src_ds"],
        "last_change": "2024-02-02T12:30:45",
        "fragment": ["f1"],
    },
    {
        # bare minimum – timestamps fall back to the pinned default / now
        "id": "bob",
        "schema": "Person",
        "properties": {"name": ["Bob"]},
    },
]


def _safe_rows(tmp_path, datas) -> list[dict]:
    """Rows the safe CLI path would hand to the parquet writer."""
    fp = tmp_path / "in.ftm.json"
    with open(fp, "wb") as fh:
        for data in datas:
            fh.write(orjson.dumps(data, option=orjson.OPT_APPEND_NEWLINE))
    buffer = EntityBuffer("test", "bulk", last_seen=PIN)
    for proxy in smart_read_proxies(str(fp)):
        buffer.add_entity(
            proxy, origin=_extract_origin(proxy), fragment=_extract_fragment(proxy)
        )
    rows = []
    for stmt in buffer.flush_buffer():
        data = pack_statement(stmt)
        data["first_seen"] = data.get("first_seen") or NOW
        data["deleted_at"] = stmt.deleted_at
        data["last_seen"] = stmt.deleted_at or data.get("last_seen") or NOW
        data.pop("canonical_id", None)
        rows.append(data)
    return rows


def test_explode_parity_with_safe_path(tmp_path):
    pinned = iso_datetime(PIN.isoformat())
    safe = {(r["id"], r["fragment"]): r for r in _safe_rows(tmp_path, PAYLOADS)}
    unsafe = {
        (r["id"], r["fragment"]): r
        for data in PAYLOADS
        for r in explode_unsafe(data, "test", now=NOW, origin="bulk", last_seen=pinned)
    }
    assert set(safe) == set(unsafe)
    for key, safe_row in safe.items():
        assert unsafe[key] == safe_row, key


def test_explode_ids_independent_of_payload_datasets():
    """Two payloads differing only in ``datasets`` context land identically –
    statement ids (and the BASE checksum) hash under the target dataset."""
    plain = {
        "id": "x",
        "schema": "Person",
        "properties": {"name": ["X"]},
        "last_change": "2024-01-01T00:00:00",
    }
    foreign = {**plain, "datasets": ["somewhere_else"]}
    rows_plain = list(explode_unsafe(plain, "test", now=NOW, origin="bulk"))
    rows_foreign = list(explode_unsafe(foreign, "test", now=NOW, origin="bulk"))
    assert rows_plain == rows_foreign
    assert rows_plain[0]["id"] == Statement.make_key("test", "x", "name", "X", False)
    assert rows_plain[0]["id"] == rows_foreign[0]["id"]


def test_strip_namespace():
    assert strip_namespace("jane") == "jane"
    assert strip_namespace("jane.f00d") == "jane"
    # Namespace.parse semantics: only the last dot-segment is a signature
    assert strip_namespace("a.b.c") == "a.b"
    # the registry clean runs first - ids the safe path drops yield None
    assert strip_namespace("jane doe") is None


def test_explode_drops_unclean_ids():
    """Ids failing the registry clean vanish exactly like in the safe path."""
    junk = {"id": "jane doe", "schema": "Person", "properties": {"name": ["x"]}}
    assert not list(explode_unsafe(junk, "test", now=NOW, origin="bulk"))
    bad_ref = {
        "id": "jane",
        "schema": "Person",
        "properties": {"name": ["x"], "proof": ["bad ref"]},
    }
    rows = list(explode_unsafe(bad_ref, "test", now=NOW, origin="bulk"))
    assert [r["prop"] for r in rows] == ["name", "id"]  # unclean ref dropped


def test_explode_unknown_schema_raises():
    with pytest.raises(InvalidData):
        list(
            explode_unsafe(
                {"id": "x", "schema": "Nope", "properties": {}},
                "test",
                now=NOW,
                origin="bulk",
            )
        )


def test_explode_skips_unknown_props_and_incomplete_payloads():
    rows = list(
        explode_unsafe(
            {"id": "x", "schema": "Person", "properties": {"nope": ["y"]}},
            "test",
            now=NOW,
            origin="bulk",
        )
    )
    assert [r["prop"] for r in rows] == ["id"]  # only the BASE checksum row
    for data in ({}, {"id": "x"}, {"schema": "Person"}):
        assert not list(explode_unsafe(data, "test", now=NOW, origin="bulk"))


def test_explode_override_origin():
    data = {**PAYLOADS[0], "origin": ["crawl"]}
    rows = list(
        explode_unsafe(data, "test", now=NOW, origin="forced", override_origin=False)
    )
    assert {r["origin"] for r in rows} == {"crawl"}
    rows = list(
        explode_unsafe(data, "test", now=NOW, origin="forced", override_origin=True)
    )
    assert {r["origin"] for r in rows} == {"forced"}


def test_statement_row_unsafe_fields():
    row = {
        "entity_id": "e1",
        "schema": "Person",
        "prop": "birthDate",
        "value": "1980-01-01",
        "dataset": "src",
        "id": "carried-over",
        "lang": "de",
        "external": "true",
        "first_seen": "2024-01-01T00:00:00",
        "fragment": "f1",
    }
    packed = statement_row_unsafe(row, "dst", now=NOW, origin="bulk")
    assert packed is not None
    assert packed["dataset"] == "dst"
    assert packed["lang"] is None  # date is a non-linguistic type
    assert packed["external"] is True
    assert packed["fragment"] == "f1"
    assert packed["origin"] == "bulk"
    # the id is always re-keyed under the target dataset; the row's own id
    # and source dataset are ignored
    assert packed["id"] == Statement.make_key(
        "dst", "e1", "birthDate", "1980-01-01", True
    )
    # last_seen falls back to first_seen
    assert packed["last_seen"] == packed["first_seen"]
    assert packed["first_seen"] == iso_datetime("2024-01-01T00:00:00")

    assert (
        statement_row_unsafe({"schema": "Person"}, "dst", now=NOW, origin="b") is None
    )


def test_statement_row_unsafe_parity_with_safe_path():
    """A CSV row through the unsafe path matches the safe LakeStatement path."""
    row = {
        "entity_id": "jane",
        "schema": "Person",
        "prop": "name",
        "value": "Jane Doe",
        "dataset": "src",
        "origin": "test",
        "external": "false",
        "first_seen": "2024-01-01T00:00:00",
        "last_seen": "2024-06-01T00:00:00",
        "fragment": "",
        "id": "given-id",
    }
    buffer = EntityBuffer("dst", "bulk")
    buffer.add_statement(
        LakeStatement(
            id=row["id"],
            entity_id=row["entity_id"],
            prop=row["prop"],
            schema=row["schema"],
            value=row["value"],
            dataset=row["dataset"],
            external=False,
            first_seen=row["first_seen"],
            last_seen=row["last_seen"],
            origin=row["origin"],
            fragment=row["fragment"],
        )
    )
    (safe_stmt,) = list(buffer.flush_buffer())
    safe = pack_statement(safe_stmt)
    safe["first_seen"] = safe.get("first_seen") or NOW
    safe["deleted_at"] = safe_stmt.deleted_at
    safe["last_seen"] = safe_stmt.deleted_at or safe.get("last_seen") or NOW
    safe.pop("canonical_id", None)

    unsafe = statement_row_unsafe(row, "dst", now=NOW, origin="bulk")
    assert unsafe == safe
    # both paths re-key the carried-over id under the target dataset
    assert safe["id"] != "given-id"
    assert safe["id"] == Statement.make_key("dst", "jane", "name", "Jane Doe", False)


def test_row_buffer_dedupes_and_sorts():
    buffer = RowBuffer()
    buffer.add(None)
    assert not buffer
    buffer.add({"id": "a", "fragment": "", "origin": "x"})
    buffer.add({"id": "a", "fragment": "", "origin": "x", "value": "v2"})  # wins
    buffer.add({"id": "a", "fragment": "f1", "origin": "x"})  # distinct fragment
    buffer.add(
        {"id": "a", "fragment": "", "origin": "y"}
    )  # distinct origin survives - the store keys rows per origin
    assert len(buffer) == 3

    table = buffer.flush()
    assert table.schema.equals(JOURNAL_SCHEMA)
    rows = table.to_pylist()
    assert {r["value"] for r in rows if r["origin"] == "x"} == {None, "v2"}
    assert not buffer and len(buffer) == 0
