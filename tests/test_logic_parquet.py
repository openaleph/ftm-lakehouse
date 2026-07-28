"""Tests for the DuckDB merge query in ``ftm_lakehouse.logic.parquet``."""

from datetime import datetime, timedelta, timezone

import pyarrow as pa
import pytest

from ftm_lakehouse.logic.parquet import build_changed_sql, build_merge_sql
from ftm_lakehouse.model.statement import SHARDED_SCHEMA, TABLE_RAW
from tests.duck import make_duckdb


def _table(rows: list[dict]) -> pa.Table:
    cols: dict[str, list] = {f.name: [] for f in SHARDED_SCHEMA}
    for r in rows:
        for k in cols:
            # fragment uses the empty-string sentinel, never NULL
            cols[k].append(r.get(k, "") if k == "fragment" else r.get(k))
    return pa.table(cols, schema=SHARDED_SCHEMA)


@pytest.fixture
def now() -> datetime:
    return datetime(2026, 5, 12, 12, 0, 0, tzinfo=timezone.utc)


def _run(
    table: pa.Table, *, shard: str, bucket: str, origin: str, grace_cutoff: datetime
):
    con = make_duckdb()
    con.register(TABLE_RAW.name, table)
    sql = build_merge_sql(shard, bucket, origin, grace_cutoff)
    return con.execute(sql).to_arrow_table()


def test_merge_collapses_duplicates(now):
    """Two rows with the same id collapse to the row with latest last_seen."""
    early = now - timedelta(hours=1)
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": early,
                "last_seen": early,
                "deleted_at": None,
            },
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert out.num_rows == 1
    row = out.to_pylist()[0]
    # first_seen folded to the min across the id group; last_seen kept as max
    assert row["first_seen"] == early
    assert row["last_seen"] == now


def test_merge_drops_old_tombstone(now):
    """Tombstone older than the grace cutoff is dropped."""
    old = now - timedelta(days=14)
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": old,
                "last_seen": old,
                "deleted_at": old,
            }
        ]
    )
    grace_cutoff = now - timedelta(days=7)
    out = _run(
        table, shard="0", bucket="thing", origin="ingest", grace_cutoff=grace_cutoff
    )
    assert out.num_rows == 0


def test_merge_keeps_recent_tombstone(now):
    """Tombstone newer than the grace cutoff is kept."""
    recent = now - timedelta(days=1)
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": recent,
                "last_seen": recent,
                "deleted_at": recent,
            }
        ]
    )
    grace_cutoff = now - timedelta(days=7)
    out = _run(
        table, shard="0", bucket="thing", origin="ingest", grace_cutoff=grace_cutoff
    )
    assert out.num_rows == 1


def test_merge_filters_to_partition(now):
    """Rows outside the target partition are not selected."""
    table = _table(
        [
            {
                "id": "s1",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
            {
                "id": "s2",
                "entity_id": "e2",
                "shard": "1",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert out.num_rows == 1
    assert out.to_pylist()[0]["id"] == "s1"


def _row(now, **kwargs) -> dict:
    """A statement row with partition defaults, for fragment test tables."""
    defaults = {
        "shard": "0",
        "bucket": "thing",
        "origin": "ingest",
        "schema": "Company",
        "first_seen": now,
        "last_seen": now,
        "deleted_at": None,
    }
    return {**defaults, **kwargs}


def test_merge_fragment_supersession_single_value(now):
    """A later emission of the same (entity_id, prop, fragment) replaces
    the older one, even though the statement ids differ."""
    t2 = now + timedelta(hours=1)
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(
                now,
                id="h2",
                entity_id="acme",
                prop="name",
                fragment="row42",
                first_seen=t2,
                last_seen=t2,
            ),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert out.num_rows == 1
    row = out.to_pylist()[0]
    assert row["id"] == "h2"
    # first_seen folded to the group minimum, spanning superseded rows
    assert row["first_seen"] == now


def test_merge_fragment_supersession_multi_value(now):
    """Multi-valued props of the latest emission survive together because
    they share last_seen; the whole earlier emission is superseded."""
    t1, t2 = now, now + timedelta(hours=1)
    table = _table(
        [
            _row(t1, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(t1, id="h2", entity_id="acme", prop="name", fragment="row42"),
            _row(
                t1,
                id="h3",
                entity_id="acme",
                prop="name",
                fragment="row42",
                first_seen=t2,
                last_seen=t2,
            ),
            _row(
                t1,
                id="h4",
                entity_id="acme",
                prop="name",
                fragment="row42",
                first_seen=t2,
                last_seen=t2,
            ),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert sorted(r["id"] for r in out.to_pylist()) == ["h3", "h4"]


def test_merge_fragment_prop_dropped_between_emissions(now):
    """Supersession is per (entity_id, prop, fragment), not per fragment as
    a whole: a prop absent from the later emission keeps its older row."""
    t2 = now + timedelta(hours=1)
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(now, id="h2", entity_id="acme", prop="country", fragment="row42"),
            _row(
                now,
                id="h3",
                entity_id="acme",
                prop="name",
                fragment="row42",
                first_seen=t2,
                last_seen=t2,
            ),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert sorted(r["id"] for r in out.to_pylist()) == ["h2", "h3"]


def test_merge_fragment_contract_violation_partial_supersession(now):
    """Jittered last_seen within one logical emission is a producer bug:
    only the very latest row survives (degraded, not corrupt)."""
    jitter = now + timedelta(microseconds=1)
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(
                now,
                id="h2",
                entity_id="acme",
                prop="name",
                fragment="row42",
                last_seen=jitter,
            ),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert [r["id"] for r in out.to_pylist()] == ["h2"]


def test_merge_same_id_under_multiple_fragments(now):
    """The same content-addressed id under two fragments is two rows –
    distinct supersession groups."""
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row1"),
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row2"),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert sorted(r["fragment"] for r in out.to_pylist()) == ["row1", "row2"]


def test_merge_fragment_and_nonfragment_isolated(now):
    """The same content in fragment and non-fragment mode coexists – the
    branches never interact, and a later non-fragment row does not
    supersede the fragment row (or vice versa)."""
    t2 = now + timedelta(hours=1)
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(now, id="h1", entity_id="acme", prop="name", last_seen=t2),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert sorted(r["fragment"] for r in out.to_pylist()) == ["", "row42"]


def test_merge_fragment_tombstone_within_grace(now):
    """A fragment tombstone supersedes the group's live rows and is kept
    as the group's canonical row while within grace."""
    deleted = now + timedelta(hours=1)
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(
                now,
                id="h1",
                entity_id="acme",
                prop="name",
                fragment="row42",
                last_seen=deleted,
                deleted_at=deleted,
            ),
        ]
    )
    grace_cutoff = now - timedelta(days=7)
    out = _run(
        table, shard="0", bucket="thing", origin="ingest", grace_cutoff=grace_cutoff
    )
    assert out.num_rows == 1
    assert out.to_pylist()[0]["deleted_at"] == deleted


def test_merge_fragment_tombstone_past_grace_removes_group(now):
    """A fragment tombstone past grace removes the whole group: the live
    rows lost the window, the tombstone is reaped."""
    old = now - timedelta(days=14)
    deleted = now - timedelta(days=10)
    table = _table(
        [
            _row(old, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(
                old,
                id="h1",
                entity_id="acme",
                prop="name",
                fragment="row42",
                last_seen=deleted,
                deleted_at=deleted,
            ),
        ]
    )
    grace_cutoff = now - timedelta(days=7)
    out = _run(
        table, shard="0", bucket="thing", origin="ingest", grace_cutoff=grace_cutoff
    )
    assert out.num_rows == 0


def test_merge_fragment_cross_origin_isolation(now):
    """The same fragment in two origins is two independent supersession
    groups – merge only ever sees one origin per partition."""
    t2 = now + timedelta(hours=1)
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(
                now,
                id="h2",
                entity_id="acme",
                prop="name",
                fragment="row42",
                origin="other",
                first_seen=t2,
                last_seen=t2,
            ),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    # the newer row in origin "other" does not supersede origin "ingest"
    assert [r["id"] for r in out.to_pylist()] == ["h1"]


def test_merge_fragment_identical_duplicates_collapse(now):
    """Physically identical fragment rows (same id, fragment, last_seen –
    e.g. the same statements.csv imported twice) collapse to one row."""
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(now, id="h2", entity_id="acme", prop="name", fragment="row42"),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    # h1's duplicate collapses; the distinct-id tie partner h2 survives
    assert sorted(r["id"] for r in out.to_pylist()) == ["h1", "h2"]


def test_merge_idempotent(now):
    """Feeding merge output through the merge again changes nothing – the
    per-id tiebreak makes repeated optimize passes converge."""
    t2 = now + timedelta(hours=1)
    table = _table(
        [
            # identical fragment duplicates + a superseded emission
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(
                now,
                id="h2",
                entity_id="acme",
                prop="name",
                fragment="row42",
                first_seen=t2,
                last_seen=t2,
            ),
            # identical non-fragment duplicates
            _row(now, id="n1", entity_id="acme", prop="address"),
            _row(now, id="n1", entity_id="acme", prop="address"),
        ]
    )
    out1 = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    out2 = _run(out1, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert sorted(r["id"] for r in out1.to_pylist()) == ["h2", "n1"]
    assert out2.to_pylist() == out1.to_pylist()


def test_merge_tombstone_wins_same_second_tie(now):
    """A live row and its tombstone sharing one second-granular last_seen
    tie deterministically: the ``deleted_at DESC`` tiebreak keeps the
    tombstone, so the delete survives into the grace filter."""
    table = _table(
        [
            # non-fragment branch
            _row(now, id="s1", entity_id="acme", prop="name"),
            _row(now, id="s1", entity_id="acme", prop="name", deleted_at=now),
            # fragment branch
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row42"),
            _row(
                now,
                id="h1",
                entity_id="acme",
                prop="name",
                fragment="row42",
                deleted_at=now,
            ),
        ]
    )
    grace_cutoff = now - timedelta(days=7)
    out = _run(
        table, shard="0", bucket="thing", origin="ingest", grace_cutoff=grace_cutoff
    )
    rows = {(r["id"], r["fragment"]): r for r in out.to_pylist()}
    assert len(rows) == 2
    assert rows[("s1", "")]["deleted_at"] == now
    assert rows[("h1", "row42")]["deleted_at"] == now


def test_merge_output_sorted_with_fragments(now):
    """Output rows are sorted by (entity_id, fragment, prop, id)."""
    table = _table(
        [
            _row(now, id="z", entity_id="e1", prop="name", fragment="b"),
            _row(now, id="a", entity_id="e1", prop="name", fragment="a"),
            _row(now, id="m", entity_id="e1", prop="address"),
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    assert [(r["fragment"], r["id"]) for r in out.to_pylist()] == [
        ("", "m"),
        ("a", "a"),
        ("b", "z"),
    ]


def _run_changed(table: pa.Table, *, shard: str, bucket: str, since: datetime):
    con = make_duckdb()
    con.register(TABLE_RAW.name, table)
    return con.execute(build_changed_sql(shard, bucket, since)).to_arrow_table()


def test_changed_sql_deleted_entity_yields_zero_rows(now):
    """A deleted-but-unmerged entity has no canonical live rows: its
    tombstones shadow the live rows and are filtered – so the diff can
    emit a DEL without a merge having run."""
    deleted = now + timedelta(hours=1)
    table = _table(
        [
            _row(now, id="s1", entity_id="acme", prop="name"),
            _row(
                now,
                id="s1",
                entity_id="acme",
                prop="name",
                last_seen=deleted,
                deleted_at=deleted,
            ),
            # untouched second entity, outside the change window
            _row(now - timedelta(days=1), id="s2", entity_id="other", prop="name"),
        ]
    )
    out = _run_changed(table, shard="0", bucket="thing", since=now)
    assert out.num_rows == 0


def test_changed_sql_multi_origin_slice_keeps_per_origin_rows(now):
    """The changed slice spans all origins of a partition: the same id
    tombstoned in one origin must not shadow its live twin in another."""
    deleted = now + timedelta(hours=1)
    table = _table(
        [
            _row(now, id="s1", entity_id="acme", prop="name", origin="a"),
            _row(now, id="s1", entity_id="acme", prop="name", origin="b"),
            _row(
                now,
                id="s1",
                entity_id="acme",
                prop="name",
                origin="a",
                last_seen=deleted,
                deleted_at=deleted,
            ),
        ]
    )
    out = _run_changed(table, shard="0", bucket="thing", since=now)
    rows = out.to_pylist()
    assert [(r["origin"], r["id"]) for r in rows] == [("b", "s1")]


def test_changed_sql_supersession_applied(now):
    """The changed slice returns canonical rows: a superseded fragment
    emission is dropped even though both rows are physically present."""
    t2 = now + timedelta(hours=1)
    table = _table(
        [
            _row(now, id="h1", entity_id="acme", prop="name", fragment="row1"),
            _row(
                now,
                id="h2",
                entity_id="acme",
                prop="name",
                fragment="row1",
                first_seen=t2,
                last_seen=t2,
            ),
        ]
    )
    out = _run_changed(table, shard="0", bucket="thing", since=now)
    assert [r["id"] for r in out.to_pylist()] == ["h2"]


def test_merge_output_sorted(now):
    """Output rows are sorted by (entity_id, id, last_seen DESC)."""
    table = _table(
        [
            {
                "id": "z",
                "entity_id": "e2",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
            {
                "id": "a",
                "entity_id": "e1",
                "shard": "0",
                "bucket": "thing",
                "origin": "ingest",
                "schema": "Person",
                "first_seen": now,
                "last_seen": now,
                "deleted_at": None,
            },
        ]
    )
    out = _run(table, shard="0", bucket="thing", origin="ingest", grace_cutoff=now)
    rows = out.to_pylist()
    assert [r["entity_id"] for r in rows] == ["e1", "e2"]
