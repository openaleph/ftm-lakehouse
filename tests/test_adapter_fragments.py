"""Tests for the ftmq.store.fragments-compatible adapter."""

from datetime import datetime, timedelta, timezone

import pytest
from followthemoney import EntityProxy

from ftm_lakehouse.adapters.fragments import (
    DEFAULT_FRAGMENT,
    LakehouseBulkLoader,
    LakehouseFragments,
    LakehouseStore,
    get_fragments,
    get_store,
)
from ftm_lakehouse.storage.journal import get_journal

DATASET = "adapter_test"


@pytest.fixture(autouse=True)
def _isolate_journal():
    get_journal.cache_clear()
    yield
    get_journal.cache_clear()


def _entity(id_: str, name: str = "Alice", schema: str = "Person") -> EntityProxy:
    return EntityProxy.from_dict(
        {"id": id_, "schema": schema, "properties": {"name": [name]}}
    )


def test_get_store_returns_lakehouse_store(tmp_path):
    store = get_store(tmp_path)
    assert isinstance(store, LakehouseStore)


def test_store_get_returns_fragments(tmp_path):
    store = get_store(tmp_path)
    ds = store.get(DATASET)
    assert isinstance(ds, LakehouseFragments)
    assert ds.name == DATASET


def test_get_fragments_factory(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    assert isinstance(ds, LakehouseFragments)
    assert ds.name == DATASET


def test_bulk_put_iterate_roundtrip(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk(size=10) as bulk:
        bulk.put(_entity("alice", "Alice"))
        bulk.put(_entity("bob", "Bob"))
    ds._repo.flush()  # write the journal to parquet

    ids = {e.id for e in ds.iterate()}
    assert ids == {"alice", "bob"}


def test_put_accepts_dict(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put({"id": "alice", "schema": "Person", "properties": {"name": ["Alice"]}})
    ds._repo.flush()
    assert {e.id for e in ds.iterate()} == {"alice"}


def test_put_one_shot(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    ds.put(_entity("alice"))
    ds._repo.flush()
    assert {e.id for e in ds.iterate()} == {"alice"}


def test_default_fragment_is_accepted(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put(_entity("alice"), fragment=DEFAULT_FRAGMENT)
    ds._repo.flush()
    assert {e.id for e in ds.iterate()} == {"alice"}


def test_non_default_fragment_raises(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        with pytest.raises(NotImplementedError):
            bulk.put(_entity("alice"), fragment="some-source")


def test_iterate_filters_by_entity_id(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put(_entity("alice"))
        bulk.put(_entity("bob"))
    ds._repo.flush()

    only_alice = list(ds.iterate(entity_id="alice"))
    assert {e.id for e in only_alice} == {"alice"}


def test_iterate_filters_by_schema(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put(_entity("alice", schema="Person"))
        bulk.put(_entity("acme", "Acme Inc", schema="Company"))
    ds._repo.flush()

    persons = {e.id for e in ds.iterate(schema="Person")}
    assert persons == {"alice"}


def test_iterate_filters_by_origin(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk(origin="src_a") as bulk:
        bulk.put(_entity("alice"))
    with ds.bulk(origin="src_b") as bulk:
        bulk.put(_entity("bob"))
    ds._repo.flush()

    src_a = {e.id for e in ds.iterate(origin="src_a")}
    assert src_a == {"alice"}


def test_iterate_since_until_window(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put(_entity("alice"))
    ds._repo.flush()

    now = datetime.now(timezone.utc)
    future = now + timedelta(hours=1)
    past = now - timedelta(hours=1)

    # `now` is inside [past, future]: should match
    in_window = {e.id for e in ds.iterate(since=past, until=future)}
    assert "alice" in in_window

    # Window entirely in the past — neither first_seen nor last_seen falls in it
    way_past = now - timedelta(days=365)
    older = now - timedelta(days=300)
    out_of_window = {e.id for e in ds.iterate(since=way_past, until=older)}
    assert out_of_window == set()


def test_get_returns_entity(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put(_entity("alice", "Alice"))
    ds._repo.flush()

    e = ds.get("alice")
    assert e is not None
    assert e.id == "alice"
    assert ds.get("nobody") is None


def test_len_matches_entity_count(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    assert len(ds) == 0
    with ds.bulk() as bulk:
        bulk.put(_entity("alice"))
        bulk.put(_entity("bob"))
    ds._repo.flush()
    assert len(ds) == ds._repo.get_statistics().entity_count == 2


def test_iter_dunder(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put(_entity("alice"))
    ds._repo.flush()
    assert {e.id for e in ds} == {"alice"}


def test_delete_many_then_merge(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put(_entity("alice"))
        bulk.put(_entity("bob"))
    ds._repo.flush()
    assert {e.id for e in ds.iterate()} == {"alice", "bob"}

    ds.delete_many(["alice"])
    ds._repo.flush()
    ds._repo.merge(grace_period_days=0)

    assert {e.id for e in ds.iterate()} == {"bob"}


def test_drop_wipes_dataset(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with ds.bulk() as bulk:
        bulk.put(_entity("alice"))
    ds._repo.flush()
    assert len(ds) == 1

    ds.drop()
    # New repo against the same uri must see an empty store
    fresh = get_fragments(DATASET, uri=tmp_path)
    assert list(fresh.iterate()) == []


@pytest.mark.parametrize(
    "name",
    [
        "iterate_batched",
        "partials",
        "fragments",
        "statements",
        "get_sorted_ids",
        "get_sorted_id_batches",
        "delete",
    ],
)
def test_out_of_mvp_methods_raise(tmp_path, name):
    ds = get_fragments(DATASET, uri=tmp_path)
    with pytest.raises(NotImplementedError):
        getattr(ds, name)()


def test_bulk_context_rolls_back_on_exception(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    with pytest.raises(RuntimeError):
        with ds.bulk() as bulk:
            bulk.put(_entity("alice"))
            raise RuntimeError("boom")
    ds._repo.flush()  # flush whatever survived
    # Rollback dropped the in-flight transaction; alice should not be visible.
    assert list(ds.iterate()) == []


def test_bulk_loader_can_be_used_without_context_manager(tmp_path):
    ds = get_fragments(DATASET, uri=tmp_path)
    bulk: LakehouseBulkLoader = ds.bulk(size=2)
    bulk.put(_entity("alice"))
    bulk.put(_entity("bob"))
    # size threshold hit during the second put → already flushed once; closer
    # writes don't add anything until we explicitly flush.
    bulk._close()
    ds._repo.flush()
    assert {e.id for e in ds.iterate()} == {"alice", "bob"}
