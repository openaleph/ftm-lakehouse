"""Tests for MakeOperation - the drain + exports workflow."""

from ftmq.util import make_entity

from ftm_lakehouse.operation import factories as op
from ftm_lakehouse.repository import EntityRepository
from tests.shared import BOB, JANE, JOHN

DATASET = "make_test"


def record_calls(monkeypatch, *names: str) -> list[str]:
    """Record every call to the named `EntityRepository` methods, in order.

    Patched on the class, so the repositories the operation resolves through
    the factories are covered too.
    """
    calls: list[str] = []
    for name in names:
        original = getattr(EntityRepository, name)

        def wrapper(self, *args, _name=name, _original=original, **kwargs):
            calls.append(_name)
            return _original(self, *args, **kwargs)

        monkeypatch.setattr(EntityRepository, name, wrapper)
    return calls


def test_operation_make_flushes_and_exports(tmp_path, monkeypatch):
    """One `make` drains the journal and runs the export kinds; it never
    merges – reads reconcile, so a merge is `optimize`'s business."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))  # a duplicate the read reconciles

    calls = record_calls(monkeypatch, "flush", "merge")
    result = op.make(DATASET, tmp_path)

    assert result.done == 1
    assert "merge" not in calls
    assert "flush" in calls
    assert repo.needs_merge  # left for `optimize`
    assert {e.id for e in repo.stream()} == {"jane", "john"}


def test_operation_make_converges_when_nothing_changed(tmp_path, monkeypatch):
    """A second `make` with nothing written in between skips as fresh.

    It keys on ``statements/last_updated``, which only landing rows move; the
    drain its `prepare` runs probes an empty journal and lands none.
    """
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
    op.make(DATASET, tmp_path)

    calls = record_calls(monkeypatch, "flush", "merge")
    result = op.make(DATASET, tmp_path)

    assert result.done == 0  # skipped as fresh
    assert "merge" not in calls


def test_operation_make_stale_after_write(tmp_path):
    """Rows journalled after a `make` make the next run do work: its `prepare`
    drains them first, and the rows landing moves the clock."""
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
    op.make(DATASET, tmp_path)

    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(BOB))

    assert op.make(DATASET, tmp_path).done == 1
    assert {e.id for e in repo.stream()} == {"jane", "bob"}
