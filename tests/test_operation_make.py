"""Tests for MakeOperation - the drain + merge + exports workflow."""

from ftmq.util import make_entity

from ftm_lakehouse.operation import factories as op
from ftm_lakehouse.operation.export import ExportJob, ExportKind, ExportOperation
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


def test_operation_make_drains_and_merges_once(tmp_path, monkeypatch):
    """One `make` is one drain and one merge pass, not one per export kind.

    `MakeOperation.handle` runs three `ExportOperation`s and each used to
    prepare for itself – a full drain plus a store-wide merge three times
    over, on top of the operation's own. They are constructed ``prepared``
    now, and the drain the merge does itself is the only one.
    """
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
        writer.add_entity(make_entity(JOHN))

    calls = record_calls(monkeypatch, "flush", "merge")
    result = op.make(DATASET, tmp_path)

    assert result.done == 1
    assert calls.count("merge") == 1
    assert calls.count("flush") == 1  # the one `merge` does itself


def test_operation_make_converges_when_nothing_changed(tmp_path, monkeypatch):
    """A second `make` with nothing written in between does no work at all.

    ``make`` depended on ``journal/last_updated``, which every writer stamps,
    so a continuously-fed dataset was never fresh and the run never
    converged. It keys on the canonical-content clock instead, and `prepare`
    runs ahead of the check, so outstanding rows still cannot hide from it.
    """
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
    op.make(DATASET, tmp_path)

    calls = record_calls(monkeypatch, "flush", "merge")
    result = op.make(DATASET, tmp_path)

    assert result.done == 0  # skipped as fresh
    assert calls == []  # the prepare guard didn't take the write fence either


def test_operation_make_prepared_export_skips_the_drain(tmp_path, monkeypatch):
    """``prepared`` holds even with rows outstanding – which is the point.

    Under continuous ingest the journal always has new rows by the time the
    next export kind starts, so an unguarded `prepare` found both its
    conditions true every time and drained and merged the whole store once
    per kind.
    """
    repo = EntityRepository(dataset=DATASET, uri=tmp_path)
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
    repo.merge()

    with repo.writer(origin="test") as writer:  # a producer, mid-run
        writer.add_entity(make_entity(BOB))

    calls = record_calls(monkeypatch, "flush", "merge")
    job = ExportJob.make(dataset=DATASET, kind=ExportKind.index)
    ExportOperation(job, tmp_path, prepared=True).run(force=True)

    assert calls == []
