"""UTC-everywhere regression tests.

All persisted timestamps – tags, job lifecycle, statement data flowing out
of DuckDB – must be tz-aware UTC. The pytest ``filterwarnings`` gate turns
followthemoney's "datetime_iso expects UTC" warning into an error, so any
naive/local leak in the read path fails the suite at large; these tests pin
the remaining surfaces directly.
"""

from datetime import datetime, timedelta, timezone

from ftmq.util import make_entity

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.model.job import JobModel
from ftm_lakehouse.repository import EntityRepository
from ftm_lakehouse.storage.tags import TagStore
from tests.shared import JANE

DATASET = "utc_test"


def _is_utc(ts: datetime) -> bool:
    return ts.tzinfo is not None and ts.utcoffset() == timedelta()


def test_tagstore_set_and_touch_are_utc(tmp_path):
    tags = TagStore(tmp_path)

    ts = tags.set("some/tag")
    assert _is_utc(ts)
    assert _is_utc(tags.get("some/tag"))

    with tags.touch("touched/tag") as now:
        assert _is_utc(now)
    assert _is_utc(tags.get("touched/tag"))


def test_tagstore_touch_only_persists_on_success(tmp_path):
    tags = TagStore(tmp_path)
    try:
        with tags.touch("failed/tag"):
            raise RuntimeError("boom")
    except RuntimeError:
        pass
    assert tags.get("failed/tag") is None


def test_job_lifecycle_timestamps_are_utc():
    job = JobModel.make()
    job.touch()
    assert _is_utc(job.last_updated)
    job.started = datetime.now(timezone.utc)
    job.stop()
    assert _is_utc(job.stopped)
    assert job.took >= timedelta()


def test_statement_roundtrip_timestamps_are_utc(tmp_path):
    """Statements read back from the parquet store and exported to CSV carry
    UTC offsets, not the host timezone (DuckDB session is forced to UTC)."""
    repo = EntityRepository(DATASET, tmp_path)
    with repo.writer(origin="test") as writer:
        writer.add_entity(make_entity(JANE))
    repo.flush()

    for stmt in repo._statements.query_statements():
        last_seen = stmt.last_seen
        if isinstance(last_seen, datetime):
            assert _is_utc(last_seen), last_seen
        else:
            assert "+00:00" in last_seen or last_seen.endswith("Z"), last_seen

    repo.export_statements_csv()
    csv_content = (tmp_path / path.EXPORTS_STATEMENTS).read_text()
    header, first_row = csv_content.splitlines()[:2]
    # UTC may be rendered as `+00:00` or the `Z` (Zulu) suffix – both are UTC.
    # ``Z`` only appears in the timestamp columns (checksums/ids are hex), and
    # they are no longer the last column (``fragment`` trails them).
    assert "+00:00" in first_row or "Z" in first_row, first_row
    assert "+01:00" not in csv_content and "+02:00" not in csv_content
