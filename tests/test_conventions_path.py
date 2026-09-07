"""Path primitives and the layout they compose.

The shapes asserted here *are* the storage layout – a change to one of them
moves data on disk – so they are spelled out literally rather than derived
from the code under test.
"""

import copy
import os
import pickle
from datetime import datetime, timezone

import pytest

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.logic.path import (
    CallableKey,
    DateTimeKey,
    JobsKey,
    ScopedKey,
    StoreKey,
    make_ts,
)

TS = datetime(2026, 1, 16, 10, 30, 0, 123456, tzinfo=timezone.utc)
TS_KEY = "20260116T103000123456Z"
CHECKSUM = "5a6acf229ba576d9a40b09292595658bbb74ef56ab12cd34ef56ab12cd34ef56"
CHECKSUM_TREE = f"5a/6a/cf/{CHECKSUM}"


def test_conventions_path_suffixed():
    """A codec is appended as one more part, never replacing an extension."""
    key = StoreKey("entities.ftm.json")
    assert str(key + "zst") == "entities.ftm.json.zst"
    # ... unlike `with_suffix`, which would eat the `.json`
    assert str(key.with_suffix(".zst")) == "entities.ftm.zst"
    # an absent codec passes straight through, so a caller need not branch
    assert str(key + None) == "entities.ftm.json"
    # the parent is kept
    assert str(StoreKey("exports/statements.csv") + "gz") == (
        "exports/statements.csv.gz"
    )
    # be a bit lenient
    assert (key + "zst") == (key + ".zst")
    assert str(key[0]) == "entities.0.ftm.json"


def test_conventions_path_infixed():
    """A scope qualifies the stem, so it goes before the extension.

    ``[]`` selects a variant, ``+`` appends a codec – not interchangeable, so
    they get different operators.
    """
    key = StoreKey("exports/documents.csv")
    assert str(key["crawl"]) == "exports/documents.crawl.csv"
    assert str(key[None]) == "exports/documents.csv"
    # the two transforms compose in the order the layout needs
    assert str(key["crawl"] + "zst") == "exports/documents.crawl.csv.zst"


def test_conventions_path_arithmetic_degrades_to_base():
    """Deriving a path off a builder yields a key, not another builder.

    `PurePosixPath` routes every derived path through ``type(self)(...)``
    with segments alone, which a builder's ``__init__`` cannot accept – so
    `StoreKey.with_segments` hands back the base type instead of raising.
    """
    archive = path.ArchiveKey(CHECKSUM)
    assert isinstance(archive, path.ArchiveKey)
    for derived in (archive / "x", archive.parent, archive.with_name("x")):
        assert type(derived) is StoreKey
    assert str(archive / "blob") == f"archive/{CHECKSUM_TREE}/blob"


def test_conventions_path_is_a_store_key():
    """A key is `os.PathLike` and posix-joined, so anystore takes it as a uri."""
    key = StoreKey("exports/documents.csv")
    assert os.fspath(key) == "exports/documents.csv"
    assert str(key) == "exports/documents.csv"
    assert f"diffs/{key}" == "diffs/exports/documents.csv"
    # survives the round-trips a value passed around gets put through
    assert str(copy.deepcopy(key)) == "exports/documents.csv"
    assert str(pickle.loads(pickle.dumps(key))) == "exports/documents.csv"


def test_conventions_path_does_not_equal_str():
    """A key is not its string – compare `str(key)`, don't mix them as dict keys.

    Pinned because the hashes *do* collide, so a dict mixing the two looks
    like it works right up until the lookup silently misses.
    """
    key = StoreKey("exports/documents.csv")
    assert key != "exports/documents.csv"
    assert str(key) == "exports/documents.csv"
    assert {key: 1}.get("exports/documents.csv") is None


def test_conventions_path_scoped_key():
    """A scoped key renders with its default, and ``[]`` swaps the scope."""
    assert isinstance(path.TAGS, ScopedKey)
    assert str(path.TAGS) == "tags/lakehouse"
    assert str(path.TAGS / "foo") == "tags/lakehouse/foo"
    assert str(path.TAGS["tenant1"] / "foo") == "tags/tenant1/foo"
    assert str(path.TAGS[None] / "foo") == "tags/lakehouse/foo"
    assert str(path.LOCKS / "merge") == ".locks/lakehouse/merge"
    assert str(path.LOCKS["tenant1"] / "merge") == ".locks/tenant1/merge"
    # the scope is picked once - swapping yields a plain key, not another scope
    assert type(path.TAGS["tenant1"]) is StoreKey
    with pytest.raises(ValueError):
        path.TAGS["../escape"]


def test_conventions_path_jobs_key():
    """``[]`` is the job type's prefix, ``()`` is one run inside it.

    A job type is a segment *under* the fixed ``jobs/runs/`` prefix, not a
    swap of it – so this is deliberately not a `ScopedKey`, which would
    replace ``runs`` and move every job run.
    """
    assert isinstance(path.JOB_RUNS, JobsKey)
    assert not isinstance(path.JOB_RUNS, ScopedKey)
    assert str(path.JOB_RUNS) == "jobs/runs"
    with pytest.raises(ValueError):
        str(path.JOB_RUNS[None])
    assert str(path.JOB_RUNS["ExportJob"]) == "jobs/runs/ExportJob"
    assert str(path.JOB_RUNS("ExportJob", "r1")) == "jobs/runs/ExportJob/r1.json"
    # an omitted id is minted time-ordered, so runs sort chronologically
    first, second = path.JOB_RUNS("ExportJob"), path.JOB_RUNS("ExportJob")
    assert str(first) < str(second)
    for bad in ("../escape", "a/b"):
        with pytest.raises(ValueError):
            path.JOB_RUNS[bad]
        with pytest.raises(ValueError):
            path.JOB_RUNS(bad)


def test_conventions_path_callable_key():
    """A callable key is addressable as a prefix and callable as a factory."""
    assert str(path.VERSIONS) == "versions"
    assert isinstance(path.VERSIONS, path.VersionsKey)
    assert isinstance(path.VERSIONS, CallableKey)
    assert str(path.VERSIONS("config.yml", TS)) == (
        f"versions/2026/01/{TS_KEY}/config.yml"
    )
    # a pre-rendered timestamp is taken as-is, so a caller holding the label
    # (the diff state does) lands in the same directory
    assert str(path.VERSIONS("index.json", TS_KEY)) == (
        f"versions/2026/01/{TS_KEY}/index.json"
    )
    # an omitted timestamp is now, so a snapshot lands in the current month
    now = datetime.now(timezone.utc)
    assert str(path.VERSIONS("config.yml")).startswith(f"versions/{now:%Y}/{now:%m}/")
    with pytest.raises(NotImplementedError):
        CallableKey("x")()


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (
            path.TAGS / "exports" / "statements.csv",
            "tags/lakehouse/exports/statements.csv",
        ),
        (path.LOCKS / "merge", ".locks/lakehouse/merge"),
        (path.ENTITIES_JSON, "entities.ftm.json"),
        (path.ENTITIES_JSON + "zst", "entities.ftm.json.zst"),
        (path.EXPORTS_STATEMENTS, "exports/statements.csv"),
        (path.EXPORTS_STATEMENTS + "gz", "exports/statements.csv.gz"),
        (path.EXPORTS_DOCUMENTS, "exports/documents.csv"),
        (path.EXPORTS_DOCUMENTS["crawl"], "exports/documents.crawl.csv"),
        (
            path.EXPORTS_DOCUMENTS["crawl"] + "zst",
            "exports/documents.crawl.csv.zst",
        ),
        (path.EXPORTS_STATISTICS, "exports/statistics.json"),
        (path.JOB_RUNS["ExportJob"], "jobs/runs/ExportJob"),
        (path.JOB_RUNS("ExportJob", "r1"), "jobs/runs/ExportJob/r1.json"),
    ],
)
def test_conventions_path_layout(key, expected):
    """The layout, spelled out: these keys name data already on disk."""
    assert str(key) == expected


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (path.ArchiveKey(CHECKSUM), f"archive/{CHECKSUM_TREE}"),
        (path.ArchiveKey(CHECKSUM).blob, f"archive/{CHECKSUM_TREE}/blob"),
        (
            path.ArchiveKey(CHECKSUM).meta("file-abc"),
            f"archive/{CHECKSUM_TREE}/file-abc.json",
        ),
        (path.ArchiveKey(CHECKSUM).txt("ocr"), f"archive/{CHECKSUM_TREE}/ocr.txt"),
    ],
)
def test_conventions_path_archive(key, expected):
    assert str(key) == expected


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (path.DIFFS_ENTITIES, "diffs/entities.ftm.json"),
        (path.DIFFS_DOCUMENTS, "diffs/exports/documents.csv"),
        (path.DIFFS_DOCUMENTS["crawl"], "diffs/exports/documents.crawl.csv"),
        (path.DIFFS_DOCUMENTS[None], "diffs/exports/documents.csv"),
        (
            path.DIFFS_ENTITIES(TS),
            f"diffs/entities.ftm.json/{TS_KEY}.delta.json",
        ),
        (
            path.DIFFS_ENTITIES(TS) + "zst",
            f"diffs/entities.ftm.json/{TS_KEY}.delta.json.zst",
        ),
        (
            path.DIFFS_DOCUMENTS(TS),
            f"diffs/exports/documents.csv/{TS_KEY}.diff.csv",
        ),
        (
            path.DIFFS_DOCUMENTS["crawl"](TS) + "zst",
            f"diffs/exports/documents.crawl.csv/{TS_KEY}.diff.csv.zst",
        ),
    ],
)
def test_conventions_path_diffs(key, expected):
    assert str(key) == expected


def test_conventions_path_diffs_ext_lands_on_entries_only():
    """`ext` names what the series holds, so it never reaches the directory.

    The directory is the freshness tag and diff-state key, so an extension
    there would move both – and a scoped variant has to stay callable to have
    entries at all.
    """
    assert str(path.DIFFS_DOCUMENTS) == "diffs/exports/documents.csv"
    assert str(path.DIFFS_DOCUMENTS["crawl"]) == "diffs/exports/documents.crawl.csv"
    assert isinstance(path.DIFFS_DOCUMENTS["crawl"], DateTimeKey)
    assert str(path.DIFFS_DOCUMENTS["crawl"](TS)).endswith(
        f"{TS_KEY}.{path.EXT_DOCUMENTS_DELTA}"
    )


def test_conventions_path_diffs_scope_stays_callable():
    """A scoped series keeps its own entries, so ``[]`` yields another series."""
    scoped = path.DIFFS_DOCUMENTS["crawl"]
    assert isinstance(scoped, DateTimeKey)
    assert str(scoped(TS)) == (f"diffs/exports/documents.crawl.csv/{TS_KEY}.diff.csv")
    # ... and it is named after the export it diffs
    assert str(scoped) == f"diffs/{path.EXPORTS_DOCUMENTS['crawl']}"


def test_conventions_path_diff_dirs_are_codec_free():
    """A diff directory is named after the export's identity, not its artifact.

    It doubles as the series' freshness tag and diff-state key, so configuring
    compression must not move it – only the files inside it carry the codec.
    """
    for compression in (None, "gz", "zst"):
        # the directory holds neither extension nor codec, whatever the files do
        assert str(path.DIFFS_DOCUMENTS) == "diffs/exports/documents.csv"
        assert str(path.DIFFS_DOCUMENTS["crawl"]) == (
            "diffs/exports/documents.crawl.csv"
        )
        assert str(path.DIFFS_DOCUMENTS(TS) + compression).startswith(
            "diffs/exports/documents.csv/"
        )
        assert str(path.DIFFS_ENTITIES(TS) + compression).startswith(
            "diffs/entities.ftm.json/"
        )


def test_conventions_path_validates_caller_input():
    """Caller-supplied segments are validated before they reach a key."""
    for bad in ("../escape", "a/b", "", "."):
        with pytest.raises(ValueError):
            path.ArchiveKey(CHECKSUM).txt(bad)
    for bad in ("../escape", "a/b", "."):
        with pytest.raises(ValueError):
            path.EXPORTS_DOCUMENTS[bad]
        with pytest.raises(ValueError):
            path.EXPORTS_STATEMENTS + bad
    with pytest.raises(ValueError):
        path.ArchiveKey(CHECKSUM).meta("../escape")


def test_conventions_path_make_ts():
    """A format renders the layout's compact stamp; without one, ISO 8601."""
    assert make_ts(TS, path.TS_FORMAT) == TS_KEY
    assert make_ts(TS) == "2026-01-16T10:30:00.123456+00:00"
    # omitted means now, in the same rendering
    assert datetime.strptime(make_ts(format=path.TS_FORMAT), path.TS_FORMAT).replace(
        tzinfo=timezone.utc
    ) <= datetime.now(timezone.utc)


def test_conventions_path_datetime_key_defaults():
    """Without a format or an extension, an entry is a bare ISO timestamp."""
    series = DateTimeKey(StoreKey("series"))
    assert str(series) == "series"
    assert str(series(TS)) == "series/2026-01-16T10:30:00.123456+00:00"
    # ... and both are independent: a format without an extension, or vice versa
    assert str(DateTimeKey(StoreKey("s"), path.TS_FORMAT)(TS)) == f"s/{TS_KEY}"
    assert str(DateTimeKey(StoreKey("s"), None, "json")(TS)) == (
        "s/2026-01-16T10:30:00.123456+00:00.json"
    )


@pytest.mark.parametrize(
    ("key", "expected"),
    [
        (path.INDEX, "index.json"),
        (path.CONFIG, "config.yml"),
        (path.STATISTICS, "statistics.json"),
        (path.VERSIONS, "versions"),
        (path.LOCK, ".LOCK"),
        (path.LOCK_APPENDS, ".LOCK-APPENDS"),
        (path.TAGS, "tags/lakehouse"),
        (path.LOCKS, ".locks/lakehouse"),
        (path.ARCHIVE, "archive"),
        (path.ARCHIVE_BLOB, "blob"),
        (path.STATEMENTS, "statements"),
        (path.EXPORTS, "exports"),
        (path.EXPORTS_CYPHER, "exports/graph.cypher"),
        (path.DIFFS, "diffs"),
        (path.JOBS, "jobs"),
        (path.JOB_RUNS, "jobs/runs"),
        (path.TENANT, "lakehouse"),
        (path.TS_FORMAT, "%Y%m%dT%H%M%S%fZ"),
        (path.EXT_ENTITIES_DELTA, "delta.json"),
        (path.EXT_DOCUMENTS_DELTA, "diff.csv"),
    ],
)
def test_conventions_path_constants(key, expected):
    """Every bare constant of the layout, spelled out."""
    assert str(key) == expected


def test_conventions_path_archive_validates_checksum():
    """The archive tree is derived from the checksum, so it has to be one."""
    for bad in ("not-a-checksum", "", "../escape", CHECKSUM[:-1]):
        with pytest.raises(ValueError):
            path.ArchiveKey(bad)
