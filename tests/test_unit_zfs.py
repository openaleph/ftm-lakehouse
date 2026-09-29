"""Lakehouse-specific ZFS provisioning and replication.

The transport (subprocess / socket agent, chown, peer auth) is the external
``zfs-agent`` package and tested there – these tests cover only what stays in
the lakehouse: the per-storage-type tuning, the composition of the
``ensure_zfs_dataset`` hierarchy, transfer planning and the stream plumbing
(real pipes and threads, the ``zfs-agent`` calls faked).
"""

import asyncio
import os
import threading
import time
from unittest.mock import call, patch

import pytest

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.zfs import (
    ARCHIVE,
    CHUNK_SIZE,
    PARENT_PROPS,
    STATEMENTS,
    DatasetConfig,
    SendStream,
    Transfer,
    arun_receive,
    check_target,
    ensure_zfs_dataset,
    plan_transfer,
    run_receive,
    select_parts,
    snapshot_dataset,
    zfs_dataset,
)
from ftm_lakehouse.core.zfs.client import _client


@pytest.fixture(autouse=True)
def clear_ensure_cache():
    ensure_zfs_dataset.cache_clear()
    yield
    ensure_zfs_dataset.cache_clear()


def test_dataset_configs():
    """The tuned per-storage-type properties are a deliberate contract."""
    assert ARCHIVE.to_props()["compression"] == "zstd-9"
    assert ARCHIVE.to_props()["recordsize"] == "1M"
    # parquet compresses itself - ZFS compression on top burns CPU for nothing
    assert STATEMENTS.to_props()["compression"] == "off"
    assert STATEMENTS.to_props()["recordsize"] == "1M"
    assert PARENT_PROPS["atime"] == "off"

    custom = DatasetConfig(extra={"quota": "1T"})
    assert custom.to_props()["quota"] == "1T"
    assert custom.to_props()["compression"] == "zstd"


@patch("ftm_lakehouse.core.zfs.main.zfs_create")
def test_ensure_zfs_dataset_hierarchy(mock_create):
    """One parent + one child per storage type, each with its tuning."""
    ensure_zfs_dataset("tank/lake", "my_dataset")
    assert mock_create.call_args_list == [
        call("tank/lake/my_dataset", **PARENT_PROPS),
        call(f"tank/lake/my_dataset/{path.ARCHIVE}", **ARCHIVE.to_props()),
        call(f"tank/lake/my_dataset/{path.STATEMENTS}", **STATEMENTS.to_props()),
    ]


@patch("ftm_lakehouse.core.zfs.main.zfs_create")
def test_ensure_zfs_dataset_cached_per_process(mock_create):
    ensure_zfs_dataset("tank/lake", "my_dataset")
    ensure_zfs_dataset("tank/lake", "my_dataset")
    assert mock_create.call_count == 3  # only the first call fires

    ensure_zfs_dataset("tank/lake", "other_dataset")
    assert mock_create.call_count == 6


@patch("ftm_lakehouse.core.zfs.main.zfs_create")
def test_ensure_zfs_dataset_rejects_invalid_names(mock_create):
    with pytest.raises(ValueError):
        ensure_zfs_dataset("tank/lake", "Invalid Name!")
    with pytest.raises(ValueError):  # reserved by the lakehouse
        ensure_zfs_dataset("tank/lake", "catalog")
    mock_create.assert_not_called()


# --- replication: planning ---


def snap(name: str, guid: str | None = None, txg: int = 0) -> dict:
    return {"name": name, "guid": guid or f"g-{name}", "createtxg": txg}


def status(*snapshots: dict, exists: bool = True, token: str | None = None) -> dict:
    return {"exists": exists, "snapshots": list(snapshots), "resume_token": token}


SOURCE = status(snap("s1"), snap("s2"), snap("s3"))


@pytest.mark.parametrize(
    "target,snapshot,flags,expected",
    [
        # nothing there yet: full send
        (status(exists=False), "s3", {}, Transfer(snapshot="s3")),
        # newest common guid is the base – matched by guid, named as on the
        # source, since that's where ``zfs send -i`` looks it up
        (status(snap("x", "g-s2")), "s3", {}, Transfer("s3", "s2", base="g-s2")),
        (status(snap("s1"), snap("s2")), "s3", {}, Transfer("s3", "s2", base="g-s2")),
        # the target already has it
        (status(snap("s1"), snap("s2"), snap("s3")), "s3", {}, None),
        (status(snap("s1"), snap("s2")), "s2", {}, None),
        # an older snapshot of the source is fine as a target too
        (status(snap("s1")), "s2", {}, Transfer("s2", "s1", base="g-s1")),
        # target newer than the base: destroyed by -F only with force
        (
            status(snap("s1"), snap("t9")),
            "s3",
            {"force": True},
            Transfer("s3", "s1", base="g-s1"),
        ),
        # an existing target without snapshots only gets replaced on request
        (status(), "s3", {"replace": True}, Transfer(snapshot="s3")),
        # a pending resume comes first, carrying the base it applies on
        (
            status(snap("s1"), token="1-abc"),
            "s3",
            {},
            Transfer(token="1-abc", base="g-s1"),
        ),
        # an interrupted receive that was creating the dataset leaves it
        # behind without snapshots – resuming it needs no replace
        (status(token="1-abc"), "s3", {}, Transfer(token="1-abc")),
        # a colon-named common base (sanoid / zrepl) is as good as any
        (
            status(snap("auto_2026-09-28_11:00:00", "g-s2")),
            "s3",
            {},
            Transfer("s3", "s2", base="g-s2"),
        ),
    ],
)
def test_plan_transfer(target, snapshot, flags, expected):
    assert plan_transfer(SOURCE, target, snapshot, **flags) == expected


@pytest.mark.parametrize(
    "target,snapshot,flags,match",
    [
        (status(snap("s1"), snap("t8"), snap("t9")), "s3", {}, "t8, t9"),
        (status(), "s3", {}, "use replace"),
        # force is about snapshots – it doesn't let a full receive replace
        (status(), "s3", {"force": True}, "use replace"),
        (status(snap("other")), "s3", {}, "share no snapshot"),
        (status(exists=False), "nope", {}, "no snapshot `nope`"),
        (status(snap("s1"), snap("s3", "g-other")), "s3", {}, "different snapshot"),
        # a resume is refused too once the target got newer snapshots – the
        # receive's -F would destroy them just the same
        (status(snap("s1"), snap("keep"), token="1-abc"), "s3", {}, "keep"),
    ],
)
def test_plan_transfer_refuses(target, snapshot, flags, match):
    with pytest.raises(ValueError, match=match):
        plan_transfer(SOURCE, target, snapshot, **flags)


def test_plan_transfer_ignores_source_snapshots_after_target_snapshot():
    """Sending s2 must not use s3 – which the target has – as its base."""
    target = status(snap("s1"), snap("s3"))
    assert plan_transfer(SOURCE, target, "s2", force=True) == Transfer(
        "s2", "s1", base="g-s1"
    )


def test_check_target():
    target = status(snap("s1"), snap("s2"))
    check_target(target, "g-s2")
    check_target(target, "g-s1", force=True)
    with pytest.raises(ValueError, match="would destroy: s2 "):
        check_target(target, "g-s1")
    with pytest.raises(ValueError, match="doesn't have the incremental base"):
        check_target(target, "g-nope", force=True)
    with pytest.raises(ValueError, match="share no snapshot"):
        check_target(target, None, force=True, replace=True)
    check_target(status(exists=False), None)


def test_zfs_dataset_parts():
    assert zfs_dataset("tank/lake", "ds") == "tank/lake/ds"
    assert zfs_dataset("tank/lake", "ds", "archive") == "tank/lake/ds/archive"
    with pytest.raises(ValueError):
        zfs_dataset("tank/lake", "ds", "journal")
    for name in ("Invalid Name!", "catalog", "default"):
        with pytest.raises(ValueError):
            zfs_dataset("tank/lake", name)
    assert select_parts() == ["base", "archive", "statements"]
    assert select_parts(archive=False) == ["base", "statements"]
    assert select_parts(False, False) == ["base"]


@patch("ftm_lakehouse.core.zfs.main.zfs_snapshot")
def test_snapshot_only_selected_parts(mock_snapshot):
    name = snapshot_dataset("tank", "ds", ["base", "statements"], "n1")
    assert name == "n1"
    mock_snapshot.assert_called_once_with("tank/ds@n1", "tank/ds/statements@n1")
    assert len(snapshot_dataset("tank", "ds", ["base"])) == 14  # YYYYmmddHHMMSS


def test_peer_client_credentials(monkeypatch):
    """The lakehouse api credentials never reach a replication peer."""
    monkeypatch.setenv("LAKEHOUSE_API_KEY", "lake-key")
    monkeypatch.setenv("LAKEHOUSE_API_SECRET", "lake-secret")
    monkeypatch.delenv("LAKEHOUSE_ZFS_PEER_KEY", raising=False)
    monkeypatch.delenv("LAKEHOUSE_ZFS_PEER_SECRET", raising=False)
    with _client("http://peer") as client:
        assert "x-api-key" not in client.headers
        assert "x-api-secret" not in client.headers
    monkeypatch.setenv("LAKEHOUSE_ZFS_PEER_KEY", "peer-key")
    monkeypatch.setenv("LAKEHOUSE_ZFS_PEER_SECRET", "peer-secret")
    with _client("http://peer") as client:
        assert client.headers["x-api-key"] == "peer-key"
        assert client.headers["x-api-secret"] == "peer-secret"


# --- replication: streams through real pipes ---


def write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


PAYLOAD = os.urandom(3 * CHUNK_SIZE + 12345)


def test_send_stream_streams_everything():
    def fake_send(dataset, fd, snapshot=None, since=None, token=None):
        assert (dataset, snapshot, since) == ("tank/ds", "s2", "s1")
        write_all(fd, PAYLOAD)

    with patch("ftm_lakehouse.core.zfs.stream.zfs_send", fake_send):
        stream = SendStream("tank/ds", 2 * CHUNK_SIZE, snapshot="s2", since="s1")
        chunks = [stream.first(), *stream]
    assert b"".join(chunks) == PAYLOAD
    assert all(len(c) <= CHUNK_SIZE for c in chunks)


def test_send_stream_async():
    def fake_send(dataset, fd, **kwargs):
        write_all(fd, PAYLOAD)

    async def consume(stream):
        return [chunk async for chunk in stream]

    with patch("ftm_lakehouse.core.zfs.stream.zfs_send", fake_send):
        stream = SendStream("tank/ds", CHUNK_SIZE, snapshot="s1")
        assert b"".join(asyncio.run(consume(stream))) == PAYLOAD


def test_send_stream_raises_after_its_output():
    def fake_send(dataset, fd, **kwargs):
        write_all(fd, b"partial")
        time.sleep(0.2)  # the reader hands over what's there meanwhile
        raise RuntimeError("zfs send failed: boom")

    with patch("ftm_lakehouse.core.zfs.stream.zfs_send", fake_send):
        stream = SendStream("tank/ds", CHUNK_SIZE, snapshot="s1")
        assert stream.first() == b"partial"
        with pytest.raises(RuntimeError, match="boom"):
            list(stream)


def test_send_stream_error_before_output_surfaces_in_first():
    def fake_send(dataset, fd, **kwargs):
        raise RuntimeError("zfs send failed: no such snapshot")

    with patch("ftm_lakehouse.core.zfs.stream.zfs_send", fake_send):
        with pytest.raises(RuntimeError, match="no such snapshot"):
            SendStream("tank/ds", CHUNK_SIZE, snapshot="nope").first()


def test_send_stream_empty():
    with patch(
        "ftm_lakehouse.core.zfs.stream.zfs_send", lambda dataset, fd, **kw: None
    ):
        stream = SendStream("tank/ds", CHUNK_SIZE, snapshot="s1")
        assert stream.first() == b""
        assert list(stream) == []


@pytest.mark.parametrize("from_thread", [False, True])
def test_send_stream_closed_early_ends_the_send(from_thread):
    """A consumer that goes away must fail the send, not leave it blocked –
    also when ``close`` comes from another thread while iteration waits."""
    done = threading.Event()

    def endless_send(dataset, fd, **kwargs):
        try:
            while True:
                os.write(fd, b"x" * 65536)
        except BrokenPipeError:
            raise RuntimeError("zfs send failed: broken pipe")
        finally:
            done.set()

    with patch("ftm_lakehouse.core.zfs.stream.zfs_send", endless_send):
        stream = SendStream("tank/ds", CHUNK_SIZE, snapshot="s1")
        stream.first()
        if from_thread:
            threading.Timer(0.2, stream.close).start()
            for _ in stream:  # drains, then ends once closed
                pass
        else:
            stream.close()
        assert done.wait(5)


def test_send_stream_close_reaches_a_stalled_send():
    """A send that stalls (a slow disk) must still find the pipe closed the
    next time it writes – the reader may not sit in a blocking read."""
    result = {}

    def stalling_send(dataset, fd, **kwargs):
        os.write(fd, b"x")
        time.sleep(0.5)  # stalled while the consumer goes away
        try:
            os.write(fd, b"x")
            result["second_write"] = "ok"
        except BrokenPipeError:
            result["second_write"] = "epipe"
            raise RuntimeError("zfs send failed: broken pipe")

    with patch("ftm_lakehouse.core.zfs.stream.zfs_send", stalling_send):
        stream = SendStream("tank/ds", CHUNK_SIZE, snapshot="s1")
        assert stream.first() == b"x"
        stream.close()
        stream._send.join(5)
    assert result == {"second_write": "epipe"}


def fake_receive_into(sink: dict):
    def fake_receive(dataset, fd, props=None, force=False, resumable=False):
        chunks = []
        while data := os.read(fd, 1 << 20):
            chunks.append(data)
        sink[dataset] = (b"".join(chunks), props, force, resumable)

    return fake_receive


def test_run_receive_gets_everything():
    sink: dict = {}
    small = [PAYLOAD[i : i + 1000] for i in range(0, len(PAYLOAD), 1000)]
    with patch("ftm_lakehouse.core.zfs.stream.zfs_receive", fake_receive_into(sink)):
        run_receive("tank/ds", iter(small), CHUNK_SIZE, props={"atime": "off"})
    assert sink["tank/ds"] == (PAYLOAD, {"atime": "off"}, False, False)


def test_run_receive_survives_short_writes(monkeypatch):
    """A signal can cut a blocking pipe write short – the rest of the chunk
    must still go out, not vanish from the middle of the stream."""
    sink: dict = {}
    real_write = os.write
    monkeypatch.setattr(os, "write", lambda fd, data: real_write(fd, data[:4096]))
    with patch("ftm_lakehouse.core.zfs.stream.zfs_receive", fake_receive_into(sink)):
        run_receive("tank/ds", iter([PAYLOAD]), CHUNK_SIZE)
    assert sink["tank/ds"][0] == PAYLOAD


def test_run_receive_early_failure_stops_feeding():
    """A receive that gives up raises its error rather than wedging the
    writer on a pipe nobody reads."""
    fed = []

    def rejecting_receive(dataset, fd, **kwargs):
        os.read(fd, 10)
        raise RuntimeError("zfs receive failed: destination has been modified")

    def chunks():
        for _ in range(100):
            fed.append(1)
            yield b"x" * CHUNK_SIZE

    with patch("ftm_lakehouse.core.zfs.stream.zfs_receive", rejecting_receive):
        with pytest.raises(RuntimeError, match="has been modified"):
            run_receive("tank/ds", chunks(), CHUNK_SIZE)
    assert len(fed) < 100


def test_run_receive_keeps_the_feeding_error():
    """Ctrl-C (or a network error) while feeding is what's raised – not the
    receive's complaint about the stream it cut short."""

    def cut_receive(dataset, fd, **kwargs):
        while os.read(fd, 1 << 20):
            pass
        raise RuntimeError("zfs receive failed: incomplete stream")

    def chunks():
        yield b"x" * 100
        raise KeyboardInterrupt

    with patch("ftm_lakehouse.core.zfs.stream.zfs_receive", cut_receive):
        with pytest.raises(KeyboardInterrupt):
            run_receive("tank/ds", chunks(), CHUNK_SIZE)


def test_arun_receive_from_async_stream():
    sink: dict = {}

    async def body():
        for i in range(0, len(PAYLOAD), 50_000):
            yield PAYLOAD[i : i + 50_000]

    with patch("ftm_lakehouse.core.zfs.stream.zfs_receive", fake_receive_into(sink)):
        # a one-chunk buffer: the feed backs off, waiting on the receive
        asyncio.run(arun_receive("tank/ds", body(), CHUNK_SIZE, force=True))
    assert sink["tank/ds"][0] == PAYLOAD
    assert sink["tank/ds"][2] is True
