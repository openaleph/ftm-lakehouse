"""ZFS replication end to end: ``push`` / ``pull`` over HTTP against the api.

ZFS itself is an in-memory fake standing in for the ``zfs-agent`` calls –
everything around it is real: pipes, threads, the stream buffers, the
FastAPI routes on a live server, HTTP streaming both ways. The server runs
in-process, so one fake serves both hosts, told apart by pool.
"""

import itertools
import json
import os
import socket
import subprocess
import sys
import threading
import time
import uuid
from contextlib import contextmanager
from unittest.mock import patch

import httpx
import pytest
from typer.testing import CliRunner

from ftm_lakehouse.api import main as api_main
from ftm_lakehouse.api.main import get_app, get_zfs_app
from ftm_lakehouse.cli import cli
from ftm_lakehouse.core import zfs as zfs_ops
from ftm_lakehouse.core.zfs import PART_PROPS
from ftm_lakehouse.core.zfs import client as zfs_client
from ftm_lakehouse.core.zfs import main as zfs_main
from ftm_lakehouse.core.zfs import stream as zfs_stream
from tests.conftest import live_test_api_server

LOCAL = "local/lake"
REMOTE = "remote/lake"
DATASET = "my_dataset"
PARTS = ("", "/archive", "/statements")


def read_all(fd: int) -> bytes:
    chunks = []
    while data := os.read(fd, 1 << 20):
        chunks.append(data)
    return b"".join(chunks)


def write_all(fd: int, data: bytes) -> None:
    view = memoryview(data)
    while view:
        view = view[os.write(fd, view) :]


class FakeZfs:
    """Datasets as ``{path: [snapshot]}`` plus content per snapshot.

    A send stream is a JSON header line (the snapshots it carries, the guid
    it applies on top of, the payload size) and the payload. The receive
    checks what ``zfs receive -F`` would, keeps guids, and on a short stream
    with ``resumable`` leaves a token, as ZFS does.
    """

    def __init__(self) -> None:
        self.snapshots: dict[str, list[dict]] = {}
        self.content: dict[tuple[str, str], bytes] = {}
        self.tokens: dict[str, str] = {}
        self.pending: dict[str, bytes] = {}  # token -> stream to resume with
        self.props: dict[str, dict] = {}
        self.txg = itertools.count(1)
        self.cut_next_receive = False
        self.partial_new: set[str] = set()  # created by an interrupted receive
        self.lock = threading.Lock()
        self.events: list[tuple[str, ...]] = []  # what happened, in order

    def create(self, dataset: str, data: bytes = b"") -> None:
        self.snapshots[dataset] = []
        self.content[(dataset, "")] = data

    def write(self, dataset: str, data: bytes) -> None:
        self.content[(dataset, "")] = data

    def names(self, dataset: str) -> list[str]:
        return [s["name"] for s in self.snapshots.get(dataset, [])]

    def data(self, dataset: str, name: str) -> bytes:
        return self.content[(dataset, name)]

    # the zfs-agent api

    def zfs_status(self, dataset):
        return {
            "exists": dataset in self.snapshots,
            "snapshots": [dict(s) for s in self.snapshots.get(dataset, [])],
            "resume_token": self.tokens.get(dataset),
        }

    def zfs_snapshot(self, *snapshots):
        self.events.append(("snapshot", *snapshots))
        with self.lock:
            for spec in snapshots:
                dataset, name = spec.split("@")
                if dataset not in self.snapshots:
                    raise RuntimeError(f"zfs snapshot failed: no {dataset}")
                if name in self.names(dataset):
                    raise RuntimeError(f"zfs snapshot failed: {spec} exists")
                self.snapshots[dataset].append(
                    {
                        "name": name,
                        "guid": uuid.uuid4().hex,
                        "createtxg": next(self.txg),
                    }
                )
                self.content[(dataset, name)] = self.content[(dataset, "")]

    def zfs_send(self, dataset, fd, snapshot=None, since=None, token=None):
        if token:
            if token not in self.pending:
                raise RuntimeError(
                    "zfs send failed: cannot resume send: the snapshot used in "
                    "the initial send no longer exists"
                )
            write_all(fd, self.pending.pop(token))
            return
        names = self.names(dataset)
        if snapshot not in names:
            raise RuntimeError(f"zfs send failed: no {dataset}@{snapshot}")
        start = names.index(since) + 1 if since else 0
        snaps = self.snapshots[dataset][start : names.index(snapshot) + 1]
        base = self.snapshots[dataset][names.index(since)]["guid"] if since else None
        payload = self.content[(dataset, snapshot)]
        header = {"snapshots": snaps, "base": base, "size": len(payload)}
        write_all(fd, json.dumps(header).encode() + b"\n" + payload)

    def zfs_abort(self, dataset):
        token = self.tokens.pop(dataset, None)
        if token is None:
            raise RuntimeError("zfs receive -A failed: no partial state")
        self.pending.pop(token, None)
        if dataset in self.partial_new:  # -A destroys what the receive created
            self.partial_new.discard(dataset)
            del self.snapshots[dataset]

    def zfs_receive(self, dataset, fd, props=None, force=False, resumable=False):
        self.events.append(("receive", dataset))
        stream = read_all(fd)
        line, _, payload = stream.partition(b"\n")
        header = json.loads(line)
        if self.cut_next_receive:
            self.cut_next_receive = False
            payload = payload[: len(payload) // 2]
        if len(payload) < header["size"]:
            if resumable:
                token = f"1-{uuid.uuid4().hex}"
                self.tokens[dataset] = token
                self.pending[token] = stream
                if dataset not in self.snapshots:
                    # as ZFS does: an interrupted receive that was creating
                    # the dataset leaves it behind, snapshotless
                    self.snapshots[dataset] = []
                    self.partial_new.add(dataset)
            raise RuntimeError("zfs receive failed: incomplete stream")
        with self.lock:
            if dataset in self.partial_new:  # resuming what it had started
                self.partial_new.discard(dataset)
                del self.snapshots[dataset]
            target = self.snapshots.get(dataset)
            if header["base"] is None:
                if target:
                    raise RuntimeError("zfs receive failed: destination has snapshots")
                if target is not None and not force:
                    raise RuntimeError("zfs receive failed: destination exists")
                target = self.snapshots[dataset] = []
            else:
                if target is None:
                    raise RuntimeError("zfs receive failed: no destination")
                guids = [s["guid"] for s in target]
                if header["base"] not in guids:
                    raise RuntimeError(
                        "zfs receive failed: incremental source mismatch"
                    )
                if guids[-1] != header["base"]:
                    if not force:
                        raise RuntimeError("zfs receive failed: destination modified")
                    del target[guids.index(header["base"]) + 1 :]
            for s in header["snapshots"]:
                target.append(dict(s))
                self.content[(dataset, s["name"])] = payload
            self.content[(dataset, "")] = payload
            self.tokens.pop(dataset, None)
            self.props[dataset] = props


@pytest.fixture
def fake():
    fake = FakeZfs()
    # patched where they're called: dataset ops in `main`, streams in `stream`
    with (
        patch.multiple(
            zfs_main,
            zfs_status=fake.zfs_status,
            zfs_snapshot=fake.zfs_snapshot,
            zfs_abort=fake.zfs_abort,
        ),
        patch.multiple(
            zfs_stream, zfs_send=fake.zfs_send, zfs_receive=fake.zfs_receive
        ),
    ):
        yield fake


@contextmanager
def zfs_server(pool: str = REMOTE):
    with patch.object(api_main.settings, "zfs_pool", pool):
        app = get_zfs_app()
    with live_test_api_server(app) as url:
        yield url


def snapshot_all(fake: FakeZfs, pool: str, name: str, parts=PARTS) -> None:
    """A snapshot taken by hand – automatic names only have second
    resolution, so a test can't take two in a row."""
    fake.zfs_snapshot(*(f"{pool}/{DATASET}{suffix}@{name}" for suffix in parts))


def make_dataset(fake: FakeZfs, pool: str, content: bytes = b"v1") -> None:
    for suffix in PARTS:
        # large enough to take several chunks through the buffers
        fake.create(f"{pool}/{DATASET}{suffix}", content * (3 << 20))


def assert_replicated(fake: FakeZfs, source: str, target: str, parts=PARTS) -> None:
    for suffix in parts:
        src, dst = f"{source}/{DATASET}{suffix}", f"{target}/{DATASET}{suffix}"
        assert fake.snapshots[dst] == fake.snapshots[src], suffix
        name = fake.names(dst)[-1]
        assert fake.data(dst, name) == fake.data(src, name), suffix


BUFFER = 8 << 20


@contextmanager
def raw(url: str):
    """A plain client against the routes, bypassing ``push`` / ``pull``."""
    with httpx.Client(base_url=url) as client:
        yield client


def interrupted_push(fake: FakeZfs, url: str, snapshot: str) -> None:
    """A base-only push whose receive gets cut short, leaving a token."""
    fake.cut_next_receive = True
    with pytest.raises(RuntimeError, match="incomplete stream"):
        zfs_ops.push(
            url,
            LOCAL,
            DATASET,
            BUFFER,
            archive=False,
            statements=False,
            snapshot=snapshot,
        )
    assert fake.zfs_status(f"{REMOTE}/{DATASET}")["resume_token"]


def test_push_full_then_incremental(fake):
    make_dataset(fake, LOCAL)
    with zfs_server() as url:
        first = zfs_ops.push(url, LOCAL, DATASET, BUFFER)
        assert_replicated(fake, LOCAL, REMOTE)
        assert fake.props[f"{REMOTE}/{DATASET}/archive"] == PART_PROPS["archive"]

        fake.write(f"{LOCAL}/{DATASET}", b"v2" * 100)
        snapshot_all(fake, LOCAL, "s2")
        assert zfs_ops.push(url, LOCAL, DATASET, BUFFER, snapshot="s2") == "s2"
        assert_replicated(fake, LOCAL, REMOTE)
        assert fake.names(f"{REMOTE}/{DATASET}") == [first, "s2"]

        # nothing new: an already replicated snapshot is a no-op
        zfs_ops.push(url, LOCAL, DATASET, BUFFER, snapshot="s2")
        assert fake.names(f"{REMOTE}/{DATASET}") == [first, "s2"]


def test_push_leaves_out_children(fake):
    make_dataset(fake, LOCAL)
    with zfs_server() as url:
        zfs_ops.push(url, LOCAL, DATASET, BUFFER, archive=False)
    assert f"{REMOTE}/{DATASET}/archive" not in fake.snapshots
    assert_replicated(fake, LOCAL, REMOTE, parts=("", "/statements"))
    # only the parts sent got a snapshot
    assert fake.names(f"{LOCAL}/{DATASET}/archive") == []


def test_push_refuses_to_destroy_newer_target_snapshots(fake):
    make_dataset(fake, LOCAL)
    with zfs_server() as url:
        zfs_ops.push(url, LOCAL, DATASET, BUFFER)
        fake.zfs_snapshot(f"{REMOTE}/{DATASET}@remote_only")
        fake.write(f"{LOCAL}/{DATASET}", b"v2")
        snapshot_all(fake, LOCAL, "s2", parts=("",))
        base_only = {"archive": False, "statements": False, "snapshot": "s2"}
        with pytest.raises(ValueError, match="remote_only"):
            zfs_ops.push(url, LOCAL, DATASET, BUFFER, **base_only)
        zfs_ops.push(url, LOCAL, DATASET, BUFFER, force=True, **base_only)
    assert "remote_only" not in fake.names(f"{REMOTE}/{DATASET}")
    assert_replicated(fake, LOCAL, REMOTE, parts=("",))


def test_push_resumes_an_interrupted_receive(fake):
    make_dataset(fake, LOCAL)
    snapshot_all(fake, LOCAL, "s1", parts=("",))
    with zfs_server() as url:
        interrupted_push(fake, url, "s1")
        zfs_ops.push(
            url, LOCAL, DATASET, BUFFER, archive=False, statements=False, snapshot="s1"
        )
    assert fake.zfs_status(f"{REMOTE}/{DATASET}")["resume_token"] is None
    assert_replicated(fake, LOCAL, REMOTE, parts=("",))


def test_resume_refused_once_the_target_moved_on(fake):
    """A snapshot taken on the target after an interrupted receive would be
    destroyed by the resumed ``receive -F`` – that needs force, too."""
    make_dataset(fake, LOCAL)
    base_only = {"archive": False, "statements": False}
    with zfs_server() as url:
        zfs_ops.push(url, LOCAL, DATASET, BUFFER, **base_only)
        fake.write(f"{LOCAL}/{DATASET}", b"v2")
        snapshot_all(fake, LOCAL, "s2", parts=("",))
        interrupted_push(fake, url, "s2")
        fake.zfs_snapshot(f"{REMOTE}/{DATASET}@keep")

        with pytest.raises(ValueError, match="keep"):
            zfs_ops.push(url, LOCAL, DATASET, BUFFER, snapshot="s2", **base_only)
        assert "keep" in fake.names(f"{REMOTE}/{DATASET}")

        # force: the interrupted receive is discarded, `keep` knowingly lost
        zfs_ops.push(
            url, LOCAL, DATASET, BUFFER, snapshot="s2", force=True, **base_only
        )
    assert "keep" not in fake.names(f"{REMOTE}/{DATASET}")
    assert_replicated(fake, LOCAL, REMOTE, parts=("",))


def test_unresumable_receive_is_discarded_with_force(fake):
    """A token whose snapshot is gone can't be resumed – without an abort it
    would block the part for good."""
    make_dataset(fake, LOCAL)
    snapshot_all(fake, LOCAL, "s1", parts=("",))
    base_only = {"archive": False, "statements": False, "snapshot": "s1"}
    with zfs_server() as url:
        interrupted_push(fake, url, "s1")
        fake.pending.clear()  # e.g. the source snapshot was pruned meanwhile
        with pytest.raises(RuntimeError, match="use force"):
            zfs_ops.push(url, LOCAL, DATASET, BUFFER, **base_only)
        zfs_ops.push(url, LOCAL, DATASET, BUFFER, force=True, **base_only)
    assert fake.zfs_status(f"{REMOTE}/{DATASET}")["resume_token"] is None
    assert_replicated(fake, LOCAL, REMOTE, parts=("",))


def test_server_checks_the_base_before_reading(fake):
    """The server doesn't rely on the client's plan: an incremental onto a
    target that moved past its base is refused before the body is read."""
    make_dataset(fake, LOCAL)
    with zfs_server() as url:
        zfs_ops.push(url, LOCAL, DATASET, BUFFER)
        fake.zfs_snapshot(f"{REMOTE}/{DATASET}@newer")
        old_base = fake.snapshots[f"{REMOTE}/{DATASET}"][0]["guid"]
        before = list(fake.snapshots[f"{REMOTE}/{DATASET}"])
        receives = [e for e in fake.events if e[0] == "receive"]
        with raw(url) as client:
            res = client.put(
                f"/{DATASET}/_api/zfs/base/receive",
                params={"base": old_base},
                content=b"x" * 1000,
            )
    assert res.status_code == 409
    assert "newer" in res.json()["detail"]
    assert fake.snapshots[f"{REMOTE}/{DATASET}"] == before
    assert [e for e in fake.events if e[0] == "receive"] == receives


def test_replace_for_targets_without_snapshots(fake):
    """A target the other side pre-created (an api read, ``_api/ensure``)
    exists without snapshots: replacing it takes ``replace`` – not ``force``,
    which would also lift the newer-snapshot guard."""
    make_dataset(fake, LOCAL)
    make_dataset(fake, REMOTE, content=b"pre")
    with zfs_server() as url:
        with pytest.raises(ValueError, match="use replace"):
            zfs_ops.push(url, LOCAL, DATASET, BUFFER, force=True)
        zfs_ops.push(url, LOCAL, DATASET, BUFFER, replace=True)
    assert_replicated(fake, LOCAL, REMOTE)


def test_refusal_leaves_every_part_as_it_was(fake):
    """All parts are planned before anything moves – a refused part doesn't
    leave the others at the new snapshot."""
    make_dataset(fake, LOCAL)
    with zfs_server() as url:
        zfs_ops.push(url, LOCAL, DATASET, BUFFER)
        before = {p: list(fake.snapshots[f"{REMOTE}/{DATASET}{p}"]) for p in PARTS}
        fake.zfs_snapshot(f"{REMOTE}/{DATASET}/archive@remote_only")
        before["/archive"] = list(fake.snapshots[f"{REMOTE}/{DATASET}/archive"])
        snapshot_all(fake, LOCAL, "s2")
        with pytest.raises(ValueError, match="remote_only"):
            zfs_ops.push(url, LOCAL, DATASET, BUFFER, snapshot="s2")
    assert {p: fake.snapshots[f"{REMOTE}/{DATASET}{p}"] for p in PARTS} == before


def test_pull_full_then_incremental(fake):
    make_dataset(fake, REMOTE)
    with zfs_server() as url:
        zfs_ops.pull(url, LOCAL, DATASET, BUFFER)
        assert_replicated(fake, REMOTE, LOCAL)
        assert fake.props[f"{LOCAL}/{DATASET}"] == PART_PROPS["base"]

        fake.write(f"{REMOTE}/{DATASET}/statements", b"v2" * 1000)
        snapshot_all(fake, REMOTE, "s2", parts=("", "/statements"))
        zfs_ops.pull(url, LOCAL, DATASET, BUFFER, archive=False, snapshot="s2")
        assert_replicated(fake, REMOTE, LOCAL, parts=("", "/statements"))
        assert len(fake.names(f"{LOCAL}/{DATASET}")) == 2
        assert len(fake.names(f"{LOCAL}/{DATASET}/archive")) == 1


def test_errors_carry_the_zfs_message(fake):
    make_dataset(fake, LOCAL)
    with zfs_server() as url:
        with pytest.raises(ValueError, match="no snapshot `nope`"):
            zfs_ops.push(url, LOCAL, DATASET, BUFFER, snapshot="nope")
        # a send error before the first byte is a 409 with zfs's message
        with pytest.raises(RuntimeError, match=r"no remote/lake/my_dataset@nope"):
            fake.create(f"{REMOTE}/{DATASET}")
            with patch.object(
                zfs_client,
                "plan_transfer",
                lambda *a, **kw: zfs_ops.Transfer(snapshot="nope"),
            ):
                zfs_ops.pull(url, "pulled/lake", DATASET, BUFFER, snapshot="nope")


def test_client_abort_ends_the_send(fake):
    """A client gone mid-download never closes the response's iterator –
    the send must end anyway rather than block on a full pipe for good."""
    done = threading.Event()

    def endless_send(dataset, fd, **kwargs):
        try:
            while True:
                os.write(fd, b"x" * 65536)
        except BrokenPipeError:
            raise RuntimeError("zfs send failed: broken pipe")
        finally:
            done.set()

    with patch.object(zfs_stream, "zfs_send", endless_send), zfs_server() as url:
        with raw(url) as client:
            endpoint = f"/{DATASET}/_api/zfs/base/send"
            with client.stream("GET", endpoint, params={"snapshot": "s1"}) as res:
                next(res.iter_raw())
        assert done.wait(10)


def test_receive_failing_midstream_reports_its_error(fake):
    """A receive that gives up after the upload started: the server reads
    the rest of the body rather than leave the client blocked writing, so
    the client gets zfs's message – and the server keeps serving."""
    make_dataset(fake, LOCAL)

    def failing_receive(dataset, fd, **kwargs):
        os.read(fd, 10)
        raise RuntimeError("zfs receive failed: out of space")

    with patch.object(zfs_stream, "zfs_receive", failing_receive), zfs_server() as url:
        with pytest.raises(RuntimeError, match="409 .*out of space"):
            zfs_ops.push(url, LOCAL, DATASET, BUFFER)
        with raw(url) as client:
            assert client.get(f"/{DATASET}/_api/zfs").status_code == 200


GRANIAN_APP = """
import os
from ftm_lakehouse.api import main as api_main
from ftm_lakehouse.core.zfs import main as zfs_main
from ftm_lakehouse.core.zfs import stream as zfs_stream

def status(dataset):
    return {"exists": False, "snapshots": [], "resume_token": None}

def failing_receive(dataset, fd, **kwargs):
    os.read(fd, 10)
    raise RuntimeError("zfs receive failed: out of space")

zfs_main.zfs_status = status
zfs_stream.zfs_receive = failing_receive
api_main.settings.zfs_pool = "remote/lake"
app = api_main.get_zfs_app()
"""


def test_granian_rejected_receive_does_not_wedge(tmp_path):
    """Under granian (the production server), a receive that fails early
    must neither leave the client blocked writing nor tie up the worker:
    granian stops reading an unconsumed body without closing the
    connection, so the route has to read it."""
    pytest.importorskip("granian")
    (tmp_path / "wedge_app.py").write_text(GRANIAN_APP)
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        port = sock.getsockname()[1]
    env = {**os.environ, "PYTHONPATH": str(tmp_path)}
    server = subprocess.Popen(
        [sys.executable, "-m", "granian", "--interface", "asgi"]
        + ["--port", str(port), "--backpressure", "2", "wedge_app:app"],
        env=env,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    url = f"http://127.0.0.1:{port}"
    try:
        for _ in range(100):
            try:
                httpx.get(f"{url}/{DATASET}/_api/zfs", timeout=1)
                break
            except httpx.TransportError:
                time.sleep(0.1)

        def body():
            for _ in range(64):
                yield b"x" * (1 << 20)

        with httpx.Client(base_url=url, timeout=20) as client:
            for _ in range(3):  # more than granian's backpressure permits
                res = client.put(f"/{DATASET}/_api/zfs/base/receive", content=body())
                assert res.status_code == 409
                assert "out of space" in res.json()["detail"]
            assert client.get(f"/{DATASET}/_api/zfs").status_code == 200
    finally:
        server.terminate()
        stopped = server.wait(timeout=15)
    assert stopped is not None


def test_status_routes(fake):
    make_dataset(fake, REMOTE)
    fake.zfs_snapshot(f"{REMOTE}/{DATASET}@s1")
    with zfs_server() as url:
        status = zfs_ops.remote_status(url, DATASET)
    assert set(status) == {"base", "archive", "statements"}
    assert [s["name"] for s in status["base"]["snapshots"]] == ["s1"]
    assert status["archive"]["snapshots"] == []


def test_invalid_names_rejected(fake):
    """By the server itself, not just the client's own check."""
    with zfs_server() as url, raw(url) as client:
        for name in ("Invalid_Name", "catalog", "default"):
            assert client.get(f"/{name}/_api/zfs").status_code == 400, name
        res = client.get(f"/{DATASET}/_api/zfs/journal/send?snapshot=s1")
        assert res.status_code == 400
        res = client.put(f"/{DATASET}/_api/zfs/journal/receive", content=b"x")
        assert res.status_code == 400
    with pytest.raises(ValueError):
        zfs_ops.remote_status("http://unused", "catalog")


def test_unavailable_agent_is_a_503(fake):
    """Not a 404 via the FileNotFoundError handler, not a retried 500."""

    def no_agent(dataset):
        raise FileNotFoundError(2, "No such file or directory")

    with patch.object(zfs_main, "zfs_status", no_agent), zfs_server() as url:
        with raw(url) as client:
            res = client.get(f"/{DATASET}/_api/zfs")
        with pytest.raises(RuntimeError, match="503 .*zfs-agent unavailable"):
            zfs_ops.remote_status(url, DATASET)
    assert res.status_code == 503
    assert "zfs-agent unavailable" in res.json()["detail"]


def test_receive_clears_the_repository_caches(fake):
    """A received ``config.yml`` may change ``shards``: repositories built
    before must not keep serving the old layout."""
    make_dataset(fake, LOCAL)
    with patch("ftm_lakehouse.api.routes.zfs.clear_caches") as mock_clear:
        with zfs_server() as url:
            zfs_ops.push(url, LOCAL, DATASET, BUFFER)
    assert mock_clear.call_count == len(PARTS)


class FlushRecorder:
    """Stands in for the entity repository; records its flush."""

    def __init__(self, fake: FakeZfs) -> None:
        self.fake = fake

    def flush(self) -> int:
        self.fake.events.append(("flush",))
        return 0


def test_pull_flushes_the_journal_before_the_snapshot(fake):
    """Statements still in the journal belong in the replica too."""
    make_dataset(fake, REMOTE)
    with (
        patch("ftm_lakehouse.api.routes.zfs.dataset_exists", return_value=True),
        patch(
            "ftm_lakehouse.api.routes.zfs.get_entities",
            return_value=FlushRecorder(fake),
        ),
        zfs_server() as url,
    ):
        zfs_ops.pull(url, "pulled/lake", DATASET, BUFFER)
    assert fake.events[0] == ("flush",)
    assert fake.events[1][0] == "snapshot"


def test_push_flushes_the_journal_before_the_snapshot(fake):
    make_dataset(fake, LOCAL)
    with (
        patch("ftm_lakehouse.cli.zfs.dataset_exists", return_value=True),
        patch("ftm_lakehouse.cli.zfs.get_entities", return_value=FlushRecorder(fake)),
        zfs_server() as url,
    ):
        res = runner.invoke(cli, ["-d", DATASET, "zfs", "push", url, "-p", LOCAL])
        assert res.exit_code == 0, res.output
    assert fake.events[0] == ("flush",)
    assert fake.events[1][0] == "snapshot"


def test_refused_run_takes_no_snapshot(fake):
    """A run refused by the target is refused before the snapshot – no
    orphan left behind, and a retry isn't tripped by its name."""
    make_dataset(fake, LOCAL)
    make_dataset(fake, REMOTE, content=b"pre")
    with zfs_server() as url:
        with pytest.raises(ValueError, match="use replace"):
            zfs_ops.push(url, LOCAL, DATASET, BUFFER)
    assert fake.names(f"{LOCAL}/{DATASET}") == []
    assert not any(e[0] == "snapshot" for e in fake.events)


# --- mode 1: mounted into the lakehouse api ---


def test_api_mounts_zfs_routes_only_when_enabled(fake, tmp_path, monkeypatch):
    make_dataset(fake, REMOTE)
    monkeypatch.setattr(api_main.settings, "zfs_pool", REMOTE)

    monkeypatch.setattr(api_main.settings, "zfs_api", False)
    with live_test_api_server(get_app(lake_uri=str(tmp_path))) as url:
        with raw(url) as client:
            assert client.get(f"/{DATASET}/_api/zfs").status_code == 404

    monkeypatch.setattr(api_main.settings, "zfs_api", True)
    monkeypatch.setattr(api_main.settings, "on_zfs", True)
    with patch.object(api_main, "ensure_zfs_dataset") as mock_ensure:
        with live_test_api_server(get_app(lake_uri=str(tmp_path))) as url:
            zfs_ops.pull(url, LOCAL, DATASET, BUFFER)
            fake.write(f"{LOCAL}/{DATASET}", b"v2")
            snapshot_all(fake, LOCAL, "s2")
            zfs_ops.push(url, LOCAL, DATASET, BUFFER, snapshot="s2")
            # the replication writes must not pre-create the datasets
            mock_ensure.assert_not_called()
            with raw(url) as client:
                client.post(f"/{DATASET}/_api/ensure")
            mock_ensure.assert_called_once_with(REMOTE, DATASET)
    assert_replicated(fake, LOCAL, REMOTE)


def test_api_zfs_needs_a_pool(monkeypatch, tmp_path):
    monkeypatch.setattr(api_main.settings, "zfs_api", True)
    monkeypatch.setattr(api_main.settings, "zfs_pool", None)
    with pytest.raises(RuntimeError, match="LAKEHOUSE_ZFS_POOL"):
        get_app(lake_uri=str(tmp_path))


# --- cli ---


runner = CliRunner()


def test_cli_push_status_pull(fake, monkeypatch):
    make_dataset(fake, LOCAL)
    with zfs_server() as url:
        res = runner.invoke(
            cli,
            ["-d", DATASET, "zfs", "push", url, "--pool", LOCAL, "--buffer", "8MiB"],
        )
        assert res.exit_code == 0, res.output
        assert_replicated(fake, LOCAL, REMOTE)

        res = runner.invoke(cli, ["-d", DATASET, "zfs", "status", url, "-p", LOCAL])
        assert res.exit_code == 0, res.output
        status = json.loads(res.stdout)
        assert status["local"]["base"] == status["remote"]["base"]

        monkeypatch.setenv("LAKEHOUSE_ZFS_POOL", "third/lake")
        pushed = fake.names(f"{REMOTE}/{DATASET}")[-1]
        res = runner.invoke(
            cli,
            ["-d", DATASET, "zfs", "pull", url, "--no-archive", "--snapshot", pushed],
        )
        assert res.exit_code == 0, res.output
        assert f"third/lake/{DATASET}/archive" not in fake.snapshots
        assert_replicated(fake, REMOTE, "third/lake", parts=("", "/statements"))


def test_cli_errors(fake):
    res = runner.invoke(cli, ["zfs", "push", "http://x", "-p", LOCAL])
    assert res.exit_code == 1
    # tests run with DEBUG=1, which re-raises instead of printing
    with zfs_server() as url:
        res = runner.invoke(cli, ["-d", DATASET, "zfs", "push", url, "-p", LOCAL])
    assert res.exit_code == 1
    assert isinstance(res.exception, RuntimeError)
    assert "zfs snapshot failed" in str(res.exception)


@patch("ftm_lakehouse.cli.zfs.ensure_zfs_dataset")
def test_cli_init(mock_ensure):
    res = runner.invoke(cli, ["-d", DATASET, "zfs", "init", "--pool", LOCAL])
    assert res.exit_code == 0, res.output
    mock_ensure.assert_called_once_with(LOCAL, DATASET)

    res = runner.invoke(cli, ["zfs", "init", "--pool", LOCAL])
    assert res.exit_code == 1
    assert mock_ensure.call_count == 1


def test_cli_errors_without_debug(fake, monkeypatch):
    """Outside DEBUG, ``ZfsContext`` turns a failure into a plain exit 1."""
    monkeypatch.setattr("ftm_lakehouse.cli.zfs.settings.debug", False)
    res = runner.invoke(cli, ["zfs", "status", "-p", LOCAL])
    assert res.exit_code == 1
    assert isinstance(res.exception, SystemExit)
    with zfs_server() as url:
        res = runner.invoke(cli, ["-d", DATASET, "zfs", "push", url, "-p", LOCAL])
    assert res.exit_code == 1
    assert isinstance(res.exception, SystemExit)  # not the RuntimeError
