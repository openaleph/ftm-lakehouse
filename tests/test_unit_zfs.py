import socket
import threading
from unittest.mock import patch

import orjson
import pytest

from ftm_lakehouse.core.zfs.agent import (
    handle_connection,
    handle_request,
    validate_dataset,
)
from ftm_lakehouse.core.zfs.helpers import zfs_create, zfs_create_socket

# --- Validation tests ---


class TestValidateDataset:
    def test_valid_name(self):
        assert validate_dataset("tank/lakehouse/my_dataset/archive", None) is None

    def test_valid_with_pool(self):
        assert validate_dataset("tank/lakehouse/ds", "tank/lakehouse") is None

    def test_empty_name(self):
        assert validate_dataset("", None) is not None

    def test_invalid_chars_in_leaf(self):
        assert validate_dataset("tank/ds; rm -rf /", None) is not None

    def test_invalid_chars_in_parent(self):
        assert validate_dataset("tank/ds; rm/leaf", None) is not None

    def test_path_traversal(self):
        assert validate_dataset("tank/../etc/shadow", None) is not None

    def test_pool_mismatch(self):
        err = validate_dataset("other/pool/ds", "tank/lakehouse")
        assert err is not None
        assert "not under pool" in err

    def test_hyphens_allowed_in_parents(self):
        assert validate_dataset("tank/lakehouse-dev/my_dataset", None) is None

    def test_dots_allowed_in_parents(self):
        assert validate_dataset("tank/lake.house/my_dataset", None) is None

    def test_uppercase_allowed_in_parents(self):
        assert validate_dataset("Tank/Lakehouse/my_dataset", None) is None

    def test_hyphens_rejected_in_leaf(self):
        assert validate_dataset("tank/my-dataset", None) is not None

    def test_uppercase_rejected_in_leaf(self):
        assert validate_dataset("tank/MyDataset", None) is not None


# --- Request handler tests ---


class TestHandleRequest:
    @patch("ftm_lakehouse.core.zfs.agent.zfs_create_local")
    def test_create_success(self, mock_create):
        resp = handle_request(
            {
                "action": "create",
                "dataset": "tank/ds",
                "props": {"compression": "zstd"},
            },
            None,
        )
        assert resp == {"ok": True}
        mock_create.assert_called_once_with(
            "tank/ds", {"compression": "zstd"}, exist_ok=True, owner=None
        )

    @patch("ftm_lakehouse.core.zfs.agent.zfs_create_local")
    def test_create_with_owner(self, mock_create):
        resp = handle_request(
            {
                "action": "create",
                "dataset": "tank/ds",
                "props": {"compression": "zstd"},
            },
            None,
            owner="1000:1000",
        )
        assert resp == {"ok": True}
        mock_create.assert_called_once_with(
            "tank/ds", {"compression": "zstd"}, exist_ok=True, owner="1000:1000"
        )

    @patch("ftm_lakehouse.core.zfs.agent.zfs_create_local")
    def test_create_no_props(self, mock_create):
        resp = handle_request({"action": "create", "dataset": "tank/ds"}, None)
        assert resp == {"ok": True}
        mock_create.assert_called_once_with("tank/ds", {}, exist_ok=True, owner=None)

    def test_unknown_action(self):
        resp = handle_request({"action": "destroy", "dataset": "tank/ds"}, None)
        assert resp["ok"] is False
        assert "unknown action" in resp["error"]

    def test_invalid_dataset_rejected(self):
        resp = handle_request(
            {"action": "create", "dataset": "tank/ds; rm -rf /"},
            None,
        )
        assert resp["ok"] is False
        assert "invalid path component" in resp["error"]

    def test_pool_enforced(self):
        resp = handle_request(
            {"action": "create", "dataset": "rogue/pool"},
            "tank/lakehouse",
        )
        assert resp["ok"] is False
        assert "not under pool" in resp["error"]

    @patch(
        "ftm_lakehouse.core.zfs.agent.zfs_create_local",
        side_effect=RuntimeError("boom"),
    )
    def test_create_failure_forwarded(self, _mock_create):
        resp = handle_request(
            {"action": "create", "dataset": "tank/ds"},
            None,
        )
        assert resp["ok"] is False
        assert "boom" in resp["error"]


# --- Socket integration tests ---


def _make_socketpair():
    """Create a connected pair of Unix sockets."""
    return socket.socketpair(socket.AF_UNIX, socket.SOCK_STREAM)


class TestHandleConnection:
    @patch("ftm_lakehouse.core.zfs.agent.zfs_create_local")
    def test_roundtrip(self, _mock_create):
        client, server_conn = _make_socketpair()
        try:
            request = orjson.dumps({"action": "create", "dataset": "tank/ds"})
            client.sendall(request + b"\n")
            client.shutdown(socket.SHUT_WR)

            handle_connection(server_conn, None)

            response = orjson.loads(client.makefile().readline())
            assert response["ok"] is True
        finally:
            client.close()

    @patch("ftm_lakehouse.core.zfs.agent.zfs_create_local")
    def test_invalid_json(self, _mock_create):
        client, server_conn = _make_socketpair()
        try:
            client.sendall(b"not json\n")
            client.shutdown(socket.SHUT_WR)

            handle_connection(server_conn, None)

            response = orjson.loads(client.makefile().readline())
            assert response["ok"] is False
            assert "invalid JSON" in response["error"]
        finally:
            client.close()


class TestZfsCreateSocket:
    """Test zfs_create_socket against a mock agent running in a thread."""

    def test_success(self, tmp_path):
        sock_path = str(tmp_path / "test.sock")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(sock_path)
        server.listen(1)

        def agent():
            conn, _ = server.accept()
            data = orjson.loads(conn.makefile().readline())
            assert data["action"] == "create"
            assert data["dataset"] == "tank/test"
            conn.sendall(orjson.dumps({"ok": True}) + b"\n")
            conn.close()

        t = threading.Thread(target=agent)
        t.start()

        zfs_create_socket(sock_path, "tank/test", {"compression": "zstd"})

        t.join(timeout=5)
        server.close()

    def test_error_response(self, tmp_path):
        sock_path = str(tmp_path / "test.sock")
        server = socket.socket(socket.AF_UNIX, socket.SOCK_STREAM)
        server.bind(sock_path)
        server.listen(1)

        def agent():
            conn, _ = server.accept()
            conn.makefile().readline()
            conn.sendall(
                orjson.dumps({"ok": False, "error": "permission denied"}) + b"\n"
            )
            conn.close()

        t = threading.Thread(target=agent)
        t.start()

        with pytest.raises(RuntimeError, match="permission denied"):
            zfs_create_socket(sock_path, "tank/test")

        t.join(timeout=5)
        server.close()


class TestZfsCreateDispatch:
    """Test that zfs_create dispatches to socket or local based on settings."""

    @patch("ftm_lakehouse.core.zfs.helpers.zfs_create_local")
    @patch("ftm_lakehouse.core.zfs.helpers.Settings")
    def test_dispatch_local(self, mock_settings_cls, mock_local):
        mock_settings_cls.return_value.zfs_socket = None
        mock_settings_cls.return_value.zfs_owner = None
        zfs_create("tank/ds", {"compression": "zstd"})
        mock_local.assert_called_once_with(
            "tank/ds", {"compression": "zstd"}, True, None
        )

    @patch("ftm_lakehouse.core.zfs.helpers.zfs_create_local")
    @patch("ftm_lakehouse.core.zfs.helpers.Settings")
    def test_dispatch_local_with_owner(self, mock_settings_cls, mock_local):
        mock_settings_cls.return_value.zfs_socket = None
        mock_settings_cls.return_value.zfs_owner = "1000:1000"
        zfs_create("tank/ds", {"compression": "zstd"})
        mock_local.assert_called_once_with(
            "tank/ds", {"compression": "zstd"}, True, "1000:1000"
        )

    @patch("ftm_lakehouse.core.zfs.helpers.zfs_create_socket")
    @patch("ftm_lakehouse.core.zfs.helpers.Settings")
    def test_dispatch_socket(self, mock_settings_cls, mock_socket):
        mock_settings_cls.return_value.zfs_socket = "/run/zfs.sock"
        zfs_create("tank/ds", {"compression": "zstd"})
        mock_socket.assert_called_once_with(
            "/run/zfs.sock", "tank/ds", {"compression": "zstd"}
        )
