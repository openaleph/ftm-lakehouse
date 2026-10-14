"""Lakehouse-specific ZFS provisioning.

The transport (subprocess / socket agent, chown, peer auth) is the external
``zfs-agent`` package and tested there – these tests cover only what stays in
the lakehouse: the per-storage-type tuning and the composition of the
``ensure_zfs_dataset`` hierarchy.
"""

from unittest.mock import call, patch

import pytest

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.settings import Settings
from ftm_lakehouse.core.zfs import (
    ARCHIVE,
    PARENT_PROPS,
    STATEMENTS,
    DatasetConfig,
    ensure_zfs_dataset,
)
from ftm_lakehouse.repository.base import ensure_zfs


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


@patch("ftm_lakehouse.core.zfs.zfs_create")
def test_ensure_zfs_dataset_hierarchy(mock_create):
    """One parent + one child per storage type, each with its tuning."""
    ensure_zfs_dataset("tank/lake", "my_dataset")
    assert mock_create.call_args_list == [
        call("tank/lake/my_dataset", **PARENT_PROPS),
        call(f"tank/lake/my_dataset/{path.ARCHIVE}", **ARCHIVE.to_props()),
        call(f"tank/lake/my_dataset/{path.STATEMENTS}", **STATEMENTS.to_props()),
    ]


@patch("ftm_lakehouse.core.zfs.zfs_create")
def test_ensure_zfs_dataset_cached_per_process(mock_create):
    ensure_zfs_dataset("tank/lake", "my_dataset")
    ensure_zfs_dataset("tank/lake", "my_dataset")
    assert mock_create.call_count == 3  # only the first call fires

    ensure_zfs_dataset("tank/lake", "other_dataset")
    assert mock_create.call_count == 6


@patch("ftm_lakehouse.core.zfs.zfs_create")
def test_ensure_zfs_dataset_rejects_invalid_names(mock_create):
    with pytest.raises(ValueError):
        ensure_zfs_dataset("tank/lake", "Invalid Name!")
    mock_create.assert_not_called()


@patch("ftm_lakehouse.core.zfs.zfs_create")
def test_ensure_zfs_dataset_statements_pool(mock_create):
    """With a statements pool, the statements child sits there under a parent
    of its own."""
    ensure_zfs_dataset("tank/lake", "my_dataset", "ssd/lake")
    assert mock_create.call_args_list == [
        call("tank/lake/my_dataset", **PARENT_PROPS),
        call(f"tank/lake/my_dataset/{path.ARCHIVE}", **ARCHIVE.to_props()),
        call("ssd/lake/my_dataset", **PARENT_PROPS),
        call(f"ssd/lake/my_dataset/{path.STATEMENTS}", **STATEMENTS.to_props()),
    ]


@pytest.mark.parametrize(
    "env",
    [
        {"LAKEHOUSE_STATEMENTS_URI": "/ssd/lake"},
        {"LAKEHOUSE_ZFS_STATEMENTS_POOL": "ssd/lake"},
    ],
)
def test_settings_zfs_statements_pool_pairs(monkeypatch, env):
    """On ZFS the statements uri and pool come together – either alone would
    provision the statements child where its table is not."""
    monkeypatch.setenv("LAKEHOUSE_ON_ZFS", "1")
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    with pytest.raises(ValueError, match="together"):
        Settings()

    monkeypatch.setenv("LAKEHOUSE_STATEMENTS_URI", "/ssd/lake")
    monkeypatch.setenv("LAKEHOUSE_ZFS_STATEMENTS_POOL", "ssd/lake")
    assert Settings().zfs_statements_pool == "ssd/lake"
    monkeypatch.setenv("LAKEHOUSE_ON_ZFS", "0")
    monkeypatch.delenv("LAKEHOUSE_ZFS_STATEMENTS_POOL")
    assert Settings().statements_uri == "/ssd/lake"  # off ZFS, no pool needed


def test_ensure_zfs_passes_statements_pool(tmp_path, monkeypatch):
    """A local dataset provisions its statements child in the statements pool."""
    monkeypatch.setenv("LAKEHOUSE_ON_ZFS", "1")
    monkeypatch.setenv("LAKEHOUSE_ZFS_POOL", "tank/lake")
    monkeypatch.setenv("LAKEHOUSE_STATEMENTS_URI", str(tmp_path / "ssd"))
    monkeypatch.setenv("LAKEHOUSE_ZFS_STATEMENTS_POOL", "ssd/lake")
    with patch("ftm_lakehouse.repository.base.ensure_zfs_dataset") as ensure:
        ensure_zfs.__wrapped__("my_dataset", str(tmp_path / "hdd" / "my_dataset"))
    ensure.assert_called_once_with("tank/lake", "my_dataset", "ssd/lake")
