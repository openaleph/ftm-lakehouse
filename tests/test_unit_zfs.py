"""Lakehouse-specific ZFS provisioning.

The transport (subprocess / socket agent, chown, peer auth) is the external
``zfs-agent`` package and tested there – these tests cover only what stays in
the lakehouse: the per-storage-type tuning and the composition of the
``ensure_zfs_dataset`` hierarchy.
"""

from unittest.mock import call, patch

import pytest

from ftm_lakehouse.core.conventions import path
from ftm_lakehouse.core.zfs import (
    ARCHIVE,
    PARENT_PROPS,
    STATEMENTS,
    DatasetConfig,
    ensure_zfs_dataset,
)


@pytest.fixture(autouse=True)
def clear_ensure_cache():
    ensure_zfs_dataset.cache_clear()
    yield
    ensure_zfs_dataset.cache_clear()


def test_dataset_configs():
    """The tuned per-storage-type properties are a deliberate contract."""
    assert ARCHIVE.to_props()["compression"] == "zstd-9"
    assert ARCHIVE.to_props()["recordsize"] == "128K"
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
