import pytest

from ftm_lakehouse import util


def test_util():
    ch = "5a6acf229ba576d9a40b09292595658bbb74ef56"
    assert util.make_checksum_key(ch) == f"5a/6a/cf/{ch}"
    with pytest.raises(ValueError):
        util.make_checksum_key("abcde")

    assert util.render("{{ foo }}", {"foo": "bar"}) == "bar"
