import pytest

from ftm_lakehouse import util


def test_util():
    ch = "bbb1f047ff1f0c333560e09cff0c4a052eb87a2998d6d16775a276645877c5b7"
    assert util.make_checksum_key(ch) == f"bb/b1/f0/{ch}"
    with pytest.raises(ValueError):
        util.make_checksum_key("abcde")

    assert util.render("{{ foo }}", {"foo": "bar"}) == "bar"
