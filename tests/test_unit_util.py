import pytest

from ftm_lakehouse import util


def test_util():
    ch = "bbb1f047ff1f0c333560e09cff0c4a052eb87a2998d6d16775a276645877c5b7"
    assert util.make_checksum_key(ch) == f"bb/b1/f0/{ch}"
    with pytest.raises(ValueError):
        util.make_checksum_key("abcde")

    assert util.render("{{ foo }}", {"foo": "bar"}) == "bar"


def test_util_parse_byte_size():
    assert util.parse_byte_size("1024") == 1024
    assert util.parse_byte_size("8GB") == 8 * 10**9
    assert util.parse_byte_size("8gb") == 8 * 10**9
    assert util.parse_byte_size(" 64G ") == 64 * 10**9
    assert util.parse_byte_size("512 MiB") == 512 * 2**20
    assert util.parse_byte_size("1.5GB") == 1_500_000_000
    assert util.parse_byte_size("2TiB") == 2 * 2**40
    with pytest.raises(ValueError):
        util.parse_byte_size("80%")
    with pytest.raises(ValueError):
        util.parse_byte_size("GB")
    with pytest.raises(ValueError):
        util.parse_byte_size("8 flops")
