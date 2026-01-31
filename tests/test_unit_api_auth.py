"""Unit tests for TokenData.allows - method and path prefix matching."""

from ftm_lakehouse.api.auth import TokenData


class TestTokenDataAllows:
    """Test cases from the auth module docstring examples."""

    def test_empty_defaults_deny_all(self):
        token = TokenData()
        assert not token.allows("GET", "/")
        assert not token.allows("GET", "/dataset/archive/foo")

    def test_allow_all(self):
        token = TokenData(methods=["*"], prefixes=["/"])
        assert token.allows("GET", "/")
        assert token.allows("POST", "/dataset/archive/foo")
        assert token.allows("DELETE", "/anything")

    def test_read_only(self):
        token = TokenData(methods=["GET", "HEAD", "OPTIONS"], prefixes=["/"])
        assert token.allows("GET", "/dataset/archive/foo")
        assert token.allows("HEAD", "/dataset/tags")
        assert token.allows("OPTIONS", "/")
        assert not token.allows("POST", "/dataset/archive/foo")
        assert not token.allows("DELETE", "/dataset/tags")

    def test_archive_glob_all_datasets(self):
        token = TokenData(methods=["*"], prefixes=["/*/archive/*"])
        assert token.allows("GET", "/dataset_1/archive/foo")
        assert token.allows("POST", "/dataset_2/archive/bar")
        assert not token.allows("GET", "/dataset_1/tags/foo")
        assert not token.allows("GET", "/")

    def test_specific_dataset_tags(self):
        token = TokenData(
            methods=["*"],
            prefixes=["/dataset_1/tags", "/dataset_2/tags"],
        )
        assert token.allows("GET", "/dataset_1/tags")
        assert token.allows("GET", "/dataset_1/tags/foo")
        assert token.allows("POST", "/dataset_2/tags/bar")
        assert not token.allows("GET", "/dataset_3/tags/foo")
        assert not token.allows("GET", "/dataset_1/archive/foo")

    def test_method_case_insensitive(self):
        token = TokenData(methods=["GET"], prefixes=["/"])
        assert token.allows("get", "/foo")
        assert token.allows("Get", "/foo")

    def test_wildcard_method_with_restricted_prefix(self):
        token = TokenData(methods=["*"], prefixes=["/only/here"])
        assert token.allows("GET", "/only/here")
        assert token.allows("GET", "/only/here/sub")
        assert not token.allows("GET", "/only/other")

    def test_no_methods_denies(self):
        token = TokenData(methods=[], prefixes=["/"])
        assert not token.allows("GET", "/")

    def test_no_prefixes_denies(self):
        token = TokenData(methods=["*"], prefixes=[])
        assert not token.allows("GET", "/")


class TestTokenDataFromPayload:
    def test_full_payload(self):
        data = TokenData.from_payload(
            {"methods": ["GET", "HEAD"], "prefixes": ["/foo", "/bar"]}
        )
        assert data.methods == ["GET", "HEAD"]
        assert data.prefixes == ["/foo", "/bar"]

    def test_empty_payload_defaults(self):
        data = TokenData.from_payload({})
        assert data.methods == []
        assert data.prefixes == []

    def test_partial_payload(self):
        data = TokenData.from_payload({"methods": ["*"]})
        assert data.methods == ["*"]
        assert data.prefixes == []
