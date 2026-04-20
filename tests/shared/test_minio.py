"""
Unit tests for shared/minio.py

boto3 and zstandard are patched so no real MinIO connection is needed.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _mock_client(mocker, bucket_exists=True):
    """Return a mock boto3 S3 client with module-level singletons reset."""
    from botocore.exceptions import ClientError

    import shared.minio as mc
    # Reset the singleton client and bucket-check flag so each test starts clean.
    mocker.patch.object(mc, "_boto3_client", None)
    mocker.patch.object(mc, "_bucket_checked", False)
    mock_client = MagicMock()
    if not bucket_exists:
        error = ClientError(
            {"Error": {"Code": "404", "Message": "NoSuchBucket"}},
            "HeadBucket",
        )
        mock_client.head_bucket.side_effect = error
    mocker.patch("shared.minio.get_boto3_client", return_value=mock_client)
    return mock_client


# ---------------------------------------------------------------------------
# make_key
# ---------------------------------------------------------------------------

class TestMakeKey:
    def test_hive_partition_structure(self):
        from shared.minio import make_key
        dt = datetime(2026, 4, 19, 12, 0, 0, tzinfo=timezone.utc)
        key = make_key("results_page", dt, file_id="fixed-uuid")
        assert key == "html/year=2026/month=4/artifact_type=results_page/fixed-uuid.html.zst"

    def test_detail_page_type(self):
        from shared.minio import make_key
        dt = datetime(2026, 1, 3)
        key = make_key("detail_page", dt, file_id="abc")
        assert "artifact_type=detail_page" in key
        assert "year=2026/month=1" in key

    def test_generates_uuid_when_file_id_omitted(self):
        from shared.minio import make_key
        dt = datetime(2026, 4, 19)
        key1 = make_key("results_page", dt)
        key2 = make_key("results_page", dt)
        filename1 = key1.rsplit("/", 1)[-1]
        filename2 = key2.rsplit("/", 1)[-1]
        assert filename1.endswith(".html.zst")
        assert filename1 != filename2

    def test_parses_iso_string_fetched_at(self):
        from shared.minio import make_key
        key = make_key("results_page", "2026-07-15T08:30:00+00:00", file_id="x")
        assert "year=2026/month=7" in key

    def test_uses_local_datetime(self):
        from shared.minio import make_key
        dt = datetime(2026, 12, 1, 0, 0, 0)  # naive datetime
        key = make_key("results_page", dt, file_id="y")
        assert "year=2026/month=12" in key


# ---------------------------------------------------------------------------
# write_html
# ---------------------------------------------------------------------------

class TestWriteHtml:
    def test_returns_s3_uri(self, mocker):
        from shared.minio import BUCKET, write_html
        _mock_client(mocker)
        uri = write_html("html/year=2026/month=4/artifact_type=results_page/t.html.zst",
                         b"<html></html>")
        assert uri == f"s3://{BUCKET}/html/year=2026/month=4/artifact_type=results_page/t.html.zst"

    def test_put_object_called_with_correct_key(self, mocker):
        from shared.minio import BUCKET, write_html
        mock_c = _mock_client(mocker)
        key = "html/year=2026/month=4/artifact_type=detail_page/myfile.html.zst"
        write_html(key, b"<html></html>")
        mock_c.put_object.assert_called_once()
        call_kwargs = mock_c.put_object.call_args[1]
        assert call_kwargs["Key"] == key
        assert call_kwargs["Bucket"] == BUCKET

    def test_body_is_zstd_compressed(self, mocker):
        import zstandard as zstd
        mock_c = _mock_client(mocker)
        from shared.minio import write_html
        original = b"<html>test content</html>"
        write_html("html/test.html.zst", original)
        body = mock_c.put_object.call_args[1]["Body"]
        assert body != original
        decompressed = zstd.ZstdDecompressor().decompress(body)
        assert decompressed == original

    def test_ensures_bucket_exists(self, mocker):
        mock_c = _mock_client(mocker, bucket_exists=True)
        from shared.minio import write_html
        write_html("html/test.html.zst", b"data")
        mock_c.head_bucket.assert_called_once()


# ---------------------------------------------------------------------------
# read_html
# ---------------------------------------------------------------------------

class TestReadHtml:
    def _compressed(self, data: bytes) -> bytes:
        import zstandard as zstd
        return zstd.ZstdCompressor(level=3).compress(data)

    def test_decompresses_and_returns_bytes(self, mocker):
        original = b"<html>hello world</html>"
        compressed = self._compressed(original)
        mock_c = _mock_client(mocker)
        mock_body = MagicMock()
        mock_body.read.return_value = compressed
        mock_c.get_object.return_value = {"Body": mock_body}

        from shared.minio import read_html
        result = read_html("s3://bronze/html/year=2026/test.html.zst")
        assert result == original

    def test_parses_full_s3_uri_bucket_and_key(self, mocker):
        mock_c = _mock_client(mocker)
        mock_body = MagicMock()
        mock_body.read.return_value = self._compressed(b"x")
        mock_c.get_object.return_value = {"Body": mock_body}

        from shared.minio import read_html
        read_html("s3://mybucket/some/path/file.html.zst")
        mock_c.get_object.assert_called_once_with(Bucket="mybucket", Key="some/path/file.html.zst")

    def test_bare_key_uses_default_bucket(self, mocker):
        mock_c = _mock_client(mocker)
        mock_body = MagicMock()
        mock_body.read.return_value = self._compressed(b"x")
        mock_c.get_object.return_value = {"Body": mock_body}

        from shared.minio import BUCKET, read_html
        read_html("html/year=2026/month=4/test.html.zst")
        mock_c.get_object.assert_called_once_with(
            Bucket=BUCKET, Key="html/year=2026/month=4/test.html.zst"
        )


# ---------------------------------------------------------------------------
# ensure_bucket
# ---------------------------------------------------------------------------

class TestEnsureBucket:
    def test_creates_bucket_on_404(self, mocker):
        mock_c = _mock_client(mocker, bucket_exists=False)
        from shared.minio import ensure_bucket
        ensure_bucket()
        mock_c.create_bucket.assert_called_once()

    def test_does_not_create_bucket_when_exists(self, mocker):
        mock_c = _mock_client(mocker, bucket_exists=True)
        from shared.minio import ensure_bucket
        ensure_bucket()
        mock_c.create_bucket.assert_not_called()

    def test_non_404_client_error_propagates(self, mocker):
        from botocore.exceptions import ClientError

        import shared.minio as mc
        mocker.patch.object(mc, "_boto3_client", None)
        mocker.patch.object(mc, "_bucket_checked", False)
        mock_c = MagicMock()
        error = ClientError(
            {"Error": {"Code": "403", "Message": "Forbidden"}},
            "HeadBucket",
        )
        mock_c.head_bucket.side_effect = error
        mocker.patch("shared.minio.get_boto3_client", return_value=mock_c)

        from shared.minio import ensure_bucket
        with pytest.raises(ClientError):
            ensure_bucket()

    def test_checked_once_per_process(self, mocker):
        mock_c = _mock_client(mocker, bucket_exists=True)
        from shared.minio import ensure_bucket
        ensure_bucket()
        ensure_bucket()
        ensure_bucket()
        # head_bucket should only be called on the first invocation
        assert mock_c.head_bucket.call_count == 1
