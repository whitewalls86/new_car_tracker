"""Unit tests for processors/scrape_detail.py"""
import pytest
from unittest.mock import MagicMock, mock_open, call

from processors.scrape_detail import scrape_detail_fetch, scrape_detail_dummy

# n8n reads all 13 of these keys from every artifact
N8N_ARTIFACT_KEYS = {
    "source",
    "artifact_type",
    "search_key",
    "search_scope",
    "page_num",
    "url",
    "fetched_at",
    "http_status",
    "content_type",
    "content_bytes",
    "sha256",
    "filepath",
    "error",
}

RUN_ID = "run-test-0000-0000-000000000001"
LISTING_ID = "listing-0000-0000-0000-000000000001"
VIN = "1HGCM82633A123456"


# ---------------------------------------------------------------------------
# scrape_detail_fetch
# ---------------------------------------------------------------------------

class TestScrapeDetailFetch:
    def test_missing_listing_id_returns_error(self, mocker):
        mocker.patch("os.makedirs")
        result = scrape_detail_fetch(run_id=RUN_ID, payload={})
        assert result["error"] == "payload.listing_id is required"
        assert result["artifacts"] == []

    def test_success_200(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"<html>detail</html>"
        mock_resp.url = f"https://www.cars.com/vehicledetail/{LISTING_ID}/"
        mock_resp.headers = {"content-type": "text/html; charset=utf-8"}

        result = scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID, "vin": VIN})

        assert result["error"] is None
        assert len(result["artifacts"]) == 1
        art = result["artifacts"][0]
        assert art["http_status"] == 200
        assert art["error"] is None
        assert len(art["sha256"]) == 64  # hex SHA-256

    def test_non_200_sets_error(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 403
        mock_resp.content = b"<html>blocked</html>"
        mock_resp.url = f"https://www.cars.com/vehicledetail/{LISTING_ID}/"
        mock_resp.headers = {"content-type": "text/html"}

        result = scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID})

        assert result["error"] == "HTTP 403"
        assert result["artifacts"][0]["error"] == "HTTP 403"
        assert result["artifacts"][0]["http_status"] == 403

    def test_url_defaults_from_listing_id(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = f"https://www.cars.com/vehicledetail/{LISTING_ID}/"
        mock_resp.headers = {}

        scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID})
        called_url = mock_session.get.call_args[0][0]
        assert called_url == f"https://www.cars.com/vehicledetail/{LISTING_ID}/"

    def test_url_override_used(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        custom_url = "https://www.cars.com/vehicledetail/custom-path/"
        mock_resp.url = custom_url
        mock_resp.headers = {}

        scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID, "url": custom_url})
        called_url = mock_session.get.call_args[0][0]
        assert called_url == custom_url

    def test_timeout_default_is_30(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {}

        scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID})
        kwargs = mock_session.get.call_args[1]
        assert kwargs["timeout"] == 30

    def test_timeout_override(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {}

        scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID, "timeout_s": 60})
        kwargs = mock_session.get.call_args[1]
        assert kwargs["timeout"] == 60

    def test_network_exception_writes_error_file(self, mocker):
        mocker.patch("os.makedirs")
        mock_open_fn = mock_open()
        mocker.patch("builtins.open", mock_open_fn)
        mock_session = MagicMock(get=MagicMock(side_effect=ConnectionError("refused")))
        mocker.patch(
            "processors.scrape_detail.cf_requests.Session",
            return_value=mock_session,
        )
        # Mock _get_cf_session to return cached session without bootstrap HTML
        mocker.patch(
            "processors.scrape_detail._get_cf_session",
            return_value=(mock_session, None, None),
        )

        result = scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID})

        assert "ConnectionError" in result["error"]
        assert result["artifacts"][0]["http_status"] is None
        # Error file must have been written
        open_calls = mock_open_fn.call_args_list
        assert any("ERROR.txt" in str(c) for c in open_calls)

    def test_vin_used_as_search_key(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {}

        result = scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID, "vin": VIN})
        assert result["artifacts"][0]["search_key"] == VIN

    def test_artifact_keys_match_n8n_contract(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {"content-type": "text/html"}

        result = scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID})
        art = result["artifacts"][0]
        missing = N8N_ARTIFACT_KEYS - art.keys()
        assert missing == set(), f"Artifact missing n8n fields: {missing}"

    def test_raw_base_from_env(self, mock_cf_session, mocker):
        mocker.patch("os.environ.get", return_value="/tmp/custom_raw")
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {}

        result = scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID})
        assert "/tmp/custom_raw" in result["artifacts"][0]["filepath"]


# ---------------------------------------------------------------------------
# scrape_detail_dummy
# ---------------------------------------------------------------------------

class TestScrapeDetailDummy:
    def test_missing_listing_id_returns_error(self, mocker):
        mocker.patch("os.makedirs")
        result = scrape_detail_dummy(run_id=RUN_ID, payload={})
        assert result["error"] == "payload.listing_id is required"
        assert result["artifacts"] == []

    def test_success(self, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        result = scrape_detail_dummy(run_id=RUN_ID, payload={"listing_id": LISTING_ID, "vin": VIN})

        assert result["error"] is None
        assert result["meta"]["wrote"] is True
        art = result["artifacts"][0]
        assert art["http_status"] == 200
        assert art["content_type"] == "text/html; charset=utf-8"
        assert art["error"] is None

    def test_write_failure_returns_error(self, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", side_effect=PermissionError("denied"))
        result = scrape_detail_dummy(run_id=RUN_ID, payload={"listing_id": LISTING_ID})

        assert result["error"] is not None
        assert "PermissionError" in result["error"]
        assert result["meta"]["wrote"] is False

    def test_dummy_html_contains_listing_id(self, mocker):
        mocker.patch("os.makedirs")
        written_data = []
        m = mock_open()
        m.return_value.__enter__.return_value.write.side_effect = lambda d: written_data.append(d)
        mocker.patch("builtins.open", m)

        scrape_detail_dummy(run_id=RUN_ID, payload={"listing_id": LISTING_ID})
        combined = b"".join(written_data)
        assert LISTING_ID.encode() in combined

    def test_dummy_html_contains_vin(self, mocker):
        mocker.patch("os.makedirs")
        written_data = []
        m = mock_open()
        m.return_value.__enter__.return_value.write.side_effect = lambda d: written_data.append(d)
        mocker.patch("builtins.open", m)

        scrape_detail_dummy(run_id=RUN_ID, payload={"listing_id": LISTING_ID, "vin": VIN})
        combined = b"".join(written_data)
        assert VIN.encode() in combined

    def test_artifact_keys_match_n8n_contract(self, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        result = scrape_detail_dummy(run_id=RUN_ID, payload={"listing_id": LISTING_ID, "vin": VIN})
        art = result["artifacts"][0]
        missing = N8N_ARTIFACT_KEYS - art.keys()
        assert missing == set(), f"Dummy artifact missing n8n fields: {missing}"

    def test_meta_has_listing_id(self, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        result = scrape_detail_dummy(run_id=RUN_ID, payload={"listing_id": LISTING_ID})
        assert result["meta"]["listing_id"] == LISTING_ID
