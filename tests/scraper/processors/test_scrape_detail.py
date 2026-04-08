"""Unit tests for processors/scrape_detail.py"""
from unittest.mock import MagicMock, mock_open

import pytest
import scraper.processors.scrape_detail as sd
from scraper.processors.scrape_detail import scrape_detail_batch, scrape_detail_dummy, scrape_detail_fetch


@pytest.fixture(autouse=True)
def reset_adaptive_delay():
    """Reset the module-level adaptive delay before each test so tests don't bleed state."""
    sd._detail_adaptive_delay = 0.0
    yield
    sd._detail_adaptive_delay = 0.0

# n8n reads all 14 of these keys from every artifact
N8N_ARTIFACT_KEYS = {
    "source",
    "artifact_type",
    "listing_id",
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
BATCH_ID = "batch-test-0000-0000-000000000001"
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
            "scraper.processors.scrape_detail.cf_requests.Session",
            return_value=mock_session,
        )
        # Mock _get_cf_credentials to return a cache hit so _fetch_url calls session.get()
        mocker.patch(
            "scraper.processors.scrape_detail._get_cf_credentials",
            return_value=({"cookies": {}, "user_agent": "test-ua"}, None, None),
        )

        result = scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID})

        assert "ConnectionError" in result["error"]
        assert result["artifacts"][0]["http_status"] is None
        # Error file must have been written
        open_calls = mock_open_fn.call_args_list
        assert any("ERROR.txt" in str(c) for c in open_calls)

    def test_batch_id_used_as_search_key(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {}

        payload = {
            "listing_id": LISTING_ID,
            "vin": VIN,
            "batch_id": BATCH_ID,
        }
        result = scrape_detail_fetch(run_id=RUN_ID, payload=payload)
        assert result["artifacts"][0]["search_key"] == BATCH_ID
        assert result["artifacts"][0]["listing_id"] == LISTING_ID

    def test_batch_id_defaults_to_run_id(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {}

        result = scrape_detail_fetch(run_id=RUN_ID, payload={"listing_id": LISTING_ID})
        assert result["artifacts"][0]["search_key"] == RUN_ID

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


# ---------------------------------------------------------------------------
# scrape_detail_batch
# ---------------------------------------------------------------------------

class TestScrapeDetailBatch:
    def test_empty_batch_returns_empty_artifacts(self, mocker):
        mocker.patch("os.makedirs")
        result = scrape_detail_batch(run_id=RUN_ID, batch_id=BATCH_ID, listings=[])
        assert result["artifacts"] == []
        assert result["meta"]["total"] == 0
        assert result["meta"]["errors"] == 0

    def test_batch_returns_artifact_per_listing(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"<html>detail</html>"
        mock_resp.url = "https://www.cars.com/vehicledetail/l1/"
        mock_resp.headers = {"content-type": "text/html; charset=utf-8"}

        listings = [
            {"listing_id": "l1", "vin": "VIN1"},
            {"listing_id": "l2", "vin": "VIN2"},
        ]
        result = scrape_detail_batch(run_id=RUN_ID, batch_id=BATCH_ID, listings=listings)

        assert len(result["artifacts"]) == 2
        assert result["meta"]["total"] == 2
        assert result["meta"]["succeeded"] == 2
        assert result["meta"]["errors"] == 0

    def test_403_increments_adaptive_delay(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 403
        mock_resp.content = b"<html>blocked</html>"
        mock_resp.url = "https://www.cars.com/vehicledetail/l3/"
        mock_resp.headers = {}

        sd._detail_adaptive_delay = 0.0
        scrape_detail_batch(run_id=RUN_ID, batch_id=BATCH_ID, listings=[{"listing_id": "l3"}])
        assert sd._detail_adaptive_delay > 0

    def test_403_invalidates_credentials(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 403
        mock_resp.content = b"blocked"
        mock_resp.url = "https://www.cars.com/vehicledetail/l4/"
        mock_resp.headers = {}

        sd._cf_credentials_expires_at = 9999999999.0  # set far future
        scrape_detail_batch(run_id=RUN_ID, batch_id=BATCH_ID, listings=[{"listing_id": "l4"}])
        assert sd._cf_credentials_expires_at == 0.0

    def test_meta_has_required_keys(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {}

        listings = [{"listing_id": "l5"}]
        result = scrape_detail_batch(
            run_id=RUN_ID, batch_id=BATCH_ID, listings=listings
        )
        for key in ("mode", "total", "succeeded", "errors"):
            assert key in result["meta"], f"meta missing key: {key}"

    def test_artifacts_have_n8n_keys(self, mock_cf_session, mocker):
        mocker.patch("os.makedirs")
        mocker.patch("builtins.open", mock_open())
        mock_session, mock_resp = mock_cf_session
        mock_resp.status_code = 200
        mock_resp.content = b"ok"
        mock_resp.url = "https://example.com/"
        mock_resp.headers = {"content-type": "text/html"}

        listings = [{"listing_id": "l6", "vin": VIN}]
        result = scrape_detail_batch(
            run_id=RUN_ID, batch_id=BATCH_ID, listings=listings
        )
        art = result["artifacts"][0]
        missing = N8N_ARTIFACT_KEYS - art.keys()
        assert missing == set(), f"Batch artifact missing n8n fields: {missing}"
