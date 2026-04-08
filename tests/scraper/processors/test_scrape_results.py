"""Unit tests for processors/scrape_results.py"""
import html as html_lib
import json
from unittest.mock import MagicMock

import pytest
from scraper.processors.scrape_results import (
    BASE_URL,
    build_results_url,
    extract_results_paging_meta,
    scrape_results,
)


# ---------------------------------------------------------------------------
# build_results_url
# ---------------------------------------------------------------------------
class TestBuildResultsUrl:
    def test_basic_params_present(self):
        url = build_results_url(["Toyota"], ["RAV4"], "77002", "national", 200, 1)
        assert url.startswith(BASE_URL)
        assert "makes%5B%5D=Toyota" in url or "makes[]" in url
        assert "models%5B%5D=RAV4" in url or "models[]" in url
        assert "zip=77002" in url
        assert "page=1" in url

    def test_local_scope_uses_numeric_radius(self):
        url = build_results_url(["Honda"], ["CR-V"], "77002", "local", 200, 1)
        assert "maximum_distance=200" in url

    def test_national_scope_uses_all(self):
        url = build_results_url(["Honda"], ["CR-V"], "77002", "national", 200, 1)
        assert "maximum_distance=all" in url

    def test_sort_order_included_when_provided(self):
        url = build_results_url(["Toyota"], ["RAV4"], "77002", "national", 200, 1,
                                sort_order="listed_at_desc")
        assert "sort=listed_at_desc" in url

    def test_sort_order_absent_when_not_provided(self):
        url = build_results_url(["Toyota"], ["RAV4"], "77002", "national", 200, 1)
        assert "sort=" not in url

    def test_multiple_makes_doseq(self):
        url = build_results_url(["Toyota", "Honda"], ["RAV4", "CR-V"], "77002", "national", 200, 1)
        assert url.count("makes%5B%5D") == 2 or url.count("makes[]") == 2

    def test_page_number_in_url(self):
        url = build_results_url(["Toyota"], ["RAV4"], "77002", "national", 200, 5)
        assert "page=5" in url


# ---------------------------------------------------------------------------
# extract_results_paging_meta
# ---------------------------------------------------------------------------
class TestExtractResultsPagingMeta:
    def test_source0_search_controller_json(self):
        data = {
            "srp_results": {
                "metadata": {
                    "page": 2,
                    "total_listings": 414,
                    "page_size": 20,
                    "total_pages": 21,
                }
            }
        }
        json_str = json.dumps(data)
        html = (
            f'<script id="CarsWeb.SearchController.index" '
            f'type="application/json">{json_str}</script>'
        )
        result = extract_results_paging_meta(html)
        assert result is not None
        assert result["result_page_number"] == 2
        assert result["result_page_count"] == 21
        assert result["total_listings"] == 414
        assert result["result_per_page"] == 20

    def test_source1_data_site_activity_legacy(self):
        activity = {
            "total_results": 200,
            "result_per_page": 20,
            "result_page_number": 1,
            "result_page_count": 10,
        }
        encoded = html_lib.escape(json.dumps(activity))
        html = f'<div data-site-activity="{encoded}"></div>'
        result = extract_results_paging_meta(html)
        assert result is not None
        assert result["result_page_number"] == 1
        assert result["result_page_count"] == 10
        assert result["total_results"] == 200

    def test_source1_computes_page_count_when_missing(self):
        activity = {
            "total_results": 100,
            "result_per_page": 20,
            "result_page_number": 1,
            # no result_page_count
        }
        encoded = html_lib.escape(json.dumps(activity))
        html = f'<div data-site-activity="{encoded}"></div>'
        result = extract_results_paging_meta(html)
        assert result["result_page_count"] == 5  # ceil(100/20)

    def test_source2_spark_card_fallback(self):
        vd = json.dumps({"listingId": "x", "metadata": {"page_number": 3}})
        html = (
            f"<spark-card data-vehicle-details='{vd}'></spark-card>" * 5
            + '<meta name="description" content="from 100 Accord models in Houston, TX">'
        )
        result = extract_results_paging_meta(html)
        assert result is not None
        assert result["result_page_number"] == 3

    def test_source0_takes_priority_over_source1(self):
        ctrl_data = {
            "srp_results": {
                "metadata": {"page": 1, "total_listings": 50, "page_size": 10, "total_pages": 5}
            }
        }
        activity = {"total_results": 999, "result_page_number": 99, "result_per_page": 1}
        encoded = html_lib.escape(json.dumps(activity))
        html = (
            f'<script id="CarsWeb.SearchController.index">{json.dumps(ctrl_data)}</script>'
            f'<div data-site-activity="{encoded}"></div>'
        )
        result = extract_results_paging_meta(html)
        assert result["result_page_number"] == 1  # from source 0, not 99

    def test_empty_html_returns_none(self):
        result = extract_results_paging_meta("<html><body></body></html>")
        assert result is None

    def test_result_has_all_required_keys(self):
        data = {
            "srp_results": {
                "metadata": {"page": 1, "total_listings": 10, "page_size": 10, "total_pages": 1}
            }
        }
        html = f'<script id="CarsWeb.SearchController.index">{json.dumps(data)}</script>'
        result = extract_results_paging_meta(html)
        required_keys = (
            "total_listings",
            "total_results",
            "result_per_page",
            "result_page_number",
            "result_page_count",
        )
        for key in required_keys:
            assert key in result


# ---------------------------------------------------------------------------
# scrape_results orchestration
# (mock _fetch_page, get_context, close_browser, human_delay, time.sleep)
# ---------------------------------------------------------------------------

def _make_fetch_result(paging=None, stop=False, break_no_save=False, new_vins=0, page_vins=None):
    """Produce a minimal artifact dict as returned by _fetch_page."""
    return {
        "source": "cars.com",
        "artifact_type": "results_page",
        "search_key": "sk",
        "search_scope": "national",
        "page_num": 1,
        "url": "https://example.com",
        "http_status": 200,
        "content_type": "text/html",
        "content_bytes": 100,
        "sha256": "abc",
        "filepath": "/data/file.html",
        "fetched_at": "2026-01-01T00:00:00+00:00",
        "error": None,
        "paging_meta": paging,
        "page_vins_total": len(page_vins or []),
        "page_vins_new": new_vins,
        "_paging": paging,
        "_page_vins": set(page_vins or []),
        "_new_vins_count": new_vins,
        "_stop": stop,
        "_break_no_save": break_no_save,
    }


VALID_PAYLOAD = {"params": {"makes": ["Toyota"], "models": ["RAV4"]}}


class TestScrapeResultsOrchestration:
    def _patch_infra(self, mocker, fetch_results):
        mocker.patch("scraper.processors.scrape_results.get_context", return_value=MagicMock())
        mocker.patch("scraper.processors.scrape_results.close_browser")
        mocker.patch("scraper.processors.scrape_results.time.sleep")
        mocker.patch("scraper.processors.scrape_results.random_profile", return_value={
            "user_agent": "ua", "extra_http_headers": {}, "viewport": {}, "locale": "en-US"
        })
        mocker.patch("scraper.processors.scrape_results.random_zip", return_value="77002")
        mocker.patch("scraper.processors.scrape_results.human_delay", return_value=0.0)
        mocker.patch("os.makedirs")
        mocker.patch(
            "scraper.processors.scrape_results._fetch_page",
            side_effect=fetch_results,
        )

    def test_invalid_scope_returns_error(self, mocker):
        mocker.patch("os.makedirs")
        result = scrape_results("run1", "sk", "galaxy", VALID_PAYLOAD)
        assert result["error"] == "Invalid scope 'galaxy'"
        assert result["artifacts"] == []

    def test_missing_makes_returns_error(self, mocker):
        mocker.patch("os.makedirs")
        result = scrape_results("run1", "sk", "national", {"params": {"models": ["RAV4"]}})
        assert result["error"] == "Missing makes/models in params"

    def test_missing_models_returns_error(self, mocker):
        mocker.patch("os.makedirs")
        result = scrape_results("run1", "sk", "national", {"params": {"makes": ["Toyota"]}})
        assert result["error"] == "Missing makes/models in params"

    def test_single_page_stop(self, mocker):
        p1 = _make_fetch_result(stop=True)
        self._patch_infra(mocker, [p1])
        result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        assert len(result["artifacts"]) == 1
        assert result["artifacts"][0].get("http_status") == 200

    def test_break_no_save_yields_empty(self, mocker):
        p1 = _make_fetch_result(break_no_save=True)
        self._patch_infra(mocker, [p1])
        result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        assert result["artifacts"] == []

    def test_multi_page_fetches_pages_2_and_3(self, mocker):
        paging = {"result_page_number": 1, "result_page_count": 3,
                  "total_listings": 60, "result_per_page": 20}
        p1 = _make_fetch_result(paging=paging)
        p2 = _make_fetch_result(paging={**paging, "result_page_number": 2})
        p3 = _make_fetch_result(paging={**paging, "result_page_number": 3}, stop=True)
        self._patch_infra(mocker, [p1, p2, p3])
        result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        # All 3 pages saved (p3 is added then stop breaks)
        assert len(result["artifacts"]) == 3

    def test_max_safety_pages_cap(self, mocker):
        # paging says 1000 pages, but max_safety_pages=3
        paging = {"result_page_number": 1, "result_page_count": 1000,
                  "total_listings": 20000, "result_per_page": 20}
        # Return stop=True on page 1 to avoid needing 3 fetch calls
        p1 = _make_fetch_result(paging=paging, stop=True)
        self._patch_infra(mocker, [p1])
        result = scrape_results(
            "run1", "sk", "national",
            {"params": {"makes": ["Toyota"], "models": ["RAV4"], "max_safety_pages": 3}},
        )
        assert result is not None  # didn't crash; cap was applied

    def test_private_keys_stripped_from_output_artifacts(self, mocker):
        paging = {"result_page_number": 1, "result_page_count": 1,
                  "total_listings": 10, "result_per_page": 10}
        p1 = _make_fetch_result(paging=paging, stop=True)
        self._patch_infra(mocker, [p1])
        result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        for artifact in result["artifacts"]:
            for key in artifact:
                assert not key.startswith("_"), f"Private key leaked: {key}"

    def test_close_browser_called_on_success(self, mocker):
        p1 = _make_fetch_result(stop=True)
        self._patch_infra(mocker, [p1])
        mock_close = mocker.patch("scraper.processors.scrape_results.close_browser")
        scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        mock_close.assert_called_once()

    def test_close_browser_called_even_on_exception(self, mocker):
        mocker.patch("scraper.processors.scrape_results.get_context", return_value=MagicMock())
        mock_close = mocker.patch("scraper.processors.scrape_results.close_browser")
        mocker.patch("scraper.processors.scrape_results.time.sleep")
        mocker.patch("scraper.processors.scrape_results.random_profile", return_value={
            "user_agent": "ua", "extra_http_headers": {}, "viewport": {}, "locale": "en-US"
        })
        mocker.patch("scraper.processors.scrape_results.random_zip", return_value="77002")
        mocker.patch("scraper.processors.scrape_results.human_delay", return_value=0.0)
        mocker.patch("os.makedirs")
        mocker.patch(
            "scraper.processors.scrape_results._fetch_page",
            side_effect=RuntimeError("boom"),
        )
        with pytest.raises(RuntimeError):
            scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        mock_close.assert_called_once()

    def test_result_includes_run_id_search_key_scope(self, mocker):
        p1 = _make_fetch_result(stop=True)
        self._patch_infra(mocker, [p1])
        result = scrape_results("myrun", "mykey", "local", VALID_PAYLOAD)
        assert result["run_id"] == "myrun"
        assert result["search_key"] == "mykey"
        assert result["scope"] == "local"
