"""Unit tests for processors/scrape_results.py"""
import html as html_lib
import json
from unittest.mock import MagicMock, mock_open

import pytest

import scraper.processors.scrape_results as sr
from scraper.processors.scrape_results import (
    BASE_URL,
    _fetch_page,
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
        "_is_error": False,
    }


VALID_PAYLOAD = {"params": {"makes": ["Toyota"], "models": ["RAV4"]}}


class TestScrapeResultsOrchestration:
    def _patch_infra(self, mocker, fetch_results):
        mocker.patch("scraper.processors.scrape_results.time.sleep")
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

    def test_exception_in_fetch_page_propagates(self, mocker):
        mocker.patch("scraper.processors.scrape_results.time.sleep")
        mocker.patch("scraper.processors.scrape_results.random_zip", return_value="77002")
        mocker.patch("scraper.processors.scrape_results.human_delay", return_value=0.0)
        mocker.patch("os.makedirs")
        mocker.patch(
            "scraper.processors.scrape_results._fetch_page",
            side_effect=RuntimeError("boom"),
        )
        with pytest.raises(RuntimeError):
            scrape_results("run1", "sk", "national", VALID_PAYLOAD)

    def test_result_includes_run_id_search_key_scope(self, mocker):
        p1 = _make_fetch_result(stop=True)
        self._patch_infra(mocker, [p1])
        result = scrape_results("myrun", "mykey", "local", VALID_PAYLOAD)
        assert result["run_id"] == "myrun"
        assert result["search_key"] == "mykey"
        assert result["scope"] == "local"

    def test_single_zero_new_vin_page_does_not_stop_early(self, mocker):
        # Regression: with random page ordering we may hit a high-numbered
        # (old) page before low-numbered pages that have new VINs.
        # A single zero-new-VIN page must not terminate the loop.
        known = {"VIN" + str(i) for i in range(100)}
        paging = {"result_page_number": 1, "result_page_count": 6,
                  "total_listings": 120, "result_per_page": 20}

        def make_page(page_num, new_vin_count, vins):
            r = _make_fetch_result(
                paging={**paging, "result_page_number": page_num},
                new_vins=new_vin_count,
                page_vins=list(vins),
            )
            r["page_num"] = page_num
            return r

        # Page 1: 20 new VINs (already appended by orchestrator before loop)
        p1_vins = {"NEW" + str(i) for i in range(20)}
        p1 = make_page(1, 20, p1_vins)

        # Pages 2 and 3 (fetched first in shuffled order): 0 new VINs
        p2 = make_page(2, 0, set(list(known)[:20]))
        p3 = make_page(3, 0, set(list(known)[20:40]))

        # Pages 4, 5, 6: more new VINs — should be reached despite p2/p3 zeros
        extra_new = {"EXTRA" + str(i) for i in range(10)}
        p4 = make_page(4, 10, extra_new)
        p5 = make_page(5, 0, set(list(known)[40:60]))
        p6 = make_page(6, 0, set(list(known)[60:80]))

        payload = {**VALID_PAYLOAD, "known_vins": list(known)}
        # _fetch_page is called for pages 1-6 in whatever shuffle order;
        # the orchestrator always fetches p1 first then shuffles the rest.
        # We control the side_effect sequence: p1, then p2..p6 in order.
        self._patch_infra(mocker, [p1, p2, p3, p4, p5, p6])
        result = scrape_results("run1", "sk", "national", payload)

        # Must have fetched more than just p1 + the two zero pages
        assert len(result["artifacts"]) > 3

    def test_rolling_average_stops_after_five_zero_new_vin_pages(self, mocker):
        # The rolling average over 5 pages should still terminate the loop
        # once sustained low novelty is confirmed.
        known = {"VIN" + str(i) for i in range(200)}
        paging = {"result_page_number": 1, "result_page_count": 10,
                  "total_listings": 200, "result_per_page": 20}

        def make_page(page_num):
            r = _make_fetch_result(
                paging={**paging, "result_page_number": page_num},
                new_vins=0,
                page_vins=list(known)[page_num * 20: page_num * 20 + 20],
            )
            r["page_num"] = page_num
            return r

        pages = [make_page(i) for i in range(1, 11)]
        payload = {**VALID_PAYLOAD, "known_vins": list(known)}
        self._patch_infra(mocker, pages)
        result = scrape_results("run1", "sk", "national", payload)

        # Should stop before fetching all 10 pages once 5 consecutive zero pages seen
        assert len(result["artifacts"]) < 10


# ---------------------------------------------------------------------------
# page_1_blocked flag
# ---------------------------------------------------------------------------
class TestPage1Blocked:
    def _patch_infra(self, mocker, fetch_results):
        mocker.patch("scraper.processors.scrape_results.time.sleep")
        mocker.patch("scraper.processors.scrape_results.random_zip", return_value="77002")
        mocker.patch("scraper.processors.scrape_results.human_delay", return_value=0.0)
        mocker.patch("os.makedirs")
        mocker.patch(
            "scraper.processors.scrape_results._fetch_page",
            side_effect=fetch_results,
        )

    def test_true_when_page_1_is_403(self, mocker):
        p1 = _make_fetch_result(stop=True)
        p1["http_status"] = 403
        self._patch_infra(mocker, [p1])
        result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        assert result["page_1_blocked"] is True

    def test_false_when_page_1_is_200_but_stops(self, mocker):
        p1 = _make_fetch_result(stop=True)  # http_status=200
        self._patch_infra(mocker, [p1])
        result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        assert result["page_1_blocked"] is False

    def test_false_on_break_no_save(self, mocker):
        p1 = _make_fetch_result(break_no_save=True)
        self._patch_infra(mocker, [p1])
        result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        assert result["page_1_blocked"] is False

    def test_false_on_multi_page_success(self, mocker):
        paging = {"result_page_number": 1, "result_page_count": 2,
                  "total_listings": 40, "result_per_page": 20}
        p1 = _make_fetch_result(paging=paging)
        p2 = _make_fetch_result(paging={**paging, "result_page_number": 2}, stop=True)
        self._patch_infra(mocker, [p1, p2])
        result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
        assert result["page_1_blocked"] is False

    def test_always_present_in_response(self, mocker):
        for fetch in [
            [_make_fetch_result(stop=True)],
            [_make_fetch_result(break_no_save=True)],
        ]:
            self._patch_infra(mocker, fetch)
            result = scrape_results("run1", "sk", "national", VALID_PAYLOAD)
            assert "page_1_blocked" in result, "page_1_blocked missing from response"


# ---------------------------------------------------------------------------
# _fetch_page — inner SRP page fetcher
# ---------------------------------------------------------------------------

_PAGE_URL = "https://www.cars.com/shopping/results/?page=3"
_RUN_DIR = "/data/raw/run_test"
_SEARCH_KEY = "ford-escape"
_SCOPE = "national"
_PAGE_NUM = 3


def _make_ctrl_html(page_num: int, total_pages: int = 5, page_size: int = 20,
                    total_listings: int = 100) -> bytes:
    """Minimal Cars.com SearchController JSON block for paging tests."""
    data = {
        "srp_results": {
            "metadata": {
                "page": page_num,
                "total_listings": total_listings,
                "page_size": page_size,
                "total_pages": total_pages,
            }
        }
    }
    return (
        f'<script id="CarsWeb.SearchController.index" type="application/json">'
        f'{json.dumps(data)}</script>'
    ).encode()


class TestFetchPage:
    @pytest.fixture(autouse=True)
    def reset_penalty(self):
        sr._srp_adaptive_penalty = 0.0
        yield
        sr._srp_adaptive_penalty = 0.0

    def _mock_http(self, mocker, status=200, content=b"<html></html>",
                   content_type="text/html"):
        """Wire up a fake HTTP response via make_cf_session / get_cf_credentials."""
        mock_resp = MagicMock()
        mock_resp.status_code = status
        mock_resp.content = content
        mock_resp.headers = {"content-type": content_type}

        mock_session = MagicMock()
        mock_session.get.return_value = mock_resp

        mocker.patch(
            "scraper.processors.scrape_results.get_cf_credentials",
            return_value=({"cookies": {}, "user_agent": "ua"}, None, None),
        )
        mocker.patch(
            "scraper.processors.scrape_results.make_cf_session",
            return_value=mock_session,
        )
        return mock_session, mock_resp

    def test_success_200_artifact_fields(self, mocker):
        mocker.patch("builtins.open", mock_open())
        self._mock_http(mocker, 200, b"<html></html>")

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        assert result["http_status"] == 200
        assert result["error"] is None
        assert result["_is_error"] is False
        assert result["source"] == "cars.com"
        assert result["artifact_type"] == "results_page"
        assert result["search_key"] == _SEARCH_KEY
        assert result["search_scope"] == _SCOPE
        assert result["page_num"] == _PAGE_NUM

    def test_success_sha256_populated(self, mocker):
        mocker.patch("builtins.open", mock_open())
        self._mock_http(mocker, 200, b"<html>content</html>")

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        assert result["sha256"] is not None
        assert len(result["sha256"]) == 64

    def test_file_written_with_status_in_name(self, mocker):
        mock_open_fn = mock_open()
        mocker.patch("builtins.open", mock_open_fn)
        self._mock_http(mocker, 200, b"<html></html>")

        _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        open_calls = [str(c) for c in mock_open_fn.call_args_list]
        assert any(f"page_{_PAGE_NUM:04d}" in c and "__200" in c for c in open_calls)

    def test_non_200_sets_is_error_and_stop(self, mocker):
        mocker.patch("builtins.open", mock_open())
        self._mock_http(mocker, 404, b"not found")

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        assert result["http_status"] == 404
        assert result["_is_error"] is True
        assert result["_stop"] is True
        assert result["error"] == "HTTP 404"

    def test_403_retry_succeeds_and_invalidates_credentials(self, mocker):
        mocker.patch("builtins.open", mock_open())

        resp_403 = MagicMock()
        resp_403.status_code = 403
        resp_403.content = b"blocked"
        resp_403.headers = {}

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.content = b"<html></html>"
        resp_200.headers = {"content-type": "text/html"}

        mock_session = MagicMock()
        mock_session.get.side_effect = [resp_403, resp_200]

        mocker.patch(
            "scraper.processors.scrape_results.get_cf_credentials",
            return_value=({"cookies": {}, "user_agent": "ua"}, None, None),
        )
        mocker.patch(
            "scraper.processors.scrape_results.make_cf_session",
            return_value=mock_session,
        )
        mock_invalidate = mocker.patch(
            "scraper.processors.scrape_results.invalidate_cf_credentials"
        )

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        mock_invalidate.assert_called_once()
        assert result["http_status"] == 200
        assert result["_is_error"] is False

    def test_403_retry_still_403_returns_error(self, mocker):
        mocker.patch("builtins.open", mock_open())

        resp_403 = MagicMock()
        resp_403.status_code = 403
        resp_403.content = b"blocked"
        resp_403.headers = {}

        mock_session = MagicMock()
        mock_session.get.return_value = resp_403

        mocker.patch(
            "scraper.processors.scrape_results.get_cf_credentials",
            return_value=({"cookies": {}, "user_agent": "ua"}, None, None),
        )
        mocker.patch(
            "scraper.processors.scrape_results.make_cf_session",
            return_value=mock_session,
        )
        mocker.patch("scraper.processors.scrape_results.invalidate_cf_credentials")

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        assert result["_is_error"] is True
        assert "403" in result["error"]

    def test_403_backs_off_srp_penalty(self, mocker):
        mocker.patch("builtins.open", mock_open())

        resp_403 = MagicMock()
        resp_403.status_code = 403
        resp_403.content = b"blocked"
        resp_403.headers = {}

        mock_session = MagicMock()
        mock_session.get.return_value = resp_403

        mocker.patch(
            "scraper.processors.scrape_results.get_cf_credentials",
            return_value=({"cookies": {}, "user_agent": "ua"}, None, None),
        )
        mocker.patch(
            "scraper.processors.scrape_results.make_cf_session",
            return_value=mock_session,
        )
        mocker.patch("scraper.processors.scrape_results.invalidate_cf_credentials")

        sr._srp_adaptive_penalty = 0.0
        _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())
        assert sr._srp_adaptive_penalty >= 45.0

    def test_transient_error_retries_after_sleep(self, mocker):
        mocker.patch("builtins.open", mock_open())
        mock_sleep = mocker.patch("scraper.processors.scrape_results.time.sleep")

        resp_200 = MagicMock()
        resp_200.status_code = 200
        resp_200.content = b"<html></html>"
        resp_200.headers = {}

        mock_session = MagicMock()
        mock_session.get.side_effect = [ConnectionError("timeout"), resp_200]

        mocker.patch(
            "scraper.processors.scrape_results.get_cf_credentials",
            return_value=({"cookies": {}, "user_agent": "ua"}, None, None),
        )
        mocker.patch(
            "scraper.processors.scrape_results.make_cf_session",
            return_value=mock_session,
        )

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        mock_sleep.assert_called_with(10)
        assert result["http_status"] == 200
        assert result["_is_error"] is False

    def test_transient_error_both_attempts_returns_error(self, mocker):
        mocker.patch("builtins.open", mock_open())
        mocker.patch("scraper.processors.scrape_results.time.sleep")

        mock_session = MagicMock()
        mock_session.get.side_effect = ConnectionError("refused")

        mocker.patch(
            "scraper.processors.scrape_results.get_cf_credentials",
            return_value=({"cookies": {}, "user_agent": "ua"}, None, None),
        )
        mocker.patch(
            "scraper.processors.scrape_results.make_cf_session",
            return_value=mock_session,
        )

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        assert result["_is_error"] is True
        assert "ConnectionError" in result["error"]

    def test_break_no_save_when_page_clamped(self, mocker):
        # Cars.com returned page 1 but we requested page 3 → duplicate territory
        html = _make_ctrl_html(page_num=1, total_pages=5)
        mocker.patch("builtins.open", mock_open())
        self._mock_http(mocker, 200, html)

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        assert result["_break_no_save"] is True

    def test_stop_when_no_cards_on_page(self, mocker):
        html = _make_ctrl_html(page_num=_PAGE_NUM, total_pages=5, page_size=0)
        mocker.patch("builtins.open", mock_open())
        self._mock_http(mocker, 200, html)

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        assert result["_stop"] is True

    def test_stop_on_last_page(self, mocker):
        # actual_page == total_pages → stop
        html = _make_ctrl_html(page_num=5, total_pages=5)
        mocker.patch("builtins.open", mock_open())
        self._mock_http(mocker, 200, html)

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, 5, set())

        assert result["_stop"] is True

    def test_vin_extraction(self, mocker):
        vin = "1HGCM82633A123456"
        html = f'<html><body><script>{{"vin": "{vin}"}}</script></body></html>'.encode()
        mocker.patch("builtins.open", mock_open())
        self._mock_http(mocker, 200, html)

        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, set())

        assert vin in result["_page_vins"]
        assert result["page_vins_total"] == 1

    def test_known_vins_counted_as_new(self, mocker):
        vin_known = "1HGCM82633A000001"
        vin_new = "1HGCM82633A000002"
        html = (
            f'<script>{{"vin": "{vin_known}"}}{{"vin": "{vin_new}"}}</script>'
        ).encode()
        mocker.patch("builtins.open", mock_open())
        self._mock_http(mocker, 200, html)

        known = {vin_known}
        result = _fetch_page(_PAGE_URL, _RUN_DIR, _SEARCH_KEY, _SCOPE, _PAGE_NUM, known)

        assert result["page_vins_new"] == 1
        assert result["page_vins_total"] == 2
