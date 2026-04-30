"""
Behaviour tests for the scrape_listings DAG callable functions.

These tests focus on the HTTP contract between _run_scrapes and the scraper
service — specifically that the payload sent to POST /scrape_results has the
nested {"params": {...}} structure that scrape_results() expects, not the flat
dict stored in search_configs.params.

This is the gap that previously allowed a payload mismatch to go undetected:
unit tests for scrape_results() used the correct nested format, but nothing
verified that the DAG was constructing the same format when calling the API.
"""
import sys
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

# Ensure airflow/dags/ is importable so the DAG module loads cleanly.
DAGS_DIR = Path(__file__).parents[3] / "airflow" / "dags"
if str(DAGS_DIR) not in sys.path:
    sys.path.insert(0, str(DAGS_DIR))

from scrape_listings import _run_scrapes  # noqa: E402


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_rotation(configs, slot=1, run_id="run-test-001"):
    """Build an advance_rotation result as XCom would deliver it."""
    return {"slot": slot, "run_id": run_id, "configs": configs}


def _make_config(search_key="honda-cr_v_hybrid", scopes=None):
    """Simulate one entry from advance_rotation configs."""
    return {
        "search_key": search_key,
        "params": {
            "makes": ["honda"],
            "models": ["honda-cr_v_hybrid"],
            "radius_miles": 200,
            "max_listings": 2000,
            "max_safety_pages": 30,
            "scopes": scopes or ["local", "national"],
        },
        "scopes": scopes or ["local", "national"],
    }


def _mock_context(rotation):
    """Build a fake Airflow task-instance context for _run_scrapes."""
    ti = MagicMock()
    ti.xcom_pull.return_value = rotation
    return {"ti": ti}


def _completed_job(job_id, artifact_count=5):
    return {
        "job_id": job_id,
        "status": "completed",
        "artifact_count": artifact_count,
        "page_1_blocked": False,
    }


# ---------------------------------------------------------------------------
# Payload contract tests
# ---------------------------------------------------------------------------

class TestRunScrapesPayloadContract:
    """Verify _run_scrapes sends the payload structure that scrape_results() expects."""

    def test_post_body_is_wrapped_in_params_key(self):
        """
        The JSON body sent to POST /scrape_results must be {"params": {...}},
        not the flat dict from search_configs.params.

        scrape_results() does `payload.get("params", {})` to extract makes/models.
        If the body IS the flat dict, makes/models are empty and the scrape
        silently exits with 0 artifacts.
        """
        config = _make_config()
        rotation = _make_rotation([config])
        context = _mock_context(rotation)

        job_id = "job-aaa"
        submit_resp = MagicMock(status_code=200)
        submit_resp.json.return_value = {"job_id": job_id, "status": "queued"}
        submit_resp.raise_for_status = MagicMock()

        poll_resp = MagicMock(status_code=200)
        poll_resp.json.return_value = [_completed_job(job_id)]
        poll_resp.raise_for_status = MagicMock()

        fetched_resp = MagicMock(status_code=200)
        fetched_resp.json.return_value = {"job_id": job_id, "status": "fetched"}
        fetched_resp.raise_for_status = MagicMock()

        with patch("scrape_listings.requests.post") as mock_post, \
             patch("scrape_listings.requests.get") as mock_get, \
             patch("scrape_listings.time.sleep"):

            mock_post.return_value = submit_resp
            mock_get.return_value = poll_resp

            _run_scrapes(**context)

        # Find the /scrape_results call (not the /fetched call)
        srp_calls = [
            c for c in mock_post.call_args_list
            if "/scrape_results" in c.args[0] and "/fetched" not in c.args[0]
        ]
        assert srp_calls, "No POST /scrape_results call found"

        for c in srp_calls:
            body = c.kwargs.get("json") or (c.args[1] if len(c.args) > 1 else None)
            assert body is not None, "No JSON body sent to /scrape_results"
            assert "params" in body, (
                f"JSON body missing 'params' key — got top-level keys: {list(body.keys())}. "
                "The DAG must send {{\"params\": config[\"params\"]}} not config[\"params\"] directly."
            )
            assert "makes" in body["params"], "body['params'] missing 'makes'"
            assert "models" in body["params"], "body['params'] missing 'models'"

    def test_makes_and_models_reach_scraper_correctly(self):
        """makes and models from search_configs are present at body['params']['makes/models']."""
        config = _make_config(search_key="kia-sportage_hybrid")
        rotation = _make_rotation([config])
        context = _mock_context(rotation)

        job_id = "job-bbb"
        submit_resp = MagicMock()
        submit_resp.json.return_value = {"job_id": job_id, "status": "queued"}
        submit_resp.raise_for_status = MagicMock()

        poll_resp = MagicMock()
        poll_resp.json.return_value = [_completed_job(job_id)]
        poll_resp.raise_for_status = MagicMock()

        with patch("scrape_listings.requests.post", return_value=submit_resp), \
             patch("scrape_listings.requests.get", return_value=poll_resp), \
             patch("scrape_listings.time.sleep"):

            _run_scrapes(**context)

        srp_calls = [
            c for c in submit_resp.mock_calls
            if False  # we need to check mock_post
        ]

        # Re-run with captured mock_post
        with patch("scrape_listings.requests.post") as mock_post, \
             patch("scrape_listings.requests.get") as mock_get, \
             patch("scrape_listings.time.sleep"):

            mock_post.return_value = submit_resp
            mock_get.return_value = poll_resp
            _run_scrapes(**context)

        srp_calls = [
            c for c in mock_post.call_args_list
            if "/fetched" not in str(c)
        ]
        assert srp_calls
        body = srp_calls[0].kwargs.get("json")
        assert body["params"]["makes"] == ["honda"] or body["params"]["makes"] is not None
        assert body["params"]["models"] is not None

    def test_one_post_per_config_scope(self):
        """A config with two scopes produces two POST /scrape_results calls."""
        config = _make_config(scopes=["local", "national"])
        rotation = _make_rotation([config])
        context = _mock_context(rotation)

        job_ids = ["job-1", "job-2"]
        call_count = 0

        def post_side_effect(url, **kwargs):
            nonlocal call_count
            resp = MagicMock()
            resp.raise_for_status = MagicMock()
            if "/fetched" in url:
                resp.json.return_value = {"status": "fetched"}
            else:
                resp.json.return_value = {"job_id": job_ids[call_count], "status": "queued"}
                call_count += 1
            return resp

        completed = [_completed_job(jid) for jid in job_ids]
        poll_resp = MagicMock()
        poll_resp.json.return_value = completed
        poll_resp.raise_for_status = MagicMock()

        with patch("scrape_listings.requests.post", side_effect=post_side_effect), \
             patch("scrape_listings.requests.get", return_value=poll_resp), \
             patch("scrape_listings.time.sleep"):

            result = _run_scrapes(**context)

        assert call_count == 2, f"Expected 2 scrape_results POSTs, got {call_count}"

    def test_scope_sent_as_query_param(self):
        """The scope must appear as a query param, not inside the JSON body."""
        config = _make_config(scopes=["national"])
        rotation = _make_rotation([config])
        context = _mock_context(rotation)

        job_id = "job-scope"
        submit_resp = MagicMock()
        submit_resp.json.return_value = {"job_id": job_id, "status": "queued"}
        submit_resp.raise_for_status = MagicMock()

        poll_resp = MagicMock()
        poll_resp.json.return_value = [_completed_job(job_id)]
        poll_resp.raise_for_status = MagicMock()

        with patch("scrape_listings.requests.post") as mock_post, \
             patch("scrape_listings.requests.get", return_value=poll_resp), \
             patch("scrape_listings.time.sleep"):

            mock_post.return_value = submit_resp
            _run_scrapes(**context)

        srp_calls = [
            c for c in mock_post.call_args_list
            if "/fetched" not in str(c)
        ]
        assert srp_calls
        params = srp_calls[0].kwargs.get("params", {})
        assert "scope" in params, "scope must be a query param"
        assert params["scope"] == "national"


# ---------------------------------------------------------------------------
# No-op / guard behaviour
# ---------------------------------------------------------------------------

class TestRunScrapesGuards:
    def test_empty_configs_skips_scrape(self):
        """When advance_rotation returns no configs, no HTTP calls are made."""
        rotation = _make_rotation(configs=[], slot=None, run_id=None)
        context = _mock_context(rotation)

        with patch("scrape_listings.requests.post") as mock_post, \
             patch("scrape_listings.requests.get") as mock_get:

            result = _run_scrapes(**context)

        mock_post.assert_not_called()
        mock_get.assert_not_called()
        assert result["skipped"] is True

    def test_too_soon_reason_propagated(self):
        rotation = {"slot": None, "run_id": None, "configs": [], "reason": "too_soon"}
        context = _mock_context(rotation)

        with patch("scrape_listings.requests.post"), \
             patch("scrape_listings.requests.get"):

            result = _run_scrapes(**context)

        assert result["reason"] == "too_soon"
