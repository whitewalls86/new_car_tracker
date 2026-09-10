"""Plan 136 Stage 2: scraper-owned outcome counters.

The counters live on the default registry and are process-global, so every
assertion here is a *delta* around the call under test. Absolute values would
couple these tests to execution order and to every other test that happens to
run a fetch.
"""
from unittest.mock import MagicMock

import pytest
from prometheus_client import REGISTRY

import scraper.metrics as scraper_metrics
import scraper.processors.cf_session as cf_session
from scraper.metrics import (
    DETAIL_FETCH_OUTCOMES,
    SOLVER_OUTCOMES,
    record_detail_fetch,
    record_solver_outcome,
)


def _solver(outcome: str) -> float:
    return REGISTRY.get_sample_value(
        "cartracker_solver_requests_total", {"outcome": outcome}
    )


def _detail(outcome: str) -> float:
    return REGISTRY.get_sample_value(
        "cartracker_detail_fetch_total", {"outcome": outcome}
    )


class TestSeriesExistBeforeAnyTraffic:
    """The reason every label child is pre-initialized in scraper.metrics.

    A Counter with labels publishes nothing until its first inc(). Without the
    pre-initialization loop, `outcome="ok"` would not exist until the first
    success -- and both alerts ask whether the ok count is zero, which a missing
    series cannot answer. It would read NoData on a cold start and flap into
    existence on first traffic.
    """

    @pytest.mark.parametrize("outcome", SOLVER_OUTCOMES)
    def test_every_solver_outcome_is_published(self, outcome):
        assert _solver(outcome) is not None, (
            f"cartracker_solver_requests_total{{outcome={outcome!r}}} is absent; "
            "the alert expressions cannot evaluate against a missing series"
        )

    @pytest.mark.parametrize("outcome", DETAIL_FETCH_OUTCOMES)
    def test_every_detail_outcome_is_published(self, outcome):
        assert _detail(outcome) is not None

    def test_the_label_sets_are_exactly_what_the_alerts_select(self):
        """ct-solver-not-solving selects outcome!="ok", so a fourth solver
        outcome added later would silently join the failure side. Pin both.

        **The detail set widened on 2026-09-09 and the solver set did not.**
        Plan 162 Stage AB split the detail metric's catch-all `error` into
        `transient` (a 5xx from the origin) and `redirected` (a 3xx, which for
        cars.com means the listing has gone), leaving `error` for a raised
        exception or an unclassified status. Widening is safe *here* and would
        not have been on the solver metric: the two detail rules select
        `outcome="ok"` and `outcome="403"` by name and one sums across all
        outcomes, whereas ct-solver-not-solving's `!=` puts every new label on
        the failure side by construction.

        So `ok` and `403` are pinned because alerts name them, and the tuple is
        pinned in full because a label nobody intended is exactly what this
        test exists to catch."""
        assert SOLVER_OUTCOMES == ("ok", "challenge", "error")
        assert DETAIL_FETCH_OUTCOMES == ("ok", "403", "transient", "redirected", "error")
        assert "ok" in DETAIL_FETCH_OUTCOMES and "403" in DETAIL_FETCH_OUTCOMES


class TestDetailFetchOutcomeMapping:
    @pytest.mark.parametrize(
        "status,errored,expected",
        [
            (200, False, "ok"),
            (403, False, "403"),
            # Plan 162 Stage AB: the four 5xx cars.com actually returns are
            # `transient` rather than a shared `error` bucket, so the alert
            # description's advice -- "mixed `error` points at the fetch path
            # or the site" -- is now answerable from the metric.
            (500, False, "transient"),
            (502, False, "transient"),
            (503, False, "transient"),
            (504, False, "transient"),
            (302, False, "redirected"),
            # 404 is not in the recorded corpus, so it takes the UNKNOWN
            # catchall and keeps the residual `error` label, alongside the two
            # cases that have no status at all.
            (404, False, "error"),
            (None, False, "error"),
            (200, True, "error"),
        ],
    )
    def test_status_maps_to_outcome(self, status, errored, expected):
        before = {o: _detail(o) for o in DETAIL_FETCH_OUTCOMES}
        record_detail_fetch(status, errored=errored)
        after = {o: _detail(o) for o in DETAIL_FETCH_OUTCOMES}
        moved = [o for o in DETAIL_FETCH_OUTCOMES if after[o] != before[o]]
        assert moved == [expected]
        assert after[expected] == before[expected] + 1

    def test_a_302_is_not_counted_as_success(self):
        """Only HTTP 200 is `ok`. A redirect or a 5xx served in place of the
        page is not a fetch that produced data, and counting it as one would
        keep the ok rate non-zero through exactly the outage this detects."""
        before = _detail("ok")
        record_detail_fetch(302)
        assert _detail("ok") == before


class TestSolverResponseClassification:
    """The distinction the 2026-08-14 outage turned on.

    `trawl` returned status=ok with real cf_clearance cookies for eight hours
    while every page behind them was an interstitial. A counter that trusted the
    solver's self-report would have recorded eight hours of successes.
    """

    def test_a_real_page_is_ok(self):
        html = b"<html><head><title>New 2026 Toyota RAV4 for sale</title></head></html>"
        assert cf_session._solver_outcome(html, 200) == "ok"

    @pytest.mark.parametrize(
        "title",
        ["Just a moment...", "Attention Required! | Cloudflare", "Checking your browser"],
    )
    def test_an_interstitial_title_is_a_challenge(self, title):
        html = f"<html><head><title>{title}</title></head></html>".encode()
        assert cf_session._solver_outcome(html, 200) == "challenge"

    def test_a_403_is_a_challenge_whatever_the_title(self):
        assert cf_session._solver_outcome(b"<html></html>", 403) == "challenge"

    def test_an_empty_body_is_not_guessed_into_a_challenge(self):
        """No title is not evidence of an interstitial. Guessing here would
        turn every odd-but-working response into a page."""
        assert cf_session._solver_outcome(b"", 200) == "ok"


def _solver_response(html="<html><title>ok</title></html>", status=200, api_status="ok"):
    resp = MagicMock()
    resp.json.return_value = {
        "status": api_status,
        "solution": {
            "userAgent": "Mozilla/5.0 Chrome/136.0.0.0",
            "response": html,
            "status": status,
            "cookies": [{"name": "cf_clearance", "value": "token"}],
        },
    }
    return resp


class TestSolverCountingThroughGetCfCredentials:
    @pytest.fixture(autouse=True)
    def _reset(self):
        cf_session._cf_credentials = None
        cf_session._cf_credentials_expires_at = 0.0
        yield
        cf_session._cf_credentials = None
        cf_session._cf_credentials_expires_at = 0.0

    def test_a_successful_bootstrap_counts_ok(self, mocker):
        mocker.patch.object(
            cf_session.stdlib_requests, "post", return_value=_solver_response()
        )
        before = _solver("ok")
        cf_session.get_cf_credentials("https://www.cars.com/", 30)
        assert _solver("ok") == before + 1

    def test_an_interstitial_behind_a_status_ok_counts_challenge(self, mocker):
        """The outage's actual shape, and the one no healthcheck can see."""
        mocker.patch.object(
            cf_session.stdlib_requests,
            "post",
            return_value=_solver_response(
                html="<html><head><title>Just a moment...</title></head></html>"
            ),
        )
        before_ok, before_challenge = _solver("ok"), _solver("challenge")
        cf_session.get_cf_credentials("https://www.cars.com/", 30)
        assert _solver("challenge") == before_challenge + 1
        assert _solver("ok") == before_ok

    def test_a_raising_solver_counts_error_and_still_raises(self, mocker):
        """The 500s of 2026-08-14. The count must not swallow the exception --
        the caller's fallback to plain curl_cffi is what produced the 403s."""
        mocker.patch.object(
            cf_session.stdlib_requests, "post", side_effect=RuntimeError("boom")
        )
        before = _solver("error")
        with pytest.raises(RuntimeError):
            cf_session.get_cf_credentials("https://www.cars.com/", 30)
        assert _solver("error") == before + 1

    def test_a_non_ok_api_status_counts_error_once(self, mocker):
        """The RuntimeError this module raises itself is inside the counted
        block, so the guard against double-counting it is worth pinning."""
        mocker.patch.object(
            cf_session.stdlib_requests,
            "post",
            return_value=_solver_response(api_status="error"),
        )
        before = _solver("error")
        with pytest.raises(RuntimeError):
            cf_session.get_cf_credentials("https://www.cars.com/", 30)
        assert _solver("error") == before + 1

    def test_a_cache_hit_is_not_a_solver_request(self, mocker):
        """Credentials cache for 25 minutes, and only a miss reaches the solver.
        Counting cache hits would make the healthy rate look like fetch volume
        and destroy the meaning of the `> bool 5` guard in
        ct-solver-not-solving."""
        post = mocker.patch.object(
            cf_session.stdlib_requests, "post", return_value=_solver_response()
        )
        cf_session.get_cf_credentials("https://www.cars.com/", 30)
        before = {o: _solver(o) for o in SOLVER_OUTCOMES}

        cf_session.get_cf_credentials("https://www.cars.com/", 30)

        assert post.call_count == 1
        assert {o: _solver(o) for o in SOLVER_OUTCOMES} == before


class TestTelemetryNeverBreaksAScrape:
    def test_a_failing_counter_is_swallowed(self, mocker):
        """A metrics backend problem must never fail a fetch. The counters are
        the observability layer, not the work."""
        mocker.patch.object(
            scraper_metrics.solver_requests_total, "labels", side_effect=ValueError
        )
        record_solver_outcome("ok")  # must not raise

        mocker.patch.object(
            scraper_metrics.detail_fetch_total, "labels", side_effect=ValueError
        )
        record_detail_fetch(200)  # must not raise
