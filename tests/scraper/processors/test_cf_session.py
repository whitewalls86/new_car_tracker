"""Unit tests for processors/cf_session.py"""
import time
from unittest.mock import MagicMock

import pytest

import scraper.processors.cf_session as cf_session
from scraper.processors.cf_session import (
    get_cf_credentials,
    invalidate_cf_credentials,
    make_cf_session,
)


@pytest.fixture(autouse=True)
def reset_cf_state():
    """Reset process-wide CF credential cache before/after each test."""
    cf_session._cf_credentials = None
    cf_session._cf_credentials_expires_at = 0.0
    yield
    cf_session._cf_credentials = None
    cf_session._cf_credentials_expires_at = 0.0


def _flaresolverr_response(page_num=200, ua="Mozilla/5.0 Chrome/136.0.0.0",
                            html="<html>ok</html>",
                            cookies=None) -> MagicMock:
    """Build a mock FlareSolverr HTTP response."""
    if cookies is None:
        cookies = [{"name": "cf_clearance", "value": "token123"}]
    mock_resp = MagicMock()
    mock_resp.json.return_value = {
        "status": "ok",
        "solution": {
            "userAgent": ua,
            "response": html,
            "status": page_num,
            "cookies": cookies,
        },
    }
    return mock_resp


# ---------------------------------------------------------------------------
# get_cf_credentials
# ---------------------------------------------------------------------------

class TestGetCfCredentials:
    def test_returns_none_triple_when_no_flaresolverr_url(self):
        original = cf_session.FLARESOLVERR_URL
        cf_session.FLARESOLVERR_URL = ""
        try:
            result = get_cf_credentials("https://example.com", 30)
        finally:
            cf_session.FLARESOLVERR_URL = original
        assert result == (None, None, None)

    def test_cache_hit_returns_credentials_without_http_call(self, mocker):
        creds = {"cookies": {"cf_clearance": "abc"}, "user_agent": "ua"}
        cf_session._cf_credentials = creds
        cf_session._cf_credentials_expires_at = time.monotonic() + 9999

        mock_post = mocker.patch("scraper.processors.cf_session.stdlib_requests.post")
        result = get_cf_credentials("https://example.com", 30)

        mock_post.assert_not_called()
        assert result == (creds, None, None)

    def test_cache_miss_calls_flaresolverr_and_returns_html(self, mocker):
        mocker.patch(
            "scraper.processors.cf_session.stdlib_requests.post",
            return_value=_flaresolverr_response(html="<html>solved</html>"),
        )

        credentials, html, status = get_cf_credentials("https://cars.com/", 30)

        assert credentials is not None
        assert credentials["cookies"] == {"cf_clearance": "token123"}
        assert "Chrome/136" in credentials["user_agent"]
        assert html == b"<html>solved</html>"
        assert status == 200

    def test_cache_miss_populates_cache_for_next_call(self, mocker):
        mocker.patch(
            "scraper.processors.cf_session.stdlib_requests.post",
            return_value=_flaresolverr_response(),
        )

        get_cf_credentials("https://cars.com/", 30)

        assert cf_session._cf_credentials is not None
        assert cf_session._cf_credentials_expires_at > time.monotonic()

    def test_second_call_is_cache_hit(self, mocker):
        mock_post = mocker.patch(
            "scraper.processors.cf_session.stdlib_requests.post",
            return_value=_flaresolverr_response(),
        )

        get_cf_credentials("https://cars.com/", 30)
        get_cf_credentials("https://cars.com/", 30)

        mock_post.assert_called_once()  # second call was served from cache

    def test_flaresolverr_error_status_raises(self, mocker):
        mock_resp = MagicMock()
        mock_resp.json.return_value = {"status": "error", "message": "unsolvable"}
        mocker.patch("scraper.processors.cf_session.stdlib_requests.post", return_value=mock_resp)

        with pytest.raises(RuntimeError, match="FlareSolverr failed"):
            get_cf_credentials("https://cars.com/", 30)

    def test_empty_cookies_list_produces_empty_dict(self, mocker):
        mocker.patch(
            "scraper.processors.cf_session.stdlib_requests.post",
            return_value=_flaresolverr_response(cookies=[]),
        )

        credentials, _, _ = get_cf_credentials("https://cars.com/", 30)
        assert credentials["cookies"] == {}


# ---------------------------------------------------------------------------
# invalidate_cf_credentials
# ---------------------------------------------------------------------------

class TestInvalidateCfCredentials:
    def test_zeros_expires_at(self):
        cf_session._cf_credentials_expires_at = 9999999999.0
        invalidate_cf_credentials()
        assert cf_session._cf_credentials_expires_at == 0.0

    def test_next_get_triggers_re_bootstrap(self, mocker):
        cf_session._cf_credentials_expires_at = 9999999999.0
        invalidate_cf_credentials()

        mock_post = mocker.patch(
            "scraper.processors.cf_session.stdlib_requests.post",
            return_value=_flaresolverr_response(),
        )
        get_cf_credentials("https://cars.com/", 30)
        mock_post.assert_called_once()


# ---------------------------------------------------------------------------
# make_cf_session
# ---------------------------------------------------------------------------

class TestMakeCfSession:
    @pytest.fixture(autouse=True)
    def fresh_session_mock(self):
        """Reset the shared Session stub so calls don't accumulate across tests."""
        cf_session.cf_requests.Session.reset_mock()
        cf_session.cf_requests.Session.return_value = MagicMock()
        yield

    def test_with_credentials_sets_user_agent_header(self):
        creds = {
            "user_agent": "Mozilla/5.0 Chrome/136.0.0.0 Safari/537.36",
            "cookies": {},
        }
        session = make_cf_session(creds)
        session.headers.update.assert_called_once_with({"User-Agent": creds["user_agent"]})

    def test_with_credentials_sets_each_cookie(self):
        creds = {
            "user_agent": "Mozilla/5.0 Chrome/136.0.0.0",
            "cookies": {"cf_clearance": "abc", "session_id": "xyz"},
        }
        session = make_cf_session(creds)
        assert session.cookies.set.call_count == 2

    def test_without_credentials_no_header_or_cookie_calls(self):
        session = make_cf_session(None)
        session.headers.update.assert_not_called()
        session.cookies.set.assert_not_called()

    def test_returns_session_object(self):
        session = make_cf_session(None)
        assert session is not None

    def test_with_credentials_returns_session(self):
        creds = {"user_agent": "Mozilla/5.0 Chrome/136.0.0.0", "cookies": {}}
        session = make_cf_session(creds)
        assert session is not None
