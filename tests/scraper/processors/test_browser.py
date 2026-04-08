"""Unit tests for processors/browser.py"""
import threading
from unittest.mock import MagicMock

import pytest

import scraper.processors.browser as browser_mod


@pytest.fixture(autouse=True)
def reset_thread_local():
    """
    The module-level `_local = threading.local()` persists between tests that
    run on the same thread.  Reset context and pw before/after every test so
    state doesn't leak.
    """
    browser_mod._local.context = None
    browser_mod._local.pw = None
    yield
    browser_mod._local.context = None
    browser_mod._local.pw = None


def _make_mock_pw_and_context():
    """Return (mock_pw_context, mock_pw, mock_context) and wire up the call chain."""
    mock_context = MagicMock()

    mock_pw = MagicMock()
    mock_pw.chromium.launch_persistent_context.return_value = mock_context

    mock_pw_context = MagicMock()
    mock_pw_context.start.return_value = mock_pw

    return mock_pw_context, mock_pw, mock_context


class TestGetBrowser:
    def test_launches_chromium_on_first_call(self, mocker):
        mock_pw_ctx, mock_pw, mock_context = _make_mock_pw_and_context()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }
        result = browser_mod.get_context(profile=profile)

        mock_pw_ctx.start.assert_called_once()
        mock_pw.chromium.launch_persistent_context.assert_called_once()
        assert result is mock_context

    def test_returns_cached_instance_on_second_call(self, mocker):
        mock_pw_ctx, mock_pw, mock_context = _make_mock_pw_and_context()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }
        b1 = browser_mod.get_context(profile)
        b2 = browser_mod.get_context(profile)

        assert b1 is b2
        assert mock_pw.chromium.launch_persistent_context.call_count == 1

    def test_returns_same_context_on_multiple_calls(self, mocker):
        mock_pw_ctx, mock_pw, mock_context = _make_mock_pw_and_context()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }
        # Multiple calls return the same context instance
        c1 = browser_mod.get_context(profile)
        c2 = browser_mod.get_context(profile)
        assert c1 is c2
        assert mock_pw.chromium.launch_persistent_context.call_count == 1

    def test_returns_context_object(self, mocker):
        mock_pw_ctx, _, mock_context = _make_mock_pw_and_context()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)
        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }
        assert browser_mod.get_context(profile) is mock_context


class TestCloseBrowser:
    def test_calls_context_close_and_pw_stop(self, mocker):
        mock_pw_ctx, mock_pw, mock_context = _make_mock_pw_and_context()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }
        browser_mod.get_context(profile)   # populate _local
        browser_mod.close_browser()

        mock_context.close.assert_called_once()
        mock_pw.stop.assert_called_once()

    def test_resets_local_state_to_none(self, mocker):
        mock_pw_ctx, _, _ = _make_mock_pw_and_context()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }
        browser_mod.get_context(profile)
        browser_mod.close_browser()

        assert getattr(browser_mod._local, "context", None) is None
        assert getattr(browser_mod._local, "pw", None) is None

    def test_noop_if_no_browser(self):
        # Should not raise even when called on a thread with no browser
        browser_mod.close_browser()  # no exception

    def test_swallows_context_close_exception(self, mocker):
        mock_pw_ctx, mock_pw, mock_context = _make_mock_pw_and_context()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)
        mock_context.close.side_effect = Exception("already closed")

        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }
        browser_mod.get_context(profile)
        browser_mod.close_browser()  # must not propagate the exception

        # pw.stop() should still be called
        mock_pw.stop.assert_called_once()

    def test_swallows_pw_stop_exception(self, mocker):
        mock_pw_ctx, mock_pw, mock_context = _make_mock_pw_and_context()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)
        mock_pw.stop.side_effect = Exception("already stopped")

        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }
        browser_mod.get_context(profile)
        browser_mod.close_browser()  # must not propagate


class TestThreadIsolation:
    def test_each_thread_gets_own_context(self, mocker):
        """Each worker thread must receive a distinct context instance."""
        contexts_seen = []

        def _make_per_thread_pw():
            """Each call to sync_playwright().start() yields a new unique context."""
            mock_context = MagicMock()
            mock_pw = MagicMock()
            mock_pw.chromium.launch_persistent_context.return_value = mock_context
            mock_ctx = MagicMock()
            mock_ctx.start.return_value = mock_pw
            return mock_ctx

        # Return a fresh mock on every invocation
        mocker.patch.object(
            browser_mod, "sync_playwright", side_effect=_make_per_thread_pw
        )

        profile = {
            "user_agent": "test-agent",
            "extra_http_headers": {},
            "viewport": {"width": 1280, "height": 720},
            "locale": "en-US",
        }

        def worker():
            c = browser_mod.get_context(profile)
            contexts_seen.append(id(c))
            browser_mod.close_browser()

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start()
        t2.start()
        t1.join()
        t2.join()

        assert len(contexts_seen) == 2
        # The two threads should have gotten different context objects
        assert contexts_seen[0] != contexts_seen[1]
