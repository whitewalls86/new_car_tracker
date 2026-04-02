"""Unit tests for processors/browser.py"""
import threading
import pytest
from unittest.mock import MagicMock

import processors.browser as browser_mod


@pytest.fixture(autouse=True)
def reset_thread_local():
    """
    The module-level `_local = threading.local()` persists between tests that
    run on the same thread.  Reset browser and pw before/after every test so
    state doesn't leak.
    """
    browser_mod._local.browser = None
    browser_mod._local.pw = None
    yield
    browser_mod._local.browser = None
    browser_mod._local.pw = None


def _make_mock_pw_and_browser():
    """Return (mock_pw_instance, mock_browser) and wire up the call chain."""
    mock_browser = MagicMock()
    mock_browser.is_connected.return_value = True

    mock_pw = MagicMock()
    mock_pw.chromium.launch.return_value = mock_browser

    mock_pw_context = MagicMock()
    mock_pw_context.start.return_value = mock_pw

    return mock_pw_context, mock_pw, mock_browser


class TestGetBrowser:
    def test_launches_chromium_on_first_call(self, mocker):
        mock_pw_ctx, mock_pw, mock_browser = _make_mock_pw_and_browser()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        result = browser_mod.get_browser()

        mock_pw_ctx.start.assert_called_once()
        mock_pw.chromium.launch.assert_called_once_with(headless=True)
        assert result is mock_browser

    def test_returns_cached_instance_on_second_call(self, mocker):
        mock_pw_ctx, mock_pw, mock_browser = _make_mock_pw_and_browser()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        b1 = browser_mod.get_browser()
        b2 = browser_mod.get_browser()

        assert b1 is b2
        assert mock_pw.chromium.launch.call_count == 1

    def test_reconnects_if_browser_disconnected(self, mocker):
        mock_pw_ctx, mock_pw, mock_browser = _make_mock_pw_and_browser()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        # First call: connected
        b1 = browser_mod.get_browser()
        assert mock_pw.chromium.launch.call_count == 1

        # Simulate disconnect
        mock_browser.is_connected.return_value = False

        # Second call: should re-launch
        b2 = browser_mod.get_browser()
        assert mock_pw.chromium.launch.call_count == 2

    def test_returns_browser_object(self, mocker):
        mock_pw_ctx, _, mock_browser = _make_mock_pw_and_browser()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)
        assert browser_mod.get_browser() is mock_browser


class TestCloseBrowser:
    def test_calls_browser_close_and_pw_stop(self, mocker):
        mock_pw_ctx, mock_pw, mock_browser = _make_mock_pw_and_browser()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        browser_mod.get_browser()   # populate _local
        browser_mod.close_browser()

        mock_browser.close.assert_called_once()
        mock_pw.stop.assert_called_once()

    def test_resets_local_state_to_none(self, mocker):
        mock_pw_ctx, _, _ = _make_mock_pw_and_browser()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)

        browser_mod.get_browser()
        browser_mod.close_browser()

        assert getattr(browser_mod._local, "browser", None) is None
        assert getattr(browser_mod._local, "pw", None) is None

    def test_noop_if_no_browser(self):
        # Should not raise even when called on a thread with no browser
        browser_mod.close_browser()  # no exception

    def test_swallows_browser_close_exception(self, mocker):
        mock_pw_ctx, mock_pw, mock_browser = _make_mock_pw_and_browser()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)
        mock_browser.close.side_effect = Exception("already closed")

        browser_mod.get_browser()
        browser_mod.close_browser()  # must not propagate the exception

        # pw.stop() should still be called
        mock_pw.stop.assert_called_once()

    def test_swallows_pw_stop_exception(self, mocker):
        mock_pw_ctx, mock_pw, mock_browser = _make_mock_pw_and_browser()
        mocker.patch.object(browser_mod, "sync_playwright", return_value=mock_pw_ctx)
        mock_pw.stop.side_effect = Exception("already stopped")

        browser_mod.get_browser()
        browser_mod.close_browser()  # must not propagate


class TestThreadIsolation:
    def test_each_thread_gets_own_browser(self, mocker):
        """Each worker thread must receive a distinct browser instance."""
        browsers_seen = []

        def _make_per_thread_pw():
            """Each call to sync_playwright().start() yields a new unique browser."""
            mock_browser = MagicMock()
            mock_browser.is_connected.return_value = True
            mock_pw = MagicMock()
            mock_pw.chromium.launch.return_value = mock_browser
            mock_ctx = MagicMock()
            mock_ctx.start.return_value = mock_pw
            return mock_ctx

        # Return a fresh mock on every invocation
        mocker.patch.object(
            browser_mod, "sync_playwright", side_effect=_make_per_thread_pw
        )

        def worker():
            b = browser_mod.get_browser()
            browsers_seen.append(id(b))
            browser_mod.close_browser()

        t1 = threading.Thread(target=worker)
        t2 = threading.Thread(target=worker)
        t1.start(); t2.start()
        t1.join(); t2.join()

        assert len(browsers_seen) == 2
        # The two threads should have gotten different browser objects
        assert browsers_seen[0] != browsers_seen[1]
