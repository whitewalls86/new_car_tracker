"""Lazy-initialized Playwright browser singleton for SRP scraping."""
from playwright.sync_api import sync_playwright, Browser

_pw = None
_browser: Browser | None = None


def get_browser() -> Browser:
    global _pw, _browser
    if _browser is None or not _browser.is_connected():
        _pw = sync_playwright().start()
        _browser = _pw.chromium.launch(headless=True)
    return _browser


def close_browser() -> None:
    """Tear down the browser singleton so the next call to get_browser() starts fresh.

    Call this after each scrape_results() run so that the next search gets a new
    browser with a clean TLS session — prevents Akamai from blocking based on a
    previously-flagged session.
    """
    global _pw, _browser
    if _browser is not None:
        try:
            _browser.close()
        except Exception:
            pass
        _browser = None
    if _pw is not None:
        try:
            _pw.stop()
        except Exception:
            pass
        _pw = None
