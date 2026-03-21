"""Thread-local Patchright browser for SRP scraping.

Each worker thread gets its own browser instance so parallel scrape jobs
don't share (and clobber) each other's browser state.

Uses Patchright (a Playwright fork) which patches common automation
detection vectors: CDP Runtime.enable leak, navigator.webdriver,
HeadlessChrome sec-ch-ua, and automation-related Chrome flags.
"""
import threading

try:
    from patchright.sync_api import sync_playwright, Browser
except ImportError:
    # Fallback to regular playwright if patchright isn't installed yet
    from playwright.sync_api import sync_playwright, Browser

_local = threading.local()


def get_browser() -> Browser:
    browser: Browser | None = getattr(_local, "browser", None)
    if browser is None or not browser.is_connected():
        _local.pw = sync_playwright().start()
        _local.browser = _local.pw.chromium.launch(headless=True)
    return _local.browser


def close_browser() -> None:
    """Tear down this thread's browser so the next call to get_browser() starts fresh.

    Call this after each scrape_results() run so that the next search gets a new
    browser with a clean TLS session — prevents Akamai from blocking based on a
    previously-flagged session.
    """
    browser = getattr(_local, "browser", None)
    if browser is not None:
        try:
            browser.close()
        except Exception:
            pass
        _local.browser = None

    pw = getattr(_local, "pw", None)
    if pw is not None:
        try:
            pw.stop()
        except Exception:
            pass
        _local.pw = None
