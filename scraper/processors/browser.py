"""Thread-local Patchright browser context for SRP scraping.

Each worker thread gets its own persistent browser context so parallel scrape
jobs don't share state.

Uses launch_persistent_context() instead of launch() so the browser has a real
user profile directory — this prevents Akamai from flagging the session based on
the absence of browser history/cache that headless browsers typically lack.

The context is created once per thread per session (with the chosen fingerprint
profile baked in) and reused across all page fetches in that session. Cookies
(including cf_clearance) therefore persist between page requests.
"""
import os
import threading
from typing import Dict

try:
    from patchright.sync_api import sync_playwright, BrowserContext
except ImportError:
    from playwright.sync_api import sync_playwright, BrowserContext

_local = threading.local()

_USER_DATA_DIR = os.environ.get("PATCHRIGHT_PROFILE_DIR", "/tmp/patchright-profile")


def get_context(profile: Dict) -> BrowserContext:
    """Return this thread's persistent browser context, creating it if needed."""
    ctx: BrowserContext | None = getattr(_local, "context", None)
    if ctx is None:
        _local.pw = sync_playwright().start()
        _local.context = _local.pw.chromium.launch_persistent_context(
            user_data_dir=_USER_DATA_DIR,
            headless=True,
            user_agent=profile["user_agent"],
            extra_http_headers=profile["extra_http_headers"],
            viewport=profile["viewport"],
            locale=profile["locale"],
            args=[
                "--disable-blink-features=AutomationControlled",
                "--no-sandbox",
                "--disable-dev-shm-usage",
            ],
        )
    return _local.context


def close_browser() -> None:
    """Tear down this thread's browser context so the next call starts fresh.

    Call this after each scrape_results() run so that the next search gets a
    new context with a clean TLS session.
    """
    ctx = getattr(_local, "context", None)
    if ctx is not None:
        try:
            ctx.close()
        except Exception:
            pass
        _local.context = None

    pw = getattr(_local, "pw", None)
    if pw is not None:
        try:
            pw.stop()
        except Exception:
            pass
        _local.pw = None
