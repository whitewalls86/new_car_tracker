"""The one definition of what a Cloudflare interstitial title looks like.

Two callers classify challenge pages, and they need the *same* marker set for
different reasons:

- ``processing`` decides whether a stored detail page is a genuine block rather
  than a scrape result. It gates the title check behind the presence of a
  parseable ``initial-activity-data`` blob, so a real page with an odd title is
  never misread as a challenge. See
  ``processing.processors.parse_detail_page._detect_challenge``.
- ``scraper`` classifies the solver's bootstrap response for
  ``cartracker_solver_requests_total`` (Plan 136 Stage 2). That response comes
  back from ``https://www.cars.com/``, which carries no
  ``initial-activity-data`` at all, so the data-presence gate does not apply and
  the title is the whole signal.

Only the marker set is shared. Duplicating the regex would let the two drift,
and the 2026-08-14 outage was eight hours of pages that *were* interstitials
being counted as successful scrapes.
"""
from __future__ import annotations

import re

CHALLENGE_TITLE_RE = re.compile(
    r"just a moment|attention required|checking your browser", re.IGNORECASE
)


def html_title(content: bytes | None) -> str | None:
    """Read the ``<title>`` out of raw response bytes, without a parser.

    Reads the first 4 KB only. The title is in the head, and running
    BeautifulSoup over every fetched page to classify one counter would put a
    parser on the scrape hot path for no gain.
    """
    if not content:
        return None
    try:
        text = content[:4096].decode("utf-8", errors="replace")
    except Exception:
        return None
    m = re.search(r"<title[^>]*>(.*?)</title>", text, re.IGNORECASE | re.DOTALL)
    if not m:
        return None
    return re.sub(r"\s+", " ", m.group(1)).strip()[:200]


def title_looks_like_challenge(title: str | None) -> bool:
    """True when *title* carries a Cloudflare interstitial marker.

    Title-only, deliberately. Do NOT key on ``cdn-cgi/challenge-platform``:
    Cloudflare injects that script reference into *every* cars.com page,
    including valid ones, so it is not a discriminator.
    """
    return bool(title) and bool(CHALLENGE_TITLE_RE.search(title))
