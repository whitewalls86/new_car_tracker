"""Browser fingerprint profiles for SRP scraping.

Each profile provides a consistent set of user agent, sec-ch-ua headers,
platform, and viewport dimensions.  One profile is selected per scrape
session and used for every page in that session so the requests look like
a single user browsing.
"""
import random
from typing import Dict, Tuple

# ---------------------------------------------------------------------------
# Chrome version profiles – keep versions close to the Patchright/Playwright
# bundled Chromium so JS feature-detection doesn't reveal a mismatch.
# ---------------------------------------------------------------------------
_CHROME_VERSIONS = ["132", "133", "134", "135"]

_VIEWPORTS: list[Tuple[int, int]] = [
    (1920, 1080),
    (1536, 864),
    (1440, 900),
    (1366, 768),
    (1600, 900),
    (1280, 720),
]

# ---------------------------------------------------------------------------
# ZIP code pools
# ---------------------------------------------------------------------------
# National: spread across US metros.  With maximum_distance=all the ZIP
# doesn't affect results — it only changes the fingerprint.
ZIP_POOL_NATIONAL = [
    "10001",  # New York
    "90210",  # Beverly Hills
    "60601",  # Chicago
    "33101",  # Miami
    "85001",  # Phoenix
    "98101",  # Seattle
    "30301",  # Atlanta
    "02101",  # Boston
    "80202",  # Denver
    "75201",  # Dallas
]

# Local: Houston metro ZIPs — all within the same ~200-mile radius.
ZIP_POOL_LOCAL = [
    "77080",  # Spring Branch
    "77024",  # Memorial
    "77040",  # Northwest Houston
    "77056",  # Galleria
    "77084",  # West Houston
    "77002",  # Downtown
    "77494",  # Katy
    "77479",  # Sugar Land
    "77058",  # Clear Lake
    "77338",  # Humble
]


def random_profile() -> Dict:
    """Return a complete, internally-consistent browser fingerprint."""
    version = random.choice(_CHROME_VERSIONS)
    width, height = random.choice(_VIEWPORTS)
    return {
        "user_agent": (
            f"Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
            f"AppleWebKit/537.36 (KHTML, like Gecko) "
            f"Chrome/{version}.0.0.0 Safari/537.36"
        ),
        "extra_http_headers": {
            "sec-ch-ua": (
                f'"Chromium";v="{version}", '
                f'"Google Chrome";v="{version}", '
                f'"Not:A-Brand";v="99"'
            ),
            "sec-ch-ua-mobile": "?0",
            "sec-ch-ua-platform": '"Windows"',
        },
        "viewport": {"width": width, "height": height},
        "locale": "en-US",
    }


def random_zip(scope: str) -> str:
    """Pick a random ZIP code appropriate for the given scope."""
    if scope == "local":
        return random.choice(ZIP_POOL_LOCAL)
    return random.choice(ZIP_POOL_NATIONAL)


def human_delay(page_num: int) -> float:
    """Return a human-like delay in seconds between page fetches.

    - Baseline 8-20s (much slower than the old 3-8s)
    - First 1-2 pages get extra time (user 'reading' results)
    - 10% chance of a longer pause (user distracted)
    """
    base = random.uniform(8, 20)

    # First pages: user is absorbing results
    if page_num <= 2:
        base += random.uniform(5, 15)

    # Occasional distraction pause
    if random.random() < 0.10:
        base += random.uniform(15, 45)

    return base
