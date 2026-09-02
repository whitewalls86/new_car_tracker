"""Content-addressed URLs for the landing page's static assets.

Plan 138 Stage 3c caches ``/static_ops/*`` for a year with ``immutable``. That
is only safe while a changed file arrives at a changed URL, so the template asks
for every asset it loads through :func:`asset_url`, which appends a hash of the
file's own bytes. A deploy that changes the stylesheet changes the query string,
and the browser fetches it; a deploy that does not, does not.

The hash is read once per path per process. These files are baked into the ops
image and cannot change under a running container, which is exactly why the
``generated/`` subtree is refused below: that side is bind-mounted from the
checkout and republishes on ``git pull``, so a year-long immutable URL computed
at image-build time would freeze it until the next image build -- silently, and
for a year.
"""
import hashlib
from functools import lru_cache
from pathlib import Path

STATIC_DIR = Path(__file__).parent / "static_ops"

# Enough to make an accidental collision impossible in practice and short enough
# to read in a page source.
_DIGEST_LENGTH = 12

# Stage 7's mount seam, from this side.
_MOUNTED = "generated/"


@lru_cache(maxsize=None)
def asset_url(path: str) -> str:
    """``/static_ops/<path>?v=<hash>`` for an authored asset."""
    if path.startswith(_MOUNTED):
        raise ValueError(
            f"{path} is published by git pull, not by an image build. A "
            f"fingerprinted URL for it would be computed once at startup and "
            f"then cached for a year past every republish."
        )
    digest = hashlib.sha256((STATIC_DIR / path).read_bytes()).hexdigest()
    return f"/static_ops/{path}?v={digest[:_DIGEST_LENGTH]}"
