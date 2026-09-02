"""Plan 138 Stage 2: the Caddy route contract, and the coupling underneath it.

**Why this file exists, stated plainly.** ``dashboard/Dockerfile`` runs Streamlit
with no ``--server.baseUrlPath``, so Streamlit believes it is mounted at ``/``
and serves its own machinery from the root: ``/_stcore/health`` (the Compose
healthcheck calls exactly that path), ``/_stcore/stream`` for the websocket, and
its static bundle. Nothing rewrites those paths. They resolved, before Stage 2
and after it, only because the Caddyfile's final catch-all forwards everything
unmatched to ``dashboard:8501``.

Stage 2 takes ``/`` away from that catch-all. Widen the new root handler from
``/`` to ``/*`` and the dashboard loses its assets and its websocket **while**
``/dashboard`` **itself still returns 200** -- so Gate 2's status-code checks
pass and the page is blank. That is the regression this file is here to catch,
and it is why the assertion was written in the slice that made the change rather
than deferred to Stage 5.

**These tests resolve paths rather than read the file.** Caddy does not evaluate
``handle`` blocks in source order; it sorts them by matcher specificity. A test
that asserted "the root block appears before the catch-all" would assert
something Caddy does not do, and would pass while the routing was wrong.
``_resolve`` below reproduces the specificity rule instead, so each assertion is
about where a request actually lands.
"""
from __future__ import annotations

import re
from pathlib import Path

import pytest

_CADDYFILE = Path(__file__).parent.parent / "Caddyfile"

# The upstream a request is expected to reach, by name rather than by port, so a
# failure message says "the dashboard" rather than "8501".
OPS = "ops:8060"
DASHBOARD = "dashboard:8501"


class Block:
    """One ``handle`` block: what it matches, where it sends, and whether it authenticates."""

    def __init__(self, paths: list[str], body: str) -> None:
        self.paths = paths
        self.body = body

    @property
    def upstream(self) -> str | None:
        found = re.search(r"reverse_proxy\s+(\S+)", self.body)
        return found.group(1) if found else None

    @property
    def authenticated(self) -> bool:
        return "forward_auth" in self.body

    @property
    def is_catch_all(self) -> bool:
        return not self.paths

    def matches(self, path: str) -> bool:
        if self.is_catch_all:
            return True
        return any(_matches(pattern, path) for pattern in self.paths)

    def specificity(self, path: str) -> int:
        """Caddy orders by matcher length; the catch-all is always last."""
        if self.is_catch_all:
            return -1
        return max(
            len(pattern.rstrip("*"))
            for pattern in self.paths
            if _matches(pattern, path)
        )


def _matches(pattern: str, path: str) -> bool:
    if pattern.endswith("*"):
        return path.startswith(pattern[:-1])
    return path == pattern


def _parse() -> list[Block]:
    """Every ``handle`` block in the site, with its named matchers resolved."""
    text = _CADDYFILE.read_text(encoding="utf-8")
    named = dict(
        re.findall(r"^\s*@(\w+)\s+path\s+(.+)$", text, flags=re.MULTILINE)
    )

    blocks: list[Block] = []
    lines = text.splitlines()
    index = 0
    while index < len(lines):
        opener = re.match(r"^\s*handle(_path)?\s*(.*?)\s*\{\s*$", lines[index])
        if not opener:
            index += 1
            continue
        matcher = opener.group(2)
        if matcher.startswith("@"):
            paths = named[matcher[1:]].split()
        else:
            paths = matcher.split()

        depth = 1
        body: list[str] = []
        index += 1
        while index < len(lines) and depth:
            depth += lines[index].count("{") - lines[index].count("}")
            if depth:
                body.append(lines[index])
            index += 1
        blocks.append(Block(paths, "\n".join(body)))
    return blocks


def _resolve(path: str) -> Block:
    """The block Caddy would hand this path to."""
    candidates = [block for block in _parse() if block.matches(path)]
    assert candidates, f"nothing in the Caddyfile matches {path}"
    return max(candidates, key=lambda block: block.specificity(path))


# ---------------------------------------------------------------------------
# The Streamlit root-path coupling
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "path",
    [
        "/dashboard/_stcore/health",
        "/dashboard/_stcore/stream",
        "/dashboard/static/js/index.js",
    ],
)
def test_streamlits_own_paths_resolve_under_its_base_path(path):
    """Rewritten 2026-09-02, after the first version of this file shipped a
    regression it could not see.

    It used to assert that ``/_stcore/*`` at the **origin root** reached the
    dashboard, because Streamlit ran with no ``--server.baseUrlPath`` and served
    its machinery from there. That assertion was true, was mutation-tested three
    ways, stayed true through the whole 2026-09-02 incident -- and the dashboard
    was unusable, because the failure was never about ``/_stcore/*``. Streamlit
    answers *any* unrecognised path with its SPA shell, whose asset links are
    relative, so moving ``/`` moved what those links resolved against.

    Build-order step 3b gave Streamlit a base path. It now serves its app, its
    assets, its websocket and its health endpoint under ``/dashboard/`` and
    **404s at the origin root**, so these paths must reach it through the
    ``/dashboard*`` block rather than through catch-all ordering. That is the
    coupling replaced by something stated, which is what this file is for.
    """
    block = _resolve(path)
    assert block.upstream == DASHBOARD, (
        f"{path} is Streamlit's own path and now resolves to {block.upstream}"
    )
    assert not block.is_catch_all, (
        f"{path} is reaching the dashboard through the catch-all again. The base "
        f"path exists so that /dashboard* carries it explicitly"
    )
    assert block.authenticated, f"{path} must stay behind the role check"


def test_the_catch_all_is_no_longer_what_serves_the_dashboard():
    """It still exists and still points at Streamlit, and nothing depends on it.

    Kept rather than deleted: removing the catch-all is a routing change with its
    own blast radius and belongs to Plan 165, not here. What changed is that it
    is no longer load-bearing -- asserted by the test above, which requires every
    Streamlit path to resolve somewhere else.
    """
    catch_alls = [block for block in _parse() if block.is_catch_all]
    assert len(catch_alls) == 1
    assert catch_alls[0].upstream == DASHBOARD
    assert catch_alls[0].authenticated, "the catch-all must not become public"


def test_the_public_root_matches_only_the_root():
    """An exact matcher, asserted as exact.

    ``handle /`` and ``handle /*`` are one character apart and the second one
    silently breaks the dashboard.
    """
    root = _resolve("/")
    assert root.paths == ["/"], f"the root handler matches {root.paths}, not ['/']"
    assert root.upstream == OPS
    assert not root.authenticated, "the landing page is public"


# ---------------------------------------------------------------------------
# The public surface
# ---------------------------------------------------------------------------

@pytest.mark.parametrize(
    "path",
    [
        "/",
        "/info",
        "/recaps",
        "/recaps/2026-08-30",
        "/robots.txt",
        "/sitemap.xml",
        "/static_ops/generated/project-updates.json",
    ],
)
def test_public_paths_reach_ops_without_authentication(path):
    block = _resolve(path)
    assert block.upstream == OPS
    assert not block.authenticated, f"{path} is public and must not enter OAuth"


@pytest.mark.parametrize(
    "path",
    ["/dashboard", "/dashboard/anything", "/admin", "/admin/searches/", "/grafana"],
)
def test_protected_paths_still_authenticate(path):
    assert _resolve(path).authenticated, f"{path} lost its authentication"


def test_a_path_that_merely_starts_with_recaps_is_not_public():
    """``/recaps`` is two exact-ish matchers, not a prefix.

    ``handle /recaps*`` would also hand ``/recapsecret`` to ops. The matcher is
    written as ``/recaps /recaps/*`` so it cannot.
    """
    assert _resolve("/recapsecret").is_catch_all
