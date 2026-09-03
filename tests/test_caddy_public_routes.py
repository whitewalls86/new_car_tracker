"""Plan 138: the Caddy route contract, and the two things underneath it.

Stage 2 put the public routes here. Stage 3c added the response policy those
routes carry, and the line it must not cross.

**Why this file exists, stated plainly.** ``dashboard/Dockerfile`` runs Streamlit
with ``--server.baseUrlPath=dashboard``, so it serves its app, its
relatively-linked assets, its ``/_stcore/stream`` websocket and the
``/_stcore/health`` path the Compose healthcheck calls all under
``/dashboard/*``, and it 404s at the origin root. The ``/dashboard*`` block
carries those paths explicitly.

That base path is what makes the public root safe, and it exists because the
first Stage 2 deploy did not have it. With Streamlit believing it owned ``/``,
taking the root away removed the fallback its relative asset links resolved
against, and the dashboard went blank **while** ``/dashboard`` **itself still
returned 200** -- so Gate 2's status-code checks passed and the page was empty.
That is the regression this file is here to catch, which is why the assertion
was written in the slice that made the change rather than deferred to Stage 5.
The tests below hold the arrangement from both ends: Streamlit's own paths must
reach it through ``/dashboard*`` rather than through catch-all ordering, and the
root matcher must stay exactly ``/``.

**Stage 3c's line is the second thing.** The public response policy -- a
``default-src 'none'`` CSP, the security headers, compression -- belongs to the
six public handle blocks and to none of the others. Grafana, Airflow, Streamlit
and MinIO each serve inline script and style of their own, so importing it there
would leave every one of them answering 200 with a blank page: the same shape of
failure as above, from the opposite direction.

**These tests resolve paths rather than read the file.** Caddy does not evaluate
``handle`` blocks in source order; it sorts them by matcher specificity. A test
that asserted "the root block appears before the catch-all" would assert
something Caddy does not do, and would pass while the routing was wrong.
``_resolve`` below reproduces the specificity rule instead, so each assertion is
about where a request actually lands.
"""
from __future__ import annotations

import base64
import hashlib
import re
from pathlib import Path

import pytest

from scripts.build_public_recaps import _STYLE

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


# ---------------------------------------------------------------------------
# Plan 138 Stage 3c: the public response policy, and where it stops
# ---------------------------------------------------------------------------

# The public documents, and one public asset. Everything else this site serves
# is behind Google and a role check, and most of it is somebody else's
# application -- explicitly out of this policy's scope.
PUBLIC_DOCUMENTS = [
    "/",
    "/info",
    "/recaps",
    "/recaps/2026-08-30",
    "/robots.txt",
    "/sitemap.xml",
]
PUBLIC_ASSETS = "/static_ops/generated/project-updates.json"

POLICY = "public_response_policy"
DOCUMENT_CACHE = "public_document_cache"


def _snippet(name: str) -> str:
    """The body of a top-level Caddyfile snippet, by name."""
    text = _CADDYFILE.read_text(encoding="utf-8")
    start = text.index(f"({name}) {{")
    depth = 0
    for end, char in enumerate(text[start:], start):
        depth += (char == "{") - (char == "}")
        if depth == 0 and char == "}":
            return text[start:end + 1]
    raise AssertionError(f"snippet ({name}) is never closed")


def _csp() -> str:
    found = re.search(r'Content-Security-Policy "([^"]+)"', _snippet(POLICY))
    assert found, "the policy snippet carries no Content-Security-Policy"
    return found.group(1)


def _directives(csp: str) -> dict[str, str]:
    parts = (part.strip() for part in csp.split(";"))
    return {
        part.split(" ", 1)[0]: part.split(" ", 1)[1] if " " in part else ""
        for part in parts
        if part
    }


class TestThePublicResponsePolicy:
    @pytest.mark.parametrize("path", PUBLIC_DOCUMENTS + [PUBLIC_ASSETS])
    def test_every_public_route_carries_it(self, path):
        assert f"import {POLICY}" in _resolve(path).body, (
            f"{path} is public and serves without the CSP or the security headers"
        )

    @pytest.mark.parametrize(
        "path",
        [
            "/dashboard",
            "/dashboard/_stcore/stream",
            "/admin",
            "/admin/searches/",
            "/grafana",
            "/airflow",
            "/pgadmin",
            "/minio/",
            "/oauth2/callback",
            "/request-access",
            "/anything-else",
        ],
    )
    def test_no_other_route_carries_it(self, path):
        """The plan is explicit about this, and names the cost.

        Grafana, Airflow, Streamlit and MinIO each serve inline script and style
        of their own. A ``default-src 'none'`` policy would leave every one of
        them answering 200 with a blank page -- the same shape of failure Stage 2
        found at the root, and the same one a status-code check cannot see.
        """
        assert f"import {POLICY}" not in _resolve(path).body, (
            f"{path} is not this stage's surface and would break under the policy"
        )

    def test_it_names_every_header_the_stage_owes(self):
        body = _snippet(POLICY)
        for header in (
            "Content-Security-Policy",
            "X-Content-Type-Options",
            "Referrer-Policy",
            "Permissions-Policy",
        ):
            assert header in body, f"the policy is missing {header}"
        assert "nosniff" in body
        assert "frame-ancestors 'none'" in _csp()

    def test_it_compresses_the_text_the_public_surface_is_made_of(self):
        """Caddy's default match already covers ``text/*``, ``application/json``,
        ``application/xml``, ``application/javascript`` and ``image/svg+xml`` --
        the whole list this stage owes. Restating them in a ``match`` block would
        be a second copy of Caddy's own list, free to drift from it.
        """
        assert re.search(r"^\s+encode\b.*\bzstd\b.*\bgzip\b", _snippet(POLICY), re.M)

    def test_the_policy_defaults_to_refusing(self):
        directives = _directives(_csp())
        assert directives["default-src"] == "'none'"
        assert directives["base-uri"] == "'none'"
        assert directives["script-src"] == "'self'"
        # Neither public document contains a form.
        assert directives["form-action"] == "'none'"
        assert "'unsafe-inline'" not in _csp()
        assert "'unsafe-eval'" not in _csp()
        # Pico draws its own form controls with fourteen data: URIs.
        assert directives["img-src"] == "'self' data:"
        # The one fetch on the landing page, for the roadmap projection.
        assert directives["connect-src"] == "'self'"

    def test_the_style_hash_is_the_recap_generators_own_stylesheet(self):
        """The recap pages keep their stylesheet inline, by decision: Stage 3d
        settled that they do not share the landing page's ``info.css``.

        ``'unsafe-inline'`` would have bought that for one word, and would also
        have admitted any style arriving through a recap's Markdown. The hash
        admits the generator's constant and nothing else, at the price of this
        coupling -- change ``_STYLE`` and the Caddyfile must change with it. This
        test *is* that coupling. Without it the recap pages would render
        unstyled in production and nothing here would fail.
        """
        inline = "\n" + _STYLE
        digest = base64.b64encode(hashlib.sha256(inline.encode()).digest()).decode()

        assert f"'sha256-{digest}'" in _directives(_csp())["style-src"], (
            "the CSP's style hash no longer matches scripts/build_public_recaps.py"
        )

    def test_the_public_documents_revalidate(self):
        assert 'Cache-Control "no-cache"' in _snippet(DOCUMENT_CACHE)
        for path in PUBLIC_DOCUMENTS:
            assert f"import {DOCUMENT_CACHE}" in _resolve(path).body, path


class TestTheStaticAssetCachePolicy:
    """One route, two policies, and getting them the wrong way round is silent.

    ``/static_ops/*`` serves both the authored assets, which ship in the ops
    image and are addressed by a hash of their own bytes, and the generated
    artifacts Stage 7 publishes with ``git pull`` at a stable URL. Only the first
    kind may be kept for a year.
    """

    def test_a_fingerprinted_url_is_kept_for_a_year(self):
        body = _resolve(PUBLIC_ASSETS).body
        assert "@fingerprinted query v=*" in body
        assert (
            'header @fingerprinted Cache-Control "public, max-age=31536000, immutable"'
            in body
        )

    def test_everything_else_there_is_kept_briefly(self):
        body = _resolve(PUBLIC_ASSETS).body
        assert "@unversioned not query v=*" in body
        found = re.search(
            r'header @unversioned Cache-Control "public, max-age=(\d+)"', body
        )
        assert found, "unversioned assets have no cache policy of their own"
        assert int(found.group(1)) <= 3600, (
            "a git-pull-published artifact would sit in browsers this long"
        )

    def test_the_two_matchers_cannot_both_apply(self):
        """``not query v=*`` is the exact complement of ``query v=*``. Written as
        two independent matchers they could overlap, and which ``Cache-Control``
        won would then depend on Caddy's directive ordering rather than on the
        request.
        """
        body = _resolve(PUBLIC_ASSETS).body
        assert body.count("query v=*") == 2
        assert body.count("not query v=*") == 1

    def test_the_asset_route_does_not_take_the_document_cache(self):
        assert f"import {DOCUMENT_CACHE}" not in _resolve(PUBLIC_ASSETS).body
