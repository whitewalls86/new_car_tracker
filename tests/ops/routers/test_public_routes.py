"""Plan 138 Stage 2: the public route contract, asserted at the application.

``tests/test_caddy_public_routes.py`` asserts that Caddy sends these paths to
``ops`` without authentication. This file asserts what ``ops`` answers with when
it gets there: the canonical root, the ``/info`` forward, the recap route, and
the two files a crawler asks for before anything else.
"""
import json
import re
from types import MappingProxyType

import pytest

from ops.public_stats import PresentationSnapshot
from ops.routers import public


def _presentation(stats=None, *, status="ok", stale=False):
    return PresentationSnapshot(
        stats=MappingProxyType(stats or {}),
        status=status,
        stale=stale,
        last_success_at="2026-08-18T18:00:00Z" if stats else None,
    )


@pytest.fixture
def landing(mock_client, mocker):
    """The landing page, with a snapshot that does not touch the database."""
    mocker.patch(
        "ops.routers.info.public_stats_cache.get",
        return_value=_presentation({"active_listings": 500}),
    )
    return mock_client.get("/").text


@pytest.fixture
def head(landing):
    return landing[: landing.index("</head>")]


# ---------------------------------------------------------------------------
# The canonical root, and the URL that is printed on a resume
# ---------------------------------------------------------------------------

class TestCanonicalRoot:
    """The landing page moved from ``/info`` to ``/``.

    ``/info`` is not retired with it. It is the URL on a resume, a LinkedIn
    profile and a GitHub profile -- copies this repository cannot edit -- so it
    has to keep resolving for as long as those do.
    """

    def test_info_permanently_redirects_to_the_root(self, mock_client):
        response = mock_client.get("/info", follow_redirects=False)

        assert response.status_code == 308
        assert response.headers["location"] == "/"

    def test_the_redirect_is_one_hop_and_ends_on_the_page(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation({"active_listings": 500}),
        )

        response = mock_client.get("/info")

        assert response.status_code == 200
        assert len(response.history) == 1, "more than one redirect hop"
        assert response.url.path == "/"

    def test_the_root_no_longer_redirects_into_the_admin_console(self, mock_client, mocker):
        """``ops`` answered ``/`` with a redirect to ``/admin/searches/`` until now.

        Caddy never routed ``/`` here before this stage, so nothing external
        depended on it -- but ``/`` is public now, and a visitor bounced into
        ``/admin`` would meet a Google sign-in page instead of the project.
        """
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation({"active_listings": 500}),
        )

        response = mock_client.get("/", follow_redirects=False)

        assert response.status_code == 200

    def test_admin_keeps_its_redirect(self, mock_client):
        response = mock_client.get("/admin", follow_redirects=False)

        assert response.status_code == 307
        assert response.headers["location"] == "/admin/searches/"


# ---------------------------------------------------------------------------
# Search metadata -- Stage 2 items 5 to 7
# ---------------------------------------------------------------------------

class TestSearchMetadata:
    def test_the_title_describes_the_project(self, head):
        title = re.search(r"<title>(.*?)</title>", head).group(1)

        assert title != "CarTracker", "a bare product name says nothing in a result list"
        assert len(title) <= 70, f"title is {len(title)} chars and will be truncated"

    def test_a_meta_description_is_present_and_the_right_length(self, head):
        description = re.search(
            r'<meta name="description" content="([^"]+)"', head
        ).group(1)

        assert 50 <= len(description) <= 160, (
            f"description is {len(description)} chars; results show about 155"
        )

    def test_the_canonical_url_is_the_root(self, head):
        """The page answers at ``/`` and at ``/info``. One of them is canonical."""
        canonical = re.search(r'<link rel="canonical" href="([^"]+)"', head).group(1)

        assert canonical == "https://cartracker.info/"

    @pytest.mark.parametrize(
        "prop", ["og:title", "og:description", "og:url", "og:image", "og:type"]
    )
    def test_open_graph_metadata_is_present(self, head, prop):
        assert f'property="{prop}"' in head

    def test_twitter_card_metadata_is_present(self, head):
        assert 'name="twitter:card"' in head
        assert 'name="twitter:title"' in head

    def test_the_social_image_is_an_absolute_url(self, head):
        """A relative og:image is ignored by every scraper that reads it."""
        image = re.search(r'property="og:image" content="([^"]+)"', head).group(1)

        assert image.startswith("https://")

    def test_a_favicon_is_linked(self, head):
        assert 'rel="icon"' in head

    def test_json_ld_names_the_project_and_its_author(self, head):
        blocks = re.findall(
            r'<script type="application/ld\+json">(.*?)</script>', head, re.DOTALL
        )

        assert blocks, "no JSON-LD on the page"
        parsed = [json.loads(block) for block in blocks]
        assert any(entry.get("@type") == "SoftwareSourceCode" for entry in parsed)
        assert any("author" in entry for entry in parsed)


class TestFirstPartyLinks:
    def test_the_recap_link_points_at_the_canonical_route(self, landing):
        """Stage 2 item 8. It pointed at the GitHub directory until this stage."""
        assert 'href="/recaps"' in landing
        assert "tree/master/docs/recaps" not in landing, (
            "the recaps have a route of their own now"
        )


# ---------------------------------------------------------------------------
# The recap route
# ---------------------------------------------------------------------------

class TestRecapRoutes:
    def test_the_index_is_served_at_the_canonical_route(self, mock_client):
        response = mock_client.get("/recaps")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert "Weekly recaps" in response.text

    def test_a_recap_page_is_served_without_a_file_extension(self, mock_client):
        slug = public.published_slugs()[0]

        response = mock_client.get(f"/recaps/{slug}")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/html")
        assert f"https://cartracker.info/recaps/{slug}" in response.text, (
            "the page does not name its own canonical URL"
        )

    def test_the_index_links_the_canonical_route_not_the_generated_file(self, mock_client):
        """The generated files also answer under ``/static_ops/``.

        Relative ``<slug>.html`` links would resolve to whichever of the two the
        reader arrived through. Route-absolute links resolve to the canonical
        one from both, which is what makes the static path a duplicate of one
        address rather than a second address.
        """
        body = mock_client.get("/recaps").text

        assert 'href="/recaps/' in body
        assert '.html"' not in body

    def test_an_unpublished_week_is_a_404_not_a_stack_trace(self, mock_client):
        assert mock_client.get("/recaps/1999-01-03").status_code == 404

    @pytest.mark.parametrize(
        "slug",
        ["index", "index.html", "../../app", "2026-08-30.html", "not-a-date"],
    )
    def test_the_route_only_resolves_a_recap_slug(self, mock_client, slug):
        """The date shape is the whole path guard; nothing else may reach the disk."""
        assert mock_client.get(f"/recaps/{slug}").status_code == 404


# ---------------------------------------------------------------------------
# robots.txt and sitemap.xml
# ---------------------------------------------------------------------------

class TestRobots:
    def test_robots_is_plain_text_and_allows_the_public_root(self, mock_client):
        response = mock_client.get("/robots.txt")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("text/plain")
        assert "Allow: /" in response.text

    def test_robots_points_at_the_sitemap(self, mock_client):
        assert "Sitemap: https://cartracker.info/sitemap.xml" in (
            mock_client.get("/robots.txt").text
        )

    @pytest.mark.parametrize("path", ["/dashboard", "/admin", "/request-access"])
    def test_protected_paths_are_disallowed(self, mock_client, path):
        """Every one of these answers with a Google sign-in page, not content."""
        assert f"Disallow: {path}" in mock_client.get("/robots.txt").text


class TestSitemap:
    def test_the_sitemap_is_xml(self, mock_client):
        response = mock_client.get("/sitemap.xml")

        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/xml")

    def test_it_lists_the_generated_pages_exactly_once_each(self, mock_client):
        """Stage 2's exit: exactly the pages Stage 1e rendered, once each.

        The URL set is read off the generated directory rather than kept by
        hand, so this asserts the derivation as well as the content.
        """
        locs = re.findall(r"<loc>([^<]+)</loc>", mock_client.get("/sitemap.xml").text)

        assert len(locs) == len(set(locs)), "a URL is listed twice"
        expected = {"https://cartracker.info/", "https://cartracker.info/recaps"} | {
            f"https://cartracker.info/recaps/{slug}"
            for slug in public.published_slugs()
        }
        assert set(locs) == expected

    def test_the_static_path_is_not_published_as_a_second_url(self, mock_client):
        """``handle /static_ops/*`` is public, so the pages have two addresses.

        The sitemap names one of them. Publishing both would ask a crawler to
        index the same recap twice under different URLs.
        """
        body = mock_client.get("/sitemap.xml").text

        assert "/static_ops/" not in body

    def test_no_protected_path_is_listed(self, mock_client):
        body = mock_client.get("/sitemap.xml").text

        for path in ("/dashboard", "/admin", "/request-access", "/grafana", "/airflow"):
            assert path not in body, f"{path} is behind OAuth and must not be indexed"


class TestHeadIsAnsweredEverywhereGetIs:
    """Plan 138 Stage 6, measured 2026-09-04.

    FastAPI's ``@router.get`` registers GET alone, so every public route
    answered HEAD with 405 until the external route matrix was finally run in
    full. No browser does a bare HEAD on a page, which is why it survived from
    Stage 2 through Stage 10 unnoticed -- but uptime monitors and link checkers
    do, and to them a 405 reads as the site being down.

    Asserted on the status *and* the content type, because the failure returned
    ``application/json`` -- FastAPI's error body -- on routes whose GET is HTML.
    A fix that answered HEAD with the wrong type would look correct in a status
    check.
    """

    def _assert_head_matches_get(self, mock_client, path):
        get = mock_client.get(path, follow_redirects=False)
        head = mock_client.head(path, follow_redirects=False)

        assert head.status_code == get.status_code, (
            f"HEAD {path} answered {head.status_code} where GET answered "
            f"{get.status_code}"
        )
        assert head.headers.get("content-type") == get.headers.get("content-type")

    def test_the_landing_page_answers_head(self, mock_client):
        mock_client.head("/")
        self._assert_head_matches_get(mock_client, "/")

    def test_the_recap_index_answers_head(self, mock_client):
        mock_client.head("/recaps")
        self._assert_head_matches_get(mock_client, "/recaps")

    def test_a_recap_page_answers_head(self, mock_client):
        # A literal path, not an f-string: the routing-table assertion in
        # tests/test_testing_contract.py reads request literals out of the
        # source, and a computed slug is invisible to it. The guard below is
        # what keeps the literal honest if this recap ever stops being
        # published.
        assert "2026-08-30" in public.published_slugs(), (
            "this test names a recap page by hand so the routing-table "
            "assertion can see it; pick another published slug"
        )

        mock_client.head("/recaps/2026-08-30")
        self._assert_head_matches_get(mock_client, "/recaps/2026-08-30")

    def test_robots_answers_head(self, mock_client):
        mock_client.head("/robots.txt")
        self._assert_head_matches_get(mock_client, "/robots.txt")

    def test_the_sitemap_answers_head(self, mock_client):
        mock_client.head("/sitemap.xml")
        self._assert_head_matches_get(mock_client, "/sitemap.xml")

    def test_the_info_redirect_answers_head_too(self, mock_client):
        """The URL printed on a resume, so a link checker will HEAD it."""
        head = mock_client.head("/info", follow_redirects=False)

        assert head.status_code == 308
        assert head.headers["location"] == "/"
