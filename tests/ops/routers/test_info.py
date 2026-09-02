import re
from types import MappingProxyType

from ops.public_stats import PresentationSnapshot
from ops.routers.info import _fmt_stat


def _presentation(stats=None, *, status="ok", stale=False):
    return PresentationSnapshot(
        stats=MappingProxyType(stats or {}),
        status=status,
        stale=stale,
        last_success_at="2026-08-18T18:00:00Z" if stats else None,
    )


class TestFmtStat:
    def test_millions(self):
        assert _fmt_stat(1_500_000) == "1.5M"

    def test_ten_thousands(self):
        assert _fmt_stat(15_000) == "15K"

    def test_thousands(self):
        assert _fmt_stat(1_200) == "1.2K"

    def test_small(self):
        assert _fmt_stat(42) == "42"


class TestInfoEndpoint:
    def test_full_snapshot_renders_analytics_data_boundary(self, mock_client, mocker):
        stats = {
            "active_listings": 500,
            "price_observations": 1_200_000,
            "make_model_pairs": 42,
            "artifacts_per_hour": 10,
            "observations_per_hour": 5,
            "analytics_data_through_iso": "2026-08-18T17:00:00Z",
        }
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(stats),
        )

        response = mock_client.get("/")

        assert response.status_code == 200
        assert "1.2M" in response.text
        assert "Analytics data through" in response.text
        assert "Last pipeline run" not in response.text
        assert "2026-08-18T17:00:00Z" in response.text

    def test_partial_snapshot_returns_200_and_omits_missing_fields(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation({"active_listings": 500}),
        )

        response = mock_client.get("/")

        assert response.status_code == 200
        assert "Active listings" in response.text
        assert "Total price observations" not in response.text

    def test_stale_snapshot_is_labeled_honestly(self, mock_client, mocker):
        stats = {"analytics_data_through_iso": "2026-08-18T17:00:00Z"}
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(stats, status="failed", stale=True),
        )

        response = mock_client.get("/")

        assert response.status_code == 200
        assert "Analytics data through (stale)" in response.text

    def test_empty_snapshot_keeps_narrative_available(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(status="not_ready", stale=True),
        )

        response = mock_client.get("/")

        assert response.status_code == 200
        assert "A production data pipeline for tracking car prices" in response.text
        assert "<h2>Live stats</h2>" not in response.text

    def test_request_path_does_not_touch_storage_or_upstream(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation({"active_listings": 1}),
        )
        duckdb_connect = mocker.patch(
            "duckdb.connect", side_effect=AssertionError("DuckDB must not be queried")
        )
        postgres_connect = mocker.patch(
            "psycopg2.connect", side_effect=AssertionError("Postgres must not be queried")
        )
        upstream_get = mocker.patch(
            "requests.get", side_effect=AssertionError("upstream must not be called")
        )
        refresh = mocker.patch(
            "ops.public_stats.PublicStatsCache.refresh",
            side_effect=AssertionError("request must not refresh files"),
        )

        response = mock_client.get("/")

        assert response.status_code == 200
        duckdb_connect.assert_not_called()
        postgres_connect.assert_not_called()
        upstream_get.assert_not_called()
        refresh.assert_not_called()


# ---------------------------------------------------------------------------
# Plan 138 Stage 1b — the landing page's structure and its truth contract.
#
# The section order below is the plan's §1b sequence, and the barred phrases are
# the dispositions Gate 0 assigned in
# docs/evidence/plan_138_stage_0_baseline_2026-08-31.md. Both are enumerations,
# which the testing contract's first rule normally forbids -- sanctioned here
# because this plan's Stage 5 scopes public-surface drift detection as "the
# absence of the known stale phrases", an assertion about wording rather than
# about the repository. The assertions that can be derived instead are, and sit
# beside them.
# ---------------------------------------------------------------------------

_SECTION_SEQUENCE = [
    "A production data pipeline for tracking car prices",  # 1. hero
    "Live stats",                                          # 2. stats + freshness
    "How data flows",                                      # 3. data journey
    "Production architecture",                             # 4. service cards
    "Platform evolution",                                  # 5. migration track
    "Recent work",                                         # 6. planned / completed
    "Decisions worth explaining",                          # 7. decision stories
    "How it is tested",                                    # 8. testing + CTA
]

# Every phrase Gate 0 disposed of as Replace or Delete, plus the ones the truth
# contract's §3 bars outright. A phrase here must never return to the page.
_BARRED_PHRASES = [
    # §3, barred outright
    "without manual intervention",
    "every failure alerts",
    "Every service exposes",
    # §1b: marketing language this stage replaces with neutral description
    "Cloudflare bypass",
    "anti-detection",
    # Gate 0 rows 1-6: hardcoded counts that drifted
    "13+",
    "40+ make/model",
    "eleven Docker containers",
    "Twelve DAGs",
    "fifteen dbt models",
    "Nine alert rules",
    "971 tests",
    "36 Flyway",
    # Gate 0 rows 7-8: mechanism stated wrongly
    "to HOT tables and Parquet",
    "lives entirely in a dbt",
]

def _flat(html: str) -> str:
    """Collapse whitespace, so an assertion can quote a sentence the template wraps."""
    return re.sub(r"\s+", " ", html)


def _body_only(html: str) -> str:
    """The markup below </head>.

    The stylesheet comments its own blocks with the same section names, and they
    sit above the hero, so an order check over the whole document reads the
    stylesheet's order instead of the page's.
    """
    _, sep, body = html.partition("</head>")
    assert sep, "response is not a full HTML document"
    return body


_FULL_STATS = {
    "active_listings": 500,
    "price_observations": 1_200_000,
    "make_model_pairs": 14,
    "artifacts_per_hour": 10,
    "observations_per_hour": 5,
    "analytics_data_through_iso": "2026-08-18T17:00:00Z",
}


class TestLandingPageStructure:
    def test_sections_appear_in_the_plans_order(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        body = _body_only(mock_client.get("/").text)

        positions = []
        for heading in _SECTION_SEQUENCE:
            assert heading in body, f"section missing from the page: {heading}"
            positions.append(body.index(heading))
        assert positions == sorted(positions), (
            "sections are out of the order Plan 138 §1b specifies: "
            f"{_SECTION_SEQUENCE}"
        )

    def test_the_data_journey_names_its_stages_in_order(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        # The stylesheet carries data-layer selectors too, and they sit above the
        # diagram, so order has to be read from the body alone.
        body = _body_only(mock_client.get("/").text)

        # §1b item 3's journey, plus the two nodes a linear strip cannot show:
        # the operational fork and the queue that closes the loop back to fetch.
        journey = ["fetch", "bronze", "operational", "parse", "staging",
                   "decides", "silver", "mart", "serving"]
        positions = [body.index(f'data-layer="{layer}"') for layer in journey]
        assert positions == sorted(positions), (
            "the diagram does not run Fetch -> Bronze -> Parse/HOT state -> "
            "Silver Parquet -> dbt/DuckDB marts -> Dashboard"
        )

    def test_the_diagram_is_described_for_a_reader_who_cannot_see_it(
        self, mock_client, mocker
    ):
        """An SVG carrying the page's central explanation owes a text equivalent."""
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        flat = _flat(mock_client.get("/").text)

        assert 'role="img"' in flat
        assert 'aria-labelledby="flow-title flow-desc"' in flat
        assert '<title id="flow-title">' in flat
        desc = flat[flat.index('<desc id="flow-desc">'):flat.index("</desc>")]
        # The description has to carry the two forks and the loop, not just name
        # the boxes -- those three facts are the reason the diagram exists.
        for claim in ("two places at once", "single Postgres transaction",
                      "control loop"):
            assert claim in desc, f"the description omits {claim!r}"

    def test_the_control_loop_is_not_signalled_by_colour_alone(
        self, mock_client, mocker
    ):
        """Stage 3a's rule, met on the way in rather than retrofitted."""
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        flat = _flat(mock_client.get("/").text)

        assert 'class="edge loop"' in flat
        assert ".flow-diagram .edge.loop { stroke-dasharray" in flat
        assert "what to fetch next" in flat
        # Every node states its layer as text, so the stroke colour is redundant.
        assert flat.count('class="badge"') >= 9

    def test_the_layer_table_states_a_grain_for_every_storage_layer(
        self, mock_client, mocker
    ):
        """The five layers the README's table names, with the same grains.

        A data journey without grains is a picture; the table is what makes the
        section legible to a reader who wants to know what a row *is*.
        """
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        flat = _flat(mock_client.get("/").text)

        for layer in ("bronze", "operational", "staging", "silver", "mart"):
            assert f'<tr data-layer="{layer}">' in flat, f"no table row for {layer}"
        for grain in (
            "One compressed HTML object per fetched results or detail page",
            "One current row per artifact, listing, VIN mapping, claim, or cooldown",
            "Append-only mutations and typed observations awaiting bulk export",
            "One typed observation per listing appearance, partitioned by source and date",
            "VIN-, listing-, hour-, cohort-, and benchmark-grain products",
        ):
            assert grain in flat, f"grain missing from the table: {grain}"

    def test_the_archiver_is_not_described_as_writing_hot_state(self, mock_client, mocker):
        """Gate 0 row 7. Processing writes the HOT row and its event together."""
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        body = mock_client.get("/").text

        flat = _flat(body)
        assert "It does not populate the HOT tables." in flat
        assert "same Postgres transaction" in flat

    def test_the_executable_backoff_is_placed_in_postgres_not_dbt(
        self, mock_client, mocker
    ):
        """Gate 0 row 8. dbt reads the events; the ops view makes the decision."""
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        body = mock_client.get("/").text

        assert "ops.ops_detail_scrape_queue" in body

    def test_iceberg_is_labeled_a_migration_track_and_not_a_capability(
        self, mock_client, mocker
    ):
        """Truth contract §2: nothing may imply the public dashboard reads Iceberg."""
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        body = mock_client.get("/").text

        flat = _flat(body)
        assert "Migration track" in flat
        assert "the dashboard reads duckdb" in flat.lower()
        assert (
            "Iceberg work is a migration track with its own gates, not a shipped "
            "capability" in flat
        )

    def test_recent_work_carries_both_lists_and_the_recap_pointer(
        self, mock_client, mocker
    ):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        body = mock_client.get("/").text

        assert 'id="work-planned"' in body
        assert 'id="work-completed"' in body
        assert "Planned next" in body
        assert "Recently completed" in body
        # Stage 2 gave the recaps a route; the pointer was a GitHub directory
        # link until then. tests/ops/routers/test_public_routes.py holds the
        # canonical form.
        assert 'href="/recaps"' in body

    def test_no_barred_phrase_survives_on_the_page(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        body = mock_client.get("/").text.lower()

        found = [p for p in _BARRED_PHRASES if p.lower() in body]
        assert not found, f"phrases Gate 0 removed have returned to the page: {found}"

    def test_the_only_make_model_count_on_the_page_is_the_live_stat(
        self, mock_client, mocker
    ):
        """Gate 0 row 1. Three different values rendered on one page load before."""
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        body = mock_client.get("/").text

        assert body.count(">14<") == 1, (
            "a tracked-pair count appears somewhere other than the live stats tile"
        )


    def test_the_heading_outline_skips_no_level(self, mock_client, mocker):
        """Stage 3a's hierarchy rule, derived from the page rather than listed.

        This one is not an enumeration: it walks whatever headings the template
        renders and fails on any jump of more than one level, so a new section
        cannot reintroduce the h2 -> h4 gap the card titles used to open.
        """
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(_FULL_STATS),
        )

        body = _body_only(mock_client.get("/").text)

        outline = [
            (int(m.group(1)), re.sub(r"<[^>]+>", "", m.group(2)).strip())
            for m in re.finditer(r"<h([1-6])[^>]*>(.*?)</h\1>", body, re.S)
        ]
        assert outline and outline[0][0] == 1, "the page does not open with an h1"
        assert sum(1 for level, _ in outline if level == 1) == 1, "more than one h1"

        skips = [
            (prev_text, level, text)
            for (prev, prev_text), (level, text) in zip(outline, outline[1:])
            if level > prev + 1
        ]
        assert not skips, f"heading levels skipped: {skips}"

    def test_the_narrative_survives_an_empty_snapshot(self, mock_client, mocker):
        """Goal 6: the page stays useful when the marts are locked or unavailable."""
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(status="not_ready", stale=True),
        )

        body = _body_only(mock_client.get("/").text)

        assert "<h2>Live stats</h2>" not in body
        for heading in _SECTION_SEQUENCE:
            if heading == "Live stats":
                continue
            assert heading in body, f"lost with the stats snapshot: {heading}"
