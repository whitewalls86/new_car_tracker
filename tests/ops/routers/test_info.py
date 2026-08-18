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

        response = mock_client.get("/info")

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

        response = mock_client.get("/info")

        assert response.status_code == 200
        assert "Active listings" in response.text
        assert "Total price observations" not in response.text

    def test_stale_snapshot_is_labeled_honestly(self, mock_client, mocker):
        stats = {"analytics_data_through_iso": "2026-08-18T17:00:00Z"}
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(stats, status="failed", stale=True),
        )

        response = mock_client.get("/info")

        assert response.status_code == 200
        assert "Analytics data through (stale)" in response.text

    def test_empty_snapshot_keeps_narrative_available(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info.public_stats_cache.get",
            return_value=_presentation(status="not_ready", stale=True),
        )

        response = mock_client.get("/info")

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

        response = mock_client.get("/info")

        assert response.status_code == 200
        duckdb_connect.assert_not_called()
        postgres_connect.assert_not_called()
        upstream_get.assert_not_called()
        refresh.assert_not_called()
