"""Unit tests for ops/routers/info.py — _load_stats() and GET /info."""
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock

from ops.routers.info import _fmt_stat, _load_stats

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_TS = datetime(2026, 4, 1, 12, 0, 0, tzinfo=timezone.utc)


def _duckdb_ctx(fetchone_val):
    """Return a mock context manager whose __enter__ yields a DuckDB-like con."""
    con = MagicMock()
    con.execute.return_value.fetchone.return_value = fetchone_val
    ctx = MagicMock()
    ctx.__enter__ = MagicMock(return_value=con)
    ctx.__exit__ = MagicMock(return_value=False)
    return ctx


def _patch_all_ok(mocker):
    """Patch DuckDB (4 calls) and Postgres db_cursor to all succeed."""
    mocker.patch(
        "ops.routers.info.duckdb.connect",
        side_effect=[
            _duckdb_ctx((500,)),           # active_listings
            _duckdb_ctx((1_200_000,)),     # price_observations
            _duckdb_ctx((42,)),            # make_model_pairs
            _duckdb_ctx((10.0, 5.0)),      # artifacts_per_hour, observations_per_hour
        ],
    )
    pg_cursor = MagicMock()
    pg_cursor.fetchone.return_value = (_TS,)

    @contextmanager
    def _fake_cursor(**kw):
        yield pg_cursor

    mocker.patch("ops.routers.info.db_cursor", side_effect=_fake_cursor)


# ---------------------------------------------------------------------------
# _fmt_stat
# ---------------------------------------------------------------------------

class TestFmtStat:
    def test_millions(self):
        assert _fmt_stat(1_500_000) == "1.5M"

    def test_ten_thousands(self):
        assert _fmt_stat(15_000) == "15K"

    def test_thousands(self):
        assert _fmt_stat(1_200) == "1.2K"

    def test_small(self):
        assert _fmt_stat(42) == "42"


# ---------------------------------------------------------------------------
# _load_stats — all succeed
# ---------------------------------------------------------------------------

class TestLoadStatsAllOk:
    def test_all_keys_present(self, mocker):
        _patch_all_ok(mocker)
        stats = _load_stats()
        assert "active_listings" in stats
        assert "price_observations" in stats
        assert "make_model_pairs" in stats
        assert "last_pipeline_run_iso" in stats
        assert "artifacts_per_hour" in stats
        assert "observations_per_hour" in stats

    def test_active_listings_value(self, mocker):
        _patch_all_ok(mocker)
        assert _load_stats()["active_listings"] == 500

    def test_price_observations_value(self, mocker):
        _patch_all_ok(mocker)
        assert _load_stats()["price_observations"] == 1_200_000

    def test_make_model_pairs_value(self, mocker):
        _patch_all_ok(mocker)
        assert _load_stats()["make_model_pairs"] == 42

    def test_last_pipeline_run_iso_format(self, mocker):
        _patch_all_ok(mocker)
        assert _load_stats()["last_pipeline_run_iso"] == "2026-04-01T12:00:00Z"

    def test_artifacts_per_hour_rounded(self, mocker):
        _patch_all_ok(mocker)
        assert _load_stats()["artifacts_per_hour"] == 10

    def test_observations_per_hour_rounded(self, mocker):
        _patch_all_ok(mocker)
        assert _load_stats()["observations_per_hour"] == 5


# ---------------------------------------------------------------------------
# _load_stats — individual query failures
# ---------------------------------------------------------------------------

class TestLoadStatsIndividualFailures:
    def _pg_ok(self, mocker):
        pg_cursor = MagicMock()
        pg_cursor.fetchone.return_value = (_TS,)

        @contextmanager
        def _fake_cursor(**kw):
            yield pg_cursor

        mocker.patch("ops.routers.info.db_cursor", side_effect=_fake_cursor)

    def _pg_fail(self, mocker):
        mocker.patch("ops.routers.info.db_cursor", side_effect=Exception("pg down"))

    def test_active_listings_fails_key_absent(self, mocker):
        mocker.patch(
            "ops.routers.info.duckdb.connect",
            side_effect=[
                Exception("duckdb down"),
                _duckdb_ctx((1_000_000,)),
                _duckdb_ctx((42,)),
                _duckdb_ctx((10.0, 5.0)),
            ],
        )
        self._pg_ok(mocker)
        stats = _load_stats()
        assert "active_listings" not in stats
        assert "price_observations" in stats

    def test_price_observations_fails_key_absent(self, mocker):
        mocker.patch(
            "ops.routers.info.duckdb.connect",
            side_effect=[
                _duckdb_ctx((500,)),
                Exception("duckdb down"),
                _duckdb_ctx((42,)),
                _duckdb_ctx((10.0, 5.0)),
            ],
        )
        self._pg_ok(mocker)
        stats = _load_stats()
        assert "price_observations" not in stats
        assert "active_listings" in stats

    def test_make_model_pairs_fails_key_absent(self, mocker):
        mocker.patch(
            "ops.routers.info.duckdb.connect",
            side_effect=[
                _duckdb_ctx((500,)),
                _duckdb_ctx((1_000_000,)),
                Exception("duckdb down"),
                _duckdb_ctx((10.0, 5.0)),
            ],
        )
        self._pg_ok(mocker)
        stats = _load_stats()
        assert "make_model_pairs" not in stats
        assert "active_listings" in stats

    def test_last_pipeline_run_fails_key_absent(self, mocker):
        mocker.patch(
            "ops.routers.info.duckdb.connect",
            side_effect=[
                _duckdb_ctx((500,)),
                _duckdb_ctx((1_000_000,)),
                _duckdb_ctx((42,)),
                _duckdb_ctx((10.0, 5.0)),
            ],
        )
        self._pg_fail(mocker)
        stats = _load_stats()
        assert "last_pipeline_run_iso" not in stats
        assert "active_listings" in stats

    def test_throughput_fails_keys_absent(self, mocker):
        mocker.patch(
            "ops.routers.info.duckdb.connect",
            side_effect=[
                _duckdb_ctx((500,)),
                _duckdb_ctx((1_000_000,)),
                _duckdb_ctx((42,)),
                Exception("duckdb down"),
            ],
        )
        self._pg_ok(mocker)
        stats = _load_stats()
        assert "artifacts_per_hour" not in stats
        assert "observations_per_hour" not in stats
        assert "active_listings" in stats


# ---------------------------------------------------------------------------
# _load_stats — all queries fail
# ---------------------------------------------------------------------------

class TestLoadStatsAllFail:
    def test_all_queries_fail_returns_empty_dict(self, mocker):
        mocker.patch(
            "ops.routers.info.duckdb.connect",
            side_effect=Exception("duckdb down"),
        )
        mocker.patch("ops.routers.info.db_cursor", side_effect=Exception("pg down"))
        stats = _load_stats()
        assert stats == {}


# ---------------------------------------------------------------------------
# GET /info endpoint
# ---------------------------------------------------------------------------

class TestInfoEndpoint:
    def test_returns_200(self, mock_client, mocker):
        mocker.patch("ops.routers.info._load_stats", return_value={})
        resp = mock_client.get("/info")
        assert resp.status_code == 200

    def test_load_stats_raises_still_returns_200(self, mock_client, mocker):
        mocker.patch(
            "ops.routers.info._load_stats", side_effect=Exception("unexpected crash")
        )
        resp = mock_client.get("/info")
        assert resp.status_code == 200

    def test_response_is_html(self, mock_client, mocker):
        mocker.patch("ops.routers.info._load_stats", return_value={})
        resp = mock_client.get("/info")
        assert "text/html" in resp.headers["content-type"]
