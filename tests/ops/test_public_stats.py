import json
from datetime import datetime, timezone

import pytest

from ops.public_stats import PublicStatsCache


def _document(*, status="ok", last_success_at="2026-08-18T18:00:00Z", stats=None):
    return {
        "schema_version": 1,
        "backend": "duckdb",
        "refresh": {
            "status": status,
            "attempted_at": "2026-08-18T18:00:00Z",
            "last_success_at": last_success_at,
            "duration_seconds": 0.1,
        },
        "data_through": "2026-08-18T17:00:00Z",
        "metrics": {},
        "public_stats": stats
        or {
            "active_listings": 500,
            "price_observations": 1_200_000,
            "make_model_pairs": 42,
            "artifacts_per_hour": 10,
            "observations_per_hour": 5,
        },
        "errors": {},
    }


def _write(path, document):
    path.write_text(json.dumps(document), encoding="utf-8")


def test_refresh_publishes_immutable_full_snapshot(tmp_path):
    path = tmp_path / "snapshot.json"
    _write(path, _document())
    cache = PublicStatsCache(path)

    result = cache.refresh(now=datetime(2026, 8, 18, 18, 5, tzinfo=timezone.utc))

    assert result.status == "ok"
    assert result.stale is False
    assert result.stats["active_listings"] == 500
    assert result.stats["analytics_data_through_iso"] == "2026-08-18T17:00:00Z"
    with pytest.raises(TypeError):
        result.stats["active_listings"] = 1


def test_partial_snapshot_preserves_available_fields(tmp_path):
    path = tmp_path / "snapshot.json"
    _write(path, _document(stats={"active_listings": 500}))
    cache = PublicStatsCache(path)

    result = cache.refresh(now=datetime(2026, 8, 18, 18, 5, tzinfo=timezone.utc))

    assert result.stats["active_listings"] == 500
    assert "price_observations" not in result.stats


def test_failed_or_old_snapshot_is_stale(tmp_path):
    path = tmp_path / "snapshot.json"
    _write(path, _document(status="failed"))
    cache = PublicStatsCache(path)
    assert cache.refresh(now=datetime(2026, 8, 18, 18, 5, tzinfo=timezone.utc)).stale

    _write(path, _document(last_success_at="2026-08-18T17:00:00Z"))
    assert cache.refresh(now=datetime(2026, 8, 18, 18, 5, tzinfo=timezone.utc)).stale


def test_missing_or_unsupported_snapshot_is_empty(tmp_path):
    cache = PublicStatsCache(tmp_path / "missing.json")
    assert cache.refresh().status == "not_ready"
    assert not cache.get().stats

    path = tmp_path / "snapshot.json"
    _write(path, {"schema_version": 99})
    cache = PublicStatsCache(path)
    assert cache.refresh().status == "not_ready"
    assert not cache.get().stats


def test_refresh_failure_retains_last_known_good_presentation(tmp_path):
    path = tmp_path / "snapshot.json"
    _write(path, _document())
    cache = PublicStatsCache(path)
    cache.refresh(now=datetime(2026, 8, 18, 18, 5, tzinfo=timezone.utc))
    path.write_text("{partial", encoding="utf-8")

    result = cache.refresh(now=datetime(2026, 8, 18, 18, 6, tzinfo=timezone.utc))

    assert result.status == "unavailable"
    assert result.stale is True
    assert result.stats["active_listings"] == 500
