import json
import re
from datetime import datetime, timezone
from pathlib import Path

import pytest

from ops.public_stats import (
    DAG_REFRESH_INTERVAL_SECONDS,
    DEFAULT_STALE_SECONDS,
    STALE_GRACE_SECONDS,
    PublicStatsCache,
)

_REPO_ROOT = Path(__file__).parent.parent.parent
_PRODUCER_DAG = _REPO_ROOT / "airflow" / "dags" / "hourly_analytics_refresh.py"


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

    # Two missed hourly runs. 17:00 would no longer qualify: the threshold is
    # now the producer's own interval plus slack, so one on-time cycle is fresh.
    _write(path, _document(last_success_at="2026-08-18T16:00:00Z"))
    assert cache.refresh(now=datetime(2026, 8, 18, 18, 5, tzinfo=timezone.utc)).stale


def test_a_snapshot_from_the_last_hourly_run_is_not_stale(tmp_path):
    """The defect this threshold was raised to fix, pinned.

    Measured in production on 2026-09-04: the snapshot read ``status: ok`` with
    ``last_success_at`` 57 minutes old -- a normal hourly cycle -- and the live
    page said "Analytics data through (stale)". At 900 seconds it said that for
    45 of every 60 minutes.
    """
    path = tmp_path / "snapshot.json"
    _write(path, _document(last_success_at="2026-08-18T17:01:29Z"))
    cache = PublicStatsCache(path)

    result = cache.refresh(now=datetime(2026, 8, 18, 17, 58, tzinfo=timezone.utc))

    assert result.status == "ok"
    assert result.stale is False


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


def test_the_stale_threshold_tracks_the_producer_dag_schedule():
    """The threshold is derived from the DAG's cadence, so both must move together.

    Read as text rather than imported: ``apache-airflow`` lives in its own image
    and its own CI venv, so this suite cannot import a DAG module.

    If this fails because the DAG's cadence changed, the fix is to change
    ``DAG_REFRESH_INTERVAL_SECONDS`` to match and leave the grace alone -- not to
    relax the assertion. A threshold shorter than the interval labels every
    healthy snapshot stale; one much longer hides a producer that has stopped.
    """
    source = _PRODUCER_DAG.read_text(encoding="utf-8")
    schedules = re.findall(r'^\s*schedule="([^"]+)",', source, re.MULTILINE)
    assert schedules == ["0 * * * *"], (
        f"{_PRODUCER_DAG.name} declares {schedules!r}. This test only understands "
        f"an hourly cron of the form 'M * * * *'. If the producer's cadence "
        f"changed, set DAG_REFRESH_INTERVAL_SECONDS in ops/public_stats.py to the "
        f"new interval and teach this test the new form."
    )

    minute, hour, dom, month, dow = schedules[0].split()
    assert (hour, dom, month, dow) == ("*", "*", "*", "*")
    assert minute.isdigit(), f"unsupported minute field {minute!r}"

    assert DAG_REFRESH_INTERVAL_SECONDS == 3600
    assert STALE_GRACE_SECONDS == 300
    assert DEFAULT_STALE_SECONDS == DAG_REFRESH_INTERVAL_SECONDS + STALE_GRACE_SECONDS
