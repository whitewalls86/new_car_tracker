import math

from prometheus_client import generate_latest

from dbt_runner.analytics_snapshot import METRIC_NAMES, empty_snapshot
from dbt_runner.metrics import (
    DATA_GAUGES,
    LAST_SUCCESS,
    REFRESH_DURATION,
    REFRESH_SUCCESS,
    REGISTRY,
    publish_snapshot,
)


def _snapshot(*, status="ok", missing_metric=None):
    document = empty_snapshot()
    document["refresh"] = {
        "status": status,
        "attempted_at": "2026-08-18T18:00:00Z",
        "last_success_at": "2026-08-18T17:00:00Z",
        "duration_seconds": 0.25,
    }
    document["metrics"] = {
        name: None if name == missing_metric else index + 1
        for index, name in enumerate(METRIC_NAMES)
    }
    return document


def test_complete_snapshot_publishes_all_stable_gauges():
    publish_snapshot(_snapshot())

    assert {name for name in DATA_GAUGES} == set(METRIC_NAMES)
    assert all(not math.isnan(gauge._value.get()) for gauge in DATA_GAUGES.values())
    assert LAST_SUCCESS._value.get() == 1787072400.0
    assert REFRESH_SUCCESS._value.get() == 1
    assert REFRESH_DURATION._value.get() == 0.25


def test_failed_snapshot_sets_data_gauges_nan_and_preserves_last_success():
    publish_snapshot(_snapshot(status="failed"))

    assert all(math.isnan(gauge._value.get()) for gauge in DATA_GAUGES.values())
    assert LAST_SUCCESS._value.get() == 1787072400.0
    assert REFRESH_SUCCESS._value.get() == 0


def test_invalid_metric_is_nan_without_querying_storage():
    missing = METRIC_NAMES[0]
    publish_snapshot(_snapshot(missing_metric=missing))

    assert math.isnan(DATA_GAUGES[missing]._value.get())


def test_restart_loaded_empty_snapshot_is_explicit_not_ready():
    publish_snapshot(empty_snapshot())

    assert all(math.isnan(gauge._value.get()) for gauge in DATA_GAUGES.values())
    assert math.isnan(LAST_SUCCESS._value.get())
    assert REFRESH_SUCCESS._value.get() == 0


def test_prometheus_payload_contains_stable_and_refresh_health_metrics():
    payload = generate_latest(REGISTRY).decode()

    for name in METRIC_NAMES:
        assert name in payload
    assert "cartracker_metrics_last_success_timestamp_seconds" in payload
    assert "cartracker_analytics_snapshot_refresh_success" in payload
    assert "cartracker_analytics_snapshot_refresh_duration_seconds" in payload


def test_metrics_endpoint_has_no_analytics_reader_endpoint(mock_client):
    assert mock_client.get("/metrics").status_code == 200
    assert mock_client.get("/analytics/metrics").status_code == 404


def test_failed_build_releases_build_lock(mock_client, mock_dbt_build_happy_path, mocker):
    mock_dbt_build_happy_path["subprocess_run"].return_value = mocker.MagicMock(
        returncode=1,
        stdout="",
        stderr="failed",
    )

    response = mock_client.post("/dbt/build", json={})

    assert response.status_code == 500
    from dbt_runner import app

    assert app._build_lock.acquire(blocking=False)
    app._build_lock.release()
