from unittest.mock import MagicMock

from dbt_runner import app


def _result(row):
    result = MagicMock()
    result.fetchone.return_value = row
    return result


def test_read_duckdb_metrics_returns_complete_snapshot(mocker):
    con = MagicMock()
    con.execute.side_effect = [
        _result((1200, 14)),
        _result((7,)),
        _result((96.5,)),
        _result((4.25,)),
        _result((32,)),
        _result((3,)),
    ]
    connect = mocker.patch("duckdb.connect", return_value=con)

    result = app._read_duckdb_metrics()

    assert result == {
        "ok": True,
        "backend": "duckdb",
        "values": {
            "cartracker_observation_count_last_hour": 1200,
            "cartracker_artifact_count_last_hour": 14,
            "cartracker_block_events_last_hour": 7,
            "cartracker_extraction_yield_last_day": 96.5,
            "cartracker_stale_listings_pct": 4.25,
            "cartracker_cooldown_backlog": 32,
            "cartracker_cooldown_permanent": 3,
        },
        "errors": {},
    }
    connect.assert_called_once_with(app._DUCKDB_PATH, read_only=True)
    con.close.assert_called_once_with()


def test_read_duckdb_metrics_preserves_partial_success(mocker):
    con = MagicMock()
    con.execute.side_effect = [
        _result((1200, 14)),
        RuntimeError("mart_block_rate unavailable"),
        _result((96.5,)),
        _result((4.25,)),
        _result((32,)),
        _result((3,)),
    ]
    mocker.patch("duckdb.connect", return_value=con)

    result = app._read_duckdb_metrics()

    assert result["ok"] is False
    assert result["values"]["cartracker_observation_count_last_hour"] == 1200
    assert result["values"]["cartracker_block_events_last_hour"] is None
    assert "mart_block_rate unavailable" in result["errors"][
        "cartracker_block_events_last_hour"
    ]
    con.close.assert_called_once_with()


def test_read_duckdb_metrics_connection_failure_is_all_unknown(mocker):
    mocker.patch("duckdb.connect", side_effect=RuntimeError("database unavailable"))

    result = app._read_duckdb_metrics()

    assert result["ok"] is False
    assert result["backend"] == "duckdb"
    assert set(result["values"]) == app._ANALYTICS_METRIC_NAMES
    assert all(value is None for value in result["values"].values())
    assert result["errors"] == {"connection": "database unavailable"}


def test_metrics_endpoint_returns_snapshot(mock_client, mocker):
    snapshot = {
        "ok": True,
        "backend": "duckdb",
        "values": {"metric": 1},
        "errors": {},
    }
    mocker.patch("dbt_runner.app.is_idle", return_value=True)
    mocker.patch("dbt_runner.app._read_duckdb_metrics", return_value=snapshot)

    response = mock_client.get("/analytics/metrics")

    assert response.status_code == 200
    assert response.json() == snapshot


def test_metrics_endpoint_refuses_while_duckdb_is_busy(mock_client):
    assert app._duckdb_lock.acquire(blocking=False)
    try:
        response = mock_client.get("/analytics/metrics")
    finally:
        app._duckdb_lock.release()

    assert response.status_code == 503
    assert response.json()["detail"]["reason"] == "duckdb_busy"


def test_failed_build_releases_duckdb_lock(
    mock_client, mock_dbt_build_happy_path, mocker
):
    mock_dbt_build_happy_path["subprocess_run"].return_value = mocker.MagicMock(
        returncode=1,
        stdout="",
        stderr="failed",
    )

    response = mock_client.post("/dbt/build", json={})

    assert response.status_code == 500
    assert app._duckdb_lock.acquire(blocking=False)
    app._duckdb_lock.release()
