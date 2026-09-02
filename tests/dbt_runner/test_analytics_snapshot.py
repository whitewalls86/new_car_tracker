import json
import threading
from datetime import datetime, timezone
from unittest.mock import MagicMock

import pytest

from dbt_runner.analytics_snapshot import (
    METRIC_NAMES,
    PUBLIC_STAT_NAMES,
    AnalyticsSnapshotManager,
    atomic_write_snapshot,
    load_snapshot,
    validate_snapshot,
)
from dbt_runner.queries import ANALYTICS_METRIC_COLUMNS, PUBLIC_STATS_COLUMNS


def _valid_snapshot():
    return {
        "schema_version": 1,
        "backend": "duckdb",
        "refresh": {
            "status": "ok",
            "attempted_at": "2026-08-18T18:00:00Z",
            "last_success_at": "2026-08-18T18:00:00Z",
            "duration_seconds": 0.12,
        },
        "data_through": "2026-08-18T17:00:00Z",
        "metrics": {name: index + 1 for index, name in enumerate(METRIC_NAMES)},
        "public_stats": {name: index + 10 for index, name in enumerate(PUBLIC_STAT_NAMES)},
        "errors": {},
    }


class _Result:
    def __init__(self, columns, row):
        self.description = [(column,) for column in columns]
        self._row = row

    def fetchone(self):
        return self._row


class _Connection:
    def __init__(self, metric_row, public_row):
        self._results = iter(
            [
                _Result(ANALYTICS_METRIC_COLUMNS, metric_row),
                _Result(PUBLIC_STATS_COLUMNS, public_row),
            ]
        )

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def execute(self, _sql):
        return next(self._results)


def _successful_connection():
    metric_row = tuple(range(1, len(METRIC_NAMES) + 1)) + (
        datetime(2026, 8, 18, 17, tzinfo=timezone.utc),
    )
    public_row = tuple(range(10, 10 + len(PUBLIC_STAT_NAMES)))
    return _Connection(metric_row, public_row)


def test_validate_requires_finite_values_for_the_metrics_present():
    document = _valid_snapshot()
    document["metrics"][METRIC_NAMES[0]] = float("nan")
    with pytest.raises(ValueError, match="finite"):
        validate_snapshot(document)

    document = _valid_snapshot()
    document["metrics"] = []
    with pytest.raises(ValueError, match="metrics must be an object"):
        validate_snapshot(document)


def test_snapshot_written_by_another_release_survives_the_deploy(tmp_path):
    """Adding a metric must not blank the ones already being served.

    The persisted file outlives the container. When exact set equality was
    required, the first restart after a release that added a metric name read
    its own on-disk snapshot as invalid: status fell back to not_ready, so
    `publish_snapshot` set *every* analytics gauge to NaN and /info served an
    empty public_stats until the next hourly refresh.
    """
    path = tmp_path / "snapshot.json"
    previous_release = _valid_snapshot()
    added_since = METRIC_NAMES[-1]
    del previous_release["metrics"][added_since]
    previous_release["metrics"]["cartracker_metric_removed_since"] = 7
    path.write_text(json.dumps(previous_release), encoding="utf-8")

    loaded = load_snapshot(path)

    assert loaded["refresh"]["status"] == "ok"
    assert loaded["errors"] == {}
    assert loaded["public_stats"]
    assert loaded["metrics"][added_since] is None
    assert "cartracker_metric_removed_since" not in loaded["metrics"]
    assert all(
        loaded["metrics"][name] is not None
        for name in METRIC_NAMES
        if name != added_since
    )


def test_atomic_write_round_trips_complete_document(tmp_path):
    path = tmp_path / "snapshot.json"

    atomic_write_snapshot(path, _valid_snapshot())

    assert load_snapshot(path) == validate_snapshot(_valid_snapshot())
    assert not list(tmp_path.glob("*.tmp"))


def test_atomic_replace_failure_retains_last_good_document(tmp_path, mocker):
    path = tmp_path / "snapshot.json"
    atomic_write_snapshot(path, _valid_snapshot())
    original = path.read_text(encoding="utf-8")
    mocker.patch("dbt_runner.analytics_snapshot.os.replace", side_effect=OSError("disk full"))

    with pytest.raises(OSError, match="disk full"):
        atomic_write_snapshot(path, _valid_snapshot())

    assert path.read_text(encoding="utf-8") == original
    assert not list(tmp_path.glob("*.tmp"))


def test_startup_loads_snapshot_without_opening_duckdb(tmp_path, mocker):
    path = tmp_path / "snapshot.json"
    atomic_write_snapshot(path, _valid_snapshot())
    connect = mocker.patch("duckdb.connect", side_effect=AssertionError("must not connect"))

    manager = AnalyticsSnapshotManager(path)

    assert manager.get_snapshot()["refresh"]["status"] == "ok"
    connect.assert_not_called()


def test_absent_or_unsupported_snapshot_is_explicit_not_ready(tmp_path):
    assert load_snapshot(tmp_path / "missing.json")["refresh"]["status"] == "not_ready"
    path = tmp_path / "snapshot.json"
    path.write_text(json.dumps({"schema_version": 99}), encoding="utf-8")
    loaded = load_snapshot(path)
    assert loaded["refresh"]["status"] == "not_ready"
    assert loaded["errors"]["snapshot"] == "snapshot_invalid_or_unsupported"


def test_successful_refresh_executes_queries_and_publishes(tmp_path):
    manager = AnalyticsSnapshotManager(tmp_path / "snapshot.json")

    result = manager.refresh(
        connection_factory=_successful_connection,
        attempted_at="2026-08-18T18:00:00Z",
    )

    assert result["ok"] is True
    persisted = load_snapshot(manager.path)
    assert persisted["refresh"]["last_success_at"] == "2026-08-18T18:00:00Z"
    assert persisted["data_through"] == "2026-08-18T17:00:00Z"
    assert set(persisted["metrics"]) == set(METRIC_NAMES)
    assert set(persisted["public_stats"]) == set(PUBLIC_STAT_NAMES)


def test_failed_refresh_records_attempt_and_retains_last_known_good_values(tmp_path):
    path = tmp_path / "snapshot.json"
    atomic_write_snapshot(path, _valid_snapshot())
    manager = AnalyticsSnapshotManager(path)

    result = manager.refresh(
        connection_factory=MagicMock(side_effect=RuntimeError("query failed")),
        attempted_at="2026-08-18T19:00:00Z",
    )

    assert result["ok"] is False
    failed = load_snapshot(path)
    assert failed["refresh"]["status"] == "failed"
    assert failed["refresh"]["attempted_at"] == "2026-08-18T19:00:00Z"
    assert failed["refresh"]["last_success_at"] == "2026-08-18T18:00:00Z"
    assert failed["metrics"] == validate_snapshot(_valid_snapshot())["metrics"]
    assert failed["public_stats"] == validate_snapshot(_valid_snapshot())["public_stats"]
    assert "query failed" in failed["errors"]["refresh"]


def test_concurrent_readers_only_observe_complete_documents(tmp_path):
    path = tmp_path / "snapshot.json"
    first = _valid_snapshot()
    atomic_write_snapshot(path, first)
    observed = []

    def read_repeatedly():
        for _ in range(100):
            try:
                observed.append(json.loads(path.read_text(encoding="utf-8")))
            except PermissionError:
                # Windows can briefly deny a read while os.replace swaps the file.
                continue

    reader = threading.Thread(target=read_repeatedly)
    reader.start()
    second = _valid_snapshot()
    second["refresh"]["attempted_at"] = "2026-08-18T19:00:00Z"
    second["refresh"]["last_success_at"] = "2026-08-18T19:00:00Z"
    atomic_write_snapshot(path, second)
    reader.join()

    allowed = {
        validate_snapshot(first)["refresh"]["attempted_at"],
        validate_snapshot(second)["refresh"]["attempted_at"],
    }
    assert observed
    assert {document["refresh"]["attempted_at"] for document in observed} <= allowed
