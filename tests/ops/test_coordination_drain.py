"""Scoped drain aggregation tests for Plan 142."""

from datetime import datetime, timezone

import requests

from ops import coordination_drain
from ops.mutation_contract import DRAIN_SOURCES


def _state(phase="draining", scope=None):
    return {"phase": phase, "scope": scope or ["processing"], "generation": 4}


def test_processing_evidence_counts_only_in_flight_not_backlog(mocker):
    database_count = mocker.patch(
        "ops.coordination_drain._database_count",
        return_value={"source": "processing_artifacts", "status": "known", "count": 0},
    )

    coordination_drain._processing_artifacts()

    sql = database_count.call_args.args[1]
    assert "q.status = 'processing'" in sql
    assert "pending" not in sql
    assert "retry" not in sql


def test_service_503_body_is_still_known_positive_evidence(mocker):
    response = mocker.Mock()
    response.json.return_value = {
        "detail": {
            "active_jobs": 2,
            "oldest_started_at": "2026-08-25T01:00:00+00:00",
        }
    }
    mocker.patch("ops.coordination_drain.requests.get", return_value=response)

    result = coordination_drain._service_jobs("processing_jobs")

    assert result["status"] == "known"
    assert result["count"] == 2


def test_scraper_evidence_is_partitioned_by_surface(mocker):
    response = mocker.Mock()
    response.json.return_value = {
        "active_by_surface": {"detail_fetch": 3, "listing_fetch": 1},
        "oldest_by_surface": {
            "detail_fetch": "2026-08-25T01:00:00+00:00",
            "listing_fetch": "2026-08-25T02:00:00+00:00",
        },
    }
    mocker.patch("ops.coordination_drain.requests.get", return_value=response)

    detail = coordination_drain._service_jobs("scraper_detail_jobs")
    listing = coordination_drain._service_jobs("scraper_listing_jobs")

    assert detail["count"] == 3
    assert listing["count"] == 1


def test_unreachable_service_is_unknown_not_zero(mocker):
    mocker.patch(
        "ops.coordination_drain.requests.get",
        side_effect=requests.ConnectionError("unreachable"),
    )

    result = coordination_drain._service_jobs("archiver_jobs")

    assert result["status"] == "unknown"
    assert result["count"] is None


def test_database_count_serializes_oldest_timestamp(mock_cursor_context):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = (2, datetime(2026, 8, 25, 1, tzinfo=timezone.utc))

    result = coordination_drain._database_count("test", "SELECT evidence")

    assert result["count"] == 2
    assert result["oldest_started_at"] == "2026-08-25T01:00:00+00:00"


def test_drained_requires_draining_phase_all_known_and_zero(mocker):
    mocker.patch(
        "ops.coordination_drain.required_drain_sources",
        return_value=frozenset({"processing_jobs", "processing_artifacts"}),
    )
    read = mocker.patch(
        "ops.coordination_drain._read_source",
        side_effect=lambda source, scope, generation: {
            "source": source,
            "status": "known",
            "count": 0,
            "oldest_started_at": None,
        },
    )

    result = coordination_drain.collect_drain_status(_state())

    assert result["drained"] is True
    assert result["blockers"] == []
    assert read.call_count == 2
    assert len(result["sources"]) == len(DRAIN_SOURCES)
    assert any(item["status"] == "not_applicable" for item in result["sources"])


def test_positive_or_unknown_evidence_blocks_and_non_draining_never_reports_drained(mocker):
    mocker.patch(
        "ops.coordination_drain.required_drain_sources",
        return_value=frozenset({"processing_jobs", "processing_artifacts"}),
    )
    evidence = {
        "processing_jobs": {
            "source": "processing_jobs",
            "status": "known",
            "count": 1,
            "oldest_started_at": None,
        },
        "processing_artifacts": {
            "source": "processing_artifacts",
            "status": "unknown",
            "count": None,
            "oldest_started_at": None,
        },
    }
    mocker.patch(
        "ops.coordination_drain._read_source",
        side_effect=lambda source, scope, generation: evidence[source],
    )

    draining = coordination_drain.collect_drain_status(_state())
    requested = coordination_drain.collect_drain_status(_state(phase="requested"))

    assert draining["drained"] is False
    assert draining["blockers"] == ["processing_artifacts", "processing_jobs"]
    assert requested["drained"] is False


def test_gate_evidence_counts_active_runs_that_have_not_observed_generation(mocker):
    database_count = mocker.patch(
        "ops.coordination_drain._database_count",
        return_value={
            "source": "airflow_gate_observations",
            "status": "known",
            "count": 0,
        },
    )

    coordination_drain._airflow_gate_observations(frozenset({"processing"}), 7)

    sql, params = database_count.call_args.args[1:]
    assert "dr.state IN ('queued', 'running')" in sql
    assert "observed.generation = %s" in sql
    assert params[-1] == 7
    assert "results_processing" in params
    assert "scrape_listings" not in params


def test_gate_evidence_without_current_generation_fails_closed():
    result = coordination_drain._airflow_gate_observations(
        frozenset({"processing"}), None
    )

    assert result["status"] == "unknown"
    assert result["count"] is None
