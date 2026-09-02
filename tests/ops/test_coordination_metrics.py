"""Plan 142 scrape-time coordination and gate-health metrics."""

from datetime import datetime, timedelta, timezone

from ops.coordination_metrics import CoordinationCollector


def _samples(families):
    return {
        sample.name: sample
        for family in families
        for sample in family.samples
    }


def test_describe_never_queries_postgres(mocker):
    db_cursor = mocker.patch("ops.coordination_metrics.db_cursor")

    names = {family.name for family in CoordinationCollector().describe()}

    assert names == {
        "cartracker_coordination_state_readable",
        "cartracker_coordination_gate_evidence_known",
        "cartracker_coordination_state_info",
        "cartracker_coordination_state_age_seconds",
        "cartracker_coordination_gate_unobserved_runs",
    }
    db_cursor.assert_not_called()


def test_active_state_exports_age_and_current_generation_gate_evidence(
    mock_cursor_context, mocker
):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "kind": "service_maintenance",
        "phase": "draining",
        "generation": 9,
        "scope": ["processing"],
        "updated_at": datetime.now(timezone.utc) - timedelta(minutes=5),
    }
    gate = mocker.patch(
        "ops.coordination_metrics._airflow_gate_observations",
        return_value={"status": "known", "count": 2},
    )

    samples = _samples(list(CoordinationCollector().collect()))

    assert samples["cartracker_coordination_state_readable"].value == 1
    assert samples["cartracker_coordination_state_info"].labels == {
        "kind": "service_maintenance",
        "phase": "draining",
    }
    assert samples["cartracker_coordination_state_age_seconds"].value >= 300
    assert samples["cartracker_coordination_gate_evidence_known"].value == 1
    assert samples["cartracker_coordination_gate_unobserved_runs"].value == 2
    gate.assert_called_once_with(frozenset({"processing"}), 9)


def test_idle_state_does_not_poll_gate_evidence(mock_cursor_context, mocker):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "kind": None,
        "phase": "none",
        "generation": 9,
        "scope": [],
        "updated_at": datetime.now(timezone.utc),
    }
    gate = mocker.patch("ops.coordination_metrics._airflow_gate_observations")

    samples = _samples(list(CoordinationCollector().collect()))

    assert samples["cartracker_coordination_gate_evidence_known"].value == 1
    assert samples["cartracker_coordination_gate_unobserved_runs"].value == 0
    gate.assert_not_called()


def test_database_uncertainty_exports_unknown_never_healthy(
    mock_db_connection_error, mock_logger_error
):
    samples = _samples(list(CoordinationCollector().collect()))

    assert samples["cartracker_coordination_state_readable"].value == 0
    assert samples["cartracker_coordination_gate_evidence_known"].value == 0
    assert "cartracker_coordination_state_info" not in samples


def test_unknown_gate_evidence_is_explicit(mock_cursor_context, mocker):
    _, cursor = mock_cursor_context
    cursor.fetchone.return_value = {
        "kind": "deploy",
        "phase": "draining",
        "generation": 3,
        "scope": ["archive"],
        "updated_at": datetime.now(timezone.utc),
    }
    mocker.patch(
        "ops.coordination_metrics._airflow_gate_observations",
        return_value={"status": "unknown", "count": None},
    )

    samples = _samples(list(CoordinationCollector().collect()))

    assert samples["cartracker_coordination_gate_evidence_known"].value == 0
    assert samples["cartracker_coordination_gate_unobserved_runs"].value == 0
