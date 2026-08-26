"""Plan 142 Stage 3 stack-release gate behavior."""

from ops import coordination_release


def _state(**overrides):
    state = {"phase": "validating", "kind": "host_maintenance"}
    state.update(overrides)
    return state


def test_expected_services_gate_rejects_an_absent_expected_service(mocker):
    values = {service: 1 for service in coordination_release.EXPECTED_SERVICES}
    values.pop("ops")
    mocker.patch("ops.coordination_release._container_health_values", return_value=values)

    result = coordination_release._expected_services_present(_state())

    assert result == {
        "gate": "expected_services_present",
        "status": "fail",
        "reason": "expected services absent: ops",
    }


def test_expected_services_gate_passes_with_every_expected_metric(mocker):
    mocker.patch(
        "ops.coordination_release._container_health_values",
        return_value={service: 1 for service in coordination_release.EXPECTED_SERVICES},
    )

    assert coordination_release._expected_services_present(_state()) == {
        "gate": "expected_services_present",
        "status": "pass",
    }


def test_expected_services_gate_fails_closed_on_missing_metric_evidence(mocker):
    mocker.patch(
        "ops.coordination_release._container_health_values", side_effect=ValueError("offline")
    )

    assert coordination_release._expected_services_present(_state())["status"] == "unknown"


def test_container_health_rejects_unconfigured_and_absent_services(mocker):
    values = {service: 1 for service in coordination_release.EXPECTED_SERVICES}
    values["ops"] = -1
    values.pop("grafana")
    mocker.patch("ops.coordination_release._container_health_values", return_value=values)

    result = coordination_release._container_health(_state())

    assert result["status"] == "fail"
    assert "grafana" in result["reason"]
    assert "ops" in result["reason"]


def test_container_health_passes_only_healthy_expected_services(mocker):
    mocker.patch(
        "ops.coordination_release._container_health_values",
        return_value={service: 1 for service in coordination_release.EXPECTED_SERVICES},
    )

    assert coordination_release._container_health(_state()) == {
        "gate": "container_health",
        "status": "pass",
    }


def test_container_health_fails_closed_on_missing_metric_evidence(mocker):
    mocker.patch(
        "ops.coordination_release._container_health_values", side_effect=ValueError("offline")
    )

    assert coordination_release._container_health(_state())["status"] == "unknown"


def test_readiness_gate_reports_every_failed_probe(mocker):
    mocker.patch(
        "ops.coordination_release._ready",
        side_effect=lambda target: target != coordination_release.READINESS_TARGETS["loki"],
    )

    result = coordination_release._service_readiness(_state())

    assert result == {
        "gate": "service_readiness",
        "status": "fail",
        "reason": "readiness failed: loki",
    }


def test_readiness_gate_passes_all_direct_probes(mocker):
    mocker.patch("ops.coordination_release._ready", return_value=True)

    assert coordination_release._service_readiness(_state()) == {
        "gate": "service_readiness",
        "status": "pass",
    }


def test_observability_gate_fails_closed_when_evidence_is_unavailable(mocker):
    mocker.patch(
        "ops.coordination_release._prometheus_scalar", side_effect=ValueError("offline")
    )

    result = coordination_release._observability_fresh(_state())

    assert result == {
        "gate": "observability_fresh",
        "status": "unknown",
        "reason": "observability evidence unavailable or malformed",
    }


def test_observability_gate_passes_fresh_scrapes_clean_promtail_and_recent_logs(mocker):
    mocker.patch("ops.coordination_release._prometheus_scalar", side_effect=[12, 0, 1024])
    mocker.patch("ops.coordination_release._loki_has_recent_ingestion", return_value=True)

    assert coordination_release._observability_fresh(_state()) == {
        "gate": "observability_fresh",
        "status": "pass",
    }


def test_observability_gate_rejects_stale_scrapes_and_promtail_errors(mocker):
    mocker.patch("ops.coordination_release._prometheus_scalar", side_effect=[61, 2, 1024])
    mocker.patch("ops.coordination_release._loki_has_recent_ingestion", return_value=True)

    assert "scrape age" in coordination_release._observability_fresh(_state())["reason"]

    mocker.patch("ops.coordination_release._prometheus_scalar", side_effect=[12, 2, 1024])
    assert "Promtail client error" in coordination_release._observability_fresh(_state())["reason"]


def test_observability_gate_rejects_promtail_replay_storm(mocker):
    mocker.patch(
        "ops.coordination_release._prometheus_scalar",
        side_effect=[12, 0, coordination_release.MAX_PROMTAIL_READ_BYTES_5M + 1],
    )
    mocker.patch("ops.coordination_release._loki_has_recent_ingestion", return_value=True)

    assert "replay storm" in coordination_release._observability_fresh(_state())["reason"]


def test_auxiliary_gate_is_keyed_on_sibling_project(mocker):
    response = mocker.Mock()
    response.json.side_effect = [
        {"known": True, "services": ["lakekeeper"]},
        {"known": True, "services": []},
    ]
    mocker.patch("ops.coordination_release.requests.get", return_value=response)

    result = coordination_release._auxiliary_still_stopped(_state())

    assert result == {
        "gate": "auxiliary_still_stopped",
        "status": "fail",
        "reason": "auxiliary services running: cartracker-lakehouse/lakekeeper",
    }


def test_auxiliary_gate_passes_when_all_siblings_remain_stopped(mocker):
    response = mocker.Mock()
    response.json.return_value = {"known": True, "services": []}
    mocker.patch("ops.coordination_release.requests.get", return_value=response)

    assert coordination_release._auxiliary_still_stopped(_state()) == {
        "gate": "auxiliary_still_stopped",
        "status": "pass",
    }


def test_auxiliary_gate_fails_closed_when_sibling_evidence_is_unreadable(mocker):
    response = mocker.Mock()
    response.json.return_value = {"known": False}
    mocker.patch("ops.coordination_release.requests.get", return_value=response)

    assert coordination_release._auxiliary_still_stopped(_state())["status"] == "unknown"


def test_coordination_gate_requires_validating_host_maintenance():
    assert coordination_release._coordination_expected(_state()) == {
        "gate": "coordination_expected",
        "status": "pass",
    }
    assert coordination_release._coordination_expected(_state(kind="deploy"))["status"] == "fail"


def test_release_status_returns_full_gate_list_and_fails_closed(mocker):
    mocker.patch(
        "ops.coordination_release.RELEASE_GATES",
        {
            "pass": lambda _: {"gate": "pass", "status": "pass"},
            "unknown": lambda _: {
                "gate": "unknown", "status": "unknown", "reason": "offline"
            },
        },
    )

    result = coordination_release.collect_release_status(_state())

    assert result["release_ready"] is False
    assert result["blockers"] == ["unknown"]
    assert len(result["gates"]) == 2
