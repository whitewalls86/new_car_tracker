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


def test_container_health_passes_an_exempt_service_reading_unconfigured(mocker):
    """The gate has to be able to open on a host that is behaving correctly.

    ``oauth2-proxy`` is expected, distroless, and has read -1 permanently since
    2026-08-20 by design, with ``ct-container-health-unconfigured`` alerting on
    it daily. Requiring 1 from it made the resume gate one that could never
    open -- found 2026-08-29 while scoping the Stage 4 window, before any
    window had been paused against it.
    """
    values = {service: 1 for service in coordination_release.EXPECTED_SERVICES}
    values["oauth2-proxy"] = -1
    mocker.patch("ops.coordination_release._container_health_values", return_value=values)

    assert coordination_release._container_health(_state()) == {
        "gate": "container_health",
        "status": "pass",
    }


def test_container_health_still_rejects_unconfigured_non_exempt_services(mocker):
    """The exemption is the documented list, not a general tolerance for -1."""
    values = {service: 1 for service in coordination_release.EXPECTED_SERVICES}
    values["grafana"] = -1
    mocker.patch("ops.coordination_release._container_health_values", return_value=values)

    result = coordination_release._container_health(_state())

    assert result["status"] == "fail"
    assert "grafana" in result["reason"]


def test_container_health_still_rejects_an_exempt_service_that_is_gone(mocker):
    """An exemption excuses a missing healthcheck contract, never absence.

    An expected service Docker no longer reports publishes 0, and 0 from
    ``oauth2-proxy`` means the front door is down -- exactly the state Stage 3
    refuses to resume onto.
    """
    values = {service: 1 for service in coordination_release.EXPECTED_SERVICES}
    values["oauth2-proxy"] = 0
    mocker.patch("ops.coordination_release._container_health_values", return_value=values)

    result = coordination_release._container_health(_state())

    assert result["status"] == "fail"
    assert "oauth2-proxy" in result["reason"]


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


# scrape age, dropped entries, send failures, read bytes -- the order
# `_observability_fresh` reads them in.
def _observability(scrape_age=12, dropped=0, send_failures=0, read_bytes=1024):
    return [scrape_age, dropped, send_failures, read_bytes]


def test_observability_gate_passes_fresh_scrapes_clean_promtail_and_recent_logs(mocker):
    mocker.patch("ops.coordination_release._prometheus_scalar", side_effect=_observability())
    mocker.patch("ops.coordination_release._loki_has_recent_ingestion", return_value=True)

    assert coordination_release._observability_fresh(_state()) == {
        "gate": "observability_fresh",
        "status": "pass",
    }


def test_observability_gate_rejects_stale_scrapes_and_promtail_errors(mocker):
    mocker.patch(
        "ops.coordination_release._prometheus_scalar", side_effect=_observability(scrape_age=61)
    )
    mocker.patch("ops.coordination_release._loki_has_recent_ingestion", return_value=True)

    assert "scrape age" in coordination_release._observability_fresh(_state())["reason"]

    mocker.patch(
        "ops.coordination_release._prometheus_scalar",
        side_effect=_observability(
            send_failures=coordination_release.MAX_PROMTAIL_SEND_FAILURES_5M + 1
        ),
    )
    assert "Promtail client error" in coordination_release._observability_fresh(_state())["reason"]


def test_observability_gate_rejects_any_dropped_log_entries(mocker):
    """Dropped entries are permanent loss, so this one is absolute.

    ``promtail_dropped_entries_total`` read 0 across seven days of production
    including the 2026-08-25 full-stack recreation, so a non-zero increase is a
    real signal rather than restart noise.
    """
    mocker.patch(
        "ops.coordination_release._prometheus_scalar", side_effect=_observability(dropped=1)
    )
    mocker.patch("ops.coordination_release._loki_has_recent_ingestion", return_value=True)

    reason = coordination_release._observability_fresh(_state())["reason"]

    assert "dropped 1 log entries" in reason


def test_observability_gate_tolerates_the_failed_sends_a_restart_produces(mocker):
    """The gate is read right after `start`, when Loki has just come back.

    A failed send is retried; Promtail has logged 9 in this host's entire life
    and 2 in the last seven days. Failing on the first one would have made this
    gate refuse during exactly the window it exists to clear -- the same
    can-never-open shape as the `oauth2-proxy` defect.
    """
    mocker.patch(
        "ops.coordination_release._prometheus_scalar",
        side_effect=_observability(
            send_failures=coordination_release.MAX_PROMTAIL_SEND_FAILURES_5M
        ),
    )
    mocker.patch("ops.coordination_release._loki_has_recent_ingestion", return_value=True)

    assert coordination_release._observability_fresh(_state())["status"] == "pass"


def test_every_counter_query_survives_a_series_that_has_never_fired():
    """The invariant the original defect had no test for.

    `sum()` over a counter with no series is an empty vector, and
    `_prometheus_scalar` demands exactly one sample -- so an unguarded counter
    query turns the quietest possible Promtail into "evidence unavailable" and
    a permanent blocker. The gate asked for `promtail_client_request_errors_total`,
    which this Promtail publishes under no condition, so `observability_fresh`
    was `unknown` from the day it was written until 2026-08-31.

    Mocking `_prometheus_scalar` is what hid it: the tests above never see the
    query strings. This one does.
    """
    for query in (
        coordination_release.PROMTAIL_DROPPED_ENTRIES_5M,
        coordination_release.PROMTAIL_SEND_FAILURES_5M,
        coordination_release.PROMTAIL_READ_BYTES_5M,
    ):
        assert query.endswith("or vector(0)"), (
            f"{query!r} can return an empty vector, which reads as unknown and "
            "blocks release on a healthy host"
        )

    # `up` is deliberately unguarded: nothing being scraped is unreadable
    # evidence, not a quiet counter, and 0 would read as perfectly fresh.
    assert "vector(0)" not in coordination_release.SCRAPE_AGE_SECONDS


def test_observability_gate_rejects_promtail_replay_storm(mocker):
    mocker.patch(
        "ops.coordination_release._prometheus_scalar",
        side_effect=_observability(read_bytes=coordination_release.MAX_PROMTAIL_READ_BYTES_5M + 1),
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
