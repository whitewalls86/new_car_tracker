"""Drift tests for Plan 142's service-to-surface contract."""

from pathlib import Path

import pytest
import yaml

from ops.coordination_contract import (
    SERVICE_CONTRACTS,
    SERVICE_LIFECYCLES,
    SURFACES,
    expand_targets,
)

REPO_ROOT = Path(__file__).parents[2]


def _compose_services() -> dict:
    return yaml.safe_load((REPO_ROOT / "docker-compose.yml").read_text())["services"]


def _dependencies(spec: dict | None) -> set[str]:
    depends_on = (spec or {}).get("depends_on", {})
    return set(depends_on or {})


def _follower_targets() -> set[str]:
    targets = set()
    for line in (REPO_ROOT / "deploy-followers.txt").read_text().splitlines():
        if line and not line.startswith(("#", " ", "\t")):
            targets.add(line.split()[0])
    return targets


def test_every_compose_service_has_exactly_one_contract_and_no_contract_is_stale():
    compose = set(_compose_services())
    registered = set(SERVICE_CONTRACTS)
    assert registered == compose, (
        f"missing={sorted(compose - registered)}, stale={sorted(registered - compose)}. "
        "Every new service must choose affected surfaces or justify no pause."
    )


def test_compose_dependency_edges_cannot_change_without_contract_review():
    for service, spec in _compose_services().items():
        assert SERVICE_CONTRACTS[service].compose_dependencies == _dependencies(spec), (
            f"{service} depends_on changed. Review whether taking it or its new dependency "
            "offline changes the target's surface and drain closure."
        )


def test_every_surface_is_known_and_empty_contracts_have_a_reason():
    for service, contract in SERVICE_CONTRACTS.items():
        assert contract.surfaces <= SURFACES, service
        assert contract.lifecycle in SERVICE_LIFECYCLES, service
        if not contract.surfaces:
            assert len(contract.no_pause_reason) >= 40, (
                f"{service} claims no production pause without a reviewable reason"
            )
        else:
            assert not contract.no_pause_reason, (
                f"{service} both affects surfaces and claims no pause"
            )


def test_service_lifecycle_describes_current_compose_behavior_not_future_intent():
    assert SERVICE_CONTRACTS["pack-worker"].lifecycle == "continuous"
    assert SERVICE_CONTRACTS["snapshot-worker"].lifecycle == "one_shot"
    assert SERVICE_CONTRACTS["trawl"].lifecycle == "profile_continuous"
    assert SERVICE_CONTRACTS["redis-trawl"].lifecycle == "profile_continuous"
    assert SERVICE_CONTRACTS["flyway"].lifecycle == "initialization"
    assert SERVICE_CONTRACTS["airflow-init"].lifecycle == "initialization"


def test_cached_peer_registry_and_contract_name_the_same_targets():
    declared = {name for name, contract in SERVICE_CONTRACTS.items() if contract.followers}
    assert declared == _follower_targets()


def test_statsd_recreate_expands_to_airflow_control_followers():
    targets, surfaces = expand_targets({"statsd-exporter"})
    assert targets == {
        "statsd-exporter",
        "airflow-scheduler",
        "airflow-dag-processor",
        "airflow-triggerer",
        "airflow-apiserver",
    }
    assert surfaces == {"observability", "airflow_control"}


def test_unknown_target_fails_closed():
    with pytest.raises(ValueError, match="unknown coordination targets"):
        expand_targets({"future-service"})


def test_host_target_expands_to_every_surface():
    targets, surfaces = expand_targets({"host"})

    assert targets == {"host"}
    assert surfaces == SURFACES


def test_host_cannot_be_combined_with_service_targets():
    with pytest.raises(ValueError, match="host cannot be combined"):
        expand_targets({"host", "postgres"})
