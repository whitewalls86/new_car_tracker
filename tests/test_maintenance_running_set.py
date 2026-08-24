"""Plan 142 Stage 0 item 2: the running-set manifest's expected state.

Plan 142 stops the whole stack and reboots the host. What it starts again comes
from a manifest, not from walking the filesystem for Compose files -- because
walking the filesystem has a known, observed failure: the Plan 140 Stage 1 soak
found four stale `unhealthy` containers belonging to the deliberately-paused
`cartracker-lakehouse` and `cartracker-mlflow` projects, and
``container_health/collector.py`` records that ``up -d`` on either sibling
brings that condition straight back.

These tests check ``maintenance-running-set.txt`` against the Compose sources so
the classification cannot drift from the files it describes. They assert the
registry is complete and honest; they do not assert that the defects it records
have been fixed. ``caddy``'s missing restart policy is the live example -- it is
documented here and owed to Stage 2.
"""
from pathlib import Path

import pytest
import yaml

from tests.test_deploy_script import load_health_exemptions

_REPO_ROOT = Path(__file__).parent.parent
_REGISTRY = _REPO_ROOT / "maintenance-running-set.txt"
_MAIN = _REPO_ROOT / "docker-compose.yml"

DEFAULT_PROJECT = "cartracker"

CLASSES = {
    "oneshot",
    "on-demand",
    "profile-running",
    "aux-paused",
    "aux-foreign",
    "restart-gap",
}

# Classes whose services must NOT be expected in a running state once the
# restore finishes. Deliberately not "services the restore does not execute":
# `up -d` does run `flyway` and `airflow-init`, because the Airflow services
# gate on them with `service_completed_successfully` -- they run, complete and
# exit, and a gate that waits for them to be *running* waits forever.
#
# The two classes absent here are the point of the distinction: `profile-running`
# services are restored (with their profile flag), and `restart-gap` services
# are expected running and merely fail to restore themselves. Treating either
# as not-expected-running is how a service silently stays down.
NOT_EXPECTED_RUNNING = {"oneshot", "on-demand", "aux-paused", "aux-foreign"}

# project -> the Compose file that declares it. Overrides (override/test/ci/
# local/a3) add to a project rather than declaring one, so they are not keys.
PROJECT_FILES = {
    DEFAULT_PROJECT: "docker-compose.yml",
    "cartracker-lakehouse": "docker-compose.lakehouse.yml",
    "cartracker-mlflow": "docker-compose.mlflow.yml",
    "cartracker-test": "docker-compose.test.yml",
}


def load_registry() -> dict[str, tuple[str, str]]:
    """``{key: (class, reason)}`` from the shared allowlist parser."""
    entries = {}
    for key, reason in load_health_exemptions(_REGISTRY).items():
        klass, _, rest = reason.partition(" ")
        entries[key] = (klass, rest.strip())
    return entries


def _services(filename: str) -> dict:
    return yaml.safe_load((_REPO_ROOT / filename).read_text())["services"]


def _split(key: str) -> tuple[str, str]:
    project, sep, service = key.rpartition("/")
    return (project, service) if sep else (DEFAULT_PROJECT, key)


class TestRegistryShape:
    def test_the_file_exists(self):
        assert _REGISTRY.exists(), (
            "maintenance-running-set.txt is missing. Plan 142's restore reads "
            "it to tell a deliberately-stopped service from a forgotten one; "
            "without it there is no difference between the two."
        )

    def test_every_entry_declares_a_known_class(self):
        for key, (klass, _) in load_registry().items():
            assert klass in CLASSES, (
                f"{key} has class {klass!r}, which is not one of "
                f"{sorted(CLASSES)}. Classes drive restore behaviour, so an "
                "unrecognised one is a service nobody knows how to bring back."
            )

    def test_every_entry_carries_a_reason(self):
        for key, (klass, reason) in load_registry().items():
            assert len(reason) > 40, (
                f"{key} ({klass}) has no written reason. This list decides "
                "what stays down after a production reboot; an unexplained "
                "entry is one nobody can safely re-evaluate at 2am."
            )

    def test_every_entry_names_a_real_service(self):
        for key in load_registry():
            project, service = _split(key)
            assert project in PROJECT_FILES, (
                f"{key} names project {project!r}, which no Compose file "
                f"declares. Known: {sorted(PROJECT_FILES)}"
            )
            assert service in _services(PROJECT_FILES[project]), (
                f"{key} names a service absent from {PROJECT_FILES[project]}. "
                "A renamed service leaves an entry behind that then silently "
                "covers whatever takes its name next."
            )


class TestDefaultProjectIsFullyClassified:
    """The point of the file: no service in the default project is unaccounted
    for. Anything not named is expected running, so an omission is a service
    that gets started when it should not be -- or one whose absence after a
    restore reads as correct."""

    def test_profile_gated_services_are_all_classified(self):
        registry = load_registry()
        gated = {
            name for name, spec in _services("docker-compose.yml").items()
            if (spec or {}).get("profiles")
        }
        missing = gated - set(registry)
        assert not missing, (
            f"profile-gated services are unclassified: {sorted(missing)}. A "
            "profile-gated service is either restored with its profile flag "
            "(profile-running) or never restored (on-demand), and a plain "
            "`docker compose up -d` silently gets it wrong in one direction."
        )

    def test_services_without_a_restart_policy_are_classified(self):
        registry = load_registry()
        for name, spec in _services("docker-compose.yml").items():
            spec = spec or {}
            if spec.get("restart") not in (None, "no"):
                continue
            assert name in registry, (
                f"{name} declares no `restart:` policy and is not in "
                "maintenance-running-set.txt. Docker will not start it after a "
                "host reboot. Either it is a one-shot/on-demand service and "
                "says so there, or it is a restart-gap and the fleet comes "
                "back missing a service nobody is looking for."
            )

    def test_long_running_services_are_not_silently_absent(self):
        """Every default-project service is classified or expected running."""
        registry = load_registry()
        not_running = {
            key for key, (klass, _) in registry.items()
            if klass in NOT_EXPECTED_RUNNING
        }
        expected_running = {
            name for name, spec in _services("docker-compose.yml").items()
            if not (spec or {}).get("profiles") and name not in not_running
        } | {
            key for key, (klass, _) in registry.items()
            if klass == "profile-running"
        }
        assert "postgres" in expected_running
        assert "ops" in expected_running
        assert "caddy" in expected_running, (
            "caddy is a restart-gap, not an exclusion -- it is expected "
            "running and merely does not restore itself."
        )
        assert "trawl" in expected_running, (
            "trawl is profile-running: it is gated by a profile but must still "
            "be restored, which is exactly what a plain `up -d` gets wrong."
        )
        assert "flyway" not in expected_running
        assert "dbt" not in expected_running


class TestKnownFindingsStayRecorded:
    """These encode findings that are true today and owed to a later stage.
    They fail if the finding is silently dropped from the registry -- not if it
    is fixed, which is a registry edit in the same commit as the fix."""

    def test_caddy_restart_gap_is_recorded_while_it_exists(self):
        spec = _services("docker-compose.yml")["caddy"]
        if spec.get("restart") in (None, "no"):
            klass, _ = load_registry()["caddy"]
            assert klass == "restart-gap", (
                "caddy still has no restart policy but is no longer classed "
                "restart-gap. The public site does not come back on its own "
                "after a reboot and the registry has stopped saying so."
            )

    def test_the_four_soak_containers_are_all_aux_paused(self):
        registry = load_registry()
        for key in (
            "cartracker-lakehouse/lakekeeper",
            "cartracker-lakehouse/lakekeeper-postgres",
            "cartracker-lakehouse/lakekeeper-migrate",
            "cartracker-mlflow/mlflow",
        ):
            assert registry[key][0] == "aux-paused", (
                f"{key} is one of the four stale unhealthy containers the Plan "
                "140 Stage 1 soak found. Restoring it re-arms that finding."
            )

    def test_trawl_and_redis_trawl_are_restored_together(self):
        registry = load_registry()
        assert registry["trawl"][0] == "profile-running"
        assert registry["redis-trawl"][0] == "profile-running", (
            "redis-trawl backs trawl. Restoring the solver without its Redis "
            "is the 2026-08-14 outage with an extra step."
        )
