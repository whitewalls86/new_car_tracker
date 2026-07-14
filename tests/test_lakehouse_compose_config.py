"""
Plan 112 Gate A1: config tests for the standalone Lakekeeper Compose stack.

Asserts the isolation invariants the Gate A/B implementation plan requires:
the main docker-compose.yml is never touched to add Lakekeeper services, the
Lakekeeper stack never references production Postgres/volumes, and the CI
override adds a throwaway MinIO on a non-external network. No live Docker
required -- these are pure YAML-parsing assertions.
"""
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parent.parent


def _load(filename):
    path = _REPO_ROOT / filename
    assert path.exists(), f"{filename} missing"
    return yaml.safe_load(path.read_text())


class TestMainComposeUntouched:
    def test_main_compose_has_no_lakekeeper_services(self):
        doc = _load("docker-compose.yml")
        services = doc["services"]
        assert "lakekeeper" not in services
        assert "lakekeeper-postgres" not in services
        assert "lakehouse-worker" not in services

    def test_main_compose_volumes_unaffected(self):
        doc = _load("docker-compose.yml")
        assert "lakekeeper_pgdata" not in doc["volumes"]


class TestLakehouseComposeStandalone:
    @staticmethod
    def _services():
        doc = _load("docker-compose.lakehouse.yml")
        return doc["services"]

    def test_lakekeeper_and_postgres_present(self):
        services = self._services()
        assert "lakekeeper" in services
        assert "lakekeeper-migrate" in services
        assert "lakekeeper-postgres" in services

    def test_uses_public_lakekeeper_catalog_image(self):
        services = self._services()
        for service_name in ("lakekeeper", "lakekeeper-migrate"):
            image = services[service_name]["image"]
            assert "quay.io/lakekeeper/catalog:" in image
            assert "quay.io/lakekeeper/lakekeeper" not in image

    def test_lakekeeper_postgres_uses_own_named_volume(self):
        service = self._services()["lakekeeper-postgres"]
        volume_names = {v.split(":")[0] for v in service["volumes"]}
        assert "lakekeeper_pgdata" in volume_names
        assert "cartracker_pgdata" not in volume_names

    def test_lakekeeper_pgdata_volume_declared_and_not_external(self):
        doc = yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.yml").read_text())
        assert "lakekeeper_pgdata" in doc["volumes"]
        # No `external: true` -- this volume is owned entirely by this
        # standalone project, unlike cartracker_pgdata in the main file.
        spec = doc["volumes"]["lakekeeper_pgdata"]
        assert not spec or not spec.get("external")

    def test_joins_external_cartracker_net(self):
        doc = yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.yml").read_text())
        assert doc["networks"]["cartracker-net"]["external"] is True

    def test_no_production_postgres_service_referenced(self):
        """The Lakekeeper stack must declare no production `postgres`
        service and no dependency on one."""
        services = self._services()
        assert "postgres" not in services
        for name, spec in services.items():
            depends_on = spec.get("depends_on") or {}
            assert "postgres" not in depends_on, f"{name} depends on production postgres"

    def test_lakekeeper_migrate_depends_on_isolated_postgres(self):
        service = self._services()["lakekeeper-migrate"]
        depends_on = service.get("depends_on") or {}
        assert depends_on["lakekeeper-postgres"]["condition"] == "service_healthy"

    def test_lakekeeper_depends_on_successful_migration(self):
        service = self._services()["lakekeeper"]
        depends_on = service.get("depends_on") or {}
        assert depends_on["lakekeeper-migrate"]["condition"] == "service_completed_successfully"

    def test_lakekeeper_commands_are_explicit(self):
        services = self._services()
        assert services["lakekeeper-migrate"]["command"] == ["migrate"]
        assert services["lakekeeper"]["command"] == ["serve"]

    def test_lakekeeper_has_container_healthcheck(self):
        service = self._services()["lakekeeper"]
        healthcheck = service["healthcheck"]
        assert healthcheck["test"] == ["CMD", "/home/nonroot/lakekeeper", "healthcheck"]
        assert healthcheck["retries"] >= 30

    def test_no_flyway_or_production_volume_reference(self):
        doc = yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.yml").read_text())
        assert "flyway" not in doc["services"]
        declared_volumes = set(doc.get("volumes") or {})
        assert declared_volumes == {"lakekeeper_pgdata"}


class TestLakehouseWorkerService:
    """Plan 112 Gate A2: lakehouse-worker must exist only in the standalone
    lakehouse file, be profile-gated so a bare `up` never starts it, and
    build from the isolated lakehouse/ image -- never the shared
    FastAPI-services images."""

    @staticmethod
    def _service():
        doc = yaml.safe_load(
            (_REPO_ROOT / "docker-compose.lakehouse.yml").read_text()
        )
        return doc["services"]["lakehouse-worker"]

    def test_present_in_standalone_lakehouse_file(self):
        service = self._service()
        assert service is not None

    def test_profile_gated(self):
        service = self._service()
        assert service.get("profiles") == ["lakehouse-worker"]

    def test_not_started_by_bare_up(self):
        """A profile-gated service must not appear among services with no
        `profiles` key (those are the ones a bare `up` starts)."""
        doc = yaml.safe_load(
            (_REPO_ROOT / "docker-compose.lakehouse.yml").read_text()
        )
        bare_up_services = {
            name for name, spec in doc["services"].items() if not spec.get("profiles")
        }
        assert "lakehouse-worker" not in bare_up_services

    def test_builds_from_isolated_lakehouse_dockerfile(self):
        service = self._service()
        build = service["build"]
        assert build["dockerfile"] == "lakehouse/Dockerfile"

    def test_joins_cartracker_net(self):
        service = self._service()
        assert "cartracker-net" in service["networks"]

    def test_has_bounded_memory_limit(self):
        service = self._service()
        assert service["mem_limit"] == "6g"

    def test_depends_on_lakekeeper(self):
        service = self._service()
        depends_on = service.get("depends_on") or {}
        assert "lakekeeper" in depends_on

    def test_no_production_postgres_reference(self):
        service = self._service()
        env = service.get("environment") or {}
        assert "PGHOST" not in env
        depends_on = service.get("depends_on") or {}
        assert "postgres" not in depends_on
        assert "flyway" not in depends_on


class TestLakehouseComposeCiOverride:
    @staticmethod
    def _doc():
        return yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.ci.yml").read_text())

    def test_adds_throwaway_minio(self):
        services = self._doc()["services"]
        assert "minio" in services

    def test_network_made_non_external(self):
        doc = self._doc()
        assert doc["networks"]["cartracker-net"]["external"] is False

    def test_uses_distinct_non_default_ports(self):
        services = self._doc()["services"]
        minio_ports = services["minio"]["ports"]
        assert any(p.startswith("19000:") for p in minio_ports)
        lakekeeper_ports = services["lakekeeper"]["ports"]
        assert any(p.startswith("18181:") for p in lakekeeper_ports)

    def test_no_production_postgres_service(self):
        services = self._doc()["services"]
        assert "postgres" not in services

    def test_lakekeeper_still_depends_on_migration(self):
        services = self._doc()["services"]
        depends_on = services["lakekeeper"].get("depends_on") or {}
        assert depends_on["lakekeeper-migrate"]["condition"] == "service_completed_successfully"
