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

    def test_no_flyway_reference(self):
        doc = yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.yml").read_text())
        assert "flyway" not in doc["services"]

    def test_only_expected_volumes_declared(self):
        """Only lakekeeper_pgdata (owned by this project) may be declared in
        the base file -- no production volume reference here at all. The A3
        analytics_db mount lives only in the separate
        docker-compose.lakehouse.a3.yml override (never loaded by CI), so
        that a CI run of the base file (+ its own ci.yml override) never
        needs an external volume that doesn't exist on the runner."""
        doc = yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.yml").read_text())
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


class TestLakehouseA3Override:
    """Plan 112 Gate A3: the read-only analytics DuckDB mount lives only in
    docker-compose.lakehouse.a3.yml, a separate VM/local-manual-only
    override -- never in the base docker-compose.lakehouse.yml, which the CI
    `lakehouse` job also runs A2 against and which has no such external
    volume on the runner."""

    @staticmethod
    def _doc():
        return yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.a3.yml").read_text())

    def test_mounts_analytics_db_read_only(self):
        service = self._doc()["services"]["lakehouse-worker"]
        assert "cartracker_analytics_db:/data/analytics:ro" in service["volumes"]

    def test_analytics_db_volume_declared_external(self):
        spec = self._doc()["volumes"]["cartracker_analytics_db"]
        assert spec["external"] is True

    def test_no_volume_mount_is_writable(self):
        """Defense in depth: every lakehouse-worker volume mount in this
        override must be read-only -- this worker never writes to a
        main-project volume."""
        service = self._doc()["services"]["lakehouse-worker"]
        for mount in service["volumes"]:
            assert mount.endswith(":ro"), f"non-read-only mount: {mount}"

    def test_base_file_has_no_analytics_volume_reference(self):
        """The CI job never loads this override, so the base file must be
        fully CI-safe on its own: no analytics_db mount, no external volume
        declaration that doesn't exist on a CI runner."""
        base_doc = yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.yml").read_text())
        base_service = base_doc["services"]["lakehouse-worker"]
        assert "cartracker_analytics_db" not in (base_doc.get("volumes") or {})
        for mount in base_service.get("volumes") or []:
            assert "analytics" not in mount


class TestLakehouseLocalOverride:
    """Plan 112 Gate A4: docker-compose.lakehouse.local.yml makes the stack
    fully self-contained on a dev box -- throwaway MinIO, non-external
    network, host-published ports, and a read-only bind mount of a *local*
    analytics directory. It must reference no production Docker volume
    (unlike the VM-only a3 override) and no external resource at all."""

    @staticmethod
    def _doc():
        return yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.local.yml").read_text())

    def test_adds_throwaway_minio(self):
        services = self._doc()["services"]
        assert "minio" in services

    def test_network_made_non_external(self):
        doc = self._doc()
        assert doc["networks"]["cartracker-net"]["external"] is False

    def test_publishes_local_host_ports(self):
        services = self._doc()["services"]
        assert any(p.startswith("19000:") for p in services["minio"]["ports"])
        assert any(p.startswith("18181:") for p in services["lakekeeper"]["ports"])

    def test_mounts_local_analytics_dir_read_only(self):
        service = self._doc()["services"]["lakehouse-worker"]
        mounts = service["volumes"]
        analytics_mounts = [m for m in mounts if ":/data/analytics" in m]
        assert len(analytics_mounts) == 1
        mount = analytics_mounts[0]
        assert mount.endswith(":ro"), f"analytics mount not read-only: {mount}"
        # A bind mount of a local host directory, not a named Docker volume.
        source = mount.split(":/data/analytics")[0]
        assert source.startswith("${LAKEHOUSE_LOCAL_ANALYTICS_DIR:-./"), (
            f"expected an env-overridable local bind mount, got: {source}"
        )

    def test_no_volume_mount_is_writable(self):
        service = self._doc()["services"]["lakehouse-worker"]
        for mount in service["volumes"]:
            assert mount.endswith(":ro"), f"non-read-only mount: {mount}"

    def test_sets_duckdb_path(self):
        service = self._doc()["services"]["lakehouse-worker"]
        assert service["environment"]["DUCKDB_PATH"] == "/data/analytics/analytics.duckdb"

    def test_declares_no_volumes_at_all(self):
        """No named volumes, external or otherwise -- in particular never
        the VM's cartracker_analytics_db or any production volume."""
        doc = self._doc()
        assert not doc.get("volumes")

    def test_no_production_volume_or_service_referenced(self):
        """Comments may mention production volume names as documentation, so
        check the parsed YAML's real references, not the raw text."""
        doc = self._doc()
        volume_refs = str(doc.get("volumes")) + str(
            [spec.get("volumes") for spec in doc["services"].values()]
        )
        assert "cartracker_analytics_db" not in volume_refs
        assert "cartracker_pgdata" not in volume_refs
        assert "postgres" not in doc["services"]
        for spec in doc["services"].values():
            for vol_spec in (spec.get("volumes") or []):
                assert "external" not in str(vol_spec)

    def test_ci_override_untouched_by_a4(self):
        """The CI override must not have gained an analytics mount -- CI's
        A2 round-trip needs none, and adding one would break the runner."""
        ci_doc = yaml.safe_load((_REPO_ROOT / "docker-compose.lakehouse.ci.yml").read_text())
        worker = ci_doc["services"].get("lakehouse-worker") or {}
        for mount in worker.get("volumes") or []:
            assert "analytics" not in mount


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
