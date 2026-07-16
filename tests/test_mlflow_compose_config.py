"""
Plan 112 Gate B: config tests for the standalone MLflow Compose stack.

Asserts the isolation invariants the Gate B provenance-bridge chunk requires:
the main docker-compose.yml is never touched to add MLflow, this first chunk
uses an isolated SQLite backend (no production Postgres, no Flyway), and the
artifact store targets an isolated MinIO prefix. No live Docker required --
pure YAML-parsing assertions, same style as tests/test_lakehouse_compose_config.py.
"""
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parent.parent


def _load(filename):
    path = _REPO_ROOT / filename
    assert path.exists(), f"{filename} missing"
    return yaml.safe_load(path.read_text())


class TestMainComposeUntouched:
    def test_main_compose_has_no_mlflow_service(self):
        doc = _load("docker-compose.yml")
        assert "mlflow" not in doc["services"]

    def test_main_compose_volumes_unaffected(self):
        doc = _load("docker-compose.yml")
        assert "mlflow_store" not in (doc.get("volumes") or {})


class TestMlflowComposeStandalone:
    @staticmethod
    def _doc():
        return _load("docker-compose.mlflow.yml")

    def _service(self):
        return self._doc()["services"]["mlflow"]

    def test_mlflow_service_present(self):
        assert "mlflow" in self._doc()["services"]

    def test_builds_from_isolated_mlflow_dockerfile(self):
        assert self._service()["build"]["dockerfile"] == "mlflow/Dockerfile"

    def test_backend_store_is_isolated_sqlite_not_postgres(self):
        """This first Gate B chunk must NOT couple to production Postgres:
        the backend store is SQLite on this project's own volume."""
        service = self._service()
        env = service["environment"]
        assert env["MLFLOW_BACKEND_STORE_URI"].startswith("sqlite:")
        assert "postgres" not in env["MLFLOW_BACKEND_STORE_URI"].lower()
        # And the command must agree with the env var.
        assert "sqlite:" in service["command"]
        assert "postgres" not in service["command"].lower()

    def test_no_production_postgres_or_flyway_dependency(self):
        service = self._service()
        depends_on = service.get("depends_on") or {}
        assert "postgres" not in depends_on
        assert "flyway" not in depends_on
        # No production DB service declared here at all.
        assert "postgres" not in self._doc()["services"]

    def test_artifact_store_uses_isolated_mlflow_prefix(self):
        env = self._service()["environment"]
        dest = env["MLFLOW_ARTIFACTS_DESTINATION"]
        assert "/mlflow/artifacts" in dest
        # Never a production data prefix.
        for forbidden in ("silver", "ops_normalized", "lakehouse_spike", "html/"):
            assert forbidden not in dest

    def test_owns_its_own_named_volume_only(self):
        doc = self._doc()
        assert set(doc["volumes"]) == {"mlflow_store"}
        # Not external -- owned entirely by this standalone project.
        spec = doc["volumes"]["mlflow_store"]
        assert not spec or not spec.get("external")
        assert "cartracker_pgdata" not in str(doc["volumes"])

    def test_joins_external_cartracker_net(self):
        doc = self._doc()
        assert doc["networks"]["cartracker-net"]["external"] is True

    def test_publishes_non_default_host_port(self):
        assert any(p.startswith("15000:") for p in self._service()["ports"])

    def test_has_bounded_memory_limit(self):
        assert self._service()["mem_limit"] == "1g"

    def test_minio_credentials_have_local_defaults(self):
        env = self._service()["environment"]
        assert env["AWS_ACCESS_KEY_ID"] == "${MINIO_ROOT_USER:-cartracker}"
        assert env["AWS_SECRET_ACCESS_KEY"] == "${MINIO_ROOT_PASSWORD:-cartracker123}"
