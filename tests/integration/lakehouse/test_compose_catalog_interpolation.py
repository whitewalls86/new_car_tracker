"""
Plan 125 Gate 0.5: proves docker-compose.lakehouse.yml's catalog-URI fallback
chain resolves the way the config contract claims, for every combination of
host-side env vars.

This exists because the contract lives in two places that a pure-YAML test
cannot join up: compose interpolation decides what lands in the container, and
shared/iceberg_catalog.py::catalog_uri() decides what consumers do with it.
Compose *always* populates ICEBERG_CATALOG_URI, so catalog_uri()'s runtime
fallback never fires inside the worker -- the legacy fallback has to happen at
interpolation time instead. tests/test_lakehouse_compose_config.py asserts the
raw string contains the nesting; only running compose proves it resolves.

Needs the docker CLI but no running stack: `compose config` interpolates and
prints, it does not pull images or start containers. Gated with a
collection-safe shutil.which check (not importorskip) so it deselects cleanly
under `-m "not integration"`, matching this repo's other conditionally-runnable
integration tests.
"""
import os
import shutil
import subprocess
from pathlib import Path

import pytest
import yaml

pytestmark = [
    pytest.mark.integration,
    pytest.mark.skipif(shutil.which("docker") is None, reason="requires the docker CLI"),
]

_REPO_ROOT = Path(__file__).resolve().parents[3]
_DEFAULT_URI = "http://lakekeeper:8181/catalog"


def _worker_catalog_env(**host_env) -> dict:
    """Resolved lakehouse-worker env, as compose would build it for a host
    shell exporting exactly `host_env`."""
    # Inherit the real environment (the docker CLI needs HOME/USERPROFILE etc.
    # to locate its compose plugin), then pin exactly the vars under test --
    # a developer's shell exporting a real ICEBERG_CATALOG_URI or
    # LAKEKEEPER_CATALOG_URI must not decide this test's outcome.
    env = os.environ.copy()
    env.pop("ICEBERG_CATALOG_URI", None)
    env.pop("LAKEKEEPER_CATALOG_URI", None)
    env.update(
        {
            "MINIO_ROOT_USER": "cartracker",
            "MINIO_ROOT_PASSWORD": "cartracker123",
            "LAKEKEEPER_DB_PASSWORD": "test",
            "LAKEKEEPER_PG_ENCRYPTION_KEY": "test",
        }
    )
    env.update(host_env)
    result = subprocess.run(
        [
            "docker", "compose", "--profile", "lakehouse-worker",
            "-f", "docker-compose.lakehouse.yml",
            "-p", "plan125-interpolation-check", "config",
        ],
        cwd=_REPO_ROOT,
        capture_output=True,
        text=True,
        env=env,
    )
    assert result.returncode == 0, f"compose config failed: {result.stderr}"
    doc = yaml.safe_load(result.stdout)
    return doc["services"]["lakehouse-worker"]["environment"]


class TestWorkerCatalogUriInterpolation:
    def test_no_host_override_uses_the_lakekeeper_default(self):
        env = _worker_catalog_env()
        assert env["ICEBERG_CATALOG_URI"] == _DEFAULT_URI
        assert env["LAKEKEEPER_CATALOG_URI"] == _DEFAULT_URI

    def test_legacy_only_override_repoints_consumers_too(self):
        """The compatibility path: an existing A2/A3/A4 shell exports only
        LAKEKEEPER_CATALOG_URI. Consumers must follow it to the same endpoint,
        not silently keep using the baked-in default."""
        env = _worker_catalog_env(LAKEKEEPER_CATALOG_URI="http://legacy:1/catalog")
        assert env["ICEBERG_CATALOG_URI"] == "http://legacy:1/catalog"
        assert env["LAKEKEEPER_CATALOG_URI"] == "http://legacy:1/catalog"

    def test_neutral_only_override_repoints_consumers_but_not_provisioning(self):
        """A catalog swap points consumers elsewhere; provisioning must keep
        addressing the Lakekeeper server that is still running."""
        env = _worker_catalog_env(ICEBERG_CATALOG_URI="http://neutral:2/catalog")
        assert env["ICEBERG_CATALOG_URI"] == "http://neutral:2/catalog"
        assert env["LAKEKEEPER_CATALOG_URI"] == _DEFAULT_URI

    def test_both_set_keeps_each_path_on_its_own_var(self):
        env = _worker_catalog_env(
            ICEBERG_CATALOG_URI="http://neutral:2/catalog",
            LAKEKEEPER_CATALOG_URI="http://legacy:1/catalog",
        )
        assert env["ICEBERG_CATALOG_URI"] == "http://neutral:2/catalog"
        assert env["LAKEKEEPER_CATALOG_URI"] == "http://legacy:1/catalog"
