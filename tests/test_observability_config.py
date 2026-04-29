"""
Smoke tests for Plan 86 observability configuration files.

Parses prometheus.yml and all Grafana dashboard JSON files to catch syntax
errors before they cause silent startup failures in production containers.
No external services required.
"""
import json
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parent.parent


class TestPrometheusConfig:
    def test_prometheus_yml_parses(self):
        path = _REPO_ROOT / "prometheus" / "prometheus.yml"
        assert path.exists(), "prometheus/prometheus.yml missing"
        doc = yaml.safe_load(path.read_text())
        assert "scrape_configs" in doc

    def test_all_expected_jobs_present(self):
        path = _REPO_ROOT / "prometheus" / "prometheus.yml"
        doc = yaml.safe_load(path.read_text())
        job_names = {job["job_name"] for job in doc["scrape_configs"]}
        expected = {"airflow", "postgres", "minio", "ops", "processing", "node"}
        assert expected == job_names, f"Unexpected jobs: {job_names ^ expected}"


class TestGrafanaProvisioning:
    def test_datasource_yml_parses(self):
        path = _REPO_ROOT / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
        assert path.exists()
        doc = yaml.safe_load(path.read_text())
        assert doc["datasources"][0]["type"] == "prometheus"

    def test_dashboards_yml_parses(self):
        path = _REPO_ROOT / "grafana" / "provisioning" / "dashboards" / "dashboards.yml"
        assert path.exists()
        doc = yaml.safe_load(path.read_text())
        assert doc["providers"][0]["type"] == "file"


class TestGrafanaDashboards:
    _DASHBOARD_DIR = _REPO_ROOT / "grafana" / "dashboards"
    _EXPECTED = {"pipeline_health.json", "infrastructure.json", "service_latency.json"}

    def test_all_dashboards_present(self):
        found = {p.name for p in self._DASHBOARD_DIR.glob("*.json")}
        assert self._EXPECTED <= found, f"Missing dashboards: {self._EXPECTED - found}"

    def test_pipeline_health_parses(self):
        doc = json.loads((self._DASHBOARD_DIR / "pipeline_health.json").read_text())
        assert doc["uid"] == "cartracker-pipeline-health"
        assert len(doc["panels"]) > 0

    def test_infrastructure_parses(self):
        doc = json.loads((self._DASHBOARD_DIR / "infrastructure.json").read_text())
        assert doc["uid"] == "cartracker-infrastructure"
        assert len(doc["panels"]) > 0

    def test_service_latency_parses(self):
        doc = json.loads((self._DASHBOARD_DIR / "service_latency.json").read_text())
        assert doc["uid"] == "cartracker-service-latency"
        assert len(doc["panels"]) > 0
