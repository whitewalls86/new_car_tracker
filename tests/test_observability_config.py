"""
Smoke tests for Plan 86 (observability) and Plan 104 (logging) configuration files.

Parses prometheus.yml, loki.yml, promtail.yml, and all Grafana dashboard JSON files
to catch syntax errors before they cause silent startup failures in production containers.
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
        expected = {"airflow", "postgres", "minio", "minio_bucket", "ops", "processing", "node"}
        assert expected == job_names, f"Unexpected jobs: {job_names ^ expected}"


class TestPrometheusAndLokiConfig:
    def test_loki_yml_parses(self):
        path = _REPO_ROOT / "loki" / "loki.yml"
        assert path.exists(), "loki/loki.yml missing"
        doc = yaml.safe_load(path.read_text())
        assert "server" in doc
        assert doc["server"]["http_listen_port"] == 3100
        assert "schema_config" in doc

    def test_promtail_yml_parses(self):
        path = _REPO_ROOT / "promtail" / "promtail.yml"
        assert path.exists(), "promtail/promtail.yml missing"
        doc = yaml.safe_load(path.read_text())
        assert "server" in doc
        assert "clients" in doc
        assert "scrape_configs" in doc
        assert len(doc["scrape_configs"]) == 5

    def test_promtail_all_services_present(self):
        path = _REPO_ROOT / "promtail" / "promtail.yml"
        doc = yaml.safe_load(path.read_text())
        job_names = {job["job_name"] for job in doc["scrape_configs"]}
        expected = {"ops", "scraper", "processing", "dbt_runner", "archiver"}
        assert expected == job_names, f"Unexpected promtail jobs: {job_names ^ expected}"


class TestGrafanaProvisioning:
    def test_prometheus_datasource_yml_parses(self):
        path = _REPO_ROOT / "grafana" / "provisioning" / "datasources" / "prometheus.yml"
        assert path.exists()
        doc = yaml.safe_load(path.read_text())
        assert doc["datasources"][0]["type"] == "prometheus"
        assert doc["datasources"][0]["uid"] == "cartracker-prometheus"
        assert doc["datasources"][0]["isDefault"] is True

    def test_loki_datasource_yml_parses(self):
        path = _REPO_ROOT / "grafana" / "provisioning" / "datasources" / "loki.yml"
        assert path.exists(), "grafana/provisioning/datasources/loki.yml missing"
        doc = yaml.safe_load(path.read_text())
        assert len(doc["datasources"]) == 1
        assert doc["datasources"][0]["type"] == "loki"
        assert doc["datasources"][0]["uid"] == "cartracker-loki"

    def test_dashboards_yml_parses(self):
        path = _REPO_ROOT / "grafana" / "provisioning" / "dashboards" / "dashboards.yml"
        assert path.exists()
        doc = yaml.safe_load(path.read_text())
        assert doc["providers"][0]["type"] == "file"


class TestGrafanaDashboards:
    _DASHBOARD_DIR = _REPO_ROOT / "grafana" / "dashboards"
    _EXPECTED = {"pipeline_health.json", "infrastructure.json", "service_latency.json", "logs.json"}

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

    def test_logs_parses(self):
        doc = json.loads((self._DASHBOARD_DIR / "logs.json").read_text())
        assert doc["uid"] == "cartracker-logs"
        assert len(doc["panels"]) == 3
        assert all(p["datasource"]["uid"] == "cartracker-loki" for p in doc["panels"])


class TestGrafanaAlertingProvisioning:
    _ALERTING_DIR = _REPO_ROOT / "grafana" / "provisioning" / "alerting"

    def test_contact_points_yml_parses(self):
        path = self._ALERTING_DIR / "contact_points.yml"
        assert path.exists(), "contact_points.yml missing"
        doc = yaml.safe_load(path.read_text())
        assert doc["contactPoints"][0]["receivers"][0]["type"] == "telegram"

    def test_notification_policies_yml_parses(self):
        path = self._ALERTING_DIR / "notification_policies.yml"
        assert path.exists(), "notification_policies.yml missing"
        doc = yaml.safe_load(path.read_text())
        assert doc["policies"][0]["receiver"] == "telegram"

    def test_rules_yml_parses(self):
        path = self._ALERTING_DIR / "rules.yml"
        assert path.exists(), "rules.yml missing"
        doc = yaml.safe_load(path.read_text())
        assert len(doc["groups"]) >= 2

    def test_rules_yml_all_uids_present(self):
        path = self._ALERTING_DIR / "rules.yml"
        doc = yaml.safe_load(path.read_text())
        all_uids = {r["uid"] for g in doc["groups"] for r in g["rules"]}
        expected = {
            "ct-log-error-spike", "ct-403-log-spike",
            "ct-pipeline-failures", "ct-service-down",
            "ct-scrape-volume-drop", "ct-extraction-yield-drop",
            "ct-stale-listings", "ct-cooldown-backlog", "ct-block-events-spike",
        }
        assert expected <= all_uids, f"Missing rule UIDs: {expected - all_uids}"
