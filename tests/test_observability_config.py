"""
Smoke tests for Plan 86 (observability) and Plan 104 (logging) configuration files.

Parses prometheus.yml, loki.yml, promtail.yml, and all Grafana dashboard JSON files
to catch syntax errors before they cause silent startup failures in production containers.
No external services required.
"""
import json
import re
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
        expected = {
            "airflow",
            "postgres",
            "minio",
            "minio_bucket",
            "ops",
            "dbt_runner",
            "processing",
            "node",
            "container-health",
            "scraper",
        }
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
        assert len(doc["scrape_configs"]) == 7

    def test_promtail_all_services_present(self):
        path = _REPO_ROOT / "promtail" / "promtail.yml"
        doc = yaml.safe_load(path.read_text())
        job_names = {job["job_name"] for job in doc["scrape_configs"]}
        expected = {
            "ops", "scraper", "processing", "dbt_runner", "archiver",
            "pack-worker", "docker-operations",
        }
        assert expected == job_names, f"Unexpected promtail jobs: {job_names ^ expected}"

    def test_promtail_pack_worker_path(self):
        path = _REPO_ROOT / "promtail" / "promtail.yml"
        doc = yaml.safe_load(path.read_text())
        job = next(
            job for job in doc["scrape_configs"] if job["job_name"] == "pack-worker"
        )
        labels = job["static_configs"][0]["labels"]
        assert labels["service"] == "pack-worker"
        assert labels["__path__"] == "/logs/pack-worker/app.log*"

    def test_container_stdout_selection_is_explicit_and_excludes_loki(self):
        path = _REPO_ROOT / "promtail" / "promtail.yml"
        doc = yaml.safe_load(path.read_text())
        job = next(
            job for job in doc["scrape_configs"]
            if job["job_name"] == "docker-operations"
        )
        filters = job["docker_sd_configs"][0]["filters"]
        assert filters == [{"name": "label", "values": ["promtail.enable=true"]}]

        compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text())
        selected = {
            name for name, service in compose["services"].items()
            if service.get("labels", {}).get("promtail.enable") == "true"
        }
        assert selected == {
            "oauth2-proxy",
            "airflow-dag-processor",
            "airflow-scheduler",
            "airflow-apiserver",
        }

    def test_stage_5_retention_is_single_90_day_policy(self):
        loki = yaml.safe_load((_REPO_ROOT / "loki" / "loki.yml").read_text())
        assert loki["compactor"]["retention_enabled"] is True
        assert loki["compactor"]["delete_request_store"] == "filesystem"
        assert loki["limits_config"]["retention_period"] == "90d"

    def test_promtail_container_discovery_mounts_are_read_only(self):
        compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text())
        mounts = compose["services"]["promtail"]["volumes"]
        assert "/var/run/docker.sock:/var/run/docker.sock:ro" in mounts
        assert "/var/lib/docker/containers:/var/lib/docker/containers:ro" in mounts
        assert "promtail_positions:/positions" in mounts

        promtail = yaml.safe_load(
            (_REPO_ROOT / "promtail" / "promtail.yml").read_text()
        )
        assert promtail["positions"]["filename"] == "/positions/positions.yaml"

    def test_airflow_container_stdout_keeps_only_actionable_lines(self):
        promtail = yaml.safe_load(
            (_REPO_ROOT / "promtail" / "promtail.yml").read_text()
        )
        job = next(
            item for item in promtail["scrape_configs"]
            if item["job_name"] == "docker-operations"
        )
        match = next(
            stage["match"]
            for stage in job["pipeline_stages"]
            if stage.get("match", {}).get("drop_counter_reason")
            == "airflow_non_actionable_control_plane"
        )
        assert match == {
            "selector": (
                '{service=~"airflow-(apiserver|scheduler|dag-processor)"} '
                '!~ "(?i)(warn|error|critical|exception|traceback)"'
            ),
            "action": "drop",
            "drop_counter_reason": "airflow_non_actionable_control_plane",
        }

    def test_successful_oauth_auth_subrequest_noise_is_dropped(self):
        promtail = yaml.safe_load(
            (_REPO_ROOT / "promtail" / "promtail.yml").read_text()
        )
        job = next(
            item for item in promtail["scrape_configs"]
            if item["job_name"] == "docker-operations"
        )
        matches = [stage["match"] for stage in job["pipeline_stages"] if "match" in stage]
        oauth_match = next(
            match for match in matches if match["selector"] == '{service="oauth2-proxy"}'
        )
        assert oauth_match["stages"] == [
            {
                "drop": {
                    "expression": '.*"/oauth2/auth[^"]*" HTTP/1\\.1 "[^"]*" 202 .*',
                    "drop_counter_reason": "oauth2_successful_auth_subrequest",
                }
            }
        ]

    def test_docker_29_compatible_promtail_and_nonempty_stream_labels(self):
        compose = yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text())
        assert compose["services"]["promtail"]["image"] == "grafana/promtail:3.5.8"

        promtail = yaml.safe_load((_REPO_ROOT / "promtail" / "promtail.yml").read_text())
        job = next(
            item for item in promtail["scrape_configs"]
            if item["job_name"] == "docker-operations"
        )
        assert {
            "target_label": "job",
            "replacement": "docker-operations",
        } in job["relabel_configs"]


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


class TestDockerComposeSnapshotWorker:
    """Plan 120 Gate C.5: snapshot-worker must exist, be inert by default, and
    never disturb the production archiver service."""

    @staticmethod
    def _services():
        path = _REPO_ROOT / "docker-compose.yml"
        assert path.exists(), "docker-compose.yml missing"
        doc = yaml.safe_load(path.read_text())
        return doc["services"]

    def test_snapshot_worker_service_exists(self):
        services = self._services()
        assert "snapshot-worker" in services

    def test_snapshot_worker_has_no_ports(self):
        service = self._services()["snapshot-worker"]
        assert "ports" not in service

    def test_snapshot_worker_is_profile_gated(self):
        """Profile-gated services are inert under `docker compose up`; they
        only run when explicitly invoked, e.g. `docker compose run --rm
        snapshot-worker ...`."""
        service = self._services()["snapshot-worker"]
        assert service.get("profiles"), "snapshot-worker must declare profiles"
        assert "snapshot-worker" not in self._default_profile_services()

    def _default_profile_services(self):
        services = self._services()
        return {
            name for name, spec in services.items()
            if not spec.get("profiles")
        }

    def test_snapshot_worker_reuses_archiver_build_context(self):
        service = self._services()["snapshot-worker"]
        archiver = self._services()["archiver"]
        assert service["build"]["dockerfile"] == archiver["build"]["dockerfile"]
        assert service["build"]["context"] == archiver["build"]["context"]

    def test_snapshot_worker_has_no_restart_policy(self):
        """A one-shot `docker compose run` target should not auto-restart."""
        service = self._services()["snapshot-worker"]
        assert "restart" not in service

    def test_snapshot_worker_has_distinct_container_name(self):
        service = self._services()["snapshot-worker"]
        archiver = self._services()["archiver"]
        assert service["container_name"] != archiver["container_name"]

    def test_archiver_service_unaffected(self):
        """Adding snapshot-worker must not change the production archiver
        service's restart/port/profile shape."""
        archiver = self._services()["archiver"]
        assert archiver.get("restart") == "unless-stopped"
        assert "profiles" not in archiver


class TestDockerComposeTrawlMemoryGuardrails:
    """Plan 124: hard resource limits on the browser-solver stack so a
    camoufox-bin memory spike can't OOM the host (2026-07-12 incident)."""

    @staticmethod
    def _services():
        path = _REPO_ROOT / "docker-compose.yml"
        assert path.exists(), "docker-compose.yml missing"
        doc = yaml.safe_load(path.read_text())
        return doc["services"]

    def test_trawl_memory_limits(self):
        service = self._services()["trawl"]
        assert service["mem_limit"] == "4g"
        assert service["memswap_limit"] == "4g"
        assert service["pids_limit"] == 512

    def test_redis_trawl_memory_limits(self):
        service = self._services()["redis-trawl"]
        assert service["mem_limit"] == "512m"
        assert service["memswap_limit"] == "512m"


class TestAnalyticsSnapshotContract:
    """Plan 143 producer, mount, and scrape ownership."""

    @staticmethod
    def _services():
        path = _REPO_ROOT / "docker-compose.yml"
        return yaml.safe_load(path.read_text())["services"]

    def test_snapshot_has_one_writer_and_read_only_ops_consumer(self):
        services = self._services()
        assert "analytics_snapshot:/data/analytics_snapshot" in services["dbt_runner"][
            "volumes"
        ]
        assert "analytics_snapshot:/data/analytics_snapshot:ro" in services["ops"][
            "volumes"
        ]
        assert "analytics_db:/data/analytics:ro" not in services["ops"]["volumes"]

    def test_ops_has_no_analytics_reader_or_duckdb_path(self):
        env = self._services()["ops"]["environment"]
        assert "ANALYTICS_READER_URL" not in env
        assert "DUCKDB_PATH" not in env
        assert env["ANALYTICS_SNAPSHOT_PATH"].endswith("analytics_snapshot.json")

    def test_dbt_runner_is_the_direct_prometheus_target(self):
        path = _REPO_ROOT / "prometheus" / "prometheus.yml"
        jobs = {job["job_name"]: job for job in yaml.safe_load(path.read_text())["scrape_configs"]}
        assert jobs["dbt_runner"]["static_configs"][0]["targets"] == ["dbt_runner:8080"]

    def test_stable_metric_names_match_grafana_consumers(self):
        from dbt_runner.analytics_snapshot import METRIC_NAMES

        dashboard = json.loads(
            (
                _REPO_ROOT / "grafana" / "dashboards" / "pipeline_health.json"
            ).read_text()
        )
        dashboard_expressions = {
            target["expr"]
            for panel in dashboard["panels"]
            for target in panel.get("targets", [])
        }
        assert {
            f'{metric_name}{{job="dbt_runner"}}' for metric_name in METRIC_NAMES
        } <= dashboard_expressions

        rules = yaml.safe_load(
            (
                _REPO_ROOT
                / "grafana"
                / "provisioning"
                / "alerting"
                / "rules.yml"
            ).read_text()
        )
        stable_names = set(METRIC_NAMES) | {
            "cartracker_metrics_last_success_timestamp_seconds"
        }
        alert_expressions = [
            data["model"]["expr"]
            for group in rules["groups"]
            for rule in group["rules"]
            for data in rule["data"]
            if "expr" in data.get("model", {})
            and any(name in data["model"]["expr"] for name in stable_names)
        ]
        assert alert_expressions
        assert all('{job="dbt_runner"}' in expr for expr in alert_expressions)
        assert all("cartracker_cooldown_backlog_high" not in expr for expr in alert_expressions)

    def test_rejected_proxy_and_embedded_sql_are_absent(self):
        runner_source = (_REPO_ROOT / "dbt_runner" / "app.py").read_text()
        runner_python = "\n".join(
            path.read_text() for path in (_REPO_ROOT / "dbt_runner").glob("*.py")
        )
        ops_source = (_REPO_ROOT / "ops" / "app.py").read_text()
        assert '"/analytics/metrics"' not in runner_source
        assert "ANALYTICS_READER_URL" not in ops_source
        assert "analytics_gauges" not in ops_source
        assert not re.search(r"\bSELECT\b.+\bFROM\b", runner_python, flags=re.IGNORECASE)

    def test_raw_s3_and_modeled_analytics_helpers_stay_separate(self):
        modeled = (_REPO_ROOT / "shared" / "analytics_connection.py").read_text()
        raw_s3 = (_REPO_ROOT / "shared" / "duckdb_s3.py").read_text()
        assert "MINIO" not in modeled
        assert "read_only=True" in modeled
        assert "analytics.duckdb" not in raw_s3
        assert "s3_endpoint" in raw_s3


class TestAirflowConnectionBudget:
    """Plan 136 Stage 0a: the apiserver wedged on an exhausted SQLAlchemy pool.

    Raising the pool is two lines; the reason it needs a test is the *budget*.
    ``x-airflow-common-env`` is a YAML anchor shared by every Airflow service,
    so the natural place to put a pool setting multiplies it by four against a
    fixed ``max_connections``. The apiserver-only placement is the whole fix,
    and nothing about the compose file makes that obvious to the next edit.
    """

    # Airflow's stock SQLAlchemy pool, which is what the apiserver was running
    # when it wedged. Every service that does not override these gets them.
    _DEFAULT_POOL_SIZE = 5
    _DEFAULT_MAX_OVERFLOW = 10

    # airflow-init is excluded from the worst case because it cannot overlap
    # with the services below: they each gate on it with
    # `condition: service_completed_successfully`, so it has exited before any
    # of them opens a connection.
    _LONG_RUNNING = {
        "airflow-apiserver", "airflow-scheduler",
        "airflow-dag-processor", "airflow-triggerer",
    }

    @staticmethod
    def _services():
        path = _REPO_ROOT / "docker-compose.yml"
        return yaml.safe_load(path.read_text())["services"]

    @classmethod
    def _max_connections(cls) -> int:
        """Read Postgres's own ceiling rather than hardcoding it, so lowering
        it in the compose file fails this test instead of production."""
        command = cls._services()["postgres"]["command"]
        setting = next(
            part for part in command.split()
            if part.startswith("max_connections=")
        )
        return int(setting.split("=", 1)[1])

    @classmethod
    def _worst_case(cls, service: dict) -> int:
        env = service["environment"]
        return (
            int(env.get("AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE", cls._DEFAULT_POOL_SIZE))
            + int(env.get("AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW", cls._DEFAULT_MAX_OVERFLOW))
        )

    def test_apiserver_pool_is_sized_above_the_stock_default(self):
        env = self._services()["airflow-apiserver"]["environment"]
        assert int(env["AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE"]) > self._DEFAULT_POOL_SIZE
        assert int(env["AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW"]) > self._DEFAULT_MAX_OVERFLOW

    def test_pool_settings_are_not_on_the_shared_anchor(self):
        """The anchor reaches four long-running services at once. A pool set
        there is the version of this fix that trades an apiserver outage for a
        Postgres-side one."""
        services = self._services()
        for name in self._LONG_RUNNING - {"airflow-apiserver"}:
            env = services[name]["environment"]
            assert "AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE" not in env, (
                f"{name} inherited a pool size; it was set on the shared anchor"
            )
            assert "AIRFLOW__DATABASE__SQL_ALCHEMY_MAX_OVERFLOW" not in env

    def test_every_airflow_service_is_accounted_for(self):
        """If a fifth long-running Airflow service is added, the budget below
        silently stops covering it. Fail here instead."""
        services = self._services()
        airflow_services = {
            name for name, service in services.items()
            if service.get("image") == "cartracker-airflow"
        }
        assert airflow_services == self._LONG_RUNNING | {"airflow-init"}

    def test_airflow_worst_case_stays_within_max_connections(self):
        services = self._services()
        total = sum(self._worst_case(services[name]) for name in self._LONG_RUNNING)
        assert total < self._max_connections(), (
            f"Airflow's worst-case pool total is {total} against "
            f"max_connections={self._max_connections()}"
        )


class TestServiceHealthCoverage:
    """Plan 140 Stage 3: coverage is asserted, not enumerated.

    Every monitoring gap in this system's history has the same shape --
    *nobody added X to the list*. ``/mnt/data`` was never added to
    node-exporter. Airflow was never added to ``ct-service-down``. Each was
    fixed by appending to a list, which set up the next one. On 2026-08-18 the
    measurement was 31 services and 7 healthchecks, and Docker reports no
    health status *at all* for a container without one -- so an unwatched
    service and a healthy service looked identical.

    Hence a **deny**-list. Every service is in scope by default, and a new one
    fails this file until someone either gives it a healthcheck or writes down
    why it cannot have one. An allowlist would reproduce the defect exactly:
    a service added later would be silently unwatched, which is the entire
    class of gap this exists to close.

    Airflow's connection budget is the other Plan 140 Stage 3 coverage
    invariant; it already lives in ``TestAirflowConnectionBudget`` above.
    """

    # Exempt services, and why. Every entry needs a written reason, because a
    # deny-list that grows without justification is an allowlist in disguise.
    _DENY_LIST = {
        "flyway":
            "Runs the migrations to completion and exits. Its contract is "
            "`condition: service_completed_successfully`, and a health status "
            "on a container that is supposed to be gone is meaningless.",
        "airflow-init":
            "Same shape: a one-shot DB migrate / user-create that all four "
            "long-running Airflow services gate on with "
            "service_completed_successfully before they open a connection.",
        "dbt":
            "Profile-gated tools image. Never started by `docker compose up`; "
            "invoked as a one-shot `docker compose run`.",
        "dbt_test":
            "Profile-gated tools image, same as `dbt`.",
        "snapshot-worker":
            "Profile-gated one-shot `docker compose run` target for CI lake "
            "snapshot generation (Plan 120 Gate C.5). No ports, no restart "
            "policy, never serves traffic.",
        "oauth2-proxy":
            "The only entry here that is a real hole rather than a contract "
            "that does not apply. quay.io/oauth2-proxy/oauth2-proxy:latest is "
            "distroless -- verified 2026-08-18 that `docker exec "
            "cartracker-oauth2-proxy sh` fails with exec: \"sh\": executable "
            "file not found in $PATH. No shell, no curl, no wget, no busybox, "
            "so no `healthcheck:` can be expressed against this image at all. "
            "A `latest-alpine` tag exists and serves /ping on 4180, but "
            "swapping the image that fronts every authenticated route is its "
            "own change with its own blast radius. See "
            "docs/plan_140_service_health_contract.md.",
    }

    # Profile-gated, but long-running once up -- so the profile flag does not
    # get to decide scope. The 2026-08-14 solver outage is why this is spelled
    # out rather than inferred from `profiles:`.
    _PROFILE_GATED_IN_SCOPE = {"trawl", "redis-trawl"}

    @staticmethod
    def _compose() -> dict:
        return yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text())

    @classmethod
    def _services(cls) -> dict:
        return cls._compose()["services"]

    @classmethod
    def _default_profile_services(cls) -> set:
        return {
            name for name, spec in cls._services().items()
            if not spec.get("profiles")
        }

    @staticmethod
    def _has_enabled_healthcheck(service: dict) -> bool:
        """A present-but-disabled or empty block still produces no status."""
        healthcheck = service.get("healthcheck")
        return (
            isinstance(healthcheck, dict)
            and healthcheck.get("disable") is not True
            and bool(healthcheck.get("test"))
        )

    def test_every_default_profile_service_has_a_healthcheck(self):
        """The rule the whole plan reduces to. A service added to
        docker-compose.yml without health coverage fails here, not in
        production four days after it silently stops working."""
        missing = {
            name for name in self._default_profile_services()
            if not self._has_enabled_healthcheck(self._services()[name])
        } - set(self._DENY_LIST)
        assert not missing, (
            f"{sorted(missing)} have no enabled healthcheck with a test and "
            "no deny-list entry. Add a working `healthcheck:` block, or add "
            "the service to _DENY_LIST with a written reason."
        )

    def test_long_running_profile_gated_services_are_in_scope_too(self):
        """`profiles:` means "not started by default", not "not worth
        watching". trawl was healthy and useless for eight hours."""
        for name in self._PROFILE_GATED_IN_SCOPE:
            service = self._services()[name]
            assert service.get("profiles"), f"{name} is no longer profile-gated"
            assert self._has_enabled_healthcheck(service), (
                f"{name} is profile-gated but long-running when up, so the "
                "profile flag does not exempt it from an enabled healthcheck"
            )

    def test_every_deny_list_entry_carries_a_reason(self):
        for name, reason in self._DENY_LIST.items():
            assert reason and len(reason) > 40, (
                f"{name}'s deny-list reason is missing or too thin to be an "
                "actual justification"
            )

    def test_deny_list_entries_all_name_real_services(self):
        """A renamed or deleted service leaves a stale exemption behind, which
        then silently covers for whatever takes its name next."""
        stale = set(self._DENY_LIST) - set(self._services())
        assert not stale, f"deny-list names services that do not exist: {sorted(stale)}"

    def test_probes_never_reach_across_to_another_container(self):
        """Shallow by construction: process liveness, never dependency health.

        A probe that curls another service turns one Postgres blip into a fan
        of unhealthy containers, and a cascading healthcheck is a worse signal
        than none. Every HTTP probe must therefore target its own loopback.
        """
        for name, service in self._services().items():
            for token in service.get("healthcheck", {}).get("test", []):
                for url in re.findall(r"https?://([^/'\"\s]+)", token):
                    host = url.rsplit(":", 1)[0] if ":" in url else url
                    assert host in ("localhost", "127.0.0.1"), (
                        f"{name}'s healthcheck probes {host}; healthchecks "
                        "must not depend on another container"
                    )

    @classmethod
    def _dockerfile(cls, service: dict):
        build = service.get("build")
        if not isinstance(build, dict):
            return None
        path = _REPO_ROOT / build["context"] / build["dockerfile"]
        return path.read_text() if path.exists() else None

    def test_images_without_an_http_client_do_not_probe_with_one(self):
        """The failure mode Plan 135 recorded and Plan 140 nearly repeated.

        Plan 140 drafted `curl --fail` for every service. A sweep of the
        running production containers on 2026-08-18 found that ops, scraper,
        processing, archiver, pack-worker, dbt_runner and dashboard have
        *neither* curl nor wget -- they are python:*-slim, which ships no HTTP
        client, and no Dockerfile here apt-installs one. Such a probe fails
        because the tool is missing, which manufactures a false unhealthy:
        strictly worse than no healthcheck at all.

        So the rule is mechanical rather than remembered. A python-base image
        that installs no client must probe with `python -c urllib`.
        """
        for name, service in self._services().items():
            dockerfile = self._dockerfile(service)
            if not dockerfile:
                continue
            base = next(
                line.split()[1] for line in dockerfile.splitlines()
                if line.strip().upper().startswith("FROM ")
            )
            installs_client = any(
                tool in line
                for line in dockerfile.splitlines() if "apt-get" in line
                for tool in ("curl", "wget")
            )
            if not base.startswith("python:") or installs_client:
                continue
            test = service.get("healthcheck", {}).get("test", [])
            assert not ({"curl", "wget"} & set(test)), (
                f"{name} builds from {base}, which ships no curl and no wget, "
                f"but its healthcheck invokes one: {test}. Use "
                "`python -c \"import urllib.request; ...\"` instead."
            )


class TestContainerHealthExporterWiring:
    """Plan 140 Stage 2: where the Docker grant lands, and what it sits next to.

    The obvious host for this was `pack-worker` -- it already carries the
    writable node_textfile mount from Plan 135. It is wrong on four counts: it
    is a batch worker for bronze packing and this is unrelated work; it holds
    Postgres and MinIO credentials a health reader has no business near; it
    would report on its own health, so a wedged pack-worker would read as a
    healthy fleet; and every Plan 131/132 change would redeploy the monitoring.
    """

    _SOCKET = "/var/run/docker.sock"

    @classmethod
    def _services(cls) -> dict:
        return yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text())["services"]

    @classmethod
    def _compose(cls) -> dict:
        return yaml.safe_load((_REPO_ROOT / "docker-compose.yml").read_text())

    def test_the_exporter_is_its_own_service(self):
        exporter = self._services()["container-health"]
        assert exporter["build"]["dockerfile"] == "container_health/Dockerfile"
        assert exporter["restart"] == "unless-stopped"

    def test_prometheus_scrapes_the_exporter_directly(self):
        """Plan 143's pattern, not Plan 135's: the service that owns the data
        exposes /metrics and Prometheus scrapes it, so the value is computed
        when Prometheus asks and cannot be stale."""
        doc = yaml.safe_load((_REPO_ROOT / "prometheus" / "prometheus.yml").read_text())
        job = next(j for j in doc["scrape_configs"] if j["job_name"] == "container-health")
        assert job["static_configs"][0]["targets"] == ["container-health:9110"]
        command = " ".join(
            (_REPO_ROOT / "container_health" / "Dockerfile").read_text().split()
        )
        assert "9110" in command, "exporter port drifted from the scrape target"

    def test_the_exporter_holds_no_other_credentials(self):
        """The point of a dedicated service. A health reader has no business
        near the Postgres, MinIO or Telegram secrets pack-worker carries."""
        environment = self._services()["container-health"].get("environment", {})
        assert set(environment) == {"DOCKER_API_URL", "COMPOSE_PROJECT"}

    def test_the_project_scope_is_set_explicitly(self):
        """Compose otherwise derives the project name from the deploy directory,
        so renaming that directory would silently empty this metric. Set here,
        a mismatch instead raises and takes up{job="container-health"} to 0."""
        environment = self._services()["container-health"]["environment"]
        assert environment["COMPOSE_PROJECT"] == "cartracker"

    def test_the_exporter_never_touches_the_socket_itself(self):
        exporter = self._services()["container-health"]
        assert not any(
            self._SOCKET in str(volume) for volume in exporter.get("volumes", [])
        )
        assert exporter["environment"]["DOCKER_API_URL"].startswith(
            "http://docker-socket-proxy:"
        )

    def test_the_proxy_enforces_read_only_by_method_not_by_ro(self):
        """`:ro` makes the socket *file* read-only and does nothing to the API
        behind it: any client that can connect can POST a restart, a kill, or a
        privileged container. POST=0 is the part that actually enforces it, and
        CONTAINERS=1 is the only section granted."""
        proxy = self._services()["docker-socket-proxy"]
        assert str(proxy["environment"]["POST"]) == "0"
        assert str(proxy["environment"]["CONTAINERS"]) == "1"

    def test_the_proxy_is_not_reachable_from_the_shared_network(self):
        """Otherwise all ~26 containers on cartracker-net could enumerate the
        fleet. Only the exporter needs to."""
        compose = self._compose()
        proxy = compose["services"]["docker-socket-proxy"]
        assert proxy["networks"] == ["docker-socket"]
        assert compose["networks"]["docker-socket"]["internal"] is True
        assert set(compose["services"]["container-health"]["networks"]) == {
            "cartracker-net", "docker-socket"
        }

    def test_socket_access_stays_confined_to_the_proxy(self):
        """Plan 136 Stage 4 grants exactly one verb through this proxy when it
        needs restart authority. It must not add a second socket path.

        promtail's mount is pre-existing -- Plan 135 Stage 5 uses it for
        log-discovery metadata -- and is deliberately grandfathered rather than
        treated as a precedent that settles the decision.
        """
        holders = {
            name for name, service in self._services().items()
            if any(self._SOCKET in str(volume) for volume in service.get("volumes", []))
        }
        assert holders == {"docker-socket-proxy", "promtail"}, (
            f"unexpected Docker socket mount: {sorted(holders)}. Read health "
            "through docker-socket-proxy instead of opening a second path."
        )


class TestNodeExporterFilesystemVisibility:
    """Plan 135 Stage 1: without --path.rootfs, node-exporter reads the host
    mount table, sees /dev/sdb at /mnt/data, then statfs()es it inside its own
    namespace where /mnt does not exist. The 196 GB data volume emitted
    node_filesystem_device_error and no capacity series -- so both disk alerts
    silently covered / alone."""

    @staticmethod
    def _node_exporter():
        path = _REPO_ROOT / "docker-compose.yml"
        doc = yaml.safe_load(path.read_text())
        return doc["services"]["node-exporter"]

    def test_rootfs_flag_present(self):
        assert "--path.rootfs=/rootfs" in self._node_exporter()["command"]

    def test_rootfs_bind_mount_backs_the_flag(self):
        """The flag is inert without the recursive bind mount it points at."""
        assert "/:/rootfs:ro" in self._node_exporter()["volumes"]

    def test_ramfs_credentials_mounts_excluded(self):
        """run/credentials/* is ramfs: statfs reports zero blocks and zero
        inodes. It is not tmpfs, so the alert selectors do not filter it, and
        once rootfs resolves it divides by zero into every filesystem panel."""
        exclude = next(
            arg for arg in self._node_exporter()["command"]
            if arg.startswith("--collector.filesystem.mount-points-exclude=")
        )
        assert "run/credentials/.+" in exclude


class TestDiskWatchlistWiring:
    """Plan 135 Stage 4: pack-worker measures, node-exporter publishes.

    The wiring is the fragile part -- every mistake here fails silently. A
    read-only textfile mount on the writer, a missing collector flag, or the
    env var on the wrong service all produce a healthy-looking stack with no
    metrics.
    """

    @staticmethod
    def _services():
        path = _REPO_ROOT / "docker-compose.yml"
        doc = yaml.safe_load(path.read_text())
        return doc["services"]

    @staticmethod
    def _volumes():
        path = _REPO_ROOT / "docker-compose.yml"
        return yaml.safe_load(path.read_text())["volumes"]

    def test_node_exporter_reads_the_textfile_directory(self):
        node_exporter = self._services()["node-exporter"]
        assert "--collector.textfile.directory=/textfile" in node_exporter["command"]
        assert "node_textfile:/textfile:ro" in node_exporter["volumes"]

    def test_pack_worker_can_write_the_textfile_directory(self):
        """Same volume, and this side must NOT be :ro."""
        assert "node_textfile:/textfile" in self._services()["pack-worker"]["volumes"]

    def test_textfile_volume_is_declared(self):
        assert "node_textfile" in self._volumes()

    def test_pack_worker_mounts_every_watched_root_path_read_only(self):
        from archiver.processors.disk_usage import DEFAULT_ROOT_PREFIX, ROOT_PATHS

        volumes = self._services()["pack-worker"]["volumes"]
        for path in ROOT_PATHS:
            assert f"{path}:{DEFAULT_ROOT_PREFIX}{path}:ro" in volumes, (
                f"{path} is on the watchlist but not mounted"
            )

    def test_pack_worker_mounts_the_docker_volume_root_read_only(self):
        from archiver.processors.disk_usage import DEFAULT_VOLUME_PREFIX

        volumes = self._services()["pack-worker"]["volumes"]
        assert f"/mnt/data/docker-volumes:{DEFAULT_VOLUME_PREFIX}:ro" in volumes

    def test_the_job_is_enabled_on_pack_worker_only(self):
        """Its absence on archiver is the 409 -- see _require_disk_usage_host_mounts."""
        services = self._services()
        assert services["pack-worker"]["environment"]["DISK_USAGE_TEXTFILE_DIR"] == "/textfile"
        assert "DISK_USAGE_TEXTFILE_DIR" not in services["archiver"]["environment"]

    def test_host_mounts_are_not_granted_to_the_regular_archiver(self):
        """pack-worker gets the host paths because it needs them; archiver
        holds the same credentials and does not."""
        assert not [
            volume for volume in self._services()["archiver"]["volumes"]
            if volume.startswith("/")
        ]


class TestCaddySnapshotDownloadRoute:
    """Plan 120 Gate F: script-token snapshot downloads must reach ops
    directly instead of being intercepted by the browser OAuth /admin* block."""

    @staticmethod
    def _caddyfile() -> str:
        path = _REPO_ROOT / "Caddyfile"
        assert path.exists(), "Caddyfile missing"
        return path.read_text()

    def test_snapshot_download_route_precedes_generic_admin_auth(self):
        text = self._caddyfile()
        snapshot_route = text.index("handle /admin/snapshots/adaptive-refresh*")
        generic_admin_route = text.index("handle /admin*")
        assert snapshot_route < generic_admin_route

    def test_snapshot_download_route_uses_ops_token_auth_not_oauth_redirect(self):
        text = self._caddyfile()
        start = text.index("handle /admin/snapshots/adaptive-refresh*")
        end = text.index("handle /admin/users*")
        block = text[start:end]
        assert "reverse_proxy ops:8060" in block
        assert "forward_auth" not in block
        assert "oauth2-proxy" not in block


class TestOpsRuntimeRequirements:
    """Config-level checks for dependencies required by ops routes."""

    @staticmethod
    def _requirements() -> set[str]:
        path = _REPO_ROOT / "ops" / "requirements.txt"
        assert path.exists(), "ops/requirements.txt missing"
        return {
            line.strip().split("==", 1)[0].split(">=", 1)[0].split("[", 1)[0].lower()
            for line in path.read_text().splitlines()
            if line.strip() and not line.strip().startswith("#")
        }

    def test_snapshot_downloads_have_boto3_available(self):
        """Gate F uses shared.minio.read_json/object_size/open_stream in ops."""
        assert "boto3" in self._requirements()


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

    def _infrastructure_panels(self):
        doc = json.loads((self._DASHBOARD_DIR / "infrastructure.json").read_text())
        return doc["panels"]

    def test_infrastructure_panel_ids_are_unique(self):
        ids = [p["id"] for p in self._infrastructure_panels()]
        assert len(ids) == len(set(ids)), f"duplicate panel ids: {ids}"

    def test_infrastructure_charts_both_capacity_and_inodes(self):
        """Plan 135 Stage 2. Neither existed before; inode % is the panel that
        would have shown 61% while the byte panel looked fine."""
        exprs = [
            t["expr"]
            for p in self._infrastructure_panels() for t in p.get("targets", [])
        ]
        assert any("node_filesystem_avail_bytes" in e for e in exprs)
        assert any("node_filesystem_files_free" in e for e in exprs)

    def test_infrastructure_filesystem_panels_match_the_alert_selectors(self):
        """A panel that charts different filesystems than the alert evaluates
        is worse than no panel -- it reads as confirmation."""
        selector = '{fstype!="tmpfs",mountpoint!~"/boot.*"}'
        for metric in ("node_filesystem_avail_bytes", "node_filesystem_files_free"):
            target = next(
                t for p in self._infrastructure_panels()
                for t in p.get("targets", []) if metric in t["expr"]
            )
            assert target["expr"].count(selector) == 2
            assert target["legendFormat"] == "{{mountpoint}}"

    def test_mean_object_size_uses_the_authoritative_object_count(self):
        """sum(minio_bucket_objects_size_distribution) carries two overlapping
        bucket schemes and double-counts 1 KB - 1 MB. usage_object_total is the
        exact sum of the mutually-exclusive set. See plan_135."""
        exprs = [
            t["expr"]
            for p in self._infrastructure_panels() for t in p.get("targets", [])
        ]
        assert any("minio_bucket_usage_object_total" in e for e in exprs)
        assert not any("minio_bucket_objects_size_distribution" in e for e in exprs)

    def test_infrastructure_answers_what_is_filling_each_disk(self):
        """Plan 135 Stage 4, success criterion 5: one panel per disk."""
        exprs = [
            t["expr"]
            for p in self._infrastructure_panels() for t in p.get("targets", [])
        ]
        assert any("cartracker_path_bytes" in e for e in exprs), "no root-disk panel"
        assert any("cartracker_volume_bytes" in e for e in exprs), "no /mnt/data panel"

    def test_disk_breakdown_panels_are_stacked(self):
        """The question is always "which band grew", which unstacked lines
        cannot answer and which a stacked total also reconciles against df."""
        for metric in ("cartracker_path_bytes", "cartracker_volume_bytes"):
            panel = next(
                p for p in self._infrastructure_panels()
                if any(metric in t["expr"] for t in p.get("targets", []))
                and p["type"] == "timeseries"
            )
            stacking = panel["fieldConfig"]["defaults"]["custom"]["stacking"]
            assert stacking["mode"] == "normal"

    def test_measurement_age_is_charted(self):
        """The MinIO volume is walked weekly and carried forward in between, so
        a frozen band is only distinguishable from a flat one by its age."""
        exprs = [
            t["expr"]
            for p in self._infrastructure_panels() for t in p.get("targets", [])
        ]
        assert any(
            "cartracker_disk_usage_measured_timestamp_seconds" in e for e in exprs
        )

    def test_watchlist_metric_names_match_the_processor(self):
        """The dashboard and the writer must not drift apart silently."""
        from archiver.processors import disk_usage

        exprs = " ".join(
            t["expr"]
            for p in self._infrastructure_panels() for t in p.get("targets", [])
        )
        for metric in (disk_usage.PATH_METRIC, disk_usage.VOLUME_METRIC,
                       disk_usage.MEASURED_AT_METRIC):
            assert metric in exprs, f"{metric} is written but never charted"

    def _pipeline_panels(self):
        path = _REPO_ROOT / "grafana" / "dashboards" / "pipeline_health.json"
        return json.loads(path.read_text())["panels"]

    def test_solver_outcomes_are_chartable_not_only_alertable(self):
        """Plan 136 Stage 2, and not decoration.

        Open question 2 -- does the solve rate decay gradually or fall off a
        cliff? -- is what chooses Stage 3's recycle interval, and the only way to
        read it is the outcome split over time. Without these panels the
        interval gets picked by guess, which is what the plan says not to do.

        Pinned against the label sets the scraper actually publishes, so
        renaming an outcome fails here rather than silently emptying a panel.
        """
        from scraper.metrics import DETAIL_FETCH_OUTCOMES, SOLVER_OUTCOMES

        exprs = " ".join(
            t["expr"]
            for p in self._pipeline_panels() for t in p.get("targets", [])
        )
        assert "cartracker_solver_requests_total" in exprs
        assert "cartracker_detail_fetch_total" in exprs
        assert SOLVER_OUTCOMES and DETAIL_FETCH_OUTCOMES
        assert exprs.count("sum by (outcome)") == 2, (
            "both counters should be split by outcome; a total alone cannot "
            "distinguish a refusing solver from a lying one"
        )

    def test_pipeline_panel_ids_are_unique(self):
        ids = [p["id"] for p in self._pipeline_panels()]
        assert len(ids) == len(set(ids))

    def test_service_health_is_visible_without_waiting_for_an_alert(self):
        """Plan 140 Stage 2. A metric nothing renders is a metric nobody looks
        at until it pages, and "is the fleet up?" is the first question this
        dashboard should answer -- so the row sits above the storage panels
        rather than below eight of them."""
        panels = self._infrastructure_panels()
        row = next(p for p in panels if p["type"] == "row" and p["title"] == "Service Health")
        assert row["gridPos"]["y"] == 0, "Service Health was pushed below the fold"
        health_panels = [
            p for p in panels
            if any("cartracker_container_health" in t.get("expr", "")
                   for t in p.get("targets", []))
        ]
        assert len(health_panels) == 4, "expected three stat tiles and one timeline"

    def test_all_three_health_states_are_rendered_distinctly(self):
        """The `-1` state is the whole reason this metric has three values. A
        panel that charted only healthy-vs-unhealthy would put the coverage gap
        back where it started: invisible."""
        panels = self._infrastructure_panels()
        timeline = next(p for p in panels if p["type"] == "state-timeline")
        mappings = timeline["fieldConfig"]["defaults"]["mappings"][0]["options"]
        assert {"1", "0", "-1"} == set(mappings)
        assert mappings["1"]["color"] == "green"
        assert mappings["0"]["color"] == "red"
        assert mappings["-1"]["color"] == "orange"
        assert mappings["-1"]["text"] == "no healthcheck"

        tiles = {
            p["title"]: p["targets"][0]["expr"]
            for p in panels if p["type"] == "stat"
        }
        assert "== 0" in tiles["Unhealthy Containers"]
        assert "== -1" in tiles["No Healthcheck Configured"]
        assert "== 1" in tiles["Healthy Containers"]

    def test_health_tiles_read_zero_rather_than_no_data(self):
        """`count()` over an empty match returns no series, which Grafana draws
        as "No data" -- indistinguishable at a glance from a broken exporter.
        `or vector(0)` makes a quiet fleet read as an explicit zero."""
        for panel in self._infrastructure_panels():
            if panel["type"] != "stat":
                continue
            expr = panel["targets"][0]["expr"]
            if "cartracker_container_health" not in expr:
                continue
            assert "or vector(0)" in expr, f"{panel['title']} can render No data"

    def test_minio_logical_panel_is_not_titled_as_disk_usage(self):
        """"MinIO Storage Used" is what made a payload-bytes gauge read as a df
        reading, which is the misreading this whole plan started from."""
        titles = [p["title"] for p in self._infrastructure_panels()]
        assert "MinIO Storage Used (bytes)" not in titles
        assert "MinIO Logical Object Bytes" in titles

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
            "ct-metrics-freshness",
            "ct-stale-listings", "ct-cooldown-backlog", "ct-block-events-spike",
            "ct-pack-verification-refused",
            "ct-disk-space-warning", "ct-disk-space-critical",
            "ct-inode-warning", "ct-inode-critical",
            "ct-inode-exhaustion-forecast",
            "ct-container-unhealthy", "ct-container-health-unconfigured",
            "ct-solver-not-solving", "ct-detail-fetch-failing",
        }
        assert expected <= all_uids, f"Missing rule UIDs: {expected - all_uids}"

    def _rule(self, uid):
        doc = yaml.safe_load((self._ALERTING_DIR / "rules.yml").read_text())
        for group in doc["groups"]:
            for rule in group["rules"]:
                if rule["uid"] == uid:
                    return rule
        raise AssertionError(f"rule {uid} not found")

    def test_metrics_freshness_alert_uses_the_plan_143_contract(self):
        rule = self._rule("ct-metrics-freshness")
        assert rule["noDataState"] == "Alerting"
        assert rule["execErrState"] == "Alerting"
        assert rule["for"] == "0s"
        assert rule["data"][0]["model"]["expr"] == (
            'time() - cartracker_metrics_last_success_timestamp_seconds'
            '{job="dbt_runner"}'
        )
        condition = rule["data"][-1]["model"]["conditions"][0]["evaluator"]
        assert condition == {"type": "gt", "params": [60 * 60 + 15 * 60]}
        hourly_dag = (
            _REPO_ROOT / "airflow" / "dags" / "hourly_analytics_refresh.py"
        ).read_text()
        assert 'schedule="0 * * * *"' in hourly_dag

    def test_pack_verification_refused_watches_the_worker(self):
        """Scheduled pack runs log under service="pack-worker", not "archiver".

        Only pack-worker sets ARCHIVER_ALLOW_PACK_JOBS, so an archiver-scoped
        query would be permanently silent for every scheduled run.
        """
        rule = self._rule("ct-pack-verification-refused")
        expr = rule["data"][0]["model"]["expr"]
        assert "pack-worker" in expr
        assert '|= "REFUSED"' in expr

    def test_pack_verification_refused_alerts_on_any_occurrence(self):
        """"Should be zero; alert on any" -- not a spike threshold."""
        rule = self._rule("ct-pack-verification-refused")
        assert rule["for"] == "0s"
        condition = rule["data"][-1]["model"]["conditions"][0]["evaluator"]
        assert condition["type"] == "gt"
        assert condition["params"] == [0]

    def test_pipeline_failures_cannot_match_a_label_less_series(self):
        """Plan 136 Stage 0c. The summary renders {{ $labels.dag_id }}, so a
        series without that label produced a second instance reading
        "DAG [no value] failed" beside the real one."""
        rule = self._rule("ct-pipeline-failures")
        assert 'dag_id!=""' in rule["data"][0]["model"]["expr"]

    def test_inode_rules_cover_the_same_filesystems_as_the_byte_rules(self):
        """Plan 135 Stage 3. If the two families drift apart, one disk gets
        byte alerting and the other gets inode alerting and nobody notices."""
        selector = '{fstype!="tmpfs",mountpoint!~"/boot.*"}'
        for uid in ("ct-inode-warning", "ct-inode-critical",
                    "ct-disk-space-warning", "ct-disk-space-critical"):
            expr = self._rule(uid)["data"][0]["model"]["expr"]
            assert expr.count(selector) == 2, f"{uid} selector drifted"

    def test_inode_rules_measure_inodes_not_bytes(self):
        """The whole point is that inodes bind before bytes; a copy-paste from
        the byte rules would look correct and alert on the wrong thing."""
        for uid in ("ct-inode-warning", "ct-inode-critical"):
            expr = self._rule(uid)["data"][0]["model"]["expr"]
            assert "node_filesystem_files_free" in expr
            assert "node_filesystem_avail_bytes" not in expr

    def test_inode_thresholds_and_durations_mirror_the_byte_rules(self):
        for inode_uid, disk_uid, threshold in (
            ("ct-inode-warning", "ct-disk-space-warning", 80),
            ("ct-inode-critical", "ct-disk-space-critical", 90),
        ):
            inode_rule, disk_rule = self._rule(inode_uid), self._rule(disk_uid)
            assert inode_rule["for"] == disk_rule["for"]
            evaluator = inode_rule["data"][-1]["model"]["conditions"][0]["evaluator"]
            assert evaluator["type"] == "gt"
            assert evaluator["params"] == [threshold]

    def test_inode_forecast_window_covers_its_own_range(self):
        """predict_linear over [6h] returns no data if Grafana only hands the
        query a 10-minute window -- the rule would be silent, not wrong."""
        rule = self._rule("ct-inode-exhaustion-forecast")
        query = rule["data"][0]
        assert "predict_linear" in query["model"]["expr"]
        assert "[6h]" in query["model"]["expr"]
        assert query["relativeTimeRange"]["from"] >= 6 * 3600

    def test_inode_forecast_watches_the_data_volume_and_fires_below_zero(self):
        rule = self._rule("ct-inode-exhaustion-forecast")
        assert 'mountpoint="/mnt/data"' in rule["data"][0]["model"]["expr"]
        evaluator = rule["data"][-1]["model"]["conditions"][0]["evaluator"]
        assert evaluator["type"] == "lt"
        assert evaluator["params"] == [0]

    # ── Plan 140 Stage 2 ──────────────────────────────────────────────────────

    def test_service_down_covers_every_scrape_job(self):
        """The test that stops this row re-rotting.

        ct-service-down selected up{job=~"ops|processing"} -- two of eight
        scrape jobs -- so airflow, postgres, minio, minio_bucket, dbt_runner and
        node were unwatched at the target level. That is the same
        never-added-to-the-list defect Plan 140 exists to close, sitting in the
        alert file. Exact set equality, in the style this module already uses
        for the Promtail job set, because the next added scrape target must fail
        here rather than be silently unwatched.

        It is load-bearing for a second reason now: container-health computes at
        scrape time and has no freshness metric, so `up` is its entire liveness
        contract.
        """
        prometheus = yaml.safe_load(
            (_REPO_ROOT / "prometheus" / "prometheus.yml").read_text()
        )
        scrape_jobs = {job["job_name"] for job in prometheus["scrape_configs"]}
        expr = self._rule("ct-service-down")["data"][0]["model"]["expr"]
        selected = set(re.search(r'up\{job=~"([^"]+)"\}', expr).group(1).split("|"))
        assert selected == scrape_jobs, (
            "ct-service-down's job set drifted from prometheus.yml: "
            f"{sorted(selected ^ scrape_jobs)}"
        )

    def test_container_health_rules_cannot_be_silently_merged(self):
        """Two states, two rules. A single `< 1` threshold would look correct
        and would fold "no healthcheck configured" back into the incident
        channel -- undoing the entire reason -1 exists as a third state."""
        unhealthy = self._rule("ct-container-unhealthy")["data"][0]["model"]["expr"]
        unconfigured = self._rule(
            "ct-container-health-unconfigured"
        )["data"][0]["model"]["expr"]
        assert "cartracker_container_health" in unhealthy
        assert "cartracker_container_health" in unconfigured
        assert "== bool 0" in unhealthy and "== bool -1" not in unhealthy
        assert "== bool -1" in unconfigured and "== bool 0" not in unconfigured

    def test_container_health_rules_cannot_drop_a_recovered_series(self):
        """The false page this stage shipped on 2026-08-20 and corrected.

        The rules first used `count by (container) (metric == 0)`. A *filtering*
        comparison drops the series entirely when a container is healthy, and
        Grafana's `reduce: last` over a 600s relativeTimeRange then keeps the
        last value it saw alive for the rest of that window. Grafana's own
        restart put container="grafana" at 0 for a single 15-second sample at
        18:38:15; the ghost series satisfied the 5m `for` and the rule was
        Alerting at 18:44 for a container healthy since 18:38:30. Left alone,
        that pages on every deploy, for every container restarted.

        `== bool` returns 1-or-0 for every series and drops none, so a recovered
        container reads 0 on the next evaluation. The `reduce: last` +
        `relativeTimeRange` shape is inherited from every other rule in the
        file, so the guard belongs on the expression.
        """
        for uid in ("ct-container-unhealthy", "ct-container-health-unconfigured"):
            query = self._rule(uid)["data"][0]
            expr = query["model"]["expr"]
            assert "bool" in expr, (
                f"{uid} uses a filtering comparison, so a healthy container's "
                "series vanishes and its last value haunts the "
                f"{query['relativeTimeRange']['from']}s reduce window"
            )
            assert not expr.startswith("count"), (
                f"{uid} aggregates away the per-container series; use the "
                "bool comparison so every container reports every evaluation"
            )

    def test_container_health_rules_read_the_exporters_own_job(self):
        for uid in ("ct-container-unhealthy", "ct-container-health-unconfigured"):
            expr = self._rule(uid)["data"][0]["model"]["expr"]
            assert 'job="container-health"' in expr

    def test_unhealthy_alert_outlasts_the_slowest_healthcheck_start(self):
        """`starting` is published as 0, so the `for` duration is the only
        thing standing between a slow-starting container and a false page.

        Docker leaves `starting` within start_period + retries * (interval +
        timeout). Worst case across this compose file is 230s against a 300s
        `for`. Widening a start_period past that would page on every deploy,
        and this is where that gets caught.
        """
        def seconds(value, default):
            if value is None:
                return default
            text = str(value)
            units = {"s": 1, "m": 60, "h": 3600}
            if text[-1] in units:
                return int(text[:-1]) * units[text[-1]]
            return int(text)

        services = yaml.safe_load(
            (_REPO_ROOT / "docker-compose.yml").read_text()
        )["services"]
        worst = {}
        for name, service in services.items():
            check = service.get("healthcheck")
            if not isinstance(check, dict) or not check.get("test"):
                continue
            worst[name] = (
                seconds(check.get("start_period"), 0)
                + int(check.get("retries", 3))
                * (seconds(check.get("interval"), 30) + seconds(check.get("timeout"), 30))
            )

        alert_for = seconds(self._rule("ct-container-unhealthy")["for"], 0)
        slowest = max(worst, key=worst.get)
        assert worst[slowest] < alert_for, (
            f"{slowest} can sit in Docker's `starting` state for "
            f"{worst[slowest]}s, past ct-container-unhealthy's {alert_for}s "
            "`for`, so a normal restart would page"
        )

    def test_container_health_has_no_freshness_metric_or_staleness_rule(self):
        """The amended design in one assertion.

        Plan 135's textfile collector needed a companion timestamp metric and a
        staleness alert because a 456-second disk walk cannot happen inside a
        scrape handler. This exporter computes when Prometheus asks, so a stale
        value is structurally impossible. A freshness rule appearing here would
        mean the design drifted back to a cached .prom file, which is what
        Plan 143 spent a 24-hour soak correcting and Plan 136 D2 named first.
        """
        doc = yaml.safe_load((self._ALERTING_DIR / "rules.yml").read_text())
        for group in doc["groups"]:
            for rule in group["rules"]:
                for query in rule["data"]:
                    expr = query["model"].get("expr", "")
                    assert not ("container_health" in expr and "timestamp" in expr), (
                        f"{rule['uid']} pairs container health with a freshness "
                        "timestamp; the exporter has no cached value to go stale"
                    )

    def test_unconfigured_coverage_alert_is_routed_off_the_incident_cadence(self):
        """"Not an incident; a coverage alert, routed accordingly."

        A missing healthcheck needs a compose edit, not a 3 a.m. response.
        Repeating it on the 4h incident cadence is how an operator learns to
        ignore the channel -- which is the channel the 2026-08-14 solver outage
        needed. Same receiver, daily repeat.
        """
        rule = self._rule("ct-container-health-unconfigured")
        assert rule.get("labels", {}).get("severity") == "coverage"
        assert "severity" not in self._rule("ct-container-unhealthy").get("labels", {})

        policies = yaml.safe_load(
            (self._ALERTING_DIR / "notification_policies.yml").read_text()
        )["policies"]
        routes = policies[0]["routes"]
        coverage = [
            route for route in routes
            if ["severity", "=", "coverage"] in route["object_matchers"]
        ]
        assert len(coverage) == 1, "no route matches severity=coverage"
        assert coverage[0]["repeat_interval"] != policies[0]["repeat_interval"]

    # ── Plan 136 Stage 2 ──────────────────────────────────────────────────────

    _SOLVER_RULES = ("ct-solver-not-solving", "ct-detail-fetch-failing")

    def test_solver_alerts_cannot_drop_a_recovered_series(self):
        """Plan 140's `== bool` lesson, applied before it can bite again.

        Plan 136 specified this alert as
        `rate(...{outcome="ok"}[15m]) == 0 and rate(...[15m]) > 0`. Both `== 0`
        and `and` are *filtering* operators: the moment the solver recovers, the
        expression returns no series at all, and `reduce: last` over the 600s
        relativeTimeRange keeps the last bad value alive for the rest of that
        window. That is exactly the shape that false-paged ct-container-unhealthy
        on 2026-08-20, where flaresolverr stayed firing for eleven minutes after
        recovering.

        Written as a product of `bool` comparisons, both expressions yield
        exactly one series that reads 0 on the first evaluation after recovery.
        """
        for uid in self._SOLVER_RULES:
            expr = self._rule(uid)["data"][0]["model"]["expr"]
            assert " and " not in expr, (
                f"{uid} uses `and`, which drops the series when the guard stops "
                "matching; the last bad value then haunts the reduce window"
            )
            assert re.search(r"==\s*bool\s+0", expr), (
                f"{uid} must test the ok count with `== bool 0`, not a filtering "
                "`== 0`"
            )
            assert expr.count("bool") == 2, (
                f"{uid} should be a product of two bool comparisons, so both the "
                "failure test and the traffic guard keep their series"
            )

    def test_solver_alerts_stay_quiet_when_nothing_is_being_scraped(self):
        """The guard that keeps an idle scraper off the incident channel.

        `scrape_detail_pages` is `*/15`, so there are long stretches with no
        traffic at all, and "nothing succeeded" is trivially true then. Each rule
        multiplies by a volume guard, and the thresholds are the operating
        thresholds -- not >0, which one transient error in a quiet window would
        satisfy.
        """
        thresholds = {
            "ct-solver-not-solving": 5,
            "ct-detail-fetch-failing": 20,
        }
        for uid, minimum in thresholds.items():
            expr = self._rule(uid)["data"][0]["model"]["expr"]
            assert re.search(rf">\s*bool\s+{minimum}\b", expr), (
                f"{uid} lost its volume guard of >{minimum}; without it an idle "
                "scraper reads as a failing one"
            )

    def test_solver_alerts_read_the_scrapers_own_job(self):
        for uid in self._SOLVER_RULES:
            expr = self._rule(uid)["data"][0]["model"]["expr"]
            assert 'job="scraper"' in expr

    def test_the_detail_window_spans_a_whole_scrape_cycle(self):
        """Detail traffic is bursty -- ~100 fetches in about two minutes, then
        idle until the next `*/15` run. A rate window shorter than the schedule
        would keep falling into zero-traffic stretches where the volume guard
        reads 0 and resets the `for`, making the rule quieter rather than
        faster."""
        dag = (_REPO_ROOT / "airflow" / "dags" / "scrape_detail_pages.py").read_text()
        minutes = int(re.search(r'schedule="\*/(\d+) \* \* \* \*"', dag).group(1))
        expr = self._rule("ct-detail-fetch-failing")["data"][0]["model"]["expr"]
        windows = {int(w) for w in re.findall(r"\[(\d+)m\]", expr)}
        assert windows, "no rate window found"
        assert min(windows) > minutes, (
            f"ct-detail-fetch-failing's window {min(windows)}m does not span the "
            f"{minutes}m detail schedule, so it can evaluate against no traffic"
        )

    def test_the_two_solver_rules_watch_different_metrics(self):
        """They are not a fast and a slow copy of one signal.

        A refusing solver raises before caching, so it is re-asked on every fetch
        and solver volume spikes -- ct-solver-not-solving sees that in minutes.
        A *lying* solver caches normally for the 25-minute TTL and its request
        volume never moves; only the 403s on detail fetches show it. Collapsing
        these into one rule loses one of the two modes.
        """
        exprs = {
            uid: self._rule(uid)["data"][0]["model"]["expr"]
            for uid in self._SOLVER_RULES
        }
        assert "cartracker_solver_requests_total" in exprs["ct-solver-not-solving"]
        assert "cartracker_detail_fetch_total" in exprs["ct-detail-fetch-failing"]
        assert "cartracker_detail_fetch_total" not in exprs["ct-solver-not-solving"]

    def test_the_solver_rule_counts_challenges_as_failures(self):
        """A solver returning interstitials behind `status: ok` is failing. If
        this selected only `outcome="error"`, the exact 2026-08-14 shape --
        cookies returned, challenge page behind them -- would not reach the
        guard."""
        expr = self._rule("ct-solver-not-solving")["data"][0]["model"]["expr"]
        assert 'outcome!="ok"' in expr, (
            "the failure side must be everything that is not ok, so `challenge` "
            "counts toward it"
        )

    def test_scrape_volume_drop_is_labelled_as_data_quality_not_liveness(self):
        """D1's durable half. This rule reads a dbt mart and lags the entire
        pipeline; it stayed silent through the 8-hour outage and then fired 40
        minutes into the healthy recovery. It keeps its place as a data-quality
        signal, but an operator reading it as liveness is the mistake that cost
        eight hours, so the annotation has to say so.
        """
        rule = self._rule("ct-scrape-volume-drop")
        description = rule["annotations"]["description"]
        assert "not a liveness" in description
        for uid in self._SOLVER_RULES:
            assert uid in description, (
                "the description should point at the rules that do answer "
                "liveness"
            )

    def test_the_volume_snapshot_reads_a_complete_hour(self):
        """The other half of D1, and the reason that rule fired at 05:51.

        mart_scrape_volume buckets on date_trunc('hour', fetched_at) and the
        build runs at `0 * * * *`, so an unfiltered MAX(hour) is the hour
        currently in progress -- a few minutes of data published as an hourly
        total, under a threshold of 100. The gauge's own description already
        said "most recent complete scrape hour"; this makes that true.
        """
        sql = (
            _REPO_ROOT / "dbt_runner" / "sql" / "analytics_metrics_snapshot.sql"
        ).read_text()
        assert re.search(
            r"hour\s*<\s*CAST\(date_trunc\('hour',\s*now\(\)\s*AT TIME ZONE 'UTC'\)",
            sql,
        ), "the snapshot no longer excludes the in-progress hour"
