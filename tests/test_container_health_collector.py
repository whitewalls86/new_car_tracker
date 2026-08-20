"""Plan 140 Stage 2: the container-health metric renderer.

The scoping rule is the assumption most likely to be loosened later by someone
simplifying the collector, so it is pinned here rather than only observed in
production.
"""
import pytest

from container_health.collector import (
    METRIC_NAME,
    ContainerHealthCollector,
    NoContainersFound,
    health_value,
    health_values,
)

PROJECT = "cartracker"


def container(service, *, project=PROJECT, status="running", health="healthy", oneoff=None):
    """One inspect payload, shaped like the Docker API's actual response.

    `health=None` reproduces the case that motivates the whole three-state
    design: Docker omits State.Health entirely for a container with no
    healthcheck -- verified against cartracker-oauth2-proxy on 2026-08-20,
    whose State keys stop at Status/Running/Paused/Restarting/Dead.
    """
    labels = {
        "com.docker.compose.project": project,
        "com.docker.compose.service": service,
    }
    if oneoff is not None:
        labels["com.docker.compose.oneoff"] = oneoff
    state = {"Status": status}
    if health is not None:
        state["Health"] = {"Status": health, "FailingStreak": 0}
    return {"Config": {"Labels": labels}, "State": state}


class TestHealthValue:
    def test_healthy_is_one(self):
        assert health_value(container("ops")) == 1

    def test_unhealthy_is_zero(self):
        assert health_value(container("trawl", health="unhealthy")) == 0

    def test_no_healthcheck_is_minus_one_not_absent(self):
        """The state the plan exists for. Collapsing this into "no series" is
        how a monitoring gap disguises itself as a healthy system."""
        assert health_value(container("oauth2-proxy", health=None)) == -1

    def test_starting_is_not_healthy(self):
        """"Not yet known to be healthy" is not healthy. It is bounded by
        start_period + retries * (interval + timeout), and the alert's 5m `for`
        is what keeps a slow start from paging -- not a fourth state here."""
        assert health_value(container("loki", health="starting")) == 0

    @pytest.mark.parametrize("status", ["restarting", "paused"])
    def test_not_running_is_unhealthy(self, status):
        """A crash loop must read as unhealthy rather than vanish from the
        metric. A restarting container's stale Health can still say healthy."""
        assert health_value(container("scraper", status=status)) == 0


class TestScoping:
    def test_all_three_states_render_together(self):
        values = health_values(
            [
                container("ops"),
                container("trawl", health="unhealthy"),
                container("oauth2-proxy", health=None),
            ],
            PROJECT,
        )
        assert values == {"oauth2-proxy": -1, "ops": 1, "trawl": 0}

    def test_a_sibling_project_is_invisible_not_broken(self):
        """The 2026-08-20 soak finding, and the reason "compose-managed" is not
        a sufficient filter.

        cartracker-lakekeeper, -lakekeeper-postgres and cartracker-mlflow were
        all compose-managed -- by the separate cartracker-lakehouse and
        cartracker-mlflow projects supporting Plans 125 and 112. They were
        stopped at a host restart, carry no restart policy, and Docker still
        held their last `unhealthy` state. A collector that scoped to
        "compose-managed" would publish permanent 0s and page forever for
        services nobody intends to be running.

        They were removed on 2026-08-20, but `up -d` recreates the condition,
        so this pins the rule rather than the absence of those four names.
        """
        values = health_values(
            [
                container("ops"),
                container("lakekeeper", project="cartracker-lakehouse", health="unhealthy"),
                container("mlflow", project="cartracker-mlflow", health="unhealthy"),
            ],
            PROJECT,
        )
        assert values == {"ops": 1}

    def test_the_project_label_is_required_not_merely_checked(self):
        """A container with no project label at all is not "close enough"."""
        stray = {"Config": {"Labels": {}}, "State": {"Status": "running"}}
        with pytest.raises(NoContainersFound):
            health_values([stray], PROJECT)

    def test_oneoff_run_containers_do_not_count_as_services(self):
        """`docker compose run --rm dbt` carries the project label. Without
        this it would surface as an unconfigured (-1) service, and page the
        coverage alert, for as long as the invocation ran."""
        values = health_values(
            [container("ops"), container("dbt", health=None, oneoff="True")],
            PROJECT,
        )
        assert values == {"ops": 1}

    def test_an_empty_fleet_refuses_rather_than_publishes_nothing(self):
        """This exporter is itself a member of the fleet, so zero containers
        means the project label stopped matching -- a renamed deploy directory,
        say. Publishing nothing would read as a healthy system; raising makes
        up{job="container-health"} go to 0 instead."""
        with pytest.raises(NoContainersFound):
            health_values([], PROJECT)


class TestCollector:
    class _Api:
        def __init__(self, inspections):
            self.inspections = inspections
            self.calls = []

        def inspect_project_containers(self, project):
            self.calls.append(project)
            return self.inspections

    def _samples(self, inspections):
        api = self._Api(inspections)
        families = list(ContainerHealthCollector(api, PROJECT).collect())
        assert len(families) == 1
        assert families[0].name == METRIC_NAME
        return {s.labels["container"]: s.value for s in families[0].samples}, api

    def test_collect_emits_one_labelled_sample_per_service(self):
        samples, api = self._samples([
            container("ops"),
            container("caddy", health=None),
            container("lakekeeper", project="cartracker-lakehouse", health="unhealthy"),
        ])
        assert samples == {"caddy": -1.0, "ops": 1.0}
        assert api.calls == [PROJECT]

    def test_collect_reads_docker_on_every_scrape(self):
        """No cached value, so no staleness. If this ever stops being true, the
        design has drifted back toward the .prom file it was amended away from
        and needs a freshness metric and a staleness alert to be safe."""
        api = self._Api([container("ops")])
        collector = ContainerHealthCollector(api, PROJECT)
        for _ in range(3):
            list(collector.collect())
        assert api.calls == [PROJECT] * 3

    def test_a_docker_failure_propagates_instead_of_serving_a_healthy_fleet(self):
        """/metrics must 500 so `up` goes to 0 and ct-service-down fires."""
        class _Broken:
            def inspect_project_containers(self, project):
                raise OSError("connection refused")

        with pytest.raises(OSError):
            list(ContainerHealthCollector(_Broken(), PROJECT).collect())
