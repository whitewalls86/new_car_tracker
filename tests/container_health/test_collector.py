"""Plan 140 Stage 2: the container-health metric renderer.

The scoping rule is the assumption most likely to be loosened later by someone
simplifying the collector, so it is pinned here rather than only observed in
production.
"""
import pytest

from container_health.collector import (
    MEMORY_LIMIT_METRIC_NAME,
    MEMORY_METRIC_NAME,
    METRIC_NAME,
    ContainerHealthCollector,
    NoContainersFound,
    health_value,
    health_values,
    memory_capped,
    memory_usage,
    oneoff_processes,
)

PROJECT = "cartracker"


def container(
    service,
    *,
    project=PROJECT,
    status="running",
    health="healthy",
    oneoff=None,
    mem_limit=0,
    started_at=None,
):
    """One inspect payload, shaped like the Docker API's actual response.

    `health=None` reproduces the case that motivates the whole three-state
    design: Docker omits State.Health entirely for a container with no
    healthcheck -- verified against cartracker-oauth2-proxy on 2026-08-20,
    whose State keys stop at Status/Running/Paused/Restarting/Dead.

    `mem_limit=0` is Docker's own spelling of "uncapped", not a placeholder --
    `HostConfig.Memory` is 0 for every container without a `mem_limit`, which
    is why Plan 136 Stage 3a can read membership straight off the payload.
    """
    labels = {
        "com.docker.compose.project": project,
        "com.docker.compose.service": service,
    }
    if oneoff is not None:
        labels["com.docker.compose.oneoff"] = oneoff
    state = {"Status": status}
    if started_at is not None:
        state["StartedAt"] = started_at
    if health is not None:
        state["Health"] = {"Status": health, "FailingStreak": 0}
    return {
        "Id": f"id-{service}",
        "Config": {"Labels": labels},
        "State": state,
        "HostConfig": {"Memory": mem_limit},
    }


GIB = 1024 ** 3


def stats(usage, *, inactive=0, key="inactive_file", limit=4 * GIB):
    """A one-shot stats payload, trimmed to the section this exporter reads."""
    return {
        "memory_stats": {
            "usage": usage,
            "limit": limit,
            "stats": {key: inactive},
        }
    }


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

    def test_live_oneoffs_are_separate_drain_evidence(self):
        processes = oneoff_processes(
            [
                container("ops"),
                container(
                    "snapshot-worker",
                    oneoff="True",
                    started_at="2026-08-25T04:00:00Z",
                ),
                container("dbt", project="other", oneoff="True"),
            ],
            PROJECT,
        )

        assert processes == [
            {
                "service": "snapshot-worker",
                "container_id": "id-snapshot-worker",
                "started_at": "2026-08-25T04:00:00Z",
            }
        ]

    def test_stopped_oneoff_is_not_active_work(self):
        assert oneoff_processes(
            [container("dbt", status="exited", oneoff="True")], PROJECT
        ) == []

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


class TestAbsentExpectedServices:
    """Plan 140 Stage 4a: a stopped service reads 0 instead of disappearing.

    `inspect_project_containers` filters to running/restarting/paused, so
    before this the only trace of a stopped service was a series that stopped
    existing -- and an absent series reads as a healthy system. Plan 142 Stage 0
    item 2 found the live instance while `caddy` still had no restart policy:
    the public site would not come back after a reboot, "and nothing reports
    it".

    Stage 4 is what made it load-bearing. Demoting `http_health_sensor` to a
    gate removed the DAG-failure page that was the only notification a stopped
    `archiver` or `pack-worker` produced -- neither is a Prometheus scrape job,
    so `ct-service-down` does not cover them.
    """

    def test_an_expected_service_that_is_gone_reads_zero(self):
        values = health_values([container("ops")], PROJECT, expected={"ops", "archiver"})
        assert values == {"archiver": 0, "ops": 1}

    def test_a_running_service_keeps_its_real_state(self):
        """The backfill fills gaps; it never overwrites an observation. An
        expected service sitting at -1 must keep reading -1, or `oauth2-proxy`
        would silently move from the coverage alert to the incident one."""
        values = health_values(
            [container("oauth2-proxy", health=None)],
            PROJECT,
            expected={"oauth2-proxy"},
        )
        assert values == {"oauth2-proxy": -1}

    def test_an_unexpected_running_service_is_still_published(self):
        """Drift is a CI concern, not a runtime one. Dropping a service that is
        running but unlisted would hide one someone started by hand."""
        values = health_values([container("ops"), container("stray")], PROJECT,
                               expected={"ops"})
        assert values == {"ops": 1, "stray": 1}

    def test_an_empty_fleet_still_refuses_instead_of_zeroing_everything(self):
        """Order matters, and this is the test that pins it.

        If the project label stops matching, backfilling first would publish a
        0 for every expected service and page for the entire fleet at once,
        burying the one fact that matters: the exporter cannot see Docker. The
        guard runs first, so that stays a single up{job="container-health"}
        failure.
        """
        with pytest.raises(NoContainersFound):
            health_values([], PROJECT, expected={"ops", "archiver", "scraper"})

    def test_no_expected_set_is_the_stage_2_behaviour(self):
        """The parameter defaults to empty, so the collector degrades to what
        Stage 2 shipped rather than to a fleet of zeroes."""
        assert health_values([container("ops")], PROJECT) == {"ops": 1}


class TestMemoryScoping:
    """Plan 136 Stage 3a. Membership is derived, never listed."""

    def test_only_capped_containers_are_measured(self):
        capped = memory_capped(
            [
                container("trawl", mem_limit=4 * GIB),
                container("scraper"),
                container("ops"),
            ],
            PROJECT,
        )
        assert capped == {"trawl": ("id-trawl", 4 * GIB)}

    def test_a_service_capped_later_needs_no_change_here(self):
        """The failure mode open question 5 describes, avoided by construction:
        a hardcoded service list would leave this container unmeasured until
        someone remembered to edit it."""
        assert "redis-trawl" in memory_capped(
            [container("redis-trawl", mem_limit=512 * 1024 ** 2)], PROJECT
        )

    def test_the_limit_comes_from_the_container_not_the_compose_file(self):
        """A `mem_limit` edited in compose but never applied -- the service was
        not recreated -- must read as the limit actually in force, or the
        headroom fraction Stage 3d gates on is computed against fiction."""
        capped = memory_capped([container("trawl", mem_limit=2 * GIB)], PROJECT)
        assert capped["trawl"][1] == 2 * GIB

    def test_a_sibling_project_is_not_measured(self):
        assert memory_capped(
            [container("mlflow", project="cartracker-mlflow", mem_limit=GIB)],
            PROJECT,
        ) == {}

    def test_oneoff_runs_are_not_measured(self):
        assert memory_capped(
            [container("dbt", oneoff="True", mem_limit=12 * GIB)], PROJECT
        ) == {}

    def test_an_uncapped_fleet_is_empty_not_an_error(self):
        """Unlike health, no memory series is a legitimate state -- it means
        nothing declares a cap. Raising here would 500 /metrics and take the
        health metric down with it."""
        assert memory_capped([container("ops"), container("caddy")], PROJECT) == {}


class TestMemoryUsage:
    def test_reclaimable_page_cache_is_excluded(self):
        """Verified against production 2026-08-23: usage 1583955968 minus
        inactive_file 171794432 is 1346.7 MiB, which is what `docker stats`
        printed for cartracker-trawl in the same second."""
        assert memory_usage(stats(1583955968, inactive=171794432)) == 1412161536

    def test_cgroup_v1_spelling_is_accepted(self):
        """The host runs v2. v1 is handled so the metric cannot quietly change
        meaning by a factor of the page cache if that ever moves."""
        assert memory_usage(
            stats(1000, inactive=400, key="total_inactive_file")
        ) == 600

    def test_a_payload_without_usage_is_absent_not_zero(self):
        """A restarting container returns an empty memory_stats. Publishing 0
        would read as an empty container with full headroom -- the most
        dangerous possible lie for a gauge Stage 3d gates a restart on."""
        assert memory_usage({"memory_stats": {}}) is None
        assert memory_usage({}) is None

    def test_usage_never_goes_negative(self):
        assert memory_usage(stats(100, inactive=500)) == 0


class TestCollector:
    class _Api:
        def __init__(self, inspections, stats_by_id=None, stats_error=None):
            self.inspections = inspections
            self.calls = []
            self.stats_calls = []
            self.stats_by_id = stats_by_id or {}
            self.stats_error = stats_error

        def inspect_project_containers(self, project):
            self.calls.append(project)
            return self.inspections

        def container_stats(self, container_id):
            self.stats_calls.append(container_id)
            if self.stats_error is not None:
                raise self.stats_error
            return self.stats_by_id[container_id]

    def _samples(self, inspections):
        """The health family specifically. Stage 3a added two memory families
        alongside it, so this selects by name rather than assuming a lone one."""
        api = self._Api(inspections)
        families = {f.name: f for f in ContainerHealthCollector(api, PROJECT).collect()}
        assert METRIC_NAME in families
        return {
            s.labels["container"]: s.value for s in families[METRIC_NAME].samples
        }, api

    def test_collect_emits_one_labelled_sample_per_service(self):
        samples, api = self._samples([
            container("ops"),
            container("caddy", health=None),
            container("lakekeeper", project="cartracker-lakehouse", health="unhealthy"),
        ])
        assert samples == {"caddy": -1.0, "ops": 1.0}
        assert api.calls == [PROJECT]

    def test_the_expected_set_reaches_the_rendered_family(self):
        """Plan 140 Stage 4a, end to end: an expected service that Docker no
        longer reports still produces a sample, at 0."""
        api = self._Api([container("ops")])
        collector = ContainerHealthCollector(api, PROJECT, {"ops", "archiver"})
        families = {f.name: f for f in collector.collect()}
        samples = {
            s.labels["container"]: s.value for s in families[METRIC_NAME].samples
        }
        assert samples == {"archiver": 0.0, "ops": 1.0}

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


class TestMemoryCollector:
    """Plan 136 Stage 3a: the series D7 asked for and nothing published."""

    def _families(self, api):
        return {f.name: f for f in ContainerHealthCollector(api, PROJECT).collect()}

    def test_usage_and_limit_are_published_per_capped_container(self):
        api = TestCollector._Api(
            [container("trawl", mem_limit=4 * GIB), container("ops")],
            stats_by_id={"id-trawl": stats(2 * GIB, inactive=GIB)},
        )
        families = self._families(api)
        used = {s.labels["container"]: s.value for s in families[MEMORY_METRIC_NAME].samples}
        limit = {
            s.labels["container"]: s.value
            for s in families[MEMORY_LIMIT_METRIC_NAME].samples
        }
        assert used == {"trawl": float(GIB)}
        assert limit == {"trawl": float(4 * GIB)}

    def test_only_capped_containers_cost_a_stats_call(self):
        """One extra round trip per capped container, three in production. An
        exporter that stat-ed all 26 on a 15s interval would be a different
        proposition entirely."""
        api = TestCollector._Api(
            [container("trawl", mem_limit=4 * GIB)] + [container(f"svc{i}") for i in range(9)],
            stats_by_id={"id-trawl": stats(GIB)},
        )
        self._families(api)
        assert api.stats_calls == ["id-trawl"]

    def test_health_still_publishes_when_a_stats_read_fails(self):
        """The asymmetry that matters. A memory read failing must not blind the
        health signal -- that would trade the metric this exporter exists for
        against the one Plan 136 added later."""
        api = TestCollector._Api(
            [container("trawl", mem_limit=4 * GIB)],
            stats_error=OSError("connection refused"),
        )
        families = self._families(api)
        assert [s.value for s in families[METRIC_NAME].samples] == [1.0]
        assert families[MEMORY_METRIC_NAME].samples == []

    def test_a_failed_stats_read_omits_the_sample_rather_than_zeroing_it(self):
        """Absence is the signal Stage 3d's gate treats as "do not recycle and
        alert". A 0 would instead read as maximum headroom and green-light a
        restart on no information."""
        api = TestCollector._Api(
            [container("trawl", mem_limit=4 * GIB)],
            stats_error=OSError("boom"),
        )
        families = self._families(api)
        assert families[MEMORY_METRIC_NAME].samples == []
        limits = [s.labels["container"] for s in families[MEMORY_LIMIT_METRIC_NAME].samples]
        assert limits == ["trawl"], "the limit is known from inspect and stays published"

    def test_a_fleet_inspect_failure_still_takes_the_whole_endpoint_down(self):
        """Unchanged from Plan 140: /metrics must 500 so up goes to 0. Stage 3a
        must not have softened this by adding a second, failure-tolerant read."""
        class _Broken:
            def inspect_project_containers(self, project):
                raise OSError("connection refused")

        with pytest.raises(OSError):
            list(ContainerHealthCollector(_Broken(), PROJECT).collect())
