"""Plan 140 Stage 2: container health as three explicit states.

Docker reports no health status *at all* for a container without a healthcheck
-- not "unhealthy", not "unknown", nothing. So a two-state metric makes an
unwatched service and a healthy service look identical, which is the exact
shape of every monitoring gap in this system's history. Hence `-1`:

    cartracker_container_health{container="ops"}     1   # healthy
    cartracker_container_health{container="trawl"}   0   # unhealthy
    cartracker_container_health{container="caddy"}  -1   # NO HEALTHCHECK

`-1` is ugly on a graph. That is the point.

Values are computed inside the `/metrics` handler rather than written to a
file ahead of time, so a stale reading is structurally impossible. Plan 135's
textfile collector is right for a 456-second disk walk and wrong for this: the
whole read is one list call plus one inspect per container, ~120ms measured
against 26 containers in production.
"""
from __future__ import annotations

from typing import Any, Dict, Iterable, Iterator, Mapping, Optional, Tuple

from prometheus_client.core import GaugeMetricFamily

METRIC_NAME = "cartracker_container_health"
METRIC_DOC = (
    "Docker health of each non-transient container in the default compose "
    "project: 1 healthy, 0 unhealthy, not yet healthy, or expected but absent, "
    "-1 no healthcheck configured"
)

HEALTHY = 1
UNHEALTHY = 0
UNCONFIGURED = -1

MEMORY_METRIC_NAME = "cartracker_container_memory_bytes"
MEMORY_METRIC_DOC = (
    "Resident memory of each memory-capped container, excluding reclaimable "
    "page cache -- the same figure `docker stats` prints"
)
MEMORY_LIMIT_METRIC_NAME = "cartracker_container_memory_limit_bytes"
MEMORY_LIMIT_METRIC_DOC = (
    "The container's configured memory limit, from its own inspect payload "
    "rather than from the compose file"
)

PROJECT_LABEL = "com.docker.compose.project"
SERVICE_LABEL = "com.docker.compose.service"
# `docker compose run` containers carry the project label but are one-shots.
# Without this, a `dbt`, `dbt_test`, or `snapshot-worker` invocation would
# surface as an unconfigured (-1) service for as long as it ran.
ONEOFF_LABEL = "com.docker.compose.oneoff"


class NoContainersFound(RuntimeError):
    """The fleet is never empty -- this exporter is itself a member of it.

    An empty result means the project label stopped matching (a renamed deploy
    directory, a `COMPOSE_PROJECT_NAME` change) and the metric would otherwise
    publish nothing at all. Publishing nothing reads as a healthy system, which
    is the failure mode this whole plan exists to close, so it raises instead:
    /metrics returns 500 and `up{job="container-health"}` goes to 0.
    """


def oneoff_processes(inspections: Iterable[Mapping], project: str) -> list[dict[str, Any]]:
    """Return running Compose one-offs as drain evidence, never expected services."""
    processes = []
    for inspection in inspections:
        labels = (inspection.get("Config") or {}).get("Labels") or {}
        if labels.get(PROJECT_LABEL) != project or labels.get(ONEOFF_LABEL) != "True":
            continue
        state = inspection.get("State") or {}
        if state.get("Status") not in {"running", "restarting", "paused"}:
            continue
        processes.append(
            {
                "service": labels.get(SERVICE_LABEL),
                "container_id": inspection.get("Id"),
                "started_at": state.get("StartedAt"),
            }
        )
    return sorted(processes, key=lambda item: (item["service"] or "", item["container_id"] or ""))


def health_value(inspection: Mapping) -> int:
    """One container's inspect payload to one of the three states.

    `starting` maps to 0 alongside `unhealthy`, because "not yet known to be
    healthy" is not "healthy". It is bounded -- Docker leaves `starting` for
    healthy or unhealthy within `start_period + retries * (interval + timeout)`,
    a worst case of 230s across this compose file -- and the alert's 5m `for`
    is what keeps a slow start from paging. `tests/test_observability_config.py`
    asserts that relationship so a widened `start_period` cannot break it
    silently.
    """
    state = inspection.get("State") or {}
    if state.get("Status") != "running":
        # restarting or paused: enumerated on purpose, so a crash loop reads as
        # unhealthy rather than disappearing from the metric.
        return UNHEALTHY
    health = state.get("Health")
    if health is None:
        return UNCONFIGURED
    return HEALTHY if health.get("Status") == "healthy" else UNHEALTHY


def health_values(
    inspections: Iterable[Mapping],
    project: str,
    expected: Iterable[str] = (),
) -> Dict[str, int]:
    """Scope to one compose project's own long-running services.

    "Compose-managed" is *not* a sufficient filter. The four stale `unhealthy`
    containers the Stage 1 soak found -- `cartracker-lakekeeper`,
    `-lakekeeper-migrate`, `-lakekeeper-postgres`, `cartracker-mlflow` -- were
    all compose-managed, just by the separate `cartracker-lakehouse` and
    `cartracker-mlflow` projects that Plans 125 and 112 use. That filter would
    not have excluded a single one of them, and this metric would page forever
    for services nobody intends to be running. The project label is the filter
    that works, and `up -d` on either sibling project brings the condition
    straight back, so this must not be relaxed to "has a compose label".

    `expected` closes the removed-or-stopped gap Stage 2 recorded (Plan 140
    Stage 4a). The status filter above admits only running, restarting and
    paused containers, so a service that is stopped or gone produced no series
    at all -- and an absent series reads as a healthy system, which is the
    failure mode this whole plan exists to close. Anything named there and not
    seen is published as UNHEALTHY rather than as a fourth state: 0 already
    means "should be healthy and is not", and gone is a strict case of that.

    The backfill happens *after* the empty-fleet guard, and the order is
    load-bearing. If the project label stops matching, every expected service
    would otherwise read 0 at once and page for the entire fleet, burying the
    one fact that matters -- that the exporter cannot see Docker. Raising keeps
    that a single `up{job="container-health"}` failure.

    A container that is running but *not* expected still publishes its real
    state. Drift in the other direction is a CI concern, not a runtime one, and
    dropping it here would hide a service someone started by hand.
    """
    values = {}
    for inspection in inspections:
        labels = (inspection.get("Config") or {}).get("Labels") or {}
        if labels.get(PROJECT_LABEL) != project:
            continue
        if labels.get(ONEOFF_LABEL) == "True":
            continue
        values[labels[SERVICE_LABEL]] = health_value(inspection)
    if not values:
        raise NoContainersFound(
            f"no running containers carry {PROJECT_LABEL}={project!r}; refusing "
            "to publish an empty fleet as a healthy one"
        )
    for service in expected:
        values.setdefault(service, UNHEALTHY)
    return dict(sorted(values.items()))


def memory_capped(
    inspections: Iterable[Mapping], project: str
) -> Dict[str, Tuple[str, int]]:
    """Service -> (container id, limit) for containers that declare a cap.

    Membership is read from each container's own `HostConfig.Memory` rather
    than from a list of service names kept here. That matters more than it
    looks: a hardcoded allowlist has the failure mode this plan's open question
    5 describes, where a service capped later is silently unmeasured. Derived
    from the inspect payload, a new `mem_limit` in the compose file starts
    publishing on the next scrape with no change to this file.

    Containers without a cap are skipped deliberately. An unbounded number has
    no headroom to be read against, and Plan 136 Stage 3d gates on the fraction
    of a limit -- a series with no limit could not feed it.
    """
    capped = {}
    for inspection in inspections:
        labels = (inspection.get("Config") or {}).get("Labels") or {}
        if labels.get(PROJECT_LABEL) != project:
            continue
        if labels.get(ONEOFF_LABEL) == "True":
            continue
        limit = ((inspection.get("HostConfig") or {}).get("Memory")) or 0
        if limit <= 0:
            continue
        capped[labels[SERVICE_LABEL]] = (inspection["Id"], limit)
    return dict(sorted(capped.items()))


def memory_usage(stats: Mapping) -> Optional[int]:
    """The number `docker stats` prints, not the raw cgroup total.

    `memory_stats.usage` includes reclaimable page cache, which on a container
    that reads files makes the figure both larger than the process footprint
    and unrelated to how close it is to being killed. Docker's own CLI
    subtracts the inactive file cache, and every number this plan reasons about
    -- the runbook's percentages, D7's 3.18 GB -- came from that CLI. Publishing
    the raw value would silently disagree with all of them.

    cgroup v2 spells it `inactive_file` and v1 `total_inactive_file`; the host
    runs v2, and v1 is accepted so the metric does not quietly change meaning
    if that ever moves.
    """
    memory = stats.get("memory_stats") or {}
    usage = memory.get("usage")
    if usage is None:
        return None
    detail = memory.get("stats") or {}
    inactive = detail.get("inactive_file")
    if inactive is None:
        inactive = detail.get("total_inactive_file") or 0
    return max(usage - inactive, 0)


class ContainerHealthCollector:
    """Computes on every scrape. There is no cached value to go stale."""

    def __init__(self, api, project: str, expected: Iterable[str] = ()) -> None:
        self._api = api
        self._project = project
        # Frozen at construction: an expected set that could change between
        # scrapes would make the metric's own membership a moving target.
        self._expected = frozenset(expected)

    def collect(self) -> Iterator[GaugeMetricFamily]:
        inspections = self._api.inspect_project_containers(self._project)
        family = GaugeMetricFamily(METRIC_NAME, METRIC_DOC, labels=["container"])
        for service, value in health_values(
            inspections, self._project, self._expected
        ).items():
            family.add_metric([service], value)
        yield family
        yield from self._memory(inspections)

    def _memory(self, inspections) -> Iterator[GaugeMetricFamily]:
        """Plan 136 Stage 3a. One extra GET per capped container, three today.

        A per-container stats read is allowed to fail soft where the fleet
        inspect above is not, and the asymmetry is deliberate. The inspect is
        the health metric's only input, so its failure must take
        `up{job="container-health"}` to 0. A single stats call failing should
        not: it would blind the health signal to repair a memory one.

        The cost of failing soft is a missing sample, and Stage 3d is built
        knowing that -- its gate treats an absent reading as "do not recycle
        **and** alert", never as permission to proceed. Absence is the signal,
        so it does not need to be an exception here as well.
        """
        used = GaugeMetricFamily(
            MEMORY_METRIC_NAME, MEMORY_METRIC_DOC, labels=["container"]
        )
        limits = GaugeMetricFamily(
            MEMORY_LIMIT_METRIC_NAME, MEMORY_LIMIT_METRIC_DOC, labels=["container"]
        )
        for service, (container_id, limit) in memory_capped(
            inspections, self._project
        ).items():
            limits.add_metric([service], limit)
            try:
                stats = self._api.container_stats(container_id)
            except (OSError, ValueError):
                continue
            value = memory_usage(stats)
            if value is not None:
                used.add_metric([service], value)
        yield used
        yield limits
