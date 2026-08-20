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

from typing import Dict, Iterable, Iterator, Mapping

from prometheus_client.core import GaugeMetricFamily

METRIC_NAME = "cartracker_container_health"
METRIC_DOC = (
    "Docker health of each non-transient container in the default compose "
    "project: 1 healthy, 0 unhealthy or not yet healthy, -1 no healthcheck "
    "configured"
)

HEALTHY = 1
UNHEALTHY = 0
UNCONFIGURED = -1

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


def health_values(inspections: Iterable[Mapping], project: str) -> Dict[str, int]:
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
    return dict(sorted(values.items()))


class ContainerHealthCollector:
    """Computes on every scrape. There is no cached value to go stale."""

    def __init__(self, api, project: str) -> None:
        self._api = api
        self._project = project

    def collect(self) -> Iterator[GaugeMetricFamily]:
        inspections = self._api.inspect_project_containers(self._project)
        family = GaugeMetricFamily(METRIC_NAME, METRIC_DOC, labels=["container"])
        for service, value in health_values(inspections, self._project).items():
            family.add_metric([service], value)
        yield family
