"""Layer 4: every container_health route, reached through the routing table.

This suite exists because `/project-status/{project}` 404d in production for
roughly eleven hours on 2026-08-30 while both of its interesting endpoints were
under test the whole time -- by tests that called
`app.active_oneoff_processes()` and `app.project_status()` directly. Calling the
handler proves the function works and says nothing about the URL, the method,
the prefix, or whether the router was included at all.

So every assertion here goes through `TestClient` and checks the status code
first. G6 in docs/TESTING.md is the rule; this file and the ops suite are what
empty its waiver list.

Plan 162 Stage 6.
"""
import pytest

pytestmark = pytest.mark.integration

_HEALTH_GAUGE = "cartracker_container_health{"


def _health_gauge(body: str) -> dict[str, float]:
    """`container -> value` for the health gauge, read out of the exposition.

    Parsed from the served text rather than from the registry, because the
    text is what Prometheus scrapes and the registry is not.
    """
    return {
        line.split('container="')[1].split('"')[0]: float(line.rsplit(" ", 1)[1])
        for line in body.splitlines()
        if line.startswith(_HEALTH_GAUGE)
    }


def test_health_is_reachable_and_shallow(api_client):
    """`/health` is not exempt from route coverage, and is the reason why.

    It is deliberately shallow -- it never probes Docker, because a cascading
    healthcheck is a worse signal than none. That makes it exactly the endpoint
    a handler-level test would call correct while the URL was gone, and it is
    what another service's drain logic reads.
    """
    response = api_client.get("/health")

    assert response.status_code == 200
    assert response.json() == {"ok": True}


def test_metrics_serves_the_prometheus_exposition(api_client):
    """The scrape target itself, through the full collector path.

    Reaching this route runs `ContainerHealthCollector.collect`, which issues
    the fleet inspect and one stats read per capped container -- so a 200 here
    is evidence about `DockerApi`, not only about the route.
    """
    response = api_client.get("/metrics")

    assert response.status_code == 200
    assert response.headers["content-type"].startswith("text/plain")
    assert "cartracker_container_health" in response.text


def test_metrics_publishes_each_health_state_from_the_recorded_fleet(api_client):
    """The three states, distinguished -- not merely "some metrics came back".

    The fixture fleet is built so each one is reachable: `ops` runs with a
    passing healthcheck (1), `scraper` runs with none at all (-1), and the
    contract's whole point is that those two are different. A collector that
    published -1 for everything would satisfy the route rule and be useless.
    """
    values = _health_gauge(api_client.get("/metrics").text)

    assert values["ops"] == 1.0, "a container with a passing healthcheck is healthy"
    assert values["scraper"] == -1.0, "a container with no healthcheck is unconfigured"


def test_metrics_excludes_containers_from_other_compose_projects(api_client):
    """The project label is the filter, and the recording proves it is applied.

    The corpus was captured on a daemon that also had an unrelated `de-podcast`
    project running, and `lakekeeper` belongs to `cartracker-lakehouse`. Neither
    may appear here: publishing another project's containers is what made the
    metric page forever for services nobody intended to run.
    """
    containers = set(_health_gauge(api_client.get("/metrics").text))

    assert "lakekeeper" not in containers
    assert not any(name.startswith("de-podcast") for name in containers)


def test_oneoff_processes_reports_the_live_one_shot(api_client):
    """Plan 142 drain evidence, through the URL its caller actually uses."""
    response = api_client.get("/oneoff-processes")

    assert response.status_code == 200
    payload = response.json()
    assert payload["known"] is True
    assert payload["active_processes"] == 1
    assert payload["processes"][0]["service"] == "snapshot-worker"
    assert payload["processes"][0]["started_at"]


def test_oneoff_processes_excludes_long_running_services(api_client):
    """A one-off is a label, not a guess, and `ops` must not read as drain work.

    If long-running services leaked into this list the auxiliary release gate
    would never see the fleet drain -- which is the failure Plan 142's first
    production deploy actually hit.
    """
    services = {
        process["service"]
        for process in api_client.get("/oneoff-processes").json()["processes"]
    }

    assert services == {"snapshot-worker"}


def test_project_status_reads_the_named_sibling_project(api_client):
    """The route whose 404 went unnoticed for eleven hours.

    The path parameter is what makes it fragile -- and what a handler-level
    call cannot exercise at all.
    """
    response = api_client.get("/project-status/cartracker-lakehouse")

    assert response.status_code == 200
    assert response.json() == {
        "known": True,
        "project": "cartracker-lakehouse",
        "services": ["lakekeeper"],
    }


def test_the_client_speaks_the_pinned_api_version(api_client, wire_paths):
    """`API_VERSION` is a promise about the wire, so assert it on the wire.

    `docker_api` pins `v1.44` so a daemon upgrade cannot change the response
    shape underneath us. That guarantee is only worth anything if the prefix is
    actually sent, and the fake strips it before matching -- so nothing else in
    this suite would notice it disappearing.
    """
    api_client.get("/oneoff-processes")

    assert wire_paths, "no request reached the fake, so this asserts nothing"
    assert all(path.startswith("/v1.44/") for path in wire_paths), (
        f"requests did not carry the pinned API version: "
        f"{sorted({p.split('/')[1] for p in wire_paths})}"
    )
