"""Read-only Docker API access, through docker-socket-proxy.

This module never talks to `/var/run/docker.sock` directly. Mounting the socket
with `:ro` makes the socket *file* read-only and does nothing to the API behind
it -- any client that can connect can `POST /containers/{id}/restart`, `kill`,
or create a privileged container. `docker-socket-proxy` with `CONTAINERS=1` and
`POST=0` is what actually enforces read-only, so this client only ever needs to
speak plain HTTP to it. See docs/plan_140_service_health_contract.md, Stage 2.
"""
from __future__ import annotations

import json
import urllib.parse
import urllib.request
from typing import Any, Dict, List, Mapping, Optional

from container_health.collector import PROJECT_LABEL

# The daemon's advertised MinAPIVersion (Docker 29.1.3 reports 1.44). Pinning
# means a daemon upgrade cannot silently change the response shape underneath
# us, and `.State.Health` has been stable well below this version. A daemon old
# enough to reject it fails the request outright, which reads as
# up{job="container-health"} == 0 rather than as a healthy empty fleet.
API_VERSION = "v1.44"


class DockerApi:
    """The one read this exporter needs, and nothing else.

    The project label is applied server-side as an optimisation, so a busy host
    is not inspected container by container. It is *not* the scoping rule --
    that lives in `collector.health_values`, where it is unit-tested, and is
    re-applied there against what actually came back.
    """

    def __init__(self, base_url: str, timeout: float = 5.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._timeout = timeout

    def _get(self, path: str, params: Optional[Mapping[str, str]] = None) -> Any:
        url = f"{self._base_url}/{API_VERSION}{path}"
        if params:
            url = f"{url}?{urllib.parse.urlencode(params)}"
        with urllib.request.urlopen(url, timeout=self._timeout) as response:
            return json.loads(response.read())

    def inspect_project_containers(self, project: str) -> List[Dict[str, Any]]:
        """Inspect every non-transient container of one compose project.

        `status` is spelled out rather than left to the endpoint's default so
        that restarting and paused containers are enumerated too -- a crash
        loop should read as unhealthy, not vanish from the metric. Exited
        containers are deliberately absent: `flyway` and `airflow-init` are
        one-shots whose contract is `service_completed_successfully`, and a
        health status on a container that is supposed to be gone is noise.
        """
        filters = json.dumps({
            "status": ["running", "restarting", "paused"],
            "label": [f"{PROJECT_LABEL}={project}"],
        })
        summaries = self._get("/containers/json", {"all": "true", "filters": filters})
        return [self._get(f"/containers/{summary['Id']}/json") for summary in summaries]
