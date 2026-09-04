"""Replay the container_health fixture corpus through the real socket proxy.

`tests/integration/container_health/` runs the four routes against a strict
fake that serves this corpus, which is what makes a Layer 4 suite possible
without a Docker daemon. But a fake is a recording, and a recording is only
worth what it was recorded from: nothing in that suite can notice the day
Docker or the proxy changes a response shape underneath it. The unit and
Layer 4 suites only ever ask the recording.

This asks the real thing. It stands up `tecnativa/docker-socket-proxy` against
a throwaway labelled fleet, issues **the same requests the corpus records**
through the production `DockerApi`, and asserts the live responses still carry
what `collector.py` reads out of them. Needs a Docker daemon, so it is a CI
step rather than a test -- see the `container-health-contract` job.

    python scripts/verify_container_health_docker_contract.py            # verify
    python scripts/verify_container_health_docker_contract.py --record   # refresh

This is the same split Plan 141 already uses for Promtail, deliberately:
`shared/log_ingestion_policy` is a Python model of a Go pipeline,
`tests/test_observability_config.py` asks the model, and
`scripts/verify_promtail_contract.py` replays the corpus through the real
image. One corpus, two consumers, neither importing the other -- so what runs
where is a CI-wiring question rather than a code change.

**What this deliberately does not assert.** Not the values -- a throwaway fleet
has no opinion about whether `ops` should be healthy. Only the *shape*: that
every field the collector reads is still present and still typed the way the
corpus recorded it. Values are the Layer 4 suite's business, where they are
fixed by the recording and therefore assertable.

Plan 162 Stage H.
"""
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

from container_health.collector import (  # noqa: E402
    ONEOFF_LABEL,
    PROJECT_LABEL,
    SERVICE_LABEL,
)
from container_health.docker_api import API_VERSION, DockerApi  # noqa: E402

CORPUS = REPO_ROOT / "tests" / "fixtures" / "container_health" / "docker_api_contract.json"

PROXY_IMAGE = "tecnativa/docker-socket-proxy:0.3.0"
FLEET_IMAGE = "alpine"
NAME_PREFIX = "ch-contract"
PROXY_PORT = 12375
PROJECT = "cartracker"
SIBLING_PROJECT = "cartracker-lakehouse"

# The fleet is defined here rather than in the corpus so that --record and
# --verify stand up the *same* fleet. A corpus recorded from one shape and
# verified against another would compare two different things and call the
# difference drift.
#
# Each entry exists for a branch in collector.py, not for realism:
#   ops             -- running + healthy + a memory cap, so health_value returns
#                      HEALTHY and memory_capped has something to yield
#   scraper         -- running with no healthcheck, which is UNCONFIGURED and
#                      the state most easily confused with healthy
#   snapshot-worker -- a live one-off, the only input /oneoff-processes has
#   lakekeeper      -- a sibling project, so the label filter has something to
#                      exclude and /project-status/{project} has something to find
FLEET = (
    {
        "name": "ops",
        "project": PROJECT,
        "service": "ops",
        "oneoff": False,
        "memory": 268435456,
        "healthcheck": True,
    },
    {
        "name": "scraper",
        "project": PROJECT,
        "service": "scraper",
        "oneoff": False,
        "memory": 0,
        "healthcheck": False,
    },
    {
        "name": "oneoff",
        "project": PROJECT,
        "service": "snapshot-worker",
        "oneoff": True,
        "memory": 0,
        "healthcheck": False,
    },
    {
        "name": "lakekeeper",
        "project": SIBLING_PROJECT,
        "service": "lakekeeper",
        "oneoff": False,
        "memory": 0,
        "healthcheck": False,
    },
)

# Every field collector.py and app.py actually read, as (dotted path, type).
# Derived by reading those modules, and the reason this script is worth running:
# a daemon that stops sending one of these breaks production silently, and the
# corpus would keep serving the old shape forever.
INSPECT_FIELDS = (
    ("Id", str),
    ("Config.Labels", dict),
    ("State.Status", str),
    ("HostConfig.Memory", int),
)
STATS_FIELDS = (("memory_stats", dict),)


class ContractError(RuntimeError):
    """The live API no longer carries what the corpus recorded."""


def _run(*args: str, check: bool = True) -> str:
    result = subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=False
    , encoding="utf-8")
    if check and result.returncode != 0:
        raise ContractError(f"docker {' '.join(args)} failed: {result.stderr.strip()}")
    return result.stdout.strip()


def _teardown() -> None:
    names = [f"{NAME_PREFIX}-{entry['name']}" for entry in FLEET]
    _run("rm", "-f", f"{NAME_PREFIX}-proxy", *names, check=False)


def _start_fleet() -> None:
    _teardown()
    for entry in FLEET:
        args = [
            "run", "-d", "--name", f"{NAME_PREFIX}-{entry['name']}",
            "-l", f"{PROJECT_LABEL}={entry['project']}",
            "-l", f"{SERVICE_LABEL}={entry['service']}",
        ]
        if entry["oneoff"]:
            args += ["-l", f"{ONEOFF_LABEL}=True"]
        if entry["memory"]:
            args += ["--memory", str(entry["memory"])]
        if entry["healthcheck"]:
            args += [
                "--health-cmd", "true", "--health-interval", "1s",
                "--health-retries", "1", "--health-start-period", "0s",
            ]
        _run(*args, FLEET_IMAGE, "sleep", "3600")

    _run(
        "run", "-d", "--name", f"{NAME_PREFIX}-proxy",
        "-p", f"{PROXY_PORT}:2375",
        "-e", "CONTAINERS=1", "-e", "POST=0",
        "-v", "/var/run/docker.sock:/var/run/docker.sock:ro",
        PROXY_IMAGE,
    )


def _await_proxy(timeout: float = 30.0) -> None:
    """`/_ping` is granted by the proxy's own defaults, so this probes the proxy
    rather than reaching through it -- the same endpoint the compose healthcheck
    uses."""
    deadline = time.monotonic() + timeout
    last = ""
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(
                f"http://localhost:{PROXY_PORT}/_ping", timeout=2
            ) as response:
                if response.status == 200:
                    return
        except (urllib.error.URLError, OSError) as error:  # not up yet
            last = str(error)
        time.sleep(0.5)
    raise ContractError(f"proxy did not answer /_ping within {timeout}s: {last}")


def _await_healthy(timeout: float = 30.0) -> None:
    """The `ops` fixture must reach `healthy`, or HEALTHY is never recorded.

    Without this the corpus records `starting`, which is a real state but not
    the one the fixture exists to provide -- and the failure would be a
    confusing value mismatch in the Layer 4 suite rather than a timeout here.
    """
    deadline = time.monotonic() + timeout
    name = f"{NAME_PREFIX}-ops"
    while time.monotonic() < deadline:
        status = _run(
            "inspect", "--format", "{{.State.Health.Status}}", name, check=False
        )
        if status == "healthy":
            return
        time.sleep(0.5)
    raise ContractError(f"{name} did not become healthy within {timeout}s")


def _capture(api: DockerApi) -> List[Dict[str, Any]]:
    """Every request the production client makes, with what came back.

    Recorded by spying on `DockerApi._get` rather than by re-deriving the URLs
    here. A corpus built from a second copy of the URL construction would pass
    forever while production drifted away from it, which is the paraphrase
    failure docs/TESTING.md names for SQL, in another costume.
    """
    exchanges: List[Dict[str, Any]] = []
    original = DockerApi._get

    def spy(self, path, params=None):
        response = original(self, path, params)
        exchanges.append(
            {"path": path, "params": dict(params) if params else None,
             "response": response}
        )
        return response

    DockerApi._get = spy
    try:
        inspections = api.inspect_project_containers(PROJECT)
        for inspection in inspections:
            limit = ((inspection.get("HostConfig") or {}).get("Memory")) or 0
            if limit > 0:
                api.container_stats(inspection["Id"])
        api.inspect_project_containers(SIBLING_PROJECT)
    finally:
        DockerApi._get = original
    return exchanges


def _dotted(payload: Any, path: str) -> Any:
    for part in path.split("."):
        if not isinstance(payload, dict) or part not in payload:
            raise KeyError(path)
        payload = payload[part]
    return payload


def _check_shape(exchanges: List[Dict[str, Any]]) -> List[str]:
    problems = []
    for exchange in exchanges:
        path, response = exchange["path"], exchange["response"]
        if path.endswith("/stats"):
            fields = STATS_FIELDS
        elif path.endswith("/json") and "/containers/" in path and path != "/containers/json":
            fields = INSPECT_FIELDS
        else:
            continue
        for dotted, expected in fields:
            try:
                value = _dotted(response, dotted)
            except KeyError:
                problems.append(f"{path}: missing {dotted}")
                continue
            if not isinstance(value, expected):
                problems.append(
                    f"{path}: {dotted} is {type(value).__name__}, "
                    f"expected {expected.__name__}"
                )
    return problems


def _request_kinds(paths: "set[str] | List[str]") -> set:
    """Collapse per-container paths to the kind of request they are.

    Container ids change on every run, so comparing raw paths would report
    drift on a fleet that behaved identically. The question this comparison
    asks is whether the same *kinds* of request still work, and an id is not
    a kind.
    """
    kinds = set()
    for path in paths:
        if path == "/containers/json":
            kinds.add(path)
        elif path.endswith("/stats"):
            kinds.add("/containers/<id>/stats")
        elif path.endswith("/json"):
            kinds.add("/containers/<id>/json")
        else:
            kinds.add(path)
    return kinds


def _fleet_seen(exchanges: List[Dict[str, Any]]) -> List[str]:
    """The fleet is only a fixture if the proxy actually returned it."""
    services = set()
    for exchange in exchanges:
        response = exchange["response"]
        if isinstance(response, dict):
            labels = (response.get("Config") or {}).get("Labels") or {}
            if labels.get(SERVICE_LABEL):
                services.add(labels[SERVICE_LABEL])
    expected = {entry["service"] for entry in FLEET}
    return sorted(expected - services)


def _corpus(exchanges: List[Dict[str, Any]], daemon: str) -> Dict[str, Any]:
    return {
        "captured_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "source": (
            f"{PROXY_IMAGE} against Docker {daemon}, "
            f"requested at {API_VERSION} by container_health.docker_api"
        ),
        "notes": (
            "Recorded by scripts/verify_container_health_docker_contract.py "
            "--record against a throwaway labelled fleet. Not hand-written: "
            "regenerate rather than edit, or the Layer 4 suite is asserting "
            "against something no daemon ever sent."
        ),
        "fleet": [dict(entry) for entry in FLEET],
        "exchanges": exchanges,
    }


def _daemon_version() -> str:
    return _run("version", "--format", "{{.Server.Version}} (api {{.Server.APIVersion}})")


def main(argv: List[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument(
        "--record", action="store_true",
        help="refresh the committed corpus from the live API",
    )
    parser.add_argument("--keep", action="store_true", help="leave the fleet running")
    args = parser.parse_args(argv)

    report: Callable[[str], None] = lambda line: print(  # noqa: E731
        f"verify_container_health_docker_contract: {line}"
    )

    try:
        _start_fleet()
        _await_proxy()
        _await_healthy()
        exchanges = _capture(DockerApi(f"http://localhost:{PROXY_PORT}"))
    finally:
        if not args.keep:
            _teardown()

    missing = _fleet_seen(exchanges)
    if missing:
        report(f"FAIL the proxy never returned these fixture services: {missing}")
        return 1

    problems = _check_shape(exchanges)
    if problems:
        report("FAIL the live API no longer carries what the corpus records:")
        for problem in problems:
            report(f"  {problem}")
        return 1

    if args.record:
        CORPUS.parent.mkdir(parents=True, exist_ok=True)
        CORPUS.write_text(
            json.dumps(_corpus(exchanges, _daemon_version()), indent=2) + "\n",
            encoding="utf-8",
        )
        report(f"recorded {len(exchanges)} exchanges to {CORPUS.relative_to(REPO_ROOT)}")
        return 0

    if not CORPUS.exists():
        report(f"FAIL no corpus at {CORPUS.relative_to(REPO_ROOT)}; run --record")
        return 1

    committed = json.loads(CORPUS.read_text(encoding="utf-8"))
    recorded_paths = {exchange["path"] for exchange in committed["exchanges"]}
    live_paths = {exchange["path"] for exchange in exchanges}
    only_live = _request_kinds(live_paths) - _request_kinds(recorded_paths)
    only_recorded = _request_kinds(recorded_paths) - _request_kinds(live_paths)
    if only_live or only_recorded:
        report("FAIL the request set has drifted from the corpus:")
        for path in sorted(only_live):
            report(f"  live but not recorded: {path}")
        for path in sorted(only_recorded):
            report(f"  recorded but not live: {path}")
        report("  run --record if this is intended")
        return 1

    report(
        f"ok {len(exchanges)} live exchanges match the corpus recorded "
        f"{committed['captured_at']}"
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
