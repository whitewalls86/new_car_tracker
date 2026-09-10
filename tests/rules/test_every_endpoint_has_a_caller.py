"""Every endpoint a service serves has a caller, or is externally reachable.

Plan 162 Stage AL, gap G33 — statement 3 of §*How a service reaches another
service*.

**The caller side of this seam was watched and the callee side was the half
that was actually wrong.** ``test_every_response_we_ask_for_has_its_status_read``
waived six calls to ``dbt_runner`` endpoints deleted in April and May, found
four and a half months later by accident — and the finding was not the stale
waivers, it was that those endpoints *kept answering*. An endpoint nobody
calls is an endpoint whose breakage, drift, or deletion is invisible, and this
repository has already paid for that once.

**"Externally reachable" is pointed at declarations that already exist rather
than a second list.** The ``Caddyfile`` is what decides which routes a browser
can reach, and ``tests/test_caddy_public_routes.py`` already parses it with
Caddy's own specificity rule — this rule resolves each declared route through
that parser rather than keeping its own copy. ``forward_auth`` is read too,
because ``/auth/check`` has exactly one caller and it is Caddy itself. The
other two machine-readable callers are ``docker-compose.yml``'s healthchecks
and Prometheus's scrape configs, each read from its own file.

**A ledger entry is a caller the machine cannot see, or no caller found — and
the two are annotated apart.** Ten of the seventeen are the coordination and
deploy surface, called by ``scripts/redeploy.sh`` and
``scripts/host_maintenance.py`` over ``localhost:8060`` — real callers,
invisible to a reader keyed on compose hostnames. The other seven resolve to
no caller anywhere in the tree, which is precisely the ``dbt_runner`` shape
this rule exists to make loud; whether each is called by hand, awaits a
caller, or should be deleted is draining work, recorded per entry below.
"""
from __future__ import annotations

import re

import yaml

from tests.rules.test_no_service_imports_another_services_package import (
    service_code_roots,
)
from tests.rules.test_testing_contract import REPO_ROOT
from tests.rules.test_the_artifact_declares_what_the_handler_returns import (
    artifact_declarations,
)
from tests.service_contracts import caller_endpoints, owned_hosts
from tests.test_caddy_public_routes import _resolve

_PARAM = re.compile(r"\{[^}]+\}")


def _normalised(service: str, verb: str, path: str) -> tuple[str, str, str]:
    return (service, verb.upper(), _PARAM.sub("{}", path))


def called_endpoints() -> set[tuple[str, str, str]]:
    """Every declared endpoint some production module resolves a call to."""
    found: set[tuple[str, str, str]] = set()
    for root in sorted(service_code_roots()):
        for path in sorted((REPO_ROOT / root).rglob("*.py")):
            if "__pycache__" in path.parts:
                continue
            relative = path.relative_to(REPO_ROOT).as_posix()
            for package, verb, route in caller_endpoints(relative):
                found.add(_normalised(package, verb, route))
    return found


def healthcheck_endpoints() -> set[tuple[str, str, str]]:
    """The paths ``docker-compose.yml``'s healthchecks curl, per owned package."""
    compose = yaml.safe_load(
        (REPO_ROOT / "docker-compose.yml").read_text(encoding="utf-8")
    )
    found: set[tuple[str, str, str]] = set()
    for name, service in (compose.get("services") or {}).items():
        package = owned_hosts().get(name)
        if package is None:
            continue
        test = (service.get("healthcheck") or {}).get("test")
        if not test:
            continue
        text = " ".join(test) if isinstance(test, list) else str(test)
        for match in re.finditer(r"https?://[^/\s]+(/[^\s\"']*)", text):
            found.add(_normalised(package, "GET", match.group(1)))
    return found


def scraped_endpoints() -> set[tuple[str, str, str]]:
    """The metrics paths Prometheus's own config scrapes from owned hosts."""
    prometheus = yaml.safe_load(
        (REPO_ROOT / "prometheus" / "prometheus.yml").read_text(encoding="utf-8")
    )
    found: set[tuple[str, str, str]] = set()
    for job in prometheus.get("scrape_configs") or []:
        path = job.get("metrics_path", "/metrics")
        for config in job.get("static_configs") or []:
            for target in config.get("targets") or []:
                package = owned_hosts().get(target.split(":")[0])
                if package is not None:
                    found.add(_normalised(package, "GET", path))
    return found


def forward_auth_endpoints() -> set[tuple[str, str, str]]:
    """The routes Caddy itself calls: ``forward_auth <host> { uri <path> }``."""
    text = (REPO_ROOT / "Caddyfile").read_text(encoding="utf-8")
    found: set[tuple[str, str, str]] = set()
    for match in re.finditer(
        r"forward_auth\s+([a-z0-9_-]+):\d+\s*\{\s*\n\s*uri\s+(\S+)", text
    ):
        package = owned_hosts().get(match.group(1))
        if package is not None:
            found.add(_normalised(package, "GET", match.group(2).split("?")[0]))
    return found


def _caddy_reachable(service: str, route: str) -> bool:
    """Does a browser's request for *route* land on *service* through Caddy?

    Resolved with the specificity rule ``tests/test_caddy_public_routes.py``
    reproduces from Caddy's own behaviour, not with source order. Path
    parameters are substituted with a literal segment, because a matcher
    never sees the template.
    """
    block = _resolve(_PARAM.sub("x", route))
    upstream = (block.upstream or "").split(":")[0]
    return owned_hosts().get(upstream) == service


def uncalled_endpoints() -> set[str]:
    """Every declared endpoint with no machine-readable caller of any kind."""
    covered = (
        called_endpoints()
        | healthcheck_endpoints()
        | scraped_endpoints()
        | forward_auth_endpoints()
    )
    found: set[str] = set()
    for service, route, verb, _op_id, _codes in artifact_declarations():
        if _normalised(service, verb, route) in covered:
            continue
        if _caddy_reachable(service, route):
            continue
        found.add(f"{service} {verb.upper()} {route}")
    return found


# Keyed on service-verb-route, never a line number. Two kinds of entry, told
# apart on purpose, because their drains are different work:
#
# **Called, but not machine-readably** — the whole coordination and deploy
# surface, called by `scripts/redeploy.sh` and `scripts/host_maintenance.py`
# over `localhost:8060`. The callers are real; a reader keyed on compose
# hostnames cannot see them. Draining these means the scripts' calls becoming
# visible to a reader, not the endpoints growing callers.
#
# **No caller found anywhere in the tree** — the `dbt_runner` shape. Each of
# these is either called by hand (curl against a route built for operators),
# awaiting a caller, or a candidate for deletion, and deciding which is
# draining work this ledger holds open rather than settles.
UNCALLED_ENDPOINT_LEDGER: tuple[str, ...] = (
    # called by scripts/host_maintenance.py (localhost)
    "ops POST /coordination/request",
    "ops POST /coordination/complete",
    "ops POST /coordination/host-evidence",
    # called by both scripts/host_maintenance.py and scripts/redeploy.sh
    "ops POST /coordination/authorize",
    "ops POST /coordination/begin-drain",
    "ops POST /coordination/begin-validation",
    "ops GET /coordination/drain-status",
    "ops GET /coordination/status",
    # called by scripts/redeploy.sh (and deploy/complete by scripts/deploy.sh)
    "ops POST /deploy/start",
    "ops POST /deploy/complete",
    # no caller found anywhere in the tree
    "ops POST /coordination/cancel",
    "ops GET /coordination/local-drain",
    "ops GET /coordination/release-status",
    "ops GET /deploy/status",
    "processing POST /process/artifact/{artifact_id}",
    "scraper POST /scrape_detail",
    "scraper GET /scrape_results/jobs",
)


def test_the_endpoint_coverage_readers_are_not_blind():
    """The floor, and its direction is the unusual one.

    A blind *caller* reader makes this rule louder, not quieter — endpoints
    lose coverage and fail unwaived. The quiet failure is a *coverage* reader
    over-crediting, so the two derived equalities below pin the coverage sets
    to what other files declare: every owned service healthchecks over HTTP,
    and the services Prometheus scrapes are exactly the ones whose contracts
    declare ``GET /metrics`` — a `/metrics` nobody scrapes is dead
    observability, and a scrape of a route no contract declares is Prometheus
    polling a 404.
    """
    assert called_endpoints(), (
        "no production module resolves a call to any declared endpoint; "
        "either the URL constants moved or the caller reader went blind."
    )

    checked = {package for package, _verb, _path in healthcheck_endpoints()}
    owned = set(owned_hosts().values())
    assert checked == owned, (
        f"compose healthchecks curl {sorted(checked)} and this repository "
        f"builds {sorted(owned)}. A service whose healthcheck stops being an "
        f"HTTP request drops out of this rule's coverage set silently."
    )

    scraped = {package for package, _verb, _path in scraped_endpoints()}
    declaring = {
        service
        for service, route, verb, _op_id, _codes in artifact_declarations()
        if route == "/metrics" and verb == "get"
    }
    assert scraped == declaring, (
        f"Prometheus scrapes {sorted(scraped)} and contracts declare GET "
        f"/metrics for {sorted(declaring)}. The difference is a gauge nobody "
        f"reads or a scrape of a route that does not exist."
    )

    assert forward_auth_endpoints(), (
        "no forward_auth block in the Caddyfile resolves to an owned service; "
        "/auth/check's only caller is Caddy itself, so losing this reader "
        "moves that route into the ledger under a false description."
    )


def test_every_endpoint_has_a_caller_or_is_declared_externally_reachable():
    """An endpoint nobody calls is an endpoint nothing can notice breaking.

    Both directions: an uncalled endpoint outside the ledger fails, and a
    ledger entry that gains a caller fails until the entry is deleted — so
    the list can only shrink, and a reader that goes blind strands its
    entries loudly.
    """
    found = uncalled_endpoints()
    ledgered = set(UNCALLED_ENDPOINT_LEDGER)

    unwaived = sorted(found - ledgered)
    assert not unwaived, (
        "these declared endpoints have no caller this rule can find and are "
        "not reachable through the Caddyfile:\n  " + "\n  ".join(unwaived)
        + "\n\nGive the endpoint its caller, expose it deliberately, or "
        "delete it. Adding a ledger entry instead is a decision, not a "
        "convenience."
    )

    stale = sorted(ledgered - found)
    assert not stale, (
        "these ledger entries now have a visible caller and must be "
        "deleted:\n  " + "\n  ".join(stale)
    )
