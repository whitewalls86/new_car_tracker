"""Executable source and parsing contract for logs retained in Loki.

Promtail remains the production implementation. This module is the compact,
deterministic model used by fixture tests to keep its pipeline stages, Grafana
selectors, and Compose coverage from drifting independently.
"""

from __future__ import annotations

import json
import re
from dataclasses import dataclass
from enum import Enum
from typing import Final

NORMALIZED_LEVELS: Final = frozenset(
    {
        "DEBUG",
        "INFO",
        "WARNING",
        "ERROR",
        "CRITICAL",
    }
)
ERROR_PANEL_LEVELS: Final = frozenset({"WARNING", "ERROR", "CRITICAL"})
APPLICATION_SERVICES: Final = frozenset(
    {
        "ops",
        "scraper",
        "processing",
        "dbt_runner",
        "archiver",
        "pack-worker",
    }
)
SELECTED_STDOUT_SERVICES: Final = frozenset(
    {
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
        "oauth2-proxy",
    }
)
AIRFLOW_SERVICES: Final = frozenset(
    {
        "airflow-apiserver",
        "airflow-scheduler",
        "airflow-dag-processor",
    }
)
# Three severity shapes reach the control-plane containers and all three must
# classify, because anything unmatched is dropped as unclassified rather than
# retained without a level:
#   structlog (observed)  2026-08-25T14:01:37Z [error    ] msg [logger]
#   gunicorn supervisor   [2026/08/25 14:07:22] [7] [CRITICAL] WORKER TIMEOUT
#   classic task logger   [2026-08-25 14:07:22,918] {ti.py:1234} ERROR - msg
# The first alternative takes the leftmost bracketed severity within a bounded
# prefix, which covers both bracketed forms regardless of how many fields
# precede it; the second takes the unbracketed severity after a {file:line}
# field; the third (empty) allows a line that opens with its own severity.
# `\b` is what keeps "Errors 0" from reading as ERROR.
AIRFLOW_LEVEL_PATTERN: Final = (
    r"^(?:.{0,80}?\[\s*"
    r"|(?:\[[^\]]*\]\s+)?\{[^}]*\}\s+"
    r"|)"
    r"(?P<level>(?i:debug|info|warning|error|critical))\b"
)
OAUTH_ACCESS_PATTERN: Final = (
    r'^.*?\s(?P<method>[A-Z]+) - "(?P<path>[^"]+)" HTTP/[0-9.]+ '
    r'"[^"]*" (?P<status>[0-9]{3}) \d+ [0-9.]+$'
)
OAUTH_LIFECYCLE_LEVEL_PATTERN: Final = (
    r"^\[[^\]]+\]\s+\[[^\]]+\]\s+(?P<oauth_level>Warning|ERROR|Error):"
)


class IngestionRoute(str, Enum):
    APPLICATION_FILE = "application_file"
    SELECTED_STDOUT = "selected_stdout"
    INTENTIONAL_EXCLUSION = "intentional_exclusion"
    TRANSIENT_EXEMPTION = "transient_exemption"


@dataclass(frozen=True)
class SourcePolicy:
    route: IngestionRoute
    reason: str
    privacy: str


@dataclass(frozen=True)
class IngestionDecision:
    retained: bool
    labels: dict[str, str]
    drop_reason: str | None = None


def _policy(route: IngestionRoute, reason: str, privacy: str) -> SourcePolicy:
    return SourcePolicy(route=route, reason=reason, privacy=privacy)


_NO_LOKI = (
    "Excluded stdout remains available only through bounded local Docker logs; "
    "adding it to Loki requires a measured-volume use case."
)
_TRANSIENT = (
    "One-shot output remains in bounded local Docker logs and deployment/CI "
    "evidence; it is not a durable telemetry stream."
)


SOURCE_POLICY: Final[dict[str, SourcePolicy]] = {
    "ops": _policy(
        IngestionRoute.APPLICATION_FILE,
        "The API emits the shared bounded JSON application log.",
        "Only the formatter's scalar field allowlist is durable.",
    ),
    "scraper": _policy(
        IngestionRoute.APPLICATION_FILE,
        "Scrape outcomes and warnings use the shared bounded JSON application log.",
        "Only the formatter's scalar field allowlist is durable.",
    ),
    "processing": _policy(
        IngestionRoute.APPLICATION_FILE,
        "Processing outcomes use the shared bounded JSON application log.",
        "Only the formatter's scalar field allowlist is durable.",
    ),
    "dbt_runner": _policy(
        IngestionRoute.APPLICATION_FILE,
        "Build orchestration uses the shared bounded JSON application log.",
        "Only the formatter's scalar field allowlist is durable.",
    ),
    "archiver": _policy(
        IngestionRoute.APPLICATION_FILE,
        "Archival outcomes use the shared bounded JSON application log.",
        "Only the formatter's scalar field allowlist is durable.",
    ),
    "pack-worker": _policy(
        IngestionRoute.APPLICATION_FILE,
        "Pack verification needs the same durable JSON evidence as archiver.",
        "Only the formatter's scalar field allowlist is durable.",
    ),
    "airflow-apiserver": _policy(
        IngestionRoute.SELECTED_STDOUT,
        "Retain only parsed warning, error, and critical control-plane events.",
        "Routine chatter and unparsed continuation lines are dropped.",
    ),
    "airflow-scheduler": _policy(
        IngestionRoute.SELECTED_STDOUT,
        "Retain only parsed warning, error, and critical control-plane events.",
        "Routine chatter and unparsed continuation lines are dropped.",
    ),
    "airflow-dag-processor": _policy(
        IngestionRoute.SELECTED_STDOUT,
        "Retain only parsed warning, error, and critical control-plane events.",
        "Routine chatter and unparsed continuation lines are dropped.",
    ),
    "oauth2-proxy": _policy(
        IngestionRoute.SELECTED_STDOUT,
        "Retain auth failures and interactive or lifecycle events; "
        "drop successful auth subrequests.",
        "Successful auth chatter is dropped because it repeats user and request metadata.",
    ),
    "postgres": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Database health and capacity are covered by postgres-exporter metrics.",
        _NO_LOKI,
    ),
    "flaresolverr": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "This vestigial solver is not part of the active scrape path.",
        _NO_LOKI,
    ),
    "redis-trawl": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Solver queue health is covered by the solver stack's metrics and probes.",
        _NO_LOKI,
    ),
    "trawl": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Solver efficacy is covered by bounded outcome counters and alerts.",
        _NO_LOKI,
    ),
    "minio": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Object-store health and capacity are covered by MinIO metrics.",
        _NO_LOKI,
    ),
    "dashboard": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Dashboard request chatter has no approved durable diagnostic use case.",
        _NO_LOKI,
    ),
    "pgadmin": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Administrative UI stdout is local-only and may contain operator metadata.",
        _NO_LOKI,
    ),
    "caddy": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Proxy access logging has no approved durable volume and privacy budget.",
        _NO_LOKI,
    ),
    "airflow-triggerer": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "The selected Airflow control-plane set excludes triggerer routine chatter.",
        _NO_LOKI,
    ),
    "statsd-exporter": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Exporter behavior is observed through scrape health and exported metrics.",
        _NO_LOKI,
    ),
    "postgres-exporter": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Exporter behavior is observed through Prometheus scrape health.",
        _NO_LOKI,
    ),
    "node-exporter": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Exporter behavior is observed through Prometheus scrape health.",
        _NO_LOKI,
    ),
    "prometheus": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Prometheus self-health is metric based and stdout stays locally bounded.",
        _NO_LOKI,
    ),
    "grafana": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Grafana self-health is probed and its request logs may contain user metadata.",
        _NO_LOKI,
    ),
    "loki": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Ingesting Loki stdout into Loki would create a feedback loop.",
        _NO_LOKI,
    ),
    "promtail": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Promtail self-errors are checked locally during deploy and soak validation.",
        _NO_LOKI,
    ),
    "docker-socket-proxy": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "The read-only Docker metadata proxy has no durable event contract.",
        _NO_LOKI,
    ),
    "container-health": _policy(
        IngestionRoute.INTENTIONAL_EXCLUSION,
        "Container state is exported as the Plan 140 health metric contract.",
        _NO_LOKI,
    ),
    "flyway": _policy(
        IngestionRoute.TRANSIENT_EXEMPTION,
        "Flyway exits after applying schema migrations during deployment.",
        _TRANSIENT,
    ),
    "dbt": _policy(
        IngestionRoute.TRANSIENT_EXEMPTION,
        "The tools-profile dbt container is an operator-invoked one-shot.",
        _TRANSIENT,
    ),
    "dbt_test": _policy(
        IngestionRoute.TRANSIENT_EXEMPTION,
        "The tools-profile dbt test container is a CI/operator one-shot.",
        _TRANSIENT,
    ),
    "snapshot-worker": _policy(
        IngestionRoute.TRANSIENT_EXEMPTION,
        "The profile-gated snapshot worker exits after its requested export.",
        _TRANSIENT,
    ),
    "airflow-init": _policy(
        IngestionRoute.TRANSIENT_EXEMPTION,
        "Airflow initialization exits after database and account setup.",
        _TRANSIENT,
    ),
    "april-processor": _policy(
        IngestionRoute.TRANSIENT_EXEMPTION,
        "The profile-gated April reconciliation worker exits after the "
        "materialize, parse or compare run it was invoked for.",
        _TRANSIENT,
    ),
}


def classify_line(service: str, source: str, line: str) -> IngestionDecision:
    """Return the labels/drop result the Promtail contract requires."""
    labels = {"service": service, "source": source}

    if source == "application_file":
        try:
            record = json.loads(line)
        except (json.JSONDecodeError, TypeError):
            return IngestionDecision(False, labels, "application_file_unclassified")
        # Promtail's app-log jobs drop on `level=""` alone. A missing `logger`
        # simply yields no logger label there, so requiring one here modelled a
        # stricter pipeline than production runs. The shared formatter always
        # emits a level name from NORMALIZED_LEVELS; an unrecognised non-empty
        # level is retained, exactly as the drop selector would.
        level = record.get("level")
        if not isinstance(level, str) or not level:
            return IngestionDecision(False, labels, "application_file_unclassified")
        labels["level"] = level
        logger = record.get("logger")
        if isinstance(logger, str) and logger:
            labels["logger"] = logger
        return IngestionDecision(True, labels)

    if service in AIRFLOW_SERVICES:
        match = re.search(AIRFLOW_LEVEL_PATTERN, line)
        if match is None:
            return IngestionDecision(False, labels, "airflow_unclassified_control_plane")
        level = match.group("level").upper()
        labels["level"] = level
        if level in {"DEBUG", "INFO"}:
            return IngestionDecision(False, labels, "airflow_non_actionable_control_plane")
        return IngestionDecision(True, labels)

    if service == "oauth2-proxy":
        # Order mirrors the Promtail stage order: INFO, then status, then the
        # lifecycle severity, then the successful-subrequest drop. The
        # lifecycle stages run last, so on a line matching both patterns the
        # lifecycle severity wins -- an `elif` here inverted that.
        level = "INFO"
        access = re.search(OAUTH_ACCESS_PATTERN, line)
        lifecycle = re.search(OAUTH_LIFECYCLE_LEVEL_PATTERN, line)
        status = access.group("status") if access is not None else None
        if status is not None:
            labels["status"] = status
            if status.startswith("4"):
                level = "WARNING"
            elif status.startswith("5"):
                level = "ERROR"
        if lifecycle is not None:
            level = "WARNING" if lifecycle.group("oauth_level") == "Warning" else "ERROR"
        labels["level"] = level
        if (
            status == "202"
            and access.group("path").split("?", 1)[0] == "/oauth2/auth"
        ):
            return IngestionDecision(False, labels, "oauth2_successful_auth_subrequest")
        return IngestionDecision(True, labels)

    raise ValueError(f"{service!r} is not an intentionally retained stdout source")


def matches_error_dashboard(decision: IngestionDecision) -> bool:
    return decision.retained and decision.labels.get("level") in ERROR_PANEL_LEVELS
