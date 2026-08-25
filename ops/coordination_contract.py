"""Checked-in service-to-surface coordination contract for Plan 142."""

from dataclasses import dataclass

SURFACES = frozenset(
    {
        "detail_fetch",
        "listing_fetch",
        "processing",
        "archive",
        "analytics",
        "airflow_control",
        "observability",
        "ingress",
        "database",
        "host",
    }
)

HOST_TARGET = "host"
SERVICE_LIFECYCLES = frozenset(
    {"continuous", "profile_continuous", "initialization", "one_shot"}
)


@dataclass(frozen=True)
class ServiceContract:
    surfaces: frozenset[str]
    lifecycle: str
    compose_dependencies: frozenset[str] = frozenset()
    followers: frozenset[str] = frozenset()
    no_pause_reason: str = ""


def _c(
    *surfaces: str,
    lifecycle: str,
    dependencies: tuple[str, ...] = (),
    followers: tuple[str, ...] = (),
    no_pause_reason: str = "",
) -> ServiceContract:
    return ServiceContract(
        frozenset(surfaces),
        lifecycle,
        frozenset(dependencies),
        frozenset(followers),
        no_pause_reason,
    )


# Exact coverage is enforced against docker-compose.yml. A new service cannot
# silently inherit an empty/no-pause contract; adding it makes CI fail until a
# reviewer chooses its surfaces and records why an empty set is truthful.
SERVICE_CONTRACTS = {
    "postgres": _c(
        "database",
        "detail_fetch",
        "listing_fetch",
        "processing",
        "archive",
        "analytics",
        "airflow_control",
        lifecycle="continuous",
    ),
    "flyway": _c("database", lifecycle="initialization", dependencies=("postgres",)),
    "ops": _c(
        "detail_fetch",
        "listing_fetch",
        "processing",
        "archive",
        "analytics",
        lifecycle="continuous",
        dependencies=("flyway",),
    ),
    "flaresolverr": _c("detail_fetch", lifecycle="continuous"),
    "redis-trawl": _c("detail_fetch", lifecycle="profile_continuous"),
    "trawl": _c(
        "detail_fetch", lifecycle="profile_continuous", dependencies=("redis-trawl",)
    ),
    "scraper": _c(
        "detail_fetch",
        "listing_fetch",
        lifecycle="continuous",
        dependencies=("flaresolverr", "minio"),
    ),
    "dbt": _c("analytics", lifecycle="one_shot"),
    "dbt_test": _c("analytics", lifecycle="one_shot"),
    "dbt_runner": _c("analytics", lifecycle="continuous", dependencies=("minio",)),
    "minio": _c(
        "detail_fetch",
        "listing_fetch",
        "processing",
        "archive",
        "analytics",
        lifecycle="continuous",
    ),
    "archiver": _c(
        "archive", "analytics", lifecycle="continuous", dependencies=("flyway", "minio")
    ),
    "pack-worker": _c(
        "archive", lifecycle="continuous", dependencies=("flyway", "minio")
    ),
    "snapshot-worker": _c(
        "analytics", lifecycle="one_shot", dependencies=("flyway", "minio")
    ),
    "dashboard": _c(
        lifecycle="continuous",
        dependencies=("flyway",),
        no_pause_reason=(
            "Read-only presentation service; losing it admits no production mutation."
        ),
    ),
    "pgadmin": _c(
        lifecycle="continuous",
        no_pause_reason="Read-only operator UI; losing it admits no production mutation."
    ),
    "oauth2-proxy": _c("ingress", lifecycle="continuous"),
    "caddy": _c(
        "ingress",
        lifecycle="continuous",
        dependencies=("airflow-apiserver", "dashboard", "grafana", "oauth2-proxy", "ops"),
    ),
    "processing": _c(
        "processing", lifecycle="continuous", dependencies=("flyway", "minio")
    ),
    "airflow-init": _c(
        "database",
        "airflow_control",
        lifecycle="initialization",
        dependencies=("flyway", "postgres"),
    ),
    "airflow-apiserver": _c(
        "airflow_control", lifecycle="continuous", dependencies=("airflow-init", "postgres")
    ),
    "airflow-scheduler": _c(
        "airflow_control", lifecycle="continuous", dependencies=("airflow-init", "postgres")
    ),
    "airflow-dag-processor": _c(
        "airflow_control", lifecycle="continuous", dependencies=("airflow-init", "postgres")
    ),
    "airflow-triggerer": _c(
        "airflow_control", lifecycle="continuous", dependencies=("airflow-init", "postgres")
    ),
    "statsd-exporter": _c(
        "observability",
        lifecycle="continuous",
        followers=(
            "airflow-scheduler",
            "airflow-dag-processor",
            "airflow-triggerer",
            "airflow-apiserver",
        ),
    ),
    "postgres-exporter": _c(
        "observability", lifecycle="continuous", dependencies=("flyway",)
    ),
    "node-exporter": _c("observability", lifecycle="continuous"),
    "prometheus": _c("observability", lifecycle="continuous"),
    "grafana": _c(
        "observability", lifecycle="continuous", dependencies=("loki", "prometheus")
    ),
    "loki": _c("observability", lifecycle="continuous"),
    "promtail": _c("observability", lifecycle="continuous", dependencies=("loki",)),
    "docker-socket-proxy": _c("observability", lifecycle="continuous"),
    "container-health": _c(
        "observability", lifecycle="continuous", dependencies=("docker-socket-proxy",)
    ),
}


def expand_targets(targets: set[str]) -> tuple[frozenset[str], frozenset[str]]:
    """Return immutable target and surface closure, including cached peers."""
    if HOST_TARGET in targets:
        if targets != {HOST_TARGET}:
            raise ValueError("host cannot be combined with service targets")
        return frozenset({HOST_TARGET}), SURFACES

    unknown = targets - SERVICE_CONTRACTS.keys()
    if unknown:
        raise ValueError(f"unknown coordination targets: {sorted(unknown)}")

    expanded = set(targets)
    for target in tuple(targets):
        expanded.update(SERVICE_CONTRACTS[target].followers)

    surfaces = {surface for target in expanded for surface in SERVICE_CONTRACTS[target].surfaces}
    return frozenset(expanded), frozenset(surfaces)
