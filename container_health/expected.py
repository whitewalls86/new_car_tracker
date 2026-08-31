"""Plan 140 Stage 4a: the services that are supposed to be running.

Stage 2 shipped a metric that can only describe containers Docker still knows
about. ``docker_api.inspect_project_containers`` filters to
``status: [running, restarting, paused]``, so a service that is **removed or
fully stopped leaves the metric entirely** rather than reading 0 -- recorded
there as a known limitation, reproduced live during the Stage 2 soak when
``dbt_runner`` dropped the series count from 28 to 27 for one evaluation while
being recreated, and named independently by Plan 142 Stage 0 item 2:

    A stopped container does not read as unhealthy; it leaves the metric
    altogether. [...] One absent service is therefore silent by construction.

That gap only became load-bearing in Stage 4. Demoting ``http_health_sensor``
to a gate removes the DAG-failure page that was, for two services, the *only*
notification a stopped container produced: ``archiver`` and ``pack-worker`` are
sensed by nine of the sixteen sensor call sites and are not Prometheus scrape
jobs, so ``ct-service-down`` does not cover them either. Flipping the sensors
without this set would have traded a mis-named page for silence.

Absent expected services are published as ``UNHEALTHY`` (0) rather than as a
fourth state. Stage 2 argued that a fourth value re-opens the ambiguity ``-1``
exists to close, and 0 is already exactly what ``ct-container-unhealthy``
means: this should be healthy and is not. Gone is a strict case of not healthy.

## Where the list comes from, and why it is not defined here

**Plan 142 owns the manifest.** ``maintenance-running-set.txt`` already records
which services are expected running, exceptions-only, with a class and a written
reason per entry; ``tests/test_maintenance_running_set.py`` checks it against
the Compose sources. Plan 140 owns making absence visible, and Plan 142 Stage 3
consumes that as its resume gate ("neither unhealthy nor unconfigured services
hidden as absence"). So this file holds the *resolved* set and no rule of its
own -- ``expected_running_services()`` in that test module is the rule.

Restating the rule here instead was tried and was wrong in a way worth
recording: "expected running == declares a restart policy other than ``no``"
looks equivalent and silently drops the ``restart-gap`` class, which is a
service that *is* expected running and merely does not restore itself after a
reboot. ``caddy`` was exactly that until 2026-08-24, and it serves :80 and :443.

## Why a resolved constant rather than a runtime read

Stage 2 considered and rejected parsing ``docker-compose.yml`` in the exporter.
The image deliberately ``COPY``s only this package and reads no repo file at
runtime, because the container holding the Docker grant should carry as little
else as possible -- and the manifest is not self-contained anyway: resolving it
needs the Compose file too, since it records only the exceptions.

So the set is resolved at build time and frozen here, and
``TestExpectedServicesMatchTheManifest`` in tests/test_observability_config.py
asserts it equals ``expected_running_services()`` by exact set equality. The
duplication is real; what makes it safe is that it cannot drift silently, which
is the same bargain ``ct-service-down``'s job set and Promtail's job set already
make in that file. Regenerate with:

    python -c "from tests.test_maintenance_running_set import \
expected_running_services as e; print(chr(10).join(sorted(e())))"
"""
from __future__ import annotations

from typing import FrozenSet

EXPECTED_SERVICES: FrozenSet[str] = frozenset({
    "airflow-apiserver",
    "airflow-dag-processor",
    "airflow-scheduler",
    "airflow-triggerer",
    "archiver",
    "caddy",
    "container-health",
    "dashboard",
    "dbt_runner",
    "docker-socket-proxy",
    "flaresolverr",
    "grafana",
    "loki",
    "minio",
    "node-exporter",
    "oauth2-proxy",
    "ops",
    "pack-worker",
    "pgadmin",
    "postgres",
    "postgres-exporter",
    "processing",
    "prometheus",
    "promtail",
    "redis-trawl",
    "scraper",
    "statsd-exporter",
    "trawl",
})

# ## The services that can never read 1, and why that is not drift
#
# Plan 142 Stage 3 makes this metric the resume gate for a host window, and the
# gate is deliberately fail-closed: an expected service reading anything but
# healthy holds the window at `validating`. That is right, and it does not open
# for `oauth2-proxy`, which reads -1 (unconfigured) permanently and by design --
# `quay.io/oauth2-proxy/oauth2-proxy:latest` is distroless, so no `healthcheck:`
# can be expressed against it at all. Left unhandled, one documented hole turns
# a fail-closed gate into a gate that cannot open.
#
# So the gate honours the *documented* exemption and only that: an exempt
# service may read -1, a non-exempt service reading -1 still fails, and an
# exempt service reading 0 -- absent, or a healthcheck that started working and
# then failed -- still fails. The exemption excuses the missing contract, never
# a service that is actually gone.
#
# Resolved here for the same reason `EXPECTED_SERVICES` is: the image COPYs only
# this package and reads no repo file at runtime, and the container holding the
# Docker grant should carry as little else as possible.
# `TestHealthcheckExemptionsMatchTheDenyList` in
# tests/test_observability_config.py asserts this equals the parsed
# `healthcheck-exemptions.txt` by exact set equality, so it cannot drift
# silently. Regenerate with:
#
#     python -c "from tests.test_deploy_script import load_health_exemptions
#                as l; print(chr(10).join(sorted(l())))"
HEALTHCHECK_EXEMPT_SERVICES: FrozenSet[str] = frozenset({
    "airflow-init",
    "dbt",
    "dbt_test",
    "flyway",
    "oauth2-proxy",
    "snapshot-worker",
})
