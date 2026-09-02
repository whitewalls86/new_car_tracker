"""Plan 138 step 3b: Streamlit is confined to its own base path.

**Why this file exists.** Streamlit serves its SPA shell for every path it does
not recognise, and that shell references its assets *relatively*. Run with no
``--server.baseUrlPath`` it therefore behaves as though it owns the origin root:
its machinery answers there, and its client router *falls back* there. The
2026-09-02 Stage 2 deploy moved ``/`` to the landing page, took that fallback
away, and broke ``/dashboard`` while every other Gate 2 check passed.

The base path removes the ambiguity: the app, its assets, its websocket and its
health endpoint all live under ``/dashboard/``, which is what the Caddyfile
already routes.

**Three ways this can silently regress**, which is why they are asserted rather
than remembered:

- the flag is dropped from the Dockerfile and Streamlit reclaims the root;
- the flag changes but the Compose healthcheck does not, so the container
  reports unhealthy forever while the app is fine;
- the Caddyfile stops routing the prefix the base path is named for, so nothing
  reaches the app at all.
"""
from __future__ import annotations

import json
import re
from pathlib import Path

import yaml

_REPO_ROOT = Path(__file__).parent.parent

DOCKERFILE = _REPO_ROOT / "dashboard" / "Dockerfile"
COMPOSE = _REPO_ROOT / "docker-compose.yml"
CADDYFILE = _REPO_ROOT / "Caddyfile"


def _cmd() -> list[str]:
    """The Dockerfile's CMD, as the exec-form list it is written in."""
    text = DOCKERFILE.read_text(encoding="utf-8")
    found = re.search(r"^CMD\s+(\[.*\])\s*$", text, flags=re.MULTILINE)
    assert found, "dashboard/Dockerfile has no exec-form CMD"
    return json.loads(found.group(1))


def base_url_path() -> str:
    """The value of ``--server.baseUrlPath``, however it is spelled."""
    cmd = _cmd()
    for index, token in enumerate(cmd):
        if token == "--server.baseUrlPath":
            return cmd[index + 1]
        if token.startswith("--server.baseUrlPath="):
            return token.split("=", 1)[1]
    raise AssertionError(
        "dashboard/Dockerfile does not set --server.baseUrlPath. Without it "
        "Streamlit serves its shell for every path and its router falls back to "
        "the origin root -- which Plan 138 Stage 2 gives to the landing page."
    )


def _healthcheck_url() -> str:
    compose = yaml.safe_load(COMPOSE.read_text(encoding="utf-8"))
    test = compose["services"]["dashboard"]["healthcheck"]["test"]
    found = re.search(r"https?://[^']+", " ".join(test))
    assert found, f"no URL in the dashboard healthcheck: {test}"
    return found.group(0)


def test_the_dashboard_declares_a_base_path():
    assert base_url_path() == "dashboard"


def test_the_healthcheck_uses_the_declared_base_path():
    """The pair that breaks silently.

    Streamlit serves the health endpoint under the base path like everything
    else. A healthcheck left at ``/_stcore/health`` 404s forever, Compose reports
    the container unhealthy, and the application is perfectly fine -- so the
    signal points at the wrong thing.
    """
    assert _healthcheck_url().endswith(f"/{base_url_path()}/_stcore/health"), (
        f"the healthcheck hits {_healthcheck_url()}, which is not under the "
        f"base path {base_url_path()!r}"
    )


def test_the_healthcheck_does_not_cross_the_proxy():
    """It must stay on localhost, or it stops being a liveness check."""
    assert _healthcheck_url().startswith("http://localhost:8501/")


def test_caddy_routes_the_prefix_the_base_path_is_named_for():
    """The base path is only reachable if the proxy sends that prefix along.

    ``handle /dashboard*`` passes the full path through -- it is not
    ``handle_path``, which would strip the prefix and leave Streamlit looking for
    a base path that never arrives.
    """
    caddyfile = CADDYFILE.read_text(encoding="utf-8")
    prefix = base_url_path()

    assert f"handle /{prefix}*" in caddyfile, (
        f"the Caddyfile does not route /{prefix}* to the dashboard"
    )
    assert f"handle_path /{prefix}" not in caddyfile, (
        f"handle_path strips /{prefix} before it reaches Streamlit, which is "
        f"the one thing the base path cannot survive"
    )
