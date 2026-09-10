"""Every declared response carries a shape, or the kind of body it is.

Plan 162 Stage AL, gap G33 — the row "no schema" had been standing in for
six different things.

**An undeclared body is a body a test may fabricate freely**, because
``service_response()`` has nothing to build from and ``body_violations()``
nothing to compare against. The cause is exact rather than general: Stage Y
made every route declare the *statuses* it can return, and declaring a code
and declaring that code's body were two obligations of which only the first
was asked for.

**The mechanical form**: a declared non-redirect response either carries a
JSON schema (a shape), or carries a media type without one (a kind —
``text/html`` and ``text/plain`` are how the contract says "a rendered page"
today), or carries nothing and is ledgered. Redirects correctly have no
body and are excluded, exactly 16 of the 237 declarations.

**Not one of the 76 error entries closes by declaring ``ErrorResponse``.**
§Stage AL's family table is the argument: 37 are JSON routes where it is
right, 28 render ``admin/error.html`` where a JSON schema would make the
artifact lie — ``service_response()`` would hand a caller a fabricated
``{"detail": ""}`` the service never sends, this plan's defect rebuilt
inside its remedy — and 11 are a file stream, Prometheus exposition, or
genuinely no body at all. Hence a declared *kind*, not a schema for
everything, and hence a ledger rather than a sweep.

**The 13 bodyless 2xx rows ride in the same ledger.** The recorded
population statement — "76, every one an error response" — scoped to the
error cause; the ``/metrics`` 200s, the recap pages and the archive
download declare no body either, and their drain is the same declaration
work with a different kind.
"""
from __future__ import annotations

import json

from tests.rules.test_testing_contract import REPO_ROOT

CONTRACTS = REPO_ROOT / "contracts"

_VERBS = frozenset({"get", "post", "put", "patch", "delete", "head", "options"})


def undeclared_bodies() -> set[str]:
    """Every non-redirect declared response with neither shape nor kind."""
    found: set[str] = set()
    for file in sorted(CONTRACTS.glob("*.json")):
        spec = json.loads(file.read_text(encoding="utf-8"))
        for route, operations in sorted(spec["paths"].items()):
            for verb, operation in operations.items():
                if verb not in _VERBS or not isinstance(operation, dict):
                    continue
                for code, response in (operation.get("responses") or {}).items():
                    if code.startswith("3"):
                        continue
                    if not (response.get("content") or {}):
                        found.add(f"{file.stem} {verb.upper()} {route} {code}")
    return found


# Keyed on service-verb-route-code, never a line number. Seeded 2026-09-10
# at 89: the 76 error responses the measurement recorded — all of them in
# `ops` — plus 13 bodyless 2xx rows (`/metrics`, the recap pages,
# `/sitemap.xml`, `/auth/check`'s bodyless 200 and the archive download)
# whose declarations are the same missing work under a different kind. An
# entry drains by the route declaring its body: a `$ref` for JSON, a media
# type for a rendered page, a stream, or exposition — and the `none` kind
# for the responses that genuinely carry nothing, which is a declaration
# too.
UNDECLARED_BODY_LEDGER: tuple[str, ...] = (
    "container_health GET /metrics 200",
    "dbt_runner GET /metrics 200",
    "ops GET /admin/searches/ 503",
    "ops GET /admin/searches/{search_key}/edit 503",
    "ops GET /admin/snapshots/adaptive-refresh/latest 401",
    "ops GET /admin/snapshots/adaptive-refresh/latest 403",
    "ops GET /admin/snapshots/adaptive-refresh/latest 404",
    "ops GET /admin/snapshots/adaptive-refresh/latest 503",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id} 400",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id} 401",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id} 403",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id} 404",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id} 409",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id} 503",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download 200",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download 400",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download 401",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download 403",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download 404",
    "ops GET /admin/snapshots/adaptive-refresh/{snapshot_id}/download 503",
    "ops GET /auth/check 200",
    "ops GET /auth/check 403",
    "ops GET /auth/check 503",
    "ops GET /coordination/drain-status 503",
    "ops GET /coordination/release-status 503",
    "ops GET /coordination/status 503",
    "ops GET /metrics 200",
    "ops GET /recaps 200",
    "ops GET /recaps 404",
    "ops GET /recaps/{slug} 200",
    "ops GET /recaps/{slug} 404",
    "ops GET /sitemap.xml 200",
    "ops HEAD /recaps 200",
    "ops HEAD /recaps 404",
    "ops HEAD /recaps/{slug} 200",
    "ops HEAD /recaps/{slug} 404",
    "ops HEAD /sitemap.xml 200",
    "ops POST /admin/access-requests/{req_id}/approve 404",
    "ops POST /admin/access-requests/{req_id}/approve 503",
    "ops POST /admin/access-requests/{req_id}/deny 404",
    "ops POST /admin/access-requests/{req_id}/deny 503",
    "ops POST /admin/deploy/release 409",
    "ops POST /admin/deploy/release 500",
    "ops POST /admin/deploy/release 503",
    "ops POST /admin/deploy/request 409",
    "ops POST /admin/deploy/request 500",
    "ops POST /admin/deploy/request 503",
    "ops POST /admin/searches/ 422",
    "ops POST /admin/searches/ 503",
    "ops POST /admin/searches/{search_key} 404",
    "ops POST /admin/searches/{search_key} 422",
    "ops POST /admin/searches/{search_key} 503",
    "ops POST /admin/searches/{search_key}/delete 404",
    "ops POST /admin/searches/{search_key}/delete 503",
    "ops POST /admin/searches/{search_key}/toggle 404",
    "ops POST /admin/searches/{search_key}/toggle 503",
    "ops POST /admin/users/{user_id}/revoke 404",
    "ops POST /admin/users/{user_id}/revoke 503",
    "ops POST /admin/users/{user_id}/role 400",
    "ops POST /admin/users/{user_id}/role 404",
    "ops POST /admin/users/{user_id}/role 503",
    "ops POST /coordination/authorize 409",
    "ops POST /coordination/authorize 503",
    "ops POST /coordination/begin-drain 409",
    "ops POST /coordination/begin-drain 503",
    "ops POST /coordination/begin-validation 409",
    "ops POST /coordination/begin-validation 503",
    "ops POST /coordination/cancel 409",
    "ops POST /coordination/cancel 503",
    "ops POST /coordination/complete 409",
    "ops POST /coordination/complete 503",
    "ops POST /coordination/host-evidence 409",
    "ops POST /coordination/host-evidence 422",
    "ops POST /coordination/host-evidence 503",
    "ops POST /coordination/request 409",
    "ops POST /coordination/request 422",
    "ops POST /coordination/request 500",
    "ops POST /coordination/request 503",
    "ops POST /deploy/complete 409",
    "ops POST /deploy/complete 500",
    "ops POST /deploy/complete 503",
    "ops POST /deploy/start 409",
    "ops POST /deploy/start 422",
    "ops POST /deploy/start 500",
    "ops POST /deploy/start 503",
    "ops POST /request-access 400",
    "ops POST /request-access 503",
    "processing GET /metrics 200",
    "scraper GET /metrics 200",
)


def test_the_body_reader_still_tells_the_three_apart():
    """The floor. Shape, kind and nothing must all still be visible.

    The corpus can drift under this file — a contracts regeneration is the
    normal way it changes — so the floor asserts the classifier still finds
    all three populations rather than pinning any count: responses with a
    JSON schema, responses with a media type and no schema, and the
    redirect exclusion actually excluding.
    """
    shaped = kinded = redirects = 0
    for file in sorted(CONTRACTS.glob("*.json")):
        spec = json.loads(file.read_text(encoding="utf-8"))
        for operations in spec["paths"].values():
            for verb, operation in operations.items():
                if verb not in _VERBS or not isinstance(operation, dict):
                    continue
                for code, response in (operation.get("responses") or {}).items():
                    if code.startswith("3"):
                        redirects += 1
                        continue
                    content = response.get("content") or {}
                    # `text/html` carries a vestigial `{"type": "string"}`
                    # schema FastAPI writes for a response_class; the *kind*
                    # is the media type, so the split is on that rather than
                    # on schema presence.
                    if (content.get("application/json") or {}).get("schema"):
                        shaped += 1
                    elif content:
                        kinded += 1
    assert shaped and kinded and redirects, (
        f"the classifier found {shaped} shaped, {kinded} kinded and "
        f"{redirects} redirect responses; any of the three reading zero "
        f"means the contract layout moved under this reader, and the rule "
        f"below is then counting wrongly in whichever direction emptied."
    )


def test_every_response_declares_a_shape_or_a_kind():
    """"No schema" stops standing in for six different things.

    Both directions: a new bodyless declaration fails on arrival, and one
    that gains its shape or kind fails until its entry is deleted.
    """
    found = undeclared_bodies()
    ledgered = set(UNDECLARED_BODY_LEDGER)

    unwaived = sorted(found - ledgered)
    assert not unwaived, (
        "these declared responses carry neither a shape nor a kind:\n  "
        + "\n  ".join(unwaived)
        + "\n\nDeclare the body: a model for JSON, a media type for a "
        "rendered page, a stream or exposition — or the `none` kind where "
        "there is genuinely no body. Adding a ledger entry is a decision, "
        "not a convenience."
    )

    stale = sorted(ledgered - found)
    assert not stale, (
        "these ledger entries now declare a body and must be deleted:\n  "
        + "\n  ".join(stale)
    )
