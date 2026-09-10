"""Scraper-owned outcome counters (Plan 136 Stage 2).

The 2026-08-14 outage ran for eight hours because every signal this system had
was downstream of the failure. `ct-scrape-volume-drop` reads a dbt mart, so it
sits behind scrape -> processing -> silver flush -> dbt build -> DuckDB and
cannot answer "is work succeeding *now*". These two counters can: the scraper
knows every outcome at the moment it happens, and Prometheus scrapes it
directly.

Two counters, because the solver fails in two shapes with opposite signatures
and neither counter sees both.

`cartracker_solver_requests_total` is the fast one, for the *refusing* shape --
the 2026-08-14 outage. `/v1` returned HTTP 500, `get_cf_credentials` raised
before ever assigning `_cf_credentials_expires_at`, and so every single fetch
re-bootstrapped. Solver volume goes *up* during that failure, which means six
failed attempts accumulate inside one batch and `ct-solver-not-solving` trips in
minutes. Note the healthy baseline is the opposite: credentials cache for
`_CF_SESSION_TTL` (25 minutes), so a working system bootstraps roughly 2.4 times
an hour.

`cartracker_detail_fetch_total` is the shape-independent one, for the *lying*
shape -- the solver returns `status: ok` with real cf_clearance cookies and an
interstitial behind them. Credentials cache normally, solver volume never moves,
and only the 403s on detail fetches show it. `scrape_detail_pages` runs `*/15`
with a batch of 100, so `ct-detail-fetch-failing`'s 20m window always spans a
whole cycle.

The `challenge` outcome is what makes the second shape nameable rather than
merely absent, and it is why `_solver_outcome` classifies the returned page
instead of trusting the solver's own `status` field. `trawl`'s healthcheck
reported `status:ok` for all eight hours; so did its API.

**Every label child is pre-initialized below.** A `Counter` with labels
publishes nothing until its first `inc()`, so without this the `outcome="ok"`
series would not exist until the first success — and an alert asking "is the ok
rate zero?" cannot be answered by a series that is absent. It would read as
NoData on a cold start and flap into existence on first traffic. Six series
exist from import, all at 0.
"""
from __future__ import annotations

from prometheus_client import Counter

from scraper.fetch_outcomes import METRIC_LABELS, classify

SOLVER_OUTCOMES = ("ok", "challenge", "error")
# Derived from the one classification rather than restated, and **wider than
# it was**: Plan 162 Stage AB split the old catch-all `error`. `ok` and `403`
# keep exactly their previous meanings because `ct-detail-fetch-failing` keys
# on `outcome="ok"` and the block-rate rule keys on `outcome="403"`; what
# was one `error` bucket is now `transient` (a 5xx from the origin),
# `redirected` (a 3xx, which for cars.com means the listing has gone) and a
# residual `error` for a raised exception or a status nobody has classified.
# That bucket's own alert told an operator "mixed `error` points at the
# fetch path or the site" and gave them no way to tell which.
DETAIL_FETCH_OUTCOMES = tuple(dict.fromkeys(METRIC_LABELS.values()))

solver_requests_total = Counter(
    "cartracker_solver_requests_total",
    "Cloudflare solver bootstrap attempts by outcome. "
    "ok=credentials returned for a real page, "
    "challenge=solver claimed success but returned an interstitial, "
    "error=solver refused, errored, or was unreachable.",
    ["outcome"],
)

detail_fetch_total = Counter(
    "cartracker_detail_fetch_total",
    "Detail-page fetch attempts by outcome. "
    "ok=HTTP 200, 403=blocked, error=any other status or a raised exception.",
    ["outcome"],
)

for _outcome in SOLVER_OUTCOMES:
    solver_requests_total.labels(outcome=_outcome)
for _outcome in DETAIL_FETCH_OUTCOMES:
    detail_fetch_total.labels(outcome=_outcome)


def record_solver_outcome(outcome: str) -> None:
    """Count one solver bootstrap. Never raises — telemetry must not break a scrape."""
    try:
        solver_requests_total.labels(outcome=outcome).inc()
    except Exception:  # pragma: no cover - defensive
        pass


def record_detail_fetch(status: int | None, errored: bool = False) -> None:
    """Count one detail fetch, mapping HTTP status to an outcome label.

    `errored=True` covers a raised exception, where there is no status at all.

    The mapping is `scraper.fetch_outcomes` and not a second copy of it. This
    function used to hold its own two-status opinion -- 200, 403, and an
    `else` for everything -- while `_update_detail_delay` held a different
    one, and the 3,103 responses that fell through both are what Plan 162
    Stage AB found.
    """
    outcome = METRIC_LABELS[classify(None if errored else status)]
    try:
        detail_fetch_total.labels(outcome=outcome).inc()
    except Exception:  # pragma: no cover - defensive
        pass
