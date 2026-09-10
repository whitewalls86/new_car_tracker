"""What a cars.com HTTP status means, decided once.

Plan 162 Stage AB / CAR-106.

Before this module, four places decided independently what a status meant and
three of them only knew about two statuses:

* ``scraper/metrics.record_detail_fetch`` mapped 200 to ``ok``, 403 to ``403``
  and **everything else to** ``error``, alongside raised exceptions.
* ``scrape_detail._update_detail_delay`` took a boolean, ``is_403``. Every
  other status took the ``else`` arm, which is the *recovery* arm.
* ``scrape_results._update_srp_penalty`` took the same boolean and did the same
  thing.
* ``scrape_detail_fetch`` enqueued the artifact for parsing before it looked at
  the status at all.

**The consequences were measured, not inferred.** Production returned 302, 500,
502, 503 and 504 3,103 times in 91 days. Every one of them recovered the
adaptive delay as though the fetch had succeeded -- so the scraper accelerated
into an origin that was telling it to slow down -- and every one of them was
enqueued for parsing, where ``processing.parse_detail_page`` reads
``listing_state = (unlisted or {}).get("listing_state") or "active"`` and
publishes a cars.com *error page* as an active listing. That observation has
``source='detail'``, and ``mart_vehicle_snapshot`` trusts a detail observation
over every other signal.

**The rule this module exists to serve.** Stage AB's corpus records which
statuses cars.com has actually returned;
``test_every_observed_cars_com_status_is_handled`` asserts every one of them
classifies to a named outcome here. A status production starts returning that
nobody has decided about fails that test rather than silently taking an
``else`` branch. ``UNKNOWN`` is the catchall for statuses the corpus has *not*
seen, and it is deliberately handled the conservative way -- backed off and not
enqueued -- because the one thing worse than not knowing is guessing "active".

**This module imports nothing from the rest of the scraper**, so ``metrics``
and both processors can read it without an import cycle.
"""
from __future__ import annotations

from enum import StrEnum


class FetchOutcome(StrEnum):
    """What the scraper should do about a status, rather than what it is.

    Named for the response rather than the number because the number is
    cars.com's and the response is ours: two statuses that call for the same
    handling belong to the same member, and a member exists only where the
    handling actually differs.
    """

    OK = "ok"
    """200. Parse it."""

    BLOCKED = "blocked"
    """403. Cloudflare refused us: cool the listing down, back off, and drop
    the cached credentials. The page body is still worth parsing, because
    ``parse_detail_page`` classifies a challenge page as ``blocked`` and Plan
    128 depends on that."""

    TRANSIENT = "transient"
    """5xx. The origin is failing or overloaded and the listing is fine. Back
    off, and do not publish anything about the listing -- an error page is not
    evidence that a car is for sale."""

    REDIRECTED = "redirected"
    """3xx. The listing moved or went away and the response body describes
    somewhere else. Not a block and not an origin failure, so no cooldown and
    no back-off, but emphatically not an observation of this listing."""

    UNKNOWN = "unknown"
    """Anything the corpus has never seen. Handled as conservatively as
    ``TRANSIENT``: a status nobody has reasoned about must not reach the
    parser, where the default is ``active``."""


#: Status -> outcome, for every status the recorded corpus contains plus the
#: ones the code has always handled. Written as explicit members rather than
#: as ranges: ``500 <= status < 600`` would silently absorb a 599 nobody has
#: seen, and absorbing-without-deciding is the defect this module closes.
#:
#: 3xx reaching a caller at all is worth a word, and where it comes from was
#: measured rather than reasoned about. ``_fetch_url`` requests with
#: ``allow_redirects=True``, so a redirect the session follows never surfaces.
#: Every 302 comes from the **FlareSolverr bootstrap**, which reports the
#: status its browser landed on -- established by exact equality over 91 days
#: of production logs: 1,172 detail fetches reported 302 and 1,172 bootstraps
#: reported 302. Not a burst either: 36-52 a day, every day, about 6% of
#: bootstraps.
#:
#: **That path is why a 302 is the most dangerous status here rather than the
#: dullest.** The credential cache expires every 25 minutes and the next detail
#: fetch bootstraps against *that listing's* URL; when cars.com redirects a
#: listing that has been removed, ``get_cf_credentials`` hands back the
#: redirect target's body and ``_fetch_url`` returns it as the artifact for the
#: listing -- with ``final_url`` still reported as the URL that was asked for.
#: Parsed, that body has no unlisted marker, so it published a removed listing
#: as ``active``.
_HANDLING: dict[int, FetchOutcome] = {
    200: FetchOutcome.OK,
    302: FetchOutcome.REDIRECTED,
    403: FetchOutcome.BLOCKED,
    500: FetchOutcome.TRANSIENT,
    502: FetchOutcome.TRANSIENT,
    503: FetchOutcome.TRANSIENT,
    504: FetchOutcome.TRANSIENT,
}


def classify(status: int | None) -> FetchOutcome:
    """The outcome for *status*, or ``UNKNOWN`` for one nobody has decided about.

    ``None`` means the fetch raised before there was a response at all. That is
    a transport failure rather than a status, and it classifies ``UNKNOWN`` for
    the same reason an unrecognised status does: both mean "we have no page",
    and both must be handled as though we have no page.
    """
    if status is None:
        return FetchOutcome.UNKNOWN
    return _HANDLING.get(status, FetchOutcome.UNKNOWN)


def handled_statuses() -> frozenset[int]:
    """Every status this module has an opinion about.

    The test reads this rather than ``_HANDLING`` so that the mapping stays
    private and the population stays derived from it.
    """
    return frozenset(_HANDLING)


def should_back_off(outcome: FetchOutcome) -> bool:
    """Whether this outcome means "ask for less, more slowly".

    ``BLOCKED`` is the case the adaptive delay was built for. ``TRANSIENT`` is
    the case it was missing: a 503 is the origin saying *slow down* in the
    plainest terms available to it, and recovering the delay on one is the
    exact opposite of what it asked for. ``UNKNOWN`` backs off because a status
    nobody has reasoned about is not evidence that things are going well.

    ``REDIRECTED`` does not: the origin answered promptly and correctly, it
    simply answered about somewhere else.
    """
    return outcome in (FetchOutcome.BLOCKED, FetchOutcome.TRANSIENT, FetchOutcome.UNKNOWN)


def should_enqueue_for_parsing(outcome: FetchOutcome) -> bool:
    """Whether the body is worth handing to the parser.

    ``OK`` obviously. ``BLOCKED`` because a Cloudflare challenge page is a
    *fact about the listing's availability* that ``parse_detail_page`` already
    detects and records as ``listing_state='blocked'`` -- Plan 128 exists
    because those pages were being counted as successful scrapes, and refusing
    to enqueue them now would lose the signal it built.

    Everything else, no. The parser's default is ``active``, it cannot see the
    status because ``ops.artifacts_queue`` does not carry one, and a cars.com
    502 page parses to an active listing with no price. The artifact is still
    written to MinIO -- it is the only copy of what the site actually said --
    it just does not become an observation.
    """
    return outcome in (FetchOutcome.OK, FetchOutcome.BLOCKED)


#: How an outcome projects onto ``cartracker_detail_fetch_total``'s labels.
#:
#: **The projection exists so the metric's label set can be wider than it was
#: without breaking what reads it.** ``ok`` and ``403`` keep exactly their old
#: meanings, because ``ct-detail-fetch-failing`` keys on ``outcome="ok"`` and
#: the block-rate alert keys on ``outcome="403"``. What changes is that the old
#: catch-all ``error`` bucket splits: that bucket's own alert description told
#: an operator that *"mixed `error` points at the fetch path or the site"* and
#: then gave them no way to tell which.
METRIC_LABELS: dict[FetchOutcome, str] = {
    FetchOutcome.OK: "ok",
    FetchOutcome.BLOCKED: "403",
    FetchOutcome.TRANSIENT: "transient",
    FetchOutcome.REDIRECTED: "redirected",
    FetchOutcome.UNKNOWN: "error",
}
