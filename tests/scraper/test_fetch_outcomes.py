"""What the scraper does about each status cars.com actually returns.

Plan 162 Stage AB / CAR-106.

``tests/test_external_vocabularies.py`` asserts that every status in the
recorded corpus has been *decided about*. This asserts what those decisions
are, per status, so that changing one is a visible edit rather than a
side-effect of touching a mapping.

**Every status here is in the corpus**, and the counts in the ids are the
measured ones — these are not hypotheticals.
"""
from __future__ import annotations

import pytest

from scraper.fetch_outcomes import (
    METRIC_LABELS,
    FetchOutcome,
    classify,
    handled_statuses,
    should_back_off,
    should_enqueue_for_parsing,
)


@pytest.mark.parametrize(
    ("status", "expected"),
    [
        pytest.param(200, FetchOutcome.OK, id="200-the-page"),
        pytest.param(302, FetchOutcome.REDIRECTED, id="302-listing-gone-1172-in-91d"),
        pytest.param(403, FetchOutcome.BLOCKED, id="403-cloudflare-30981-in-91d"),
        pytest.param(500, FetchOutcome.TRANSIENT, id="500-origin-36-in-91d"),
        pytest.param(502, FetchOutcome.TRANSIENT, id="502-gateway-275-in-91d"),
        pytest.param(503, FetchOutcome.TRANSIENT, id="503-overload-1604-in-91d"),
        pytest.param(504, FetchOutcome.TRANSIENT, id="504-timeout-16-in-91d"),
    ],
)
def test_each_observed_status_classifies_the_way_it_was_decided(status, expected):
    assert classify(status) is expected


@pytest.mark.parametrize(
    ("outcome", "backs_off"),
    [
        (FetchOutcome.OK, False),
        (FetchOutcome.BLOCKED, True),
        (FetchOutcome.TRANSIENT, True),
        (FetchOutcome.REDIRECTED, False),
        (FetchOutcome.UNKNOWN, True),
    ],
)
def test_back_off_is_decided_per_outcome(outcome, backs_off):
    """``TRANSIENT`` is the one this stage changed, and it is the whole point.

    A 503 is the origin asking for less traffic. Recovering the adaptive delay
    on one — which is what a bare ``is_403`` boolean did — answers that request
    by sending more.
    """
    assert should_back_off(outcome) is backs_off


@pytest.mark.parametrize(
    ("outcome", "enqueued"),
    [
        (FetchOutcome.OK, True),
        (FetchOutcome.BLOCKED, True),
        (FetchOutcome.TRANSIENT, False),
        (FetchOutcome.REDIRECTED, False),
        (FetchOutcome.UNKNOWN, False),
    ],
)
def test_only_a_page_about_this_listing_reaches_the_parser(outcome, enqueued):
    """``BLOCKED`` enqueues and the other three failures do not.

    A challenge page is a fact about the listing — ``parse_detail_page``
    records it as ``listing_state='blocked'`` and Plan 128 depends on that. A
    502 page and a redirect target are facts about something else, and the
    parser's ``listing_state`` default is ``'active'``.
    """
    assert should_enqueue_for_parsing(outcome) is enqueued


def test_every_outcome_has_a_metric_label():
    """A new outcome with no label would raise at the counter, in production."""
    missing = [outcome for outcome in FetchOutcome if outcome not in METRIC_LABELS]
    assert not missing, f"outcomes with no metric label: {missing}"


def test_the_labels_the_alerts_key_on_are_unchanged():
    """``ok`` and ``403`` are load-bearing outside this repository's tests.

    ``grafana/provisioning/alerting/rules.yml`` keys ``ct-detail-fetch-failing``
    on ``outcome="ok"`` and the block-rate rule on ``outcome="403"``. Widening
    the label set was safe; renaming either of these would silently retire an
    alert, which is the failure mode Plan 128 spent eight hours inside.
    """
    assert METRIC_LABELS[FetchOutcome.OK] == "ok"
    assert METRIC_LABELS[FetchOutcome.BLOCKED] == "403"


def test_the_handled_set_is_not_empty():
    """The corpus rule is a set difference and this is its floor."""
    assert len(handled_statuses()) >= 7, (
        "fewer statuses are handled than the corpus has observed, so "
        "test_every_observed_cars_com_status_is_handled is checking less than "
        "it did"
    )
