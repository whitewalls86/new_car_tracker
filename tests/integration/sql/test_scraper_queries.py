"""
Layer 2 — SQL smoke tests for the scraper service's statements.

Imports the constants from ``scraper.queries`` — the same module the scraper's
processors import — and executes them against Postgres with Flyway's migrations
applied. Nothing here retypes a statement: what runs in this file is the text
that runs in production, which is the only arrangement in which a passing test
means anything.

Two groups. The enqueue statements were inline at their call sites in
``scraper/processors/scrape_detail.py`` and ``scrape_results.py`` until Plan 162
Stage 7, so they could not be imported and nothing executed them. The
blocked-cooldown statements were already ``.sql`` files but had no Layer 2 test
of their own — ``tests/integration/scraper/test_blocked_cooldown.py`` reaches
them through the real code path, which proves the path and not the text.

Every test runs inside the ``cur`` fixture's transaction, which is rolled back,
so the writes here never survive the test that made them.
"""
import uuid

import pytest

from scraper.queries import (
    ENQUEUE_DETAIL_ARTIFACT,
    ENQUEUE_RESULTS_ARTIFACT,
    GET_BLOCKED_COOLDOWN_ATTEMPTS,
    INSERT_BLOCKED_COOLDOWN_EVENT,
    INSERT_DETAIL_ARTIFACT_EVENT,
    INSERT_RESULTS_ARTIFACT_EVENT,
    UPSERT_BLOCKED_COOLDOWN,
)

pytestmark = pytest.mark.integration

FETCHED_AT = "2099-01-01T00:00:00+00:00"


class TestArtifactEnqueueQueries:
    """Plan 97's artifacts_queue enqueue, both artifact types and both halves.

    The scraper writes the ``ops.artifacts_queue`` row and its
    ``staging.artifacts_queue_events`` twin in one transaction, so each test
    here runs the pair in that order and feeds the returned ``artifact_id``
    into the event insert exactly as production does.
    """

    def test_enqueue_detail_artifact_and_event(self, cur):
        cur.execute(
            ENQUEUE_DETAIL_ARTIFACT,
            ("s3://test/detail.html", "test-listing-1", "test-run-1", FETCHED_AT),
        )
        row = cur.fetchone()
        artifact_id = row["artifact_id"]
        assert artifact_id is not None

        cur.execute(
            INSERT_DETAIL_ARTIFACT_EVENT,
            (artifact_id, "s3://test/detail.html", FETCHED_AT,
             "test-listing-1", "test-run-1"),
        )
        assert cur.rowcount == 1

    def test_enqueue_results_artifact_and_event(self, cur):
        cur.execute(
            ENQUEUE_RESULTS_ARTIFACT,
            ("s3://test/results.html", "test-run-1", FETCHED_AT, "test-search-key"),
        )
        row = cur.fetchone()
        artifact_id = row["artifact_id"]
        assert artifact_id is not None

        cur.execute(
            INSERT_RESULTS_ARTIFACT_EVENT,
            (artifact_id, "s3://test/results.html", FETCHED_AT, "test-run-1"),
        )
        assert cur.rowcount == 1

    def test_enqueue_detail_artifact_accepts_a_null_run_id(self, cur):
        # The call site passes `run_id or None`, so a null run_id is a real
        # production input rather than a contrived one. listing_id is not one:
        # it is passed as `str(listing_id)` and is never null here.
        cur.execute(
            ENQUEUE_DETAIL_ARTIFACT,
            ("s3://test/detail.html", "test-listing-2", None, FETCHED_AT),
        )
        assert cur.fetchone()["artifact_id"] is not None


class TestBlockedCooldownQueries:
    """The 403 cooldown trio, executed as the detail fetcher executes it.

    These three ``.sql`` files predate Plan 162 Stage L and were waived for
    having no Layer 2 test. Order matters: the upsert has to land before the
    attempt count can be read, and the event insert records what that count was.
    """

    def test_upsert_then_read_attempts_then_record_event(self, cur):
        # ops.blocked_cooldown.listing_id is uuid, not text (V018).
        listing_id = str(uuid.uuid4())

        cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        assert cur.rowcount == 1

        cur.execute(GET_BLOCKED_COOLDOWN_ATTEMPTS, {"listing_id": listing_id})
        assert cur.fetchone()["num_of_attempts"] == 1

        # Second 403 for the same listing takes the ON CONFLICT branch.
        cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(GET_BLOCKED_COOLDOWN_ATTEMPTS, {"listing_id": listing_id})
        assert cur.fetchone()["num_of_attempts"] == 2

        cur.execute(INSERT_BLOCKED_COOLDOWN_EVENT, {
            "listing_id": listing_id,
            "event_type": "incremented",
            "num_of_attempts": 2,
        })
        assert cur.rowcount == 1

    def test_get_blocked_cooldown_attempts_matching_nothing(self, cur):
        cur.execute(GET_BLOCKED_COOLDOWN_ATTEMPTS, {"listing_id": str(uuid.uuid4())})
        assert cur.fetchone() is None
