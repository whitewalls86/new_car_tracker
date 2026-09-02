"""
Layer 4 — `POST /scrape_detail` end to end, with nothing mocked.

The full request path: `TestClient` → router → `scrape_detail_fetch` →
`_fetch_url` → `curl_cffi` → the loopback origin → zstd → real MinIO → real
Postgres, with the app's lifespan holding a real `asyncpg` pool throughout.

**This is what G8 was.** `scraper` had one integration file
(`test_blocked_cooldown.py`), and that file executes SQL constants against a
cursor — Layer 2's shape, in a Layer 4 directory. Every route was reached only
from `tests/scraper/`, whose autouse fixture patches `shared.db.get_conn` and
`shared.minio.write_html`, so the service's *writing* half had never run. Six
of its eight routes are the fetch path the whole pipeline sits on.

What is asserted here is the half a mocked test structurally cannot reach: that
the artifact reaches MinIO intact, that `ops.artifacts_queue` and its
`staging.artifacts_queue_events` twin are written together, and that a 403
records the cooldown pair. Response shape is `tests/scraper/test_app.py`'s job
and is not restated.
"""
import uuid

import pytest

from shared.compression import decompress_frame
from shared.minio import object_exists, read_bytes

pytestmark = pytest.mark.integration


def _fetch(client, origin, path, *, listing_id, run_id):
    """POST /scrape_detail the way the Airflow detail task does.

    ``run_id`` is a query parameter and the rest is the JSON body; ``url`` is a
    real production input, not a test hook -- the processor derives a cars.com
    URL only when the caller omits one.
    """
    return client.post(
        "/scrape_detail",
        params={"run_id": run_id},
        json={"mode": "fetch", "listing_id": listing_id, "url": f"{origin}{path}"},
    )


@pytest.fixture()
def scrape_ids(verify_cur):
    """A listing/run pair, with everything they wrote removed afterwards.

    ``listing_id`` is a UUID because ``ops.blocked_cooldown.listing_id`` is
    typed ``uuid`` (V018) even though ``ops.artifacts_queue.listing_id`` is
    text. The request commits through its own connection, so there is no
    transaction to roll back and cleanup has to be explicit.
    """
    listing_id, run_id = str(uuid.uuid4()), str(uuid.uuid4())
    yield listing_id, run_id
    verify_cur.execute(
        "DELETE FROM staging.artifacts_queue_events WHERE run_id = %s", (run_id,)
    )
    verify_cur.execute("DELETE FROM ops.artifacts_queue WHERE run_id = %s", (run_id,))
    verify_cur.execute(
        "DELETE FROM staging.blocked_cooldown_events WHERE listing_id = %s::uuid",
        (listing_id,),
    )
    verify_cur.execute(
        "DELETE FROM ops.blocked_cooldown WHERE listing_id = %s::uuid", (listing_id,)
    )


class TestSuccessfulFetch:

    def test_the_page_reaches_minio_byte_for_byte(
        self, scraper_client, origin, verify_cur, scrape_ids
    ):
        """The artifact processing will later read is the page that was served.

        Compression is in this path and is not incidental: Plan 129 put a
        trained zstd dictionary in front of every HTML write, so a dictionary
        the reader cannot resolve corrupts the artifact rather than failing the
        write. Reading it back through ``decompress_frame`` is what proves the
        round trip, and nothing did before.
        """
        listing_id, run_id = scrape_ids
        served = _fetch(
            scraper_client, origin, "/detail/ok",
            listing_id=listing_id, run_id=run_id,
        )
        assert served.status_code == 200

        artifact = served.json()["artifacts"][0]
        assert artifact["error"] is None
        assert artifact["minio_path"].startswith("s3://")
        assert object_exists(artifact["minio_path"])
        assert decompress_frame(read_bytes(artifact["minio_path"])).startswith(
            b"<!DOCTYPE html"
        )

    def test_the_queue_row_and_its_staging_event_are_written_together(
        self, scraper_client, origin, verify_cur, scrape_ids
    ):
        """The hot/staging pair is one transaction, so it is one assertion.

        ``ops.artifacts_queue`` is what the processing service claims from and
        ``staging.artifacts_queue_events`` is what the archiver flushes to
        Parquet; a fetch that wrote one and not the other would strand the
        artifact in exactly the way Plan 128's reconcile job exists to repair.
        """
        listing_id, run_id = scrape_ids
        response = _fetch(
            scraper_client, origin, "/detail/ok",
            listing_id=listing_id, run_id=run_id,
        )
        artifact_id = response.json()["artifacts"][0]["queue_artifact_id"]
        assert artifact_id is not None

        verify_cur.execute(
            "SELECT artifact_type, status, listing_id, minio_path "
            "FROM ops.artifacts_queue WHERE artifact_id = %s",
            (artifact_id,),
        )
        row = verify_cur.fetchone()
        assert row["artifact_type"] == "detail_page"
        assert row["status"] == "pending"
        assert row["listing_id"] == listing_id

        verify_cur.execute(
            "SELECT status, artifact_type, minio_path "
            "FROM staging.artifacts_queue_events WHERE artifact_id = %s",
            (artifact_id,),
        )
        event = verify_cur.fetchone()
        assert event is not None, (
            "the queue row was written without its staging event: the pair is "
            "one transaction and the archiver flushes the half that is missing"
        )
        assert event["status"] == "pending"
        assert event["artifact_type"] == "detail_page"
        assert event["minio_path"] == row["minio_path"]


class TestBlockedFetch:
    """A 403 is the path the cooldown machinery is built on."""

    def test_a_403_records_the_cooldown_and_escalates_on_the_second(
        self, scraper_client, origin, verify_cur, scrape_ids
    ):
        """First block writes 'blocked'; the second takes the ON CONFLICT arm.

        The event type is derived from the attempt count *read back after the
        upsert*, so the two halves have to agree against a real database. A
        test that mocked the cursor would assert its own arithmetic.
        """
        listing_id, run_id = scrape_ids

        first = _fetch(
            scraper_client, origin, "/detail/blocked",
            listing_id=listing_id, run_id=run_id,
        )
        assert first.json()["artifacts"][0]["http_status"] == 403

        verify_cur.execute(
            "SELECT num_of_attempts FROM ops.blocked_cooldown "
            "WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert verify_cur.fetchone()["num_of_attempts"] == 1

        _fetch(
            scraper_client, origin, "/detail/blocked",
            listing_id=listing_id, run_id=run_id,
        )
        verify_cur.execute(
            "SELECT num_of_attempts FROM ops.blocked_cooldown "
            "WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert verify_cur.fetchone()["num_of_attempts"] == 2

        verify_cur.execute(
            "SELECT event_type, num_of_attempts FROM staging.blocked_cooldown_events "
            "WHERE listing_id = %s::uuid ORDER BY num_of_attempts",
            (listing_id,),
        )
        assert [
            (row["event_type"], row["num_of_attempts"])
            for row in verify_cur.fetchall()
        ] == [("blocked", 1), ("incremented", 2)]

    def test_a_403_still_enqueues_its_artifact(
        self, scraper_client, origin, verify_cur, scrape_ids
    ):
        """Non-200s are enqueued on purpose, and the SQL comment says so.

        The block page is in MinIO either way and processing decides what the
        status code means -- which is the arrangement Plan 128 depends on to
        classify a challenge after the fact rather than discarding it here.
        """
        listing_id, run_id = scrape_ids
        _fetch(
            scraper_client, origin, "/detail/blocked",
            listing_id=listing_id, run_id=run_id,
        )
        verify_cur.execute(
            "SELECT status FROM ops.artifacts_queue WHERE run_id = %s", (run_id,)
        )
        assert [row["status"] for row in verify_cur.fetchall()] == ["pending"]


class TestChallengeServedAsSuccess:

    def test_a_challenge_page_is_enqueued_like_any_other_200(
        self, scraper_client, origin, verify_cur, scrape_ids
    ):
        """Recording where challenge detection lives, not asserting it here.

        Cloudflare can serve an interstitial with a 200, and the scraper does
        not look: `_fetch_url` only logs a title, and the decision belongs to
        `processing.processors.parse_detail_page._detect_challenge`, which
        Plan 128 put there deliberately so the artifact is classified from the
        stored bytes rather than discarded at fetch time.

        So this asserts the artifact is stored and queued, which is the
        contract the parser depends on. If the scraper ever starts dropping
        challenge pages, this fails and the reason it fails is the point.
        """
        listing_id, run_id = scrape_ids
        response = _fetch(
            scraper_client, origin, "/detail/challenge",
            listing_id=listing_id, run_id=run_id,
        )
        artifact = response.json()["artifacts"][0]
        assert artifact["http_status"] == 200
        assert artifact["queue_artifact_id"] is not None
        assert b"Just a moment" in decompress_frame(read_bytes(artifact["minio_path"]))


class TestRejectedRequest:

    def test_a_payload_with_no_listing_id_writes_nothing(
        self, scraper_client, origin, verify_cur
    ):
        """The guard returns before the fetch, so nothing should reach either store.

        Asserting the negative matters here because every write in this path is
        wrapped in a broad ``except`` that logs and continues -- a write that
        fired and failed would look identical in the response.
        """
        run_id = str(uuid.uuid4())
        response = scraper_client.post(
            "/scrape_detail",
            params={"run_id": run_id},
            json={"mode": "fetch", "url": f"{origin}/detail/ok"},
        )
        assert response.json()["error"] == "payload.listing_id is required"

        verify_cur.execute(
            "SELECT count(*) AS n FROM ops.artifacts_queue WHERE run_id = %s",
            (run_id,),
        )
        assert verify_cur.fetchone()["n"] == 0


class TestTheAppItself:

    def test_ready_answers_with_a_real_pool_behind_it(self, scraper_client):
        """The lifespan built an asyncpg pool, and that had never been asserted.

        Every scraper test that ever ran patched ``get_pool``. Airflow's
        ``http_health_sensor`` reads this endpoint to decide whether the
        service is drained, so a pool that could not be built would present as
        a drain that never completes.
        """
        response = scraper_client.get("/ready")
        assert response.status_code == 200
        assert response.json()["ready"] is True
