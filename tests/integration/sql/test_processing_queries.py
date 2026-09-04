"""
Layer 2 — SQL smoke tests for the processing service.

Tests every SQL statement issued by processing/processor.py against a real DB
with Flyway migrations applied. Does not invoke MinIO or the parsers — those
are tested elsewhere. Goal: catch schema breakage before it hits production.
"""
import uuid
from datetime import datetime, timezone

import pytest

from processing.queries import (
    BATCH_LOOKUP_VIN_TO_LISTING,
    CLAIM_ARTIFACT,
    CLAIM_ARTIFACTS,
    CLEAR_BLOCKED_COOLDOWN,
    DELETE_PRICE_OBSERVATION,
    DELETE_PRICE_OBSERVATION_BY_VIN,
    DELETE_PRICE_OBSERVATIONS_FOR_MISSING_LISTINGS,
    GET_TRACKED_MODELS,
    INSERT_ARTIFACT_EVENT,
    INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT,
    INSERT_DETAIL_CLAIM_EVENT,
    INSERT_PRICE_OBSERVATION_EVENT,
    INSERT_SILVER_OBSERVATIONS,
    INSERT_TRACKED_MODEL_EVENT,
    INSERT_VIN_TO_LISTING_EVENT,
    LOOKUP_VIN_COLLISION,
    MARK_ARTIFACT_STATUS,
    RELEASE_DETAIL_CLAIMS,
    UPSERT_PRICE_OBSERVATION,
    UPSERT_TRACKED_MODEL,
    UPSERT_VIN_TO_LISTING,
)

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers / fixtures
# ---------------------------------------------------------------------------

def _insert_artifact(cur, artifact_type="results_page", status="pending") -> int:
    minio_path = (
        f"s3://bronze/html/year=2026/month=4"
        f"/artifact_type={artifact_type}/{uuid.uuid4()}.html.zst"
    )
    cur.execute(
        """
        INSERT INTO ops.artifacts_queue
            (minio_path, artifact_type, fetched_at, status)
        VALUES (%s, %s, now(), %s)
        RETURNING artifact_id, minio_path, artifact_type, listing_id, run_id, fetched_at
        """,
        (minio_path, artifact_type, status),
    )
    return cur.fetchone()


def _random_listing_id() -> str:
    return str(uuid.uuid4())


# ---------------------------------------------------------------------------
# Schema: V019 — ops.price_observations
# ---------------------------------------------------------------------------

class TestPriceObservationsSchema:

    def test_columns_exist(self, cur):
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'price_observations'
            ORDER BY ordinal_position
        """)
        cols = {r["column_name"]: r["data_type"] for r in cur.fetchall()}
        assert "listing_id" in cols
        assert "vin" in cols
        assert "price" in cols
        assert "customer_id" in cols
        assert "last_seen_at" in cols
        assert "last_artifact_id" in cols

    def test_vin_is_text_not_uuid(self, cur):
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'price_observations'
              AND column_name = 'vin'
        """)
        assert cur.fetchone()["data_type"] == "text"

    def test_listing_id_is_uuid(self, cur):
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'price_observations'
              AND column_name = 'listing_id'
        """)
        assert cur.fetchone()["data_type"] == "uuid"


# ---------------------------------------------------------------------------
# Schema: V019 — ops.vin_to_listing
# ---------------------------------------------------------------------------

class TestVinToListingSchema:

    def test_columns_exist(self, cur):
        cur.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'vin_to_listing'
            ORDER BY ordinal_position
        """)
        cols = {r["column_name"]: r["data_type"] for r in cur.fetchall()}
        assert "vin" in cols
        assert "listing_id" in cols
        assert "mapped_at" in cols
        assert "artifact_id" in cols

    def test_vin_is_text_not_uuid(self, cur):
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'ops' AND table_name = 'vin_to_listing'
              AND column_name = 'vin'
        """)
        assert cur.fetchone()["data_type"] == "text"


# ---------------------------------------------------------------------------
# Schema: V020 — staging.detail_scrape_claim_events
# ---------------------------------------------------------------------------

class TestDetailScrapeClaimEventsSchema:

    def test_run_id_is_text_not_uuid(self, cur):
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'staging'
              AND table_name = 'detail_scrape_claim_events'
              AND column_name = 'run_id'
        """)
        assert cur.fetchone()["data_type"] == "text"

    def test_vin_is_text_not_uuid(self, cur):
        cur.execute("""
            SELECT data_type FROM information_schema.columns
            WHERE table_schema = 'staging'
              AND table_name = 'detail_scrape_claim_events'
              AND column_name = 'vin'
        """)
        assert cur.fetchone()["data_type"] == "text"


# ---------------------------------------------------------------------------
# claim_batch SQL
# ---------------------------------------------------------------------------

class TestClaimBatchSQL:

    def test_claims_pending_artifacts(self, cur):
        artifact = _insert_artifact(cur, status="pending")
        artifact_id = artifact["artifact_id"]

        cur.execute(
            """
            UPDATE ops.artifacts_queue SET status = 'processing'
            WHERE artifact_id IN (
                SELECT artifact_id FROM ops.artifacts_queue
                WHERE status IN ('pending', 'retry')
                ORDER BY artifact_id LIMIT 10
                FOR UPDATE SKIP LOCKED
            )
            RETURNING artifact_id
            """,
        )
        claimed_ids = [r["artifact_id"] for r in cur.fetchall()]
        assert artifact_id in claimed_ids

    def test_claims_retry_artifacts(self, cur):
        artifact = _insert_artifact(cur, status="retry")
        artifact_id = artifact["artifact_id"]

        cur.execute(
            """
            UPDATE ops.artifacts_queue SET status = 'processing'
            WHERE artifact_id IN (
                SELECT artifact_id FROM ops.artifacts_queue
                WHERE status IN ('pending', 'retry')
                ORDER BY artifact_id LIMIT 10
                FOR UPDATE SKIP LOCKED
            )
            RETURNING artifact_id
            """,
        )
        claimed_ids = [r["artifact_id"] for r in cur.fetchall()]
        assert artifact_id in claimed_ids

    def test_skips_complete_artifacts(self, cur):
        artifact = _insert_artifact(cur, status="complete")
        artifact_id = artifact["artifact_id"]

        cur.execute(
            """
            UPDATE ops.artifacts_queue SET status = 'processing'
            WHERE artifact_id IN (
                SELECT artifact_id FROM ops.artifacts_queue
                WHERE status IN ('pending', 'retry')
                ORDER BY artifact_id LIMIT 10
                FOR UPDATE SKIP LOCKED
            )
            RETURNING artifact_id
            """,
        )
        claimed_ids = [r["artifact_id"] for r in cur.fetchall()]
        assert artifact_id not in claimed_ids

    def test_artifact_type_filter(self, cur):
        results_artifact = _insert_artifact(cur, artifact_type="results_page")
        detail_artifact = _insert_artifact(cur, artifact_type="detail_page")

        cur.execute(
            """
            UPDATE ops.artifacts_queue SET status = 'processing'
            WHERE artifact_id IN (
                SELECT artifact_id FROM ops.artifacts_queue
                WHERE status IN ('pending', 'retry')
                  AND artifact_type = %s
                ORDER BY artifact_id LIMIT 10
                FOR UPDATE SKIP LOCKED
            )
            RETURNING artifact_id
            """,
            ("results_page",),
        )
        claimed_ids = [r["artifact_id"] for r in cur.fetchall()]
        assert results_artifact["artifact_id"] in claimed_ids
        assert detail_artifact["artifact_id"] not in claimed_ids

    def test_processing_event_written(self, cur):
        artifact = _insert_artifact(cur)
        artifact_id = artifact["artifact_id"]

        cur.execute(
            """
            INSERT INTO staging.artifacts_queue_events
                (artifact_id, status, minio_path, artifact_type, fetched_at, listing_id, run_id)
            VALUES (%s, 'processing', %s, %s, %s, %s, %s)
            """,
            (
                artifact_id, artifact["minio_path"], artifact["artifact_type"],
                artifact["fetched_at"], artifact["listing_id"], artifact["run_id"],
            ),
        )
        cur.execute(
            "SELECT status FROM staging.artifacts_queue_events WHERE artifact_id = %s",
            (artifact_id,),
        )
        assert cur.fetchone()["status"] == "processing"


# ---------------------------------------------------------------------------
# _set_status SQL
# ---------------------------------------------------------------------------

class TestSetStatusSQL:

    def test_set_complete(self, cur):
        artifact = _insert_artifact(cur)
        artifact_id = artifact["artifact_id"]

        cur.execute(
            "UPDATE ops.artifacts_queue SET status = 'complete' WHERE artifact_id = %s",
            (artifact_id,),
        )
        cur.execute(
            "SELECT status FROM ops.artifacts_queue WHERE artifact_id = %s",
            (artifact_id,),
        )
        assert cur.fetchone()["status"] == "complete"

    def test_set_retry(self, cur):
        artifact = _insert_artifact(cur)
        cur.execute(
            "UPDATE ops.artifacts_queue SET status = 'retry' WHERE artifact_id = %s",
            (artifact["artifact_id"],),
        )
        cur.execute(
            "SELECT status FROM ops.artifacts_queue WHERE artifact_id = %s",
            (artifact["artifact_id"],),
        )
        assert cur.fetchone()["status"] == "retry"

    def test_status_event_written(self, cur):
        artifact = _insert_artifact(cur)
        artifact_id = artifact["artifact_id"]

        for status in ("processing", "complete"):
            cur.execute(
                """
                INSERT INTO staging.artifacts_queue_events
                    (artifact_id, status, minio_path, artifact_type, fetched_at, listing_id, run_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    artifact_id, status, artifact["minio_path"],
                    artifact["artifact_type"], artifact["fetched_at"],
                    artifact["listing_id"], artifact["run_id"],
                ),
            )

        cur.execute(
            "SELECT status FROM staging.artifacts_queue_events"
            " WHERE artifact_id = %s ORDER BY event_id",
            (artifact_id,),
        )
        statuses = [r["status"] for r in cur.fetchall()]
        assert statuses == ["processing", "complete"]


# ---------------------------------------------------------------------------
# price_observations upsert (results_page and detail_page paths)
# ---------------------------------------------------------------------------

class TestPriceObservationsUpsert:

    def test_insert_new_observation(self, cur):
        artifact = _insert_artifact(cur)
        listing_id = _random_listing_id()

        cur.execute(
            """
            INSERT INTO ops.price_observations
                (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
            """,
            (listing_id, "1HGCM82633A004352", 35000, "Honda", "Accord",
             "cust-001", artifact["artifact_id"]),
        )
        cur.execute(
            """
                SELECT vin, price, customer_id 
                FROM ops.price_observations WHERE listing_id = %s::uuid
            """,
            (listing_id,),
        )
        row = cur.fetchone()
        assert row["vin"] == "1HGCM82633A004352"
        assert row["price"] == 35000
        assert row["customer_id"] == "cust-001"

    def test_upsert_updates_existing(self, cur):
        artifact = _insert_artifact(cur)
        listing_id = _random_listing_id()

        for price in (35000, 33000):
            cur.execute(
                """
                INSERT INTO ops.price_observations
                    (listing_id, vin, price, make, model, 
                     customer_id, last_seen_at, last_artifact_id)
                VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
                ON CONFLICT (listing_id) DO UPDATE SET
                    price            = EXCLUDED.price,
                    customer_id      = COALESCE(EXCLUDED.customer_id, 
                                                ops.price_observations.customer_id),
                    last_seen_at     = EXCLUDED.last_seen_at,
                    last_artifact_id = EXCLUDED.last_artifact_id
                """,
                (listing_id, "1HGCM82633A004352", price, "Honda", "Accord",
                 "cust-001", artifact["artifact_id"]),
            )

        cur.execute(
            "SELECT price FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["price"] == 33000

    def test_customer_id_not_downgraded_by_srp(self, cur):
        """customer_id set by detail write is preserved when SRP writes NULL."""
        artifact = _insert_artifact(cur)
        listing_id = _random_listing_id()

        # Detail write sets customer_id
        cur.execute(
            """
            INSERT INTO ops.price_observations
                (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
            """,
            (listing_id, "1HGCM82633A004352", 35000, "Honda", "Accord",
             "cust-detail", artifact["artifact_id"]),
        )
        # SRP write with NULL customer_id
        cur.execute(
            """
            INSERT INTO ops.price_observations
                (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
            ON CONFLICT (listing_id) DO UPDATE SET
                price            = EXCLUDED.price,
                customer_id      = COALESCE(EXCLUDED.customer_id, 
                                            ops.price_observations.customer_id),
                last_seen_at     = EXCLUDED.last_seen_at,
                last_artifact_id = EXCLUDED.last_artifact_id
            """,
            (listing_id, "1HGCM82633A004352", 34000, "Honda", "Accord",
             None, artifact["artifact_id"]),
        )

        cur.execute(
            "SELECT price, customer_id FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        row = cur.fetchone()
        assert row["price"] == 34000          # price updated by SRP
        assert row["customer_id"] == "cust-detail"  # customer_id preserved

    def test_vin_accepts_text_not_uuid(self, cur):
        artifact = _insert_artifact(cur)
        listing_id = _random_listing_id()
        vin = "1FTFW1ET5DFC10312"  # real-format VIN, not a UUID

        cur.execute(
            """
            INSERT INTO ops.price_observations
                (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
            """,
            (listing_id, vin, 42000, "Ford", "F-150", None, artifact["artifact_id"]),
        )
        cur.execute(
            "SELECT vin FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["vin"] == vin

    def test_delete_unlisted(self, cur):
        artifact = _insert_artifact(cur)
        listing_id = _random_listing_id()

        cur.execute(
            """
            INSERT INTO ops.price_observations
                (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
            """,
            (listing_id, None, 30000, "Toyota", "RAV4", None, artifact["artifact_id"]),
        )
        cur.execute(
            "DELETE FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.price_observations WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 0


# ---------------------------------------------------------------------------
# vin_to_listing upsert
# ---------------------------------------------------------------------------

class TestVinToListingUpsert:

    def test_insert_new_mapping(self, cur):
        artifact = _insert_artifact(cur, artifact_type="detail_page")
        vin = f"TEST{uuid.uuid4().hex[:13].upper()}"
        listing_id = _random_listing_id()

        cur.execute(
            """
            INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)
            VALUES (%s, %s::uuid, now(), %s)
            """,
            (vin, listing_id, artifact["artifact_id"]),
        )
        cur.execute(
            "SELECT listing_id FROM ops.vin_to_listing WHERE vin = %s",
            (vin,),
        )
        assert str(cur.fetchone()["listing_id"]) == listing_id

    def test_upsert_updates_listing_id(self, cur):
        artifact = _insert_artifact(cur, artifact_type="detail_page")
        vin = f"TEST{uuid.uuid4().hex[:13].upper()}"
        listing_id_1 = _random_listing_id()
        listing_id_2 = _random_listing_id()

        for listing_id in (listing_id_1, listing_id_2):
            cur.execute(
                """
                INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)
                VALUES (%s, %s::uuid, now(), %s)
                ON CONFLICT (vin) DO UPDATE SET
                    listing_id  = EXCLUDED.listing_id,
                    mapped_at   = EXCLUDED.mapped_at,
                    artifact_id = EXCLUDED.artifact_id
                """,
                (vin, listing_id, artifact["artifact_id"]),
            )

        cur.execute(
            "SELECT listing_id FROM ops.vin_to_listing WHERE vin = %s",
            (vin,),
        )
        assert str(cur.fetchone()["listing_id"]) == listing_id_2


# ---------------------------------------------------------------------------
# detail_scrape_claims release
# ---------------------------------------------------------------------------

class TestClaimRelease:

    def _insert_claim(self, cur, listing_id: str) -> None:
        cur.execute(
            """
            INSERT INTO ops.detail_scrape_claims
                (listing_id, claimed_by, claimed_at, status)
            VALUES (%s::uuid, 'test-run', now(), 'running')
            ON CONFLICT (listing_id) DO NOTHING
            """,
            (listing_id,),
        )

    def test_claim_deleted_after_processing(self, cur):
        listing_id = _random_listing_id()
        self._insert_claim(cur, listing_id)

        cur.execute(
            "DELETE FROM ops.detail_scrape_claims WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.detail_scrape_claims WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 0

    def test_claim_event_written_with_text_run_id(self, cur):
        listing_id = _random_listing_id()
        run_id = str(uuid.uuid4())  # text, not uuid column type

        cur.execute(
            """
            INSERT INTO staging.detail_scrape_claim_events
                (listing_id, run_id, status)
            VALUES (%s::uuid, %s, 'processed')
            """,
            (listing_id, run_id),
        )
        cur.execute(
            "SELECT run_id, status FROM staging.detail_scrape_claim_events"
            " WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        row = cur.fetchone()
        assert row["status"] == "processed"
        assert row["run_id"] == run_id


# ---------------------------------------------------------------------------
# queue_is_empty SQL
# ---------------------------------------------------------------------------

class TestQueueIsEmpty:

    def test_returns_zero_when_empty(self, cur):
        # Any pre-existing pending rows will be rolled back by the test transaction
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.artifacts_queue"
            " WHERE status IN ('pending', 'retry')"
        )
        # Just verifies the query executes without error
        assert cur.fetchone()["cnt"] >= 0

    def test_nonzero_after_insert(self, cur):
        _insert_artifact(cur, status="pending")
        cur.execute(
            "SELECT COUNT(*) AS cnt FROM ops.artifacts_queue"
            " WHERE status IN ('pending', 'retry')"
        )
        assert cur.fetchone()["cnt"] >= 1


# ===========================================================================
# Statements imported from processing.queries — Plan 162 Stage 7
# ===========================================================================

class TestExtractedProcessingStatements:
    """The first statements in this file that are the production text.

    Same repair as ``test_ops_queries.py``: everything above retypes SQL that
    resembles what processing runs, and a retyped statement cannot notice the
    original changed. These import from ``processing.queries``.
    """

    def test_claim_artifact_matching_nothing(self, cur):
        cur.execute(CLAIM_ARTIFACT, {"artifact_id": -1})
        assert cur.fetchone() is None

    def test_claim_artifact_claims_a_pending_row(self, cur):
        row = _insert_artifact(cur, status="pending")
        cur.execute(CLAIM_ARTIFACT, {"artifact_id": row["artifact_id"]})
        claimed = cur.fetchone()
        assert claimed is not None
        assert claimed["artifact_id"] == row["artifact_id"]

    def test_delete_price_observations_for_missing_listings(self, cur):
        cur.execute(
            DELETE_PRICE_OBSERVATIONS_FOR_MISSING_LISTINGS,
            (["NOSUCHVIN00000000"], [str(uuid.uuid4())]),
        )
        assert cur.rowcount == 0


# ===========================================================================
# Every remaining processing statement, executed — Plan 162 Stage 7
# ===========================================================================

class TestProcessingQueueStatements:
    """The artifacts_queue lifecycle, in the order processing runs it."""

    def test_claim_artifacts_by_type(self, cur):
        row = _insert_artifact(cur, artifact_type="detail_page", status="pending")
        # type_filter is a .format() slot, not a bind parameter: the caller
        # composes it in processing/routers/batch.py. Both branches are shapes
        # production issues, so both are executed here.
        cur.execute(
            CLAIM_ARTIFACTS.format(type_filter="AND artifact_type = 'detail_page'"),
            {"limit": 10},
        )
        claimed = {r["artifact_id"] for r in cur.fetchall()}
        assert row["artifact_id"] in claimed

    def test_claim_artifacts_without_a_type_filter(self, cur):
        _insert_artifact(cur, status="pending")
        cur.execute(CLAIM_ARTIFACTS.format(type_filter=""), {"limit": 1})
        assert len(cur.fetchall()) == 1

    def test_mark_artifact_status(self, cur):
        row = _insert_artifact(cur)
        cur.execute(
            MARK_ARTIFACT_STATUS,
            {"status": "complete", "artifact_id": row["artifact_id"]},
        )
        assert cur.rowcount == 1

    def test_insert_artifact_event(self, cur):
        row = _insert_artifact(cur)
        cur.execute(INSERT_ARTIFACT_EVENT, {
            "artifact_id": row["artifact_id"],
            "status": "complete",
            "minio_path": row["minio_path"],
            "artifact_type": row["artifact_type"],
            "fetched_at": row["fetched_at"],
            "listing_id": None,
            "run_id": None,
        })
        assert cur.rowcount == 1


class TestPriceObservationStatements:
    """ops.price_observations and its staging twin."""

    def _observation(self, listing_id, artifact_id):
        return {
            "listing_id": listing_id,
            "vin": "1HGCM82633A004352",
            "price": 25000,
            "make": "Honda",
            "model": "Accord",
            "customer_id": "dealer-1",
            "last_seen_at": "2099-01-01T00:00:00+00:00",
            "last_artifact_id": artifact_id,
            "last_detail_enriched_at": None,
        }

    def test_upsert_then_lookup_then_delete(self, cur):
        artifact = _insert_artifact(cur)
        listing_id = _random_listing_id()
        payload = self._observation(listing_id, artifact["artifact_id"])

        cur.execute(UPSERT_PRICE_OBSERVATION, payload)
        assert cur.rowcount == 1
        # Re-running the same observation must take the ON CONFLICT branch
        # rather than raising: processing writes every sighting, not just new
        # ones.
        cur.execute(UPSERT_PRICE_OBSERVATION, payload)
        assert cur.rowcount == 1

        cur.execute(
            LOOKUP_VIN_COLLISION,
            {"vin": payload["vin"], "listing_id": _random_listing_id()},
        )
        assert any(r["listing_id"] == listing_id for r in cur.fetchall())

        cur.execute(DELETE_PRICE_OBSERVATION, {"listing_id": listing_id})
        assert cur.rowcount == 1

    def test_delete_price_observation_by_vin(self, cur):
        artifact = _insert_artifact(cur)
        listing_id = _random_listing_id()
        cur.execute(
            UPSERT_PRICE_OBSERVATION,
            self._observation(listing_id, artifact["artifact_id"]),
        )
        cur.execute(DELETE_PRICE_OBSERVATION_BY_VIN, {"old_listing_id": listing_id})
        assert cur.rowcount == 1

    def test_insert_price_observation_event(self, cur):
        artifact = _insert_artifact(cur)
        cur.execute(INSERT_PRICE_OBSERVATION_EVENT, {
            "listing_id": _random_listing_id(),
            "vin": "1HGCM82633A004352",
            "price": 25000,
            "make": "Honda",
            "model": "Accord",
            "artifact_id": artifact["artifact_id"],
            # CHECK (event_type IN ('upserted', 'deleted')) -- V021.
            "event_type": "upserted",
            "source": "detail",
        })
        assert cur.rowcount == 1


class TestVinToListingStatements:

    def test_upsert_batch_lookup_and_event(self, cur):
        artifact = _insert_artifact(cur)
        listing_id = _random_listing_id()
        vin = "1HGCM82633A004353"

        cur.execute(UPSERT_VIN_TO_LISTING, {
            "vin": vin,
            "listing_id": listing_id,
            "mapped_at": "2099-01-01T00:00:00+00:00",
            "artifact_id": artifact["artifact_id"],
        })
        assert cur.rowcount == 1

        cur.execute(BATCH_LOOKUP_VIN_TO_LISTING, {"listing_ids": [listing_id]})
        assert [r["vin"] for r in cur.fetchall()] == [vin]

        cur.execute(INSERT_VIN_TO_LISTING_EVENT, {
            "vin": vin,
            "listing_id": listing_id,
            "artifact_id": artifact["artifact_id"],
            "event_type": "mapped",
            "previous_listing_id": None,
        })
        assert cur.rowcount == 1

    def test_batch_lookup_matching_nothing(self, cur):
        cur.execute(BATCH_LOOKUP_VIN_TO_LISTING, {"listing_ids": [_random_listing_id()]})
        assert cur.fetchall() == []


class TestTrackedModelStatements:

    def test_upsert_is_idempotent_then_get_and_event(self, cur, seed_search_config):
        payload = {"search_key": seed_search_config, "make": "Honda", "model": "Accord"}

        cur.execute(UPSERT_TRACKED_MODEL, payload)
        assert cur.rowcount == 1
        # ON CONFLICT DO NOTHING: the second write is a no-op, not an error.
        cur.execute(UPSERT_TRACKED_MODEL, payload)
        assert cur.rowcount == 0

        cur.execute(GET_TRACKED_MODELS)
        assert ("Honda", "Accord") in {(r["make"], r["model"]) for r in cur.fetchall()}

        cur.execute(INSERT_TRACKED_MODEL_EVENT, dict(payload, event_type="added"))
        assert cur.rowcount == 1


class TestDetailClaimAndCooldownStatements:

    def test_insert_detail_claim_event(self, cur):
        cur.execute(INSERT_DETAIL_CLAIM_EVENT, {
            "listing_id": _random_listing_id(),
            "run_id": _random_listing_id(),
            "status": "claimed",
        })
        assert cur.rowcount == 1

    def test_release_detail_claims_matching_nothing(self, cur):
        cur.execute(RELEASE_DETAIL_CLAIMS, {"listing_id": _random_listing_id()})
        assert cur.rowcount == 0

    def test_clear_blocked_cooldown_matching_nothing(self, cur):
        cur.execute(CLEAR_BLOCKED_COOLDOWN, {"listing_id": _random_listing_id()})
        assert cur.fetchone() is None

    def test_clear_blocked_cooldown_removes_an_existing_entry(self, cur):
        """The clear that actually clears, moved here by Plan 162 Stage 8.

        It came from ``tests/integration/scraper/test_blocked_cooldown.py``,
        which was a Layer 2 suite living in a Layer 4 directory: it executed
        statements against ``cur`` and never touched a route. The rest of that
        file duplicated ``test_scraper_queries.py`` and was deleted; this case
        did not, because the sibling above only proves the statement plans
        against a listing that matches nothing. A ``DELETE`` whose predicate
        never matches would pass that test forever.

        The seeding statement is the scraper's, because the two services share
        this table: the scraper writes the cooldown on a 403 and processing
        clears it on the next success.
        """
        from scraper.queries import UPSERT_BLOCKED_COOLDOWN

        listing_id = _random_listing_id()
        cur.execute(UPSERT_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(
            "SELECT count(*) AS cnt FROM ops.blocked_cooldown "
            "WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 1

        cur.execute(CLEAR_BLOCKED_COOLDOWN, {"listing_id": listing_id})
        cur.execute(
            "SELECT count(*) AS cnt FROM ops.blocked_cooldown "
            "WHERE listing_id = %s::uuid",
            (listing_id,),
        )
        assert cur.fetchone()["cnt"] == 0

    def test_insert_blocked_cooldown_cleared_event(self, cur):
        cur.execute(INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT, {
            "listing_id": _random_listing_id(),
            "num_of_attempts": 3,
        })
        assert cur.rowcount == 1


class TestSilverObservationWrite:
    """The batch insert into `staging.silver_observations` — Plan 162.

    The head of the whole silver pipeline: what this writes, the archiver
    flushes to Parquet, dbt reads and every mart is built on. It carries 37
    columns in a fixed order that positional tuples are built against, and it
    lived in a Python literal with nothing executing it until now — so a column
    a migration added in the middle would have shifted every value one place to
    the right, silently, in production.
    """

    def test_the_column_list_matches_the_order_the_rows_are_built_in(self, cur):
        """Not a parse check. `execute_values` binds tuples positionally, so the
        statement's column list and `_POSTGRES_COLS` are one contract in two
        files, and this is the only place they meet."""
        from psycopg2.extras import execute_values

        from processing.writers.silver_writer import _POSTGRES_COLS

        row = {col: None for col in _POSTGRES_COLS}
        row["artifact_id"] = 987_654_321
        row["listing_id"] = _random_listing_id()
        row["vin"] = "VIN00000000000001"
        row["source"] = "detail"
        row["listing_state"] = "active"
        row["fetched_at"] = datetime.now(timezone.utc)
        row["price"] = 31_500
        row["make"] = "toyota"
        row["model"] = "camry"
        row["year"] = 2026

        execute_values(
            cur, INSERT_SILVER_OBSERVATIONS,
            [tuple(row[col] for col in _POSTGRES_COLS)],
        )
        assert cur.rowcount == 1

        cur.execute(
            "SELECT vin, source, price, make, year FROM staging.silver_observations "
            "WHERE artifact_id = %s",
            (row["artifact_id"],),
        )
        written = cur.fetchone()
        # Read back by name: a shifted column list inserts successfully and puts
        # the year in the price, which only a value comparison catches.
        assert written["vin"] == "VIN00000000000001"
        assert written["source"] == "detail"
        assert written["price"] == 31_500
        assert written["make"] == "toyota"
        assert written["year"] == 2026
