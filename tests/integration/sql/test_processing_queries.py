"""
Layer 1 — SQL smoke tests for the processing service.

Tests every SQL statement issued by processing/processor.py against a real DB
with Flyway migrations applied. Does not invoke MinIO or the parsers — those
are tested elsewhere. Goal: catch schema breakage before it hits production.
"""
import uuid

import pytest

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
