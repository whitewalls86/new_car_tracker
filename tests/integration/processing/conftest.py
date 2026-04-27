"""
Integration test fixtures for the processing service.

Provides seed helpers for artifacts_queue, price_observations,
vin_to_listing, detail_scrape_claims, and search_configs.
"""
import uuid

import pytest


@pytest.fixture()
def seed_artifact(cur):
    """
    Factory fixture: insert an artifacts_queue row.

    Usage:
        artifact = seed_artifact(artifact_type="results_page", listing_id="...")
    """
    def _factory(
        artifact_type="results_page",
        listing_id=None,
        minio_path="s3://bronze/test/artifact.html.zst",
        status="pending",
        run_id=None,
    ):
        artifact_id_val = None
        listing_id = listing_id or str(uuid.uuid4())
        run_id = run_id or str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO ops.artifacts_queue
                (minio_path, artifact_type, listing_id, run_id, fetched_at, status)
            VALUES (%s, %s, %s, %s, now(), %s)
            RETURNING artifact_id
            """,
            (minio_path, artifact_type, listing_id, run_id, status),
        )
        artifact_id_val = cur.fetchone()["artifact_id"]
        return {
            "artifact_id": artifact_id_val,
            "artifact_type": artifact_type,
            "listing_id": listing_id,
            "minio_path": minio_path,
            "run_id": run_id,
            "status": status,
        }

    return _factory


@pytest.fixture()
def seed_price_observation(cur):
    """Factory: insert a price_observations row."""
    def _factory(listing_id=None, vin=None, price=25000, make="Honda", model="CR-V",
                 customer_id=None, artifact_id=1):
        listing_id = listing_id or str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO ops.price_observations
                (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
            VALUES (%s::uuid, %s, %s, %s, %s, %s, now(), %s)
            """,
            (listing_id, vin, price, make, model, customer_id, artifact_id),
        )
        return listing_id

    return _factory


@pytest.fixture()
def seed_vin_to_listing(cur):
    """Factory: insert a vin_to_listing row."""
    def _factory(vin, listing_id, artifact_id=1, mapped_at=None):
        cur.execute(
            """
            INSERT INTO ops.vin_to_listing (vin, listing_id, mapped_at, artifact_id)
            VALUES (%s, %s::uuid, COALESCE(%s, now()), %s)
            """,
            (vin, listing_id, mapped_at, artifact_id),
        )

    return _factory


@pytest.fixture()
def seed_detail_claim(cur):
    """Factory: insert a detail_scrape_claims row."""
    def _factory(listing_id, run_id=None):
        run_id = run_id or str(uuid.uuid4())
        cur.execute(
            """
            INSERT INTO ops.detail_scrape_claims (listing_id, claimed_by, status)
            VALUES (%s::uuid, %s, 'running')
            """,
            (listing_id, run_id),
        )
        return run_id

    return _factory


@pytest.fixture()
def seed_honda_search_config(cur):
    """Insert a search_config for Honda that carousel filtering can match against."""
    key = f"test-honda-{uuid.uuid4().hex[:8]}"
    cur.execute(
        """
        INSERT INTO search_configs
            (search_key, enabled, params, rotation_order, created_at, updated_at)
        VALUES (%s, true, '{"makes": ["honda"], "models": ["honda-cr_v"]}'::jsonb, 1, now(), now())
        """,
        (key,),
    )
    return key
