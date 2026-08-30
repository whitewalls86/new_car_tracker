"""
Layer 3 — scrape coordination integration tests.

Covers:
  POST /scrape/rotation/advance
  POST /scrape/claims/claim-batch
  POST /scrape/claims/release

All tests run against a real Postgres instance — no mocked DB.
"""
import datetime
import uuid

import pytest

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture()
def seed_search_config(verify_cur, test_key_prefix):
    """
    Factory fixture that inserts a minimal search_configs row and cleans up after.

    Usage:
        key = seed_search_config(rotation_slot=1, last_queued_at=None)
    """
    inserted_keys = []

    def _seed(
        search_key: str | None = None,
        rotation_slot: int | None = None,
        last_queued_at: str | None = None,
        enabled: bool = True,
    ) -> str:
        key = search_key or f"{test_key_prefix}sc-{uuid.uuid4().hex[:6]}"
        verify_cur.execute("""
            INSERT INTO search_configs (
                search_key, params, enabled, rotation_slot, last_queued_at
            )
            VALUES (
                %s,
                '{"makes": ["honda"], "models": ["cr-v"], "scopes": ["national"]}'::jsonb,
                %s,
                %s,
                %s
            )
        """, (key, enabled, rotation_slot, last_queued_at))
        inserted_keys.append(key)
        return key

    yield _seed

    verify_cur.execute(
        "DELETE FROM search_configs WHERE search_key = ANY(%s)",
        (inserted_keys,),
    )


@pytest.fixture()
def seed_claim(verify_cur):
    """
    Factory fixture that inserts a detail_scrape_claims row and cleans up after.
    """
    inserted_listing_ids = []

    def _seed(listing_id: str, claimed_by: str, status: str = "running"):
        verify_cur.execute(
            """INSERT INTO detail_scrape_claims (listing_id, claimed_by, status)
               VALUES (%s, %s, %s)
               ON CONFLICT (listing_id) DO UPDATE
                 SET claimed_by = EXCLUDED.claimed_by, status = EXCLUDED.status""",
            (listing_id, claimed_by, status),
        )
        inserted_listing_ids.append(listing_id)

    yield _seed

    verify_cur.execute(
        "DELETE FROM detail_scrape_claims WHERE listing_id = ANY(%s::uuid[])",
        (inserted_listing_ids,),
    )


@pytest.fixture()
def seed_observation(verify_cur):
    """
    Factory fixture that inserts an ops.price_observations row (with the
    artifacts_queue row its FK-free last_artifact_id refers to) and cleans up
    after. Needed to observe last_detail_fetched_at, which the release endpoint
    writes.
    """
    inserted_listing_ids = []
    inserted_artifact_ids = []

    def _seed(listing_id: str, last_detail_fetched_at=None):
        verify_cur.execute(
            """INSERT INTO ops.artifacts_queue
                   (minio_path, artifact_type, fetched_at, status)
               VALUES (%s, 'detail_page', now(), 'pending')
               RETURNING artifact_id""",
            (f"s3://bronze/test/{uuid.uuid4()}.html.zst",),
        )
        artifact_id = verify_cur.fetchone()["artifact_id"]
        verify_cur.execute(
            """INSERT INTO ops.price_observations
                   (listing_id, price, make, model, last_seen_at,
                    last_artifact_id, last_detail_fetched_at)
               VALUES (%s::uuid, 30000, 'honda', 'crv', now(), %s, %s)""",
            (listing_id, artifact_id, last_detail_fetched_at),
        )
        inserted_listing_ids.append(listing_id)
        inserted_artifact_ids.append(artifact_id)
        return artifact_id

    yield _seed

    verify_cur.execute(
        "DELETE FROM ops.price_observations WHERE listing_id = ANY(%s::uuid[])",
        (inserted_listing_ids,),
    )
    verify_cur.execute(
        "DELETE FROM ops.artifacts_queue WHERE artifact_id = ANY(%s)",
        (inserted_artifact_ids,),
    )


# ---------------------------------------------------------------------------
# POST /scrape/rotation/advance
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_advance_rotation_does_not_return_recently_queued_config(api_client, seed_search_config):
    # Use a high slot number with no other configs so nothing else can trigger it.
    # Pass a real datetime so psycopg2 sets last_queued_at correctly.
    unique_slot = 9901
    seed_search_config(
        rotation_slot=unique_slot,
        last_queued_at=datetime.datetime.now(datetime.timezone.utc),
    )

    response = api_client.post("/scrape/rotation/advance", params={"min_idle_minutes": 1439})

    assert response.status_code == 200
    assert response.json().get("slot") != unique_slot


@pytest.mark.integration
def test_advance_rotation_returns_slot_when_due(api_client, verify_cur, seed_search_config):
    # Park all existing eligible configs so they won't be chosen over our test data.
    # Pass min_gap_minutes=0 to bypass the gap guard (which would fire because
    # the parked configs now have last_queued_at = now()).
    verify_cur.execute(
        "UPDATE search_configs SET last_queued_at = now() WHERE enabled = true"
    )

    key = seed_search_config(rotation_slot=2, last_queued_at=None)

    response = api_client.post(
        "/scrape/rotation/advance",
        params={"min_idle_minutes": 1, "min_gap_minutes": 0},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["slot"] == 2
    assert data["run_id"] is not None
    uuid.UUID(data["run_id"])  # valid UUID — no runs row written, just returned
    assert any(c["search_key"] == key for c in data["configs"])

    # last_queued_at should be set on the claimed row
    verify_cur.execute(
        "SELECT last_queued_at FROM search_configs WHERE search_key = %s", (key,)
    )
    row = verify_cur.fetchone()
    assert row["last_queued_at"] is not None


@pytest.mark.integration
def test_advance_rotation_returns_too_soon_within_gap(api_client, seed_search_config):
    # Seed a config with a very recent last_queued_at to trigger the gap guard.
    # The gap check now reads MAX(search_configs.last_queued_at) — no runs row needed.
    seed_search_config(
        rotation_slot=3,
        last_queued_at=datetime.datetime.now(datetime.timezone.utc),
    )

    response = api_client.post(
        "/scrape/rotation/advance",
        params={"min_idle_minutes": 1, "min_gap_minutes": 9999},
    )

    assert response.status_code == 200
    data = response.json()
    assert data["slot"] is None
    assert data.get("reason") == "too_soon"


@pytest.mark.integration
def test_advance_rotation_updates_last_queued_at_for_all_configs_in_slot(
    api_client, verify_cur, seed_search_config
):
    # Park all existing eligible configs so our slot-4 pair is chosen.
    # Pass min_gap_minutes=0 to bypass the gap guard.
    verify_cur.execute(
        "UPDATE search_configs SET last_queued_at = now() WHERE enabled = true"
    )

    key_a = seed_search_config(rotation_slot=4, last_queued_at=None)
    key_b = seed_search_config(rotation_slot=4, last_queued_at=None)

    api_client.post(
        "/scrape/rotation/advance",
        params={"min_idle_minutes": 1, "min_gap_minutes": 0},
    )

    for key in (key_a, key_b):
        verify_cur.execute(
            "SELECT last_queued_at FROM search_configs WHERE search_key = %s", (key,)
        )
        assert verify_cur.fetchone()["last_queued_at"] is not None


# ---------------------------------------------------------------------------
# POST /scrape/claims/claim-batch
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_claim_batch_returns_run_id(api_client):
    response = api_client.post("/scrape/claims/claim-batch")

    assert response.status_code == 200
    data = response.json()
    assert "run_id" in data
    assert "listings" in data
    # run_id should be a valid UUID
    uuid.UUID(data["run_id"])


# ---------------------------------------------------------------------------
# POST /scrape/claims/release
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_release_claims_deletes_claim_rows(api_client, verify_cur, seed_claim):
    run_id = str(uuid.uuid4())
    listing_id = str(uuid.uuid4())
    seed_claim(listing_id=listing_id, claimed_by=run_id)

    api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [{"listing_id": listing_id, "status": "ok"}],
    })

    verify_cur.execute(
        "SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
    )
    assert verify_cur.fetchone() is None


@pytest.mark.integration
def test_release_claims_counts_errors_correctly(api_client, verify_cur, seed_claim):
    run_id = str(uuid.uuid4())
    listing_ok = str(uuid.uuid4())
    listing_fail = str(uuid.uuid4())
    seed_claim(listing_id=listing_ok, claimed_by=run_id)
    seed_claim(listing_id=listing_fail, claimed_by=run_id)

    response = api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [
            {"listing_id": listing_ok, "status": "ok"},
            {"listing_id": listing_fail, "status": "failed"},
        ],
    })

    assert response.status_code == 200
    data = response.json()
    assert data["errors"] == 1
    assert data["total"] == 2


@pytest.mark.integration
def test_release_claims_empty_results(api_client):
    run_id = str(uuid.uuid4())

    response = api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [],
    })

    assert response.status_code == 200
    assert response.json()["total"] == 0


# ---------------------------------------------------------------------------
# POST /scrape/claims/release — the fetch fact (Plan 147)
# ---------------------------------------------------------------------------

def _fetched_at(verify_cur, listing_id):
    verify_cur.execute(
        "SELECT last_detail_fetched_at FROM ops.price_observations"
        " WHERE listing_id = %s::uuid",
        (listing_id,),
    )
    return verify_cur.fetchone()["last_detail_fetched_at"]


@pytest.mark.integration
def test_release_records_fetch_for_ok_and_failed(
    api_client, verify_cur, seed_claim, seed_observation,
):
    """The component that spent the request records having spent it."""
    run_id = str(uuid.uuid4())
    listing_ok = str(uuid.uuid4())
    listing_failed = str(uuid.uuid4())
    for lid in (listing_ok, listing_failed):
        seed_observation(lid)
        seed_claim(listing_id=lid, claimed_by=run_id)

    response = api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [
            {"listing_id": listing_ok, "status": "ok"},
            {"listing_id": listing_failed, "status": "failed"},
        ],
    })

    assert response.status_code == 200
    assert response.json()["fetches_recorded"] == 2
    assert _fetched_at(verify_cur, listing_ok) is not None
    assert _fetched_at(verify_cur, listing_failed) is not None, (
        "a failed fetch spent a request and must back off too"
    )


@pytest.mark.integration
def test_release_does_not_record_fetch_for_skipped(
    api_client, verify_cur, seed_claim, seed_observation,
):
    """A skipped listing was never attempted, so no request was spent."""
    run_id = str(uuid.uuid4())
    listing_id = str(uuid.uuid4())
    seed_observation(listing_id)
    seed_claim(listing_id=listing_id, claimed_by=run_id)

    api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [{"listing_id": listing_id, "status": "skipped"}],
    })

    assert _fetched_at(verify_cur, listing_id) is None
    verify_cur.execute(
        "SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
    )
    assert verify_cur.fetchone() is None, "the claim is still released"


@pytest.mark.integration
def test_release_stamps_fetch_and_deletes_claim_together(
    api_client, verify_cur, seed_claim, seed_observation,
):
    """The claim delete and the fetch record share one transaction.

    This is the property that closes the loop: a released claim without a
    recorded fetch is exactly the state that produced the re-scrape.
    """
    run_id = str(uuid.uuid4())
    listing_id = str(uuid.uuid4())
    seed_observation(listing_id)
    seed_claim(listing_id=listing_id, claimed_by=run_id)

    api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [{"listing_id": listing_id, "status": "ok"}],
    })

    verify_cur.execute(
        "SELECT 1 FROM detail_scrape_claims WHERE listing_id = %s::uuid", (listing_id,)
    )
    assert verify_cur.fetchone() is None
    assert _fetched_at(verify_cur, listing_id) is not None


@pytest.mark.integration
def test_released_listing_leaves_the_queue_without_any_processing(
    api_client, verify_cur, seed_claim, seed_observation,
):
    """Plan 147's whole point, end to end.

    An unenriched listing is queued; the scraper claims, fetches and releases;
    processing never runs. Before this change the listing was back in the queue
    on the next */15 tick. Now the fetch backoff holds it.
    """
    run_id = str(uuid.uuid4())
    listing_id = str(uuid.uuid4())
    seed_observation(listing_id)

    verify_cur.execute(
        "SELECT 1 FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
        (listing_id,),
    )
    assert verify_cur.fetchone() is not None, "unenriched listing should be queued"

    seed_claim(listing_id=listing_id, claimed_by=run_id)
    api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [{"listing_id": listing_id, "status": "ok"}],
    })

    verify_cur.execute(
        "SELECT 1 FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
        (listing_id,),
    )
    assert verify_cur.fetchone() is None, (
        "a fetched listing must not be re-claimed while processing is down"
    )
