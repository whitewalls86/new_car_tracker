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
def seed_run(verify_cur):
    """
    Factory fixture that inserts a runs row and cleans up after.
    """
    inserted_ids = []

    def _seed(status: str = "running", trigger: str = "detail scrape") -> str:
        run_id = str(uuid.uuid4())
        verify_cur.execute(
            "INSERT INTO runs (run_id, status, trigger) VALUES (%s::uuid, %s, %s)",
            (run_id, status, trigger),
        )
        inserted_ids.append(run_id)
        return run_id

    yield _seed

    verify_cur.execute(
        "DELETE FROM runs WHERE run_id = ANY(%s::uuid[])",
        (inserted_ids,),
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
    # Seed a config with no last_queued_at — always due
    key = seed_search_config(rotation_slot=2, last_queued_at=None)

    response = api_client.post("/scrape/rotation/advance", params={"min_idle_minutes": 1})

    assert response.status_code == 200
    data = response.json()
    assert data["slot"] == 2
    assert any(c["search_key"] == key for c in data["configs"])

    # last_queued_at should be set on the claimed row
    verify_cur.execute(
        "SELECT last_queued_at FROM search_configs WHERE search_key = %s", (key,)
    )
    row = verify_cur.fetchone()
    assert row["last_queued_at"] is not None


@pytest.mark.integration
def test_advance_rotation_returns_too_soon_within_gap(api_client, verify_cur, seed_search_config):
    seed_search_config(rotation_slot=3, last_queued_at=None)

    # Seed a very recent search scrape run
    run_id = str(uuid.uuid4())
    verify_cur.execute(
        """
        INSERT INTO runs 
            (run_id, status, trigger) 
        VALUES (%s::uuid, 'completed', 'search scrape')
        """,
        (run_id,),
    )
    try:
        response = api_client.post(
            "/scrape/rotation/advance",
            params={"min_idle_minutes": 1, "min_gap_minutes": 9999},
        )

        assert response.status_code == 200
        data = response.json()
        assert data["slot"] is None
        assert data.get("reason") == "too_soon"
    finally:
        verify_cur.execute("DELETE FROM runs WHERE run_id = %s", (run_id,))


@pytest.mark.integration
def test_advance_rotation_updates_last_queued_at_for_all_configs_in_slot(
    api_client, verify_cur, seed_search_config
):
    key_a = seed_search_config(rotation_slot=4, last_queued_at=None)
    key_b = seed_search_config(rotation_slot=4, last_queued_at=None)

    api_client.post("/scrape/rotation/advance", params={"min_idle_minutes": 1})

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


@pytest.mark.integration
def test_claim_batch_creates_run_row_in_db(api_client, verify_cur):
    response = api_client.post("/scrape/claims/claim-batch")

    run_id = response.json()["run_id"]

    verify_cur.execute("SELECT status, trigger FROM runs WHERE run_id = %s::uuid", (run_id,))
    row = verify_cur.fetchone()
    assert row is not None
    assert row["trigger"] == "detail scrape"

    # Cleanup
    verify_cur.execute("DELETE FROM runs WHERE run_id = %s::uuid", (run_id,))


@pytest.mark.integration
def test_claim_batch_marks_run_skipped_when_queue_empty(api_client, verify_cur):
    # This test assumes the queue may be empty in the test DB.
    # If the queue has rows, the test is still valid — we just check the run exists.
    response = api_client.post("/scrape/claims/claim-batch")

    data = response.json()
    run_id = data["run_id"]

    verify_cur.execute("SELECT status FROM runs WHERE run_id = %s::uuid", (run_id,))
    row = verify_cur.fetchone()

    if data["listings"]:
        # Queue had rows — run should be running
        assert row["status"] == "running"
        # Cleanup claims and run
        listing_ids = [listing["listing_id"] for listing in data["listings"]]
        verify_cur.execute(
            "DELETE FROM detail_scrape_claims WHERE listing_id = ANY(%s::uuid[])", (listing_ids,)
        )
    else:
        # Queue empty — run should be skipped
        assert row["status"] == "skipped"

    verify_cur.execute("DELETE FROM runs WHERE run_id = %s::uuid", (run_id,))


# ---------------------------------------------------------------------------
# POST /scrape/claims/release
# ---------------------------------------------------------------------------

@pytest.mark.integration
def test_release_claims_marks_run_completed(api_client, verify_cur, seed_run, seed_claim):
    run_id = seed_run(status="running")
    listing_id = str(uuid.uuid4())
    seed_claim(listing_id=listing_id, claimed_by=run_id)

    response = api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [{"listing_id": listing_id, "status": "ok"}],
    })

    assert response.status_code == 200
    data = response.json()
    assert data["status"] == "completed"
    assert data["total"] == 1
    assert data["errors"] == 0

    verify_cur.execute("SELECT status FROM runs WHERE run_id = %s::uuid", (run_id,))
    assert verify_cur.fetchone()["status"] == "completed"


@pytest.mark.integration
def test_release_claims_deletes_claim_rows(api_client, verify_cur, seed_run, seed_claim):
    run_id = seed_run(status="running")
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
def test_release_claims_marks_run_failed_when_all_results_failed(
    api_client, verify_cur, seed_run, seed_claim
):
    run_id = seed_run(status="running")
    listing_a = str(uuid.uuid4())
    listing_b = str(uuid.uuid4())
    seed_claim(listing_id=listing_a, claimed_by=run_id)
    seed_claim(listing_id=listing_b, claimed_by=run_id)

    response = api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [
            {"listing_id": listing_a, "status": "failed"},
            {"listing_id": listing_b, "status": "failed"},
        ],
    })

    assert response.status_code == 200
    assert response.json()["status"] == "failed"

    verify_cur.execute("SELECT status FROM runs WHERE run_id = %s::uuid", (run_id,))
    assert verify_cur.fetchone()["status"] == "failed"


@pytest.mark.integration
def test_release_claims_completed_when_mix_of_ok_and_failed(
    api_client, verify_cur, seed_run, seed_claim
):
    run_id = seed_run(status="running")
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
    assert data["status"] == "completed"
    assert data["errors"] == 1
    assert data["total"] == 2


@pytest.mark.integration
def test_release_claims_empty_results(api_client, verify_cur, seed_run):
    run_id = seed_run(status="running")

    response = api_client.post("/scrape/claims/release", json={
        "run_id": run_id,
        "results": [],
    })

    assert response.status_code == 200
    assert response.json()["status"] == "completed"
    assert response.json()["total"] == 0
