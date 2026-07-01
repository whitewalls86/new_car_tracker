"""
Layer 1 — SQL smoke tests for ops_vehicle_staleness and ops_detail_scrape_queue.

Both views are plain Postgres views (V040) reading directly from ops.price_observations
and ops.blocked_cooldown. Tests seed HOT table rows and assert staleness flags and
queue membership. Per-test rollback — no committed state.

Staleness model (Plan 115 circuit breaker):
  is_full_details_stale = customer_id IS NULL AND (last_detail_scraped_at IS NULL
                          OR last_detail_scraped_at < now() - 7 days)
  is_price_stale        = last_seen_at < now() - 24h (any source)
  stale_reason          = dealer_unenriched | price_only | not_stale

Queue blocked_cooldown formula (inlined in V040):
  next_eligible_at = last_attempted_at + 12h * 2^(num_of_attempts - 1)
  fully_blocked    = num_of_attempts >= 5
"""
import uuid

import pytest

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _random_listing_id() -> str:
    return str(uuid.uuid4())


def _insert_artifact(cur, artifact_type="results_page") -> int:
    minio_path = (
        f"s3://bronze/html/year=2026/month=4"
        f"/artifact_type={artifact_type}/{uuid.uuid4()}.html.zst"
    )
    cur.execute(
        """
        INSERT INTO ops.artifacts_queue
            (minio_path, artifact_type, fetched_at, status)
        VALUES (%s, %s, now(), 'pending')
        RETURNING artifact_id
        """,
        (minio_path, artifact_type),
    )
    return cur.fetchone()["artifact_id"]


def _insert_price_obs(
    cur,
    artifact_id: int,
    listing_id: str,
    *,
    vin: str = None,
    price: int = 30000,
    customer_id: str = None,
    age_hours: float = 1.0,
    last_detail_scraped_at_hours_ago: float = None,
):
    """Insert one row into ops.price_observations at a controlled age.

    last_detail_scraped_at_hours_ago: if provided, sets last_detail_scraped_at
    to now() minus that many hours. None leaves the column NULL.
    """
    if last_detail_scraped_at_hours_ago is not None:
        cur.execute(
            """
            INSERT INTO ops.price_observations
                (listing_id, vin, price, make, model, customer_id, last_seen_at,
                 last_artifact_id, last_detail_scraped_at)
            VALUES (
                %s::uuid, %s, %s, 'honda', 'crv', %s,
                now() - (%s || ' hours')::interval,
                %s,
                now() - (%s || ' hours')::interval
            )
            """,
            (listing_id, vin, price, customer_id, str(age_hours), artifact_id,
             str(last_detail_scraped_at_hours_ago)),
        )
    else:
        cur.execute(
            """
            INSERT INTO ops.price_observations
                (listing_id, vin, price, make, model, customer_id, last_seen_at, last_artifact_id)
            VALUES (
                %s::uuid, %s, %s, 'honda', 'crv', %s,
                now() - (%s || ' hours')::interval,
                %s
            )
            """,
            (listing_id, vin, price, customer_id, str(age_hours), artifact_id),
        )


def _insert_cooldown(cur, listing_id: str, num_of_attempts: int, last_attempted_hours_ago: float):
    cur.execute(
        """
        INSERT INTO ops.blocked_cooldown
            (listing_id, first_attempted_at, last_attempted_at, num_of_attempts)
        VALUES (
            %s::uuid,
            now() - interval '7 days',
            now() - (%s || ' hours')::interval,
            %s
        )
        """,
        (listing_id, str(last_attempted_hours_ago), num_of_attempts),
    )


# ---------------------------------------------------------------------------
# ops_vehicle_staleness
# ---------------------------------------------------------------------------

class TestOpsVehicleStaleness:

    def test_fresh_enriched_is_not_stale(self, cur):
        """Listing seen 1h ago, customer_id set → not_stale."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id="cust-1", age_hours=1)

        cur.execute(
            "SELECT is_price_stale, is_full_details_stale, stale_reason"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["is_price_stale"] is False
        assert row["is_full_details_stale"] is False
        assert row["stale_reason"] == "not_stale"

    def test_stale_price_no_customer_id_is_dealer_unenriched(self, cur):
        """Listing never detail-scraped (customer_id IS NULL) → dealer_unenriched
        regardless of age."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1)

        cur.execute(
            "SELECT is_full_details_stale, stale_reason"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["is_full_details_stale"] is True
        assert row["stale_reason"] == "dealer_unenriched"

    def test_old_enriched_listing_is_price_only_stale(self, cur):
        """Listing enriched (customer_id set) but not seen in 25h → price_only."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id="cust-2", age_hours=25)

        cur.execute(
            "SELECT is_price_stale, is_full_details_stale, stale_reason"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["is_price_stale"] is True
        assert row["is_full_details_stale"] is False
        assert row["stale_reason"] == "price_only"

    def test_dealer_unenriched_takes_priority_over_price_stale(self, cur):
        """customer_id IS NULL + old age → stale_reason is dealer_unenriched, not price_only."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=48)

        cur.execute(
            "SELECT stale_reason FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone()["stale_reason"] == "dealer_unenriched"

    def test_current_listing_url_constructed_correctly(self, cur):
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id="c")

        cur.execute(
            "SELECT current_listing_url FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        url = cur.fetchone()["current_listing_url"]
        assert url == f"https://www.cars.com/vehicledetail/{lid}/"

    # -----------------------------------------------------------------------
    # Circuit-breaker tests (Plan 115)
    # -----------------------------------------------------------------------

    def test_customer_id_null_last_detail_scraped_now_is_not_stale(self, cur):
        """customer_id NULL + recently detail-scraped → not dealer_unenriched (circuit breaker)."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_scraped_at_hours_ago=0.1)

        cur.execute(
            "SELECT is_full_details_stale, stale_reason"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["is_full_details_stale"] is False
        assert row["stale_reason"] == "not_stale"

    def test_customer_id_null_last_detail_scraped_8_days_ago_is_stale(self, cur):
        """customer_id NULL + last_detail_scraped_at 8 days ago → dealer_unenriched again."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_scraped_at_hours_ago=8 * 24)

        cur.execute(
            "SELECT is_full_details_stale, stale_reason"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["is_full_details_stale"] is True
        assert row["stale_reason"] == "dealer_unenriched"

    def test_last_detail_scraped_at_exposed_in_view(self, cur):
        """last_detail_scraped_at is returned from the view for diagnostics."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_scraped_at_hours_ago=1)

        cur.execute(
            "SELECT last_detail_scraped_at"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["last_detail_scraped_at"] is not None


# ---------------------------------------------------------------------------
# ops_detail_scrape_queue
# ---------------------------------------------------------------------------

class TestOpsDetailScrapeQueue:

    def test_stale_no_cooldown_appears_in_queue(self, cur):
        """Stale listing with no cooldown record → priority 1 in queue."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id="cust-q1", age_hours=25)

        cur.execute(
            "SELECT listing_id, priority FROM ops.ops_detail_scrape_queue"
            " WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["priority"] == 1

    def test_not_stale_not_in_queue(self, cur):
        """Fresh enriched listing → not in queue."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id="cust-fresh", age_hours=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None

    def test_fully_blocked_not_in_queue(self, cur):
        """5 cooldown attempts → fully blocked, excluded from queue."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=48)
        _insert_cooldown(cur, lid, num_of_attempts=5, last_attempted_hours_ago=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None

    def test_cooldown_not_elapsed_excluded(self, cur):
        """2 attempts, last 1h ago → next_eligible_at = 1h ago + 24h = 23h from now → excluded."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=36)
        _insert_cooldown(cur, lid, num_of_attempts=2, last_attempted_hours_ago=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None

    def test_cooldown_elapsed_appears_in_queue(self, cur):
        """1 attempt, last 13h ago → next_eligible_at = 13h ago + 12h = 1h ago → eligible."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=48)
        _insert_cooldown(cur, lid, num_of_attempts=1, last_attempted_hours_ago=13)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None

    def test_dealer_partition_one_per_customer_id(self, cur):
        """Two stale listings from the same dealer → only one appears at priority 1."""
        artifact_id = _insert_artifact(cur)
        lid1 = _random_listing_id()
        lid2 = _random_listing_id()
        # Both enriched, both stale, same customer_id
        _insert_price_obs(cur, artifact_id, lid1, customer_id="shared-dealer", age_hours=25)
        _insert_price_obs(cur, artifact_id, lid2, customer_id="shared-dealer", age_hours=30)

        cur.execute(
            """
            SELECT listing_id, priority
            FROM ops.ops_detail_scrape_queue
            WHERE listing_id IN (%s::uuid, %s::uuid)
            ORDER BY priority
            """,
            (lid1, lid2),
        )
        rows = cur.fetchall()
        # Both should appear (pool 1 + pool 3), but only one at priority 1
        priorities = [r["priority"] for r in rows]
        assert priorities.count(1) == 1

    def test_unenriched_listing_appears_at_priority_1(self, cur):
        """customer_id IS NULL (never detail-scraped) → queued at priority 1."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=2)

        cur.execute(
            "SELECT priority FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["priority"] == 1

    def test_force_stale_second_dealer_vehicle_at_priority_2(self, cur):
        """Two listings from same dealer, both >36h stale → second one at priority 2."""
        artifact_id = _insert_artifact(cur)
        lid1 = _random_listing_id()
        lid2 = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid1, customer_id="dealer-x", age_hours=37)
        _insert_price_obs(cur, artifact_id, lid2, customer_id="dealer-x", age_hours=40)

        cur.execute(
            """
            SELECT listing_id, priority
            FROM ops.ops_detail_scrape_queue
            WHERE listing_id IN (%s::uuid, %s::uuid)
            ORDER BY priority
            """,
            (lid1, lid2),
        )
        rows = cur.fetchall()
        priorities = sorted(r["priority"] for r in rows)
        assert 1 in priorities
        assert 2 in priorities


# ---------------------------------------------------------------------------
# Circuit-breaker queue suppression (Plan 115)
# ---------------------------------------------------------------------------

class TestCircuitBreakerQueue:

    def test_unenriched_null_last_detail_is_in_queue(self, cur):
        """customer_id NULL, last_detail_scraped_at NULL → in queue (never scraped)."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None

    def test_unenriched_recently_scraped_not_in_queue(self, cur):
        """customer_id NULL, last_detail_scraped_at now → suppressed by circuit breaker."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_scraped_at_hours_ago=0.25)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None

    def test_unenriched_scraped_8_days_ago_back_in_queue(self, cur):
        """customer_id NULL, last_detail_scraped_at 8 days ago → back in queue."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_scraped_at_hours_ago=8 * 24)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None

    def test_enriched_fresh_not_in_queue(self, cur):
        """customer_id NOT NULL, last_seen_at fresh → not queued."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id="cust-1", age_hours=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None

    def test_enriched_stale_in_queue_as_price_only(self, cur):
        """customer_id NOT NULL, last_seen_at > 24h → in queue as price_only."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id="cust-2", age_hours=25)

        cur.execute(
            "SELECT listing_id, stale_reason FROM ops.ops_detail_scrape_queue"
            " WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row is not None
        assert row["stale_reason"] == "price_only"

    def test_second_detail_cycle_with_null_customer_id_suppressed(self, cur):
        """Regression: simulate two successful detail cycles with customer_id NULL.

        After the first cycle sets last_detail_scraped_at, the listing must be
        absent from the queue immediately on the next DAG run.
        """
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()

        # First cycle: no last_detail_scraped_at yet → in queue
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1)
        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None, "listing should be queued before first scrape"

        # First cycle completes: set last_detail_scraped_at to now
        cur.execute(
            "UPDATE ops.price_observations SET last_detail_scraped_at = now()"
            " WHERE listing_id = %s::uuid",
            (lid,),
        )

        # Second cycle: listing must not be in queue immediately
        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None, "listing must be suppressed after first detail scrape"
