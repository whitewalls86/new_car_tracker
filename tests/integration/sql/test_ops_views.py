"""
Layer 2 — SQL smoke tests for ops_vehicle_staleness and ops_detail_scrape_queue.

Both views are plain Postgres views (rebuilt by V049) reading directly from
ops.price_observations and ops.blocked_cooldown. Tests seed HOT table rows and
assert staleness flags and queue membership. Per-test rollback — no committed
state.

Staleness model (Plan 115 circuit breaker, re-owned by Plan 147 / V049):
  is_full_details_stale = customer_id IS NULL AND (last_detail_enriched_at IS
                          NULL OR last_detail_enriched_at < now() - 7 days)
                          -- V048 read through the legacy last_detail_scraped_at
                          -- as well, because the writers setting the new column
                          -- deployed after the view did. V049 dropped the legacy
                          -- column and collapsed the COALESCE.
  is_price_stale        = last_seen_at < now() - 24h (any source)
  stale_reason          = dealer_unenriched | price_only | not_stale

Queue fetch backoff (V048, Plan 147; carried through V049 verbatim):
  a listing is excluded while last_detail_fetched_at > now() - 6 hours. NULL
  means never fetched and the predicate does not bind.

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
    last_detail_fetched_at_hours_ago: float = None,
    last_detail_enriched_at_hours_ago: float = None,
):
    """Insert one row into ops.price_observations at a controlled age.

    Each *_hours_ago argument sets its column to now() minus that many hours;
    None leaves the column NULL.

    last_detail_fetched_at is scraper-owned and drives the loop guard;
    last_detail_enriched_at is processor-owned and drives the 7-day enrichment
    window. They replaced the single V040 last_detail_scraped_at column, which
    V049 dropped.
    """
    columns = [
        "listing_id", "vin", "price", "make", "model", "customer_id",
        "last_seen_at", "last_artifact_id",
    ]
    values = [
        "%s::uuid", "%s", "%s", "'honda'", "'crv'", "%s",
        "now() - (%s || ' hours')::interval", "%s",
    ]
    params = [listing_id, vin, price, customer_id, str(age_hours), artifact_id]

    for column, hours_ago in (
        ("last_detail_fetched_at", last_detail_fetched_at_hours_ago),
        ("last_detail_enriched_at", last_detail_enriched_at_hours_ago),
    ):
        if hours_ago is not None:
            columns.append(column)
            values.append("now() - (%s || ' hours')::interval")
            params.append(str(hours_ago))

    cur.execute(
        f"INSERT INTO ops.price_observations ({', '.join(columns)})"
        f" VALUES ({', '.join(values)})",
        params,
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

    def test_customer_id_null_recently_enriched_is_not_stale(self, cur):
        """customer_id NULL + recently enriched → not dealer_unenriched (circuit breaker)."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_enriched_at_hours_ago=0.1)

        cur.execute(
            "SELECT is_full_details_stale, stale_reason"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["is_full_details_stale"] is False
        assert row["stale_reason"] == "not_stale"

    def test_customer_id_null_enriched_8_days_ago_is_stale(self, cur):
        """customer_id NULL + last_detail_enriched_at 8 days ago → dealer_unenriched again."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_enriched_at_hours_ago=8 * 24)

        cur.execute(
            "SELECT is_full_details_stale, stale_reason"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["is_full_details_stale"] is True
        assert row["stale_reason"] == "dealer_unenriched"


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
        """customer_id NULL, last_detail_enriched_at NULL → in queue (never enriched)."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None

    def test_unenriched_recently_enriched_not_in_queue(self, cur):
        """customer_id NULL, last_detail_enriched_at now → suppressed by circuit breaker."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_enriched_at_hours_ago=0.25)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None

    def test_unenriched_enriched_8_days_ago_back_in_queue(self, cur):
        """customer_id NULL, last_detail_enriched_at 8 days ago → back in queue."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_enriched_at_hours_ago=8 * 24)

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

        After the first cycle sets last_detail_enriched_at, the listing must be
        absent from the queue immediately on the next DAG run.
        """
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()

        # First cycle: no last_detail_enriched_at yet → in queue
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1)
        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None, "listing should be queued before first scrape"

        # First cycle completes: set last_detail_enriched_at to now
        cur.execute(
            "UPDATE ops.price_observations SET last_detail_enriched_at = now()"
            " WHERE listing_id = %s::uuid",
            (lid,),
        )

        # Second cycle: listing must not be in queue immediately
        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None, "listing must be suppressed after first detail scrape"


# ---------------------------------------------------------------------------
# Fetch backoff (Plan 147 Stage 1 / V048)
# ---------------------------------------------------------------------------

class TestFetchBackoffQueue:
    """last_detail_fetched_at is the loop guard: a spent detail request keeps a
    listing out of the queue for six hours whatever became of the artifact.

    This is what makes a processing outage stop producing re-fetches. Under
    V040 the only guard was written by the processor, so a paused or crashed
    processing service meant the same batch was re-claimed every 15 minutes.
    """

    def test_fetched_recently_is_not_reclaimed(self, cur):
        """Fetched 1h ago, never enriched → held out of the queue by the backoff."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_fetched_at_hours_ago=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None

    def test_fetched_beyond_backoff_is_reclaimed(self, cur):
        """Fetched 7h ago and still unenriched → back in the queue.

        The guard is a delay, not a deletion: a listing whose enrichment never
        arrived must eventually be retried.
        """
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_fetched_at_hours_ago=7)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None

    def test_null_fetched_at_leaves_queue_membership_unchanged(self, cur):
        """last_detail_fetched_at NULL → queue membership is exactly V040's.

        This is the entire table on the day of the migration, which is what
        makes V048 inert on its own.
        """
        artifact_id = _insert_artifact(cur)
        unenriched = _random_listing_id()
        price_stale = _random_listing_id()
        fresh = _random_listing_id()

        _insert_price_obs(cur, artifact_id, unenriched, customer_id=None, age_hours=1)
        _insert_price_obs(cur, artifact_id, price_stale, customer_id="cust-a", age_hours=25)
        _insert_price_obs(cur, artifact_id, fresh, customer_id="cust-b", age_hours=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue"
            " WHERE listing_id = ANY(%s::uuid[])",
            ([unenriched, price_stale, fresh],),
        )
        queued = {str(r["listing_id"]) for r in cur.fetchall()}
        assert queued == {unenriched, price_stale}

    def test_price_stale_listing_is_also_held_by_the_backoff(self, cur):
        """The backoff applies to the price_only pool too — it is a fetch guard,
        not an enrichment guard, and a price-stale fetch spends a request the
        same way."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id="cust-c", age_hours=25,
                          last_detail_fetched_at_hours_ago=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None

    def test_processing_outage_does_not_reclaim_within_the_window(self, cur):
        """The motivating scenario: fetch released, processing never ran.

        Claim deleted, no enrichment timestamp written, and under V040 the
        listing was re-queued on the next */15 run. With the fetch backoff it
        stays out for six hours and then returns once.
        """
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1)

        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None, "listing should be queued before any fetch"

        # release_claims stamps the fetch; processing is down, so nothing else
        # is written.
        cur.execute(
            "UPDATE ops.price_observations SET last_detail_fetched_at = now()"
            " WHERE listing_id = %s::uuid",
            (lid,),
        )
        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is None, "listing must not be re-claimed inside the backoff"

        # Six hours later, with processing still down, it is retried.
        cur.execute(
            "UPDATE ops.price_observations"
            " SET last_detail_fetched_at = now() - interval '7 hours'"
            " WHERE listing_id = %s::uuid",
            (lid,),
        )
        cur.execute(
            "SELECT listing_id FROM ops.ops_detail_scrape_queue WHERE listing_id = %s::uuid",
            (lid,),
        )
        assert cur.fetchone() is not None, "backoff is a delay, not a deletion"


# ---------------------------------------------------------------------------
# Fetch and enrichment are separate facts (Plan 147 / V049)
# ---------------------------------------------------------------------------

class TestFetchAndEnrichmentAreSeparateFacts:
    """The split V049 made permanent: spending a request and learning something
    from it are two facts with two owners, and neither stands in for the other.

    V048 carried a COALESCE over the legacy last_detail_scraped_at because the
    view deployed before the writers that set the new column. V049 dropped that
    column, so the tests for the dual-write window are gone with it; the 7-day
    circuit breaker is asserted through last_detail_enriched_at alone in
    TestOpsVehicleStaleness and TestCircuitBreakerQueue above.
    """

    def test_fetch_does_not_advance_enrichment(self, cur):
        """A spent request is not enrichment: last_detail_fetched_at alone
        leaves is_full_details_stale true, so the listing returns once the
        backoff expires rather than being suppressed for seven days."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_fetched_at_hours_ago=7)

        cur.execute(
            "SELECT is_full_details_stale, stale_reason"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["is_full_details_stale"] is True
        assert row["stale_reason"] == "dealer_unenriched"

    def test_new_columns_exposed_in_view(self, cur):
        """Both new columns are returned from the staleness view for diagnostics
        and for the Stage 3 fetched-but-unenriched gauge."""
        artifact_id = _insert_artifact(cur)
        lid = _random_listing_id()
        _insert_price_obs(cur, artifact_id, lid, customer_id=None, age_hours=1,
                          last_detail_fetched_at_hours_ago=2,
                          last_detail_enriched_at_hours_ago=3)

        cur.execute(
            "SELECT last_detail_fetched_at, last_detail_enriched_at"
            " FROM ops.ops_vehicle_staleness WHERE listing_id = %s::uuid",
            (lid,),
        )
        row = cur.fetchone()
        assert row["last_detail_fetched_at"] is not None
        assert row["last_detail_enriched_at"] is not None
