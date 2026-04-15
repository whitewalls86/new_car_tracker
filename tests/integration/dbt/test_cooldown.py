import datetime

import pytest

pytestmark = pytest.mark.integration


def _seed_cooldown(cur, listing_id: str, num_of_attempts: int):
        cur.execute("""
                    INSERT INTO public.blocked_cooldown
                        (listing_id, first_attempt_at, last_attempted_at, num_of_attempts)
                    VALUES (%s, now() - interval '1 day', now(), %s)
                    """, (listing_id, num_of_attempts))
        

def test_not_blocked_attempts_1_through_4(cur):
    """Attempts 1-4: fully_blocked = False, next_eligible_at is correct"""
    cases = [
        ("cooldown_test_1", 1 , datetime.timedelta(hours=12)), # 12 * 2^0
        ("cooldown_test_2", 2 , datetime.timedelta(hours=24)), # 12 * 2^0
        ("cooldown_test_3", 3 , datetime.timedelta(hours=48)), # 12 * 2^0
        ("cooldown_test_4", 4 , datetime.timedelta(hours=96)), # 12 * 2^0
    ]

    for listing_id, attempts, expected_delta in cases:
        _seed_cooldown(cur, listing_id, attempts)
    
    cur.execute(
        """
        SELECT listing_id, last_attempted_at, next_eligible_at, fully_blocked
        FROM analytics_ci.stg_blocked_cooldown
        WHERE listing_id IN (
            'cooldown_test_1','cooldown_test_2','cooldown_test_3','cooldown_test_4'
        )
        ORDER BY num_of_attempts
        """
    )
    rows = {r["listing_id"]: r for r in cur.fetchall()}

    for listing_id, attempts, expected_delta in cases:
        row = rows[listing_id]
        assert row["fully_blocked"] is False, f"attempts={attempts} should not be fully blocked"
        expected_time = row["last_attempted_at"] + expected_delta
        diff = abs(row["next_eligible_at"] - expected_time)
        assert diff.total_seconds() < 1, (
            f"attempts={attempts}: next_eligible_at off by {diff}"
        )


def test_fully_blocked_at_5_and_above(cur):
    """Attempts >= 5: fully_blocked=true, next_eligible_at=NULL."""
    for listing_id, attempts in [("cooldown_test_5", 5), ("cooldown_test_6", 6)]:
        _seed_cooldown(cur, listing_id, attempts)

    cur.execute(
        """
        SELECT listing_id, next_eligible_at, fully_blocked
        FROM analytics_ci.stg_blocked_cooldown
        WHERE listing_id IN ('cooldown_test_5', 'cooldown_test_6')
        """
    )
    rows = cur.fetchall()
    assert len(rows) == 2
    for row in rows:
        assert row["fully_blocked"] is True
        assert row["next_eligible_at"] is None
