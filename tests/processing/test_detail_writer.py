"""Unit tests for processing/writers/detail_writer.py.

Covers active, unlisted, and blocked paths plus carousel filtering
and VIN relisting logic.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from processing.writers.detail_writer import (
    write_detail_active,
    write_detail_unlisted,
)

FETCHED_AT = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def mock_cursor():
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.fetchone.return_value = None
    cursor.rowcount = 0

    from contextlib import contextmanager

    @contextmanager
    def fake_db_cursor(error_context="", dict_cursor=False):
        yield cursor

    with patch("processing.writers.detail_writer.db_cursor", fake_db_cursor):
        yield cursor


@pytest.fixture
def mock_silver(mocker):
    return mocker.patch(
        "processing.writers.detail_writer.write_silver_observations_postgres", return_value=1
    )


@pytest.fixture
def mock_search_configs(mocker):
    """Patch _get_tracked_models to return a known set of (make, model)."""
    mocker.patch(
        "processing.writers.detail_writer._get_tracked_models",
        return_value={("honda", "accord"), ("toyota", "camry")},
    )



# ---------------------------------------------------------------------------
# _clear_cooldown
# ---------------------------------------------------------------------------

class TestClearCooldown:
    def test_emits_cleared_event_when_row_removed(self):
        from processing.queries import INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT
        from processing.writers.detail_writer import _clear_cooldown

        cur = MagicMock()
        cur.fetchone.return_value = (3,)  # a row was deleted, attempts=3

        _clear_cooldown(cur, "listing-1")

        insert_calls = [
            c for c in cur.execute.call_args_list
            if c[0][0] == INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT
        ]
        assert len(insert_calls) == 1
        assert insert_calls[0][0][1] == {"listing_id": "listing-1", "num_of_attempts": 3}

    def test_no_event_when_nothing_cleared(self):
        from processing.queries import INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT
        from processing.writers.detail_writer import _clear_cooldown

        cur = MagicMock()
        cur.fetchone.return_value = None  # listing was not blocked

        _clear_cooldown(cur, "listing-1")

        insert_calls = [
            c for c in cur.execute.call_args_list
            if c[0][0] == INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT
        ]
        assert insert_calls == []


# ---------------------------------------------------------------------------
# Active path
# ---------------------------------------------------------------------------

class TestWriteDetailActive:
    def test_active_upserts_primary(self, mock_cursor, mock_silver, mock_search_configs):
        primary = {
            "listing_id": "aaa", "vin": "VIN001", "price": 25000,
            "make": "Honda", "model": "CR-V", "mileage": 30000,
        }
        result = write_detail_active(
            primary, carousel=[], artifact_id=1, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run1",
        )
        assert result["upserted"] == 1
        assert result["vin"] == "VIN001"

    def test_vin_relisting_deletes_old_row(self, mock_cursor, mock_silver, mock_search_configs):
        # VIN collision: VIN001 exists at listing BBB
        mock_cursor.fetchall.return_value = []  # batch lookup
        mock_cursor.fetchone.side_effect = [
            ("old-listing-id", "VIN001"),  # collision found
            None,  # blocked cooldown attempts (not reached)
        ]

        primary = {
            "listing_id": "new-listing", "vin": "VIN001",
            "price": 30000, "make": "Honda", "model": "Accord",
        }
        result = write_detail_active(
            primary, carousel=[], artifact_id=2, fetched_at=FETCHED_AT,
            listing_id="new-listing", run_id="run2",
        )
        assert result["vin_collision_deleted"] is True

    def test_carousel_filtered_by_search_configs(
        self, mock_cursor, mock_silver, mock_search_configs,
    ):
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 20000,
            "make": "Honda", "model": "CR-V",
        }
        carousel = [
            {"listing_id": "c1", "price": 18000,
             "body": "Used 2023 Honda Accord EX", "mileage": 5000},
            {"listing_id": "c2", "price": 40000, "body": "New 2025 BMW X5 xDrive", "mileage": 10},
        ]
        result = write_detail_active(
            primary, carousel=carousel, artifact_id=3, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run3",
        )
        # Honda matches, BMW does not
        assert result["carousel_upserted"] == 1
        assert result["carousel_filtered"] == 1

    def test_carousel_unmatched_goes_to_silver(self, mock_cursor, mock_silver, mock_search_configs):
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 20000,
            "make": "Honda", "model": "CR-V",
        }
        carousel = [
            {"listing_id": "c1", "price": 40000, "body": "New 2025 BMW X5 xDrive", "mileage": 10},
        ]
        write_detail_active(
            primary, carousel=carousel, artifact_id=4, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run4",
        )
        # Silver should include both primary and the unmatched carousel hint
        silver_rows = mock_silver.call_args[0][0]
        assert len(silver_rows) == 2
        sources = {r["source"] for r in silver_rows}
        assert sources == {"detail", "carousel"}

    def test_carousel_vin_from_lookup(self, mock_cursor, mock_silver, mock_search_configs):
        # Batch lookup returns VIN for carousel listing
        mock_cursor.fetchall.return_value = [{"listing_id": "c1", "vin": "CAROUSEL_VIN"}]
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 20000,
            "make": "Honda", "model": "CR-V",
        }
        carousel = [
            {"listing_id": "c1", "price": 15000,
             "body": "Used 2022 Toyota Camry LE", "mileage": 40000},
        ]
        write_detail_active(
            primary, carousel=carousel, artifact_id=5, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run5",
        )
        # Silver row for carousel should have the looked-up VIN
        silver_rows = mock_silver.call_args[0][0]
        carousel_row = [r for r in silver_rows if r["source"] == "carousel"][0]
        assert carousel_row["vin"] == "CAROUSEL_VIN"

    def test_carousel_vin_null_when_unknown(self, mock_cursor, mock_silver, mock_search_configs):
        mock_cursor.fetchall.return_value = []  # no lookup results
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 20000,
            "make": "Honda", "model": "CR-V",
        }
        carousel = [
            {"listing_id": "c1", "price": 15000,
             "body": "Used 2022 Toyota Camry LE", "mileage": 40000},
        ]
        write_detail_active(
            primary, carousel=carousel, artifact_id=6, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run6",
        )
        silver_rows = mock_silver.call_args[0][0]
        carousel_row = [r for r in silver_rows if r["source"] == "carousel"][0]
        assert carousel_row["vin"] is None

    def test_carousel_sanity_filter_drops_null_price(
        self, mock_cursor, mock_silver, mock_search_configs,
    ):
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 20000,
            "make": "Honda", "model": "CR-V",
        }
        carousel = [
            {"listing_id": "c1", "price": None, "body": "Used 2022 Honda Civic", "mileage": 5000},
        ]
        result = write_detail_active(
            primary, carousel=carousel, artifact_id=7, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run7",
        )
        assert result["carousel_upserted"] == 0
        assert result["carousel_filtered"] == 0  # filtered by sanity, not search_config

    def test_carousel_sanity_filter_drops_null_body(
        self, mock_cursor, mock_silver, mock_search_configs,
    ):
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 20000,
            "make": "Honda", "model": "CR-V",
        }
        carousel = [
            {"listing_id": "c1", "price": 15000, "body": None, "mileage": 5000},
        ]
        result = write_detail_active(
            primary, carousel=carousel, artifact_id=8, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run8",
        )
        assert result["carousel_upserted"] == 0


# ---------------------------------------------------------------------------
# Unlisted path
# ---------------------------------------------------------------------------

class TestWriteDetailUnlisted:
    def test_unlisted_produces_delete(self, mock_cursor, mock_silver):
        primary = {
            "listing_id": "aaa", "vin": "VIN001",
            "listing_state": "unlisted",
            "make": "Honda", "model": "CR-V",
        }
        result = write_detail_unlisted(
            primary, artifact_id=10, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run10",
        )
        assert result["deleted"] is True

        # Verify DELETE was called (first execute call should be the delete)
        first_execute = mock_cursor.execute.call_args_list[0]
        assert "DELETE" in first_execute[0][0]

    def test_unlisted_writes_silver_with_null_price(self, mock_cursor, mock_silver):
        primary = {"listing_id": "bbb", "vin": None, "make": "Toyota", "model": "Camry"}
        write_detail_unlisted(
            primary, artifact_id=11, fetched_at=FETCHED_AT,
            listing_id="bbb", run_id="run11",
        )
        silver_rows = mock_silver.call_args[0][0]
        assert silver_rows[0]["price"] is None
        assert silver_rows[0]["listing_state"] == "unlisted"


# ---------------------------------------------------------------------------
# Circuit-breaker: last_detail_scraped_at writer tests (Plan 115)
# ---------------------------------------------------------------------------

class TestLastDetailScrapedAt:
    """Verify last_detail_scraped_at is set on primary detail writes and absent
    from carousel and SRP writes."""

    def _upsert_calls(self, mock_cursor) -> list:
        """Return all execute calls that touch price_observations."""
        return [
            c for c in mock_cursor.execute.call_args_list
            if "price_observations" in str(c)
        ]

    def _find_upsert_params(self, mock_cursor, source_hint: str = None) -> list[dict]:
        """Return the params dicts passed to UPSERT_PRICE_OBSERVATION calls."""
        params = []
        for call in mock_cursor.execute.call_args_list:
            args = call[0]
            if len(args) >= 2 and isinstance(args[1], dict):
                d = args[1]
                if "last_seen_at" in d and "listing_id" in d:
                    params.append(d)
        return params

    def test_primary_detail_sets_last_detail_scraped_at(
        self, mock_cursor, mock_silver, mock_search_configs,
    ):
        """Primary detail active write passes last_detail_scraped_at = fetched_at."""
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 25000,
            "make": "Honda", "model": "CR-V", "customer_id": "cust-1",
        }
        write_detail_active(
            primary, carousel=[], artifact_id=1, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run1",
        )
        upsert_params = self._find_upsert_params(mock_cursor)
        assert len(upsert_params) >= 1
        primary_params = upsert_params[0]
        assert primary_params["last_detail_scraped_at"] == FETCHED_AT

    def test_primary_detail_sets_last_detail_scraped_at_when_customer_id_null(
        self, mock_cursor, mock_silver, mock_search_configs,
    ):
        """Primary detail active write sets last_detail_scraped_at even when customer_id is NULL."""
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 25000,
            "make": "Honda", "model": "CR-V", "customer_id": None,
        }
        write_detail_active(
            primary, carousel=[], artifact_id=2, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run2",
        )
        upsert_params = self._find_upsert_params(mock_cursor)
        primary_params = upsert_params[0]
        assert primary_params["customer_id"] is None
        assert primary_params["last_detail_scraped_at"] == FETCHED_AT

    def test_carousel_upsert_does_not_set_last_detail_scraped_at(
        self, mock_cursor, mock_silver, mock_search_configs,
    ):
        """Carousel upserts pass last_detail_scraped_at = None."""
        primary = {
            "listing_id": "aaa", "vin": "V1", "price": 25000,
            "make": "Honda", "model": "CR-V",
        }
        carousel = [
            {"listing_id": "c1", "price": 22000,
             "body": "Used 2022 Honda Accord EX", "mileage": 10000},
        ]
        write_detail_active(
            primary, carousel=carousel, artifact_id=3, fetched_at=FETCHED_AT,
            listing_id="aaa", run_id="run3",
        )
        upsert_params = self._find_upsert_params(mock_cursor)
        # First params = primary (last_detail_scraped_at set), rest = carousel
        carousel_params = [p for p in upsert_params if p["listing_id"] == "c1"]
        assert len(carousel_params) == 1
        assert carousel_params[0]["last_detail_scraped_at"] is None

