"""Unit tests for processing/writers/srp_writer.py.

All DB calls are mocked via db_cursor patch. Tests verify the correct SQL
is called with the correct parameters for each scenario.
"""
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest

from processing.writers.srp_writer import write_srp_observations


@pytest.fixture
def mock_cursor():
    """Yields a mock cursor and patches db_cursor to return it."""
    cursor = MagicMock()
    cursor.fetchall.return_value = []
    cursor.rowcount = 0

    from contextlib import contextmanager

    @contextmanager
    def fake_db_cursor(error_context="", dict_cursor=False):
        yield cursor

    with patch("processing.writers.srp_writer.db_cursor", fake_db_cursor):
        yield cursor


@pytest.fixture
def mock_silver(mocker):
    return mocker.patch(
        "processing.writers.srp_writer.write_silver_observations_postgres", return_value=3
    )


FETCHED_AT = datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc)


class TestSrpWriter:
    def test_listing_with_vin_upserts_price_obs(self, mock_cursor, mock_silver):
        listings = [
            {"listing_id": "aaa", "vin": "VIN001", "price": 25000,
             "make": "Honda", "model": "CR-V"},
        ]
        result = write_srp_observations(listings, artifact_id=1, fetched_at=FETCHED_AT)

        assert result["upserted"] == 1
        # Verify UPSERT_PRICE_OBSERVATION was called
        calls = mock_cursor.execute.call_args_list
        upsert_calls = [c for c in calls if "price_observations" in str(c)]
        assert len(upsert_calls) >= 1

    def test_listing_without_vin_uses_lookup(self, mock_cursor, mock_silver):
        # First call returns VIN lookup, subsequent calls are upserts
        mock_cursor.fetchall.return_value = [{"listing_id": "aaa", "vin": "LOOKUP_VIN"}]
        listings = [
            {"listing_id": "aaa", "vin": None, "price": 30000,
             "make": "Toyota", "model": "RAV4"},
        ]

        result = write_srp_observations(listings, artifact_id=2, fetched_at=FETCHED_AT)
        assert result["upserted"] == 1

        # Silver should get the resolved VIN
        silver_call = mock_silver.call_args[0][0]
        assert silver_call[0]["vin"] == "LOOKUP_VIN"

    def test_srp_source_is_srp(self, mock_cursor, mock_silver):
        listings = [
            {"listing_id": "bbb", "vin": "V1", "price": 10000,
             "make": "Ford", "model": "F-150"},
        ]
        write_srp_observations(listings, artifact_id=3, fetched_at=FETCHED_AT)

        silver_call = mock_silver.call_args[0][0]
        assert all(row["source"] == "srp" for row in silver_call)

    def test_vin_to_listing_recency_guard(self, mock_cursor, mock_silver):
        """VIN upsert uses mapped_at = fetched_at; SQL has WHERE clause for recency."""
        mock_cursor.fetchall.return_value = []
        mock_cursor.rowcount = 1  # indicates the upsert succeeded (newer)
        listings = [
            {"listing_id": "ccc", "vin": "VIN002", "price": 20000,
             "make": "Honda", "model": "Accord"},
        ]

        result = write_srp_observations(listings, artifact_id=4, fetched_at=FETCHED_AT)
        assert result["vin_mapped"] == 1

    def test_vin_to_listing_not_updated_when_older(self, mock_cursor, mock_silver):
        """When rowcount is 0 after vin_to_listing upsert, mapping was not updated (older)."""
        mock_cursor.fetchall.return_value = []
        mock_cursor.rowcount = 0  # recency guard prevented update
        listings = [
            {"listing_id": "ddd", "vin": "VIN003", "price": 15000,
             "make": "Toyota", "model": "Camry"},
        ]

        result = write_srp_observations(listings, artifact_id=5, fetched_at=FETCHED_AT)
        assert result["vin_mapped"] == 0

    def test_empty_listings_returns_zeros(self, mock_cursor, mock_silver):
        result = write_srp_observations([], artifact_id=6, fetched_at=FETCHED_AT)
        assert result == {"upserted": 0, "vin_mapped": 0, "silver_written": 0}

    def test_listing_without_listing_id_skipped(self, mock_cursor, mock_silver):
        listings = [{"listing_id": None, "vin": "V1", "price": 5000}]
        result = write_srp_observations(
            listings, artifact_id=7, fetched_at=FETCHED_AT,
        )
        assert result["upserted"] == 0

    def test_tracked_models_upserted_when_search_key(
        self, mock_cursor, mock_silver,
    ):
        listings = [
            {"listing_id": "aaa", "vin": "V1", "price": 25000,
             "make": "Honda", "model": "CR-V"},
            {"listing_id": "bbb", "vin": "V2", "price": 30000,
             "make": "Honda", "model": "Accord"},
        ]
        write_srp_observations(
            listings, artifact_id=8, fetched_at=FETCHED_AT,
            search_key="honda-cr_v",
        )
        # Verify UPSERT_TRACKED_MODEL was called for distinct models
        calls = mock_cursor.execute.call_args_list
        tracked_calls = [
            c for c in calls if "tracked_models" in str(c)
        ]
        assert len(tracked_calls) >= 2  # at least one per distinct model

    def test_tracked_models_skipped_without_search_key(
        self, mock_cursor, mock_silver,
    ):
        listings = [
            {"listing_id": "aaa", "vin": "V1", "price": 25000,
             "make": "Honda", "model": "CR-V"},
        ]
        write_srp_observations(
            listings, artifact_id=9, fetched_at=FETCHED_AT,
        )
        calls = mock_cursor.execute.call_args_list
        tracked_calls = [
            c for c in calls if "tracked_models" in str(c)
        ]
        assert len(tracked_calls) == 0
