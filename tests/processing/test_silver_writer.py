"""Unit tests for processing/writers/silver_writer.py.

DB calls (execute_values) are patched. Tests verify row construction logic —
field mapping, type coercion, and the non-raising failure contract.
"""
from contextlib import contextmanager
from datetime import datetime, timezone
from unittest.mock import MagicMock


def _make_obs(**overrides):
    base = {
        "artifact_id": 1,
        "listing_id": "aaaa-0000-0000-0000-000000000001",
        "vin": "1HGCM82633A123456",
        "canonical_detail_url": "https://www.cars.com/vehicledetail/aaaa-0000-0000-0000-000000000001/",
        "source": "cars.com",
        "listing_state": "active",
        "fetched_at": datetime(2026, 4, 20, 12, 0, 0, tzinfo=timezone.utc),
        "price": 28000,
        "make": "Honda",
        "model": "Accord",
        "trim": "Sport",
        "year": 2024,
        "mileage": 5000,
        "msrp": 29500,
        "stock_type": "new",
        "fuel_type": "Gasoline",
        "body_style": "Sedan",
        "dealer_name": "Best Auto",
        "dealer_zip": "77002",
        "customer_id": "cust-77",
        "seller_id": "seller-42",
        "dealer_street": None,
        "dealer_city": None,
        "dealer_state": None,
        "dealer_phone": None,
        "dealer_website": None,
        "dealer_cars_com_url": None,
        "dealer_rating": None,
        "financing_type": None,
        "seller_zip": None,
        "seller_customer_id": None,
        "page_number": None,
        "position_on_page": None,
        "trid": None,
        "isa_context": None,
        "body": None,
        "condition": None,
    }
    base.update(overrides)
    return base


@contextmanager
def _fake_cursor(cursor):
    yield cursor


class TestWriteSilverObservationsPostgres:
    def _patched_write(self, mocker, observations):
        from processing.writers.silver_writer import write_silver_observations_postgres

        cursor = MagicMock()
        cursor.rowcount = len(observations)
        mocker.patch(
            "processing.writers.silver_writer.db_cursor",
            side_effect=lambda **kw: _fake_cursor(cursor),
        )
        mock_ev = mocker.patch("psycopg2.extras.execute_values")
        result = write_silver_observations_postgres(observations)
        return result, cursor, mock_ev

    def test_empty_list_returns_zero_no_db_call(self, mocker):
        from processing.writers.silver_writer import write_silver_observations_postgres

        mock_ev = mocker.patch("psycopg2.extras.execute_values")
        result = write_silver_observations_postgres([])
        assert result == 0
        mock_ev.assert_not_called()

    def test_single_observation_returns_rowcount(self, mocker):
        result, _, _ = self._patched_write(mocker, [_make_obs()])
        assert result == 1

    def test_calls_execute_values_once(self, mocker):
        _, _, mock_ev = self._patched_write(mocker, [_make_obs()])
        assert mock_ev.call_count == 1

    def test_fetched_at_string_parsed_to_datetime(self, mocker):
        obs = _make_obs(fetched_at="2026-04-20T12:00:00+00:00")
        _, _, mock_ev = self._patched_write(mocker, [obs])
        rows = mock_ev.call_args[0][2]  # positional arg: rows list
        # fetched_at is index 6 in _POSTGRES_COLS
        from processing.writers.silver_writer import _POSTGRES_COLS
        idx = _POSTGRES_COLS.index("fetched_at")
        assert isinstance(rows[0][idx], datetime)

    def test_naive_fetched_at_gets_utc_tzinfo(self, mocker):
        obs = _make_obs(fetched_at=datetime(2026, 4, 20, 12, 0, 0))  # no tzinfo
        _, _, mock_ev = self._patched_write(mocker, [obs])
        rows = mock_ev.call_args[0][2]
        from processing.writers.silver_writer import _POSTGRES_COLS
        idx = _POSTGRES_COLS.index("fetched_at")
        assert rows[0][idx].tzinfo is not None

    def test_listing_id_coerced_to_str(self, mocker):
        obs = _make_obs(listing_id="aaaa-0000-0000-0000-000000000001")
        _, _, mock_ev = self._patched_write(mocker, [obs])
        rows = mock_ev.call_args[0][2]
        from processing.writers.silver_writer import _POSTGRES_COLS
        idx = _POSTGRES_COLS.index("listing_id")
        assert isinstance(rows[0][idx], str)

    def test_listing_state_defaults_to_active_when_none(self, mocker):
        obs = _make_obs(listing_state=None)
        _, _, mock_ev = self._patched_write(mocker, [obs])
        rows = mock_ev.call_args[0][2]
        from processing.writers.silver_writer import _POSTGRES_COLS
        idx = _POSTGRES_COLS.index("listing_state")
        assert rows[0][idx] == "active"

    def test_empty_string_int_fields_coerced_to_none(self, mocker):
        obs = _make_obs(price="", year="", mileage="")
        _, _, mock_ev = self._patched_write(mocker, [obs])
        rows = mock_ev.call_args[0][2]
        from processing.writers.silver_writer import _POSTGRES_COLS
        for field in ("price", "year", "mileage"):
            idx = _POSTGRES_COLS.index(field)
            assert rows[0][idx] is None, f"{field} should be None for empty string"

    def test_float_int_fields_cast_to_int(self, mocker):
        obs = _make_obs(price=35499.99, year=2024.0, mileage=15000.0)
        _, _, mock_ev = self._patched_write(mocker, [obs])
        rows = mock_ev.call_args[0][2]
        from processing.writers.silver_writer import _POSTGRES_COLS
        for field, expected in (("price", 35499), ("year", 2024), ("mileage", 15000)):
            idx = _POSTGRES_COLS.index(field)
            assert rows[0][idx] == expected
            assert isinstance(rows[0][idx], int), f"{field} should be int, got {type(rows[0][idx])}"

    def test_db_failure_returns_zero_does_not_raise(self, mocker):
        from processing.writers.silver_writer import write_silver_observations_postgres

        mocker.patch(
            "psycopg2.extras.execute_values",
            side_effect=Exception("connection refused"),
        )
        cursor = MagicMock()
        mocker.patch(
            "processing.writers.silver_writer.db_cursor",
            side_effect=lambda **kw: _fake_cursor(cursor),
        )
        result = write_silver_observations_postgres([_make_obs()])
        assert result == 0

    def test_multiple_observations_all_written(self, mocker):
        obs_list = [_make_obs(artifact_id=i) for i in range(1, 4)]
        cursor = MagicMock()
        cursor.rowcount = 3
        mocker.patch(
            "processing.writers.silver_writer.db_cursor",
            side_effect=lambda **kw: _fake_cursor(cursor),
        )
        mock_ev = mocker.patch("psycopg2.extras.execute_values")
        from processing.writers.silver_writer import write_silver_observations_postgres
        result = write_silver_observations_postgres(obs_list)
        rows = mock_ev.call_args[0][2]
        assert len(rows) == 3
        assert result == 3
