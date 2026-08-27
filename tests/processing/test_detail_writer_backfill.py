from contextlib import contextmanager
from datetime import datetime, timezone

from processing.writers import detail_writer


@contextmanager
def _cursor(_error_context):
    yield _Cursor()


class _Cursor:
    def __init__(self):
        self.executed = []

    def execute(self, query, params):
        self.executed.append((query, params))


def _primary():
    return {"vin": "VIN123", "price": 22000, "make": "Honda", "model": "Civic"}


def test_active_backfill_uses_only_silver_and_historical_event(monkeypatch):
    cur = _Cursor()

    @contextmanager
    def cursor(**_kwargs):
        yield cur

    rows = []
    monkeypatch.setattr(detail_writer, "db_cursor", cursor)
    monkeypatch.setattr(
        detail_writer,
        "write_silver_observations_with_cursor",
        lambda c, value: rows.extend(value) or 1,
    )
    result = detail_writer.write_detail_active(
        _primary(),
        [{"listing_id": "carousel"}],
        9,
        datetime(2026, 4, 21, tzinfo=timezone.utc),
        "listing",
        None,
        backfill=True,
    )

    assert result["backfill"] is True
    assert len(rows) == 1
    assert rows[0]["listing_id"] == "listing"
    assert len(cur.executed) == 1
    assert cur.executed[0][1]["event_type"] == "upserted"
    assert cur.executed[0][1]["event_at"] == datetime(2026, 4, 21, tzinfo=timezone.utc)


def test_unlisted_backfill_never_looks_up_or_mutates_live_state(monkeypatch):
    cur = _Cursor()

    @contextmanager
    def cursor(**_kwargs):
        yield cur

    monkeypatch.setattr(detail_writer, "db_cursor", cursor)
    monkeypatch.setattr(detail_writer, "write_silver_observations_with_cursor", lambda _c, _rows: 1)
    result = detail_writer.write_detail_unlisted(
        _primary(),
        9,
        datetime(2026, 4, 21, tzinfo=timezone.utc),
        "listing",
        None,
        backfill=True,
    )

    assert result["deleted"] is True
    assert len(cur.executed) == 1
    assert cur.executed[0][1]["event_type"] == "deleted"
