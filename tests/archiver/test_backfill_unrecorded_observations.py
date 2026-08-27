# ruff: noqa: E501
import hashlib

import pytest

from archiver.processors import backfill_unrecorded_observations as backfill


def _row(content):
    return {
        "ledger_fingerprint": "ledger",
        "disposition": "recover_car19",
        "http_status": 200,
        "source_key": "html/key",
        "sha256": hashlib.sha256(content).hexdigest(),
        "listing_id": "listing",
        "fetched_at": "2026-04-21T01:02:03+00:00",
        "artifact_id": 9,
        "url": "https://www.cars.com/vehicledetail/listing/",
    }


def test_process_row_is_dry_run_by_default_and_validates_identity(monkeypatch):
    content = b"page"
    monkeypatch.setattr(backfill, "read_html", lambda _key: content)
    monkeypatch.setattr(
        backfill,
        "parse_cars_detail_page_html_v1",
        lambda *_args: ({"listing_id": "listing", "listing_state": "active"}, [], {}),
    )
    result = backfill.process_row(_row(content), "ledger", apply=False)
    assert result == {"listing_id": "listing", "state": "active", "written": False}


def test_process_row_refuses_a_hash_mismatch(monkeypatch):
    monkeypatch.setattr(backfill, "read_html", lambda _key: b"wrong")
    with pytest.raises(backfill.BackfillRefusal, match="sha256"):
        backfill.process_row(_row(b"expected"), "ledger", apply=False)
