# ruff: noqa: E501
import hashlib
import json
from datetime import datetime, timedelta, timezone

import pytest

from archiver.processors import backfill_unrecorded_observations as backfill
from scripts import build_april_ledger as ledger


def _legacy(key, content, status, listing_id=None, fetched_at="2026-04-21T12:00:00+00:00", **extra):
    return {
        "legacy_key": f"artifacts/year=2026/month=4/{key}.parquet",
        "row_group": 0,
        "row_offset": 0,
        "artifact_id": extra.pop("artifact_id", 999),
        "content_bytes": content,
        "sha256": hashlib.sha256(content).hexdigest() if content else None,
        "http_status": status,
        "listing_id": listing_id,
        "fetched_at": fetched_at,
        "legacy_object_size": 123,
        "legacy_etag": '"etag"',
        "legacy_last_modified": "2026-08-21T00:00:00+00:00",
        **extra,
    }


def _sidecar(content, *, artifact_id=None, source_key="html/source"):
    return {
        "raw_sha256": hashlib.sha256(content).hexdigest(),
        "artifact_id": artifact_id,
        "source_key": source_key,
        "pack_key": "html_packs/detail_page/2026/04/pack-00000.zpack",
        "sidecar_key": "html_packs/detail_page/2026/04/pack-00000.idx.parquet",
    }


def _corpus():
    current = b"current"
    orphan_200 = b"orphan-200"
    orphan_403 = b"orphan-403"
    no_price = b"no-price"
    absent = b"absent"
    changed = b"changed"
    stable = b"stable"
    five_xx = b"five-xx"
    mismatch = b"mismatch"
    legacy_rows = [
        _legacy("current", current, 200, "current", artifact_id=999),
        _legacy("orphan-200", orphan_200, 200, "orphan"),
        _legacy("orphan-403", orphan_403, 403),
        _legacy("no-price", no_price, 200, "no-price"),
        _legacy("absent", absent, 200, "absent"),
        _legacy("changed", changed, 200, "changed"),
        _legacy("stable", stable, 200, "stable"),
        _legacy("five-xx", five_xx, 503),
        _legacy("empty", b"", 200),
        _legacy("mismatch", mismatch, 200, "mismatch", sha256="0" * 64),
    ]
    sidecars = [
        _sidecar(current, artifact_id=700, source_key="html/current"),
        _sidecar(orphan_200, source_key="html/orphan-200"),
        _sidecar(orphan_403, source_key="html/orphan-403"),
    ]
    base = datetime(2026, 4, 21, 12, tzinfo=timezone.utc)
    silver_rows = [
        {"listing_id": "current", "fetched_at": base, "price": 100},
        {"listing_id": "no-price", "fetched_at": base - timedelta(hours=1), "price": None},
        {"listing_id": "changed", "fetched_at": base - timedelta(hours=1), "price": 100},
        {"listing_id": "changed", "fetched_at": base + timedelta(hours=1), "price": 90},
        {"listing_id": "stable", "fetched_at": base - timedelta(hours=1), "price": 100},
        {"listing_id": "stable", "fetched_at": base + timedelta(hours=1), "price": 100},
        {"listing_id": "mismatch", "fetched_at": base, "price": 100},
    ]
    return legacy_rows, sidecars, [{"artifact_id": 700}, {"artifact_id": 999}], silver_rows


def test_full_miniature_corpus_reconciles_every_row_and_never_joins_legacy_artifact_id():
    legacy_rows, sidecars, queue_events, silver_rows = _corpus()
    rows = ledger.build_ledger(
        legacy_rows, sidecars, queue_events=queue_events, silver_rows=silver_rows
    )
    by_listing = {row["listing_id"]: row for row in rows if row["listing_id"]}
    dispositions = {row["disposition"] for row in rows}

    assert len(rows) == len(legacy_rows)
    assert {
        "recover_car19",
        "recover_car20",
        "preserve_car21",
        "redundant",
        "challenge_page",
        "empty",
        "unresolved",
    } <= dispositions
    assert (
        by_listing["current"]["queue_event_count"] == 1
    )  # current sidecar ID 700, not legacy ID 999
    assert by_listing["current"]["silver_match_count"] == 1
    assert by_listing["orphan"]["disposition"] == "recover_car19"
    assert by_listing["no-price"]["information_value_cohorts"] == ["no_priced_observation"]
    assert by_listing["absent"]["information_value_cohorts"] == [
        "listing_absent_from_silver",
        "no_priced_observation",
    ]
    assert by_listing["absent"]["preservation_reasons"] == ["listing_absent_from_silver"]
    assert by_listing["changed"]["information_value_cohorts"] == ["bracketed_price_changed"]
    assert by_listing["stable"]["disposition"] == "redundant"
    assert by_listing["mismatch"]["unresolved_reason"] == "stored_sha256_mismatch"
    assert ledger.canonical_fingerprint(rows) == ledger.canonical_fingerprint(list(rows))


def test_fixture_driver_writes_deterministic_parquet_report_and_fingerprint(tmp_path):
    legacy_rows, sidecars, queue_events, silver_rows = _corpus()
    fixture = tmp_path / "fixture.json"
    fixture.write_text(
        json.dumps(
            {
                "legacy_rows": [
                    {**row, "content_bytes": row["content_bytes"].decode()} for row in legacy_rows
                ],
                "sidecars": sidecars,
                "queue_events": queue_events,
                "silver_rows": [
                    {**row, "fetched_at": row["fetched_at"].isoformat()} for row in silver_rows
                ],
            }
        )
    )
    output = tmp_path / "ledger.parquet"

    assert ledger.main(["--fixture", str(fixture), "--output", str(output)]) == 0
    fingerprint = output.with_suffix(".sha256").read_text().strip()
    report = json.loads(output.with_suffix(".report.json").read_text())

    assert output.exists()
    assert output.with_suffix(".report.md").exists()
    assert report["fingerprint"] == fingerprint
    assert report["rows"] == len(legacy_rows)


def test_car19_shaped_manifest_from_frozen_fixture_ledger_is_dry_run_only(monkeypatch):
    legacy_rows, sidecars, queue_events, silver_rows = _corpus()
    rows = ledger.build_ledger(
        legacy_rows,
        sidecars,
        queue_events=queue_events,
        silver_rows=silver_rows,
    )
    ledger_fingerprint = ledger.canonical_fingerprint(rows)
    row = next(row for row in rows if row["disposition"] == "recover_car19")
    manifest_row = {**row, "ledger_fingerprint": ledger_fingerprint}
    source = next(item for item in legacy_rows if item["listing_id"] == "orphan")
    monkeypatch.setattr(backfill, "read_html", lambda _key: source["content_bytes"])
    monkeypatch.setattr(
        backfill,
        "parse_cars_detail_page_html_v1",
        lambda *_args: ({"listing_id": "orphan", "listing_state": "active"}, [], {}),
    )

    receipt = backfill.run_manifest(
        [manifest_row], ledger_fingerprint=ledger_fingerprint, apply=False, cap=1
    )

    assert receipt["attempted"] == 1
    assert receipt["dry_run"] == 1
    assert receipt["results"][0]["written"] is False


def test_production_baseline_fails_closed_on_any_documented_drift():
    legacy_rows, sidecars, queue_events, silver_rows = _corpus()
    rows = ledger.build_ledger(
        legacy_rows, sidecars, queue_events=queue_events, silver_rows=silver_rows
    )
    with pytest.raises(ledger.LedgerDriftError, match="baseline drift"):
        ledger.assert_baseline(rows, object_count=1, sidecars=sidecars)
