"""Unit tests for scripts/reconcile_april_detail.py (Plan 145, Stage 1).

Stage 1 is the run that decides whether 13.66 GiB of legacy Parquet may
eventually be deleted, so these tests are about the *gates* holding, not about
the scan executing:

  A - legacy identity: listing_id from URL, status bucketing, time normalization
  B - the empty-body case, which the Plan 72 writer created and which breaks
      the naive "recomputed hash must equal stored hash" reading
  C - hash verification fails closed on a real corruption
  D - exact-duplicate collapse, deterministic donor choice, and the refusal to
      pick a donor when duplicates disagree
  E - baseline drift stops the run
  F - manifests are deterministic and fingerprints are reproducible
  G - CLI wiring
  H - a real fixture Parquet through the real streaming loop
  I - materialize: deterministic keys and disposition rules
  J - materialize: write path
  K - dedupe (Stage 3a): the sidecar-hash join, the delete guard, the rate gate
  L - unpack (Stage 3b): original keys, frame grouping, the verify-or-stop rule
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path

import pytest

from scripts.reconcile_april_detail import (
    BASELINE_OBJECTS,
    BASELINE_ROWS,
    BASELINE_STATUS_CENSUS,
    HTML_COLUMN,
    OBSERVATION_FIELDS,
    OCCURRENCE_FIELDS,
    CensusAccumulator,
    ReconcileError,
    _normalize_fetched_at,
    check_baseline,
    classify_row,
    extract_listing_id,
    parse_args,
    status_bucket,
    write_outputs,
)

LISTING_A = "11111111-2222-3333-4444-555555555555"
LISTING_B = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"


def make_row(
    *,
    key="html/year=2026/month=4/artifact_type=detail_page/part-0.parquet",
    row_group=0,
    row_offset=0,
    listing=LISTING_A,
    fetched_at="2026-04-15T12:00:00+00:00",
    status=200,
    html=b"<html>car</html>",
    stored_sha=None,
    artifact_id=1,
):
    """Build one raw Parquet row in the Plan 72 archiver's schema."""
    if stored_sha is None:
        stored_sha = hashlib.sha256(html).hexdigest()
    return {
        "artifact_id": artifact_id,
        "run_id": "run-1",
        "source": "cars.com",
        "artifact_type": "detail_page",
        "search_key": "key",
        "search_scope": "scope",
        "url": f"https://www.cars.com/vehicledetail/{listing}/",
        "fetched_at": fetched_at,
        "http_status": status,
        "content_bytes": len(html),
        "sha256": stored_sha,
        "error": None,
        "page_num": None,
        HTML_COLUMN: html,
        "legacy_object_key": key,
        "row_group": row_group,
        "row_offset": row_offset,
    }


# -- A: legacy identity ----------------------------------------------------

def test_listing_id_is_extracted_from_the_detail_url():
    assert extract_listing_id(
        f"https://www.cars.com/vehicledetail/{LISTING_A}/"
    ) == LISTING_A


def test_listing_id_is_none_when_the_url_carries_no_uuid():
    # The legacy schema has no listing_id column, so a URL that cannot yield
    # one leaves the row unidentified rather than guessed at.
    assert extract_listing_id("https://www.cars.com/shopping/") is None
    assert extract_listing_id(None) is None


@pytest.mark.parametrize("status,expected", [
    (200, "200"), (403, "403"), (500, "5xx"), (502, "5xx"), (599, "5xx"),
    (None, "null"), (404, "404"),
])
def test_status_bucketing_matches_the_plan_census(status, expected):
    # A status outside the plan's three buckets gets its own label: an
    # unexpected bucket appearing is drift the gate must catch, not something
    # to fold into a neighbour.
    assert status_bucket(status) == expected


def test_naive_times_are_normalized_to_utc():
    assert _normalize_fetched_at("2026-04-15T12:00:00") == "2026-04-15T12:00:00.000000+00:00"


def test_offset_times_are_converted_not_truncated():
    assert _normalize_fetched_at("2026-04-15T08:00:00-04:00") == \
        "2026-04-15T12:00:00.000000+00:00"


# -- B: the empty-body case ------------------------------------------------

def test_empty_body_is_flagged_and_not_hashed():
    # The Plan 72 writer archived b"" when the disk file was already gone,
    # while still copying the original page's hash from Postgres.
    row = make_row(html=b"", stored_sha="deadbeef" * 8)
    occurrence = classify_row(row)
    assert occurrence["is_empty"] is True
    assert occurrence["html_len"] == 0
    assert occurrence["recomputed_sha256"] is None
    assert occurrence["stored_sha256"] == "deadbeef" * 8


def test_empty_body_with_a_stored_hash_is_counted_not_treated_as_corruption():
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(make_row(html=b"", stored_sha="ab" * 32)))
    assert accumulator.hash_mismatches == []
    assert accumulator.empty_with_stored_hash == 1
    accumulator.check_hashes()  # must not raise


def test_empty_bodied_success_is_censused_but_never_a_recovery_candidate():
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(make_row(html=b"", stored_sha="ab" * 32)))
    observations, stats = accumulator.collapse()

    assert stats["http_200_occurrences"] == 1
    assert stats["http_200_occurrences_empty"] == 1
    assert stats["identities_empty_only"] == 1
    # Stage 4 would have no bytes to write, so it must not be offered any.
    assert observations == []


# -- C: hash verification fails closed ------------------------------------

def test_a_non_empty_body_disagreeing_with_its_stored_hash_stops_the_run():
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(
        make_row(html=b"<html>real</html>", stored_sha="00" * 32)
    ))
    with pytest.raises(ReconcileError, match="disagree with their stored hash"):
        accumulator.check_hashes()


def test_matching_hashes_pass():
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(make_row()))
    accumulator.check_hashes()


def test_mismatch_examples_are_bounded():
    accumulator = CensusAccumulator(max_examples=2)
    for i in range(5):
        accumulator.add(classify_row(
            make_row(row_offset=i, html=b"x" * (i + 1), stored_sha="00" * 32)
        ))
    detailed = [m for m in accumulator.hash_mismatches if not m.get("truncated")]
    assert len(detailed) == 2
    assert len(accumulator.hash_mismatches) == 5


# -- D: duplicate collapse -------------------------------------------------

def test_exact_duplicates_collapse_to_one_observation_retaining_the_count():
    html = b"<html>same</html>"
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(make_row(row_offset=0, html=html)))
    accumulator.add(classify_row(make_row(row_offset=1, html=html)))

    observations, stats = accumulator.collapse()
    assert len(observations) == 1
    assert observations[0]["occurrence_count"] == 2
    assert stats["http_200_occurrences"] == 2
    assert stats["distinct_identities"] == 1
    assert stats["duplicate_occurrences_collapsed"] == 1


def test_same_parsed_value_at_different_times_is_two_observations():
    # Plan 145: "A stable price observed twice is still two observations."
    html = b"<html>same</html>"
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(
        make_row(fetched_at="2026-04-15T12:00:00+00:00", html=html)))
    accumulator.add(classify_row(
        make_row(row_offset=1, fetched_at="2026-04-16T12:00:00+00:00", html=html)))

    observations, _ = accumulator.collapse()
    assert len(observations) == 2


def test_the_donor_is_the_lowest_locator_so_reruns_are_identical():
    html = b"<html>same</html>"
    accumulator = CensusAccumulator()
    for key, offset in [("z.parquet", 5), ("a.parquet", 9), ("a.parquet", 2)]:
        accumulator.add(classify_row(make_row(key=key, row_offset=offset, html=html)))

    observations, _ = accumulator.collapse()
    assert observations[0]["donor_legacy_object_key"] == "a.parquet"
    assert observations[0]["donor_row_offset"] == 2


def test_an_empty_row_never_wins_the_donor_slot():
    html = b"<html>real</html>"
    accumulator = CensusAccumulator()
    # The empty row sorts first by locator but has no bytes to donate.
    accumulator.add(classify_row(
        make_row(key="a.parquet", row_offset=0, html=b"", stored_sha="ab" * 32)))
    accumulator.add(classify_row(
        make_row(key="b.parquet", row_offset=0, html=html)))

    observations, _ = accumulator.collapse()
    assert len(observations) == 1
    assert observations[0]["donor_legacy_object_key"] == "b.parquet"
    assert observations[0]["recomputed_sha256"] == hashlib.sha256(html).hexdigest()


def test_duplicates_disagreeing_on_content_stop_rather_than_choosing():
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(make_row(row_offset=0, html=b"<html>one</html>")))
    accumulator.add(classify_row(make_row(row_offset=1, html=b"<html>two</html>")))

    with pytest.raises(ReconcileError, match="does not select a donor"):
        accumulator.collapse()


def test_non_success_rows_are_censused_but_excluded_from_the_manifest():
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(make_row(status=200)))
    accumulator.add(classify_row(make_row(row_offset=1, status=403, html=b"<html>nope</html>")))
    accumulator.add(classify_row(make_row(row_offset=2, status=503, html=b"err")))

    assert accumulator.total_rows == 3
    assert accumulator.status_census == {"200": 1, "403": 1, "5xx": 1}
    observations, stats = accumulator.collapse()
    assert stats["http_200_occurrences"] == 1
    assert len(observations) == 1


def test_a_success_with_an_unusable_url_is_counted_as_unidentified():
    accumulator = CensusAccumulator()
    row = make_row()
    row["url"] = "https://www.cars.com/shopping/"
    accumulator.add(classify_row(row))

    observations, stats = accumulator.collapse()
    assert accumulator.missing_listing_id == 1
    assert stats["unidentified_occurrences"] == 1
    assert observations == []


# -- E: baseline drift -----------------------------------------------------

def _baseline_accumulator():
    accumulator = CensusAccumulator()
    accumulator.total_rows = BASELINE_ROWS
    accumulator.status_census.update(BASELINE_STATUS_CENSUS)
    return accumulator


def test_matching_baseline_reports_no_drift():
    objects = [{"legacy_object_key": f"k{i}", "size_bytes": 1}
               for i in range(BASELINE_OBJECTS)]
    assert check_baseline(objects, _baseline_accumulator(), strict=True) == []


def test_a_missing_object_stops_the_run():
    objects = [{"legacy_object_key": f"k{i}", "size_bytes": 1}
               for i in range(BASELINE_OBJECTS - 1)]
    with pytest.raises(ReconcileError, match="baseline drift"):
        check_baseline(objects, _baseline_accumulator(), strict=True)


def test_an_unexpected_status_bucket_stops_the_run():
    objects = [{"legacy_object_key": f"k{i}", "size_bytes": 1}
               for i in range(BASELINE_OBJECTS)]
    accumulator = _baseline_accumulator()
    accumulator.status_census["404"] = 3
    accumulator.total_rows += 3
    with pytest.raises(ReconcileError, match="unexpected status buckets"):
        check_baseline(objects, accumulator, strict=True)


def test_allow_drift_reports_instead_of_stopping():
    objects = [{"legacy_object_key": "k0", "size_bytes": 1}]
    drifts = check_baseline(objects, _baseline_accumulator(), strict=False)
    assert any("objects:" in d for d in drifts)


# -- F: deterministic manifests -------------------------------------------

def _write(tmp_path: Path, name: str) -> dict:
    accumulator = CensusAccumulator()
    accumulator.add(classify_row(make_row(listing=LISTING_B, row_offset=1)))
    accumulator.add(classify_row(make_row(listing=LISTING_A, row_offset=0)))
    observations, stats = accumulator.collapse()
    return write_outputs(
        tmp_path / name,
        objects=[{"legacy_object_key": "k0", "size_bytes": 10,
                  "etag": "e", "last_modified": "2026-04-30T00:00:00+00:00"}],
        accumulator=accumulator,
        observations=observations,
        stats=stats,
        drifts=[],
        context={"bucket": "bronze", "prefix": "p/"},
    )


def test_manifest_fingerprints_are_reproducible(tmp_path):
    first = _write(tmp_path, "one")
    second = _write(tmp_path, "two")
    for name in ("object_census.csv", "occurrences_http200.csv",
                 "observations_distinct.csv"):
        assert first["fingerprints"][name] == second["fingerprints"][name]


def test_observations_are_sorted_independently_of_arrival_order(tmp_path):
    _write(tmp_path, "sorted")
    rows = (tmp_path / "sorted" / "observations_distinct.csv").read_text().splitlines()
    assert rows[0] == ",".join(OBSERVATION_FIELDS)
    assert rows[1].startswith(LISTING_A)
    assert rows[2].startswith(LISTING_B)


def test_the_occurrence_manifest_carries_the_legacy_locator(tmp_path):
    _write(tmp_path, "locator")
    header = (tmp_path / "locator" / "occurrences_http200.csv").read_text().splitlines()[0]
    assert header == ",".join(OCCURRENCE_FIELDS)
    for column in ("legacy_object_key", "row_group", "row_offset",
                   "stored_sha256", "recomputed_sha256"):
        assert column in header


def test_the_report_separates_the_two_distinct_hash_counts(tmp_path):
    report = _write(tmp_path, "report")
    saved = json.loads((tmp_path / "report" / "stage1_report.json").read_text())
    # Stage 2's pack join is content-based, so it needs the recomputed count;
    # the stored count is what the old system believed. Reporting one number
    # for "the SHA" would hide the difference these tests exist to expose.
    assert "distinct_recomputed_sha256" in saved["identity"]
    assert "distinct_stored_sha256" in saved["identity"]
    assert saved["stage"] == "plan_145_stage_1_census"
    assert report["fingerprints"]["stage1_report.json"]


def test_the_report_cross_tabs_empty_against_status(tmp_path):
    _write(tmp_path, "crosstab")
    saved = json.loads((tmp_path / "crosstab" / "stage1_report.json").read_text())
    assert "empty_by_status" in saved["rows"]
    assert "empty_rows_carrying_stored_hash" in saved["rows"]


# -- G: CLI ----------------------------------------------------------------

def test_census_mode_requires_an_output_directory():
    with pytest.raises(SystemExit):
        parse_args(["census"])


def test_census_defaults_are_read_only_and_strict():
    args = parse_args(["census", "--out-dir", "/tmp/out"])
    assert args.mode == "census"
    assert args.allow_drift is False
    assert args.max_objects == 0
    assert args.prefix is None


def test_a_mode_is_required():
    with pytest.raises(SystemExit):
        parse_args([])


# -- H: a real fixture Parquet through the real streaming loop -------------

def _write_fixture_parquet(path: Path, rows: list[dict], *, row_group_size: int = 2):
    """Write fixture Parquet in the exact Plan 72 archiver schema.

    Reproduced from archiver/processors/archive_artifacts.py at commit 1798a99
    so the test exercises the same column types the production scan meets --
    notably large_binary HTML and a tz-aware microsecond timestamp.

    `year`, `month` and `artifact_type` are deliberately absent: the writer
    passed them as ``partition_cols``, so pyarrow encodes them in the key path
    and leaves them out of the file. A fixture that stored them as columns
    would have hidden exactly the schema mismatch the first production run hit.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("artifact_id", pa.int64()),
        pa.field("run_id", pa.string()),
        pa.field("source", pa.string()),
        pa.field("search_key", pa.string()),
        pa.field("search_scope", pa.string()),
        pa.field("url", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
        pa.field("http_status", pa.int32()),
        pa.field("content_bytes", pa.int64()),
        pa.field("sha256", pa.string()),
        pa.field("error", pa.string()),
        pa.field("page_num", pa.int32()),
        pa.field("html", pa.large_binary()),
    ])
    table = pa.Table.from_pylist(
        [{k: v for k, v in row.items() if k in schema.names} for row in rows],
        schema=schema,
    )
    pq.write_table(table, path, compression="zstd", row_group_size=row_group_size)


def test_a_fixture_parquet_streams_through_the_real_loop(tmp_path):
    from datetime import datetime, timezone

    from scripts.reconcile_april_detail import iter_rows

    html_a = b"<html>listing a</html>"
    html_dup = b"<html>duplicated capture</html>"
    when = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
    later = datetime(2026, 4, 16, 12, 0, tzinfo=timezone.utc)

    rows = [
        make_row(listing=LISTING_A, fetched_at=when, html=html_a, artifact_id=1),
        # exact duplicate occurrence of one capture -> collapses to one
        make_row(listing=LISTING_B, fetched_at=when, html=html_dup, artifact_id=2),
        make_row(listing=LISTING_B, fetched_at=when, html=html_dup, artifact_id=3),
        # a 403 challenge page and an empty-bodied success
        make_row(listing=LISTING_A, fetched_at=later, status=403,
                 html=b"<html>denied</html>", artifact_id=4),
        make_row(listing=LISTING_A, fetched_at=later, html=b"",
                 stored_sha="ab" * 32, artifact_id=5),
    ]
    fixture = tmp_path / "part-0.parquet"
    _write_fixture_parquet(fixture, rows)

    objects = [{"legacy_object_key": "part-0.parquet", "size_bytes": fixture.stat().st_size}]
    accumulator = CensusAccumulator()
    streamed = list(iter_rows(
        "bronze", objects, progress_every=0, opener=lambda key: fixture.open("rb"),
    ))
    assert len(streamed) == 5
    # row_group_size=2 over 5 rows means the locator must span groups, which is
    # the part a single-row-group fixture would not prove.
    assert {r["row_group"] for r in streamed} == {0, 1, 2}

    for row in streamed:
        accumulator.add(classify_row(row))

    accumulator.check_hashes()
    observations, stats = accumulator.collapse()

    assert accumulator.total_rows == 5
    assert accumulator.status_census == {"200": 4, "403": 1}
    assert accumulator.empty_by_status == {"200": 1}
    assert stats["duplicate_occurrences_collapsed"] == 1
    assert stats["identities_empty_only"] == 1
    assert stats["distinct_recomputed_sha256"] == 2

    # Two recoverable observations: listing A's real capture and listing B's
    # collapsed duplicate pair. The 403 and the empty-bodied 200 yield nothing.
    assert len(observations) == 2
    by_listing = {o["listing_id"]: o for o in observations}
    assert by_listing[LISTING_A]["recomputed_sha256"] == hashlib.sha256(html_a).hexdigest()
    assert by_listing[LISTING_B]["occurrence_count"] == 2
    assert by_listing[LISTING_B]["donor_row_group"] == 0


def test_a_fixture_missing_a_schema_column_stops_the_run(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.reconcile_april_detail import iter_rows

    fixture = tmp_path / "truncated.parquet"
    pq.write_table(pa.table({"artifact_id": [1]}), fixture)

    objects = [{"legacy_object_key": "truncated.parquet", "size_bytes": 1}]
    with pytest.raises(ReconcileError, match="missing expected columns"):
        list(iter_rows("bronze", objects, progress_every=0,
                       opener=lambda key: fixture.open("rb")))


# -- I: materialize -- deterministic keys and disposition rules -------------

from scripts.reconcile_april_detail import (  # noqa: E402
    DISPOSITIONS,
    _shard_key,
    materialize_row,
    plan_row,
    sha_to_file_id,
)

SHA_A = "3e193dfdadc2f0be546e9867c1e96bae713f7556c67ebefd20a6be3ca53d4a3f"


def _occurrence(**over):
    row = make_row(**{k: v for k, v in over.items() if k in {
        "key", "row_group", "row_offset", "listing", "fetched_at", "status",
        "html", "stored_sha", "artifact_id"}})
    return classify_row(row)


def test_file_id_is_derived_from_content_not_random():
    # A random UUID would make every re-run write a second copy of the whole
    # population; the stem has to be a pure function of the bytes.
    assert sha_to_file_id(SHA_A) == "3e193dfd-adc2-f0be-546e-9867c1e96bae"
    assert sha_to_file_id(SHA_A) == sha_to_file_id(SHA_A)


def test_file_id_refuses_a_hash_it_cannot_use():
    with pytest.raises(ReconcileError, match="cannot derive a file id"):
        sha_to_file_id("abc")
    with pytest.raises(ReconcileError, match="cannot derive a file id"):
        sha_to_file_id(None)


def test_a_successful_row_plans_an_april_detail_key_from_its_own_hash():
    rec = plan_row(_occurrence(html=b"<html>car</html>"))
    assert rec["disposition"] == "written"
    assert rec["object_key"].startswith(
        "html/year=2026/month=4/artifact_type=detail_page/")
    assert rec["object_key"].endswith(".html.zst")
    assert sha_to_file_id(rec["raw_sha256"]) in rec["object_key"]


def test_identical_bytes_plan_the_same_key():
    a = plan_row(_occurrence(html=b"<html>same</html>", row_offset=0))
    b = plan_row(_occurrence(html=b"<html>same</html>", row_offset=1))
    assert a["object_key"] == b["object_key"]


def test_an_empty_body_is_recorded_and_never_written():
    rec = plan_row(_occurrence(html=b"", stored_sha="ab" * 32))
    assert rec["disposition"] == "skipped_empty"
    assert rec["object_key"] is None


@pytest.mark.parametrize("status", [403, 503])
def test_non_success_bodies_are_recorded_and_never_written(status):
    # They parse to a blocked/failed state and yield no observation, so they
    # are recorded for accounting rather than carried into the parse stage.
    rec = plan_row(_occurrence(status=status, html=b"<html>denied</html>"))
    assert rec["disposition"] == "skipped_non_success"
    assert rec["object_key"] is None


def test_every_disposition_is_a_declared_one():
    for occ in (_occurrence(html=b"<html>ok</html>"),
                _occurrence(html=b"", stored_sha="ab" * 32),
                _occurrence(status=403, html=b"<html>no</html>")):
        assert plan_row(occ)["disposition"] in DISPOSITIONS


def test_the_manifest_shard_is_named_for_its_source_file():
    assert _shard_key("html/year=2026/month=4/artifact_type=detail_page/part-abc-0.parquet") \
        == "recovery/plan145/materialized/part-abc-0.parquet"


# -- J: materialize -- write path ------------------------------------------

def test_dry_run_writes_nothing(monkeypatch):
    import shared.minio as minio
    calls = []
    monkeypatch.setattr(minio, "object_exists", lambda k: False)
    monkeypatch.setattr(minio, "write_html", lambda k, c: calls.append(k))
    rec = plan_row(_occurrence(html=b"<html>car</html>"))
    out = materialize_row(rec, b"<html>car</html>", apply=False)
    assert calls == []
    assert out["disposition"] == "written"


def test_an_existing_object_is_not_rewritten(monkeypatch):
    import shared.minio as minio
    calls = []
    monkeypatch.setattr(minio, "object_exists", lambda k: True)
    monkeypatch.setattr(minio, "write_html", lambda k, c: calls.append(k))
    rec = plan_row(_occurrence(html=b"<html>car</html>"))
    out = materialize_row(rec, b"<html>car</html>", apply=True)
    assert calls == []
    assert out["disposition"] == "exists"


def test_apply_writes_and_verifies_the_read_back(monkeypatch):
    import shared.minio as minio
    html = b"<html>car</html>"
    written = {}
    monkeypatch.setattr(minio, "object_exists", lambda k: False)
    monkeypatch.setattr(minio, "write_html", lambda k, c: written.update({k: c}))
    monkeypatch.setattr(minio, "read_html", lambda p: html)
    monkeypatch.setattr(minio, "object_size", lambda p: 1234)
    rec = plan_row(_occurrence(html=html))
    out = materialize_row(rec, html, apply=True)
    assert list(written) == [rec["object_key"]]
    assert out["compressed_len"] == 1234


def test_a_read_back_that_does_not_match_stops_the_run(monkeypatch):
    # Verification reads through the production path rather than comparing the
    # in-memory bytes to themselves, so a corrupted store is actually caught.
    import shared.minio as minio
    monkeypatch.setattr(minio, "object_exists", lambda k: False)
    monkeypatch.setattr(minio, "write_html", lambda k, c: None)
    monkeypatch.setattr(minio, "read_html", lambda p: b"<html>corrupted</html>")
    monkeypatch.setattr(minio, "object_size", lambda p: 1)
    rec = plan_row(_occurrence(html=b"<html>car</html>"))
    with pytest.raises(ReconcileError, match="read-back mismatch"):
        materialize_row(rec, b"<html>car</html>", apply=True)


def test_materialize_defaults_to_a_dry_run():
    args = parse_args(["materialize"])
    assert args.mode == "materialize"
    assert args.apply is False
    assert args.force is False
    assert args.no_verify is False


# -- K: dedupe (Stage 3a) -- the sidecar-hash join and the delete guard ----

from scripts.reconcile_april_detail import (  # noqa: E402
    MATERIALIZE_PREFIX,
    MAX_DELETE_BATCH,
    _dedupe_receipt_key,
    _dedupe_shard_key,
    delete_objects_in_batches,
    plan_deletions,
)

SHA_IN_PACK = "aa" * 32
SHA_NOT_IN_PACK = "bb" * 32
SIDECAR_KEY = "html_packs/detail_page/2026/04/pack-00000.idx.parquet"


def _mrow(object_key, sha, disposition="written", *, offset=0,
          legacy="html/year=2026/month=4/artifact_type=detail_page/part-0.parquet"):
    return {
        "disposition": disposition,
        "object_key": object_key,
        "raw_sha256": sha,
        "legacy_object_key": legacy,
        "row_group": 0,
        "row_offset": offset,
    }


def test_a_hash_in_a_sidecar_plans_a_deletion_one_absent_does_not():
    sidecar = {SHA_IN_PACK: (SIDECAR_KEY, "html/.../claimed.html.zst")}
    planned = plan_deletions(
        [_mrow("k1", SHA_IN_PACK), _mrow("k2", SHA_NOT_IN_PACK, offset=1)],
        sidecar,
    )
    assert [p["object_key"] for p in planned] == ["k1"]
    assert planned[0]["claimed_by_sidecar"] == SIDECAR_KEY
    assert planned[0]["claimed_by_source_key"] == "html/.../claimed.html.zst"
    assert planned[0]["legacy_object_key"].endswith("part-0.parquet")


@pytest.mark.parametrize("disposition", ["skipped_empty", "skipped_non_success"])
def test_skipped_dispositions_are_never_planned_for_deletion(disposition):
    sidecar = {SHA_IN_PACK: (SIDECAR_KEY, "src")}
    # Even a row that wrongly carries an object_key is ineligible on its
    # disposition alone -- those rows never produced an object.
    rows = [
        _mrow(None, SHA_IN_PACK, disposition),
        _mrow("ghost-key", SHA_IN_PACK, disposition, offset=1),
    ]
    assert plan_deletions(rows, sidecar) == []


def test_identical_bytes_across_two_rows_plan_exactly_one_deletion():
    sidecar = {SHA_IN_PACK: (SIDECAR_KEY, "src")}
    planned = plan_deletions(
        [_mrow("same", SHA_IN_PACK, "written"),
         _mrow("same", SHA_IN_PACK, "exists", offset=1)],
        sidecar,
    )
    assert len(planned) == 1


def test_plan_deletions_is_sorted_independently_of_row_order():
    shas = {f"{i:02x}" * 32: (SIDECAR_KEY, "src") for i in range(5)}
    rows = [_mrow(f"k{i}", f"{i:02x}" * 32, offset=i) for i in (3, 0, 4, 1, 2)]
    planned = plan_deletions(rows, shas)
    assert [p["object_key"] for p in planned] == ["k0", "k1", "k2", "k3", "k4"]


def test_deletion_refuses_a_key_whose_content_is_not_in_a_sidecar():
    with pytest.raises(ReconcileError, match="not in any April pack sidecar"):
        delete_objects_in_batches(
            None, "bronze", [{"object_key": "k1", "raw_sha256": SHA_IN_PACK}],
            apply=True, batch_size=1000, verified_hashes={},
        )


def test_a_dry_run_delete_plans_receipts_and_calls_nothing():
    class Boom:
        def delete_objects(self, **kwargs):
            raise AssertionError("a dry run must not delete")

    receipts = delete_objects_in_batches(
        Boom(), "bronze",
        [{"object_key": f"k{i}", "raw_sha256": SHA_IN_PACK} for i in range(3)],
        apply=False, batch_size=2, verified_hashes={SHA_IN_PACK: 1},
    )
    assert [r["result"] for r in receipts] == ["planned", "planned", "planned"]


def test_deletes_are_capped_at_the_s3_batch_limit_with_a_receipt_per_key():
    sizes = []

    class FakeClient:
        def delete_objects(self, Bucket, Delete):
            keys = [o["Key"] for o in Delete["Objects"]]
            sizes.append(len(keys))
            return {"Deleted": [{"Key": k} for k in keys], "Errors": []}

    records = [{"object_key": f"k{i}", "raw_sha256": SHA_IN_PACK} for i in range(2500)]
    receipts = delete_objects_in_batches(
        FakeClient(), "bronze", records,
        apply=True, batch_size=99999, verified_hashes={SHA_IN_PACK: 1},
    )
    assert sizes == [MAX_DELETE_BATCH, MAX_DELETE_BATCH, 500]
    assert len(receipts) == 2500
    assert all(r["result"] == "deleted" for r in receipts)


def test_a_delete_error_lands_on_the_receipt_rather_than_ending_the_run():
    class FakeClient:
        def delete_objects(self, Bucket, Delete):
            keys = [o["Key"] for o in Delete["Objects"]]
            return {
                "Deleted": [{"Key": keys[0]}],
                "Errors": [{"Key": keys[1], "Code": "AccessDenied"}],
            }

    receipts = delete_objects_in_batches(
        FakeClient(), "bronze",
        [{"object_key": "k0", "raw_sha256": SHA_IN_PACK},
         {"object_key": "k1", "raw_sha256": SHA_IN_PACK}],
        apply=True, batch_size=10, verified_hashes={SHA_IN_PACK: 1},
    )
    assert {r["object_key"]: r["result"] for r in receipts} == {
        "k0": "deleted", "k1": "error:AccessDenied",
    }


def test_dedupe_manifest_and_receipt_keys_mirror_the_source_shard():
    src = f"{MATERIALIZE_PREFIX}/part-abc-0.parquet"
    assert _dedupe_shard_key(src) == "recovery/plan145/dedupe/part-abc-0.parquet"
    assert _dedupe_receipt_key(src) == \
        "recovery/plan145/dedupe/receipts/part-abc-0.parquet"


def test_dedupe_defaults_to_a_dry_run():
    args = parse_args(["dedupe"])
    assert args.mode == "dedupe"
    assert args.apply is False
    assert args.allow_rate_drift is False


def _patch_dedupe(monkeypatch, events, rows, sidecar_hashes):
    """Wire run_dedupe onto in-memory fakes, recording write/delete order."""
    import scripts.reconcile_april_detail as mod
    import shared.minio as minio

    monkeypatch.setattr(mod, "_s3_client", lambda: object())
    monkeypatch.setattr(mod, "_list_keys", lambda c, b, prefix, suffix: (
        [SIDECAR_KEY] if suffix == ".idx.parquet"
        else [f"{MATERIALIZE_PREFIX}/part-0.parquet"]
    ))
    monkeypatch.setattr(mod, "load_sidecar_hashes",
                        lambda c, b, keys: dict(sidecar_hashes))
    monkeypatch.setattr(mod, "_read_parquet_rows",
                        lambda c, b, k, columns=None: list(rows))
    monkeypatch.setattr(mod, "_write_parquet_shard",
                        lambda key, schema, records: events.append(
                            ("write", key, len(records))))
    monkeypatch.setattr(minio, "object_exists", lambda k: False)

    def fake_delete(client, bucket, records, *, apply, batch_size, verified_hashes):
        events.append(("delete", [r["object_key"] for r in records]))
        return [{"object_key": r["object_key"], "raw_sha256": r["raw_sha256"],
                 "result": "deleted"} for r in records]

    monkeypatch.setattr(mod, "delete_objects_in_batches", fake_delete)
    return mod


def test_run_dedupe_writes_the_deletion_manifest_before_any_delete(monkeypatch):
    events: list = []
    mod = _patch_dedupe(
        monkeypatch, events,
        rows=[_mrow("k1", SHA_IN_PACK)],
        sidecar_hashes={SHA_IN_PACK: (SIDECAR_KEY, "src")},
    )
    rc = mod.run_dedupe(mod.parse_args(
        ["dedupe", "--apply", "--expect-rate", "1.0", "--rate-tolerance", "1.0"]))
    assert rc == 0
    assert [e[0] for e in events] == ["write", "delete", "write"]
    assert events[0][1] == "recovery/plan145/dedupe/part-0.parquet"
    assert events[2][1] == "recovery/plan145/dedupe/receipts/part-0.parquet"


def test_run_dedupe_stops_before_deleting_when_the_rate_is_off_band(monkeypatch):
    events: list = []
    mod = _patch_dedupe(
        monkeypatch, events,
        rows=[_mrow("k1", SHA_IN_PACK)],  # 1 of 1 candidate -> 100%, far off 45.6%
        sidecar_hashes={SHA_IN_PACK: (SIDECAR_KEY, "src")},
    )
    with pytest.raises(ReconcileError, match="outside the expected"):
        mod.run_dedupe(mod.parse_args(["dedupe", "--apply"]))
    # the deletion manifest is still written -- it is the reviewer's evidence --
    # but nothing is deleted.
    assert [e[0] for e in events] == ["write"]


def test_run_dedupe_allow_rate_drift_reports_instead_of_stopping(monkeypatch):
    events: list = []
    mod = _patch_dedupe(
        monkeypatch, events,
        rows=[_mrow("k1", SHA_IN_PACK)],
        sidecar_hashes={SHA_IN_PACK: (SIDECAR_KEY, "src")},
    )
    rc = mod.run_dedupe(mod.parse_args(
        ["dedupe", "--apply", "--allow-rate-drift"]))
    assert rc == 0
    assert "delete" in [e[0] for e in events]


# -- L: unpack (Stage 3b) -- original keys, frame grouping, verify-or-stop -

import itertools  # noqa: E402

import shared.compression as _compression  # noqa: E402
from scripts.reconcile_april_detail import (  # noqa: E402
    _unpack_shard_key,
    iter_members_by_frame,
    unpack_member,
)
from shared.packfile import (  # noqa: E402
    PackIndexEntry,
    PackMember,
    PackReader,
    build_pack,
    index_key,
    write_index_parquet,
)

DETAIL_KEY = "html/year=2026/month=4/artifact_type=detail_page/{}.html.zst"


class _Body:
    def __init__(self, data: bytes):
        self._data = data

    def read(self) -> bytes:
        return self._data


def _entry(source_key, frame_ordinal, offset, content, **over):
    return PackIndexEntry(
        source_key=source_key,
        frame_ordinal=frame_ordinal,
        offset_in_frame=offset,
        length=len(content),
        raw_sha256=hashlib.sha256(content).hexdigest(),
        artifact_id=over.get("artifact_id"),
        listing_id=over.get("listing_id"),
        fetched_at=over.get("fetched_at"),
    )


def test_unpack_shard_key_is_named_for_its_pack():
    assert _unpack_shard_key("html_packs/detail_page/2026/04/pack-00007.zpack") == \
        "recovery/plan145/unpacked/pack-00007.parquet"


def test_members_are_iterated_in_frame_then_offset_order():
    out_of_order = [
        PackIndexEntry("c", 1, 0, 5, "x"),
        PackIndexEntry("a", 0, 0, 3, "x"),
        PackIndexEntry("b", 0, 3, 4, "x"),
    ]
    assert [e.source_key for e in iter_members_by_frame(out_of_order)] == \
        ["a", "b", "c"]


def _small_pack(n=8):
    members = [
        PackMember(
            source_key=DETAIL_KEY.format(f"uuid-{i}"),
            content=f"<html>listing {i} ".encode() + b"x" * 80,
            artifact_id=1000 + i,
            listing_id=LISTING_A,
        )
        for i in range(n)
    ]
    # Same listing for every member, so only the hard ceiling seals frames:
    # ~100-byte members and a 150-byte ceiling give ~2 members per frame.
    pack = build_pack(members, frame_target_bytes=150, frame_max_bytes=150)
    return members, pack


def test_walking_members_in_frame_order_decompresses_each_frame_once(monkeypatch):
    _members, pack = _small_pack()
    assert pack.frame_count >= 2

    calls: list = []
    real = _compression.decompress_frame
    monkeypatch.setattr(
        _compression, "decompress_frame",
        lambda frame: (calls.append(1), real(frame))[1],
    )

    reader = PackReader.from_bytes(pack.data, max_cached_frames=1)
    for entry in iter_members_by_frame(pack.entries):
        reader.read_member(entry)
    assert sum(calls) == pack.frame_count

    # Interleaving frames with a single-frame cache pays for each frame switch,
    # which is exactly what the frame grouping exists to avoid.
    calls.clear()
    by_frame: dict = {}
    for entry in pack.entries:
        by_frame.setdefault(entry.frame_ordinal, []).append(entry)
    interleaved = [
        e for e in itertools.chain.from_iterable(
            itertools.zip_longest(*by_frame.values())
        ) if e is not None
    ]
    reader2 = PackReader.from_bytes(pack.data, max_cached_frames=1)
    for entry in interleaved:
        reader2.read_member(entry)
    assert sum(calls) > pack.frame_count


def test_unpack_writes_under_the_original_source_key_not_a_content_key(monkeypatch):
    import shared.minio as minio
    written: dict = {}
    monkeypatch.setattr(minio, "object_exists", lambda k: False)
    monkeypatch.setattr(minio, "write_html", lambda k, c: written.__setitem__(k, c))

    content = b"<html>a real captured page</html>"
    entry = _entry(DETAIL_KEY.format("original-uuid"), 0, 0, content,
                   artifact_id=42, listing_id=LISTING_A)

    class FakeReader:
        def read_member(self, e):
            return content

    rec = unpack_member(
        FakeReader(), entry, "html_packs/detail_page/2026/04/pack-00000.zpack",
        apply=True,
    )
    assert list(written) == [entry.source_key]
    assert written[entry.source_key] == content
    assert rec["disposition"] == "written"
    assert rec["artifact_id"] == 42
    assert rec["pack_key"].endswith("pack-00000.zpack")


def test_a_member_that_does_not_match_its_sidecar_hash_stops_the_run(monkeypatch):
    import shared.minio as minio
    monkeypatch.setattr(minio, "object_exists", lambda k: False)
    monkeypatch.setattr(minio, "write_html",
                        lambda k, c: pytest.fail("must not write on a mismatch"))

    entry = PackIndexEntry(DETAIL_KEY.format("x"), 0, 0, 4, "00" * 32)

    class FakeReader:
        def read_member(self, e):
            return b"real"

    with pytest.raises(ReconcileError, match="the store moved"):
        unpack_member(FakeReader(), entry, "pack", apply=True)


def test_an_existing_key_is_skipped_rather_than_re_read_or_rewritten(monkeypatch):
    import shared.minio as minio
    calls: list = []
    monkeypatch.setattr(minio, "object_exists", lambda k: True)
    monkeypatch.setattr(minio, "write_html", lambda k, c: calls.append(k))

    entry = PackIndexEntry(DETAIL_KEY.format("x"), 2, 8, 4, "00" * 32)

    class FakeReader:
        def read_member(self, e):
            raise AssertionError("an existing key must not be read from the pack")

    rec = unpack_member(FakeReader(), entry, "pack", apply=True)
    assert rec["disposition"] == "exists"
    assert calls == []


def test_unpack_dry_run_verifies_but_writes_nothing(monkeypatch):
    import shared.minio as minio
    calls: list = []
    monkeypatch.setattr(minio, "object_exists", lambda k: False)
    monkeypatch.setattr(minio, "write_html", lambda k, c: calls.append(k))

    content = b"<html>x</html>"
    entry = _entry(DETAIL_KEY.format("x"), 0, 0, content)

    class FakeReader:
        def read_member(self, e):
            return content

    rec = unpack_member(FakeReader(), entry, "pack", apply=False)
    assert calls == []
    assert rec["disposition"] == "written"


def test_unpack_defaults_to_a_dry_run():
    args = parse_args(["unpack"])
    assert args.mode == "unpack"
    assert args.apply is False
    assert args.no_verify is False


def test_run_unpack_writes_every_member_under_its_original_key(monkeypatch):
    import scripts.reconcile_april_detail as mod
    import shared.minio as minio

    members, pack = _small_pack()
    assert pack.frame_count >= 2
    pack_key = "html_packs/detail_page/2026/04/pack-00000.zpack"
    store = {pack_key: pack.data, index_key(pack_key): write_index_parquet(pack.entries)}
    written: dict = {}
    shards: dict = {}

    class FakeClient:
        def head_object(self, Bucket, Key):
            return {"ContentLength": len(store[Key])}

        def get_object(self, Bucket, Key, Range=None):
            data = store[Key]
            if Range:
                lo, hi = Range.removeprefix("bytes=").split("-")
                data = data[int(lo):int(hi) + 1]
            return {"Body": _Body(data)}

        def list_objects_v2(self, **kwargs):
            prefix = kwargs["Prefix"]
            return {
                "Contents": [{"Key": k} for k in store if k.startswith(prefix)],
                "IsTruncated": False,
            }

    monkeypatch.setattr(mod, "_s3_client", lambda: FakeClient())
    monkeypatch.setattr(minio, "object_exists", lambda k: k in written)
    monkeypatch.setattr(minio, "write_html", lambda k, c: written.__setitem__(k, c))
    monkeypatch.setattr(mod, "_write_parquet_shard",
                        lambda key, schema, records: shards.__setitem__(key, list(records)))

    rc = mod.run_unpack(mod.parse_args(["unpack", "--apply"]))
    assert rc == 0
    assert set(written) == {m.source_key for m in members}
    for member in members:
        assert written[member.source_key] == member.content
    assert list(shards) == ["recovery/plan145/unpacked/pack-00000.parquet"]
    assert len(shards["recovery/plan145/unpacked/pack-00000.parquet"]) == len(members)


# -- M: parse (Stage 4) ----------------------------------------------------

def _parse_record(**overrides):
    body = overrides.pop("body", b"<html>detail</html>")
    record = {
        "object_key": DETAIL_KEY.format("parse"),
        "raw_sha256": hashlib.sha256(body).hexdigest(),
        "html_len": len(body),
        "input_kind": "unpacked",
        "legacy_object_key": None,
        "row_group": None,
        "row_offset": None,
        "legacy_artifact_id": None,
        "pack_key": "pack.zpack",
        "frame_ordinal": 0,
    }
    record.update(overrides)
    return body, record


def _identity(listing_id=LISTING_A, fetched_at="2026-04-15T12:00:00+00:00",
              listing_id_source="queue_events", fetched_at_source="queue_events"):
    return {
        "listing_id": listing_id,
        "listing_id_source": listing_id_source,
        "fetched_at": fetched_at,
        "fetched_at_source": fetched_at_source,
        "identity_disagreement": False,
    }


def _primary(**overrides):
    row = {
        "listing_state": "active",
        "listing_id": LISTING_A,
        "vin": "VIN1",
        "price": 25000,
        "make": "Honda",
        "model": "Civic",
        "dealer_name": "Dealer",
    }
    row.update(overrides)
    return row


def test_parse_decodes_invalid_utf8_exactly_like_production():
    from scripts.reconcile_april_detail import parse_one_input

    body, record = _parse_record(body=b"before\xffafter")
    seen = {}

    def parser(text, url):
        seen["text"] = text
        return _primary(), [], {}

    rows, audit = parse_one_input(
        record, _identity(), reader=lambda _key: body, parser=parser,
    )
    assert seen["text"] == body.decode("utf-8", errors="replace")
    assert audit["outcome"] == "parsed"
    assert len(rows) == 1


def test_parse_input_is_materialized_minus_deleted_union_unpacked():
    from scripts.reconcile_april_detail import build_parse_units

    mat = [(
        "recovery/plan145/materialized/a.parquet",
        [
            {"object_key": "keep", "disposition": "written", "raw_sha256": "a"},
            {"object_key": "delete", "disposition": "exists", "raw_sha256": "b"},
            {"object_key": None, "disposition": "skipped_empty"},
        ],
    )]
    unpack = [(
        "recovery/plan145/unpacked/p.parquet",
        [{"source_key": "unpacked", "disposition": "written", "raw_sha256": "c"}],
    )]
    units = build_parse_units(mat, {"delete"}, unpack)
    assert [r["object_key"] for _, rows in units for r in rows] == ["keep", "unpacked"]


def test_parse_input_counts_repeated_materialized_keys_once():
    from scripts.reconcile_april_detail import build_parse_units

    repeated = {
        "object_key": "content-derived-key",
        "disposition": "written",
        "raw_sha256": "same-content",
    }
    materialized = [
        ("recovery/plan145/materialized/a.parquet", [repeated]),
        ("recovery/plan145/materialized/b.parquet", [repeated]),
    ]
    units = build_parse_units(materialized, set(), [])
    assert sum(len(rows) for _, rows in units) == 1
    assert units[0][1][0]["object_key"] == "content-derived-key"
    assert units[1][1] == []


def test_identity_tiers_prefer_legacy_then_queue_and_count_disagreement():
    from scripts.reconcile_april_detail import resolve_manifest_identity

    record = {"object_key": "key", "raw_sha256": "sha"}
    legacy = {"sha": {"listing_id": LISTING_A, "fetched_at": "2026-04-01T00:00:00+00:00"}}
    queue = {"key": {"listing_id": LISTING_B, "fetched_at": "2026-04-02T00:00:00+00:00"}}
    resolved = resolve_manifest_identity(record, legacy, queue)
    assert resolved["listing_id"] == LISTING_A
    assert resolved["listing_id_source"] == "legacy_manifest"
    assert resolved["fetched_at_source"] == "legacy_manifest"
    assert resolved["identity_disagreement"] is True

    resolved = resolve_manifest_identity(record, {}, queue)
    assert resolved["listing_id"] == LISTING_B
    assert resolved["listing_id_source"] == "queue_events"


def test_parsed_page_identity_has_no_time_and_is_unimportable():
    from scripts.reconcile_april_detail import parse_one_input

    body, record = _parse_record()
    rows, audit = parse_one_input(
        record, _identity(None, None, "none", "none"),
        reader=lambda _key: body,
        parser=lambda text, url: (_primary(listing_id=LISTING_B), [], {}),
    )
    assert rows[0]["listing_id"] == LISTING_B
    assert audit["listing_id_source"] == "parsed_page"
    assert audit["fetched_at_source"] == "none"
    assert audit["importable"] is False


def test_unpack_sidecar_listing_id_never_reaches_parser_or_output():
    from scripts.reconcile_april_detail import build_parse_units, parse_one_input

    sidecar_id = "ffffffff-ffff-ffff-ffff-ffffffffffff"
    unpack = [(
        "recovery/plan145/unpacked/p.parquet",
        [{
            "source_key": "key", "raw_sha256": "sha", "html_len": 1,
            "disposition": "written", "listing_id": sidecar_id,
        }],
    )]
    record = build_parse_units([], set(), unpack)[0][1][0]
    body = b"x"
    record["raw_sha256"] = hashlib.sha256(body).hexdigest()
    seen = {}

    def parser(text, url):
        seen["url"] = url
        return _primary(listing_id=LISTING_B), [], {}

    rows, _ = parse_one_input(
        record, _identity(None, None, "none", "none"),
        reader=lambda _key: body, parser=parser,
    )
    assert seen["url"] is None
    assert rows[0]["listing_id"] == LISTING_B
    assert sidecar_id not in repr(rows)


def test_primary_and_qualifying_carousel_rows_mirror_production_drop_rules():
    from scripts.reconcile_april_detail import build_observation_rows

    _, record = _parse_record()
    carousel = [
        {"listing_id": LISTING_B, "price": 100, "body": "SUV", "year": 2025},
        {"listing_id": None, "price": 100, "body": "SUV"},
        {"listing_id": "no-price", "price": None, "body": "SUV"},
        {"listing_id": "no-body", "price": 100, "body": None},
    ]
    rows, drops, audit = build_observation_rows(
        record, _identity(), _primary(), carousel,
    )
    assert [row["source"] for row in rows] == ["detail", "carousel"]
    assert drops == {"listing_id": 1, "price": 1, "body": 1}
    assert rows[1]["vin"] is None
    assert rows[1]["dealer_name"] == "Dealer"
    assert all("artifact_id" not in row for row in rows)
    assert audit["importable"] is True


def test_unlisted_page_yields_one_null_price_row_and_no_carousel():
    from scripts.reconcile_april_detail import build_observation_rows

    _, record = _parse_record()
    rows, _, _ = build_observation_rows(
        record, _identity(),
        _primary(listing_state="unlisted", price=100, mileage=99), [],
    )
    assert len(rows) == 1
    assert rows[0]["listing_state"] == "unlisted"
    assert rows[0]["price"] is None
    assert rows[0]["mileage"] is None


@pytest.mark.parametrize("primary,outcome", [
    ({"listing_state": "blocked", "listing_id": LISTING_A}, "blocked_cloudflare"),
    ({"listing_state": "active", "listing_id": None, "vin": None,
      "price": None, "make": None}, "blocked_other"),
])
def test_block_pages_are_distinct_and_emit_no_rows(primary, outcome):
    from scripts.reconcile_april_detail import parse_one_input

    body, record = _parse_record()
    rows, audit = parse_one_input(
        record, _identity(None, None, "none", "none"),
        reader=lambda _key: body,
        parser=lambda text, url: (primary, [], {}),
    )
    assert rows == []
    assert audit["outcome"] == outcome


@pytest.mark.parametrize("body,outcome", [
    (b"<html><head><title>Just a moment...</title></head></html>",
     "blocked_cloudflare"),
    (b"<html><head><title>Access Denied</title></head><body>Reference #1</body></html>",
     "blocked_other"),
])
def test_real_production_parser_classifies_cloudflare_and_akamai(body, outcome):
    from scripts.reconcile_april_detail import parse_one_input

    _, record = _parse_record(body=body)
    rows, audit = parse_one_input(
        record, _identity(None, None, "none", "none"),
        reader=lambda _key: body,
    )
    assert rows == []
    assert audit["outcome"] == outcome


def test_parse_hash_disagreement_stops_the_run():
    from scripts.reconcile_april_detail import parse_one_input

    _, record = _parse_record()
    with pytest.raises(ReconcileError, match="store moved"):
        parse_one_input(record, _identity(), reader=lambda _key: b"changed")


def test_parse_exception_is_recorded_as_failed():
    from scripts.reconcile_april_detail import parse_one_input

    body, record = _parse_record()

    def broken(text, url):
        raise ValueError("fixture failure")

    rows, audit = parse_one_input(
        record, _identity(), reader=lambda _key: body, parser=broken,
    )
    assert rows == []
    assert audit["outcome"] == "failed"
    assert "fixture failure" in audit["error"]


def test_stage4_rows_round_trip_through_real_parquet_schemas(tmp_path):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.reconcile_april_detail import (
        _parsed_inputs_schema,
        _parsed_rows_schema,
        build_observation_rows,
    )

    _, record = _parse_record()
    rows, _, audit = build_observation_rows(record, _identity(), _primary(), [])
    audit["outcome"] = "parsed"
    row_path = tmp_path / "rows.parquet"
    input_path = tmp_path / "inputs.parquet"
    pq.write_table(pa.Table.from_pylist(rows, schema=_parsed_rows_schema()), row_path)
    pq.write_table(pa.Table.from_pylist([audit], schema=_parsed_inputs_schema()), input_path)
    recovered = pq.read_table(row_path).to_pylist()[0]
    recovered_input = pq.read_table(input_path).to_pylist()[0]
    assert recovered["listing_id"] == LISTING_A
    assert "artifact_id" not in recovered
    assert recovered_input["outcome"] == "parsed"


def test_parse_defaults_to_a_dry_run():
    args = parse_args(["parse"])
    assert args.mode == "parse"
    assert args.apply is False
    assert args.workers == 0


def test_parse_apply_gate_waits_for_every_unpack_member():
    from scripts.reconcile_april_detail import check_parse_apply_gate

    with pytest.raises(ReconcileError, match="Stage 3b may still be running"):
        check_parse_apply_gate(
            1,
            [("recovery/plan145/unpacked/one.parquet", [
                {"disposition": "written"},
            ])],
            input_count=1,
        )


# -- N: compare (Stage 5, slice 1) ---------------------------------------
#
# The predicate is `same listing_id AND abs(dt) <= 300 s`, from any source, and
# classification is an existence test -- these tests hold that boundary and the
# refusal to let a candidate supply identity or a value, the three-family
# partition, the global duplicate collapse, the Stage 4 gate, the run_id freeze
# and the read-only VIN snapshot.
from datetime import datetime as _dt  # noqa: E402
from datetime import timezone as _tz  # noqa: E402

from scripts.reconcile_april_detail import (  # noqa: E402
    COMPARE_WINDOW_US,
    EXPECTED_FLATTENED_INPUTS,
    DuplicateFingerprintConflict,
    LiteDup,
    _epoch_us,
    check_compare_apply_gate,
    classify_parsed_observation,
    compute_run_id,
    near_duplicate_window,
    resolve_global_duplicates,
    snapshot_vin_lookup,
)

_WHEN = _dt(2026, 4, 15, 12, 0, 0, tzinfo=_tz.utc)


def _prow(listing_id=LISTING_A, fetched_at=_WHEN, *, source="detail",
          fetched_at_source="legacy_manifest", **over):
    from scripts.reconcile_april_detail import _parsed_rows_schema

    row = {name: None for name in _parsed_rows_schema().names}
    row.update({
        "listing_id": listing_id,
        "fetched_at": fetched_at,
        "source": source,
        "listing_state": "active",
        "listing_id_source": "legacy_manifest",
        "fetched_at_source": fetched_at_source,
        "object_key": "html/year=2026/month=4/artifact_type=detail_page/x.html.zst",
        "content_sha256": "sha-x",
    })
    row.update(over)
    return row


def _series(*offsets_and_sources):
    base = _epoch_us(_WHEN)
    return [(base + off, src) for off, src in offsets_and_sources]


def test_one_candidate_inside_300s_is_represented():
    verdict = classify_parsed_observation(
        _prow(), {LISTING_A: _series((299_000_000, "detail"))},
    )
    assert verdict["family"] == "already_represented"
    assert verdict["match_count"] == 1


def test_a_candidate_301s_away_is_not_represented():
    verdict = classify_parsed_observation(
        _prow(), {LISTING_A: _series((301_000_000, "detail"))},
    )
    assert verdict["family"] == "to_import"


def test_a_candidate_exactly_300s_away_is_represented_since_the_plan_says_lte():
    # The plan writes `abs(delta) <= 300 s`, so the boundary is inclusive.
    verdict = classify_parsed_observation(
        _prow(), {LISTING_A: _series((COMPARE_WINDOW_US, "detail"))},
    )
    assert verdict["family"] == "already_represented"
    assert verdict["nearest_distance_s"] == 300.0


def test_a_candidate_differing_only_in_source_still_counts_as_coverage():
    verdict = classify_parsed_observation(
        _prow(source="detail"),
        {LISTING_A: _series((0, "carousel"))},
    )
    assert verdict["family"] == "already_represented"
    assert verdict["match_sources"] == ["carousel"]


def test_differing_vin_price_or_artifact_id_never_affect_classification():
    index = {LISTING_A: _series((10_000_000, "detail"))}
    a = classify_parsed_observation(_prow(vin="AAA", price=1), index)
    b = classify_parsed_observation(_prow(vin="ZZZ", price=999999), index)
    assert a == b
    assert a["family"] == "already_represented"


def test_multiple_candidates_yield_one_classification_and_no_candidate_values():
    verdict = classify_parsed_observation(
        _prow(),
        {LISTING_A: _series((5_000_000, "carousel"), (-20_000_000, "detail"),
                            (120_000_000, "detail"))},
    )
    assert verdict["family"] == "already_represented"
    assert verdict["match_count"] == 3
    assert verdict["nearest_distance_s"] == 5.0
    assert verdict["match_sources"] == ["carousel", "detail"]
    # nothing on the evidence row comes from a candidate row
    assert set(verdict) == {
        "family", "reason", "match_count", "nearest_distance_s", "match_sources",
    }


def test_a_row_with_no_capture_time_is_unclassifiable_never_to_import():
    verdict = classify_parsed_observation(
        _prow(fetched_at=None, fetched_at_source="none"),
        {LISTING_A: _series((0, "detail"))},
    )
    assert verdict["family"] == "unclassifiable"
    assert verdict["reason"] == "no_capture_time"


def test_a_row_with_no_listing_id_is_unclassifiable_never_to_import():
    # Tier-2 identity can resolve a real capture time but a NULL listing_id.
    # staging.silver_observations.listing_id is NOT NULL, so such a row is no
    # more importable than a tier-3 page with no time -- and grouping it under
    # (None, fetched_at) would fabricate duplicate-fingerprint conflicts.
    verdict = classify_parsed_observation(
        _prow(listing_id=None, fetched_at_source="queue_events"),
        {None: _series((0, "detail"))},
    )
    assert verdict["family"] == "unclassifiable"
    assert verdict["reason"] == "no_listing_id"


def test_the_three_families_partition_the_parsed_rows_exactly():
    index = {LISTING_A: _series((0, "detail"))}
    rows = [
        _prow(LISTING_A),                                    # represented
        _prow(LISTING_B),                                    # to_import
        _prow(LISTING_B, fetched_at=None, fetched_at_source="none"),  # unclassifiable
        _prow(listing_id=None, fetched_at_source="queue_events"),     # unclassifiable
    ]
    families = [classify_parsed_observation(r, index)["family"] for r in rows]
    assert sorted(families) == [
        "already_represented", "to_import", "unclassifiable", "unclassifiable",
    ]
    assert len(families) == len(rows)


def test_cross_shard_duplicate_collapse_is_deterministic_regardless_of_shard_order():
    # Same (listing_id, fetched_at) and business fingerprint, two Stage 4 shards,
    # different object keys. Feeding the two records in both orders must pick the
    # same uid as winner -- this is what makes a resumed or reordered run
    # reproducible.
    a = LiteDup(1, LISTING_A, "2026-04-15T12:00:00+00:00", 0, "fp",
                "detail", "html/k-a.zst", "h-a")
    b = LiteDup(2, LISTING_A, "2026-04-15T12:00:00+00:00", 0, "fp",
                "carousel", "html/k-b.zst", "h-b")
    win1, lose1, rep1 = resolve_global_duplicates([a, b])
    win2, lose2, _ = resolve_global_duplicates([b, a])
    assert win1 == win2 == {1}          # detail beats carousel, both orders
    assert lose1 == lose2 == {2}
    assert rep1["groups_collapsed"] == 1
    assert rep1["rows_moved_to_already_represented"] == 1


def test_duplicates_with_differing_business_fingerprints_stop_the_run():
    a = LiteDup(1, LISTING_A, "2026-04-15T12:00:00+00:00", 0, "fp-1",
                "detail", "html/k-a.zst", "h-a")
    b = LiteDup(2, LISTING_A, "2026-04-15T12:00:00+00:00", 0, "fp-2",
                "detail", "html/k-b.zst", "h-b")
    with pytest.raises(DuplicateFingerprintConflict) as excinfo:
        resolve_global_duplicates([a, b])
    assert len(excinfo.value.conflicts) == 1
    assert excinfo.value.conflicts[0]["listing_id"] == LISTING_A


def test_the_gate_refuses_fewer_than_1204_completed_units():
    with pytest.raises(ReconcileError, match="both must equal 1204"):
        check_compare_apply_gate(
            {"completed_units": 1203, "planned_units": 1203,
             "totals": {"inputs": EXPECTED_FLATTENED_INPUTS, "rows": 10}},
            real_observation_total=10,
        )


def test_the_gate_refuses_a_row_total_that_disagrees_with_the_real_counts():
    with pytest.raises(ReconcileError, match="disagrees with the summed"):
        check_compare_apply_gate(
            {"completed_units": 1204, "planned_units": 1204,
             "totals": {"inputs": EXPECTED_FLATTENED_INPUTS, "rows": 5_800_000}},
            real_observation_total=5_799_999,
        )


def test_the_gate_passes_when_stage_4_is_complete_and_totals_reconcile():
    check_compare_apply_gate(
        {"completed_units": 1204, "planned_units": 1204,
         "totals": {"inputs": EXPECTED_FLATTENED_INPUTS, "rows": 42}},
        real_observation_total=42,
    )


def test_a_changed_input_etag_forces_a_new_run_id():
    core = {"parsed_rows": [{"key": "rows/a.parquet", "size": 10, "etag": "e1"}],
            "silver": [{"key": "s/1.parquet", "size": 99, "etag": "s1"}]}
    before = compute_run_id(core)
    core["silver"][0]["etag"] = "s2"
    assert compute_run_id(core) != before


def test_the_near_duplicate_window_counts_adjacent_gaps_not_all_pairs():
    base = 0
    measured = near_duplicate_window(iter([
        # LISTING_A: a 3-capture burst -- 2 adjacent gaps <=300 s (not 3 pairs),
        # so the count stays linear even for a listing with many captures.
        (LISTING_A, base),
        (LISTING_A, base + 100_000_000),   # gap 100 s
        (LISTING_A, base + 200_000_000),   # gap 100 s
        (LISTING_A, base + 900_000_000),   # gap 700 s -> not counted
        (LISTING_B, base),                 # lone capture -> no neighbour
    ]))
    # the burst is 3 captures with a neighbour (2 adjacent gaps), not 3 pairs;
    # the isolated 900 s capture and LISTING_B's lone capture have none.
    assert measured == {
        "adjacent_pairs_within_300s": 2,
        "listings_involved": 1,
        "captures_with_a_neighbour": 3,
    }


def test_the_vin_snapshot_query_is_read_only():
    class _FakeCursor:
        def __init__(self):
            self.statements = []

        def execute(self, sql, params=None):
            self.statements.append(sql)
            self._rows = [(LISTING_A, "VIN-A")]

        def fetchall(self):
            return self._rows

    cur = _FakeCursor()
    ids = [f"{i:08d}-2222-3333-4444-555555555555" for i in range(2500)]
    result = snapshot_vin_lookup(ids + [LISTING_A, "not-a-uuid"], cur, batch_size=1000)
    assert result == {LISTING_A: "VIN-A"}
    assert len(cur.statements) == 3                    # 2501 distinct ids / 1000
    for sql in cur.statements:
        upper = sql.upper()
        assert upper.lstrip().startswith("SELECT")
        assert "INSERT" not in upper
        assert "UPDATE" not in upper
        assert "DELETE" not in upper


def test_compare_defaults_to_a_dry_run():
    args = parse_args(["compare"])
    assert args.mode == "compare"
    assert args.apply is False
    assert args.probe is False
    assert args.duckdb_threads == 1


# -- N.1: run_compare end to end, against a fake object store -------------

def _write_parsed_rows_fixture(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.reconcile_april_detail import _parsed_rows_schema

    pq.write_table(
        pa.Table.from_pylist(rows, schema=_parsed_rows_schema()), path,
        compression="zstd",
    )
    return path.read_bytes()


def _write_silver_fixture(path, listing_ids, when):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("listing_id", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    ])
    pq.write_table(
        pa.Table.from_pylist(
            [{"listing_id": lid, "fetched_at": when} for lid in listing_ids],
            schema=schema,
        ),
        path, compression="zstd",
    )
    return path.read_bytes()


class _FakeS3Store:
    def __init__(self, store):
        self.store = store

    def list_objects_v2(self, **kw):
        prefix = kw.get("Prefix", "")
        delim = kw.get("Delimiter")
        keys = [k for k in self.store if k.startswith(prefix)]
        if delim:
            commons = set()
            for key in keys:
                rest = key[len(prefix):]
                if delim in rest:
                    commons.add(prefix + rest.split(delim, 1)[0] + delim)
            return {"CommonPrefixes": [{"Prefix": p} for p in sorted(commons)],
                    "IsTruncated": False}
        return {
            "Contents": [
                {"Key": key, "Size": len(self.store[key]),
                 "ETag": '"' + hashlib.md5(self.store[key]).hexdigest() + '"'}
                for key in sorted(keys)
            ],
            "IsTruncated": False,
        }

    def get_object(self, Bucket, Key, Range=None):
        return {"Body": _Body(self.store[Key])}


class _FakeConn:
    def __init__(self, rows):
        self._rows = rows
        self.rolled_back = False

    def cursor(self):
        rows = self._rows

        class _Cur:
            def __enter__(self_):
                return self_

            def __exit__(self_, *a):
                return False

            def execute(self_, sql, params=None):
                assert sql.lstrip().upper().startswith("SELECT")
                self_._out = rows

            def fetchall(self_):
                return self_._out

        return _Cur()

    def rollback(self):
        self.rolled_back = True

    def close(self):
        pass


def _compare_fixture_store(tmp_path):
    """A minimal but complete input set: two parsed row shards, the nine
    March-May silver objects, a queue-event object and a parse_report."""
    l1 = "11111111-1111-1111-1111-111111111111"   # represented
    l2 = "22222222-2222-2222-2222-222222222222"   # to_import singleton
    l3 = "33333333-3333-3333-3333-333333333333"   # unclassifiable (no time)
    l4 = "44444444-4444-4444-4444-444444444444"   # duplicated across two shards

    shard_a = [
        _prow(l1, _WHEN, object_key="html/a1.zst", content_sha256="s1", price=100),
        _prow(l2, _WHEN, object_key="html/a2.zst", content_sha256="s2", price=200),
        _prow(l3, None, fetched_at_source="none", object_key="html/a3.zst",
              content_sha256="s3"),
        _prow(l4, _WHEN, object_key="html/a4.zst", content_sha256="s4", price=400),
    ]
    shard_b = [
        _prow(l4, _WHEN, object_key="html/b4.zst", content_sha256="s4b", price=400),
    ]

    store = {}
    store["recovery/plan145/parsed/rows/materialized-a.parquet"] = \
        _write_parsed_rows_fixture(tmp_path / "rows-a.parquet", shard_a)
    store["recovery/plan145/parsed/rows/materialized-b.parquet"] = \
        _write_parsed_rows_fixture(tmp_path / "rows-b.parquet", shard_b)
    store["recovery/plan145/parsed/inputs/materialized-a.parquet"] = b"inputs-a"
    store["recovery/plan145/parsed/inputs/materialized-b.parquet"] = b"inputs-b"

    # The nine compacted objects: three sources x March/April/May. Only
    # detail/2026-04 carries a row that matches a parsed observation (l1); the
    # rest are legitimately empty for this fixture.
    for source in ("detail", "carousel", "listings_page"):
        for month in (3, 4, 5):
            listings = [l1] if (source == "detail" and month == 4) else []
            store[
                f"silver_normalized/observations/source={source}/obs_year=2026/"
                f"obs_month={month}/part-{source}-{month}.parquet"
            ] = _write_silver_fixture(
                tmp_path / f"silver-{source}-{month}.parquet", listings, _WHEN,
            )

    store["ops_normalized/artifacts_queue_events/year=2026/month=4/"
          "part-q.parquet"] = b"queue-events"
    return store, (l1, l2, l3, l4)


def _patch_compare_io(monkeypatch, store, vin_rows, *, rows=5):
    import scripts.reconcile_april_detail as mod
    import shared.db as db
    import shared.minio as minio

    monkeypatch.setattr(mod, "_s3_client", lambda: _FakeS3Store(store))
    monkeypatch.setattr(minio, "object_exists", lambda k: k in store)
    monkeypatch.setattr(
        minio, "write_bytes",
        lambda k, data, content_type=None: store.__setitem__(k, bytes(data)),
    )
    monkeypatch.setattr(minio, "read_json", lambda _path: {
        "completed_units": 1204, "planned_units": 1204,
        "totals": {"inputs": EXPECTED_FLATTENED_INPUTS, "rows": rows},
    })
    monkeypatch.setattr(db, "get_conn", lambda: _FakeConn(vin_rows))


def test_run_compare_apply_partitions_and_freezes_then_reruns_as_a_noop(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, (l1, _l2, _l3, _l4) = _compare_fixture_store(tmp_path)
    _patch_compare_io(monkeypatch, store, [(l1, "VIN-L1")])

    rc = mod.run_compare(mod.parse_args(["compare", "--apply"]))
    assert rc == 0

    run_dirs = {
        k.split("/compared/")[1].split("/")[0]
        for k in store if "/compared/" in k
    }
    assert len(run_dirs) == 1
    run_id = run_dirs.pop()
    assert run_id.startswith("cmp-")

    import io as _io

    import pyarrow.parquet as _pq

    def _family_rows(family):
        total = []
        for key, blob in store.items():
            if f"/compared/{run_id}/{family}/" in key:
                total += _pq.read_table(_io.BytesIO(blob)).to_pylist()
        return total

    represented = _family_rows("already_represented")
    to_import = _family_rows("to_import")
    unclassifiable = _family_rows("unclassifiable")

    assert len(represented) == 2          # l1 (silver) + one l4 recovery_duplicate
    assert len(to_import) == 2            # l2 + the winning l4
    assert len(unclassifiable) == 1       # l3, no capture time
    assert len(represented) + len(to_import) + len(unclassifiable) == 5

    reasons = sorted(r["reason"] for r in represented)
    assert reasons == ["recovery_duplicate", "silver_candidate"]
    assert unclassifiable[0]["reason"] == "no_capture_time"
    assert {r["listing_id"] for r in to_import} == {
        "22222222-2222-2222-2222-222222222222",
        "44444444-4444-4444-4444-444444444444",
    }

    # the freeze and the read-only VIN snapshot both landed
    assert f"recovery/plan145/inventory/{run_id}.json" in store
    assert f"recovery/plan145/vin_snapshot/{run_id}.parquet" in store
    assert f"recovery/plan145/compared/{run_id}/compare_report.json" in store
    report = json.loads(
        store[f"recovery/plan145/compared/{run_id}/compare_report.json"])
    assert report["families"]["sum"] == 5
    assert report["duplicates"]["groups_collapsed"] == 1

    # a second run with the same inventory writes nothing new
    keys_before = set(store)
    rc = mod.run_compare(mod.parse_args(["compare", "--apply"]))
    assert rc == 0
    assert set(store) == keys_before


def test_run_compare_dry_run_writes_nothing_and_issues_no_vin_query(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, (l1, *_rest) = _compare_fixture_store(tmp_path)
    conn = _FakeConn([(l1, "VIN-L1")])
    _patch_compare_io(monkeypatch, store, [(l1, "VIN-L1")])
    import shared.db as db
    monkeypatch.setattr(db, "get_conn", lambda: conn)

    keys_before = set(store)
    rc = mod.run_compare(mod.parse_args(["compare"]))
    assert rc == 0
    assert set(store) == keys_before          # nothing written
    assert conn.rolled_back is False           # get_conn never called


def test_run_compare_apply_refuses_a_silver_shape_that_is_not_the_frozen_nine(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, (l1, *_rest) = _compare_fixture_store(tmp_path)
    for key in [k for k in store if "/source=carousel/" in k or "/source=listings_page/" in k]:
        del store[key]                       # leaves 3 silver objects, not 9
    _patch_compare_io(monkeypatch, store, [(l1, "VIN-L1")])

    with pytest.raises(ReconcileError, match="found 3"):
        mod.run_compare(mod.parse_args(["compare", "--apply"]))

    # the escape hatch lets a maintainer proceed once they have ruled on it
    rc = mod.run_compare(
        mod.parse_args(["compare", "--apply", "--allow-silver-shape-drift"]))
    assert rc == 0
    run_id = next(k for k in store if "/inventory/" in k).split("/")[-1][:-5]
    report = json.loads(
        store[f"recovery/plan145/compared/{run_id}/compare_report.json"])
    assert report["refusals"][0]["kind"] == "silver_object_count"
    assert report["refusals"][0]["enforced"] is False


def test_run_compare_apply_stops_on_any_no_listing_id_row_until_the_maintainer_rules(
        tmp_path, monkeypatch):
    import io as _io

    import pyarrow.parquet as _pq

    import scripts.reconcile_april_detail as mod

    store, (l1, *_rest) = _compare_fixture_store(tmp_path)
    # a tier-2 capture: a real fetched_at, but no listing_id resolved.
    store["recovery/plan145/parsed/rows/materialized-c.parquet"] = \
        _write_parsed_rows_fixture(
            tmp_path / "rows-c.parquet",
            [_prow(listing_id=None, fetched_at=_WHEN, fetched_at_source="queue_events",
                   object_key="html/c1.zst", content_sha256="sc1")],
        )
    _patch_compare_io(monkeypatch, store, [(l1, "VIN-L1")], rows=6)

    # default ceiling is 0: any non-zero no_listing_id cohort stops an --apply run
    with pytest.raises(ReconcileError, match="no_listing_id rows 1"):
        mod.run_compare(mod.parse_args(["compare", "--apply"]))

    # the maintainer acknowledges it by setting the ceiling to the measured
    # number -- the workflow the help text describes, which leaves the
    # no_capture_time ceiling armed (unlike --allow-unclassifiable-drift)
    rc = mod.run_compare(
        mod.parse_args(["compare", "--apply", "--max-no-listing-id", "1"]))
    assert rc == 0
    run_id = next(k for k in store if "/inventory/" in k).split("/")[-1][:-5]
    report = json.loads(
        store[f"recovery/plan145/compared/{run_id}/compare_report.json"])
    assert report["unclassifiable"]["no_listing_id"] == 1
    assert report["unclassifiable"]["no_capture_time"] == 1        # l3, unchanged
    assert report["unclassifiable"]["materially_larger"] is False  # ceiling now met
    assert report["families"]["sum"] == 6
    # the row landed in the unclassifiable family, never to_import
    unc = [
        row
        for key, blob in store.items() if f"/compared/{run_id}/unclassifiable/" in key
        for row in _pq.read_table(_io.BytesIO(blob)).to_pylist()
    ]
    assert {r["reason"] for r in unc} == {"no_listing_id", "no_capture_time"}
    to_import = [
        row
        for key, blob in store.items() if f"/compared/{run_id}/to_import/" in key
        for row in _pq.read_table(_io.BytesIO(blob)).to_pylist()
    ]
    assert to_import and all(r["listing_id"] is not None for r in to_import)


def test_run_compare_probe_measures_the_no_listing_id_cohort_instead_of_dying(
        tmp_path, monkeypatch, capsys):
    # The probe run's whole job is to learn this cohort's size against the
    # completed Stage 4 units. The gate must not stop it: nothing to protect,
    # since a probe writes nothing that can advance slice 2.
    import scripts.reconcile_april_detail as mod

    store, (l1, *_rest) = _compare_fixture_store(tmp_path)
    store["recovery/plan145/parsed/rows/materialized-c.parquet"] = \
        _write_parsed_rows_fixture(
            tmp_path / "rows-c.parquet",
            [_prow(listing_id=None, fetched_at=_WHEN, fetched_at_source="queue_events",
                   object_key="html/c1.zst", content_sha256="sc1")],
        )
    _patch_compare_io(monkeypatch, store, [(l1, "VIN-L1")], rows=6)
    written_before = set(store)

    rc = mod.run_compare(mod.parse_args(["compare", "--probe"]))
    assert rc == 0                                   # measured, did not die
    assert set(store) == written_before              # a bare probe writes nothing
    out = capsys.readouterr().out
    assert "no_listing_id 1" in out

    # --probe --apply writes only under the disposable *_probe prefix
    rc = mod.run_compare(mod.parse_args(["compare", "--probe", "--apply"]))
    assert rc == 0
    probe_report = next(
        v for k, v in store.items()
        if k.startswith("recovery/plan145/compared_probe/")
        and k.endswith("compare_report.json")
    )
    report = json.loads(probe_report)
    assert report["probe"] is True
    assert report["unclassifiable"]["no_listing_id"] == 1
    assert report["unclassifiable"]["materially_larger"] is True   # default ceiling 0
    assert not any(k.startswith("recovery/plan145/compared/") for k in store)


# -- O: assign + apply (Stage 5, slice 2) --------------------------------
#
# The first Plan 145 mode that writes to Postgres. These tests hold the four
# invariants a reviewer cannot check by reading: identity comes from a
# preserved queue event or from `nextval` and never from `max()`; one source
# object is one artifact_id across its primary and every carousel row, in one
# batch and one transaction; a carousel row mints no price event; and every
# written time is the legacy capture time rather than `now()`.

import io  # noqa: E402
import re  # noqa: E402

from scripts.reconcile_april_detail import (  # noqa: E402
    ID_ALLOCATED,
    ID_PRESERVED,
    MAX_BATCH_ARTIFACTS,
    MAX_BATCH_SILVER_ROWS,
    RECOVERED_STATUS,
    ImportSetInvalid,
    ReceiptConflict,
    _assigned_key,
    allocate_artifact_ids,
    assign_batch_name,
    assign_identities,
    build_queue_artifact_ids,
    build_recovery_price_event,
    build_recovery_queue_event,
    build_recovery_silver_row,
    check_batch_receipt,
    plan_import_batches,
    validate_import_listing_id,
    write_import_batch,
)

_RUN = "cmp-slice2fixture01"
_MAT_KEY = "html/year=2026/month=4/artifact_type=detail_page/mat1.html.zst"
_PACK_KEPT = "html/2026/04/pack/orig1.html.zst"
_PACK_ORPHAN = "html/2026/04/pack/orig2.html.zst"
_PRESERVED_ID = 4_902_400


class _FakeCursor:
    """Records every statement so a test can assert on the SQL that was sent."""

    def __init__(self, owner):
        self.owner = owner

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False

    def execute(self, sql, params=None):
        self.owner.sql.append((sql, params))
        self.owner.ops.append(("execute", sql))
        self.owner.on_execute(sql, params)

    def fetchall(self):
        return self.owner.result

    def close(self):
        pass


class _FakeWriteConn:
    """A connection that answers nextval and receipt reads and counts commits."""

    def __init__(self, *, next_id=9_000_000, receipts=None, fail_on=None):
        self.sql = []
        self.result = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self._next_id = next_id
        self.receipts = receipts or {}
        self.fail_on = fail_on
        self.executed_values = []
        # An ordered log across cur.execute, execute_values, commit and
        # rollback, so a test can assert the *sequence* the writer issued --
        # which sql and executed_values alone cannot show interleaved.
        self.ops = []

    def on_execute(self, sql, params):
        if self.fail_on and self.fail_on in sql:
            raise RuntimeError("injected failure")
        if "nextval" in sql:
            count = params[0]
            self.result = [(self._next_id + i,) for i in range(count)]
            self._next_id += count
        elif "manifest_sha256 FROM" in sql:
            self.result = [(d,) for d in self.receipts.get(params[0], [])]
        else:
            self.result = []

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1
        self.ops.append(("commit", None))

    def rollback(self):
        self.rollbacks += 1
        self.ops.append(("rollback", None))

    def close(self):
        self.closed = True


def _ti_row(listing_id, object_key, *, source="detail", when=_WHEN,
            listing_state="active", **over):
    row = _prow(listing_id, when, source=source, object_key=object_key,
                listing_state=listing_state, **over)
    row["reason"] = None
    return row


def _write_compared_shard(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.reconcile_april_detail import _compared_schema

    schema = _compared_schema("to_import")
    pq.write_table(
        pa.Table.from_pylist(
            [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
        ),
        path, compression="zstd",
    )
    return path.read_bytes()


def _write_inputs_shard(path, records):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.reconcile_april_detail import _parsed_inputs_schema

    schema = _parsed_inputs_schema()
    pq.write_table(
        pa.Table.from_pylist(
            [{k: r.get(k) for k in schema.names} for r in records], schema=schema,
        ),
        path, compression="zstd",
    )
    return path.read_bytes()


def _write_queue_events_shard(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("minio_path", pa.string()),
        pa.field("artifact_id", pa.int64()),
        pa.field("artifact_type", pa.string()),
        pa.field("listing_id", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    ])
    pq.write_table(
        pa.Table.from_pylist(
            [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
        ),
        path, compression="zstd",
    )
    return path.read_bytes()


def _write_vin_shard(path, pairs):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([pa.field("listing_id", pa.string()),
                        pa.field("vin", pa.string())])
    pq.write_table(
        pa.Table.from_pylist(
            [{"listing_id": k, "vin": v} for k, v in pairs], schema=schema,
        ),
        path, compression="zstd",
    )
    return path.read_bytes()


def _slice2_fixture_store(tmp_path):
    """One materialized object with a carousel row, one preserved pack member,
    and one pack member from the unattributed cohort."""
    l1 = "aaaaaaaa-1111-1111-1111-111111111111"   # mat1 primary
    l2 = "bbbbbbbb-2222-2222-2222-222222222222"   # mat1 carousel hint
    l3 = "cccccccc-3333-3333-3333-333333333333"   # orig1 primary (preserved)
    l4 = "dddddddd-4444-4444-4444-444444444444"   # orig2 primary (unlisted)

    store = {}
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-a.parquet"] = \
        _write_compared_shard(tmp_path / "ti-a.parquet", [
            _ti_row(l1, _MAT_KEY, price=31000, make="Honda", model="CR-V",
                    vin="VIN-PARSED-L1"),
            _ti_row(l2, _MAT_KEY, source="carousel", price=17000),
        ])
    store[f"recovery/plan145/compared/{_RUN}/to_import/unpacked-b.parquet"] = \
        _write_compared_shard(tmp_path / "ti-b.parquet", [
            _ti_row(l3, _PACK_KEPT, price=22000, make="Toyota", model="RAV4"),
            _ti_row(l4, _PACK_ORPHAN, listing_state="unlisted", make="Kia",
                    model="Niro"),
        ])
    store[f"recovery/plan145/compared/{_RUN}/compare_report.json"] = b"{}"
    store[f"recovery/plan145/inventory/{_RUN}.json"] = b"{}"

    store["recovery/plan145/parsed/inputs/materialized-a.parquet"] = \
        _write_inputs_shard(tmp_path / "in-a.parquet", [
            {"object_key": _MAT_KEY, "listing_id": l1, "fetched_at": _WHEN,
             "input_kind": "materialized"},
        ])
    store["recovery/plan145/parsed/inputs/unpacked-b.parquet"] = \
        _write_inputs_shard(tmp_path / "in-b.parquet", [
            {"object_key": _PACK_KEPT, "listing_id": l3, "fetched_at": _WHEN,
             "input_kind": "unpacked"},
            {"object_key": _PACK_ORPHAN, "listing_id": l4, "fetched_at": _WHEN,
             "input_kind": "unpacked"},
        ])

    for month in (3, 4, 5):
        rows = []
        if month == 4:
            rows = [
                {"minio_path": f"s3://bronze/{_PACK_KEPT}",
                 "artifact_id": _PRESERVED_ID, "artifact_type": "detail_page",
                 "listing_id": l3, "fetched_at": _WHEN},
                # A results_page event on the same path family must not leak in.
                {"minio_path": "s3://bronze/html/2026/04/pack/srp.html.zst",
                 "artifact_id": 7, "artifact_type": "results_page",
                 "listing_id": None, "fetched_at": _WHEN},
            ]
        store[f"ops_normalized/artifacts_queue_events/year=2026/month={month}/"
              f"part-{month}.parquet"] = _write_queue_events_shard(
            tmp_path / f"qe-{month}.parquet", rows)

    store[f"recovery/plan145/vin_snapshot/{_RUN}.parquet"] = _write_vin_shard(
        tmp_path / "vin.parquet", [(l2, "VIN-SNAPSHOT-L2"), (l4, "VIN-SNAPSHOT-L4")],
    )
    return store, (l1, l2, l3, l4)


def _patch_slice2_io(monkeypatch, store, conn=None):
    import scripts.reconcile_april_detail as mod
    import shared.db as db
    import shared.minio as minio

    monkeypatch.setattr(mod, "_s3_client", lambda: _FakeS3Store(store))
    monkeypatch.setattr(minio, "object_exists", lambda k: k.split("bronze/")[-1] in store
                        or k in store)
    monkeypatch.setattr(
        minio, "write_bytes",
        lambda k, data, content_type=None: store.__setitem__(k, bytes(data)),
    )
    monkeypatch.setattr(
        minio, "read_json",
        lambda path: json.loads(store[path.split("bronze/")[-1]].decode()),
    )
    monkeypatch.setattr(db, "get_conn", lambda: conn)
    return conn


# -- O.1: identity -------------------------------------------------------

def test_a_path_with_one_queue_event_preserves_it_and_the_rest_allocate():
    queue_ids = {_PACK_KEPT: _PRESERVED_ID}
    objects = [{"object_key": _PACK_KEPT, "silver_rows": 1},
               {"object_key": _MAT_KEY, "silver_rows": 2}]
    calls = []

    def _allocate(count):
        calls.append(count)
        return [9_000_001]

    assigned = assign_identities(objects, queue_ids, _allocate)
    by_key = {row["object_key"]: row for row in assigned}
    assert by_key[_PACK_KEPT]["artifact_id"] == _PRESERVED_ID
    assert by_key[_PACK_KEPT]["id_source"] == ID_PRESERVED
    assert by_key[_MAT_KEY]["artifact_id"] == 9_000_001
    assert by_key[_MAT_KEY]["id_source"] == ID_ALLOCATED
    assert calls == [1]          # one round trip for the whole batch


def test_allocation_uses_nextval_and_never_max():
    conn = _FakeWriteConn(next_id=7_732_178)
    cur = conn.cursor()
    assert allocate_artifact_ids(cur, 3) == [7_732_178, 7_732_179, 7_732_180]
    sql = " ".join(s for s, _ in conn.sql)
    assert "nextval('ops.artifacts_queue_artifact_id_seq')" in sql
    assert "max(" not in sql.lower()


def test_allocating_zero_ids_issues_no_statement():
    conn = _FakeWriteConn()
    assert allocate_artifact_ids(conn.cursor(), 0) == []
    assert conn.sql == []


def test_queue_identity_ignores_other_artifact_types_and_missing_ids():
    ids = build_queue_artifact_ids([
        {"minio_path": f"s3://bronze/{_PACK_KEPT}", "artifact_id": 5,
         "artifact_type": "detail_page"},
        {"minio_path": "s3://bronze/other.zst", "artifact_id": 6,
         "artifact_type": "results_page"},
        {"minio_path": "s3://bronze/nolist.zst", "artifact_id": None,
         "artifact_type": "detail_page"},
    ])
    assert ids == {_PACK_KEPT: 5}


def test_a_path_mapped_to_two_artifact_ids_stops_rather_than_choosing():
    with pytest.raises(ReconcileError, match="conflicting queue-event artifact"):
        build_queue_artifact_ids([
            {"minio_path": f"s3://bronze/{_PACK_KEPT}", "artifact_id": 5,
             "artifact_type": "detail_page"},
            {"minio_path": f"s3://bronze/{_PACK_KEPT}", "artifact_id": 6,
             "artifact_type": "detail_page"},
        ])


# -- O.2: batching -------------------------------------------------------

def test_the_artifact_cap_binds_and_never_splits_an_artifact():
    objects = [{"object_key": f"k{i:03d}", "silver_rows": 1} for i in range(5)]
    batches = plan_import_batches(objects, max_artifacts=2, max_silver_rows=1000)
    assert [len(b["objects"]) for b in batches] == [2, 2, 1]
    assert [b["bound_by"] for b in batches] == ["artifacts", "artifacts", "end"]


def test_the_row_cap_binds_before_the_artifact_cap_when_it_is_tighter():
    objects = [{"object_key": f"k{i:03d}", "silver_rows": 6} for i in range(5)]
    batches = plan_import_batches(objects, max_artifacts=100, max_silver_rows=12)
    assert [len(b["objects"]) for b in batches] == [2, 2, 1]
    assert batches[0]["bound_by"] == "silver_rows"


def test_an_artifact_larger_than_the_row_cap_becomes_its_own_batch():
    # Every row of one object shares one identity and one transaction, so the
    # cap yields rather than cutting the artifact in half.
    objects = [{"object_key": "a", "silver_rows": 1},
               {"object_key": "b", "silver_rows": 999},
               {"object_key": "c", "silver_rows": 1}]
    batches = plan_import_batches(objects, max_artifacts=100, max_silver_rows=10)
    assert [[o["object_key"] for o in b["objects"]] for b in batches] == \
        [["a"], ["b"], ["c"]]


def test_batches_are_ordered_by_object_key_independently_of_input_order():
    objects = [{"object_key": k, "silver_rows": 1} for k in ("c", "a", "b")]
    batches = plan_import_batches(objects, max_artifacts=2, max_silver_rows=99)
    assert [o["object_key"] for o in batches[0]["objects"]] == ["a", "b"]


def test_the_default_caps_are_the_plans_numbers():
    assert (MAX_BATCH_ARTIFACTS, MAX_BATCH_SILVER_ROWS) == (5000, 50000)


# -- O.3: the four writes ------------------------------------------------

def test_a_detail_row_mints_one_upserted_event_at_the_legacy_capture_time():
    silver = build_recovery_silver_row(
        _ti_row(LISTING_A, _MAT_KEY, price=1000, make="Honda", model="CR-V"),
        4242, {},
    )
    event = build_recovery_price_event(silver)
    assert event["event_type"] == "upserted"
    assert event["source"] == "detail"
    assert event["artifact_id"] == 4242
    assert event["event_at"] == _WHEN            # not now()


def test_an_unlisted_detail_row_mints_a_deleted_event():
    silver = build_recovery_silver_row(
        _ti_row(LISTING_A, _MAT_KEY, listing_state="unlisted"), 1, {},
    )
    assert build_recovery_price_event(silver)["event_type"] == "deleted"


def test_a_carousel_row_mints_no_price_event():
    # Production mints carousel events only for hints passing the search
    # configuration active at capture time, and April's is not recoverable.
    silver = build_recovery_silver_row(
        _ti_row(LISTING_A, _MAT_KEY, source="carousel", price=900), 1, {},
    )
    assert build_recovery_price_event(silver) is None


def test_a_non_uuid_listing_id_stops_at_the_silver_row(monkeypatch):
    with pytest.raises(ImportSetInvalid, match="non_uuid_listing_id"):
        build_recovery_silver_row(_ti_row("not-a-uuid", _MAT_KEY), 1, {})


@pytest.mark.parametrize("bad", [None, "", "not-a-uuid"])
def test_a_carousel_row_with_no_usable_listing_id_stops_rather_than_writing_it(bad):
    # `str(None)` is the four-character string "None", which
    # staging.silver_observations.listing_id (text NOT NULL) accepts happily --
    # so the cast would defeat the column that is supposed to catch this. A
    # carousel row never reaches build_recovery_price_event, so the silver
    # builder is the only check before the INSERT.
    with pytest.raises(ImportSetInvalid):
        build_recovery_silver_row(
            _ti_row(bad, _MAT_KEY, source="carousel", price=900), 1, {},
        )


def test_the_price_event_guard_still_stands_on_a_hand_built_row():
    # Second line of defence: even if a silver row reached the event minter
    # without passing the builder, the uuid NOT NULL column is not left to
    # catch it.
    with pytest.raises(ImportSetInvalid, match="non_uuid_listing_id"):
        build_recovery_price_event({
            "source": "detail", "listing_id": "not-a-uuid", "artifact_id": 1,
            "fetched_at": _WHEN, "listing_state": "active",
        })


def test_validate_import_listing_id_names_both_refusals():
    assert validate_import_listing_id(None) == "null_listing_id"
    assert validate_import_listing_id("") == "null_listing_id"
    assert validate_import_listing_id("nope") == "non_uuid_listing_id"
    assert validate_import_listing_id(LISTING_A) is None


def test_the_vin_snapshot_fills_a_missing_vin_and_never_beats_a_parsed_one():
    vin_map = {LISTING_A: "VIN-FROM-SNAPSHOT"}
    filled = build_recovery_silver_row(
        _ti_row(LISTING_A, _MAT_KEY, source="carousel"), 1, vin_map,
    )
    assert filled["vin"] == "VIN-FROM-SNAPSHOT"
    parsed = build_recovery_silver_row(
        _ti_row(LISTING_A, _MAT_KEY, vin="VIN-PARSED"), 1, vin_map,
    )
    assert parsed["vin"] == "VIN-PARSED"
    assert vin_map == {LISTING_A: "VIN-FROM-SNAPSHOT"}   # never written back


def test_the_silver_row_carries_the_legacy_capture_time_and_the_assigned_id():
    silver = build_recovery_silver_row(_ti_row(LISTING_A, _MAT_KEY), 555, {})
    assert silver["fetched_at"] == _WHEN
    assert silver["artifact_id"] == 555
    assert silver["listing_state"] == "active"


def test_the_queue_event_splits_the_capture_time_from_the_recovery_time():
    event = build_recovery_queue_event(
        {"object_key": _MAT_KEY, "artifact_id": 77, "listing_id": LISTING_A,
         "fetched_at": _WHEN}, "batch-1", "bronze",
    )
    assert event["status"] == RECOVERED_STATUS
    assert event["artifact_type"] == "detail_page"
    assert event["minio_path"] == f"s3://bronze/{_MAT_KEY}"
    assert event["fetched_at"] == _WHEN          # the April capture
    assert "event_at" not in event               # left to the now() default


# -- O.4: the receipt ----------------------------------------------------

def test_an_absent_receipt_lets_the_batch_run():
    conn = _FakeWriteConn(receipts={})
    assert check_batch_receipt(conn.cursor(), "b1", "a" * 64) == "absent"


def test_a_matching_receipt_reports_the_batch_already_committed():
    conn = _FakeWriteConn(receipts={"b1": ["a" * 64]})
    assert check_batch_receipt(conn.cursor(), "b1", "a" * 64) == "committed"


def test_the_same_batch_name_with_another_digest_stops_and_shows_both():
    conn = _FakeWriteConn(receipts={"b1": ["b" * 64]})
    with pytest.raises(ReceiptConflict) as exc:
        check_batch_receipt(conn.cursor(), "b1", "a" * 64)
    assert "b" * 64 in str(exc.value) and "a" * 64 in str(exc.value)


def test_a_committed_batch_writes_zero_rows_on_retry(monkeypatch):
    conn = _FakeWriteConn(receipts={"b1": ["a" * 64]})
    _record_execute_values(monkeypatch, conn)
    out = write_import_batch(conn, "b1", "a" * 64,
                             [{"listing_id": LISTING_A}], [{"listing_id": LISTING_A}],
                             [{"artifact_id": 1}])
    assert out["skipped"] is True
    assert out["silver"] == 0 and out["price_events"] == 0
    assert conn.executed_values == []            # nothing was inserted
    assert conn.commits == 0 and conn.rollbacks == 1


def _record_execute_values(monkeypatch, conn):
    import psycopg2.extras

    def _fake(cur, sql, rows, template=None, page_size=100):
        if conn.fail_on and conn.fail_on in sql:
            raise RuntimeError("injected failure")
        conn.executed_values.append((sql, list(rows)))
        conn.ops.append(("execute_values", sql))

    monkeypatch.setattr(psycopg2.extras, "execute_values", _fake)


def test_one_batch_is_one_transaction_with_the_receipt_inside_it(monkeypatch):
    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    out = write_import_batch(
        conn, "b1", "a" * 64,
        [build_recovery_silver_row(_ti_row(LISTING_A, _MAT_KEY), 7, {})],
        [build_recovery_price_event(
            build_recovery_silver_row(_ti_row(LISTING_A, _MAT_KEY), 7, {}))],
        [build_recovery_queue_event(
            {"object_key": _MAT_KEY, "artifact_id": 7, "listing_id": LISTING_A,
             "fetched_at": _WHEN}, "b1", "bronze")],
    )
    assert out == {"batch_name": "b1", "skipped": False, "silver": 1,
                   "price_events": 1, "queue_events": 1, "artifacts": 1}
    assert conn.commits == 1 and conn.rollbacks == 0
    receipt_sql = [s for s, _ in conn.sql if "plan145_recovery_batch_receipts" in s
                   and "INSERT" in s]
    assert len(receipt_sql) == 1
    assert [sql for sql, _ in conn.executed_values] != []


def test_a_failure_inside_the_batch_rolls_back_and_escapes(monkeypatch):
    # write_silver_observations_postgres would have logged a warning and
    # returned 0 here; this path must not.
    conn = _FakeWriteConn(fail_on="plan145_recovery_batch_receipts")
    _record_execute_values(monkeypatch, conn)
    with pytest.raises(RuntimeError, match="injected failure"):
        write_import_batch(conn, "b1", "a" * 64, [], [], [])
    assert conn.commits == 0 and conn.rollbacks == 1


def test_no_write_statement_names_the_protected_tables(monkeypatch):
    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    write_import_batch(
        conn, "b1", "a" * 64,
        [build_recovery_silver_row(_ti_row(LISTING_A, _MAT_KEY), 7, {})], [],
        [build_recovery_queue_event(
            {"object_key": _MAT_KEY, "artifact_id": 7, "listing_id": LISTING_A,
             "fetched_at": _WHEN}, "b1", "bronze")],
    )
    every_sql = " ".join(
        [s for s, _ in conn.sql] + [s for s, _ in conn.executed_values]
    ).lower()
    for forbidden in ("ops.artifacts_queue", "ops.price_observations",
                      "ops.vin_to_listing", "ops.blocked_cooldown",
                      "ops.detail_scrape_claims"):
        assert forbidden not in every_sql


# -- O.5: run_assign and run_apply end to end ----------------------------

def test_run_assign_dry_run_plans_without_touching_the_sequence(tmp_path,
                                                                monkeypatch, capsys):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    conn = _FakeWriteConn()
    _patch_slice2_io(monkeypatch, store, conn)
    before = set(store)

    assert mod.run_assign(mod.parse_args(["assign"])) == 0
    assert set(store) == before                  # no shard, no report
    assert conn.sql == []                        # nextval never issued
    out = capsys.readouterr().out
    assert "DRY RUN" in out and "preserved_queue_event" in out


def test_run_assign_writes_one_identity_per_object_shared_by_all_its_rows(
        tmp_path, monkeypatch):
    import pyarrow.parquet as pq

    import scripts.reconcile_april_detail as mod

    store, (l1, _l2, l3, l4) = _slice2_fixture_store(tmp_path)
    conn = _FakeWriteConn(next_id=9_000_001)
    _patch_slice2_io(monkeypatch, store, conn)

    assert mod.run_assign(mod.parse_args(["assign", "--apply"])) == 0

    batch_name = assign_batch_name(_RUN, 1)
    rows = pq.read_table(io.BytesIO(store[_assigned_key(batch_name)])).to_pylist()
    by_key = {r["object_key"]: r for r in rows}
    assert set(by_key) == {_MAT_KEY, _PACK_KEPT, _PACK_ORPHAN}

    # The preserved id comes from the queue event; the other two allocate.
    assert by_key[_PACK_KEPT]["artifact_id"] == _PRESERVED_ID
    assert by_key[_PACK_KEPT]["id_source"] == ID_PRESERVED
    assert by_key[_MAT_KEY]["id_source"] == ID_ALLOCATED
    assert by_key[_PACK_ORPHAN]["id_source"] == ID_ALLOCATED
    assert by_key[_MAT_KEY]["artifact_id"] != by_key[_PACK_ORPHAN]["artifact_id"]

    # mat1's primary and its carousel row are one artifact with two silver rows.
    assert by_key[_MAT_KEY]["silver_rows"] == 2
    assert by_key[_MAT_KEY]["detail_rows"] == 1
    assert by_key[_MAT_KEY]["listing_id"] == l1
    assert by_key[_PACK_KEPT]["listing_id"] == l3
    assert by_key[_PACK_ORPHAN]["listing_id"] == l4

    report = json.loads(store[f"recovery/plan145/assigned/{_RUN}-assign_report.json"])
    census = report["identity_census"]
    assert census[ID_PRESERVED] == 1 and census[ID_ALLOCATED] == 2
    # Exactly one unattributed pack member turned out to be import-bearing.
    assert census["unattributed_pack_members_now_import_bearing"] == 1


def test_a_rerun_after_a_crash_reuses_the_recorded_ids(tmp_path, monkeypatch):
    import pyarrow.parquet as pq

    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    first = _FakeWriteConn(next_id=9_000_001)
    _patch_slice2_io(monkeypatch, store, first)
    mod.run_assign(mod.parse_args(["assign", "--apply"]))
    key = _assigned_key(assign_batch_name(_RUN, 1))
    original = pq.read_table(io.BytesIO(store[key])).to_pylist()

    # A second run finds the shard present: it must not burn new sequence
    # values or rewrite the recorded identities.
    second = _FakeWriteConn(next_id=5_555_555)
    _patch_slice2_io(monkeypatch, store, second)
    mod.run_assign(mod.parse_args(["assign", "--apply"]))
    assert second.sql == []
    assert pq.read_table(io.BytesIO(store[key])).to_pylist() == original


def test_run_assign_never_reads_legacy_artifact_id(tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    requested = []
    real = mod._read_parquet_rows

    def _spy(client, bucket, key, *, columns=None):
        requested.append(columns)
        return real(client, bucket, key, columns=columns)

    monkeypatch.setattr(mod, "_read_parquet_rows", _spy)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign"]))

    named = {c for cols in requested if cols for c in cols}
    assert "legacy_artifact_id" not in named
    assert "artifact_id" in named                # the queue-event one, only


def test_a_null_listing_id_in_to_import_stops_assign_and_reports_the_cohort(
        tmp_path, monkeypatch, capsys):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-c.parquet"] = \
        _write_compared_shard(tmp_path / "ti-c.parquet", [
            _ti_row(None, "html/2026/04/pack/orig3.html.zst"),
        ])
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    before = set(store)

    with pytest.raises(ImportSetInvalid, match="null_listing_id"):
        mod.run_assign(mod.parse_args(["assign", "--apply"]))
    assert set(store) == before                  # a stop, before any shard
    assert "null_listing_id" in capsys.readouterr().out


def test_a_non_uuid_listing_id_stops_assign_before_any_write(tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-c.parquet"] = \
        _write_compared_shard(tmp_path / "ti-c.parquet", [
            _ti_row("12345", "html/2026/04/pack/orig3.html.zst"),
        ])
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    before = set(store)
    with pytest.raises(ImportSetInvalid, match="non_uuid_listing_id"):
        mod.run_assign(mod.parse_args(["assign", "--apply"]))
    assert set(store) == before


def test_reassigning_a_run_under_different_caps_is_refused(tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))
    with pytest.raises(ReconcileError, match="already assigned under caps"):
        mod.run_assign(mod.parse_args(
            ["assign", "--apply", "--max-artifacts", "1"],
        ))


def test_run_apply_dry_run_announces_the_blast_radius_and_writes_nothing(
        tmp_path, monkeypatch, capsys):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _patch_slice2_io(monkeypatch, store, conn)
    assert mod.run_apply(mod.parse_args(["apply"])) == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "staging.silver_observations" in out
    assert "ops.artifacts_queue" in out          # named as never touched
    assert re.search(r"^artifacts +3$", out, re.M)      # 3 source objects
    assert conn.sql == []                        # no statement at all


def test_run_apply_writes_four_things_per_batch_in_one_transaction(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, (l1, l2, l3, l4) = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn(next_id=9_000_001))
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)
    batch_name = assign_batch_name(_RUN, 1)
    assert mod.run_apply(mod.parse_args(
        ["apply", "--apply", "--batch", batch_name],
    )) == 0
    assert conn.commits == 1

    inserts = {"silver": None, "price": None, "queue": None}
    for sql, rows in conn.executed_values:
        if "silver_observations" in sql:
            inserts["silver"] = rows
        elif "price_observation_events" in sql:
            inserts["price"] = rows
        elif "artifacts_queue_events" in sql:
            inserts["queue"] = rows

    assert len(inserts["silver"]) == 4           # 3 detail + 1 carousel
    assert len(inserts["price"]) == 3            # detail rows only
    assert len(inserts["queue"]) == 3            # one per artifact

    from processing.writers.silver_writer import _POSTGRES_COLS
    from scripts.reconcile_april_detail import (
        _PRICE_EVENT_COLS,
        _QUEUE_EVENT_COLS,
    )

    silver = [dict(zip(_POSTGRES_COLS, row)) for row in inserts["silver"]]
    assert {r["fetched_at"] for r in silver} == {_WHEN}
    # mat1's primary and carousel row carry the one artifact_id assigned to it.
    mat_ids = {r["artifact_id"] for r in silver if r["listing_id"] in (l1, l2)}
    assert len(mat_ids) == 1
    # The carousel row's VIN came from the read-only snapshot.
    assert next(r for r in silver if r["listing_id"] == l2)["vin"] == "VIN-SNAPSHOT-L2"
    assert next(r for r in silver if r["listing_id"] == l1)["vin"] == "VIN-PARSED-L1"

    events = [dict(zip(_PRICE_EVENT_COLS, row)) for row in inserts["price"]]
    assert {e["event_at"] for e in events} == {_WHEN}
    assert {e["source"] for e in events} == {"detail"}
    assert {e["listing_id"] for e in events} == {l1, l3, l4}
    assert next(e for e in events if e["listing_id"] == l4)["event_type"] == "deleted"
    assert next(e for e in events if e["listing_id"] == l1)["event_type"] == "upserted"

    queue = [dict(zip(_QUEUE_EVENT_COLS, row)) for row in inserts["queue"]]
    assert {q["status"] for q in queue} == {RECOVERED_STATUS}
    assert {q["fetched_at"] for q in queue} == {_WHEN}
    assert next(q for q in queue if q["minio_path"].endswith("orig1.html.zst")
                )["artifact_id"] == _PRESERVED_ID


def test_run_apply_refuses_a_write_set_over_the_canary_row_budget(
        tmp_path, monkeypatch):
    # The gate is measured in rows, not batches: counting batches would let one
    # default-cap batch -- 50,000 silver rows -- through unapproved.
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _patch_slice2_io(monkeypatch, store, conn)
    with pytest.raises(ReconcileError, match="canary budget"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--max-unapproved-rows", "3"],
        ))
    assert conn.sql == []                    # refused before any statement


def test_a_named_approval_lets_an_oversized_write_set_through(tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)
    assert mod.run_apply(mod.parse_args(
        ["apply", "--apply", "--max-unapproved-rows", "3",
         "--maintainer-approval", "a-maintainer"],
    )) == 0
    assert conn.commits == 1


def test_several_canary_sized_batches_need_no_approval(tmp_path, monkeypatch):
    # Three one-artifact batches, four silver rows in total: a batch count
    # would have refused this, a row budget correctly permits it.
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply", "--max-artifacts", "1"]))

    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)
    assert mod.run_apply(mod.parse_args(["apply", "--apply"])) == 0
    assert conn.commits == 3                 # one transaction per batch


def test_the_canary_budget_leaves_room_for_the_plans_500_observation_canary():
    from scripts.reconcile_april_detail import (
        CANARY_ROW_BUDGET,
        MAX_BATCH_SILVER_ROWS,
    )

    assert 500 < CANARY_ROW_BUDGET < MAX_BATCH_SILVER_ROWS


def test_a_carousel_row_with_no_listing_id_stops_apply_before_any_write(
        tmp_path, monkeypatch):
    # assign would have caught it, but apply re-reads the shards independently
    # and is the last check before the INSERT.
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    rows = _read_to_import_fixture(store, "materialized-a")
    rows[1]["listing_id"] = None             # the carousel row
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-a.parquet"] = \
        _write_compared_shard(tmp_path / "ti-a-null.parquet", rows)

    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)
    with pytest.raises(ImportSetInvalid, match="null_listing_id"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--batch", assign_batch_name(_RUN, 1)],
        ))
    assert conn.executed_values == [] and conn.commits == 0


def _read_to_import_fixture(store, unit):
    import pyarrow.parquet as pq

    key = f"recovery/plan145/compared/{_RUN}/to_import/{unit}.parquet"
    return pq.read_table(io.BytesIO(store[key])).to_pylist()


# -- O.6: the bounded violation log --------------------------------------

def test_the_violation_log_counts_everything_and_keeps_a_bounded_sample():
    from scripts.reconcile_april_detail import ViolationLog

    log = ViolationLog(max_examples=3)
    for i in range(1000):
        log.add("null_fetched_at", object_key=f"k{i}")
    log.add("no_parsed_input_row", object_key="other")

    assert log.total == 1001
    assert log.counts == {"null_fetched_at": 1000, "no_parsed_input_row": 1}
    assert len(log.examples) == 3            # constant space, not 1,001 dicts
    assert bool(log) is True
    assert bool(ViolationLog()) is False


def test_a_systematic_failure_reports_its_true_size_from_a_bounded_sample(
        tmp_path, monkeypatch, capsys):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-c.parquet"] = \
        _write_compared_shard(tmp_path / "ti-c.parquet", [
            _ti_row(None, f"html/2026/04/pack/o{i}.html.zst") for i in range(50)
        ])
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())

    with pytest.raises(ImportSetInvalid) as exc:
        mod.run_assign(mod.parse_args(["assign", "--apply"]))
    # The count is the whole cohort; only the printed examples are capped.
    assert "'null_listing_id': 50" in str(exc.value)
    out = capsys.readouterr().out
    assert "null_listing_id" in out
    assert out.count("e.g.") <= 20


def test_run_apply_stops_when_the_compare_output_moved_under_the_assignment(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    # A row disappears from to_import after the identity was frozen.
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-a.parquet"] = \
        _write_compared_shard(tmp_path / "ti-a2.parquet", [
            _ti_row("aaaaaaaa-1111-1111-1111-111111111111", _MAT_KEY),
        ])
    conn = _FakeWriteConn()
    _patch_slice2_io(monkeypatch, store, conn)
    with pytest.raises(ReconcileError, match="row count their assignment recorded"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--batch", assign_batch_name(_RUN, 1)],
        ))
    assert conn.commits == 0


def test_assign_and_apply_both_default_to_a_dry_run():
    from scripts.reconcile_april_detail import parse_args

    assert parse_args(["assign"]).apply is False
    assert parse_args(["apply"]).apply is False
    assert parse_args(["apply"]).maintainer_approval is None


def test_assign_issues_nextval_and_never_an_insert(tmp_path, monkeypatch):
    # The assignment shard is written before any database insertion because
    # assign issues none: the only statement it may send is the sequence read.
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    conn = _FakeWriteConn(next_id=9_000_001)
    _patch_slice2_io(monkeypatch, store, conn)
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    assert conn.sql, "the sequence should have been read"
    for sql, _params in conn.sql:
        assert "nextval" in sql
        assert "insert" not in sql.lower()
    assert _assigned_key(assign_batch_name(_RUN, 1)) in store


def test_apply_refuses_a_run_whose_identities_were_never_assigned(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    conn = _FakeWriteConn()
    _patch_slice2_io(monkeypatch, store, conn)
    with pytest.raises(ReconcileError, match="run `assign --apply` first"):
        mod.run_apply(mod.parse_args(["apply", "--apply"]))
    assert conn.sql == []


# -- O.7: --probe on assign and apply (Plan 145 Stage 5 slice 2) --------
#
# --probe routes every Stage-5 read and write to a parallel *_probe prefix so
# slice 2 can be exercised against real data while Stage 4 is still parsing.
# assign --probe --apply calls the real nextval; apply --probe --apply runs the
# whole transaction against real Postgres and then ROLLS IT BACK. No probe run
# ever commits, and no override lifts that.

_AUTH_ONLY_KEY = "html/2026/04/pack/authonly.html.zst"

_PROBE_REMAP = (
    ("recovery/plan145/compared/", "recovery/plan145/compared_probe/"),
    ("recovery/plan145/inventory/", "recovery/plan145/inventory_probe/"),
    ("recovery/plan145/vin_snapshot/", "recovery/plan145/vin_snapshot_probe/"),
)


def _to_probe_store(store):
    """Re-key a slice-2 fixture store's Stage-5 outputs to their *_probe twins.

    ``parsed/``, ``ops_normalized/`` and ``assigned/`` are left alone: --probe
    reads the same parsed inputs and event lake, and the assignment shards are
    what the probe run itself writes.
    """
    out = {}
    for key, value in store.items():
        for auth, probe in _PROBE_REMAP:
            if key.startswith(auth):
                key = probe + key[len(auth):]
                break
        out[key] = value
    return out


def _probe_assigned_key(batch_name):
    return f"recovery/plan145/assigned_probe/{batch_name}.parquet"


def test_probe_defaults_to_off_on_assign_and_apply():
    from scripts.reconcile_april_detail import parse_args

    assert parse_args(["assign"]).probe is False
    assert parse_args(["apply"]).probe is False


def test_probe_assign_reads_the_probe_run_and_ignores_a_same_named_authoritative_one(
        tmp_path, monkeypatch):
    import pyarrow.parquet as pq

    import scripts.reconcile_april_detail as mod

    auth_store, _ = _slice2_fixture_store(tmp_path)
    store = _to_probe_store(auth_store)
    # A same-named authoritative run, complete, with a DIFFERENT object in its
    # to_import family. A probe assign must not read a byte of it.
    store[f"recovery/plan145/compared/{_RUN}/compare_report.json"] = b"{}"
    store[f"recovery/plan145/inventory/{_RUN}.json"] = b"{}"
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-x.parquet"] = \
        _write_compared_shard(
            tmp_path / "ti-x.parquet",
            [_ti_row("eeeeeeee-5555-5555-5555-555555555555", _AUTH_ONLY_KEY)],
        )

    conn = _FakeWriteConn(next_id=9_000_001)
    _patch_slice2_io(monkeypatch, store, conn)
    assert mod.run_assign(mod.parse_args(["assign", "--probe", "--apply"])) == 0

    shard = _probe_assigned_key(assign_batch_name(_RUN, 1))
    assert shard in store
    assert _assigned_key(assign_batch_name(_RUN, 1)) not in store   # not authoritative
    rows = pq.read_table(io.BytesIO(store[shard])).to_pylist()
    assert {r["object_key"] for r in rows} == {_MAT_KEY, _PACK_KEPT, _PACK_ORPHAN}
    assert _AUTH_ONLY_KEY not in {r["object_key"] for r in rows}
    # the real sequence was read
    assert any("nextval" in sql for sql, _ in conn.sql)


def test_authoritative_assign_does_not_see_a_probe_only_compare_run(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    auth_store, _ = _slice2_fixture_store(tmp_path)
    store = _to_probe_store(auth_store)          # ONLY *_probe Stage-5 outputs
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn())

    with pytest.raises(ReconcileError, match="no complete compare run"):
        mod.run_assign(mod.parse_args(["assign", "--apply"]))    # authoritative


def test_probe_assign_writes_only_under_assigned_probe_and_apply_cannot_see_it(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    auth_store, _ = _slice2_fixture_store(tmp_path)
    store = {**auth_store, **_to_probe_store(auth_store)}   # both prefixes present
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn(next_id=9_000_001))
    assert mod.run_assign(mod.parse_args(["assign", "--probe", "--apply"])) == 0

    probe_keys = [k for k in store if k.startswith("recovery/plan145/assigned_probe/")]
    assert _probe_assigned_key(assign_batch_name(_RUN, 1)) in probe_keys
    assert f"recovery/plan145/assigned_probe/{_RUN}-assign_report.json" in probe_keys
    # nothing landed in the authoritative prefix
    assert not any(k.startswith("recovery/plan145/assigned/") for k in store)

    # an authoritative apply for the same run finds no shards -- it only ever
    # lists assigned/, never assigned_probe/.
    conn = _FakeWriteConn()
    _patch_slice2_io(monkeypatch, store, conn)
    with pytest.raises(ReconcileError, match="run `assign --apply` first"):
        mod.run_apply(mod.parse_args(["apply", "--apply"]))
    assert conn.sql == []


def _seed_probe_assignment(tmp_path, monkeypatch):
    """A probe compare fixture with its assignment shards already written."""
    import scripts.reconcile_april_detail as mod

    auth_store, ids = _slice2_fixture_store(tmp_path)
    store = _to_probe_store(auth_store)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn(next_id=9_000_001))
    assert mod.run_assign(mod.parse_args(["assign", "--probe", "--apply"])) == 0
    return store, ids


def test_apply_probe_apply_issues_every_statement_then_rolls_back(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, monkeypatch)
    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)

    batch = assign_batch_name(_RUN, 1)
    assert mod.run_apply(mod.parse_args(
        ["apply", "--probe", "--apply", "--batch", batch])) == 0

    # The exact authoritative statement sequence, then ROLLBACK instead of COMMIT.
    assert [kind for kind, _ in conn.ops] == [
        "execute",          # check_batch_receipt SELECT
        "execute_values",   # staging.silver_observations
        "execute_values",   # staging.price_observation_events
        "execute_values",   # staging.artifacts_queue_events
        "execute",          # the receipt INSERT
        "rollback",
    ]
    assert conn.commits == 0
    ev = [sql for kind, sql in conn.ops if kind == "execute_values"]
    assert "silver_observations" in ev[0]
    assert "price_observation_events" in ev[1]
    assert "artifacts_queue_events" in ev[2]
    receipt_execs = [sql for kind, sql in conn.ops
                     if kind == "execute" and "plan145_recovery_batch_receipts" in sql]
    assert len(receipt_execs) == 2 and "INSERT" in receipt_execs[1]


def test_probe_apply_reports_the_would_be_write_set_like_a_dry_run(
        tmp_path, monkeypatch, capsys):
    import scripts.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, monkeypatch)
    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)

    mod.run_apply(mod.parse_args(
        ["apply", "--probe", "--apply", "--batch", assign_batch_name(_RUN, 1)]))
    out = capsys.readouterr().out
    assert "ROLLED BACK" in out
    assert re.search(r"^silver rows +4$", out, re.M)     # 3 detail + 1 carousel
    assert re.search(r"^price events +3$", out, re.M)    # detail rows only


def test_a_constraint_violation_in_probe_apply_is_not_swallowed_by_the_rollback(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, monkeypatch)
    conn = _FakeWriteConn(fail_on="staging.price_observation_events")
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)

    with pytest.raises(RuntimeError, match="injected failure"):
        mod.run_apply(mod.parse_args(
            ["apply", "--probe", "--apply", "--batch", assign_batch_name(_RUN, 1)]))
    assert conn.commits == 0
    assert conn.rollbacks >= 1          # the exception path rolled back and re-raised


def test_probe_apply_refuses_maintainer_approval_and_ignores_the_canary_budget(
        tmp_path, monkeypatch):
    import scripts.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, monkeypatch)

    # 1. --probe + --maintainer-approval is refused outright: a probe never
    #    commits, so there is nothing to approve.
    conn = _FakeWriteConn()
    _patch_slice2_io(monkeypatch, store, conn)
    with pytest.raises(ReconcileError, match="never commits"):
        mod.run_apply(mod.parse_args(
            ["apply", "--probe", "--apply", "--maintainer-approval", "someone"]))
    assert conn.sql == []

    # 2. The canary row budget caps a commit; a probe writes nothing durable, so
    #    a budget of one row does not stop it (Non-negotiable 4).
    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)
    assert mod.run_apply(mod.parse_args(
        ["apply", "--probe", "--apply", "--max-unapproved-rows", "1"])) == 0
    assert conn.commits == 0 and conn.rollbacks >= 1


def test_apply_probe_dry_run_reads_probe_prefixes_and_issues_no_statement(
        tmp_path, monkeypatch, capsys):
    import scripts.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, monkeypatch)
    conn = _FakeWriteConn()
    _patch_slice2_io(monkeypatch, store, conn)

    assert mod.run_apply(mod.parse_args(["apply", "--probe"])) == 0
    assert conn.sql == []
    assert "PROBE DRY RUN" in capsys.readouterr().out


def test_authoritative_slice2_paths_are_unchanged_by_probe(tmp_path, monkeypatch):
    # A bare authoritative assign+apply still lands in the authoritative
    # prefixes and commits, with no probe artefact anywhere.
    import scripts.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(monkeypatch, store, _FakeWriteConn(next_id=9_000_001))
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _record_execute_values(monkeypatch, conn)
    _patch_slice2_io(monkeypatch, store, conn)
    assert mod.run_apply(mod.parse_args(["apply", "--apply"])) == 0
    assert conn.commits == 1 and conn.rollbacks == 0
    assert _assigned_key(assign_batch_name(_RUN, 1)) in store
    assert not any("_probe/" in k for k in store)
