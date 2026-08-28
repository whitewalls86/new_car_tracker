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
