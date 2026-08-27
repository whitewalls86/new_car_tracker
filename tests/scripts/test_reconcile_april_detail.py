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
