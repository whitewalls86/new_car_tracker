"""Unit tests for scripts/oneoff/reconcile_april_detail.py (Plan 145, Stage 1).

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

from scripts.oneoff.reconcile_april_detail import (
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
    rows = (
        (tmp_path / "sorted" / "observations_distinct.csv")
        .read_text(encoding="utf-8")
        .splitlines()
    )
    assert rows[0] == ",".join(OBSERVATION_FIELDS)
    assert rows[1].startswith(LISTING_A)
    assert rows[2].startswith(LISTING_B)


def test_the_occurrence_manifest_carries_the_legacy_locator(tmp_path):
    _write(tmp_path, "locator")
    header = (
        (tmp_path / "locator" / "occurrences_http200.csv")
        .read_text(encoding="utf-8")
        .splitlines()[0]
    )
    assert header == ",".join(OCCURRENCE_FIELDS)
    for column in ("legacy_object_key", "row_group", "row_offset",
                   "stored_sha256", "recomputed_sha256"):
        assert column in header


def test_the_report_separates_the_two_distinct_hash_counts(tmp_path):
    report = _write(tmp_path, "report")
    saved = json.loads((tmp_path / "report" / "stage1_report.json").read_text(encoding="utf-8"))
    # Stage 2's pack join is content-based, so it needs the recomputed count;
    # the stored count is what the old system believed. Reporting one number
    # for "the SHA" would hide the difference these tests exist to expose.
    assert "distinct_recomputed_sha256" in saved["identity"]
    assert "distinct_stored_sha256" in saved["identity"]
    assert saved["stage"] == "plan_145_stage_1_census"
    assert report["fingerprints"]["stage1_report.json"]


def test_the_report_cross_tabs_empty_against_status(tmp_path):
    _write(tmp_path, "crosstab")
    saved = json.loads((tmp_path / "crosstab" / "stage1_report.json").read_text(encoding="utf-8"))
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

    from scripts.oneoff.reconcile_april_detail import iter_rows

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

    from scripts.oneoff.reconcile_april_detail import iter_rows

    fixture = tmp_path / "truncated.parquet"
    pq.write_table(pa.table({"artifact_id": [1]}), fixture)

    objects = [{"legacy_object_key": "truncated.parquet", "size_bytes": 1}]
    with pytest.raises(ReconcileError, match="missing expected columns"):
        list(iter_rows("bronze", objects, progress_every=0,
                       opener=lambda key: fixture.open("rb")))


# -- I: materialize -- deterministic keys and disposition rules -------------

from scripts.oneoff.reconcile_april_detail import (  # noqa: E402
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

def test_dry_run_writes_nothing(mocker):
    import shared.minio as minio
    calls = []
    mocker.patch.object(minio, "object_exists", lambda k: False)
    mocker.patch.object(minio, "write_html", lambda k, c: calls.append(k))
    rec = plan_row(_occurrence(html=b"<html>car</html>"))
    out = materialize_row(rec, b"<html>car</html>", apply=False)
    assert calls == []
    assert out["disposition"] == "written"


def test_an_existing_object_is_not_rewritten(mocker):
    import shared.minio as minio
    calls = []
    mocker.patch.object(minio, "object_exists", lambda k: True)
    mocker.patch.object(minio, "write_html", lambda k, c: calls.append(k))
    rec = plan_row(_occurrence(html=b"<html>car</html>"))
    out = materialize_row(rec, b"<html>car</html>", apply=True)
    assert calls == []
    assert out["disposition"] == "exists"


def test_apply_writes_and_verifies_the_read_back(mocker):
    import shared.minio as minio
    html = b"<html>car</html>"
    written = {}
    mocker.patch.object(minio, "object_exists", lambda k: False)
    mocker.patch.object(minio, "write_html", lambda k, c: written.update({k: c}))
    mocker.patch.object(minio, "read_html", lambda p: html)
    mocker.patch.object(minio, "object_size", lambda p: 1234)
    rec = plan_row(_occurrence(html=html))
    out = materialize_row(rec, html, apply=True)
    assert list(written) == [rec["object_key"]]
    assert out["compressed_len"] == 1234


def test_a_read_back_that_does_not_match_stops_the_run(mocker):
    # Verification reads through the production path rather than comparing the
    # in-memory bytes to themselves, so a corrupted store is actually caught.
    import shared.minio as minio
    mocker.patch.object(minio, "object_exists", lambda k: False)
    mocker.patch.object(minio, "write_html", lambda k, c: None)
    mocker.patch.object(minio, "read_html", lambda p: b"<html>corrupted</html>")
    mocker.patch.object(minio, "object_size", lambda p: 1)
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

from scripts.oneoff.reconcile_april_detail import (  # noqa: E402
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


def _patch_dedupe(mocker, events, rows, sidecar_hashes):
    """Wire run_dedupe onto in-memory fakes, recording write/delete order."""
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.minio as minio

    mocker.patch.object(mod, "_s3_client", lambda: object())
    mocker.patch.object(mod, "_list_keys", lambda c, b, prefix, suffix: (
        [SIDECAR_KEY] if suffix == ".idx.parquet"
        else [f"{MATERIALIZE_PREFIX}/part-0.parquet"]
    ))
    mocker.patch.object(mod, "load_sidecar_hashes",
                        lambda c, b, keys: dict(sidecar_hashes))
    mocker.patch.object(mod, "_read_parquet_rows",
                        lambda c, b, k, columns=None: list(rows))
    mocker.patch.object(mod, "_write_parquet_shard",
                        lambda key, schema, records: events.append(
                            ("write", key, len(records))))
    mocker.patch.object(minio, "object_exists", lambda k: False)

    def fake_delete(client, bucket, records, *, apply, batch_size, verified_hashes):
        events.append(("delete", [r["object_key"] for r in records]))
        return [{"object_key": r["object_key"], "raw_sha256": r["raw_sha256"],
                 "result": "deleted"} for r in records]

    mocker.patch.object(mod, "delete_objects_in_batches", fake_delete)
    return mod


def test_run_dedupe_writes_the_deletion_manifest_before_any_delete(mocker):
    events: list = []
    mod = _patch_dedupe(
        mocker, events,
        rows=[_mrow("k1", SHA_IN_PACK)],
        sidecar_hashes={SHA_IN_PACK: (SIDECAR_KEY, "src")},
    )
    rc = mod.run_dedupe(mod.parse_args(
        ["dedupe", "--apply", "--expect-rate", "1.0", "--rate-tolerance", "1.0"]))
    assert rc == 0
    assert [e[0] for e in events] == ["write", "delete", "write"]
    assert events[0][1] == "recovery/plan145/dedupe/part-0.parquet"
    assert events[2][1] == "recovery/plan145/dedupe/receipts/part-0.parquet"


def test_run_dedupe_stops_before_deleting_when_the_rate_is_off_band(mocker):
    events: list = []
    mod = _patch_dedupe(
        mocker, events,
        rows=[_mrow("k1", SHA_IN_PACK)],  # 1 of 1 candidate -> 100%, far off 45.6%
        sidecar_hashes={SHA_IN_PACK: (SIDECAR_KEY, "src")},
    )
    with pytest.raises(ReconcileError, match="outside the expected"):
        mod.run_dedupe(mod.parse_args(["dedupe", "--apply"]))
    # the deletion manifest is still written -- it is the reviewer's evidence --
    # but nothing is deleted.
    assert [e[0] for e in events] == ["write"]


def test_run_dedupe_allow_rate_drift_reports_instead_of_stopping(mocker):
    events: list = []
    mod = _patch_dedupe(
        mocker, events,
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
from scripts.oneoff.reconcile_april_detail import (  # noqa: E402
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


def test_walking_members_in_frame_order_decompresses_each_frame_once(mocker):
    _members, pack = _small_pack()
    assert pack.frame_count >= 2

    calls: list = []
    real = _compression.decompress_frame
    mocker.patch.object(
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


def test_unpack_writes_under_the_original_source_key_not_a_content_key(mocker):
    import shared.minio as minio
    written: dict = {}
    mocker.patch.object(minio, "object_exists", lambda k: False)
    mocker.patch.object(minio, "write_html", lambda k, c: written.__setitem__(k, c))

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


def test_a_member_that_does_not_match_its_sidecar_hash_stops_the_run(mocker):
    import shared.minio as minio
    mocker.patch.object(minio, "object_exists", lambda k: False)
    mocker.patch.object(minio, "write_html",
                        lambda k, c: pytest.fail("must not write on a mismatch"))

    entry = PackIndexEntry(DETAIL_KEY.format("x"), 0, 0, 4, "00" * 32)

    class FakeReader:
        def read_member(self, e):
            return b"real"

    with pytest.raises(ReconcileError, match="the store moved"):
        unpack_member(FakeReader(), entry, "pack", apply=True)


def test_an_existing_key_is_skipped_rather_than_re_read_or_rewritten(mocker):
    import shared.minio as minio
    calls: list = []
    mocker.patch.object(minio, "object_exists", lambda k: True)
    mocker.patch.object(minio, "write_html", lambda k, c: calls.append(k))

    entry = PackIndexEntry(DETAIL_KEY.format("x"), 2, 8, 4, "00" * 32)

    class FakeReader:
        def read_member(self, e):
            raise AssertionError("an existing key must not be read from the pack")

    rec = unpack_member(FakeReader(), entry, "pack", apply=True)
    assert rec["disposition"] == "exists"
    assert calls == []


def test_unpack_dry_run_verifies_but_writes_nothing(mocker):
    import shared.minio as minio
    calls: list = []
    mocker.patch.object(minio, "object_exists", lambda k: False)
    mocker.patch.object(minio, "write_html", lambda k, c: calls.append(k))

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


def test_run_unpack_writes_every_member_under_its_original_key(mocker):
    import scripts.oneoff.reconcile_april_detail as mod
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

    mocker.patch.object(mod, "_s3_client", lambda: FakeClient())
    mocker.patch.object(minio, "object_exists", lambda k: k in written)
    mocker.patch.object(minio, "write_html", lambda k, c: written.__setitem__(k, c))
    mocker.patch.object(mod, "_write_parquet_shard",
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
    from scripts.oneoff.reconcile_april_detail import parse_one_input

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
    from scripts.oneoff.reconcile_april_detail import build_parse_units

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
    from scripts.oneoff.reconcile_april_detail import build_parse_units

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
    from scripts.oneoff.reconcile_april_detail import resolve_manifest_identity

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
    from scripts.oneoff.reconcile_april_detail import parse_one_input

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
    from scripts.oneoff.reconcile_april_detail import build_parse_units, parse_one_input

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
    from scripts.oneoff.reconcile_april_detail import build_observation_rows

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
    from scripts.oneoff.reconcile_april_detail import build_observation_rows

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
    from scripts.oneoff.reconcile_april_detail import parse_one_input

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
    from scripts.oneoff.reconcile_april_detail import parse_one_input

    _, record = _parse_record(body=body)
    rows, audit = parse_one_input(
        record, _identity(None, None, "none", "none"),
        reader=lambda _key: body,
    )
    assert rows == []
    assert audit["outcome"] == outcome


def test_parse_hash_disagreement_stops_the_run():
    from scripts.oneoff.reconcile_april_detail import parse_one_input

    _, record = _parse_record()
    with pytest.raises(ReconcileError, match="store moved"):
        parse_one_input(record, _identity(), reader=lambda _key: b"changed")


def test_parse_exception_is_recorded_as_failed():
    from scripts.oneoff.reconcile_april_detail import parse_one_input

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

    from scripts.oneoff.reconcile_april_detail import (
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
    from scripts.oneoff.reconcile_april_detail import check_parse_apply_gate

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

from scripts.oneoff.reconcile_april_detail import (  # noqa: E402
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
    from scripts.oneoff.reconcile_april_detail import _parsed_rows_schema

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

    from scripts.oneoff.reconcile_april_detail import _parsed_rows_schema

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
        # A real price but no capture time: exercises the no_capture_time path
        # without also tripping the blocked_excluded signature (active + price/
        # vin/make all NULL), which quarantines the object before classifying.
        _prow(l3, None, fetched_at_source="none", object_key="html/a3.zst",
              content_sha256="s3", price=300),
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


def _patch_compare_io(mocker, store, vin_rows, *, rows=5):
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.db as db
    import shared.minio as minio

    mocker.patch.object(mod, "_s3_client", lambda: _FakeS3Store(store))
    mocker.patch.object(minio, "object_exists", lambda k: k in store)
    mocker.patch.object(
        minio, "write_bytes",
        lambda k, data, content_type=None: store.__setitem__(k, bytes(data)),
    )
    mocker.patch.object(minio, "read_json", lambda _path: {
        "completed_units": 1204, "planned_units": 1204,
        "totals": {"inputs": EXPECTED_FLATTENED_INPUTS, "rows": rows},
    })
    mocker.patch.object(db, "get_conn", lambda: _FakeConn(vin_rows))


def test_run_compare_apply_partitions_and_freezes_then_reruns_as_a_noop(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, (l1, _l2, _l3, _l4) = _compare_fixture_store(tmp_path)
    _patch_compare_io(mocker, store, [(l1, "VIN-L1")])

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
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, (l1, *_rest) = _compare_fixture_store(tmp_path)
    conn = _FakeConn([(l1, "VIN-L1")])
    _patch_compare_io(mocker, store, [(l1, "VIN-L1")])
    import shared.db as db
    mocker.patch.object(db, "get_conn", lambda: conn)

    keys_before = set(store)
    rc = mod.run_compare(mod.parse_args(["compare"]))
    assert rc == 0
    assert set(store) == keys_before          # nothing written
    assert conn.rolled_back is False           # get_conn never called


def test_run_compare_apply_refuses_a_silver_shape_that_is_not_the_frozen_nine(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, (l1, *_rest) = _compare_fixture_store(tmp_path)
    for key in [k for k in store if "/source=carousel/" in k or "/source=listings_page/" in k]:
        del store[key]                       # leaves 3 silver objects, not 9
    _patch_compare_io(mocker, store, [(l1, "VIN-L1")])

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
        tmp_path, mocker):
    import io as _io

    import pyarrow.parquet as _pq

    import scripts.oneoff.reconcile_april_detail as mod

    store, (l1, *_rest) = _compare_fixture_store(tmp_path)
    # a tier-2 capture: a real fetched_at, but no listing_id resolved.
    store["recovery/plan145/parsed/rows/materialized-c.parquet"] = \
        _write_parsed_rows_fixture(
            tmp_path / "rows-c.parquet",
            [_prow(listing_id=None, fetched_at=_WHEN, fetched_at_source="queue_events",
                   object_key="html/c1.zst", content_sha256="sc1", price=300)],
        )
    _patch_compare_io(mocker, store, [(l1, "VIN-L1")], rows=6)

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
        tmp_path, capsys, mocker):
    # The probe run's whole job is to learn this cohort's size against the
    # completed Stage 4 units. The gate must not stop it: nothing to protect,
    # since a probe writes nothing that can advance slice 2.
    import scripts.oneoff.reconcile_april_detail as mod

    store, (l1, *_rest) = _compare_fixture_store(tmp_path)
    store["recovery/plan145/parsed/rows/materialized-c.parquet"] = \
        _write_parsed_rows_fixture(
            tmp_path / "rows-c.parquet",
            [_prow(listing_id=None, fetched_at=_WHEN, fetched_at_source="queue_events",
                   object_key="html/c1.zst", content_sha256="sc1", price=300)],
        )
    _patch_compare_io(mocker, store, [(l1, "VIN-L1")], rows=6)
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

from scripts.oneoff.reconcile_april_detail import (  # noqa: E402
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

    def fetchone(self):
        return self.owner.result[0] if self.owner.result else None

    def close(self):
        pass


class _FakeWriteConn:
    """A connection that answers nextval and receipt reads and counts commits."""

    #: Sentinel so a test can pass receipt_committed_at=None deliberately --
    #: the case where the database returns no durable commit time at all.
    _DEFAULT_COMMITTED_AT = object()

    def __init__(self, *, next_id=9_000_000, receipts=None, fail_on=None,
                 receipt_committed_at=_DEFAULT_COMMITTED_AT,
                 receipt_counts=None):
        self.sql = []
        self.result = []
        self.commits = 0
        self.rollbacks = 0
        self.closed = False
        self._next_id = next_id
        self.receipts = receipts or {}
        self.fail_on = fail_on
        # V047 defaults committed_at to now() inside the writing transaction;
        # the writer reads it back with RETURNING rather than stamping its own
        # clock, so the fake has to answer it.
        self.receipt_committed_at = (
            _dt(2026, 8, 29, 7, 0, 0, tzinfo=_tz.utc)
            if receipt_committed_at is self._DEFAULT_COMMITTED_AT
            else receipt_committed_at)
        self.receipt_counts = receipt_counts or (0, 0, 0, 0)
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
        elif "RETURNING committed_at" in sql:
            self.result = [(self.receipt_committed_at,)]
        elif "SELECT committed_at" in sql:
            batch, digest = params
            self.result = ([(self.receipt_committed_at, *self.receipt_counts)]
                           if digest in self.receipts.get(batch, []) else [])
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

    from scripts.oneoff.reconcile_april_detail import _compared_schema

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

    from scripts.oneoff.reconcile_april_detail import _parsed_inputs_schema

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
    # a post-block-filter compare report (carries the blocked_excluded section)
    store[f"recovery/plan145/compared/{_RUN}/compare_report.json"] = json.dumps(
        {"blocked_excluded": {"rows": 0, "objects": 0}}).encode()
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


def _patch_slice2_io(mocker, store, conn=None):
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.db as db
    import shared.minio as minio

    mocker.patch.object(mod, "_s3_client", lambda: _FakeS3Store(store))
    mocker.patch.object(minio, "object_exists", lambda k: k.split("bronze/")[-1] in store
                        or k in store)
    mocker.patch.object(
        minio, "write_bytes",
        lambda k, data, content_type=None: store.__setitem__(k, bytes(data)),
    )
    def _read_json(path):
        # shared.minio.read_json returns None for a missing object -- model that
        # rather than KeyError, so fail-closed gates keyed on it are testable.
        key = path.split("bronze/")[-1]
        return json.loads(store[key].decode()) if key in store else None

    mocker.patch.object(minio, "read_json", _read_json)
    mocker.patch.object(db, "get_conn", lambda: conn)
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


def test_a_committed_batch_writes_zero_rows_on_retry(mocker):
    conn = _FakeWriteConn(receipts={"b1": ["a" * 64]})
    _record_execute_values(mocker, conn)
    out = write_import_batch(conn, "b1", "a" * 64,
                             [{"listing_id": LISTING_A}], [{"listing_id": LISTING_A}],
                             [{"artifact_id": 1}])
    assert out["skipped"] is True
    assert out["silver"] == 0 and out["price_events"] == 0
    assert conn.executed_values == []            # nothing was inserted
    assert conn.commits == 0 and conn.rollbacks == 1


def _record_execute_values(mocker, conn):
    import psycopg2.extras

    def _fake(cur, sql, rows, template=None, page_size=100):
        if conn.fail_on and conn.fail_on in sql:
            raise RuntimeError("injected failure")
        conn.executed_values.append((sql, list(rows)))
        conn.ops.append(("execute_values", sql))

    mocker.patch.object(psycopg2.extras, "execute_values", _fake)


def test_one_batch_is_one_transaction_with_the_receipt_inside_it(mocker):
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    out = write_import_batch(
        conn, "b1", "a" * 64,
        [build_recovery_silver_row(_ti_row(LISTING_A, _MAT_KEY), 7, {})],
        [build_recovery_price_event(
            build_recovery_silver_row(_ti_row(LISTING_A, _MAT_KEY), 7, {}))],
        [build_recovery_queue_event(
            {"object_key": _MAT_KEY, "artifact_id": 7, "listing_id": LISTING_A,
             "fetched_at": _WHEN}, "b1", "bronze")],
    )
    # the receipt row rides out with the outcome: committed_at is read back
    # with RETURNING from the transaction that set it, never stamped by the
    # caller's clock
    assert out.pop("receipt_row") == {
        "committed_at": _dt(2026, 8, 29, 7, 0, 0, tzinfo=_tz.utc),
        "artifact_count": 1, "silver_count": 1, "price_event_count": 1,
        "queue_event_count": 1,
    }
    assert out == {"batch_name": "b1", "skipped": False, "silver": 1,
                   "price_events": 1, "queue_events": 1, "artifacts": 1}
    assert conn.commits == 1 and conn.rollbacks == 0
    receipt_sql = [s for s, _ in conn.sql if "plan145_recovery_batch_receipts" in s
                   and "INSERT" in s]
    assert len(receipt_sql) == 1
    assert [sql for sql, _ in conn.executed_values] != []


def test_a_failure_inside_the_batch_rolls_back_and_escapes(mocker):
    # write_silver_observations_postgres would have logged a warning and
    # returned 0 here; this path must not.
    conn = _FakeWriteConn(fail_on="plan145_recovery_batch_receipts")
    _record_execute_values(mocker, conn)
    with pytest.raises(RuntimeError, match="injected failure"):
        write_import_batch(conn, "b1", "a" * 64, [], [], [])
    assert conn.commits == 0 and conn.rollbacks == 1


def test_no_write_statement_names_the_protected_tables(mocker):
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
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

def test_run_assign_dry_run_plans_without_touching_the_sequence(tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    before = set(store)

    assert mod.run_assign(mod.parse_args(["assign"])) == 0
    assert set(store) == before                  # no shard, no report
    assert conn.sql == []                        # nextval never issued
    out = capsys.readouterr().out
    assert "DRY RUN" in out and "preserved_queue_event" in out


def test_run_assign_writes_one_identity_per_object_shared_by_all_its_rows(
        tmp_path, mocker):
    import pyarrow.parquet as pq

    import scripts.oneoff.reconcile_april_detail as mod

    store, (l1, _l2, l3, l4) = _slice2_fixture_store(tmp_path)
    conn = _FakeWriteConn(next_id=9_000_001)
    _patch_slice2_io(mocker, store, conn)

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


def test_a_rerun_after_a_crash_reuses_the_recorded_ids(tmp_path, mocker):
    import pyarrow.parquet as pq

    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    first = _FakeWriteConn(next_id=9_000_001)
    _patch_slice2_io(mocker, store, first)
    mod.run_assign(mod.parse_args(["assign", "--apply"]))
    key = _assigned_key(assign_batch_name(_RUN, 1))
    original = pq.read_table(io.BytesIO(store[key])).to_pylist()

    # A second run finds the shard present: it must not burn new sequence
    # values or rewrite the recorded identities.
    second = _FakeWriteConn(next_id=5_555_555)
    _patch_slice2_io(mocker, store, second)
    mod.run_assign(mod.parse_args(["assign", "--apply"]))
    assert second.sql == []
    assert pq.read_table(io.BytesIO(store[key])).to_pylist() == original


def test_run_assign_never_reads_legacy_artifact_id(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    requested = []
    real = mod._read_parquet_rows

    def _spy(client, bucket, key, *, columns=None):
        requested.append(columns)
        return real(client, bucket, key, columns=columns)

    mocker.patch.object(mod, "_read_parquet_rows", _spy)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign"]))

    named = {c for cols in requested if cols for c in cols}
    assert "legacy_artifact_id" not in named
    assert "artifact_id" in named                # the queue-event one, only


def test_a_null_listing_id_in_to_import_stops_assign_and_reports_the_cohort(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-c.parquet"] = \
        _write_compared_shard(tmp_path / "ti-c.parquet", [
            _ti_row(None, "html/2026/04/pack/orig3.html.zst"),
        ])
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    before = set(store)

    with pytest.raises(ImportSetInvalid, match="null_listing_id"):
        mod.run_assign(mod.parse_args(["assign", "--apply"]))
    assert set(store) == before                  # a stop, before any shard
    assert "null_listing_id" in capsys.readouterr().out


def test_a_non_uuid_listing_id_stops_assign_before_any_write(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-c.parquet"] = \
        _write_compared_shard(tmp_path / "ti-c.parquet", [
            _ti_row("12345", "html/2026/04/pack/orig3.html.zst"),
        ])
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    before = set(store)
    with pytest.raises(ImportSetInvalid, match="non_uuid_listing_id"):
        mod.run_assign(mod.parse_args(["assign", "--apply"]))
    assert set(store) == before


def test_reassigning_a_run_under_different_caps_is_refused(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))
    with pytest.raises(ReconcileError, match="already assigned under caps"):
        mod.run_assign(mod.parse_args(
            ["assign", "--apply", "--max-artifacts", "1"],
        ))


def test_run_apply_dry_run_announces_the_blast_radius_and_writes_nothing(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_apply(mod.parse_args(["apply"])) == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "staging.silver_observations" in out
    assert "ops.artifacts_queue" in out          # named as never touched
    assert re.search(r"^artifacts +3$", out, re.M)      # 3 source objects
    assert conn.sql == []                        # no statement at all


def test_run_apply_writes_four_things_per_batch_in_one_transaction(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, (l1, l2, l3, l4) = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
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
    from scripts.oneoff.reconcile_april_detail import (
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
        tmp_path, mocker):
    # The gate is measured in rows, not batches: counting batches would let one
    # default-cap batch -- 50,000 silver rows -- through unapproved.
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="canary budget"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--max-unapproved-rows", "3"],
        ))
    assert conn.sql == []                    # refused before any statement


def test_a_named_approval_lets_an_oversized_write_set_through(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_apply(mod.parse_args(
        ["apply", "--apply", "--max-unapproved-rows", "3",
         "--maintainer-approval", "a-maintainer"],
    )) == 0
    assert conn.commits == 1


def test_several_canary_sized_batches_need_no_approval(tmp_path, mocker):
    # Three one-artifact batches, four silver rows in total: a batch count
    # would have refused this, a row budget correctly permits it.
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply", "--max-artifacts", "1"]))

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_apply(mod.parse_args(["apply", "--apply"])) == 0
    assert conn.commits == 3                 # one transaction per batch


def test_the_canary_budget_leaves_room_for_the_plans_500_observation_canary():
    from scripts.oneoff.reconcile_april_detail import (
        CANARY_ROW_BUDGET,
        MAX_BATCH_SILVER_ROWS,
    )

    assert 500 < CANARY_ROW_BUDGET < MAX_BATCH_SILVER_ROWS


def test_a_carousel_row_with_no_listing_id_stops_apply_before_any_write(
        tmp_path, mocker):
    # assign would have caught it, but apply re-reads the shards independently
    # and is the last check before the INSERT.
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    rows = _read_to_import_fixture(store, "materialized-a")
    rows[1]["listing_id"] = None             # the carousel row
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-a.parquet"] = \
        _write_compared_shard(tmp_path / "ti-a-null.parquet", rows)

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
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
    from scripts.oneoff.reconcile_april_detail import ViolationLog

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
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-c.parquet"] = \
        _write_compared_shard(tmp_path / "ti-c.parquet", [
            _ti_row(None, f"html/2026/04/pack/o{i}.html.zst") for i in range(50)
        ])
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    with pytest.raises(ImportSetInvalid) as exc:
        mod.run_assign(mod.parse_args(["assign", "--apply"]))
    # The count is the whole cohort; only the printed examples are capped.
    assert "'null_listing_id': 50" in str(exc.value)
    out = capsys.readouterr().out
    assert "null_listing_id" in out
    assert out.count("e.g.") <= 20


def test_run_apply_stops_when_the_compare_output_moved_under_the_assignment(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    # A row disappears from to_import after the identity was frozen.
    store[f"recovery/plan145/compared/{_RUN}/to_import/materialized-a.parquet"] = \
        _write_compared_shard(tmp_path / "ti-a2.parquet", [
            _ti_row("aaaaaaaa-1111-1111-1111-111111111111", _MAT_KEY),
        ])
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="row count their assignment recorded"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--batch", assign_batch_name(_RUN, 1)],
        ))
    assert conn.commits == 0


def test_assign_and_apply_both_default_to_a_dry_run():
    from scripts.oneoff.reconcile_april_detail import parse_args

    assert parse_args(["assign"]).apply is False
    assert parse_args(["apply"]).apply is False
    assert parse_args(["apply"]).maintainer_approval is None


def test_assign_issues_nextval_and_never_an_insert(tmp_path, mocker):
    # The assignment shard is written before any database insertion because
    # assign issues none: the only statement it may send is the sequence read.
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    conn = _FakeWriteConn(next_id=9_000_001)
    _patch_slice2_io(mocker, store, conn)
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    assert conn.sql, "the sequence should have been read"
    for sql, _params in conn.sql:
        assert "nextval" in sql
        assert "insert" not in sql.lower()
    assert _assigned_key(assign_batch_name(_RUN, 1)) in store


def test_apply_refuses_a_run_whose_identities_were_never_assigned(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
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
    from scripts.oneoff.reconcile_april_detail import parse_args

    assert parse_args(["assign"]).probe is False
    assert parse_args(["apply"]).probe is False


def test_probe_assign_reads_the_probe_run_and_ignores_a_same_named_authoritative_one(
        tmp_path, mocker):
    import pyarrow.parquet as pq

    import scripts.oneoff.reconcile_april_detail as mod

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
    _patch_slice2_io(mocker, store, conn)
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
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    auth_store, _ = _slice2_fixture_store(tmp_path)
    store = _to_probe_store(auth_store)          # ONLY *_probe Stage-5 outputs
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    with pytest.raises(ReconcileError, match="no complete compare run"):
        mod.run_assign(mod.parse_args(["assign", "--apply"]))    # authoritative


def test_probe_assign_with_no_probe_compare_says_to_run_one_not_to_finish_slice_1(
        tmp_path, mocker):
    # The mirror of the test above, and the coverage for roots.probe: when a
    # probe run finds nothing, the fix is a probe compare -- telling a
    # maintainer "slice 1 must finish" would send them to wait on the very
    # thing this mode exists to route around.
    import scripts.oneoff.reconcile_april_detail as mod

    auth_store, _ = _slice2_fixture_store(tmp_path)   # authoritative outputs only
    _patch_slice2_io(mocker, auth_store, _FakeWriteConn())

    with pytest.raises(ReconcileError,
                       match=r"run `compare --probe --apply` first"):
        mod.run_assign(mod.parse_args(["assign", "--probe", "--apply"]))


def test_authoritative_assign_with_both_prefixes_present_reads_the_authoritative_run(
        tmp_path, mocker):
    # The true reverse of the "probe ignores authoritative" case: both runs
    # exist, and an authoritative assign must pick the authoritative one.
    import pyarrow.parquet as pq

    import scripts.oneoff.reconcile_april_detail as mod

    auth_store, _ = _slice2_fixture_store(tmp_path)
    store = {**auth_store, **_to_probe_store(auth_store)}
    # an object only the PROBE run's to_import family carries
    store[f"recovery/plan145/compared_probe/{_RUN}/to_import/materialized-x.parquet"] = \
        _write_compared_shard(
            tmp_path / "ti-probe-x.parquet",
            [_ti_row("eeeeeeee-5555-5555-5555-555555555555", _AUTH_ONLY_KEY)],
        )
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    assert mod.run_assign(mod.parse_args(["assign", "--apply"])) == 0

    key = _assigned_key(assign_batch_name(_RUN, 1))
    rows = pq.read_table(io.BytesIO(store[key])).to_pylist()
    assert _AUTH_ONLY_KEY not in {r["object_key"] for r in rows}
    assert not any(k.startswith("recovery/plan145/assigned_probe/") for k in store)


def test_probe_assign_writes_only_under_assigned_probe_and_apply_cannot_see_it(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    auth_store, _ = _slice2_fixture_store(tmp_path)
    store = {**auth_store, **_to_probe_store(auth_store)}   # both prefixes present
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    assert mod.run_assign(mod.parse_args(["assign", "--probe", "--apply"])) == 0

    probe_keys = [k for k in store if k.startswith("recovery/plan145/assigned_probe/")]
    assert _probe_assigned_key(assign_batch_name(_RUN, 1)) in probe_keys
    report_key = f"recovery/plan145/assigned_probe/{_RUN}-assign_report.json"
    assert report_key in probe_keys
    # the report carries the durable record that this run was disposable
    assert json.loads(store[report_key])["probe"] is True
    # nothing landed in the authoritative prefix
    assert not any(k.startswith("recovery/plan145/assigned/") for k in store)

    # an authoritative apply for the same run finds no shards -- it only ever
    # lists assigned/, never assigned_probe/.
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="run `assign --apply` first"):
        mod.run_apply(mod.parse_args(["apply", "--apply"]))
    assert conn.sql == []


def _seed_probe_assignment(tmp_path, mocker):
    """A probe compare fixture with its assignment shards already written."""
    import scripts.oneoff.reconcile_april_detail as mod

    auth_store, ids = _slice2_fixture_store(tmp_path)
    store = _to_probe_store(auth_store)
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    assert mod.run_assign(mod.parse_args(["assign", "--probe", "--apply"])) == 0
    return store, ids


def test_apply_probe_apply_issues_every_statement_then_rolls_back(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

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
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    mod.run_apply(mod.parse_args(
        ["apply", "--probe", "--apply", "--batch", assign_batch_name(_RUN, 1)]))
    out = capsys.readouterr().out
    assert "ROLLED BACK" in out
    assert re.search(r"^silver rows +4$", out, re.M)     # 3 detail + 1 carousel
    assert re.search(r"^price events +3$", out, re.M)    # detail rows only


def test_a_constraint_violation_in_probe_apply_is_not_swallowed_by_the_rollback(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, mocker)
    conn = _FakeWriteConn(fail_on="staging.price_observation_events")
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    with pytest.raises(RuntimeError, match="injected failure"):
        mod.run_apply(mod.parse_args(
            ["apply", "--probe", "--apply", "--batch", assign_batch_name(_RUN, 1)]))
    assert conn.commits == 0
    assert conn.rollbacks >= 1          # the exception path rolled back and re-raised


def test_apply_probe_apply_refuses_a_bare_run_and_wants_an_explicit_batch(
        tmp_path, mocker):
    # The approval gate was the only bound on how much a bare apply --apply did;
    # a probe is exempt from approval, so it needs its own bound. --batch is it.
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, mocker)
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="no row budget"):
        mod.run_apply(mod.parse_args(["apply", "--probe", "--apply"]))
    assert conn.sql == []


def test_probe_apply_refuses_maintainer_approval_and_ignores_the_canary_budget(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, mocker)
    batch = assign_batch_name(_RUN, 1)

    # 1. --probe + --maintainer-approval is refused outright: a probe never
    #    commits, so there is nothing to approve.
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="never commits"):
        mod.run_apply(mod.parse_args(
            ["apply", "--probe", "--apply", "--batch", batch,
             "--maintainer-approval", "someone"]))
    assert conn.sql == []

    # 2. The canary row budget caps a commit; a probe writes nothing durable, so
    #    a budget of one row does not stop a named-batch probe (Non-negotiable 4).
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_apply(mod.parse_args(
        ["apply", "--probe", "--apply", "--batch", batch,
         "--max-unapproved-rows", "1"])) == 0
    assert conn.commits == 0 and conn.rollbacks >= 1


def test_apply_probe_dry_run_reads_probe_prefixes_and_issues_no_statement(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _seed_probe_assignment(tmp_path, mocker)
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)

    assert mod.run_apply(mod.parse_args(["apply", "--probe"])) == 0
    assert conn.sql == []
    out = capsys.readouterr().out
    assert "PROBE DRY RUN" in out
    assert "would write" in out
    # the dry run hands the maintainer the exact --batch string the
    # probe-apply refusal asks for.
    assert assign_batch_name(_RUN, 1) in out


def test_authoritative_slice2_paths_are_unchanged_by_probe(tmp_path, mocker):
    # A bare authoritative assign+apply still lands in the authoritative
    # prefixes and commits, with no probe artefact anywhere.
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_apply(mod.parse_args(["apply", "--apply"])) == 0
    assert conn.commits == 1 and conn.rollbacks == 0
    assert _assigned_key(assign_batch_name(_RUN, 1)) in store
    assert not any("_probe/" in k for k in store)


# -- P: control + canary-sample (Stage 5, slice 3, Phase A) --------------
#
# The parser control proves the recovery reproduces what production wrote, on
# pages production already parsed: it draws exact, same-source represented
# observations from slice 1's already_represented family and diffs every
# silver business field against the deployed silver row, ignoring exactly four
# things by name. The canary sampler picks the ~500-observation,
# artifact-whole, every-stratum selection Phase B's write canary commits.

_C3RUN = "cmp-slice3phasea01"


def _ar_row(listing_id, *, source="detail", nearest_distance_s=0.0,
            match_sources=None, reason="silver_candidate", **over):
    row = _prow(listing_id, _WHEN, source=source, **over)
    row["reason"] = reason
    row["match_count"] = 1
    row["nearest_distance_s"] = nearest_distance_s
    row["match_sources"] = list(match_sources if match_sources is not None
                                else [source])
    return row


def _write_ar_shard(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.oneoff.reconcile_april_detail import _compared_schema

    schema = _compared_schema("already_represented")
    pq.write_table(
        pa.Table.from_pylist(
            [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
        ),
        path, compression="zstd",
    )
    return path.read_bytes()


def _write_full_silver_fixture(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.oneoff.reconcile_april_detail import SILVER_FIELDS, _parsed_rows_schema

    base = _parsed_rows_schema()
    schema = pa.schema(
        [base.field(c) for c in SILVER_FIELDS if c != "source"]
        + [pa.field("artifact_id", pa.int64()),
           pa.field("written_at", pa.timestamp("us", tz="UTC"))]
    )
    pq.write_table(
        pa.Table.from_pylist(
            [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
        ),
        path, compression="zstd",
    )
    return path.read_bytes()


def _write_assigned_shard(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.oneoff.reconcile_april_detail import _assigned_schema

    schema = _assigned_schema()
    pq.write_table(
        pa.Table.from_pylist(
            [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
        ),
        path, compression="zstd",
    )
    return path.read_bytes()


def _patch_slice3_io(mocker, store):
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.minio as minio

    mocker.patch.object(mod, "_s3_client", lambda: _FakeS3Store(store))
    mocker.patch.object(minio, "object_exists", lambda k: k in store)
    mocker.patch.object(
        minio, "write_bytes",
        lambda k, data, content_type=None: store.__setitem__(k, bytes(data)),
    )


def _silver_key(source, month, name):
    return (f"silver_normalized/observations/source={source}/obs_year=2026/"
            f"obs_month={month}/part-{name}.parquet")


# -- P.1: the parser control ------------------------------------------------

def _control_store(tmp_path, *, l1_silver_price=25000):
    l1 = "11111111-1111-1111-1111-111111111111"   # exact, same-source (detail)
    l2 = "22222222-2222-2222-2222-222222222222"   # windowed, not exact
    l3 = "33333333-3333-3333-3333-333333333333"   # exact but cross-source
    l4 = "44444444-4444-4444-4444-444444444444"   # exact, same-source (carousel)

    store = {}
    store[f"recovery/plan145/compared/{_C3RUN}/already_represented/unit-a.parquet"] = \
        _write_ar_shard(tmp_path / "ar-a.parquet", [
            _ar_row(l1, source="detail", price=25000, make="Honda",
                    object_key="html/l1.zst"),
            _ar_row(l2, source="detail", nearest_distance_s=12.0,
                    object_key="html/l2.zst"),
            _ar_row(l3, source="detail", match_sources=["carousel"],
                    object_key="html/l3.zst"),
            _ar_row(l4, source="carousel", object_key="html/l4.zst"),
        ])
    # artifact_id and written_at carry recovery-vs-production values that must
    # be skipped by name, not by absence.
    store[_silver_key("detail", 4, "d")] = _write_full_silver_fixture(
        tmp_path / "sv-d.parquet", [
            {"listing_id": l1, "fetched_at": _WHEN, "source": "detail",
             "listing_state": "active", "price": l1_silver_price, "make": "Honda",
             "artifact_id": 4_902_111, "written_at": _WHEN},
        ])
    store[_silver_key("carousel", 4, "c")] = _write_full_silver_fixture(
        tmp_path / "sv-c.parquet", [
            {"listing_id": l4, "fetched_at": _WHEN, "source": "carousel",
             "listing_state": "active", "artifact_id": 4_902_222,
             "written_at": _WHEN},
        ])
    return store, (l1, l2, l3, l4)


def test_control_report_counts_only_the_two_exact_same_source_rows(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ids = _control_store(tmp_path)
    _patch_slice3_io(mocker, store)

    rc = mod.run_control(
        mod.parse_args(["control", "--run-id", _C3RUN, "--apply"]))
    assert rc == 0
    report = json.loads(
        store[f"recovery/plan145/control/{_C3RUN}-control_report.json"])
    assert report["sample"]["exact_same_source_candidates"] == 2
    assert report["sample"]["by_source"] == {"carousel": 1, "detail": 1}
    assert report["compared"] == 2
    assert report["clean"] is True
    assert report["field_disagreement_census"] == {}


def test_control_reports_a_single_differing_business_field_and_exits_nonzero(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ids = _control_store(tmp_path, l1_silver_price=25999)
    _patch_slice3_io(mocker, store)

    rc = mod.run_control(
        mod.parse_args(["control", "--run-id", _C3RUN, "--apply"]))
    assert rc == 1
    report = json.loads(
        store[f"recovery/plan145/control/{_C3RUN}-control_report.json"])
    assert report["field_disagreement_census"] == {"price": 1}
    assert report["clean"] is False
    assert report["findings_summary"]["field_disagreements"] == 1
    disagreement = next(f for f in report["findings"]
                        if f["kind"] == "field_disagreement")
    assert disagreement["field"] == "price"
    assert disagreement["parsed"] == 25000 and disagreement["silver"] == 25999


def test_control_ignores_carousel_vin_but_not_detail_vin():
    from scripts.oneoff.reconcile_april_detail import _control_field_disagreements

    carousel = _control_field_disagreements(
        {"source": "carousel", "vin": None}, {"source": "carousel", "vin": "V1"},
    )
    assert carousel == []                          # carousel_vin ignored by name

    detail = _control_field_disagreements(
        {"source": "detail", "vin": None}, {"source": "detail", "vin": "V1"},
    )
    assert [field for field, _p, _s in detail] == ["vin"]


def test_control_ignore_list_is_exactly_four_and_a_fifth_breaks_it(mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    assert len(mod.CONTROL_IGNORED_FIELDS) == 4
    mocker.patch.object(
        mod, "CONTROL_IGNORED_FIELDS", mod.CONTROL_IGNORED_FIELDS + ("year",),
    )
    with pytest.raises(AssertionError):
        mod._control_field_disagreements({"source": "detail"}, {"source": "detail"})


def test_control_renaming_an_ignore_token_makes_that_column_a_finding(
        tmp_path, mocker):
    # The token list is load-bearing: drop "artifact_id" from it (keeping the
    # count at four) and the deployed row's artifact_id starts showing up as a
    # disagreement, while "written_at" -- still named -- stays ignored.
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ids = _control_store(tmp_path)
    _patch_slice3_io(mocker, store)
    mocker.patch.object(mod, "CONTROL_IGNORED_FIELDS",
                        ("recovery_provenance", "written_at", "carousel_vin",
                         "not_artifact_id"))

    rc = mod.run_control(
        mod.parse_args(["control", "--run-id", _C3RUN, "--apply"]))
    assert rc == 1
    report = json.loads(
        store[f"recovery/plan145/control/{_C3RUN}-control_report.json"])
    census = report["field_disagreement_census"]
    assert census.get("artifact_id") == 2       # both sampled rows
    assert "written_at" not in census           # still ignored by name


def test_control_sample_size_below_one_is_refused(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ids = _control_store(tmp_path)
    _patch_slice3_io(mocker, store)

    with pytest.raises(ReconcileError, match="sample-size"):
        mod.run_control(
            mod.parse_args(["control", "--run-id", _C3RUN, "--sample-size", "0"]))


def test_control_dry_run_writes_nothing(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ids = _control_store(tmp_path)
    _patch_slice3_io(mocker, store)
    before = set(store)

    mod.run_control(mod.parse_args(["control", "--run-id", _C3RUN]))
    assert set(store) == before


def test_control_probe_reads_and_writes_only_the_probe_prefix(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ids = _control_store(tmp_path)
    store = {k.replace("recovery/plan145/compared/",
                       "recovery/plan145/compared_probe/"): v
             for k, v in store.items()}
    _patch_slice3_io(mocker, store)

    rc = mod.run_control(
        mod.parse_args(["control", "--probe", "--run-id", _C3RUN, "--apply"]))
    assert rc == 0
    assert f"recovery/plan145/control_probe/{_C3RUN}-control_report.json" in store
    assert not any(k.startswith("recovery/plan145/control/") for k in store)


# -- P.2: the canary stratified sampler -----------------------------------

_OA = "html/oa.zst"   # detail active + carousel active   (materialized/alloc)
_OB = "html/ob.zst"   # detail unlisted                   (unpacked/preserved)
_OC = "html/oc.zst"   # detail active                     (unpacked/alloc)
_OD = "html/od.zst"   # carousel active                   (materialized/preserved)


def _canary_store(tmp_path, *, drop_assignment_for=None, orphan_assignment=False):
    la = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    lac = "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
    lb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lc = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    ld = "dddddddd-0000-0000-0000-dddddddddddd"

    store = {}
    store[f"recovery/plan145/compared/{_C3RUN}/to_import/unit-a.parquet"] = \
        _write_compared_shard(tmp_path / "ti.parquet", [
            _ti_row(la, _OA, source="detail"),
            _ti_row(lac, _OA, source="carousel"),
            _ti_row(lb, _OB, source="detail", listing_state="unlisted"),
            _ti_row(lc, _OC, source="detail"),
            _ti_row(ld, _OD, source="carousel"),
        ])

    assigned = [
        {"batch_name": f"{_C3RUN}-b00001", "run_id": _C3RUN, "object_key": _OA,
         "artifact_id": 9000001, "id_source": "allocated_sequence",
         "listing_id": la, "fetched_at": _WHEN, "input_kind": "materialized",
         "source_unit": "unit-a", "silver_rows": 2, "detail_rows": 1,
         "assigned_at": _WHEN},
        {"batch_name": f"{_C3RUN}-b00001", "run_id": _C3RUN, "object_key": _OB,
         "artifact_id": 4902401, "id_source": "preserved_queue_event",
         "listing_id": lb, "fetched_at": _WHEN, "input_kind": "unpacked",
         "source_unit": "unit-a", "silver_rows": 1, "detail_rows": 1,
         "assigned_at": _WHEN},
        {"batch_name": f"{_C3RUN}-b00001", "run_id": _C3RUN, "object_key": _OC,
         "artifact_id": 9000002, "id_source": "allocated_sequence",
         "listing_id": lc, "fetched_at": _WHEN, "input_kind": "unpacked",
         "source_unit": "unit-a", "silver_rows": 1, "detail_rows": 1,
         "assigned_at": _WHEN},
        {"batch_name": f"{_C3RUN}-b00001", "run_id": _C3RUN, "object_key": _OD,
         "artifact_id": 4902402, "id_source": "preserved_queue_event",
         "listing_id": ld, "fetched_at": _WHEN, "input_kind": "materialized",
         "source_unit": "unit-a", "silver_rows": 1, "detail_rows": 0,
         "assigned_at": _WHEN},
    ]
    if drop_assignment_for:
        assigned = [a for a in assigned if a["object_key"] != drop_assignment_for]
    if orphan_assignment:
        # An assigned object with no to_import rows in the read -- i.e. a whole
        # dropped to_import shard.
        assigned.append(
            {"batch_name": f"{_C3RUN}-b00001", "run_id": _C3RUN,
             "object_key": "html/oe.zst", "artifact_id": 9000009,
             "id_source": "allocated_sequence",
             "listing_id": "eeeeeeee-eeee-eeee-eeee-eeeeeeeeeeee",
             "fetched_at": _WHEN, "input_kind": "materialized",
             "source_unit": "unit-b", "silver_rows": 1, "detail_rows": 1,
             "assigned_at": _WHEN})
    store[f"recovery/plan145/assigned/{_C3RUN}-b00001.parquet"] = \
        _write_assigned_shard(tmp_path / "asg.parquet", assigned)
    # The sampler builds the write set it freezes, so it reads the frozen VIN
    # snapshot: build_recovery_silver_row fills a carousel row's NULL vin from
    # it, and that value is part of what the manifest is a contract over.
    store[f"recovery/plan145/vin_snapshot/{_C3RUN}.parquet"] = _write_vin_shard(
        tmp_path / "canary-vin.parquet", [(lac, "VIN-SNAPSHOT-CAROUSEL")],
    )
    return store


def test_canary_sample_covers_every_stratum_and_keeps_artifacts_whole(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_store(tmp_path)
    _patch_slice3_io(mocker, store)

    rc = mod.run_canary_sample(
        mod.parse_args(["canary-sample", "--run-id", _C3RUN, "--apply"]))
    assert rc == 0
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_report.json"])
    strata = report["strata"]
    assert strata["every_stratum_covered"] is True
    assert set(strata["covered_by_sample"]) == set(strata["present_in_population"])
    assert len(strata["present_in_population"]) == 5
    assert report["no_artifact_split"] is True
    assert report["selection"] == {
        "artifacts": 4, "silver_rows": 5, "detail_rows": 3, "carousel_rows": 2,
    }


def test_canary_sample_manifest_holds_every_row_of_each_selected_object(
        tmp_path, mocker):
    import io as _io

    import pyarrow.parquet as _pq

    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_store(tmp_path)
    _patch_slice3_io(mocker, store)
    mod.run_canary_sample(
        mod.parse_args(["canary-sample", "--run-id", _C3RUN, "--apply"]))

    manifest = _pq.read_table(_io.BytesIO(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"]
    )).to_pylist()
    by_object = {row["object_key"]: row for row in manifest}
    assert by_object[_OA]["silver_rows"] == 2           # both OA rows, never split
    assert by_object[_OB]["silver_rows"] == 1
    assert sorted(by_object[_OA]["strata"]) == by_object[_OA]["strata"]
    assert all(isinstance(row["strata"], list) for row in manifest)


def test_canary_sample_stops_when_a_to_import_object_has_no_assignment(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_store(tmp_path, drop_assignment_for=_OD)
    _patch_slice3_io(mocker, store)

    with pytest.raises(ReconcileError, match="no assignment"):
        mod.run_canary_sample(
            mod.parse_args(["canary-sample", "--run-id", _C3RUN, "--apply"]))
    assert not any("/canary/" in k for k in store)


def test_canary_sample_stops_when_an_assigned_object_is_missing_from_the_read(
        tmp_path, mocker):
    # A whole dropped to_import shard: the object is gone from the read, so the
    # per-object count cross-check cannot see it -- only the assign-side check.
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_store(tmp_path, orphan_assignment=True)
    _patch_slice3_io(mocker, store)

    with pytest.raises(ReconcileError, match="dropped"):
        mod.run_canary_sample(
            mod.parse_args(["canary-sample", "--run-id", _C3RUN, "--apply"]))
    assert not any("/canary/" in k for k in store)


def test_canary_sample_stops_when_the_to_import_read_is_short_of_the_assignment(
        tmp_path, mocker):
    # OA's carousel row is missing from the to_import read; its assignment still
    # says silver_rows=2. Selecting OA would commit a half-artifact.
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_store(tmp_path)
    la = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    lb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lc = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    ld = "dddddddd-0000-0000-0000-dddddddddddd"
    store[f"recovery/plan145/compared/{_C3RUN}/to_import/unit-a.parquet"] = \
        _write_compared_shard(tmp_path / "ti-short.parquet", [
            _ti_row(la, _OA, source="detail"),
            _ti_row(lb, _OB, source="detail", listing_state="unlisted"),
            _ti_row(lc, _OC, source="detail"),
            _ti_row(ld, _OD, source="carousel"),
        ])
    _patch_slice3_io(mocker, store)

    with pytest.raises(ReconcileError, match="half-artifact"):
        mod.run_canary_sample(
            mod.parse_args(["canary-sample", "--run-id", _C3RUN, "--apply"]))
    assert not any("/canary/" in k for k in store)


def _canary_store_wide(tmp_path, *, n_common=38):
    """40 objects, 38 sharing one stratum and two carrying a rare one each --
    so covering all three strata from a small --target-rows budget is only
    possible via the stratification pass, not the top-up loop."""
    ti_rows, asg_rows = [], []
    for i in range(n_common):
        key = f"html/w{i:02d}.zst"
        lid = f"{i:08x}-0000-0000-0000-000000000000"
        ti_rows.append(_ti_row(lid, key, source="detail"))
        asg_rows.append(
            {"batch_name": f"{_C3RUN}-b00001", "run_id": _C3RUN, "object_key": key,
             "artifact_id": 9_000_000 + i, "id_source": "allocated_sequence",
             "listing_id": lid, "fetched_at": _WHEN, "input_kind": "materialized",
             "source_unit": "unit-a", "silver_rows": 1, "detail_rows": 1,
             "assigned_at": _WHEN})
    rare = [
        ("html/rare-unlisted.zst", "ffffffff-1111-0000-0000-000000000000",
         dict(source="detail", listing_state="unlisted"),
         "unpacked", "preserved_queue_event", 1),
        ("html/rare-carousel.zst", "ffffffff-2222-0000-0000-000000000000",
         dict(source="carousel"),
         "materialized", "preserved_queue_event", 0),
    ]
    for key, lid, ti_kw, input_kind, id_source, detail_rows in rare:
        ti_rows.append(_ti_row(lid, key, **ti_kw))
        asg_rows.append(
            {"batch_name": f"{_C3RUN}-b00001", "run_id": _C3RUN, "object_key": key,
             "artifact_id": 4_900_000 + len(asg_rows), "id_source": id_source,
             "listing_id": lid, "fetched_at": _WHEN, "input_kind": input_kind,
             "source_unit": "unit-a", "silver_rows": 1, "detail_rows": detail_rows,
             "assigned_at": _WHEN})
    store = {}
    store[f"recovery/plan145/compared/{_C3RUN}/to_import/unit-a.parquet"] = \
        _write_compared_shard(tmp_path / "ti-wide.parquet", ti_rows)
    store[f"recovery/plan145/assigned/{_C3RUN}-b00001.parquet"] = \
        _write_assigned_shard(tmp_path / "asg-wide.parquet", asg_rows)
    store[f"recovery/plan145/vin_snapshot/{_C3RUN}.parquet"] = _write_vin_shard(
        tmp_path / "vin-wide.parquet", [])
    return store


def test_canary_sample_stratification_pass_covers_the_rare_strata_on_a_tiny_budget(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_store_wide(tmp_path)
    _patch_slice3_io(mocker, store)

    rc = mod.run_canary_sample(mod.parse_args(
        ["canary-sample", "--run-id", _C3RUN, "--target-rows", "3", "--apply"]))
    assert rc == 0
    strata = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_report.json"])["strata"]
    assert len(strata["present_in_population"]) == 3
    assert strata["every_stratum_covered"] is True
    # 38 of 40 objects share one stratum, so hitting all three from a 3-row
    # budget means pass 1 targeted the two rare objects -- top-up alone would
    # have grabbed three near-certain common ones and tripped the coverage guard.
    assert set(strata["covered_by_sample"]) == set(strata["present_in_population"])


def test_canary_sample_dry_run_writes_nothing(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_store(tmp_path)
    _patch_slice3_io(mocker, store)
    before = set(store)

    rc = mod.run_canary_sample(
        mod.parse_args(["canary-sample", "--run-id", _C3RUN]))
    assert rc == 0
    assert set(store) == before


def test_canary_sample_probe_reads_and_writes_only_the_probe_prefix(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_store(tmp_path)
    store = {
        k.replace("recovery/plan145/compared/", "recovery/plan145/compared_probe/")
         .replace("recovery/plan145/assigned/", "recovery/plan145/assigned_probe/")
         .replace("recovery/plan145/vin_snapshot/",
                  "recovery/plan145/vin_snapshot_probe/"): v
        for k, v in store.items()
    }
    _patch_slice3_io(mocker, store)

    rc = mod.run_canary_sample(
        mod.parse_args(["canary-sample", "--probe", "--run-id", _C3RUN, "--apply"]))
    assert rc == 0
    assert f"recovery/plan145/canary_probe/{_C3RUN}-canary_sample.parquet" in store
    assert not any(k.startswith("recovery/plan145/canary/") for k in store)


# -- Q: blocked_excluded -- the block-page compare filter (Stage 5) -------
#
# Stage 4's block-page classifier only fires for objects whose identity did
# not resolve. A block page whose listing_id resolved from legacy_manifest or
# queue_events parses to an `active` detail row the parser drew no price, VIN
# or make from, and -- before this filter -- leaked into to_import or
# already_represented. `compare` now quarantines the whole object (detail row
# and any carousel rows) into a fourth family, `blocked_excluded`, with
# reason `blocked_page`, BEFORE classify_from_summary is consulted. The
# signature is on the detail row and is independent of body size by design.
# `assign` re-checks it as defence in depth for a compare run predating this.

_BLK_REP = "b1111111-1111-1111-1111-111111111111"
_BLK_IMP = "b2222222-2222-2222-2222-222222222222"
_BLK_UNC = "b3333333-3333-3333-3333-333333333333"
_BLK_PAGE = "b4444444-4444-4444-4444-444444444444"
_BLK_HINT = "b5555555-5555-5555-5555-555555555555"
_BLK_KEY = "html/blk.zst"


def _block_compare_store(tmp_path, *, extra_rows=(), extra_inputs=()):
    """One row in each of the four families: a represented detail row, a plain
    to_import row, a no-capture-time unclassifiable row (given a price, so it
    is not itself a block signature), and one block-page object whose detail
    and carousel rows are both junk."""
    rows = [
        _prow(_BLK_REP, _WHEN, object_key="html/rep.zst", content_sha256="r",
              price=100),
        _prow(_BLK_IMP, _WHEN, object_key="html/imp.zst", content_sha256="i",
              price=200),
        _prow(_BLK_UNC, None, fetched_at_source="none", object_key="html/unc.zst",
              content_sha256="u", price=300),
        _prow(_BLK_PAGE, _WHEN, object_key=_BLK_KEY, content_sha256="b"),
        _prow(_BLK_HINT, _WHEN, source="carousel", object_key=_BLK_KEY,
              content_sha256="bc"),
        *extra_rows,
    ]
    inputs = [
        {"object_key": "html/rep.zst", "size_band": "008192-016383",
         "input_kind": "materialized", "listing_id_source": "legacy_manifest"},
        {"object_key": "html/imp.zst", "size_band": "008192-016383",
         "input_kind": "materialized", "listing_id_source": "legacy_manifest"},
        {"object_key": "html/unc.zst", "size_band": "004096-008191",
         "input_kind": "unpacked", "listing_id_source": "none"},
        {"object_key": _BLK_KEY, "size_band": "000000-000511",
         "input_kind": "unpacked", "listing_id_source": "queue_events"},
        *extra_inputs,
    ]
    store = {}
    store["recovery/plan145/parsed/rows/materialized-a.parquet"] = \
        _write_parsed_rows_fixture(tmp_path / "blk-rows.parquet", rows)
    store["recovery/plan145/parsed/inputs/materialized-a.parquet"] = \
        _write_inputs_shard(tmp_path / "blk-inputs.parquet", inputs)
    for source in ("detail", "carousel", "listings_page"):
        for month in (3, 4, 5):
            listings = [_BLK_REP] if (source == "detail" and month == 4) else []
            store[
                f"silver_normalized/observations/source={source}/obs_year=2026/"
                f"obs_month={month}/part-{source}-{month}.parquet"
            ] = _write_silver_fixture(
                tmp_path / f"blk-silver-{source}-{month}.parquet", listings, _WHEN,
            )
    store["ops_normalized/artifacts_queue_events/year=2026/month=4/"
          "part-q.parquet"] = b"queue-events"
    return store


def _only_run_id(store):
    ids = {k.split("/compared/")[1].split("/")[0]
           for k in store if "/compared/" in k}
    ids |= {k.split("/compared_probe/")[1].split("/")[0]
            for k in store if "/compared_probe/" in k}
    assert len(ids) == 1, ids
    return ids.pop()


def _family(store, family):
    import io as _io

    import pyarrow.parquet as _pq

    out = []
    for key, blob in store.items():
        if f"/{family}/" in key and ("/compared/" in key or "/compared_probe/" in key):
            out += _pq.read_table(_io.BytesIO(blob)).to_pylist()
    return out


def _blk_report(store):
    run_id = _only_run_id(store)
    for key, blob in store.items():
        if key.endswith(f"{run_id}/compare_report.json"):
            return json.loads(blob)
    raise AssertionError("no compare_report.json written")


def test_a_block_page_object_is_quarantined_whole_including_its_carousel_rows(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _block_compare_store(tmp_path)
    _patch_compare_io(mocker, store, [(_BLK_REP, "VIN")], rows=5)

    assert mod.run_compare(mod.parse_args(["compare", "--apply"])) == 0

    blocked = _family(store, "blocked_excluded")
    assert {r["object_key"] for r in blocked} == {_BLK_KEY}
    assert sorted(r["source"] for r in blocked) == ["carousel", "detail"]
    assert {r["reason"] for r in blocked} == {"blocked_page"}
    # none of that object's rows reached any other family
    for fam in ("already_represented", "to_import", "unclassifiable"):
        assert _BLK_KEY not in {r["object_key"] for r in _family(store, fam)}

    report = _blk_report(store)
    assert report["families"]["blocked_excluded"] == 2
    blk = report["blocked_excluded"]
    assert blk["rows"] == 2 and blk["objects"] == 1
    assert blk["by_source"] == {"carousel": 1, "detail": 1}
    assert blk["objects_that_emitted_carousel_rows"] == 1
    assert blk["size_band"] == {"000000-000511": 1}
    assert blk["by_input_kind"] == {"unpacked": 1}
    assert blk["by_listing_id_source"] == {"queue_events": 1}
    assert blk["detail_rows_carrying_a_business_value"] == 0


def test_a_priced_detail_row_is_untouched_even_when_its_carousel_rows_are_null(
        tmp_path, mocker):
    # Write this one first and watch it fail against a row-level filter: the
    # carousel row here is active with price/vin/make all NULL, so a row-level
    # predicate would quarantine it even though its object parsed a real price.
    import scripts.oneoff.reconcile_april_detail as mod

    ok_key = "html/ok.zst"
    ok_a = "c1111111-1111-1111-1111-111111111111"
    ok_b = "c2222222-2222-2222-2222-222222222222"
    store = _block_compare_store(
        tmp_path,
        extra_rows=[
            _prow(ok_a, _WHEN, object_key=ok_key, content_sha256="ok", price=500),
            _prow(ok_b, _WHEN, source="carousel", object_key=ok_key,
                  content_sha256="okc"),
        ],
        extra_inputs=[
            {"object_key": ok_key, "size_band": "008192-016383",
             "input_kind": "materialized", "listing_id_source": "legacy_manifest"},
        ],
    )
    _patch_compare_io(mocker, store, [(_BLK_REP, "VIN")], rows=7)

    assert mod.run_compare(mod.parse_args(["compare", "--apply"])) == 0

    assert ok_key not in {r["object_key"] for r in _family(store, "blocked_excluded")}
    ti = [r for r in _family(store, "to_import") if r["object_key"] == ok_key]
    assert len(ti) == 2                       # both rows importable, neither dropped
    report = _blk_report(store)
    assert report["blocked_excluded"]["objects"] == 1   # only the real block page


def test_an_unlisted_detail_row_with_null_values_is_not_excluded(
        tmp_path, mocker):
    # An unlisted page legitimately has no price; excluding it would discard
    # real observations.
    import scripts.oneoff.reconcile_april_detail as mod

    un_key = "html/unl.zst"
    un_id = "d1111111-1111-1111-1111-111111111111"
    store = _block_compare_store(
        tmp_path,
        extra_rows=[
            _prow(un_id, _WHEN, object_key=un_key, content_sha256="un",
                  listing_state="unlisted"),
        ],
        extra_inputs=[
            {"object_key": un_key, "size_band": "004096-008191",
             "input_kind": "materialized", "listing_id_source": "legacy_manifest"},
        ],
    )
    _patch_compare_io(mocker, store, [(_BLK_REP, "VIN")], rows=6)

    assert mod.run_compare(mod.parse_args(["compare", "--apply"])) == 0

    assert un_key not in {r["object_key"] for r in _family(store, "blocked_excluded")}
    assert un_key in {r["object_key"] for r in _family(store, "to_import")}


def test_the_four_families_sum_to_the_parsed_row_total(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _block_compare_store(tmp_path)
    _patch_compare_io(mocker, store, [(_BLK_REP, "VIN")], rows=5)

    assert mod.run_compare(mod.parse_args(["compare", "--apply"])) == 0

    fam = _blk_report(store)["families"]
    assert fam["already_represented"] >= 1
    assert fam["to_import"] >= 1
    assert fam["unclassifiable"] >= 1
    assert fam["blocked_excluded"] >= 1
    assert (fam["already_represented"] + fam["to_import"] + fam["unclassifiable"]
            + fam["blocked_excluded"] == fam["sum"] == 5)


def test_a_quarantined_object_that_emitted_carousel_rows_is_reported_not_refused(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    # Stage 4 drops NULL-price carousel hints, so every carousel row that
    # exists carries a price. A quarantined object with carousel rows is a
    # maintainer signal (a block body has no carousel), never a hard stop.
    store = _block_compare_store(tmp_path, extra_rows=[
        _prow("e6666666-6666-6666-6666-666666666666", _WHEN, source="carousel",
              object_key=_BLK_KEY, content_sha256="bcm", price=41995, make="Ford"),
    ])
    _patch_compare_io(mocker, store, [(_BLK_REP, "VIN")], rows=6)

    assert mod.run_compare(mod.parse_args(["compare", "--apply"])) == 0   # no raise

    blk = _blk_report(store)["blocked_excluded"]
    assert blk["objects_that_emitted_carousel_rows"] == 1
    assert blk["by_source"]["carousel"] == 2          # _BLK_HINT + the Ford row
    assert blk["detail_rows_carrying_a_business_value"] == 0


def test_a_loosened_block_predicate_that_keeps_a_valued_detail_row_stops_apply(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    # Guard against a future edit to is_block_signature: drop the price/vin/
    # make-all-NULL requirement and a quarantined detail row can carry a value
    # -- the filter is no longer precise and an authoritative run must stop.
    mocker.patch.object(mod, "is_block_signature",
                        lambda row: row.get("listing_state") == "active")

    store = _block_compare_store(tmp_path)
    _patch_compare_io(mocker, store, [(_BLK_REP, "VIN")], rows=5)
    with pytest.raises(ReconcileError, match="no longer precise"):
        mod.run_compare(mod.parse_args(["compare", "--apply"]))

    store = _block_compare_store(tmp_path)
    _patch_compare_io(mocker, store, [(_BLK_REP, "VIN")], rows=5)
    assert mod.run_compare(mod.parse_args(["compare"])) == 0        # dry run warns
    out = capsys.readouterr().out
    assert "is_block_signature is no longer precise" in out
    assert "3 blocked detail rows carry a value" in out            # rep/imp/unc


def test_assign_refuses_a_to_import_population_carrying_the_block_signature(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    # 25 block-page objects: each a detail row that is active with price/vin/
    # make all NULL, plus a carousel row that is not counted.
    blk_rows = []
    blk_inputs = []
    for i in range(25):
        lid = f"{i:08d}-9999-9999-9999-999999999999"
        okey = f"html/2026/04/pack/blk{i}.html.zst"
        blk_rows.append(_ti_row(lid, okey))
        blk_rows.append(_ti_row(lid, okey, source="carousel"))
        blk_inputs.append({"object_key": okey, "listing_id": lid,
                           "fetched_at": _WHEN, "input_kind": "unpacked"})
    store[f"recovery/plan145/compared/{_RUN}/to_import/unpacked-c.parquet"] = \
        _write_compared_shard(tmp_path / "blk-ti.parquet", blk_rows)
    store["recovery/plan145/parsed/inputs/unpacked-c.parquet"] = \
        _write_inputs_shard(tmp_path / "blk-in.parquet", blk_inputs)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    before = set(store)

    with pytest.raises(ImportSetInvalid, match="block_page_signature"):
        mod.run_assign(mod.parse_args(["assign", "--apply"]))
    assert set(store) == before               # a stop, before any shard

    out = capsys.readouterr().out
    assert "block_page_signature" in out
    # the whole cohort is counted (25 detail rows), the printed examples capped
    assert re.search(r"block_page_signature\s+25", out)
    assert out.count("e.g.") <= 20


def test_assign_refuses_a_compare_run_that_predates_the_block_page_filter(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    # The per-row check only sees the detail row that carries the signature,
    # and a block page's detail row can sit in already_represented while only
    # its carousel rows reach to_import -- so a stale compare run is refused
    # outright, whatever shape its leakage takes.
    store, _ = _slice2_fixture_store(tmp_path)
    store[f"recovery/plan145/compared/{_RUN}/compare_report.json"] = json.dumps({
        "plan": 145, "stage": 5, "slice": 1, "mode": "compare", "run_id": _RUN,
        "families": {"already_represented": 10, "to_import": 3,
                     "unclassifiable": 1, "sum": 14},
    }).encode()
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    before = set(store)

    with pytest.raises(ReconcileError, match="predates the block-page filter"):
        mod.run_assign(mod.parse_args(["assign", "--apply"]))
    assert set(store) == before

    # a report that has the section assigns normally
    store[f"recovery/plan145/compared/{_RUN}/compare_report.json"] = json.dumps({
        "families": {"already_represented": 10, "to_import": 3,
                     "unclassifiable": 1, "blocked_excluded": 0, "sum": 14},
        "blocked_excluded": {"rows": 0, "objects": 0},
    }).encode()
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    assert mod.run_assign(mod.parse_args(["assign", "--apply"])) == 0


def test_apply_refuses_assignment_shards_from_a_pre_block_filter_compare_run(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    # assign refuses a stale run going forward; this is the same refusal for a
    # run whose assignment shards already exist -- apply re-reads them
    # independently and is the last check before the INSERT.
    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    mod.run_assign(mod.parse_args(["assign", "--apply"]))     # shards now exist
    batch_name = assign_batch_name(_RUN, 1)

    # the compare run turns out to predate the filter. --run-id is explicit:
    # that is the path with no _compare_run_complete predecessor.
    pre_filter = json.dumps({
        "families": {"already_represented": 10, "to_import": 3, "sum": 13},
    }).encode()
    store[f"recovery/plan145/compared/{_RUN}/compare_report.json"] = pre_filter
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="predates the block-page filter"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--run-id", _RUN, "--batch", batch_name]))
    assert conn.sql == []                                     # no INSERT issued

    # a missing report fails closed under --apply (keyed on apply, not the
    # report's truthiness), and a dry run only warns
    del store[f"recovery/plan145/compared/{_RUN}/compare_report.json"]
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    with pytest.raises(ReconcileError, match="predates the block-page filter"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--run-id", _RUN, "--batch", batch_name]))
    assert mod.run_apply(mod.parse_args(
        ["apply", "--run-id", _RUN, "--batch", batch_name])) == 0


# -- R: the write canary and the flush round trip (Stage 5, slice 3, Phase B)
#
# Phase A froze *which* ~500 observations the canary commits. Phase B commits
# exactly those and then proves the asynchronous flushers carried them out of
# staging and into the lake -- staging is DELETED on flush, so nothing else can.
#
# The trap these tests exist to keep shut: `apply --batch` is not the canary.
# One slice-2 batch is 5,000 artifacts / 10,157 silver rows against a 1,000-row
# budget, so `apply --apply --batch` is refused, and forcing it with
# --maintainer-approval commits 5,000 artifacts where the plan sizes the canary
# at ~234. canary-commit is manifest-scoped and carries no approval flag at all.


def _read_manifest_rows(body):
    import io

    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(body)).to_pylist()


def _read_assigned_rows(body):
    import io

    import pyarrow.parquet as pq

    return pq.read_table(io.BytesIO(body)).to_pylist()


def _write_canary_manifest(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    from scripts.oneoff.reconcile_april_detail import _canary_manifest_schema

    schema = _canary_manifest_schema()
    pq.write_table(pa.Table.from_pylist(
        [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
    ), path, compression="zstd")
    return path.read_bytes()


def _canary_commit_store(tmp_path):
    """The Phase A store plus what a real commit needs: a post-block-filter
    compare report and the frozen VIN snapshot."""
    store = _canary_store(tmp_path)
    store[f"recovery/plan145/compared/{_C3RUN}/compare_report.json"] = json.dumps(
        {"blocked_excluded": {"rows": 0, "objects": 0}}).encode()
    return store


def _seed_canary_manifest(mod, mocker, store):
    """Run the real Phase A sampler so the manifest under test is the real one."""
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    assert mod.run_canary_sample(mod.parse_args(
        ["canary-sample", "--run-id", _C3RUN, "--apply"])) == 0
    return store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"]


def _pin(store, *, rows=5):
    """The --apply pin: the manifest digest and row count a dry run measured.

    Not a widening flag -- it can only refuse. `canary-commit --apply` requires
    it so a commit names the manifest it was approved against.
    """
    digest = hashlib.sha256(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"],
    ).hexdigest()
    return ["--expect-manifest-sha256", digest, "--expect-rows", str(rows)]


def _canary_ready(mod, tmp_path, mocker):
    store = _canary_commit_store(tmp_path)
    _seed_canary_manifest(mod, mocker, store)
    return store


# -- R.1: the commit --------------------------------------------------------

def test_the_canary_commits_exactly_the_manifests_objects_and_no_more(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store))) == 0
    assert conn.commits == 1                       # one transaction
    assert conn.rollbacks == 0

    inserts = {}
    for sql, rows in conn.executed_values:
        for name in ("silver_observations", "price_observation_events",
                     "artifacts_queue_events"):
            if name in sql:
                inserts[name] = rows

    # The manifest holds 4 artifacts / 5 silver rows: _OA (detail + carousel),
    # _OB (detail unlisted), _OC (detail), _OD (carousel).
    assert len(inserts["silver_observations"]) == 5
    assert len(inserts["price_observation_events"]) == 3     # detail rows only
    assert len(inserts["artifacts_queue_events"]) == 4       # one per artifact

    from processing.writers.silver_writer import _POSTGRES_COLS
    from scripts.oneoff.reconcile_april_detail import _QUEUE_EVENT_COLS

    queue = [dict(zip(_QUEUE_EVENT_COLS, r))
             for r in inserts["artifacts_queue_events"]]
    assert {q["minio_path"].split("bronze/")[-1] for q in queue} == {
        _OA, _OB, _OC, _OD}
    assert {q["run_id"] for q in queue} == {f"{_C3RUN}-canary"}
    assert {q["status"] for q in queue} == {RECOVERED_STATUS}

    silver = [dict(zip(_POSTGRES_COLS, r)) for r in inserts["silver_observations"]]
    # the assignment shard's identity, shared by an artifact's every row
    assert {s["artifact_id"] for s in silver} == {9000001, 4902401, 9000002,
                                                  4902402}
    assert {s["fetched_at"] for s in silver} == {_WHEN}
    # the frozen VIN snapshot fills the carousel row Stage 4 left NULL
    carousel = [s for s in silver
                if s["listing_id"] == "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"]
    assert [s["vin"] for s in carousel] == ["VIN-SNAPSHOT-CAROUSEL"]


def test_the_canary_goes_through_the_real_writer_with_the_receipt_inside_it(
        tmp_path, mocker):
    """Non-negotiable 1: reuse write_import_batch, do not reimplement it."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    seen = {}
    real = mod.write_import_batch

    def _spy(c, batch_name, digest, *a, **kw):
        seen["batch_name"] = batch_name
        seen["digest"] = digest
        seen["probe"] = kw.get("probe", False)
        return real(c, batch_name, digest, *a, **kw)

    mocker.patch.object(mod, "write_import_batch", _spy)
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store))) == 0

    assert seen["batch_name"] == f"{_C3RUN}-canary"
    assert seen["probe"] is False                  # non-negotiable 3
    # the digest is the manifest object's own bytes
    assert seen["digest"] == hashlib.sha256(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"],
    ).hexdigest()

    # the receipt is inside the one transaction, after the three inserts
    kinds = [sql for op, sql in conn.ops if op in ("execute", "execute_values")]
    assert "plan145_recovery_batch_receipts" in kinds[-1]
    assert conn.ops[-1] == ("commit", None)


def test_the_canary_receipt_name_is_never_a_slice_2_batch_name(tmp_path, mocker):
    """A canary that borrowed b00001's name would mark all 5,000 of its
    artifacts committed on the strength of ~500 rows, and the full apply would
    skip that batch forever."""
    import scripts.oneoff.reconcile_april_detail as mod

    name = mod.canary_batch_name(_C3RUN)
    assert not name.startswith(f"{_C3RUN}-b")
    assert name != mod.assign_batch_name(_C3RUN, 1)

    # and `apply` cannot select it: it lists batches by the `-b` prefix
    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    store[f"recovery/plan145/assigned/{name}.parquet"] = b"not-a-shard"

    with pytest.raises(ReconcileError, match="unknown batch name"):
        mod.run_apply(mod.parse_args(
            ["apply", "--run-id", _C3RUN, "--batch", name]))


def test_the_canary_budget_is_fixed_in_code_with_no_flag_that_raises_it(
        tmp_path, mocker):
    """A widenable ceiling is --maintainer-approval under another name: an
    oversized or wrongly regenerated manifest could be committed by editing one
    number. So the budget is a constant and this mode carries no knob."""
    import scripts.oneoff.reconcile_april_detail as mod

    for widening in (["--max-rows", "5000"],
                     ["--max-unapproved-rows", "5000"],
                     ["--maintainer-approval", "me"],
                     ["--probe"]):
        with pytest.raises(SystemExit):
            mod.parse_args(["canary-commit", "--apply"] + widening)

    # 505 real rows against 1,000: the sample fits, one slice-2 batch (10,157)
    # does not, which is the whole reason this mode exists.
    assert mod.CANARY_ROW_BUDGET == 1000

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    mocker.patch.object(mod, "CANARY_ROW_BUDGET", 2)
    with pytest.raises(ReconcileError, match="over the fixed 2-row canary budget"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.sql == []                    # refused before any statement
    assert conn.executed_values == []
    assert conn.commits == 0


def test_an_over_budget_dry_run_reports_the_overage_instead_of_dying(
        tmp_path, capsys, mocker):
    """The oversized manifest is exactly when the maintainer needs the safe
    measurement run. A dry run opens no connection, so refusing one buys
    nothing and costs them the number they need."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    mocker.patch.object(mod, "CANARY_ROW_BUDGET", 2)

    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN])) == 0
    out = capsys.readouterr().out
    assert "OVER BUDGET" in out
    assert "5 silver rows against the fixed 2-row" in out
    assert "by 3" in out
    assert conn.sql == []                    # and still no statement
    assert conn.executed_values == []


def test_an_apply_must_pin_the_manifest_it_was_approved_against(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    # a bare --apply names no manifest
    with pytest.raises(ReconcileError, match="--expect-manifest-sha256"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"]))
    # half a pin is not a pin
    with pytest.raises(ReconcileError, match="--expect-manifest-sha256"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply",
             "--expect-manifest-sha256", "a" * 64]))
    # a pin that does not match the manifest on disk
    with pytest.raises(ReconcileError, match="not the one this run was approved"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply",
             "--expect-manifest-sha256", "a" * 64, "--expect-rows", "5"]))
    # the right digest but the wrong count
    with pytest.raises(ReconcileError, match="silver rows pinned 4 but measured 5"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store, rows=4)))
    assert conn.sql == []
    assert conn.executed_values == []

    # the dry run needs no pin -- it is how the two values are read
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN])) == 0


def test_the_dry_run_prints_the_pin_the_commit_will_need(tmp_path,
                                                         capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_canary_commit(mod.parse_args(["canary-commit", "--run-id", _C3RUN]))
    out = capsys.readouterr().out
    digest = hashlib.sha256(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"],
    ).hexdigest()
    assert f"--expect-manifest-sha256 {digest} --expect-rows 5" in out


def test_the_canary_dry_run_builds_the_write_set_and_issues_no_statement(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN])) == 0
    out = capsys.readouterr().out
    assert "DRY RUN" in out
    assert "staging.silver_observations" in out
    assert "ops.artifacts_queue" in out           # named as never touched
    assert re.search(r"^silver rows +5\b", out, re.M)
    assert re.search(r"^artifacts +4$", out, re.M)
    assert conn.sql == []
    assert f"recovery/plan145/canary/{_C3RUN}-canary_commit.json" not in store


def test_a_rerun_of_the_canary_skips_on_the_receipt_and_writes_zero_rows(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    digest = hashlib.sha256(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"],
    ).hexdigest()

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    first_report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"])
    assert first_report["skipped_on_receipt"] is False
    assert first_report["written_this_run"]["silver"] == 5

    # the receipt is now present for this batch name and this manifest digest
    again = _FakeWriteConn(receipts={f"{_C3RUN}-canary": [digest]})
    _record_execute_values(mocker, again)
    _patch_slice2_io(mocker, store, again)
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store))) == 0
    assert again.executed_values == []           # no INSERT of any kind
    assert again.commits == 0
    assert again.rollbacks == 1
    assert "SKIPPED" in capsys.readouterr().out
    # the original commit report is kept -- its committed_at is what bounds the
    # flush verification's object scan
    assert json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"],
    ) == first_report


def test_the_same_canary_name_with_a_changed_manifest_stops(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod
    from scripts.oneoff.reconcile_april_detail import ReceiptConflict

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn(receipts={f"{_C3RUN}-canary": ["b" * 64]})
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    with pytest.raises(ReceiptConflict):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.executed_values == []
    assert conn.commits == 0


def test_the_canary_writes_identity_from_the_shard_not_the_manifests_copy(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    key = f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"
    rows = _read_manifest_rows(store[key])
    for row in rows:
        if row["object_key"] == _OA:
            row["artifact_id"] = 7_777_777        # a value no shard allocated
    store[key] = _write_canary_manifest(tmp_path / "tampered.parquet", rows)

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError,
                       match="manifest field.*disagree with the assignment"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.executed_values == []


def test_the_canary_stops_when_a_manifest_object_has_no_assignment_row(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    assigned_key = f"recovery/plan145/assigned/{_C3RUN}-b00001.parquet"
    kept = [r for r in _read_assigned_rows(store[assigned_key])
            if r["object_key"] != _OC]
    store[assigned_key] = _write_assigned_shard(tmp_path / "trimmed.parquet", kept)

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="no row in the assignment shard"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.executed_values == []


def test_the_canary_stops_rather_than_commit_half_an_artifact(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    # drop _OA's carousel row: the object now reads 1 where both the assignment
    # and the manifest say 2
    la = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    lb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lc = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    ld = "dddddddd-0000-0000-0000-dddddddddddd"
    store[f"recovery/plan145/compared/{_C3RUN}/to_import/unit-a.parquet"] = \
        _write_compared_shard(tmp_path / "short.parquet", [
            _ti_row(la, _OA, source="detail"),
            _ti_row(lb, _OB, source="detail", listing_state="unlisted"),
            _ti_row(lc, _OC, source="detail"),
            _ti_row(ld, _OD, source="carousel"),
        ])

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError,
                       match="no longer match the frozen canary manifest"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.executed_values == []


def test_the_canary_never_names_a_protected_table_in_any_statement(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))

    issued = [sql for sql, _ in conn.sql] + [sql for sql, _ in conn.executed_values]
    for forbidden in ("ops.artifacts_queue", "ops.price_observations",
                      "ops.vin_to_listing", "ops.blocked_cooldown",
                      "ops.detail_scrape_claims"):
        assert not any(forbidden in sql for sql in issued), forbidden


def test_the_canary_refuses_a_compare_run_that_predates_the_block_page_filter(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    store[f"recovery/plan145/compared/{_C3RUN}/compare_report.json"] = json.dumps(
        {"families": {"to_import": 5}}).encode()

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="predates the block-page filter"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.sql == []


def test_the_canary_says_to_run_the_sampler_when_there_is_no_manifest(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_commit_store(tmp_path)      # no canary-sample run
    conn = _FakeWriteConn()
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="canary-sample --apply"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply",
             "--expect-manifest-sha256", "a" * 64, "--expect-rows", "5"]))
    assert conn.sql == []


# -- R.1b: the manifest is a contract over the row multiset ----------------
#
# The attack these close: mutate the *composition* of a selected artifact's
# rows without changing how many there are. Every count-based check passes, the
# object set is unchanged, the assignment still resolves -- and the write set
# quietly becomes a superset of the one the maintainer approved.


def _retarget_to_import(tmp_path, store, rows, name):
    store[f"recovery/plan145/compared/{_C3RUN}/to_import/unit-a.parquet"] = \
        _write_compared_shard(tmp_path / name, rows)


def test_a_carousel_row_flipped_to_detail_is_caught_though_the_count_holds(
        tmp_path, mocker):
    """The concrete superset: _OA keeps two rows, but its carousel row becomes
    a detail row, so build_recovery_price_event mints a historical price event
    the frozen sample never approved."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    la = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    lac = "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
    lb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lc = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    ld = "dddddddd-0000-0000-0000-dddddddddddd"
    _retarget_to_import(tmp_path, store, [
        _ti_row(la, _OA, source="detail"),
        _ti_row(lac, _OA, source="detail"),          # was carousel
        _ti_row(lb, _OB, source="detail", listing_state="unlisted"),
        _ti_row(lc, _OC, source="detail"),
        _ti_row(ld, _OD, source="carousel"),
    ], "flipped.parquet")

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError,
                       match="no longer match the frozen canary manifest") as exc:
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    # the total row count is untouched, so it is the composition that catches it
    assert "detail_rows" in str(exc.value)
    assert conn.sql == []
    assert conn.executed_values == []
    assert conn.commits == 0


def test_a_changed_business_value_is_caught_by_the_write_set_digest_alone(
        tmp_path, mocker):
    """Same object, same count, same detail/carousel split, same strata -- only
    the price moved. Nothing but the row digest sees this."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    la = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    lac = "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
    lb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lc = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    ld = "dddddddd-0000-0000-0000-dddddddddddd"
    _retarget_to_import(tmp_path, store, [
        _ti_row(la, _OA, source="detail", price=999_999),   # was the default
        _ti_row(lac, _OA, source="carousel"),
        _ti_row(lb, _OB, source="detail", listing_state="unlisted"),
        _ti_row(lc, _OC, source="detail"),
        _ti_row(ld, _OD, source="carousel"),
    ], "repriced.parquet")

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError,
                       match="rebuild to a different write set") as exc:
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    message = str(exc.value)
    # the structural checks all passed -- only the digest saw this
    assert "detail_rows" not in message and "strata" not in message
    assert conn.executed_values == []


def test_the_write_set_digest_ignores_shard_row_order():
    """A shard's row order is not part of the contract; re-reading one must not
    perturb the digest, or every commit would be a false alarm."""
    import scripts.oneoff.reconcile_april_detail as mod

    assignment = {"object_key": _OA, "artifact_id": 9000001,
                  "listing_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                  "fetched_at": _WHEN}
    rows = [_ti_row("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", _OA, source="detail"),
            _ti_row("aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa", _OA, source="carousel")]
    forward = mod.canary_write_set_digest(
        *mod.build_canary_artifact_write_set(rows, assignment, {}, "b", "bronze"))
    reverse = mod.canary_write_set_digest(
        *mod.build_canary_artifact_write_set(
            list(reversed(rows)), assignment, {}, "b", "bronze"))
    assert forward == reverse


def test_a_vin_the_snapshot_fills_changes_the_write_set_digest():
    """The hole a raw-row digest leaves open: build_recovery_silver_row fills a
    missing carousel vin from the frozen snapshot *after* the to_import row, so
    a snapshot that moves changes what gets committed."""
    import scripts.oneoff.reconcile_april_detail as mod

    hint = "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
    assignment = {"object_key": _OA, "artifact_id": 9000001,
                  "listing_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
                  "fetched_at": _WHEN}
    rows = [_ti_row(hint, _OA, source="carousel")]        # vin is NULL
    one = mod.canary_write_set_digest(
        *mod.build_canary_artifact_write_set(
            rows, assignment, {hint: "VIN-ONE"}, "b", "bronze"))
    two = mod.canary_write_set_digest(
        *mod.build_canary_artifact_write_set(
            rows, assignment, {hint: "VIN-TWO"}, "b", "bronze"))
    assert one != two


def test_the_assignment_capture_time_changes_the_write_set_digest():
    """And the other one: the queue event's historical fetched_at comes from
    the assignment, which no to_import row carries."""
    import scripts.oneoff.reconcile_april_detail as mod

    rows = [_ti_row("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa", _OA, source="detail")]
    base = {"object_key": _OA, "artifact_id": 9000001,
            "listing_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"}
    one = mod.canary_write_set_digest(
        *mod.build_canary_artifact_write_set(
            rows, {**base, "fetched_at": _WHEN}, {}, "b", "bronze"))
    two = mod.canary_write_set_digest(
        *mod.build_canary_artifact_write_set(
            rows, {**base, "fetched_at": _dt(2026, 4, 16, 12, 0, 0, tzinfo=_tz.utc)},
            {}, "b", "bronze"))
    assert one != two


@pytest.mark.parametrize("field,value", [
    ("artifact_id", 7_777_777),
    ("id_source", "allocated_sequence"),        # _OB is preserved_queue_event
    ("input_kind", "materialized"),             # _OB is unpacked
    ("page_listing_id", "99999999-9999-9999-9999-999999999999"),
    ("silver_rows", 9),
    ("detail_rows", 0),
])
def test_every_manifest_field_copied_from_the_assignment_is_bound(
        tmp_path, field, value, mocker):
    """Not just artifact_id. Each of these reaches the write set or names the
    stratum the sample was approved on."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    key = f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"
    rows = _read_manifest_rows(store[key])
    for row in rows:
        if row["object_key"] == _OB:
            row[field] = value
    store[key] = _write_canary_manifest(tmp_path / f"m-{field}.parquet", rows)

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError,
                       match="disagree with the assignment shard") as exc:
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert field in str(exc.value)
    assert conn.executed_values == []


def test_a_manifest_naming_an_assignment_batch_that_does_not_exist_stops(
        tmp_path, mocker):
    """`batch_name` is bound too, but a wrong one never reaches the field
    comparison -- it resolves to no shard. That has to be a refusal, not a
    KeyError out of the object read."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    key = f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"
    rows = _read_manifest_rows(store[key])
    for row in rows:
        if row["object_key"] == _OB:
            row["batch_name"] = f"{_C3RUN}-b00002"
    store[key] = _write_canary_manifest(tmp_path / "m-batch.parquet", rows)

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="does not exist"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.sql == []


def test_a_manifest_with_no_write_set_digest_is_refused_not_exempted(tmp_path, mocker):
    """Fail closed. A pre-write_set_digest manifest freezes only counts, which is
    exactly the contract the column exists to replace."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    key = f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"
    rows = _read_manifest_rows(store[key])
    for row in rows:
        row["write_set_digest"] = None
    store[key] = _write_canary_manifest(tmp_path / "nodigest.parquet", rows)

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="carries no write_set_digest") as exc:
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    # and it names the migration, not a re-sample: re-sampling reselects
    assert "canary-remanifest --apply" in str(exc.value)
    assert "canary-sample" not in str(exc.value)
    assert conn.sql == []


def test_the_sampler_freezes_a_write_set_digest_for_every_selected_artifact(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    rows = _read_manifest_rows(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"])
    assert rows
    for row in rows:
        assert len(row["write_set_digest"]) == 64
    # distinct artifacts, distinct digests
    assert len({r["write_set_digest"] for r in rows}) == len(rows)


# -- R.1d: migrating a pre-digest manifest, without destroying it ----------
#
# Re-running canary-sample to pick up the new columns *reselects*. Determinism
# reproduces the selection only while every input is unchanged, which is the
# assumption the digest exists to distrust -- and the aggregates (234 artifacts,
# 505 rows, 9 strata) cannot tell one 234-object set from another. The sampler
# is create-if-absent, so that route also means deleting the only record of what
# the V040 window's subject was.


def _v1_manifest(tmp_path, store, *, drop=("write_set_digest",
                                           "vin_snapshot_sha256",
                                           "page_fetched_at")):
    """Age the manifest back to what is on disk today: no digest columns."""
    key = f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"
    rows = _read_manifest_rows(store[key])
    for row in rows:
        for name in drop:
            row[name] = None
    store[key] = _write_canary_manifest(tmp_path / "v1.parquet", rows)
    return rows


def test_the_migration_preserves_the_frozen_manifest_and_its_object_set(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    source_key = f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"
    target_key = f"recovery/plan145/canary/{_C3RUN}-canary_sample_digested.parquet"
    v1_rows = _v1_manifest(tmp_path, store)
    v1_bytes = store[source_key]

    _patch_slice2_io(mocker, store, _FakeWriteConn())
    assert mod.run_canary_remanifest(mod.parse_args(
        ["canary-remanifest", "--run-id", _C3RUN, "--apply"])) == 0

    # the original is untouched, byte for byte
    assert store[source_key] == v1_bytes
    migrated = _read_manifest_rows(store[target_key])
    # the object set is identical, and the run says so by digest
    assert ([r["object_key"] for r in migrated]
            == sorted(r["object_key"] for r in v1_rows))
    out = capsys.readouterr().out
    assert "identical: True" in out
    assert "is NOT deleted and NOT overwritten" in out
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_remanifest_report.json"])
    assert report["object_set_digest"]["identical"] is True
    assert report["source_preserved"] is True
    assert report["artifacts"] == 4 and report["silver_rows"] == 5

    # and the migrated manifest is what canary-commit now reads
    for row in migrated:
        assert len(row["write_set_digest"]) == 64
        assert row["page_fetched_at"] is not None
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    digest = hashlib.sha256(store[target_key]).hexdigest()
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply",
         "--expect-manifest-sha256", digest, "--expect-rows", "5"])) == 0
    assert conn.commits == 1


def test_the_migration_refuses_when_an_input_moved_under_the_frozen_sample(
        tmp_path, mocker):
    """The whole reason not to re-sample: if an input moved, a re-selection
    would quietly pick a different set. The migration stops instead."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    _v1_manifest(tmp_path, store)
    la = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    lac = "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
    lb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lc = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    ld = "dddddddd-0000-0000-0000-dddddddddddd"
    _retarget_to_import(tmp_path, store, [
        _ti_row(la, _OA, source="detail"),
        _ti_row(lac, _OA, source="detail"),          # was carousel
        _ti_row(lb, _OB, source="detail", listing_state="unlisted"),
        _ti_row(lc, _OC, source="detail"),
        _ti_row(ld, _OD, source="carousel"),
    ], "moved.parquet")

    _patch_slice2_io(mocker, store, _FakeWriteConn())
    with pytest.raises(ReconcileError, match="no longer read the way the manifest"):
        mod.run_canary_remanifest(mod.parse_args(
            ["canary-remanifest", "--run-id", _C3RUN, "--apply"]))
    assert (f"recovery/plan145/canary/{_C3RUN}-canary_sample_digested.parquet"
            not in store)


def test_the_migration_will_not_overwrite_an_existing_migrated_manifest(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    _v1_manifest(tmp_path, store)
    store[f"recovery/plan145/canary/{_C3RUN}-canary_sample_digested.parquet"] = b"x"
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    with pytest.raises(ReconcileError, match="already exists"):
        mod.run_canary_remanifest(mod.parse_args(
            ["canary-remanifest", "--run-id", _C3RUN, "--apply"]))


def test_the_migration_dry_run_writes_nothing(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    _v1_manifest(tmp_path, store)
    before = set(store)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    assert mod.run_canary_remanifest(mod.parse_args(
        ["canary-remanifest", "--run-id", _C3RUN])) == 0
    assert set(store) == before


def test_a_migrated_manifest_beside_the_original_is_the_one_that_is_read(
        tmp_path, mocker):
    """Resolution is by existence, not by a flag: there is no way to commit
    against the weaker manifest while a migrated one sits beside it."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    _v1_manifest(tmp_path, store)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    mod.run_canary_remanifest(mod.parse_args(
        ["canary-remanifest", "--run-id", _C3RUN, "--apply"]))

    target_key = f"recovery/plan145/canary/{_C3RUN}-canary_sample_digested.parquet"
    # pinning the *original* digest now refuses -- the resolved manifest is the
    # migrated one, and the pin is taken over its exact bytes
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="not the one this run was approved"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.executed_values == []
    assert hashlib.sha256(store[target_key]).hexdigest() != hashlib.sha256(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"]).hexdigest()


def _migrated(mod, tmp_path, mocker, store):
    """Age the manifest to v1 and migrate it, returning the sibling's key."""
    _v1_manifest(tmp_path, store)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    assert mod.run_canary_remanifest(mod.parse_args(
        ["canary-remanifest", "--run-id", _C3RUN, "--apply"])) == 0
    return f"recovery/plan145/canary/{_C3RUN}-canary_sample_digested.parquet"


def _commit_the_sibling(mod, mocker, store, target_key, *, rows=5):
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    args = ["canary-commit", "--run-id", _C3RUN, "--apply",
            "--expect-manifest-sha256",
            hashlib.sha256(store[target_key]).hexdigest(),
            "--expect-rows", str(rows)]
    return conn, mod.parse_args(args)


def test_a_substituted_sibling_with_a_different_object_set_commits_nothing(
        tmp_path, mocker):
    """Resolution picks the sibling because it exists. Existence is not a
    reason to trust it: a sibling created or replaced independently could hand
    a commit an object set that is not the window's subject."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    target_key = _migrated(mod, tmp_path, mocker, store)

    # a valid, digest-bearing, self-consistent sibling -- over three artifacts
    # instead of four. Every per-artifact check inside it passes.
    rows = [r for r in _read_manifest_rows(store[target_key])
            if r["object_key"] != _OD]
    store[target_key] = _write_canary_manifest(tmp_path / "sub.parquet", rows)

    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key, rows=4)
    with pytest.raises(ReconcileError, match="not a promotion of the frozen") as exc:
        mod.run_canary_commit(parsed)
    assert "it selects different artifacts" in str(exc.value)
    assert conn.sql == []
    assert conn.executed_values == []
    assert conn.commits == 0


def test_a_same_object_set_sibling_that_changed_a_field_commits_nothing(
        tmp_path, mocker):
    """The attack object-set equality does not cover. Keep every object key,
    flip one selected carousel row to detail in `to_import`, and substitute a
    sibling that agrees with the flip: its write_set_digest matches the mutated
    inputs, so every downstream check -- which compares against those *current*
    inputs -- passes. Only the frozen manifest still remembers that _OA was
    carousel, and an extra historical price event is minted without it."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    target_key = _migrated(mod, tmp_path, mocker, store)
    sibling = _read_manifest_rows(store[target_key])

    la = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    lac = "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
    lb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lc = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    ld = "dddddddd-0000-0000-0000-dddddddddddd"
    flipped = [_ti_row(la, _OA, source="detail"),
               _ti_row(lac, _OA, source="detail")]        # was carousel
    _retarget_to_import(tmp_path, store, flipped + [
        _ti_row(lb, _OB, source="detail", listing_state="unlisted"),
        _ti_row(lc, _OC, source="detail"),
        _ti_row(ld, _OD, source="carousel"),
    ], "flip-and-substitute.parquet")

    # a sibling that agrees with the flip: same object keys, same source
    # binding, but _OA now records two detail rows and one stratum
    assignment = {"object_key": _OA, "artifact_id": 9000001,
                  "listing_id": la, "fetched_at": _WHEN,
                  "input_kind": "materialized",
                  "id_source": "allocated_sequence"}
    for row in sibling:
        if row["object_key"] == _OA:
            row["detail_rows"] = 2
            row["strata"] = ["detail|active|materialized|allocated_sequence"]
            row["write_set_digest"] = mod.canary_write_set_digest(
                *mod.build_canary_artifact_write_set(
                    flipped, assignment,
                    {lac: "VIN-SNAPSHOT-CAROUSEL"}, f"{_C3RUN}-canary", "bronze"))
    store[target_key] = _write_canary_manifest(tmp_path / "sameset.parquet",
                                               sibling)

    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key)
    with pytest.raises(ReconcileError, match="not a promotion of the frozen") as exc:
        mod.run_canary_commit(parsed)
    message = str(exc.value)
    assert "were changed" in message
    assert "detail_rows" in message or "strata" in message
    # the object set is untouched, so that branch did not fire
    assert "selects different artifacts" not in message
    assert conn.sql == []
    assert conn.executed_values == []
    assert conn.commits == 0


@pytest.mark.parametrize("field,value", [
    ("artifact_id", 7_777_777),
    ("batch_name", f"{_C3RUN}-b00002"),
    ("id_source", "preserved_queue_event"),
    ("input_kind", "unpacked"),
    ("page_listing_id", "99999999-9999-9999-9999-999999999999"),
    ("silver_rows", 9),
    ("detail_rows", 0),
    ("strata", ["something|else|entirely|here"]),
])
def test_every_field_the_frozen_manifest_carried_must_survive_promotion(
        tmp_path, field, value, mocker):
    """A promotion may add page_fetched_at, write_set_digest,
    vin_snapshot_sha256 and the two source columns. Nothing else."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    target_key = _migrated(mod, tmp_path, mocker, store)
    sibling = _read_manifest_rows(store[target_key])
    for row in sibling:
        if row["object_key"] == _OA:
            row[field] = value
    store[target_key] = _write_canary_manifest(tmp_path / f"p-{field}.parquet",
                                               sibling)

    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key)
    with pytest.raises(ReconcileError, match="were changed") as exc:
        mod.run_canary_commit(parsed)
    assert field in str(exc.value)
    assert conn.executed_values == []


def test_a_promotion_may_add_the_migration_columns_and_only_those():
    import scripts.oneoff.reconcile_april_detail as mod

    added = mod._CANARY_MIGRATION_ADDED_FIELDS
    assert added == {"page_fetched_at", "write_set_digest",
                     "vin_snapshot_sha256", "source_manifest_sha256",
                     "source_object_set_digest"}
    # every other column in the schema is checked for preservation
    carried = [n for n in mod._canary_manifest_schema().names if n not in added]
    assert carried == ["run_id", "object_key", "artifact_id", "id_source",
                       "input_kind", "batch_name", "page_listing_id",
                       "silver_rows", "detail_rows", "strata"]


def test_strata_order_is_not_part_of_the_promotion_contract(tmp_path, mocker):
    """The two manifests must name the same strata; a Parquet round trip is
    not required to preserve their order."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    target_key = _migrated(mod, tmp_path, mocker, store)
    sibling = _read_manifest_rows(store[target_key])
    for row in sibling:
        row["strata"] = list(reversed(row["strata"]))
    store[target_key] = _write_canary_manifest(tmp_path / "reordered.parquet",
                                               sibling)

    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key)
    assert mod.run_canary_commit(parsed) == 0
    assert conn.commits == 1


def test_a_sibling_promoted_from_other_bytes_commits_nothing(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    target_key = _migrated(mod, tmp_path, mocker, store)
    rows = _read_manifest_rows(store[target_key])
    for row in rows:
        row["source_manifest_sha256"] = "a" * 64
    store[target_key] = _write_canary_manifest(tmp_path / "othersrc.parquet", rows)

    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key)
    with pytest.raises(ReconcileError, match="not a promotion of the frozen") as exc:
        mod.run_canary_commit(parsed)
    assert "now hashes to" in str(exc.value)
    assert conn.executed_values == []


def test_a_sibling_naming_no_source_at_all_commits_nothing(tmp_path, mocker):
    """Only canary-remanifest may write this object, and it always names what
    it promoted."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    target_key = _migrated(mod, tmp_path, mocker, store)
    rows = _read_manifest_rows(store[target_key])
    for row in rows:
        row["source_manifest_sha256"] = None
        row["source_object_set_digest"] = None
    store[target_key] = _write_canary_manifest(tmp_path / "nosrc.parquet", rows)

    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key)
    with pytest.raises(ReconcileError,
                       match="does not name one frozen manifest"):
        mod.run_canary_commit(parsed)
    assert conn.executed_values == []


def test_the_frozen_manifest_must_still_exist_to_prove_the_promotion(
        tmp_path, mocker):
    """It is the only record of what the window's subject was. A promotion
    that can no longer be checked against it is not a promotion."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    target_key = _migrated(mod, tmp_path, mocker, store)
    del store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"]

    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key)
    with pytest.raises(ReconcileError, match="is gone; it is the only record"):
        mod.run_canary_commit(parsed)
    assert conn.executed_values == []


def test_the_frozen_slot_may_not_claim_to_be_a_promotion(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    key = f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"
    rows = _read_manifest_rows(store[key])
    for row in rows:
        row["source_manifest_sha256"] = "b" * 64
    store[key] = _write_canary_manifest(tmp_path / "fakesrc.parquet", rows)

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="slots are not interchangeable"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.executed_values == []


def test_a_good_promotion_is_reproved_and_reported_at_commit_time(
        tmp_path, capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    source_key = f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"
    target_key = _migrated(mod, tmp_path, mocker, store)

    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key)
    assert mod.run_canary_commit(parsed) == 0
    assert conn.commits == 1
    out = capsys.readouterr().out
    assert f"promoted from        {source_key}" in out
    assert "re-proved against the frozen manifest" in out
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"])
    assert report["promotion"]["source_sha256"] == hashlib.sha256(
        store[source_key]).hexdigest()


def test_the_flush_check_also_refuses_an_unproven_sibling(tmp_path, mocker):
    """It rebuilds the write set from the manifest, so it resolves the same
    sibling and must apply the same proof."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    target_key = _migrated(mod, tmp_path, mocker, store)
    conn, parsed = _commit_the_sibling(mod, mocker, store, target_key)
    mod.run_canary_commit(parsed)

    rows = [r for r in _read_manifest_rows(store[target_key])
            if r["object_key"] != _OD]
    store[target_key] = _write_canary_manifest(tmp_path / "sub2.parquet", rows)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    with pytest.raises(ReconcileError, match="not a promotion of the frozen"):
        mod.run_canary_flush_verify(mod.parse_args(
            ["canary-flush-verify", "--run-id", _C3RUN]))


# -- R.1f: --bucket is refused rather than half-honoured -------------------

@pytest.mark.parametrize("mode", ["canary-commit", "canary-remanifest",
                                  "canary-flush-verify"])
def test_phase_b_refuses_a_bucket_that_is_not_the_configured_one(
        tmp_path, mode, mocker):
    """Reads take the bucket they are given, but bare-key object_exists and
    write_bytes use the configured one -- so an override splits a run's
    inputs, its checks and its outputs across two buckets."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    argv = [mode, "--run-id", _C3RUN, "--bucket", "somewhere-else"]
    if mode == "canary-commit":
        argv += ["--apply"] + _pin(store)
    with pytest.raises(ReconcileError, match="does not match the configured"):
        mod.parse_args(argv).func(mod.parse_args(argv))
    assert conn.sql == []
    assert conn.executed_values == []


def test_phase_b_accepts_the_configured_bucket_named_explicitly(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.minio as minio

    store = _canary_ready(mod, tmp_path, mocker)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--bucket", minio.BUCKET])) == 0


# -- R.1e: the VIN snapshot is an input to the write set -------------------

def test_a_vin_snapshot_that_moved_after_sampling_stops_the_commit(
        tmp_path, mocker):
    """build_recovery_silver_row fills a missing carousel vin from the frozen
    snapshot, so changing it changes a committed VIN -- while every count,
    stratum and assignment check passes."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    store[f"recovery/plan145/vin_snapshot/{_C3RUN}.parquet"] = _write_vin_shard(
        tmp_path / "vin-moved.parquet",
        [("aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa", "VIN-SOMETHING-ELSE")],
    )

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="VIN snapshot") as exc:
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert "carousel VINs this canary would commit" in str(exc.value)
    assert conn.sql == []
    assert conn.executed_values == []


def test_an_assignment_capture_time_that_moved_stops_the_commit(tmp_path, mocker):
    """The queue event's historical fetched_at comes from the assignment, and
    no to_import row carries it."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    assigned_key = f"recovery/plan145/assigned/{_C3RUN}-b00001.parquet"
    rows = _read_assigned_rows(store[assigned_key])
    for row in rows:
        if row["object_key"] == _OB:
            row["fetched_at"] = _dt(2026, 4, 16, 12, 0, 0, tzinfo=_tz.utc)
    store[assigned_key] = _write_assigned_shard(tmp_path / "asg-moved.parquet", rows)

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError,
                       match="disagree with the assignment shard") as exc:
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert "page_fetched_at" in str(exc.value)
    assert conn.executed_values == []


# -- R.1c: the commit time is the receipt's, never a wall clock ------------

def test_a_lost_commit_report_is_repaired_with_the_receipts_own_time(
        tmp_path, mocker):
    """V047 stores committed_at inside the writing transaction. If the MinIO
    report write fails after the commit, the retry that repairs it must record
    when the batch actually landed -- canary-flush-verify uses that time as its
    LastModified bound and to pick the queue-event partition, so a retry's
    clock sends it looking in the wrong month."""
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.minio as minio

    store = _canary_ready(mod, tmp_path, mocker)
    real_commit = _dt(2026, 7, 31, 23, 50, 0, tzinfo=_tz.utc)
    digest = hashlib.sha256(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"],
    ).hexdigest()

    # first run: the transaction commits, then the report write fails
    conn = _FakeWriteConn(receipt_committed_at=real_commit)
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    def _boom(key, data, content_type=None):
        raise RuntimeError("MinIO write failed after the commit")

    mocker.patch.object(minio, "write_bytes", _boom)
    with pytest.raises(RuntimeError, match="after the commit"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert conn.commits == 1                    # the rows are durable
    commit_key = f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"
    assert commit_key not in store              # the evidence is not

    # the retry, a month later by the wall clock, skips on the receipt
    again = _FakeWriteConn(receipts={f"{_C3RUN}-canary": [digest]},
                           receipt_committed_at=real_commit,
                           receipt_counts=(4, 5, 3, 4))
    _record_execute_values(mocker, again)
    _patch_slice2_io(mocker, store, again)
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store))) == 0
    assert again.executed_values == []          # still a no-op

    report = json.loads(store[commit_key])
    assert report["skipped_on_receipt"] is True
    assert report["committed_at_source"] == "receipt"
    # July, from the receipt -- not the August wall clock of the retry
    assert report["committed_at"].startswith("2026-07-31T23:50")
    assert report["receipt_row"]["silver_count"] == 5


def test_the_first_commit_records_the_receipts_time_not_the_processs(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    real_commit = _dt(2026, 8, 29, 6, 30, 0, tzinfo=_tz.utc)
    conn = _FakeWriteConn(receipt_committed_at=real_commit)
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"])
    assert report["committed_at"] == real_commit.isoformat()
    assert report["committed_at_source"] == "receipt"


def test_no_receipt_time_is_a_refusal_not_a_wall_clock_fallback(tmp_path, mocker):
    """Falling back to a prior report, or to now(), reintroduces exactly the
    wrong LastModified bound and the wrong queue-event partition that reading
    the receipt exists to prevent. V047 declares the column NOT NULL DEFAULT
    now(), so a missing value is a receipt problem, not a timing one."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    commit_key = f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"
    # a prior report exists, and must NOT be accepted as a substitute
    store[commit_key] = json.dumps(
        {"committed_at": "2026-08-29T07:00:00+00:00"}).encode()

    conn = _FakeWriteConn(receipt_committed_at=None)
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="no receipt committed_at") as exc:
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert "refusing to write a commit report" in str(exc.value)
    # the stale report is left exactly as it was -- not updated, not trusted
    assert json.loads(store[commit_key]) == {
        "committed_at": "2026-08-29T07:00:00+00:00"}


def test_no_receipt_time_leaves_no_flush_expectation_behind(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn(receipt_committed_at=None)
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="no receipt committed_at"):
        mod.run_canary_commit(mod.parse_args(
            ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store)))
    assert f"recovery/plan145/canary/{_C3RUN}-canary_commit.json" not in store

    # and the flush check then has nothing to run against, rather than a
    # guessed window it would silently scan the wrong month with
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    with pytest.raises(ReconcileError, match="canary-commit --apply"):
        mod.run_canary_flush_verify(mod.parse_args(
            ["canary-flush-verify", "--run-id", _C3RUN]))


def test_a_commit_report_that_disagrees_with_the_receipt_is_rewritten(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    real_commit = _dt(2026, 7, 31, 23, 50, 0, tzinfo=_tz.utc)
    digest = hashlib.sha256(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_sample.parquet"],
    ).hexdigest()
    commit_key = f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"
    store[commit_key] = json.dumps(
        {"committed_at": "2026-08-29T07:00:00+00:00"}).encode()

    conn = _FakeWriteConn(receipts={f"{_C3RUN}-canary": [digest]},
                          receipt_committed_at=real_commit)
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store))) == 0
    assert json.loads(store[commit_key])["committed_at"] == real_commit.isoformat()


# -- R.2: the flush round trip ---------------------------------------------
#
# The three staging tables are flushed to Parquet and then DELETED, so a canary
# that stopped at Postgres has not proven its rows survived. These verify by
# key against the lake prefixes the two flushers actually write
# (archiver/processors/flush_silver_observations.py,
#  archiver/processors/flush_staging_events.py) -- including that a silver
# row's `source` lives in the hive path, not in the file.


def _write_lake_silver(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    # No `source`: pq.write_to_dataset(partition_cols=["source", ...]) drops the
    # partition columns from the file and encodes them in the key.
    schema = pa.schema([
        pa.field("artifact_id", pa.int64()),
        pa.field("listing_id", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    ])
    pq.write_table(pa.Table.from_pylist(
        [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
    ), path, compression="zstd")
    return path.read_bytes()


def _write_lake_price_events(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("artifact_id", pa.int64()),
        pa.field("listing_id", pa.string()),
        pa.field("event_type", pa.string()),
        pa.field("event_at", pa.timestamp("us", tz="UTC")),
    ])
    pq.write_table(pa.Table.from_pylist(
        [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
    ), path, compression="zstd")
    return path.read_bytes()


def _write_lake_queue_events(path, rows):
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("artifact_id", pa.int64()),
        pa.field("status", pa.string()),
        pa.field("run_id", pa.string()),
        pa.field("fetched_at", pa.timestamp("us", tz="UTC")),
    ])
    pq.write_table(pa.Table.from_pylist(
        [{k: r.get(k) for k in schema.names} for r in rows], schema=schema,
    ), path, compression="zstd")
    return path.read_bytes()


def _flush_the_canary(mod, tmp_path, store, *, drop=()):
    """Land the canary's committed rows in the lake, the way the flushers do."""
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"])
    committed_at = _dt.fromisoformat(report["committed_at"])
    batch = f"{_C3RUN}-canary"
    la = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    lac = "aaaaaaaa-0000-0000-0000-aaaaaaaaaaaa"
    lb = "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    lc = "cccccccc-cccc-cccc-cccc-cccccccccccc"
    ld = "dddddddd-0000-0000-0000-dddddddddddd"

    if "silver" not in drop:
        store[_silver_key("detail", 4, "canary")] = _write_lake_silver(
            tmp_path / "lake-sd.parquet", [
                {"artifact_id": 9000001, "listing_id": la, "fetched_at": _WHEN},
                {"artifact_id": 4902401, "listing_id": lb, "fetched_at": _WHEN},
                {"artifact_id": 9000002, "listing_id": lc, "fetched_at": _WHEN},
            ])
        store[_silver_key("carousel", 4, "canary")] = _write_lake_silver(
            tmp_path / "lake-sc.parquet", [
                {"artifact_id": 9000001, "listing_id": lac, "fetched_at": _WHEN},
                {"artifact_id": 4902402, "listing_id": ld, "fetched_at": _WHEN},
            ])
    if "price" not in drop:
        store["ops_normalized/price_observation_events/year=2026/month=4/"
              "part-canary.parquet"] = _write_lake_price_events(
            tmp_path / "lake-pe.parquet", [
                {"artifact_id": 9000001, "listing_id": la,
                 "event_type": "upserted", "event_at": _WHEN},
                {"artifact_id": 4902401, "listing_id": lb,
                 "event_type": "deleted", "event_at": _WHEN},
                {"artifact_id": 9000002, "listing_id": lc,
                 "event_type": "upserted", "event_at": _WHEN},
            ])
    if "queue" not in drop:
        store[f"ops_normalized/artifacts_queue_events/year={committed_at.year}/"
              f"month={committed_at.month}/part-canary.parquet"] = \
            _write_lake_queue_events(tmp_path / "lake-qe.parquet", [
                {"artifact_id": aid, "status": "recovered", "run_id": batch,
                 "fetched_at": _WHEN}
                for aid in (9000001, 4902401, 9000002, 4902402)
            ])
    return report


def _committed_canary(mod, tmp_path, mocker):
    store = _canary_ready(mod, tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_canary_commit(mod.parse_args(
        ["canary-commit", "--run-id", _C3RUN, "--apply"] + _pin(store))) == 0
    return store


def test_the_flush_round_trip_passes_when_every_row_reached_the_lake(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    _flush_the_canary(mod, tmp_path, store)
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN, "--apply"])) == 0
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_flush_report.json"])
    assert report["passed"] is True
    tables = report["tables"]
    assert tables["staging.silver_observations"]["expected_rows"] == 5
    assert tables["staging.silver_observations"]["found_rows"] == 5
    assert tables["staging.price_observation_events"]["found_rows"] == 3
    assert tables["staging.artifacts_queue_events"]["found_rows"] == 4
    # the keys are recorded, per the plan's "verify the flushed Parquet, by
    # key, and record the keys"
    assert sorted(tables["staging.silver_observations"]["lake_keys"]) == [
        _silver_key("carousel", 4, "canary"), _silver_key("detail", 4, "canary"),
    ]
    assert all(t["missing_keys"] == 0 for t in tables.values())


def test_the_flush_verification_fails_when_the_lake_objects_are_absent(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    # committed, but no flush has run: staging still holds the rows
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN, "--apply"])) == 1
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_flush_report.json"])
    assert report["passed"] is False
    assert report["tables"]["staging.silver_observations"]["missing_keys"] == 5
    assert report["tables"]["staging.price_observation_events"]["missing_keys"] == 3
    assert report["tables"]["staging.artifacts_queue_events"]["missing_keys"] == 4
    assert report["tables"]["staging.silver_observations"]["missing_examples"]


def test_one_table_left_behind_by_the_flush_fails_the_round_trip(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    _flush_the_canary(mod, tmp_path, store, drop=("queue",))
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN])) == 1


def test_a_partly_flushed_table_fails_on_the_rows_that_did_not_arrive(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    _flush_the_canary(mod, tmp_path, store)
    # the carousel partition never landed -- 2 of the 5 silver rows are gone
    del store[_silver_key("carousel", 4, "canary")]
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN, "--apply"])) == 1
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_flush_report.json"])
    silver = report["tables"]["staging.silver_observations"]
    assert silver["found_rows"] == 3
    assert silver["missing_keys"] == 2
    # the other two tables are untouched by this failure
    assert report["tables"]["staging.price_observation_events"]["passed"] is True


def test_the_flush_check_reads_a_silver_rows_source_from_the_hive_path(
        tmp_path, mocker):
    """`source` is a partition column: it is in the key, not in the file. A
    check that keyed on a file column would match nothing."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    _flush_the_canary(mod, tmp_path, store)
    # move the carousel rows under source=detail: same rows, wrong partition
    store[_silver_key("detail", 4, "misfiled")] = \
        store.pop(_silver_key("carousel", 4, "canary"))
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN, "--apply"])) == 1
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_flush_report.json"])
    assert report["tables"]["staging.silver_observations"]["missing_keys"] == 2


def test_a_duplicate_flush_is_recorded_and_is_not_a_failure(tmp_path, mocker):
    """A flush interrupted between the Parquet write and the DELETE re-runs and
    writes the rows again; the flusher's own contract calls that acceptable."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    _flush_the_canary(mod, tmp_path, store)
    store[_silver_key("detail", 4, "canary-again")] = \
        store[_silver_key("detail", 4, "canary")]
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN, "--apply"])) == 0
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_flush_report.json"])
    assert report["tables"]["staging.silver_observations"]["duplicate_rows"] == 3


def test_the_flush_check_refuses_before_the_canary_has_committed(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _canary_ready(mod, tmp_path, mocker)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    with pytest.raises(ReconcileError, match="canary-commit --apply"):
        mod.run_canary_flush_verify(mod.parse_args(
            ["canary-flush-verify", "--run-id", _C3RUN]))


def test_the_flush_check_rebuilds_the_expectation_rather_than_trusting_the_report(
        tmp_path, mocker):
    """A verification that read its expectation out of the writer's own record
    of what it wrote would pass on a writer that recorded the wrong thing."""
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    _flush_the_canary(mod, tmp_path, store)
    key = f"recovery/plan145/canary/{_C3RUN}-canary_commit.json"
    doctored = json.loads(store[key])
    doctored["committed"]["silver"] = 1
    doctored["flush_expectation"]["staging.silver_observations"]["rows"] = 1
    store[key] = json.dumps(doctored).encode()
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN, "--apply"])) == 0
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_flush_report.json"])
    assert report["tables"]["staging.silver_observations"]["expected_rows"] == 5


def test_the_flush_check_writes_nothing_without_apply(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    _flush_the_canary(mod, tmp_path, store)
    before = set(store)
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN])) == 0
    assert set(store) == before


def test_an_unreadable_lake_object_is_reported_and_does_not_crash_the_check(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store = _committed_canary(mod, tmp_path, mocker)
    _flush_the_canary(mod, tmp_path, store)
    store[_silver_key("detail", 4, "junk")] = b"not parquet at all"
    _patch_slice2_io(mocker, store, _FakeWriteConn())

    assert mod.run_canary_flush_verify(mod.parse_args(
        ["canary-flush-verify", "--run-id", _C3RUN, "--apply"])) == 0
    report = json.loads(
        store[f"recovery/plan145/canary/{_C3RUN}-canary_flush_report.json"])
    bad = report["tables"]["staging.silver_observations"]["unreadable_objects"]
    assert [b["key"] for b in bad] == [_silver_key("detail", 4, "junk")]


# -- S: apply skips what the canary already committed ----------------------
#
# Receipts are keyed by batch name. The canary commits under `<run>-canary`
# while the same artifacts also sit in b00001-b00069, so a full apply would
# write its rows a second time and nothing downstream would notice -- which is
# what the Stage 5 gate's *no duplicate (listing_id, fetched_at)* forbids.


def _commit_report(run_id, *, manifest_key, manifest_sha256, artifacts, silver):
    return json.dumps({
        "run_id": run_id, "batch_name": f"{run_id}-canary",
        "manifest_key": manifest_key, "manifest_sha256": manifest_sha256,
        "committed_at": "2026-08-29T14:51:23.182919+00:00",
        "committed_at_source": "receipt",
        "committed": {"artifacts": artifacts, "silver": silver},
    }).encode()


def _slice2_with_canary(tmp_path, mocker, *, skip=(_MAT_KEY,)):
    """A committed canary covering `skip`, over the slice-2 fixture's objects."""
    store, ids = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    mod = __import__("scripts.oneoff.reconcile_april_detail", fromlist=["x"])
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    manifest_key = f"recovery/plan145/canary/{_RUN}-canary_sample.parquet"
    rows = [{"run_id": _RUN, "object_key": k, "artifact_id": 1, "id_source": "x",
             "input_kind": "materialized", "batch_name": f"{_RUN}-b00001",
             "page_listing_id": None, "page_fetched_at": _WHEN,
             "silver_rows": 1, "detail_rows": 1, "strata": ["s"],
             "write_set_digest": "d" * 64, "vin_snapshot_sha256": "v" * 64,
             "source_manifest_sha256": None, "source_object_set_digest": None}
            for k in skip]
    store[manifest_key] = _write_canary_manifest(tmp_path / "cm.parquet", rows)
    digest = hashlib.sha256(store[manifest_key]).hexdigest()
    store[f"recovery/plan145/canary/{_RUN}-canary_commit.json"] = _commit_report(
        _RUN, manifest_key=manifest_key, manifest_sha256=digest,
        artifacts=len(skip), silver=2)
    return store, ids, digest


def test_apply_skips_the_artifacts_the_canary_already_committed(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, (l1, l2, l3, l4), digest = _slice2_with_canary(tmp_path, mocker)
    conn = _FakeWriteConn(receipts={f"{_RUN}-canary": [digest]})
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)

    assert mod.run_apply(mod.parse_args(
        ["apply", "--apply", "--maintainer-approval", "tester"])) == 0

    inserts = {}
    for sql, rows in conn.executed_values:
        for name in ("silver_observations", "price_observation_events",
                     "artifacts_queue_events"):
            if name in sql:
                inserts[name] = rows

    from processing.writers.silver_writer import _POSTGRES_COLS

    silver = [dict(zip(_POSTGRES_COLS, r)) for r in inserts["silver_observations"]]
    # the materialized object carried 2 of the fixture's 4 silver rows; the
    # canary committed it, so neither listing appears here
    assert len(silver) == 2
    assert {s["listing_id"] for s in silver} == {l3, l4}
    assert len(inserts["artifacts_queue_events"]) == 2      # 3 artifacts - 1


def test_apply_reports_the_skip_in_its_blast_radius(tmp_path,
                                                    capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _slice2_with_canary(tmp_path, mocker)
    _patch_slice2_io(mocker, store, _FakeWriteConn())
    assert mod.run_apply(mod.parse_args(["apply"])) == 0
    out = capsys.readouterr().out
    assert "canary already wrote" in out
    assert re.search(r"^artifacts +2$", out, re.M)          # 3 - 1
    # a dry run opens no connection, so it says the receipt is unverified
    assert "receipt NOT confirmed" in out


def test_the_apply_dry_run_still_opens_no_connection(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _slice2_with_canary(tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_apply(mod.parse_args(["apply"])) == 0
    assert conn.sql == [] and conn.executed_values == []


def test_the_budget_gate_still_refuses_before_any_statement(tmp_path, mocker):
    """The exclusion is computed connection-free precisely so the gate keeps
    this property: nothing is issued until the row budget has been cleared."""
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _slice2_with_canary(tmp_path, mocker)
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="canary budget"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--max-unapproved-rows", "1"]))
    assert conn.sql == []


def test_a_commit_report_with_no_receipt_stops_the_apply(tmp_path, mocker):
    """The canary was rolled back and its report left behind. Excluding would
    silently drop those artifacts from the import."""
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _slice2_with_canary(tmp_path, mocker)
    conn = _FakeWriteConn()                       # no receipt for the canary
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="no matching receipt"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--maintainer-approval", "tester"]))
    assert conn.executed_values == []
    assert conn.commits == 0


def test_a_receipt_with_no_commit_report_stops_the_apply(tmp_path, mocker):
    """The mirror failure: rows were committed that this run cannot identify,
    so it cannot avoid writing them twice."""
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, digest = _slice2_with_canary(tmp_path, mocker)
    del store[f"recovery/plan145/canary/{_RUN}-canary_commit.json"]
    conn = _FakeWriteConn(receipts={f"{_RUN}-canary": [digest]})
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="cannot avoid writing them twice"):
        mod.run_apply(mod.parse_args(
            ["apply", "--apply", "--maintainer-approval", "tester"]))
    assert conn.executed_values == []


def test_a_manifest_that_moved_under_the_commit_report_stops_the_apply(
        tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, digest = _slice2_with_canary(tmp_path, mocker)
    store[f"recovery/plan145/canary/{_RUN}-canary_sample.parquet"] += b"tamper"
    conn = _FakeWriteConn(receipts={f"{_RUN}-canary": [digest]})
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    with pytest.raises(ReconcileError, match="no longer describes what the canary"):
        mod.run_apply(mod.parse_args(["apply"]))
    assert conn.sql == []


def test_no_canary_means_no_exclusion_and_no_extra_statement(tmp_path, mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _ = _slice2_fixture_store(tmp_path)
    _patch_slice2_io(mocker, store, _FakeWriteConn(next_id=9_000_001))
    mod.run_assign(mod.parse_args(["assign", "--apply"]))

    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, store, conn)
    assert mod.run_apply(mod.parse_args(
        ["apply", "--apply", "--maintainer-approval", "tester"])) == 0
    # all 3 artifacts and all 4 silver rows, exactly as before the exclusion
    silver = next(r for sql, r in conn.executed_values
                  if "silver_observations" in sql)
    assert len(silver) == 4
    assert conn.commits == 1


def test_a_probe_apply_ignores_the_canary_entirely(tmp_path, mocker):
    """A probe commits nothing, so it cannot duplicate the canary and must not
    spend a statement checking."""
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, digest = _slice2_with_canary(tmp_path, mocker)
    probe_store = {k.replace("recovery/plan145/compared/",
                             "recovery/plan145/compared_probe/")
                    .replace("recovery/plan145/assigned/",
                             "recovery/plan145/assigned_probe/")
                    .replace("recovery/plan145/vin_snapshot/",
                             "recovery/plan145/vin_snapshot_probe/")
                    .replace("recovery/plan145/inventory/",
                             "recovery/plan145/inventory_probe/"): v
                   for k, v in store.items()}
    conn = _FakeWriteConn()
    _record_execute_values(mocker, conn)
    _patch_slice2_io(mocker, probe_store, conn)
    assert mod.run_apply(mod.parse_args(
        ["apply", "--probe", "--apply", "--run-id", _RUN, "--batch",
         assign_batch_name(_RUN, 1)])) == 0
    silver = next(r for sql, r in conn.executed_values
                  if "silver_observations" in sql)
    assert len(silver) == 4                       # nothing skipped
    assert conn.rollbacks == 1 and conn.commits == 0


# ---------------------------------------------------------------------------
# M - Stage 6: the ordering trial, the replacement proof, retirement, deletion
# ---------------------------------------------------------------------------

import dataclasses  # noqa: E402

from scripts.oneoff.reconcile_april_detail import (  # noqa: E402
    DEDUPE_PREFIX,
    LEGACY_DELETE_PREFIX,
    REPACK_PREFIX,
    RETIRE_PREFIX,
    UNPACK_PREFIX,
)

_S6_PACK_PREFIX = "html_packs/detail_page/2026/04/"
_S6_POP_PREFIX = "html/year=2026/month=4/artifact_type=detail_page/"


def _s6_row(source_key, artifact_id, listing_id, cluster_key, day=1):
    """One row in the packer's five-column member metadata shape."""
    from datetime import datetime, timezone

    return (
        source_key, artifact_id, listing_id, cluster_key,
        datetime(2026, 4, day, 12, 0, tzinfo=timezone.utc),
    )


class _FakeS6Store(_FakeS3Store):
    """The shared fake, plus what Stage 6 needs: deletes and head_object."""

    def __init__(self, store):
        super().__init__(store)
        self.deleted: list[str] = []
        self.refuse: set[str] = set()

    def list_objects_v2(self, **kw):
        from datetime import datetime, timezone

        page = super().list_objects_v2(**kw)
        for entry in page.get("Contents", []):
            entry["LastModified"] = datetime(2026, 4, 1, tzinfo=timezone.utc)
        return page

    def head_object(self, Bucket, Key):
        return {"ContentLength": len(self.store[Key])}

    def get_object(self, Bucket, Key, Range=None):
        body = self.store[Key]
        if Range:
            first, last = Range.removeprefix("bytes=").split("-")
            body = body[int(first): int(last) + 1]
        return {"Body": _Body(body)}

    def delete_objects(self, Bucket, Delete):
        deleted, errors = [], []
        for obj in Delete["Objects"]:
            key = obj["Key"]
            if key in self.refuse:
                errors.append({"Key": key, "Code": "AccessDenied"})
            elif key in self.store:
                del self.store[key]
                self.deleted.append(key)
                deleted.append({"Key": key})
            else:
                pass  # absent: neither deleted nor an error, as S3 reports it
        return {"Deleted": deleted, "Errors": errors}


def _s6_index_bytes(entries):
    from shared.packfile import write_index_parquet

    return write_index_parquet(entries)


def _s6_entry(source_key, sha, *, artifact_id=None, listing_id=None, length=10):
    from shared.packfile import PackIndexEntry

    return PackIndexEntry(
        source_key=source_key,
        frame_ordinal=0,
        offset_in_frame=0,
        length=length,
        raw_sha256=sha,
        artifact_id=artifact_id,
        listing_id=listing_id,
        fetched_at=None,
    )


def _s6_sha(text: str) -> str:
    return hashlib.sha256(_s6_body(text)).hexdigest()


def _s6_body(name: str) -> bytes:
    """The uncompressed body of one fixture member, keyed by its short name."""
    return f"<html><body>{name}</body></html>".encode()


# --- the ordering trial ----------------------------------------------------

def test_trial_orders_by_the_arm_under_test():
    from scripts.oneoff.reconcile_april_detail import order_for_arm

    rows = [
        _s6_row("k1", 1, "LB", "CA", day=1),
        _s6_row("k2", 2, "LA", "CB", day=2),
        _s6_row("k3", 3, "LA", "CA", day=3),
    ]

    assert [r[0] for r in order_for_arm(rows, "current")] == ["k1", "k3", "k2"]
    assert [r[0] for r in order_for_arm(rows, "true")] == ["k2", "k3", "k1"]


def test_trial_refuses_an_unknown_arm():
    from scripts.oneoff.reconcile_april_detail import ReconcileError, order_for_arm

    with pytest.raises(ReconcileError, match="unknown trial arm"):
        order_for_arm([], "whatever")


def test_a_member_with_no_subject_listing_is_left_out_of_the_trial():
    """In the true arm they would collapse into one enormous false cluster,
    and a member with no subject listing cannot inform a question about
    ordering by subject listing."""
    from scripts.oneoff.reconcile_april_detail import select_trial_sample

    rows = [
        _s6_row("k1", 1, "LA", "CA"),
        _s6_row("k2", 2, None, "CB"),
        _s6_row("k3", 3, "LC", None),
    ]

    kept = select_trial_sample(rows, size=10, drawn_in="current")
    assert [r[0] for r in kept] == ["k1"]

    with_nulls = select_trial_sample(
        rows, size=10, drawn_in="current", include_null_identity=True,
    )
    assert len(with_nulls) == 3


def test_the_trial_sample_is_contiguous_in_the_order_it_is_drawn_in():
    from scripts.oneoff.reconcile_april_detail import select_trial_sample

    rows = [_s6_row(f"k{i}", i, f"L{9 - i}", f"C{i}") for i in range(6)]

    current = select_trial_sample(rows, size=3, drawn_in="current")
    assert [r[3] for r in current] == ["C0", "C1", "C2"]

    true = select_trial_sample(rows, size=3, drawn_in="true")
    assert [r[2] for r in true] == ["L4", "L5", "L6"]


def test_both_arms_pack_exactly_the_same_members():
    """The whole point of a fixed population: only the order may differ."""
    from scripts.oneoff.reconcile_april_detail import pack_trial_arm

    rows = [_s6_row(f"k{i}", i, f"L{i % 3}", f"C{i % 2}") for i in range(6)]

    def fetch(key):
        return f"<html>{key}</html>".encode() * 40

    current = pack_trial_arm(
        rows, fetch, arm="current", dict_id=None,
        frame_target_bytes=1 << 20, max_pack_bytes=1 << 24,
    )
    true = pack_trial_arm(
        rows, fetch, arm="true", dict_id=None,
        frame_target_bytes=1 << 20, max_pack_bytes=1 << 24,
    )

    assert current["members"] == true["members"] == 6
    assert current["raw_bytes"] == true["raw_bytes"]


def test_the_trial_arm_decides_which_key_frames_are_cut_on():
    """Identity and placement are separable since Stage 5b; the trial is the
    caller that makes them differ on purpose."""
    from scripts.oneoff.reconcile_april_detail import pack_trial_arm
    from shared.packfile import PackWriter

    seen: list[str | None] = []
    real_add = PackWriter.add

    def spy(self, member):
        seen.append(member.placement_key())
        return real_add(self, member)

    rows = [_s6_row("k1", 1, "LA", "CX"), _s6_row("k2", 2, "LB", "CX")]

    def fetch(key):
        return b"<html>body</html>"

    with pytest.MonkeyPatch.context() as mp:
        mp.setattr(PackWriter, "add", spy)
        pack_trial_arm(rows, fetch, arm="current", dict_id=None,
                       frame_target_bytes=1 << 20, max_pack_bytes=1 << 24)
        assert seen == ["CX", "CX"]

        seen.clear()
        pack_trial_arm(rows, fetch, arm="true", dict_id=None,
                       frame_target_bytes=1 << 20, max_pack_bytes=1 << 24)
        assert seen == ["LA", "LB"]


def _trial_sample(drawn_in, current_bytes, true_bytes):
    return {
        "drawn_in": drawn_in,
        "arms": {
            "current": {"pack_bytes": current_bytes},
            "true": {"pack_bytes": true_bytes},
        },
    }


def test_true_ordering_carries_only_when_it_wins_on_every_sample():
    from scripts.oneoff.reconcile_april_detail import decide_trial_winner

    decision = decide_trial_winner([
        _trial_sample("current", 1000, 900),
        _trial_sample("true", 1000, 800),
    ])
    assert decision["winner"] == "true"
    assert decision["unanimous"] is True
    assert decision["split"] is False


def test_a_split_verdict_leaves_the_incumbent_in_place():
    from scripts.oneoff.reconcile_april_detail import decide_trial_winner

    decision = decide_trial_winner([
        _trial_sample("current", 1000, 1100),
        _trial_sample("true", 1000, 900),
    ])
    assert decision["winner"] == "current"
    assert decision["split"] is True


def test_the_trial_reports_the_size_of_the_difference_not_only_its_sign():
    from scripts.oneoff.reconcile_april_detail import decide_trial_winner

    decision = decide_trial_winner([_trial_sample("current", 1000, 800)])
    verdict = decision["per_sample"][0]
    assert verdict["delta_bytes"] == -200
    assert verdict["delta_share"] == pytest.approx(-0.2)


def test_a_trial_run_id_is_reproducible_from_the_member_set():
    from scripts.oneoff.reconcile_april_detail import _trial_run_id

    a = [{"drawn_in": "current", "source_keys": ["k1", "k2"]}]
    b = [{"drawn_in": "current", "source_keys": ["k1", "k2"]}]
    c = [{"drawn_in": "current", "source_keys": ["k1", "k3"]}]

    assert _trial_run_id(a) == _trial_run_id(b)
    assert _trial_run_id(a) != _trial_run_id(c)


def test_the_trial_defaults_to_both_samples_and_a_dry_run():
    args = parse_args(["pack-trial"])
    assert args.sample == "both"
    assert args.apply is False
    assert args.include_null_identity is False


# --- the replacement proof -------------------------------------------------

def _s6_new_member(sha, *, artifact_id=1, listing_id="L", claims=1,
                   sidecar="sc-a"):
    return {
        "raw_sha256": sha, "artifact_id": artifact_id,
        "listing_id": listing_id, "sidecar_key": sidecar, "claims": claims,
    }


def test_replacement_coverage_passes_when_everything_is_carried_over():
    from scripts.oneoff.reconcile_april_detail import check_replacement_coverage

    baseline = {"old1": "sha1", "old2": "sha2"}
    population = ["old1", "old2", "mat1"]
    new = {
        "old1": _s6_new_member("sha1"),
        "old2": _s6_new_member("sha2"),
        "mat1": _s6_new_member("sha3"),
    }

    result = check_replacement_coverage(baseline, population, new)
    assert result["passed"] is True
    assert result["new_members"] == 3


def test_an_old_member_no_replacement_holds_is_a_stop():
    from scripts.oneoff.reconcile_april_detail import check_replacement_coverage

    result = check_replacement_coverage(
        {"old1": "sha1", "old2": "sha2"}, ["old1", "old2"],
        {"old1": _s6_new_member("sha1")},
    )
    assert result["passed"] is False
    assert result["missing_old"] == 1
    assert result["examples"]["missing_old"] == ["old2"]


def test_an_old_member_whose_bytes_changed_is_a_stop():
    """The originals are deleted immediately after this passes, so a hash that
    moved is the one thing that can never be reported as a warning."""
    from scripts.oneoff.reconcile_april_detail import check_replacement_coverage

    result = check_replacement_coverage(
        {"old1": "sha1"}, ["old1"], {"old1": _s6_new_member("DIFFERENT")},
    )
    assert result["passed"] is False
    assert result["changed_old"] == 1
    assert result["examples"]["changed_old"][0]["was"] == "sha1"


def test_a_live_object_no_replacement_holds_is_a_stop():
    from scripts.oneoff.reconcile_april_detail import check_replacement_coverage

    result = check_replacement_coverage(
        {}, ["mat1", "mat2"], {"mat1": _s6_new_member("sha1")},
    )
    assert result["passed"] is False
    assert result["population_not_packed"] == 1


def test_a_member_claimed_by_two_replacement_packs_is_a_stop():
    from scripts.oneoff.reconcile_april_detail import check_replacement_coverage

    result = check_replacement_coverage(
        {"old1": "sha1"}, ["old1"], {"old1": _s6_new_member("sha1", claims=2)},
    )
    assert result["passed"] is False
    assert result["duplicated_members"] == 1


def test_identity_is_decomposed_by_where_the_member_came_from():
    """Stage 6's gate names 42,276, which is a property of the 557,065-member
    pack population. The replacement packs hold the flattened 983,043, so the
    verifier reports the decomposition instead of asserting that number."""
    from scripts.oneoff.reconcile_april_detail import describe_identity

    baseline = {"old1": "sha1", "old2": "sha2"}
    new = {
        "old1": _s6_new_member("sha1", artifact_id=7, listing_id="L1"),
        "old2": _s6_new_member("sha2", artifact_id=None, listing_id=None),
        "mat1": _s6_new_member("sha3", artifact_id=9, listing_id="L2"),
        "mat2": _s6_new_member("sha4", artifact_id=None, listing_id=None),
        "mat3": _s6_new_member("sha5", artifact_id=None, listing_id=None),
    }

    identity = describe_identity(baseline, new)

    assert identity["members"] == 5
    assert identity["null_listing_id"] == 3
    assert identity["by_origin"]["old_pack_member"]["members"] == 2
    assert identity["by_origin"]["old_pack_member"]["attributed"] == 1
    assert identity["by_origin"]["materialized"]["members"] == 3
    assert identity["by_origin"]["materialized"]["null_listing_id"] == 2


def test_a_replacement_sidecar_that_repeats_the_scrambled_column_is_caught():
    """April's old sidecar was correct for 31.4% of members, so near-total
    agreement means the run wrote the historical value again."""
    from scripts.oneoff.reconcile_april_detail import compare_identity_to_the_old_sidecars

    old = {f"k{i}": _s6_new_member("s", listing_id=f"L{i}") for i in range(10)}
    unchanged = {f"k{i}": _s6_new_member("s", listing_id=f"L{i}") for i in range(10)}
    assert compare_identity_to_the_old_sidecars(old, unchanged)["changed_share"] == 0.0

    corrected = {
        **{f"k{i}": _s6_new_member("s", listing_id=f"SUBJECT{i}") for i in range(7)},
        **{f"k{i}": _s6_new_member("s", listing_id=None) for i in range(7, 10)},
    }
    change = compare_identity_to_the_old_sidecars(old, corrected)
    assert change["same"] == 0
    assert change["differs"] == 7
    assert change["null_now"] == 3
    assert change["changed_share"] == 1.0


def test_sidecars_are_split_against_the_frozen_old_pack_set():
    from scripts.oneoff.reconcile_april_detail import split_sidecars

    old_packs = {f"{_S6_PACK_PREFIX}pack-00000.zpack"}
    old, new = split_sidecars(
        [f"{_S6_PACK_PREFIX}pack-00001.idx.parquet",
         f"{_S6_PACK_PREFIX}pack-00000.idx.parquet"],
        old_packs,
    )
    assert old == [f"{_S6_PACK_PREFIX}pack-00000.idx.parquet"]
    assert new == [f"{_S6_PACK_PREFIX}pack-00001.idx.parquet"]


def test_the_read_back_sample_is_stratified_over_the_replacement_packs():
    from scripts.oneoff.reconcile_april_detail import _sample_members_for_readback

    members = {}
    for sidecar in ("sc-a", "sc-b", "sc-c"):
        for i in range(50):
            members[f"{sidecar}-k{i}"] = _s6_new_member("s", sidecar=sidecar)

    picked = _sample_members_for_readback(members, size=9, seed=145)

    assert set(picked) == {"sc-a", "sc-b", "sc-c"}
    assert sum(len(v) for v in picked.values()) == 9
    assert _sample_members_for_readback(members, size=9, seed=145) == picked


def test_a_baseline_that_names_no_pack_refuses_rather_than_verifying_nothing(
    mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    mocker.patch.object(mod, "_s3_client", lambda: _FakeS6Store({}))
    with pytest.raises(ReconcileError, match="no Stage 3b unpack manifests"):
        mod.load_unpack_baseline(_FakeS6Store({}), "bronze")


# --- retiring the superseded packs -----------------------------------------

def test_retirement_plans_one_pack_and_one_sidecar_per_retired_pack():
    from scripts.oneoff.reconcile_april_detail import plan_pack_retirement

    planned = plan_pack_retirement([
        f"{_S6_PACK_PREFIX}pack-00001.zpack",
        f"{_S6_PACK_PREFIX}pack-00000.zpack",
    ])

    assert [p["object_key"] for p in planned] == [
        f"{_S6_PACK_PREFIX}pack-00000.zpack",
        f"{_S6_PACK_PREFIX}pack-00000.idx.parquet",
        f"{_S6_PACK_PREFIX}pack-00001.zpack",
        f"{_S6_PACK_PREFIX}pack-00001.idx.parquet",
    ]
    assert [p["kind"] for p in planned] == ["pack", "sidecar", "pack", "sidecar"]


def test_a_key_outside_the_frozen_pack_set_is_refused_by_the_guard():
    from scripts.oneoff.reconcile_april_detail import (
        delete_objects_in_batches,
        plan_pack_retirement,
    )

    frozen = {p["object_key"] for p in plan_pack_retirement(
        [f"{_S6_PACK_PREFIX}pack-00000.zpack"]
    )}

    def guard(record):
        if record["object_key"] in frozen:
            return None
        return "it is not in the frozen Stage 3b pack set"

    with pytest.raises(ReconcileError, match="frozen Stage 3b pack set"):
        delete_objects_in_batches(
            None, "bronze",
            [{"object_key": f"{_S6_PACK_PREFIX}pack-00099.zpack"}],
            apply=True, batch_size=10, guard=guard,
        )


def test_retiring_without_a_verification_report_is_refused(monkeypatch):
    import scripts.oneoff.reconcile_april_detail as mod

    store = {f"{UNPACK_PREFIX}/pack-00000.parquet": b""}
    with pytest.raises(ReconcileError, match="nothing may be retired"):
        mod._load_verify_report(_FakeS6Store(store), "bronze", None)


def test_retiring_on_a_failed_verification_report_is_refused():
    import scripts.oneoff.reconcile_april_detail as mod

    key = f"{REPACK_PREFIX}/repack-abc/verify_report.json"
    store = {key: json.dumps(
        {"passed": False, "refusals": ["3 old members are not replaced"]}
    ).encode()}

    with pytest.raises(ReconcileError, match="did not pass"):
        mod._load_verify_report(_FakeS6Store(store), "bronze", None)


def test_two_verification_reports_must_be_disambiguated_by_name():
    import scripts.oneoff.reconcile_april_detail as mod

    store = {
        f"{REPACK_PREFIX}/repack-a/verify_report.json": b"{}",
        f"{REPACK_PREFIX}/repack-b/verify_report.json": b"{}",
    }
    with pytest.raises(ReconcileError, match="--verify-run-id"):
        mod._load_verify_report(_FakeS6Store(store), "bronze", None)


# --- deleting the legacy Parquet -------------------------------------------

def _legacy_object(key, size=1000):
    return {"legacy_object_key": key, "size_bytes": size, "etag": "e"}


def _legacy_coverage(hashes, skipped=0):
    return {"hashes": set(hashes), "rows": len(hashes) + skipped,
            "skipped": skipped}


def test_a_legacy_object_is_deletable_when_every_body_is_in_a_replacement_pack():
    from scripts.oneoff.reconcile_april_detail import plan_legacy_deletions

    key = f"{_S6_POP_PREFIX}part-0.parquet"
    planned, refusals = plan_legacy_deletions(
        [_legacy_object(key)],
        {key: _legacy_coverage(["sha1", "sha2"], skipped=3)},
        {"sha1": 1, "sha2": 1},
        approved_by="the maintainer",
    )

    assert refusals == []
    assert len(planned) == 1
    assert planned[0]["recoverable_rows"] == 2
    assert planned[0]["approved_by"] == "the maintainer"


def test_a_legacy_object_with_an_uncovered_body_is_refused_not_skipped():
    """A partially recoverable legacy object is exactly the case where
    deleting loses something."""
    from scripts.oneoff.reconcile_april_detail import plan_legacy_deletions

    key = f"{_S6_POP_PREFIX}part-0.parquet"
    planned, refusals = plan_legacy_deletions(
        [_legacy_object(key)],
        {key: _legacy_coverage(["sha1", "missing"])},
        {"sha1": 1},
        approved_by="x",
    )

    assert planned == []
    assert len(refusals) == 1
    assert "in no replacement pack" in refusals[0]


def test_a_results_page_key_is_refused_by_key():
    """The 127 results-page objects are out of scope for the whole plan."""
    from scripts.oneoff.reconcile_april_detail import plan_legacy_deletions

    key = "html/year=2026/month=4/artifact_type=results_page/part-0.parquet"
    planned, refusals = plan_legacy_deletions(
        [_legacy_object(key)],
        {key: _legacy_coverage(["sha1"])},
        {"sha1": 1},
        approved_by="x",
    )

    assert planned == []
    assert "out of scope" in refusals[0]


def test_a_legacy_object_no_stage_2_manifest_describes_is_refused():
    from scripts.oneoff.reconcile_april_detail import plan_legacy_deletions

    planned, refusals = plan_legacy_deletions(
        [_legacy_object(f"{_S6_POP_PREFIX}part-0.parquet")],
        {}, {}, approved_by="x",
    )

    assert planned == []
    assert "no Stage 2 manifest" in refusals[0]


def test_an_object_whose_rows_were_all_empty_or_blocked_needs_no_coverage():
    """43,014 empty and 101,010 non-success rows produced no body, so they can
    never need covering."""
    from scripts.oneoff.reconcile_april_detail import plan_legacy_deletions

    key = f"{_S6_POP_PREFIX}part-0.parquet"
    planned, refusals = plan_legacy_deletions(
        [_legacy_object(key)], {key: _legacy_coverage([], skipped=812)}, {},
        approved_by="x",
    )

    assert refusals == []
    assert planned[0]["recoverable_rows"] == 0


def test_the_legacy_delete_guard_refuses_anything_off_the_manifest():
    from scripts.oneoff.reconcile_april_detail import delete_objects_in_batches

    planned_keys = {f"{_S6_POP_PREFIX}part-0.parquet"}

    def guard(record):
        key = record.get("object_key")
        if key not in planned_keys:
            return "it is not in the reviewed deletion manifest"
        return None

    with pytest.raises(ReconcileError, match="reviewed deletion manifest"):
        delete_objects_in_batches(
            None, "bronze", [{"object_key": f"{_S6_POP_PREFIX}other.parquet"}],
            apply=True, batch_size=10, guard=guard,
        )


def test_deleting_without_named_approval_is_refused(mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    args = parse_args(["delete-legacy", "--apply"])
    mocker.patch.object(mod, "_s3_client", lambda: _FakeS6Store({}))

    with pytest.raises(ReconcileError, match="maintainer-approval"):
        mod.run_delete_legacy(args)


def test_delete_legacy_needs_a_census_or_an_explicit_fallback():
    args = parse_args(["delete-legacy"])
    assert args.census_dir is None
    assert args.census_from_manifests is False
    assert args.allow_partial is False
    assert args.apply is False


def test_an_edited_stage_1_census_is_refused(tmp_path):
    import scripts.oneoff.reconcile_april_detail as mod

    census = tmp_path / "object_census.csv"
    census.write_text("legacy_object_key\nkey-a\n", encoding="utf-8")
    (tmp_path / "stage1_report.json").write_text(
        json.dumps({"fingerprints": {"object_census.csv": "0" * 64}}),
        encoding="utf-8",
    )

    with pytest.raises(ReconcileError, match="has been edited"):
        mod._read_census_keys(tmp_path)


def test_the_frozen_census_key_set_is_read_back_verbatim(tmp_path):
    import scripts.oneoff.reconcile_april_detail as mod

    census = tmp_path / "object_census.csv"
    census.write_text(
        "legacy_object_key,size_bytes\nkey-a,1\nkey-b,2\n", encoding="utf-8",
    )
    (tmp_path / "stage1_report.json").write_text(
        json.dumps({"fingerprints": {
            "object_census.csv": mod._fingerprint(census),
        }}),
        encoding="utf-8",
    )

    assert mod._read_census_keys(tmp_path) == {"key-a", "key-b"}


# --- end to end, through the real store shape ------------------------------

def _s6_write_parquet(rows):
    import io

    import pyarrow as pa
    import pyarrow.parquet as pq

    buf = io.BytesIO()
    pq.write_table(pa.Table.from_pylist(rows), buf)
    return buf.getvalue()


#: part-0 held old-a's body; part-1 held mat-c's. Both legacy objects also
#: held rows that produced nothing -- empty and non-success -- which is the
#: ordinary case and can never need covering.
_S6_LEGACY_KEYS = [f"{_S6_POP_PREFIX}part-{i}.parquet" for i in range(2)]


def _s6_materialize_rows(*, uncovered=False):
    return {
        _S6_LEGACY_KEYS[0]: [
            {"legacy_object_key": _S6_LEGACY_KEYS[0], "disposition": "written",
             "object_key": f"{_S6_POP_PREFIX}old-a.html.zst",
             "raw_sha256": _s6_sha("old-a")},
            {"legacy_object_key": _S6_LEGACY_KEYS[0],
             "disposition": "skipped_empty",
             "object_key": None, "raw_sha256": None},
        ],
        _S6_LEGACY_KEYS[1]: [
            {"legacy_object_key": _S6_LEGACY_KEYS[1], "disposition": "exists",
             "object_key": f"{_S6_POP_PREFIX}mat-c.html.zst",
             "raw_sha256": _s6_sha("zzz" if uncovered else "mat-c")},
        ],
    }


def _s6_seed_store(*, uncovered=False):
    """Two old packs unpacked into loose objects, plus one materialized
    survivor, replaced by a single new pack that holds all three.

    Stage 3a deleted old-a's materialized twin, which is why the materialize
    manifest names it and the population does not count it twice.
    """
    store: dict[str, bytes] = {}
    old_packs = [f"{_S6_PACK_PREFIX}pack-{i:05d}.zpack" for i in range(2)]

    members = [
        ("old-a", _s6_sha("old-a"), old_packs[0]),
        ("old-b", _s6_sha("old-b"), old_packs[1]),
    ]

    # Stage 3b's manifests: the frozen old-pack set.
    for pack in old_packs:
        rows = [
            {"source_key": f"{_S6_POP_PREFIX}{name}.html.zst",
             "raw_sha256": sha, "pack_key": pack, "disposition": "written"}
            for name, sha, owner in members if owner == pack
        ]
        stem = pack.rsplit("/", 1)[-1].replace(".zpack", "")
        store[f"{UNPACK_PREFIX}/{stem}.parquet"] = _s6_write_parquet(rows)

    # Stage 2's manifests, and Stage 3a's record of the twin it deleted.
    for i, rows in enumerate(_s6_materialize_rows(uncovered=uncovered).values()):
        store[f"{MATERIALIZE_PREFIX}/part-{i}.parquet"] = _s6_write_parquet(rows)
    store[f"{DEDUPE_PREFIX}/part-0.parquet"] = _s6_write_parquet(
        [{"object_key": f"{_S6_POP_PREFIX}old-a.html.zst",
          "raw_sha256": _s6_sha("old-a")}]
    )

    # The old sidecars, carrying the scrambled column.
    for i, pack in enumerate(old_packs):
        name, sha, _ = members[i]
        store[pack] = b"old-pack-bytes"
        store[pack.replace(".zpack", ".idx.parquet")] = _s6_index_bytes([
            _s6_entry(f"{_S6_POP_PREFIX}{name}.html.zst", sha,
                      artifact_id=100 + i, listing_id=f"SCRAMBLED{i}"),
        ])

    # The replacement pack is a *real* pack, so the verifier's read-back
    # exercises the real reader over ranged GETs rather than a stub. Under
    # *uncovered* it still holds the real body -- it is the Stage 2 manifest
    # that names a hash no pack holds, which is the shape that must stop the
    # deletion, since that body would exist nowhere else.
    from shared.packfile import PackMember, PackWriter, write_index_parquet

    writer = PackWriter(dict_id=None, frame_target_bytes=1 << 20)
    for name, artifact_id, listing in (
        ("old-a", 100, "SUBJECT0"), ("old-b", 101, "SUBJECT1"),
        ("mat-c", None, None),
    ):
        writer.add(PackMember(
            source_key=f"{_S6_POP_PREFIX}{name}.html.zst",
            content=_s6_body(name),
            artifact_id=artifact_id,
            listing_id=listing,
        ))
    pack = writer.finish()

    new_pack = f"{_S6_PACK_PREFIX}pack-00002.zpack"
    store[new_pack] = pack.data
    store[new_pack.replace(".zpack", ".idx.parquet")] = write_index_parquet(
        pack.entries
    )

    for name in ("old-a", "old-b", "mat-c"):
        store[f"{_S6_POP_PREFIX}{name}.html.zst"] = b"loose-object"

    return store, old_packs, new_pack


def _patch_verify_io(mocker, store, *, population=3):
    import scripts.oneoff.reconcile_april_detail as mod

    fake = _FakeS6Store(store)
    mocker.patch.object(mod, "_s3_client", lambda: fake)
    mocker.patch.object(mod, "EXPECTED_UNPACK_SHARDS", 2)
    mocker.patch.object(mod, "EXPECTED_FLATTENED_INPUTS", population)
    return fake


def test_repack_verify_passes_on_a_complete_replacement(capsys, mocker):
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.minio as minio

    store, old_packs, _ = _s6_seed_store()
    written: dict[str, object] = {}

    _patch_verify_io(mocker, store)
    mocker.patch.object(minio, "write_json", lambda k, o: written.__setitem__(k, o))
    mocker.patch.object(
        minio, "read_packed_html",
        lambda path: {"old-a": b"a", "old-b": b"b", "mat-c": b"c"}[
            path.rsplit("/", 1)[-1].replace(".html.zst", "")
        ],
    )

    args = parse_args([
        "repack-verify", "--pack-prefix", _S6_PACK_PREFIX, "--verify-sample", "3",
    ])
    assert mod.run_repack_verify(args) == 0

    report = next(iter(written.values()))
    assert report["passed"] is True
    assert report["coverage"]["baseline_members"] == 2
    assert report["coverage"]["population_objects"] == 3
    assert report["readback"]["mismatched"] == 0
    assert report["identity_change"]["differs"] == 2
    assert report["identity"]["by_origin"]["materialized"]["null_listing_id"] == 1


def test_repack_verify_fails_and_exits_non_zero_when_a_member_is_dropped(
    mocker):
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.minio as minio

    store, _, new_pack = _s6_seed_store()
    # The replacement forgets the materialized survivor.
    store[new_pack.replace(".zpack", ".idx.parquet")] = _s6_index_bytes([
        _s6_entry(f"{_S6_POP_PREFIX}old-a.html.zst", _s6_sha("a"),
                  artifact_id=100, listing_id="SUBJECT0"),
        _s6_entry(f"{_S6_POP_PREFIX}old-b.html.zst", _s6_sha("b"),
                  artifact_id=101, listing_id="SUBJECT1"),
    ])

    written: dict[str, object] = {}
    _patch_verify_io(mocker, store)
    mocker.patch.object(minio, "write_json", lambda k, o: written.__setitem__(k, o))
    mocker.patch.object(minio, "read_packed_html", lambda path: b"a")

    args = parse_args([
        "repack-verify", "--pack-prefix", _S6_PACK_PREFIX, "--verify-sample", "0",
    ])
    assert mod.run_repack_verify(args) == 1

    report = next(iter(written.values()))
    assert report["passed"] is False
    assert report["coverage"]["population_not_packed"] == 1


def test_retire_packs_deletes_exactly_the_frozen_set_and_nothing_else(
    mocker):
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.minio as minio

    store, old_packs, new_pack = _s6_seed_store()
    fake = _FakeS6Store(store)
    mocker.patch.object(mod, "_s3_client", lambda: fake)
    mocker.patch.object(
        minio, "write_bytes",
        lambda k, data, content_type=None: store.__setitem__(k, bytes(data)),
    )
    store[f"{REPACK_PREFIX}/repack-x/verify_report.json"] = json.dumps({
        "run_id": "repack-x",
        "passed": True,
        "old_pack_keys": sorted(old_packs),
        "new_sidecar_keys": [new_pack.replace(".zpack", ".idx.parquet")],
    }).encode()

    args = parse_args([
        "retire-packs", "--apply", "--pack-prefix", _S6_PACK_PREFIX,
    ])
    assert mod.run_retire_packs(args) == 0

    assert sorted(fake.deleted) == sorted(
        old_packs + [p.replace(".zpack", ".idx.parquet") for p in old_packs]
    )
    assert new_pack in store
    assert f"{_S6_POP_PREFIX}old-a.html.zst" in store
    assert f"{RETIRE_PREFIX}/repack-x/manifest.parquet" in store
    assert f"{RETIRE_PREFIX}/repack-x/receipts.parquet" in store


def test_a_retire_dry_run_writes_no_manifest_and_deletes_nothing(mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, old_packs, new_pack = _s6_seed_store()
    fake = _FakeS6Store(store)
    mocker.patch.object(mod, "_s3_client", lambda: fake)
    store[f"{REPACK_PREFIX}/repack-x/verify_report.json"] = json.dumps({
        "run_id": "repack-x",
        "passed": True,
        "old_pack_keys": sorted(old_packs),
        "new_sidecar_keys": [new_pack.replace(".zpack", ".idx.parquet")],
    }).encode()

    args = parse_args(["retire-packs", "--pack-prefix", _S6_PACK_PREFIX])
    assert mod.run_retire_packs(args) == 0
    assert fake.deleted == []
    assert not any(k.startswith(RETIRE_PREFIX) for k in store)


def test_retiring_after_the_replacement_set_moved_is_refused(mocker):
    """Re-verify rather than retire against a proof of a store that changed."""
    import scripts.oneoff.reconcile_april_detail as mod

    store, old_packs, new_pack = _s6_seed_store()
    fake = _FakeS6Store(store)
    mocker.patch.object(mod, "_s3_client", lambda: fake)
    store[f"{REPACK_PREFIX}/repack-x/verify_report.json"] = json.dumps({
        "run_id": "repack-x",
        "passed": True,
        "old_pack_keys": sorted(old_packs),
        "new_sidecar_keys": [f"{_S6_PACK_PREFIX}pack-00099.idx.parquet"],
    }).encode()

    args = parse_args([
        "retire-packs", "--apply", "--pack-prefix", _S6_PACK_PREFIX,
    ])
    with pytest.raises(ReconcileError, match="changed since it was verified"):
        mod.run_retire_packs(args)
    assert fake.deleted == []


def _s6_legacy_store(*, uncovered=False):
    """The store as Stage 6's last step meets it: legacy Parquet still present,
    Stage 2 manifests describing it, and a replacement pack holding its bodies."""
    store, old_packs, new_pack = _s6_seed_store(uncovered=uncovered)

    for key in _S6_LEGACY_KEYS:
        store[key] = b"legacy-parquet-bytes"

    # Out of scope, and it must survive untouched.
    results_key = (
        "html/year=2026/month=4/artifact_type=results_page/part-0.parquet"
    )
    store[results_key] = b"results-parquet"

    store[f"{REPACK_PREFIX}/repack-x/verify_report.json"] = json.dumps({
        "run_id": "repack-x",
        "passed": True,
        "old_pack_keys": sorted(old_packs),
        "new_sidecar_keys": [new_pack.replace(".zpack", ".idx.parquet")],
    }).encode()

    return store, _S6_LEGACY_KEYS, results_key


def _patch_legacy_io(mocker, store, *, baseline=2):
    import scripts.oneoff.reconcile_april_detail as mod
    import shared.minio as minio

    fake = _FakeS6Store(store)
    mocker.patch.object(mod, "_s3_client", lambda: fake)
    # The drift gate is exercised by its own test. Everywhere else the fixture
    # states its own baseline rather than reaching for --allow-drift, which is
    # a flag for overruling a measured refusal, not for making a test pass.
    mocker.patch.object(mod, "BASELINE_OBJECTS", baseline)
    mocker.patch.object(
        minio, "write_bytes",
        lambda k, data, content_type=None: store.__setitem__(k, bytes(data)),
    )
    mocker.patch.object(minio, "write_json", lambda k, o: store.__setitem__(k, b"{}"))
    return fake


def _legacy_args(*extra):
    return parse_args([
        "delete-legacy", "--prefix", _S6_POP_PREFIX,
        "--pack-prefix", _S6_PACK_PREFIX, "--census-from-manifests", *extra,
    ])


def test_delete_legacy_removes_the_parquet_and_leaves_results_pages_alone(
    mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, legacy_keys, results_key = _s6_legacy_store()
    fake = _patch_legacy_io(mocker, store)

    args = _legacy_args("--apply", "--maintainer-approval", "the maintainer")
    assert mod.run_delete_legacy(args) == 0

    assert sorted(fake.deleted) == sorted(legacy_keys)
    assert results_key in store
    assert f"{_S6_POP_PREFIX}old-a.html.zst" in store
    assert f"{LEGACY_DELETE_PREFIX}/repack-x/manifest.parquet" in store
    assert f"{LEGACY_DELETE_PREFIX}/repack-x/receipts.parquet" in store


def test_a_legacy_dry_run_deletes_nothing_and_writes_no_manifest(mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _s6_legacy_store()
    fake = _patch_legacy_io(mocker, store)

    assert mod.run_delete_legacy(_legacy_args()) == 0
    assert fake.deleted == []
    assert not any(k.startswith(LEGACY_DELETE_PREFIX) for k in store)


def test_one_uncovered_body_stops_the_whole_deletion(mocker):
    """Not a skip: the refused object is a body that exists nowhere else."""
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _s6_legacy_store(uncovered=True)
    fake = _patch_legacy_io(mocker, store)

    args = _legacy_args("--apply", "--maintainer-approval", "the maintainer")
    with pytest.raises(ReconcileError, match="not provably recoverable"):
        mod.run_delete_legacy(args)
    assert fake.deleted == []


def test_allow_partial_deletes_the_covered_objects_and_says_which_it_left(
    mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, legacy_keys, _ = _s6_legacy_store(uncovered=True)
    fake = _patch_legacy_io(mocker, store)

    args = _legacy_args(
        "--apply", "--maintainer-approval", "the maintainer", "--allow-partial",
    )
    # Non-zero: one legacy detail Parquet object still survives.
    assert mod.run_delete_legacy(args) == 1
    assert fake.deleted == [legacy_keys[0]]
    assert legacy_keys[1] in store


def test_a_drifted_legacy_population_stops_the_run(mocker):
    """The count is held to the frozen baseline, so a population that moved is
    a stop rather than a deletion against a different set of objects."""
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _s6_legacy_store()
    _patch_legacy_io(mocker, store, baseline=1172)

    with pytest.raises(ReconcileError, match="drifted from its frozen census"):
        mod.run_delete_legacy(_legacy_args())


def test_a_legacy_object_no_stage_2_shard_names_is_drift_not_a_deletion(
    mocker):
    """--census-from-manifests is a weaker attestation than the census, not an
    absent one: the Stage 2 shards still have to name every live key."""
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _s6_legacy_store()
    store[f"{_S6_POP_PREFIX}part-stray.parquet"] = b"never materialized"
    _patch_legacy_io(mocker, store, baseline=3)

    with pytest.raises(ReconcileError, match="drifted from its frozen census"):
        mod.run_delete_legacy(_legacy_args())


def test_deleting_before_any_replacement_pack_exists_is_refused(mocker):
    import scripts.oneoff.reconcile_april_detail as mod

    store, _, _ = _s6_legacy_store()
    new_sidecar = f"{_S6_PACK_PREFIX}pack-00002.idx.parquet"
    del store[new_sidecar]
    _patch_legacy_io(mocker, store)

    with pytest.raises(ReconcileError, match="no replacement sidecars"):
        mod.run_delete_legacy(_legacy_args())


def test_the_read_back_extracts_from_the_replacement_pack_not_whichever_holds_it(
):
    """While both pack sets exist the old sidecars name every replaced member,
    so a read through ``read_packed_html`` could serve the whole sample from
    the packs this stage is about to delete and report success. The bytes under
    test are the replacement's."""
    from scripts.oneoff.reconcile_april_detail import read_back_members
    from shared.packfile import read_index_parquet, write_index_parquet

    store, _, new_pack = _s6_seed_store()
    sidecar = new_pack.replace(".zpack", ".idx.parquet")
    key = f"{_S6_POP_PREFIX}old-a.html.zst"

    checked, failures = read_back_members(
        _FakeS6Store(store), "bronze", {sidecar: [key]},
    )
    assert (checked, failures) == (1, [])

    # Claim a hash the replacement pack's own bytes do not produce.
    entries = read_index_parquet(store[sidecar])
    store[sidecar] = write_index_parquet([
        dataclasses.replace(e, raw_sha256="ff" * 32) if e.source_key == key else e
        for e in entries
    ])

    checked, failures = read_back_members(
        _FakeS6Store(store), "bronze", {sidecar: [key]},
    )
    assert checked == 1
    assert failures[0]["source_key"] == key
    assert failures[0]["pack_key"] == new_pack


def test_a_sidecar_naming_a_member_its_pack_does_not_hold_is_reported():
    from scripts.oneoff.reconcile_april_detail import read_back_members

    store, _, new_pack = _s6_seed_store()
    sidecar = new_pack.replace(".zpack", ".idx.parquet")

    checked, failures = read_back_members(
        _FakeS6Store(store), "bronze", {sidecar: [f"{_S6_POP_PREFIX}ghost.html.zst"]},
    )
    assert checked == 0
    assert failures[0]["error"].startswith("absent from")
