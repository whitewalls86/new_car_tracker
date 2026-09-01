"""Unit tests for scripts/estimate_dictionary_savings.py (Plan 129, Stage 0).

Stage 0 exists to decide whether Plan 129 gets built at all, so these tests are
almost entirely about the measurement being *honest*. A dictionary estimate can
be wrong in two directions and only one of them is visible: an understated
saving kills a good plan loudly, while an overstated one ships a permanent,
critical dependency on the strength of a number nobody can reproduce.

  A - split construction: what each split is allowed to leak
  B - split validation: a leak is raised, never warned
  C - too-small samples produce no result rather than a meaningless ratio
  D - savings are computed on held-out documents only
  E - the gate, including the case where it cannot be decided
  F - sampling and fetching
  G - end to end, including the exit code an undecided gate produces
  H - CLI defaults
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from scripts.estimate_dictionary_savings import (
    GATE_SAVING_PCT,
    MIN_TEST_DOCUMENTS,
    MIN_TRAIN_DOCUMENTS,
    Document,
    Split,
    SplitLeak,
    Totals,
    best_result,
    build_splits,
    choose_holdout_months,
    collect_documents,
    evaluate_gate,
    fetch_corpus_sample,
    load_sample,
    main,
    measure_split,
    parse_args,
    split_too_small,
    validate_split,
)

# ── Shared helpers ────────────────────────────────────────────────────────────

# A page shell shared by every synthetic document, so a trained dictionary has
# something real to find. Without it the measurement is all noise and the
# saving tests would assert on rounding.
_SHELL = (
    b"<!DOCTYPE html><html><head><meta charset='utf-8'>"
    b"<script id='initial-als-data'>{'framework':'bootstrap','analytics':'on'}</script>"
    b"<link rel='stylesheet' href='/assets/app.css'>" * 12
    + b"</head><body><nav class='site-header'>listings</nav>"
)


def _document(
    artifact_id: int,
    listing_id: str,
    capture_month: str = "2026-03",
    *,
    body: bytes | None = None,
) -> Document:
    payload = body if body is not None else f"<div>vin-{listing_id}-{artifact_id}</div>".encode()
    return Document(
        artifact_id=artifact_id,
        listing_id=listing_id,
        capture_month=capture_month,
        content=_SHELL + payload + b"</body></html>",
    )


def _corpus(
    *,
    listings: int = 12,
    captures: int = 4,
    months: tuple[str, ...] = ("2026-03", "2026-04"),
) -> list[Document]:
    """A synthetic corpus with repeat captures per listing, as production has."""
    documents: list[Document] = []
    artifact_id = 1
    for month_index, month in enumerate(months):
        for listing_index in range(listings):
            listing_id = f"listing-{listing_index:03d}"
            for capture in range(captures):
                documents.append(
                    _document(
                        artifact_id,
                        listing_id,
                        month,
                        # Repeat captures of a listing differ only in a
                        # timestamp, which is what makes an artifact-level
                        # split leak.
                        body=(
                            f"<div class='vehicle'>vin-{listing_index}</div>"
                            f"<span>captured {month_index}-{capture}</span>"
                        ).encode()
                        * 8,
                    )
                )
                artifact_id += 1
    return documents


def _named_split(name: str, train: list[Document], test: list[Document], **flags) -> Split:
    return Split(
        name=name,
        description="",
        train=train,
        test=test,
        require_disjoint_listings=flags.get("listings", False),
        require_disjoint_months=flags.get("months", False),
    )


# ── A. Split construction ─────────────────────────────────────────────────────

def test_build_splits_produces_all_four_when_months_are_held_out():
    splits = build_splits(_corpus(), holdout_months=["2026-04"])

    assert [s.name for s in splits] == [
        "leaky_reference",
        "listing_disjoint",
        "month_disjoint",
        "listing_and_month_disjoint",
    ]


def test_single_month_sample_omits_the_month_disjoint_splits():
    """A one-era sample cannot say anything about drift, so it must not pretend to."""
    documents = _corpus(months=("2026-03",))

    splits = build_splits(documents, holdout_months=[])

    assert [s.name for s in splits] == ["leaky_reference", "listing_disjoint"]


def test_strict_split_excludes_a_listing_that_appears_anywhere_in_training():
    """One training capture of a listing is enough to teach its text.

    Filtering the test side only by month would leave listings whose *other*
    captures trained the dictionary, which is the leak in a different costume.
    """
    documents = _corpus()

    strict = next(
        s
        for s in build_splits(documents, holdout_months=["2026-04"])
        if s.name == "listing_and_month_disjoint"
    )

    train_listings = {d.listing_id for d in strict.train}
    assert train_listings
    assert not train_listings & {d.listing_id for d in strict.test}
    assert {d.capture_month for d in strict.test} == {"2026-04"}


def test_leaky_reference_split_really_does_leak():
    """The control has to be leaky or it is not measuring the leak."""
    leaky = next(
        s for s in build_splits(_corpus(), holdout_months=["2026-04"])
        if s.name == "leaky_reference"
    )

    shared = {d.listing_id for d in leaky.train} & {d.listing_id for d in leaky.test}
    assert shared, "artifact-level split should put the same listings on both sides"


def test_choose_holdout_months_takes_the_most_recent():
    documents = _corpus(months=("2026-02", "2026-03", "2026-04"))

    assert choose_holdout_months(documents) == ["2026-04"]
    assert choose_holdout_months(documents, count=2) == ["2026-03", "2026-04"]


def test_choose_holdout_months_refuses_to_hold_out_everything():
    documents = _corpus(months=("2026-03",))

    assert choose_holdout_months(documents) == []
    assert choose_holdout_months(documents, count=5) == []


# ── B. Split validation ───────────────────────────────────────────────────────

def test_same_listing_on_both_sides_is_rejected_not_measured():
    """The Plan 114 Stage 3 trap, as an executable assertion."""
    shared_listing = [_document(1, "listing-a"), _document(2, "listing-a")]
    split = _named_split("listing_disjoint", shared_listing[:1], shared_listing[1:], listings=True)

    with pytest.raises(SplitLeak, match="disjoint listings"):
        validate_split(split)


def test_shared_month_is_rejected_when_the_split_claims_month_disjointness():
    split = _named_split(
        "month_disjoint",
        [_document(1, "listing-a", "2026-03")],
        [_document(2, "listing-b", "2026-03")],
        months=True,
    )

    with pytest.raises(SplitLeak, match="disjoint months"):
        validate_split(split)


def test_the_same_artifact_on_both_sides_is_always_a_leak():
    """Unconditional: even the deliberately leaky split may not score itself."""
    document = _document(1, "listing-a")
    split = _named_split("leaky_reference", [document], [document])

    with pytest.raises(SplitLeak, match="both sides"):
        validate_split(split)


def test_a_split_that_claims_nothing_is_allowed_to_share_listings():
    documents = [_document(1, "listing-a"), _document(2, "listing-a")]

    validate_split(_named_split("leaky_reference", documents[:1], documents[1:]))


def test_measure_split_raises_rather_than_reporting_a_leaked_number():
    documents = _corpus()
    split = _named_split("listing_disjoint", documents, documents[:20], listings=True)

    with pytest.raises(SplitLeak):
        measure_split(split, dict_sizes_kb=[16])


# ── C. Too-small samples ──────────────────────────────────────────────────────

def test_split_too_small_names_every_shortfall():
    split = _named_split("listing_disjoint", [_document(1, "a")], [_document(2, "b")])

    reason = split_too_small(split)

    assert reason is not None
    assert "train documents" in reason
    assert "test documents" in reason
    assert "train listings" in reason


def test_a_sufficient_split_reports_no_shortfall():
    documents = _corpus(listings=12, captures=4, months=("2026-03",))
    split = _named_split("listing_disjoint", documents[:32], documents[32:], listings=False)

    assert split_too_small(split) is None


def test_too_small_split_is_skipped_rather_than_measured():
    """No ratio at all beats a ratio computed from four documents."""
    train = [_document(i, f"listing-{i}") for i in range(3)]
    test = [_document(100 + i, f"other-{i}") for i in range(3)]

    summary = measure_split(_named_split("listing_disjoint", train, test, listings=True),
                            dict_sizes_kb=[16])

    assert summary["skipped"]
    assert summary["results"] == []
    assert "baseline_compressed_bytes" not in summary


def test_minimums_are_high_enough_to_mean_something():
    assert MIN_TRAIN_DOCUMENTS >= 16
    assert MIN_TEST_DOCUMENTS >= 8


# ── D. Savings are computed on held-out documents only ────────────────────────

def test_saving_is_measured_on_test_documents_only():
    """Adding documents to the *training* side must not change the denominator."""
    documents = _corpus(months=("2026-03",))
    train, test = documents[:24], documents[24:40]
    small = measure_split(
        _named_split("s", train, test), dict_sizes_kb=[16], zstd_level=3
    )
    larger = measure_split(
        _named_split("s", train + documents[40:], test), dict_sizes_kb=[16], zstd_level=3
    )

    assert small["baseline_compressed_bytes"] == larger["baseline_compressed_bytes"]
    assert small["test_documents"] == larger["test_documents"] == len(test)


def test_baseline_and_candidate_compress_the_same_bytes_at_the_same_level():
    """Trap 2 from Plan 114: never score a level-9 candidate against level-3 bytes."""
    documents = _corpus(months=("2026-03",))
    summary = measure_split(
        _named_split("s", documents[:24], documents[24:40]),
        dict_sizes_kb=[16],
        zstd_level=7,
    )

    assert summary["zstd_level"] == 7
    result = summary["results"][0]
    expected = 100.0 * result["compressed_bytes"] / summary["baseline_compressed_bytes"]
    assert result["pct_of_baseline"] == pytest.approx(expected, abs=1e-3)
    assert result["saving_pct"] == pytest.approx(100.0 - expected, abs=1e-3)


def test_a_dictionary_beats_the_plain_baseline_on_shared_boilerplate():
    """The effect the whole plan rests on, on data where it must be present."""
    documents = _corpus(listings=16, months=("2026-03",))
    summary = measure_split(
        _named_split("s", documents[:48], documents[48:]), dict_sizes_kb=[16]
    )

    assert summary["results"][0]["saving_pct"] > 0


def test_every_result_carries_the_realised_dictionary_size_and_id():
    documents = _corpus(listings=16, months=("2026-03",))
    summary = measure_split(
        _named_split("s", documents[:48], documents[48:]), dict_sizes_kb=[16]
    )

    result = summary["results"][0]
    assert 0 < result["dictionary_bytes"] <= 16 * 1024
    assert result["dictionary_id"] > 0
    # ~112 KB spread over ~3.9M objects is the reason this cost is ignorable,
    # and the report says so numerically instead of asserting it in prose.
    assert result["dictionary_bytes_per_artifact_at_corpus_scale"] < 0.1


def test_a_dictionary_size_that_fails_to_train_is_recorded_not_fatal(mocker):
    documents = _corpus(listings=16, months=("2026-03",))
    split = _named_split("s", documents[:48], documents[48:])

    mocker.patch(
        "scripts.estimate_dictionary_savings.train_dictionary",
        side_effect=RuntimeError("srcSize too small"),
    )

    summary = measure_split(split, dict_sizes_kb=[16, 32])

    assert [r["error"] for r in summary["results"]] == ["srcSize too small"] * 2
    assert summary["baseline_compressed_bytes"] > 0


def test_best_result_picks_the_largest_saving_and_ignores_failures():
    summary = {
        "results": [
            {"dict_size_kb": 16, "saving_pct": 12.0},
            {"dict_size_kb": 256, "error": "boom"},
            {"dict_size_kb": 112, "saving_pct": 61.2},
        ]
    }

    assert best_result(summary)["dict_size_kb"] == 112
    assert best_result({"results": [{"dict_size_kb": 16, "error": "boom"}]}) is None


# ── E. The gate ───────────────────────────────────────────────────────────────

def _strict(**overrides):
    summary = {"split": "listing_and_month_disjoint", "results": [{"dict_size_kb": 112,
                                                                  "saving_pct": 61.2}]}
    summary.update(overrides)
    return summary


def test_gate_passes_on_the_strict_split():
    gate = evaluate_gate([_strict()])

    assert gate == {
        "gate_pct": GATE_SAVING_PCT,
        "decided": True,
        "passed": True,
        "best_dict_size_kb": 112,
        "saving_pct": 61.2,
    }


def test_gate_fails_below_the_threshold():
    gate = evaluate_gate([_strict(results=[{"dict_size_kb": 112, "saving_pct": 39.9}])])

    assert gate["decided"] is True
    assert gate["passed"] is False


def test_gate_ignores_the_looser_splits_however_good_they_look():
    """A 90% leaky number must never carry the decision."""
    loose = {"split": "leaky_reference", "results": [{"dict_size_kb": 112, "saving_pct": 90.0}]}
    listing_only = {"split": "listing_disjoint",
                    "results": [{"dict_size_kb": 112, "saving_pct": 80.0}]}

    gate = evaluate_gate(
        [loose, listing_only, _strict(results=[{"dict_size_kb": 112, "saving_pct": 21.0}])]
    )

    assert gate["passed"] is False
    assert gate["saving_pct"] == 21.0


def test_gate_is_undecided_without_a_strict_split_rather_than_passing():
    gate = evaluate_gate([{"split": "listing_disjoint", "results": [{"saving_pct": 99.0}]}])

    assert gate["decided"] is False
    assert "passed" not in gate
    assert "single capture month" in gate["reason"]


def test_gate_is_undecided_when_the_strict_split_was_too_small():
    gate = evaluate_gate([_strict(skipped="test documents: 3 < 8", results=[])])

    assert gate["decided"] is False
    assert "too small" in gate["reason"]


def test_gate_is_undecided_when_no_dictionary_trained():
    gate = evaluate_gate([_strict(results=[{"dict_size_kb": 112, "error": "boom"}])])

    assert gate["decided"] is False
    assert "no dictionary size trained" in gate["reason"]


# ── F. Sampling and fetching ──────────────────────────────────────────────────

def test_corpus_sample_spreads_the_budget_across_months_and_is_deterministic():
    con = MagicMock()
    con.execute.return_value.description = [("artifact_id",)]
    con.fetchall.return_value = [(1,)]

    fetch_corpus_sample(con, months=["2026-03", "2026-04"], sample_size=1000)

    query, params = con.execute.call_args[0]
    assert "month_rank <= 500" in query
    # Deterministic ordering: a storage decision must be reproducible, and
    # random() would make the sample unrepeatable across runs.
    assert "ORDER BY hash(artifact_id)" in query
    assert "random()" not in query
    assert params == ["%detail%", "2026-03", "2026-04"]


def test_corpus_sample_with_no_months_queries_nothing():
    con = MagicMock()

    assert fetch_corpus_sample(con, months=[], sample_size=100) == []
    con.execute.assert_not_called()


def test_collect_documents_keeps_raw_bytes_untouched(mocker):
    """Decoding to str with errors='replace' would change the measured lengths."""
    payload = b"<html>caf\xe9 not utf-8</html>"
    rows = [{"artifact_id": 1, "listing_id": "a", "capture_month": "2026-03",
             "minio_path": "s3://bronze/x"}]
    totals = Totals()

    mocker.patch("shared.minio.read_html", return_value=payload)

    documents = collect_documents(rows, totals)

    assert documents[0].content == payload
    assert totals.fetched == 1


def test_a_failed_fetch_is_recorded_and_the_run_continues(mocker):
    rows = [
        {"artifact_id": 1, "listing_id": "a", "capture_month": "2026-03",
         "minio_path": "s3://bronze/gone"},
        {"artifact_id": 2, "listing_id": "b", "capture_month": "2026-03",
         "minio_path": "s3://bronze/ok"},
    ]
    totals = Totals()

    mocker.patch("shared.minio.read_html", side_effect=[OSError("404"), b"<html>ok</html>"])

    documents = collect_documents(rows, totals)

    assert [d.artifact_id for d in documents] == [2]
    assert totals.failures[0]["stage"] == "fetch"
    assert totals.fetched == 1


def test_rows_without_a_path_are_counted_not_fetched(mocker):
    rows = [{"artifact_id": 1, "listing_id": "a", "capture_month": "2026-03", "minio_path": None}]
    totals = Totals()

    read_html = mocker.patch("shared.minio.read_html")

    assert collect_documents(rows, totals) == []

    read_html.assert_not_called()
    assert totals.skipped_no_path == 1


def test_max_documents_stops_the_fetch_loop(mocker):
    rows = [
        {"artifact_id": i, "listing_id": "a", "capture_month": "2026-03",
         "minio_path": f"s3://bronze/{i}"}
        for i in range(5)
    ]
    totals = Totals()

    mocker.patch("shared.minio.read_html", return_value=b"<html/>")

    documents = collect_documents(rows, totals, max_documents=2)

    assert len(documents) == 2


def test_sample_in_bypasses_the_lake_entirely(tmp_path):
    """The two-step run: sample in dbt-runner, measure locally over a tunnel."""
    path = tmp_path / "sample.json"
    path.write_text(
        json.dumps([{"artifact_id": 1, "minio_path": "s3://bronze/x"}]),
        encoding="utf-8",
    )

    rows = load_sample(parse_args(["--sample-in", str(path)]))

    assert rows == [{"artifact_id": 1, "minio_path": "s3://bronze/x"}]


# ── G. End to end ─────────────────────────────────────────────────────────────

def _end_to_end(tmp_path, mocker, months=("2026-03", "2026-04", "2026-05")):
    """Write a sample file and the blobs it points at, then run ``main``."""
    documents = _corpus(listings=20, captures=3, months=months)
    rows, blobs = [], {}
    for document in documents:
        path = f"s3://bronze/html/{document.artifact_id}"
        rows.append(
            {
                "artifact_id": document.artifact_id,
                "listing_id": document.listing_id,
                "capture_month": document.capture_month,
                "minio_path": path,
            }
        )
        blobs[path] = document.content

    sample = tmp_path / "sample.json"
    sample.write_text(json.dumps(rows), encoding="utf-8")
    report_path = tmp_path / "report.json"

    mocker.patch("shared.minio.read_html", side_effect=lambda p: blobs[p])

    code = main(
        [
            "--sample-in", str(sample),
            "--dict-sizes", "16",
            "--json-out", str(report_path),
        ]
    )
    return code, json.loads(report_path.read_text(encoding="utf-8"))


def test_end_to_end_run_reports_every_split_and_decides_the_gate(tmp_path, mocker):
    code, report = _end_to_end(tmp_path, mocker)

    assert [s["split"] for s in report["splits"]] == [
        "leaky_reference",
        "listing_disjoint",
        "month_disjoint",
        "listing_and_month_disjoint",
    ]
    assert report["sample"]["holdout_months"] == ["2026-05"]
    assert report["sample"]["failure_count"] == 0
    assert report["gate"]["decided"] is True
    # Synthetic pages are almost all shared shell, so the dictionary must win
    # here; a run that did not would mean the measurement is broken, not that
    # the corpus is unusual.
    assert report["gate"]["passed"] is True
    assert code == 0


def test_only_splits_restricts_measurement_but_still_decides_the_gate(tmp_path, mocker):
    documents = _corpus(listings=20, captures=3)
    rows, blobs = [], {}
    for document in documents:
        path = f"s3://bronze/html/{document.artifact_id}"
        rows.append(
            {
                "artifact_id": document.artifact_id,
                "listing_id": document.listing_id,
                "capture_month": document.capture_month,
                "minio_path": path,
            }
        )
        blobs[path] = document.content
    sample = tmp_path / "sample.json"
    sample.write_text(json.dumps(rows), encoding="utf-8")
    report_path = tmp_path / "report.json"

    mocker.patch("shared.minio.read_html", side_effect=lambda p: blobs[p])

    main(
        [
            "--sample-in", str(sample),
            "--dict-sizes", "16",
            "--only-splits", "listing_and_month_disjoint",
            "--json-out", str(report_path),
        ]
    )

    report = json.loads(report_path.read_text(encoding="utf-8"))
    assert [s["split"] for s in report["splits"]] == ["listing_and_month_disjoint"]
    assert report["gate"]["decided"] is True


def test_only_splits_that_drops_the_gate_split_leaves_the_gate_undecided():
    """Filtering away the strict split must not silently promote a looser one."""
    summaries = [
        {"split": "listing_disjoint", "results": [{"dict_size_kb": 16, "saving_pct": 99.0}]}
    ]

    assert evaluate_gate(summaries)["decided"] is False


def test_a_single_month_sample_exits_undecided_rather_than_claiming_a_pass(
    tmp_path, mocker
):
    """Exit 2, not 0: an unmeasurable gate must not read as a green run in CI."""
    code, report = _end_to_end(tmp_path, mocker, months=("2026-03",))

    assert report["gate"]["decided"] is False
    assert code == 2


# ── H. CLI ────────────────────────────────────────────────────────────────────

def test_cli_defaults_match_the_plan():
    args = parse_args([])

    assert args.sample_size == 2000
    assert args.dict_sizes == [16, 32, 112, 256]
    assert args.optimize_cover is False
    assert args.zstd_level == 9
    assert args.months is None
    assert args.holdout_months is None


def test_cli_accepts_the_documented_flags():
    args = parse_args(
        [
            "--months", "2026-03", "2026-04",
            "--holdout-months", "2026-04",
            "--sample-size", "500",
            "--dict-sizes", "32", "112",
            "--optimize-cover",
            "--json-out", "/tmp/report.json",
        ]
    )

    assert args.months == ["2026-03", "2026-04"]
    assert args.holdout_months == ["2026-04"]
    assert args.sample_size == 500
    assert args.dict_sizes == [32, 112]
    assert args.optimize_cover is True
    # type=Path, so compare as a path — str() renders separators per platform.
    assert args.json_out == Path("/tmp/report.json")
