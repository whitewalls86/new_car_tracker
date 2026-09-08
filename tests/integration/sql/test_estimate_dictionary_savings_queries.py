"""Layer 2 -- Plan 129's two sampling statements, against a real DuckDB.

Plan 162 Stage X. These were f-strings inside
``scripts/estimate_dictionary_savings.py`` carrying a G5 and a G15 waiver, and
the unit suite tested the Python around them by handing ``fetch_*`` a stub
connection. That proves the call shape and nothing about the SQL: a renamed
partition column, a dropped ``minio_path``, a window function DuckDB stops
accepting are all invisible to a stub.

**Why these are production SQL rather than a spike's.**
``scripts/train_html_dictionary.py`` trains the dictionary the bronze write path
compresses every object against, and it imports ``collect_documents`` from this
module. If the sampling is wrong the dictionary is trained on the wrong corpus,
and the whole of Plan 129's 73.15% rests on the sample being what it claims.
Plan 129 having archived does not make the script spent: ``scripts/oneoff/`` is
*"the owning plan has archived **and nothing binding names it**"*, and something
binding names this.

**This module authors no SQL of its own.** The fixture Parquet is written with
``pyarrow`` rather than ``COPY ... TO``, which keeps the only statements
executed here the two production ones -- so what the execution recorder
attributes to ``scripts/sql/`` is the real text and nothing staged around it.
The statements interpolate their own path, which is the same fixture-mode split
``lake_snapshot_cohort.open_duckdb_connection`` already makes for the selectors,
and it is what lets this run without MinIO.
"""
from datetime import datetime
from pathlib import Path

import pytest

from scripts.estimate_dictionary_savings import (
    SELECT_AVAILABLE_CAPTURE_MONTHS,
    SELECT_CORPUS_SAMPLE,
    fetch_available_months,
    fetch_corpus_sample,
)

pytestmark = pytest.mark.integration

duckdb = pytest.importorskip("duckdb")
pa = pytest.importorskip("pyarrow")
pq = pytest.importorskip("pyarrow.parquet")


def _observations() -> "pa.Table":
    """Two captures of L1 in April, one of L2 in May, one SRP row to exclude.

    Two months so the month-disjoint split has something to split; a repeat
    capture so ``one_row_per_artifact`` has something to collapse; a non-detail
    row so the source filter has something to drop.
    """
    return pa.table({
        "artifact_id": pa.array(["a1", "a2", "a3", "a4"], pa.string()),
        "listing_id": pa.array(["L1", "L1", "L2", "L3"], pa.string()),
        "fetched_at": pa.array([
            datetime(2026, 4, 2, 10), datetime(2026, 4, 9, 10),
            datetime(2026, 5, 3, 10), datetime(2026, 5, 4, 10),
        ], pa.timestamp("us")),
        "obs_year": pa.array([2026, 2026, 2026, 2026], pa.int64()),
        "obs_month": pa.array([4, 4, 5, 5], pa.int64()),
        "source": pa.array(["detail", "detail", "detail", "srp"], pa.string()),
    })


def _artifact_events() -> "pa.Table":
    return pa.table({
        "artifact_id": pa.array(["a1", "a2", "a3", "a4"], pa.string()),
        "artifact_type": pa.array(
            ["detail_page", "detail_page", "detail_page", "srp_page"], pa.string()
        ),
        "minio_path": pa.array([
            "bronze/detail/a1.html.zst", "bronze/detail/a2.html.zst",
            "bronze/detail/a3.html.zst", "bronze/srp/a4.html.zst",
        ], pa.string()),
    })


@pytest.fixture
def lake(tmp_path: Path):
    """A Parquet stand-in for the observation and artifact-event lakes."""
    silver = tmp_path / "silver.parquet"
    events = tmp_path / "events.parquet"
    pq.write_table(_observations(), silver)
    pq.write_table(_artifact_events(), events)
    connection = duckdb.connect()
    try:
        yield connection, silver.as_posix(), events.as_posix()
    finally:
        connection.close()


def test_the_statements_under_test_are_the_files_on_disk():
    """This suite executes `select_available_capture_months.sql` and
    `select_corpus_sample.sql`, not a copy of them.

    The origin is what the execution recorder credits, so asserting it here is
    what makes the rest of this module count as coverage of those two files
    rather than of a string that happens to resemble them. It is also the
    cheapest guard against the repair this stage exists to prevent: a statement
    quietly re-inlined into the script would still pass every assertion below
    while the `.sql` file went dead.
    """
    assert {p.name for p in SELECT_AVAILABLE_CAPTURE_MONTHS.origins} == {
        "select_available_capture_months.sql"
    }
    assert {p.name for p in SELECT_CORPUS_SAMPLE.origins} == {
        "select_corpus_sample.sql"
    }


def test_available_months_reads_the_partition_columns(lake):
    """Distinct and oldest-first, out of the real statement."""
    con, silver, _ = lake

    months = fetch_available_months(con, "%detail%", silver_path=silver)

    assert months == ["2026-04", "2026-05"], (
        "the capture-month projection or its ordering changed, and the "
        "month-disjoint split is built on this statement"
    )


def test_available_months_honours_the_bound_source_filter(lake):
    """The source pattern is bound, and it still excludes the detail months."""
    con, silver, _ = lake

    assert fetch_available_months(con, "%srp%", silver_path=silver) == ["2026-05"]


def test_corpus_sample_collapses_repeats_and_joins_its_object_path(lake):
    """One row per artifact, detail only, each carrying the object it points at.

    Three things the sample promises that a stub connection could not check:
    that ``one_row_per_artifact`` collapses the repeat capture, that the
    ``artifact_type = 'detail_page'`` filter drops the SRP row, and that the
    join yields a ``minio_path`` -- without which ``collect_documents`` has
    nothing to fetch.
    """
    con, silver, events = lake

    rows = fetch_corpus_sample(
        con, months=["2026-04", "2026-05"], sample_size=10,
        silver_path=silver, artifact_events_path=events,
    )

    assert sorted(row["artifact_id"] for row in rows) == ["a1", "a2", "a3"], (
        f"expected the three detail artifacts and no SRP one: {rows}"
    )
    assert {row["capture_month"] for row in rows} == {"2026-04", "2026-05"}
    assert all(row["minio_path"] for row in rows), (
        "every sampled row must carry the object collect_documents will fetch"
    )


def test_corpus_sample_caps_each_month(lake):
    """The per-month cap is interpolated as an int and still binds the window.

    A budget of 2 over two months is one per month, so the April pair must lose
    one -- the cap doing its job rather than the data running out.
    """
    con, silver, events = lake

    rows = fetch_corpus_sample(
        con, months=["2026-04", "2026-05"], sample_size=2,
        silver_path=silver, artifact_events_path=events,
    )

    per_month: dict[str, list[str]] = {}
    for row in rows:
        per_month.setdefault(row["capture_month"], []).append(row["artifact_id"])
    assert sorted(per_month) == ["2026-04", "2026-05"]
    assert all(len(ids) == 1 for ids in per_month.values()), per_month


def test_corpus_sample_is_reproducible(lake):
    """Hashed ordering rather than a shuffle, asserted rather than trusted: a
    storage decision that cannot be reproduced cannot be audited."""
    con, silver, events = lake

    def sample():
        return fetch_corpus_sample(
            con, months=["2026-04"], sample_size=1,
            silver_path=silver, artifact_events_path=events,
        )

    assert sample() == sample()
