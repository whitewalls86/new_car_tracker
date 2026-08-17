"""Plan 135 Stage 4: the disk_usage DAG's schedule predicate and result check."""
import datetime
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent.parent.parent / "airflow" / "dags"))

from disk_usage import (  # noqa: E402
    MINIO_WALK_WEEKDAY,
    check_disk_usage_result,
    should_include_minio,
)


class TestShouldIncludeMinio:
    def test_exactly_one_day_a_week_walks_minio(self):
        """A ~4M-inode walk that ran daily would contend with live scraping;
        one that never ran would leave the biggest volume unmeasured."""
        week = [
            datetime.date(2026, 8, 17) + datetime.timedelta(days=offset)
            for offset in range(7)
        ]
        assert sum(should_include_minio(day) for day in week) == 1

    def test_the_chosen_day_is_the_configured_one(self):
        day = datetime.date(2026, 8, 17)
        while day.weekday() != MINIO_WALK_WEEKDAY:
            day += datetime.timedelta(days=1)
        assert should_include_minio(day)

    def test_accepts_a_datetime_as_well_as_a_date(self):
        """Airflow hands the callable a pendulum datetime, not a date."""
        moment = datetime.datetime(2026, 8, 17, 4, 0)
        while moment.weekday() != MINIO_WALK_WEEKDAY:
            moment += datetime.timedelta(days=1)
        assert should_include_minio(moment)


class TestCheckDiskUsageResult:
    def test_a_clean_run_passes(self):
        check_disk_usage_result({"failed": 0, "measured": 11, "carried_forward": 0})

    def test_a_daily_run_carrying_minio_forward_passes(self):
        check_disk_usage_result({"failed": 0, "measured": 10, "carried_forward": 1})

    def test_a_failed_measurement_raises(self):
        with pytest.raises(RuntimeError, match="could not be measured"):
            check_disk_usage_result(
                {"failed": 1, "measured": 10, "unpublished": ["/usr"]}
            )

    def test_measuring_nothing_raises_even_with_no_explicit_failures(self):
        """If the mounts vanish, every target carries forward and nothing
        "fails" -- the gauges would freeze and the DAG would stay green."""
        with pytest.raises(RuntimeError, match="nothing was measured"):
            check_disk_usage_result({"failed": 0, "measured": 0, "carried_forward": 11})
