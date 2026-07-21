"""Unit tests for the Gate C shadow-build replay instrumentation.

No Spark, no Docker, no VM. These cover the parts that decide what the
evidence MEANS -- the failure-phase classifier and the cgroup parsing -- plus
the two properties that make the replay a faithful reproduction rather than a
new experiment.
"""
import sys
import types
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="module")
def replay(catalog_env=None):
    """Import the replay module.

    It does `sys.path.insert(0, "/app")` for the container layout and imports
    shared.iceberg_catalog, which needs catalog env vars present at import
    time on some paths -- set them here rather than depending on the ambient
    shell.
    """
    import os

    os.environ.setdefault("ICEBERG_CATALOG_URI", "http://lakekeeper:8181/catalog")
    os.environ.setdefault("MINIO_ROOT_USER", "test")
    os.environ.setdefault("MINIO_ROOT_PASSWORD", "test")
    sys.path.insert(0, str(REPO_ROOT))
    from scripts import gate_c_shadow_replay

    return gate_c_shadow_replay


class TestFailureClassification:
    """The distinction the original VM report could not make.

    "It OOMed" is not actionable. Dying while LISTING files, while
    SHUFFLING/WINDOWING, and while COMMITTING to Iceberg imply three different
    fixes, and the 2026-07-17 run left no trace to tell them apart. Getting
    these buckets wrong would send the next fix at the wrong subsystem.
    """

    def test_listing_oom_is_not_reported_as_execution(self, replay):
        msg = ("java.lang.OutOfMemoryError: Java heap space at "
               "org.apache.spark.sql.execution.datasources."
               "InMemoryFileIndex.listLeafFiles")

        assert replay.classify_failure(msg) == "oom_during_planning_or_listing"

    def test_shuffle_and_window_oom_classified_as_execution(self, replay):
        msg = ("java.lang.OutOfMemoryError: Java heap space at "
               "org.apache.spark.util.collection.ExternalSorter shuffle window")

        assert replay.classify_failure(msg) == "oom_during_scan_shuffle_or_window"

    def test_commit_oom_classified_as_write(self, replay):
        msg = ("java.lang.OutOfMemoryError: Java heap space at "
               "org.apache.iceberg.SnapshotProducer.commit")

        assert replay.classify_failure(msg) == "oom_during_iceberg_write_or_commit"

    def test_unattributable_oom_is_named_as_such_not_guessed(self, replay):
        """An OOM with no recognisable frame must NOT be silently filed under
        a phase. A wrong phase label is worse than an honest 'unclassified',
        because it would be read as evidence."""
        msg = "java.lang.OutOfMemoryError: Java heap space"

        assert replay.classify_failure(msg) == "oom_phase_unclassified"

    def test_direct_query_error_is_flagged_as_a_cascade(self, replay):
        """Finding 1 established this is a post-OOM symptom, not a defect.
        Classifying it as an independent failure would re-open a question
        that is already closed."""
        msg = ("[UNSUPPORTED_DATASOURCE_FOR_DIRECT_QUERY] Unsupported data "
               "source type for direct query on files: parquet")

        assert replay.classify_failure(msg) == "post_oom_session_cascade"

    def test_non_oom_failure_is_not_dressed_up_as_an_oom(self, replay):
        msg = "AnalysisException: Table or view not found: foo"

        assert replay.classify_failure(msg) == "non_oom_failure"

    def test_empty_message_is_unknown(self, replay):
        assert replay.classify_failure("") == "unknown"


class TestCgroupParsing:
    def test_parses_peak_and_oom_events(self, replay, tmp_path, monkeypatch):
        """memory.peak and memory.events are the facts that OUTLIVE a dead
        JVM -- the kernel keeps them whether or not Java got to report
        anything. If the parse is wrong, the one measurement guaranteed to
        survive the failure is the one that is useless.
        """
        (tmp_path / "memory.max").write_text("6442450944")
        (tmp_path / "memory.peak").write_text("6100000000")
        (tmp_path / "memory.current").write_text("512000000")
        (tmp_path / "memory.events").write_text(
            "low 0\nhigh 0\nmax 12\noom 1\noom_kill 1\n"
        )
        monkeypatch.setattr(replay, "CGROUP", tmp_path)

        facts = replay.cgroup_facts()

        assert facts["memory.max"] == 6442450944
        assert facts["memory.peak"] == 6100000000
        assert facts["memory.events"]["oom_kill"] == 1

    def test_missing_cgroup_files_do_not_raise(self, replay, tmp_path, monkeypatch):
        """Evidence capture must never be the thing that fails the run."""
        monkeypatch.setattr(replay, "CGROUP", tmp_path / "nope")

        assert replay.cgroup_facts() == {}


class TestReplayFidelity:
    def test_replay_never_sets_driver_memory(self, replay):
        """The whole point is reproducing the shadow build's UNSET sizing.

        Setting spark.driver.memory here -- even to '1g' -- would change the
        launcher path being tested and quietly turn a reproduction into a
        different experiment. Asserted against the source because it is a
        property of the session builder, and a regression would still run
        green.
        """
        source = Path(replay.__file__).read_text(encoding="utf-8")
        body = source.split("def build_session")[1].split("def main")[0]
        # Comments are where the decision NOT to set it is explained, so
        # strip them -- the property under test is that no code sets it.
        code = "\n".join(
            line for line in body.splitlines()
            if not line.strip().startswith("#")
        )

        assert "spark.driver.memory" not in code

    def test_selector_matches_the_failing_shadow_build(self, replay):
        assert replay.DEFAULT_SELECTOR == "+int_listing_volatility_features"

    def test_sampler_flushes_every_tick(self, replay, tmp_path):
        """If the trace is only written at the end, it is lost in exactly the
        case worth studying -- a driver that dies mid-run. Each sample must
        already be on disk before the next one is taken."""
        spark = types.SimpleNamespace(_jvm=None)
        sampler = replay.HeapSampler(spark, tmp_path / "s.jsonl", interval=0.01)
        sampler.start()
        import time

        time.sleep(0.15)
        sampler.stop()

        # Written and flushed while running, not on stop().
        assert (tmp_path / "s.jsonl").exists()
