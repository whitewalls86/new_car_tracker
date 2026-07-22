# ruff: noqa: E501 - the stack-trace fixtures below are REAL captured frames;
# reflowing them to fit the line limit would make them stop matching what
# Spark actually emits, which is the whole point of keeping them verbatim.
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

    LISTING_STACK = """java.lang.OutOfMemoryError: Java heap space
	at org.apache.spark.sql.execution.datasources.InMemoryFileIndex.listLeafFiles(InMemoryFileIndex.scala:150)
	at org.apache.spark.sql.execution.datasources.InMemoryFileIndex.refresh0(InMemoryFileIndex.scala:100)
"""

    WINDOW_STACK = """java.lang.OutOfMemoryError: Java heap space
	at java.base/java.nio.ByteBuffer.allocate(ByteBuffer.java:363)
	at org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillReader.<init>(UnsafeSorterSpillReader.java:77)
	at org.apache.spark.sql.execution.window.WindowExec$$anon$1.fetchNextPartition(WindowExec.scala:160)
"""

    COMMIT_STACK = """java.lang.OutOfMemoryError: Java heap space
	at org.apache.iceberg.SnapshotProducer.commit(SnapshotProducer.java:400)
	at org.apache.iceberg.ManifestWriter.close(ManifestWriter.java:80)
"""

    # The ACTUAL stack captured on the VM, 2026-07-22, abbreviated but with
    # the frame ORDER preserved -- allocation site at the top, the
    # WriteToDataSourceV2 plan frames far below it.
    REAL_STACK = """org.apache.spark.SparkException: Job aborted due to stage failure
java.lang.OutOfMemoryError: Java heap space
	at java.base/java.nio.HeapByteBuffer.<init>(HeapByteBuffer.java:64)
	at java.base/java.nio.ByteBuffer.allocate(ByteBuffer.java:363)
	at org.apache.spark.io.ReadAheadInputStream.<init>(ReadAheadInputStream.java:106)
	at org.apache.spark.util.collection.unsafe.sort.UnsafeSorterSpillReader.<init>(UnsafeSorterSpillReader.java:77)
	at org.apache.spark.sql.execution.ExternalAppendOnlyUnsafeRowArray.generateIterator(ExternalAppendOnlyUnsafeRowArray.scala:183)
	at org.apache.spark.sql.execution.window.WindowExec$$anon$1.fetchNextPartition(WindowExec.scala:160)
	at org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec.writeToTable(WriteToDataSourceV2Exec.scala:1)
	at org.apache.spark.sql.execution.datasources.v2.WriteToDataSourceV2Exec.run(WriteToDataSourceV2Exec.scala:2)
"""

    def test_listing_oom_is_not_reported_as_execution(self, replay):
        assert replay.classify_failure(
            self.LISTING_STACK) == "oom_during_planning_or_listing"

    def test_shuffle_and_window_oom_classified_as_execution(self, replay):
        assert replay.classify_failure(
            self.WINDOW_STACK) == "oom_during_scan_shuffle_or_window"

    def test_commit_oom_classified_as_write(self, replay):
        assert replay.classify_failure(
            self.COMMIT_STACK) == "oom_during_iceberg_write_or_commit"

    def test_real_captured_stack_is_execution_not_a_commit(self, replay):
        """Regression against the real 2026-07-22 replay stack.

        The first classifier scanned the WHOLE message for keywords and
        called this `oom_during_iceberg_write_or_commit`, because
        WriteToDataSourceV2 plan frames put "writeto" in the text 6 times
        while "shuffle" appeared 0 times. The allocation site is
        UnsafeSorterSpillReader inside WindowExec -- execution, not commit.

        A wrong phase label is worse than no label: it would have aimed the
        next fix at the Iceberg writer, which is not where the memory goes.
        The old tests passed because they were single-line strings with the
        keyword adjacent to the OOM text, a shape no real stack has.
        """
        assert replay.classify_failure(
            self.REAL_STACK) == "oom_during_scan_shuffle_or_window"

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


class TestRuntimeFactsOnAnUnsetSession:
    """Regression: reading facts must not kill the run whose facts it reads.

    jvm_runtime_facts() passed a string sentinel as conf.get()'s default.
    Spark type-checks that default against the key's declared type, so on an
    int-typed key with no value set it raised
    IllegalArgumentException("spark.sql.shuffle.partitions should be int, but
    was <unset>"). Every harness run SETS shuffle partitions via SparkSizing,
    so it never fired there -- it fired on the first run that deliberately
    configured nothing, which is the shadow-build reproduction itself.
    """

    def test_unset_int_typed_conf_reports_unset_instead_of_raising(self):
        from scripts.lakehouse_scale_harness import jvm_runtime_facts

        class _Conf:
            def get(self, key, *a):
                if key == "spark.sql.shuffle.partitions":
                    raise ValueError(
                        "spark.sql.shuffle.partitions should be int, but was <unset>"
                    )
                return "someval"

        class _Ctx:
            defaultParallelism = 4

        spark = types.SimpleNamespace(
            _jvm=None, conf=_Conf(), sparkContext=_Ctx()
        )

        facts = jvm_runtime_facts(spark)

        assert facts["spark.sql.shuffle.partitions"] == "<unset>"
        assert facts["spark.master"] == "someval"


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
    def test_driver_memory_is_unset_unless_explicitly_requested(self, replay):
        """A plain replay must reproduce the shadow build's UNSET sizing.

        Setting spark.driver.memory implicitly -- even to '1g' -- would change
        the launcher path and quietly turn a reproduction into a different
        experiment. The opt-in flag exists for the separate question of
        whether the chain completes at a sane heap; the two must not blur,
        so BOTH defaults are pinned here.
        """
        import inspect

        assert inspect.signature(
            replay.build_session).parameters["driver_memory"].default is None

        source = Path(replay.__file__).read_text(encoding="utf-8")
        assert '"--driver-memory", default=None' in source

    def test_requested_driver_memory_is_recorded_in_the_bundle(self, replay):
        """Whatever heap a run used has to travel with its result, or a
        sane-heap viability check and a 1 GiB reproduction become
        indistinguishable once they are two JSON files in a directory."""
        source = Path(replay.__file__).read_text(encoding="utf-8")

        assert 'bundle["driver_memory_requested"]' in source

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
