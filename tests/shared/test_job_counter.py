"""Unit tests for shared.job_counter."""
import threading

import pytest

import shared.job_counter as jc
from shared.job_counter import JobInFlight, active_job, is_idle, single_flight


@pytest.fixture(autouse=True)
def reset_counter():
    """Reset the global counter and in-flight set before every test."""
    with jc._lock:
        jc._count = 0
    jc._in_flight.clear()
    yield
    with jc._lock:
        jc._count = 0
    jc._in_flight.clear()


class TestIsIdle:
    def test_idle_when_no_jobs(self):
        assert is_idle() is True

    def test_not_idle_inside_active_job(self):
        with active_job():
            assert is_idle() is False

    def test_idle_again_after_job_exits(self):
        with active_job():
            pass
        assert is_idle() is True


class TestActiveJob:
    def test_counter_increments_on_enter(self):
        with active_job():
            assert jc._count == 1

    def test_counter_decrements_on_exit(self):
        with active_job():
            pass
        assert jc._count == 0

    def test_nested_jobs_accumulate(self):
        with active_job():
            with active_job():
                assert jc._count == 2
            assert jc._count == 1
        assert jc._count == 0

    def test_counter_decrements_on_exception(self):
        with pytest.raises(RuntimeError):
            with active_job():
                raise RuntimeError("boom")
        assert jc._count == 0

    def test_is_idle_false_during_exception_handling(self):
        try:
            with active_job():
                assert is_idle() is False
                raise RuntimeError("boom")
        except RuntimeError:
            pass
        assert is_idle() is True


class TestThreadSafety:
    def test_concurrent_jobs_tracked_correctly(self):
        """Multiple threads entering active_job simultaneously are all counted."""
        barrier1 = threading.Barrier(5)
        barrier2 = threading.Barrier(5)
        observed = []

        def _job():
            with active_job():
                barrier1.wait()          # all 5 inside active_job before any observes
                observed.append(jc._count)
                barrier2.wait()          # all 5 have observed before any exits

        threads = [threading.Thread(target=_job) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()

        assert all(c == 5 for c in observed)
        assert jc._count == 0

    def test_idle_only_after_all_threads_finish(self):
        ready = threading.Event()
        done = threading.Event()

        def _slow_job():
            with active_job():
                ready.set()
                done.wait()

        t = threading.Thread(target=_slow_job)
        t.start()
        ready.wait()
        assert is_idle() is False
        done.set()
        t.join()
        assert is_idle() is True


# ---------------------------------------------------------------------------
# single_flight (Plan 131 Stage 5 D3a)
#
# active_job counts and refuses nothing. This refuses: a multi-hour HTTP call
# that dies on a dropped connection leaves the job running, and the DAG's retry
# must not start a second packer on the same bucket.
# ---------------------------------------------------------------------------

class TestSingleFlight:
    def test_a_second_entry_is_refused(self):
        with single_flight("pack_bronze"):
            with pytest.raises(JobInFlight):
                with single_flight("pack_bronze"):
                    pass

    def test_the_refusal_names_the_job(self):
        with single_flight("pack_bronze"):
            with pytest.raises(JobInFlight) as exc:
                with single_flight("pack_bronze"):
                    pass
        assert exc.value.job == "pack_bronze"
        assert "pack_bronze" in str(exc.value)

    def test_the_slot_is_released_on_exit(self):
        with single_flight("pack_bronze"):
            pass
        with single_flight("pack_bronze"):
            assert "pack_bronze" in jc._in_flight

    def test_the_slot_is_released_on_exception(self):
        with pytest.raises(RuntimeError):
            with single_flight("pack_bronze"):
                raise RuntimeError("boom")
        # A run that crashes must not wedge the job out of existence until the
        # container restarts.
        assert jc._in_flight == set()

    def test_different_jobs_do_not_block_each_other(self):
        # A pack and a prune on different months are a normal thing to run at
        # once; a global lock would make working a backlog by hand impossible.
        with single_flight("pack_bronze"):
            with single_flight("pack_prune"):
                assert jc._in_flight == {"pack_bronze", "pack_prune"}

    def test_refusal_does_not_release_the_holder(self):
        with single_flight("pack_bronze"):
            with pytest.raises(JobInFlight):
                with single_flight("pack_bronze"):
                    pass
            # The loser's __exit__ must not discard the winner's slot.
            assert "pack_bronze" in jc._in_flight

    def test_it_refuses_across_threads(self):
        ready = threading.Event()
        done = threading.Event()
        refused = []

        def _holder():
            with single_flight("pack_bronze"):
                ready.set()
                done.wait()

        t = threading.Thread(target=_holder)
        t.start()
        ready.wait()
        try:
            with single_flight("pack_bronze"):
                refused.append(False)
        except JobInFlight:
            refused.append(True)
        done.set()
        t.join()

        assert refused == [True]
