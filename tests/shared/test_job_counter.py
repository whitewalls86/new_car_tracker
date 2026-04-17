"""Unit tests for shared.job_counter."""
import threading

import pytest

import shared.job_counter as jc
from shared.job_counter import active_job, is_idle


@pytest.fixture(autouse=True)
def reset_counter():
    """Reset the global counter before every test."""
    with jc._lock:
        jc._count = 0
    yield
    with jc._lock:
        jc._count = 0


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
