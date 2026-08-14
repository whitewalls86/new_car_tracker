import threading
from contextlib import contextmanager

_lock = threading.Lock()
_count = 0


@contextmanager
def active_job():
    global _count
    with _lock:
        _count += 1
    try:
        yield
    finally:
        with _lock:
            _count -= 1


def is_idle() -> bool:
    with _lock:
        return _count == 0


class JobInFlight(RuntimeError):
    """Raised when a named single-flight job is already running."""

    def __init__(self, job: str):
        super().__init__(f"{job} is already in flight")
        self.job = job


_single_flight_lock = threading.Lock()
_in_flight: set = set()


@contextmanager
def single_flight(job: str):
    """Refuse to run *job* concurrently with itself. Non-blocking.

    ``active_job`` counts work for ``/ready`` and refuses nothing. This does
    refuse: a second entry raises :class:`JobInFlight` immediately rather than
    waiting, so a caller can turn it into a 409 and skip.

    Keyed **per job name**, so unrelated jobs never block each other — a pack
    and a prune on different months are a normal thing to run at once, and a
    global lock would make working a backlog by hand impossible.

    This is an in-process guard, which covers the case it was built for: an
    HTTP call that dies on a dropped connection leaves the job running, and the
    retry must not start a second one. It does **not** see other processes —
    a ``docker exec`` CLI run in the same container is invisible to it, and
    invisible to the CLI in turn. Two packers on one month would race on
    ``next_seq`` and overwrite each other's packs; the sidecar/pack
    disagreement check refuses to delete from the result, so that is wasted
    work and a blocked pack rather than lost data. Do not hand-run a month a
    scheduled run might also take.
    """
    with _single_flight_lock:
        if job in _in_flight:
            raise JobInFlight(job)
        _in_flight.add(job)
    try:
        yield
    finally:
        with _single_flight_lock:
            _in_flight.discard(job)
