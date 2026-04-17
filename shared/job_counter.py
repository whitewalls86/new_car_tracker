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
