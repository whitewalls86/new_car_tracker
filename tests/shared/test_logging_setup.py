"""Unit tests for shared/logging_setup.py.

Covers the handler wiring only (file always, stream unless disabled) — not
log content, which is exercised by the individual modules that log.
"""
import logging
from logging.handlers import RotatingFileHandler

from shared.logging_setup import configure_logging


def test_configure_logging_adds_file_and_stream_handlers_by_default(tmp_path, monkeypatch):
    """Other test modules may import something that already called
    configure_logging() earlier in the pytest session (root handlers persist
    process-wide), so this asserts on the handlers *newly added* by this
    call, not on the full — possibly pre-populated — root handler list."""
    monkeypatch.setenv("LOG_PATH", str(tmp_path / "app.log"))
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    try:
        configure_logging()
        added = [h for h in root.handlers if h not in original_handlers]
        added_types = [type(h) for h in added]
        assert RotatingFileHandler in added_types
        assert logging.StreamHandler in added_types
        assert root.level == logging.INFO
    finally:
        root.handlers = original_handlers


def test_configure_logging_stream_false_skips_stream_handler(tmp_path, monkeypatch):
    monkeypatch.setenv("LOG_PATH", str(tmp_path / "app.log"))
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    try:
        configure_logging(stream=False)
        added = [h for h in root.handlers if h not in original_handlers]
        added_types = [type(h) for h in added]
        assert logging.StreamHandler not in added_types
        assert RotatingFileHandler in added_types
    finally:
        root.handlers = original_handlers
