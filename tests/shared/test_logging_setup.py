"""Unit tests for shared/logging_setup.py.

Covers the handler wiring only (file always, stream unless disabled) — not
log content, which is exercised by the individual modules that log.
"""

import json
import logging
from logging.handlers import RotatingFileHandler

from shared.logging_setup import _JsonFormatter, configure_logging


def _record():
    return logging.LogRecord(
        name="test.logger",
        level=logging.WARNING,
        pathname=__file__,
        lineno=1,
        msg="event %s",
        args=("happened",),
        exc_info=None,
        func=None,
        sinfo=None,
    )


def test_json_formatter_preserves_the_original_four_field_shape_without_extra():
    payload = json.loads(_JsonFormatter().format(_record()))

    assert list(payload) == ["ts", "level", "logger", "msg"]
    assert payload["level"] == "WARNING"
    assert payload["logger"] == "test.logger"
    assert payload["msg"] == "event happened"


def test_json_formatter_carries_only_bounded_allowlisted_scalar_extra_fields():
    record = _record()
    record.trace_id = "trace-123"
    record.run_id = "run-456"
    record.duration_ms = 17
    record.password = "must-never-be-emitted"
    record.request_headers = {"Authorization": "secret"}
    record.event = "x" * 513

    payload = json.loads(_JsonFormatter().format(record))

    assert payload["trace_id"] == "trace-123"
    assert payload["run_id"] == "run-456"
    assert payload["duration_ms"] == 17
    assert "password" not in payload
    assert "request_headers" not in payload
    assert "event" not in payload


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
