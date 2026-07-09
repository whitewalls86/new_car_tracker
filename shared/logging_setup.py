import json
import logging
import os
import sys
from logging.handlers import RotatingFileHandler


class _JsonFormatter(logging.Formatter):
    def format(self, record: logging.LogRecord) -> str:
        return json.dumps({
            "ts": self.formatTime(record),
            "level": record.levelname,
            "logger": record.name,
            "msg": record.getMessage(),
        })


def configure_logging(stream: bool = True) -> None:
    """Configure the root logger with a rotating JSON file handler and,
    unless *stream* is False, a stdout handler.

    The stream handler is what makes `docker logs -f <container>` useful —
    the rotating file alone is only visible via the ops log-tail endpoint
    (see ops/routers/admin.py) or by exec-ing into the container.
    """
    root = logging.getLogger()
    log_path = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
    os.makedirs(os.path.dirname(log_path), exist_ok=True)
    file_handler = RotatingFileHandler(log_path, maxBytes=5_000_000, backupCount=3)
    file_handler.setFormatter(_JsonFormatter())
    root.addHandler(file_handler)

    if stream:
        stream_handler = logging.StreamHandler(sys.stdout)
        stream_handler.setFormatter(
            logging.Formatter("%(asctime)s %(levelname)-7s %(name)s: %(message)s")
        )
        root.addHandler(stream_handler)

    root.setLevel(logging.INFO)
