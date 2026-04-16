"""
Processing service — artifact parsing and observation writes for cartracker.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI

_LOG_PATH = os.getenv("LOG_PATH", "/usr/app/logs/app.log")
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

app = FastAPI()


@app.get("/health")
def health():
    return {"ok": True}
