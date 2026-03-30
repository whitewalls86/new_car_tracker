"""
Pipeline Ops — admin UI and deploy coordination for cartracker.
"""
import logging
import os
from logging.handlers import RotatingFileHandler

from fastapi import FastAPI

from routers.deploy import router as deploy_router
from routers.admin import router as admin_router

_LOG_PATH = "/usr/app/logs/app.log"
os.makedirs(os.path.dirname(_LOG_PATH), exist_ok=True)
_log_handler = RotatingFileHandler(_LOG_PATH, maxBytes=5_000_000, backupCount=3)
_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s: %(message)s"))
logging.getLogger().addHandler(_log_handler)
logging.getLogger().setLevel(logging.INFO)

app = FastAPI()
app.include_router(deploy_router)
app.include_router(admin_router, prefix="/admin")
