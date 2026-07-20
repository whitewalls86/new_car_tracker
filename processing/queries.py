"""
SQL query loader for the processing service.

Loads all .sql files from processing/sql/ at import time and exposes them
as module-level constants. File name (without extension) becomes the constant
name in UPPER_CASE.

Usage:
    from processing.queries import UPSERT_PRICE_OBSERVATION, CLAIM_ARTIFACTS
"""
from pathlib import Path

from shared.query_loader import load_query

_SQL_DIR = Path(__file__).parent / "sql"


def _load(name: str) -> str:
    return load_query(_SQL_DIR, name)


CLAIM_ARTIFACTS = _load("claim_artifacts")
MARK_ARTIFACT_STATUS = _load("mark_artifact_status")
INSERT_ARTIFACT_EVENT = _load("insert_artifact_event")
BATCH_LOOKUP_VIN_TO_LISTING = _load("batch_lookup_vin_to_listing")
UPSERT_PRICE_OBSERVATION = _load("upsert_price_observation")
DELETE_PRICE_OBSERVATION = _load("delete_price_observation")
UPSERT_VIN_TO_LISTING = _load("upsert_vin_to_listing")
CLEAR_BLOCKED_COOLDOWN = _load("clear_blocked_cooldown")
RELEASE_DETAIL_CLAIMS = _load("release_detail_claims")
INSERT_DETAIL_CLAIM_EVENT = _load("insert_detail_claim_event")
LOOKUP_VIN_COLLISION = _load("lookup_vin_collision")
DELETE_PRICE_OBSERVATION_BY_VIN = _load("delete_price_observation_by_vin")
INSERT_PRICE_OBSERVATION_EVENT = _load("insert_price_observation_event")
INSERT_VIN_TO_LISTING_EVENT = _load("insert_vin_to_listing_event")
UPSERT_TRACKED_MODEL = _load("upsert_tracked_model")
GET_TRACKED_MODELS = _load("get_tracked_models")
INSERT_TRACKED_MODEL_EVENT = _load("insert_tracked_model_event")
