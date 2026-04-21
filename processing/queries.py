"""
SQL query loader for the processing service.

Loads all .sql files from processing/sql/ at import time and exposes them
as module-level constants. File name (without extension) becomes the constant
name in UPPER_CASE.

Usage:
    from processing.queries import UPSERT_PRICE_OBSERVATION, CLAIM_ARTIFACTS
"""
from pathlib import Path

_SQL_DIR = Path(__file__).parent / "sql"


def _load(filename: str) -> str:
    return (_SQL_DIR / filename).read_text()


CLAIM_ARTIFACTS = _load("claim_artifacts.sql")
MARK_ARTIFACT_STATUS = _load("mark_artifact_status.sql")
INSERT_ARTIFACT_EVENT = _load("insert_artifact_event.sql")
BATCH_LOOKUP_VIN_TO_LISTING = _load("batch_lookup_vin_to_listing.sql")
UPSERT_PRICE_OBSERVATION = _load("upsert_price_observation.sql")
DELETE_PRICE_OBSERVATION = _load("delete_price_observation.sql")
UPSERT_VIN_TO_LISTING = _load("upsert_vin_to_listing.sql")
UPSERT_BLOCKED_COOLDOWN = _load("upsert_blocked_cooldown.sql")
CLEAR_BLOCKED_COOLDOWN = _load("clear_blocked_cooldown.sql")
RELEASE_DETAIL_CLAIMS = _load("release_detail_claims.sql")
INSERT_DETAIL_CLAIM_EVENT = _load("insert_detail_claim_event.sql")
INSERT_BLOCKED_COOLDOWN_EVENT = _load("insert_blocked_cooldown_event.sql")
LOOKUP_VIN_COLLISION = _load("lookup_vin_collision.sql")
DELETE_PRICE_OBSERVATION_BY_VIN = _load("delete_price_observation_by_vin.sql")
GET_BLOCKED_COOLDOWN_ATTEMPTS = _load("get_blocked_cooldown_attempts.sql")
INSERT_PRICE_OBSERVATION_EVENT = _load("insert_price_observation_event.sql")
INSERT_VIN_TO_LISTING_EVENT = _load("insert_vin_to_listing_event.sql")
UPSERT_TRACKED_MODEL = _load("upsert_tracked_model.sql")
GET_TRACKED_MODELS = _load("get_tracked_models.sql")
INSERT_TRACKED_MODEL_EVENT = _load("insert_tracked_model_event.sql")
