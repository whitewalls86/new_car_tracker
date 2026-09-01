from pathlib import Path

from shared import queries as shared_queries
from shared.query_loader import load_query

_SQL_DIR = Path(__file__).parent / "sql"


def _q(name: str) -> str:
    return load_query(_SQL_DIR, name)


EXPIRE_ORPHAN_DETAIL_CLAIMS = _q("expire_orphan_detail_claims")

# Stuck-processing artifact reaper
SELECT_STUCK_PROCESSING_ARTIFACTS = _q("select_stuck_processing_artifacts")

# Blocked-cooldown cleanup / reconciliation
EVICT_DELISTED_COOLDOWNS = _q("evict_delisted_cooldowns")
SELECT_LIVE_COOLDOWN_LISTINGS = _q("select_live_cooldown_listings")
SELECT_PENDING_CLEARED_LISTINGS = _q("select_pending_cleared_listings")
INSERT_BLOCKED_COOLDOWN_EVENTS_BATCH = _q("insert_blocked_cooldown_events_batch")

# Coordination and access control
SELECT_COORDINATION_STATE_METRICS = _q("select_coordination_state_metrics")
SELECT_USER_ROLE = _q("select_user_role")

# Coordination state machine (ops/routers/coordination.py, Plan 142)
ACQUIRE_COORDINATION_LOCK = _q("acquire_coordination_lock")
SELECT_COORDINATION_STATE = _q("select_coordination_state")
SELECT_COORDINATION_STATE_ACTOR = _q("select_coordination_state_actor")
SELECT_COORDINATION_STATE_KIND = _q("select_coordination_state_kind")
INSERT_COORDINATION_STATE_EVENT = _q("insert_coordination_state_event")
INSERT_COORDINATION_RELEASE_EVIDENCE = _q("insert_coordination_release_evidence")
REQUEST_COORDINATION_STATE = _q("request_coordination_state")
# Both carry a {timestamp_column} placeholder filled from the closed literal
# table coordination._TRANSITIONS. Call sites must .format() before executing.
RELEASE_COORDINATION_STATE = _q("release_coordination_state")
ADVANCE_COORDINATION_STATE = _q("advance_coordination_state")
SELECT_COMPLETION_RECEIPT = _q("select_completion_receipt")
SELECT_RELEASE_EVIDENCE = _q("select_release_evidence")
COMPLETE_COORDINATION_STATE = _q("complete_coordination_state")
INSERT_COMPLETION_RECEIPT = _q("insert_completion_receipt")
CANCEL_COORDINATION_STATE = _q("cancel_coordination_state")
AUTHORIZE_COORDINATION_STATE = _q("authorize_coordination_state")

# Legacy deploy facade (ops/routers/deploy.py)
SELECT_DEPLOY_INTENT_STATUS = _q("select_deploy_intent_status")
SELECT_COORDINATION_STATE_FOR_DEPLOY = _q("select_coordination_state_for_deploy")
SET_DEPLOY_INTENT = _q("set_deploy_intent")
REQUEST_DEPLOY_COORDINATION = _q("request_deploy_coordination")
CLEAR_DEPLOY_INTENT = _q("clear_deploy_intent")
RELEASE_DEPLOY_COORDINATION = _q("release_deploy_coordination")

# User management and access requests (ops/routers/users.py)
SELECT_PENDING_REQUEST_FOR_EMAIL = _q("select_pending_request_for_email")
SELECT_PENDING_REQUEST_ID_FOR_EMAIL = _q("select_pending_request_id_for_email")
INSERT_ACCESS_REQUEST = _q("insert_access_request")
SELECT_AUTHORIZED_USERS = _q("select_authorized_users")
UPDATE_USER_ROLE = _q("update_user_role")
DELETE_AUTHORIZED_USER = _q("delete_authorized_user")
SELECT_ACCESS_REQUESTS = _q("select_access_requests")
SELECT_PENDING_REQUEST_DETAILS = _q("select_pending_request_details")
UPSERT_AUTHORIZED_USER = _q("upsert_authorized_user")
APPROVE_ACCESS_REQUEST = _q("approve_access_request")
SELECT_PENDING_REQUEST_NOTIFICATION_EMAIL = _q("select_pending_request_notification_email")
DENY_ACCESS_REQUEST = _q("deny_access_request")

# Scrape rotation and detail claims (ops/routers/scrape.py)
SELECT_LAST_QUEUED_AT = _q("select_last_queued_at")
SELECT_NEXT_ROTATION_SLOT = _q("select_next_rotation_slot")
SELECT_LEGACY_SEARCH_CONFIG = _q("select_legacy_search_config")
MARK_SEARCH_CONFIG_QUEUED = _q("mark_search_config_queued")
MARK_ROTATION_SLOT_QUEUED = _q("mark_rotation_slot_queued")
SELECT_ROTATION_SLOT_CONFIGS = _q("select_rotation_slot_configs")
CLAIM_DETAIL_SCRAPE_BATCH = _q("claim_detail_scrape_batch")
DELETE_DETAIL_SCRAPE_CLAIMS = _q("delete_detail_scrape_claims")
RECORD_DETAIL_FETCHES = _q("record_detail_fetches")

# Issued by this service and by the other one, against tables they share, so
# the statement lives in shared/sql/ and there is exactly one copy. Re-exported
# here rather than imported at each call site: this module is the service's
# query surface, and what it exports should not depend on where a statement
# happens to be filed.
MARK_ARTIFACT_STATUS = shared_queries.MARK_ARTIFACT_STATUS
INSERT_ARTIFACT_EVENT = shared_queries.INSERT_ARTIFACT_EVENT
INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT = (
    shared_queries.INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT
)
