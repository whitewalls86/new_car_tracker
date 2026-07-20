from pathlib import Path

from shared.query_loader import load_query

_SQL_DIR = Path(__file__).parent / "sql"


def _q(name: str) -> str:
    return load_query(_SQL_DIR, name)


EXPIRE_ORPHAN_DETAIL_CLAIMS = _q("expire_orphan_detail_claims")

# Stuck-processing artifact reaper
SELECT_STUCK_PROCESSING_ARTIFACTS = _q("select_stuck_processing_artifacts")
MARK_ARTIFACT_STATUS = _q("mark_artifact_status")
INSERT_ARTIFACT_EVENT = _q("insert_artifact_event")

# Blocked-cooldown cleanup / reconciliation
EVICT_DELISTED_COOLDOWNS = _q("evict_delisted_cooldowns")
INSERT_BLOCKED_COOLDOWN_CLEARED_EVENT = _q("insert_blocked_cooldown_cleared_event")
SELECT_LIVE_COOLDOWN_LISTINGS = _q("select_live_cooldown_listings")
SELECT_PENDING_CLEARED_LISTINGS = _q("select_pending_cleared_listings")
