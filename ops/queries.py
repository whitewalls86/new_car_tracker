from pathlib import Path

_SQL = Path(__file__).parent / "sql"


def _q(name: str) -> str:
    return (_SQL / f"{name}.sql").read_text()


EXPIRE_ORPHAN_RUNS              = _q("expire_orphan_runs")
EXPIRE_ORPHAN_PROCESSING_RUNS   = _q("expire_orphan_processing_runs")
RESET_STALE_ARTIFACT_PROCESSING = _q("reset_stale_artifact_processing")
EXPIRE_ORPHAN_DETAIL_CLAIMS     = _q("expire_orphan_detail_claims")
EXPIRE_ORPHAN_SCRAPE_JOBS       = _q("expire_orphan_scrape_jobs")
