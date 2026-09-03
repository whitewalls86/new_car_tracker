"""Cooperative pause for long-running jobs when a deploy is imminent.

Plan 131 Stage 5 D3b. A month-scale pack or prune runs for hours, and a backlog
run can run for ten. A deploy landing mid-run either restarts the container
underneath it or runs migrations against a job that is actively reading and
deleting. The job survives — a pack without a sidecar is an orphan, reported
and never deleted from, and the deleter deletes each object at most once — but
an hour of in-flight work is thrown away and the failure looks like a crash
rather than a deploy.

Nothing here kills anything and no signal handling is involved. The jobs ask,
at a boundary they already have, whether a deploy wants them to stop, and
return cleanly with everything completed so far already durable.
"""
import logging

from shared.db import db_cursor
from shared.queries import SELECT_DEPLOY_INTENT_PAUSE

logger = logging.getLogger(__name__)


def long_jobs_paused() -> bool:
    """True when a pending deploy wants long jobs to stop at their next boundary.

    **Fails open, deliberately.** Any error returns ``False`` and logs at
    WARNING. A Postgres blip must not silently stop a ten-hour job: being wrong
    in this direction costs a deploy that races a running job, which is exactly
    the situation that exists today. Being wrong in the other direction throws
    away hours of work because a connection dropped.
    """
    try:
        with db_cursor(error_context="Long-Jobs-Paused") as cur:
            cur.execute(SELECT_DEPLOY_INTENT_PAUSE)
            row = cur.fetchone()
    except Exception as exc:  # noqa: BLE001 - failing open is the whole point.
        logger.warning(
            "deploy_intent: could not read the pause flag (%s) — continuing. "
            "This fails open on purpose: a DB blip must not stop a long job.",
            exc,
        )
        return False
    return bool(row and row[0])
