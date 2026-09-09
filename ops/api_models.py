"""What ``ops``'s routes return.

Plan 162 Stage AA, gap G31. See ``shared/api_models.py`` for the filtering
hazard these are written against, and
``tests/plugins/response_model_fidelity.py`` for what checks them.

**This service is where the asymmetry was most visible.**
``ops/routers/scrape.py`` declares ``ReleaseRequest`` and ``ReleaseResult`` for
what a caller must *send*, and answers ``Dict[str, Any]`` for what it gets
back. So ``contracts/ops.json`` described the request precisely and the
response as "any object at all" -- which is why
``airflow/dags/scrape_detail_pages.py`` can read a ``status`` key off
``POST /scrape/claims/release`` that the endpoint has never returned, and log
``status=None`` on every run, with no artifact anywhere able to contradict it.
:class:`ReleaseClaimsResponse` below is what that endpoint actually answers.

**The coordination record is modelled from its own SELECT.**
``/coordination/status`` returns ``dict(row)`` over
``ops/sql/select_coordination_state.sql``, which names sixteen columns
explicitly rather than using ``SELECT *``. So the response shape is derivable
and stable, and :class:`CoordinationState` is that column list. A column added
to the SELECT and not to this model is caught by the fidelity plugin the moment
any test exercises the route with it.
"""
from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel

# ---------------------------------------------------------------------------
# Coordination
# ---------------------------------------------------------------------------

class CoordinationState(BaseModel):
    """``GET /coordination/status`` -- the authoritative record.

    The operator-facing view, wider than the metrics query beside it: every
    phase timestamp and the request's own narrative. Timestamps are ISO strings
    rather than datetimes because ``_iso`` has already converted them.
    """

    kind: str | None = None
    phase: str | None = None
    generation: int | None = None
    requested_by: str | None = None
    reason: str | None = None
    targets: Any | None = None
    scope: Any | None = None
    requested_at: str | None = None
    draining_at: str | None = None
    active_at: str | None = None
    validating_at: str | None = None
    completed_at: str | None = None
    expected_work: Any | None = None
    manifest_location: str | None = None
    operator_notes: str | None = None
    updated_at: str | None = None


class LocalDrainResponse(BaseModel):
    """``GET /coordination/local-drain`` -- this process's own job evidence.

    ``ops`` cannot ask itself over HTTP for the reason every other service is
    asked that way, so it reports ``job_snapshot()`` directly and labels the
    source. ``known`` is always ``True`` here: an in-process count cannot be
    unavailable the way a service across the network can.
    """

    source: str
    known: bool
    active_jobs: int
    oldest_started_at: str | None = None


class PhaseResponse(BaseModel):
    """The phase transitions that answer with the phase they reached.

    ``/coordination/begin-drain``, ``/coordination/cancel`` and
    ``/coordination/begin-validation``.
    """

    phase: str


class RequestedResponse(BaseModel):
    """``POST /coordination/request`` -- what was recorded, echoed back."""

    kind: str
    phase: str
    targets: Any | None = None
    scope: Any | None = None


class CompletedResponse(BaseModel):
    """``POST /coordination/complete`` -- the phase it landed in, and which generation."""

    phase: str
    generation: int | None = None


class HostEvidenceResponse(BaseModel):
    """``POST /coordination/host-evidence`` -- the receipt for a submission.

    ``gates`` and ``evidence_digests`` are echoed from the request so the
    caller can see exactly what was stored under ``evidence_id``.
    """

    evidence_id: Any
    generation: int | None = None
    actor: str | None = None
    submitted_at: str | None = None
    gates: Any | None = None
    evidence_digests: Any | None = None


class DrainStatusResponse(BaseModel):
    """``GET /coordination/drain-status`` -- fail-closed drain aggregation.

    ``sources`` carries one entry per evidence source, each of which reports
    ``known`` or ``unknown`` rather than guessing a zero -- an unreadable
    source blocks the deploy, which is the whole design.
    """

    phase: str | None = None
    scope: Any | None = None
    drained: bool | None = None
    blockers: List[Any] = []
    sources: Any | None = None


class AuthorizeResponse(BaseModel):
    """``POST /coordination/authorize`` -- the phase, and the drain that justified it."""

    phase: str
    drain: DrainStatusResponse | None = None


class ReleaseStatusResponse(BaseModel):
    """``GET /coordination/release-status`` -- the post-deploy gates."""

    phase: str | None = None
    kind: str | None = None
    release_ready: bool | None = None
    blockers: List[Any] = []
    gates: Any | None = None


# ---------------------------------------------------------------------------
# Deploy intent
# ---------------------------------------------------------------------------

class IntentStatusResponse(BaseModel):
    """``GET /deploy/status``.

    ``number_running`` and ``min_started_at`` come from the joined in-flight
    count and are absent from the no-row and read-failed shapes, which is why
    they are optional and the other four are not. ``pause_long_jobs`` is the
    column default when there is no intent -- shape, not a claim about a
    deploy, as ``_no_intent`` says.
    """

    intent: str
    requested_at: str | None = None
    requested_by: str | None = None
    pause_long_jobs: bool | None = None
    number_running: int | None = None
    min_started_at: str | None = None


# ---------------------------------------------------------------------------
# Maintenance
# ---------------------------------------------------------------------------

class AffectedResponse(BaseModel):
    """``POST /maintenance/expire-orphan-detail-claims`` -- rows the statement touched."""

    affected: int


class ReapStuckResponse(BaseModel):
    """``POST /maintenance/reap-stuck-processing``."""

    stuck: int
    retried: int
    skipped: int


class EvictCooldownsResponse(BaseModel):
    """``POST /maintenance/evict-delisted-cooldowns``."""

    evicted: int


class ReconcileCohortsResponse(BaseModel):
    """``POST /maintenance/reconcile-cooldown-cohorts``."""

    counted: int
    live: int
    pending_cleared: int
    cleared: int


# ---------------------------------------------------------------------------
# Scrape orchestration
# ---------------------------------------------------------------------------

class RotationResponse(BaseModel):
    """``POST /scrape/rotation/advance`` -- three exits, one shape.

    A slot that is due answers ``slot``, ``run_id`` and ``configs``; a request
    that arrived too soon answers ``reason`` and ``last_run_minutes_ago``
    alongside empty versions of the first three; an empty queue answers the
    first three with nothing in them. The two refusal fields are optional
    because only one exit sets them, and declaring them here is what makes
    "why did I get no configs" answerable from the contract.
    """

    slot: Any | None = None
    run_id: str | None = None
    configs: List[Dict[str, Any]] = []
    reason: str | None = None
    last_run_minutes_ago: float | None = None


class ClaimBatchResponse(BaseModel):
    """``POST /scrape/claims/claim-batch``.

    ``listings`` is built from the claim statement's own column list, so its
    element shape belongs to that SQL rather than to this file and stays
    free-form.
    """

    run_id: str
    listings: List[Dict[str, Any]]


class ReleaseClaimsResponse(BaseModel):
    """``POST /scrape/claims/release`` -- and there is no ``status`` key.

    Stated plainly because a caller has been reading one for months.
    ``airflow/dags/scrape_detail_pages.py`` logs ``result.get("status")`` from
    this endpoint and has logged ``status=None`` every run since the DAG was
    written. The repair is not to add the field -- it is this model, which
    makes the absence checkable instead of leaving it to be discovered.
    """

    run_id: str
    total: int
    errors: int
    claims_released: int
    fetches_recorded: int
