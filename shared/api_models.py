"""Response models for the shapes more than one service returns.

Plan 162 Stage AA, gap G31.

**FastAPI made the request side mandatory and the response side optional, and
this repository took the default on every route.** ``ops/routers/scrape.py``
holds the asymmetry in one file: ``ReleaseRequest`` and ``ReleaseResult`` are
declared models, and the endpoint that consumes them answers ``Dict[str,
Any]``. So ``contracts/ops.json`` describes what a caller must send precisely
and what it will get back as ``{"additionalProperties": true}`` -- a schema
that permits every body, including the ones the handler cannot produce.

That is not a documentation gap. It is why
``airflow/dags/scrape_detail_pages.py`` could read a ``status`` key from
``POST /scrape/claims/release`` for four months and log ``status=None`` on
every run: no artifact anywhere said which keys that endpoint returns, so
nothing could contradict the caller.

**Declaring a model changes behaviour, and that is the hazard this module is
written against.** ``response_model`` makes FastAPI *filter* the response to
the declared fields, so a model missing a key some caller reads deletes that
key in production, silently -- this plan's own defect rebuilt inside its
remedy. Every model here is derived from what its handler actually returns,
and a field that only some return paths set is declared optional rather than
dropped.

**Not imported by ``container_health``**, which copies only its own package
into its image on purpose: it holds the Docker socket grant and its Dockerfile
records that carrying as little else as possible is part of the point. It
declares the same two shapes locally. That is a boundary being honoured, not a
duplication that escaped notice.
"""
from __future__ import annotations

from pydantic import BaseModel


class HealthResponse(BaseModel):
    """``GET /health`` everywhere: process liveness, never dependency health."""

    ok: bool


class ReadyResponse(BaseModel):
    """``GET /ready`` for the services whose evidence is ``job_snapshot()``.

    ``archiver``, ``processing`` and ``dbt_runner`` return exactly this.
    ``scraper`` returns it with two extra per-surface fields and declares its
    own model for that reason.
    """

    ready: bool
    active_jobs: int
    oldest_started_at: str | None = None


class NotReadyDetail(ReadyResponse):
    """The body inside a ``/ready`` 503, which is evidence rather than an error.

    ``ops/coordination_drain.py:_service_jobs`` reads ``active_jobs`` out of
    this and counts a 503 as *known positive* evidence -- a draining service
    reporting two jobs in flight is answering the question, not failing. The
    reason a 503 carries a full body here rather than a message is that the
    drain aggregator is fail-closed: an unreadable body becomes ``unknown``,
    which blocks a deploy.
    """

    reason: str


class NotReadyResponse(BaseModel):
    """``/ready``'s 503 as it reaches the wire.

    ``HTTPException(detail=...)`` nests the payload under ``detail``, which is
    why the aggregator unwraps one level before reading the counts. Declared so
    the contract says so.
    """

    detail: NotReadyDetail


class ErrorResponse(BaseModel):
    """FastAPI's uniform error body: ``{"detail": "<message>"}``.

    Declared once and shared by every route whose refusals carry a string. The
    handful whose ``detail`` is a *structured* dict a caller indexes into
    declare their own model instead -- those are the ones where the shape is
    load-bearing.
    """

    detail: str
