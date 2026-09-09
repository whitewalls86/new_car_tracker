"""What ``scraper``'s routes return.

Plan 162 Stage AA, gap G31. See ``shared/api_models.py`` for the filtering
hazard these are written against, and
``tests/plugins/response_model_fidelity.py`` for what checks them.

**``/ready`` does not use the shared model, and the difference is the point.**
Every other service answers drain evidence as one ``active_jobs`` count.
``scraper`` runs two surfaces that drain independently -- detail fetches and
listing fetches -- and ``ops/coordination_drain.py`` reads them separately
through ``SERVICE_EVIDENCE``'s ``scraper_detail_jobs`` and
``scraper_listing_jobs``. A model that flattened them to the shared shape would
be declaring a smaller thing than the endpoint returns.

**``Job`` is a union of two job types.** A listing fetch carries ``search_key``,
``scope``, ``attempt`` and ``page_1_blocked``; a detail batch carries
``batch_id`` and ``listing_count``; neither carries the other's. ``job_type``
says which, and the fields that belong to one are optional rather than split
into two models -- ``/scrape_results/jobs/completed`` returns both kinds in one
list, so one model is what the endpoint actually answers.

``artifacts`` is optional for a different reason: ``GET /scrape_results/jobs``
strips it deliberately, being a debugging view, while
``/scrape_results/jobs/completed`` keeps it because the poller needs it.
"""
from __future__ import annotations

from typing import Any, Dict, List

from pydantic import BaseModel


class ScraperReadyResponse(BaseModel):
    """``GET /ready`` -- drain evidence, partitioned by surface.

    ``active_jobs`` and ``oldest_started_at`` are the totals the shared shape
    would carry; the two ``*_by_surface`` maps are what makes this endpoint
    answerable for two surfaces at once.
    """

    ready: bool
    active_jobs: int
    oldest_started_at: str | None = None
    active_by_surface: Dict[str, int]
    oldest_by_surface: Dict[str, str | None]


class ScraperNotReadyResponse(BaseModel):
    """``/ready``'s 503: the same evidence, nested under ``detail``.

    ``ops/coordination_drain.py:_service_jobs`` unwraps one level and reads
    ``active_by_surface`` out of it, counting a 503 as *known* evidence -- a
    draining scraper reporting three detail fetches in flight is answering the
    question, not failing to.
    """

    detail: ScraperReadyResponse


class JobSummary(BaseModel):
    """One in-memory scrape job, either kind, without its artifacts.

    ``GET /scrape_results/jobs`` returns these: a debugging view that strips
    ``artifacts`` on purpose. That is why this is a separate model rather than
    :class:`Job` with an optional field -- an optional field is still
    *serialised*, as ``"artifacts": null``, so one model for both endpoints
    would have put the key back into the response the endpoint exists to keep
    it out of. The suite said so immediately, which is the only reason this is
    two models and not one.
    """

    job_id: str
    run_id: str
    job_type: str
    status: str
    artifact_count: int
    error: str | None = None
    started_at: str | None = None
    queued_at: str | None = None

    # Listing fetches only.
    search_key: str | None = None
    scope: str | None = None
    attempt: int | None = None
    page_1_blocked: bool | None = None

    # Detail batches only.
    batch_id: str | None = None
    listing_count: int | None = None


class Job(JobSummary):
    """A job with its artifacts, as ``/scrape_results/jobs/completed`` returns.

    ``artifacts`` holds whatever the processors produced and is deliberately
    free-form: its element shape belongs to the parsers, and a model that
    guessed at it here would filter keys out of a parsed listing.
    """

    artifacts: List[Any]


class QueuedJobResponse(BaseModel):
    """``POST /scrape_results`` -- the job is queued, poll for the result."""

    job_id: str
    status: str


class QueuedBatchResponse(BaseModel):
    """``POST /scrape_detail/batch`` -- as above, plus what was accepted."""

    job_id: str
    batch_id: str
    status: str
    listing_count: int


class MarkFetchedResponse(BaseModel):
    """``POST /scrape_results/jobs/{job_id}/fetched``."""

    job_id: str
    status: str


class ScrapeDetailResponse(BaseModel):
    """``POST /scrape_detail`` -- the synchronous path.

    All seven return paths across ``scrape_detail_fetch``,
    ``scrape_detail_dummy`` and the unsupported-mode branch answer these same
    three keys, which is why none of them is optional. ``artifacts`` and
    ``meta`` stay free-form for the reason ``Job.artifacts`` does.
    """

    artifacts: List[Any]
    meta: Dict[str, Any]
    error: str | None = None
