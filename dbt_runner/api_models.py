"""What ``dbt_runner``'s routes return.

Plan 162 Stage AA, gap G32. See ``shared/api_models.py`` for why these exist
and for the filtering hazard every model here is written against.

**Every optional field below is a return path, not a convenience.**
``dbt_build`` answers the same seventeen keys whether dbt succeeded or exited
non-zero -- the 500 carries the *whole* result as its detail, because a failed
build's ``stdout``, ``model_timings`` and ``likely_oom`` are exactly what the
caller needs. So the 500 is not an error message here; it is the 200 body
nested one level down, and it is declared that way.
"""
from __future__ import annotations

from typing import Any, List

from pydantic import BaseModel


class DocsStatusResponse(BaseModel):
    """``GET /dbt/docs/status`` -- whether ``target/index.html`` exists."""

    available: bool


class DocsGenerateResult(BaseModel):
    """``POST /dbt/docs/generate``'s body, and its 500's detail unchanged.

    ``ok`` is redundant with the status code and is kept because the admin
    panel reads it: dropping a field to tidy the model is the behaviour change
    this stage exists to make impossible to do by accident.
    """

    ok: bool
    returncode: int
    stdout: str
    stderr: str


class DocsGenerateError(BaseModel):
    """The 500, which nests the same body under ``detail``."""

    detail: DocsGenerateResult


class ModelTiming(BaseModel):
    """One row of dbt's ``run_results.json``, as the build report carries it.

    All three are ``None`` when the artifact holds a result missing them, which
    is why none of them is required.
    """

    unique_id: str | None = None
    status: str | None = None
    execution_time: float | None = None


class AnalyticsSnapshotResult(BaseModel):
    """``AnalyticsSnapshotManager.refresh()``, whose three shapes differ.

    ``not_attempted`` carries a ``reason`` and no timings; ``ok`` and
    ``failed`` carry timings and no reason; only ``failed`` carries ``error``.
    Declaring the union rather than the happy path is deliberate -- the panel
    renders whichever arrives.
    """

    ok: bool
    status: str
    reason: str | None = None
    attempted_at: str | None = None
    last_success_at: str | None = None
    duration_seconds: float | None = None
    error: Any | None = None


class BuildResult(BaseModel):
    """``POST /dbt/build``'s body, and its 500's detail unchanged.

    ``select`` is ``list[str] | str`` because the handler substitutes the
    literal ``"all"`` when no selection was given. That is a wart, and it is
    declared rather than smoothed over: changing it changes what every existing
    caller parses, which is a decision for whoever needs it and not a side
    effect of writing this model.
    """

    ok: bool
    dbt_ok: bool
    returncode: int
    likely_oom: bool
    invocation_id: str
    started_at: str
    ended_at: str
    duration_seconds: float
    select: List[str] | str
    exclude: List[str]
    full_refresh: bool
    cmd: str
    duckdb_threads: int
    duckdb_memory_limit: str
    model_timings: List[ModelTiming]
    stdout: str
    stderr: str
    analytics_snapshot: AnalyticsSnapshotResult


class BuildFailure(BaseModel):
    """The 500: the full build result, nested."""

    detail: BuildResult


class BuildInProgress(BaseModel):
    """The 409, whose detail is structured because the panel branches on it."""

    error: str
    message: str


class BuildInProgressResponse(BaseModel):
    detail: BuildInProgress
