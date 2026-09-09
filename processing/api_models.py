"""What ``processing``'s routes return.

Plan 162 Stage AA, gap G31. See ``shared/api_models.py`` for the filtering
hazard these are written against.

**``ArtifactResult`` is a union of return paths, not a shape.**
``POST /process/artifact/{artifact_id}`` answers ``{"artifact_id": ...,
**result}``, and ``result`` is whichever of ``_process_artifact``'s six exits
ran: a retry carries ``error``, a skip carries ``reason``, a completed
results-page carries ``listings_parsed`` plus everything
``write_srp_observations`` returned, and a completed detail page carries a
different set again. Twenty keys reach that endpoint and exactly two of them
are always present.

Declaring the union is the point rather than a compromise. Before this model
the contract said ``additionalProperties: true`` -- every key permitted,
including the ones no code path emits -- and a caller could read ``vin`` off a
results-page response forever without anything contradicting it. A union of
optionals says which twenty are possible, which is a smaller set than "any",
and it is derived from the handlers rather than chosen.
"""
from __future__ import annotations

from typing import Any, List

from pydantic import BaseModel


class BatchResponse(BaseModel):
    """``POST /process/batch``.

    The six counters are the shape ``airflow/dags/results_processing.py``
    reads, and the route's own docstring already claimed they were the
    response -- this is that claim moved somewhere a caller can check.
    """

    srp_count: int
    detail_count: int
    retry_count: int
    skip_count: int
    silver_write_failures: int
    status_write_failures: int


class ArtifactResult(BaseModel):
    """``POST /process/artifact/{artifact_id}`` -- the union described above.

    ``artifact_id`` and ``status`` are the two every exit sets. Everything
    below them belongs to some paths and not others, and which is which is
    recorded here rather than left to be inferred from a sample response.
    """

    artifact_id: int
    status: str

    # Retry and skip exits.
    error: str | None = None
    reason: str | None = None

    # Completed results-page exits.
    artifact_type: str | None = None
    listings_parsed: int | None = None
    upserted: int | None = None
    silver_written: int | None = None
    listing_ids: List[str] | None = None

    # Completed detail-page exits.
    listing_id: str | None = None
    listing_state: str | None = None
    unlisted: bool | None = None
    description: str | None = None
    vin: str | None = None
    vin_mapped: Any | None = None
    vin_collision_deleted: int | None = None
    carousel_upserted: int | None = None
    carousel_filtered: int | None = None
    deleted: int | None = None
    claims_released: int | None = None
