"""What ``container_health``'s routes return.

Plan 162 Stage AA, gap G31.

**Local rather than imported from ``shared/``, on purpose.** This package's
Dockerfile copies only itself -- "the container holding the Docker grant should
carry as little else as possible" -- so ``HealthResponse`` is restated here
rather than imported. That is the image boundary being honoured; the
duplication is the cost of it and is cheaper than widening what this container
carries.

**``known`` is ``Literal[True]``, and that is a measurement rather than a
style choice.** Both routes hardcode ``"known": True`` on their only return
path, so there is no code here that can emit ``known: False``. Two callers
guard on it anyway --
``ops/coordination_drain.py:_container_processes`` and
``ops/coordination_release.py:_auxiliary_still_stopped`` both refuse a payload
whose ``known`` is not ``True`` -- and those guards are *not* dead: they
protect against something that is not this service answering on this port, a
proxy error page or a misrouted request. What is dead is a test that fabricates
``{"known": False}`` and calls it "container-health said it did not know".
Declaring the literal is what lets the contract say which of those two readings
is true.
"""
from __future__ import annotations

from typing import List, Literal

from pydantic import BaseModel


class HealthResponse(BaseModel):
    """``GET /health`` -- process liveness, never dependency health.

    The route's own docstring records why it stays shallow: probing Docker from
    here would make this container's health a function of the proxy's.
    """

    ok: bool


class OneoffProcess(BaseModel):
    """One running Compose one-off, as drain evidence.

    Every field is read straight off Docker's inspect payload and every one of
    them can be absent there, which is why all three are optional. The
    aggregator that consumes this indexes ``service`` and would raise on a
    missing key -- that is its fail-closed behaviour and it is intended.
    """

    service: str | None = None
    container_id: str | None = None
    started_at: str | None = None


class OneoffProcessesResponse(BaseModel):
    """``GET /oneoff-processes`` -- live execution evidence for drain."""

    known: Literal[True]
    active_processes: int
    processes: List[OneoffProcess]


class ProjectStatusResponse(BaseModel):
    """``GET /project-status/{project}`` -- sibling-project activity."""

    known: Literal[True]
    project: str
    services: List[str]
