"""The declared refusals a service can answer with, declared once in Python.

Plan 162 Stage AL / CAR-119.

**`docs/TESTING.md` §*What a status code means* is the owner; this module is
its importable copy** — the same arrangement `shared/db_vocabularies.py` has
with `db/migrations/`, one seam over. A service cannot read a markdown table
at runtime, so each meaning is a class here, carrying its status code as a
literal **in this file and nowhere else**: a handler raises the member, the
decorator's declaration resolves through it, and the call-site literal that
today restates the code 128 times across five packages becomes a lookup a
rename can propagate through.

**`Busy` and `DatabaseUnavailable` are both 503 and are different classes,
and that distinction is the entire point.** A 503 from `/ready` is the
service *answering the question* — jobs are in flight and the body carries
the count as evidence — while a 503 from a route whose database is down is
the service unable to work. A caller that cannot tell them apart loses the
evidence: `ops/coordination_drain.py` reads every service's `/ready`,
handles its 503 as failure, and blocks a drain with "service evidence
unavailable or malformed" instead of "2 jobs in flight". The code alone
cannot carry the difference, so the meaning must.

**Success and redirect codes are deliberately not classes.** A success is
declared by the decorator — `status_code=` and `response_model=` — and is
not raised, so there is no member for a call site to restate. The envelope
holds what a handler *refuses with*.

**`container_health` cannot import this module.** Its image copies only its
own package — the container holding the Docker socket grant carries as
little else as possible — so it holds a local copy,
``container_health/api_envelope.py``, and a rule asserts the copy equal in
both directions: the same constraint Stage AA solved for ``api_models.py``,
designed for here rather than discovered.
"""
from __future__ import annotations

from typing import Any, ClassVar

from fastapi import HTTPException


class ApiRefusal(HTTPException):
    """A declared refusal: the status code and its meaning, as one member.

    Raising one is what a converted handler does instead of retyping the
    code at the call site; ``meaning`` is the text the route's declaration
    carries, so the artifact and the handler cannot drift apart.
    """

    status_code: ClassVar[int]
    meaning: ClassVar[str]

    def __init__(
        self, detail: Any | None = None, headers: dict[str, str] | None = None
    ) -> None:
        super().__init__(
            type(self).status_code,
            self.meaning if detail is None else detail,
            headers,
        )


class BadRequest(ApiRefusal):
    """The request is unusable as sent — not a rejection of its content."""

    status_code = 400
    meaning = "The request is unusable as sent."


class Unauthenticated(ApiRefusal):
    """No usable credential was presented. Not 403."""

    status_code = 401
    meaning = "No usable credential was presented."


class Forbidden(ApiRefusal):
    """The credential does not grant what this route needs. Not 401."""

    status_code = 403
    meaning = "The credential does not grant what this route needs."


class NotFound(ApiRefusal):
    """The addressed thing does not exist — not a route that does not."""

    status_code = 404
    meaning = "The addressed thing does not exist."


class Conflict(ApiRefusal):
    """The request conflicts with the state the target is in."""

    status_code = 409
    meaning = "The request conflicts with the state the target is in."


class UnprocessableContent(ApiRefusal):
    """Well-formed, and the content is not usable. Not 400."""

    status_code = 422
    meaning = "The request is well-formed and its content is not usable."


class ServiceFailure(ApiRefusal):
    """The service failed while doing the work."""

    status_code = 500
    meaning = "The service failed while doing the work."


class Busy(ApiRefusal):
    """503 as an *answer*: refusing for now, with the evidence in the body.

    This is `/ready` under load — jobs in flight, a count to report — and a
    caller that treats it as a failure loses the evidence. It is not
    `DatabaseUnavailable`, and the four `/ready` decorators that today
    declare a dependency problem for this meaning are the live defect the
    meaning rule holds open.
    """

    status_code = 503
    meaning = "Busy: jobs are in flight, and the body carries the evidence."


class DatabaseUnavailable(ApiRefusal):
    """503 because the database this service needs is not answering."""

    status_code = 503
    meaning = "Database unavailable."


class DependencyUnreachable(ApiRefusal):
    """503 because a service this service depends on is not reachable."""

    status_code = 503
    meaning = "A dependency this service needs is not reachable."


#: Every declared refusal, by class name. Iterated by the rules that compare
#: this module against the generated contracts and against its
#: `container_health` copy; keyed by name so a lookup is a lookup, never an
#: inventory a new class can silently miss — the dict is built from the
#: classes, not beside them.
DECLARED_REFUSALS: dict[str, type[ApiRefusal]] = {
    refusal.__name__: refusal
    for refusal in (
        BadRequest,
        Unauthenticated,
        Forbidden,
        NotFound,
        Conflict,
        UnprocessableContent,
        ServiceFailure,
        Busy,
        DatabaseUnavailable,
        DependencyUnreachable,
    )
}
