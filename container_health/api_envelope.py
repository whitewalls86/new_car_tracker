"""The declared refusals, restated locally. A copy, and asserted to be one.

Plan 162 Stage AL / CAR-119.

**Local rather than imported from ``shared/``, on purpose, and for the same
reason as ``api_models.py`` beside it.** This package's Dockerfile copies
only itself — the container holding the Docker socket grant should carry as
little else as possible — so ``shared/api_envelope.py`` is restated here
rather than imported. The duplication is the cost of the image boundary and
is cheaper than widening what this container carries.

**The copy is asserted equal in both directions** —
``tests/rules/test_the_refusal_envelope_is_one_declaration.py`` compares
every class name, status code and meaning here against ``shared/``'s, so a
meaning changed in one file fails until the other moves with it. Neither
file may drift; ``docs/TESTING.md`` §*What a status code means* stays the
owner of both.
"""
from __future__ import annotations

from typing import Any, ClassVar

from fastapi import HTTPException


class ApiRefusal(HTTPException):
    """A declared refusal: the status code and its meaning, as one member."""

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
    """503 as an *answer*: refusing for now, with the evidence in the body."""

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
